"""btrfs-backup-ng: SSH Endpoint for managing remote operations.

This module provides the SSHEndpoint class, which integrates with SSHMasterManager
to handle SSH-based operations robustly, including btrfs send/receive commands.

Key features:
- Verifies remote filesystem is BTRFS before attempting transfers
- Tests SSH connectivity with a simple test file
- Uses mbuffer or pv if available to improve transfer reliability
- Provides detailed error reporting and verification
- Implements transfer method fallbacks for maximum reliability
- Includes direct SSH transfer functionality (previously in ssh_transfer.py)

Environment variables that affect behavior:
- BTRFS_BACKUP_PASSWORDLESS_ONLY: If set to 1/true/yes, disables the use of sudo
  -S flag and will only attempt passwordless sudo (-n flag), failing if a password
  would be required.
"""

import copy
import getpass
import os
import shlex
import subprocess
import threading
import time
import types
import uuid
from pathlib import Path
from subprocess import CompletedProcess
from threading import Lock
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, TypeVar, cast

# Handle paramiko import with proper typing
paramiko: Optional[types.ModuleType]
try:
    import paramiko as _paramiko

    paramiko = _paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    paramiko = None
    PARAMIKO_AVAILABLE = False

if TYPE_CHECKING:
    pass

# Handle pwd import with proper typing
_pwd: Optional[types.ModuleType]
try:
    import pwd as _pwd_module

    _pwd = _pwd_module
    _pwd_available = True
except ImportError:
    _pwd = None
    _pwd_available = False


from btrfs_backup_ng import __util__  # noqa: E402
from btrfs_backup_ng.__logger__ import logger  # noqa: E402
from btrfs_backup_ng.core.space import SpaceInfo  # noqa: E402
from btrfs_backup_ng.sshutil.master import SSHMasterManager  # noqa: E402

from .common import Endpoint  # noqa: E402

__all__ = ["SSHEndpoint"]

# Type variable for self in SSHEndpoint
_Self = TypeVar("_Self", bound="SSHEndpoint")

# Default idle timeout for remote btrfs receive (seconds).
# If no data arrives on stdin for this long, the receive process will terminate.
# This prevents zombie processes when the SSH connection is interrupted.
RECEIVE_IDLE_TIMEOUT = 300  # 5 minutes


def _build_receive_command(
    dest_path: str,
    use_sudo: bool = False,
    password_on_stdin: bool = False,
    idle_timeout: int = RECEIVE_IDLE_TIMEOUT,
) -> str:
    """Build a btrfs receive command with orphan process protection.

    The command is wrapped to ensure proper cleanup when the SSH session ends.
    This prevents zombie processes when the SSH connection is interrupted.

    Args:
        dest_path: Destination path for btrfs receive (should be shell-escaped)
        use_sudo: Whether to run with sudo
        password_on_stdin: If True, use 'sudo -S' (read password from stdin).
                          If False, use 'sudo -n' (passwordless sudo).
        idle_timeout: Seconds of stdin inactivity before terminating (default 300)

    Returns:
        Shell command string with orphan protection
    """
    # Build the base btrfs receive command
    if use_sudo:
        if password_on_stdin:
            # sudo -S reads password from stdin first, then btrfs receive reads data
            base_receive = f"sudo -S btrfs receive {dest_path}"
        else:
            # sudo -n for passwordless sudo
            base_receive = f"sudo -n btrfs receive {dest_path}"
    else:
        base_receive = f"btrfs receive {dest_path}"

    # Use a simple wrapper that sets up signal traps for cleanup.
    # When SSH disconnects, SIGHUP is sent to the shell, which triggers the trap.
    #
    # We run the receive command directly (not backgrounded) so stdin flows
    # through properly. The trap ensures cleanup on signals.
    #
    # Note: We don't use the named pipe approach as it can cause issues with
    # SSH's stdin handling and buffering.
    wrapped_cmd = (
        f"sh -c '"
        # Set up cleanup trap for disconnect signals
        # Using exec to replace the shell with btrfs receive ensures signals
        # are delivered directly to btrfs receive
        f'trap "" PIPE; exec {base_receive}'
        f"'"
    )

    return wrapped_cmd


class SSHEndpoint(Endpoint):
    """SSH-based endpoint for remote operations.

    This endpoint type handles connections to remote hosts via SSH.
    SSH username can be specified in three ways, in order of precedence:
    1. Via --ssh-username command line argument (highest priority)
    2. In the URI (e.g., ssh://user@host:/path)
    3. Current local user (fallback)

    When running as root with sudo, SSH identity files and usernames need special handling.

    Enhanced with direct SSH transfer capabilities for improved reliability:
    - Verifies remote filesystem is BTRFS before attempting transfers
    - Tests SSH connectivity with a simple test file
    - Uses mbuffer or pv if available to improve transfer reliability
    - Provides detailed error reporting and verification
    - Implements transfer method fallbacks for maximum reliability

    Note: This class incorporates the functionality previously provided by
    the separate ssh_transfer.py module, offering an integrated solution for
    reliable BTRFS transfers over SSH.
    """

    _is_remote = True
    _supports_multiprocessing = True

    # Class-level cache for sudo passwords, keyed by "user@hostname"
    # This allows password sharing across multiple SSHEndpoint instances for the same host
    _sudo_password_cache: Dict[str, str] = {}

    def __init__(
        self,
        hostname: str,
        config: Optional[Dict[str, Any]] = None,
        *,
        ssh_sudo: bool = False,
        ssh_identity_file: Optional[str] = None,
        username: Optional[str] = None,
        port: Optional[int] = None,
        ssh_opts: Optional[List[str]] = None,
        agent_forwarding: bool = False,
        passwordless: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize the SSH endpoint.

        Args:
            hostname: Remote hostname
            config: Configuration dictionary
            **kwargs: Additional keyword arguments passed to parent class
        """
        # Deep copy config to avoid shared references in multiprocessing
        if config is not None:
            config = copy.deepcopy(config)
            logger.debug("SSHEndpoint: Using provided config (deep copied)")
        else:
            config = {}
            logger.debug("SSHEndpoint: No config provided, using empty dict")

        # Initialize our config before calling parent init
        self.config: Dict[str, Any] = config or {}
        self.hostname: str = hostname
        self._instance_id: str = f"{os.getpid()}_{uuid.uuid4().hex[:8]}"
        self._lock: Lock = Lock()
        self._last_receive_log: Optional[str] = None
        self._last_transfer_snapshot: Optional[bool] = None
        logger.debug(
            "SSHEndpoint: Config keys before parent init: %s", list(config.keys())
        )
        self._cached_sudo_password: Optional[str] = None  # Add this line

        # Call parent init with both config and kwargs
        super().__init__(config=self.config, **kwargs)

        self.hostname = hostname
        logger.debug("SSHEndpoint initialized with hostname: %s", self.hostname)
        logger.debug("SSHEndpoint: kwargs provided: %s", list(kwargs.keys()))
        self.config["username"] = self.config.get("username")
        self.config["port"] = self.config.get("port")
        self.config["ssh_opts"] = self.config.get("ssh_opts", [])
        self.config["agent_forwarding"] = self.config.get("agent_forwarding", False)

        # Initialize tracking variables for verification
        self._last_receive_log = None
        self._last_transfer_snapshot = None

        # Cache for diagnostics to avoid redundant testing
        self._diagnostics_cache: Dict[str, Tuple[Dict[str, bool], float]] = {}
        self._diagnostics_cache_timeout = 300  # 5 minutes

        self.config["path"] = self.config.get("path", "/")
        self.config["ssh_sudo"] = self.config.get("ssh_sudo", False)
        self.config["passwordless"] = self.config.get("passwordless", False)
        self.config["simple_progress"] = kwargs.get("simple_progress", True)
        self.config["ssh_password_fallback"] = config.get(
            "ssh_password_fallback", False
        ) or kwargs.get("ssh_password_fallback", False)

        # Log important settings for troubleshooting
        logger.info(
            "SSH endpoint configuration: hostname=%s, sudo=%s, passwordless=%s, simple_progress=%s",
            self.hostname,
            self.config.get("ssh_sudo", False),
            self.config.get("passwordless", False),
            self.config.get("simple_progress", True),
        )

        # Username handling with clear precedence:
        # 1. Explicitly provided username (from command line via --ssh-username)
        # 2. Username from the URL (ssh://user@host/path)
        # 3. SUDO_USER environment variable if running as root with sudo
        # 4. Current user as fallback
        if not self.config.get("username"):
            # No username provided in config, check sudo environment
            if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
                self.config["username"] = os.environ.get("SUDO_USER")
                logger.debug(
                    "Using sudo original user as username: %s", self.config["username"]
                )
                logger.debug(
                    "Running as root (euid=0) with SUDO_USER=%s",
                    os.environ.get("SUDO_USER"),
                )
            else:
                logger.debug("Not running as sudo, getting current user")
                try:
                    self.config["username"] = getpass.getuser()
                    logger.debug(
                        "Using current user as username: %s", self.config["username"]
                    )
                except Exception as e:
                    # Fallback if getpass.getuser() fails
                    logger.warning(f"Error getting current username: {e}")
                    logger.debug(
                        f"getpass.getuser() failed with exception: {e}", exc_info=True
                    )
                    # Try environment variables
                    username = os.environ.get("USER") or os.environ.get("USERNAME")
                    logger.debug(
                        "Trying environment variables: USER=%s, USERNAME=%s",
                        os.environ.get("USER"),
                        os.environ.get("USERNAME"),
                    )
                    if not username:
                        # Last resort fallback
                        username = "btrfs-backup-user"
                        logger.warning(f"Using default fallback username: {username}")
                        logger.debug(
                            "No username found in environment, using hardcoded fallback"
                        )
                    self.config["username"] = username
                    logger.debug(f"Using fallback username: {username}")
        else:
            logger.debug(
                "Using explicitly configured username: %s", self.config["username"]
            )

        identity_file = self.config.get("ssh_identity_file")
        logger.debug("SSH identity file from config: %s", identity_file)

        # Auto-detect SSH key when running as sudo without explicit identity file
        running_as_sudo = os.geteuid() == 0 and os.environ.get("SUDO_USER")
        if not identity_file and running_as_sudo:
            sudo_user = os.environ.get("SUDO_USER")
            sudo_user_home = None
            if _pwd_available and _pwd is not None and sudo_user is not None:
                try:
                    sudo_user_home = _pwd.getpwnam(sudo_user).pw_dir
                except Exception:
                    pass
            if sudo_user_home is None:
                sudo_user_home = (
                    f"/home/{sudo_user}" if sudo_user != "root" else "/root"
                )

            # Check for common SSH key types
            for key_name in ["id_ed25519", "id_rsa", "id_ecdsa"]:
                key_path = os.path.join(sudo_user_home, ".ssh", key_name)
                if os.path.exists(key_path) and os.access(key_path, os.R_OK):
                    identity_file = key_path
                    self.config["ssh_identity_file"] = identity_file
                    logger.debug(
                        "Auto-detected SSH key for sudo user %s: %s",
                        sudo_user,
                        identity_file,
                    )
                    break

        if identity_file:
            running_as_sudo = os.geteuid() == 0 and os.environ.get("SUDO_USER")
            logger.debug(
                "Running as sudo check: euid=%d, SUDO_USER=%s, running_as_sudo=%s",
                os.geteuid(),
                os.environ.get("SUDO_USER"),
                running_as_sudo,
            )
            if running_as_sudo:
                sudo_user = os.environ.get("SUDO_USER")
                logger.debug("Processing identity file for sudo user: %s", sudo_user)
                sudo_user_home = None
                if sudo_user:
                    sudo_user_home = None
                    if _pwd_available and _pwd is not None:
                        try:
                            sudo_user_home = _pwd.getpwnam(sudo_user).pw_dir
                            logger.debug(
                                f"Found home directory for sudo user: {sudo_user_home}"
                            )
                        except Exception as e:
                            logger.warning(
                                f"Error getting home directory for sudo user: {e}"
                            )
                            logger.debug(f"pwd.getpwnam() failed: {e}", exc_info=True)
                            # Fall back to default location
                            sudo_user_home = None

                    # Use fallback if we couldn't get the home directory
                    if sudo_user_home is None:
                        sudo_user_home = (
                            f"/home/{sudo_user}" if sudo_user != "root" else "/root"
                        )
                        logger.debug(f"Using fallback home directory: {sudo_user_home}")
                if sudo_user_home and identity_file.startswith("~"):
                    identity_file = identity_file.replace("~", sudo_user_home, 1)
                    logger.debug("Expanded ~ in identity file path: %s", identity_file)
                if sudo_user_home and not os.path.isabs(identity_file):
                    identity_file = os.path.join(sudo_user_home, identity_file)
                    logger.debug(
                        "Converted relative path to absolute: %s", identity_file
                    )
                self.config["ssh_identity_file"] = identity_file
                logger.debug("Final identity file path: %s", identity_file)
                try:
                    id_file = Path(identity_file).absolute()
                    if not id_file.exists():
                        logger.warning("SSH identity file does not exist: %s", id_file)
                        logger.warning(
                            "When running with sudo, ensure the identity file path is absolute and accessible"
                        )
                    elif not os.access(str(id_file), os.R_OK):
                        logger.warning("SSH identity file is not readable: %s", id_file)
                        logger.warning("Check file permissions: chmod 600 %s", id_file)
                    else:
                        logger.debug("Using SSH identity file: %s (verified)", id_file)
                except Exception as e:
                    logger.warning("Error processing identity file path: %s", e)
                    self.config["ssh_identity_file"] = identity_file
            else:
                logger.debug("Using SSH identity file: %s", identity_file)

        # Log the final configuration
        logger.debug("SSH path: %s", self.config["path"])
        logger.debug("SSH username: %s", self.config["username"])
        logger.debug("SSH hostname: %s", self.hostname)
        logger.debug("SSH port: %s", self.config["port"])
        logger.debug("SSH sudo: %s", self.config["ssh_sudo"])

        # Centralized agent forwarding logic
        logger.debug("Applying agent forwarding configuration")
        self._apply_agent_forwarding()

        logger.debug(
            "Creating SSHMasterManager with: hostname=%s, username=%s, port=%s",
            self.hostname,
            self.config["username"],
            self.config["port"],
        )
        # Allow SSH password auth if flag is set or environment variable is set
        allow_password = self.config.get("ssh_password_fallback", False) or bool(
            os.environ.get("BTRFS_BACKUP_SSH_PASSWORD")
        )
        if allow_password:
            logger.debug("SSH password auth enabled")

        self.ssh_manager: SSHMasterManager = SSHMasterManager(
            hostname=self.hostname,
            username=self.config["username"],
            port=self.config["port"],
            ssh_opts=self.config["ssh_opts"],
            persist="60",
            debug=True,
            identity_file=self.config.get("ssh_identity_file"),
            allow_password_auth=allow_password,
        )
        logger.debug("SSHMasterManager created successfully")

        self._lock = Lock()  # Already set in type definition
        self._instance_id = (
            f"{os.getpid()}_{uuid.uuid4().hex[:8]}"  # Already set in type definition
        )
        logger.debug("SSHEndpoint instance ID: %s", self._instance_id)

        # Force ssh_sudo to True if requested in kwargs or config
        cli_ssh_sudo = kwargs.get("ssh_sudo") or (config and config.get("ssh_sudo"))
        logger.debug(
            f"[SSHEndpoint.__init__] Initial ssh_sudo: {self.config.get('ssh_sudo', False)}, CLI/config ssh_sudo: {cli_ssh_sudo}"
        )
        logger.debug(
            "SSH sudo propagation check: kwargs.ssh_sudo=%s, config.ssh_sudo=%s",
            kwargs.get("ssh_sudo"),
            config.get("ssh_sudo"),
        )
        if cli_ssh_sudo and not self.config.get("ssh_sudo", False):
            logger.warning("SSH sudo flag not properly propagated, forcing to True")
            self.config["ssh_sudo"] = True
        logger.debug(
            f"[SSHEndpoint.__init__] Final ssh_sudo: {self.config.get('ssh_sudo', False)}"
        )

        # Password collection is deferred to _prepare() -> start_master()
        # This allows key-based auth to be tried first, and only prompts for
        # password if keys fail. The SSHMasterManager handles this flow.

        # Skip diagnostics during __init__ - they will run in _prepare() after
        # the SSH master connection is established. This avoids connection issues
        # and redundant password prompts.
        logger.debug(
            "SSHEndpoint initialization completed (diagnostics deferred to _prepare)"
        )

    def _prepare(self) -> None:
        """Prepare the SSH endpoint by starting the master connection.

        This method is called by prepare() in the parent class and ensures
        the SSH master connection is established before any commands are run.
        This is essential for password authentication fallback to work properly.
        """
        logger.debug("Preparing SSH endpoint, starting master connection...")

        # Password should already be collected in __init__ if ssh_password_fallback is enabled
        # This is just a fallback in case _prepare is called without __init__ password collection

        # Start the SSH master connection (handles password fallback if needed)
        if not self.ssh_manager.start_master():
            logger.error(
                f"Failed to establish SSH connection to {self.config.get('username')}@{self.hostname}"
            )
            raise ConnectionError(
                f"Could not establish SSH connection to {self.hostname}. "
                "Check SSH credentials (keys or password)."
            )

        logger.debug("SSH master connection established successfully")

        # After master connection, sync password caches
        # If SSH master collected a password, use it for sudo too (common scenario)
        if (
            hasattr(self.ssh_manager, "_cached_ssh_password")
            and self.ssh_manager._cached_ssh_password
            and not self._cached_sudo_password
        ):
            logger.debug("Syncing SSH password to sudo cache (same credentials)")
            self._cached_sudo_password = self.ssh_manager._cached_ssh_password
            cache_key = f"{self.config.get('username', 'unknown')}@{self.hostname}"
            SSHEndpoint._sudo_password_cache[cache_key] = (
                self.ssh_manager._cached_ssh_password
            )

        # Now run diagnostics to detect capabilities using the actual configured path
        try:
            diag_path = self.config.get("path", "/")
            self._run_diagnostics(path=diag_path)
            logger.debug("SSH diagnostics completed")
        except Exception as e:
            logger.debug(f"SSH diagnostics failed (non-fatal): {e}")

    def __repr__(self) -> str:
        username: str = self.config.get("username", "")
        return f"(SSH) {username}@{self.hostname}:{self.config['path']}"

    def delete_snapshots(self, snapshots: List[Any], **kwargs: Any) -> None:
        """Delete the given snapshots (subvolumes) on the remote host via SSH."""
        for snapshot in snapshots:
            if hasattr(snapshot, "locks") and (
                snapshot.locks or getattr(snapshot, "parent_locks", False)
            ):
                logger.info("Skipping locked snapshot: %s", snapshot)
                continue

            # Handle remote path normalization properly
            if hasattr(snapshot, "get_path"):
                remote_path = str(snapshot.get_path())
            else:
                remote_path = str(snapshot)

            # Ensure the path is properly normalized for remote execution
            remote_path = self._normalize_path(remote_path)

            # Verify the path exists before attempting deletion
            test_cmd = ["test", "-d", remote_path]
            try:
                test_result = self._exec_remote_command(
                    test_cmd,
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                if test_result.returncode != 0:
                    logger.warning(
                        f"Snapshot path does not exist for deletion: {remote_path}"
                    )
                    continue
            except Exception as e:
                logger.warning(f"Could not verify snapshot path {remote_path}: {e}")
                continue

            # Build deletion command with proper sudo handling
            cmd = ["btrfs", "subvolume", "delete", remote_path]
            logger.debug("Executing remote deletion command: %s", cmd)

            try:
                # Use retry mechanism for commands that may require authentication
                use_sudo = self.config.get("ssh_sudo", False)
                if use_sudo:
                    result = self._exec_remote_command_with_retry(
                        cmd,
                        max_retries=2,
                        check=False,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                else:
                    result = self._exec_remote_command(
                        cmd,
                        check=False,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                if result.returncode == 0:
                    logger.info("Deleted remote snapshot subvolume: %s", remote_path)
                else:
                    stderr = (
                        result.stderr.decode(errors="replace").strip()
                        if hasattr(result, "stderr") and result.stderr
                        else "Unknown error"
                    )
                    # Check for common btrfs deletion errors
                    if "No such file or directory" in stderr:
                        logger.warning(
                            f"Snapshot already deleted or path not found: {remote_path}"
                        )
                    elif "statfs" in stderr.lower():
                        logger.error(
                            f"Filesystem access error when deleting {remote_path}: {stderr}"
                        )
                        logger.error(
                            "This may indicate the remote path is not accessible or the filesystem is unmounted"
                        )
                    else:
                        logger.error(
                            f"Failed to delete remote snapshot {remote_path}: {stderr}"
                        )
            except Exception as e:
                logger.error(
                    f"Exception while deleting remote snapshot {remote_path}: {e}"
                )
                # Log additional diagnostic information
                logger.debug(f"Deletion exception details: {e}", exc_info=True)

    def delete_old_snapshots(self, keep: int) -> None:
        """
        Delete old snapshots on the remote host, keeping only the most recent `keep` unlocked snapshots.
        """
        snapshots = self.list_snapshots()  # type: ignore
        unlocked = [  # type: ignore
            s  # type: ignore
            for s in snapshots  # type: ignore
            if not getattr(s, "locks", False) and not getattr(s, "parent_locks", False)  # type: ignore
        ]
        if keep <= 0 or len(unlocked) <= keep:  # type: ignore
            logger.debug(
                "No unlocked snapshots to delete (keep=%d, unlocked=%d)",
                keep,
                len(unlocked),  # type: ignore
            )
            return
        to_delete = unlocked[:-keep]  # type: ignore
        for snap in to_delete:  # type: ignore
            logger.info("Deleting old remote snapshot: %s", str(snap))  # type: ignore
            self.delete_snapshots([snap])

    def _apply_agent_forwarding(self) -> None:
        """
        Apply SSH agent forwarding if enabled in config.
        """
        agent_forwarding: bool = self.config.get("agent_forwarding", False)
        ssh_auth_sock: Optional[str] = os.environ.get("SSH_AUTH_SOCK")
        ssh_opts: List[str] = self.config.get("ssh_opts", []).copy()

        if agent_forwarding:
            if ssh_auth_sock:
                logger.info(
                    "Enabling SSH agent forwarding (IdentityAgent=%s)", ssh_auth_sock
                )
                # Avoid duplicate IdentityAgent entries
                identity_agent_opt = f"IdentityAgent={ssh_auth_sock}"
                if identity_agent_opt not in ssh_opts:
                    ssh_opts.append(identity_agent_opt)
                self.config["ssh_opts"] = ssh_opts
            else:
                logger.warning(
                    "SSH agent forwarding requested but SSH_AUTH_SOCK is not set. Agent forwarding will not work."
                )

    def _run_diagnostics(
        self, path: str = "/", force_refresh: bool = False
    ) -> Dict[str, bool]:
        """Run SSH and sudo diagnostics to identify potential issues.

        Attempts several tests to verify SSH connectivity, btrfs availability,
        sudo access, and filesystem type. Updates self.config["passwordless_sudo_available"]
        based on sudo test results.

        Args:
            path: Remote path to test for btrfs operations
            force_refresh: If True, bypass cache and run fresh diagnostics

        Returns:
            Dictionary with test results (True=passed, False=failed):
            {
                'ssh_connection': bool,  # Basic SSH connectivity
                'btrfs_command': bool,   # btrfs command exists on remote
                'passwordless_sudo': bool,  # Sudo without password works
                'sudo_btrfs': bool,      # Can run btrfs with sudo
                'write_permissions': bool,  # Can write to path
                'btrfs_filesystem': bool  # Path is on btrfs filesystem
            }
        """
        # Check cache first to avoid redundant testing
        current_time = time.time()
        cache_key = f"{self.hostname}:{path}"

        if not force_refresh and cache_key in self._diagnostics_cache:
            cached_result, cache_time = self._diagnostics_cache[cache_key]
            if current_time - cache_time < self._diagnostics_cache_timeout:
                logger.debug(
                    f"Using cached diagnostics for {cache_key} (age: {current_time - cache_time:.1f}s)"
                )
                # Update config with cached result
                self.config["passwordless_sudo_available"] = cached_result.get(
                    "passwordless_sudo", False
                )
                return cached_result
            else:
                logger.debug(
                    f"Diagnostics cache expired for {cache_key}, running fresh tests"
                )

        if force_refresh:
            logger.debug(f"Forcing fresh diagnostics for {cache_key}")
        else:
            logger.debug(f"Running fresh diagnostics for {cache_key}")

        # Initialize results dictionary
        results: Dict[str, bool] = {
            "ssh_connection": False,
            "btrfs_command": False,
            "passwordless_sudo": False,
            "sudo_btrfs": False,
            "write_permissions": False,
            "btrfs_filesystem": False,
        }

        # Test SSH Connection
        logger.debug("Testing SSH connection...")
        try:
            cmd_result: CompletedProcess[Any] = self._exec_remote_command(
                ["echo", "SSH connection successful"],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            results["ssh_connection"] = cmd_result.returncode == 0
            if not results["ssh_connection"]:
                logger.error("SSH connection test failed")
                logger.debug(
                    f"SSH connection stderr: {cmd_result.stderr.decode() if cmd_result.stderr else 'None'}"
                )
                return results
            else:
                logger.debug("SSH connection test passed")
        except Exception as e:
            logger.error(f"SSH connection test failed: {e}")
            logger.debug(f"SSH connection exception details: {e}", exc_info=True)
            return results

        # Test btrfs command availability
        logger.debug("Testing btrfs command availability...")
        try:
            result = self._exec_remote_command(["command", "-v", "btrfs"], check=False)
            results["btrfs_command"] = result.returncode == 0
            if results["btrfs_command"]:
                btrfs_path = result.stdout.decode().strip() if result.stdout else ""
                logger.debug(f"btrfs command found: {btrfs_path}")
                logger.debug(f"btrfs command path: {btrfs_path}")
            else:
                logger.error("btrfs command not found on remote host")
                logger.debug(
                    f"btrfs command check stderr: {result.stderr.decode() if result.stderr else 'None'}"
                )
        except Exception as e:
            logger.error(f"Error checking btrfs command: {e}")
            logger.debug(f"btrfs command check exception: {e}", exc_info=True)

        # Test passwordless sudo for btrfs commands
        # We test with btrfs directly since sudoers rules often only allow specific commands
        logger.debug("Testing passwordless sudo for btrfs...")
        try:
            # Use retry mechanism for sudo btrfs testing to handle potential authentication issues
            result = self._exec_remote_command_with_retry(
                ["sudo", "-n", "btrfs", "--version"], max_retries=2, check=False
            )
            # Both passwordless_sudo and sudo_btrfs are set based on btrfs test
            # This handles sudoers rules that only allow btrfs, not generic sudo
            results["passwordless_sudo"] = result.returncode == 0
            results["sudo_btrfs"] = result.returncode == 0
            if results["passwordless_sudo"]:
                logger.debug("Passwordless sudo for btrfs is available")
            else:
                logger.warning("Passwordless sudo is not available")
                logger.debug(
                    f"Passwordless sudo stderr: {result.stderr.decode() if result.stderr else 'None'}"
                )
        except Exception as e:
            logger.error(f"Error checking passwordless sudo: {e}")
            logger.debug(f"Passwordless sudo exception: {e}", exc_info=True)

        # Test write permissions
        logger.debug(f"Testing write permissions to path: {path}")
        try:
            test_file = f"{path}/.btrfs-backup-write-test-{uuid.uuid4().hex[:8]}"
            logger.debug(f"Testing write with test file: {test_file}")
            result = self._exec_remote_command(["touch", test_file], check=False)
            if result.returncode == 0:
                self._exec_remote_command(["rm", "-f", test_file], check=False)
                results["write_permissions"] = True
                logger.debug(f"Path is directly writable: {path}")
                logger.debug("Direct write test passed")
            else:
                logger.debug(
                    f"Direct write failed, trying with sudo. Error: {result.stderr.decode() if result.stderr else 'None'}"
                )

                # Try with passwordless sudo first
                result = self._exec_remote_command_with_retry(
                    ["sudo", "-n", "touch", test_file], max_retries=2, check=False
                )
                if result.returncode == 0:
                    self._exec_remote_command_with_retry(
                        ["sudo", "-n", "rm", "-f", test_file],
                        max_retries=2,
                        check=False,
                    )
                    results["write_permissions"] = True
                    logger.debug(f"Path is writable with passwordless sudo: {path}")
                    logger.debug("Passwordless sudo write test passed")
                else:
                    # If passwordless sudo fails, check if we have password-based sudo available
                    logger.debug(
                        "Passwordless sudo write failed, checking if password-based sudo could work"
                    )

                    # If ssh_sudo is enabled and we're not in passwordless mode,
                    # assume write permissions will work with password-based sudo
                    use_sudo = self.config.get("ssh_sudo", False)
                    passwordless_available = results.get("passwordless_sudo", False)

                    if use_sudo and not passwordless_available:
                        # We have sudo configured but not passwordless - likely will work with password
                        results["write_permissions"] = True
                        logger.debug(
                            f"Path likely writable with password-based sudo: {path}"
                        )
                        logger.debug(
                            "Assuming write permissions available via password-based sudo"
                        )
                    else:
                        # No sudo configuration or other issue
                        results["write_permissions"] = False
                        logger.error(f"Path is not writable (even with sudo): {path}")
                        logger.debug(
                            f"Sudo write failed. Error: {result.stderr.decode() if result.stderr else 'None'}"
                        )
                        logger.debug(
                            "Consider enabling ssh_sudo in configuration if elevated permissions are needed"
                        )
        except Exception as e:
            logger.error(f"Error checking write permissions: {e}")
            logger.debug(f"Write permissions exception: {e}", exc_info=True)

        # Test if filesystem is btrfs
        logger.debug(f"Testing filesystem type for path: {path}")
        try:
            result = self._exec_remote_command(
                ["stat", "-f", "-c", "%T", path],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            fs_type = result.stdout.decode().strip() if result.stdout else ""
            results["btrfs_filesystem"] = fs_type == "btrfs"
            if results["btrfs_filesystem"]:
                logger.debug(f"Path is on a btrfs filesystem: {path}")
                logger.debug(f"Filesystem type confirmed: {fs_type}")
            else:
                logger.error(f"Path is not on a btrfs filesystem (found: {fs_type})")
                logger.debug(f"Expected 'btrfs', got '{fs_type}'")
        except Exception as e:
            logger.error(f"Error checking filesystem type: {e}")
            logger.debug(f"Filesystem type check exception: {e}", exc_info=True)

        # Log summary of results
        logger.debug("Diagnostic tests completed, generating summary...")
        all_passed = all(results.values())

        # Only show full summary at INFO level if there are failures
        if all_passed:
            logger.debug("All diagnostic tests passed")
            for test_name, test_passed in results.items():
                logger.debug(f"Test {test_name}: PASSED")
        else:
            # Show summary at INFO level only for failures
            failed_tests = [t for t, passed in results.items() if not passed]
            logger.debug(f"Some diagnostic tests failed: {failed_tests}")
            logger.info("\nDiagnostic Summary:")
            logger.info("-" * 50)
            for test_name, test_passed in results.items():
                status = "PASS" if test_passed else "FAIL"
                logger.info(f"{test_name.replace('_', ' ').title():20} {status}")
            logger.info("-" * 50)

        # Provide specific recommendations based on what failed
        if not all(results.values()):
            if not results["sudo_btrfs"]:
                self._show_sudoers_fix_instructions()

            if not results["write_permissions"]:
                logger.info("\nTo fix write permissions:")
                logger.info(
                    f"Ensure that user '{self.config.get('username')}' has write permission to {path}"
                )

                # Provide more specific guidance based on sudo configuration
                use_sudo = self.config.get("ssh_sudo", False)
                passwordless_sudo = results.get("passwordless_sudo", False)

                if use_sudo and not passwordless_sudo:
                    logger.info("OR configure passwordless sudo for write operations:")
                    logger.info("  sudo visudo")
                    logger.info(
                        f"  Add: {self.config.get('username')} ALL=(ALL) NOPASSWD: /usr/bin/btrfs"
                    )
                elif not use_sudo:
                    logger.info(
                        "OR enable ssh_sudo in configuration to use elevated permissions:"
                    )
                    logger.info("  Set ssh_sudo: true in your configuration")
                else:
                    logger.info(
                        "OR ensure sudo is configured properly to allow writing to this location."
                    )

                logger.info(
                    "\nNote: Write permission errors during diagnostics may be false negatives"
                )
                logger.info(
                    "if password-based sudo is available but passwordless sudo is not configured."
                )

            if not results["btrfs_filesystem"]:
                logger.info("\nTo fix filesystem type:")
                logger.info(f"The path {path} must be on a btrfs filesystem.")
                logger.info("btrfs-backup-ng cannot work with other filesystem types.")

        # Store passwordless sudo detection result for automatic use
        self.config["passwordless_sudo_available"] = results["passwordless_sudo"]
        if results["passwordless_sudo"]:
            logger.debug(
                "Auto-detected passwordless sudo capability - will use passwordless mode by default"
            )
        else:
            logger.debug(
                "Passwordless sudo not available - will require password prompts or manual configuration"
            )

        # Cache the results to avoid redundant testing
        self._diagnostics_cache[cache_key] = (results.copy(), current_time)
        logger.debug(f"Cached diagnostics for {cache_key}")

        return results

    def _show_sudoers_fix_instructions(self) -> None:
        """Show instructions for fixing sudoers configuration."""
        logger.info("\nTo fix sudo access:")
        user = self.config.get("username")
        logger.info("Add one of these lines to /etc/sudoers via 'sudo visudo':")
        logger.info("\n# Full access to btrfs commands:")
        logger.info(f"{user} ALL=(ALL) NOPASSWD: /usr/bin/btrfs")
        logger.info("\n# Or more restricted access:")
        logger.info(
            f"{user} ALL=(ALL) NOPASSWD: /usr/bin/btrfs subvolume*, /usr/bin/btrfs send*, /usr/bin/btrfs receive*"
        )

    def get_id(self) -> str:
        """Return a unique identifier for this SSH endpoint."""
        username: str = self.config.get("username", "")
        username_part: str = f"{username}@" if username else ""
        return f"ssh://{username_part}{self.hostname}:{self.config['path']}"

    def get_space_info(self, path: Optional[str] = None) -> SpaceInfo:
        """Get space information for the remote endpoint's destination path.

        Queries filesystem space and btrfs quota information on the remote host
        via SSH, using a single compound command to minimize round trips.

        Args:
            path: Optional path to check. If None, uses self.config['path'].

        Returns:
            SpaceInfo with filesystem and quota information.
        """
        import json as _json

        if path is None:
            path = str(self.config["path"])
        else:
            path = str(path)

        logger.debug("Getting space info for remote path: %s", path)

        # Use a compound Python command to get both statvfs and quota info
        # This minimizes SSH round trips
        python_script = f"""
import os, json, subprocess
path = {path!r}
result = {{"path": path, "error": None}}
try:
    s = os.statvfs(path)
    result["total"] = s.f_blocks * s.f_frsize
    result["used"] = (s.f_blocks - s.f_bfree) * s.f_frsize
    result["available"] = s.f_bavail * s.f_frsize
    result["source"] = "statvfs"
except Exception as e:
    result["error"] = str(e)
    result["total"] = 0
    result["used"] = 0
    result["available"] = 0
    result["source"] = "error"

# Try to get quota info
result["quota_enabled"] = False
result["quota_limit"] = None
result["quota_used"] = None
try:
    qgroup_cmd = ["btrfs", "qgroup", "show", "-reF", path]
    proc = subprocess.run(qgroup_cmd, capture_output=True, text=True, timeout=30)
    if proc.returncode == 0:
        lines = proc.stdout.strip().splitlines()
        for line in lines:
            line = line.strip()
            if not line or line.startswith("qgroupid") or line.startswith("-"):
                continue
            parts = line.split()
            if len(parts) >= 5:
                try:
                    rfer = int(parts[1])
                    max_rfer = parts[3]
                    result["quota_enabled"] = True
                    result["quota_used"] = rfer
                    if max_rfer.lower() != "none":
                        result["quota_limit"] = int(max_rfer)
                    result["source"] = "statvfs+btrfs_qgroup"
                    break
                except (ValueError, IndexError):
                    pass
except Exception:
    pass

print(json.dumps(result))
"""

        try:
            # Execute the compound command on remote host
            result = self._exec_remote_command(
                ["python3", "-c", python_script],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            if result.returncode == 0 and result.stdout:
                output = result.stdout.decode("utf-8").strip()
                data = _json.loads(output)

                if data.get("error"):
                    logger.warning("Remote space check error: %s", data["error"])

                return SpaceInfo(
                    path=data["path"],
                    total_bytes=data["total"],
                    used_bytes=data["used"],
                    available_bytes=data["available"],
                    quota_enabled=data.get("quota_enabled", False),
                    quota_limit=data.get("quota_limit"),
                    quota_used=data.get("quota_used"),
                    source=data.get("source", "remote"),
                )
            else:
                # Fallback to basic df command if Python approach fails
                logger.debug("Python space check failed, falling back to df command")
                return self._get_space_info_fallback(path)

        except Exception as e:
            logger.warning("Remote space check failed: %s", e)
            return self._get_space_info_fallback(path)

    def _get_space_info_fallback(self, path: str) -> SpaceInfo:
        """Fallback space info using basic df command.

        Used when the Python-based approach fails (e.g., Python not available
        on remote host).
        """
        try:
            # Use df with POSIX output format
            result = self._exec_remote_command(
                ["df", "-P", "-B1", path],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            if result.returncode == 0 and result.stdout:
                lines = result.stdout.decode("utf-8").strip().splitlines()
                if len(lines) >= 2:
                    # Parse: Filesystem 1-blocks Used Available Capacity Mounted
                    parts = lines[1].split()
                    if len(parts) >= 4:
                        total = int(parts[1])
                        used = int(parts[2])
                        available = int(parts[3])
                        return SpaceInfo(
                            path=path,
                            total_bytes=total,
                            used_bytes=used,
                            available_bytes=available,
                            source="df",
                        )
        except Exception as e:
            logger.warning("Fallback df space check failed: %s", e)

        # Return empty SpaceInfo if all methods fail
        logger.error("Could not determine space info for remote path: %s", path)
        return SpaceInfo(
            path=path,
            total_bytes=0,
            used_bytes=0,
            available_bytes=0,
            source="error",
        )

    def _build_remote_command(self, command: List[str]) -> List[str]:
        """Prepare a remote command with optional sudo."""
        if not command:
            return command

        # Ensure all elements are strings
        command = [str(c) for c in command]

        # Check if the ssh_sudo flag is set and command needs sudo
        needs_sudo = (
            self.config.get("ssh_sudo", False)
            and command
            and (
                command[0] == "btrfs"
                or (command[0] == "test" and len(command) > 2 and "-d" in command)
            )
        )

        if needs_sudo:
            cmd_str: str = " ".join(command)
            logger.debug("Using sudo for remote command: %s", cmd_str)

            # Check config setting, environment variable, and auto-detected capability
            passwordless_config = self.config.get("passwordless", False)
            passwordless_env = os.environ.get(
                "BTRFS_BACKUP_PASSWORDLESS_ONLY", "0"
            ).lower() in ("1", "true", "yes")

            # Auto-detect passwordless sudo capability if not explicitly configured
            passwordless_available = self.config.get(
                "passwordless_sudo_available", False
            )

            # Use passwordless mode if explicitly enabled OR if auto-detected as available
            passwordless_mode = (
                passwordless_config or passwordless_env or passwordless_available
            )

            if passwordless_mode:
                if passwordless_config:
                    logger.debug("Passwordless mode enabled via config - using sudo -n")
                elif passwordless_env:
                    logger.debug(
                        "Passwordless mode enabled via environment - using sudo -n"
                    )
                else:
                    logger.debug("Passwordless mode auto-detected - using sudo -n")
            else:
                logger.debug(
                    "Password mode enabled - using sudo -S (allow password via stdin)"
                )

            # Always use -n for passwordless attempts if passwordless mode is enabled
            # Note: We don't use -E (preserve environment) as it may be blocked by sudoers
            if len(command) > 1 and command[0] == "btrfs" and command[1] == "receive":
                if passwordless_mode:
                    logger.debug("Using sudo with -n flag (passwordless mode)")
                    return ["sudo", "-n", "-P", "-p", ""] + command
                else:
                    logger.debug(
                        "Using sudo for btrfs receive command with password support"
                    )
                    return ["sudo", "-S", "-P", "-p", ""] + command
            elif command[0] == "btrfs":
                logger.debug("Using sudo for regular btrfs command")
                if passwordless_mode:
                    return ["sudo", "-n"] + command
                else:
                    return ["sudo", "-S"] + command
            elif command[0] in ["mkdir", "touch", "rm", "test"]:
                # Directory operations and basic file operations that commonly need sudo privileges
                logger.debug("Using sudo for directory/file operation: %s", command[0])
                if passwordless_mode:
                    return ["sudo", "-n"] + command
                else:
                    return ["sudo", "-S"] + command
            else:
                # For other commands, respect the password mode setting
                logger.debug("Using sudo for other command: %s", command[0])
                if passwordless_mode:
                    return ["sudo", "-n"] + command
                else:
                    return ["sudo", "-S"] + command
        else:
            logger.debug(
                "Not using sudo for remote command (ssh_sudo=False): %s", command
            )
        return command

    def _get_sudo_password(self, retry_on_failure: bool = False) -> Optional[str]:
        logger.debug("Attempting to get sudo password...")

        # Build cache key from user@hostname
        cache_key = f"{self.config.get('username', 'unknown')}@{self.hostname}"

        # Check class-level cache first (shared across all instances for same host)
        if cache_key in SSHEndpoint._sudo_password_cache and not retry_on_failure:
            logger.debug(
                f"Using cached sudo password for {cache_key} (class-level cache)"
            )
            return SSHEndpoint._sudo_password_cache[cache_key]

        # Also check instance cache for backwards compatibility
        if self._cached_sudo_password is not None and not retry_on_failure:
            logger.debug(
                "SSHEndpoint._get_sudo_password: Using cached sudo password (instance cache)."
            )
            return self._cached_sudo_password

        # Check SSH master manager's cache (often same password for SSH and sudo)
        if not retry_on_failure:
            if hasattr(self, "ssh_manager") and hasattr(
                self.ssh_manager, "_cached_ssh_password"
            ):
                if self.ssh_manager._cached_ssh_password:
                    logger.debug(
                        "Using SSH password from master cache as sudo password (same credentials)"
                    )
                    # Cache it for future use
                    self._cached_sudo_password = self.ssh_manager._cached_ssh_password
                    SSHEndpoint._sudo_password_cache[cache_key] = (
                        self.ssh_manager._cached_ssh_password
                    )
                    return self.ssh_manager._cached_ssh_password

        sudo_pw_env = os.environ.get("BTRFS_BACKUP_SUDO_PASSWORD")
        if sudo_pw_env and not retry_on_failure:
            logger.debug(
                "SSHEndpoint._get_sudo_password: Using sudo password from BTRFS_BACKUP_SUDO_PASSWORD env var."
            )
            self._cached_sudo_password = sudo_pw_env
            SSHEndpoint._sudo_password_cache[cache_key] = sudo_pw_env
            logger.debug(
                "SSHEndpoint._get_sudo_password: Cached sudo password from env var."
            )
            return sudo_pw_env

        if retry_on_failure:
            if self._cached_sudo_password:
                logger.debug(
                    "Clearing cached password and prompting for fresh password due to authentication failure"
                )
                self._cached_sudo_password = None
            if cache_key in SSHEndpoint._sudo_password_cache:
                logger.debug(f"Clearing class-level cached password for {cache_key}")
                del SSHEndpoint._sudo_password_cache[cache_key]

        logger.debug(
            "SSHEndpoint._get_sudo_password: Attempting to prompt for sudo password interactively..."
        )
        try:
            retry_msg = (
                " (retry after authentication failure)" if retry_on_failure else ""
            )
            prompt_message = f"Sudo password for {self.config.get('username', 'remote user')}@{self.hostname}{retry_msg}: "
            # Log before getpass call
            logger.debug(
                f"SSHEndpoint._get_sudo_password: About to call getpass.getpass() with prompt: '{prompt_message}'"
            )

            password = getpass.getpass(prompt_message)

            # Log after getpass call
            if password:  # Check if any password was entered
                logger.debug(
                    "SSHEndpoint._get_sudo_password: Sudo password received from prompt."
                )
                # Log length for confirmation, not the password itself
                logger.debug(
                    f"SSHEndpoint._get_sudo_password: Password of length {len(password)} received. Caching it."
                )
                # Cache in both instance and class-level cache
                self._cached_sudo_password = password
                SSHEndpoint._sudo_password_cache[cache_key] = password
                return password
            else:
                logger.warning(
                    "SSHEndpoint._get_sudo_password: Empty password received from prompt. Not caching. Will return None."
                )
                return None  # Explicitly return None for empty password
        except Exception as e:
            logger.error(
                f"SSHEndpoint._get_sudo_password: Error during interactive sudo password prompt: {type(e).__name__}: {e}"
            )
            logger.debug(
                "SSHEndpoint._get_sudo_password: Interactive password prompt failed - this is normal when running in non-interactive environments"
            )
            logger.info("Interactive password prompt not available")
            logger.info(
                "To provide sudo password non-interactively, set the BTRFS_BACKUP_SUDO_PASSWORD environment variable"
            )
            logger.info(
                "Alternatively, configure passwordless sudo for btrfs commands on the remote host"
            )
            return None

    def _get_ssh_password(self, prompt: bool = False) -> Optional[str]:
        """Get SSH password from cache, environment, or prompt user.

        Checks multiple sources in order:
        1. SSH master manager's cached password (already entered for connection)
        2. Instance sudo password cache (often same as SSH password)
        3. Class-level sudo password cache
        4. Instance SSH cache
        5. Environment variable
        6. Interactive prompt (if requested)

        Args:
            prompt: If True, prompt user interactively if no cached/env password

        Returns:
            SSH password or None if not available
        """
        # Check SSH master manager's cache first (password already entered for connection)
        if hasattr(self, "ssh_manager") and hasattr(
            self.ssh_manager, "_cached_ssh_password"
        ):
            if self.ssh_manager._cached_ssh_password:
                logger.debug("Using SSH password from master connection cache")
                return self.ssh_manager._cached_ssh_password

        # Check sudo password cache (often same as SSH password)
        if self._cached_sudo_password:
            logger.debug("Using sudo password as SSH password (same credentials)")
            return self._cached_sudo_password

        # Check class-level sudo cache
        cache_key = f"{self.config.get('username', 'unknown')}@{self.hostname}"
        if cache_key in SSHEndpoint._sudo_password_cache:
            logger.debug("Using class-level sudo cache as SSH password")
            return SSHEndpoint._sudo_password_cache[cache_key]

        # Check instance SSH cache
        ssh_cache_key = f"ssh_{cache_key}"
        if (
            hasattr(self, "_ssh_password_cache")
            and ssh_cache_key in self._ssh_password_cache
        ):
            logger.debug("Using cached SSH password from instance cache")
            return self._ssh_password_cache[ssh_cache_key]

        # Check environment variable
        ssh_password = os.environ.get("BTRFS_BACKUP_SSH_PASSWORD")
        if ssh_password:
            logger.debug("Using SSH password from BTRFS_BACKUP_SSH_PASSWORD env var")
            return ssh_password

        # Prompt interactively if requested
        if prompt:
            try:
                ssh_user = self.config.get("username", "unknown")
                password = getpass.getpass(
                    f"SSH password for {ssh_user}@{self.hostname}: "
                )
                if password:
                    if not hasattr(self, "_ssh_password_cache"):
                        self._ssh_password_cache: Dict[str, str] = {}
                    self._ssh_password_cache[ssh_cache_key] = password
                    # Also cache in SSH master manager if available
                    if hasattr(self, "ssh_manager"):
                        self.ssh_manager._cached_ssh_password = password
                    # Also cache as sudo password (often same)
                    self._cached_sudo_password = password
                    SSHEndpoint._sudo_password_cache[cache_key] = password
                    logger.debug("SSH password collected and cached")
                    return password
            except Exception as e:
                logger.debug(f"Could not get SSH password interactively: {e}")

        return None

    def _exec_remote_command_with_retry(
        self, command: List[Any], max_retries: int = 1, **kwargs: Any
    ) -> CompletedProcess[Any]:
        """Execute a command with automatic retry on authentication failures."""
        result = None
        for attempt in range(max_retries + 1):
            try:
                result = self._exec_remote_command(command, **kwargs)

                # Check if this was a sudo command that failed with authentication issues
                if (
                    result.returncode != 0
                    and attempt < max_retries
                    and any(
                        arg == "-S"
                        for arg in self._build_remote_command([str(c) for c in command])
                    )
                ):
                    stderr = (
                        str(result.stderr.decode("utf-8", errors="replace"))
                        if hasattr(result, "stderr") and result.stderr
                        else ""
                    )

                    # Check for authentication failure indicators
                    auth_failure_indicators = [
                        "Sorry, try again",
                        "incorrect password",
                        "authentication failure",
                        "3 incorrect password attempts",
                    ]

                    stderr_lower = stderr.lower()
                    auth_failed = any(
                        indicator.lower() in stderr_lower
                        for indicator in auth_failure_indicators
                    )

                    if auth_failed:
                        logger.warning(
                            f"Authentication failure detected on attempt {attempt + 1}/{max_retries + 1}"
                        )
                        logger.debug("Retrying with fresh password prompt...")

                        # Clear the cached password and get a fresh one
                        self._clear_sudo_password_cache()
                        fresh_password = self._get_sudo_password(retry_on_failure=True)

                        if fresh_password:
                            # Update kwargs with fresh password
                            if "input" in kwargs:
                                kwargs["input"] = (fresh_password + "\n").encode()
                            logger.debug(
                                f"Retrying command with fresh password (attempt {attempt + 2}/{max_retries + 1})"
                            )
                            continue
                        else:
                            logger.error("Could not obtain fresh password for retry")
                            return result

                return result

            except Exception as e:
                if attempt == max_retries:
                    raise
                logger.warning(
                    f"Command execution failed on attempt {attempt + 1}, retrying: {e}"
                )

        # This should never be reached, but just in case
        return result  # type: ignore

    def _clear_sudo_password_cache(self) -> None:
        """Clear the cached sudo password if authentication fails."""
        if self._cached_sudo_password is not None:
            logger.debug("Clearing cached sudo password due to authentication failure")
            self._cached_sudo_password = None

    def _check_and_handle_auth_failure(
        self, stderr: str, using_sudo_with_stdin: bool
    ) -> None:
        """Check for authentication failures and clear cached password if needed."""
        if not using_sudo_with_stdin or not stderr:
            return

        # Check for common sudo authentication failure messages
        auth_failure_indicators = [
            "Sorry, try again",
            "incorrect password",
            "authentication failure",
            "sudo: 3 incorrect password attempts",
            "sudo: no password was provided",
            "sudo: unable to read password",
            "sudo: a password is required",
        ]

        stderr_lower = stderr.lower()
        for indicator in auth_failure_indicators:
            if indicator.lower() in stderr_lower:
                logger.warning(f"Authentication failure detected: {indicator}")
                logger.debug(
                    "Clearing cached sudo password to allow fresh authentication attempt"
                )
                self._clear_sudo_password_cache()
                break

    def _exec_remote_command(
        self, command: List[Any], **kwargs: Any
    ) -> CompletedProcess[Any]:
        """Execute a command on the remote host via SSH."""
        # Process command arguments based on whether they're marked as paths
        string_command = []

        logger.debug("Executing remote command, original format: %s", command)
        logger.debug(
            "Command type: %s, first element type: %s",
            type(command).__name__,
            type(command[0]).__name__ if command else "None",
        )

        # Check if command is using the tuple format (arg, is_path)
        if command and isinstance(command[0], tuple) and len(command[0]) == 2:  # type: ignore
            # type: ignore
            # New format with (arg, is_path) tuples
            logger.debug("Detected tuple format command (arg, is_path)")
            for i, (arg, is_path) in enumerate(command):  # type: ignore
                logger.debug(
                    "Processing arg %d: '%s' (is_path=%s, type=%s)",
                    i,
                    arg,
                    is_path,
                    type(arg).__name__,
                )
                if is_path and isinstance(arg, (str, Path)):
                    normalized = self._normalize_path(arg)
                    logger.debug("Normalized path arg %d: %s -> %s", i, arg, normalized)
                    string_command.append(normalized)  # type: ignore
                else:
                    # Not a path, just append as-is
                    logger.debug("Using non-path arg %d as-is: %s", i, arg)
                    string_command.append(arg)  # type: ignore
            logger.debug(
                "Processed marked command arguments for remote execution: %s",
                string_command,  # type: ignore
            )
        else:
            # Legacy format - convert any Path objects in the command to strings
            logger.debug("Using legacy command format")
            for i, arg in enumerate(command):  # type: ignore
                if isinstance(arg, (str, Path)):
                    normalized = self._normalize_path(arg)
                    logger.debug("Normalized arg %d: %s -> %s", i, arg, normalized)
                    string_command.append(normalized)  # type: ignore
                else:
                    logger.debug("Using non-string arg %d as-is: %s", i, arg)
                    string_command.append(arg)  # type: ignore
            logger.debug(
                "Processed legacy command format for remote execution: %s",
                string_command,  # type: ignore
            )

        remote_cmd = self._build_remote_command(string_command)  # type: ignore
        logger.debug("Final remote command after build: %s", remote_cmd)

        # Detect if sudo -S is in the command (needs password on stdin)
        using_sudo_with_stdin = any(arg == "-S" for arg in remote_cmd)
        logger.debug(
            f"Command uses sudo -S: {using_sudo_with_stdin}, remote_cmd: {remote_cmd}"
        )

        if using_sudo_with_stdin:
            sudo_password = self._get_sudo_password()
            if sudo_password:
                logger.debug("Supplying sudo password via stdin for remote command")
                logger.debug(
                    f"Got cached password of length {len(sudo_password)}, setting as input"
                )
                kwargs["input"] = (sudo_password + "\n").encode()
                # Remove stdin if present, as input and stdin cannot both be set
                if "stdin" in kwargs:
                    del kwargs["stdin"]
            else:
                logger.warning("No sudo password available but command requires it")
        else:
            logger.debug("Command does not use sudo -S, not providing password")

        # Build the SSH command - determine if TTY allocation is needed
        needs_tty = False
        cmd_str = " ".join(map(str, remote_cmd))
        if self.config.get("ssh_sudo", False) and not self.config.get(
            "passwordless", False
        ):
            # Check if this is a command that might need TTY for sudo password
            # BUT: if we're using sudo -S with password via stdin, we DON'T want TTY
            if "sudo" in cmd_str and "-n" not in cmd_str and not using_sudo_with_stdin:
                needs_tty = True

        ssh_base_cmd = self.ssh_manager.get_ssh_base_cmd(force_tty=needs_tty)  # type: ignore
        logger.debug("SSH base command: %s", ssh_base_cmd)

        ssh_cmd = ssh_base_cmd + ["--"] + remote_cmd
        logger.debug("Complete SSH command: %s", ssh_cmd)

        # Always capture stderr if not explicitly provided
        if "stderr" not in kwargs:
            kwargs["stderr"] = subprocess.PIPE
            logger.debug("Added stderr capture to kwargs")

        # Default timeout if not specified
        if "timeout" not in kwargs:
            kwargs["timeout"] = 30
            logger.debug("Using default timeout of 30 seconds")
        else:
            logger.debug(f"Using specified timeout of {kwargs['timeout']} seconds")
            logger.debug("Set default timeout to 30 seconds")

        ssh_cmd_str = " ".join(map(str, ssh_cmd))
        logger.debug("Executing remote command: %s", ssh_cmd_str)
        logger.debug("Working directory: %s", os.getcwd())

        try:
            logger.debug("About to execute subprocess.run with command: %s", ssh_cmd)
            logger.debug(
                "subprocess.run kwargs: %s",
                {k: v for k, v in kwargs.items() if k != "input"},
            )
            if "input" in kwargs:
                logger.debug("subprocess.run has input data (password)")

            result = subprocess.run(ssh_cmd, **kwargs)  # type: ignore[misc]
            exit_code = result.returncode  # type: ignore[attr-defined]

            if exit_code != 0 and kwargs.get("check", False) is False:
                stderr = (
                    str(result.stderr.decode("utf-8", errors="replace"))  # type: ignore
                    if hasattr(result, "stderr") and result.stderr  # type: ignore
                    else ""
                )

                # Check for authentication failures and clear cached password
                self._check_and_handle_auth_failure(stderr, using_sudo_with_stdin)

                logger.debug(
                    "Command exited with non-zero code %d: %s\nError: %s",
                    exit_code,  # type: ignore
                    ssh_cmd_str,  # type: ignore
                    stderr,  # type: ignore
                )
                details: Dict[str, Any] = {
                    "command": ssh_cmd_str,
                    "exit_code": exit_code,
                    "stderr_length": len(stderr) if stderr else 0,
                    "has_stdout": result.stdout is not None,  # type: ignore[attr-defined]
                }
                logger.debug("Non-zero exit command details: %s", details)
            elif exit_code == 0:
                logger.debug("Command executed successfully: %s", ssh_cmd_str)  # type: ignore
                if result.stdout:  # type: ignore[attr-defined]
                    stdout_data = result.stdout  # type: ignore[attr-defined]
                    if stdout_data:
                        stdout_len = (
                            len(stdout_data)
                            if isinstance(stdout_data, bytes)
                            else len(str(stdout_data))  # type: ignore[arg-type]
                        )
                        logger.debug("Command stdout length: %d bytes", stdout_len)

            logger.debug("Command execution result: exit_code=%d", result.returncode)  # type: ignore[attr-defined]
            return result  # type: ignore[return-value]

        except subprocess.TimeoutExpired as e:
            logger.error(
                "Command timed out after %s seconds: %s", e.timeout, ssh_cmd_str
            )
            logger.error(
                "Timeout occurred in SSH command execution, command was: %s", ssh_cmd
            )
            logger.debug(
                "Timeout exception details: timeout=%s, cmd=%s", e.timeout, e.cmd
            )
            raise
        except Exception as e:
            logger.error(
                "Failed to execute remote command: %s\nError: %s", ssh_cmd_str, str(e)
            )
            logger.error("Exception type: %s", type(e).__name__)
            logger.error("Command that failed: %s", ssh_cmd)
            logger.debug("Full exception details: %s", e, exc_info=True)
            logger.debug(
                "SSH command details: host=%s, port=%s, user=%s",
                self.config.get("hostname", "unknown"),
                self.config.get("port", 22),
                self.config.get("username", "unknown"),
            )
            raise

    def _btrfs_send(self, source: str, stdout_pipe: Any) -> subprocess.Popen[Any]:
        """Run btrfs send locally and pipe its output."""
        command = ["btrfs", "send", source]
        logger.debug("Preparing to execute btrfs send: %s", command)
        try:
            process = subprocess.Popen(
                command, stdout=stdout_pipe, stderr=subprocess.PIPE
            )
            logger.debug("btrfs send process started successfully: %s", command)
            return process
        except Exception as e:
            logger.error("Failed to start btrfs send process: %s", e)
            raise

    def _normalize_path(self, val: Any) -> str:
        if val is None:
            return ""
        path = val
        if isinstance(val, tuple) and len(val) == 2:  # type: ignore
            path, is_path = cast(Tuple[Any, Any], val)
            logger.debug(
                f"Tuple format detected in _normalize_path: {str(path)} (is_path={str(is_path)})"
            )
            if not is_path:
                logger.debug(f"Not a path, returning as-is: {str(path)}")
                return str(path)  # type: ignore
        if isinstance(path, Path):
            logger.debug("Converting Path object to string: %s", path)
            return str(path)
        if isinstance(path, str) and "~" in path:
            logger.debug("Path contains tilde, handling expansion: %s", path)
            if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
                sudo_user = os.environ.get("SUDO_USER")
                logger.debug("Running as root via sudo user: %s", sudo_user)
                sudo_user_home = None
                if sudo_user:
                    sudo_user_home = None
                    if _pwd_available and _pwd is not None:
                        try:
                            sudo_user_home = _pwd.getpwnam(sudo_user).pw_dir
                            logger.debug("Found sudo user home: %s", sudo_user_home)
                        except Exception as e:
                            logger.warning(
                                "Error getting home directory for sudo user: {}".format(
                                    e
                                )
                            )
                            # Fall back to default location
                            sudo_user_home = None

                    # Use fallback if we couldn't get the home directory
                    if sudo_user_home is None:
                        sudo_user_home = (
                            f"/home/{sudo_user}" if sudo_user != "root" else "/root"
                        )
                        logger.debug(
                            "Using fallback home directory: %s", sudo_user_home
                        )
                # By this point sudo_user_home should be set if sudo_user was available
                # This is just a safety check in case something went wrong
                if sudo_user_home is None and sudo_user:
                    logger.warning(
                        "Home directory still not determined, using fallback"
                    )
                    if sudo_user == "root":
                        sudo_user_home = "/root"
                    else:
                        sudo_user_home = f"/home/{sudo_user}"
                if sudo_user_home and path.startswith("~"):
                    try:
                        original_path = path
                        path = path.replace("~", sudo_user_home, 1)
                        logger.debug(
                            "Expanded ~ in path: %s -> %s", original_path, path
                        )
                    except Exception as e:
                        logger.error("Error expanding ~ in path: %s", e)
            else:
                original_path = path
                path = os.path.expanduser(path)
                logger.debug("Expanded user path: %s -> %s", original_path, path)
        return str(path) if path is not None else ""  # type: ignore

    def _verify_btrfs_availability(self, use_sudo: bool = False) -> bool:
        try:
            if use_sudo:
                test_cmd = ["sudo", "-n", "which", "btrfs"]
                logger.debug("Testing btrfs availability with sudo")
                # Use retry mechanism for sudo commands to handle authentication
                test_result = self._exec_remote_command_with_retry(
                    test_cmd,
                    max_retries=2,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
            else:
                test_cmd = ["which", "btrfs"]
                logger.debug("Testing btrfs availability without sudo")
                test_result = self._exec_remote_command(
                    test_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
            if test_result.returncode != 0:
                stderr = test_result.stderr.decode("utf-8", errors="replace")
                logger.error("btrfs command not found on remote host: %s", stderr)
                return False
            btrfs_path = test_result.stdout.decode("utf-8", errors="replace").strip()
            logger.debug("Found btrfs on remote host: %s", btrfs_path)
            return True
        except Exception as e:
            logger.error("Failed to verify btrfs availability: %s", e)
            return False

    def _btrfs_receive(
        self, destination: str, stdin_pipe: Any
    ) -> subprocess.Popen[Any]:
        """Run btrfs receive on the remote host.

        This method assumes sudo credentials are already cached (either passwordless
        sudo is available, or credentials were primed via _prime_remote_sudo).

        The remote command is wrapped with orphan protection to ensure the
        btrfs receive process is terminated if the SSH connection drops.

        Args:
            destination: Remote path to receive the snapshot
            stdin_pipe: Pipe providing btrfs send stream data

        Returns:
            Popen process for the receive command
        """
        logger.debug("Preparing btrfs receive command for destination: %s", destination)

        # Build the SSH command for remote btrfs receive
        control_path = str(self.ssh_manager.control_path)
        ssh_user = self.config.get("username", "root")
        ssh_port = self.config.get("port")
        remote_host = f"{ssh_user}@{self.hostname}"

        # Build SSH command with ControlMaster
        ssh_cmd = [
            "ssh",
            "-o",
            f"ControlPath={control_path}",
            "-o",
            "BatchMode=yes",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=3",
        ]
        if ssh_port:
            ssh_cmd.extend(["-p", str(ssh_port)])

        # Build remote command with orphan protection
        escaped_dest = shlex.quote(destination)
        use_sudo = self.config.get("ssh_sudo", False)
        remote_cmd = _build_receive_command(
            escaped_dest, use_sudo=use_sudo, password_on_stdin=False
        )
        ssh_cmd.extend([remote_host, remote_cmd])

        logger.debug("SSH receive command: %s", " ".join(ssh_cmd))

        try:
            receive_process = subprocess.Popen(
                ssh_cmd,
                stdin=stdin_pipe,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )

            logger.debug(
                "btrfs receive process started with PID: %d", receive_process.pid
            )
            return receive_process

        except Exception as e:
            logger.error("Failed to start btrfs receive process: %s", e)
            raise

    def list_snapshots(self, flush_cache: bool = False) -> List[Any]:
        """
        List snapshots (btrfs subvolumes) on the remote host at the configured path.
        Returns a list of Snapshot objects.
        """
        path = self.config["path"]
        use_sudo = self.config.get("ssh_sudo", False)

        # If we need sudo with password, prime credentials first
        if use_sudo and self._is_master_active():
            # Check if we have passwordless sudo for btrfs
            # Use btrfs --version instead of sudo -n true since sudoers may only allow btrfs
            try:
                result = self._exec_remote_command(
                    ["sudo", "-n", "btrfs", "--version"],
                    check=False,
                    timeout=10,
                )
                if result.returncode != 0:
                    # Need password - prime remote sudo credentials
                    logger.debug("Priming remote sudo credentials for snapshot listing")
                    if not self._prime_remote_sudo():
                        logger.warning("Failed to prime remote sudo, listing may fail")
            except Exception:
                pass

        # Standard method - works with cached credentials
        cmd = ["btrfs", "subvolume", "list", "-o", path]
        try:
            logger.debug("Listing remote snapshots with command: %s", cmd)
            # Use retry mechanism for commands that may require authentication
            if use_sudo:
                result = self._exec_remote_command_with_retry(
                    cmd,
                    max_retries=2,
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            else:
                result = self._exec_remote_command(
                    cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
            if result.returncode != 0:
                stderr = (
                    result.stderr.decode(errors="replace").strip()
                    if result.stderr
                    else ""
                )

                # The retry mechanism should have already handled authentication failures
                # Log the final failure and provide diagnostic information
                logger.warning(f"Failed to list remote snapshots: {stderr}")

                if use_sudo and (
                    "a password is required" in stderr
                    or "sudo:" in stderr
                    or "Sorry, try again" in stderr
                ):
                    logger.error(
                        "Authentication issues detected during snapshot listing"
                    )
                    logger.error(
                        "SSH endpoint: %s@%s:%s (ssh_sudo=%s)",
                        self.config.get("username"),
                        self.hostname,
                        path,
                        use_sudo,
                    )
                    logger.info("To resolve authentication issues:")
                    logger.info(
                        "1. Configure passwordless sudo for btrfs commands on remote host, OR"
                    )
                    logger.info(
                        "2. Set BTRFS_BACKUP_SUDO_PASSWORD environment variable, OR"
                    )
                    logger.info(
                        "3. Run in an interactive terminal for password prompting"
                    )
                    logger.info(
                        f"   Example sudoers entry: {self.config.get('username')} ALL=(ALL) NOPASSWD: /usr/bin/btrfs"
                    )
                    self._run_diagnostics(path, force_refresh=True)

                return []
            output = result.stdout.decode(errors="replace") if result.stdout else ""
            return self._parse_snapshot_list(output, path)
        except Exception as e:
            logger.error(f"Exception while listing remote snapshots: {e}")
            self._run_diagnostics(path, force_refresh=True)
            return []

    def _parse_snapshot_list(self, output: str, path: str) -> List[Any]:
        """Parse btrfs subvolume list output into Snapshot objects.

        Args:
            output: Raw output from btrfs subvolume list command
            path: The path where snapshots are located

        Returns:
            List of Snapshot objects, sorted by time
        """
        from btrfs_backup_ng import __util__

        snapshots: List[Any] = []
        snap_prefix = self.config.get("snap_prefix", "")

        for line in output.splitlines():
            parts = line.split("path ", 1)
            if len(parts) == 2:
                snap_path = parts[1].strip()
                snap_name = os.path.basename(snap_path)
                if snap_name.startswith(snap_prefix):
                    date_part = snap_name[len(snap_prefix) :]
                    try:
                        time_obj = __util__.str_to_date(date_part)
                        snapshot = __util__.Snapshot(
                            self.config["path"],
                            snap_prefix,
                            self,
                            time_obj=time_obj,
                        )
                        snapshots.append(snapshot)
                    except Exception as e:
                        # Debug level - it's normal for directories to contain
                        # files that don't match the snapshot naming pattern
                        logger.debug(
                            "Skipping non-snapshot item: %r (%s)", snap_name, e
                        )
                        continue

        snapshots.sort()
        logger.info(f"Found {len(snapshots)} remote snapshots at {path}")
        logger.debug(f"Remote snapshots: {[str(s) for s in snapshots]}")
        return snapshots

    def _verify_snapshot_exists(self, dest_path: str, snapshot_name: str) -> bool:
        """Verify a snapshot exists on the remote host.

        Args:
            dest_path: Remote destination path
            snapshot_name: Name of the snapshot to verify

        Returns:
            True if the snapshot exists, False otherwise
        """
        logger.debug(
            f"Starting snapshot verification for '{snapshot_name}' in '{dest_path}'"
        )
        logger.debug(f"SSH sudo enabled: {self.config.get('ssh_sudo', False)}")

        # Try direct subvolume list first
        # Don't call _build_remote_command here - _exec_remote_command does it internally
        list_cmd = ["btrfs", "subvolume", "list", "-o", dest_path]

        logger.debug(f"Verifying snapshot existence with command: {list_cmd}")

        try:
            logger.debug("Executing subvolume list command...")

            # Use retry mechanism for commands that may require authentication
            use_sudo = self.config.get("ssh_sudo", False)
            if use_sudo:
                list_result = self._exec_remote_command_with_retry(
                    list_cmd,
                    max_retries=2,
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            else:
                list_result = self._exec_remote_command(
                    list_cmd,
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

            logger.debug(f"Subvolume list command exit code: {list_result.returncode}")
            if list_result.stdout:
                stdout_content = list_result.stdout.decode(errors="replace")
                logger.debug(f"Subvolume list output:\n{stdout_content}")
            if list_result.stderr:
                stderr_content = list_result.stderr.decode(errors="replace")
                logger.debug(f"Subvolume list stderr:\n{stderr_content}")
            if list_result.returncode != 0:
                stderr_text = (
                    list_result.stderr.decode(errors="replace")
                    if list_result.stderr
                    else ""
                )
                logger.warning(
                    f"Failed to list subvolumes (exit code {list_result.returncode}): {stderr_text}"
                )
                logger.debug("Falling back to simple path check")

                # Fall back to simple path check
                check_cmd = [
                    "test",
                    "-d",
                    f"{dest_path}/{snapshot_name}",
                    "&&",
                    "echo",
                    "EXISTS",
                ]
                # Apply proper authentication handling to the fallback command too
                check_cmd = self._build_remote_command(check_cmd)
                logger.debug(f"Fallback verification command: {' '.join(check_cmd)}")

                # Check if we need to provide password input for sudo
                fallback_input = None
                if "sudo" in check_cmd and "-S" in check_cmd:
                    sudo_password = self._get_sudo_password()
                    if sudo_password:
                        fallback_input = sudo_password.encode() + b"\n"
                        logger.debug(
                            "Providing sudo password for fallback verification command"
                        )

                # Use retry mechanism for fallback verification commands with authentication
                check_result = self._exec_remote_command_with_retry(
                    check_cmd,
                    max_retries=2,
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    input=fallback_input,
                )

                logger.debug(f"Path check exit code: {check_result.returncode}")
                if check_result.stdout and b"EXISTS" in check_result.stdout:
                    logger.debug(
                        f"Snapshot exists at path: {dest_path}/{snapshot_name}"
                    )
                    logger.debug("Path-based verification successful")
                    return True
                else:
                    logger.error(
                        f"Snapshot not found at path: {dest_path}/{snapshot_name}"
                    )
                    logger.debug(
                        f"Path check stdout: {check_result.stdout.decode() if check_result.stdout else 'None'}"
                    )
                    logger.debug(
                        f"Path check stderr: {check_result.stderr.decode() if check_result.stderr else 'None'}"
                    )
                    return False

            # Check if the snapshot appears in the subvolume list
            stdout_text = (
                list_result.stdout.decode(errors="replace")
                if list_result.stdout
                else ""
            )
            logger.debug(f"Subvolume list output length: {len(stdout_text)} characters")
            logger.debug(
                f"Searching for snapshot '{snapshot_name}' at path '{dest_path}'"
            )

            # Look for the snapshot in the subvolume list output
            # The output format is typically: "ID xxx gen xxx top level xxx path <path>"
            # We need to check if our snapshot path appears in any of these lines
            snapshot_found = False
            expected_path = f"{dest_path.rstrip('/')}/{snapshot_name}"

            if stdout_text:
                lines = stdout_text.splitlines()
                logger.debug(f"Subvolume list has {len(lines)} lines:")
                for i, line in enumerate(lines):
                    if i < 10:  # Log first 10 lines for debugging
                        logger.debug(f"  Line {i + 1}: {line}")

                    # Look for "path" keyword and check if our snapshot path is there
                    if "path " in line:
                        # Extract the path part after "path "
                        path_part = line.split("path ", 1)[-1].strip()

                        # More flexible matching - check if the snapshot name appears in the path
                        # and if the path is within our destination directory
                        if snapshot_name in path_part:
                            # Check if this path is within our destination directory
                            if (
                                path_part == expected_path  # Exact match
                                or path_part.startswith(
                                    dest_path.rstrip("/") + "/"
                                )  # Under dest_path
                                or expected_path in path_part
                            ):  # Expected path is contained
                                logger.debug(
                                    f"Snapshot found in subvolume list: {snapshot_name}"
                                )
                                logger.debug(f"  Found in path: {path_part}")
                                logger.debug(f"  Expected path: {expected_path}")
                                snapshot_found = True
                                break

                        # Also check if the path ends with our snapshot name (handles nested paths)
                        if path_part.endswith(f"/{snapshot_name}"):
                            logger.debug(
                                f"Snapshot found by suffix match: {snapshot_name}"
                            )
                            logger.debug(f"  Found in path: {path_part}")
                            snapshot_found = True
                            break

                if len(lines) > 10:
                    logger.debug(f"  ... and {len(lines) - 10} more lines")

            if snapshot_found:
                logger.debug("Subvolume-based verification successful")
                return True
            else:
                logger.error("Snapshot not found in subvolume list")
                logger.debug(f"Expected path: {expected_path}")
                logger.debug(f"Full subvolume list output:\n{stdout_text}")

                # Try simple path existence check as fallback
                logger.debug("Trying simple path existence check as fallback")
                simple_check_cmd = ["test", "-d", expected_path]
                # Apply proper authentication handling to the final fallback command too
                simple_check_cmd = self._build_remote_command(simple_check_cmd)

                # Check if we need to provide password input for sudo
                final_input = None
                if "sudo" in simple_check_cmd and "-S" in simple_check_cmd:
                    sudo_password = self._get_sudo_password()
                    if sudo_password:
                        final_input = sudo_password.encode() + b"\n"
                        logger.debug(
                            "Providing sudo password for final fallback verification command"
                        )

                # Use retry mechanism for simple path check with authentication
                simple_result = self._exec_remote_command_with_retry(
                    simple_check_cmd,
                    max_retries=2,
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    input=final_input,
                )
                if simple_result.returncode == 0:
                    logger.debug(f"Snapshot exists via path check: {expected_path}")
                    return True
                else:
                    logger.debug(f"Path check also failed for: {expected_path}")

                return False

        except Exception as e:
            logger.error(f"Error verifying snapshot: {e}")
            logger.debug(f"Verification exception details: {e}", exc_info=True)
            return False

    def _find_buffer_program(self) -> Tuple[Optional[str], Optional[str]]:
        """Find pv program to use for transfer progress display.

        Returns:
            A tuple of (program_name, command_string) or (None, None) if not found
        """
        use_simple_progress = self.config.get("simple_progress", True)

        # Check for pv
        if self._check_command_exists("pv"):
            if use_simple_progress:
                logger.debug(
                    "Found pv - using it in simple mode (no progress indicators)"
                )
                # Use pv quietly without progress display in simple mode
                return "pv", "pv -q"
            else:
                logger.debug("Found pv - using it for transfer progress")
                # Use pv with progress display (don't use -q for quiet, we want progress)
                return "pv", "pv -p -t -e -r -b"

        # Check for mbuffer as fallback
        if self._check_command_exists("mbuffer"):
            logger.debug("Found mbuffer - using it for transfer buffering")
            return "mbuffer", "mbuffer -q -s 128k -m 1G"

        # No buffer program found
        logger.debug(
            "No buffer program (pv/mbuffer) found - transfers may be less reliable"
        )
        return None, None

    def _check_command_exists(self, command: str) -> bool:
        """Check if a command exists in the PATH.

        Args:
            command: The command to check for

        Returns:
            True if command exists, False otherwise
        """
        try:
            check_cmd = ["which", command]
            result = subprocess.run(
                check_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
            )
            return result.returncode == 0
        except (subprocess.SubprocessError, FileNotFoundError):
            return False

    def _is_master_active(self) -> bool:
        """Verify the OpenSSH master socket is alive and responding."""
        control_path = str(self.ssh_manager.control_path)
        ssh_user = self.config.get("username", "root")
        remote_host = f"{ssh_user}@{self.hostname}"

        check_cmd = [
            "ssh",
            "-o",
            f"ControlPath={control_path}",
            "-O",
            "check",
            remote_host,
        ]

        try:
            result = subprocess.run(
                check_cmd,
                capture_output=True,
                timeout=10,
            )
            return result.returncode == 0
        except Exception:
            return False

    def _prime_local_sudo(self) -> bool:
        """Prime local sudo credentials using sudo -v.

        Prompts for password via stdin and caches credentials for subsequent
        sudo commands. This avoids embedding passwords in command lines.

        Returns:
            True if sudo credentials were successfully cached, False otherwise
        """
        # Check if we already have passwordless sudo for btrfs
        # Use btrfs --version instead of true since sudoers may only allow btrfs
        try:
            result = subprocess.run(
                ["sudo", "-n", "btrfs", "--version"],
                capture_output=True,
                timeout=5,
            )
            if result.returncode == 0:
                logger.debug("Local passwordless sudo for btrfs available")
                return True
        except Exception:
            pass

        # Need to prompt for password
        sudo_password = self._get_sudo_password()
        if not sudo_password:
            logger.error("No sudo password available for local priming")
            return False

        try:
            # Use sudo -S -v to validate and cache credentials
            # -S reads password from stdin, -v validates without running a command
            result = subprocess.run(
                ["sudo", "-S", "-v"],
                input=(sudo_password + "\n").encode(),
                capture_output=True,
                timeout=30,
            )
            if result.returncode == 0:
                logger.debug("Local sudo credentials cached successfully")
                return True
            else:
                stderr = result.stderr.decode(errors="replace")
                logger.error(f"Failed to cache local sudo credentials: {stderr}")
                return False
        except subprocess.TimeoutExpired:
            logger.error("Timeout while priming local sudo credentials")
            return False
        except Exception as e:
            logger.error(f"Error priming local sudo credentials: {e}")
            return False

    def _prime_remote_sudo(self) -> bool:
        """Prime remote sudo credentials using sudo -v over SSH.

        Uses the SSH ControlMaster connection to run sudo -v on the remote host,
        caching credentials for subsequent sudo commands.

        Returns:
            True if remote sudo credentials were successfully cached, False otherwise
        """
        if not self._is_master_active():
            logger.error("SSH master connection not active")
            return False

        # Check if remote has passwordless sudo for btrfs
        # Use btrfs --version instead of true since sudoers may only allow btrfs
        try:
            result = self._exec_remote_command(
                ["sudo", "-n", "btrfs", "--version"],
                check=False,
                timeout=10,
            )
            if result.returncode == 0:
                logger.debug("Remote passwordless sudo for btrfs available")
                return True
        except Exception:
            pass

        # Need to prompt for password
        sudo_password = self._get_sudo_password()
        if not sudo_password:
            logger.error("No sudo password available for remote priming")
            return False

        # Build SSH command to run sudo -S -v on remote
        control_path = str(self.ssh_manager.control_path)
        ssh_user = self.config.get("username", "root")
        ssh_port = self.config.get("port")
        remote_host = f"{ssh_user}@{self.hostname}"

        ssh_cmd = [
            "ssh",
            "-o",
            f"ControlPath={control_path}",
            "-o",
            "BatchMode=no",
        ]
        if ssh_port:
            ssh_cmd.extend(["-p", str(ssh_port)])
        ssh_cmd.extend([remote_host, "sudo", "-S", "-v"])

        try:
            result = subprocess.run(
                ssh_cmd,
                input=(sudo_password + "\n").encode(),
                capture_output=True,
                timeout=30,
            )
            if result.returncode == 0:
                logger.debug("Remote sudo credentials cached successfully")
                return True
            else:
                stderr = result.stderr.decode(errors="replace")
                logger.error(f"Failed to cache remote sudo credentials: {stderr}")
                return False
        except subprocess.TimeoutExpired:
            logger.error("Timeout while priming remote sudo credentials")
            return False
        except Exception as e:
            logger.error(f"Error priming remote sudo credentials: {e}")
            return False

    def _estimate_snapshot_size(
        self, snapshot_path: str, parent_path: Optional[str] = None
    ) -> Optional[int]:
        """Estimate the size of a snapshot transfer for progress display.

        For full transfers: Uses btrfs subvolume show to get exclusive data size.
        For incremental transfers: Returns None (indeterminate) since estimating
        the delta accurately is expensive and the actual transfer is usually fast.

        Args:
            snapshot_path: Path to the snapshot
            parent_path: Optional parent snapshot path for incremental transfers

        Returns:
            Estimated size in bytes, or None if unable to determine or incremental
        """
        # For incremental transfers, return None (indeterminate progress)
        # The delta size is hard to estimate accurately without doing the actual send
        if parent_path and os.path.exists(parent_path):
            logger.debug(
                "Incremental transfer detected - using indeterminate progress bar"
            )
            return None

        # Try btrfs subvolume show first for full transfers
        try:
            result = subprocess.run(
                ["sudo", "btrfs", "subvolume", "show", snapshot_path],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                # Parse "Exclusive" line for data size
                for line in result.stdout.splitlines():
                    if "Exclusive" in line and "Exclusive" not in line.split(":")[
                        0
                    ].strip().replace(" ", ""):
                        continue
                    if line.strip().startswith("Exclusive"):
                        # Format: "Exclusive: 1.23GiB" or similar
                        size_str = line.split(":")[-1].strip()
                        return self._parse_size_string(size_str)
        except Exception as e:
            logger.debug(f"btrfs subvolume show failed: {e}")

        # Fallback to du
        try:
            result = subprocess.run(
                ["sudo", "du", "-sb", snapshot_path],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode == 0:
                size_str = result.stdout.split()[0]
                return int(size_str)
        except Exception as e:
            logger.debug(f"du failed: {e}")

        return None

    def _parse_size_string(self, size_str: str) -> Optional[int]:
        """Parse a human-readable size string to bytes.

        Args:
            size_str: Size string like "1.23GiB", "500MiB", "1024KiB"

        Returns:
            Size in bytes, or None if parsing fails
        """
        size_str = size_str.strip()
        multipliers = {
            "B": 1,
            "KiB": 1024,
            "MiB": 1024**2,
            "GiB": 1024**3,
            "TiB": 1024**4,
            "KB": 1000,
            "MB": 1000**2,
            "GB": 1000**3,
            "TB": 1000**4,
            "K": 1024,
            "M": 1024**2,
            "G": 1024**3,
            "T": 1024**4,
        }

        for suffix, multiplier in sorted(multipliers.items(), key=lambda x: -len(x[0])):
            if size_str.endswith(suffix):
                try:
                    num = float(size_str[: -len(suffix)].strip())
                    return int(num * multiplier)
                except ValueError:
                    continue

        # Try parsing as plain number
        try:
            return int(float(size_str))
        except ValueError:
            return None

    def _do_piped_transfer(
        self,
        source_path: str,
        dest_path: str,
        snapshot_name: str,
        parent_path: Optional[str] = None,
    ) -> bool:
        """Execute btrfs send | ssh btrfs receive transfer with progress.

        Uses Paramiko for password-based sudo (secure stdin handling),
        or falls back to shell pipeline for passwordless sudo.

        Args:
            source_path: Path to the source snapshot
            dest_path: Destination path on remote
            snapshot_name: Name of the snapshot being transferred
            parent_path: Optional parent snapshot for incremental transfer

        Returns:
            True if transfer succeeded, False otherwise
        """
        use_sudo = self.config.get("ssh_sudo", False)
        passwordless_available = self.config.get("passwordless_sudo_available", False)
        sudo_password = self._cached_sudo_password

        # If we need password-based sudo but don't have a password, try to get one now
        if use_sudo and not passwordless_available and not sudo_password:
            logger.debug("No cached password, prompting for sudo password...")
            sudo_password = self._get_sudo_password()
            if sudo_password:
                self._cached_sudo_password = sudo_password
            else:
                logger.error("Password-based sudo required but no password provided")
                return False

        # Use Paramiko for password-based sudo (cleaner stdin handling)
        if (
            use_sudo
            and not passwordless_available
            and sudo_password
            and PARAMIKO_AVAILABLE
        ):
            return self._do_paramiko_transfer(
                source_path=source_path,
                dest_path=dest_path,
                snapshot_name=snapshot_name,
                parent_path=parent_path,
                sudo_password=sudo_password,
            )

        # Fall back to shell pipeline for passwordless sudo or no-sudo cases
        return self._do_shell_pipeline_transfer(
            source_path=source_path,
            dest_path=dest_path,
            snapshot_name=snapshot_name,
            parent_path=parent_path,
        )

    def _do_paramiko_transfer(
        self,
        source_path: str,
        dest_path: str,
        snapshot_name: str,
        parent_path: Optional[str],
        sudo_password: str,
    ) -> bool:
        """Execute transfer using Paramiko for clean password handling.

        Paramiko gives us direct control over stdin, allowing us to:
        1. Connect with SSH password auth (collected via getpass)
        2. Send sudo password + newline to sudo -S
        3. Then stream btrfs data
        Without any shell escaping issues.

        Args:
            source_path: Path to the source snapshot
            dest_path: Destination path on remote
            snapshot_name: Name of the snapshot being transferred
            parent_path: Optional parent snapshot for incremental transfer
            sudo_password: The sudo password to use

        Returns:
            True if transfer succeeded, False otherwise
        """

        # Get username from config (from CLI --ssh-username or ssh://user@host URL)
        ssh_user = self.config.get("username", "root")
        ssh_port = self.config.get("port", 22) or 22

        # Get SSH password - use cached password first, only prompt if not available
        ssh_password_fallback = self.config.get("ssh_password_fallback", False)
        ssh_password: Optional[str] = None
        if ssh_password_fallback:
            # First check if we already have the password cached (from _prepare or earlier)
            ssh_password = self._get_ssh_password(prompt=False)
            if not ssh_password:
                # Also check if sudo_password is available (often same as SSH password)
                ssh_password = self._cached_sudo_password or sudo_password
            if not ssh_password:
                # Last resort: prompt user
                ssh_password = self._get_ssh_password(prompt=True)
            if not ssh_password:
                logger.error("SSH password required but not provided")
                return False
            # Cache it for future use
            if hasattr(self, "ssh_manager"):
                self.ssh_manager._cached_ssh_password = ssh_password

        # Estimate snapshot size for progress display (None for incremental)
        estimated_size = self._estimate_snapshot_size(source_path, parent_path)
        is_incremental = parent_path and os.path.exists(parent_path)
        if estimated_size:
            size_mb = estimated_size / (1024 * 1024)
            logger.info(f"Estimated snapshot size: {size_mb:.1f} MiB")
        elif is_incremental:
            logger.info(
                "Incremental transfer - size will be determined during transfer"
            )

        logger.info(f"Transferring {snapshot_name} to {self.hostname}:{dest_path}")
        logger.info("Using Paramiko for secure password-based transfer")

        # Build local btrfs send command
        send_cmd = ["sudo", "btrfs", "send"]
        if is_incremental and parent_path is not None:
            send_cmd.extend(["-p", parent_path])
            logger.info(f"Using parent: {os.path.basename(parent_path)}")
        send_cmd.append(source_path)

        # Remote command with orphan protection
        # sudo -S reads password from stdin, then receives btrfs stream
        escaped_dest = shlex.quote(dest_path)
        remote_cmd = _build_receive_command(
            escaped_dest, use_sudo=True, password_on_stdin=True
        )

        # Ensure paramiko is available
        if paramiko is None:
            logger.error("Paramiko is not available for SSH transfer")
            return False

        client: Optional[Any] = None
        send_proc: Optional[subprocess.Popen[bytes]] = None

        try:
            # Connect via Paramiko using the pattern:
            # 1. Get username (from CLI/URL - already in ssh_user)
            # 2. Get password (via getpass - in ssh_password)
            # 3. Connect with those credentials
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs: Dict[str, Any] = {
                "hostname": self.hostname,
                "port": ssh_port,
                "username": ssh_user,
                "timeout": 30,
            }

            # Use password auth if we have an SSH password
            if ssh_password:
                connect_kwargs["password"] = ssh_password
                connect_kwargs["allow_agent"] = False
                connect_kwargs["look_for_keys"] = False
                logger.debug(
                    f"Connecting to {ssh_user}@{self.hostname}:{ssh_port} with password auth"
                )
            else:
                # Fall back to key-based auth
                ssh_identity_file = self.config.get("ssh_identity_file")
                if ssh_identity_file and os.path.exists(ssh_identity_file):
                    connect_kwargs["key_filename"] = ssh_identity_file
                else:
                    connect_kwargs["allow_agent"] = True
                    connect_kwargs["look_for_keys"] = True
                logger.debug(
                    f"Connecting to {ssh_user}@{self.hostname}:{ssh_port} with key auth"
                )

            client.connect(**connect_kwargs)
            logger.debug("Paramiko SSH connection established")

            # Open channel and execute remote command
            transport = client.get_transport()
            if not transport:
                logger.error("Failed to get SSH transport")
                return False

            channel = transport.open_session()
            channel.exec_command(remote_cmd)

            # Start local btrfs send process
            send_proc = subprocess.Popen(
                send_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # Send password first (sudo -S reads from stdin)
            channel.sendall((sudo_password + "\n").encode())
            logger.debug("Sent sudo password to remote")

            # Give sudo a moment to process password
            time.sleep(0.1)

            # Stream btrfs send output to remote with rich progress bar
            from rich.progress import (
                BarColumn,
                DownloadColumn,
                Progress,
                TextColumn,
                TimeRemainingColumn,
                TransferSpeedColumn,
            )

            bytes_sent = 0
            start_time = time.time()
            chunk_size = 65536

            with Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(bar_width=40),
                "[progress.percentage]{task.percentage:>3.1f}%",
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                transient=False,
            ) as progress:
                task = progress.add_task(
                    f"Sending {snapshot_name}",
                    total=estimated_size if estimated_size else None,
                )

                while True:
                    if send_proc.stdout is None:
                        break
                    chunk = send_proc.stdout.read(chunk_size)
                    if not chunk:
                        break
                    channel.sendall(chunk)
                    bytes_sent += len(chunk)
                    progress.update(task, advance=len(chunk))

            # Signal end of data
            channel.shutdown_write()

            # Wait for send process
            send_proc.wait()
            send_stderr = (
                send_proc.stderr.read().decode(errors="replace")
                if send_proc.stderr
                else ""
            )

            if send_proc.returncode != 0:
                logger.error(
                    f"btrfs send failed (exit {send_proc.returncode}): {send_stderr}"
                )
                return False

            # Wait for remote command to complete
            exit_status = channel.recv_exit_status()

            # Get any remote output
            remote_stdout = b""
            remote_stderr = b""
            while channel.recv_ready():
                remote_stdout += channel.recv(4096)
            while channel.recv_stderr_ready():
                remote_stderr += channel.recv_stderr(4096)

            if exit_status != 0:
                stderr_text = remote_stderr.decode(errors="replace")
                logger.error(
                    f"btrfs receive failed (exit {exit_status}): {stderr_text}"
                )
                return False

            elapsed = time.time() - start_time
            rate = bytes_sent / elapsed if elapsed > 0 else 0
            logger.info(
                f"Transfer completed: {bytes_sent / (1024 * 1024):.1f} MiB in {elapsed:.1f}s "
                f"({rate / (1024 * 1024):.1f} MiB/s)"
            )

            # Verify snapshot exists on remote
            if self._verify_snapshot_exists(dest_path, snapshot_name):
                logger.info(f"Snapshot {snapshot_name} verified on remote")
                return True
            else:
                logger.error(
                    "Transfer verification failed - snapshot not found on remote"
                )
                return False

        except Exception as e:
            # Handle paramiko-specific exceptions
            if paramiko is not None:
                if isinstance(e, paramiko.AuthenticationException):
                    logger.error(f"SSH authentication failed: {e}")
                    return False
                if isinstance(e, paramiko.SSHException):
                    logger.error(f"SSH error: {e}")
                    return False
            logger.error(f"Transfer failed: {e}")
            return False
        finally:
            if send_proc and send_proc.poll() is None:
                send_proc.terminate()
            if client:
                client.close()

    def _do_shell_pipeline_transfer(
        self,
        source_path: str,
        dest_path: str,
        snapshot_name: str,
        parent_path: Optional[str] = None,
    ) -> bool:
        """Execute transfer using shell pipeline (for passwordless sudo).

        Args:
            source_path: Path to the source snapshot
            dest_path: Destination path on remote
            snapshot_name: Name of the snapshot being transferred
            parent_path: Optional parent snapshot for incremental transfer

        Returns:
            True if transfer succeeded, False otherwise
        """
        import sys

        control_path = str(self.ssh_manager.control_path)
        ssh_user = self.config.get("username", "root")
        ssh_port = self.config.get("port")
        remote_host = f"{ssh_user}@{self.hostname}"
        use_sudo = self.config.get("ssh_sudo", False)

        # Estimate snapshot size for progress display (None for incremental)
        estimated_size = self._estimate_snapshot_size(source_path, parent_path)
        is_incremental = parent_path and os.path.exists(parent_path)
        if estimated_size:
            size_mb = estimated_size / (1024 * 1024)
            logger.info(f"Estimated snapshot size: {size_mb:.1f} MiB")
        elif is_incremental:
            logger.info(
                "Incremental transfer - size will be determined during transfer"
            )

        # Build send command
        send_parts = ["sudo", "btrfs", "send"]
        if is_incremental and parent_path is not None:
            send_parts.extend(["-p", shlex.quote(parent_path)])
            logger.info(f"Using parent: {os.path.basename(parent_path)}")
        send_parts.append(shlex.quote(source_path))
        send_cmd = " ".join(send_parts)

        # Build pv command for progress
        has_pv = self._check_command_exists("pv")
        if has_pv:
            if estimated_size:
                pv_cmd = f"pv -f -p -t -e -r -b -s {estimated_size}"
            else:
                pv_cmd = "pv -f -p -t -e -r -b"
        else:
            pv_cmd = "cat"
            logger.warning("pv not found - progress display unavailable")

        # Build SSH command
        ssh_parts = ["ssh", "-o", f"ControlPath={shlex.quote(control_path)}"]
        ssh_parts.extend(["-o", "BatchMode=yes"])
        ssh_parts.append("-T")
        if ssh_port:
            ssh_parts.extend(["-p", str(ssh_port)])
        ssh_parts.append(remote_host)

        # Build remote command with orphan protection (passwordless sudo)
        escaped_dest = shlex.quote(dest_path)
        remote_cmd = _build_receive_command(
            escaped_dest, use_sudo=use_sudo, password_on_stdin=False
        )
        ssh_parts.append(shlex.quote(remote_cmd))
        ssh_cmd = " ".join(ssh_parts)

        full_pipeline = f"{send_cmd} | {pv_cmd} | {ssh_cmd}"

        logger.info(f"Transferring {snapshot_name} to {self.hostname}:{dest_path}")
        logger.debug(f"Pipeline: {full_pipeline}")

        try:
            proc = subprocess.Popen(
                full_pipeline,
                shell=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            stderr_lines: List[str] = []

            def stream_stderr() -> None:
                stderr_stream = proc.stderr
                if stderr_stream is not None:
                    for chunk in iter(lambda: stderr_stream.read(80), b""):
                        if chunk:
                            text = chunk.decode(errors="replace")
                            sys.stderr.write(text)
                            sys.stderr.flush()
                            stderr_lines.append(text)

            stderr_thread = threading.Thread(target=stream_stderr, daemon=True)
            stderr_thread.start()

            proc.wait()
            stderr_thread.join(timeout=5)

            if proc.returncode != 0:
                stderr_output = "".join(stderr_lines)
                logger.error(f"Transfer failed (exit {proc.returncode})")
                if stderr_output:
                    logger.error(f"Error output: {stderr_output}")
                return False

            logger.info("Transfer completed successfully")

            if self._verify_snapshot_exists(dest_path, snapshot_name):
                logger.info(f"Snapshot {snapshot_name} verified on remote")
                return True
            else:
                logger.error(
                    "Transfer verification failed - snapshot not found on remote"
                )
                return False

        except Exception as e:
            logger.error(f"Transfer failed with exception: {e}")
            return False

    def _try_sudo_cached_transfer(
        self,
        source_path: str,
        dest_path: str,
        snapshot_name: str,
        parent_path: Optional[str] = None,
    ) -> bool:
        """Transfer using pre-fetched sudo password with pv for progress.

        This method:
        1. Pre-fetches sudo password via getpass (stored securely in memory)
        2. Primes local sudo credentials
        3. Primes remote sudo credentials via SSH
        4. Runs transfer: sudo btrfs send | pv | ssh 'sudo btrfs receive'

        Password is passed via environment variable to shell (not in cmdline),
        then injected into stdin for sudo -S on the remote side.

        Args:
            source_path: Path to the source snapshot
            dest_path: Destination path on remote
            snapshot_name: Name of the snapshot being transferred
            parent_path: Optional parent snapshot for incremental transfer

        Returns:
            True if transfer succeeded, False otherwise
        """
        # Verify SSH master connection is active
        if not self._is_master_active():
            logger.error("SSH master connection is down")
            try:
                self.ssh_manager.stop_master()
                self.ssh_manager.start_master()
                if not self._is_master_active():
                    logger.error("Failed to re-establish SSH master connection")
                    return False
                logger.info("SSH master connection re-established")
            except Exception as e:
                logger.error(f"Failed to restart SSH master: {e}")
                return False

        # Step 0: Pre-fetch sudo password upfront (before any transfers)
        if not self._cached_sudo_password:
            logger.info("Collecting sudo password for transfer...")
            password = self._get_sudo_password()
            if not password:
                logger.error(
                    "No sudo password provided - cannot proceed with password-based sudo"
                )
                return False
            self._cached_sudo_password = password
            logger.debug("Sudo password collected and cached securely")

        # Step 1: Prime local sudo credentials
        logger.info("Priming local sudo credentials...")
        if not self._prime_local_sudo():
            logger.error("Failed to prime local sudo credentials")
            return False

        # Step 2: Prime remote sudo credentials
        logger.info("Priming remote sudo credentials...")
        if not self._prime_remote_sudo():
            logger.error("Failed to prime remote sudo credentials")
            return False

        # Step 3: Run the actual transfer with pv for progress
        logger.info(f"Starting transfer of {snapshot_name}...")
        return self._do_piped_transfer(
            source_path=source_path,
            dest_path=dest_path,
            snapshot_name=snapshot_name,
            parent_path=parent_path,
        )

    def _try_direct_transfer(
        self,
        source_path: str,
        dest_path: str,
        snapshot_name: str,
        parent_path: Optional[str] = None,
        max_wait_time: int = 3600,
        **kwargs: Any,
    ) -> bool:
        """Direct SSH transfer for btrfs-backup-ng, using robust logic and logging."""
        logger.debug("Entering _try_direct_transfer")
        logger.debug(f"Source path: {source_path}")
        logger.debug(f"Destination path: {dest_path}")
        logger.debug(f"Snapshot name: {snapshot_name}")
        logger.debug(f"Parent path: {parent_path}")
        logger.debug(f"SSH sudo: {self.config.get('ssh_sudo', False)}")

        # Check if source path exists
        if not os.path.exists(source_path):
            logger.error(f"Source path does not exist: {source_path}")
            return False

        # Run pre-transfer diagnostics
        logger.info("Verifying SSH connectivity and filesystem readiness...")
        diagnostics = self._run_diagnostics(dest_path)
        if not all(
            [
                diagnostics["ssh_connection"],
                diagnostics["btrfs_command"],
                diagnostics["write_permissions"],
                diagnostics["btrfs_filesystem"],
            ]
        ):
            logger.error("Pre-transfer diagnostics failed")
            return False

        # Use invoke transfer if ssh_sudo and no passwordless sudo
        if self.config.get("ssh_sudo", False) and not diagnostics.get(
            "passwordless_sudo", False
        ):
            logger.info("Using sudo credential caching for password-based sudo")
            return self._try_sudo_cached_transfer(
                source_path=source_path,
                dest_path=dest_path,
                snapshot_name=snapshot_name,
                parent_path=parent_path,
            )

        # Find buffer program for progress display and reliability
        buffer_name, buffer_cmd = self._find_buffer_program()

        # Get the source snapshot object to use proper send method

        # Find the source endpoint (should be passed in or accessible)
        # For now, we'll create a minimal snapshot object to use the source endpoint's send method
        try:
            # Create a snapshot object that represents our source
            # We need to get this from the source path - this is a limitation of the current design
            # For now, we'll use the traditional approach but with better process management

            # Determine parent for incremental transfer
            if parent_path and os.path.exists(parent_path):
                logger.info(f"Using incremental transfer with parent: {parent_path}")
                # We'll handle incremental logic in the actual send call
            else:
                logger.info("Using full transfer")
        except Exception as e:
            logger.error(f"Error setting up transfer parameters: {e}")
            return False

        try:
            # Build the proper btrfs send command
            logger.info(f"Starting transfer from {source_path}...")
            start_time = time.time()

            # Create the btrfs send command
            send_cmd = ["btrfs", "send"]
            if parent_path and os.path.exists(parent_path):
                send_cmd.extend(["-p", parent_path])
                logger.debug(f"Using incremental send with parent: {parent_path}")
            send_cmd.append(source_path)

            logger.debug(f"Local send command: {' '.join(send_cmd)}")

            # Start the local btrfs send process
            send_process = subprocess.Popen(
                send_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0
            )

            # Set up buffering if available
            if buffer_cmd:
                logger.debug(f"Using {buffer_name} to improve transfer reliability")
                buffer_args = buffer_cmd.split()
                buffer_process = subprocess.Popen(
                    buffer_args,
                    stdin=send_process.stdout,
                    stdout=subprocess.PIPE,
                    bufsize=0,
                )
                if send_process.stdout:  # Only close if stdout exists
                    send_process.stdout.close()  # Allow send_process to receive SIGPIPE
                pipe_output = buffer_process.stdout
            else:
                pipe_output = send_process.stdout
                buffer_process = None

            # Start the remote receive process
            logger.debug("Starting remote btrfs receive process")
            receive_process = self._btrfs_receive(dest_path, pipe_output)

            if not receive_process:
                logger.error("Failed to start remote receive process")
                return False

            # Use the new enhanced monitoring system
            processes = {
                "send": send_process,
                "receive": receive_process,
                "buffer": buffer_process,
            }

            # Choose monitoring system based on configuration
            use_simple_progress = self.config.get("simple_progress", True)
            if use_simple_progress:
                logger.debug(
                    "SYSTEM: Using simplified monitoring system for basic process tracking (default)..."
                )
                transfer_succeeded = self._simple_transfer_monitor(
                    processes=processes,
                    start_time=start_time,
                    dest_path=dest_path,
                    snapshot_name=snapshot_name,
                    max_wait_time=max_wait_time,
                )
            else:
                logger.debug(
                    "SYSTEM: Using enhanced monitoring system for real-time progress..."
                )
                transfer_succeeded = self._monitor_transfer_progress(
                    processes=processes,
                    start_time=start_time,
                    dest_path=dest_path,
                    snapshot_name=snapshot_name,
                    max_wait_time=max_wait_time,
                )

            # Final verification if we timed out
            if not transfer_succeeded:
                logger.warning(
                    "Reached maximum wait time, performing final verification..."
                )
                try:
                    if self._verify_snapshot_exists(dest_path, snapshot_name):
                        logger.info(
                            "SUCCESS: Transfer completed successfully (final check)"
                        )
                        transfer_succeeded = True
                    else:
                        logger.error(
                            "FAILED: Transfer failed - no snapshot found after maximum wait time"
                        )
                except Exception as e:
                    logger.error(f"Final verification failed: {e}")

            # Clean up processes
            all_processes = [send_process, receive_process]
            if buffer_process:
                all_processes.append(buffer_process)

            for proc in all_processes:
                if proc.poll() is None:
                    logger.debug("Terminating remaining process...")
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except Exception:
                        proc.kill()

            # Set dummy results for compatibility
            send_result = 0 if transfer_succeeded else 1
            receive_result = 0 if transfer_succeeded else 1

            elapsed_time = time.time() - start_time
            logger.info(f"Transfer completed in {elapsed_time:.2f} seconds")

            # Check process results
            if send_result != 0:
                stderr_output = (
                    send_process.stderr.read().decode(errors="replace")
                    if send_process.stderr
                    else ""
                )
                logger.error(
                    f"Local send process failed with exit code {send_result}: {stderr_output}"
                )
                return False

            if receive_result != 0:
                logger.error(
                    f"Remote receive process failed with exit code {receive_result}"
                )
                return False

            # Prioritize actual transfer verification over exit codes
            logger.debug("=== TRANSFER VERIFICATION (Primary Check) ===")
            logger.debug("Verifying snapshot was created on remote host...")
            logger.debug(f"Looking for snapshot '{snapshot_name}' in '{dest_path}'")

            # Check if transfer actually succeeded first
            transfer_actually_succeeded = False
            try:
                verification_result = self._verify_snapshot_exists(
                    dest_path, snapshot_name
                )
                logger.debug(f"Snapshot existence verification: {verification_result}")

                if verification_result:
                    logger.info(
                        "SUCCESS: TRANSFER ACTUALLY SUCCEEDED - Snapshot exists on remote host"
                    )
                    transfer_actually_succeeded = True
                else:
                    # Try alternative verification methods
                    logger.debug(
                        "Primary verification failed, trying alternative methods..."
                    )
                    ls_cmd = ["ls", "-la", dest_path]
                    ls_result = self._exec_remote_command(
                        ls_cmd, check=False, stdout=subprocess.PIPE
                    )
                    if ls_result.returncode == 0 and ls_result.stdout:
                        ls_output = ls_result.stdout.decode(errors="replace")
                        logger.debug(f"Directory listing: {ls_output}")
                        if snapshot_name in ls_output:
                            logger.info(
                                "SUCCESS: TRANSFER ACTUALLY SUCCEEDED - Snapshot found in directory listing"
                            )
                            transfer_actually_succeeded = True

            except Exception as e:
                logger.error(f"Exception during verification: {e}")

            # Check log files for diagnostic purposes, but don't let exit codes override actual success
            logger.debug("=== LOG FILE DIAGNOSTICS ===")
            if hasattr(self, "_last_receive_log"):
                try:
                    logger.debug(
                        f"Checking log files for diagnostics: {self._last_receive_log}"
                    )
                    # Check exit code file
                    exitcode_cmd = ["cat", f"{self._last_receive_log}.exitcode"]
                    exitcode_result = self._exec_remote_command(
                        exitcode_cmd, check=False, stdout=subprocess.PIPE
                    )

                    if exitcode_result.returncode == 0 and exitcode_result.stdout:
                        actual_exitcode = exitcode_result.stdout.decode(
                            errors="replace"
                        ).strip()
                        logger.debug(f"Process exit code: {actual_exitcode}")

                        if actual_exitcode != "0":
                            # Read the error log for diagnostics
                            log_cmd = ["cat", self._last_receive_log]
                            log_result = self._exec_remote_command(
                                log_cmd, check=False, stdout=subprocess.PIPE
                            )
                            if log_result.returncode == 0 and log_result.stdout:
                                log_content = log_result.stdout.decode(errors="replace")

                                if transfer_actually_succeeded:
                                    logger.warning(
                                        f"Process reported error (exit code {actual_exitcode}) but transfer succeeded"
                                    )
                                    logger.warning(
                                        f"Error details (informational): {log_content}"
                                    )
                                    logger.debug(
                                        "This may indicate timing issues or benign process termination"
                                    )
                                else:
                                    logger.error(
                                        f"Process failed with exit code {actual_exitcode} and no snapshot found"
                                    )
                                    logger.error(f"Error details: {log_content}")
                                    return False
                        else:
                            logger.debug("Process completed cleanly (exit code 0)")
                    else:
                        logger.warning(
                            f"Could not read exit code file - command returned {exitcode_result.returncode}"
                        )
                except Exception as e:
                    logger.warning(f"Could not check receive log files: {e}")
            else:
                logger.warning("No log files available for diagnostics")

            # Final decision based on actual transfer success
            if transfer_actually_succeeded:
                logger.info("SUCCESS: TRANSFER VERIFICATION SUCCESSFUL")
                return True
            else:
                logger.error(
                    "FAILED: TRANSFER FAILED - No snapshot found on remote host"
                )
                return False

        except Exception as e:
            logger.error(f"Error during transfer: {e}")
            logger.debug(f"Full error details: {e}", exc_info=True)
            return False

    def send_receive(
        self,
        snapshot: "__util__.Snapshot",
        parent: Optional["__util__.Snapshot"] = None,
        clones: Optional[List["__util__.Snapshot"]] = None,
        timeout: int = 3600,
    ) -> bool:
        """Perform direct SSH pipe transfer with verification.

        This method implements a direct SSH pipe for btrfs send/receive operations,
        providing better reliability and verification than traditional methods.

        Args:
            snapshot: The snapshot object to transfer
            parent: Optional parent snapshot for incremental transfers
            clones: Optional clones for the transfer (not currently used)
            timeout: Timeout in seconds for the transfer operation

        Returns:
            bool: True if transfer was successful and verified, False otherwise
        """
        logger.debug("Starting direct SSH pipe transfer for %s", snapshot)

        # Get snapshot details
        snapshot_path = str(snapshot.get_path())
        snapshot_name = snapshot.get_name()
        dest_path = self.config["path"]

        logger.debug("Source snapshot path: %s", snapshot_path)
        logger.debug("Destination path: %s", dest_path)
        logger.debug("Snapshot name: %s", snapshot_name)

        # Check if parent is provided for incremental transfers
        parent_path = None
        if parent:
            parent_path = str(parent.get_path())
            logger.debug("Parent snapshot path: %s", parent_path)

        # Verify destination path exists and create if needed
        try:
            if hasattr(self, "_exec_remote_command"):
                normalized_path = self._normalize_path(dest_path)
                logger.debug(
                    "Ensuring remote destination path exists: %s", normalized_path
                )

                cmd = ["test", "-d", normalized_path]
                result = self._exec_remote_command(cmd, check=False)
                if result.returncode != 0:
                    logger.warning(
                        "Destination path doesn't exist, creating it: %s",
                        normalized_path,
                    )
                    mkdir_cmd = ["mkdir", "-p", normalized_path]
                    mkdir_result = self._exec_remote_command(mkdir_cmd, check=False)
                    if mkdir_result.returncode != 0:
                        stderr = (
                            mkdir_result.stderr.decode("utf-8", errors="replace")
                            if mkdir_result.stderr
                            else ""
                        )
                        logger.error(
                            "Failed to create destination directory: %s", stderr
                        )
                        return False
        except Exception as e:
            logger.error("Error verifying/creating destination: %s", e)
            return False

        # Run diagnostics to ensure everything is ready
        logger.debug("Verifying pre-transfer readiness")
        diagnostics = self._run_diagnostics(dest_path)
        if not all(
            [
                diagnostics["ssh_connection"],
                diagnostics["btrfs_command"],
                diagnostics["write_permissions"],
                diagnostics["btrfs_filesystem"],
            ]
        ):
            logger.error("Pre-transfer diagnostics failed")
            return False

        # Use the existing _try_direct_transfer method which has all the logic
        try:
            success = self._try_direct_transfer(
                source_path=snapshot_path,
                dest_path=dest_path,
                snapshot_name=snapshot_name,
                parent_path=parent_path,
                max_wait_time=timeout,
            )

            if success:
                logger.debug("Direct SSH pipe transfer completed successfully")
                return True
            else:
                logger.error("Direct SSH pipe transfer failed")
                return False

        except Exception as e:
            logger.error("Error during direct SSH pipe transfer: %s", e)
            return False

    def _monitor_transfer_progress(
        self,
        processes: Dict[str, Any],
        start_time: float,
        dest_path: str,
        snapshot_name: str,
        max_wait_time: int = 3600,
    ) -> bool:
        """Enhanced transfer monitoring with real-time progress feedback.

        Args:
            processes: Dict containing 'send', 'receive', and optionally 'buffer' processes
            start_time: Transfer start time
            dest_path: Destination path for verification
            snapshot_name: Name of snapshot being transferred
            max_wait_time: Maximum time to wait in seconds

        Returns:
            bool: True if transfer succeeded, False otherwise
        """
        logger.debug("Starting advanced transfer monitoring...")

        send_process = processes["send"]
        receive_process = processes["receive"]
        buffer_process = processes.get("buffer")

        # Type guards to ensure processes are not None
        if send_process is None or receive_process is None:
            logger.error("CRITICAL: Required processes are None")
            return False

        transfer_succeeded = False
        last_status_time = start_time
        last_verification_time = start_time
        status_interval = 5  # Status updates every 5 seconds
        verification_interval = 30  # Verify snapshot every 30 seconds

        while time.time() - start_time < max_wait_time:
            current_time = time.time()
            elapsed = current_time - start_time

            # Check process status
            send_alive = send_process.poll() is None
            receive_alive = receive_process.poll() is None
            buffer_alive = buffer_process.poll() is None if buffer_process else True

            # Check for critical failures
            if not send_alive and send_process.returncode != 0:
                logger.error(
                    f"CRITICAL: Send process failed (exit code: {send_process.returncode})"
                )
                self._log_process_error(send_process, "send")
                return False

            # Regular status updates
            if current_time - last_status_time >= status_interval:
                self._log_transfer_status(
                    elapsed, send_alive, receive_alive, buffer_alive, buffer_process
                )
                last_status_time = current_time

            # Periodic verification
            if current_time - last_verification_time >= verification_interval:
                logger.debug("Performing verification check...")
                try:
                    if self._verify_snapshot_exists(dest_path, snapshot_name):
                        logger.info("SUCCESS: Transfer verification successful!")
                        return True
                    else:
                        logger.debug("STATUS: Transfer still in progress...")
                except Exception as e:
                    logger.debug(
                        f"Verification check failed (normal during transfer): {e}"
                    )
                last_verification_time = current_time

            # Check if all processes finished
            if (
                not send_alive
                and not receive_alive
                and not (buffer_process and buffer_alive)
            ):
                logger.debug(
                    "STATUS: All processes completed, performing final verification..."
                )
                break

            # Handle receive process warnings (but don't fail immediately)
            if not receive_alive and receive_process.returncode not in [None, 0]:
                logger.warning(
                    f"WARNING: Receive process exit code: {receive_process.returncode}"
                )
                logger.debug(
                    "STATUS: Checking if transfer succeeded despite exit code..."
                )

            time.sleep(0.5)  # Short sleep for responsive monitoring

        # Final verification
        logger.debug(
            "COMPLETE: Transfer monitoring complete, performing final verification..."
        )
        try:
            transfer_succeeded = self._verify_snapshot_exists(dest_path, snapshot_name)
            if transfer_succeeded:
                elapsed_final = time.time() - start_time
                logger.info(
                    f"SUCCESS: Transfer completed successfully in {elapsed_final:.1f}s"
                )
            else:
                logger.error(
                    "FAILED: Transfer failed - snapshot not found on remote host"
                )
        except Exception as e:
            logger.error(f"ERROR: Final verification failed: {e}")

        return transfer_succeeded

    def _log_transfer_status(
        self,
        elapsed: float,
        send_alive: bool,
        receive_alive: bool,
        buffer_alive: bool,
        buffer_process: Any,
    ) -> None:
        """Log detailed transfer status with professional indicators."""
        minutes = elapsed / 60

        logger.debug(f"STATUS: Transfer Progress ({elapsed:.1f}s / {minutes:.1f}m)")
        logger.debug(f"   Send: {'ACTIVE' if send_alive else 'COMPLETE'}")
        logger.debug(f"   Receive: {'ACTIVE' if receive_alive else 'COMPLETE'}")

        if buffer_process:
            logger.debug(f"   Buffer: {'ACTIVE' if buffer_alive else 'COMPLETE'}")

        # Show activity indicator
        active_count = sum([send_alive, receive_alive, buffer_alive])
        total_count = 2 + (1 if buffer_process else 0)
        logger.debug(f"   Active Processes: {active_count}/{total_count}")

        if elapsed > 60:  # After 1 minute
            logger.debug("   STATUS: Transfer progressing normally...")

    def _log_process_error(self, process: Any, process_name: str) -> None:
        """Log detailed error information for a failed process."""
        try:
            if process.stderr:
                stderr_data = process.stderr.read().decode("utf-8", errors="replace")
                if stderr_data.strip():
                    logger.error(f"{process_name} process stderr: {stderr_data}")
        except Exception as e:
            logger.debug(f"Could not read stderr from {process_name} process: {e}")

    def _simple_transfer_monitor(
        self,
        processes: Dict[str, Any],
        start_time: float,
        dest_path: str,
        snapshot_name: str,
        max_wait_time: int = 3600,
    ) -> bool:
        """Simplified transfer monitoring with basic process tracking.

        This method provides basic process monitoring that just tracks process completion
        and exit codes without the complex threading and real-time progress monitoring.

        Args:
            processes: Dict containing 'send', 'receive', and optionally 'buffer' processes
            start_time: Transfer start time
            dest_path: Destination path for verification
            snapshot_name: Name of snapshot being transferred
            max_wait_time: Maximum time to wait in seconds

        Returns:
            bool: True if transfer succeeded, False otherwise
        """
        logger.debug("Using simplified transfer monitoring...")

        send_process = processes["send"]
        receive_process = processes["receive"]
        buffer_process = processes.get("buffer")

        # Type guards to ensure processes are not None
        if send_process is None or receive_process is None:
            logger.error("CRITICAL: Required processes are None")
            return False

        # Wait for processes to complete
        processes_to_wait = [send_process, receive_process]
        if buffer_process:
            processes_to_wait.append(buffer_process)

        logger.debug("STATUS: Waiting for transfer processes to complete...")

        # Simple polling loop with timeout
        while time.time() - start_time < max_wait_time:
            all_finished = True

            for proc in processes_to_wait:
                if proc.poll() is None:  # Still running
                    all_finished = False
                    break

            if all_finished:
                break

            # Log status every 30 seconds
            elapsed = time.time() - start_time
            if int(elapsed) % 30 == 0:
                active_count = sum(
                    1 for proc in processes_to_wait if proc.poll() is None
                )
                logger.debug(
                    f"STATUS: Transfer in progress... ({elapsed:.0f}s elapsed, {active_count} processes active)"
                )

            time.sleep(1)

        # Check results
        elapsed_final = time.time() - start_time

        # Check send process
        send_code = send_process.returncode
        if send_code != 0:
            logger.error(f"FAILED: Send process failed with exit code {send_code}")
            self._log_simple_process_error(send_process, "send")
            return False

        logger.info("SUCCESS: Send process completed successfully")

        # Check receive process
        receive_code = receive_process.returncode
        if receive_code != 0:
            logger.warning(f"WARNING: Receive process exit code: {receive_code}")
            # Don't fail immediately - some exit codes may be acceptable
        else:
            logger.info("SUCCESS: Receive process completed successfully")

        # Check buffer process if present
        if buffer_process:
            buffer_code = buffer_process.returncode
            if buffer_code != 0:
                logger.warning(f"WARNING: Buffer process exit code: {buffer_code}")
            else:
                logger.info("SUCCESS: Buffer process completed successfully")

        # Final verification
        logger.debug("STATUS: Performing final verification...")
        try:
            if self._verify_snapshot_exists(dest_path, snapshot_name):
                logger.info(
                    f"SUCCESS: Transfer completed successfully in {elapsed_final:.1f}s"
                )
                return True
            else:
                logger.error(
                    "FAILED: Transfer failed - snapshot not found on remote host"
                )
                return False
        except Exception as e:
            logger.error(f"ERROR: Final verification failed: {e}")
            return False

    def _log_simple_process_error(self, process: Any, process_name: str) -> None:
        """Log error information for a failed process in simple monitoring mode."""
        try:
            if hasattr(process, "stderr") and process.stderr:
                stderr_data = process.stderr.read().decode("utf-8", errors="replace")
                if stderr_data.strip():
                    logger.error(f"{process_name} process stderr: {stderr_data}")
        except Exception as e:
            logger.debug(f"Could not read stderr from {process_name} process: {e}")
