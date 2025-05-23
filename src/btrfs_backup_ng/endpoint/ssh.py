# pyright: strict

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
import os
import subprocess
import time
import uuid
from pathlib import Path
from threading import Lock
from typing import Optional, List, Any, Dict, Tuple, cast, TypeVar
from subprocess import CompletedProcess
import getpass

try:
    import pwd

    _pwd = pwd
    _pwd_available = True
except ImportError:
    _pwd = None
    _pwd_available = False


from btrfs_backup_ng.__logger__ import logger
from btrfs_backup_ng.sshutil.master import SSHMasterManager
from .common import Endpoint

# Type variable for self in SSHEndpoint
_Self = TypeVar("_Self", bound="SSHEndpoint")


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
        self.config: Dict[str, Any] = config if config is not None else {}
        self.hostname: str = hostname
        self._instance_id: str = f"{os.getpid()}_{uuid.uuid4().hex[:8]}"
        self._lock: Lock = Lock()
        self._last_receive_log: Optional[str] = None
        self._last_transfer_snapshot: Optional[bool] = None
        self.ssh_manager: SSHMasterManager
        logger.debug(
            "SSHEndpoint: Config keys before parent init: %s", list(config.keys())
        )

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
        self.config["path"] = self.config.get("path", "/")
        self.config["ssh_sudo"] = self.config.get("ssh_sudo", False)
        self.config["passwordless"] = self.config.get("passwordless", False)

        # Log important settings for troubleshooting
        logger.info(
            "SSH endpoint configuration: hostname=%s, sudo=%s, passwordless=%s",
            self.hostname,
            self.config.get("ssh_sudo", False),
            self.config.get("passwordless", False),
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
                        logger.info("Using SSH identity file: %s (verified)", id_file)
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
        self.ssh_manager: SSHMasterManager = SSHMasterManager(
            hostname=self.hostname,
            username=self.config["username"],
            port=self.config["port"],
            ssh_opts=self.config["ssh_opts"],
            persist="60",
            debug=True,
            identity_file=self.config.get("ssh_identity_file"),
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
        logger.debug("SSHEndpoint initialization completed")

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
            remote_path = (
                str(snapshot.get_path())
                if hasattr(snapshot, "get_path")
                else str(snapshot)
            )
            cmd = ["btrfs", "subvolume", "delete", remote_path]
            logger.debug("Executing remote deletion command: %s", cmd)
            try:
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
                        if hasattr(result.stderr, "decode")
                        else result.stderr
                    )
                    logger.error(
                        "Failed to delete remote snapshot %s: %s", remote_path, stderr
                    )
            except Exception as e:
                logger.error(
                    "Exception while deleting remote snapshot %s: %s", remote_path, e
                )

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

    def _run_diagnostics(self, path: str = "/") -> Dict[str, bool]:
        """Run SSH and sudo diagnostics to identify potential issues.

        Attempts several tests to verify SSH connectivity, btrfs availability,
        sudo access, and filesystem type. Updates self.config["passwordless_sudo_available"]
        based on sudo test results.

        Args:
            path: Remote path to test for btrfs operations

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
                logger.info(f"btrfs command found: {btrfs_path}")
                logger.debug(f"btrfs command path: {btrfs_path}")
            else:
                logger.error("btrfs command not found on remote host")
                logger.debug(
                    f"btrfs command check stderr: {result.stderr.decode() if result.stderr else 'None'}"
                )
        except Exception as e:
            logger.error(f"Error checking btrfs command: {e}")
            logger.debug(f"btrfs command check exception: {e}", exc_info=True)

        # Test passwordless sudo
        logger.debug("Testing passwordless sudo...")
        try:
            result = self._exec_remote_command(["sudo", "-n", "true"], check=False)
            results["passwordless_sudo"] = result.returncode == 0
            if results["passwordless_sudo"]:
                logger.info("Passwordless sudo is available")
                logger.debug("Passwordless sudo test passed")
            else:
                logger.warning("Passwordless sudo is not available")
                logger.debug(
                    f"Passwordless sudo stderr: {result.stderr.decode() if result.stderr else 'None'}"
                )
        except Exception as e:
            logger.error(f"Error checking passwordless sudo: {e}")
            logger.debug(f"Passwordless sudo exception: {e}", exc_info=True)

        # Test sudo with btrfs
        logger.debug("Testing sudo with btrfs command...")
        try:
            result = self._exec_remote_command(
                ["sudo", "-n", "btrfs", "--version"], check=False
            )
            results["sudo_btrfs"] = result.returncode == 0
            if results["sudo_btrfs"]:
                logger.info("Sudo btrfs command works")
                logger.debug("Sudo btrfs test passed")
                if result.stdout:
                    logger.debug(f"btrfs version: {result.stdout.decode().strip()}")
            else:
                logger.warning("Cannot run btrfs with passwordless sudo")
                logger.debug(
                    f"Sudo btrfs stderr: {result.stderr.decode() if result.stderr else 'None'}"
                )
        except Exception as e:
            logger.error(f"Error checking sudo btrfs: {e}")
            logger.debug(f"Sudo btrfs exception: {e}", exc_info=True)

        # Test write permissions
        logger.debug(f"Testing write permissions to path: {path}")
        try:
            test_file = f"{path}/.btrfs-backup-write-test-{uuid.uuid4().hex[:8]}"
            logger.debug(f"Testing write with test file: {test_file}")
            result = self._exec_remote_command(["touch", test_file], check=False)
            if result.returncode == 0:
                self._exec_remote_command(["rm", "-f", test_file], check=False)
                results["write_permissions"] = True
                logger.info(f"Path is directly writable: {path}")
                logger.debug("Direct write test passed")
            else:
                logger.debug(
                    f"Direct write failed, trying with sudo. Error: {result.stderr.decode() if result.stderr else 'None'}"
                )
                # Try with sudo
                result = self._exec_remote_command(
                    ["sudo", "-n", "touch", test_file], check=False
                )
                if result.returncode == 0:
                    self._exec_remote_command(
                        ["sudo", "-n", "rm", "-f", test_file], check=False
                    )
                    results["write_permissions"] = True
                    logger.info(f"Path is writable with sudo: {path}")
                    logger.debug("Sudo write test passed")
                else:
                    logger.error(f"Path is not writable (even with sudo): {path}")
                    logger.debug(
                        f"Sudo write failed. Error: {result.stderr.decode() if result.stderr else 'None'}"
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
                logger.info(f"Path is on a btrfs filesystem: {path}")
                logger.debug(f"Filesystem type confirmed: {fs_type}")
            else:
                logger.error(f"Path is not on a btrfs filesystem (found: {fs_type})")
                logger.debug(f"Expected 'btrfs', got '{fs_type}'")
        except Exception as e:
            logger.error(f"Error checking filesystem type: {e}")
            logger.debug(f"Filesystem type check exception: {e}", exc_info=True)

        # Log summary of results
        logger.debug("Diagnostic tests completed, generating summary...")
        logger.info("\nDiagnostic Summary:")
        logger.info("-" * 50)
        for test, result in results.items():
            status = "✓" if result else "✗"
            logger.info(f"{test.replace('_', ' ').title():20} {status}")
            logger.debug(f"Test {test}: {'PASSED' if result else 'FAILED'}")
        logger.info("-" * 50)

        # Debug overall status
        all_passed = all(results.values())
        logger.debug(
            f"Overall diagnostics status: {'ALL PASSED' if all_passed else 'SOME FAILED'}"
        )
        if not all_passed:
            failed_tests = [test for test, result in results.items() if not result]
            logger.debug(f"Failed tests: {failed_tests}")

        # Provide specific recommendations based on what failed
        if not all(results.values()):
            if not results["sudo_btrfs"]:
                self._show_sudoers_fix_instructions()

            if not results["write_permissions"]:
                logger.info("\nTo fix write permissions:")
                logger.info(
                    f"Ensure that user '{self.config.get('username')}' has write permission to {path}"
                )
                logger.info(
                    "or that sudo is configured properly to allow writing to this location."
                )

            if not results["btrfs_filesystem"]:
                logger.info("\nTo fix filesystem type:")
                logger.info(f"The path {path} must be on a btrfs filesystem.")
                logger.info("btrfs-backup-ng cannot work with other filesystem types.")

        return results

    def _show_sudoers_fix_instructions(self) -> None:
        """Show instructions for fixing sudoers configuration."""
        logger.info("\nTo fix sudo access:")
        user = self.config.get("username")
        logger.info(f"Add one of these lines to /etc/sudoers via 'sudo visudo':")
        logger.info(f"\n# Full access to btrfs commands:")
        logger.info(f"{user} ALL=(ALL) NOPASSWD: /usr/bin/btrfs")
        logger.info(f"\n# Or more restricted access:")
        logger.info(
            f"{user} ALL=(ALL) NOPASSWD: /usr/bin/btrfs subvolume*, /usr/bin/btrfs send*, /usr/bin/btrfs receive*"
        )

    def get_id(self) -> str:
        """Return a unique identifier for this SSH endpoint."""
        username: str = self.config.get("username", "")
        username_part: str = f"{username}@" if username else ""
        return f"ssh://{username_part}{self.hostname}:{self.config['path']}"

    def _build_remote_command(self, command: List[str]) -> List[str]:
        """Prepare a remote command with optional sudo."""
        if not command:
            return command

        # Ensure all elements are strings
        command = [str(c) for c in command]

        # Check if the ssh_sudo flag is set
        if self.config.get("ssh_sudo", False):
            cmd_str: str = " ".join(command)
            logger.debug("Using sudo for remote command: %s", cmd_str)

            passwordless_only = os.environ.get(
                "BTRFS_BACKUP_PASSWORDLESS_ONLY", "0"
            ).lower() in ("1", "true", "yes")
            # Always use -n for passwordless attempts if passwordless_only is set
            if len(command) > 1 and command[0] == "btrfs" and command[1] == "receive":
                if passwordless_only:
                    logger.debug("Using sudo with -n flag (passwordless only mode)")
                    return ["sudo", "-n", "-E", "-P", "-p", ""] + command
                else:
                    logger.debug(
                        "Using sudo for btrfs receive command with password support"
                    )
                    logger.warning(
                        "Note: If the remote host requires a sudo password, transfer may fail"
                    )
                    logger.warning(
                        "Consider setting up passwordless sudo for btrfs commands on remote host"
                    )
                    return ["sudo", "-S", "-E", "-P", "-p", ""] + command
            elif command[0] == "btrfs":
                logger.debug("Using sudo for regular btrfs command")
                return ["sudo", "-n", "-E"] + command
            else:
                return ["sudo", "-n"] + command
        else:
            logger.debug(
                "Not using sudo for remote command (ssh_sudo=False): %s", command
            )
        return command

    def _get_sudo_password(self) -> Optional[str]:
        # First try environment variable
        sudo_pw = os.environ.get("BTRFS_BACKUP_SUDO_PASSWORD")
        if sudo_pw:
            return sudo_pw

        # Then try interactive password prompt
        try:
            return getpass.getpass(
                f"Sudo password for {self.config.get('username', 'remote user')}: "
            )
        except Exception as e:
            logger.warning(f"Error getting sudo password via getpass: {e}")
            logger.info(
                "You may need to run this with sudo privileges or ensure terminal supports password input"
            )
            return None

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
        needs_tty = False
        sudo_password = None
        # Detect if sudo -S is in the command (needs password on stdin)
        sudo_password = None
        if any(arg == "-S" for arg in remote_cmd):
            sudo_password = self._get_sudo_password()
            if sudo_password:
                logger.debug("Supplying sudo password via stdin for remote command.")
                kwargs["input"] = (sudo_password + "\n").encode()
                # Remove stdin if present, as input and stdin cannot both be set
                if "stdin" in kwargs:
                    del kwargs["stdin"]
            else:
                logger.warning("No sudo password available but command requires it")
        # Build the SSH command - if using sudo for this command, consider forcing TTY allocation
        needs_tty = False
        cmd_str = " ".join(map(str, remote_cmd))
        if self.config.get("ssh_sudo", False) and not self.config.get(
            "passwordless", False
        ):
            # Check if this is a command that might need TTY for sudo password
            if "sudo" in cmd_str and "-n" not in cmd_str:
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

            result = subprocess.run(ssh_cmd, **kwargs)  # type: ignore
            exit_code = result.returncode  # type: ignore

            if exit_code != 0 and kwargs.get("check", False) is False:
                stderr = (
                    str(result.stderr.decode("utf-8", errors="replace"))  # type: ignore
                    if hasattr(result, "stderr") and result.stderr  # type: ignore
                    else ""
                )
                logger.debug(
                    "Command exited with non-zero code %d: %s\nError: %s",
                    exit_code,  # type: ignore
                    ssh_cmd_str,  # type: ignore
                    stderr,  # type: ignore
                )
                details = {
                    "command": ssh_cmd_str,
                    "exit_code": exit_code,
                    "stderr_length": len(stderr) if stderr else 0,
                    "has_stdout": hasattr(result, "stdout")
                    and getattr(result, "stdout", None) is not None,
                }
                logger.debug("Non-zero exit command details: %s", details)
            elif exit_code == 0:
                logger.debug("Command executed successfully: %s", ssh_cmd_str)  # type: ignore
                if hasattr(result, "stdout") and getattr(result, "stdout", None):
                    stdout_data = getattr(result, "stdout", None)
                    if stdout_data:
                        stdout_len = (
                            len(stdout_data)
                            if isinstance(stdout_data, bytes)
                            else len(str(stdout_data))
                        )
                        logger.debug("Command stdout length: %d bytes", stdout_len)

            logger.debug("Command execution result: exit_code=%d", result.returncode)  # type: ignore
            return result  # type: ignore

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
        """Run btrfs receive on the remote host."""
        receive_cmd = ["btrfs", "receive", destination]
        logger.debug("Preparing btrfs receive command: %s", receive_cmd)

        # If we're testing in this run, we'll disable sudo password prompt
        passwordless_only = os.environ.get(
            "BTRFS_BACKUP_PASSWORDLESS_ONLY", "0"
        ).lower() in ("1", "true", "yes")
        if passwordless_only:
            logger.warning(
                "Running in passwordless-only mode - will not prompt for sudo password"
            )

        # First ensure the destination directory exists and is writable
        destination_dir = os.path.dirname(destination)
        if destination_dir:
            try:
                logger.debug(
                    f"Ensuring destination directory exists: {destination_dir}"
                )
                mkdir_cmd = ["mkdir", "-p", destination_dir]
                result = self._exec_remote_command(mkdir_cmd, check=False)
                if result.returncode != 0:
                    logger.warning(
                        f"Could not create destination directory: {destination_dir}"
                    )
            except Exception as e:
                logger.warning(f"Error creating destination directory: {e}")

        # Test if remote destination directory is writeable without sudo first
        dest_dir = os.path.dirname(destination)
        if dest_dir:
            try:
                logger.debug(
                    f"Testing if destination directory is directly writeable without sudo: {dest_dir}"
                )
                test_cmd = ["touch", f"{dest_dir}/.test_write_access"]
                rm_cmd = ["rm", "-f", f"{dest_dir}/.test_write_access"]
                result = self._exec_remote_command(test_cmd, check=False)
                if result.returncode == 0:
                    logger.info(
                        "Destination directory is directly writeable without sudo!"
                    )
                    # Clean up test file
                    self._exec_remote_command(rm_cmd, check=False)
                    # Also check if btrfs command is available without sudo
                    btrfs_cmd = ["btrfs", "--version"]
                    btrfs_result = self._exec_remote_command(btrfs_cmd, check=False)
                    if btrfs_result.returncode == 0:
                        logger.info(
                            "btrfs command is available without sudo - will try direct receive"
                        )
                        logger.debug("Using direct btrfs receive without sudo")
                    else:
                        logger.debug(
                            "btrfs command requires sudo even though directory is writeable"
                        )
                else:
                    logger.debug("Destination directory requires sudo for writing")
            except Exception as e:
                logger.debug(f"Error testing direct write access: {e}")

        # Force using sudo if ssh_sudo option is enabled, regardless of write access
        if self.config.get("ssh_sudo", False):
            logger.debug(
                "ssh_sudo option is enabled, forcing use of sudo for receive command"
            )
            # (No assignment to requires_sudo)

        # Modify the receive command with sudo if needed
        sudo_enabled = self.config.get("ssh_sudo", False)
        if sudo_enabled:
            logger.info(
                "Using sudo for remote commands - ensure passwordless sudo is configured"
            )
        else:
            logger.warning(
                "btrfs commands require sudo on remote host but --ssh-sudo not specified"
            )
            logger.warning(
                "Consider using --ssh-sudo option to enable sudo on remote host"
            )

        # Build the remote command with optional sudo and proper logging
        receive_log = f"/tmp/btrfs-receive-{int(time.time())}.log"

        # Properly escape the destination path for shell execution
        escaped_dest = destination.replace("'", "'\"'\"'")
        escaped_log = receive_log.replace("'", "'\"'\"'")

        if self.config.get("ssh_sudo", False):
            # Use sudo for btrfs receive with logging
            receive_shell_cmd = f"sudo -n -E btrfs receive '{escaped_dest}' 2>'{escaped_log}'; echo $? >'{escaped_log}.exitcode'"
        else:
            # Direct command with logging
            receive_shell_cmd = f"btrfs receive '{escaped_dest}' 2>'{escaped_log}'; echo $? >'{escaped_log}.exitcode'"

        # Use shell to execute the command with logging
        receive_cmd = ["sh", "-c", receive_shell_cmd]

        # Log the command for debugging
        logger.debug("Built receive shell command: %s", receive_shell_cmd)
        logger.debug("SSH command array: %s", receive_cmd)

        # Determine if we need a TTY for this command (needed if sudo might prompt for password)
        needs_tty = False
        # Always set needs_tty to True if ssh_sudo is enabled, regardless of passwordless setting
        if self.config.get("ssh_sudo", False):
            logger.debug(
                "SSH sudo enabled - forcing TTY allocation for sudo authentication"
            )
            needs_tty = True
        elif not self.config.get("passwordless", False):
            # If using sudo without passwordless, we might need a TTY
            logger.debug("Sudo configuration detected that may require TTY")
            needs_tty = True

        # Build the SSH command with appropriate TTY settings
        ssh_base_cmd = self.ssh_manager.get_ssh_base_cmd(force_tty=needs_tty)

        # Add options to make SSH more resilient to network issues
        # ServerAliveInterval sends a keep-alive packet every N seconds
        # ServerAliveCountMax defines how many missed responses before disconnect
        ssh_options = [
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=3",
            "-o",
            "TCPKeepAlive=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ExitOnForwardFailure=yes",
        ]

        # If we need TTY, ensure related SSH options are set correctly
        if needs_tty:
            ssh_options.extend(
                [
                    "-o",
                    "RequestTTY=yes",
                    "-o",
                    "BatchMode=no",  # Allow password prompts if necessary
                ]
            )
            logger.debug("Adding TTY-related SSH options for sudo authentication")

        # Insert options after the SSH command but before any other arguments
        for i, opt in enumerate(ssh_options):
            ssh_base_cmd.insert(i + 1, opt)

        ssh_cmd = ssh_base_cmd + ["--"] + receive_cmd

        # Log the full command for debugging
        logger.debug("Full SSH btrfs receive command: %s", " ".join(map(str, ssh_cmd)))

        try:
            # Start the btrfs receive process
            logger.debug("Starting btrfs receive process")

            # Set up the environment for better handling of SSH processes
            env = os.environ.copy()

            # Environment setup for SSH authentication
            if needs_tty:
                # If we're using TTY for sudo, try to make session interactive
                if "SSH_ASKPASS" in env:
                    env["SSH_ASKPASS_REQUIRE"] = "force"
                if "DISPLAY" not in env and os.environ.get("SUDO_USER"):
                    sudo_user = os.environ.get("SUDO_USER")
                    if sudo_user:
                        proc = subprocess.run(
                            ["sudo", "-u", sudo_user, "printenv", "DISPLAY"],
                            capture_output=True,
                            text=True,
                            check=False,
                        )
                        if proc.returncode == 0 and proc.stdout and proc.stdout.strip():
                            env["DISPLAY"] = proc.stdout.strip()
                            logger.debug(
                                f"Set DISPLAY for SSH session: {env['DISPLAY']}"
                            )
            else:
                env["SSH_ASKPASS_REQUIRE"] = "never"
            receive_process = subprocess.Popen(
                ssh_cmd,
                stdin=stdin_pipe,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,  # Use unbuffered mode for better streaming
                env=env,
                text=False,  # Use binary mode for streams
                start_new_session=True,  # Avoid signal propagation issues
            )
            logger.debug(
                "btrfs receive process started with PID: %d", receive_process.pid
            )
            logger.debug(
                "If receive fails, verify remote user has sudo access for btrfs commands"
            )
            self._last_receive_log = receive_log
            self._last_transfer_snapshot = True
            return receive_process
        except Exception as e:
            logger.error("Failed to start btrfs receive process: %s", e)
            if isinstance(e, (BrokenPipeError, ConnectionError, ConnectionResetError)):
                logger.error(
                    "SSH connection error detected. The connection might be broken."
                )
                raise ConnectionError(f"SSH connection error: {e}")
            raise

    def list_snapshots(self, flush_cache: bool = False) -> List[Any]:
        """
        List snapshots (btrfs subvolumes) on the remote host at the configured path.
        Returns a list of Snapshot objects.
        """
        path = self.config["path"]
        use_sudo = self.config.get("ssh_sudo", False)
        cmd = ["btrfs", "subvolume", "list", "-o", path]
        if use_sudo:
            cmd = ["sudo", "-n"] + cmd  # Try passwordless sudo first
        try:
            logger.debug(f"Listing remote snapshots with command: %s", cmd)
            result = self._exec_remote_command(
                cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            if result.returncode != 0:
                stderr = (
                    result.stderr.decode(errors="replace").strip()
                    if result.stderr
                    else ""
                )
                # Detect sudo password prompt error
                if use_sudo and (
                    "a password is required" in stderr or "sudo:" in stderr
                ):
                    logger.warning(f"Passwordless sudo failed: {stderr}")
                    # Try password-based sudo, but do NOT let _build_remote_command add another sudo
                    cmd_pw = ["sudo", "-S", "btrfs", "subvolume", "list", "-o", path]
                    logger.info(
                        "Retrying remote snapshot listing with password-based sudo..."
                    )
                    orig_ssh_sudo = self.config.get("ssh_sudo", False)
                    self.config["ssh_sudo"] = False
                    try:
                        result_pw = self._exec_remote_command(
                            cmd_pw,
                            check=False,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                        )
                    finally:
                        self.config["ssh_sudo"] = orig_ssh_sudo
                    if result_pw.returncode == 0:
                        output = (
                            result_pw.stdout.decode(errors="replace")
                            if result_pw.stdout
                            else ""
                        )
                        snapshots: List[Any] = []
                        snap_prefix = self.config.get("snap_prefix", "")
                        for line in output.splitlines():
                            parts = line.split("path ", 1)
                            if len(parts) == 2:
                                snap_path = parts[1].strip()
                                snap_name = os.path.basename(snap_path)
                                if snap_name.startswith(snap_prefix):
                                    date_part = snap_name[len(snap_prefix):]
                                    try:
                                        from btrfs_backup_ng import __util__
                                        time_obj = __util__.str_to_date(date_part)
                                        snapshot = __util__.Snapshot(
                                            self.config["path"], snap_prefix, self, time_obj=time_obj
                                        )
                                        snapshots.append(snapshot)
                                    except Exception as e:
                                        logger.warning(
                                            "Could not parse date from: %r (%s)", snap_name, e
                                        )
                                        continue
                        logger.warning(
                            "Passwordless sudo is not available, but password-based sudo succeeded for remote snapshot listing."
                        )
                        logger.info(
                            f"Found {len(snapshots)} remote snapshots at {path}"
                        )
                        logger.debug(f"Remote snapshot names: {snapshots}")
                        return snapshots
                    else:
                        stderr_pw = (
                            result_pw.stderr.decode(errors="replace").strip()
                            if result_pw.stderr
                            else ""
                        )
                        logger.error(
                            f"Failed to list remote snapshots with password-based sudo: {stderr_pw}"
                        )
                        logger.error(
                            "Passwordless sudo is not available for the remote user '%s' on host '%s'.",
                            self.config.get("username"),
                            self.hostname,
                        )
                        logger.error(
                            "To enable passwordless sudo, add the user to sudoers with NOPASSWD, e.g.:"
                        )
                        logger.error(
                            "    %s ALL=(ALL) NOPASSWD: /usr/bin/btrfs",
                            self.config.get("username"),
                        )
                        logger.error(
                            "SSH endpoint: %s@%s:%s (ssh_sudo=%s)",
                            self.config.get("username"),
                            self.hostname,
                            path,
                            use_sudo,
                        )
                        self._run_diagnostics(path)
                        return []
                else:
                    logger.warning(f"Failed to list remote snapshots: {stderr}")
                    return []
            output = result.stdout.decode(errors="replace") if result.stdout else ""
            snapshots: List[Any] = []
            snap_prefix = self.config.get("snap_prefix", "")
            for line in output.splitlines():
                parts = line.split("path ", 1)
                if len(parts) == 2:
                    snap_path = parts[1].strip()
                    snap_name = os.path.basename(snap_path)
                    if snap_name.startswith(snap_prefix):
                        date_part = snap_name[len(snap_prefix):]
                        try:
                            from btrfs_backup_ng import __util__
                            time_obj = __util__.str_to_date(date_part)
                            snapshot = __util__.Snapshot(
                                self.config["path"], snap_prefix, self, time_obj=time_obj
                            )
                            snapshots.append(snapshot)
                        except Exception as e:
                            logger.warning(
                                "Could not parse date from: %r (%s)", snap_name, e
                            )
                            continue
            snapshots.sort()
            logger.info(f"Found {len(snapshots)} remote snapshots at {path}")
            logger.debug(f"Remote snapshots: {[str(s) for s in snapshots]}")
            return snapshots
        except Exception as e:
            logger.error(f"Exception while listing remote snapshots: {e}")
            self._run_diagnostics(path)
            return []

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
        list_cmd = ["btrfs", "subvolume", "list", "-o", dest_path]
        if self.config.get("ssh_sudo", False):
            list_cmd = ["sudo", "-n"] + list_cmd

        logger.info(f"Verifying snapshot existence with command: {' '.join(list_cmd)}")

        try:
            logger.info("Executing subvolume list command...")
            list_result = self._exec_remote_command(
                list_cmd,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            logger.info(f"Subvolume list command exit code: {list_result.returncode}")
            if list_result.stdout:
                stdout_content = list_result.stdout.decode(errors="replace")
                logger.info(f"Subvolume list output:\n{stdout_content}")
            if list_result.stderr:
                stderr_content = list_result.stderr.decode(errors="replace") 
                logger.info(f"Subvolume list stderr:\n{stderr_content}")
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
                logger.debug(f"Fallback verification command: {' '.join(check_cmd)}")
                check_result = self._exec_remote_command(
                    check_cmd,
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

                logger.debug(f"Path check exit code: {check_result.returncode}")
                if check_result.stdout and b"EXISTS" in check_result.stdout:
                    logger.info(f"Snapshot exists at path: {dest_path}/{snapshot_name}")
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

            # Check if the snapshot name appears in the subvolume list
            stdout_text = (
                list_result.stdout.decode(errors="replace")
                if list_result.stdout
                else ""
            )
            logger.debug(f"Subvolume list output length: {len(stdout_text)} characters")
            logger.debug(f"Searching for snapshot name '{snapshot_name}' in output")

            if list_result.stdout and snapshot_name in stdout_text:
                logger.info(f"Snapshot found in subvolume list: {snapshot_name}")
                logger.debug("Subvolume-based verification successful")
                return True
            else:
                logger.error(f"Snapshot not found in subvolume list")
                logger.debug(f"Subvolume list output: {stdout_text}")

                # Log each line of output for debugging
                if stdout_text:
                    lines = stdout_text.splitlines()
                    logger.debug(f"Subvolume list has {len(lines)} lines:")
                    for i, line in enumerate(lines[:10]):  # Limit to first 10 lines
                        logger.debug(f"  Line {i+1}: {line}")
                    if len(lines) > 10:
                        logger.debug(f"  ... and {len(lines) - 10} more lines")
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
        # Check for pv
        if self._check_command_exists("pv"):
            logger.debug("Found pv - using it for transfer progress")
            return "pv", "pv -q"

        # No buffer program found
        logger.debug("No buffer program (pv) found - transfers may be less reliable")
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

    def _try_direct_transfer(
        self,
        source_path: str,
        dest_path: str,
        snapshot_name: str,
        parent_path: Optional[str] = None,
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
        logger.info(f"Testing SSH connectivity and filesystem...")
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

        # Find buffer program for progress display and reliability
        buffer_name, buffer_cmd = self._find_buffer_program()

        # Get the source snapshot object to use proper send method
        from btrfs_backup_ng import __util__
        
        # Find the source endpoint (should be passed in or accessible)
        # For now, we'll create a minimal snapshot object to use the source endpoint's send method
        try:
            # Create a snapshot object that represents our source
            source_endpoint = None
            # We need to get this from the source path - this is a limitation of the current design
            # For now, we'll use the traditional approach but with better process management
            
            # Determine parent for incremental transfer
            parent_snapshot = None
            if parent_path and os.path.exists(parent_path):
                logger.info(f"Using incremental transfer with parent: {parent_path}")
                # We'll handle incremental logic in the actual send call
            else:
                logger.info(f"Using full transfer")
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
                send_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0
            )
            
            # Set up buffering if available
            if buffer_cmd:
                logger.info(f"Using {buffer_name} to improve transfer reliability")
                buffer_args = buffer_cmd.split()
                buffer_process = subprocess.Popen(
                    buffer_args,
                    stdin=send_process.stdout,
                    stdout=subprocess.PIPE,
                    bufsize=0
                )
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
            
            # Wait for all processes to complete
            logger.debug("Waiting for all transfer processes to complete...")
            
            # Wait for send process
            send_result = send_process.wait()
            logger.debug(f"Send process completed with exit code: {send_result}")
            
            # Wait for buffer process if used
            if buffer_process:
                buffer_result = buffer_process.wait()
                logger.debug(f"Buffer process completed with exit code: {buffer_result}")
            
            # Wait for receive process
            receive_result = receive_process.wait()
            logger.debug(f"Receive process completed with exit code: {receive_result}")
            
            elapsed_time = time.time() - start_time
            logger.info(f"Transfer completed in {elapsed_time:.2f} seconds")
            
            # Check process results
            if send_result != 0:
                stderr_output = send_process.stderr.read().decode(errors="replace") if send_process.stderr else ""
                logger.error(f"Local send process failed with exit code {send_result}: {stderr_output}")
                return False
            
            if receive_result != 0:
                logger.error(f"Remote receive process failed with exit code {receive_result}")
                return False
            
            # Check the actual btrfs receive result from log files
            if hasattr(self, '_last_receive_log'):
                try:
                    # Check exit code file first
                    exitcode_cmd = ["cat", f"{self._last_receive_log}.exitcode"]
                    exitcode_result = self._exec_remote_command(exitcode_cmd, check=False, stdout=subprocess.PIPE)
                    if exitcode_result.returncode == 0 and exitcode_result.stdout:
                        actual_exitcode = exitcode_result.stdout.decode(errors="replace").strip()
                        logger.debug(f"Actual btrfs receive exit code: {actual_exitcode}")
                        if actual_exitcode != "0":
                            # Read the error log
                            log_cmd = ["cat", self._last_receive_log]
                            log_result = self._exec_remote_command(log_cmd, check=False, stdout=subprocess.PIPE)
                            if log_result.returncode == 0 and log_result.stdout:
                                log_content = log_result.stdout.decode(errors="replace")
                                logger.error(f"btrfs receive failed with exit code {actual_exitcode}")
                                logger.error(f"Error details: {log_content}")
                            return False
                except Exception as e:
                    logger.warning(f"Could not check receive log files: {e}")
            
            # Verify the transfer was successful
            logger.debug("Verifying snapshot was created on remote host...")
            if self._verify_snapshot_exists(dest_path, snapshot_name):
                logger.info("Transfer verification successful")
                return True
            else:
                logger.error("Transfer verification failed - snapshot not found on remote host")
                return False
                
        except Exception as e:
            logger.error(f"Error during transfer: {e}")
            logger.debug(f"Full error details: {e}", exc_info=True)
            return False

    def send_receive(self, snapshot, parent=None, clones=None, timeout=3600) -> bool:
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
        logger.info("Starting direct SSH pipe transfer for %s", snapshot)
        
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
                logger.debug("Ensuring remote destination path exists: %s", normalized_path)
                
                cmd = ["test", "-d", normalized_path]
                result = self._exec_remote_command(cmd, check=False)
                if result.returncode != 0:
                    logger.warning("Destination path doesn't exist, creating it: %s", normalized_path)
                    mkdir_cmd = ["mkdir", "-p", normalized_path]
                    mkdir_result = self._exec_remote_command(mkdir_cmd, check=False)
                    if mkdir_result.returncode != 0:
                        stderr = mkdir_result.stderr.decode("utf-8", errors="replace") if mkdir_result.stderr else ""
                        logger.error("Failed to create destination directory: %s", stderr)
                        return False
        except Exception as e:
            logger.error("Error verifying/creating destination: %s", e)
            return False
        
        # Run diagnostics to ensure everything is ready
        logger.debug("Running pre-transfer diagnostics")
        diagnostics = self._run_diagnostics(dest_path)
        if not all([
            diagnostics["ssh_connection"],
            diagnostics["btrfs_command"], 
            diagnostics["write_permissions"],
            diagnostics["btrfs_filesystem"]
        ]):
            logger.error("Pre-transfer diagnostics failed")
            return False
        
        # Use the existing _try_direct_transfer method which has all the logic
        try:
            success = self._try_direct_transfer(
                source_path=snapshot_path,
                dest_path=dest_path,
                snapshot_name=snapshot_name,
                parent_path=parent_path
            )
            
            if success:
                logger.info("Direct SSH pipe transfer completed successfully")
                return True
            else:
                logger.error("Direct SSH pipe transfer failed")
                return False
                
        except Exception as e:
            logger.error("Error during direct SSH pipe transfer: %s", e)
            return False
