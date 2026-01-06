import getpass
import os
import pwd
import shutil
import subprocess
import threading
from pathlib import Path
from typing import List, Optional

from btrfs_backup_ng.__logger__ import logger


class SSHMasterManager:
    """Manages SSH master connections with password fallback support.

    This class handles SSH connections using control sockets for connection
    reuse. It supports multiple authentication methods:
    1. SSH key-based authentication (preferred)
    2. SSH agent forwarding
    3. Password authentication fallback (when keys fail)

    Password authentication can be provided via:
    - BTRFS_BACKUP_SSH_PASSWORD environment variable
    - Interactive prompt (when running in a TTY)
    - sshpass utility (if available)
    """

    def __init__(
        self,
        hostname: str,
        username: Optional[str] = None,
        port: Optional[int] = None,
        ssh_opts: Optional[List[str]] = None,
        control_dir: Optional[str] = None,
        persist: str = "60",
        debug: bool = False,
        identity_file: Optional[str] = None,
        allow_password_auth: bool = False,
    ):
        self.hostname = hostname
        self.username = username or getpass.getuser()
        self.port = port
        self.ssh_opts = ssh_opts or []
        self.persist = persist
        self.debug = debug
        self.identity_file = identity_file
        self.allow_password_auth = allow_password_auth

        # Password caching
        self._cached_ssh_password: Optional[str] = None
        self._password_auth_failed = False

        self.running_as_sudo = (
            os.environ.get("SUDO_USER") is not None and os.geteuid() == 0
        )
        self.sudo_user = os.environ.get("SUDO_USER")

        if self.running_as_sudo and self.sudo_user:
            self.ssh_config_dir = Path(pwd.getpwnam(self.sudo_user).pw_dir) / ".ssh"
        else:
            self.ssh_config_dir = Path.home() / ".ssh"

        if control_dir:
            self.control_dir = Path(control_dir)
        else:
            if self.running_as_sudo:
                self.control_dir = Path(f"/tmp/ssh-controlmasters-{self.sudo_user}")
            else:
                self.control_dir = self.ssh_config_dir / "controlmasters"

        self.control_dir.mkdir(mode=0o700, exist_ok=True)
        self._instance_id = f"{os.getpid()}_{threading.get_ident()}"
        self.control_path = (
            self.control_dir
            / f"cm_{self.username}_{self.hostname}_{self._instance_id}.sock"
        )
        self._lock = threading.Lock()
        self._master_started = False

    def _ssh_base_cmd(self, force_tty: bool = False) -> List[str]:
        """Build base SSH command with appropriate options.

        Args:
            force_tty: Whether to force TTY allocation with -tt flag

        Returns:
            List of command arguments for SSH
        """
        cmd = ["ssh"]
        if force_tty:
            cmd.append("-tt")

        # SSH options for better reliability
        opts = [
            f"ControlPath={self.control_path}",
            "ControlMaster=auto",
            f"ControlPersist={self.persist}",
            "ServerAliveInterval=5",
            "ServerAliveCountMax=6",
            "TCPKeepAlive=yes",
            "ConnectTimeout=30",
            "ConnectionAttempts=3",
            "StrictHostKeyChecking=accept-new",
            "PasswordAuthentication=yes",
            "PubkeyAuthentication=yes",
            "PreferredAuthentications=publickey,keyboard-interactive,password",
        ]

        # Only use BatchMode if password authentication is not needed
        if not self.allow_password_auth:
            opts.append("BatchMode=yes")
        else:
            opts.append("BatchMode=no")

        for opt in opts:
            cmd.extend(["-o", opt])

        if self.port:
            cmd.extend(["-p", str(self.port)])

        if self.identity_file:
            cmd.extend(["-i", str(self.identity_file)])

        cmd.append(f"{self.username}@{self.hostname}")
        return cmd

    def _get_ssh_password(self, retry: bool = False) -> Optional[str]:
        """Get SSH password from environment or interactive prompt.

        Args:
            retry: If True, clear cached password and prompt again

        Returns:
            Password string or None if unavailable
        """
        # Clear cache on retry
        if retry:
            self._cached_ssh_password = None

        # Return cached password if available
        if self._cached_ssh_password:
            return self._cached_ssh_password

        # Check environment variable
        env_password = os.environ.get("BTRFS_BACKUP_SSH_PASSWORD")
        if env_password:
            logger.debug(
                "Using SSH password from BTRFS_BACKUP_SSH_PASSWORD environment variable"
            )
            self._cached_ssh_password = env_password
            return env_password

        # Try interactive prompt - getpass reads from /dev/tty directly,
        # so it can work even when stdin isn't a TTY (e.g., under sudo)
        def can_prompt_password() -> bool:
            """Check if we can prompt for a password via /dev/tty."""
            try:
                with open("/dev/tty", "r"):
                    return True
            except (OSError, IOError):
                return False

        if can_prompt_password():
            try:
                prompt = f"SSH password for {self.username}@{self.hostname}: "
                password = getpass.getpass(prompt)
                if password:
                    self._cached_ssh_password = password
                    return password
            except (EOFError, KeyboardInterrupt):
                logger.debug("Password prompt cancelled")
            except Exception as e:
                logger.debug(f"Failed to get password interactively: {e}")
        else:
            logger.debug("Cannot access /dev/tty, cannot prompt for SSH password")

        return None

    def _has_sshpass(self) -> bool:
        """Check if sshpass utility is available."""
        return shutil.which("sshpass") is not None

    def _find_ssh_agent_socket(self, uid: int) -> Optional[str]:
        """Find SSH agent socket for a given user.

        Searches common locations for SSH agent sockets. This is useful when
        running with sudo where SSH_AUTH_SOCK may not be preserved.

        The search prioritizes sockets that are likely to have keys loaded:
        1. Traditional ssh-agent sockets in /tmp (from ssh-agent or ssh -A)
        2. GNOME Keyring socket
        3. GPG agent with SSH support
        4. systemd ssh-agent socket (often empty/unused)

        Args:
            uid: User ID to search for agent sockets

        Returns:
            Path to agent socket if found, None otherwise
        """
        import glob
        import subprocess

        # Common locations for ssh-agent sockets, ordered by likelihood of having keys
        search_paths = [
            # Traditional ssh-agent in /tmp - most likely to have keys from ssh-add
            "/tmp/ssh-*/agent.*",
            # GNOME Keyring - often has keys from login
            f"/run/user/{uid}/keyring/ssh",
            # gpg-agent with ssh support
            f"/run/user/{uid}/gnupg/S.gpg-agent.ssh",
            # systemd ssh-agent - often exists but may be empty
            f"/run/user/{uid}/ssh-agent.socket",
        ]

        def socket_has_keys(sock_path: str) -> bool:
            """Check if an agent socket has any keys loaded."""
            try:
                env = os.environ.copy()
                env["SSH_AUTH_SOCK"] = sock_path
                result = subprocess.run(
                    ["ssh-add", "-l"],
                    env=env,
                    capture_output=True,
                    timeout=5,
                )
                # Return code 0 means keys are loaded
                # Return code 1 means "no identities"
                return result.returncode == 0
            except Exception:
                return False

        # First pass: find sockets with keys loaded
        for pattern in search_paths:
            if "*" in pattern:
                # Glob pattern - find matching sockets owned by the user
                for sock in glob.glob(pattern):
                    try:
                        stat = os.stat(sock)
                        if stat.st_uid == uid and socket_has_keys(sock):
                            logger.debug(f"Found agent socket with keys: {sock}")
                            return sock
                    except (OSError, IOError):
                        continue
            else:
                # Exact path
                if os.path.exists(pattern):
                    try:
                        stat = os.stat(pattern)
                        if stat.st_uid == uid and socket_has_keys(pattern):
                            logger.debug(f"Found agent socket with keys: {pattern}")
                            return pattern
                    except (OSError, IOError):
                        continue

        # Second pass: return any accessible socket (even without keys)
        # This allows password auth fallback to work
        for pattern in search_paths:
            if "*" in pattern:
                for sock in glob.glob(pattern):
                    try:
                        stat = os.stat(sock)
                        if stat.st_uid == uid:
                            logger.debug(f"Found agent socket (no keys): {sock}")
                            return sock
                    except (OSError, IOError):
                        continue
            else:
                if os.path.exists(pattern):
                    try:
                        stat = os.stat(pattern)
                        if stat.st_uid == uid:
                            logger.debug(f"Found agent socket (no keys): {pattern}")
                            return pattern
                    except (OSError, IOError):
                        continue

        return None

    def _try_key_auth(self, env: dict) -> bool:
        """Try SSH connection with key-based authentication.

        Args:
            env: Environment dictionary for subprocess

        Returns:
            True if connection succeeded, False otherwise
        """
        cmd = self._ssh_base_cmd()
        cmd.insert(1, "-MNf")
        # Force batch mode for key auth test
        cmd.insert(1, "-o")
        cmd.insert(2, "BatchMode=yes")

        try:
            result = subprocess.run(cmd, env=env, capture_output=True, timeout=30)
            if result.returncode == 0:
                self._master_started = True
                logger.debug("SSH key-based authentication succeeded")
                return True
            else:
                stderr = result.stderr.decode("utf-8", errors="replace")
                logger.debug(f"SSH key auth failed: {stderr}")
                return False
        except subprocess.TimeoutExpired:
            logger.debug("SSH key auth timed out")
            return False
        except Exception as e:
            logger.debug(f"SSH key auth error: {e}")
            return False

    def _try_password_auth(self, env: dict, password: str) -> bool:
        """Try SSH connection with password authentication using sshpass.

        Args:
            env: Environment dictionary for subprocess
            password: SSH password to use

        Returns:
            True if connection succeeded, False otherwise
        """
        if not self._has_sshpass():
            logger.debug("sshpass not available for password authentication")
            return False

        # Build command with sshpass
        ssh_cmd = self._ssh_base_cmd()
        ssh_cmd.insert(1, "-MNf")

        cmd = ["sshpass", "-e"] + ssh_cmd

        # Set password in environment for sshpass -e
        password_env = env.copy()
        password_env["SSHPASS"] = password

        try:
            result = subprocess.run(
                cmd, env=password_env, capture_output=True, timeout=30
            )
            if result.returncode == 0:
                self._master_started = True
                logger.debug("SSH password authentication succeeded via sshpass")
                return True
            else:
                stderr = result.stderr.decode("utf-8", errors="replace")
                if "Permission denied" in stderr or "password" in stderr.lower():
                    self._password_auth_failed = True
                logger.debug(f"SSH password auth failed: {stderr}")
                return False
        except subprocess.TimeoutExpired:
            logger.debug("SSH password auth timed out")
            return False
        except Exception as e:
            logger.debug(f"SSH password auth error: {e}")
            return False

    def _try_interactive_password_auth(self, env: dict) -> bool:
        """Try SSH connection with interactive password authentication.

        This allows SSH to prompt for password directly if /dev/tty is accessible.
        Works even when stdin isn't a TTY (e.g., under sudo).

        Args:
            env: Environment dictionary for subprocess

        Returns:
            True if connection succeeded, False otherwise
        """
        # Check if /dev/tty is accessible for interactive prompts
        try:
            with open("/dev/tty", "r"):
                pass
        except (OSError, IOError):
            logger.debug("Cannot access /dev/tty, cannot use interactive password auth")
            return False

        cmd = self._ssh_base_cmd()
        # Use -tt to force TTY allocation for password prompt
        cmd.insert(1, "-tt")
        cmd.insert(2, "-MNf")

        try:
            # Run without capturing output to allow interactive password prompt
            result = subprocess.run(
                cmd,
                env=env,
                timeout=60,  # Give user time to enter password
            )
            if result.returncode == 0:
                self._master_started = True
                logger.debug("SSH interactive password authentication succeeded")
                return True
            else:
                logger.debug("SSH interactive password auth failed")
                return False
        except subprocess.TimeoutExpired:
            logger.debug("SSH interactive auth timed out")
            return False
        except Exception as e:
            logger.debug(f"SSH interactive auth error: {e}")
            return False

    def start_master(self) -> bool:
        """Start SSH master connection with authentication fallback.

        Tries authentication methods in order:
        1. Key-based authentication
        2. Password via sshpass (if password available and sshpass installed)
        3. Interactive password prompt (if in TTY)

        Returns:
            True if master connection started successfully, False otherwise
        """
        with self._lock:
            if self.is_master_alive():
                return True

            env = os.environ.copy()
            if self.running_as_sudo and self.sudo_user:
                sudo_user_info = pwd.getpwnam(self.sudo_user)
                env["HOME"] = sudo_user_info.pw_dir
                env["USER"] = self.sudo_user

                # Preserve SSH_AUTH_SOCK for ssh-agent access
                # When running with sudo, the agent socket variable is often cleared
                # but the socket itself may still be accessible
                if "SSH_AUTH_SOCK" not in env or not os.path.exists(
                    env.get("SSH_AUTH_SOCK", "")
                ):
                    agent_sock = self._find_ssh_agent_socket(sudo_user_info.pw_uid)
                    if agent_sock:
                        env["SSH_AUTH_SOCK"] = agent_sock
                        logger.debug(f"Found SSH agent socket: {agent_sock}")

            # Try key-based auth first
            logger.debug("Attempting SSH key-based authentication...")
            if self._try_key_auth(env):
                return True

            # If password auth is allowed, try fallback methods
            if self.allow_password_auth:
                logger.debug("Key auth failed, attempting password fallback...")

                # If sshpass is available, try password from env or prompt
                if self._has_sshpass():
                    password = self._get_ssh_password()
                    if password:
                        logger.debug("Trying sshpass password authentication...")
                        if self._try_password_auth(env, password):
                            return True

                        # If password failed, try prompting for new one
                        if self._password_auth_failed:
                            logger.warning(
                                "SSH password authentication failed, trying again..."
                            )
                            password = self._get_ssh_password(retry=True)
                            if password and self._try_password_auth(env, password):
                                return True

                # Interactive password prompt (let SSH prompt natively)
                if self._try_interactive_password_auth(env):
                    return True

                logger.error(
                    f"All SSH authentication methods failed for {self.username}@{self.hostname}"
                )
                logger.info(
                    "Ensure SSH keys are properly configured or provide password via:"
                )
                logger.info("  - BTRFS_BACKUP_SSH_PASSWORD environment variable")
                logger.info("  - Interactive prompt (when running in a terminal)")
                logger.info("  - Install 'sshpass' for non-interactive password auth")
            else:
                logger.error(
                    f"SSH key authentication failed for {self.username}@{self.hostname}"
                )
                logger.info(
                    "To enable password fallback, the connection must allow password auth"
                )

            return False

    def stop_master(self) -> bool:
        """Stop the SSH master connection.

        Returns:
            True if stopped successfully or wasn't running, False on error
        """
        if not self._master_started:
            return True

        with self._lock:
            cmd = [
                "ssh",
                "-O",
                "exit",
                "-o",
                f"ControlPath={self.control_path}",
                f"{self.username}@{self.hostname}",
            ]
            try:
                subprocess.run(cmd, check=True, capture_output=True)
                self._master_started = False
                return True
            except Exception as e:
                logger.error(f"Failed to stop SSH master: {e}")
                return False

    def is_master_alive(self) -> bool:
        """Check if master connection is still alive.

        Returns:
            True if master is alive, False otherwise
        """
        if not self.control_path.exists():
            return False

        cmd = [
            "ssh",
            "-O",
            "check",
            "-o",
            f"ControlPath={self.control_path}",
            f"{self.username}@{self.hostname}",
        ]

        try:
            subprocess.run(cmd, check=True, capture_output=True)
            return True
        except Exception:
            return False

    def cleanup_socket(self) -> None:
        """Clean up the control socket file."""
        try:
            if self.control_path.exists():
                self.control_path.unlink()
        except Exception as e:
            logger.error(f"Failed to cleanup socket: {e}")

    def get_ssh_base_cmd(self, force_tty: bool = False) -> List[str]:
        """Get the base SSH command with all necessary options.

        Args:
            force_tty: Whether to force TTY allocation with -tt flag

        Returns:
            List of SSH command arguments
        """
        return self._ssh_base_cmd(force_tty=force_tty)
