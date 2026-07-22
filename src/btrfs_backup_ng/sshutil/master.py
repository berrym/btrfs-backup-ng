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
        ssh_auth_sock: Optional[str] = None,
    ):
        self.hostname = hostname
        self.username = username or getpass.getuser()
        self.port = port
        self.ssh_opts = ssh_opts or []
        self.persist = persist
        self.debug = debug
        self.identity_file = identity_file
        self.allow_password_auth = allow_password_auth
        # Explicit ssh-agent socket override (config `ssh_auth_sock` / CLI / the
        # BTRFS_BACKUP_SSH_AUTH_SOCK env var). Takes precedence over auto-discovery so a
        # multi-agent or non-standard deployment can pin exactly which agent to use.
        self.ssh_auth_sock = ssh_auth_sock or os.environ.get(
            "BTRFS_BACKUP_SSH_AUTH_SOCK"
        )
        # The agent socket actually used for the last connection (for diagnostics/errors),
        # and whether it had keys loaded (drives the auth-failure guidance).
        self._resolved_agent_sock: Optional[str] = None
        self._agent_socket_had_keys: bool = True

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

        # parents=True so a missing ~/.ssh (fresh account, CI, container) does not
        # break endpoint construction with FileNotFoundError.
        self.control_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
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

    @staticmethod
    def _owned_socket(path: Optional[str], uid: int, *, follow: bool) -> bool:
        """True if ``path`` is a unix socket owned by ``uid`` (or root).

        The ownership gate stops a hostile path (in the environment/config or planted in a
        world-writable search dir) from making root connect to an attacker's agent -- only
        root can create a root-owned file, and a non-target user cannot create one owned by
        the target uid. ``follow=False`` uses lstat, so a planted symlink is rejected
        outright (no TOCTOU / redirection). ``follow=True`` is for a user-pinned override /
        preserved SSH_AUTH_SOCK, where a symlink is the user's own choice but the resolved
        target must still be a socket they own.
        """
        if not path:
            return False
        import stat as _stat

        try:
            st = os.stat(path) if follow else os.lstat(path)
        except OSError:
            return False
        return _stat.S_ISSOCK(st.st_mode) and st.st_uid in (uid, 0)

    def _agent_status(self, sock_path: str) -> int:
        """`ssh-add -l` status for an agent socket.

        0 = the agent has keys, 1 = the agent is reachable but has no keys, 2 = the socket
        is dead/unreachable (a leftover file whose agent process died, or a timeout). A
        dead socket must never be chosen -- setting SSH_AUTH_SOCK to it makes ssh waste a
        round trip on an agent it cannot talk to before falling back to password auth."""
        try:
            sub_env = os.environ.copy()
            sub_env["SSH_AUTH_SOCK"] = sock_path
            result = subprocess.run(
                ["ssh-add", "-l"], env=sub_env, capture_output=True, timeout=5
            )
            return result.returncode
        except Exception:  # noqa: BLE001 - any failure means "treat as unreachable"
            return 2

    def _resolve_agent_socket(self, uid: int, env: dict) -> Optional[str]:
        """Resolve which ssh-agent socket to use.

        Precedence: (1) explicit override (config ``ssh_auth_sock`` /
        ``BTRFS_BACKUP_SSH_AUTH_SOCK``), (2) a preserved ``SSH_AUTH_SOCK`` in the
        environment (``sudo -E`` / ``--preserve-env``), (3) auto-discovery. Every source is
        validated to be a socket owned by the invoking user (or root) so a hostile path
        cannot make root connect to an attacker-controlled agent.
        """
        if self.ssh_auth_sock:
            if self._owned_socket(self.ssh_auth_sock, uid, follow=True):
                return self.ssh_auth_sock
            logger.warning(
                "Configured ssh_auth_sock %s is not a usable agent socket owned by the "
                "backup user; falling back to auto-discovery.",
                self.ssh_auth_sock,
            )
        env_sock = env.get("SSH_AUTH_SOCK")
        if env_sock and self._owned_socket(env_sock, uid, follow=True):
            return env_sock
        return self._find_ssh_agent_socket(uid, env)

    def _find_ssh_agent_socket(self, uid: int, env: dict) -> Optional[str]:
        """Find an ssh-agent socket owned by ``uid``, preferring one with keys loaded.

        Needed because sudo strips SSH_AUTH_SOCK, so a passphrase-protected key -- whose
        usable key lives only in the agent -- would otherwise be unusable. Candidates are
        validated as real unix sockets owned by the user (symlinks rejected). This is
        best-effort breadth; setups not covered here (or multi-agent systems) should pin
        ``ssh_auth_sock`` explicitly.

        Args:
            uid: User ID whose agent socket to find.
            env: Environment already prepared by start_master (correct HOME under sudo).

        Returns:
            Path to an agent socket if found, else None.
        """
        import glob

        # Owning user's home for ~/.ssh sockets. Derive from the uid so it is correct under
        # sudo; if the uid is not in the password db (containers/NSS), fall back to the
        # HOME already resolved into env by start_master -- NOT os.path.expanduser, which
        # under sudo would wrongly resolve to root's home.
        try:
            user_home = pwd.getpwuid(uid).pw_dir
        except (KeyError, OSError):
            user_home = env.get("HOME") or os.path.expanduser("~")

        search_paths = [
            "/tmp/ssh-*/agent.*",  # traditional ssh-agent
            f"{user_home}/.ssh/agent/*",  # custom agent managers / keychains
            f"{user_home}/.ssh/agent.sock",
            f"{user_home}/.ssh/*.sock",
            f"{user_home}/.1password/agent.sock",  # 1Password
            f"{user_home}/.bitwarden-ssh-agent.sock",  # Bitwarden
            f"/run/user/{uid}/keyring/ssh",  # GNOME Keyring
            f"/run/user/{uid}/gcr/ssh",  # gcr (newer GNOME)
            f"/run/user/{uid}/gnupg/S.gpg-agent.ssh",  # gpg-agent with ssh support
            f"/run/user/{uid}/ssh-agent.socket",  # systemd per-user
            f"/run/user/{uid}/openssh_agent",  # openssh per-user
        ]

        def candidates():
            # sorted() so a deterministic socket is chosen when a glob matches several.
            for pattern in search_paths:
                if "*" in pattern:
                    yield from sorted(glob.glob(pattern))
                elif os.path.exists(pattern):
                    yield pattern

        # Pass 1: a live agent that actually has keys (what enables passphrase-key auth).
        for sock in candidates():
            if (
                self._owned_socket(sock, uid, follow=False)
                and self._agent_status(sock) == 0
            ):
                logger.debug("Found agent socket with keys: %s", sock)
                self._agent_socket_had_keys = True
                return sock
        # Pass 2: a REACHABLE agent with no keys (status 1). A dead/stale socket (status 2 --
        # a leftover file whose agent process died) is skipped so we never set SSH_AUTH_SOCK
        # to a socket ssh cannot talk to, which would only add latency/noise before password
        # fallback. Kept so the failure path can say "an agent is running but has no keys"
        # instead of "none found".
        for sock in candidates():
            if (
                self._owned_socket(sock, uid, follow=False)
                and self._agent_status(sock) == 1
            ):
                logger.debug("Found reachable agent socket (no keys loaded): %s", sock)
                self._agent_socket_had_keys = False
                return sock
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

        # Reset per-attempt state so a stale failure flag from a prior attempt cannot
        # skew the retry decision in start_master.
        self._password_auth_failed = False

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
                agent_uid = sudo_user_info.pw_uid
            else:
                agent_uid = os.getuid()

            # Resolve the ssh-agent socket. A passphrase-protected key can only be used
            # for signing via its agent; sudo strips SSH_AUTH_SOCK, so without this the
            # server accepts the offered public key but the client cannot sign it ->
            # "Permission denied". Precedence: explicit override, then a preserved
            # SSH_AUTH_SOCK, then auto-discovery across common socket locations.
            agent_sock = self._resolve_agent_socket(agent_uid, env)
            if agent_sock:
                env["SSH_AUTH_SOCK"] = agent_sock
                self._resolved_agent_sock = agent_sock
                logger.debug("Using ssh-agent socket: %s", agent_sock)
            else:
                # No usable agent. Clear any inherited SSH_AUTH_SOCK so ssh does not try a
                # stale/foreign socket before falling back to password auth.
                env.pop("SSH_AUTH_SOCK", None)
                self._resolved_agent_sock = None
                logger.debug("No usable ssh-agent socket found")

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
                self._log_auth_failure_help()
                logger.info("  - BTRFS_BACKUP_SSH_PASSWORD environment variable")
                logger.info("  - Interactive prompt (when running in a terminal)")
                logger.info("  - Install 'sshpass' for non-interactive password auth")
            else:
                logger.error(
                    f"SSH key authentication failed for {self.username}@{self.hostname}"
                )
                self._log_auth_failure_help()

            return False

    def _log_auth_failure_help(self) -> None:
        """Emit actionable remediation for an SSH auth failure.

        The most common failure under sudo is a passphrase-protected key whose usable
        (decrypted) key lives only in the user's ssh-agent, which sudo cannot see because
        it strips SSH_AUTH_SOCK. Tell the user exactly how to fix that."""
        if self._resolved_agent_sock:
            if self._agent_socket_had_keys:
                logger.info(
                    "An ssh-agent was used (%s) but did not provide a key the server "
                    "accepted; check `ssh-add -l` and that the key is authorized on the "
                    "server.",
                    self._resolved_agent_sock,
                )
            else:
                logger.info(
                    "An ssh-agent was found (%s) but has NO keys loaded; run `ssh-add` to "
                    "load your key, or point ssh_auth_sock at the agent that holds it.",
                    self._resolved_agent_sock,
                )
            return
        logger.info("No usable ssh-agent was found for %s.", self.username)
        if self.running_as_sudo:
            logger.info(
                "sudo strips SSH_AUTH_SOCK, so a passphrase-protected key (whose key "
                "lives in your agent) cannot sign. Fix with ONE of:"
            )
            logger.info(
                "  - run: sudo --preserve-env=SSH_AUTH_SOCK <command>  (or sudo -E)"
            )
        else:
            logger.info(
                "  - ensure your ssh-agent is running and holds the key: "
                "`eval $(ssh-agent)` then `ssh-add`"
            )
        logger.info(
            "  - set ssh_auth_sock in the target config, export "
            "BTRFS_BACKUP_SSH_AUTH_SOCK=$SSH_AUTH_SOCK, or pass --ssh-auth-sock"
        )
        logger.info("  - or use a passphrase-less key via ssh_key")

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
