# pyright: strict

"""SSHMasterManager: Robust SSH ControlMaster lifecycle and socket management.

Provides:
- Unique socket path generation (per host/user/process/thread)
- Master connection start/stop/status
- Cleanup of stale sockets
- Context manager for safe usage
"""

import os
import pwd
import subprocess
import threading
import time
import tempfile
import getpass
from pathlib import Path
from typing import Optional, List, Type, Any, List

from btrfs_backup_ng.__logger__ import logger


class SSHMasterManager:
    """
    Robustly manages an SSH ControlMaster persistent connection.
    Ensures unique socket per process/thread, status checks, and cleanup.
    """

    def __init__(
        self,
        hostname: str,
        username: Optional[str] = None,
        port: Optional[int] = None,
        ssh_opts: Optional[list] = None,
        control_dir: Optional[str] = None,
        persist: str = "60",
        debug: bool = False,
        identity_file: Optional[str] = None,
    ):
        self.hostname = hostname
        self.username = username or getpass.getuser()
        self.port = port
        self.ssh_opts = ssh_opts or []
        self.persist = persist
        self.debug = debug

        # Detect if we're running as root via sudo but should use regular user's SSH config
        self.running_as_sudo = (
            os.environ.get("SUDO_USER") is not None and os.geteuid() == 0
        )
        self.sudo_user = os.environ.get("SUDO_USER")

        # Store the sudo user's home directory for proper path expansion
        self.sudo_user_home = None
        if self.running_as_sudo and self.sudo_user:
            try:
                self.sudo_user_home = pwd.getpwnam(self.sudo_user).pw_dir
                if self.debug:
                    logger.debug(f"Sudo user home directory: {self.sudo_user_home}")
            except Exception as e:
                if self.debug:
                    logger.warning(f"Failed to get sudo user home directory: {e}")

        # Process identity file path immediately to handle ~ expansion correctly
        if identity_file:
            # Save the original identity file path for debugging
            self.original_identity_file = identity_file

            if self.running_as_sudo and self.sudo_user_home:
                # Convert relative paths to absolute using sudo user's home
                if identity_file.startswith("~"):
                    identity_file = identity_file.replace("~", self.sudo_user_home, 1)
                    if self.debug:
                        logger.debug(f"Expanded identity file path: {identity_file}")

                # If still not absolute, prefix with sudo user's home
                if not os.path.isabs(identity_file):
                    identity_file = os.path.join(self.sudo_user_home, identity_file)
                    if self.debug:
                        logger.debug(
                            f"Made identity file path absolute: {identity_file}"
                        )

                # Ensure the file exists and is readable
                id_path = Path(identity_file)
                if not id_path.exists():
                    logger.warning(f"Identity file does not exist: {id_path}")
                elif not os.access(str(id_path), os.R_OK):
                    logger.warning(f"Identity file is not readable: {id_path}")

                if self.debug:
                    logger.debug(
                        f"Using identity file (after sudo expansion): {identity_file}"
                    )

        # Store identity file path (now properly expanded if needed)
        self.identity_file = identity_file

        if self.running_as_sudo and self.debug:
            logger.debug(f"Detected running as sudo from user {self.sudo_user}")

        # Figure out which home directory to use for SSH config
        if self.running_as_sudo and self.sudo_user:
            # Use the sudo user's home directory for SSH config
            import pwd

            sudo_user_home = pwd.getpwnam(self.sudo_user).pw_dir
            self.ssh_config_dir = Path(sudo_user_home) / ".ssh"
            if self.debug:
                logger.debug(
                    f"Using sudo user's SSH config directory: {self.ssh_config_dir}"
                )
        else:
            # Use current user's home directory
            self.ssh_config_dir = Path.home() / ".ssh"

        # Use a dedicated directory for control sockets
        if control_dir:
            self.control_dir = Path(control_dir)
        else:
            # Create controlmasters in /tmp when running as sudo to avoid permission issues
            if self.running_as_sudo:
                self.control_dir = Path(f"/tmp/ssh-controlmasters-{self.sudo_user}")
            else:
                self.control_dir = self.ssh_config_dir / "controlmasters"

        self.control_dir.mkdir(mode=0o700, exist_ok=True)

        if self.debug:
            logger.debug(f"Using control directory: {self.control_dir}")

        # Unique socket per host/user/process/thread
        self._instance_id = f"{os.getpid()}_{threading.get_ident()}"
        self.control_path = (
            self.control_dir
            / f"cm_{self.username}_{self.hostname.replace(':', '_')}_{self._instance_id}.sock"
        )

        self._lock = threading.Lock()
        self._master_started = False

    def _ssh_base_cmd(self, force_tty=False):
        """Build the base SSH command.

        Args:
            force_tty: If True, add -t to force TTY allocation (needed for sudo)
        """
        cmd = [
            "ssh",
            "-o",
            f"ControlPath={self.control_path}",
            "-o",
            "ControlMaster=auto",
            "-o",
            f"ControlPersist={self.persist}",
            "-o",
            "BatchMode=yes",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=6",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ConnectionAttempts=3",
            "-o",
            "TCPKeepAlive=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "ExitOnForwardFailure=yes",
            "-o",
            "IPQoS=throughput",
            "-o",
            "PreferredAuthentications=publickey,password",
        ]

        # Handle TTY allocation if requested
        if force_tty:
            # Force TTY allocation (for interactive sudo)
            cmd.append("-t")
            logger.debug("Forcing TTY allocation for SSH command")

        # When running as root via sudo, explicitly use the regular user's SSH config
        if self.running_as_sudo and self.sudo_user:
            # Use the regular user's SSH config
            known_hosts_path = Path(f"{self.ssh_config_dir}/known_hosts")
            ssh_config_path = Path(f"{self.ssh_config_dir}/config")

            if known_hosts_path.exists():
                cmd.extend(["-o", f"UserKnownHostsFile={str(known_hosts_path)}"])

            if ssh_config_path.exists():
                cmd.extend(["-F", f"{str(ssh_config_path)}"])

            # Log what we're using
            if self.debug:
                logger.debug(
                    f"Using sudo user's SSH config directory: {self.ssh_config_dir}"
                )
                if known_hosts_path.exists():
                    logger.debug(f"Using known_hosts file: {known_hosts_path}")
                if ssh_config_path.exists():
                    logger.debug(f"Using SSH config file: {ssh_config_path}")

            # Only add default identity files if no explicit one is provided
            if not self.identity_file:
                # Try to find viable identity files
                potential_ids = [
                    Path(f"{self.ssh_config_dir}/id_rsa"),
                    Path(f"{self.ssh_config_dir}/id_ed25519"),
                    Path(f"{self.ssh_config_dir}/id_ecdsa"),
                    Path(f"{self.ssh_config_dir}/id_dsa"),
                ]

                found_id = False
                for id_path in potential_ids:
                    if id_path.exists() and os.access(str(id_path), os.R_OK):
                        cmd.extend(["-i", str(id_path)])
                        found_id = True
                        if self.debug:
                            logger.debug(
                                f"Using auto-detected identity file: {id_path}"
                            )

                if not found_id and self.debug:
                    logger.warning(
                        f"No readable identity files found in {self.ssh_config_dir}"
                    )
                    logger.warning(
                        "SSH authentication will likely fail without identity files"
                    )

                    # Try agent-based authentication as a fallback
                    if "SSH_AUTH_SOCK" in os.environ:
                        logger.debug(
                            "SSH agent socket found: %s",
                            os.environ.get("SSH_AUTH_SOCK"),
                        )
                        logger.debug("Will try using SSH agent authentication")

            if self.debug:
                logger.debug(f"SSH command configured for sudo user {self.sudo_user}")

        if self.port:
            cmd += ["-p", str(self.port)]
        for opt in self.ssh_opts:
            cmd += ["-o", opt]

        # Add explicit identity file if provided
        if self.identity_file:
            # Use the identity file path that was properly expanded in __init__
            identity_path = Path(self.identity_file)
            cmd += ["-i", str(identity_path)]

            if self.debug:
                logger.debug(f"Using explicit identity file: {identity_path}")
                if not identity_path.exists():
                    logger.warning(f"Identity file does not exist: {identity_path}")
                    if self.running_as_sudo:
                        logger.warning(
                            f"When running with sudo, ensure file permissions allow root to read it"
                        )
                        logger.warning(
                            f"Or try using an absolute path like: /home/{self.sudo_user}/.ssh/id_ed25519"
                        )
                elif not os.access(str(identity_path), os.R_OK):
                    logger.warning(
                        f"Identity file exists but is not readable: {identity_path}"
                    )
                    if self.running_as_sudo:
                        logger.warning(
                            f"Check file permissions: chmod 600 {identity_path}"
                        )
                else:
                    logger.debug(f"Identity file exists and is readable")

        connect_str = f"{self.username}@{self.hostname}"
        cmd.append(connect_str)

        if self.debug:
            logger.debug(f"SSH base command: {' '.join(cmd)}")

        return cmd

    def get_ssh_base_cmd(self, force_tty: bool = False) -> List[str]:
        """Get the base SSH command with control options.

        Args:
            force_tty: If True, add -t to force TTY allocation (needed for sudo)

        Returns:
            List of command arguments for SSH
        """
        cmd = ["ssh"]

        # Add control path and other connection options
        cmd.extend(
            [
                "-o",
                f"ControlPath={self.control_path}",
                "-o",
                "ControlMaster=auto",
                "-o",
                f"ControlPersist={self.persist}",
                "-o",
                "BatchMode=yes",
                "-o",
                "ServerAliveInterval=5",
                "-o",
                "ServerAliveCountMax=3",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "ExitOnForwardFailure=yes",
            ]
        )

        # Add force TTY if requested
        if force_tty:
            cmd.append("-t")

        # Add port if specified
        if self.port:
            cmd.extend(["-p", str(self.port)])

        # Add SSH options from config
        if self.ssh_opts:
            cmd.extend(self.ssh_opts)

        # Add username and host
        dest = f"{self.username}@{self.hostname}" if self.username else self.hostname
        cmd.append(dest)

        return cmd

    def start_master(self, timeout: float = 20.0, retries: int = 3) -> bool:
        """Start the SSH master connection if not already running."""
        with self._lock:
            if self.is_master_alive():
                if self.debug:
                    logger.debug(f"SSH master already running at {self.control_path}")
                self._master_started = True
                return True

            # Clean up any stale socket first
            self.cleanup_socket()

            # Log additional info when running as sudo
            if self.running_as_sudo and self.debug:
                logger.debug(
                    f"Starting SSH master while running as sudo (original user: {self.sudo_user})"
                )
                logger.debug(f"Using SSH config: {self.ssh_config_dir}")
                logger.debug(f"Control path: {self.control_path}")
                if self.identity_file:
                    logger.debug(
                        f"Original identity file before sudo adjustment: {self.identity_file}"
                    )
                    if self.sudo_user_home and self.identity_file.startswith("~"):
                        adjusted_path = self.identity_file.replace(
                            "~", self.sudo_user_home, 1
                        )
                        logger.debug(
                            f"Adjusted identity file for sudo user: {adjusted_path}"
                        )

            for attempt in range(1, retries + 1):
                cmd = self._ssh_base_cmd()
                cmd.insert(1, "-MNf")  # -M: master, -N: no command, -f: background

                if self.debug:
                    logger.debug(
                        f"Starting SSH master as {self.username}@{self.hostname} (attempt {attempt}/{retries}): {' '.join(map(str, cmd))}"
                    )

                try:
                    # Try to verify SSH keys are accepted before starting master
                    test_cmd = self._ssh_base_cmd() + [
                        "-o",
                        "ControlMaster=no",
                        "true",
                    ]

                    if self.running_as_sudo and self.debug:
                        logger.debug(
                            f"Testing SSH connectivity with: {' '.join(map(str, test_cmd))}"
                        )

                    # Make the environment explicit when running as sudo
                    env = os.environ.copy()
                    if self.running_as_sudo and self.sudo_user:
                        # Ensure SSH doesn't look for root's config
                        # Use the actual home directory of the sudo user instead of assuming /home
                        env["HOME"] = pwd.getpwnam(self.sudo_user).pw_dir
                        env["USER"] = self.sudo_user
                        # Ensure we don't try to use askpass programs when running non-interactively
                        env["SSH_ASKPASS_REQUIRE"] = "never"
                        # Also tell SSH to allocate a TTY if we're going to use sudo
                        if "sudo" in " ".join(test_cmd):
                            logger.debug(
                                "Sudo command detected in test, will try to allocate TTY"
                            )
                        # Also set SSH_AUTH_SOCK if it exists in the original environment
                        sudo_auth_sock = None

                        # Try to find the original user's SSH_AUTH_SOCK
                        try:
                            # Read /proc/pid/environ for the sudo process to find original SSH_AUTH_SOCK
                            sudo_pid = os.environ.get("SUDO_COMMAND", "").split()[0]
                            if sudo_pid and sudo_pid.isdigit():
                                with open(f"/proc/{sudo_pid}/environ", "rb") as f:
                                    env_data = f.read().split(b"\0")
                                    for var in env_data:
                                        if var.startswith(b"SSH_AUTH_SOCK="):
                                            sudo_auth_sock = var.decode(
                                                "utf-8", errors="ignore"
                                            ).split("=", 1)[1]
                                            break
                        except Exception:
                            pass

                        if sudo_auth_sock:
                            env["SSH_AUTH_SOCK"] = sudo_auth_sock
                            if self.debug:
                                logger.debug(
                                    f"Using sudo user's SSH_AUTH_SOCK: {sudo_auth_sock}"
                                )

                        if self.debug:
                            logger.debug(
                                f"Using modified environment: HOME={env['HOME']}, USER={env['USER']}"
                            )

                    test_proc = subprocess.run(
                        test_cmd,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                        timeout=timeout / 2,
                        check=False,
                        env=env,
                    )

                    if test_proc.returncode != 0:
                        logger.warning(
                            f"SSH connectivity test failed on attempt {attempt}: {test_proc.stderr.decode().strip()}"
                        )
                        if attempt < retries:
                            time.sleep(1)
                            continue
                        return False

                    # Make the environment explicit when running as sudo
                    env = os.environ.copy()
                    if self.running_as_sudo and self.sudo_user:
                        # Ensure SSH doesn't look for root's config
                        # Use the actual home directory of the sudo user instead of assuming /home
                        env["HOME"] = pwd.getpwnam(self.sudo_user).pw_dir
                        env["USER"] = self.sudo_user

                    # If we're using sudo, we might need TTY allocation
                    if "sudo" in " ".join(cmd) and not any(
                        x in " ".join(cmd) for x in ["btrfs send", "btrfs receive"]
                    ):
                        # Regular sudo commands may need TTY
                        # Set proper indent
                        cmd_to_use = self._ssh_base_cmd(force_tty=True) + cmd[1:]
                        logger.debug(
                            "Using TTY-enabled command for sudo: %s",
                            " ".join(cmd_to_use),
                        )
                    else:
                        # For non-sudo or btrfs commands, use regular SSH
                        cmd_to_use = cmd

                    # Add -o PasswordAuthentication=yes to ensure password auth is allowed during master setup
                    if "-o" in cmd_to_use and "PasswordAuthentication=no" in " ".join(
                        cmd_to_use
                    ):
                        logger.debug(
                            "Ensuring password authentication is enabled for master setup"
                        )
                        # Find and replace PasswordAuthentication=no with PasswordAuthentication=yes
                        for i, arg in enumerate(cmd_to_use):
                            if arg == "PasswordAuthentication=no":
                                cmd_to_use[i] = "PasswordAuthentication=yes"

                    proc = subprocess.run(
                        cmd_to_use,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                        timeout=timeout,
                        check=False,
                        env=env,
                    )
                    if proc.returncode != 0:
                        logger.error(
                            f"Failed to start SSH master (attempt {attempt}): {proc.stderr.decode().strip()}"
                        )
                        if attempt < retries:
                            time.sleep(1)
                            continue
                        return False
                except Exception as e:
                    logger.error(
                        f"Exception starting SSH master (attempt {attempt}): {e}"
                    )
                    if attempt < retries:
                        time.sleep(1)
                        continue
                    return False

                # Wait for socket to appear and verify it's working properly
                start_time = time.time()
                while time.time() - start_time < timeout:
                    if self.is_master_alive():
                        # Extra verification - run a simple command to ensure connection is truly ready
                        try:
                            test_cmd = self._ssh_base_cmd() + [
                                "echo",
                                "Connection verified",
                            ]
                            test_proc = subprocess.run(
                                test_cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                timeout=5,
                                check=False,
                            )
                            if test_proc.returncode == 0:
                                self._master_started = True
                                if self.debug:
                                    logger.debug(
                                        f"SSH master started and verified at {self.control_path}"
                                    )
                                    # Log the effective SSH configuration
                                    logger.debug(
                                        f"SSH connection established with: user={self.username}, host={self.hostname}"
                                    )
                                    if self.identity_file:
                                        logger.debug(
                                            f"Used identity file: {self.identity_file}"
                                        )
                                return True
                            else:
                                logger.warning(
                                    "SSH master started but command test failed, will retry"
                                )
                        except Exception as e:
                            logger.warning(
                                f"SSH master started but verification failed: {e}"
                            )
                    time.sleep(0.1)

                if self.debug:
                    logger.debug(
                        f"SSH master did not start within {timeout} seconds (attempt {attempt})"
                    )
                    if self.running_as_sudo:
                        logger.debug(
                            "SSH issues might be related to sudo. Check if SSH keys are accessible to root."
                        )
                        logger.debug(f"Consider these troubleshooting steps:")
                        logger.debug(
                            f"1. Ensure {self.ssh_config_dir}/id_rsa exists and has correct permissions"
                        )
                        logger.debug(
                            f"2. Run 'ssh-add' as your regular user before using sudo"
                        )
                        logger.debug(
                            f"3. Consider using ssh-agent forwarding with sudo"
                        )
                        logger.debug(
                            f"4. Make sure sudo on the remote host is configured to allow passwordless sudo for btrfs commands"
                        )
                        logger.debug(
                            f"5. Try manually running: ssh {self.username}@{self.hostname} 'sudo -S btrfs receive /path/to/dest'"
                        )

                if attempt < retries:
                    self.cleanup_socket()
                    time.sleep(1)
                    continue

                logger.error(f"SSH master failed to start after {retries} attempts")
                if self.running_as_sudo:
                    logger.error(
                        f"SSH connection failure may be due to running as sudo. Your SSH keys may not be accessible to root."
                    )
                    logger.error(
                        f"Try running without sudo, or use ssh-agent forwarding with sudo"
                    )
                return False

    def stop_master(self, timeout: float = 5.0) -> bool:
        """Stop the SSH master connection and clean up the socket."""
        with self._lock:
            if not self.is_master_alive():
                self.cleanup_socket()
                self._master_started = False
                return True

            if self.debug:
                logger.debug(f"Stopping SSH master for {self.username}@{self.hostname}")

            cmd = [
                "ssh",
                "-O",
                "exit",
                "-o",
                f"ControlPath={self.control_path}",
                f"{self.username}@{self.hostname}",
            ]
            if self.port:
                cmd += ["-p", str(self.port)]
            try:
                proc = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=timeout,
                    check=False,
                )
                if proc.returncode != 0 and self.debug:
                    logger.debug(
                        f"SSH master exit command returned non-zero: {proc.stderr.decode().strip()}"
                    )
            except Exception as e:
                logger.warning(f"Exception stopping SSH master: {e}")

            # Wait for socket to disappear
            start_time = time.time()
            while time.time() - start_time < timeout:
                if not self.control_path.exists():
                    self._master_started = False
                    if self.debug:
                        logger.debug(
                            f"SSH master stopped for {self.username}@{self.hostname}"
                        )
                    return True
                time.sleep(0.1)

            # If still exists, try to kill any related SSH processes
            try:
                # Try to find and kill related SSH processes
                kill_cmd = ["pkill", "-f", f"ssh.*ControlPath={self.control_path}"]
                subprocess.run(
                    kill_cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            except Exception:
                pass

            # Force cleanup
            self.cleanup_socket()
            self._master_started = False
            success = not self.control_path.exists()
            if self.debug:
                logger.debug(
                    f"SSH master forcibly stopped for {self.username}@{self.hostname}: {success}"
                )
            return success

    def is_master_alive(self) -> bool:
        """Check if the SSH master connection is alive and usable."""
        if not self.control_path.exists():
            logger.debug(f"SSH master control path does not exist: {self.control_path}")
            return False

        # First do a control check
        cmd = [
            "ssh",
            "-O",
            "check",
            "-o",
            f"ControlPath={self.control_path}",
            f"{self.username}@{self.hostname}",
        ]
        if self.port:
            cmd += ["-p", str(self.port)]

        try:
            logger.debug(f"Checking SSH master connection: {' '.join(cmd)}")
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5,  # Give a bit more time for check
                check=False,
            )

            if proc.returncode == 0:
                logger.debug("SSH control socket check passed")

                # Now do a real command test to ensure the connection is truly functional
                test_cmd = self._ssh_base_cmd() + ["true"]
                logger.debug(f"Running secondary SSH check: {' '.join(test_cmd)}")

                test_proc = subprocess.run(
                    test_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=5,
                    check=False,
                )

                if test_proc.returncode == 0:
                    # Both checks passed
                    logger.debug("SSH connection is fully functional")
                    return True
                else:
                    # Control socket exists but can't run commands
                    stderr = test_proc.stderr.decode(errors="replace").strip()
                    logger.warning(
                        f"SSH control socket exists but command test failed: {stderr}"
                    )

                    # The socket might be stale or broken
                    logger.info(
                        "SSH socket appears to be stale or broken, will remove it"
                    )
                    self.cleanup_socket()
                    return False
            else:
                stderr = proc.stderr.decode(errors="replace").strip()
                logger.warning(f"SSH master socket check failed: {stderr}")

                # Socket exists but check failed, try to clean it up
                if "No such file or directory" in stderr:
                    logger.info(
                        "Control socket file not found even though path exists, cleaning up"
                    )
                    self.cleanup_socket()
                return False

        except subprocess.TimeoutExpired:
            logger.warning("SSH master connection check timed out after 5 seconds")
            # This is a sign the socket may be stale
            self.cleanup_socket()
            return False
        except Exception as e:
            logger.warning(f"SSH master check exception: {e}")
            return False

    def cleanup_socket(self):
        """Remove the control socket file if it exists."""
        try:
            if self.control_path.exists():
                # Check if the socket is in use
                try:
                    if hasattr(os, "stat") and hasattr(os, "S_ISSOCK"):
                        stat_info = os.stat(self.control_path)
                        if not os.S_ISSOCK(stat_info.st_mode):
                            if self.debug:
                                logger.debug(
                                    f"Path is not a socket: {self.control_path}"
                                )
                            # Not a socket, just remove it
                            self.control_path.unlink()
                            return
                except Exception:
                    pass  # Proceed with regular removal

                # Try to gracefully close any connections using this socket
                try:
                    test_cmd = [
                        "ssh",
                        "-O",
                        "stop",
                        "-o",
                        f"ControlPath={self.control_path}",
                        "localhost",
                    ]
                    subprocess.run(
                        test_cmd,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        timeout=2,
                        check=False,
                    )
                except Exception:
                    pass  # Ignore errors, just try

                # Now actually remove the socket
                self.control_path.unlink()
                if self.debug:
                    logger.debug(f"Removed SSH control socket: {self.control_path}")
        except Exception as e:
            logger.warning(f"Failed to remove SSH control socket: {e}")

    def __enter__(self):
        # Try up to 3 times to start the master
        for attempt in range(3):
            if self.start_master():
                return self
            logger.warning(
                f"SSH master connection attempt {attempt+1}/3 failed, retrying..."
            )
            time.sleep(1)

        raise RuntimeError(
            f"Failed to start SSH master connection to {self.username}@{self.hostname} after 3 attempts"
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_master()

    def __del__(self):
        # Best effort cleanup
        try:
            if self._master_started:
                if self.debug:
                    logger.debug(
                        f"Cleaning up SSH master in __del__ for {self.username}@{self.hostname}"
                    )
                self.stop_master()
        except Exception as e:
            if self.debug:
                logger.debug(f"Error during SSH master cleanup in __del__: {e}")

    @classmethod
    def cleanup_all_stale_sockets(cls, control_dir: Optional[str] = None):
        """Remove all stale SSH control sockets in the given directory."""
        dir_path = (
            Path(control_dir)
            if control_dir
            else Path.home() / ".ssh" / "controlmasters"
        )
        if not dir_path.exists():
            return
        for sock in dir_path.glob("cm_*.sock"):
            try:
                sock.unlink()
                logger.debug(f"Cleaned up stale SSH control socket: {sock}")
            except Exception as e:
                logger.warning(f"Failed to remove stale SSH control socket {sock}: {e}")
