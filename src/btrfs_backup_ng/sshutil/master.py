# pyright: strict

"""SSHMasterManager: Robust SSH ControlMaster lifecycle and socket management.

Provides:
- Unique socket path generation (per host/user/process/thread)
- Master connection start/stop/status
- Cleanup of stale sockets
- Context manager for safe usage
"""

import os
import subprocess
import threading
import time
import tempfile
import getpass
from pathlib import Path
from typing import Optional

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
    ):
        self.hostname = hostname
        self.username = username or getpass.getuser()
        self.port = port
        self.ssh_opts = ssh_opts or []
        self.persist = persist
        self.debug = debug

        # Use a dedicated directory for control sockets
        self.control_dir = (
            Path(control_dir)
            if control_dir
            else Path.home() / ".ssh" / "controlmasters"
        )
        self.control_dir.mkdir(mode=0o700, exist_ok=True)

        # Unique socket per host/user/process/thread
        self._instance_id = f"{os.getpid()}_{threading.get_ident()}"
        self.control_path = (
            self.control_dir
            / f"cm_{self.username}_{self.hostname.replace(':', '_')}_{self._instance_id}.sock"
        )

        self._lock = threading.Lock()
        self._master_started = False

    def _ssh_base_cmd(self):
        cmd = [
            "ssh",
            "-o", f"ControlPath={self.control_path}",
            "-o", "ControlMaster=auto",
            "-o", f"ControlPersist={self.persist}",
            "-o", "BatchMode=yes",
            "-o", "ServerAliveInterval=5",
            "-o", "ServerAliveCountMax=6",
        ]
        if self.port:
            cmd += ["-p", str(self.port)]
        for opt in self.ssh_opts:
            cmd += ["-o", opt]
        connect_str = f"{self.username}@{self.hostname}"
        cmd.append(connect_str)
        return cmd

    def start_master(self, timeout: float = 10.0) -> bool:
        """Start the SSH master connection if not already running."""
        with self._lock:
            if self.is_master_alive():
                if self.debug:
                    logger.debug(f"SSH master already running at {self.control_path}")
                self._master_started = True
                return True

            # Clean up any stale socket first
            self.cleanup_socket()

            cmd = self._ssh_base_cmd()
            cmd.insert(1, "-MNf")  # -M: master, -N: no command, -f: background

            if self.debug:
                logger.debug(f"Starting SSH master as {self.username}@{self.hostname}: {' '.join(map(str, cmd))}")

            try:
                proc = subprocess.run(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    timeout=timeout,
                    check=False,
                )
                if proc.returncode != 0:
                    logger.error(f"Failed to start SSH master: {proc.stderr.decode().strip()}")
                    return False
            except Exception as e:
                logger.error(f"Exception starting SSH master: {e}")
                return False

            # Wait for socket to appear
            start_time = time.time()
            while time.time() - start_time < timeout:
                if self.is_master_alive():
                    self._master_started = True
                    if self.debug:
                        logger.debug(f"SSH master started at {self.control_path}")
                    return True
                time.sleep(0.1)
            logger.error(f"SSH master did not start within {timeout} seconds")
            return False

    def stop_master(self, timeout: float = 5.0) -> bool:
        """Stop the SSH master connection and clean up the socket."""
        with self._lock:
            if not self.is_master_alive():
                self.cleanup_socket()
                self._master_started = False
                return True
            cmd = [
                "ssh",
                "-O", "exit",
                "-o", f"ControlPath={self.control_path}",
                f"{self.username}@{self.hostname}",
            ]
            if self.port:
                cmd += ["-p", str(self.port)]
            try:
                subprocess.run(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=timeout,
                    check=False,
                )
            except Exception as e:
                logger.warning(f"Exception stopping SSH master: {e}")
            # Wait for socket to disappear
            start_time = time.time()
            while time.time() - start_time < timeout:
                if not self.control_path.exists():
                    self._master_started = False
                    return True
                time.sleep(0.1)
            # If still exists, force cleanup
            self.cleanup_socket()
            self._master_started = False
            return not self.control_path.exists()

    def is_master_alive(self) -> bool:
        """Check if the SSH master connection is alive."""
        if not self.control_path.exists():
            return False
        cmd = [
            "ssh",
            "-O", "check",
            "-o", f"ControlPath={self.control_path}",
            f"{self.username}@{self.hostname}",
        ]
        if self.port:
            cmd += ["-p", str(self.port)]
        try:
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=3,
                check=False,
            )
            if proc.returncode == 0:
                return True
            if self.debug:
                logger.debug(f"SSH master check failed: {proc.stderr.decode().strip()}")
            return False
        except Exception as e:
            if self.debug:
                logger.debug(f"SSH master check exception: {e}")
            return False

    def cleanup_socket(self):
        """Remove the control socket file if it exists."""
        try:
            if self.control_path.exists():
                self.control_path.unlink()
                if self.debug:
                    logger.debug(f"Removed SSH control socket: {self.control_path}")
        except Exception as e:
            logger.warning(f"Failed to remove SSH control socket: {e}")

    def __enter__(self):
        if not self.start_master():
            raise RuntimeError("Failed to start SSH master connection")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_master()

    def __del__(self):
        # Best effort cleanup
        try:
            self.stop_master()
        except Exception:
            pass

    @classmethod
    def cleanup_all_stale_sockets(cls, control_dir: Optional[str] = None):
        """Remove all stale SSH control sockets in the given directory."""
        dir_path = Path(control_dir) if control_dir else Path.home() / ".ssh" / "controlmasters"
        if not dir_path.exists():
            return
        for sock in dir_path.glob("cm_*.sock"):
            try:
                sock.unlink()
                logger.debug(f"Cleaned up stale SSH control socket: {sock}")
            except Exception as e:
                logger.warning(f"Failed to remove stale SSH control socket {sock}: {e}")
