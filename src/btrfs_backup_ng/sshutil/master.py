import os
import pwd
import subprocess
import threading
import time
import tempfile
import getpass
from pathlib import Path
from typing import Optional, List, Type, Any, TypeVar, Union
from types import TracebackType
import stat

from btrfs_backup_ng.__logger__ import logger

class SSHMasterManager:
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
        
        self.running_as_sudo = os.environ.get("SUDO_USER") is not None and os.geteuid() == 0
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
        self.control_path = self.control_dir / f"cm_{self.username}_{self.hostname}_{self._instance_id}.sock"
        self._lock = threading.Lock()
        self._master_started = False
        
    def _ssh_base_cmd(self, force_tty=False):
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
            "ConnectTimeout=30",  # Increased from 15 to allow password entry
            "ConnectionAttempts=3",
            "StrictHostKeyChecking=accept-new",
            "PasswordAuthentication=yes"
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
        
    def start_master(self):
        with self._lock:
            if self.is_master_alive():
                return True
                
            cmd = self._ssh_base_cmd()
            cmd.insert(1, "-MNf")
            
            env = os.environ.copy()
            if self.running_as_sudo:
                env["HOME"] = pwd.getpwnam(self.sudo_user).pw_dir
                env["USER"] = self.sudo_user
            
            try:
                proc = subprocess.run(cmd, env=env, check=True)
                self._master_started = True
                return True
            except Exception as e:
                logger.error(f"Failed to start SSH master: {e}")
                return False
                
    def stop_master(self):
        if not self._master_started:
            return True
            
        with self._lock:
            cmd = ["ssh", "-O", "exit", "-o", f"ControlPath={self.control_path}", 
                  f"{self.username}@{self.hostname}"]
            try:
                subprocess.run(cmd, check=True)
                self._master_started = False
                return True
            except Exception as e:
                logger.error(f"Failed to stop SSH master: {e}")
                return False
                
    def is_master_alive(self):
        if not self.control_path.exists():
            return False
            
        cmd = ["ssh", "-O", "check", "-o", f"ControlPath={self.control_path}",
               f"{self.username}@{self.hostname}"]
               
        try:
            subprocess.run(cmd, check=True, capture_output=True)
            return True
        except Exception:
            return False
            
    def cleanup_socket(self):
        try:
            if self.control_path.exists():
                self.control_path.unlink()
        except Exception as e:
            logger.error(f"Failed to cleanup socket: {e}")
            
    def get_ssh_base_cmd(self, force_tty=False):
        """Get the base SSH command with all necessary options.
        
        Args:
            force_tty (bool): Whether to force TTY allocation with -tt flag
            
        Returns:
            List[str]: The base SSH command as a list of strings
        """
        return self._ssh_base_cmd(force_tty=force_tty)