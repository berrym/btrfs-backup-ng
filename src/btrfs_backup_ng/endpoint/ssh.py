# pyright: strict

"""btrfs-backup-ng: SSH Endpoint for managing remote operations.

This module provides the SSHEndpoint class, which integrates with SSHMasterManager
to handle SSH-based operations robustly, including btrfs send/receive commands.
"""

import copy
import os
import pwd
import subprocess
import uuid
import logging
from pathlib import Path
from threading import Lock
from typing import Optional, List, Union

from btrfs_backup_ng.__logger__ import logger
from btrfs_backup_ng.sshutil.master import SSHMasterManager
from .common import Endpoint


class SSHEndpoint(Endpoint):
    """SSH-based endpoint for remote operations.
    
    This endpoint type handles connections to remote hosts via SSH.
    SSH username can be specified in three ways, in order of precedence:
    1. Via --ssh-username command line argument (highest priority)
    2. In the URI (e.g., ssh://user@host:/path)
    3. Current local user (fallback)
    
    When running as root with sudo, SSH identity files and usernames need special handling.
    """

    _is_remote = True
    _supports_multiprocessing = True

    def __init__(self, hostname: str, config: Optional[dict] = None, **kwargs) -> None:
        # Deep copy config to avoid shared references in multiprocessing
        if config is not None:
            config = copy.deepcopy(config)
        super().__init__(config=config, **kwargs)

        self.hostname = hostname
        logger.debug("SSHEndpoint initialized with hostname: %s", self.hostname)
        
        # Set default values for config parameters
        self.config["port"] = self.config.get("port")
        self.config["ssh_opts"] = self.config.get("ssh_opts", [])
        self.config["path"] = self.config.get("path", "/")
        self.config["ssh_sudo"] = self.config.get("ssh_sudo", False)
        self.config["passwordless"] = self.config.get("passwordless", False)
        
        # Username handling with clear precedence:
        # 1. Explicitly provided username (from command line via --ssh-username)
        # 2. Username from the URL (ssh://user@host/path)
        # 3. SUDO_USER environment variable if running as root with sudo
        # 4. Current user as fallback
        if not self.config.get("username"):
            # No username provided in config, check sudo environment
            if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
                self.config["username"] = os.environ.get("SUDO_USER")
                logger.debug("Using sudo original user as username: %s", self.config["username"])
            else:
                # Default to current user if nothing else specified
                import getpass
                self.config["username"] = getpass.getuser()
                logger.debug("Using current user as username: %s", self.config["username"])
        else:
            # Username explicitly provided in config
            logger.debug("Using explicitly configured username: %s", self.config["username"])
        
        # Handle SSH identity file path, especially when running under sudo
        identity_file = self.config.get("ssh_identity_file")
        if identity_file:
            # Check if running as root via sudo
            running_as_sudo = os.geteuid() == 0 and os.environ.get("SUDO_USER")
            if running_as_sudo:
                sudo_user = os.environ.get("SUDO_USER")
                try:
                    # Get the sudo user's home directory
                    sudo_user_home = pwd.getpwnam(sudo_user).pw_dir
                    
                    # Expand ~ to sudo user's home directory
                    if identity_file.startswith("~"):
                        identity_file = identity_file.replace("~", sudo_user_home, 1)
                        logger.debug("Expanded ~ in identity file path: %s", identity_file)
                    
                    # If path is relative, make it absolute using sudo user's home
                    if not os.path.isabs(identity_file):
                        identity_file = os.path.join(sudo_user_home, identity_file)
                        logger.debug("Converted relative path to absolute: %s", identity_file)
                    
                    # Store the expanded path
                    self.config["ssh_identity_file"] = identity_file
                    logger.debug("Final identity file path: %s", identity_file)
                    
                    # Verify the file exists and is readable
                    id_file = Path(identity_file).absolute()
                    if not id_file.exists():
                        logger.warning("SSH identity file does not exist: %s", id_file)
                        logger.warning("When running with sudo, ensure the identity file path is absolute and accessible")
                    elif not os.access(str(id_file), os.R_OK):
                        logger.warning("SSH identity file is not readable: %s", id_file)
                        logger.warning("Check file permissions: chmod 600 %s", id_file)
                    else:
                        logger.info("Using SSH identity file: %s (verified)", id_file)
                except Exception as e:
                    logger.warning("Error processing identity file path: %s", e)
                    # Keep the original path as a fallback
                    self.config["ssh_identity_file"] = identity_file
            else:
                # Not running as sudo, just log the path
                logger.debug("Using SSH identity file: %s", identity_file)
        
        # Log the final configuration
        logger.debug("SSH path: %s", self.config["path"])
        logger.debug("SSH username: %s", self.config["username"])
        logger.debug("SSH hostname: %s", self.hostname)
        logger.debug("SSH port: %s", self.config["port"])
        logger.debug("SSH sudo: %s", self.config["ssh_sudo"])
        
        # Add SSH agent forwarding if available when running as sudo
        if os.geteuid() == 0 and os.environ.get("SUDO_USER") and "SSH_AUTH_SOCK" in os.environ:
            logger.debug("Adding SSH agent forwarding for sudo user")
            ssh_opts = self.config.get("ssh_opts", []).copy()
            ssh_opts.append(f"IdentityAgent={os.environ['SSH_AUTH_SOCK']}")
            self.config["ssh_opts"] = ssh_opts

        self.ssh_manager = SSHMasterManager(
            hostname=self.hostname,
            username=self.config["username"],
            port=self.config["port"],
            ssh_opts=self.config["ssh_opts"],
            persist="60",
            debug=True,
            identity_file=self.config.get("ssh_identity_file"),
        )

        self._lock = Lock()
        self._instance_id = f"{os.getpid()}_{uuid.uuid4().hex[:8]}"

    def __repr__(self) -> str:
        username = self.config.get('username', '')
        return f"(SSH) {username}@{self.hostname}:{self.config['path']}"

    def get_id(self) -> str:
        """Return a unique identifier for this SSH endpoint."""
        username = self.config.get('username', '')
        username_part = f"{username}@" if username else ""
        return f"ssh://{username_part}{self.hostname}:{self.config['path']}"

    def _build_remote_command(self, command: List[str]) -> List[str]:
        """Prepare a remote command with optional sudo."""
        if not command:
            return command
            
        # Ensure all elements are strings
        command = [str(c) for c in command]
        
        if self.config.get("ssh_sudo", False):
            cmd_str = " ".join(command)
            logger.debug("Using sudo for remote command: %s", cmd_str)
            
            # Special handling for btrfs receive which needs to be root
            if command[0] == "btrfs":
                # Use -n to avoid password prompt, but allow passing through TTY if available
                # Add -E to preserve environment variables if needed
                logger.debug("Using sudo for btrfs command")
                return ["sudo", "-n", "-E"] + command
            else:
                # Use -n to avoid password prompt for other commands
                return ["sudo", "-n"] + command
        return command

    def _exec_remote_command(self, command: List[str], **kwargs) -> subprocess.CompletedProcess:
        """Execute a command on the remote host."""
        # Convert any Path objects in the command to strings
        string_command = [
            self._normalize_path(arg) if isinstance(arg, (str, Path)) else arg
            for arg in command
        ]
        remote_cmd = self._build_remote_command(string_command)
        ssh_cmd = self.ssh_manager._ssh_base_cmd() + ["--"] + remote_cmd

        # Always capture stderr if not explicitly provided
        if "stderr" not in kwargs:
            kwargs["stderr"] = subprocess.PIPE

        # Default timeout if not specified
        if "timeout" not in kwargs:
            kwargs["timeout"] = 30

        cmd_str = " ".join(map(str, ssh_cmd))
        logger.debug("Executing remote command: %s", cmd_str)

        try:
            result = subprocess.run(ssh_cmd, **kwargs)
            exit_code = result.returncode

            if exit_code != 0 and kwargs.get("check", False) is False:
                stderr = (
                    result.stderr.decode("utf-8", errors="replace")
                    if hasattr(result, "stderr") and result.stderr
                    else ""
                )
                logger.debug(
                    "Command exited with non-zero code %d: %s\nError: %s",
                    exit_code,
                    cmd_str,
                    stderr,
                )
            elif exit_code == 0:
                logger.debug("Command executed successfully: %s", cmd_str)

            return result

        except subprocess.TimeoutExpired as e:
            logger.error("Command timed out after %s seconds: %s", e.timeout, cmd_str)
            raise
        except Exception as e:
            logger.error(
                "Failed to execute remote command: %s\nError: %s", cmd_str, str(e)
            )
            raise

    def _btrfs_send(self, source: str, stdout_pipe) -> subprocess.Popen:
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

    def _normalize_path(self, path):
        """Override to handle remote paths properly."""
        if path is None:
            return None
        # For SSH paths, we just want to ensure they're strings,
        # not convert them to Path objects or resolve them locally
        if isinstance(path, Path):
            return str(path)
            
        # If it's a string with a tilde, we need special handling
        if isinstance(path, str) and "~" in path:
            # Check if running as root via sudo
            if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
                sudo_user = os.environ.get("SUDO_USER")
                try:
                    # Get the sudo user's home directory
                    sudo_user_home = pwd.getpwnam(sudo_user).pw_dir
                    # Replace ~ with sudo user's home
                    if path.startswith("~"):
                        path = path.replace("~", sudo_user_home, 1)
                        logger.debug("Expanded ~ in path to sudo user's home: %s", path)
                except Exception as e:
                    logger.debug("Error expanding ~ in path: %s", e)
            else:
                # Normal user, let Path handle it
                path = os.path.expanduser(path)
                
        return str(path) if path is not None else None
        
    def _verify_btrfs_availability(self, use_sudo=False):
        """Verify that btrfs command is available on the remote host."""
        # Check if btrfs is available
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
        
    def _btrfs_receive(self, destination, stdin_pipe):
        """Run btrfs receive on the remote host."""
        receive_cmd = ["btrfs", "receive", destination]
        logger.debug("Preparing btrfs receive command: %s", receive_cmd)
        
        # Build the remote command with optional sudo
        receive_cmd = self._build_remote_command(receive_cmd)
        
        # Build the SSH command
        ssh_cmd = self.ssh_manager._ssh_base_cmd() + ["--"] + receive_cmd
        logger.debug("Full SSH btrfs receive command: %s", " ".join(ssh_cmd))
        
        try:
            # Start the btrfs receive process
            logger.debug("Starting btrfs receive process")
            receive_process = subprocess.Popen(
                ssh_cmd,
                stdin=stdin_pipe,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            
            logger.debug("btrfs receive process started with PID: %d", receive_process.pid)
            return receive_process
        except Exception as e:
            logger.error("Failed to start btrfs receive process: %s", e)
            raise

    def _listdir(self, location):
        """List directory contents on remote host."""
        location = self._normalize_path(location)
        logger.debug(
            "SSH _listdir: Listing directory contents of %s on %s",
            location,
            self.hostname,
        )

        cmd = ["ls", "-1a", location]
        try:
            with self.ssh_manager:
                result = self._exec_remote_command(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
                )

                if result.returncode != 0:
                    stderr = (
                        result.stderr.decode("utf-8", errors="replace")
                        if hasattr(result, "stderr")
                        else ""
                    )
                    logger.error(
                        "Failed to list directory %s on %s: %s",
                        location,
                        self.hostname,
                        stderr,
                    )
                    return []

                if hasattr(result, "stdout") and result.stdout:
                    output = result.stdout.decode(
                        "utf-8", errors="replace"
                    ).splitlines()
                    # Filter out '.' and '..' entries
                    filtered = [
                        os.path.join(location, item)
                        for item in output
                        if item not in (".", "..")
                    ]
                    logger.debug("Found %d items in %s", len(filtered), location)
                    return filtered
                return []
        except Exception as e:
            logger.error(
                "Error listing directory %s on %s: %s", location, self.hostname, e
            )
            return []

    def send_receive(self, source: str, destination: str) -> None:
        """Perform btrfs send/receive operation."""
        logger.info(
            "Starting btrfs send/receive operation from %s to %s", source, destination
        )
        logger.debug("Source path: %s", source)
        logger.debug("Destination path: %s", destination)
        
        # Test if destination already exists
        try:
            test_cmd = ["test", "-e", destination]
            result = self._exec_remote_command(test_cmd, check=False)
            if result.returncode == 0:
                logger.warning(
                    "Destination path already exists: %s - this might cause btrfs receive to fail", 
                    destination
                )
        except Exception as e:
            logger.warning("Could not check if destination exists: %s", e)
        
        # Establish SSH connection with context manager
        with self.ssh_manager:
            try:
                # Start the send process
                send_proc = subprocess.Popen(
                    ["btrfs", "send", source],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                
                # Start the receive process
                recv_proc = self._btrfs_receive(destination, send_proc.stdout)

                # Close stdout after passing to receive process to allow proper SIGPIPE
                logger.debug(
                    "Closing send_proc stdout to allow SIGPIPE if recv_proc exits"
                )
                send_proc.stdout.close()

                # Wait for processes to complete
                logger.debug("Waiting for btrfs send process to complete...")
                send_returncode = send_proc.wait()
                logger.debug("btrfs send process completed with return code: %d", send_returncode)

                logger.debug("Waiting for btrfs receive process to complete...")
                recv_returncode = recv_proc.wait()
                logger.debug("btrfs receive process completed with return code: %d", recv_returncode)

                # Check for errors and log output
                if send_returncode != 0:
                    send_stderr = send_proc.stderr.read().decode(errors="replace")
                    logger.error("btrfs send failed with return code %d", send_returncode)
                    logger.error("btrfs send stderr: %s", send_stderr)
                    raise RuntimeError(f"btrfs send failed: {send_stderr}")

                if recv_returncode != 0:
                    recv_stderr = recv_proc.stderr.read().decode(errors="replace")
                    logger.error("btrfs receive failed with return code %d", recv_returncode)
                    logger.error("btrfs receive stderr: %s", recv_stderr)
                    raise RuntimeError(f"btrfs receive failed: {recv_stderr}")
                
                # Verify transfer completion
                logger.debug("Verifying transfer completion on remote host...")
                verify_cmd = self._build_remote_command(["ls", "-l", destination])
                verify_proc = self._exec_remote_command(verify_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if verify_proc.returncode != 0:
                    logger.error("Verification failed: %s", verify_proc.stderr.decode(errors="replace"))
                    raise RuntimeError("Failed to verify transfer on remote host")
                logger.debug("Transfer verified successfully on remote host.")
                
            finally:
                # Ensure proper cleanup of processes
                if 'send_proc' in locals():
                    if hasattr(send_proc, "stderr") and send_proc.stderr:
                        send_proc.stderr.close()
                
                if 'recv_proc' in locals():
                    if hasattr(recv_proc, "stdout") and recv_proc.stdout:
                        recv_proc.stdout.close()
                    if hasattr(recv_proc, "stderr") and recv_proc.stderr:
                        recv_proc.stderr.close()

        logger.info("btrfs send/receive completed successfully")

    def _prepare(self) -> None:
        """Prepare the SSH endpoint by ensuring SSH connectivity."""
        logger.debug("Preparing SSH endpoint for hostname: %s", self.hostname)
        # Ensure path is a string
        path = self._normalize_path(self.config["path"])
        logger.debug("Preparing remote directory: %s", path)
        
        # Log detailed SSH connection settings for debugging
        logger.debug("SSH connection settings:")
        logger.debug("  Hostname: %s", self.hostname)
        logger.debug("  Username: %s", self.config.get("username"))
        logger.debug("  Port: %s", self.config.get("port"))
        logger.debug("  SSH options: %s", self.config.get("ssh_opts", []))
        logger.debug("  Identity file: %s", self.config.get("ssh_identity_file"))
        
        # Check if running as root via sudo
        if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
            sudo_user = os.environ.get("SUDO_USER")
            logger.debug("Running as root via sudo from user: %s", sudo_user)
            logger.debug("Effective UID: %s", os.geteuid())
            logger.debug("Real UID: %s", os.getuid())
            
            # Check SSH agent forwarding
            if "SSH_AUTH_SOCK" in os.environ:
                logger.debug("SSH_AUTH_SOCK is set: %s", os.environ.get("SSH_AUTH_SOCK"))
            else:
                logger.warning("SSH_AUTH_SOCK is not set, agent forwarding won't work")
                logger.warning("Consider: sudo SSH_AUTH_SOCK=$SSH_AUTH_SOCK btrfs-backup-ng ...")
        
        if not self.ssh_manager.start_master(timeout=30.0, retries=3):
            logger.error("Failed to establish SSH connection to %s", self.hostname)
            
            # Provide detailed error information
            error_msg = f"Cannot establish SSH connection to {self.hostname}"
            
            # Check for common issues
            identity_file = self.config.get("ssh_identity_file")
            if identity_file:
                id_path = Path(identity_file)
                if not id_path.exists():
                    error_msg += f"\n- Identity file does not exist: {id_path}"
                elif not os.access(str(id_path), os.R_OK):
                    error_msg += f"\n- Identity file is not readable: {id_path}"
                else:
                    error_msg += f"\n- Using identity file: {id_path}"
            else:
                error_msg += "\n- No identity file specified"
            
            if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
                sudo_user = os.environ.get("SUDO_USER")
                error_msg += f"\n- Running as root via sudo from user {sudo_user}"
                if not "SSH_AUTH_SOCK" in os.environ:
                    error_msg += "\n- SSH_AUTH_SOCK is not set, agent forwarding won't work"
                    error_msg += f"\n  Try: sudo SSH_AUTH_SOCK=$SSH_AUTH_SOCK btrfs-backup-ng ..."
            
            raise RuntimeError(error_msg)
        
        # If we're using sudo, verify sudo works
        if self.config.get("ssh_sudo", False):
            logger.debug("Testing sudo access on remote host")
            try:
                test_cmd = ["sudo", "-n", "echo", "sudo test successful"]
                logger.debug("Testing sudo access with command: %s", test_cmd)
                result = self._exec_remote_command(test_cmd, check=False)
                if result.returncode != 0:
                    stderr = result.stderr.decode("utf-8", errors="replace") if hasattr(result, "stderr") and result.stderr else ""
                    logger.warning("Remote sudo test failed. Error: %s", stderr)
                    logger.warning("The remote user may not have passwordless sudo permissions.")
                    logger.warning("You may need to configure /etc/sudoers on the remote system to allow passwordless sudo for btrfs commands.")
                    logger.warning("Add this line to /etc/sudoers: %s ALL=(ALL) NOPASSWD: /usr/bin/btrfs", 
                                  self.config.get("username", "username"))
                    
                    # Check for common sudo issues
                    if "tty" in stderr.lower() or "terminal" in stderr.lower():
                        logger.warning("Sudo is requiring a terminal. You may need to add 'Defaults:%s !requiretty' to sudoers", 
                                      self.config.get("username", "username"))
                    
                    # Don't fail here as sudo might still work interactively or for specific commands
                else:
                    logger.debug("Remote sudo test successful")
                    
                # Also test btrfs with sudo specifically
                btrfs_test_cmd = ["sudo", "-n", "btrfs", "--version"]
                logger.debug("Testing btrfs with sudo: %s", btrfs_test_cmd)
                btrfs_result = self._exec_remote_command(btrfs_test_cmd, check=False)
                if btrfs_result.returncode == 0:
                    btrfs_version = btrfs_result.stdout.decode("utf-8", errors="replace") if hasattr(btrfs_result, "stdout") and btrfs_result.stdout else ""
                    logger.debug("Sudo access for btrfs commands verified: %s", btrfs_version.strip())
                else:
                    btrfs_stderr = btrfs_result.stderr.decode("utf-8", errors="replace") if hasattr(btrfs_result, "stderr") and btrfs_result.stderr else ""
                    logger.warning("Sudo access for btrfs failed: %s", btrfs_stderr)
            except Exception as e:
                logger.warning("Error testing sudo: %s", e)
            
        # Check if the directory exists and create if needed
        try:
            # First check if the path exists
            logger.debug("Checking if remote path exists: %s", path)
            check_cmd = ["test", "-d", path]
            logger.debug("Executing remote command: %s", check_cmd)
            result = self._exec_remote_command(check_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
            logger.debug("Remote path check returned: %d", result.returncode)
            
            if result.returncode != 0:
                logger.debug("Remote directory %s does not exist, creating it", path)
                mkdir_cmd = ["mkdir", "-p", path]
                create_result = self._exec_remote_command(mkdir_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
                
                if create_result.returncode != 0:
                    stderr = create_result.stderr.decode('utf-8', errors='replace') if hasattr(create_result, 'stderr') else "Unknown error"
                    logger.error("Failed to create remote directory: %s, Error: %s", path, stderr)
                    
                    # If regular mkdir failed and sudo is enabled, try with sudo
                    if self.config.get("ssh_sudo", False):
                        logger.debug("Trying to create directory with sudo")
                        sudo_mkdir_cmd = ["sudo", "mkdir", "-p", path]
                        sudo_result = self._exec_remote_command(sudo_mkdir_cmd, check=False)
                        
                        if sudo_result.returncode != 0:
                            sudo_stderr = sudo_result.stderr.decode('utf-8', errors='replace') if hasattr(sudo_result, 'stderr') else ""
                            logger.error("Failed to create directory with sudo: %s", sudo_stderr)
                            raise RuntimeError(f"Cannot create destination directory {path} even with sudo: {sudo_stderr}")
                        
                        # Also set permissions
                        chmod_cmd = ["sudo", "chmod", "755", path]
                        self._exec_remote_command(chmod_cmd, check=False)
                        logger.debug("Created directory with sudo: %s", path)
                    else:
                        raise RuntimeError(f"Failed to create remote directory {path}: {stderr}")
                    
                # Verify the directory was created
                verify_cmd = ["test", "-d", path]
                verify_result = self._exec_remote_command(verify_cmd, check=False)
                if verify_result.returncode != 0:
                    logger.error("Directory creation verification failed for %s", path)
                    raise RuntimeError(f"Directory creation verification failed for {path}")
                    
                logger.debug("Remote directory created and verified: %s", path)
            else:
                logger.debug("Remote directory already exists: %s", path)
                
            # Verify we can write to the directory
            logger.debug("Verifying write permissions for remote directory: %s", path)
            test_write_cmd = ["touch", f"{path}/.btrfs-backup-ng-write-test"]
            write_result = self._exec_remote_command(test_write_cmd, check=False)
            
            if write_result.returncode != 0:
                stderr = write_result.stderr.decode('utf-8', errors='replace') if hasattr(write_result, 'stderr') else "Unknown error"
                logger.warning("Write permission test failed for %s: %s", path, stderr)
                
                # If using sudo, try again with sudo
                if self.config.get("ssh_sudo", False):
                    logger.debug("Retrying write test with explicit sudo")
                    sudo_test_cmd = ["sudo", "touch", f"{path}/.btrfs-backup-ng-write-test"]
                    sudo_result = self._exec_remote_command(sudo_test_cmd, check=False)
                    if sudo_result.returncode == 0:
                        logger.debug("Write permission test passed with explicit sudo")
                        # Clean up test file
                        self._exec_remote_command(["sudo", "rm", f"{path}/.btrfs-backup-ng-write-test"], check=False)
                    else:
                        sudo_stderr = sudo_result.stderr.decode('utf-8', errors='replace') if hasattr(sudo_result, 'stderr') else ""
                        logger.warning("Write permission test with sudo also failed: %s", sudo_stderr)
            else:
                # Clean up test file
                self._exec_remote_command(
                    ["rm", f"{path}/.btrfs-backup-ng-write-test"], check=False
                )
                logger.debug("Write permission test passed for: %s", path)
                
            logger.info("Remote directory prepared successfully: %s", path)
        except Exception as e:
            logger.error("Error preparing remote directory: %s", e)
            raise RuntimeError(f"Failed to prepare SSH endpoint: {e}") from e