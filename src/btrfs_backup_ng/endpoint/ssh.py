# pyright: strict

"""btrfs-backup-ng: SSH Endpoint for managing remote operations.

This module provides the SSHEndpoint class, which integrates with SSHMasterManager
to handle SSH-based operations robustly, including btrfs send/receive commands.

Environment variables that affect behavior:
- BTRFS_BACKUP_PASSWORDLESS_ONLY: If set to 1/true/yes, disables the use of sudo
  -S flag and will only attempt passwordless sudo (-n flag), failing if a password
  would be required.
"""

import copy
import os
import pwd
import select
import subprocess
import time
import uuid
import logging
from pathlib import Path
from threading import Lock
from typing import Optional, List, Union, Dict, Any, Tuple

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
        
        # Log important settings for troubleshooting
        logger.info("SSH endpoint configuration: hostname=%s, sudo=%s, passwordless=%s", 
                   self.hostname, 
                   self.config.get("ssh_sudo", False),
                   self.config.get("passwordless", False))
        
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
            if command[0] == "btrfs" and command[1] == "receive":
                # For btrfs receive, we want to ensure sudo works even if it requires a password
                # Add -E to preserve environment variables
                # Add -P to preserve PATH which is important for finding the btrfs binary
                
                # Check if we should force passwordless mode
                passwordless_only = os.environ.get("BTRFS_BACKUP_PASSWORDLESS_ONLY", "0").lower() in ("1", "true", "yes")
                
                if passwordless_only:
                    # Use -n flag to fail rather than prompt for password
                    logger.debug("Using sudo with -n flag (passwordless only mode)")
                    return ["sudo", "-n", "-E", "-P"] + command
                else:
                    # Use -S to read password from stdin if needed (stdin will be connected to our pipe)
                    logger.debug("Using sudo for btrfs receive command with password support")
                    logger.warning("Note: If the remote host requires a sudo password, transfer may fail")
                    logger.warning("Consider setting up passwordless sudo for btrfs commands on remote host")
                    return ["sudo", "-S", "-E", "-P"] + command
            elif command[0] == "btrfs":
                # For other btrfs commands, use -n which will fail rather than prompt for password
                logger.debug("Using sudo for regular btrfs command")
                return ["sudo", "-n", "-E"] + command
            else:
                # Use -n to avoid password prompt for other commands
                return ["sudo", "-n"] + command
        return command

    def _exec_remote_command(self, command, **kwargs) -> subprocess.CompletedProcess:
        """Execute a command on the remote host."""
        # Process command arguments based on whether they're marked as paths
        string_command = []
        
        logger.debug("Executing remote command, original format: %s", command)
        logger.debug("Command type: %s, first element type: %s", 
                    type(command).__name__, 
                    type(command[0]).__name__ if command else "None")
        
        # Check if command is using the tuple format (arg, is_path)
        if command and isinstance(command[0], tuple) and len(command[0]) == 2:
            # New format with (arg, is_path) tuples
            logger.debug("Detected tuple format command (arg, is_path)")
            for i, (arg, is_path) in enumerate(command):
                logger.debug("Processing arg %d: '%s' (is_path=%s, type=%s)", 
                            i, arg, is_path, type(arg).__name__)
                if is_path and isinstance(arg, (str, Path)):
                    normalized = self._normalize_path(arg)
                    logger.debug("Normalized path arg %d: %s -> %s", i, arg, normalized)
                    string_command.append(normalized)
                else:
                    # Not a path, just append as-is
                    logger.debug("Using non-path arg %d as-is: %s", i, arg)
                    string_command.append(arg)
            logger.debug("Processed marked command arguments for remote execution: %s", string_command)
        else:
            # Legacy format - convert any Path objects in the command to strings
            logger.debug("Using legacy command format")
            for i, arg in enumerate(command):
                if isinstance(arg, (str, Path)):
                    normalized = self._normalize_path(arg)
                    logger.debug("Normalized arg %d: %s -> %s", i, arg, normalized)
                    string_command.append(normalized)
                else:
                    logger.debug("Using non-string arg %d as-is: %s", i, arg)
                    string_command.append(arg)
            logger.debug("Processed legacy command format for remote execution: %s", string_command)
        
        remote_cmd = self._build_remote_command(string_command)
        logger.debug("Final remote command after build: %s", remote_cmd)
        # Build the SSH command - if using sudo for this command, consider forcing TTY allocation
        needs_tty = False
        if self.config.get("ssh_sudo", False) and not self.config.get("passwordless", False):
            # Check if this is a command that might need TTY for sudo password
            cmd_str = " ".join(remote_cmd)
            if "sudo" in cmd_str and "-n" not in cmd_str:
                needs_tty = True
            
        ssh_base_cmd = self.ssh_manager._ssh_base_cmd(force_tty=needs_tty)
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
            logger.debug("Set default timeout to 30 seconds")

        cmd_str = " ".join(map(str, ssh_cmd))
        logger.debug("Executing remote command: %s", cmd_str)
        logger.debug("Working directory: %s", os.getcwd())

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

            logger.debug("Command execution result: exit_code=%d", result.returncode)
            return result

        except subprocess.TimeoutExpired as e:
            logger.error("Command timed out after %s seconds: %s", e.timeout, cmd_str)
            logger.error("Timeout occurred in SSH command execution, command was: %s", ssh_cmd)
            raise
        except Exception as e:
            logger.error(
                "Failed to execute remote command: %s\nError: %s", cmd_str, str(e)
            )
            logger.error("Exception type: %s", type(e).__name__)
            logger.error("Command that failed: %s", ssh_cmd)
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
            
        # If the path is actually a tuple from our new command format, extract just the path part
        if isinstance(path, tuple) and len(path) == 2:
            path, is_path = path
            logger.debug("Tuple format detected in _normalize_path: %s (is_path=%s)", path, is_path)
            if not is_path:
                # If it's not a path, just return as-is
                logger.debug("Not a path, returning as-is: %s", path)
                return path
        
        # For SSH paths, we just want to ensure they're strings,
        # not convert them to Path objects or resolve them locally
        if isinstance(path, Path):
            logger.debug("Converting Path object to string: %s", path)
            return str(path)
            
        # If it's a string with a tilde, we need special handling
        if isinstance(path, str) and "~" in path:
            logger.debug("Path contains tilde, handling expansion: %s", path)
            # Check if running as root via sudo
            if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
                sudo_user = os.environ.get("SUDO_USER")
                logger.debug("Running as root via sudo user: %s", sudo_user)
                try:
                    # Get the sudo user's home directory
                    sudo_user_home = pwd.getpwnam(sudo_user).pw_dir
                    logger.debug("Found sudo user home: %s", sudo_user_home)
                    # Replace ~ with sudo user's home
                    if path.startswith("~"):
                        original_path = path
                        path = path.replace("~", sudo_user_home, 1)
                        logger.debug("Expanded ~ in path: %s -> %s", original_path, path)
                except Exception as e:
                    logger.error("Error expanding ~ in path: %s", e)
            else:
                # Normal user, let Path handle it
                original_path = path
                path = os.path.expanduser(path)
                logger.debug("Expanded user path: %s -> %s", original_path, path)
                
        result = str(path) if path is not None else None
        logger.debug("Final normalized path result: %s", result)
        return result
        
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
        
        # If we're testing in this run, we'll disable sudo password prompt
        passwordless_only = os.environ.get("BTRFS_BACKUP_PASSWORDLESS_ONLY", "0").lower() in ("1", "true", "yes")
        if passwordless_only:
            logger.warning("Running in passwordless-only mode - will not prompt for sudo password")
        
        # First ensure the destination directory exists and is writable
        destination_dir = os.path.dirname(destination)
        if destination_dir:
            try:
                logger.debug(f"Ensuring destination directory exists: {destination_dir}")
                mkdir_cmd = ["mkdir", "-p", destination_dir]
                result = self._exec_remote_command(mkdir_cmd, check=False)
                if result.returncode != 0:
                    logger.warning(f"Could not create destination directory: {destination_dir}")
            except Exception as e:
                logger.warning(f"Error creating destination directory: {e}")
        
        # Test if remote destination directory is writeable without sudo first
        dest_dir = os.path.dirname(destination)
        if dest_dir:
            try:
                logger.debug(f"Testing if destination directory is directly writeable without sudo: {dest_dir}")
                test_cmd = ["touch", f"{dest_dir}/.test_write_access"]
                rm_cmd = ["rm", "-f", f"{dest_dir}/.test_write_access"]
                result = self._exec_remote_command(test_cmd, check=False)
                if result.returncode == 0:
                    logger.info("Destination directory is directly writeable without sudo!")
                    # Clean up test file
                    self._exec_remote_command(rm_cmd, check=False)
                    # Also check if btrfs command is available without sudo
                    btrfs_cmd = ["btrfs", "--version"]
                    btrfs_result = self._exec_remote_command(btrfs_cmd, check=False)
                    if btrfs_result.returncode == 0:
                        logger.info("btrfs command is available without sudo - will try direct receive")
                        logger.debug("Using direct btrfs receive without sudo")
                        return receive_cmd
                    else:
                        logger.debug("btrfs command requires sudo even though directory is writeable")
                else:
                    logger.debug("Destination directory requires sudo for writing")
            except Exception as e:
                logger.debug(f"Error testing direct write access: {e}")
        
        # Build the remote command with optional sudo
        receive_cmd = self._build_remote_command(receive_cmd)
        
        # Determine if we need a TTY for this command (needed if sudo might prompt for password)
        needs_tty = False
        if self.config.get("ssh_sudo", False) and not self.config.get("passwordless", False):
            # If using sudo without passwordless, we might need a TTY
            logger.debug("Sudo configuration detected that may require TTY")
            needs_tty = True
        
        # Build the SSH command with appropriate TTY settings
        ssh_base_cmd = self.ssh_manager._ssh_base_cmd(force_tty=needs_tty)
        
        # Add options to make SSH more resilient to network issues
        # ServerAliveInterval sends a keep-alive packet every N seconds
        # ServerAliveCountMax defines how many missed responses before disconnect
        ssh_options = [
            "-o", "ServerAliveInterval=5", 
            "-o", "ServerAliveCountMax=3",
            "-o", "TCPKeepAlive=yes",
            "-o", "ConnectTimeout=10"
        ]
        
        # If we need TTY, ensure related SSH options are set correctly
        if needs_tty:
            ssh_options.extend([
                "-o", "RequestTTY=yes",
                "-o", "BatchMode=no"  # Allow password prompts if necessary
            ])
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
                    env["SSH_ASKPASS_REQUIRE"] = "force"  # Force using askpass if available
                if "DISPLAY" not in env and os.environ.get("SUDO_USER"):
                    # Try to get DISPLAY from original user's environment
                    try:
                        sudo_user = os.environ.get("SUDO_USER")
                        proc = subprocess.run(
                            ["sudo", "-u", sudo_user, "printenv", "DISPLAY"],
                            capture_output=True,
                            text=True,
                            check=False
                        )
                        if proc.returncode == 0 and proc.stdout.strip():
                            env["DISPLAY"] = proc.stdout.strip()
                            logger.debug(f"Set DISPLAY={env['DISPLAY']} from sudo user")
                    except Exception as e:
                        logger.debug(f"Could not get DISPLAY from sudo user: {e}")
            else:
                # Not using TTY, ensure non-interactive mode
                env["SSH_ASKPASS_REQUIRE"] = "never"
            
            # Create named FIFO pipes for better stream handling if necessary
            receive_process = subprocess.Popen(
                ssh_cmd,
                stdin=stdin_pipe,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,  # Use unbuffered mode for better streaming
                env=env,
                text=False,  # Use binary mode for streams
            )
            
            logger.debug("btrfs receive process started with PID: %d", receive_process.pid)
            return receive_process
        except Exception as e:
            logger.error("Failed to start btrfs receive process: %s", e)
            # Check if this is a connection-related error
            if isinstance(e, (BrokenPipeError, ConnectionError, ConnectionResetError)):
                logger.error("SSH connection error detected. The connection might be broken.")
                # Re-raise as a more specific exception for better retry handling
                raise ConnectionError(f"SSH connection error: {e}")
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

    def send_receive(self, source: str, destination: str, max_retries: int = 3) -> None:
        """Perform btrfs send/receive operation with retry capability."""
        logger.info(
            "Starting btrfs send/receive operation from %s to %s", source, destination
        )
        logger.debug("Source path: %s", source)
        logger.debug("Destination path: %s", destination)
        
        # Test if destination already exists
        try:
            # First check if the destination exists
            test_cmd = ["test", "-e", destination]
            result = self._exec_remote_command(test_cmd, check=False)
            if result.returncode == 0:
                logger.warning(
                    "Destination path already exists: %s - this might cause btrfs receive to fail", 
                    destination
                )
                
                # Also check what type of file it is
                file_type_cmd = ["stat", "-c", "%F", destination]
                type_result = self._exec_remote_command(file_type_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
                if type_result.returncode == 0:
                    file_type = type_result.stdout.decode(errors="replace").strip()
                    logger.warning("Existing destination is of type: %s", file_type)
                    if "directory" in file_type.lower():
                        logger.error("Destination is a directory - btrfs receive cannot write to a directory!")
                        logger.error("Make sure the destination path points to a subvolume, not a directory")
        except Exception as e:
            logger.warning("Could not check if destination exists: %s", e)
        
        retry_count = 0
        last_exception = None
        
        while retry_count <= max_retries:
            # If this is a retry, log it and wait before trying again
            if retry_count > 0:
                logger.info(f"Retry attempt {retry_count}/{max_retries} for send/receive operation")
                time.sleep(5)  # Wait 5 seconds between retries
                
                # Re-establish SSH connection if needed
                if not self.ssh_manager.is_master_alive():
                    logger.info("SSH connection lost, attempting to re-establish...")
                    if not self.ssh_manager.start_master():
                        logger.error("Failed to re-establish SSH connection")
                        raise RuntimeError("Failed to re-establish SSH connection after disconnect")
                    logger.info("SSH connection re-established successfully")
            
            send_proc = None
            recv_proc = None
                
            # Establish SSH connection with context manager
            with self.ssh_manager:
                try:
                    # Start the send process
                    logger.debug("Starting local btrfs send process for %s", source)
                    send_cmd = ["btrfs", "send", source]
                    logger.info("Running local command: %s", " ".join(send_cmd))
                    send_proc = subprocess.Popen(
                        send_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        bufsize=0,  # Use unbuffered mode for better streaming
                    )
                    
                    # Start the receive process
                    logger.debug("Starting btrfs receive process on remote host")
                    
                    # Check if we're in passwordless-only mode
                    passwordless_only = os.environ.get("BTRFS_BACKUP_PASSWORDLESS_ONLY", "0").lower() in ("1", "true", "yes")
                    if passwordless_only:
                        logger.warning("Running in passwordless-only mode - any sudo password prompt will cause failure")
                    
                    # Check for sudo requirements on remote host
                    if self.config.get("ssh_sudo", False):
                        logger.info("Verifying sudo access on remote host...")
                        try:
                            # First verify sudo access in general
                            sudo_test = self._exec_remote_command(
                                ["sudo", "-n", "-v"], 
                                check=False, 
                                timeout=10
                            )
                                
                            if sudo_test.returncode != 0:
                                logger.warning("Passwordless sudo is not available on remote host.")
                                logger.warning("This will likely cause the transfer to fail if sudo is required.")
                            else:
                                logger.info("Passwordless sudo is available on the remote host")
                                
                            # Then specifically test btrfs receive with sudo
                            btrfs_test = self._exec_remote_command(
                                ["sudo", "-n", "btrfs", "--version"],
                                check=False,
                                timeout=10,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE
                            )
                                
                            if btrfs_test.returncode != 0:
                                logger.warning("Cannot run btrfs commands with passwordless sudo on remote host.")
                                logger.warning("Transfer will fail if sudo password is required for btrfs commands.")
                                logger.warning("Add the following to sudoers on remote host to fix this:")
                                logger.warning("    %sudo ALL=(ALL) NOPASSWD: /usr/bin/btrfs")
                                # Log the error output to help diagnose the issue
                                stderr = btrfs_test.stderr.decode(errors="replace").strip()
                                if stderr:
                                    logger.warning("Remote sudo error: %s", stderr)
                            else:
                                btrfs_version = btrfs_test.stdout.decode(errors="replace").strip()
                                logger.info("Passwordless sudo for btrfs verified on remote host: %s", btrfs_version)
                                
                            # Also check direct write access to destination directory
                            dest_dir = os.path.dirname(destination)
                            if dest_dir:
                                write_test = self._exec_remote_command(
                                    ["touch", f"{dest_dir}/.test_write_access"],
                                    check=False,
                                    timeout=10,
                                    stderr=subprocess.PIPE
                                )
                                if write_test.returncode == 0:
                                    logger.info("Destination directory is directly writeable without sudo")
                                    self._exec_remote_command(
                                        ["rm", "-f", f"{dest_dir}/.test_write_access"],
                                        check=False
                                    )
                                else:
                                    stderr = write_test.stderr.decode(errors="replace").strip()
                                    logger.info("Destination directory requires sudo for writing: %s", stderr)
                            
                                    # Test if it's writable with sudo
                                    sudo_write_test = self._exec_remote_command(
                                        ["sudo", "-n", "touch", f"{dest_dir}/.test_write_access_sudo"],
                                        check=False,
                                        timeout=10
                                    )
                                    if sudo_write_test.returncode == 0:
                                        logger.info("Destination directory is writeable with sudo")
                                        self._exec_remote_command(
                                            ["sudo", "-n", "rm", "-f", f"{dest_dir}/.test_write_access_sudo"],
                                            check=False
                                        )
                                    else:
                                        logger.warning("Destination directory is not writeable even with sudo!")
                            else:
                                logger.warning("Cannot determine destination directory for write testing")
                                
                        except Exception as e:
                            logger.warning(f"Could not verify sudo access: {e}")
                        
                    recv_proc = self._btrfs_receive(destination, send_proc.stdout)

                    # Close stdout after passing to receive process to allow proper SIGPIPE
                    logger.debug(
                        "Closing send_proc stdout to allow SIGPIPE if recv_proc exits"
                    )
                    send_proc.stdout.close()
                    
                    # If sudo requires a password and we're using -S flag, 
                    # it will be read from stdin (which is connected to our pipe)
                    logger.debug("btrfs receive process started with PID: %d", recv_proc.pid)
                    
                    # Log additional debug info about the receive process
                    logger.debug("Process information: stdin=%s, stdout=%s, stderr=%s", 
                                recv_proc.stdin, recv_proc.stdout, recv_proc.stderr)

                    # Use select to wait for processes with timeout to detect stalled connections
                    def wait_with_timeout(proc, timeout=30):
                        """Wait for process with timeout to detect stalled connections."""
                        end_time = time.time() + timeout
                        
                        # Setup file descriptors to monitor for activity
                        fds_to_watch: Dict[int, Tuple[Any, str]] = {}
                        if hasattr(proc, 'stderr') and proc.stderr:
                            fds_to_watch[proc.stderr.fileno()] = (proc.stderr, 'stderr')
                        
                        while proc.poll() is None and time.time() < end_time:
                            if fds_to_watch:
                                # Wait for activity on file descriptors or timeout
                                try:
                                    ready, _, _ = select.select(fds_to_watch.keys(), [], [], 0.5)
                                    for fd in ready:
                                        fd_obj, fd_name = fds_to_watch[fd]
                                        data = fd_obj.read(1024)
                                        if not data:  # EOF
                                            del fds_to_watch[fd]
                                        else:
                                            logger.debug(f"Activity on {fd_name}: {data.decode(errors='replace').strip()}")
                                except (select.error, ValueError, IOError) as e:
                                    # Handle select errors (including closed file descriptors)
                                    logger.debug(f"Select error: {e}")
                                    break
                            else:
                                time.sleep(0.1)
                                
                        return proc.poll() is not None

                    # Wait for processes to complete with monitoring
                    logger.debug("Waiting for btrfs send process to complete...")
                    if not wait_with_timeout(send_proc, timeout=60):  # Increased timeout
                        logger.warning("btrfs send process appears stalled, checking SSH connection")
                        if not self.ssh_manager.is_master_alive():
                            logger.error("SSH connection appears to be broken")
                            # Try to kill the stuck process before raising error
                            try:
                                logger.warning("Terminating stuck send process...")
                                send_proc.terminate()
                                time.sleep(0.5)
                                if send_proc.poll() is None:
                                    logger.warning("Forcefully killing stuck send process...")
                                    send_proc.kill()
                            except Exception as e:
                                logger.error(f"Error cleaning up send process: {e}")
                            raise ConnectionError("SSH connection broken during transfer")
                        else:
                            logger.warning("SSH connection is alive but transfer appears stalled - continuing to wait")
                    send_returncode = send_proc.wait()
                    logger.debug("btrfs send process completed with return code: %d", send_returncode)

                    logger.debug("Waiting for btrfs receive process to complete...")
                    if not wait_with_timeout(recv_proc, timeout=60):  # Increased timeout
                        logger.warning("btrfs receive process appears stalled, checking SSH connection")
                        if not self.ssh_manager.is_master_alive():
                            logger.error("SSH connection appears to be broken")
                            # Try to kill the stuck process before raising error
                            try:
                                logger.warning("Terminating stuck receive process...")
                                recv_proc.terminate()
                                time.sleep(0.5)
                                if recv_proc.poll() is None:
                                    logger.warning("Forcefully killing stuck receive process...")
                                    recv_proc.kill()
                            except Exception as e:
                                logger.error(f"Error cleaning up receive process: {e}")
                            raise ConnectionError("SSH connection broken during receive operation")
                        else:
                            logger.warning("SSH connection is alive but receive process appears stalled - continuing to wait")
                    recv_returncode = recv_proc.wait()
                    logger.debug("btrfs receive process completed with return code: %d", recv_returncode)

                    # Check for errors and log output
                    if send_returncode != 0:
                        send_stderr = send_proc.stderr.read().decode(errors="replace")
                        logger.error("btrfs send failed with return code %d", send_returncode)
                        logger.error("btrfs send stderr: %s", send_stderr)
                        
                        # Detect specific error conditions
                        if "Broken pipe" in send_stderr or "Connection reset" in send_stderr:
                            raise ConnectionError(f"btrfs send failed due to connection issue: {send_stderr}")
                        elif "not a btrfs subvolume" in send_stderr.lower():
                            raise ValueError(f"Source path is not a btrfs subvolume: {source}")
                        elif "failed to open" in send_stderr.lower():
                            raise FileNotFoundError(f"Failed to open source subvolume: {source}")
                        else:
                            raise RuntimeError(f"btrfs send failed: {send_stderr}")

                    if recv_returncode != 0:
                        recv_stderr = recv_proc.stderr.read().decode(errors="replace")
                        logger.error("btrfs receive failed with return code %d", recv_returncode)
                        logger.error("btrfs receive stderr: %s", recv_stderr)
                        
                        # Detect specific error conditions
                        if "Connection closed" in recv_stderr or "broken pipe" in recv_stderr.lower():
                            raise ConnectionError(f"btrfs receive failed due to connection issue: {recv_stderr}")
                        elif "sudo" in recv_stderr.lower() and "password" in recv_stderr.lower():
                            logger.error("=============================================================")
                            logger.error("SUDO PASSWORD REQUIRED ON REMOTE HOST")
                            logger.error("This transfer cannot complete because sudo requires a password")
                            logger.error("To fix this, run the following on the remote host as root:")
                            logger.error("echo '%sudo ALL=(ALL) NOPASSWD: /usr/bin/btrfs' >> /etc/sudoers.d/btrfs")
                            logger.error("=============================================================")
                            raise PermissionError(f"Sudo password required but not provided: {recv_stderr}")
                        elif "permission denied" in recv_stderr.lower():
                            logger.error("=============================================================")
                            logger.error("PERMISSION DENIED ON REMOTE HOST")
                            logger.error("Make sure the destination path is writable by your SSH user")
                            logger.error("=============================================================")
                            raise PermissionError(f"Permission denied on remote host: {recv_stderr}")
                        elif "not a btrfs" in recv_stderr.lower():
                            raise ValueError(f"Destination is not on a btrfs filesystem: {destination}")
                        elif "no space left" in recv_stderr.lower():
                            raise OSError(f"No space left on remote device: {recv_stderr}")
                        elif "askpass" in recv_stderr.lower():
                            logger.error("=============================================================")
                            logger.error("SUDO ASKPASS PROBLEM DETECTED")
                            logger.error("This may be related to sudo requiring a password prompt")
                            logger.error("Try setting up passwordless sudo for btrfs on the remote host")
                            logger.error("=============================================================")
                            raise PermissionError(f"Sudo askpass problem: {recv_stderr}")
                        else:
                            raise RuntimeError(f"btrfs receive failed: {recv_stderr}")
                    
                    # Verify transfer completion
                    logger.debug("Verifying transfer completion on remote host...")
                    verify_cmd = self._build_remote_command(["ls", "-l", destination])
                    verify_proc = self._exec_remote_command(verify_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    if verify_proc.returncode != 0:
                        logger.error("Verification failed: %s", verify_proc.stderr.decode(errors="replace"))
                        raise RuntimeError("Failed to verify transfer on remote host")
                    logger.debug("Transfer verified successfully on remote host.")
                    
                    # If we get here without exceptions, the transfer was successful
                    logger.info("btrfs send/receive completed successfully")
                    return
                    
                except (ConnectionError, ConnectionResetError, BrokenPipeError, OSError) as e:
                    # These are network-related errors we can retry
                    last_exception = e
                    logger.warning(f"Network error during transfer (attempt {retry_count+1}/{max_retries+1}): {e}")
                    retry_count += 1
                    
                    # Clean up any potentially stuck processes
                    if send_proc and send_proc.poll() is None:
                        try:
                            logger.warning("Terminating stuck send process during retry...")
                            send_proc.terminate()
                            time.sleep(0.5)
                            if send_proc.poll() is None:
                                logger.warning("Forcefully killing stuck send process during retry...")
                                send_proc.kill()
                        except Exception as e:
                            logger.error(f"Error cleaning up send process during retry: {e}")
                    
                    if recv_proc and recv_proc.poll() is None:
                        try:
                            logger.warning("Terminating stuck receive process during retry...")
                            recv_proc.terminate()
                            time.sleep(0.5)
                            if recv_proc.poll() is None:
                                logger.warning("Forcefully killing stuck receive process during retry...")
                                recv_proc.kill()
                        except Exception as e:
                            logger.error(f"Error cleaning up receive process during retry: {e}")
                    
                    # Force cleanup of SSH connection to ensure clean slate for retry
                    self.ssh_manager.stop_master()
                    continue
                    
                except Exception as e:
                    # Log other exceptions that we won't retry
                    logger.error(f"Error during transfer: {e}")
                    raise
                    
                finally:
                    # Ensure proper cleanup of processes
                    if send_proc:
                        # Ensure process is terminated before cleaning up
                        if send_proc.poll() is None:
                            try:
                                send_proc.terminate()
                                time.sleep(0.5)
                                if send_proc.poll() is None:
                                    send_proc.kill()
                            except Exception as e:
                                logger.debug(f"Error terminating send process: {e}")
                                
                        # Close file descriptors
                        if hasattr(send_proc, "stderr") and send_proc.stderr:
                            send_proc.stderr.close()
                    
                    if recv_proc:
                        # Ensure process is terminated before cleaning up
                        if recv_proc.poll() is None:
                            try:
                                recv_proc.terminate()
                                time.sleep(0.5)
                                if recv_proc.poll() is None:
                                    recv_proc.kill()
                            except Exception as e:
                                logger.debug(f"Error terminating receive process: {e}")
                                
                        # Close file descriptors
                        if hasattr(recv_proc, "stdout") and recv_proc.stdout:
                            recv_proc.stdout.close()
                        if hasattr(recv_proc, "stderr") and recv_proc.stderr:
                            recv_proc.stderr.close()
        
        # If we've exhausted all retries, raise the last exception
        if last_exception:
            logger.error(f"Failed to complete transfer after {max_retries+1} attempts")
            raise last_exception
        
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