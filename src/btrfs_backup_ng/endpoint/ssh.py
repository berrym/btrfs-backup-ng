# pyright: strict

"""btrfs-backup-ng: SSH Endpoint for managing remote operations.

This module provides the SSHEndpoint class, which integrates with SSHMasterManager
to handle SSH-based operations robustly, including btrfs send/receive commands.
"""

import copy
import os
import subprocess
import uuid
from threading import Lock
from typing import Optional

from btrfs_backup_ng.__logger__ import logger
from btrfs_backup_ng.sshutil.master import SSHMasterManager
from .common import Endpoint


class SSHEndpoint(Endpoint):
    """SSH-based endpoint for remote operations."""

    _is_remote = True
    _supports_multiprocessing = True

    def __init__(self, hostname: str, config: Optional[dict] = None, **kwargs) -> None:
        # Deep copy config to avoid shared references in multiprocessing
        if config is not None:
            config = copy.deepcopy(config)
        super().__init__(config=config, **kwargs)

        self.hostname = hostname
        logger.debug("SSHEndpoint initialized with hostname: %s", self.hostname)
        self.config["username"] = self.config.get("username")
        self.config["port"] = self.config.get("port")
        self.config["ssh_opts"] = self.config.get("ssh_opts", [])
        self.config["path"] = self.config.get("path", "/")
        self.config["ssh_sudo"] = self.config.get("ssh_sudo", False)
        self.config["passwordless"] = self.config.get("passwordless", False)

        self.ssh_manager = SSHMasterManager(
            hostname=self.hostname,
            username=self.config["username"],
            port=self.config["port"],
            ssh_opts=self.config["ssh_opts"],
            persist="60",
            debug=True,
        )

        self._lock = Lock()
        self._instance_id = f"{os.getpid()}_{uuid.uuid4().hex[:8]}"

    def __repr__(self) -> str:
        return f"(SSH) {self.hostname}:{self.config['path']}"

    def get_id(self) -> str:
        """Return a unique identifier for this SSH endpoint."""
        return f"ssh://{self.hostname}:{self.config['path']}"

    def _build_remote_command(self, command: list[str]) -> list[str]:
        """Prepare a remote command with optional sudo."""
        if self.config.get("ssh_sudo", False):
            return ["sudo", "-n"] + command
        return command

    def _exec_remote_command(self, command: list[str], **kwargs) -> subprocess.CompletedProcess:
        """Execute a command on the remote host."""
        remote_cmd = self._build_remote_command(command)
        ssh_cmd = self.ssh_manager._ssh_base_cmd() + ["--"] + remote_cmd
        logger.debug("Executing remote command: %s", " ".join(map(str, ssh_cmd)))
        return subprocess.run(ssh_cmd, **kwargs)

    def _btrfs_send(self, source: str, stdout_pipe) -> subprocess.Popen:
        """Run btrfs send locally and pipe its output."""
        command = ["btrfs", "send", source]
        logger.debug("Preparing to execute btrfs send: %s", command)
        try:
            process = subprocess.Popen(command, stdout=stdout_pipe, stderr=subprocess.PIPE)
            logger.debug("btrfs send process started successfully: %s", command)
            return process
        except Exception as e:
            logger.error("Failed to start btrfs send process: %s", e)
            raise

    def _btrfs_receive(self, destination: str, stdin_pipe) -> subprocess.Popen:
        """Run btrfs receive remotely and pipe its input."""
        command = ["btrfs", "receive", destination]
        remote_cmd = self._build_remote_command(command)
        ssh_cmd = self.ssh_manager._ssh_base_cmd() + ["--"] + remote_cmd
        logger.debug("Preparing to execute btrfs receive: %s", " ".join(map(str, ssh_cmd)))
        try:
            process = subprocess.Popen(ssh_cmd, stdin=stdin_pipe, stderr=subprocess.PIPE)
            logger.debug("btrfs receive process started successfully: %s", ssh_cmd)
            return process
        except FileNotFoundError as e:
            logger.error("btrfs receive command not found: %s", e)
            raise RuntimeError("btrfs receive failed to execute") from e
        except Exception as e:
            logger.error("Failed to start btrfs receive process: %s", e)
            raise

    def send_receive(self, source: str, destination: str) -> None:
        """Perform btrfs send/receive operation."""
        logger.info("Starting btrfs send/receive operation from %s to %s", source, destination)
        logger.debug("Source path: %s", source)
        logger.debug("Destination path: %s", destination)
        with self.ssh_manager:
            with subprocess.Popen(
                ["btrfs", "send", source],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ) as send_proc, self._btrfs_receive(destination, send_proc.stdout) as recv_proc:
                logger.debug("Closing send_proc stdout to allow SIGPIPE if recv_proc exits")
                send_proc.stdout.close()
                logger.debug("Waiting for btrfs send process to complete...")
                send_returncode = send_proc.wait()
                logger.debug("btrfs send process completed with return code: %d", send_returncode)

                logger.debug("Waiting for btrfs receive process to complete...")
                recv_returncode = recv_proc.wait()
                logger.debug("btrfs receive process completed with return code: %d", recv_returncode)

                if send_returncode != 0:
                    logger.error("btrfs send failed with return code %d", send_returncode)
                    logger.error("btrfs send stderr: %s", send_proc.stderr.read().decode())
                    raise RuntimeError("btrfs send failed")

                if recv_returncode != 0:
                    logger.error("btrfs receive failed with return code %d", recv_returncode)
                    logger.error("btrfs receive stderr: %s", recv_proc.stderr.read().decode())
                    raise RuntimeError("btrfs receive failed")

                logger.debug("Verifying transfer completion on remote host...")
                verify_cmd = self._build_remote_command(["ls", "-l", destination])
                verify_proc = self._exec_remote_command(verify_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if verify_proc.returncode != 0:
                    logger.error("Verification failed: %s", verify_proc.stderr.decode())
                    raise RuntimeError("Failed to verify transfer on remote host")
                logger.debug("Transfer verified successfully on remote host.")

        logger.info("btrfs send/receive completed successfully")

    def _prepare(self) -> None:
        """Prepare the SSH endpoint by ensuring SSH connectivity."""
        logger.debug("Preparing SSH endpoint for hostname: %s", self.hostname)
        with self.ssh_manager:
            try:
                self._exec_remote_command(["mkdir", "-p", self.config["path"]], check=True)
                logger.debug("Remote directory ensured: %s", self.config["path"])
            except subprocess.CalledProcessError as e:
                logger.error("Failed to prepare remote directory: %s", e)
                raise RuntimeError("Failed to prepare SSH endpoint") from e
