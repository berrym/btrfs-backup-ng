# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/endpoint/ssh.py
Create commands with ssh endpoints.
"""

import copy
import subprocess
import tempfile
from pathlib import Path

from btrfs_backup_ng import __util__
from btrfs_backup_ng.__logger__ import logger

from .common import Endpoint


class SSHEndpoint(Endpoint):
    """Commands for creating an SSH endpoint."""

    def __init__(self, hostname, config=None, **kwargs) -> None:
        """
        Initialize the SSHEndpoint with a hostname and configuration.

        Args:
            hostname (str): The SSH hostname.
            config (dict): Configuration dictionary containing endpoint settings.
            kwargs: Additional keyword arguments for backward compatibility.
        """
        super().__init__(config=config, **kwargs)
        self.config["hostname"] = hostname
        self.config["port"] = self.config.get("port")
        self.config["username"] = self.config.get("username")
        self.config["ssh_opts"] = self.config.get("ssh_opts", [])
        self.config["ssh_sudo"] = self.config.get("ssh_sudo", False)

        # SSHFS options
        self.config["sshfs_opts"] = copy.deepcopy(self.config["ssh_opts"])
        self.config["sshfs_opts"] += ["auto_unmount", "reconnect", "cache=no"]

        # Resolve paths
        if self.config.get("source"):
            self.config["source"] = Path(self.config["source"]).resolve()
            if self.config["path"] and not str(self.config["path"]).startswith("/"):
                self.config["path"] = self.config["source"] / self.config["path"]
        self.config["path"] = Path(self.config["path"]).resolve()
        self.sshfs = None

    def __repr__(self) -> str:
        return (
            f"(SSH) {self._build_connect_string(with_port=True)}{self.config['path']}"
        )

    def get_id(self) -> str:
        """Return a unique identifier for this SSH endpoint."""
        s = self.config["hostname"]
        if self.config["username"]:
            s = f"{self.config['username']}@{s}"
        if self.config["port"]:
            s = f"{s}:{self.config['port']}"
        return f"ssh://{s}{self.config['path']}"

    def _prepare(self) -> None:
        """Prepare the SSH endpoint by checking SSH availability and mounting SSHFS."""
        # Check whether SSH is available
        logger.debug("Checking for ssh ...")
        cmd = ["ssh"]
        try:
            __util__.exec_subprocess(
                cmd,
                method="call",
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError as e:
            logger.debug("  -> got exception: %s", e)
            logger.info("ssh command is not available")
            raise __util__.AbortError

        logger.debug("  -> ssh is available")

        # SSHFS is useful for listing directories and reading/writing locks
        tempdir = tempfile.mkdtemp()
        logger.debug("Created tempdir: %s", tempdir)
        mount_point = Path(tempdir) / "mnt"
        mount_point.mkdir()
        logger.debug("Created directory: %s", mount_point)
        logger.debug("Mounting sshfs ...")

        cmd = ["sshfs"]
        if self.config["port"]:
            cmd += ["-p", str(self.config["port"])]
        for opt in self.config["sshfs_opts"]:
            cmd += ["-o", opt]
        cmd += [f"{self._build_connect_string()}:/", str(mount_point)]
        try:
            __util__.exec_subprocess(
                cmd,
                method="check_call",
                stdout=subprocess.DEVNULL,
            )
        except FileNotFoundError as e:
            logger.debug("  -> got exception: %s", e)
            if self.config.get("source"):
                # SSHFS is mandatory for sourcing from SSH
                logger.info(
                    "  The sshfs command is not available but it is "
                    "mandatory for sourcing from SSH.",
                )
                raise __util__.AbortError
        else:
            self.sshfs = mount_point
            logger.debug("  -> sshfs is available")

        # Create directories, if needed
        dirs = []
        if self.config.get("source"):
            dirs.append(self.config["source"])
        dirs.append(self.config["path"])
        if self.sshfs:
            for d in dirs:
                sshfs_path = self._path_to_sshfs(d)
                if not sshfs_path.is_dir():
                    logger.info("Creating directory: %s", d)
                    try:
                        sshfs_path.mkdir(parents=True, exist_ok=True)
                    except OSError as e:
                        logger.error("Error creating new location %s: %s", d, e)
                        raise __util__.AbortError
        else:
            cmd = ["mkdir", "-p", *[str(d) for d in dirs]]
            self._exec_command(cmd)

    def _collapse_commands(self, commands, abort_on_failure=True):
        """Concatenate all given commands, using ';' as a separator."""
        collapsed = []
        for i, cmd in enumerate(commands):
            if isinstance(cmd, (list, tuple)):
                collapsed.extend(cmd)
                if len(commands) > i + 1:
                    collapsed.append("&&" if abort_on_failure else ";")

        return [collapsed]

    def _exec_command(self, options, **kwargs):
        """Execute the command on the remote host."""
        new_cmd = ["ssh"]
        if self.config["port"]:
            new_cmd += ["-p", str(self.config["port"])]
        for opt in self.config["ssh_opts"]:
            new_cmd += ["-o", opt]
        new_cmd += [self._build_connect_string()]
        if self.config["ssh_sudo"]:
            new_cmd += ["sudo"]
        new_cmd.extend(options)

        return __util__.exec_subprocess(new_cmd, **kwargs)

    def _listdir(self, location):
        """List directory contents remotely via 'ls -1A'."""
        if self.sshfs:
            items = [str(item) for item in self._path_to_sshfs(location).iterdir()]
        else:
            cmd = ["ls", "-1A", str(location)]
            output = self._exec_command(cmd, universal_newlines=True)
            items = output.splitlines()
        return items

    def _get_lock_file_path(self):
        """Get the lock file path, adjusted for SSHFS."""
        return self._path_to_sshfs(super()._get_lock_file_path())

    # Custom methods

    def _build_connect_string(self, with_port=False):
        """Build the SSH connection string."""
        s = self.config["hostname"]
        if self.config["username"]:
            s = f"{self.config['username']}@{s}"
        if with_port and self.config["port"]:
            s = f"{s}:{self.config['port']}"
        return s

    def _path_to_sshfs(self, path):
        """Join the given path with the SSHFS mount point."""
        if not self.sshfs:
            msg = "sshfs not mounted"
            raise ValueError(msg)
        path = Path(path)
        return self.sshfs / path.relative_to("/")
