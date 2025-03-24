# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/endpoint/ssh.py
Create commands with ssh endpoints.
"""

import copy
import os
import subprocess
import tempfile

from btrfs_backup_ng import __util__
from btrfs_backup_ng.__logger__ import logger

from .common import Endpoint


class SSHEndpoint(Endpoint):
    """Commands for creating an ssh endpoint."""

    def __init__(
        self,
        hostname,
        port=None,
        username=None,
        ssh_opts=None,
        ssh_sudo=False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.hostname = hostname
        self.port = port
        self.username = username
        self.ssh_opts = ssh_opts or []
        self.sshfs_opts = copy.deepcopy(self.ssh_opts)
        self.sshfs_opts += ["auto_unmount", "reconnect", "cache=no"]
        self.ssh_sudo = ssh_sudo
        if self.source:
            self.source = os.path.normpath(self.source)
            if self.path is not None and not self.path.startswith("/"):
                self.path = os.path.join(self.source, self.path)
        self.path = os.path.normpath(self.path)
        self.sshfs = None

    def __repr__(self) -> str:
        return f"(SSH) {self._build_connect_string(with_port=True)}{self.path}"

    def get_id(self) -> str:
        s = self.hostname
        if self.username:
            s = f"{self.username}@{s}"
        if self.port:
            s = f"{s}:{self.port}"
        return f"ssh://{s}{self.path}"

    def _prepare(self) -> None:
        # check whether ssh is available
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

        # sshfs is useful for listing directories and reading/writing locks
        tempdir = tempfile.mkdtemp()
        logger.debug("Created tempdir: %s", tempdir)
        mount_point = os.path.join(tempdir, "mnt")
        os.makedirs(mount_point)
        logger.debug("Created directory: %s", mount_point)
        logger.debug("Mounting sshfs ...")

        cmd = ["sshfs"]
        if self.port:
            cmd += ["-p", str(self.port)]
        for opt in self.sshfs_opts:
            cmd += ["-o", opt]
        cmd += [f"{self._build_connect_string()}:/", mount_point]
        try:
            __util__.exec_subprocess(
                cmd,
                method="check_call",
                stdout=subprocess.DEVNULL,
            )
        except FileNotFoundError as e:
            logger.debug("  -> got exception: %s", e)
            if self.source:
                # we need that for the locks
                logger.info(
                    "  The sshfs command is not available but it is "
                    "mandatory for sourcing from SSH.",
                )
                raise __util__.AbortError
        else:
            self.sshfs = mount_point
            logger.debug("  -> sshfs is available")

        # create directories, if needed
        dirs = []
        if self.source is not None:
            dirs.append(self.source)
        dirs.append(self.path)
        if self.sshfs:
            for d in dirs:
                if not os.path.isdir(self._path_to_sshfs(d)):
                    logger.info("Creating directory: %s", d)
                    try:
                        os.makedirs(self._path_to_sshfs(d))
                    except OSError as e:
                        logger.error("Error creating new location %s: %s", d, e)
                        raise __util__.AbortError
        else:
            cmd = ["mkdir", "-p", *dirs]
            self._exec_command(cmd)

    def _collapse_commands(self, commands, abort_on_failure=True):
        """Concatenates all given commands, ';' is inserted as separator."""
        collapsed = []
        for i, cmd in enumerate(commands):
            if isinstance(cmd, (list, tuple)):
                collapsed.extend(cmd)
                if len(commands) > i + 1:
                    collapsed.append("&&" if abort_on_failure else ";")

        return [collapsed]

    def _exec_command(self, command, **kwargs):
        """Executes the command at the remote host."""
        new_cmd = ["ssh"]
        if self.port:
            new_cmd += ["-p", str(self.port)]
        for opt in self.ssh_opts:
            new_cmd += ["-o", opt]
        new_cmd += [self._build_connect_string()]
        if self.ssh_sudo:
            new_cmd += ["sudo"]
        new_cmd.extend(command)

        return __util__.exec_subprocess(new_cmd, **kwargs)

    def _listdir(self, location):
        """Operates remotely via 'ls -1A'."""
        if self.sshfs:
            items = os.listdir(self._path_to_sshfs(location))
        else:
            cmd = ["ls", "-1A", location]
            output = self._exec_command(cmd, universal_newlines=True)
            items = output.splitlines()
        return items

    def _get_lock_file_path(self):
        return self._path_to_sshfs(super()._get_lock_file_path())

    # Custom methods

    def _build_connect_string(self, with_port=False):
        s = self.hostname
        if self.username:
            s = f"{self.username}@{s}"
        if with_port and self.port:
            s = f"{s}:{self.port}"
        return s

    def _path_to_sshfs(self, path):
        """Joins the given ``path`` with the sshfs mount_point."""
        if not self.sshfs:
            msg = "sshfs not mounted"
            raise ValueError(msg)
        path = path.removeprefix("/")
        return str(os.path.join(self.sshfs, path))
