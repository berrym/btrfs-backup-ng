# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/endpoint/ssh.py
Create commands with ssh endpoints.
"""

import copy
import os
import subprocess
import tempfile
from pathlib import Path

from btrfs_backup_ng import __util__
from btrfs_backup_ng.__logger__ import logger

from .common import Endpoint
import shlex
import threading


class SSHEndpoint(Endpoint):
    """Commands for creating an SSH endpoint."""

    def __init__(self, hostname, config=None, **kwargs) -> None:
        super().__init__(config=config, **kwargs)
        self._parse_host_string(hostname)
        self.config["port"] = self.config.get("port")
        self.config["ssh_opts"] = self.config.get("ssh_opts", [])
        self.config["ssh_sudo"] = self.config.get("ssh_sudo", False)
        self.config["sshfs_opts"] = copy.deepcopy(self.config["ssh_opts"]) + [
            "auto_unmount",
            "reconnect",
            "cache=no",
        ]
        # Resolve paths
        if self.config.get("source"):
            self.config["source"] = Path(self.config["source"]).resolve()
            if self.config.get("path") and not str(self.config["path"]).startswith("/"):
                self.config["path"] = self.config["source"] / self.config["path"]
        self.config["path"] = Path(self.config["path"]).resolve()
        self.sshfs = None
        self._ssh_lock = threading.Lock()

    def _parse_host_string(self, hoststr):
        """
        Parse [user@]host[:/path] or [user@]host[:port][/path] into config.
        Ensures the path component is always absolute from the remote root (/).
        Strips any ssh:, sshfs:, user, host, or port from the path.
        """
        user = None
        host = None
        port = None
        path = None

        if "@" in hoststr:
            user, hoststr = hoststr.split("@", 1)
        if ":" in hoststr:
            host, rest = hoststr.split(":", 1)
            if "/" in rest:
                port_or_path, *path_parts = rest.split("/", 1)
                if port_or_path.isdigit():
                    port = int(port_or_path)
                    path = "/" + path_parts[0] if path_parts else "/"
                else:
                    path = "/" + rest if not rest.startswith("/") else rest
            else:
                if rest.isdigit():
                    port = int(rest)
                else:
                    path = "/" + rest if not rest.startswith("/") else rest
        else:
            host = hoststr

        if user:
            self.config["username"] = user
        self.config["hostname"] = host
        if port:
            self.config["port"] = port

        # Always ensure path is absolute from remote root and does not include user/host/scheme/port
        if path:
            for prefix in ("sshfs:", "ssh:", "//"):
                if path.startswith(prefix):
                    path = path[len(prefix) :]
            if "@" in path:
                path = path.split("@", 1)[-1]
            if ":" in path and path.split(":")[0].replace("/", "").isdigit():
                path = path.split(":", 1)[-1]
            if ":" in path:
                path = path.split(":", 1)[-1]
            path = "/" + path.lstrip("/")
            self.config["path"] = str(Path(path).resolve())

    def __repr__(self) -> str:
        return (
            f"(SSH) {self._build_connect_string(with_port=True)}{self.config['path']}"
        )

    def get_id(self) -> str:
        """Return a unique identifier for this SSH endpoint."""
        s = self._build_connect_string(with_port=True)
        return f"ssh://{s}{self.config['path']}"

    def _build_connect_string(self, with_port=False):
        """Build the SSH/SSHFS connection string, always including username if specified."""
        s = self.config["hostname"]
        if self.config.get("username"):
            s = f"{self.config['username']}@{s}"
        if with_port and self.config.get("port"):
            s = f"{s}:{self.config['port']}"
        return s

    def _prepare(self) -> None:
        """Prepare the SSH endpoint by checking SSH availability and creating directories via SSH (prefer over SSHFS)."""
        logger.debug("Checking for ssh ...")
        try:
            __util__.exec_subprocess(
                ["ssh"],
                method="call",
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError as e:
            logger.debug("  -> got exception: %s", e)
            logger.info("ssh command is not available")
            raise __util__.AbortError

        logger.debug("  -> ssh is available")

        # Prefer creating directories via SSH, not SSHFS
        dirs = []
        if self.config.get("source"):
            dirs.append(self.config["source"])
        dirs.append(self.config["path"])
        self._exec_command(["mkdir", "-p", *map(str, dirs)])

        # Optionally mount SSHFS only if explicitly requested (not default)
        if getattr(self, "force_sshfs", False):
            self._mount_sshfs()

    def _mount_sshfs(self):
        tempdir = tempfile.mkdtemp()
        logger.debug("Created tempdir: %s", tempdir)
        mount_point = Path(tempdir) / "mnt"
        mount_point.mkdir()
        logger.debug("Created directory: %s", mount_point)
        logger.debug("Mounting sshfs ...")

        cmd = ["sshfs"]
        if self.config.get("port"):
            cmd += ["-p", str(self.config["port"])]
        for opt in self.config["sshfs_opts"]:
            cmd += ["-o", opt]
        sshfs_connect_str = self._build_connect_string()
        cmd += [f"{sshfs_connect_str}:/", str(mount_point)]
        try:
            __util__.exec_subprocess(
                cmd,
                method="check_call",
                stdout=subprocess.DEVNULL,
            )
        except FileNotFoundError as e:
            logger.debug("  -> got exception: %s", e)
            if self.config.get("source"):
                logger.info(
                    "  The sshfs command is not available but it is "
                    "mandatory for sourcing from SSH.",
                )
                raise __util__.AbortError
        else:
            self.sshfs = mount_point
            logger.debug("  -> sshfs is available")

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
        """
        Execute the command on the remote host.
        Prefer SSHFS for file operations if mounted, otherwise use SSH.
        Execute all commands with sudo locally (SSHFS), and remotely (SSH) only if ssh_sudo is set.
        Escalate privileges (sudo) for all commands if requested.
        Never log in as root unless 'root' is explicitly specified in the spec.
        Properly run remote commands by quoting and joining as needed.
        All remote commands are executed using 'sh -c' for POSIX compatibility.
        """
        cmd_to_run = (
            list(options) if isinstance(options, (list, tuple)) else [str(options)]
        )

        # Always escalate privileges for all commands locally (SSHFS)
        if self.sshfs:
            if cmd_to_run and cmd_to_run[0] != "sudo":
                cmd_to_run = ["sudo"] + cmd_to_run

            # For btrfs send/receive, ensure the source snapshot is transferred to the absolute pure path of the host
            if cmd_to_run and "btrfs" in cmd_to_run and "send" in cmd_to_run:
                self._rewrite_btrfs_send_paths(cmd_to_run)

            # Replace any remaining absolute paths with their SSHFS equivalents, but only if under the pure host path
            self._rewrite_sshfs_paths(cmd_to_run)
            return __util__.exec_subprocess(cmd_to_run, **kwargs)

        # Otherwise, use SSH for remote execution
        ssh_cmd = self._build_ssh_base_cmd()
        remote_cmd_to_run = self._prepare_remote_command(cmd_to_run)

        # --- FIX: Avoid sh -c for btrfs receive to prevent hangs with piped input ---
        # If the command is a btrfs receive (with or without sudo), do NOT wrap in sh -c
        if (remote_cmd_to_run[:3] == ["sudo", "btrfs", "receive"]) or (
            remote_cmd_to_run[:2] == ["btrfs", "receive"]
        ):
            ssh_cmd += remote_cmd_to_run
        else:
            quoted = " ".join(shlex.quote(str(arg)) for arg in remote_cmd_to_run)
            remote_cmd = ["sh", "-c", f"exec {quoted}"]
            ssh_cmd += remote_cmd

        # Allow concurrency (as before)
        with self._ssh_lock:
            return __util__.exec_subprocess(ssh_cmd, **kwargs)

    def _rewrite_btrfs_send_paths(self, cmd_to_run):
        """Rewrite btrfs send/receive source snapshot paths for SSHFS."""
        for i, arg in enumerate(cmd_to_run):
            if (
                isinstance(arg, str)
                and arg.startswith("/")
                and not arg.startswith("--")
                and self.config.get("source")
            ):
                try:
                    rel = Path(arg).relative_to(self.config["source"])
                    dest_path = Path(self.config["path"]) / rel
                    cmd_to_run[i] = str(self._path_to_sshfs(dest_path))
                except Exception:
                    pass
                break

    def _rewrite_sshfs_paths(self, cmd_to_run):
        """Replace any remaining absolute paths with their SSHFS equivalents, but only if under the pure host path."""
        for i, arg in enumerate(cmd_to_run):
            if (
                isinstance(arg, str)
                and arg.startswith("/")
                and not arg.startswith("--")
                and self.config.get("path")
            ):
                try:
                    rel = Path(arg).relative_to(self.config["path"])
                    cmd_to_run[i] = str(
                        self._path_to_sshfs(Path(self.config["path"]) / rel)
                    )
                except Exception:
                    pass

    def _build_ssh_base_cmd(self):
        """Build the base SSH command with options and user/host."""
        ssh_cmd = ["ssh"]
        if self.config.get("port"):
            ssh_cmd += ["-p", str(self.config["port"])]
        for opt in self.config.get("ssh_opts", []):
            ssh_cmd += ["-o", opt]

        username = self.config.get("username")
        hostname = self.config["hostname"]
        connect_user = username if username else None
        if connect_user is None and os.geteuid() == 0:
            connect_user = os.environ.get("SUDO_USER") or os.environ.get("USER")
        if connect_user and connect_user != "root":
            connect_str = f"{connect_user}@{hostname}"
        else:
            connect_str = hostname
        if self.config.get("port"):
            connect_str += f":{self.config['port']}"
        ssh_cmd += [connect_str]
        return ssh_cmd

    def _prepare_remote_command(self, cmd_to_run):
        """Prepare the remote command, adding sudo if needed, and stripping if not."""
        remote_cmd_to_run = list(cmd_to_run)
        if self.config.get("ssh_sudo"):
            if remote_cmd_to_run and remote_cmd_to_run[0] != "sudo":
                remote_cmd_to_run = ["sudo"] + remote_cmd_to_run
        else:
            if remote_cmd_to_run and remote_cmd_to_run[0] == "sudo":
                remote_cmd_to_run = remote_cmd_to_run[1:]
        return remote_cmd_to_run

    def _listdir(self, location):
        """List directory contents remotely via 'ls -1A'."""
        if self.sshfs:
            return [str(item) for item in self._path_to_sshfs(location).iterdir()]
        output = self._exec_command(
            ["ls", "-1A", str(location)], universal_newlines=True
        )
        return output.splitlines()

    def _get_lock_file_path(self):
        """Get the lock file path, adjusted for SSHFS."""
        return self._path_to_sshfs(super()._get_lock_file_path())

    def _path_to_sshfs(self, path):
        """Join the given path with the SSHFS mount point."""
        if not self.sshfs:
            raise ValueError("sshfs not mounted")
        return self.sshfs / Path(path).relative_to("/")
