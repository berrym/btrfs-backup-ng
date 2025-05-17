# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/endpoint/common.py
Common functionality among modules.
"""

import contextlib
import getpass
import logging
import os
import subprocess
from pathlib import Path

from filelock import FileLock

from btrfs_backup_ng import __util__
from btrfs_backup_ng.__logger__ import logger


def require_source(method):
    """Decorator that ensures source is set on the object the called method belongs to."""

    def wrapped(self, *args, **kwargs):
        if self.config["source"] is None:
            msg = "source hasn't been set"
            raise ValueError(msg)
        return method(self, *args, **kwargs)

    return wrapped


class Endpoint:
    """Generic structure of a command endpoint."""

    def __init__(self, config=None, **kwargs) -> None:
        """
        Initialize the Endpoint with a configuration dictionary.

        Args:
            config (dict): Configuration dictionary containing endpoint settings.
            kwargs: Additional keyword arguments for backward compatibility.
        """
        config = config or {}
        self.config = {}

        # Always resolve source to absolute path
        val = config.get("source")
        if val is not None:
            path = Path(val).expanduser()
            if not path.is_absolute():
                path = Path.cwd() / path
            self.config["source"] = path.resolve()
        else:
            self.config["source"] = None

        # For path (destination), only expanduser, do not resolve unless absolute
        val = config.get("path")
        if val is not None:
            path = Path(val).expanduser()
            if not path.is_absolute():
                path = path.resolve()
            self.config["path"] = path
        else:
            self.config["path"] = None

        # Copy other config keys as before
        for key in [
            "snap_prefix",
            "convert_rw",
            "subvolume_sync",
            "btrfs_debug",
            "fs_checks",
            "lock_file_name",
        ]:
            self.config[key] = config.get(key, self.config.get(key))

        self.btrfs_flags = ["-vv"] if self.config["btrfs_debug"] else []
        self.__cached_snapshots = None

        for key, value in kwargs.items():
            self.config[key] = value
        self._lock = None  # Initialize lock

    def prepare(self):
        """Public access to _prepare, which is called after creating an endpoint."""
        logger.info("Preparing endpoint %r ...", self)
        return self._prepare()

    @require_source
    def snapshot(self, readonly=True, sync=True):
        """Takes a snapshot and returns the created object."""
        source_path = Path(self.config["source"])
        snapshot_dir = source_path / ".btrfs-backup-ng" / "snapshots"
        # Ensure the snapshot directory exists with correct permissions
        snapshot_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

        snapshot = __util__.Snapshot(snapshot_dir, self.config["snap_prefix"], self)
        snapshot_path = snapshot.get_path()
        logger.info("%s -> %s", self.config["source"], snapshot_path)

        # Lock file in the snapshot folder
        lock_path = snapshot_dir / ".btrfs-backup-ng.snapshot.lock"
        with FileLock(lock_path):
            self._remount(self.config["source"], read_write=True)
            commands = [
                self._build_snapshot_cmd(
                    self.config["source"], snapshot_path, readonly=readonly
                ),
            ]
            if sync:
                commands.append(self._build_sync_command())
            for cmd in self._collapse_commands(commands, abort_on_failure=True):
                self._exec_command({"command": cmd})
                self.add_snapshot(snapshot)
        return snapshot

    # @require_source
    # def snapshot(self, readonly=True, sync=True):
    #     """Takes a snapshot and returns the created object."""
    #     snapshot_dir = Path(self.config.get("snapshot_dir", "snapshot"))
    #     snapshot_path_base = self.config["path"] / snapshot_dir
    #     # Ensure the snapshot directory exists with correct permissions
    #     snapshot_path_base.mkdir(parents=True, exist_ok=True, mode=0o700)

    #     snapshot = __util__.Snapshot(
    #         snapshot_path_base, self.config["snap_prefix"], self
    #     )
    #     snapshot_path = snapshot.get_path()
    #     logger.info("%s -> %s", self.config["source"], snapshot_path)

    #     # Lock file in the snapshot folder
    #     lock_path = self.config["path"] / ".btrfs-backup-ng.snapshot.lock"
    #     with FileLock(lock_path):
    #         self._remount(self.config["source"], read_write=True)
    #         commands = [
    #             self._build_snapshot_cmd(
    #                 self.config["source"], snapshot_path, readonly=readonly
    #             ),
    #         ]
    #         if sync:
    #             commands.append(self._build_sync_command())
    #         for cmd in self._collapse_commands(commands, abort_on_failure=True):
    #             self._exec_command({"command": cmd})
    #         self.add_snapshot(snapshot)
    #     return snapshot

    @require_source
    def send(self, snapshot, parent=None, clones=None):
        """Calls 'btrfs send' for the given snapshot and returns its Popen object."""
        cmd = self._build_send_command(snapshot, parent=parent, clones=clones)
        return self._exec_command(
            {"command": cmd}, method="Popen", stdout=subprocess.PIPE
        )

    def receive(self, stdin):
        """Calls 'btrfs receive', setting the given pipe as its stdin."""
        cmd = self._build_receive_command(self.config["path"])
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        return self._exec_command(
            {"command": cmd}, method="Popen", stdin=stdin, stdout=stdout
        )

    def list_snapshots(self, flush_cache=False):
        """Returns a list with all snapshots found at ``self.path``."""
        logger.debug("Listing snapshots in: %s", self.config["path"])
        logger.debug("Snapshot prefix: %s", self.config["snap_prefix"])

        if self.__cached_snapshots is not None and not flush_cache:
            logger.debug(
                "Returning %d cached snapshots for %r.",
                len(self.__cached_snapshots),
                self,
            )
            return list(self.__cached_snapshots)

        logger.debug("Building snapshot cache of %r ...", self)
        snapshots = []
        listdir = self._listdir(self.config["path"])
        logger.debug("Directory contents: %r", listdir)
        for item in listdir:
            if item.startswith(self.config["snap_prefix"]):
                time_str = item[len(self.config["snap_prefix"]) :]
                try:
                    time_obj = __util__.str_to_date(time_str)
                except ValueError:
                    continue
                else:
                    snapshot = __util__.Snapshot(
                        self.config["path"],
                        self.config["snap_prefix"],
                        self,
                        time_obj=time_obj,
                    )
                    snapshots.append(snapshot)

        # Apply locks
        if self.config["source"]:
            lock_dict = self._read_locks()
            for snapshot in snapshots:
                snap_entry = lock_dict.get(snapshot.get_name(), {})
                for lock_type, locks in snap_entry.items():
                    getattr(snapshot, lock_type).update(locks)

        # Sort by date, then time
        snapshots.sort()

        # Populate cache
        self.__cached_snapshots = snapshots
        logger.debug(
            "Populated snapshot cache of %r with %d items.",
            self,
            len(snapshots),
        )

        return list(snapshots)

    @require_source
    def set_lock(self, snapshot, lock_id, lock_state, parent=False) -> None:
        """Adds/removes the given lock from ``snapshot`` and calls
        ``_write_locks`` with the updated locks.
        """
        if lock_state:
            if parent:
                snapshot.parent_locks.add(lock_id)
            else:
                snapshot.locks.add(lock_id)
        elif parent:
            snapshot.parent_locks.discard(lock_id)
        else:
            snapshot.locks.discard(lock_id)
        lock_dict = {}
        for _snapshot in self.list_snapshots():
            snap_entry = {}
            if _snapshot.locks:
                snap_entry["locks"] = list(_snapshot.locks)
            if _snapshot.parent_locks:
                snap_entry["parent_locks"] = list(_snapshot.parent_locks)
            if snap_entry:
                lock_dict[_snapshot.get_name()] = snap_entry
        self._write_locks(lock_dict)
        logger.debug(
            "Lock state for %s and lock_id %s changed to %s (parent = %s)",
            snapshot,
            lock_id,
            lock_state,
            parent,
        )

    def add_snapshot(self, snapshot, rewrite=True) -> None:
        """Adds a snapshot to the cache. If ``rewrite`` is set, a new
        ``__util__.Snapshot`` object is created with the original ``prefix``
        and ``time_obj``. However, ``path`` and ``endpoint`` are set to
        belong to this endpoint. The original snapshot object is
        dropped in that case.
        """
        if self.__cached_snapshots is None:
            return

        if rewrite:
            snapshot = __util__.Snapshot(
                self.config["path"],
                snapshot.prefix,
                self,
                time_obj=snapshot.time_obj,
            )

        self.__cached_snapshots.append(snapshot)
        self.__cached_snapshots.sort()

        return

    def delete_snapshots(self, snapshots, **kwargs) -> None:
        """Deletes the given snapshots, passing all keyword arguments to
        ``_build_deletion_commands``. Actually deletes the snapshot subvolumes.
        """
        # Only remove snapshots that have no lock remaining
        to_remove = [
            snapshot
            for snapshot in snapshots
            if not snapshot.locks and not snapshot.parent_locks
        ]

        logger.info("Removing %d snapshot(s) from %r:", len(to_remove), self)
        for snapshot in snapshots:
            if snapshot in to_remove:
                logger.info("  %s", snapshot)
            else:
                logger.info("  %s - is locked, keeping it", snapshot)

        for snapshot in to_remove:
            # Actually delete the subvolume using btrfs subvolume delete
            cmd = ["btrfs", "subvolume", "delete", str(snapshot.get_path())]
            try:
                self._exec_command({"command": cmd})
                logger.info("Deleted snapshot subvolume: %s", snapshot.get_path())
            except Exception as e:
                logger.error("Failed to delete snapshot %s: %s", snapshot.get_path(), e)

            # Remove from cache if present
            if self.__cached_snapshots is not None:
                with contextlib.suppress(ValueError):
                    self.__cached_snapshots.remove(snapshot)

    def delete_snapshot(self, snapshot, **kwargs) -> None:
        """Delete a snapshot."""
        self.delete_snapshots([snapshot], **kwargs)

    def delete_old_snapshots(self, keep):
        """
        Delete old snapshots, keeping only the most recent `keep` snapshots.
        """
        # List all snapshots (assume self.list_snapshots() returns sorted by creation time, oldest first)
        snapshots = self.list_snapshots()
        if keep <= 0 or len(snapshots) <= keep:
            return  # Nothing to delete

        # Determine which snapshots to delete
        to_delete = snapshots[:-keep]
        for snap in to_delete:
            logger.info("Deleting old snapshot: %s", snap)
            self.delete_snapshots([snap])

    # The following methods may be implemented by endpoints unless the
    # default behaviour is wanted.

    def __repr__(self) -> str:
        return f"{self.config['path']}"

    def get_id(self) -> str:
        """Return an id string to identify this endpoint over multiple runs."""
        return f"unknown://{self.config['path']}"

    def _prepare(self) -> None:
        """Is called after endpoint creation. Various endpoint-related
        checks may be implemented here.
        """

    @staticmethod
    def _build_snapshot_cmd(source, destination, readonly=True):
        """Should return a command which, when executed, creates a
        snapshot of ``source`` at ``destination``. If ``readonly`` is set,
        the snapshot should be read only.
        """
        cmd = ["btrfs", "subvolume", "snapshot"]
        if readonly:
            cmd += ["-r"]
        cmd += [str(source), str(destination)]
        logger.debug("Snapshot command: %s", cmd)
        return cmd

    @staticmethod
    def _build_sync_command():
        """Should return the 'sync' command."""
        return ["sync"]

    def _build_send_command(self, snapshot, parent=None, clones=None):
        """Should return a command which, when executed, writes the send
        stream of given ``snapshot`` to stdout. ``parent`` and ``clones``
        may be used as well.
        """
        cmd = ["btrfs", "send", *self.btrfs_flags]
        # from WARNING level onwards, pass --quiet
        log_level = logging.getLogger().getEffectiveLevel()
        if log_level >= logging.WARNING:
            cmd += ["--quiet"]
        if parent:
            cmd += ["-p", str(parent.get_path())]
        if clones:
            for clone in clones:
                cmd += [str(clone.get_path())]
        cmd += [str(snapshot.get_path())]
        return cmd

    def _build_receive_command(self, destination):
        """Should return a command to receive a snapshot to ``dest``.
        The stream is piped into stdin when the command is running.
        """
        return ["btrfs", "receive", *self.btrfs_flags, str(destination)]

    def _build_deletion_commands(self, snapshots, convert_rw=None, subvolume_sync=None):
        """Should return a list of commands that, when executed in order,
        delete the given ``snapshots``. ``convert_rw`` and
        ``subvolume_sync`` should be regarded as well.
        """
        if convert_rw is None:
            convert_rw = self.config["convert_rw"]
        if subvolume_sync is None:
            subvolume_sync = self.config["subvolume_sync"]

        commands = []

        if convert_rw:
            commands.extend(
                [
                    "btrfs",
                    "property",
                    "set",
                    "-ts",
                    str(snapshot.get_path()),
                    "ro",
                    "false",
                ]
                for snapshot in snapshots
            )

        cmd = ["btrfs", "subvolume", "delete"]
        cmd.extend([str(snapshot.get_path()) for snapshot in snapshots])
        commands.append(cmd)

        if subvolume_sync:
            commands.append(["btrfs", "subvolume", "sync", str(self.config["path"])])

        return commands

    # pylint: disable=unused-argument
    def _collapse_commands(self, commands, abort_on_failure=True):
        """This might be re-implemented to group commands together wherever
        possible. The default implementation simply returns the given command
        list unchanged.
        If ``abort_on_failure`` is set, the implementation must assure that
        every collapsed command in the returned list aborts immediately
        after one of the original commands included in it fail. If it is
        unset, the opposite behaviour is expected (subsequent commands have
        to be run even in case a previous one fails).
        """
        return commands

    def _exec_command(self, options, **kwargs):
        """
        Execute a command using __util__.exec_subprocess, with options dict.
        options must contain at least 'command': a list of command arguments.
        """
        command = options.get("command")
        if not command:
            raise ValueError("No command specified in options for _exec_command")

        lock_path = Path("/tmp") / f".btrfs-backup-ng.{getpass.getuser()}.lock"
        lock = FileLock(lock_path)
        with lock:
            if os.geteuid() != 0 and command and command[0] == "btrfs":
                if options.get("no_password_sudo"):
                    command = ["sudo", "-n"] + command
                else:
                    command = ["sudo"] + command
            return __util__.exec_subprocess(command, **kwargs)

    def _listdir(self, location):
        location = Path(location).resolve()
        if not location.exists():
            return []
        return [str(item) for item in location.iterdir()]

    def _remount(self, path, read_write=True):
        """Remounts the given path as read-write or read-only."""
        logger.debug("Remounting %s as read-write: %r", path, read_write)
        if read_write:
            mode = "rw"
        else:
            mode = "ro"

        # Check if already mounted with the desired mode
        try:
            output = subprocess.check_output(["mount"], text=True).splitlines()
            for line in output:
                if str(path) in line and f"(flags:{mode})" in line:
                    logger.debug("%s already mounted as %s", path, mode)
                    return  # Already mounted with the correct mode
        except subprocess.CalledProcessError as e:
            logger.error("Failed to check mount status %r", e)
            raise __util__.AbortError from e

        cmd = ["mount", "-o", f"remount,{mode}", str(path)]
        if os.geteuid() != 0:
            cmd = ["sudo"] + cmd
        logger.debug("Executing remount command: %s", cmd)
        try:
            env = os.environ.copy()
            logger.debug("Environment variables: %s", env)
            subprocess.check_call(cmd, env=env)
        except subprocess.CalledProcessError as e:
            logger.error(
                "Failed to remount %s as %s: %r %r %r",
                path,
                mode,
                e.returncode,
                e.stderr,
                e.stdout,
            )
            raise __util__.AbortError from e

    @require_source
    def _get_lock_file_path(self):
        """Is used by the default ``_read/write_locks`` methods and should
        return the file in which the locks are stored.
        """
        if self.config["path"] is None:
            raise ValueError
        return self.config["path"] / str(self.config["lock_file_name"])

    @require_source
    def _read_locks(self):
        """Should read the locks and return a dict like
        ``__util__.read_locks`` returns it.
        """
        path = self._get_lock_file_path()
        try:
            if not path.is_file():
                return {}
            with open(path, encoding="utf-8") as f:
                return __util__.read_locks(f.read())
        except (OSError, ValueError) as e:
            logger.error("Error on reading lock file %s: %s", path, e)
            raise __util__.AbortError

    @require_source
    def _write_locks(self, lock_dict) -> None:
        """Should write the locks given as ``lock_dict`` like
        ``__util__.read_locks`` returns it.
        """
        path = self._get_lock_file_path()
        try:
            logger.debug("Writing lock file: %s", path)
            with open(path, "w", encoding="utf-8") as f:
                f.write(__util__.write_locks(lock_dict))
        except OSError as e:
            logger.error("Error on writing lock file %s: %s", path, e)
            raise __util__.AbortError
