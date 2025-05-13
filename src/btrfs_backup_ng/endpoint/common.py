# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/endpoint/common.py
Common functionality among modules.
"""

import contextlib
import logging
import subprocess
from pathlib import Path

from btrfs_backup_ng import __util__
from btrfs_backup_ng.__logger__ import logger


def require_source(method):
    """Decorator that ensures source is set on the object the called method belongs to."""

    def wrapped(self, *args, **kwargs):
        if self.source is None:
            msg = "source hasn't been set"
            raise ValueError(msg)
        return method(self, *args, **kwargs)

    return wrapped


class Endpoint:
    """Generic structure of a command endpoint."""

    # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        path=None,
        snap_prefix="",
        convert_rw=False,
        subvolume_sync=False,
        btrfs_debug=False,
        source=None,
        fs_checks=True,
        **kwargs,
    ) -> None:
        # pylint: disable=too-many-arguments
        # pylint: disable=too-many-positional-arguments
        self.path = Path(path) if path else None
        self.snap_prefix = snap_prefix
        self.convert_rw = convert_rw
        self.subvolume_sync = subvolume_sync
        self.btrfs_debug = btrfs_debug
        self.btrfs_flags = []
        if self.btrfs_debug:
            self.btrfs_flags += ["-vv"]
        self.source = Path(source) if source else None
        self.fs_checks = fs_checks
        self.lock_file_name = ".outstanding_transfers"
        self.__cached_snapshots = None
        kwargs = kwargs if kwargs else {}

    def prepare(self):
        """Public access to _prepare, which is called after creating an endpoint."""
        logger.info("Preparing endpoint %r ...", self)
        return self._prepare()

    @require_source
    def snapshot(self, readonly=True, sync=True):
        """Takes a snapshot and returns the created object."""
        snapshot = __util__.Snapshot(self.path, self.snap_prefix, self)
        snapshot_path = snapshot.get_path()
        logger.info("%s -> %s", self.source, snapshot_path)

        commands = [
            self._build_snapshot_cmd(self.source, snapshot_path, readonly=readonly),
        ]

        # sync disks
        if sync:
            commands.append(self._build_sync_command())

        for cmd in self._collapse_commands(commands, abort_on_failure=True):
            self._exec_command(cmd)

        self.add_snapshot(snapshot)
        return snapshot

    @require_source
    def send(self, snapshot, parent=None, clones=None):
        """Calls 'btrfs send' for the given snapshot and returns its
        Popen object.
        """
        cmd = self._build_send_command(snapshot, parent=parent, clones=clones)
        return self._exec_command(cmd, method="Popen", stdout=subprocess.PIPE)

    def receive(self, stdin):
        """Calls 'btrfs receive', setting the given pipe as its stdin.
        The receiving process's Popen object is returned.
        """
        cmd = self._build_receive_command(self.path)
        # from WARNING level onwards, hide stdout
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        return self._exec_command(cmd, method="Popen", stdin=stdin, stdout=stdout)

    def list_snapshots(self, flush_cache=False):
        """Returns a list with all snapshots found at ``self.path``.
        If ``flush_cache`` is not set, cached results will be used
        if available.
        """
        if self.__cached_snapshots is not None and not flush_cache:
            logger.debug(
                "Returning %d cached snapshots for %r.",
                len(self.__cached_snapshots),
                self,
            )
            return list(self.__cached_snapshots)

        logger.debug("Building snapshot cache of %r ...", self)
        snapshots = []
        listdir = self._listdir(self.path)
        for item in listdir:
            if item.startswith(self.snap_prefix):
                time_str = item[len(self.snap_prefix) :]
                try:
                    time_obj = __util__.str_to_date(time_str)
                except ValueError:
                    # no valid name for current prefix + time string
                    continue
                else:
                    snapshot = __util__.Snapshot(
                        self.path,
                        self.snap_prefix,
                        self,
                        time_obj=time_obj,
                    )
                    snapshots.append(snapshot)

        # apply locks
        if self.source:
            lock_dict = self._read_locks()
            for snapshot in snapshots:
                snap_entry = lock_dict.get(snapshot.get_name(), {})
                for lock_type, locks in snap_entry.items():
                    getattr(snapshot, lock_type).update(locks)

        # sort by date, then time;
        snapshots.sort()

        # populate cache
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
                self.path,
                snapshot.prefix,
                self,
                time_obj=snapshot.time_obj,
            )

        self.__cached_snapshots.append(snapshot)
        self.__cached_snapshots.sort()

        return

    def delete_snapshots(self, snapshots, **kwargs) -> None:
        """Deletes the given snapshots, passing all keyword arguments to
        ``_build_deletion_cmds``.
        """
        # only remove snapshots that have no lock remaining
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

        if to_remove:
            # finally delete them
            cmds = self._build_deletion_commands(to_remove, **kwargs)
            cmds = self._collapse_commands(cmds, abort_on_failure=True)
            for cmd in cmds:
                self._exec_command(cmd)

            if self.__cached_snapshots is not None:
                for snapshot in to_remove:
                    with contextlib.suppress(ValueError):
                        self.__cached_snapshots.remove(snapshot)

    def delete_snapshot(self, snapshot, **kwargs) -> None:
        """Delete a snapshot."""
        self.delete_snapshots([snapshot], **kwargs)

    def delete_old_snapshots(self, keep_num, **kwargs) -> None:
        """Delete all but the value in keep_num newest snapshots at endpoints."""
        snapshots = self.list_snapshots()

        if len(snapshots) > keep_num:
            # delete oldest snapshots
            to_remove = snapshots[:-keep_num]
            self.delete_snapshots(to_remove, **kwargs)

    # The following methods may be implemented by endpoints unless the
    # default behaviour is wanted.

    def __repr__(self) -> str:
        return f"{self.path}"

    def get_id(self) -> str:
        """Return an id string to identify this endpoint over multiple runs."""
        return f"unknown://{self.path}"

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
            convert_rw = self.convert_rw
        if subvolume_sync is None:
            subvolume_sync = self.subvolume_sync

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
            commands.append(["btrfs", "subvolume", "sync", str(self.path)])

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

    def _exec_command(self, command, **kwargs):
        """Finally, the command should be executed via
        ``__util__.exec_subprocess``, which should get all given keyword
        arguments. This could be re-implemented to execute via SSH,
        for instance.
        """
        return __util__.exec_subprocess(command, **kwargs)

    def _listdir(self, location):
        """Should return all items present at the given ``location``."""
        return [str(item) for item in location.iterdir()]

    @require_source
    def _get_lock_file_path(self):
        """Is used by the default ``_read/write_locks`` methods and should
        return the file in which the locks are stored.
        """
        if self.path is None:
            raise ValueError
        return self.path / self.lock_file_name

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
