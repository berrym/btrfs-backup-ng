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
    """Decorator to ensure the endpoint has a source set."""

    def wrapped(self, *args, **kwargs):
        if self.config["source"] is None:
            raise ValueError("source hasn't been set")
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

        # Normalize source and path
        self.config["source"] = self._normalize_path(config.get("source"))
        self.config["path"] = self._normalize_path(config.get("path"))
        self.config["snap_prefix"] = config.get("snap_prefix", "")
        self.config["convert_rw"] = config.get("convert_rw", False)
        self.config["subvolume_sync"] = config.get("subvolume_sync", False)
        self.config["btrfs_debug"] = config.get("btrfs_debug", False)
        self.config["fs_checks"] = config.get("fs_checks", False)
        self.config["lock_file_name"] = config.get(
            "lock_file_name", ".btrfs-backup-ng.locks"
        )
        self.config["snapshot_folder"] = config.get(
            "snapshot_folder", ".btrfs-backup-ng/snapshots"
        )

        self.btrfs_flags = ["-vv"] if self.config["btrfs_debug"] else []
        self.__cached_snapshots = None

        for key, value in kwargs.items():
            self.config[key] = value

    def _normalize_path(self, val):
        if val is None:
            return None
        path = Path(val).expanduser()
        return path.resolve() if not path.is_absolute() else path

    def prepare(self):
        """Public access to _prepare, which is called after creating an endpoint."""
        logger.info("Preparing endpoint %r ...", self)
        return self._prepare()

    @require_source
    def snapshot(self, readonly=True, sync=True):
        """Take a snapshot and return the created object."""
        base_path = Path(self.config["source"]).resolve()
        snapshot_dir = (base_path / self.config["snapshot_folder"]).resolve()
        self.config["path"] = snapshot_dir
        snap_prefix = self.config["snap_prefix"]

        snapshot_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        snapshot = __util__.Snapshot(snapshot_dir, snap_prefix, self)
        snapshot_path = snapshot.get_path()
        logger.info("%s -> %s", self.config["source"], snapshot_path)

        lock_path = snapshot_dir / ".btrfs-backup-ng.snapshot.lock"
        with FileLock(lock_path):
            self._remount(self.config["source"], read_write=True)
            commands = [
                self._build_snapshot_cmd(
                    self.config["source"], snapshot_path, readonly=readonly
                )
            ]
            if sync:
                commands.append(self._build_sync_command())
            for cmd in self._collapse_commands(commands):
                self._exec_command({"command": cmd})
                self.add_snapshot(snapshot)
        return snapshot

    @require_source
    def send(self, snapshot, parent=None, clones=None):
        """Call 'btrfs send' for the given snapshot and return its Popen object."""
        cmd = self._build_send_command(snapshot, parent=parent, clones=clones)
        return self._exec_command(
            {"command": cmd}, method="Popen", stdout=subprocess.PIPE
        )

    def receive(self, stdin):
        """Call 'btrfs receive', setting the given pipe as its stdin."""
        cmd = self._build_receive_command(self.config["path"])
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        return self._exec_command(
            {"command": cmd}, method="Popen", stdin=stdin, stdout=stdout
        )

    def list_snapshots(self, flush_cache=False):
        """
        Return a list of all snapshots found directly in self.config['path'] using $snap_prefix.
        Populates a cache for efficient repeated access and removable snapshot checks.
        """
        snapshot_dir = Path(self.config["path"]).resolve()
        snap_prefix = self.config["snap_prefix"]
        snapshot_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

        logger.debug("Listing snapshots in: %s", snapshot_dir)
        logger.debug("Snapshot prefix: %s", snap_prefix)

        # Use or refresh the cache
        if self.__cached_snapshots is not None and not flush_cache:
            logger.debug(
                "Returning %d cached snapshots for %r.",
                len(self.__cached_snapshots),
                self,
            )
            return list(self.__cached_snapshots)

        snapshots = []
        for item in self._listdir(snapshot_dir):
            item_path = Path(item)
            # Only consider items that are direct children of snapshot_dir and match the prefix
            if item_path.parent.resolve() == snapshot_dir and item_path.name.startswith(
                snap_prefix
            ):
                date_part = item_path.name[len(snap_prefix) :]
                logger.debug("Parsing date from: %r", date_part)
                try:
                    time_obj = __util__.str_to_date(date_part)
                except Exception as e:
                    logger.warning(
                        "Could not parse date from: %r (%s)", item_path.name, e
                    )
                    continue
                snapshot = __util__.Snapshot(
                    snapshot_dir, snap_prefix, self, time_obj=time_obj
                )
                snapshots.append(snapshot)
        snapshots.sort()
        self.__cached_snapshots = snapshots
        logger.debug(
            "Populated snapshot cache of %r with %d items.", self, len(snapshots)
        )
        return list(snapshots)

    @require_source
    def set_lock(self, snapshot, lock_id, lock_state, parent=False) -> None:
        """Add or remove the given lock from ``snapshot`` and update the lock file."""
        if lock_state:
            (snapshot.parent_locks if parent else snapshot.locks).add(lock_id)
        else:
            (snapshot.parent_locks if parent else snapshot.locks).discard(lock_id)
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
        """Add a snapshot to the cache."""
        if self.__cached_snapshots is None:
            return
        if rewrite:
            snapshot = __util__.Snapshot(
                self.config["path"], snapshot.prefix, self, time_obj=snapshot.time_obj
            )
        self.__cached_snapshots.append(snapshot)
        self.__cached_snapshots.sort()

    def delete_snapshots(self, snapshots, **kwargs) -> None:
        """Delete the given snapshots (subvolumes)."""
        for snapshot in snapshots:
            if snapshot.locks or snapshot.parent_locks:
                logger.info("Skipping locked snapshot: %s", snapshot)
                continue
            cmd = ["btrfs", "subvolume", "delete", str(snapshot.get_path())]
            try:
                self._exec_command({"command": cmd})
                logger.info("Deleted snapshot subvolume: %s", snapshot.get_path())
            except Exception as e:
                logger.error("Failed to delete snapshot %s: %s", snapshot.get_path(), e)
            if self.__cached_snapshots is not None:
                with contextlib.suppress(ValueError):
                    self.__cached_snapshots.remove(snapshot)

    def delete_snapshot(self, snapshot, **kwargs) -> None:
        """Delete a snapshot."""
        self.delete_snapshots([snapshot], **kwargs)

    def delete_old_snapshots(self, keep):
        """
        Delete old snapshots, keeping only the most recent `keep` unlocked snapshots.
        """
        snapshots = self.list_snapshots()
        unlocked = [s for s in snapshots if not s.locks and not s.parent_locks]
        if keep <= 0 or len(unlocked) <= keep:
            logger.debug(
                "No unlocked snapshots to delete (keep=%d, unlocked=%d)",
                keep,
                len(unlocked),
            )
            return
        to_delete = unlocked[:-keep]
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
        """Called after endpoint creation for additional checks."""
        pass

    @staticmethod
    def _build_snapshot_cmd(source, destination, readonly=True):
        cmd = ["btrfs", "subvolume", "snapshot"]
        if readonly:
            cmd += ["-r"]
        cmd += [str(source), str(destination)]
        logger.debug("Snapshot command: %s", cmd)
        return cmd

    @staticmethod
    def _build_sync_command():
        return ["sync"]

    def _build_send_command(self, snapshot, parent=None, clones=None):
        cmd = ["btrfs", "send", *self.btrfs_flags]
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
        return ["btrfs", "receive", *self.btrfs_flags, str(destination)]

    def _build_deletion_commands(self, snapshots, convert_rw=None, subvolume_sync=None):
        convert_rw = (
            self.config.get("convert_rw", False) if convert_rw is None else convert_rw
        )
        subvolume_sync = (
            self.config.get("subvolume_sync", False)
            if subvolume_sync is None
            else subvolume_sync
        )
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
        cmd = ["btrfs", "subvolume", "delete"] + [
            str(snapshot.get_path()) for snapshot in snapshots
        ]
        commands.append(cmd)
        if subvolume_sync:
            commands.append(["btrfs", "subvolume", "sync", str(self.config["path"])])
        return commands

    def _collapse_commands(self, commands, abort_on_failure=True):
        return commands

    def _exec_command(self, options, **kwargs):
        command = options.get("command")
        if not command:
            raise ValueError("No command specified in options for _exec_command")
        lock_path = Path("/tmp") / f".btrfs-backup-ng.{getpass.getuser()}.lock"
        with FileLock(lock_path):
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
        logger.debug("Remounting %s as read-write: %r", path, read_write)
        mode = "rw" if read_write else "ro"
        try:
            output = subprocess.check_output(["mount"], text=True).splitlines()
            for line in output:
                if str(path) in line and f"(flags:{mode})" in line:
                    logger.debug("%s already mounted as %s", path, mode)
                    return
        except subprocess.CalledProcessError as e:
            logger.error("Failed to check mount status %r", e)
            raise __util__.AbortError from e
        cmd = ["mount", "-o", f"remount,{mode}", str(path)]
        if os.geteuid() != 0:
            cmd = ["sudo"] + cmd
        logger.debug("Executing remount command: %s", cmd)
        try:
            env = os.environ.copy()
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
        if self.config["path"] is None:
            raise ValueError
        return self.config["path"] / str(self.config["lock_file_name"])

    @require_source
    def _read_locks(self):
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
        path = self._get_lock_file_path()
        try:
            logger.debug("Writing lock file: %s", path)
            with open(path, "w", encoding="utf-8") as f:
                f.write(__util__.write_locks(lock_dict))
        except OSError as e:
            logger.error("Error on writing lock file %s: %s", path, e)
            raise __util__.AbortError
