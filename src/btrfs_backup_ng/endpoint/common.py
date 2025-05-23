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
from typing import Any, Optional, List, Dict

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

    def __init__(self, config: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
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

    def _normalize_path(self, val: Any) -> Any:
        if val is None:
            return None

        # Import logger here to avoid circular imports
        from btrfs_backup_ng.__logger__ import logger

        logger.debug("Normalizing path: %s (type: %s)", val, type(val).__name__)

        # Handle string paths
        if isinstance(val, str):
            # Just expanduser for remote paths to avoid resolving them locally
            if hasattr(self, "_is_remote") and getattr(self, "_is_remote", False):
                expanded = str(Path(val).expanduser())
                logger.debug("Remote path expanded: %s -> %s", val, expanded)
                return expanded

            # For local paths, handle carefully
            try:
                path = Path(val).expanduser()
                logger.debug("Local path expanded: %s -> %s", val, path)
                # If path is absolute, no need to resolve
                if path.is_absolute():
                    logger.debug("Using absolute path as-is: %s", path)
                    return path

                # Safely resolve relative path
                try:
                    resolved = path.resolve()
                    logger.debug("Resolved relative path: %s -> %s", path, resolved)
                    return resolved
                except (FileNotFoundError, PermissionError) as e:
                    # If resolving fails, manually make it absolute with cwd
                    logger.warning(f"Path resolution failed for {path}: {e}")
                    cwd_path = Path(os.getcwd()) / path
                    logger.debug(
                        "Manually made absolute with cwd: %s -> %s", path, cwd_path
                    )
                    return cwd_path
            except Exception as e:
                logger.error(f"Path handling error: {e}")
                # Return original string if all else fails
                logger.debug("Returning original string due to error: %s", val)
                return val

        # If it's already a Path object
        if isinstance(val, Path):
            # For remote paths, convert to string to avoid resolution issues
            if hasattr(self, "_is_remote") and getattr(self, "_is_remote", False):
                return str(val)

            # For local paths, handle safely
            if val.is_absolute():
                return val

            try:
                return val.resolve()
            except (FileNotFoundError, PermissionError) as e:
                # If resolving fails, manually make it absolute
                logger.warning(f"Path resolution failed for {val}: {e}")
                return Path(os.getcwd()) / val

        # For other types, just convert to string
        return str(val)

    def prepare(self) -> None:
        """Public access to _prepare, which is called after creating an endpoint."""
        logger.info("Preparing endpoint %r ...", self)
        return self._prepare()

    @require_source
    def snapshot(self, readonly: bool = True, sync: bool = True) -> Any:
        """Take a snapshot and return the created object."""
        base_path = Path(self.config["source"]).resolve()
        snapshot_dir = (base_path / self.config["snapshot_folder"]).resolve()
        self.config["path"] = snapshot_dir
        snap_prefix = self.config["snap_prefix"]

        snapshot_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        snapshot = __util__.Snapshot(snapshot_dir, snap_prefix, self)
        snapshot_path = snapshot.get_path()
        logger.info(
            "Creating snapshot from source: %s to destination: %s",
            self.config["source"],
            snapshot_path,
        )
        logger.debug("Snapshot directory: %s", snapshot_dir)
        logger.debug("Snapshot prefix: %s", snap_prefix)

        lock_path = snapshot_dir / ".btrfs-backup-ng.snapshot.lock"
        logger.debug("Acquiring snapshot lock: %s", lock_path)
        with FileLock(lock_path):
            logger.debug("Snapshot lock acquired: %s", lock_path)
            self._remount(self.config["source"], read_write=True)
            commands = [
                self._build_snapshot_cmd(
                    self.config["source"], snapshot_path, readonly=readonly
                )
            ]
            if sync:
                commands.append(self._build_sync_command())
            for cmd in self._collapse_commands(commands):
                logger.debug("Executing snapshot command: %s", cmd)
                self._exec_command({"command": cmd})
                logger.debug("Snapshot command executed successfully: %s", cmd)
                self.add_snapshot(snapshot)
        return snapshot

    @require_source
    def send(
        self, snapshot: Any, parent: Any = None, clones: Optional[List[Any]] = None
    ) -> Any:
        """Call 'btrfs send' for the given snapshot and return its Popen object."""
        cmd = self._build_send_command(snapshot, parent=parent, clones=clones)
        return self._exec_command(
            {"command": cmd}, method="Popen", stdout=subprocess.PIPE
        )

    def receive(self, stdin: Any) -> Any:
        """Call 'btrfs receive', setting the given pipe as its stdin."""
        # Make sure we use the raw path without local resolution
        path = self.config["path"]
        # Ensure path is properly normalized for this endpoint type
        normalized_path = self._normalize_path(path)
        logger.debug(
            "Receive path: %s (type: %s)",
            normalized_path,
            type(normalized_path).__name__,
        )

        # Log more details for debugging
        logger.debug("Receive endpoint type: %s", type(self).__name__)
        logger.debug("Is remote endpoint: %s", getattr(self, "_is_remote", False))

        # Verify path exists or create it
        try:
            if isinstance(normalized_path, (str, Path)) and not getattr(
                self, "_is_remote", False
            ):
                path_obj = (
                    Path(normalized_path)
                    if isinstance(normalized_path, str)
                    else normalized_path
                )
                if not path_obj.exists():
                    logger.warning(
                        "Destination path doesn't exist, creating it: %s", path_obj
                    )
                    path_obj.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(
                "Error verifying or creating path %s: %s", normalized_path, e
            )

        cmd = self._build_receive_command(normalized_path)
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None

        logger.debug("Running receive command: %s", cmd)
        try:
            return self._exec_command(
                {"command": cmd}, method="Popen", stdin=stdin, stdout=stdout
            )
        except Exception as e:
            logger.error("Error executing receive command: %s", e)
            raise

    def list_snapshots(self, flush_cache: bool = False) -> List[Any]:
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
    def set_lock(
        self, snapshot: Any, lock_id: Any, lock_state: bool, parent: bool = False
    ) -> None:
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

    def add_snapshot(self, snapshot: Any, rewrite: bool = True) -> None:
        """Add a snapshot to the cache."""
        if self.__cached_snapshots is None:
            return
        if rewrite:
            snapshot = __util__.Snapshot(
                self.config["path"], snapshot.prefix, self, time_obj=snapshot.time_obj
            )
        self.__cached_snapshots.append(snapshot)
        self.__cached_snapshots.sort()

    def delete_snapshots(self, snapshots: List[Any], **kwargs: Any) -> None:
        """Delete the given snapshots (subvolumes)."""
        for snapshot in snapshots:
            if snapshot.locks or snapshot.parent_locks:
                logger.info("Skipping locked snapshot: %s", snapshot)
                continue
            cmd = [
                ("btrfs", False),
                ("subvolume", False),
                ("delete", False),
                (str(snapshot.get_path()), True),
            ]
            logger.debug(
                "Executing deletion command: %s",
                [(arg, is_path) for arg, is_path in cmd],
            )
            try:
                logger.debug("Deleting snapshot with path: %s", snapshot.get_path())
                self._exec_command({"command": cmd})
                logger.info("Deleted snapshot subvolume: %s", snapshot.get_path())
            except Exception as e:
                logger.error("Failed to delete snapshot %s: %s", snapshot.get_path(), e)
                logger.error(
                    "Deletion command was: %s", [(arg, is_path) for arg, is_path in cmd]
                )
            if self.__cached_snapshots is not None:
                with contextlib.suppress(ValueError):
                    self.__cached_snapshots.remove(snapshot)

    def delete_snapshot(self, snapshot: Any, **kwargs: Any) -> None:
        """Delete a snapshot."""
        self.delete_snapshots([snapshot], **kwargs)

    def delete_old_snapshots(self, keep: int) -> None:
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
        # Ensure path is normalized to string for consistent IDs
        path = self._normalize_path(self.config["path"])
        return f"unknown://{path}"

    def _prepare(self) -> None:
        """Called after endpoint creation for additional checks."""
        pass

    @staticmethod
    def _build_snapshot_cmd(
        source: Any, destination: Any, readonly: bool = True
    ) -> List[Any]:
        # Use tuples to mark command arguments that shouldn't be normalized as paths
        cmd = [("btrfs", False), ("subvolume", False), ("snapshot", False)]
        if readonly:
            cmd += [("-r", False)]
        cmd += [(str(source), True), (str(destination), True)]
        logger.debug("Snapshot command: %s", [arg for arg, _ in cmd])
        return cmd

    @staticmethod
    def _build_sync_command() -> List[Any]:
        return [("sync", False)]

    def _build_send_command(
        self, snapshot: Any, parent: Any = None, clones: Optional[List[Any]] = None
    ) -> List[Any]:
        # Use tuples to mark command arguments that shouldn't be normalized as paths
        cmd = [("btrfs", False), ("send", False)]
        # Add btrfs flags
        for flag in self.btrfs_flags:
            cmd.append((flag, False))

        log_level = logging.getLogger().getEffectiveLevel()
        if log_level >= logging.WARNING:
            cmd.append(("--quiet", False))
        if parent:
            cmd.append(("-p", False))
            cmd.append((str(parent.get_path()), True))
            logger.debug("Using parent for send: %s", parent.get_path())
        if clones:
            for clone in clones:
                cmd.append((str(clone.get_path()), True))
                logger.debug("Added clone for send: %s", clone.get_path())
        cmd.append((str(snapshot.get_path()), True))
        logger.debug("Built send command: %s", [(a, p) for a, p in cmd])
        return cmd

    def _build_receive_command(self, destination: Any) -> List[Any]:
        # Ensure destination is properly formatted as a string without resolving
        # This avoids path resolution that could break remote paths
        dest_str = str(destination)
        logger.debug("Building receive command with destination: %s", dest_str)

        # Add more debug info about destination
        if isinstance(destination, Path):
            logger.debug("Destination is a Path object, converting to string")

        # Use tuples to mark command arguments that shouldn't be normalized as paths
        cmd = [("btrfs", False), ("receive", False)]
        for flag in self.btrfs_flags:
            cmd.append((flag, False))
        cmd.append((dest_str, True))
        logger.debug("Receive command: %s", [arg for arg, _ in cmd])
        return cmd

    def _build_deletion_commands(
        self,
        snapshots: List[Any],
        convert_rw: Optional[bool] = None,
        subvolume_sync: Optional[bool] = None,
    ) -> List[Any]:
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
            for snapshot in snapshots:
                # Use tuples to mark command arguments that shouldn't be normalized as paths
                commands.append(
                    [
                        ("btrfs", False),
                        ("property", False),
                        ("set", False),
                        ("-ts", False),
                        (str(snapshot.get_path()), True),
                        ("ro", False),
                        ("false", False),
                    ]
                )
        for snapshot in snapshots:
            commands.append(
                [
                    ("btrfs", False),
                    ("subvolume", False),
                    ("delete", False),
                    (str(snapshot.get_path()), True),
                ]
            )
        if subvolume_sync:
            commands.append(
                [
                    ("btrfs", False),
                    ("subvolume", False),
                    ("sync", False),
                    (str(self.config["path"]), True),
                ]
            )
        return commands

    def _collapse_commands(
        self, commands: List[Any], abort_on_failure: bool = True
    ) -> List[Any]:
        return commands

    def _exec_command(self, options: Dict[str, Any], **kwargs: Any) -> Any:
        command = options.get("command")
        if not command:
            raise ValueError("No command specified in options for _exec_command")

        # Process command based on whether arguments are marked as paths or not
        try:
            normalized_command = []
            logger.debug("Original command to normalize: %s", command)

            # Check if command is using the tuple format (arg, is_path)
            if command and isinstance(command[0], tuple) and len(command[0]) == 2:
                # New format with (arg, is_path) tuples
                logger.debug("Using tuple format for command normalization")
                for i, (arg, is_path) in enumerate(command):
                    logger.debug(
                        "Processing arg %d: %s (is_path=%s, type=%s)",
                        i,
                        arg,
                        is_path,
                        type(arg).__name__,
                    )
                    if is_path and isinstance(arg, (str, Path)):
                        try:
                            normalized_arg = self._normalize_path(arg)
                            logger.debug(
                                "Normalized path arg %s to: %s", arg, normalized_arg
                            )
                            normalized_command.append(normalized_arg)
                        except Exception as e:
                            logger.warning(
                                "Path normalization failed for %s: %s", arg, e
                            )
                            # Use original argument if normalization fails
                            normalized_command.append(arg)
                    else:
                        # Not a path, just append as-is
                        logger.debug("Using non-path arg as-is: %s", arg)
                        normalized_command.append(arg)
                logger.debug(
                    "Processed marked command arguments: %s", normalized_command
                )
            else:
                # Legacy format - attempt to guess which args are paths
                logger.debug("Using legacy format for command normalization")
                for i, arg in enumerate(command):
                    if isinstance(arg, (str, Path)):
                        # First argument is a command - don't normalize it as a path
                        if i == 0 or (isinstance(arg, str) and arg.startswith("-")):
                            normalized_command.append(arg)
                            logger.debug("Not normalizing argument %d: %s", i, arg)
                        else:
                            try:
                                normalized_arg = self._normalize_path(arg)
                                logger.debug(
                                    "Normalized path arg %d %s to: %s",
                                    i,
                                    arg,
                                    normalized_arg,
                                )
                                normalized_command.append(normalized_arg)
                            except Exception as e:
                                logger.warning(
                                    "Path normalization failed for %s: %s", arg, e
                                )
                                # Use original argument if normalization fails
                                normalized_command.append(arg)
                    else:
                        logger.debug("Keeping non-string/path arg %d as-is: %s", i, arg)
                        normalized_command.append(arg)
                logger.debug("Processed legacy command format: %s", normalized_command)

            command = normalized_command
        except Exception as e:
            logger.error("Error normalizing command arguments: %s", e)
            # Continue with original command if normalization completely fails
            logger.warning("Using original command without normalization")

        # Convert all command arguments to strings for subprocess
        command = [str(arg) for arg in command]
        logger.debug("Executing command: %s", command)
        lock_path = Path("/tmp") / f".btrfs-backup-ng.{getpass.getuser()}.lock"

        try:
            with FileLock(lock_path):
                # Convert command to string for proper detection
                first_cmd = str(command[0]) if command else ""

                if os.geteuid() != 0 and command and first_cmd == "btrfs":
                    # Find the full path to btrfs command if needed
                    if first_cmd == "btrfs" and "/" not in first_cmd:
                        # This preserves just using "btrfs" which will use PATH
                        pass

                    if options.get("no_password_sudo"):
                        command = ["sudo", "-n"] + command
                    else:
                        command = ["sudo"] + command

                # Ensure all command arguments are strings
                command = [str(arg) for arg in command]
                logger.debug("Final command after sudo adjustment: %s", command)
                # Log the command with file paths, useful for debugging
                cwd = os.getcwd()
                logger.debug("Current working directory: %s", cwd)
                logger.debug("Executing command with absolute paths:")
                for i, arg in enumerate(command):
                    logger.debug("  Arg %d: %s", i, arg)
                # Make sure the environment includes the PATH
                env = kwargs.get("env", os.environ.copy())
                logger.debug("Environment PATH: %s", env.get("PATH", "Not set"))
                kwargs["env"] = env
                return __util__.exec_subprocess(command, **kwargs)
        except Exception as e:
            logger.error("Error in _exec_command: %s", e)
            raise

    def _listdir(self, location: Any) -> List[str]:
        # For remote endpoints, don't try to resolve the path locally
        if hasattr(self, "_is_remote") and getattr(self, "_is_remote", False):
            # Remote endpoints should implement their own _listdir
            logger.debug(
                "Using default _listdir implementation on remote path: %s", location
            )
            location = Path(location)
        else:
            location = Path(location).resolve()

        if not location.exists():
            logger.debug("Path does not exist for _listdir: %s", location)
            return []

        logger.debug("Listing directory contents: %s", location)
        return [str(item) for item in location.iterdir()]

    def _remount(self, path: Any, read_write: bool = True) -> None:
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
    def _get_lock_file_path(self) -> Path:
        if self.config["path"] is None:
            raise ValueError
        return self.config["path"] / str(self.config["lock_file_name"])

    @require_source
    def _read_locks(self) -> Dict[str, Any]:
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
    def _write_locks(self, lock_dict: Dict[str, Any]) -> None:
        path = self._get_lock_file_path()
        try:
            logger.debug("Writing lock file: %s", path)
            with open(path, "w", encoding="utf-8") as f:
                f.write(__util__.write_locks(lock_dict))
        except OSError as e:
            logger.error("Error on writing lock file %s: %s", path, e)
            raise __util__.AbortError
