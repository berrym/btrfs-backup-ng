# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/__util__.py
Common utility code shared between modules.
"""

import functools
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

from .__logger__ import logger

DATE_FORMAT = "%Y%m%d-%H%M%S"
MOUNTS_FILE = "/proc/mounts"


class AbortError(Exception):
    """Exception where btrfs-backup-ng should abort."""


class SnapshotTransferError(AbortError):
    """Error when transferring a snapshot."""


@functools.total_ordering
class Snapshot:
    """Represents a snapshot with comparison by prefix and time_obj."""

    def __init__(self, location, prefix, endpoint, time_obj=None) -> None:
        self.location = Path(location)
        self.prefix = prefix
        self.endpoint = endpoint
        if time_obj is None:
            time_obj = str_to_date()
        self.time_obj = time_obj
        self.locks = set()
        self.parent_locks = set()

    def __eq__(self, other):
        return self.prefix == other.prefix and self.time_obj == other.time_obj

    def __lt__(self, other):
        if self.prefix != other.prefix:
            msg = f"prefixes don't match: {self.prefix} vs {other.prefix}"
            raise NotImplementedError(
                msg,
            )
        return self.time_obj < other.time_obj

    def __repr__(self) -> str:
        return self.get_name()

    def get_name(self):
        """Return a snapshot's name."""
        return self.prefix + date_to_str(self.time_obj)

    def get_path(self):
        """Return full path to a snapshot."""
        return self.location / self.get_name()

    def find_parent(self, present_snapshots):
        """Returns object from ``present_snapshot`` most suitable for being
        used as a parent for transferring this one or ``None``,
        if none found.
        """
        if self in present_snapshots:
            # snapshot already transferred
            return None
        for present_snapshot in reversed(present_snapshots):
            if present_snapshot < self:
                return present_snapshot
        # no snapshot older than snapshot is present ...
        if present_snapshots:
            # ... hence we choose the oldest one present as parent
            return present_snapshots[0]

        return None


def exec_subprocess(command, method="check_output", **kwargs):
    """Executes ``getattr(subprocess, method)(cmd, **kwargs)`` and takes
    care of proper logging and error handling. ``AbortError`` is raised
    in case of a ``subprocess.CalledProcessError``.
    """
    logger.debug("Executing: %s", command)
    m = getattr(subprocess, method)

    # Ensure environment is set up correctly
    if "env" not in kwargs:
        kwargs["env"] = os.environ.copy()

    # Ensure all command arguments are strings
    command = [str(arg) for arg in command]

    try:
        return m(command, **kwargs)
    except FileNotFoundError as e:
        # Handle case where command is not found
        logger.error("Command not found: %s", command[0])
        logger.error("PATH: %s", kwargs["env"].get("PATH", "Not set"))
        logger.error("Working directory: %s", os.getcwd())

        # Try to locate the command in the system path
        if command and "/" not in command[0]:
            logger.info("Attempting to find command '%s' in PATH", command[0])
            try:
                # Try to find the executable in PATH with 'which' command
                which_result = subprocess.run(
                    ["which", command[0]], capture_output=True, text=True, check=False
                )
                if which_result.returncode == 0:
                    full_path = which_result.stdout.strip()
                    logger.info("Found command at: %s", full_path)
                    # Replace command with full path and retry
                    command[0] = full_path
                    logger.info("Retrying with full path: %s", command)
                    return m(command, **kwargs)
                else:
                    logger.error("Command '%s' not found in PATH", command[0])
            except Exception as find_e:
                logger.error("Error finding command: %s", find_e)

        # If all else fails, raise the original error
        logger.error("Cannot execute command: %s", e)
        raise AbortError(f"Command not found: {command[0]}") from e
    except subprocess.CalledProcessError as e:
        logger.error("Error on command: %s\nCaught: %s", command, e)
        raise AbortError from e
    except Exception as e:
        logger.error("Unexpected error executing command: %s\nError: %s", command, e)
        raise AbortError(f"Error executing {command[0]}: {e}") from e


def log_heading(caption) -> str:
    """Formatted heading for logging output sections."""
    return f"{f'--[ {caption} ]':-<50}"


def date_to_str(timestamp=None, fmt=None):
    """Convert date format to string."""
    if timestamp is None:
        timestamp = time.localtime()
    if fmt is None:
        fmt = DATE_FORMAT
    return time.strftime(fmt, timestamp)


def str_to_date(time_string=None, fmt=None):
    """Convert date string to date object."""
    if time_string is None:
        # we don't simply return time.localtime() because this would have
        # a higher precision than the result converted from string
        time_string = date_to_str()
    if fmt is None:
        fmt = DATE_FORMAT
    return time.strptime(time_string, fmt)


def is_btrfs(path):
    """Checks whether path is inside a btrfs file system."""
    path = Path(path).resolve()
    logger.debug("Checking for btrfs filesystem: %s", path)
    best_match = ""
    best_match_fs_type = ""
    logger.debug("  Reading mounts file: %s", MOUNTS_FILE)
    with open(MOUNTS_FILE, encoding="utf-8") as f:
        for line in f:
            try:
                mount_point, fs_type = line.split(" ")[1:3]
            except ValueError as e:
                logger.debug("  Couldn't split line, skipping: %s\nCaught: %s", line, e)
                continue
            mount_point_prefix = Path(mount_point)
            if path == mount_point_prefix or path.is_relative_to(mount_point_prefix):
                if len(str(mount_point)) > len(best_match):
                    best_match = mount_point
                    best_match_fs_type = fs_type
                    logger.debug(
                        "  New best_match with filesystem type %s: %s",
                        best_match_fs_type,
                        best_match,
                    )
        result = best_match_fs_type == "btrfs"
        logger.debug(
            "  -> best_match_fs_type is %s, result is %r",
            best_match_fs_type,
            result,
        )
    return result


def is_subvolume(path):
    """Checks whether the given path is a btrfs subvolume."""
    if not is_btrfs(path):
        return False
    logger.debug("Checking for btrfs subvolume: %s", path)
    # subvolumes always have inode 256
    path = Path(path).resolve()
    st = path.stat()
    result = st.st_ino == 256
    logger.debug("  -> Inode is %d, result is %r", st.st_ino, result)
    return result


def read_locks(s):
    """Reads locks from lock file content given as string.
    Returns ``{'snap_name': {'locks': ['lock', ...], ...}, 'parent_locks': ['lock', ...]}``.
    If format is invalid, ``ValueError`` is raised.
    """
    s = s.strip()
    if not s:
        return {}

    try:
        content = json.loads(s)
        assert isinstance(content, dict)
        for snapshot_name, snapshot_entry in content.items():
            assert isinstance(snapshot_name, str)
            assert isinstance(snapshot_entry, dict)
            for lock_type, locks in dict(snapshot_entry).items():
                assert lock_type in {"locks", "parent_locks"}
                assert isinstance(locks, list)
                for lock in locks:
                    assert isinstance(lock, str)
                # eliminate multiple occurrences of locks
                snapshot_entry[lock_type] = list(set(locks))
    except (AssertionError, json.JSONDecodeError) as e:
        logger.error("Lock file couldn't be parsed: %s", e)
        msg = "invalid lock file format"
        raise ValueError(msg) from e

    return content


def write_locks(lock_dict):
    """Converts ``lock_dict`` back to the string readable by ``read_locks``."""
    return json.dumps(lock_dict, indent=4)
