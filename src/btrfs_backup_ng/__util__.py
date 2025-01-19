# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/__util__.py
Common __util__ity code shared between modules.
"""

import argparse
import functools
import json
import os
import subprocess
import sys
import time

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

    def __init__(self, location, prefix, endpoint, time_obj=None):
        self.location = location
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
            raise NotImplementedError(
                f"prefixes don't match: {self.prefix} vs {other.prefix}"
            )
        return self.time_obj < other.time_obj

    def __repr__(self):
        return self.get_name()

    def get_name(self):
        """Return a snapshot's name."""
        return self.prefix + date_to_str(self.time_obj)

    def get_path(self):
        """Return full path to a snapshot."""
        return os.path.join(self.location, self.get_name())

    def find_parent(self, present_snapshots):
        """Returns object from ``present_snapshot`` most suitable for being
        used as a parent for transferring this one or ``None``,
        if none found."""
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
    in case of a ``subprocess.CalledProcessError``."""
    logger.debug("Executing: %s", command)
    m = getattr(subprocess, method)
    try:
        return m(command, **kwargs)
    except subprocess.CalledProcessError as e:
        logger.error("Error on command: %s\nCaught: %s", command, e)
        raise AbortError() from e


def log_heading(caption):
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
    """Checks whether path is inside a btrfs file system"""
    path = os.path.normpath(os.path.abspath(path))
    logger.debug("Checking for btrfs filesystem: %s", path)
    best_match = ""
    best_match_fs_type = ""
    logger.debug("  Reading mounts file: %s", MOUNTS_FILE)
    for line in open(MOUNTS_FILE, encoding="utf-8"):
        try:
            mount_point, fs_type = line.split(" ")[1:3]
        except ValueError as e:
            logger.debug("  Couldn't split line, skipping: %s\nCaught: %s", line, e)
            continue
        mount_point_prefix = mount_point
        if not mount_point_prefix.endswith(os.sep):
            mount_point_prefix += os.sep
        if (path == mount_point or path.startswith(mount_point_prefix)) and len(
            mount_point
        ) > len(best_match):
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
    st = os.stat(path)
    result = st.st_ino == 256
    logger.debug("  -> Inode is %d, result is %r", st.st_ino, result)
    return result


def read_locks(s):
    """Reads locks from lock file content given as string.
    Returns ``{'snap_name': {'locks': ['lock', ...], ...}, 'parent_locks': ['lock', ...]}``.
    If format is invalid, ``ValueError`` is raised."""
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
                assert lock_type in ("locks", "parent_locks")
                assert isinstance(locks, list)
                for lock in locks:
                    assert isinstance(lock, str)
                # eliminate multiple occurrences of locks
                snapshot_entry[lock_type] = list(set(locks))
    except (AssertionError, json.JSONDecodeError) as e:
        logger.error("Lock file couldn't be parsed: %s", e)
        raise ValueError("invalid lock file format") from e

    return content


def write_locks(lock_dict):
    """Converts ``lock_dict`` back to the string readable by ``read_locks``."""
    return json.dumps(lock_dict, indent=4)


# argparse related classes


class MyArgumentParser(argparse.ArgumentParser):
    """Custom parser that allows for comments in argument files."""

    def _read_args_from_files(self, arg_strings):
        """Overloaded to make nested imports relative to their parents."""
        # expand arguments referencing files
        new_arg_strings = []
        for arg_string in arg_strings:
            # for regular arguments, just add them back into the list
            if (
                not arg_string
                or self.fromfile_prefix_chars is not None
                and arg_string[0] not in self.fromfile_prefix_chars
            ):
                new_arg_strings.append(arg_string)
            # replace arguments referencing files with the file content
            else:
                arg_strings = []
                try:
                    with open(arg_string[1:], encoding="utf-8") as args_file:
                        for arg_line in args_file.read().splitlines():
                            for arg in self.convert_arg_line_to_args(arg_line):
                                # make nested includes relative to their parent
                                if (
                                    self.fromfile_prefix_chars is not None
                                    and arg.startswith(self.fromfile_prefix_chars)
                                ):
                                    dir_name = os.path.dirname(arg_string[1:])
                                    path = os.path.join(dir_name, arg[1:])
                                    # eliminate ../foo/../foo constructs
                                    path = os.path.normpath(path)
                                    arg = arg[0] + path
                                arg_strings.append(arg)
                except OSError:
                    err = sys.exc_info()[1]
                    self.error(str(err))
                arg_strings = self._read_args_from_files(arg_strings)
                new_arg_strings.extend(arg_strings)

        # return the modified argument list
        return new_arg_strings

    def convert_arg_line_to_args(self, arg_line):
        stripped = arg_line.strip()
        # ignore blank lines and comments
        if not stripped or stripped.startswith("#"):
            return []
        if stripped.startswith(tuple(self.prefix_chars)):
            # split at first whitespace/tab, empty strings are removed
            # e.g. "-a    b c" -> ["-a", "b c"]
            return stripped.split(None, 1)
        # must be a positional argument which shouldn't be split
        return [stripped]


class MyHelpFormatter(argparse.HelpFormatter):
    """Custom formatter that keeps explicit line breaks in help texts
    if the text starts with 'N|'. That special prefix is removed anyway."""

    def _split_lines(self, text, width):
        if text.startswith("N|"):
            _lines = text[2:].splitlines()
        else:
            _lines = [text]
        lines = []
        for line in _lines:
            # this is the RawTextHelpFormatter._split_lines
            lines.extend(argparse.HelpFormatter._split_lines(self, line, width))
        return lines
