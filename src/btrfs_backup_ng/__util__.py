"""btrfs-backup-ng: btrfs_backup_ng/__util__.py
Common utility code shared between modules.
"""

import contextlib
import fcntl
import functools
import json
import errno
import os
import stat as stat_module
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

from .__logger__ import logger

__all__ = [
    "AbortError",
    "SnapshotTransferError",
    "InsufficientSpaceError",
    "Snapshot",
    "exec_subprocess",
    "log_heading",
    "date_to_str",
    "str_to_date",
    "is_btrfs",
    "is_subvolume",
    "is_mounted",
    "get_mount_info",
    "read_locks",
    "write_locks",
    "atomic_write_bytes",
    "delete_subvolume",
    "DATE_FORMAT",
    "MOUNTS_FILE",
    "infer_snapshot_prefix",
    "parse_snapshot_time",
]

DATE_FORMAT = "%Y%m%d-%H%M%S"
MOUNTS_FILE = "/proc/mounts"


class AbortError(Exception):
    """Exception where btrfs-backup-ng should abort."""


class SnapshotTransferError(AbortError):
    """Error when transferring a snapshot."""


class InsufficientSpaceError(AbortError):
    """Destination has insufficient space for the transfer.

    Raised when pre-flight space checks determine that the destination
    does not have enough available space (including safety margin) to
    complete the backup operation.
    """


def _endpoint_timestamp_format(endpoint: Any) -> str:
    """Return the timestamp format configured on an endpoint, or the default."""
    config = getattr(endpoint, "config", None)
    if isinstance(config, dict):
        fmt = config.get("timestamp_format")
        if fmt:
            return fmt
    return DATE_FORMAT


@functools.total_ordering
class Snapshot:
    """Represents a snapshot with comparison by prefix and time_obj."""

    def __init__(
        self,
        location: str | Path,
        prefix: str,
        endpoint: Any,
        time_obj: time.struct_time | None = None,
        time_format: str | None = None,
    ) -> None:
        self.location = Path(location)
        self.prefix = prefix
        self.endpoint = endpoint
        if time_obj is None:
            time_obj = str_to_date()
        self.time_obj = time_obj
        # The format used to render/parse this snapshot's timestamp. Stored per
        # instance so a snapshot parsed under a legacy format regenerates the
        # exact on-disk name even when a different timestamp_format is configured.
        if time_format is None:
            time_format = _endpoint_timestamp_format(endpoint)
        self.time_format = time_format
        self.locks: set = set()
        self.parent_locks: set = set()
        # btrfs subvolume identity, populated best-effort at enumeration (Phase 0).
        # ``uuid`` is this snapshot's own UUID; ``received_uuid`` is set on a subvolume
        # produced by ``btrfs receive`` and equals the source subvolume's UUID -- the
        # correspondence btrfs incremental send/receive actually uses. Empty when it
        # could not be read (non-root, non-btrfs, older btrfs-progs). NOT part of
        # identity yet: __eq__/__lt__ remain name/time based in Phase 0.
        self.uuid = ""
        self.received_uuid = ""

    def __eq__(self, other: object) -> bool:
        # NotImplemented, not an AttributeError. Annotating this signature (which
        # must take `object` -- Python compares a Snapshot against anything)
        # exposed that the body assumed the other side was a Snapshot, so
        # `snapshot == None` raised AttributeError instead of returning False.
        # Returning NotImplemented lets Python fall back to identity, which is
        # the documented contract and what every caller already assumed.
        if not isinstance(other, Snapshot):
            return NotImplemented
        return self.prefix == other.prefix and self.time_obj == other.time_obj

    def __lt__(self, other: "Snapshot") -> bool:
        if self.prefix != other.prefix:
            msg = f"prefixes don't match: {self.prefix} vs {other.prefix}"
            raise NotImplementedError(
                msg,
            )
        return self.time_obj < other.time_obj

    def __repr__(self) -> str:
        return self.get_name()

    def get_name(self) -> str:
        """Return a snapshot's name."""
        return self.prefix + date_to_str(self.time_obj, fmt=self.time_format)

    def get_path(self) -> Path:
        """Return full path to a snapshot."""
        return self.location / self.get_name()

    def find_parent(self, present_snapshots: list["Snapshot"]) -> "Snapshot | None":
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


def parse_subvolume_list(output: str) -> list[dict[str, str]]:
    """Parse ``btrfs subvolume list -o -u -R`` output into per-subvolume identity.

    Returns a list of dicts ``{'name', 'uuid', 'received_uuid', 'path'}``. Parsed
    token-wise (locating the ``uuid`` / ``received_uuid`` / ``path`` markers) rather than
    by fixed column position, so it tolerates btrfs-progs version differences in column
    ordering and spacing. An unset value (``-``) becomes an empty string. ``name`` is the
    final path component (the on-disk snapshot name). Unparseable lines are skipped.
    """
    entries = []
    for line in output.splitlines():
        tokens = line.split()
        if not tokens:
            continue
        uuid = received_uuid = path = ""
        i = 0
        while i < len(tokens):
            tok = tokens[i]
            if tok == "uuid" and i + 1 < len(tokens):
                uuid = tokens[i + 1]
                i += 2
                continue
            if tok == "received_uuid" and i + 1 < len(tokens):
                received_uuid = tokens[i + 1]
                i += 2
                continue
            if tok == "path" and i + 1 < len(tokens):
                # ``path`` is always the terminal field of a ``btrfs subvolume list`` line,
                # so the remainder is the path (which may legitimately contain spaces). Any
                # uuid/received_uuid always precede it and have already been captured.
                path = " ".join(tokens[i + 1 :])
                break
            i += 1
        if not path:
            continue
        entries.append(
            {
                "name": path.rsplit("/", 1)[-1],
                "uuid": "" if uuid == "-" else uuid,
                "received_uuid": "" if received_uuid == "-" else received_uuid,
                "path": path,
            }
        )
    return entries


def parse_subvolume_show(output: str) -> dict[str, str]:
    """Parse ``btrfs subvolume show <path>`` output for a subvolume's identity.

    Returns ``{'uuid': ..., 'received_uuid': ...}`` (empty strings when a field is unset
    ``-`` or absent). ``Received UUID`` is matched exactly so it is never confused with the
    plain ``UUID`` line, and ``Parent UUID`` is ignored. Unlike ``subvolume list``, ``show``
    targets one exact path, so the identity is unambiguous even when the subvolume lives
    under a mounted (non-filesystem-root) subvolume."""
    uuid = received_uuid = ""
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("Received UUID:"):
            v = stripped.split(":", 1)[1].strip()
            received_uuid = "" if v == "-" else v
        elif stripped.startswith("UUID:"):
            v = stripped.split(":", 1)[1].strip()
            uuid = "" if v == "-" else v
    return {"uuid": uuid, "received_uuid": received_uuid}


def exec_subprocess(
    command: list[str], method: str = "check_output", **kwargs: Any
) -> Any:
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
        # Give the AbortError a real message. A bare ``raise AbortError from e`` left
        # it empty, so callers logged garbled lines like "Failed to create snapshot: "
        # with no reason. Include the command's exit status and its stderr when it was
        # captured so the failure is self-explanatory.
        stderr = e.stderr
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", "replace")
        detail = (stderr or "").strip()
        cmd_name = str(command[0]) if command else "command"
        if detail:
            raise AbortError(
                f"{cmd_name} failed (exit {e.returncode}): {detail}"
            ) from e
        raise AbortError(f"{cmd_name} failed with exit status {e.returncode}") from e
    except Exception as e:
        logger.error("Unexpected error executing command: %s\nError: %s", command, e)
        raise AbortError(f"Error executing {command[0]}: {e}") from e


def log_heading(caption: str) -> str:
    """Formatted heading for logging output sections."""
    return f"{f'--[ {caption} ]':-<50}"


def date_to_str(
    timestamp: time.struct_time | None = None, fmt: str | None = None
) -> str:
    """Convert date format to string."""
    if timestamp is None:
        timestamp = time.localtime()
    if fmt is None:
        fmt = DATE_FORMAT
    return time.strftime(fmt, timestamp)


def str_to_date(
    time_string: str | None = None, fmt: str | None = None
) -> time.struct_time:
    """Convert date string to date object."""
    if time_string is None:
        # we don't simply return time.localtime() because this would have
        # a higher precision than the result converted from string
        time_string = date_to_str()
    if fmt is None:
        fmt = DATE_FORMAT
    return time.strptime(time_string, fmt)


def parse_snapshot_time(
    time_string: str, preferred_fmt: str | None = None
) -> tuple[time.struct_time, str]:
    """Parse a snapshot timestamp into a ``(time_obj, matched_fmt)`` pair.

    ``preferred_fmt`` (a configured ``timestamp_format``) is tried first when
    given, then the built-in ``DATE_FORMAT`` is tried as a fallback so snapshots
    created under a previous format stay readable after the format changes.
    ``matched_fmt`` is the format that actually parsed the string, so the caller
    can regenerate the identical on-disk name. Raises ``ValueError`` if no
    candidate format matches.
    """
    formats = []
    if preferred_fmt:
        formats.append(preferred_fmt)
    if DATE_FORMAT not in formats:
        formats.append(DATE_FORMAT)
    last_error = None
    for fmt in formats:
        try:
            return time.strptime(time_string, fmt), fmt
        except ValueError as e:
            last_error = e
    raise last_error or ValueError(f"unparseable snapshot timestamp: {time_string!r}")


def infer_snapshot_prefix(name: str, preferred_fmt: str | None = None) -> str | None:
    """Return the snapshot prefix ``name`` would need to parse, or None.

    A snapshot name is ``<prefix><timestamp>``, and every listing filters on the
    prefix then requires the remainder to parse as a timestamp. When an operator
    supplies the wrong prefix -- or none -- every real snapshot is silently
    discarded and the location reports as empty. Recovering the prefix from the
    names actually present turns that dead end into an instruction.

    Splits are tried left to right, so the FIRST match is the one with the
    longest timestamp, which is the intended reading: ``home-20260818-021031``
    yields ``home-`` rather than a longer prefix and a shorter, coincidental
    timestamp. Returns None when no split parses under any candidate format.
    """
    for i in range(len(name) + 1):
        try:
            parse_snapshot_time(name[i:], preferred_fmt)
        except (ValueError, TypeError):
            continue
        return name[:i]
    return None


def is_btrfs(path: str | Path) -> bool:
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


def is_subvolume(path: str | Path) -> bool:
    """Checks whether the given path is a btrfs subvolume.

    Args:
        path: Path to check

    Returns:
        True if path is a btrfs subvolume, False otherwise
    """
    path = Path(path).resolve()
    if not path.exists():
        return False
    if not is_btrfs(path):
        return False
    logger.debug("Checking for btrfs subvolume: %s", path)
    # subvolumes always have inode 256
    st = path.stat()
    result = st.st_ino == 256
    logger.debug("  -> Inode is %d, result is %r", st.st_ino, result)
    return result


def delete_subvolume(path: str | Path) -> None:
    """Delete a btrfs subvolume.

    Args:
        path: Path to the subvolume to delete

    Raises:
        AbortError: If deletion fails
    """
    path = Path(path).resolve()
    logger.debug("Deleting btrfs subvolume: %s", path)
    if not is_subvolume(path):
        raise AbortError(f"Path is not a subvolume: {path}")
    exec_subprocess(["btrfs", "subvolume", "delete", str(path)])
    logger.debug("  -> Subvolume deleted successfully")


def is_mounted(path: str | Path) -> bool:
    """Check if path is an active mount point.

    This verifies that a filesystem is actually mounted at the given path,
    which is useful for detecting when an external drive or network share
    is not connected.

    Args:
        path: Path to check

    Returns:
        True if path is an active mount point, False otherwise
    """
    path = Path(path).resolve()
    logger.debug("Checking if path is a mount point: %s", path)

    with open(MOUNTS_FILE, encoding="utf-8") as f:
        for line in f:
            try:
                mount_point = line.split(" ")[1]
            except (ValueError, IndexError):
                continue
            if Path(mount_point).resolve() == path:
                logger.debug("  -> Path is an active mount point")
                return True

    logger.debug("  -> Path is NOT a mount point")
    return False


def get_mount_info(path: str | Path) -> dict[str, str] | None:
    """Get mount information for the filesystem containing path.

    Args:
        path: Path to check

    Returns:
        Dict with 'mount_point', 'fs_type', 'device', or None if not found
    """
    path = Path(path).resolve()
    logger.debug("Getting mount info for: %s", path)
    best_match = None
    best_match_len = 0

    with open(MOUNTS_FILE, encoding="utf-8") as f:
        for line in f:
            try:
                parts = line.split(" ")
                device = parts[0]
                mount_point = parts[1]
                fs_type = parts[2]
            except (ValueError, IndexError):
                continue

            mount_path = Path(mount_point)
            if path == mount_path or path.is_relative_to(mount_path):
                if len(str(mount_point)) > best_match_len:
                    best_match_len = len(str(mount_point))
                    best_match = {
                        "mount_point": mount_point,
                        "fs_type": fs_type,
                        "device": device,
                    }

    if best_match:
        logger.debug("  -> Mount info: %s", best_match)
    else:
        logger.debug("  -> No mount info found")
    return best_match


def atomic_write_bytes(
    path: str | Path, data: bytes | str, *, mode: int = 0o600, fsync: bool = True
) -> None:
    """Crash-atomically replace ``path`` with ``data``.

    Writes a sibling temp file (in the SAME directory, so ``os.replace`` is a
    same-filesystem rename and can never fail with ``EXDEV``), fsyncs it, atomically
    renames it over the target, then fsyncs the parent directory so the rename itself
    survives a power loss. A crash at any point leaves either the OLD complete file or
    the NEW complete file -- never a half-written / truncated one. This is the single
    atomic-write primitive shared by lock files, raw ``.meta`` sidecars, operation
    state, and transfer manifests (R7): a torn state/manifest would break resume and a
    torn lock file would be misread as "no locks" and let retention prune a locked
    snapshot.

    The temp is opened ``O_CREAT|O_EXCL|O_NOFOLLOW`` at ``mode``: ``O_NOFOLLOW`` refuses
    a symlink planted at the temp path (defense when writing into a directory that may
    hold untrusted content, e.g. a raw target walked as root), ``O_EXCL`` refuses a
    pre-existing temp, and any stale temp left by a prior crash is unlinked first (the
    temp name is a fixed ``<name>.tmp`` sibling, so it is reclaimed rather than left to
    accumulate). Callers writing the SAME target concurrently must serialize themselves
    (the lock writer does, via its FileLock); the atomic replace still guarantees no
    torn file even if such a race occurs.

    ``data`` may be ``str`` (encoded UTF-8) or ``bytes``. Raises ``OSError`` on any
    failure, after removing the temp; the target file is left untouched. Set
    ``fsync=False`` only where durability is not required (e.g. throwaway test dirs) --
    the atomic replace still holds, only the power-loss durability guarantee is dropped.
    """
    path = Path(path)
    if isinstance(data, str):
        data = data.encode("utf-8")
    tmp = path.with_name(path.name + ".tmp")
    try:
        # Clear a leftover temp from a prior crash so the O_EXCL create below succeeds.
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        fd = os.open(
            str(tmp),
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            mode,
        )
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            if fsync:
                os.fsync(f.fileno())
        os.replace(str(tmp), str(path))
        # fsync the parent directory: the content fsync above does not guarantee the new
        # directory entry (the rename) survives a crash, and a lost rename would silently
        # revert to the old file.
        if fsync:
            with contextlib.suppress(OSError):
                dfd = os.open(str(path.parent), os.O_RDONLY)
                try:
                    os.fsync(dfd)
                finally:
                    os.close(dfd)
    except OSError:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def _privileged_fs(
    direct: Callable[[], Any],
    argv: list[str],
    *,
    action: str,
    path: str | Path,
    stdin_bytes: bytes | None = None,
    allow_prompt: bool = False,
    refuse_symlink: bool = False,
) -> Any:
    """Perform a filesystem operation directly, elevating only if that fails.

    Try the plain operation FIRST, whatever our uid. "Not root, therefore must
    sudo" is wrong twice over: it shells out for a path the running user already
    owns, and under the sudoers policy this project documents -- NOPASSWD limited
    to ``/usr/bin/btrfs`` -- that shell-out is REFUSED, because mkdir, tee and
    chmod are not btrfs. Measured on a real host: `sudo -n btrfs` allowed,
    `sudo -n mkdir` answered "a password is required", and a restore into a
    destination the user could have written directly died on the mkdir.

    Elevation is the fallback for a path we genuinely cannot write, not the
    default. Running as root skips the fallback entirely: root failing to write
    is a real filesystem error and escalating it would only obscure that.

    When both routes fail the raised ``PermissionError`` says what was attempted,
    what sudo said, and what would make it work -- as opposed to a bare
    ``CalledProcessError`` repr naming an argv the operator never typed.

    ``allow_prompt`` decides whether the fallback may ask for a password. It
    defaults to False -- ``sudo -n`` -- because most callers run headless, where
    an interactive sudo does not ask anyone anything, it just hangs. Foreground
    commands where a person is already waiting (a restore) pass True, so a user
    with full sudo is prompted exactly as they were before rather than being told
    to start over as root.
    """
    # The DIRECT route no longer needs this: it opens O_NOFOLLOW, so the kernel
    # refuses a symlink as part of the operation and there is no window at all.
    #
    # This check exists for the ELEVATED route, which shells out to `sudo tee` /
    # `sudo chmod` -- separate processes that follow links and cannot be handed
    # an already-opened descriptor, since the whole reason we are escalating is
    # that this user could not open it. That check is therefore BEST EFFORT: it
    # closes the common case (a link already sitting there) and narrows, but
    # cannot close, a link swapped in after the check and before sudo runs.
    # Refusing early also means the usual outcome is a clear error rather than a
    # root-privileged write to someone else's file.
    if refuse_symlink and os.path.islink(path):
        raise PermissionError(
            f"Refusing to {action} {path}: it is a symbolic link pointing at "
            f"{os.path.realpath(path)}, and following it would write somewhere "
            f"this command was not asked to touch. Remove or replace the link, "
            f"or point this at a real path."
        )

    try:
        return direct()
    except OSError as direct_error:
        if os.geteuid() == 0:
            raise
        # Escalate only when the direct attempt was refused for PERMISSION
        # reasons. `except (PermissionError, OSError)` is just `except OSError`,
        # so every failure retried itself as root: a full filesystem, a
        # mistyped path, a file where a directory belongs. None of those are
        # fixed by being root, and retrying them there is how an unrelated
        # error turns into a root-privileged write -- ENOSPC in particular
        # would succeed by eating the reserved blocks a normal user is
        # correctly denied.
        if direct_error.errno not in (errno.EACCES, errno.EPERM):
            raise
        first_error = direct_error

    sudo = ["sudo"] if allow_prompt else ["sudo", "-n"]
    proc = subprocess.run(
        [*sudo, *argv],
        input=stdin_bytes,
        capture_output=True,
    )
    if proc.returncode == 0:
        return None

    stderr_lines = (proc.stderr or b"").decode(errors="replace").strip().splitlines()
    detail = stderr_lines[-1] if stderr_lines else f"sudo exited {proc.returncode}"
    reason = getattr(first_error, "strerror", None) or str(first_error)
    raise PermissionError(
        f"Cannot {action} {path}: {reason}. Elevation was refused as well "
        f"({detail}). Either run this command as root, or give {_current_user()} "
        f"write access to {Path(path).parent}. Note that the sudoers rule this "
        f"project documents grants NOPASSWD for /usr/bin/btrfs only, which does "
        f"not cover {argv[0]}."
    )


def _current_user() -> str:
    """The running user's name, for error messages; the uid if it has no name."""
    try:
        import pwd

        return pwd.getpwuid(os.geteuid()).pw_name
    except Exception:
        return f"uid {os.geteuid()}"


def privileged_mkdir(
    path: str | Path,
    *,
    parents: bool = True,
    exist_ok: bool = True,
    allow_prompt: bool = False,
) -> None:
    """Create ``path``, elevating only if the direct mkdir is refused."""
    path = Path(path)
    return _privileged_fs(
        lambda: path.mkdir(parents=parents, exist_ok=exist_ok),
        ["mkdir", "-p", str(path)] if parents else ["mkdir", str(path)],
        action="create directory",
        path=path,
        allow_prompt=allow_prompt,
    )


def privileged_write_bytes(
    path: str | Path, data: bytes | str, *, allow_prompt: bool = False
) -> None:
    """Write ``data`` to ``path``, elevating only if the direct write is refused."""
    path = Path(path)
    if isinstance(data, str):
        data = data.encode("utf-8")

    def _direct():
        # O_NOFOLLOW makes the kernel do the symlink check AS PART OF the open,
        # so there is no window between deciding the path is safe and using it.
        # `Path.write_bytes` follows links, which left a race the separate
        # islink() guard could only narrow, never close.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            os.ftruncate(fd, 0)
            os.write(fd, data)
        finally:
            os.close(fd)

    return _privileged_fs(
        _direct,
        ["tee", str(path)],
        action="write",
        path=path,
        stdin_bytes=data,
        allow_prompt=allow_prompt,
        refuse_symlink=True,
    )


def _chmod_nofollow(path: Path, mode: int) -> None:
    """chmod without following a symlink, atomically.

    `Path.chmod` follows links, so a separate islink() check leaves a window in
    which the path can be replaced. Opening O_NOFOLLOW and using fchmod moves the
    check into the kernel: if it is a link the open fails outright and nothing is
    changed.
    """
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fchmod(fd, mode)
    finally:
        os.close(fd)


def privileged_chmod(
    path: str | Path, mode: int, *, allow_prompt: bool = False
) -> None:
    """chmod ``path``, elevating only if the direct chmod is refused."""
    path = Path(path)
    return _privileged_fs(
        lambda: _chmod_nofollow(path, mode),
        ["chmod", format(mode, "o"), str(path)],
        action="set permissions on",
        path=path,
        allow_prompt=allow_prompt,
        refuse_symlink=True,
    )


def privileged_rmtree(path: str | Path, *, allow_prompt: bool = False) -> None:
    """Remove ``path`` recursively, elevating only if the direct remove fails.

    Best-effort by nature -- every caller is already cleaning up after a failure
    -- so callers keep their own guard around it rather than this swallowing the
    error and reporting a cleanup that did not happen.
    """
    import shutil

    path = Path(path)
    return _privileged_fs(
        lambda: shutil.rmtree(path),
        ["rm", "-rf", str(path)],
        action="remove",
        path=path,
        allow_prompt=allow_prompt,
    )


def read_locks(s: str) -> dict[str, Any]:
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


def write_locks(lock_dict: dict[str, Any]) -> str:
    """Converts ``lock_dict`` back to the string readable by ``read_locks``."""
    return json.dumps(lock_dict, indent=4)


def open_failure_reason(e: OSError) -> str:
    """A plain-language reason opening a path failed, for a user-facing message.

    Translates the errno so a regular user sees why the file could not be opened
    and what to check, instead of a bare ``[Errno NN]`` repr (whose default text
    is sometimes misleading -- e.g. ELOOP prints 'Too many levels of symbolic
    links' for a single planted symlink). Used by every O_NOFOLLOW open, which is
    what surfaces ELOOP for a planted symlink.
    """
    reasons = {
        errno.ELOOP: "it is a symlink (refused for safety)",
        errno.EISDIR: "it is a directory, not a file",
        errno.ENXIO: "it is a FIFO/special file with no reader (refused)",
        errno.EACCES: (
            "permission denied -- check the directory's ownership and permissions"
        ),
        errno.EPERM: (
            "operation not permitted -- check the directory's ownership and permissions"
        ),
        errno.EROFS: "the filesystem is read-only",
        errno.ENOTDIR: "a parent path component is not a directory",
    }
    if e.errno is None:
        return str(e)
    return reasons.get(e.errno, str(e))


@contextlib.contextmanager
def exclusive_lock(lockfile: Path, *, timeout: float, subject: str) -> Any:
    """Hold an exclusive ``flock`` on ``lockfile``, or fail in a bounded way.

    The one implementation of "only one of these at a time", so a second caller
    cannot arrive with a weaker version of the same idea. ``subject`` names what
    is being locked and appears in every message ("raw target /mnt/x", "this
    run"), which is the only thing that differs between callers.

    A bounded-blocking wait: it retries for up to ``timeout`` seconds so
    legitimate contention SERIALISES rather than fails, then raises RuntimeError.
    The lock is released when the fd closes and is auto-released if the process
    dies, so it can never go stale.

    Failure posture -- every one of these raises RuntimeError with a plain
    reason rather than escaping as an uncaught OSError, so a hostile or
    mis-created lock file degrades to the same bounded failure as ordinary
    contention instead of crashing (or hanging) the caller:

      * ``O_NOFOLLOW`` refuses a planted symlink, which could otherwise redirect
        an often-root open somewhere else entirely;
      * ``O_NONBLOCK`` makes a planted FIFO return ENXIO at once instead of
        blocking the open forever waiting for a reader -- without it a single
        FIFO wedges every run silently, which is a permanent denial of service;
      * anything that opens but is not a REGULAR file (a FIFO that happened to
        have a reader, a device, a socket) is refused after an fstat, because it
        must not be trusted to coordinate anything;
      * an errno other than EAGAIN/EWOULDBLOCK from ``flock`` (ENOLCK on a
        filesystem that cannot lock, say) will never clear, so it fails at once
        rather than polling for the full timeout and calling it "busy".
    """
    try:
        fd = os.open(
            lockfile,
            os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
            0o600,
        )
    except OSError as e:
        raise RuntimeError(
            f"{subject}: cannot acquire its lock file {lockfile} -- "
            f"{open_failure_reason(e)}. The directory must not be writable by "
            "untrusted users."
        ) from e
    try:
        is_regular = stat_module.S_ISREG(os.fstat(fd).st_mode)
    except OSError as e:
        os.close(fd)
        raise RuntimeError(
            f"{subject}: cannot stat its lock file {lockfile} -- "
            f"{open_failure_reason(e)}"
        ) from e
    if not is_regular:
        os.close(fd)
        raise RuntimeError(
            f"{subject}: lock file {lockfile} is not a regular file (a FIFO, "
            "device, or socket may have been planted); refusing to use it"
        )
    deadline = time.monotonic() + max(0.0, timeout)
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as e:
                if e.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise RuntimeError(
                        f"{subject}: cannot lock {lockfile} ({e}); the filesystem "
                        "may not support flock"
                    ) from e
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"{subject} is busy (another operation holds the lock); "
                        "retry when it finishes"
                    ) from None
                time.sleep(0.2)
        yield
    finally:
        os.close(fd)  # releases the flock


def process_io_bytes(pid: int) -> int | None:
    """Bytes this process has read plus written, from ``/proc/<pid>/io``.

    Used to tell a SLOW transfer from a STUCK one. A wall-clock limit cannot:
    it fires on a healthy first sync of a large subvolume over a slow link, and
    waits the full hour on a pipe that died in the first minute.

    Returns None when the number cannot be had, which is not an error and must
    not be treated as "no bytes moved":

    * not Linux, or no procfs;
    * the process has already exited;
    * **the process belongs to another user.** The local ``btrfs send`` runs
      under sudo, so its io file is root-owned and unreadable to us. The ssh
      process is ours and readable, which is enough -- bytes leaving on the
      socket is the same evidence.

    Counting rchar+wchar rather than read_bytes/write_bytes deliberately: the
    latter count actual block-device traffic, so a transfer served entirely from
    page cache would look stalled while moving at full speed.
    """
    try:
        with open(f"/proc/{pid}/io", encoding="ascii") as handle:
            total = 0
            found = False
            for line in handle:
                key, _, value = line.partition(":")
                if key in ("rchar", "wchar"):
                    total += int(value.strip())
                    found = True
            return total if found else None
    except (OSError, ValueError):
        return None


def any_bytes_moved(pids: list[int]) -> int | None:
    """Total io across ``pids``, ignoring the ones that cannot be read.

    None means NOTHING in the set could be measured -- the caller must then
    disable stall detection rather than conclude the transfer is stuck, and say
    so, because silently degrading a safety check to a false positive would kill
    healthy transfers.
    """
    total = 0
    measured = False
    for pid in pids:
        value = process_io_bytes(pid)
        if value is not None:
            total += value
            measured = True
    return total if measured else None
