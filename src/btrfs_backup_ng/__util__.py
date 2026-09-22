"""btrfs-backup-ng: btrfs_backup_ng/__util__.py
Common utility code shared between modules.
"""

import contextlib
import fcntl
import functools
import json
import errno
import os
from collections.abc import Iterator
import stat as stat_module
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
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
    "indistinguishable_period",
    "derive_snapshot_time",
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


#: A trailing ``_N`` in a snapshot name -- btrbk's collision counter. Bounded
#: at 9 digits so a pathological name cannot allocate a huge int; anything
#: longer is treated as not-an-ordinal.
_TRAILING_ORDINAL_RE = re.compile(r"_(\d{1,9})$")


def _name_ordinal(name: str) -> int:
    """The numeric value of a trailing ``_N`` in a snapshot name, else 0.

    ORDERING metadata only, never identity: two snapshots sharing a timestamp
    sort by it, so ``X_2`` precedes ``X_10`` where the bare name string would
    order them the other way round. The value is DERIVED from the name on each
    comparison; nothing stores it.
    """
    match = _TRAILING_ORDINAL_RE.search(name)
    return int(match.group(1)) if match else 0


@functools.total_ordering
class Snapshot:
    """A snapshot whose identity is its NAME, observed once and never recomputed.

    A listing constructs this with the on-disk name it OBSERVED; the creation
    path renders the name exactly once, in ``__init__``, from the endpoint's
    configured timestamp_format. Either way ``get_name()``/``get_path()``
    always resolve to the entry the snapshot came from -- prune deletes and
    lock keys go through them, so a name that parses but does not re-render
    identically (strptime accepts single-digit fields the format would pad)
    must never be rebuilt from its timestamp. ``time_obj`` is a derived
    attribute for ordering and retention math, not a source of the name.
    """

    def __init__(
        self,
        location: str | Path,
        prefix: str,
        endpoint: Any,
        time_obj: time.struct_time | None = None,
        name: str | None = None,
    ) -> None:
        self.location = Path(location)
        self.prefix = prefix
        self.endpoint = endpoint
        if time_obj is None and name is None:
            # The creation path defaults to "now". localtime() directly, NOT
            # str_to_date(): that round-trips through DATE_FORMAT, and strptime
            # returns tm_gmtoff=None / tm_isdst=-1, so a snapshot created under
            # a timestamp_format containing %z was named without its offset and
            # the tool could then not resolve its own path. The round trip's
            # stated purpose was to drop sub-second precision, which
            # struct_time cannot hold in the first place.
            #
            # A listing that passes an observed NAME passes the time it derived
            # from that name -- or None when the name yields none. "Now" is
            # never invented for an observed snapshot: a fictitious age would
            # feed ordering and retention a fact the filesystem does not hold.
            time_obj = time.localtime()
        self.time_obj = time_obj
        if name is None:
            # The creation path: render the name ONCE, here, under the
            # endpoint's configured timestamp_format. From this point the name
            # is a fact about the snapshot, never a function of its attributes;
            # every listing passes the observed on-disk string instead.
            name = prefix + date_to_str(
                time_obj, fmt=_endpoint_timestamp_format(endpoint)
            )
        self.name = name
        # Set by listings: True when this name would have been INVISIBLE to
        # releases that required a plain timestamp parse -- a trailing _N, or
        # no derivable timestamp at all. The listing announces such snapshots
        # and prune marks them, because the first run that can see one is also
        # the first run that could delete it.
        self.newly_visible = False
        self.locks: set = set()
        self.parent_locks: set = set()
        # btrfs subvolume identity, populated best-effort at enumeration (Phase 0).
        # ``uuid`` is this snapshot's own UUID; ``received_uuid`` is set on a subvolume
        # produced by ``btrfs receive`` and equals the source subvolume's UUID -- the
        # correspondence btrfs incremental send/receive actually uses. Empty when it
        # could not be read (non-root, non-btrfs, older btrfs-progs). NOT part
        # of identity: __eq__ is name-based, __lt__ time-then-name.
        self.uuid = ""
        self.received_uuid = ""

    @property
    def stream_uuid(self) -> str:
        """The uuid a ``btrfs send`` of this subvolume carries: ``received_uuid``
        when set, else its own ``uuid``.

        ``btrfs send`` emits a subvolume's received_uuid in place of its uuid
        when it has one, and ``btrfs receive`` records whatever the stream
        carried as the new copy's received_uuid. Identity therefore propagates
        along a whole chain of send/receive: a copy of a copy of O still has
        received_uuid == O.uuid, not the uuid of the copy it was sent from.
        This is the value every correspondence comparison uses (a candidate
        corresponds when ``candidate.received_uuid == source.stream_uuid``);
        comparing against ``uuid`` alone matched only the first hop. Empty
        when identity could not be read, which callers treat as "unknown",
        never as a match.
        """
        return self.received_uuid or self.uuid

    def __eq__(self, other: object) -> bool:
        # Identity is the NAME -- the one fact a comparison shares with the
        # filesystem. Duck-typed via get_name(), mirroring RawSnapshot.__eq__,
        # so a raw backup equals the btrfs snapshot it came from. Two snapshots
        # sharing a timestamp but not a name (a trailing _N, a foreign spelling)
        # are DIFFERENT snapshots and no longer collide in presence checks,
        # dedup, or lock keys. NotImplemented, not AttributeError, for anything
        # without get_name(): Python then falls back to identity, so
        # `snapshot == None` is False rather than a crash -- the documented
        # contract every caller assumes.
        other_get_name = getattr(other, "get_name", None)
        if other_get_name is None:
            return NotImplemented
        return self.name == other_get_name()

    def __lt__(self, other: "Snapshot") -> bool:
        if self.prefix != other.prefix:
            msg = f"prefixes don't match: {self.prefix} vs {other.prefix}"
            raise NotImplementedError(
                msg,
            )
        other_time = getattr(other, "time_obj", None)
        if self.time_obj is not None and other_time is not None:
            if self.time_obj != other_time:
                return self.time_obj < other_time
        elif (self.time_obj is None) != (other_time is None):
            # A snapshot with no derivable time sorts AFTER every timestamped
            # one. Unknown age is never treated as old: "oldest" is where
            # count-based deletion slices from, and a decision that needs age
            # must partition these out explicitly rather than rely on order.
            return other_time is None
        # Same timestamp: break the tie so ordering is total and deterministic
        # -- find_parent must never pick one of two same-second snapshots
        # arbitrarily. A trailing _N (btrbk's collision counter) orders
        # numerically, so X_2 precedes X_10; any other difference falls back
        # to the name string. Ordering metadata only -- the ordinal is derived
        # from the name, never stored on the snapshot.
        self_name = self.get_name()
        other_name = other.get_name()
        return (_name_ordinal(self_name), self_name) < (
            _name_ordinal(other_name),
            other_name,
        )

    def __repr__(self) -> str:
        return self.get_name()

    def get_name(self) -> str:
        """Return the snapshot's name: observed at listing, or rendered once
        at creation. Never recomputed -- prune deletes by the path built from
        this, so it must be the string the filesystem actually holds."""
        return self.name

    def get_path(self) -> Path:
        """Return full path to a snapshot."""
        return self.location / self.get_name()

    def find_parent(self, present_snapshots: list["Snapshot"]) -> "Snapshot | None":
        """Returns object from ``present_snapshot`` most suitable for being
        used as a parent for transferring this one or ``None``,
        if none found.
        """
        if self in present_snapshots:
            # snapshot already transferred (name identity -- works whether or
            # not either side carries a timestamp)
            return None
        if self.time_obj is None:
            # No derivable time means no honest claim about which present
            # snapshot is older: no parent, full send -- which always works.
            logger.debug(
                "find_parent(%s): the name yields no timestamp; "
                "sending in full rather than guessing at a parent.",
                self.get_name(),
            )
            return None
        candidates = [
            p for p in present_snapshots if getattr(p, "time_obj", None) is not None
        ]
        if len(candidates) != len(present_snapshots):
            logger.debug(
                "find_parent(%s): ignoring %d present snapshot(s) with no "
                "derivable timestamp as parent candidates.",
                self.get_name(),
                len(present_snapshots) - len(candidates),
            )
        for present_snapshot in reversed(candidates):
            if present_snapshot < self:
                return present_snapshot
        # no snapshot older than snapshot is present ...
        if candidates:
            # ... hence we choose the oldest one present as parent
            return candidates[0]

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


#: A host this project will hand to ssh. Deliberately narrower than DNS: an
#: allow-list of what a hostname, IPv4 literal or bracketed IPv6 literal may
#: contain, with an optional user, because everything outside it is either
#: meaningless to ssh or dangerous.
_SSH_HOST_RE = re.compile(
    r"^(?:[A-Za-z0-9_][A-Za-z0-9_.\-]*@)?"
    r"(?:\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9](?:[A-Za-z0-9.\-]*[A-Za-z0-9])?)$"
)


# TOML basic strings must escape the backslash, the double quote, and every C0
# control character plus DEL (TOML 1.0, "Basic strings"). Tab is legal raw but is
# escaped here too, so the output is readable.
_TOML_SIMPLE_ESCAPES = {
    "\\": "\\\\",
    '"': '\\"',
    "\b": "\\b",
    "\t": "\\t",
    "\n": "\\n",
    "\f": "\\f",
    "\r": "\\r",
}


def toml_str(value: str) -> str:
    """Return ``value`` as a quoted, escaped TOML basic string.

    THE canonical way this project turns a Python string into TOML. Config
    generators interpolated values straight into ``f'key = "{value}"'``, which
    fails two ways on a path a user can legitimately have. A double quote ends
    the string early and the file will not parse -- loud, at least. A backslash
    is worse: ``/mnt/a\\backup`` emits ``"/mnt/a\\backup"``, TOML reads ``\\b`` as
    a backspace, and the config loads CLEANLY pointing at ``/mnt/a\\x08ackup``.
    A backup tool then snapshots and prunes a directory the operator never named.

    Escaping every character TOML requires makes the round trip lossless, so a
    generated config means what the source it was converted from meant.
    """
    out: list[str] = []
    for char in str(value):
        escape = _TOML_SIMPLE_ESCAPES.get(char)
        if escape is not None:
            out.append(escape)
        elif char < " " or char == "\x7f":
            out.append(f"\\u{ord(char):04X}")
        else:
            out.append(char)
    return '"' + "".join(out) + '"'


def validated_ssh_host(host: str, *, username: str | None = None) -> str:
    """Return ``host`` (or ``user@host``) after checking ssh can be given it.

    The host reached two kinds of harm unvalidated:

    * **The shell.** ``_do_shell_pipeline_transfer`` joins its ssh arguments into
      ONE string and runs it with ``shell=True``, quoting the ControlPath and the
      remote command but not the host -- so a host containing ``;`` ran a command
      on the machine doing the backup, which is running ``btrfs send``, typically
      as root.
    * **ssh's own option parser.** A host beginning with ``-`` is read as an
      option however it is quoted, so ``-oProxyCommand=...`` runs a command even
      on the argv paths, where no shell is involved.

    Validated once, where the endpoint is built, so every present and future call
    site is covered. A check at the config-import boundary alone would leave a
    hand-written config, a wizard entry and the direct CLI forms unprotected.
    """
    candidate = f"{username}@{host}" if username else host
    if not host or not _SSH_HOST_RE.fullmatch(candidate):
        raise ValueError(
            f"{candidate!r} is not a usable ssh host: expected [user@]host where "
            f"host is a hostname, an IPv4 address, or a bracketed IPv6 address"
        )
    return candidate


def date_to_str(
    timestamp: time.struct_time | None = None, fmt: str | None = None
) -> str:
    """Convert date format to string.

    ``%z`` is rendered here rather than by ``time.strftime``, which takes the
    offset from ``tm_zone`` -- a field ``time.strptime`` leaves as None. So a
    name that CARRIED an offset lost it on re-render: parsing
    ``20260919T011855-0400`` gives ``tm_gmtoff=-14400`` but ``tm_zone=None``,
    and strftime returned ``20260919T011855``. Since ``Snapshot.get_name()``
    regenerates the on-disk name through here and ``get_path()`` builds a path
    from it, every caller that resolves a path from a Snapshot -- delete, lock,
    send, verify -- was pointed at a name that does not exist. Reachable from a
    shipped feature: ``config import`` emits ``%Y%m%dT%H%M%S%z`` for btrbk's
    ``long-iso``.

    ``%Z`` (the zone NAME) is deliberately left to strftime: it has no
    equivalent in ``tm_gmtoff`` and inventing one would be worse than omitting
    it.
    """
    if timestamp is None:
        timestamp = time.localtime()
    if fmt is None:
        fmt = DATE_FORMAT
    offset = getattr(timestamp, "tm_gmtoff", None)
    if "%z" in fmt and offset is not None:
        try:
            aware = datetime(*timestamp[:6], tzinfo=timezone(timedelta(seconds=offset)))
        except (ValueError, TypeError, OverflowError):
            # datetime rejects a leap second (tm_sec == 60) where strftime
            # accepts it. Naming a snapshot is too central to fail over
            # rendering an offset, so fall back rather than refuse to name it.
            # A struct_time malformed beyond that is NOT rescued here -- the
            # fallback rejects it too (measured) -- but the stdlib does not
            # produce one.
            return time.strftime(fmt, timestamp)
        return aware.strftime(fmt)
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
    ``matched_fmt`` is the format that actually parsed the string. Nothing may
    regenerate a name from it -- names are observed and remembered
    (``Snapshot.name``) -- but a caller can still use it to report WHICH format
    matched. Raises ``ValueError`` if no candidate format matches.
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


def derive_snapshot_time(
    time_string: str, preferred_fmt: str | None = None
) -> tuple[time.struct_time | None, bool]:
    """Best-effort timestamp DERIVATION from an observed name's timestamp part.

    Returns ``(time_obj, parsed_as_written)``. ``time_obj`` is None when no
    timestamp can be derived -- never an exception, because under the
    remembered-name contract an unparseable name is a fact about a snapshot,
    not an error. ``parsed_as_written`` is True only when the string parsed
    without help; False means the snapshot was invisible to releases that
    required a plain parse (callers mark it ``newly_visible``).

    The string is tried AS WRITTEN first (``parse_snapshot_time``: the
    configured format, then the default), and only then with one trailing
    ``_N`` stripped -- btrbk's collision counter, which it appends whenever a
    timestamp recurs under a coarse format (its daily ``short`` format plus an
    hourly schedule makes such names the norm, not the exception). With
    today's two candidate formats the two orderings agree on every input
    (measured; a stripped base can only match a date-only format, which can
    only arrive as ``preferred_fmt``, in which case the full string does not
    parse either) -- the as-written-first order is kept because it FAILS SAFE
    if a date-only candidate is ever added: ``20260904_120000`` must stay
    noon-with-no-ordinal, never midnight-with-ordinal-120000.
    """
    try:
        time_obj, _ = parse_snapshot_time(time_string, preferred_fmt)
        return time_obj, True
    except ValueError:
        pass
    match = _TRAILING_ORDINAL_RE.search(time_string)
    if match:
        try:
            time_obj, _ = parse_snapshot_time(
                time_string[: match.start()], preferred_fmt
            )
            return time_obj, False
        except ValueError:
            pass
    return None, False


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


def indistinguishable_period(fmt: str) -> str | None:
    """Return the widest period within which ``fmt`` renders identical names, or None.

    Decides which diagnosis a snapshot-name collision gets. Under a
    seconds-resolving format a collision means two requests in the same second
    (or the repeated hour of a daylight-saving fall-back), and "wait a second
    and retry" is real advice. Under a coarser format the format itself cannot
    name a second snapshot within its period, and that advice cannot work --
    the previous message gave it anyway, so a user with
    ``timestamp_format = "%Y%m%d"`` got one snapshot per day and a misdiagnosis
    on every later run.

    The format is probed with real instants rather than inspected as a string:
    two moments are rendered and compared, so every strftime directive -- and
    any literal text -- is judged by what it actually produces. Probes render
    with ``time.gmtime`` on a fixed epoch, so the verdict cannot depend on the
    machine's timezone or clock. The base instant sits mid-period (12:30:30 on
    January 15th): a probe step from a boundary would roll the next field over
    and misread the resolution (from 12:30:59, one second later is 12:31:00,
    which a minute-coarse format renders differently -- it would look
    seconds-fine).

    Returns ``None`` when instants one second apart render differently (the
    format resolves seconds); otherwise ``"minute"``, ``"hour"`` or ``"day"``
    -- the period whose instants all share one rendering, named by the first
    probe step that renders differently -- or ``"more than a day"`` when even
    day-apart instants share a name.
    """
    base = 979561830  # 2001-01-15T12:30:30Z
    reference = time.strftime(fmt, time.gmtime(base))
    for period, step in (
        ("second", 1),
        ("minute", 60),
        ("hour", 3600),
        ("day", 86400),
    ):
        if time.strftime(fmt, time.gmtime(base + step)) != reference:
            return None if period == "second" else period
    return "more than a day"


def unescape_mount_field(field: str) -> str:
    """Decode the octal escapes the kernel writes into ``/proc/mounts``.

    Every path field there is escaped: space becomes ``\\040``, tab ``\\011``,
    newline ``\\012`` and backslash ``\\134``. Comparing an undecoded field
    against a real path therefore fails for any mount point containing one of
    them -- and on a systemd desktop that is the norm rather than an edge case.
    udisks2 mounts removable drives at ``/run/media/<user>/<Volume Label>``, and
    volume labels routinely contain spaces, so the single most common external
    drive layout could not be matched at all.

    Only well-formed three-digit octal escapes are decoded; anything else is left
    alone, so a literal backslash in a name survives unharmed.
    """
    if "\\" not in field:
        return field
    out: list[str] = []
    i = 0
    while i < len(field):
        if field[i] == "\\" and len(field) - i >= 4:
            digits = field[i + 1 : i + 4]
            if len(digits) == 3 and all(c in "01234567" for c in digits):
                out.append(chr(int(digits, 8)))
                i += 4
                continue
        out.append(field[i])
        i += 1
    return "".join(out)


def iter_mounts(mounts_file: str | None = None) -> Iterator[tuple[str, str, str]]:
    """Yield ``(device, mount_point, fs_type)`` for each mount, paths DECODED.

    THE single reader of the mount table. Four separate parsers used to split the
    line by hand and compare the raw field, so every one of them was blind to a
    mount point containing a space -- including ``is_btrfs``, which would report
    a btrfs drive as not-btrfs purely because of its volume label.
    """
    path = mounts_file or MOUNTS_FILE
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            parts = line.split(" ")
            if len(parts) < 3:
                continue
            yield (
                unescape_mount_field(parts[0]),
                unescape_mount_field(parts[1]),
                parts[2],
            )


def is_btrfs(path: str | Path) -> bool:
    """Checks whether path is inside a btrfs file system."""
    path = Path(path).resolve()
    logger.debug("Checking for btrfs filesystem: %s", path)
    best_match = ""
    best_match_fs_type = ""
    logger.debug("  Reading mounts file: %s", MOUNTS_FILE)
    for _device, mount_point, fs_type in iter_mounts():
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

    for _device, mount_point, _fs_type in iter_mounts():
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

    for device, mount_point, fs_type in iter_mounts():
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


def missing_backup_location_message(what: str, path: str | Path) -> str:
    """The one diagnosis for a backup location that is not there.

    A backup location -- a source, a target, a snapshot base, however it was
    given -- is a statement that something is there, not a request to make
    it (#102). The likely causes need different remedies, so every refusal
    names them all and says that nothing was created.
    """
    return (
        f"{what} {path} does not exist. btrfs-backup-ng does not create a "
        f"backup location: if it lives on a removable or network filesystem, "
        f"it is most likely not mounted. Mount it, check the path for a typo, "
        f"or create the directory yourself. Nothing was created."
    )


def create_below(
    base: str | Path,
    *parts: str,
    mode: int | None = None,
    what: str = "Directory",
) -> Path:
    """Create ``base/parts...`` one component at a time, never ``base`` itself.

    This is the only way a directory under a backup location may be created.
    ``base`` -- the source, the target, the snapshot base -- must already
    exist, whoever named it and however; each
    component under it is created with a plain ``mkdir`` -- no ``parents`` --
    so the call is structurally unable to invent an ancestor. If the base
    vanishes between the check and the mkdir (a drive unmounted mid-run), the
    kernel refuses the first component and that refusal is reported as the
    missing base, not repaired by rebuilding the tree on whatever filesystem
    is underneath.

    ``parts`` are relative; a component may contain ``/`` and is split. An
    absolute or ``..`` component is a programming error, not a path to create.
    Returns the leaf.
    """
    base_path = Path(base)
    if not base_path.is_dir():
        raise AbortError(missing_backup_location_message(what, base_path))
    components = [c for part in parts for c in str(part).split("/") if c]
    if any(c == ".." for c in components) or any(
        str(part).startswith("/") for part in parts
    ):
        raise ValueError(f"create_below: {parts!r} must be relative to {base_path}")
    current = base_path
    for component in components:
        current = current / component
        try:
            if mode is None:
                current.mkdir(exist_ok=True)
            else:
                current.mkdir(mode=mode, exist_ok=True)
        except FileNotFoundError as e:
            # The base (or a component just created) is gone: the filesystem
            # holding it went away. Rebuilding it here is exactly the
            # defect this primitive exists to make impossible.
            raise AbortError(missing_backup_location_message(what, base_path)) from e
        except FileExistsError as e:
            raise AbortError(
                f"Cannot create {current}: the path exists but is not a directory."
            ) from e
    return current


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
