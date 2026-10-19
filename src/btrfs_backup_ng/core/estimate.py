"""Backup size estimation for pre-transfer planning.

Provides estimates of data that will be transferred, helping users:
- Plan bandwidth usage
- Estimate backup time
- Verify expected transfer sizes before proceeding

This module holds the one local measurement of a snapshot's size. The
progress bar, the engine's space check and the ssh endpoint's local send
each had a copy of its ladder; they delegate here now.
"""

import logging
import os
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from .. import __util__

logger = logging.getLogger(__name__)


#: Methods whose result is a LOWER BOUND, not an estimate.
#:
#: ``send_no_data`` measures a ``btrfs send --no-data`` stream, which carries
#: metadata and no file contents -- the real transfer runs roughly 10-100x it.
#: ``size_diff`` subtracts two exclusive subvolume sizes, which misses anything
#: rewritten in place and ignores what a send stream actually encodes.
#:
#: Both are useful for "is this change big or small". Neither is a number to
#: plan bandwidth or a maintenance window against, and presenting one as though
#: it were is the reason this constant exists rather than being inlined.
UNDERESTIMATING_METHODS = frozenset({"send_no_data", "size_diff"})

#: Methods whose result is an UPPER BOUND.
#:
#: ``du -sb`` reports apparent size: a sparse file counts in full although a
#: send stream skips its holes, and data two names share by reflink or
#: hardlink counts under each name although a send stream carries it once.
#: Measured at 4.5x ``btrfs filesystem du`` on a tree holding one sparse file
#: and within 0.3% of it on an ordinary tree. A figure that fits proves the
#: transfer fits; one that does not fit proves nothing, which is why the space
#: check is told where its number came from.
OVERCOUNTING_METHODS = frozenset({"du"})


def bound_of(method: str) -> str:
    """What a figure from ``method`` proves.

    ``"measurement"`` is taken at face value, ``"upper"`` proves the transfer
    fits when the figure does, ``"lower"`` proves it does not fit when the
    figure does not.
    """
    if method in UNDERESTIMATING_METHODS:
        return "lower"
    if method in OVERCOUNTING_METHODS:
        return "upper"
    return "measurement"


@dataclass(frozen=True)
class SizeEstimate:
    """A size in bytes with the method that produced it.

    The space check decides on ``bound``: it refuses on a measurement, lets an
    upper bound that does not fit through with a warning, and never mistakes
    a floor for either. Before this the check saw an integer alone and
    applied one rule to figures of different quality.
    """

    bytes: int
    method: str

    @property
    def bound(self) -> str:
        return bound_of(self.method)


@dataclass
class SnapshotEstimate:
    """Size estimate for a single snapshot transfer.

    Attributes:
        name: Snapshot name
        full_size: Total size of snapshot data
        incremental_size: Estimated incremental transfer size (if parent available)
        parent_name: Name of parent snapshot for incremental
        is_incremental: Whether this would be an incremental transfer
        method: Method used to estimate ("subvolume_show", "filesystem_du", "du", "send_estimate")
    """

    name: str
    full_size: Optional[int] = None
    incremental_size: Optional[int] = None
    parent_name: Optional[str] = None
    is_incremental: bool = False
    method: str = "unknown"

    @property
    def counts_at_full_size(self) -> bool:
        """Whether this snapshot's full size is what the transfer total counts."""
        return not (self.is_incremental and self.incremental_size)


@dataclass
class TransferEstimate:
    """Aggregate estimate for a backup operation.

    Attributes:
        snapshots: List of individual snapshot estimates
        total_full_size: Sum of all full sizes
        total_incremental_size: Sum of incremental sizes (what would actually transfer)
        snapshot_count: Number of snapshots
        new_snapshot_count: Snapshots that need to be transferred
        skipped_count: Snapshots already at destination
        estimation_time: Time taken to compute estimates
    """

    snapshots: list[SnapshotEstimate] = field(default_factory=list)
    total_full_size: int = 0
    total_incremental_size: int = 0
    snapshot_count: int = 0
    new_snapshot_count: int = 0
    skipped_count: int = 0
    estimation_time: float = 0.0

    def add_snapshot(self, estimate: SnapshotEstimate) -> None:
        """Add a snapshot estimate to the aggregate."""
        self.snapshots.append(estimate)
        self.snapshot_count += 1

        if estimate.full_size:
            self.total_full_size += estimate.full_size

        if estimate.is_incremental and estimate.incremental_size:
            self.total_incremental_size += estimate.incremental_size
        elif estimate.full_size:
            self.total_incremental_size += estimate.full_size

    @property
    def unmeasured_count(self) -> int:
        """Snapshots whose transfer size could not be determined at all.

        Such a snapshot contributes nothing to ``total_incremental_size``, so a
        total of zero means either "nothing to transfer" or "nothing could be
        measured" -- opposite facts about whether it is safe to proceed. The
        space check used to read a zero total as the former in both cases and
        announce "No data to transfer", skipping the check precisely when the
        numbers backing it were missing.
        """
        unmeasured = 0
        for estimate in self.snapshots:
            if estimate.is_incremental and estimate.incremental_size is not None:
                continue
            if estimate.full_size is not None:
                continue
            unmeasured += 1
        return unmeasured

    @property
    def total_is_lower_bound(self) -> bool:
        """Whether the transfer total is a floor rather than an estimate.

        True as soon as ONE incremental contributed an underestimating method:
        the sum is then at least this, and there is no honest way to say by how
        much it might exceed it. Reported so the caller can label the number
        instead of quoting it with a confidence it does not have.
        """
        return any(
            snapshot.is_incremental
            and snapshot.incremental_size
            and snapshot.method in UNDERESTIMATING_METHODS
            for snapshot in self.snapshots
        )

    @property
    def total_is_upper_bound(self) -> bool:
        """Whether the transfer total is a ceiling rather than an estimate.

        True when a snapshot counted at a full size that an overcounting
        method produced (``du``, taken where nothing better could measure)
        and no floor is in the sum. A floor and a ceiling added together
        bound nothing, so a total holding both keeps the floor's label: it
        is at least that much, and that is all that can be said.
        """
        return not self.total_is_lower_bound and any(
            snapshot.counts_at_full_size
            and snapshot.full_size
            and snapshot.method in OVERCOUNTING_METHODS
            for snapshot in self.snapshots
        )

    @property
    def total_bound(self) -> str:
        """``"lower"``, ``"upper"`` or ``"measurement"``, as ``bound_of`` names them."""
        if self.total_is_lower_bound:
            return "lower"
        if self.total_is_upper_bound:
            return "upper"
        return "measurement"


def format_size(size_bytes: Optional[int]) -> str:
    """Format byte size in human readable form.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string like "1.23 GiB"
    """
    if size_bytes is None:
        return "unknown"

    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes / 1024:.2f} KiB"
    elif size_bytes < 1024**3:
        return f"{size_bytes / 1024**2:.2f} MiB"
    elif size_bytes < 1024**4:
        return f"{size_bytes / 1024**3:.2f} GiB"
    else:
        return f"{size_bytes / 1024**4:.2f} TiB"


def _run_measurement(
    cmd: list[str],
    timeout: int,
    parse: Callable[[Any], Optional[int]],
    *,
    elevate: bool,
    unprivileged: bool,
    text: bool = True,
) -> Optional[int]:
    """Run one measuring command and return what ``parse`` makes of its output.

    The one place a size measurement decides how to escalate. With
    ``elevate`` the command runs through ``sudo -n`` first: snapshots are
    usually made by root with their modes kept, so a snapshot of ``/`` holds
    directories nobody else can walk, and an attempt as this user would walk
    the whole tree to fail. A probe never prompts. When the elevated run
    gives no answer -- sudo refuses, is not installed, or the tool failed
    under it -- and the method has an ``unprivileged`` form, the command runs
    once more as this user. The same tool run by the same user returns the
    same figure or none, so a retry that was not needed costs a walk of the
    tree, never a different number; that is why sudo's refusal is not
    detected by matching its text, which is localised. As root the command
    runs directly, once. A timeout ends the method, elevated or not: the
    second walk would be the same walk.
    """
    if not elevate:
        argvs = [cmd]
    else:
        try:
            prefix: Optional[list[str]] = __util__.sudo_argv()
        except __util__.AbortError as e:
            logger.debug("%s is not elevated: %s", cmd[0], e)
            prefix = None
        if prefix == []:
            argvs = [cmd]  # root runs it as it is
        elif prefix:
            argvs = [[*prefix, *cmd], cmd] if unprivileged else [[*prefix, *cmd]]
        else:
            argvs = [cmd] if unprivileged else []
    for argv in argvs:
        try:
            result = subprocess.run(
                argv, capture_output=True, text=text, timeout=timeout
            )
        except subprocess.TimeoutExpired as e:
            logger.debug("%s timed out: %s", cmd[0], e)
            return None
        except OSError as e:
            logger.debug("%s could not be run: %s", cmd[0], e)
            continue
        if result.returncode != 0:
            logger.debug("%s exited %d", " ".join(argv), result.returncode)
            continue
        answer = parse(result.stdout)
        if answer is not None:
            return answer
        logger.debug("%s gave no usable answer", " ".join(argv))
    return None


def _parse_subvolume_show(out: str) -> Optional[int]:
    """The ``Exclusive`` line of ``btrfs subvolume show``, when quotas give one.

    btrfs-progs 7.1 prints the field as ``Usage exclusive``, which this does
    not match, and exclusive bytes are what is unique to this subvolume, not
    what a full send carries; a test pins that a ``Usage exclusive`` line is
    not taken as the size, so that remains a decision rather than an accident.
    """
    for line in out.splitlines():
        if line.strip().startswith("Exclusive"):
            parts = line.split(":")
            if len(parts) >= 2:
                parsed = _parse_size(parts[1].strip())
                if parsed and parsed > 0:
                    return parsed
    return None


def _parse_filesystem_du(out: str) -> Optional[int]:
    """The Total of ``btrfs filesystem du -s --raw``; a Total of 0 is no answer.

    The tool exits 0 with a Total of 0 for a tree it cannot read in full and
    for a tree of files small enough to live inline in metadata, so 0 is
    "not measured", never a size. The progress estimate used to return it as
    one, and the space check then passed the transfer against nothing.
    """
    lines = out.strip().splitlines()
    if len(lines) >= 2:
        parts = lines[1].split()
        if parts:
            try:
                total = int(parts[0])
            except ValueError:
                return None
            if total > 0:
                return total
    return None


def _parse_du(out: str) -> Optional[int]:
    """The byte count of ``du -sb``; 0 is no answer."""
    parts = out.split()
    if parts:
        try:
            size = int(parts[0])
        except ValueError:
            return None
        if size > 0:
            return size
    return None


def estimate_snapshot_full_size(
    snapshot_path: str | Path,
    use_sudo: bool = False,
) -> tuple[Optional[int], str]:
    """Estimate the full size of a snapshot.

    Tries multiple methods in order of preference:
    1. btrfs subvolume show (requires quotas, exclusive data); elevated
       only, since it cannot read a subvolume without privilege
    2. btrfs filesystem du (referenced data, shared extents once)
    3. Regular du (apparent size: an upper bound, see OVERCOUNTING_METHODS)

    Each method is elevated through ``sudo -n`` when not root, and the two
    that need no privilege on a readable tree run once more as this user
    when sudo gave no answer (``_run_measurement``). Root never asks sudo.

    Args:
        snapshot_path: Path to the snapshot
        use_sudo: Elevate even where the uid alone would not

    Returns:
        Tuple of (size_in_bytes, method_used)
    """
    path = str(snapshot_path)
    elevate = use_sudo or os.geteuid() != 0
    methods: tuple[
        tuple[str, list[str], int, Callable[[Any], Optional[int]], bool], ...
    ]
    methods = (
        (
            "subvolume_show",
            ["btrfs", "subvolume", "show", path],
            30,
            _parse_subvolume_show,
            False,
        ),
        (
            "filesystem_du",
            ["btrfs", "filesystem", "du", "-s", "--raw", path],
            120,
            _parse_filesystem_du,
            True,
        ),
        ("du", ["du", "-sb", path], 120, _parse_du, True),
    )
    for method, cmd, timeout, parse, unprivileged in methods:
        size = _run_measurement(
            cmd, timeout, parse, elevate=elevate, unprivileged=unprivileged
        )
        if size is not None:
            return size, method
    return None, "failed"


def measure_snapshot_full_size(
    snapshot_path: str | Path,
    use_sudo: bool = False,
) -> Optional[SizeEstimate]:
    """``estimate_snapshot_full_size`` as a ``SizeEstimate``, None when it failed."""
    size, method = estimate_snapshot_full_size(snapshot_path, use_sudo)
    return None if size is None else SizeEstimate(size, method)


def estimate_incremental_size(
    snapshot_path: str | Path,
    parent_path: str | Path,
    use_sudo: bool = False,
) -> tuple[Optional[int], str]:
    """Estimate the incremental transfer size between snapshots.

    Uses 'btrfs send --no-data -p <parent>' to calculate stream size without
    actually transferring data. This gives an accurate estimate of what would
    be transferred.

    Args:
        snapshot_path: Path to the snapshot to send
        parent_path: Path to the parent snapshot
        use_sudo: Whether to use sudo

    Returns:
        Tuple of (size_in_bytes, method_used)
    """
    snapshot_path = str(snapshot_path)
    parent_path = str(parent_path)
    elevate = use_sudo or os.geteuid() != 0

    # Use btrfs send --no-data to estimate stream size. A send needs
    # CAP_SYS_ADMIN, so there is no unprivileged form to fall back to.
    stream_size = _run_measurement(
        ["btrfs", "send", "--no-data", "-p", parent_path, snapshot_path],
        300,  # 5 minutes timeout
        len,
        elevate=elevate,
        unprivileged=False,
        text=False,
    )
    if stream_size is not None:
        # The --no-data option produces a stream with metadata only. Its size
        # is a lower bound: empirically, actual data is roughly 10-100x the
        # metadata stream, but the ratio is fairly consistent, so it indicates
        # the relative change size.
        logger.debug("Incremental send --no-data stream size: %d bytes", stream_size)
        return stream_size, "send_no_data"

    # Fallback: compare exclusive sizes
    try:
        snap_size, _ = estimate_snapshot_full_size(snapshot_path, use_sudo)
        parent_size, _ = estimate_snapshot_full_size(parent_path, use_sudo)

        if snap_size and parent_size:
            # Rough estimate: difference in exclusive sizes
            # This is very approximate but better than nothing
            diff = max(0, snap_size - parent_size)
            return diff, "size_diff"
    except Exception as e:
        logger.debug("Size diff estimation failed: %s", e)

    return None, "failed"


def estimate_transfer(
    source_endpoint,
    dest_endpoint,
    snapshots: list | None = None,
) -> TransferEstimate:
    """Estimate the size of a backup transfer operation.

    Args:
        source_endpoint: Source endpoint with snapshots
        dest_endpoint: Destination endpoint
        snapshots: Optional list of snapshots to estimate (None = all)

    Returns:
        TransferEstimate with aggregate statistics
    """
    start_time = time.time()
    estimate = TransferEstimate()

    # Get snapshots from source
    if snapshots is None:
        source_snapshots = source_endpoint.list_snapshots()
    else:
        source_snapshots = snapshots

    # Get existing snapshots at destination
    try:
        dest_snapshots = dest_endpoint.list_snapshots()
        dest_names = {s.get_name() for s in dest_snapshots}
    except Exception:
        dest_names = set()

    # Use sudo if configured
    use_sudo = source_endpoint.config.get("ssh_sudo", False)
    # A remote source's snapshots live on the remote host; the local ladder
    # aimed at config["path"] / name measured nothing there and every row read
    # "unknown". The endpoint that holds them measures them, as the engine's
    # space check already asks (measure_transfer_size). The incremental
    # estimate runs `btrfs send --no-data` locally and has no remote form, so a
    # remote source gets full sizes only, said once below.
    remote_source = getattr(source_endpoint, "_is_remote", False) is True
    if remote_source:
        logger.info(
            "The source is on %s: full sizes are measured there; incremental "
            "sizes are not estimated for a remote source.",
            getattr(source_endpoint, "hostname", "the remote host"),
        )

    # Sort snapshots by time for incremental parent detection. A snapshot
    # whose name yields no timestamp is not part of the estimate -- the
    # planner will not transfer it -- and is reported, not silently dropped.
    undated = [s for s in source_snapshots if getattr(s, "time_obj", None) is None]
    if undated:
        logger.info(
            "%d snapshot(s) have no derivable timestamp and are not part of "
            "the transfer estimate: %s",
            len(undated),
            ", ".join(s.get_name() for s in undated[:5])
            + (f" (and {len(undated) - 5} more)" if len(undated) > 5 else ""),
        )
    sorted_snapshots = sorted(
        (s for s in source_snapshots if getattr(s, "time_obj", None) is not None),
        key=lambda s: s.time_obj,
    )

    last_snapshot = None
    for snap in sorted_snapshots:
        name = snap.get_name()

        # Check if already at destination
        if name in dest_names:
            estimate.skipped_count += 1
            continue

        # Estimate full size. A remote endpoint's path is a string naming a
        # directory on the other host, not a local Path, and is never joined.
        if remote_source:
            measured = source_endpoint.measure_transfer_size(snap)
            full_size = measured.bytes if measured is not None else None
            method = measured.method if measured is not None else "failed"
        else:
            snap_path = source_endpoint.config["path"] / name
            full_size, method = estimate_snapshot_full_size(snap_path, use_sudo)

        # Check for incremental opportunity
        parent_name = None
        incremental_size = None
        is_incremental = False

        if last_snapshot and not remote_source:
            parent_path = source_endpoint.config["path"] / last_snapshot.get_name()
            if parent_path.exists():
                parent_name = last_snapshot.get_name()
                incr_size, incr_method = estimate_incremental_size(
                    snap_path, parent_path, use_sudo
                )
                if incr_size is not None:
                    incremental_size = incr_size
                    is_incremental = True
                    method = incr_method

        snap_estimate = SnapshotEstimate(
            name=name,
            full_size=full_size,
            incremental_size=incremental_size,
            parent_name=parent_name,
            is_incremental=is_incremental,
            method=method,
        )

        estimate.add_snapshot(snap_estimate)
        estimate.new_snapshot_count += 1
        last_snapshot = snap

    estimate.estimation_time = time.time() - start_time
    return estimate


def print_estimate(
    estimate: TransferEstimate,
    source_name: str = "source",
    dest_name: str = "destination",
) -> None:
    """Print a formatted transfer estimate.

    Args:
        estimate: The transfer estimate to print
        source_name: Display name for source
        dest_name: Display name for destination
    """
    print("\nBackup Size Estimate")
    print(f"{'=' * 60}")
    print(f"Source: {source_name}")
    print(f"Destination: {dest_name}")
    print()

    if estimate.skipped_count > 0:
        print(f"Snapshots already at destination: {estimate.skipped_count}")

    if estimate.new_snapshot_count == 0:
        print("No new snapshots to transfer.")
        return

    print(f"Snapshots to transfer: {estimate.new_snapshot_count}")
    print()

    # Table header
    print(f"{'Snapshot':<40} {'Size':<12} {'Type':<12} {'Parent'}")
    print(f"{'-' * 40} {'-' * 12} {'-' * 12} {'-' * 20}")

    for snap in estimate.snapshots:
        if snap.is_incremental and snap.incremental_size:
            size_str = format_size(snap.incremental_size)
            if snap.method in UNDERESTIMATING_METHODS:
                # Marked per row as well as in the total: a reader scanning the
                # table for the big one must not take these at face value.
                size_str = f">= {size_str}"
            type_str = "incremental"
            parent_str = snap.parent_name or ""
        else:
            size_str = format_size(snap.full_size)
            if snap.full_size and snap.method in OVERCOUNTING_METHODS:
                size_str = f"<= {size_str}"
            type_str = "full"
            parent_str = ""

        # Truncate long names
        name = snap.name[:38] + ".." if len(snap.name) > 40 else snap.name

        print(f"{name:<40} {size_str:<12} {type_str:<12} {parent_str}")

    print()
    print(f"{'-' * 60}")
    if estimate.total_is_lower_bound:
        print(
            f"Total data to transfer: AT LEAST "
            f"{format_size(estimate.total_incremental_size)}"
        )
        print("  Incremental sizes are measured with `btrfs send --no-data`, which")
        print("  carries metadata and no file contents, so the real transfer is")
        print("  larger -- often by a factor of 10-100. Treat this as a floor, not")
        print("  a figure to size a link or a maintenance window against. A full")
        print("  transfer (no parent) is measured directly and is not affected.")
    elif estimate.total_is_upper_bound:
        print(
            f"Total data to transfer: AT MOST "
            f"{format_size(estimate.total_incremental_size)}"
        )
        print("  A size marked <= was measured with `du`, which counts sparse-file")
        print("  holes and data shared between files in full; a send stream skips")
        print("  the holes and carries shared data once. `btrfs filesystem du`,")
        print("  which measures what the stream carries, could not be run here.")
    else:
        print(f"Total data to transfer: {format_size(estimate.total_incremental_size)}")
    print(f"Full size (uncompressed): {format_size(estimate.total_full_size)}")
    print(f"Estimation time: {estimate.estimation_time:.2f}s")


def _parse_size(size_str: str) -> Optional[int]:
    """Parse a size string like '1.23GiB', '512M' or '1024' to bytes.

    The one table: the progress module and the ssh endpoint read it through
    their old names. A bare K, M, G or T is binary, as btrfs prints them.
    """
    size_str = size_str.strip()

    # Check longer suffixes first to avoid "B" matching "GiB"
    multipliers = [
        ("TiB", 1024**4),
        ("GiB", 1024**3),
        ("MiB", 1024**2),
        ("KiB", 1024),
        ("TB", 1000**4),
        ("GB", 1000**3),
        ("MB", 1000**2),
        ("KB", 1000),
        ("B", 1),
        ("T", 1024**4),
        ("G", 1024**3),
        ("M", 1024**2),
        ("K", 1024),
    ]

    for suffix, multiplier in multipliers:
        if size_str.endswith(suffix):
            try:
                value = float(size_str[: -len(suffix)].strip())
                return int(value * multiplier)
            except ValueError:
                return None

    try:
        return int(float(size_str))
    except ValueError:
        return None
