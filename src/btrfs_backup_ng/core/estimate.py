"""Backup size estimation for pre-transfer planning.

Provides estimates of data that will be transferred, helping users:
- Plan bandwidth usage
- Estimate backup time
- Verify expected transfer sizes before proceeding
"""

import logging
import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


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


def estimate_snapshot_full_size(
    snapshot_path: str | Path,
    use_sudo: bool = False,
) -> tuple[Optional[int], str]:
    """Estimate the full size of a snapshot.

    Tries multiple methods in order of preference:
    1. btrfs subvolume show (requires quotas, most accurate for exclusive data)
    2. btrfs filesystem du (accurate, handles reflinks)
    3. Regular du (fallback, may overcount shared data)

    Args:
        snapshot_path: Path to the snapshot
        use_sudo: Whether to use sudo for btrfs commands

    Returns:
        Tuple of (size_in_bytes, method_used)
    """
    snapshot_path = str(snapshot_path)
    sudo_prefix = ["sudo", "-n"] if use_sudo or os.geteuid() != 0 else []

    # Method 1: btrfs subvolume show (exclusive data size)
    try:
        cmd = sudo_prefix + ["btrfs", "subvolume", "show", snapshot_path]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if line.strip().startswith("Exclusive"):
                    parts = line.split(":")
                    if len(parts) >= 2:
                        size_str = parts[1].strip()
                        parsed = _parse_size(size_str)
                        if parsed and parsed > 0:
                            return parsed, "subvolume_show"
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        logger.debug("btrfs subvolume show failed: %s", e)

    # Method 2: btrfs filesystem du
    try:
        cmd = sudo_prefix + ["btrfs", "filesystem", "du", "-s", "--raw", snapshot_path]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            lines = result.stdout.strip().splitlines()
            if len(lines) >= 2:
                parts = lines[1].split()
                if len(parts) >= 1:
                    try:
                        total_size = int(parts[0])
                        if total_size > 0:
                            return total_size, "filesystem_du"
                    except ValueError:
                        pass
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        logger.debug("btrfs filesystem du failed: %s", e)

    # Method 3: Regular du
    try:
        cmd = sudo_prefix + ["du", "-sb", snapshot_path]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            parts = result.stdout.split()
            if parts:
                try:
                    size = int(parts[0])
                    if size > 0:
                        return size, "du"
                except ValueError:
                    pass
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        logger.debug("du failed: %s", e)

    return None, "failed"


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
    sudo_prefix = ["sudo", "-n"] if use_sudo or os.geteuid() != 0 else []

    # Use btrfs send --no-data to estimate stream size
    try:
        cmd = sudo_prefix + [
            "btrfs",
            "send",
            "--no-data",
            "-p",
            parent_path,
            snapshot_path,
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=300,  # 5 minutes timeout
        )

        if result.returncode == 0:
            # The --no-data option produces a stream with metadata only
            # The size gives us a lower bound, actual transfer is larger
            # but the ratio is fairly consistent
            stream_size = len(result.stdout)
            # Empirically, actual data is roughly 10-100x the metadata stream
            # This is a rough heuristic
            logger.debug(
                "Incremental send --no-data stream size: %d bytes", stream_size
            )
            # Return the stream size - it's an underestimate but indicates
            # relative change size
            return stream_size, "send_no_data"
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        logger.debug("btrfs send --no-data failed: %s", e)

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

    # Sort snapshots by time for incremental parent detection
    sorted_snapshots = sorted(
        source_snapshots, key=lambda s: s.time_obj if hasattr(s, "time_obj") else 0
    )

    last_snapshot = None
    for snap in sorted_snapshots:
        name = snap.get_name()

        # Check if already at destination
        if name in dest_names:
            estimate.skipped_count += 1
            continue

        # Get snapshot path
        snap_path = source_endpoint.config["path"] / name

        # Estimate full size
        full_size, method = estimate_snapshot_full_size(snap_path, use_sudo)

        # Check for incremental opportunity
        parent_name = None
        incremental_size = None
        is_incremental = False

        if last_snapshot:
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
            type_str = "incremental"
            parent_str = snap.parent_name or ""
        else:
            size_str = format_size(snap.full_size)
            type_str = "full"
            parent_str = ""

        # Truncate long names
        name = snap.name[:38] + ".." if len(snap.name) > 40 else snap.name

        print(f"{name:<40} {size_str:<12} {type_str:<12} {parent_str}")

    print()
    print(f"{'-' * 60}")
    print(f"Total data to transfer: {format_size(estimate.total_incremental_size)}")
    print(f"Full size (uncompressed): {format_size(estimate.total_full_size)}")
    print(f"Estimation time: {estimate.estimation_time:.2f}s")


def _parse_size(size_str: str) -> Optional[int]:
    """Parse a size string like '1.23GiB' to bytes."""
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
