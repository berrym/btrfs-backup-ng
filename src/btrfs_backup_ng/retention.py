"""Time-based retention policy implementation.

Implements a clear, predictable retention model:

1. `min` - Absolute minimum retention period. Nothing deleted before this age.
2. Time buckets (hourly, daily, weekly, monthly, yearly) evaluated newest-to-oldest.
3. First snapshot in each bucket is kept.
4. Latest snapshot is always preserved.
5. Snapshots needed for incremental chains are preserved automatically.

Example:
    min = "1d"      # Keep everything for at least 1 day
    hourly = 24     # Then keep 24 hourly (1 per hour)
    daily = 7       # Then keep 7 daily (1 per day)
    weekly = 4      # Then keep 4 weekly
    monthly = 12    # Then keep 12 monthly
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

from .config import RetentionConfig

logger = logging.getLogger(__name__)


# Duration parsing regex
DURATION_PATTERN = re.compile(r"^(?P<value>\d+)\s*(?P<unit>[smhdwMy])$")

DURATION_UNITS = {
    "s": "seconds",
    "m": "minutes",
    "h": "hours",
    "d": "days",
    "w": "weeks",
    "M": "months",  # Approximate: 30 days
    "y": "years",  # Approximate: 365 days
}


def parse_duration(duration_str: str) -> timedelta:
    """Parse a duration string into a timedelta.

    Supported formats:
        - "30s" - 30 seconds
        - "5m" - 5 minutes
        - "2h" - 2 hours
        - "1d" - 1 day
        - "1w" - 1 week
        - "1M" - 1 month (30 days)
        - "1y" - 1 year (365 days)

    Args:
        duration_str: Duration string to parse

    Returns:
        timedelta representing the duration

    Raises:
        ValueError: If the duration string is invalid
    """
    duration_str = duration_str.strip()

    match = DURATION_PATTERN.match(duration_str)
    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}")

    value = int(match.group("value"))
    unit = match.group("unit")

    if unit == "s":
        return timedelta(seconds=value)
    elif unit == "m":
        return timedelta(minutes=value)
    elif unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)
    elif unit == "w":
        return timedelta(weeks=value)
    elif unit == "M":
        return timedelta(days=value * 30)  # Approximate
    elif unit == "y":
        return timedelta(days=value * 365)  # Approximate
    else:
        raise ValueError(f"Unknown duration unit: {unit}")


@dataclass
class SnapshotInfo:
    """Information about a snapshot for retention processing."""

    name: str
    timestamp: datetime
    snapshot: object  # The actual snapshot object
    keep: bool = False
    keep_reason: str = ""


def extract_timestamp(snapshot_name: str, prefix: str = "") -> datetime | None:
    """Extract timestamp from snapshot name.

    Tries multiple common timestamp formats.

    Args:
        snapshot_name: Name of the snapshot
        prefix: Optional prefix to strip

    Returns:
        datetime if parsed successfully, None otherwise
    """
    name = snapshot_name
    if prefix and name.startswith(prefix):
        name = name[len(prefix) :]

    # Common timestamp formats
    formats = [
        "%Y%m%d-%H%M%S",  # 20240115-143022
        "%Y-%m-%d_%H%M%S",  # 2024-01-15_143022
        "%Y-%m-%d-%H%M%S",  # 2024-01-15-143022
        "%Y%m%d%H%M%S",  # 20240115143022
        "%Y-%m-%dT%H:%M:%S",  # 2024-01-15T14:30:22
    ]

    for fmt in formats:
        try:
            return datetime.strptime(name, fmt)
        except ValueError:
            continue

    # Try to find timestamp pattern anywhere in name
    patterns = [
        (r"(\d{8})-(\d{6})", "%Y%m%d-%H%M%S"),
        (r"(\d{8})_(\d{6})", "%Y%m%d_%H%M%S"),
        (r"(\d{14})", "%Y%m%d%H%M%S"),
    ]

    for pattern, fmt in patterns:
        match = re.search(pattern, name)
        if match:
            try:
                timestamp_str = "".join(match.groups())
                # Reconstruct with separator if needed
                if "-" in fmt or "_" in fmt:
                    timestamp_str = match.group(0)
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue

    return None


def get_bucket_key(timestamp: datetime, bucket_type: str) -> str:
    """Get the bucket key for a timestamp.

    Args:
        timestamp: The timestamp to bucket
        bucket_type: One of 'hourly', 'daily', 'weekly', 'monthly', 'yearly'

    Returns:
        String key representing the bucket
    """
    if bucket_type == "hourly":
        return timestamp.strftime("%Y-%m-%d-%H")
    elif bucket_type == "daily":
        return timestamp.strftime("%Y-%m-%d")
    elif bucket_type == "weekly":
        # ISO week number
        return timestamp.strftime("%Y-W%W")
    elif bucket_type == "monthly":
        return timestamp.strftime("%Y-%m")
    elif bucket_type == "yearly":
        return timestamp.strftime("%Y")
    else:
        raise ValueError(f"Unknown bucket type: {bucket_type}")


def apply_retention(
    snapshots: list,
    config: RetentionConfig,
    get_name: Callable[[Any], str] | None = None,
    prefix: str = "",
    now: datetime | None = None,
) -> tuple[list, list]:
    """Apply retention policy to a list of snapshots.

    Args:
        snapshots: List of snapshot objects
        config: Retention configuration
        get_name: Function to get name from snapshot (default: str(snapshot))
        prefix: Snapshot name prefix to strip for timestamp parsing
        now: Current time (default: datetime.now())

    Returns:
        Tuple of (snapshots_to_keep, snapshots_to_delete)
    """
    if not snapshots:
        return [], []

    if now is None:
        now = datetime.now()

    # Use provided get_name or default to str()
    name_func: Callable[[Any], str] = get_name if get_name is not None else str

    # Parse minimum retention duration
    try:
        min_age = parse_duration(config.min)
    except ValueError:
        logger.warning("Invalid min duration '%s', defaulting to 1 day", config.min)
        min_age = timedelta(days=1)

    min_cutoff = now - min_age

    # Build snapshot info list with timestamps
    snapshot_infos: list[SnapshotInfo] = []
    for snap in snapshots:
        name = name_func(snap)
        timestamp = extract_timestamp(name, prefix)

        if timestamp is None:
            # Can't parse timestamp, keep it to be safe
            logger.debug("Cannot parse timestamp from '%s', keeping", name)
            snapshot_infos.append(
                SnapshotInfo(
                    name=name,
                    timestamp=now,  # Treat as current
                    snapshot=snap,
                    keep=True,
                    keep_reason="unparseable timestamp",
                )
            )
        else:
            snapshot_infos.append(
                SnapshotInfo(
                    name=name,
                    timestamp=timestamp,
                    snapshot=snap,
                )
            )

    # Sort by timestamp, newest first
    snapshot_infos.sort(key=lambda s: s.timestamp, reverse=True)

    # Rule 1: Always keep the latest snapshot
    if snapshot_infos and not snapshot_infos[0].keep:
        snapshot_infos[0].keep = True
        snapshot_infos[0].keep_reason = "latest"

    # Rule 2: Keep everything within min retention period
    for info in snapshot_infos:
        if not info.keep and info.timestamp >= min_cutoff:
            info.keep = True
            info.keep_reason = f"within min ({config.min})"

    # Rule 3: Apply time bucket retention
    bucket_types = [
        ("hourly", config.hourly),
        ("daily", config.daily),
        ("weekly", config.weekly),
        ("monthly", config.monthly),
        ("yearly", config.yearly),
    ]

    for bucket_type, count in bucket_types:
        if count <= 0:
            continue

        _apply_bucket_retention(snapshot_infos, bucket_type, count, min_cutoff)

    # Split into keep and delete lists
    to_keep = [info.snapshot for info in snapshot_infos if info.keep]
    to_delete = [info.snapshot for info in snapshot_infos if not info.keep]

    # Log decisions
    logger.debug("Retention decisions for %d snapshots:", len(snapshot_infos))
    for info in snapshot_infos:
        status = "KEEP" if info.keep else "DELETE"
        reason = f" ({info.keep_reason})" if info.keep_reason else ""
        logger.debug("  %s: %s%s", status, info.name, reason)

    return to_keep, to_delete


def _apply_bucket_retention(
    snapshot_infos: list[SnapshotInfo],
    bucket_type: str,
    count: int,
    min_cutoff: datetime,
) -> None:
    """Apply bucket-based retention to snapshot list.

    Modifies snapshot_infos in place, setting keep=True for retained snapshots.

    Args:
        snapshot_infos: List of SnapshotInfo (sorted newest-first)
        bucket_type: Type of bucket (hourly, daily, etc.)
        count: Number of buckets to keep
        min_cutoff: Minimum retention cutoff time
    """
    buckets_seen: dict[str, SnapshotInfo] = {}

    # Process oldest-to-newest so we keep the oldest in each bucket
    for info in reversed(snapshot_infos):
        # Skip if already within min retention
        if info.timestamp >= min_cutoff:
            continue

        bucket_key = get_bucket_key(info.timestamp, bucket_type)

        if bucket_key not in buckets_seen:
            buckets_seen[bucket_key] = info

    # Keep the first `count` buckets (sorted by bucket key, newest first)
    sorted_buckets = sorted(buckets_seen.keys(), reverse=True)[:count]

    for bucket_key in sorted_buckets:
        info = buckets_seen[bucket_key]
        if not info.keep:
            info.keep = True
            info.keep_reason = f"{bucket_type} ({bucket_key})"


def format_retention_summary(
    to_keep: list,
    to_delete: list,
    get_name: Callable[[object], str] | None = None,
) -> str:
    """Format a human-readable retention summary.

    Args:
        to_keep: Snapshots being kept
        to_delete: Snapshots being deleted
        get_name: Function to get name from snapshot

    Returns:
        Formatted summary string
    """
    # Use provided get_name or default to str()
    name_func: Callable[[object], str] = get_name if get_name is not None else str

    lines = [
        f"Retention: keeping {len(to_keep)}, deleting {len(to_delete)}",
    ]

    if to_delete:
        lines.append("To delete:")
        for snap in to_delete[:10]:
            lines.append(f"  - {name_func(snap)}")
        if len(to_delete) > 10:
            lines.append(f"  ... and {len(to_delete) - 10} more")

    return "\n".join(lines)
