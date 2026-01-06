"""Space availability checking for backup operations.

Provides pre-flight checks to verify sufficient space exists at destinations
before transfers begin, including btrfs quota (qgroup) awareness.
"""

import logging
import os
import subprocess
from dataclasses import dataclass
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# Default safety margin: 10%
DEFAULT_SAFETY_MARGIN_PERCENT = 10.0
# Minimum safety margin in bytes: 100 MiB
MIN_SAFETY_BYTES = 100 * 1024 * 1024


@dataclass
class SpaceInfo:
    """Information about available space at a location.

    Attributes:
        path: The path queried for space information
        total_bytes: Total filesystem capacity in bytes
        used_bytes: Currently used space in bytes
        available_bytes: Available space for non-root users in bytes
        quota_enabled: Whether btrfs quotas (qgroups) are active
        quota_limit: Quota limit in bytes (None = no limit/unlimited)
        quota_used: Current quota usage in bytes
        source: Method used to obtain info ("statvfs", "btrfs_qgroup", "df")
    """

    path: str
    total_bytes: int
    used_bytes: int
    available_bytes: int
    quota_enabled: bool = False
    quota_limit: Optional[int] = None
    quota_used: Optional[int] = None
    source: str = "unknown"

    @property
    def quota_remaining(self) -> Optional[int]:
        """Calculate remaining quota space, if quotas are enabled."""
        if not self.quota_enabled or self.quota_limit is None:
            return None
        if self.quota_used is None:
            return self.quota_limit
        return max(0, self.quota_limit - self.quota_used)

    @property
    def effective_available(self) -> int:
        """Return the more restrictive of filesystem space or quota remaining."""
        quota_rem = self.quota_remaining
        if quota_rem is not None:
            return min(self.available_bytes, quota_rem)
        return self.available_bytes


@dataclass
class SpaceCheck:
    """Result of checking space availability for an operation.

    Attributes:
        space_info: The underlying space information
        estimated_size: Estimated bytes required for the operation
        sufficient: Whether there is sufficient space (with safety margin)
        safety_margin_percent: Safety margin percentage applied
        effective_limit: The effective space limit (min of fs and quota)
        required_with_margin: Total bytes needed including safety margin
        available_after: Estimated available space after operation
        warning_message: Optional warning about the space check
    """

    space_info: SpaceInfo
    estimated_size: int
    sufficient: bool
    safety_margin_percent: float = DEFAULT_SAFETY_MARGIN_PERCENT
    effective_limit: int = 0
    required_with_margin: int = 0
    available_after: int = 0
    warning_message: Optional[str] = None


def get_filesystem_space(
    path: str,
    exec_func: Optional[Callable] = None,
) -> tuple[int, int, int]:
    """Get filesystem space information for a path.

    Args:
        path: Path on the filesystem to check
        exec_func: Optional execution function for remote commands.
                   If None, uses local os.statvfs().

    Returns:
        Tuple of (total_bytes, used_bytes, available_bytes)

    Raises:
        OSError: If the path cannot be accessed
    """
    if exec_func is None:
        # Local filesystem check using statvfs
        stat = os.statvfs(path)
        total = stat.f_blocks * stat.f_frsize
        available = stat.f_bavail * stat.f_frsize  # Available to non-root
        used = (stat.f_blocks - stat.f_bfree) * stat.f_frsize
        logger.debug(
            "statvfs for %s: total=%d, used=%d, available=%d",
            path,
            total,
            used,
            available,
        )
        return total, used, available
    else:
        # Remote execution - use Python one-liner via exec_func
        # The exec_func should return stdout as string
        python_cmd = (
            f"import os,json; s=os.statvfs({path!r}); "
            f"print(json.dumps({{'total':s.f_blocks*s.f_frsize,"
            f"'used':(s.f_blocks-s.f_bfree)*s.f_frsize,"
            f"'available':s.f_bavail*s.f_frsize}}))"
        )
        result = exec_func(["python3", "-c", python_cmd])
        if isinstance(result, bytes):
            result = result.decode("utf-8")
        import json

        data = json.loads(result.strip())
        return data["total"], data["used"], data["available"]


def get_btrfs_quota_info(
    path: str,
    exec_func: Optional[Callable] = None,
    use_sudo: bool = False,
) -> Optional[tuple[Optional[int], int]]:
    """Get btrfs quota (qgroup) information for a path.

    Queries the qgroup associated with the subvolume at path and returns
    the quota limit and current usage.

    Args:
        path: Path to a btrfs subvolume or directory within one
        exec_func: Optional execution function for remote commands.
                   If None, runs commands locally.
        use_sudo: Whether to use sudo for btrfs commands

    Returns:
        Tuple of (quota_limit_bytes, quota_used_bytes) or None if quotas
        are not enabled or the query fails. quota_limit_bytes is None if
        no limit is set.
    """
    sudo_prefix = ["sudo", "-n"] if use_sudo or os.geteuid() != 0 else []
    # Use --raw for byte values, -r for referenced/exclusive, -e for exclusive
    # We query the specific path to get its qgroup info
    cmd = sudo_prefix + ["btrfs", "qgroup", "show", "-re", "--raw", path]

    try:
        if exec_func is None:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                logger.debug(
                    "btrfs qgroup show failed (quotas may not be enabled): %s",
                    result.stderr,
                )
                return None
            output = result.stdout
        else:
            try:
                result = exec_func(cmd)
                if isinstance(result, bytes):
                    output = result.decode("utf-8")
                else:
                    output = result
            except Exception as e:
                logger.debug("Remote qgroup query failed: %s", e)
                return None

        return _parse_qgroup_output(output, path)

    except subprocess.TimeoutExpired:
        logger.debug("btrfs qgroup show timed out for %s", path)
        return None
    except FileNotFoundError:
        logger.debug("btrfs command not found")
        return None
    except Exception as e:
        logger.debug("Error getting quota info for %s: %s", path, e)
        return None


def _parse_qgroup_output(output: str, path: str) -> Optional[tuple[Optional[int], int]]:
    """Parse btrfs qgroup show output to extract limit and usage.

    The output format (with --raw) is:
    Qgroupid    Referenced    Exclusive  Max referenced  Max exclusive   Path
    --------    ----------    ---------  --------------  -------------   ----
    0/365            16384        16384       104857600           none   space-test-dest

    Args:
        output: Output from 'btrfs qgroup show -re --raw <path>'
        path: Original path (for logging)

    Returns:
        Tuple of (limit_bytes, used_bytes) where limit_bytes is None for unlimited,
        or None if parsing fails
    """
    lines = output.strip().splitlines()

    # Skip header lines (lines starting with 'Qgroupid' or dashes or empty)
    data_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.lower().startswith("qgroupid") or stripped.startswith("-"):
            continue
        data_lines.append(stripped)

    if not data_lines:
        logger.debug("No qgroup data found in output for %s", path)
        return None

    # Extract the basename of the path to match against qgroup path names
    path_basename = os.path.basename(path.rstrip("/"))

    # Try to find a qgroup line that matches our path
    # Fall back to the last line if no match (which is often the target when
    # querying a specific subvolume path)
    best_match = None
    for line in data_lines:
        parts = line.split()
        if len(parts) < 5:
            continue

        try:
            # Format: qgroupid rfer excl max_rfer max_excl [path]
            # With --raw flag, values are raw bytes
            qgroupid = parts[0]
            rfer = int(parts[1])  # Referenced size (used)
            # excl = int(parts[2])  # Exclusive size
            max_rfer = parts[3]  # Max referenced (limit)
            # max_excl = parts[4]  # Max exclusive

            # Parse max_rfer - could be "none" or a number
            if max_rfer.lower() == "none":
                limit = None
            else:
                limit = int(max_rfer)

            # Check if this line has a path that matches
            if len(parts) >= 6:
                qgroup_path = parts[5]
                if qgroup_path == path_basename or qgroup_path.endswith(
                    "/" + path_basename
                ):
                    logger.debug(
                        "Found matching qgroup %s for %s: used=%d, limit=%s",
                        qgroupid,
                        path,
                        rfer,
                        limit if limit is not None else "unlimited",
                    )
                    return limit, rfer

            # Store as potential fallback (last non-toplevel entry)
            if not qgroupid.endswith("/5"):  # Skip toplevel qgroup
                best_match = (limit, rfer, qgroupid)

        except (ValueError, IndexError) as e:
            logger.debug("Failed to parse qgroup line '%s': %s", line, e)
            continue

    # Use best match if found
    if best_match:
        limit, rfer, qgroupid = best_match
        logger.debug(
            "Using qgroup %s for %s: used=%d, limit=%s",
            qgroupid,
            path,
            rfer,
            limit if limit is not None else "unlimited",
        )
        return limit, rfer

    logger.debug("Could not parse any qgroup data for %s", path)
    return None


def check_space_availability(
    space_info: SpaceInfo,
    required_bytes: int,
    safety_margin_percent: float = DEFAULT_SAFETY_MARGIN_PERCENT,
    min_safety_bytes: int = MIN_SAFETY_BYTES,
) -> SpaceCheck:
    """Check if sufficient space is available for an operation.

    Applies a safety margin to the required bytes and checks against
    the effective available space (considering both filesystem and quota
    limits).

    Args:
        space_info: Space information for the destination
        required_bytes: Estimated bytes needed for the operation
        safety_margin_percent: Percentage to add as safety margin (default 10%)
        min_safety_bytes: Minimum safety margin in bytes (default 100 MiB)

    Returns:
        SpaceCheck with the result of the availability check
    """
    # Calculate effective limit (most restrictive of fs and quota)
    effective_limit = space_info.effective_available

    # Calculate required bytes with safety margin
    margin_bytes = max(
        int(required_bytes * safety_margin_percent / 100),
        min_safety_bytes,
    )
    required_with_margin = required_bytes + margin_bytes

    # Check sufficiency
    sufficient = effective_limit >= required_with_margin

    # Calculate what would remain after the operation
    available_after = max(0, effective_limit - required_bytes)

    # Generate warning messages for edge cases
    warning_message = None
    if not sufficient:
        shortfall = required_with_margin - effective_limit
        warning_message = (
            f"Insufficient space: need {_format_size(required_with_margin)} "
            f"(including {safety_margin_percent:.0f}% safety margin), "
            f"but only {_format_size(effective_limit)} available. "
            f"Short by {_format_size(shortfall)}."
        )
    elif available_after < min_safety_bytes:
        warning_message = (
            f"Warning: Operation would leave only {_format_size(available_after)} "
            f"free space remaining."
        )

    return SpaceCheck(
        space_info=space_info,
        estimated_size=required_bytes,
        sufficient=sufficient,
        safety_margin_percent=safety_margin_percent,
        effective_limit=effective_limit,
        required_with_margin=required_with_margin,
        available_after=available_after,
        warning_message=warning_message,
    )


def get_space_info(
    path: str,
    exec_func: Optional[Callable] = None,
    use_sudo: bool = False,
) -> SpaceInfo:
    """Get complete space information for a path.

    Combines filesystem space information with btrfs quota information
    (if available).

    Args:
        path: Path to check
        exec_func: Optional execution function for remote commands
        use_sudo: Whether to use sudo for btrfs commands

    Returns:
        SpaceInfo with all available space information
    """
    # Get filesystem space
    total, used, available = get_filesystem_space(path, exec_func)

    # Try to get quota info
    quota_info = get_btrfs_quota_info(path, exec_func, use_sudo)

    if quota_info is not None:
        quota_limit, quota_used = quota_info
        return SpaceInfo(
            path=path,
            total_bytes=total,
            used_bytes=used,
            available_bytes=available,
            quota_enabled=True,
            quota_limit=quota_limit,
            quota_used=quota_used,
            source="statvfs+btrfs_qgroup",
        )
    else:
        return SpaceInfo(
            path=path,
            total_bytes=total,
            used_bytes=used,
            available_bytes=available,
            quota_enabled=False,
            source="statvfs",
        )


def format_space_check(check: SpaceCheck) -> str:
    """Format a space check result for human-readable display.

    Args:
        check: The space check result to format

    Returns:
        Multi-line formatted string
    """
    lines = []
    info = check.space_info

    lines.append("Destination Space Check")
    lines.append("-" * 60)

    # Filesystem space
    lines.append(
        f"Filesystem space:  {_format_size(info.available_bytes)} available "
        f"of {_format_size(info.total_bytes)}"
    )

    # Quota info if enabled
    if info.quota_enabled:
        if info.quota_limit is not None:
            quota_remaining = info.quota_remaining or 0
            lines.append(
                f"Quota limit:       {_format_size(info.quota_limit)} "
                f"({_format_size(info.quota_used or 0)} used, "
                f"{_format_size(quota_remaining)} remaining)"
            )
        else:
            lines.append(
                f"Quota usage:       {_format_size(info.quota_used or 0)} "
                f"(no limit set)"
            )

        # Show which is more restrictive
        if info.quota_limit is not None:
            quota_rem = info.quota_remaining or 0
            if quota_rem < info.available_bytes:
                lines.append(
                    f"Effective limit:   {_format_size(check.effective_limit)} "
                    f"(quota is more restrictive)"
                )
            else:
                lines.append(
                    f"Effective limit:   {_format_size(check.effective_limit)} "
                    f"(filesystem is more restrictive)"
                )

    # Required space
    lines.append(
        f"Required:          {_format_size(check.estimated_size)} "
        f"(+ {check.safety_margin_percent:.0f}% safety margin = "
        f"{_format_size(check.required_with_margin)})"
    )

    # Status
    if check.sufficient:
        status = "OK - Sufficient space available"
        if check.warning_message and "Warning:" in check.warning_message:
            status = check.warning_message
    else:
        status = f"INSUFFICIENT - {check.warning_message}"

    lines.append(f"Status:            {status}")

    return "\n".join(lines)


def _format_size(size_bytes: int) -> str:
    """Format byte size in human readable form.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string like "1.23 GiB"
    """
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
