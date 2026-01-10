"""Btrfs subvolume scanning and detection.

Provides functions to detect btrfs filesystems and enumerate subvolumes
on the local system.
"""

from __future__ import annotations

import logging
import re
import subprocess
from pathlib import Path

from .models import BtrfsMountInfo, DetectedSubvolume, DetectionResult

logger = logging.getLogger(__name__)

# Path to /proc/mounts (can be overridden for testing)
MOUNTS_FILE = "/proc/mounts"


class DetectionError(Exception):
    """Error during subvolume detection."""

    pass


class PermissionDeniedError(DetectionError):
    """Insufficient permissions for full detection."""

    pass


# Mount points that indicate removable/external media (not backup sources)
REMOVABLE_MEDIA_PREFIXES = (
    "/run/media/",  # systemd/udisks2 automount for removable media
    "/media/",  # Traditional automount location
    "/mnt/",  # Manual mount point (often used for external drives)
)


def is_removable_media(mount_point: str) -> bool:
    """Check if a mount point is likely removable/external media.

    Removable media are typically backup targets, not sources, and should
    be excluded from detection to avoid confusion (e.g., an external drive's
    top-level subvolume has path "/" which conflicts with the system root).

    Args:
        mount_point: The filesystem mount point path.

    Returns:
        True if the mount point appears to be removable media.
    """
    return mount_point.startswith(REMOVABLE_MEDIA_PREFIXES)


def parse_proc_mounts(
    content: str | None = None,
    mounts_file: str = MOUNTS_FILE,
    *,
    exclude_removable: bool = True,
) -> list[BtrfsMountInfo]:
    """Parse /proc/mounts for btrfs filesystems.

    Args:
        content: Optional mount file content (for testing).
        mounts_file: Path to mounts file (default: /proc/mounts).
        exclude_removable: If True (default), exclude removable/external media
                          mounts (under /run/media/, /media/, /mnt/).

    Returns:
        List of BtrfsMountInfo for each btrfs mount.
    """
    if content is None:
        try:
            content = Path(mounts_file).read_text()
        except OSError as e:
            logger.warning("Cannot read %s: %s", mounts_file, e)
            return []

    mounts: list[BtrfsMountInfo] = []

    for line in content.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue

        device, mount_point, fs_type, options_str = parts[:4]

        if fs_type != "btrfs":
            continue

        # Skip removable/external media if requested
        if exclude_removable and is_removable_media(mount_point):
            logger.debug("Skipping removable media mount: %s", mount_point)
            continue

        # Parse mount options
        options: dict[str, str] = {}
        subvol_path = ""
        subvol_id = 0

        for opt in options_str.split(","):
            if "=" in opt:
                key, value = opt.split("=", 1)
                options[key] = value
                if key == "subvol":
                    subvol_path = value
                elif key == "subvolid":
                    try:
                        subvol_id = int(value)
                    except ValueError:
                        pass
            else:
                options[opt] = ""

        mounts.append(
            BtrfsMountInfo(
                device=device,
                mount_point=mount_point,
                subvol_path=subvol_path,
                subvol_id=subvol_id,
                options=options,
            )
        )

    return mounts


def list_subvolumes(
    mount_point: str,
    *,
    include_snapshots: bool = True,
) -> list[DetectedSubvolume]:
    """List all subvolumes under a btrfs mount point.

    Runs 'btrfs subvolume list -a' which requires root privileges.

    Args:
        mount_point: Path to a btrfs mount point.
        include_snapshots: Whether to include snapshot subvolumes.

    Returns:
        List of DetectedSubvolume objects.

    Raises:
        PermissionDeniedError: If root access is required but not available.
        DetectionError: If btrfs command fails for other reasons.
    """
    # Build command
    # -a: show all subvolumes (including those not accessible from mount point)
    # -o: only show subvolumes below this path (use without -a for that)
    # Using -a to get complete picture
    cmd = ["btrfs", "subvolume", "list", "-a", mount_point]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as e:
        raise DetectionError(
            "btrfs command not found. Please install btrfs-progs."
        ) from e

    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "Permission denied" in stderr or "Operation not permitted" in stderr:
            raise PermissionDeniedError(
                "Root privileges required for complete subvolume detection."
            )
        raise DetectionError(f"btrfs subvolume list failed: {stderr}")

    return _parse_subvolume_list(result.stdout, mount_point)


def _parse_subvolume_list(
    output: str,
    mount_point: str,
) -> list[DetectedSubvolume]:
    """Parse output from 'btrfs subvolume list -a'.

    Example output format:
        ID 256 gen 12345 top level 5 path <FS_TREE>/home
        ID 257 gen 12346 top level 5 path <FS_TREE>/@

    Args:
        output: Command output to parse.
        mount_point: Mount point for context.

    Returns:
        List of DetectedSubvolume objects.
    """
    subvolumes: list[DetectedSubvolume] = []

    # Pattern for parsing btrfs subvolume list output
    # ID <id> gen <gen> top level <top> path <path>
    pattern = re.compile(r"ID\s+(\d+)\s+gen\s+(\d+)\s+top level\s+(\d+)\s+path\s+(.+)")

    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue

        match = pattern.match(line)
        if not match:
            logger.debug("Could not parse subvolume list line: %s", line)
            continue

        subvol_id = int(match.group(1))
        gen = int(match.group(2))
        top_level = int(match.group(3))
        path = match.group(4)

        # Clean up path - remove <FS_TREE>/ prefix if present
        if path.startswith("<FS_TREE>/"):
            path = path[len("<FS_TREE>/") :]
        elif path.startswith("<FS_TREE>"):
            path = path[len("<FS_TREE>") :]

        # Ensure path starts with /
        if not path.startswith("/"):
            path = "/" + path

        subvolumes.append(
            DetectedSubvolume(
                id=subvol_id,
                path=path,
                gen=gen,
                top_level=top_level,
            )
        )

    return subvolumes


def get_subvolume_details(
    subvol_path: str,
) -> dict[str, str]:
    """Get detailed information about a specific subvolume.

    Runs 'btrfs subvolume show' to get UUID, parent UUID, etc.

    Args:
        subvol_path: Path to the subvolume.

    Returns:
        Dictionary of subvolume properties.
    """
    cmd = ["btrfs", "subvolume", "show", subvol_path]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return {}

    if result.returncode != 0:
        return {}

    # Parse key: value pairs from output
    details: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if ":" in line:
            key, _, value = line.partition(":")
            details[key.strip().lower()] = value.strip()

    return details


def correlate_mounts_and_subvolumes(
    mounts: list[BtrfsMountInfo],
    subvolumes: list[DetectedSubvolume],
) -> list[DetectedSubvolume]:
    """Correlate mount points with detected subvolumes.

    Updates subvolumes with mount_point and device information.
    Also adds mounted subvolumes that weren't detected by 'btrfs subvolume list'
    (e.g., the top-level subvolume ID 5 which is often the root filesystem).

    Args:
        mounts: List of btrfs mount info.
        subvolumes: List of detected subvolumes.

    Returns:
        Updated list of subvolumes with mount info populated.
    """
    # Build lookup by subvol_id
    mount_by_id: dict[int, BtrfsMountInfo] = {}
    for mount in mounts:
        mount_by_id[mount.subvol_id] = mount

    # Also try to match by path
    mount_by_path: dict[str, BtrfsMountInfo] = {}
    for mount in mounts:
        if mount.subvol_path:
            # Normalize path
            normalized = mount.subvol_path
            if not normalized.startswith("/"):
                normalized = "/" + normalized
            mount_by_path[normalized] = mount

    # Track which subvol IDs we've seen
    seen_ids: set[int] = {subvol.id for subvol in subvolumes}

    # Update subvolumes
    for subvol in subvolumes:
        # Try by ID first
        if subvol.id in mount_by_id:
            mount = mount_by_id[subvol.id]
            subvol.mount_point = mount.mount_point
            subvol.device = mount.device
        # Fall back to path matching
        elif subvol.path in mount_by_path:
            mount = mount_by_path[subvol.path]
            subvol.mount_point = mount.mount_point
            subvol.device = mount.device

    # Add mounted subvolumes that weren't in the btrfs subvolume list
    # This is important for the top-level subvolume (ID 5) which is typically
    # not shown by 'btrfs subvolume list' but may be mounted as /
    for mount in mounts:
        if mount.subvol_id not in seen_ids:
            # Determine path from mount info
            path = mount.subvol_path or mount.mount_point
            if not path.startswith("/"):
                path = "/" + path

            subvolumes.append(
                DetectedSubvolume(
                    id=mount.subvol_id,
                    path=path,
                    mount_point=mount.mount_point,
                    device=mount.device,
                    top_level=0,  # Top-level subvolumes have no parent
                )
            )
            seen_ids.add(mount.subvol_id)
            logger.debug(
                "Added mounted subvolume not in list: id=%d path=%s mount=%s",
                mount.subvol_id,
                path,
                mount.mount_point,
            )

    # Set device for unmounted subvolumes from any mount on same filesystem
    if mounts:
        default_device = mounts[0].device
        for subvol in subvolumes:
            if subvol.device is None:
                subvol.device = default_device

    return subvolumes


def scan_system(
    allow_partial: bool = False,
) -> DetectionResult:
    """Scan the system for btrfs subvolumes.

    This is the main entry point for detection. It:
    1. Parses /proc/mounts for btrfs filesystems
    2. Runs 'btrfs subvolume list' for each filesystem
    3. Correlates mount points with subvolumes
    4. Returns a complete detection result

    Args:
        allow_partial: If True, return partial results on permission errors.
                      If False, raise PermissionDeniedError.

    Returns:
        DetectionResult with all discovered information.

    Raises:
        PermissionDeniedError: If root access needed and allow_partial=False.
        DetectionError: If detection fails for other reasons.
    """
    result = DetectionResult()

    # Step 1: Parse mounted btrfs filesystems
    result.filesystems = parse_proc_mounts()

    if not result.filesystems:
        result.error_message = "No btrfs filesystems found."
        return result

    logger.info("Found %d btrfs mount(s)", len(result.filesystems))

    # Group mounts by device to avoid scanning same filesystem multiple times
    devices_seen: set[str] = set()
    unique_mounts: list[BtrfsMountInfo] = []
    for mount in result.filesystems:
        if mount.device not in devices_seen:
            devices_seen.add(mount.device)
            unique_mounts.append(mount)

    # Step 2: List subvolumes for each unique filesystem
    all_subvolumes: list[DetectedSubvolume] = []

    for mount in unique_mounts:
        try:
            subvols = list_subvolumes(mount.mount_point)
            all_subvolumes.extend(subvols)
            logger.info(
                "Found %d subvolume(s) on %s",
                len(subvols),
                mount.mount_point,
            )
        except PermissionDeniedError as e:
            if allow_partial:
                result.is_partial = True
                result.error_message = str(e)
                logger.warning("Partial detection: %s", e)
            else:
                raise
        except DetectionError as e:
            logger.warning("Could not list subvolumes on %s: %s", mount.mount_point, e)

    # Step 3: Correlate mount points
    result.subvolumes = correlate_mounts_and_subvolumes(
        result.filesystems, all_subvolumes
    )

    # If we only have mount info (partial detection), create subvolumes from mounts
    if result.is_partial and not result.subvolumes:
        for mount in result.filesystems:
            result.subvolumes.append(
                DetectedSubvolume(
                    id=mount.subvol_id,
                    path=mount.subvol_path or mount.mount_point,
                    mount_point=mount.mount_point,
                    device=mount.device,
                )
            )

    return result
