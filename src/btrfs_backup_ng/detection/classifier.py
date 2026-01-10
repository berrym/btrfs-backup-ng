"""Subvolume classification and backup suggestion generation.

Provides heuristics to classify detected subvolumes and generate
prioritized backup suggestions for the init wizard.
"""

from __future__ import annotations

import re
from typing import Any

from .models import (
    BackupSuggestion,
    DetectedSubvolume,
    DetectionResult,
    SubvolumeClass,
)

# Patterns for snapshot detection
SNAPSHOT_PATTERNS = [
    # Snapper-style: /.snapshots/123/snapshot
    re.compile(r"^/\.snapshots/\d+/snapshot$"),
    re.compile(r"^\.snapshots/\d+/snapshot$"),
    # The .snapshots directory itself (snapper's snapshot container subvolume)
    re.compile(r"^/\.snapshots$"),
    re.compile(r"^\.snapshots$"),
    re.compile(r"/\.snapshots$"),
    # Generic .snapshots subdirectory
    re.compile(r"/\.snapshots/"),
    re.compile(r"^\.snapshots/"),
    # Timeshift-style
    re.compile(r"/timeshift-btrfs/snapshots/"),
    # Date-stamped snapshots (btrfs-backup-ng style)
    re.compile(r"/\d{8}-\d{6}$"),
]

# Paths to auto-exclude (system internal)
INTERNAL_PATHS = [
    "/var/lib/machines",
    "/var/lib/portables",
    "/var/lib/docker",
    "/var/lib/containers",
    "/var/lib/libvirt/images",
]

# Low-priority variable data paths
VARIABLE_PATHS = [
    "/var/cache",
    "/var/tmp",
    "/var/log",
    "/var/spool",
]

# System data paths (optional backup)
SYSTEM_DATA_PATHS = [
    "/opt",
    "/srv",
    "/usr/local",
]


def classify_subvolume(subvol: DetectedSubvolume) -> SubvolumeClass:
    """Classify a subvolume based on its path and mount point.

    Classification rules are applied in order of specificity.

    Args:
        subvol: The subvolume to classify.

    Returns:
        SubvolumeClass indicating the type of data.
    """
    path = subvol.path
    mount = subvol.mount_point or ""

    # Normalize paths
    if not path.startswith("/"):
        path = "/" + path

    # Rule 1: Snapshot patterns (highest priority - always exclude)
    for pattern in SNAPSHOT_PATTERNS:
        if pattern.search(path):
            return SubvolumeClass.SNAPSHOT

    # Also check if it has a parent UUID (indicates it's a snapshot)
    if subvol.parent_uuid:
        return SubvolumeClass.SNAPSHOT

    # Rule 2: Internal system paths (auto-exclude)
    for internal_path in INTERNAL_PATHS:
        if path.startswith(internal_path) or mount.startswith(internal_path):
            return SubvolumeClass.INTERNAL

    # Rule 3: User data - /home or under /home
    if mount == "/home" or path == "/home":
        return SubvolumeClass.USER_DATA
    if mount.startswith("/home/") or path.startswith("/home/"):
        return SubvolumeClass.USER_DATA

    # Rule 4: System root
    if mount == "/" or path == "/" or path == "/@":
        return SubvolumeClass.SYSTEM_ROOT

    # Rule 5: Variable data paths
    for var_path in VARIABLE_PATHS:
        if path.startswith(var_path) or mount.startswith(var_path):
            return SubvolumeClass.VARIABLE

    # Rule 6: System data paths
    for sys_path in SYSTEM_DATA_PATHS:
        if path.startswith(sys_path) or mount.startswith(sys_path):
            return SubvolumeClass.SYSTEM_DATA

    # Default: Unknown
    return SubvolumeClass.UNKNOWN


def classify_all_subvolumes(
    subvolumes: list[DetectedSubvolume],
) -> list[DetectedSubvolume]:
    """Classify all subvolumes in a list.

    Updates each subvolume's classification field in place.

    Args:
        subvolumes: List of subvolumes to classify.

    Returns:
        The same list with classifications populated.
    """
    for subvol in subvolumes:
        subvol.classification = classify_subvolume(subvol)
        # Also set is_snapshot flag
        subvol.is_snapshot = subvol.classification == SubvolumeClass.SNAPSHOT

    return subvolumes


def generate_suggestions(
    subvolumes: list[DetectedSubvolume],
) -> list[BackupSuggestion]:
    """Generate prioritized backup suggestions from classified subvolumes.

    Args:
        subvolumes: List of classified subvolumes.

    Returns:
        List of BackupSuggestion objects, sorted by priority.
    """
    suggestions: list[BackupSuggestion] = []

    for subvol in subvolumes:
        # Skip snapshots and internal
        if subvol.classification in (SubvolumeClass.SNAPSHOT, SubvolumeClass.INTERNAL):
            continue

        priority, reason = _get_priority_and_reason(subvol)

        suggestions.append(
            BackupSuggestion(
                subvolume=subvol,
                suggested_prefix=subvol.suggested_prefix,
                suggested_snapshot_dir=_suggest_snapshot_dir(subvol),
                priority=priority,
                reason=reason,
            )
        )

    # Sort by priority (lower is higher priority)
    suggestions.sort(key=lambda s: s.priority)

    return suggestions


def _get_priority_and_reason(subvol: DetectedSubvolume) -> tuple[int, str]:
    """Get priority and reason for a subvolume based on classification.

    Args:
        subvol: Classified subvolume.

    Returns:
        Tuple of (priority, reason_string).
    """
    classification = subvol.classification

    if classification == SubvolumeClass.USER_DATA:
        return 1, "User data - highly recommended for backup"

    if classification == SubvolumeClass.SYSTEM_ROOT:
        return 2, "System root - recommended for disaster recovery"

    if classification == SubvolumeClass.SYSTEM_DATA:
        return 3, "System data - optional, contains applications/services"

    if classification == SubvolumeClass.VARIABLE:
        # Different priorities within variable data
        path = subvol.mount_point or subvol.path
        if "/var/log" in path:
            return 4, "Logs - optional, useful for auditing"
        return 5, "Variable data - typically not backed up"

    # Unknown - medium-low priority
    return 4, "Unknown classification - review manually"


def _suggest_snapshot_dir(subvol: DetectedSubvolume) -> str:
    """Suggest a snapshot directory for a subvolume.

    Args:
        subvol: The subvolume.

    Returns:
        Suggested snapshot directory path.
    """
    # If mounted, suggest .snapshots relative to mount
    if subvol.mount_point:
        return ".snapshots"

    # For unmounted subvolumes, suggest path-based
    return ".snapshots"


def process_detection_result(result: DetectionResult) -> DetectionResult:
    """Process a DetectionResult to add classifications and suggestions.

    This is the main entry point for classification. It:
    1. Classifies all detected subvolumes
    2. Generates backup suggestions
    3. Updates the result in place

    Args:
        result: DetectionResult from scanner.

    Returns:
        The same result with classifications and suggestions populated.
    """
    # Classify all subvolumes
    classify_all_subvolumes(result.subvolumes)

    # Generate suggestions
    result.suggestions = generate_suggestions(result.subvolumes)

    return result


# Snapper config name to SubvolumeClass mapping
# Based on common snapper configuration naming conventions
SNAPPER_NAME_CLASSIFICATIONS: dict[str, SubvolumeClass] = {
    "root": SubvolumeClass.SYSTEM_ROOT,
    "home": SubvolumeClass.USER_DATA,
    "opt": SubvolumeClass.SYSTEM_DATA,
    "srv": SubvolumeClass.SYSTEM_DATA,
    "var": SubvolumeClass.VARIABLE,
    "var_log": SubvolumeClass.VARIABLE,
    "var-log": SubvolumeClass.VARIABLE,
    "varlog": SubvolumeClass.VARIABLE,
    "log": SubvolumeClass.VARIABLE,
    "tmp": SubvolumeClass.VARIABLE,
}


def classify_from_snapper_config(
    subvolumes: list[DetectedSubvolume],
    snapper_configs: list[Any],
) -> dict[str, Any]:
    """Enhance subvolume classification using snapper configuration data.

    This function uses snapper config metadata to:
    1. Properly classify subvolumes that snapper manages
    2. Build a mapping of subvolume paths to their snapper configs

    Snapper config names like "root", "home" provide strong hints about
    the subvolume's purpose, which is more reliable than path-based guessing.

    Args:
        subvolumes: List of detected subvolumes (will be modified in place)
        snapper_configs: List of SnapperConfig objects from snapper scanner

    Returns:
        Dictionary mapping subvolume display_path to SnapperConfig
    """
    # Build path -> config mapping
    snapper_path_map: dict[str, Any] = {}

    for cfg in snapper_configs:
        # Store by the subvolume path string
        subvol_path = str(cfg.subvolume)
        snapper_path_map[subvol_path] = cfg

        # Also try normalized path (resolve / to /)
        if subvol_path == "/":
            snapper_path_map["/"] = cfg

    # Enhance classification for each subvolume
    for subvol in subvolumes:
        # Skip snapshots - they should stay as SNAPSHOT
        if subvol.classification == SubvolumeClass.SNAPSHOT:
            continue

        # Try to find matching snapper config
        snapper_cfg = None
        display_path = subvol.display_path

        # Try exact match first
        if display_path in snapper_path_map:
            snapper_cfg = snapper_path_map[display_path]
        # Try mount point
        elif subvol.mount_point and subvol.mount_point in snapper_path_map:
            snapper_cfg = snapper_path_map[subvol.mount_point]

        if snapper_cfg is None:
            continue

        # Use snapper config name to determine classification
        config_name = snapper_cfg.name.lower()

        # Check for exact match in our mapping
        if config_name in SNAPPER_NAME_CLASSIFICATIONS:
            new_class = SNAPPER_NAME_CLASSIFICATIONS[config_name]
            subvol.classification = new_class
            continue

        # Check for partial matches (e.g., "home_user" -> home)
        for name_pattern, classification in SNAPPER_NAME_CLASSIFICATIONS.items():
            if config_name.startswith(name_pattern):
                subvol.classification = classification
                break

    return snapper_path_map


def reclassify_with_snapper(
    result: DetectionResult,
    snapper_configs: list[Any],
) -> dict[str, Any]:
    """Reclassify subvolumes and regenerate suggestions using snapper data.

    This should be called after initial detection when snapper configs
    are available. It updates classifications based on snapper metadata
    and regenerates the backup suggestions.

    Also adds snapper-managed subvolumes that weren't detected. This can
    happen when the system is booted from a snapper snapshot - the original
    subvolume (e.g., /) may not appear in the detection results, but snapper
    still manages it and it should be offered for backup.

    Args:
        result: DetectionResult that has already been processed
        snapper_configs: List of SnapperConfig objects

    Returns:
        Dictionary mapping subvolume paths to SnapperConfig objects
    """
    # First, add any snapper-managed subvolumes that aren't in the detected list
    # This handles cases like booting from a snapshot where / isn't directly detected
    _add_snapper_managed_subvolumes(result, snapper_configs)

    # Enhance classifications with snapper data
    snapper_path_map = classify_from_snapper_config(result.subvolumes, snapper_configs)

    # Regenerate suggestions with updated classifications
    result.suggestions = generate_suggestions(result.subvolumes)

    return snapper_path_map


def _add_snapper_managed_subvolumes(
    result: DetectionResult,
    snapper_configs: list[Any],
) -> None:
    """Add snapper-managed subvolumes that weren't detected.

    When booted from a snapper snapshot, the original subvolume may not
    appear in the btrfs subvolume list or mounts. But snapper still manages
    it and the user may want to back it up.

    This function adds "virtual" subvolume entries for snapper-managed
    paths that aren't already in the detection results.

    Args:
        result: DetectionResult to modify (subvolumes list may be extended)
        snapper_configs: List of SnapperConfig objects
    """
    # Build set of existing mount points for non-snapshot subvolumes
    # We check mount_point specifically because a subvolume with path="/"
    # might be an external drive mounted elsewhere, not the actual system root
    existing_mount_points: set[str] = set()
    for subvol in result.subvolumes:
        # Skip snapshots - we want to find if the original subvol exists
        if subvol.classification == SubvolumeClass.SNAPSHOT:
            continue
        if subvol.mount_point:
            existing_mount_points.add(subvol.mount_point)

    # Check each snapper config
    for cfg in snapper_configs:
        subvol_path = str(cfg.subvolume)

        # Skip if there's a non-snapshot subvolume mounted at this path
        # This ensures we only add virtual entries for truly missing subvolumes
        if subvol_path in existing_mount_points:
            continue

        # Determine classification from snapper config name
        config_name = cfg.name.lower()
        classification = SNAPPER_NAME_CLASSIFICATIONS.get(
            config_name, SubvolumeClass.UNKNOWN
        )

        # Check partial matches
        if classification == SubvolumeClass.UNKNOWN:
            for name_pattern, cls in SNAPPER_NAME_CLASSIFICATIONS.items():
                if config_name.startswith(name_pattern):
                    classification = cls
                    break

        # Get device from existing filesystems if available
        device = None
        if result.filesystems:
            device = result.filesystems[0].device

        # Create a virtual subvolume entry for this snapper-managed path
        # Use ID 0 to indicate it wasn't directly detected
        virtual_subvol = DetectedSubvolume(
            id=0,  # Unknown ID - not directly detected
            path=subvol_path,
            mount_point=subvol_path,  # Assume mounted at same path
            classification=classification,
            device=device,
        )

        result.subvolumes.append(virtual_subvol)
        existing_mount_points.add(subvol_path)
