"""Subvolume classification and backup suggestion generation.

Provides heuristics to classify detected subvolumes and generate
prioritized backup suggestions for the init wizard.
"""

from __future__ import annotations

import re

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
    # Generic .snapshots directory
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
