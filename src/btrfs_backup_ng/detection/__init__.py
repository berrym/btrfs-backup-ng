"""Btrfs subvolume detection module.

Provides automatic detection of btrfs subvolumes on the local system,
classification of subvolumes for backup prioritization, and generation
of backup suggestions for the init wizard.

Example usage:
    from btrfs_backup_ng.detection import detect_subvolumes

    result = detect_subvolumes()
    if result.error_message:
        print(f"Warning: {result.error_message}")

    for suggestion in result.suggestions:
        if suggestion.is_recommended:
            print(f"Recommended: {suggestion.subvolume.display_path}")
"""

from .classifier import (
    classify_all_subvolumes,
    classify_from_snapper_config,
    classify_subvolume,
    generate_suggestions,
    process_detection_result,
    reclassify_with_snapper,
)
from .models import (
    BackupSuggestion,
    BtrfsMountInfo,
    DetectedSubvolume,
    DetectionResult,
    SubvolumeClass,
)
from .scanner import (
    DetectionError,
    PermissionDeniedError,
    correlate_mounts_and_subvolumes,
    list_subvolumes,
    parse_proc_mounts,
    scan_system,
)

__all__ = [
    # Models
    "BackupSuggestion",
    "BtrfsMountInfo",
    "DetectedSubvolume",
    "DetectionResult",
    "SubvolumeClass",
    # Scanner
    "DetectionError",
    "PermissionDeniedError",
    "correlate_mounts_and_subvolumes",
    "list_subvolumes",
    "parse_proc_mounts",
    "scan_system",
    # Classifier
    "classify_all_subvolumes",
    "classify_from_snapper_config",
    "classify_subvolume",
    "generate_suggestions",
    "process_detection_result",
    "reclassify_with_snapper",
    # High-level API
    "detect_subvolumes",
]


def detect_subvolumes(
    *,
    allow_partial: bool = True,
) -> DetectionResult:
    """Detect and classify btrfs subvolumes on the system.

    This is the main entry point for subvolume detection. It:
    1. Scans the system for btrfs filesystems and subvolumes
    2. Classifies each subvolume (user data, system, snapshot, etc.)
    3. Generates prioritized backup suggestions

    Args:
        allow_partial: If True (default), return partial results when
                      root access is not available. If False, raise
                      PermissionDeniedError.

    Returns:
        DetectionResult containing:
        - filesystems: All detected btrfs mount points
        - subvolumes: All detected subvolumes with classifications
        - suggestions: Prioritized backup suggestions
        - is_partial: True if detection was incomplete
        - error_message: Description of any issues encountered

    Raises:
        PermissionDeniedError: If allow_partial=False and root needed.
        DetectionError: If detection fails for other reasons.

    Example:
        result = detect_subvolumes()

        # Show recommended backup candidates
        for suggestion in result.suggestions:
            if suggestion.is_recommended:
                print(f"{suggestion.subvolume.display_path} ({suggestion.reason})")

        # Check if detection was partial
        if result.is_partial:
            print(f"Note: {result.error_message}")
    """
    # Scan the system
    result = scan_system(allow_partial=allow_partial)

    # Process with classification and suggestions
    process_detection_result(result)

    return result
