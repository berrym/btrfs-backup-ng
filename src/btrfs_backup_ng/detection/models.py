"""Data models for btrfs subvolume detection.

Provides dataclasses representing detected btrfs filesystems, subvolumes,
and backup suggestions for the init wizard.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class SubvolumeClass(Enum):
    """Classification of detected subvolumes for backup prioritization."""

    USER_DATA = "user_data"  # /home, user directories - high priority
    SYSTEM_ROOT = "system_root"  # / - medium priority
    SYSTEM_DATA = "system_data"  # /opt, /srv - optional
    VARIABLE = "variable"  # /var/log, /var/cache - low priority
    SNAPSHOT = "snapshot"  # Snapshot directories - auto-exclude
    INTERNAL = "internal"  # System internal (/var/lib/machines) - auto-exclude
    UNKNOWN = "unknown"  # Cannot classify


@dataclass
class BtrfsMountInfo:
    """Information about a mounted btrfs filesystem.

    Parsed from /proc/mounts entries with btrfs filesystem type.
    """

    device: str
    """Block device path (e.g., /dev/sda1, /dev/mapper/luks-xxx)."""

    mount_point: str
    """Where the subvolume is mounted (e.g., /home, /)."""

    subvol_path: str
    """Subvolume path from mount options (e.g., /home, /@home)."""

    subvol_id: int
    """Subvolume ID from mount options."""

    options: dict[str, str] = field(default_factory=dict)
    """All mount options as key-value pairs."""

    def __hash__(self) -> int:
        """Hash by device and subvol_id for deduplication."""
        return hash((self.device, self.subvol_id))

    def __eq__(self, other: object) -> bool:
        """Compare by device and subvol_id."""
        if not isinstance(other, BtrfsMountInfo):
            return NotImplemented
        return self.device == other.device and self.subvol_id == other.subvol_id


@dataclass
class DetectedSubvolume:
    """A detected btrfs subvolume with metadata.

    Combines information from btrfs subvolume list and mount information.
    """

    id: int
    """Btrfs subvolume ID."""

    path: str
    """Subvolume path as reported by btrfs (may be relative to top-level)."""

    mount_point: str | None = None
    """Where this subvolume is currently mounted, if at all."""

    gen: int = 0
    """Generation number."""

    top_level: int = 0
    """Parent subvolume ID (top level)."""

    uuid: str | None = None
    """Subvolume UUID if available."""

    parent_uuid: str | None = None
    """Parent UUID for snapshots."""

    classification: SubvolumeClass = SubvolumeClass.UNKNOWN
    """Classification for backup prioritization."""

    is_snapshot: bool = False
    """Whether this appears to be a snapshot (has parent_uuid or matches patterns)."""

    device: str | None = None
    """Block device this subvolume resides on."""

    def __hash__(self) -> int:
        """Hash by id and device for deduplication."""
        return hash((self.id, self.device))

    def __eq__(self, other: object) -> bool:
        """Compare by id and device."""
        if not isinstance(other, DetectedSubvolume):
            return NotImplemented
        return self.id == other.id and self.device == other.device

    @property
    def display_path(self) -> str:
        """Path to display to user (mount point or subvol path)."""
        return self.mount_point or self.path

    @property
    def suggested_prefix(self) -> str:
        """Generate a suggested snapshot prefix from the path.

        Examples:
            /home -> home
            / -> root
            /var/log -> var-log
        """
        path = self.mount_point or self.path
        # Strip leading/trailing slashes and replace internal slashes with dashes
        clean = path.strip("/")
        if not clean:
            return "root"
        return clean.replace("/", "-")


@dataclass
class BackupSuggestion:
    """A suggested backup configuration for a detected subvolume."""

    subvolume: DetectedSubvolume
    """The subvolume to back up."""

    suggested_prefix: str
    """Suggested snapshot prefix (e.g., 'home', 'root')."""

    suggested_snapshot_dir: str = ".snapshots"
    """Suggested snapshot directory."""

    priority: int = 5
    """Backup priority (1=highest, 5=lowest)."""

    reason: str = ""
    """Human-readable reason for the suggestion."""

    @property
    def is_recommended(self) -> bool:
        """Whether this is a recommended (high priority) backup candidate."""
        return self.priority <= 2


@dataclass
class DetectionResult:
    """Complete results from a system detection scan."""

    filesystems: list[BtrfsMountInfo] = field(default_factory=list)
    """All detected btrfs mount points."""

    subvolumes: list[DetectedSubvolume] = field(default_factory=list)
    """All detected subvolumes."""

    suggestions: list[BackupSuggestion] = field(default_factory=list)
    """Prioritized backup suggestions."""

    is_partial: bool = False
    """True if detection was incomplete (e.g., no root access)."""

    error_message: str | None = None
    """Error message if detection failed or was partial."""

    @property
    def recommended_subvolumes(self) -> list[DetectedSubvolume]:
        """Subvolumes recommended for backup (high priority)."""
        return [s.subvolume for s in self.suggestions if s.is_recommended]

    @property
    def excluded_subvolumes(self) -> list[DetectedSubvolume]:
        """Subvolumes auto-excluded (snapshots, internal)."""
        excluded_classes = {SubvolumeClass.SNAPSHOT, SubvolumeClass.INTERNAL}
        return [sv for sv in self.subvolumes if sv.classification in excluded_classes]

    @property
    def optional_subvolumes(self) -> list[DetectedSubvolume]:
        """Subvolumes that could be backed up but aren't recommended."""
        recommended_ids = {s.subvolume.id for s in self.suggestions if s.is_recommended}
        excluded_classes = {SubvolumeClass.SNAPSHOT, SubvolumeClass.INTERNAL}
        return [
            sv
            for sv in self.subvolumes
            if sv.id not in recommended_ids
            and sv.classification not in excluded_classes
        ]

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "is_partial": self.is_partial,
            "error_message": self.error_message,
            "filesystems": [
                {
                    "device": fs.device,
                    "mount_point": fs.mount_point,
                    "subvol_path": fs.subvol_path,
                    "subvol_id": fs.subvol_id,
                }
                for fs in self.filesystems
            ],
            "subvolumes": [
                {
                    "id": sv.id,
                    "path": sv.path,
                    "mount_point": sv.mount_point,
                    "classification": sv.classification.value,
                    "is_snapshot": sv.is_snapshot,
                }
                for sv in self.subvolumes
            ],
            "suggestions": [
                {
                    "path": s.subvolume.display_path,
                    "prefix": s.suggested_prefix,
                    "priority": s.priority,
                    "reason": s.reason,
                    "recommended": s.is_recommended,
                }
                for s in self.suggestions
            ],
        }
