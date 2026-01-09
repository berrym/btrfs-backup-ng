"""Snapper snapshot representation.

This module provides the SnapperSnapshot class that represents a snapshot
managed by snapper, with all its associated metadata.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from .metadata import SnapperMetadata

__all__ = ["SnapperSnapshot"]


@dataclass
class SnapperSnapshot:
    """Represents a snapper-managed snapshot.

    Attributes:
        config_name: Name of the snapper config (e.g., 'root', 'home')
        number: Snapshot number
        metadata: Full snapshot metadata
        subvolume_path: Path to the actual snapshot subvolume
        info_xml_path: Path to the info.xml file
    """

    config_name: str
    number: int
    metadata: SnapperMetadata
    subvolume_path: Path
    info_xml_path: Path

    @property
    def snapshot_type(self) -> str:
        """Snapshot type ('single', 'pre', or 'post')."""
        return self.metadata.type

    @property
    def date(self) -> datetime:
        """Snapshot creation date."""
        return self.metadata.date

    @property
    def description(self) -> str:
        """Snapshot description."""
        return self.metadata.description

    @property
    def cleanup(self) -> str:
        """Cleanup algorithm."""
        return self.metadata.cleanup

    @property
    def pre_num(self) -> Optional[int]:
        """For 'post' snapshots, the paired 'pre' snapshot number."""
        return self.metadata.pre_num

    def get_backup_name(self, date_format: str = "%Y%m%d-%H%M%S") -> str:
        """Generate backup name for this snapshot.

        Format: {config_name}-{number}-{date}

        Args:
            date_format: strftime format for the date portion

        Returns:
            Backup name string
        """
        date_str = self.metadata.date.strftime(date_format)
        return f"{self.config_name}-{self.number}-{date_str}"

    def exists(self) -> bool:
        """Check if the snapshot subvolume exists."""
        return self.subvolume_path.exists()

    def __repr__(self) -> str:
        return (
            f"SnapperSnapshot(config={self.config_name!r}, "
            f"num={self.number}, type={self.snapshot_type!r}, "
            f"date={self.date.isoformat()})"
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, SnapperSnapshot):
            return False
        return self.config_name == other.config_name and self.number == other.number

    def __hash__(self) -> int:
        return hash((self.config_name, self.number))

    def __lt__(self, other) -> bool:
        """Sort by config name, then by number."""
        if not isinstance(other, SnapperSnapshot):
            return NotImplemented
        if self.config_name != other.config_name:
            return self.config_name < other.config_name
        return self.number < other.number
