"""Snapper integration for btrfs-backup-ng.

This module provides integration with openSUSE's Snapper snapshot management tool,
enabling btrfs-backup-ng to detect, understand, and backup snapper-managed snapshots
while preserving their metadata for proper restoration.
"""

from .metadata import SnapperMetadata, generate_info_xml, parse_info_xml
from .scanner import SnapperConfig, SnapperScanner
from .snapshot import SnapperSnapshot

__all__ = [
    "SnapperConfig",
    "SnapperMetadata",
    "SnapperScanner",
    "SnapperSnapshot",
    "generate_info_xml",
    "parse_info_xml",
]
