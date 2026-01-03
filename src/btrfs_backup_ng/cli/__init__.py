"""CLI subcommand system for btrfs-backup-ng.

This module provides the subcommand-based CLI architecture with
legacy mode support for backwards compatibility.
"""

from .dispatcher import is_legacy_mode, main

__all__ = [
    "main",
    "is_legacy_mode",
]
