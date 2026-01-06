"""btrfs-backup-ng: btrfs-backup_ng/__init__.py."""

from pathlib import Path

__version__ = "0.8.1"


def encode_path_for_dir(path: Path) -> str:
    """Replace '/' with '_' and remove leading slash"""
    return str(path).lstrip("/").replace("/", "_")
