"""btrfs-backup-ng: btrfs-backup_ng/__init__.py."""

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

try:
    __version__ = version("btrfs-backup-ng")
except PackageNotFoundError:  # running from a source tree without an installed dist
    __version__ = "0.0.0+unknown"


def encode_path_for_dir(path: Path) -> str:
    """Replace '/' with '_' and remove leading slash"""
    return str(path).lstrip("/").replace("/", "_")
