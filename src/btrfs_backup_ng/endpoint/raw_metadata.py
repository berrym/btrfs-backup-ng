"""Raw target metadata handling for btrfs send streams.

This module provides classes for tracking metadata about raw backup files
(btrfs send streams saved to files with optional compression/encryption).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btrfs_backup_ng import __version__

# Compression tool configurations
COMPRESSION_CONFIG: dict[str, dict[str, Any]] = {
    "gzip": {
        "extension": ".gz",
        "compress_cmd": ["gzip", "-c"],
        "decompress_cmd": ["gzip", "-d", "-c"],
    },
    "pigz": {
        "extension": ".gz",
        "compress_cmd": ["pigz", "-c"],
        "decompress_cmd": ["pigz", "-d", "-c"],
    },
    "zstd": {
        "extension": ".zst",
        "compress_cmd": ["zstd", "-c"],
        "decompress_cmd": ["zstd", "-d", "-c"],
    },
    "lz4": {
        "extension": ".lz4",
        "compress_cmd": ["lz4", "-c"],
        "decompress_cmd": ["lz4", "-d", "-c"],
    },
    "xz": {
        "extension": ".xz",
        "compress_cmd": ["xz", "-c"],
        "decompress_cmd": ["xz", "-d", "-c"],
    },
    "lzo": {
        "extension": ".lzo",
        "compress_cmd": ["lzop", "-c"],
        "decompress_cmd": ["lzop", "-d", "-c"],
    },
    "pbzip2": {
        "extension": ".bz2",
        "compress_cmd": ["pbzip2", "-c"],
        "decompress_cmd": ["pbzip2", "-d", "-c"],
    },
    "bzip2": {
        "extension": ".bz2",
        "compress_cmd": ["bzip2", "-c"],
        "decompress_cmd": ["bzip2", "-d", "-c"],
    },
}


@dataclass
class RawSnapshot:
    """Metadata for a raw backup file (btrfs send stream).

    Attributes:
        name: Snapshot name (e.g., 'root.20240115T120000')
        stream_path: Path to the stream file
        uuid: Btrfs subvolume UUID
        parent_uuid: Parent subvolume UUID for incremental backups
        parent_name: Parent snapshot name for chain reference
        created: Creation timestamp
        size: Stream file size in bytes
        compress: Compression algorithm used (or None)
        encrypt: Encryption method used (or None)
        gpg_recipient: GPG recipient if encrypted
    """

    name: str
    stream_path: Path
    uuid: str = ""
    parent_uuid: str | None = None
    parent_name: str | None = None
    created: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    size: int = 0
    compress: str | None = None
    encrypt: str | None = None
    gpg_recipient: str | None = None

    @property
    def metadata_path(self) -> Path:
        """Get the path to the metadata file for this snapshot."""
        return self.stream_path.with_suffix(self.stream_path.suffix + ".meta")

    @property
    def is_incremental(self) -> bool:
        """Check if this is an incremental backup."""
        return self.parent_uuid is not None or self.parent_name is not None

    def to_dict(self) -> dict[str, Any]:
        """Serialize snapshot metadata to a dictionary."""
        return {
            "version": 1,
            "name": self.name,
            "uuid": self.uuid,
            "parent_uuid": self.parent_uuid,
            "parent_name": self.parent_name,
            "created": self.created.isoformat(),
            "size": self.size,
            "pipeline": {
                "compress": self.compress,
                "encrypt": self.encrypt,
                "gpg_recipient": self.gpg_recipient,
            },
            "btrfs_backup_ng_version": __version__,
        }

    def save_metadata(self) -> None:
        """Save metadata to the metadata file."""
        with open(self.metadata_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def from_dict(cls, data: dict[str, Any], stream_path: Path) -> RawSnapshot:
        """Create a RawSnapshot from a metadata dictionary.

        Args:
            data: Metadata dictionary (from JSON file)
            stream_path: Path to the stream file

        Returns:
            RawSnapshot instance
        """
        pipeline = data.get("pipeline", {})
        created_str = data.get("created")

        if created_str:
            # Parse ISO format datetime
            created = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        else:
            created = datetime.now(timezone.utc)

        return cls(
            name=data.get("name", ""),
            stream_path=stream_path,
            uuid=data.get("uuid", ""),
            parent_uuid=data.get("parent_uuid"),
            parent_name=data.get("parent_name"),
            created=created,
            size=data.get("size", 0),
            compress=pipeline.get("compress"),
            encrypt=pipeline.get("encrypt"),
            gpg_recipient=pipeline.get("gpg_recipient"),
        )

    @classmethod
    def load_metadata(cls, metadata_path: Path) -> RawSnapshot:
        """Load a RawSnapshot from a metadata file.

        Args:
            metadata_path: Path to the .meta file

        Returns:
            RawSnapshot instance

        Raises:
            FileNotFoundError: If metadata file doesn't exist
            json.JSONDecodeError: If metadata file is invalid JSON
        """
        with open(metadata_path, encoding="utf-8") as f:
            data = json.load(f)

        # Derive stream path from metadata path (remove .meta suffix)
        stream_path = metadata_path.with_suffix("")

        return cls.from_dict(data, stream_path)


def get_file_extension(compress: str | None, encrypt: str | None) -> str:
    """Generate the file extension for a raw backup file.

    Args:
        compress: Compression algorithm (gzip, zstd, lz4, etc.) or None
        encrypt: Encryption method (gpg, openssl_enc) or None

    Returns:
        File extension string (e.g., '.btrfs.zst.gpg')
    """
    ext = ".btrfs"

    if compress and compress in COMPRESSION_CONFIG:
        ext += COMPRESSION_CONFIG[compress]["extension"]

    if encrypt == "gpg":
        ext += ".gpg"
    elif encrypt == "openssl_enc":
        ext += ".enc"

    return ext


def parse_stream_filename(filename: str) -> dict[str, Any]:
    """Parse a raw stream filename to extract metadata.

    Parses filenames like:
    - root.20240115T120000.btrfs
    - root.20240115T120000.btrfs.zst
    - root.20240115T120000.btrfs.zst.gpg
    - root.20240115T120000.btrfs.zst.enc

    Args:
        filename: The stream filename (without directory)

    Returns:
        Dictionary with parsed components:
        - name: Snapshot name (e.g., 'root.20240115T120000')
        - compress: Detected compression or None
        - encrypt: Detected encryption or None
    """
    result: dict[str, Any] = {
        "name": "",
        "compress": None,
        "encrypt": None,
    }

    # Check for encryption suffix
    if filename.endswith(".gpg"):
        result["encrypt"] = "gpg"
        filename = filename[:-4]
    elif filename.endswith(".enc"):
        result["encrypt"] = "openssl_enc"
        filename = filename[:-4]

    # Check for compression suffixes
    for algo, config in COMPRESSION_CONFIG.items():
        ext = config["extension"]
        if filename.endswith(ext):
            result["compress"] = algo
            filename = filename[: -len(ext)]
            break

    # Remove .btrfs suffix
    if filename.endswith(".btrfs"):
        filename = filename[:-6]

    result["name"] = filename
    return result


def discover_raw_snapshots(
    directory: Path,
    prefix: str = "",
) -> list[RawSnapshot]:
    """Discover raw snapshots in a directory.

    Scans for .meta files and loads corresponding snapshot metadata.
    Falls back to parsing filenames if metadata files are missing.

    Args:
        directory: Directory to scan
        prefix: Optional prefix filter for snapshot names

    Returns:
        List of RawSnapshot instances, sorted by creation time
    """
    snapshots: list[RawSnapshot] = []

    if not directory.exists():
        return snapshots

    # First pass: find all .meta files
    meta_files = set()
    for item in directory.iterdir():
        if item.suffix == ".meta" and item.is_file():
            meta_files.add(item)

    # Load snapshots from metadata files
    for meta_path in meta_files:
        try:
            snapshot = RawSnapshot.load_metadata(meta_path)
            if not prefix or snapshot.name.startswith(prefix):
                snapshots.append(snapshot)
        except (json.JSONDecodeError, OSError):
            # Skip invalid metadata files
            continue

    # Second pass: find stream files without metadata
    loaded_names = {s.name for s in snapshots}
    for item in directory.iterdir():
        if not item.is_file() or item.suffix == ".meta":
            continue

        # Check if it's a btrfs stream file
        if ".btrfs" not in item.name:
            continue

        parsed = parse_stream_filename(item.name)
        name = parsed["name"]

        # Skip if already loaded from metadata
        if name in loaded_names:
            continue

        # Skip if doesn't match prefix
        if prefix and not name.startswith(prefix):
            continue

        # Create snapshot from filename parsing
        stat = item.stat()
        snapshot = RawSnapshot(
            name=name,
            stream_path=item,
            created=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            size=stat.st_size,
            compress=parsed["compress"],
            encrypt=parsed["encrypt"],
        )
        snapshots.append(snapshot)

    # Sort by creation time
    snapshots.sort(key=lambda s: s.created)
    return snapshots
