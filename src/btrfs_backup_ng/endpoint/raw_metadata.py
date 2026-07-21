"""Raw target metadata handling for btrfs send streams.

This module provides classes for tracking metadata about raw backup files
(btrfs send streams saved to files with optional compression/encryption).
"""

from __future__ import annotations

import functools
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from btrfs_backup_ng import __version__
from btrfs_backup_ng.__logger__ import logger

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


def _to_utc(dt: datetime) -> datetime:
    """Return ``dt`` as a UTC-aware datetime so sidecars record unambiguous
    ISO-8601 UTC (a naive datetime is assumed to already be UTC)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fsync_directory(directory: Path) -> None:
    """Best-effort fsync of a directory so a rename into it is durable."""
    try:
        dfd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass


@functools.total_ordering
@dataclass(eq=False, repr=False)
class RawSnapshot:
    """Metadata for a raw backup file (btrfs send stream).

    Implements the ``__util__.Snapshot`` interface (get_name/get_path/time_obj/
    locks/find_parent/comparison) so raw backups can be listed, restored,
    verified, and pruned like btrfs snapshots. Identity is by NAME: a source
    btrfs snapshot and the raw stream backed up from it share a name, so
    name-based equality is the correct cross-type "same snapshot" relation.

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
        prefix / time_format: Snapshot-interface fields (name is authoritative)
        endpoint: the raw endpoint that owns this snapshot (set on discovery)
        locks / parent_locks: retention lock sets (Snapshot-interface parity)
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
    openssl_cipher: str | None = None
    # --- authoritative sidecar (0.8.5) ---
    # checksum_value: hex sha256 of the committed ciphertext, sealed at write time
    # by reading the file back after the atomic commit (so it reflects the bytes on
    # disk). None for legacy sidecars or when the read-back failed (best-effort).
    checksum_value: str | None = None
    # The digest algorithm ``checksum_value`` was computed with. Always "sha256"
    # today; carried so ``raw verify`` refuses to compare (reports "unverifiable")
    # rather than false-flag corruption if a sidecar ever records another algorithm.
    checksum_algorithm: str = "sha256"
    provenance_origin: str = "native-write"
    # Whether the stream is known to be a COMPLETE btrfs send. "complete" for a
    # native atomic commit (a failed transfer never commits); "unknown" for a
    # backfilled or filename-inferred legacy stream, which could be truncated. This
    # is a MARKER for callers to treat such a record conservatively (do not assume a
    # verified complete backup); it is not itself an enforced prune guard.
    stream_completeness: str = "complete"
    # For a `raw encrypt` remediation: the filename of the plaintext stream this
    # encrypted stream was produced from (audit trail). None otherwise.
    remediation_source: str | None = None
    # --- Snapshot-interface compatibility (0.8.5) ---
    prefix: str = ""
    time_format: str | None = None
    endpoint: Any = None
    locks: set[str] = field(default_factory=set)
    parent_locks: set[str] = field(default_factory=set)

    @property
    def metadata_path(self) -> Path:
        """Get the path to the metadata file for this snapshot."""
        return self.stream_path.with_suffix(self.stream_path.suffix + ".meta")

    @property
    def is_incremental(self) -> bool:
        """Check if this is an incremental backup."""
        return self.parent_uuid is not None or self.parent_name is not None

    # --- __util__.Snapshot interface -------------------------------------
    def get_name(self) -> str:
        """Return the snapshot name (authoritative, stored on disk)."""
        return self.name

    def get_path(self) -> Path:
        """Return the path to the stream file (the raw analogue of a subvol)."""
        return self.stream_path

    @property
    def time_obj(self) -> time.struct_time:
        """Creation time as a ``struct_time``, matching __util__.Snapshot.time_obj.

        Consumers call ``time.strftime(fmt, snap.time_obj)`` (restore listing /
        interactive picker) and compare ``snap.time_obj <= target`` where target
        is a struct_time (``restore --before``); returning the same type as a
        btrfs Snapshot makes those paths work uniformly across snapshot types
        (and makes cross-type ordering well-defined) instead of raising TypeError.
        """
        return self.created.timetuple()

    def __repr__(self) -> str:
        return self.name

    def __eq__(self, other: object) -> bool:
        # Identity by name so a raw backup equals the btrfs snapshot it came
        # from (both expose get_name()). NotImplemented lets Python try the
        # reflected comparison for unrelated types.
        get_name = getattr(other, "get_name", None)
        if get_name is None:
            return NotImplemented
        return self.name == get_name()

    def __lt__(self, other: object) -> bool:
        other_time = getattr(other, "time_obj", None)
        if other_time is None:
            return NotImplemented
        return self.time_obj < other_time

    # Defining __eq__ without __hash__ makes instances unhashable, matching
    # __util__.Snapshot and keeping set-based paths consistent across snapshot
    # types.

    def find_parent(self, present_snapshots: list[RawSnapshot]) -> RawSnapshot | None:
        """Most suitable already-present snapshot to use as an incremental
        parent, or None. Mirrors __util__.Snapshot.find_parent."""
        if self in present_snapshots:
            return None
        for present_snapshot in reversed(present_snapshots):
            if present_snapshot < self:
                return present_snapshot
        if present_snapshots:
            return present_snapshots[0]
        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize snapshot metadata to the v2 sidecar dict.

        v2 is additive over v1: the nested ``pipeline`` block gains
        ``openssl_cipher``, and ``checksum``/``provenance`` blocks are added. A v1
        reader ignores the new keys; ``from_dict`` reads either version. ``created``
        is ISO-8601 in UTC. ``checksum.value`` is the sha256 of the committed
        ciphertext (null for legacy sidecars or a best-effort read-back failure).
        """
        return {
            "version": 2,
            "name": self.name,
            "uuid": self.uuid,
            "parent_uuid": self.parent_uuid,
            "parent_name": self.parent_name,
            "created": _to_utc(self.created).isoformat(),
            "size": self.size,
            "pipeline": {
                "compress": self.compress,
                "encrypt": self.encrypt,
                "gpg_recipient": self.gpg_recipient,
                "openssl_cipher": self.openssl_cipher,
            },
            "checksum": {
                "algorithm": self.checksum_algorithm,
                "value": self.checksum_value,
            },
            "provenance": {
                "origin": self.provenance_origin,
                "stream_completeness": self.stream_completeness,
                "remediated_from": self.remediation_source,
                "tool_version": __version__,
            },
            # Kept for backward compatibility with v1 readers.
            "btrfs_backup_ng_version": __version__,
        }

    def serialize(self) -> bytes:
        """Return the canonical on-disk sidecar bytes (pretty JSON, UTF-8).

        Single definition of the wire format so the local writer
        (``save_metadata``) and the raw+ssh remote writer cannot drift -- both a
        local ``.meta`` and its remote twin are byte-for-byte the same document."""
        return json.dumps(self.to_dict(), indent=2).encode("utf-8")

    def save_metadata(self) -> None:
        """Write the sidecar atomically (temp -> fsync -> rename -> dir fsync) at
        mode 0600, so a crash never leaves a partial/half-written .meta and the
        metadata dossier is not world-readable.

        O_NOFOLLOW on the temp open refuses to follow a symlink at the ``.meta.tmp``
        path: writing while walking an untrusted directory (``raw backfill-metadata``
        over a target with foreign/legacy content, typically as root) must not let a
        pre-planted ``<name>.meta.tmp`` symlink redirect the O_CREAT|O_TRUNC write to
        an arbitrary file."""
        meta = self.metadata_path
        tmp = meta.with_name(meta.name + ".tmp")
        payload = self.serialize()
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        try:
            os.write(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, meta)
        _fsync_directory(meta.parent)

    @classmethod
    def from_dict(cls, data: dict[str, Any], stream_path: Path) -> RawSnapshot:
        """Create a RawSnapshot from a metadata dictionary.

        Args:
            data: Metadata dictionary (from JSON file)
            stream_path: Path to the stream file

        Returns:
            RawSnapshot instance
        """
        # `or {}` (not a default arg) so an explicit JSON null for a block does
        # not become None and crash the .get() calls below.
        pipeline = data.get("pipeline") or {}
        checksum = data.get("checksum") or {}
        provenance = data.get("provenance") or {}
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
            # v2 fields (absent in v1 -> defaults):
            openssl_cipher=pipeline.get("openssl_cipher"),
            checksum_value=checksum.get("value"),
            checksum_algorithm=checksum.get("algorithm") or "sha256",
            provenance_origin=provenance.get("origin", "native-write"),
            stream_completeness=provenance.get("stream_completeness") or "complete",
            remediation_source=provenance.get("remediated_from"),
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
        except Exception as e:
            # A sidecar EXISTS but could not be parsed (corrupt/truncated JSON, wrong
            # types, non-UTF8, pathologically-nested JSON). Two rules:
            #  1. Skip only THIS file, never abort the whole directory listing -- so a
            #     broad except (a RecursionError from deeply-nested JSON is a
            #     RuntimeError, NOT in the old narrow tuple, and would otherwise blind
            #     the tool to every healthy backup in the directory).
            #  2. Do NOT silently pretend the sidecar is absent: WARN by name. The
            #     stream still falls back to filename inference in the second pass, but
            #     that loses the authoritative compress/encrypt/cipher/checksum, so the
            #     operator must know their metadata was damaged.
            logger.warning(
                "Raw sidecar %s is unreadable/corrupt and was ignored (%s); its "
                "stream will be listed from the filename only, losing the recorded "
                "compress/encrypt/cipher/checksum. Re-run the backup, or "
                "'raw backfill-metadata' to rebuild a (non-authoritative) sidecar.",
                meta_path,
                e,
            )
            continue
        # An empty/missing name yields a meaningless phantom record (and, paired with
        # the second pass, double-counts the stream). Treat it as invalid: warn + skip
        # so the stream is instead listed from its filename with a real name.
        if not snapshot.name:
            logger.warning(
                "Raw sidecar %s records no snapshot name; ignoring it (the stream will "
                "be listed from its filename instead).",
                meta_path,
            )
            continue
        # A sidecar whose recorded name disagrees with its own filename is suspicious
        # (a renamed stream, or a hand-crafted/corrupt sidecar). Keep the authoritative
        # record, but warn so the operator can reconcile -- and the path-based dedup in
        # the second pass ensures the stream is not ALSO counted under its filename.
        filename_name = parse_stream_filename(meta_path.with_suffix("").name)["name"]
        if snapshot.name != filename_name:
            logger.warning(
                "Raw sidecar %s records name %r but its stream file is named %r; using "
                "the sidecar's name.",
                meta_path,
                snapshot.name,
                filename_name,
            )
        if not prefix or snapshot.name.startswith(prefix):
            snapshots.append(snapshot)

    # Second pass: find stream files without metadata
    loaded_names = {s.name for s in snapshots}
    # Dedup by resolved stream PATH as well as by name, so a stream whose sidecar
    # records a name different from its filename is never counted twice (once from the
    # sidecar, once inferred from the filename).
    loaded_paths = {str(s.stream_path) for s in snapshots}
    inferred_names: set[str] = set()
    # Sorted so that when two streams collide on a derived name the winner is
    # DETERMINISTIC across runs and hosts (iterdir order is otherwise arbitrary). The
    # winner is the lexicographically-first filename (so a plaintext ".btrfs" beats a
    # ".btrfs.zst" of the same name) -- arbitrary-but-stable; a real collision is a
    # warned anomaly the operator should resolve, not a normal state.
    for item in sorted(directory.iterdir()):
        if not item.is_file() or item.suffix == ".meta":
            continue

        # Skip in-progress artifacts and the per-target lock file: a ".part" is an
        # uncommitted stream, a ".tmp" is an uncommitted sidecar, and
        # ".btrfs-backup-ng.lock" is the mutual-exclusion lock. All contain ".btrfs"
        # in their name, so without this they would be parsed as phantom backups.
        if item.name.endswith((".part", ".tmp", ".lock")):
            continue

        # Check if it's a btrfs stream file
        if ".btrfs" not in item.name:
            continue

        # This exact stream is already represented by a sidecar (matched by path, so a
        # sidecar whose recorded name differs from the filename does not cause a
        # second, filename-inferred entry for the same file).
        if str(item) in loaded_paths:
            continue

        parsed = parse_stream_filename(item.name)
        name = parsed["name"]

        # Skip if already loaded from metadata (by name)
        if name in loaded_names:
            continue

        # Skip if doesn't match prefix
        if prefix and not name.startswith(prefix):
            continue

        # Two DIFFERENT sidecar-less stream files that derive the SAME name (e.g. a
        # plaintext `x.btrfs` and a compressed `x.btrfs.zst`) would both be listed under
        # one name, violating the name-based identity restore/prune rely on. Keep the
        # first, warn on the collision.
        if name in inferred_names:
            logger.warning(
                "Two raw streams in %s derive the same name %r (e.g. a plaintext and a "
                "compressed copy); keeping the first and ignoring %s.",
                directory,
                name,
                item.name,
            )
            continue
        inferred_names.add(name)

        # Create snapshot from filename parsing. Mark it honestly: this record was
        # reconstructed from the filename (no authoritative sidecar), so its origin
        # is "filename-inferred" and its completeness is unknown -- never present it
        # as a native atomic backup.
        try:
            stat = item.stat()
        except OSError:
            # The stream vanished between iterdir() and stat() (e.g. a concurrent
            # prune). Skip just this one rather than aborting the whole listing --
            # mirrors the raw+ssh second pass, and the same one-bad-file-blinds-all
            # class the sidecar loop above closes.
            continue
        snapshot = RawSnapshot(
            name=name,
            stream_path=item,
            created=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
            size=stat.st_size,
            compress=parsed["compress"],
            encrypt=parsed["encrypt"],
            provenance_origin="filename-inferred",
            stream_completeness="unknown",
        )
        snapshots.append(snapshot)

    # Sort by creation time
    snapshots.sort(key=lambda s: s.created)
    return snapshots
