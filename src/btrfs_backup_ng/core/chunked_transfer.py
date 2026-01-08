"""Resumable chunked transfer system for btrfs-backup-ng.

This module implements application-level chunked transfers that enable
resuming failed snapshot transfers from the last successful chunk.
This is a unique capability - no other btrfs backup tool offers this.

Key Features:
    - Split btrfs send streams into checksummed chunks
    - Transfer chunks individually with verification
    - Resume from last successful chunk on failure
    - Seamless reassembly for btrfs receive

Architecture:
    ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
    │ btrfs send  │────▶│ ChunkWriter  │────▶│   Chunks    │
    │  (stream)   │     │ + Checksum   │     │  (storage)  │
    └─────────────┘     └──────────────┘     └─────────────┘
                                                    │
                                                    ▼
    ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
    │btrfs receive│◀────│ ChunkReader  │◀────│  Transfer   │
    │  (stream)   │     │ + Verify     │     │  + Verify   │
    └─────────────┘     └──────────────┘     └─────────────┘

Usage:
    from btrfs_backup_ng.core.chunked_transfer import (
        ChunkedTransferManager,
        TransferConfig,
    )

    manager = ChunkedTransferManager()

    # Start a new chunked transfer
    transfer = manager.start_transfer(
        snapshot_path="/mnt/btrfs/.snapshots/root-20240101",
        destination=ssh_endpoint,
        parent_path="/mnt/btrfs/.snapshots/root-20231231",
    )

    # Resume a failed transfer
    transfer = manager.resume_transfer(transfer_id="abc123")

    # List incomplete transfers
    incomplete = manager.get_incomplete_transfers()
"""

import hashlib
import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import IO, Any, Callable, Iterator, Optional

from .errors import (
    BackupError,
    ChunkChecksumError,
    ChunkTransferError,
    PermanentCorruptedError,
    TransientNetworkError,
)
from .retry import RetryPolicy, with_retry

logger = logging.getLogger(__name__)

# Default paths
DEFAULT_CACHE_DIR = Path.home() / ".cache" / "btrfs-backup-ng" / "transfers"
DEFAULT_CHUNK_SIZE = 64 * 1024 * 1024  # 64 MB


class ChunkStatus(Enum):
    """Status of a single chunk."""

    PENDING = "pending"
    WRITING = "writing"
    WRITTEN = "written"
    TRANSFERRING = "transferring"
    TRANSFERRED = "transferred"
    VERIFIED = "verified"
    FAILED = "failed"


class TransferStatus(Enum):
    """Status of the overall transfer."""

    INITIALIZING = "initializing"
    CHUNKING = "chunking"
    TRANSFERRING = "transferring"
    REASSEMBLING = "reassembling"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


@dataclass
class ChunkInfo:
    """Information about a single chunk."""

    sequence: int
    size: int
    checksum: str
    status: ChunkStatus = ChunkStatus.PENDING
    filename: str = ""
    transfer_attempts: int = 0
    last_error: Optional[str] = None

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "sequence": self.sequence,
            "size": self.size,
            "checksum": self.checksum,
            "status": self.status.value,
            "filename": self.filename,
            "transfer_attempts": self.transfer_attempts,
            "last_error": self.last_error,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ChunkInfo":
        """Deserialize from dictionary."""
        return cls(
            sequence=data["sequence"],
            size=data["size"],
            checksum=data["checksum"],
            status=ChunkStatus(data["status"]),
            filename=data.get("filename", ""),
            transfer_attempts=data.get("transfer_attempts", 0),
            last_error=data.get("last_error"),
        )


@dataclass
class TransferManifest:
    """Manifest tracking the entire chunked transfer."""

    transfer_id: str
    snapshot_name: str
    snapshot_path: str
    parent_name: Optional[str]
    parent_path: Optional[str]
    destination: str
    total_size: Optional[int]  # None if unknown (streaming)
    chunk_size: int
    checksum_algorithm: str
    chunks: list[ChunkInfo] = field(default_factory=list)
    status: TransferStatus = TransferStatus.INITIALIZING
    created_at: str = ""
    updated_at: str = ""
    completed_at: Optional[str] = None
    bytes_transferred: int = 0
    resume_count: int = 0
    error_message: Optional[str] = None

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        self.updated_at = datetime.now().isoformat()

    @property
    def chunk_count(self) -> int:
        """Total number of chunks."""
        return len(self.chunks)

    @property
    def completed_chunks(self) -> int:
        """Number of successfully transferred chunks."""
        return sum(
            1 for c in self.chunks if c.status in (ChunkStatus.VERIFIED, ChunkStatus.TRANSFERRED)
        )

    @property
    def pending_chunks(self) -> list[ChunkInfo]:
        """Chunks that still need to be transferred."""
        return [
            c
            for c in self.chunks
            if c.status not in (ChunkStatus.VERIFIED, ChunkStatus.TRANSFERRED)
        ]

    @property
    def progress_percent(self) -> float:
        """Transfer progress as percentage."""
        if not self.chunks:
            return 0.0
        return (self.completed_chunks / len(self.chunks)) * 100

    @property
    def is_resumable(self) -> bool:
        """Check if this transfer can be resumed."""
        return self.status in (
            TransferStatus.TRANSFERRING,
            TransferStatus.FAILED,
            TransferStatus.PAUSED,
        )

    def get_resume_point(self) -> Optional[int]:
        """Get the sequence number to resume from."""
        for chunk in self.chunks:
            if chunk.status not in (ChunkStatus.VERIFIED, ChunkStatus.TRANSFERRED):
                return chunk.sequence
        return None

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "transfer_id": self.transfer_id,
            "snapshot_name": self.snapshot_name,
            "snapshot_path": self.snapshot_path,
            "parent_name": self.parent_name,
            "parent_path": self.parent_path,
            "destination": self.destination,
            "total_size": self.total_size,
            "chunk_size": self.chunk_size,
            "checksum_algorithm": self.checksum_algorithm,
            "chunks": [c.to_dict() for c in self.chunks],
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "completed_at": self.completed_at,
            "bytes_transferred": self.bytes_transferred,
            "resume_count": self.resume_count,
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TransferManifest":
        """Deserialize from dictionary."""
        manifest = cls(
            transfer_id=data["transfer_id"],
            snapshot_name=data["snapshot_name"],
            snapshot_path=data["snapshot_path"],
            parent_name=data.get("parent_name"),
            parent_path=data.get("parent_path"),
            destination=data["destination"],
            total_size=data.get("total_size"),
            chunk_size=data["chunk_size"],
            checksum_algorithm=data["checksum_algorithm"],
            status=TransferStatus(data["status"]),
            created_at=data["created_at"],
            updated_at=data.get("updated_at", ""),
            completed_at=data.get("completed_at"),
            bytes_transferred=data.get("bytes_transferred", 0),
            resume_count=data.get("resume_count", 0),
            error_message=data.get("error_message"),
        )
        manifest.chunks = [ChunkInfo.from_dict(c) for c in data.get("chunks", [])]
        return manifest

    def save(self, path: Path) -> None:
        """Save manifest to file."""
        self.updated_at = datetime.now().isoformat()
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> "TransferManifest":
        """Load manifest from file."""
        with open(path) as f:
            return cls.from_dict(json.load(f))


@dataclass
class TransferConfig:
    """Configuration for chunked transfers."""

    enabled: bool = True
    chunk_size_mb: int = 64
    checksum_algorithm: str = "sha256"  # sha256, xxhash, none
    verify_on_receive: bool = True
    cleanup_on_success: bool = True
    resume_incomplete: bool = True
    max_chunk_retries: int = 3
    cache_directory: Optional[Path] = None

    @property
    def chunk_size_bytes(self) -> int:
        """Chunk size in bytes."""
        return self.chunk_size_mb * 1024 * 1024

    @property
    def cache_dir(self) -> Path:
        """Get the cache directory path."""
        return self.cache_directory or DEFAULT_CACHE_DIR


class ChunkedStreamWriter:
    """Reads from a stream and writes checksummed chunks to disk.

    This class consumes a btrfs send stream and splits it into
    fixed-size chunks with checksums for later transfer.
    """

    def __init__(
        self,
        stream: IO[bytes],
        output_dir: Path,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        checksum_algorithm: str = "sha256",
        on_chunk_complete: Optional[Callable[[ChunkInfo], None]] = None,
    ):
        self.stream = stream
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.checksum_algorithm = checksum_algorithm
        self.on_chunk_complete = on_chunk_complete
        self._sequence = 0
        self._total_bytes = 0

    def _get_hasher(self) -> Any:
        """Get a hasher for the configured algorithm."""
        if self.checksum_algorithm == "sha256":
            return hashlib.sha256()
        elif self.checksum_algorithm == "md5":
            return hashlib.md5()
        elif self.checksum_algorithm == "none":
            return None
        else:
            # Try xxhash if available
            try:
                import xxhash

                if self.checksum_algorithm == "xxhash":
                    return xxhash.xxh64()
            except ImportError:
                pass
            # Fall back to sha256
            logger.warning(
                "Unknown checksum algorithm '%s', using sha256",
                self.checksum_algorithm,
            )
            return hashlib.sha256()

    def _compute_checksum(self, data: bytes) -> str:
        """Compute checksum of data."""
        hasher = self._get_hasher()
        if hasher is None:
            return ""
        hasher.update(data)
        return hasher.hexdigest()

    def write_chunks(self) -> Iterator[ChunkInfo]:
        """Read stream and yield ChunkInfo for each written chunk."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        while True:
            chunk_data = self.stream.read(self.chunk_size)
            if not chunk_data:
                break

            filename = f"chunk_{self._sequence:06d}.bin"
            chunk_path = self.output_dir / filename

            # Write chunk to disk
            with open(chunk_path, "wb") as f:
                f.write(chunk_data)

            # Compute checksum
            checksum = self._compute_checksum(chunk_data)

            chunk_info = ChunkInfo(
                sequence=self._sequence,
                size=len(chunk_data),
                checksum=checksum,
                status=ChunkStatus.WRITTEN,
                filename=filename,
            )

            self._sequence += 1
            self._total_bytes += len(chunk_data)

            if self.on_chunk_complete:
                self.on_chunk_complete(chunk_info)

            yield chunk_info

        logger.info(
            "Chunking complete: %d chunks, %d bytes total",
            self._sequence,
            self._total_bytes,
        )

    @property
    def total_bytes(self) -> int:
        """Total bytes written so far."""
        return self._total_bytes

    @property
    def chunk_count(self) -> int:
        """Number of chunks written so far."""
        return self._sequence


class ChunkedStreamReader:
    """Reads chunks from disk and provides a stream for btrfs receive.

    This class reassembles chunks into a continuous stream that can
    be piped to btrfs receive.
    """

    def __init__(
        self,
        chunk_dir: Path,
        manifest: TransferManifest,
        verify_checksums: bool = True,
    ):
        self.chunk_dir = chunk_dir
        self.manifest = manifest
        self.verify_checksums = verify_checksums
        self._current_sequence = 0

    def _compute_checksum(self, data: bytes) -> str:
        """Compute checksum of data."""
        if self.manifest.checksum_algorithm == "sha256":
            return hashlib.sha256(data).hexdigest()
        elif self.manifest.checksum_algorithm == "md5":
            return hashlib.md5(data).hexdigest()
        elif self.manifest.checksum_algorithm == "none":
            return ""
        else:
            try:
                import xxhash

                if self.manifest.checksum_algorithm == "xxhash":
                    return xxhash.xxh64(data).hexdigest()
            except ImportError:
                pass
            return hashlib.sha256(data).hexdigest()

    def read_chunks(self) -> Iterator[bytes]:
        """Yield chunk data in sequence order."""
        for chunk in sorted(self.manifest.chunks, key=lambda c: c.sequence):
            chunk_path = self.chunk_dir / chunk.filename

            if not chunk_path.exists():
                raise PermanentCorruptedError(
                    f"Missing chunk file: {chunk.filename}",
                    context={"chunk_sequence": chunk.sequence},
                )

            with open(chunk_path, "rb") as f:
                data = f.read()

            if self.verify_checksums and chunk.checksum:
                actual_checksum = self._compute_checksum(data)
                if actual_checksum != chunk.checksum:
                    raise ChunkChecksumError(
                        chunk.sequence,
                        chunk.checksum,
                        actual_checksum,
                    )

            self._current_sequence = chunk.sequence
            yield data

    def pipe_to_process(self, process: subprocess.Popen) -> int:
        """Pipe all chunks to a process's stdin.

        Args:
            process: Process with stdin available for writing

        Returns:
            Total bytes written
        """
        total_bytes = 0

        if process.stdin is None:
            raise ValueError("Process stdin not available")

        for chunk_data in self.read_chunks():
            process.stdin.write(chunk_data)
            total_bytes += len(chunk_data)

        process.stdin.close()
        return total_bytes


class ChunkedTransferManager:
    """Manages chunked transfers with resume capability.

    This is the main interface for chunked transfers. It handles:
    - Creating new transfers
    - Resuming failed transfers
    - Cleaning up completed/orphaned transfers
    - Tracking transfer state
    """

    def __init__(self, config: Optional[TransferConfig] = None):
        self.config = config or TransferConfig()
        self._ensure_cache_dir()

    def _ensure_cache_dir(self) -> None:
        """Ensure cache directory exists."""
        self.config.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_transfer_dir(self, transfer_id: str) -> Path:
        """Get the directory for a specific transfer."""
        return self.config.cache_dir / transfer_id

    def _get_manifest_path(self, transfer_id: str) -> Path:
        """Get the manifest file path for a transfer."""
        return self._get_transfer_dir(transfer_id) / "manifest.json"

    def create_transfer(
        self,
        snapshot_path: str,
        snapshot_name: str,
        destination: str,
        parent_path: Optional[str] = None,
        parent_name: Optional[str] = None,
        total_size: Optional[int] = None,
    ) -> TransferManifest:
        """Create a new chunked transfer.

        Args:
            snapshot_path: Full path to the snapshot
            snapshot_name: Name of the snapshot
            destination: String representation of destination
            parent_path: Optional parent snapshot path for incremental
            parent_name: Optional parent snapshot name
            total_size: Optional total size if known

        Returns:
            TransferManifest for the new transfer
        """
        transfer_id = str(uuid.uuid4())[:8]
        transfer_dir = self._get_transfer_dir(transfer_id)
        transfer_dir.mkdir(parents=True, exist_ok=True)

        manifest = TransferManifest(
            transfer_id=transfer_id,
            snapshot_name=snapshot_name,
            snapshot_path=snapshot_path,
            parent_name=parent_name,
            parent_path=parent_path,
            destination=destination,
            total_size=total_size,
            chunk_size=self.config.chunk_size_bytes,
            checksum_algorithm=self.config.checksum_algorithm,
        )

        manifest.save(self._get_manifest_path(transfer_id))
        logger.info("Created chunked transfer %s for %s", transfer_id, snapshot_name)

        return manifest

    def chunk_stream(
        self,
        manifest: TransferManifest,
        stream: IO[bytes],
        on_progress: Optional[Callable[[int, int, int], None]] = None,
    ) -> TransferManifest:
        """Chunk a btrfs send stream.

        Args:
            manifest: The transfer manifest
            stream: The btrfs send output stream
            on_progress: Optional callback(chunk_num, total_chunks, bytes)

        Returns:
            Updated manifest with chunk information
        """
        transfer_dir = self._get_transfer_dir(manifest.transfer_id)
        chunks_dir = transfer_dir / "chunks"
        chunks_dir.mkdir(exist_ok=True)

        manifest.status = TransferStatus.CHUNKING

        def on_chunk_complete(chunk_info: ChunkInfo) -> None:
            manifest.chunks.append(chunk_info)
            manifest.bytes_transferred += chunk_info.size
            manifest.save(self._get_manifest_path(manifest.transfer_id))
            if on_progress:
                on_progress(
                    chunk_info.sequence + 1,
                    -1,  # Unknown total during streaming
                    manifest.bytes_transferred,
                )

        writer = ChunkedStreamWriter(
            stream=stream,
            output_dir=chunks_dir,
            chunk_size=self.config.chunk_size_bytes,
            checksum_algorithm=self.config.checksum_algorithm,
            on_chunk_complete=on_chunk_complete,
        )

        # Consume the stream
        for _ in writer.write_chunks():
            pass

        manifest.total_size = writer.total_bytes
        manifest.status = TransferStatus.TRANSFERRING
        manifest.save(self._get_manifest_path(manifest.transfer_id))

        logger.info(
            "Chunking complete for %s: %d chunks, %d bytes",
            manifest.transfer_id,
            writer.chunk_count,
            writer.total_bytes,
        )

        return manifest

    def get_transfer(self, transfer_id: str) -> Optional[TransferManifest]:
        """Get a transfer by ID.

        Args:
            transfer_id: The transfer ID

        Returns:
            TransferManifest or None if not found
        """
        manifest_path = self._get_manifest_path(transfer_id)
        if not manifest_path.exists():
            return None
        return TransferManifest.load(manifest_path)

    def get_incomplete_transfers(self) -> list[TransferManifest]:
        """Get all incomplete transfers that can be resumed."""
        incomplete = []

        if not self.config.cache_dir.exists():
            return incomplete

        for transfer_dir in self.config.cache_dir.iterdir():
            if not transfer_dir.is_dir():
                continue

            manifest_path = transfer_dir / "manifest.json"
            if not manifest_path.exists():
                continue

            try:
                manifest = TransferManifest.load(manifest_path)
                if manifest.is_resumable:
                    incomplete.append(manifest)
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(
                    "Invalid manifest in %s: %s",
                    transfer_dir,
                    e,
                )

        return sorted(incomplete, key=lambda m: m.created_at, reverse=True)

    def resume_transfer(self, transfer_id: str) -> Optional[TransferManifest]:
        """Mark a transfer as ready to resume.

        Args:
            transfer_id: The transfer ID to resume

        Returns:
            Updated manifest or None if not found/not resumable
        """
        manifest = self.get_transfer(transfer_id)
        if manifest is None:
            logger.error("Transfer %s not found", transfer_id)
            return None

        if not manifest.is_resumable:
            logger.error(
                "Transfer %s is not resumable (status: %s)",
                transfer_id,
                manifest.status.value,
            )
            return None

        manifest.resume_count += 1
        manifest.status = TransferStatus.TRANSFERRING
        manifest.save(self._get_manifest_path(transfer_id))

        logger.info(
            "Resuming transfer %s from chunk %d/%d",
            transfer_id,
            manifest.get_resume_point() or 0,
            manifest.chunk_count,
        )

        return manifest

    def mark_chunk_transferred(
        self,
        manifest: TransferManifest,
        chunk_sequence: int,
    ) -> None:
        """Mark a chunk as successfully transferred.

        Args:
            manifest: The transfer manifest
            chunk_sequence: The sequence number of the transferred chunk
        """
        for chunk in manifest.chunks:
            if chunk.sequence == chunk_sequence:
                chunk.status = ChunkStatus.TRANSFERRED
                break

        manifest.save(self._get_manifest_path(manifest.transfer_id))

    def mark_chunk_failed(
        self,
        manifest: TransferManifest,
        chunk_sequence: int,
        error: str,
    ) -> None:
        """Mark a chunk as failed.

        Args:
            manifest: The transfer manifest
            chunk_sequence: The sequence number of the failed chunk
            error: Error message
        """
        for chunk in manifest.chunks:
            if chunk.sequence == chunk_sequence:
                chunk.status = ChunkStatus.FAILED
                chunk.transfer_attempts += 1
                chunk.last_error = error
                break

        manifest.save(self._get_manifest_path(manifest.transfer_id))

    def complete_transfer(self, manifest: TransferManifest) -> None:
        """Mark a transfer as completed.

        Args:
            manifest: The transfer manifest
        """
        manifest.status = TransferStatus.COMPLETED
        manifest.completed_at = datetime.now().isoformat()
        manifest.save(self._get_manifest_path(manifest.transfer_id))

        logger.info(
            "Transfer %s completed: %d chunks, %d bytes, %d resumes",
            manifest.transfer_id,
            manifest.chunk_count,
            manifest.bytes_transferred,
            manifest.resume_count,
        )

        if self.config.cleanup_on_success:
            self.cleanup_transfer(manifest.transfer_id)

    def fail_transfer(self, manifest: TransferManifest, error: str) -> None:
        """Mark a transfer as failed.

        Args:
            manifest: The transfer manifest
            error: Error message
        """
        manifest.status = TransferStatus.FAILED
        manifest.error_message = error
        manifest.save(self._get_manifest_path(manifest.transfer_id))

        logger.error(
            "Transfer %s failed at chunk %d/%d: %s",
            manifest.transfer_id,
            manifest.completed_chunks,
            manifest.chunk_count,
            error,
        )

    def pause_transfer(self, manifest: TransferManifest) -> None:
        """Pause a transfer.

        Args:
            manifest: The transfer manifest
        """
        manifest.status = TransferStatus.PAUSED
        manifest.save(self._get_manifest_path(manifest.transfer_id))

        logger.info(
            "Transfer %s paused at chunk %d/%d",
            manifest.transfer_id,
            manifest.completed_chunks,
            manifest.chunk_count,
        )

    def cleanup_transfer(self, transfer_id: str, force: bool = False) -> bool:
        """Remove all files for a transfer.

        Args:
            transfer_id: The transfer ID
            force: If True, delete even if not completed

        Returns:
            True if cleanup was performed
        """
        transfer_dir = self._get_transfer_dir(transfer_id)
        if not transfer_dir.exists():
            return False

        manifest = self.get_transfer(transfer_id)
        if manifest and not force:
            if manifest.status not in (
                TransferStatus.COMPLETED,
                TransferStatus.FAILED,
            ):
                logger.warning(
                    "Transfer %s is still in progress, use force=True to delete",
                    transfer_id,
                )
                return False

        shutil.rmtree(transfer_dir)
        logger.info("Cleaned up transfer %s", transfer_id)
        return True

    def cleanup_stale_transfers(self, max_age_hours: int = 48) -> int:
        """Remove transfers older than max_age_hours.

        Args:
            max_age_hours: Maximum age in hours

        Returns:
            Number of transfers cleaned up
        """
        cleaned = 0
        cutoff = time.time() - (max_age_hours * 3600)

        for transfer in self.get_incomplete_transfers():
            created = datetime.fromisoformat(transfer.created_at)
            if created.timestamp() < cutoff:
                if self.cleanup_transfer(transfer.transfer_id, force=True):
                    cleaned += 1

        if cleaned:
            logger.info("Cleaned up %d stale transfers", cleaned)

        return cleaned

    def get_chunk_path(self, transfer_id: str, chunk_sequence: int) -> Optional[Path]:
        """Get the path to a chunk file.

        Args:
            transfer_id: The transfer ID
            chunk_sequence: The chunk sequence number

        Returns:
            Path to chunk file or None if not found
        """
        manifest = self.get_transfer(transfer_id)
        if manifest is None:
            return None

        for chunk in manifest.chunks:
            if chunk.sequence == chunk_sequence:
                chunk_path = (
                    self._get_transfer_dir(transfer_id) / "chunks" / chunk.filename
                )
                if chunk_path.exists():
                    return chunk_path

        return None

    def create_reassembly_reader(
        self,
        manifest: TransferManifest,
    ) -> ChunkedStreamReader:
        """Create a reader to reassemble chunks for btrfs receive.

        Args:
            manifest: The transfer manifest

        Returns:
            ChunkedStreamReader instance
        """
        chunks_dir = self._get_transfer_dir(manifest.transfer_id) / "chunks"
        return ChunkedStreamReader(
            chunk_dir=chunks_dir,
            manifest=manifest,
            verify_checksums=self.config.verify_on_receive,
        )


def estimate_chunk_count(size_bytes: int, chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
    """Estimate the number of chunks for a given size.

    Args:
        size_bytes: Total size in bytes
        chunk_size: Chunk size in bytes

    Returns:
        Estimated number of chunks
    """
    return (size_bytes + chunk_size - 1) // chunk_size
