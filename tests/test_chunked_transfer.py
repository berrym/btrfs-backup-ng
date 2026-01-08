"""Tests for the chunked transfer module."""

import io
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core.chunked_transfer import (
    ChunkedStreamReader,
    ChunkedStreamWriter,
    ChunkedTransferManager,
    ChunkInfo,
    ChunkStatus,
    TransferConfig,
    TransferManifest,
    TransferStatus,
    estimate_chunk_count,
)
from btrfs_backup_ng.core.errors import ChunkChecksumError, PermanentCorruptedError


class TestChunkInfo:
    """Tests for ChunkInfo dataclass."""

    def test_creation(self):
        """Test basic ChunkInfo creation."""
        chunk = ChunkInfo(
            sequence=0,
            size=1024,
            checksum="abc123",
        )
        assert chunk.sequence == 0
        assert chunk.size == 1024
        assert chunk.checksum == "abc123"
        assert chunk.status == ChunkStatus.PENDING

    def test_to_dict(self):
        """Test serialization to dict."""
        chunk = ChunkInfo(
            sequence=5,
            size=2048,
            checksum="def456",
            status=ChunkStatus.TRANSFERRED,
            filename="chunk_00005.bin",
        )
        data = chunk.to_dict()
        assert data["sequence"] == 5
        assert data["size"] == 2048
        assert data["checksum"] == "def456"
        assert data["status"] == "transferred"
        assert data["filename"] == "chunk_00005.bin"

    def test_from_dict(self):
        """Test deserialization from dict."""
        data = {
            "sequence": 10,
            "size": 4096,
            "checksum": "xyz789",
            "status": "verified",
            "filename": "chunk_00010.bin",
            "transfer_attempts": 2,
            "last_error": "Network error",
        }
        chunk = ChunkInfo.from_dict(data)
        assert chunk.sequence == 10
        assert chunk.size == 4096
        assert chunk.checksum == "xyz789"
        assert chunk.status == ChunkStatus.VERIFIED
        assert chunk.transfer_attempts == 2
        assert chunk.last_error == "Network error"


class TestTransferManifest:
    """Tests for TransferManifest dataclass."""

    def test_creation(self):
        """Test basic manifest creation."""
        manifest = TransferManifest(
            transfer_id="abc123",
            snapshot_name="root-20240101",
            snapshot_path="/mnt/.snapshots/root-20240101",
            parent_name=None,
            parent_path=None,
            destination="ssh://backup/snapshots",
            total_size=1024 * 1024 * 100,
            chunk_size=64 * 1024 * 1024,
            checksum_algorithm="sha256",
        )
        assert manifest.transfer_id == "abc123"
        assert manifest.snapshot_name == "root-20240101"
        assert manifest.status == TransferStatus.INITIALIZING
        assert manifest.created_at  # Should be set automatically

    def test_chunk_properties(self):
        """Test chunk-related properties."""
        manifest = TransferManifest(
            transfer_id="test",
            snapshot_name="test",
            snapshot_path="/test",
            parent_name=None,
            parent_path=None,
            destination="test",
            total_size=None,
            chunk_size=1024,
            checksum_algorithm="sha256",
        )
        manifest.chunks = [
            ChunkInfo(0, 1024, "a", ChunkStatus.VERIFIED),
            ChunkInfo(1, 1024, "b", ChunkStatus.TRANSFERRED),
            ChunkInfo(2, 1024, "c", ChunkStatus.PENDING),
            ChunkInfo(3, 1024, "d", ChunkStatus.FAILED),
        ]

        assert manifest.chunk_count == 4
        assert manifest.completed_chunks == 2
        assert len(manifest.pending_chunks) == 2
        assert manifest.progress_percent == 50.0

    def test_resume_point(self):
        """Test getting resume point."""
        manifest = TransferManifest(
            transfer_id="test",
            snapshot_name="test",
            snapshot_path="/test",
            parent_name=None,
            parent_path=None,
            destination="test",
            total_size=None,
            chunk_size=1024,
            checksum_algorithm="sha256",
        )
        manifest.chunks = [
            ChunkInfo(0, 1024, "a", ChunkStatus.VERIFIED),
            ChunkInfo(1, 1024, "b", ChunkStatus.VERIFIED),
            ChunkInfo(2, 1024, "c", ChunkStatus.FAILED),
            ChunkInfo(3, 1024, "d", ChunkStatus.PENDING),
        ]

        assert manifest.get_resume_point() == 2

    def test_is_resumable(self):
        """Test resumability check."""
        manifest = TransferManifest(
            transfer_id="test",
            snapshot_name="test",
            snapshot_path="/test",
            parent_name=None,
            parent_path=None,
            destination="test",
            total_size=None,
            chunk_size=1024,
            checksum_algorithm="sha256",
        )

        manifest.status = TransferStatus.TRANSFERRING
        assert manifest.is_resumable

        manifest.status = TransferStatus.FAILED
        assert manifest.is_resumable

        manifest.status = TransferStatus.PAUSED
        assert manifest.is_resumable

        manifest.status = TransferStatus.COMPLETED
        assert not manifest.is_resumable

        manifest.status = TransferStatus.INITIALIZING
        assert not manifest.is_resumable

    def test_serialization_round_trip(self):
        """Test that serialization/deserialization preserves data."""
        manifest = TransferManifest(
            transfer_id="test123",
            snapshot_name="root-20240101",
            snapshot_path="/mnt/.snapshots/root-20240101",
            parent_name="root-20231231",
            parent_path="/mnt/.snapshots/root-20231231",
            destination="ssh://backup/snapshots",
            total_size=1024 * 1024 * 500,
            chunk_size=64 * 1024 * 1024,
            checksum_algorithm="sha256",
            status=TransferStatus.TRANSFERRING,
        )
        manifest.chunks = [
            ChunkInfo(0, 1024, "abc", ChunkStatus.VERIFIED),
            ChunkInfo(1, 1024, "def", ChunkStatus.PENDING),
        ]
        manifest.bytes_transferred = 1024
        manifest.resume_count = 2

        data = manifest.to_dict()
        restored = TransferManifest.from_dict(data)

        assert restored.transfer_id == manifest.transfer_id
        assert restored.snapshot_name == manifest.snapshot_name
        assert restored.parent_name == manifest.parent_name
        assert restored.status == manifest.status
        assert len(restored.chunks) == 2
        assert restored.bytes_transferred == 1024
        assert restored.resume_count == 2

    def test_save_and_load(self):
        """Test saving and loading manifest to/from file."""
        manifest = TransferManifest(
            transfer_id="test",
            snapshot_name="test",
            snapshot_path="/test",
            parent_name=None,
            parent_path=None,
            destination="test",
            total_size=1024,
            chunk_size=512,
            checksum_algorithm="sha256",
        )
        manifest.chunks = [ChunkInfo(0, 512, "abc", ChunkStatus.VERIFIED)]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "manifest.json"
            manifest.save(path)

            assert path.exists()

            loaded = TransferManifest.load(path)
            assert loaded.transfer_id == manifest.transfer_id
            assert len(loaded.chunks) == 1


class TestTransferConfig:
    """Tests for TransferConfig."""

    def test_defaults(self):
        """Test default configuration values."""
        config = TransferConfig()
        assert config.enabled
        assert config.chunk_size_mb == 64
        assert config.checksum_algorithm == "sha256"
        assert config.verify_on_receive
        assert config.cleanup_on_success
        assert config.resume_incomplete
        assert config.max_chunk_retries == 3

    def test_chunk_size_bytes(self):
        """Test chunk_size_bytes property."""
        config = TransferConfig(chunk_size_mb=128)
        assert config.chunk_size_bytes == 128 * 1024 * 1024

    def test_cache_dir(self):
        """Test cache_dir property."""
        config = TransferConfig()
        assert config.cache_dir == Path.home() / ".cache" / "btrfs-backup-ng" / "transfers"

        custom_dir = Path("/custom/cache")
        config = TransferConfig(cache_directory=custom_dir)
        assert config.cache_dir == custom_dir


class TestChunkedStreamWriter:
    """Tests for ChunkedStreamWriter."""

    def test_write_chunks(self):
        """Test writing stream to chunks."""
        # Create test data: 3.5 chunks worth
        chunk_size = 1024
        data = b"x" * (chunk_size * 3 + 512)
        stream = io.BytesIO(data)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            writer = ChunkedStreamWriter(
                stream=stream,
                output_dir=output_dir,
                chunk_size=chunk_size,
                checksum_algorithm="sha256",
            )

            chunks = list(writer.write_chunks())

            assert len(chunks) == 4
            assert chunks[0].size == chunk_size
            assert chunks[1].size == chunk_size
            assert chunks[2].size == chunk_size
            assert chunks[3].size == 512
            assert writer.total_bytes == len(data)
            assert writer.chunk_count == 4

            # Verify files were created
            for chunk in chunks:
                chunk_path = output_dir / chunk.filename
                assert chunk_path.exists()
                assert chunk_path.stat().st_size == chunk.size

    def test_checksums_computed(self):
        """Test that checksums are computed correctly."""
        data = b"Hello, World!"
        stream = io.BytesIO(data)

        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ChunkedStreamWriter(
                stream=stream,
                output_dir=Path(tmpdir),
                chunk_size=1024,
                checksum_algorithm="sha256",
            )

            chunks = list(writer.write_chunks())
            assert len(chunks) == 1
            assert chunks[0].checksum  # Should have a checksum
            assert len(chunks[0].checksum) == 64  # SHA256 hex length

    def test_callback_called(self):
        """Test that on_chunk_complete callback is called."""
        data = b"x" * 2048
        stream = io.BytesIO(data)
        callback = MagicMock()

        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ChunkedStreamWriter(
                stream=stream,
                output_dir=Path(tmpdir),
                chunk_size=1024,
                on_chunk_complete=callback,
            )

            chunks = list(writer.write_chunks())
            assert callback.call_count == 2


class TestChunkedStreamReader:
    """Tests for ChunkedStreamReader."""

    def test_read_chunks_in_order(self):
        """Test reading chunks in sequence order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            chunk_dir = Path(tmpdir)

            # Create chunk files
            (chunk_dir / "chunk_000000.bin").write_bytes(b"AAA")
            (chunk_dir / "chunk_000001.bin").write_bytes(b"BBB")
            (chunk_dir / "chunk_000002.bin").write_bytes(b"CCC")

            manifest = TransferManifest(
                transfer_id="test",
                snapshot_name="test",
                snapshot_path="/test",
                parent_name=None,
                parent_path=None,
                destination="test",
                total_size=9,
                chunk_size=3,
                checksum_algorithm="none",
            )
            manifest.chunks = [
                ChunkInfo(0, 3, "", filename="chunk_000000.bin"),
                ChunkInfo(1, 3, "", filename="chunk_000001.bin"),
                ChunkInfo(2, 3, "", filename="chunk_000002.bin"),
            ]

            reader = ChunkedStreamReader(
                chunk_dir=chunk_dir,
                manifest=manifest,
                verify_checksums=False,
            )

            data = list(reader.read_chunks())
            assert data == [b"AAA", b"BBB", b"CCC"]

    def test_verify_checksums(self):
        """Test checksum verification on read."""
        import hashlib

        with tempfile.TemporaryDirectory() as tmpdir:
            chunk_dir = Path(tmpdir)
            chunk_data = b"Test data"
            correct_checksum = hashlib.sha256(chunk_data).hexdigest()

            (chunk_dir / "chunk_000000.bin").write_bytes(chunk_data)

            manifest = TransferManifest(
                transfer_id="test",
                snapshot_name="test",
                snapshot_path="/test",
                parent_name=None,
                parent_path=None,
                destination="test",
                total_size=len(chunk_data),
                chunk_size=len(chunk_data),
                checksum_algorithm="sha256",
            )
            manifest.chunks = [
                ChunkInfo(0, len(chunk_data), correct_checksum, filename="chunk_000000.bin"),
            ]

            reader = ChunkedStreamReader(
                chunk_dir=chunk_dir,
                manifest=manifest,
                verify_checksums=True,
            )

            # Should succeed with correct checksum
            data = list(reader.read_chunks())
            assert data == [chunk_data]

    def test_checksum_mismatch_raises(self):
        """Test that checksum mismatch raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            chunk_dir = Path(tmpdir)
            (chunk_dir / "chunk_000000.bin").write_bytes(b"Test data")

            manifest = TransferManifest(
                transfer_id="test",
                snapshot_name="test",
                snapshot_path="/test",
                parent_name=None,
                parent_path=None,
                destination="test",
                total_size=9,
                chunk_size=9,
                checksum_algorithm="sha256",
            )
            manifest.chunks = [
                ChunkInfo(0, 9, "wrong_checksum", filename="chunk_000000.bin"),
            ]

            reader = ChunkedStreamReader(
                chunk_dir=chunk_dir,
                manifest=manifest,
                verify_checksums=True,
            )

            with pytest.raises(ChunkChecksumError):
                list(reader.read_chunks())

    def test_missing_chunk_raises(self):
        """Test that missing chunk file raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            chunk_dir = Path(tmpdir)
            # Don't create the chunk file

            manifest = TransferManifest(
                transfer_id="test",
                snapshot_name="test",
                snapshot_path="/test",
                parent_name=None,
                parent_path=None,
                destination="test",
                total_size=100,
                chunk_size=100,
                checksum_algorithm="none",
            )
            manifest.chunks = [
                ChunkInfo(0, 100, "", filename="missing_chunk.bin"),
            ]

            reader = ChunkedStreamReader(
                chunk_dir=chunk_dir,
                manifest=manifest,
                verify_checksums=False,
            )

            with pytest.raises(PermanentCorruptedError):
                list(reader.read_chunks())


class TestChunkedTransferManager:
    """Tests for ChunkedTransferManager."""

    @pytest.fixture
    def manager(self, tmp_path):
        """Create a manager with temp cache directory."""
        config = TransferConfig(cache_directory=tmp_path / "transfers")
        return ChunkedTransferManager(config)

    def test_create_transfer(self, manager):
        """Test creating a new transfer."""
        manifest = manager.create_transfer(
            snapshot_path="/mnt/.snapshots/root-20240101",
            snapshot_name="root-20240101",
            destination="ssh://backup/snapshots",
            parent_path="/mnt/.snapshots/root-20231231",
            parent_name="root-20231231",
            total_size=1024 * 1024 * 100,
        )

        assert manifest.transfer_id
        assert manifest.snapshot_name == "root-20240101"
        assert manifest.parent_name == "root-20231231"
        assert manifest.status == TransferStatus.INITIALIZING

        # Manifest should be saved
        loaded = manager.get_transfer(manifest.transfer_id)
        assert loaded is not None
        assert loaded.transfer_id == manifest.transfer_id

    def test_chunk_stream(self, manager):
        """Test chunking a stream."""
        manifest = manager.create_transfer(
            snapshot_path="/test",
            snapshot_name="test",
            destination="test",
        )

        # Create test stream
        data = b"x" * (1024 * 1024)  # 1 MB
        stream = io.BytesIO(data)

        # Reduce chunk size for testing
        manager.config = TransferConfig(
            cache_directory=manager.config.cache_dir,
            chunk_size_mb=1,  # 1 MB chunks
        )

        progress_calls = []

        def on_progress(chunk_num, total, bytes_done):
            progress_calls.append((chunk_num, bytes_done))

        updated = manager.chunk_stream(manifest, stream, on_progress)

        assert updated.status == TransferStatus.TRANSFERRING
        assert updated.chunk_count == 1
        assert updated.total_size == len(data)
        assert len(progress_calls) == 1

    def test_get_incomplete_transfers(self, manager):
        """Test listing incomplete transfers."""
        # Create some transfers with different statuses
        m1 = manager.create_transfer(
            snapshot_path="/test1",
            snapshot_name="test1",
            destination="test",
        )
        m1.status = TransferStatus.FAILED
        m1.save(manager._get_manifest_path(m1.transfer_id))

        m2 = manager.create_transfer(
            snapshot_path="/test2",
            snapshot_name="test2",
            destination="test",
        )
        m2.status = TransferStatus.COMPLETED
        m2.save(manager._get_manifest_path(m2.transfer_id))

        m3 = manager.create_transfer(
            snapshot_path="/test3",
            snapshot_name="test3",
            destination="test",
        )
        m3.status = TransferStatus.TRANSFERRING
        m3.save(manager._get_manifest_path(m3.transfer_id))

        incomplete = manager.get_incomplete_transfers()
        ids = [m.transfer_id for m in incomplete]

        assert m1.transfer_id in ids  # FAILED is resumable
        assert m2.transfer_id not in ids  # COMPLETED is not
        assert m3.transfer_id in ids  # TRANSFERRING is resumable

    def test_resume_transfer(self, manager):
        """Test resuming a transfer."""
        manifest = manager.create_transfer(
            snapshot_path="/test",
            snapshot_name="test",
            destination="test",
        )
        manifest.status = TransferStatus.FAILED
        manifest.save(manager._get_manifest_path(manifest.transfer_id))

        resumed = manager.resume_transfer(manifest.transfer_id)

        assert resumed is not None
        assert resumed.status == TransferStatus.TRANSFERRING
        assert resumed.resume_count == 1

    def test_resume_non_resumable_fails(self, manager):
        """Test that resuming a completed transfer fails."""
        manifest = manager.create_transfer(
            snapshot_path="/test",
            snapshot_name="test",
            destination="test",
        )
        manifest.status = TransferStatus.COMPLETED
        manifest.save(manager._get_manifest_path(manifest.transfer_id))

        resumed = manager.resume_transfer(manifest.transfer_id)
        assert resumed is None

    def test_mark_chunk_transferred(self, manager):
        """Test marking a chunk as transferred."""
        manifest = manager.create_transfer(
            snapshot_path="/test",
            snapshot_name="test",
            destination="test",
        )
        manifest.chunks = [
            ChunkInfo(0, 1024, "abc", ChunkStatus.PENDING),
            ChunkInfo(1, 1024, "def", ChunkStatus.PENDING),
        ]
        manifest.save(manager._get_manifest_path(manifest.transfer_id))

        manager.mark_chunk_transferred(manifest, 0)

        reloaded = manager.get_transfer(manifest.transfer_id)
        assert reloaded.chunks[0].status == ChunkStatus.TRANSFERRED
        assert reloaded.chunks[1].status == ChunkStatus.PENDING

    def test_complete_transfer(self, manager):
        """Test completing a transfer."""
        manifest = manager.create_transfer(
            snapshot_path="/test",
            snapshot_name="test",
            destination="test",
        )
        manifest.status = TransferStatus.TRANSFERRING
        manifest.save(manager._get_manifest_path(manifest.transfer_id))

        # Disable cleanup for this test
        manager.config = TransferConfig(
            cache_directory=manager.config.cache_dir,
            cleanup_on_success=False,
        )

        manager.complete_transfer(manifest)

        reloaded = manager.get_transfer(manifest.transfer_id)
        assert reloaded.status == TransferStatus.COMPLETED
        assert reloaded.completed_at is not None

    def test_cleanup_transfer(self, manager):
        """Test cleaning up a transfer."""
        manifest = manager.create_transfer(
            snapshot_path="/test",
            snapshot_name="test",
            destination="test",
        )
        manifest.status = TransferStatus.COMPLETED
        manifest.save(manager._get_manifest_path(manifest.transfer_id))

        transfer_dir = manager._get_transfer_dir(manifest.transfer_id)
        assert transfer_dir.exists()

        result = manager.cleanup_transfer(manifest.transfer_id)
        assert result
        assert not transfer_dir.exists()

    def test_cleanup_in_progress_requires_force(self, manager):
        """Test that cleaning up in-progress transfer requires force."""
        manifest = manager.create_transfer(
            snapshot_path="/test",
            snapshot_name="test",
            destination="test",
        )
        manifest.status = TransferStatus.TRANSFERRING
        manifest.save(manager._get_manifest_path(manifest.transfer_id))

        # Should fail without force
        result = manager.cleanup_transfer(manifest.transfer_id, force=False)
        assert not result

        # Should succeed with force
        result = manager.cleanup_transfer(manifest.transfer_id, force=True)
        assert result


class TestEstimateChunkCount:
    """Tests for estimate_chunk_count utility."""

    def test_exact_multiple(self):
        """Test when size is exact multiple of chunk size."""
        assert estimate_chunk_count(1024, 256) == 4
        assert estimate_chunk_count(1024 * 1024, 1024 * 1024) == 1

    def test_with_remainder(self):
        """Test when size has remainder."""
        assert estimate_chunk_count(1000, 256) == 4  # 3.9 -> 4
        assert estimate_chunk_count(257, 256) == 2

    def test_smaller_than_chunk(self):
        """Test when size is smaller than chunk size."""
        assert estimate_chunk_count(100, 1024) == 1

    def test_default_chunk_size(self):
        """Test with default chunk size."""
        # 64 MB default
        size = 100 * 1024 * 1024  # 100 MB
        assert estimate_chunk_count(size) == 2  # 100/64 = 1.56 -> 2
