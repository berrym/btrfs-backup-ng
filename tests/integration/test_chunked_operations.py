"""Integration tests for chunked transfer operations.

These tests verify that the chunked transfer system is properly wired
into the operations.py send_snapshot function and works end-to-end.
"""

import io
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch


from btrfs_backup_ng.core.chunked_transfer import (
    ChunkedTransferManager,
    TransferConfig,
    TransferStatus,
    ChunkStatus,
)
from btrfs_backup_ng.core.operations import (
    send_snapshot,
    _do_chunked_transfer,
    _transfer_chunks_local,
    _transfer_chunks_ssh,
)


class MockSnapshot:
    """Mock snapshot for testing."""

    def __init__(self, name: str, path: str):
        self._name = name
        self._path = Path(path)
        self.endpoint = MagicMock()
        self.locks = set()
        self.parent_locks = set()

    def __str__(self) -> str:
        return self._name

    def get_name(self) -> str:
        return self._name

    def get_path(self) -> Path:
        return self._path


class MockEndpoint:
    """Mock endpoint for testing."""

    def __init__(self, path: str = "/backup", is_remote: bool = False):
        self.config = {"path": path}
        self._is_remote = is_remote

    def __str__(self) -> str:
        return f"MockEndpoint({self.config['path']})"

    def receive(self, stdin):
        """Mock receive that creates a process-like object."""
        mock_process = MagicMock()
        mock_process.stdin = MagicMock()
        mock_process.stdout = MagicMock()
        mock_process.stderr = MagicMock()
        mock_process.wait.return_value = 0
        mock_process.returncode = 0
        mock_process.poll.return_value = None
        return mock_process


class TestSendSnapshotChunkedOption:
    """Tests for chunked transfer option in send_snapshot."""

    def test_use_chunked_false_returns_none(self):
        """When use_chunked is False, should return None (standard transfer)."""
        snapshot = MockSnapshot("test-snap", "/mnt/.snapshots/test-snap")
        endpoint = MockEndpoint()

        # Mock the send process
        mock_send_process = MagicMock()
        mock_send_process.stdout = io.BytesIO(b"test data")
        mock_send_process.wait.return_value = 0
        mock_send_process.returncode = 0
        snapshot.endpoint.send.return_value = mock_send_process

        # Mock the receive process
        mock_receive_process = MagicMock()
        mock_receive_process.wait.return_value = 0
        mock_receive_process.returncode = 0
        mock_receive_process.stderr = None

        with patch.object(endpoint, "receive", return_value=mock_receive_process):
            with patch("btrfs_backup_ng.core.operations._ensure_destination_exists"):
                with patch("btrfs_backup_ng.core.operations._verify_destination_space"):
                    result = send_snapshot(
                        snapshot,
                        endpoint,
                        options={"use_chunked": False, "check_space": False},
                    )

        # Standard transfer returns None
        assert result is None

    def test_use_chunked_true_creates_manager(self):
        """When use_chunked is True and no manager provided, should create one."""
        snapshot = MockSnapshot("test-snap", "/mnt/.snapshots/test-snap")
        endpoint = MockEndpoint()

        with patch(
            "btrfs_backup_ng.core.operations._do_chunked_transfer"
        ) as mock_chunked:
            mock_chunked.return_value = "test-transfer-id"

            with patch("btrfs_backup_ng.core.operations._ensure_destination_exists"):
                with patch("btrfs_backup_ng.core.operations._verify_destination_space"):
                    result = send_snapshot(
                        snapshot,
                        endpoint,
                        options={"use_chunked": True, "check_space": False},
                    )

        # Should have called _do_chunked_transfer
        assert mock_chunked.called
        # Should return a transfer ID
        assert result == "test-transfer-id"

    def test_use_chunked_with_provided_manager(self):
        """When use_chunked is True with provided manager, should use it."""
        snapshot = MockSnapshot("test-snap", "/mnt/.snapshots/test-snap")
        endpoint = MockEndpoint()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))
            manager = ChunkedTransferManager(config)

            with patch(
                "btrfs_backup_ng.core.operations._do_chunked_transfer"
            ) as mock_chunked:
                mock_chunked.return_value = "custom-id"

                with patch(
                    "btrfs_backup_ng.core.operations._ensure_destination_exists"
                ):
                    with patch(
                        "btrfs_backup_ng.core.operations._verify_destination_space"
                    ):
                        result = send_snapshot(
                            snapshot,
                            endpoint,
                            options={"use_chunked": True, "check_space": False},
                            chunked_manager=manager,
                        )

            # Should pass the provided manager
            call_kwargs = mock_chunked.call_args[1]
            assert call_kwargs["chunked_manager"] is manager
            assert result == "custom-id"


class TestDoChunkedTransfer:
    """Tests for _do_chunked_transfer function."""

    def test_creates_manifest_for_new_transfer(self):
        """Should create a new manifest for non-resume transfers."""
        snapshot = MockSnapshot("test-snap", "/mnt/.snapshots/test-snap")
        endpoint = MockEndpoint()

        # Mock send process
        mock_send = MagicMock()
        mock_send.stdout = io.BytesIO(b"x" * 1024)  # 1KB of data
        mock_send.wait.return_value = 0
        snapshot.endpoint.send.return_value = mock_send

        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(
                cache_directory=Path(tmpdir),
                chunk_size_mb=1,  # Small chunks for testing
            )
            manager = ChunkedTransferManager(config)

            with patch(
                "btrfs_backup_ng.core.operations._transfer_chunks_local"
            ) as mock_local:
                with patch("btrfs_backup_ng.core.operations.log_transaction"):
                    transfer_id = _do_chunked_transfer(
                        snapshot=snapshot,
                        destination_endpoint=endpoint,
                        parent=None,
                        clones=None,
                        options={},
                        chunked_manager=manager,
                    )

            # Should have created a transfer
            assert transfer_id is not None
            assert len(transfer_id) == 8  # UUID prefix

            # Should have called local transfer
            assert mock_local.called

    def test_resume_transfer_continues_from_checkpoint(self):
        """Should resume from existing manifest when resume_transfer_id provided."""
        snapshot = MockSnapshot("test-snap", "/mnt/.snapshots/test-snap")
        endpoint = MockEndpoint()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))
            manager = ChunkedTransferManager(config)

            # Create a transfer to resume
            manifest = manager.create_transfer(
                snapshot_path="/mnt/.snapshots/test-snap",
                snapshot_name="test-snap",
                destination="MockEndpoint(/backup)",
            )
            # Mark it as transferring (resumable state)
            manifest.status = TransferStatus.TRANSFERRING
            manifest.save(manager._get_manifest_path(manifest.transfer_id))

            with patch("btrfs_backup_ng.core.operations._transfer_chunks_local"):
                with patch("btrfs_backup_ng.core.operations.log_transaction"):
                    result_id = _do_chunked_transfer(
                        snapshot=snapshot,
                        destination_endpoint=endpoint,
                        parent=None,
                        clones=None,
                        options={},
                        chunked_manager=manager,
                        resume_transfer_id=manifest.transfer_id,
                    )

            # Should use the same transfer ID
            assert result_id == manifest.transfer_id

    def test_ssh_endpoint_uses_ssh_transfer(self):
        """Should use SSH transfer for remote endpoints."""
        snapshot = MockSnapshot("test-snap", "/mnt/.snapshots/test-snap")
        endpoint = MockEndpoint(is_remote=True)

        # Mock send process
        mock_send = MagicMock()
        mock_send.stdout = io.BytesIO(b"x" * 1024)
        mock_send.wait.return_value = 0
        snapshot.endpoint.send.return_value = mock_send

        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))
            manager = ChunkedTransferManager(config)

            with patch(
                "btrfs_backup_ng.core.operations._transfer_chunks_ssh"
            ) as mock_ssh:
                with patch("btrfs_backup_ng.core.operations.log_transaction"):
                    _do_chunked_transfer(
                        snapshot=snapshot,
                        destination_endpoint=endpoint,
                        parent=None,
                        clones=None,
                        options={},
                        chunked_manager=manager,
                    )

            # Should have called SSH transfer
            assert mock_ssh.called


class TestTransferChunksLocal:
    """Tests for _transfer_chunks_local function."""

    def test_pipes_chunks_to_receive(self):
        """Should pipe all chunks to the receive process."""
        from btrfs_backup_ng.core.chunked_transfer import ChunkInfo

        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))
            manager = ChunkedTransferManager(config)

            # Create a manifest with chunks
            manifest = manager.create_transfer(
                snapshot_path="/test/snap",
                snapshot_name="test-snap",
                destination="local",
            )

            # Create some test chunks using real ChunkInfo objects
            chunks_dir = manager._get_transfer_dir(manifest.transfer_id) / "chunks"
            chunks_dir.mkdir(parents=True)

            chunk_data = b"test chunk data"
            for i in range(3):
                chunk_file = chunks_dir / f"chunk_{i:06d}.bin"
                chunk_file.write_bytes(chunk_data)
                manifest.chunks.append(
                    ChunkInfo(
                        sequence=i,
                        size=len(chunk_data),
                        checksum="",
                        status=ChunkStatus.WRITTEN,
                        filename=f"chunk_{i:06d}.bin",
                    )
                )

            manifest.status = TransferStatus.TRANSFERRING
            manifest.save(manager._get_manifest_path(manifest.transfer_id))

            # Mock endpoint
            endpoint = MockEndpoint()
            mock_process = MagicMock()
            mock_process.wait.return_value = 0
            mock_process.returncode = 0
            mock_process.stdin = MagicMock()

            with patch.object(endpoint, "receive", return_value=mock_process):
                with patch.object(manager, "create_reassembly_reader") as mock_reader:
                    mock_reader.return_value.pipe_to_process.return_value = 45

                    _transfer_chunks_local(
                        manifest=manifest,
                        destination_endpoint=endpoint,
                        chunked_manager=manager,
                        options={},
                    )

            # Should have called pipe_to_process
            assert mock_reader.return_value.pipe_to_process.called


class TestTransferChunksSSH:
    """Tests for _transfer_chunks_ssh function."""

    def test_uses_receive_chunked_if_available(self):
        """Should use receive_chunked method if endpoint has it."""
        from btrfs_backup_ng.core.chunked_transfer import ChunkInfo

        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))
            manager = ChunkedTransferManager(config)

            manifest = manager.create_transfer(
                snapshot_path="/test/snap",
                snapshot_name="test-snap",
                destination="ssh://backup",
            )
            manifest.status = TransferStatus.TRANSFERRING

            # Add a pending chunk so the function doesn't return early
            manifest.chunks.append(
                ChunkInfo(
                    sequence=0,
                    size=1024,
                    checksum="abc123",
                    status=ChunkStatus.WRITTEN,
                    filename="chunk_000000.bin",
                )
            )

            # Create mock SSH endpoint with receive_chunked
            endpoint = MagicMock()
            endpoint._is_remote = True
            endpoint.receive_chunked.return_value = True

            with patch.object(manager, "create_reassembly_reader"):
                _transfer_chunks_ssh(
                    manifest=manifest,
                    destination_endpoint=endpoint,
                    chunked_manager=manager,
                    options={},
                )

            # Should have called receive_chunked
            assert endpoint.receive_chunked.called

    def test_falls_back_to_streaming_receive(self):
        """Should fall back to streaming if receive_chunked not available."""
        from btrfs_backup_ng.core.chunked_transfer import ChunkInfo

        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))
            manager = ChunkedTransferManager(config)

            manifest = manager.create_transfer(
                snapshot_path="/test/snap",
                snapshot_name="test-snap",
                destination="ssh://backup",
            )
            manifest.status = TransferStatus.TRANSFERRING

            # Add a pending chunk so the function doesn't return early
            manifest.chunks.append(
                ChunkInfo(
                    sequence=0,
                    size=1024,
                    checksum="abc123",
                    status=ChunkStatus.WRITTEN,
                    filename="chunk_000000.bin",
                )
            )

            # Create mock endpoint WITHOUT receive_chunked
            endpoint = MagicMock(spec=["receive", "_is_remote", "config"])
            endpoint._is_remote = True
            endpoint.config = {"path": "/backup"}

            mock_process = MagicMock()
            mock_process.stdin = MagicMock()
            mock_process.wait.return_value = 0
            mock_process.poll.return_value = None
            mock_process.returncode = 0
            endpoint.receive.return_value = mock_process

            with patch.object(manager, "create_reassembly_reader") as mock_reader:
                mock_reader.return_value.read_chunks.return_value = iter([])

                _transfer_chunks_ssh(
                    manifest=manifest,
                    destination_endpoint=endpoint,
                    chunked_manager=manager,
                    options={},
                )

            # Should have called receive (fallback)
            assert endpoint.receive.called


class TestChunkedTransferEndToEnd:
    """End-to-end tests for chunked transfer flow."""

    def test_full_chunked_transfer_flow(self):
        """Test complete chunked transfer from snapshot to destination."""
        snapshot = MockSnapshot("daily-2024-01-01", "/mnt/.snapshots/daily-2024-01-01")
        endpoint = MockEndpoint()

        # Create test data that will be "sent"
        test_data = b"x" * (64 * 1024)  # 64KB of data

        # Mock send process
        mock_send = MagicMock()
        mock_send.stdout = io.BytesIO(test_data)
        mock_send.wait.return_value = 0
        mock_send.returncode = 0
        snapshot.endpoint.send.return_value = mock_send

        # Mock receive process
        mock_receive = MagicMock()
        mock_receive.stdin = MagicMock()
        mock_receive.wait.return_value = 0
        mock_receive.returncode = 0
        mock_receive.stderr = None

        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(
                cache_directory=Path(tmpdir),
                chunk_size_mb=1,  # 1MB chunks
                cleanup_on_success=False,  # Keep files for inspection
            )
            manager = ChunkedTransferManager(config)

            with patch.object(endpoint, "receive", return_value=mock_receive):
                with patch(
                    "btrfs_backup_ng.core.operations._ensure_destination_exists"
                ):
                    with patch(
                        "btrfs_backup_ng.core.operations._verify_destination_space"
                    ):
                        transfer_id = send_snapshot(
                            snapshot,
                            endpoint,
                            options={"use_chunked": True, "check_space": False},
                            chunked_manager=manager,
                        )

            # Should have completed successfully
            assert transfer_id is not None

            # Verify the transfer was tracked
            final_manifest = manager.get_transfer(transfer_id)
            assert final_manifest is not None
            assert final_manifest.status == TransferStatus.COMPLETED
            assert final_manifest.chunk_count >= 1

    def test_chunked_transfer_with_parent(self):
        """Test chunked transfer with parent snapshot for incremental."""
        snapshot = MockSnapshot("daily-2024-01-02", "/mnt/.snapshots/daily-2024-01-02")
        parent = MockSnapshot("daily-2024-01-01", "/mnt/.snapshots/daily-2024-01-01")
        endpoint = MockEndpoint()

        test_data = b"incremental data" * 100

        mock_send = MagicMock()
        mock_send.stdout = io.BytesIO(test_data)
        mock_send.wait.return_value = 0
        mock_send.returncode = 0
        snapshot.endpoint.send.return_value = mock_send

        mock_receive = MagicMock()
        mock_receive.stdin = MagicMock()
        mock_receive.wait.return_value = 0
        mock_receive.returncode = 0
        mock_receive.stderr = None

        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(
                cache_directory=Path(tmpdir),
                cleanup_on_success=False,
            )
            manager = ChunkedTransferManager(config)

            with patch.object(endpoint, "receive", return_value=mock_receive):
                with patch(
                    "btrfs_backup_ng.core.operations._ensure_destination_exists"
                ):
                    with patch(
                        "btrfs_backup_ng.core.operations._verify_destination_space"
                    ):
                        transfer_id = send_snapshot(
                            snapshot,
                            endpoint,
                            parent=parent,
                            options={"use_chunked": True, "check_space": False},
                            chunked_manager=manager,
                        )

            # Should have passed parent to send
            snapshot.endpoint.send.assert_called_with(
                snapshot, parent=parent, clones=None
            )

            # Verify parent info in manifest
            manifest = manager.get_transfer(transfer_id)
            assert manifest.parent_name == str(parent)
