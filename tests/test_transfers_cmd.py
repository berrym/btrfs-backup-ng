"""Tests for the transfers CLI command."""

import argparse
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.cli.transfers_cmd import (
    _format_age,
    _format_bytes,
    _get_status_symbol,
    _list_transfers,
    _show_transfer,
    _resume_transfer,
    _cleanup_transfers,
    _list_operations,
    execute_transfers,
)
from btrfs_backup_ng.core.chunked_transfer import (
    ChunkedTransferManager,
    TransferConfig,
    TransferManifest,
    TransferStatus,
    ChunkInfo,
    ChunkStatus,
)
from btrfs_backup_ng.core.state import (
    OperationManager,
    OperationRecord,
    OperationState,
)


class TestFormatBytes:
    """Tests for _format_bytes helper."""

    def test_bytes(self):
        assert _format_bytes(512) == "512.0 B"

    def test_kilobytes(self):
        assert _format_bytes(1536) == "1.5 KB"

    def test_megabytes(self):
        assert _format_bytes(1024 * 1024 * 2.5) == "2.5 MB"

    def test_gigabytes(self):
        assert _format_bytes(1024 * 1024 * 1024 * 3) == "3.0 GB"


class TestFormatAge:
    """Tests for _format_age helper."""

    def test_just_now(self):
        now = datetime.now().isoformat()
        assert "just now" in _format_age(now) or "m ago" in _format_age(now)

    def test_hours_ago(self):
        two_hours_ago = (datetime.now() - timedelta(hours=2)).isoformat()
        assert "h ago" in _format_age(two_hours_ago)

    def test_days_ago(self):
        three_days_ago = (datetime.now() - timedelta(days=3)).isoformat()
        assert "3d ago" in _format_age(three_days_ago)

    def test_invalid_timestamp(self):
        assert _format_age("invalid") == "unknown"


class TestGetStatusSymbol:
    """Tests for _get_status_symbol helper."""

    def test_completed(self):
        assert _get_status_symbol(TransferStatus.COMPLETED) == "OK"

    def test_failed(self):
        assert _get_status_symbol(TransferStatus.FAILED) == "X"

    def test_transferring(self):
        assert _get_status_symbol(TransferStatus.TRANSFERRING) == "->"

    def test_paused(self):
        assert _get_status_symbol(TransferStatus.PAUSED) == "||"


class TestListTransfers:
    """Tests for _list_transfers function."""

    def test_no_transfers(self, capsys):
        """Should show message when no transfers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))

            with patch(
                "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager"
            ) as MockManager:
                mock_manager = MagicMock()
                mock_manager.get_incomplete_transfers.return_value = []
                mock_manager.config.cache_dir = Path(tmpdir)
                MockManager.return_value = mock_manager

                args = argparse.Namespace(json=False)
                result = _list_transfers(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "No incomplete transfers found" in captured.out

    def test_with_transfers(self, capsys):
        """Should list transfers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))
            manager = ChunkedTransferManager(config)

            # Create a test transfer
            manifest = manager.create_transfer(
                snapshot_path="/test/snap",
                snapshot_name="test-snapshot",
                destination="ssh://backup",
            )
            manifest.status = TransferStatus.TRANSFERRING
            manifest.chunks.append(
                ChunkInfo(sequence=0, size=1024, checksum="abc", status=ChunkStatus.TRANSFERRED)
            )
            manifest.save(manager._get_manifest_path(manifest.transfer_id))

            with patch(
                "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager",
                return_value=manager,
            ):
                args = argparse.Namespace(json=False)
                result = _list_transfers(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Incomplete Transfers" in captured.out
        assert manifest.transfer_id in captured.out

    def test_json_output(self, capsys):
        """Should output JSON when requested."""
        import logging

        # Suppress logging during this test to avoid mixing with JSON output
        logging.getLogger("btrfs_backup_ng").setLevel(logging.CRITICAL)

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                config = TransferConfig(cache_directory=Path(tmpdir))
                manager = ChunkedTransferManager(config)

                manifest = manager.create_transfer(
                    snapshot_path="/test/snap",
                    snapshot_name="test-snapshot",
                    destination="local",
                )
                manifest.status = TransferStatus.FAILED
                manifest.save(manager._get_manifest_path(manifest.transfer_id))

                with patch(
                    "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager",
                    return_value=manager,
                ):
                    args = argparse.Namespace(json=True)
                    result = _list_transfers(args)

            assert result == 0
            captured = capsys.readouterr()
            data = json.loads(captured.out)
            assert isinstance(data, list)
            assert len(data) == 1
            assert data[0]["transfer_id"] == manifest.transfer_id
        finally:
            logging.getLogger("btrfs_backup_ng").setLevel(logging.DEBUG)


class TestShowTransfer:
    """Tests for _show_transfer function."""

    def test_transfer_not_found(self, capsys):
        """Should error when transfer not found."""
        with patch(
            "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager"
        ) as MockManager:
            mock_manager = MagicMock()
            mock_manager.get_transfer.return_value = None
            MockManager.return_value = mock_manager

            args = argparse.Namespace(transfer_id="nonexistent", json=False, verbose=0)
            result = _show_transfer(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_show_transfer_details(self, capsys):
        """Should show transfer details."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))
            manager = ChunkedTransferManager(config)

            manifest = manager.create_transfer(
                snapshot_path="/mnt/.snapshots/root-20240101",
                snapshot_name="root-20240101",
                destination="ssh://backup:/backups",
                parent_name="root-20231231",
            )
            manifest.status = TransferStatus.TRANSFERRING
            manifest.total_size = 1024 * 1024 * 100
            manifest.chunks.append(
                ChunkInfo(sequence=0, size=64*1024*1024, checksum="abc123", status=ChunkStatus.TRANSFERRED)
            )
            manifest.save(manager._get_manifest_path(manifest.transfer_id))

            with patch(
                "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager",
                return_value=manager,
            ):
                args = argparse.Namespace(
                    transfer_id=manifest.transfer_id, json=False, verbose=0
                )
                result = _show_transfer(args)

        assert result == 0
        captured = capsys.readouterr()
        assert manifest.transfer_id in captured.out
        assert "root-20240101" in captured.out
        assert "ssh://backup:/backups" in captured.out


class TestResumeTransfer:
    """Tests for _resume_transfer function."""

    def test_transfer_not_found(self, capsys):
        """Should error when transfer not found."""
        with patch(
            "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager"
        ) as MockManager:
            mock_manager = MagicMock()
            mock_manager.get_transfer.return_value = None
            MockManager.return_value = mock_manager

            args = argparse.Namespace(transfer_id="nonexistent", dry_run=False)
            result = _resume_transfer(args)

        assert result == 1

    def test_not_resumable(self, capsys):
        """Should error when transfer not resumable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))
            manager = ChunkedTransferManager(config)

            manifest = manager.create_transfer(
                snapshot_path="/test/snap",
                snapshot_name="test-snap",
                destination="local",
            )
            manifest.status = TransferStatus.COMPLETED
            manifest.save(manager._get_manifest_path(manifest.transfer_id))

            with patch(
                "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager",
                return_value=manager,
            ):
                args = argparse.Namespace(
                    transfer_id=manifest.transfer_id, dry_run=False
                )
                result = _resume_transfer(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "not resumable" in captured.out

    def test_resume_success(self, capsys):
        """Should resume a failed transfer."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))
            manager = ChunkedTransferManager(config)

            manifest = manager.create_transfer(
                snapshot_path="/test/snap",
                snapshot_name="test-snap",
                destination="local",
            )
            manifest.status = TransferStatus.FAILED
            manifest.chunks.append(
                ChunkInfo(sequence=0, size=1024, checksum="abc", status=ChunkStatus.WRITTEN)
            )
            manifest.save(manager._get_manifest_path(manifest.transfer_id))

            with patch(
                "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager",
                return_value=manager,
            ):
                args = argparse.Namespace(
                    transfer_id=manifest.transfer_id, dry_run=False
                )
                result = _resume_transfer(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "marked for resume" in captured.out


class TestCleanupTransfers:
    """Tests for _cleanup_transfers function."""

    def test_cleanup_specific_transfer(self, capsys):
        """Should clean up a specific transfer."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = TransferConfig(cache_directory=Path(tmpdir))
            manager = ChunkedTransferManager(config)

            manifest = manager.create_transfer(
                snapshot_path="/test/snap",
                snapshot_name="test-snap",
                destination="local",
            )
            manifest.status = TransferStatus.COMPLETED
            manifest.save(manager._get_manifest_path(manifest.transfer_id))

            with patch(
                "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager",
                return_value=manager,
            ):
                args = argparse.Namespace(
                    transfer_id=manifest.transfer_id,
                    force=False,
                    max_age=48,
                    dry_run=False,
                )
                result = _cleanup_transfers(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Cleaned up transfer" in captured.out

    def test_cleanup_stale(self, capsys):
        """Should clean up stale transfers."""
        with patch(
            "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager"
        ) as MockManager:
            mock_manager = MagicMock()
            mock_manager.cleanup_stale_transfers.return_value = 2
            MockManager.return_value = mock_manager

            args = argparse.Namespace(
                transfer_id=None, force=False, max_age=48, dry_run=False
            )
            result = _cleanup_transfers(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Cleaned up 2 stale transfer" in captured.out


class TestListOperations:
    """Tests for _list_operations function."""

    def test_no_operations(self, capsys):
        """Should show message when no operations."""
        with patch(
            "btrfs_backup_ng.cli.transfers_cmd.OperationManager"
        ) as MockManager:
            mock_manager = MagicMock()
            mock_manager.list_operations.return_value = []
            MockManager.return_value = mock_manager

            args = argparse.Namespace(all=False, json=False)
            result = _list_operations(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "No operations found" in captured.out

    def test_with_operations(self, capsys):
        """Should list operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            op_manager = OperationManager(state_dir=Path(tmpdir))
            op = op_manager.create_operation(
                source_volume="/mnt/data",
                targets=["ssh://backup:/backups"],
            )
            op.state = OperationState.TRANSFERRING
            op_manager.update_operation(op)

            with patch(
                "btrfs_backup_ng.cli.transfers_cmd.OperationManager",
                return_value=op_manager,
            ):
                args = argparse.Namespace(all=False, json=False)
                result = _list_operations(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Backup Operations" in captured.out
        assert op.operation_id in captured.out


class TestExecuteTransfers:
    """Tests for execute_transfers main entry point."""

    def test_default_action_is_list(self, capsys):
        """Default action should be list."""
        with patch(
            "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager"
        ) as MockManager:
            mock_manager = MagicMock()
            mock_manager.get_incomplete_transfers.return_value = []
            mock_manager.config.cache_dir = Path("/tmp/test")
            MockManager.return_value = mock_manager

            args = argparse.Namespace(
                transfers_action=None, json=False, verbose=0, quiet=False
            )
            result = execute_transfers(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "No incomplete transfers found" in captured.out

    def test_routes_to_show(self, capsys):
        """Should route to show action."""
        with patch(
            "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager"
        ) as MockManager:
            mock_manager = MagicMock()
            mock_manager.get_transfer.return_value = None
            MockManager.return_value = mock_manager

            args = argparse.Namespace(
                transfers_action="show",
                transfer_id="abc123",
                json=False,
                verbose=0,
                quiet=False,
            )
            result = execute_transfers(args)

        assert result == 1  # Not found

    def test_routes_to_cleanup(self, capsys):
        """Should route to cleanup action."""
        with patch(
            "btrfs_backup_ng.cli.transfers_cmd.ChunkedTransferManager"
        ) as MockManager:
            mock_manager = MagicMock()
            mock_manager.cleanup_stale_transfers.return_value = 0
            MockManager.return_value = mock_manager

            args = argparse.Namespace(
                transfers_action="cleanup",
                transfer_id=None,
                force=False,
                max_age=48,
                dry_run=False,
                verbose=0,
                quiet=False,
            )
            result = execute_transfers(args)

        assert result == 0
