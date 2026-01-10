"""Tests for operation state persistence module."""

import json
import tempfile
from pathlib import Path

import pytest

from btrfs_backup_ng.core.state import (
    OperationContext,
    OperationManager,
    OperationRecord,
    OperationState,
    TargetState,
    TransferCheckpoint,
    TransferState,
)


class TestTransferCheckpoint:
    """Tests for TransferCheckpoint dataclass."""

    def test_creation(self):
        """Test basic checkpoint creation."""
        checkpoint = TransferCheckpoint(
            snapshot_name="root-20240101",
            parent_name="root-20231231",
        )
        assert checkpoint.snapshot_name == "root-20240101"
        assert checkpoint.parent_name == "root-20231231"
        assert checkpoint.state == TransferState.PENDING
        assert checkpoint.attempt_count == 0

    def test_serialization(self):
        """Test serialization round-trip."""
        checkpoint = TransferCheckpoint(
            snapshot_name="root-20240101",
            parent_name="root-20231231",
            state=TransferState.COMPLETED,
            started_at="2024-01-01T12:00:00",
            completed_at="2024-01-01T12:30:00",
            bytes_transferred=1024000,
            attempt_count=2,
        )

        data = checkpoint.to_dict()
        restored = TransferCheckpoint.from_dict(data)

        assert restored.snapshot_name == checkpoint.snapshot_name
        assert restored.parent_name == checkpoint.parent_name
        assert restored.state == checkpoint.state
        assert restored.bytes_transferred == checkpoint.bytes_transferred
        assert restored.attempt_count == checkpoint.attempt_count


class TestTargetState:
    """Tests for TargetState dataclass."""

    def test_creation(self):
        """Test basic target state creation."""
        target = TargetState(target_uri="ssh://backup/snapshots")
        assert target.target_uri == "ssh://backup/snapshots"
        assert target.state == OperationState.QUEUED
        assert len(target.transfers) == 0

    def test_transfer_counts(self):
        """Test transfer count properties."""
        target = TargetState(target_uri="ssh://backup/snapshots")
        target.transfers = [
            TransferCheckpoint("snap1", state=TransferState.COMPLETED),
            TransferCheckpoint("snap2", state=TransferState.COMPLETED),
            TransferCheckpoint("snap3", state=TransferState.PENDING),
            TransferCheckpoint("snap4", state=TransferState.FAILED),
        ]

        assert target.completed_count == 2
        assert target.pending_count == 1
        assert target.failed_count == 1

    def test_serialization(self):
        """Test serialization round-trip."""
        target = TargetState(
            target_uri="ssh://backup/snapshots",
            state=OperationState.TRANSFERRING,
        )
        target.transfers = [
            TransferCheckpoint("snap1", state=TransferState.COMPLETED),
        ]

        data = target.to_dict()
        restored = TargetState.from_dict(data)

        assert restored.target_uri == target.target_uri
        assert restored.state == target.state
        assert len(restored.transfers) == 1


class TestOperationRecord:
    """Tests for OperationRecord dataclass."""

    def test_creation(self):
        """Test basic operation creation."""
        operation = OperationRecord(
            operation_id="abc123",
            state=OperationState.QUEUED,
            source_volume="/mnt/btrfs",
        )
        assert operation.operation_id == "abc123"
        assert operation.state == OperationState.QUEUED
        assert operation.created_at  # Should be auto-set

    def test_is_resumable(self):
        """Test resumability check."""
        operation = OperationRecord(
            operation_id="test",
            state=OperationState.QUEUED,
            source_volume="/mnt/btrfs",
        )

        # Not resumable in QUEUED state
        assert not operation.is_resumable

        # Resumable states
        for state in [
            OperationState.TRANSFERRING,
            OperationState.FAILED,
            OperationState.PAUSED,
        ]:
            operation.state = state
            assert operation.is_resumable

        # Not resumable when complete
        operation.state = OperationState.SUCCESS
        assert not operation.is_resumable

    def test_is_complete(self):
        """Test completion check."""
        operation = OperationRecord(
            operation_id="test",
            state=OperationState.TRANSFERRING,
            source_volume="/mnt/btrfs",
        )

        assert not operation.is_complete

        operation.state = OperationState.SUCCESS
        assert operation.is_complete

        operation.state = OperationState.FAILED
        assert operation.is_complete

    def test_transfer_counts(self):
        """Test transfer count aggregation across targets."""
        operation = OperationRecord(
            operation_id="test",
            state=OperationState.TRANSFERRING,
            source_volume="/mnt/btrfs",
        )

        target1 = TargetState(target_uri="target1")
        target1.transfers = [
            TransferCheckpoint("snap1", state=TransferState.COMPLETED),
            TransferCheckpoint("snap2", state=TransferState.PENDING),
        ]

        target2 = TargetState(target_uri="target2")
        target2.transfers = [
            TransferCheckpoint("snap1", state=TransferState.COMPLETED),
            TransferCheckpoint("snap2", state=TransferState.FAILED),
        ]

        operation.targets = [target1, target2]

        assert operation.total_transfers == 4
        assert operation.completed_transfers == 2
        assert operation.pending_transfers == 1
        assert operation.failed_transfers == 1
        assert operation.progress_percent == 50.0

    def test_get_pending_transfers(self):
        """Test getting pending transfers."""
        operation = OperationRecord(
            operation_id="test",
            state=OperationState.TRANSFERRING,
            source_volume="/mnt/btrfs",
        )

        target = TargetState(target_uri="target1")
        target.transfers = [
            TransferCheckpoint("snap1", state=TransferState.COMPLETED),
            TransferCheckpoint("snap2", state=TransferState.PENDING),
            TransferCheckpoint("snap3", state=TransferState.STARTED),
        ]
        operation.targets = [target]

        pending = operation.get_pending_transfers()
        assert len(pending) == 2
        assert pending[0][0] == "target1"
        assert pending[0][1].snapshot_name == "snap2"

    def test_serialization(self):
        """Test serialization round-trip."""
        operation = OperationRecord(
            operation_id="test123",
            state=OperationState.TRANSFERRING,
            source_volume="/mnt/btrfs",
            planned_snapshots=["snap1", "snap2"],
            resume_count=1,
            metadata={"key": "value"},
        )

        target = TargetState(target_uri="ssh://backup")
        target.transfers = [TransferCheckpoint("snap1")]
        operation.targets = [target]

        data = operation.to_dict()
        restored = OperationRecord.from_dict(data)

        assert restored.operation_id == operation.operation_id
        assert restored.state == operation.state
        assert restored.planned_snapshots == operation.planned_snapshots
        assert restored.resume_count == operation.resume_count
        assert len(restored.targets) == 1

    def test_save_and_load(self):
        """Test saving and loading from file."""
        operation = OperationRecord(
            operation_id="test",
            state=OperationState.TRANSFERRING,
            source_volume="/mnt/btrfs",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "operation.json"
            operation.save(path)

            assert path.exists()

            loaded = OperationRecord.load(path)
            assert loaded.operation_id == operation.operation_id
            assert loaded.state == operation.state


class TestOperationManager:
    """Tests for OperationManager."""

    @pytest.fixture
    def manager(self, tmp_path):
        """Create a manager with temp state directory."""
        return OperationManager(state_dir=tmp_path / "state")

    def test_create_operation(self, manager):
        """Test creating a new operation."""
        operation = manager.create_operation(
            source_volume="/mnt/btrfs",
            targets=["ssh://backup1", "ssh://backup2"],
            planned_snapshots=["snap1", "snap2"],
        )

        assert operation.operation_id
        assert operation.source_volume == "/mnt/btrfs"
        assert len(operation.targets) == 2
        assert operation.planned_snapshots == ["snap1", "snap2"]

        # Should be persisted
        loaded = manager.get_operation(operation.operation_id)
        assert loaded is not None
        assert loaded.operation_id == operation.operation_id

    def test_get_operation_not_found(self, manager):
        """Test getting non-existent operation."""
        result = manager.get_operation("nonexistent")
        assert result is None

    def test_update_operation(self, manager):
        """Test updating operation state."""
        operation = manager.create_operation(
            source_volume="/mnt/btrfs",
            targets=["ssh://backup"],
        )

        operation.state = OperationState.TRANSFERRING
        manager.update_operation(operation)

        loaded = manager.get_operation(operation.operation_id)
        assert loaded.state == OperationState.TRANSFERRING

    def test_list_operations(self, manager):
        """Test listing operations."""
        op1 = manager.create_operation("/mnt/vol1", ["target1"])
        op2 = manager.create_operation("/mnt/vol2", ["target2"])
        op2.state = OperationState.SUCCESS
        manager.update_operation(op2)

        all_ops = manager.list_operations()
        assert len(all_ops) == 2

        # Filter by state
        active = manager.list_operations(state_filter=[OperationState.QUEUED])
        assert len(active) == 1
        assert active[0].operation_id == op1.operation_id

    def test_get_resumable_operations(self, manager):
        """Test getting resumable operations."""
        op1 = manager.create_operation("/mnt/vol1", ["target1"])
        op1.state = OperationState.FAILED
        manager.update_operation(op1)

        op2 = manager.create_operation("/mnt/vol2", ["target2"])
        op2.state = OperationState.SUCCESS
        manager.update_operation(op2)

        op3 = manager.create_operation("/mnt/vol3", ["target3"])
        op3.state = OperationState.PAUSED
        manager.update_operation(op3)

        resumable = manager.get_resumable_operations()
        assert len(resumable) == 2
        ids = [op.operation_id for op in resumable]
        assert op1.operation_id in ids
        assert op3.operation_id in ids

    def test_archive_operation(self, manager):
        """Test archiving completed operation."""
        operation = manager.create_operation("/mnt/btrfs", ["target"])
        operation.state = OperationState.SUCCESS
        manager.update_operation(operation)

        result = manager.archive_operation(operation.operation_id)
        assert result

        # Should still be accessible
        loaded = manager.get_operation(operation.operation_id)
        assert loaded is not None

        # Active directory should be empty
        active = manager.list_operations(include_archived=False)
        assert len(active) == 0

        # Should appear in archived list
        all_ops = manager.list_operations(include_archived=True)
        assert len(all_ops) == 1

    def test_archive_incomplete_fails(self, manager):
        """Test that archiving incomplete operation fails."""
        operation = manager.create_operation("/mnt/btrfs", ["target"])
        operation.state = OperationState.TRANSFERRING
        manager.update_operation(operation)

        result = manager.archive_operation(operation.operation_id)
        assert not result

    def test_delete_operation(self, manager):
        """Test deleting operation."""
        operation = manager.create_operation("/mnt/btrfs", ["target"])
        operation.state = OperationState.SUCCESS
        manager.update_operation(operation)

        result = manager.delete_operation(operation.operation_id)
        assert result

        loaded = manager.get_operation(operation.operation_id)
        assert loaded is None

    def test_delete_active_requires_force(self, manager):
        """Test that deleting active operation requires force."""
        operation = manager.create_operation("/mnt/btrfs", ["target"])
        operation.state = OperationState.TRANSFERRING
        manager.update_operation(operation)

        # Should fail without force
        result = manager.delete_operation(operation.operation_id, force=False)
        assert not result

        # Should succeed with force
        result = manager.delete_operation(operation.operation_id, force=True)
        assert result

    def test_detect_stale_operations(self, manager):
        """Test detecting stale operations."""
        operation = manager.create_operation("/mnt/btrfs", ["target"])
        operation.state = OperationState.TRANSFERRING
        manager.update_operation(operation)

        # Manually modify the file to set old updated_at
        path = manager._get_operation_path(operation.operation_id)
        with open(path) as f:
            data = json.load(f)
        data["updated_at"] = "2020-01-01T00:00:00"
        with open(path, "w") as f:
            json.dump(data, f)

        stale = manager.detect_stale_operations(max_age_hours=1)
        assert len(stale) == 1
        assert stale[0].operation_id == operation.operation_id

    def test_cleanup_old_operations(self, manager):
        """Test cleaning up old archived operations."""
        # Create and archive an operation
        operation = manager.create_operation("/mnt/btrfs", ["target"])
        operation.state = OperationState.SUCCESS
        operation.completed_at = "2020-01-01T00:00:00"  # Old date
        manager.update_operation(operation)
        manager.archive_operation(operation.operation_id)

        # Verify archive exists
        archive_dir = manager.state_dir / "archive"
        assert archive_dir.exists()
        archived_files = list(archive_dir.glob("*.json"))
        assert len(archived_files) == 1

        # Cleanup should delete old operations
        deleted = manager.cleanup_old_operations(max_age_days=1)
        assert deleted == 1

        # Archive should be empty now
        archived_files = list(archive_dir.glob("*.json"))
        assert len(archived_files) == 0

    def test_cleanup_old_operations_no_archive(self, manager):
        """Test cleanup when no archive directory exists."""
        deleted = manager.cleanup_old_operations(max_age_days=30)
        assert deleted == 0

    def test_cleanup_old_operations_keeps_recent(self, manager):
        """Test that cleanup keeps recent operations."""
        from datetime import datetime

        # Create and archive an operation with recent completed_at
        operation = manager.create_operation("/mnt/btrfs", ["target"])
        operation.state = OperationState.SUCCESS
        operation.completed_at = datetime.now().isoformat()
        manager.update_operation(operation)
        manager.archive_operation(operation.operation_id)

        # Cleanup should not delete recent operations
        deleted = manager.cleanup_old_operations(max_age_days=30)
        assert deleted == 0

        # Archive should still have the file
        archive_dir = manager.state_dir / "archive"
        archived_files = list(archive_dir.glob("*.json"))
        assert len(archived_files) == 1

    def test_get_operation_from_archive(self, manager):
        """Test getting operation from archive."""
        operation = manager.create_operation("/mnt/btrfs", ["target"])
        operation.state = OperationState.SUCCESS
        manager.update_operation(operation)
        op_id = operation.operation_id

        # Archive the operation
        manager.archive_operation(op_id)

        # Should still be able to get it
        loaded = manager.get_operation(op_id)
        assert loaded is not None
        assert loaded.operation_id == op_id

    def test_archive_nonexistent_operation(self, manager):
        """Test archiving non-existent operation returns False."""
        result = manager.archive_operation("nonexistent-id")
        assert result is False

    def test_delete_nonexistent_operation(self, manager):
        """Test deleting non-existent operation returns False."""
        result = manager.delete_operation("nonexistent-id")
        assert result is False


class TestOperationContext:
    """Tests for OperationContext context manager."""

    @pytest.fixture
    def manager(self, tmp_path):
        """Create a manager with temp state directory."""
        return OperationManager(state_dir=tmp_path / "state")

    def test_basic_usage(self, manager):
        """Test basic context manager usage."""
        with OperationContext(manager, "/mnt/btrfs", ["target1"]) as ctx:
            assert ctx.operation_id
            assert ctx.operation.state == OperationState.QUEUED

            ctx.start_transferring()
            assert ctx.operation.state == OperationState.TRANSFERRING

            ctx.complete_operation()

        # Verify persisted state
        loaded = manager.get_operation(ctx.operation_id)
        assert loaded.state == OperationState.SUCCESS

    def test_exception_handling(self, manager):
        """Test that exceptions mark operation as failed."""
        try:
            with OperationContext(manager, "/mnt/btrfs", ["target1"]) as ctx:
                ctx.start_transferring()
                raise ValueError("Test error")
        except ValueError:
            pass

        loaded = manager.get_operation(ctx.operation_id)
        assert loaded.state == OperationState.FAILED
        assert "Test error" in loaded.error_message

    def test_transfer_tracking(self, manager):
        """Test tracking individual transfers."""
        with OperationContext(manager, "/mnt/btrfs", ["target1"]) as ctx:
            ctx.add_transfer("snap1", "target1", parent_name="snap0")
            ctx.add_transfer("snap2", "target1", parent_name="snap1")

            ctx.start_transfer("snap1", "target1")
            ctx.complete_transfer("snap1", "target1", bytes_transferred=1000)

            ctx.start_transfer("snap2", "target1")
            ctx.fail_transfer("snap2", "target1", "Network error")

        loaded = manager.get_operation(ctx.operation_id)
        target = loaded.targets[0]

        assert len(target.transfers) == 2
        assert target.transfers[0].state == TransferState.COMPLETED
        assert target.transfers[0].bytes_transferred == 1000
        assert target.transfers[1].state == TransferState.FAILED
        assert target.transfers[1].error_message == "Network error"

    def test_resume_operation(self, manager):
        """Test resuming an existing operation."""
        # Create and partially complete an operation
        with OperationContext(manager, "/mnt/btrfs", ["target1"]) as ctx:
            original_id = ctx.operation_id
            ctx.add_transfer("snap1", "target1")
            ctx.add_transfer("snap2", "target1")
            ctx.start_transfer("snap1", "target1")
            ctx.complete_transfer("snap1", "target1")
            ctx.pause_operation()

        # Resume the operation
        with OperationContext(
            manager, "/mnt/btrfs", ["target1"], operation_id=original_id
        ) as ctx:
            assert ctx.operation_id == original_id
            assert ctx.operation.resume_count == 1
            assert ctx.operation.state == OperationState.TRANSFERRING

            # Continue from where we left off
            ctx.start_transfer("snap2", "target1")
            ctx.complete_transfer("snap2", "target1")
            ctx.complete_operation()

        loaded = manager.get_operation(original_id)
        assert loaded.state == OperationState.SUCCESS
        assert loaded.completed_transfers == 2

    def test_skip_transfer(self, manager):
        """Test skipping a transfer."""
        with OperationContext(manager, "/mnt/btrfs", ["target1"]) as ctx:
            ctx.add_transfer("snap1", "target1")
            ctx.skip_transfer("snap1", "target1")

        loaded = manager.get_operation(ctx.operation_id)
        assert loaded.targets[0].transfers[0].state == TransferState.SKIPPED
