"""Operation state persistence for btrfs-backup-ng.

This module provides state tracking and persistence for backup operations,
enabling resume from checkpoints after failures or interruptions.

Key Features:
    - Persistent operation state across runs
    - Checkpoint tracking for multi-snapshot transfers
    - Resume capability for failed/paused operations
    - Stale operation detection and cleanup

Usage:
    from btrfs_backup_ng.core.state import (
        OperationManager,
        OperationState,
    )

    # Start a new operation
    manager = OperationManager()
    operation = manager.create_operation(
        source_volume="/mnt/btrfs",
        targets=["ssh://backup/snapshots"],
    )

    # Track progress
    operation.start_transfer("root-20240101")
    # ... do transfer ...
    operation.complete_transfer("root-20240101")

    # Resume after failure
    operation = manager.get_resumable_operation(operation_id)
    if operation:
        pending = operation.get_pending_transfers()
"""

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Default state directory
DEFAULT_STATE_DIR = Path.home() / ".config" / "btrfs-backup-ng" / "state"


class OperationState(Enum):
    """State of a backup operation."""

    QUEUED = "queued"  # Operation created, not started
    PLANNING = "planning"  # Analyzing snapshots to transfer
    VALIDATING = "validating"  # Running pre-transfer validation
    TRANSFERRING = "transferring"  # Actively transferring snapshots
    VERIFYING = "verifying"  # Verifying transferred snapshots
    COMPLETING = "completing"  # Finalizing operation
    SUCCESS = "success"  # Operation completed successfully
    FAILED = "failed"  # Operation failed
    PAUSED = "paused"  # Operation paused by user


class TransferState(Enum):
    """State of a single snapshot transfer."""

    PENDING = "pending"  # Not yet started
    STARTED = "started"  # Transfer in progress
    COMPLETED = "completed"  # Transfer succeeded
    FAILED = "failed"  # Transfer failed
    SKIPPED = "skipped"  # Skipped (already present, etc.)


@dataclass
class TransferCheckpoint:
    """Checkpoint for a single snapshot transfer."""

    snapshot_name: str
    parent_name: Optional[str] = None
    state: TransferState = TransferState.PENDING
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    bytes_transferred: int = 0
    chunked_transfer_id: Optional[str] = None  # Links to chunked transfer
    attempt_count: int = 0

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "snapshot_name": self.snapshot_name,
            "parent_name": self.parent_name,
            "state": self.state.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error_message": self.error_message,
            "bytes_transferred": self.bytes_transferred,
            "chunked_transfer_id": self.chunked_transfer_id,
            "attempt_count": self.attempt_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TransferCheckpoint":
        """Deserialize from dictionary."""
        return cls(
            snapshot_name=data["snapshot_name"],
            parent_name=data.get("parent_name"),
            state=TransferState(data["state"]),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            error_message=data.get("error_message"),
            bytes_transferred=data.get("bytes_transferred", 0),
            chunked_transfer_id=data.get("chunked_transfer_id"),
            attempt_count=data.get("attempt_count", 0),
        )


@dataclass
class TargetState:
    """State for a single backup target."""

    target_uri: str
    state: OperationState = OperationState.QUEUED
    transfers: list[TransferCheckpoint] = field(default_factory=list)
    error_message: Optional[str] = None

    @property
    def completed_count(self) -> int:
        """Number of completed transfers."""
        return sum(1 for t in self.transfers if t.state == TransferState.COMPLETED)

    @property
    def pending_count(self) -> int:
        """Number of pending transfers."""
        return sum(
            1
            for t in self.transfers
            if t.state in (TransferState.PENDING, TransferState.STARTED)
        )

    @property
    def failed_count(self) -> int:
        """Number of failed transfers."""
        return sum(1 for t in self.transfers if t.state == TransferState.FAILED)

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "target_uri": self.target_uri,
            "state": self.state.value,
            "transfers": [t.to_dict() for t in self.transfers],
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TargetState":
        """Deserialize from dictionary."""
        target = cls(
            target_uri=data["target_uri"],
            state=OperationState(data["state"]),
            error_message=data.get("error_message"),
        )
        target.transfers = [
            TransferCheckpoint.from_dict(t) for t in data.get("transfers", [])
        ]
        return target


@dataclass
class OperationRecord:
    """Complete record of a backup operation."""

    operation_id: str
    state: OperationState
    source_volume: str
    targets: list[TargetState] = field(default_factory=list)
    planned_snapshots: list[str] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""
    completed_at: Optional[str] = None
    resume_count: int = 0
    error_message: Optional[str] = None
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if not self.updated_at:
            self.updated_at = datetime.now().isoformat()

    @property
    def is_resumable(self) -> bool:
        """Check if this operation can be resumed."""
        return self.state in (
            OperationState.TRANSFERRING,
            OperationState.FAILED,
            OperationState.PAUSED,
        )

    @property
    def is_complete(self) -> bool:
        """Check if this operation is finished (success or failure)."""
        return self.state in (OperationState.SUCCESS, OperationState.FAILED)

    @property
    def total_transfers(self) -> int:
        """Total number of transfers across all targets."""
        return sum(len(t.transfers) for t in self.targets)

    @property
    def completed_transfers(self) -> int:
        """Number of completed transfers across all targets."""
        return sum(t.completed_count for t in self.targets)

    @property
    def pending_transfers(self) -> int:
        """Number of pending transfers across all targets."""
        return sum(t.pending_count for t in self.targets)

    @property
    def failed_transfers(self) -> int:
        """Number of failed transfers across all targets."""
        return sum(t.failed_count for t in self.targets)

    @property
    def progress_percent(self) -> float:
        """Overall progress as percentage."""
        if self.total_transfers == 0:
            return 0.0
        return (self.completed_transfers / self.total_transfers) * 100

    def get_pending_transfers(self) -> list[tuple[str, TransferCheckpoint]]:
        """Get all pending transfers across all targets.

        Returns:
            List of (target_uri, checkpoint) tuples
        """
        pending = []
        for target in self.targets:
            for transfer in target.transfers:
                if transfer.state in (TransferState.PENDING, TransferState.STARTED):
                    pending.append((target.target_uri, transfer))
        return pending

    def get_failed_transfers(self) -> list[tuple[str, TransferCheckpoint]]:
        """Get all failed transfers across all targets.

        Returns:
            List of (target_uri, checkpoint) tuples
        """
        failed = []
        for target in self.targets:
            for transfer in target.transfers:
                if transfer.state == TransferState.FAILED:
                    failed.append((target.target_uri, transfer))
        return failed

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "operation_id": self.operation_id,
            "state": self.state.value,
            "source_volume": self.source_volume,
            "targets": [t.to_dict() for t in self.targets],
            "planned_snapshots": self.planned_snapshots,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "completed_at": self.completed_at,
            "resume_count": self.resume_count,
            "error_message": self.error_message,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "OperationRecord":
        """Deserialize from dictionary."""
        record = cls(
            operation_id=data["operation_id"],
            state=OperationState(data["state"]),
            source_volume=data["source_volume"],
            planned_snapshots=data.get("planned_snapshots", []),
            created_at=data["created_at"],
            updated_at=data.get("updated_at", ""),
            completed_at=data.get("completed_at"),
            resume_count=data.get("resume_count", 0),
            error_message=data.get("error_message"),
            metadata=data.get("metadata", {}),
        )
        record.targets = [TargetState.from_dict(t) for t in data.get("targets", [])]
        return record

    def save(self, path: Path) -> None:
        """Save operation record to file."""
        self.updated_at = datetime.now().isoformat()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> "OperationRecord":
        """Load operation record from file."""
        with open(path) as f:
            return cls.from_dict(json.load(f))


class OperationManager:
    """Manages backup operation state persistence.

    This class handles creating, updating, and querying backup operations.
    It provides the main interface for state persistence.
    """

    def __init__(self, state_dir: Optional[Path] = None):
        self.state_dir = state_dir or DEFAULT_STATE_DIR
        self._ensure_state_dir()

    def _ensure_state_dir(self) -> None:
        """Ensure state directory exists."""
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / "operations").mkdir(exist_ok=True)
        (self.state_dir / "archive").mkdir(exist_ok=True)

    def _get_operation_path(self, operation_id: str) -> Path:
        """Get the file path for an operation."""
        return self.state_dir / "operations" / f"{operation_id}.json"

    def _get_archive_path(self, operation_id: str) -> Path:
        """Get the archive path for a completed operation."""
        return self.state_dir / "archive" / f"{operation_id}.json"

    def create_operation(
        self,
        source_volume: str,
        targets: list[str],
        planned_snapshots: Optional[list[str]] = None,
        metadata: Optional[dict] = None,
    ) -> OperationRecord:
        """Create a new backup operation.

        Args:
            source_volume: Source volume path
            targets: List of target URIs
            planned_snapshots: Snapshots to transfer (optional)
            metadata: Additional metadata to store

        Returns:
            New OperationRecord
        """
        operation_id = str(uuid.uuid4())[:8]

        operation = OperationRecord(
            operation_id=operation_id,
            state=OperationState.QUEUED,
            source_volume=source_volume,
            planned_snapshots=planned_snapshots or [],
            metadata=metadata or {},
        )

        # Initialize targets
        for target_uri in targets:
            operation.targets.append(
                TargetState(
                    target_uri=target_uri,
                    state=OperationState.QUEUED,
                )
            )

        operation.save(self._get_operation_path(operation_id))
        logger.info("Created operation %s for %s", operation_id, source_volume)

        return operation

    def get_operation(self, operation_id: str) -> Optional[OperationRecord]:
        """Get an operation by ID.

        Args:
            operation_id: The operation ID

        Returns:
            OperationRecord or None if not found
        """
        path = self._get_operation_path(operation_id)
        if not path.exists():
            # Check archive
            path = self._get_archive_path(operation_id)
            if not path.exists():
                return None

        try:
            return OperationRecord.load(path)
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Failed to load operation %s: %s", operation_id, e)
            return None

    def update_operation(self, operation: OperationRecord) -> None:
        """Save updated operation state.

        Args:
            operation: The operation to save
        """
        operation.save(self._get_operation_path(operation.operation_id))

    def list_operations(
        self,
        include_archived: bool = False,
        state_filter: Optional[list[OperationState]] = None,
    ) -> list[OperationRecord]:
        """List all operations.

        Args:
            include_archived: Include completed/archived operations
            state_filter: Only return operations in these states

        Returns:
            List of OperationRecord
        """
        operations = []

        # Active operations
        ops_dir = self.state_dir / "operations"
        if ops_dir.exists():
            for path in ops_dir.glob("*.json"):
                try:
                    op = OperationRecord.load(path)
                    if state_filter is None or op.state in state_filter:
                        operations.append(op)
                except Exception as e:
                    logger.warning("Failed to load %s: %s", path, e)

        # Archived operations
        if include_archived:
            archive_dir = self.state_dir / "archive"
            if archive_dir.exists():
                for path in archive_dir.glob("*.json"):
                    try:
                        op = OperationRecord.load(path)
                        if state_filter is None or op.state in state_filter:
                            operations.append(op)
                    except Exception as e:
                        logger.warning("Failed to load %s: %s", path, e)

        # Sort by creation time (newest first)
        operations.sort(key=lambda o: o.created_at, reverse=True)
        return operations

    def get_resumable_operations(self) -> list[OperationRecord]:
        """Get all operations that can be resumed.

        Returns:
            List of resumable OperationRecord
        """
        return self.list_operations(
            state_filter=[
                OperationState.TRANSFERRING,
                OperationState.FAILED,
                OperationState.PAUSED,
            ]
        )

    def archive_operation(self, operation_id: str) -> bool:
        """Move a completed operation to archive.

        Args:
            operation_id: The operation ID

        Returns:
            True if archived, False if not found or not complete
        """
        operation = self.get_operation(operation_id)
        if operation is None:
            return False

        if not operation.is_complete:
            logger.warning("Cannot archive incomplete operation %s", operation_id)
            return False

        # Move to archive
        src_path = self._get_operation_path(operation_id)
        dst_path = self._get_archive_path(operation_id)

        if src_path.exists():
            operation.save(dst_path)
            src_path.unlink()
            logger.info("Archived operation %s", operation_id)
            return True

        return False

    def delete_operation(self, operation_id: str, force: bool = False) -> bool:
        """Delete an operation.

        Args:
            operation_id: The operation ID
            force: Delete even if not complete

        Returns:
            True if deleted
        """
        operation = self.get_operation(operation_id)
        if operation is None:
            return False

        if not force and not operation.is_complete:
            logger.warning("Cannot delete active operation %s (use force=True)", operation_id)
            return False

        # Delete from operations or archive
        for path in [
            self._get_operation_path(operation_id),
            self._get_archive_path(operation_id),
        ]:
            if path.exists():
                path.unlink()
                logger.info("Deleted operation %s", operation_id)
                return True

        return False

    def cleanup_old_operations(self, max_age_days: int = 30) -> int:
        """Delete archived operations older than max_age_days.

        Args:
            max_age_days: Maximum age in days

        Returns:
            Number of operations deleted
        """
        cutoff = time.time() - (max_age_days * 24 * 3600)
        deleted = 0

        archive_dir = self.state_dir / "archive"
        if not archive_dir.exists():
            return 0

        for path in archive_dir.glob("*.json"):
            try:
                op = OperationRecord.load(path)
                if op.completed_at:
                    completed_time = datetime.fromisoformat(op.completed_at)
                    if completed_time.timestamp() < cutoff:
                        path.unlink()
                        deleted += 1
                        logger.debug("Deleted old operation %s", op.operation_id)
            except Exception as e:
                logger.warning("Failed to process %s: %s", path, e)

        if deleted:
            logger.info("Cleaned up %d old operations", deleted)
        return deleted

    def detect_stale_operations(self, max_age_hours: int = 24) -> list[OperationRecord]:
        """Find operations that appear to be stale/abandoned.

        An operation is considered stale if:
        - It's in TRANSFERRING state
        - It hasn't been updated in max_age_hours

        Args:
            max_age_hours: Maximum hours since last update

        Returns:
            List of stale operations
        """
        cutoff = time.time() - (max_age_hours * 3600)
        stale = []

        for op in self.list_operations(state_filter=[OperationState.TRANSFERRING]):
            if op.updated_at:
                updated_time = datetime.fromisoformat(op.updated_at)
                if updated_time.timestamp() < cutoff:
                    stale.append(op)

        return stale


class OperationContext:
    """Context manager for tracking operation state.

    Usage:
        manager = OperationManager()
        with OperationContext(manager, source, targets) as op:
            op.start_planning()
            # ... plan transfers ...
            op.start_transferring()
            for snapshot in snapshots:
                op.start_transfer(snapshot.name, target_uri)
                # ... do transfer ...
                op.complete_transfer(snapshot.name, target_uri)
    """

    def __init__(
        self,
        manager: OperationManager,
        source_volume: str,
        targets: list[str],
        operation_id: Optional[str] = None,
    ):
        self.manager = manager
        self.source_volume = source_volume
        self.target_uris = targets
        self._operation: Optional[OperationRecord] = None
        self._resume_id = operation_id

    def __enter__(self) -> "OperationContext":
        if self._resume_id:
            self._operation = self.manager.get_operation(self._resume_id)
            if self._operation:
                self._operation.resume_count += 1
                self._operation.state = OperationState.TRANSFERRING
                self.manager.update_operation(self._operation)
        else:
            self._operation = self.manager.create_operation(
                self.source_volume,
                self.target_uris,
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._operation:
            if exc_type is not None:
                self._operation.state = OperationState.FAILED
                self._operation.error_message = str(exc_val)
            self.manager.update_operation(self._operation)
        return False

    @property
    def operation(self) -> OperationRecord:
        """Get the current operation record."""
        if self._operation is None:
            raise RuntimeError("OperationContext not entered")
        return self._operation

    @property
    def operation_id(self) -> str:
        """Get the operation ID."""
        return self.operation.operation_id

    def set_planned_snapshots(self, snapshots: list[str]) -> None:
        """Set the planned snapshots for transfer."""
        self.operation.planned_snapshots = snapshots
        self.manager.update_operation(self.operation)

    def start_planning(self) -> None:
        """Mark operation as planning."""
        self.operation.state = OperationState.PLANNING
        self.manager.update_operation(self.operation)

    def start_validating(self) -> None:
        """Mark operation as validating."""
        self.operation.state = OperationState.VALIDATING
        self.manager.update_operation(self.operation)

    def start_transferring(self) -> None:
        """Mark operation as transferring."""
        self.operation.state = OperationState.TRANSFERRING
        self.manager.update_operation(self.operation)

    def add_transfer(
        self,
        snapshot_name: str,
        target_uri: str,
        parent_name: Optional[str] = None,
    ) -> None:
        """Add a transfer checkpoint for tracking.

        Args:
            snapshot_name: Name of snapshot to transfer
            target_uri: Target URI
            parent_name: Optional parent snapshot name
        """
        for target in self.operation.targets:
            if target.target_uri == target_uri:
                target.transfers.append(
                    TransferCheckpoint(
                        snapshot_name=snapshot_name,
                        parent_name=parent_name,
                    )
                )
                break
        self.manager.update_operation(self.operation)

    def start_transfer(self, snapshot_name: str, target_uri: str) -> None:
        """Mark a transfer as started.

        Args:
            snapshot_name: Name of snapshot
            target_uri: Target URI
        """
        for target in self.operation.targets:
            if target.target_uri == target_uri:
                for transfer in target.transfers:
                    if transfer.snapshot_name == snapshot_name:
                        transfer.state = TransferState.STARTED
                        transfer.started_at = datetime.now().isoformat()
                        transfer.attempt_count += 1
                        break
                break
        self.manager.update_operation(self.operation)

    def complete_transfer(
        self,
        snapshot_name: str,
        target_uri: str,
        bytes_transferred: int = 0,
    ) -> None:
        """Mark a transfer as completed.

        Args:
            snapshot_name: Name of snapshot
            target_uri: Target URI
            bytes_transferred: Number of bytes transferred
        """
        for target in self.operation.targets:
            if target.target_uri == target_uri:
                for transfer in target.transfers:
                    if transfer.snapshot_name == snapshot_name:
                        transfer.state = TransferState.COMPLETED
                        transfer.completed_at = datetime.now().isoformat()
                        transfer.bytes_transferred = bytes_transferred
                        break
                break
        self.manager.update_operation(self.operation)

    def fail_transfer(
        self,
        snapshot_name: str,
        target_uri: str,
        error: str,
    ) -> None:
        """Mark a transfer as failed.

        Args:
            snapshot_name: Name of snapshot
            target_uri: Target URI
            error: Error message
        """
        for target in self.operation.targets:
            if target.target_uri == target_uri:
                for transfer in target.transfers:
                    if transfer.snapshot_name == snapshot_name:
                        transfer.state = TransferState.FAILED
                        transfer.error_message = error
                        break
                break
        self.manager.update_operation(self.operation)

    def skip_transfer(self, snapshot_name: str, target_uri: str) -> None:
        """Mark a transfer as skipped.

        Args:
            snapshot_name: Name of snapshot
            target_uri: Target URI
        """
        for target in self.operation.targets:
            if target.target_uri == target_uri:
                for transfer in target.transfers:
                    if transfer.snapshot_name == snapshot_name:
                        transfer.state = TransferState.SKIPPED
                        break
                break
        self.manager.update_operation(self.operation)

    def complete_operation(self) -> None:
        """Mark the entire operation as successful."""
        self.operation.state = OperationState.SUCCESS
        self.operation.completed_at = datetime.now().isoformat()
        self.manager.update_operation(self.operation)

    def fail_operation(self, error: str) -> None:
        """Mark the entire operation as failed."""
        self.operation.state = OperationState.FAILED
        self.operation.error_message = error
        self.operation.completed_at = datetime.now().isoformat()
        self.manager.update_operation(self.operation)

    def pause_operation(self) -> None:
        """Pause the operation."""
        self.operation.state = OperationState.PAUSED
        self.manager.update_operation(self.operation)
