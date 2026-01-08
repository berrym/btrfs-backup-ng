"""Core backup operations for btrfs-backup-ng.

This module contains the extracted backup logic from the original
monolithic __main__.py, organized into focused modules.
"""

from .execution import execute_parallel, run_task
from .operations import send_snapshot, sync_snapshots
from .planning import delete_corrupt_snapshots, plan_transfers

# Error classification and retry framework
from .errors import (
    BackupError,
    ChunkChecksumError,
    ChunkTransferError,
    ErrorCategory,
    InsufficientSpaceError,
    PermanentConfigError,
    PermanentCorruptedError,
    PermanentError,
    PermanentNotFoundError,
    PermanentPermissionError,
    PermanentUnsupportedError,
    SnapshotTransferError,
    SSHAuthenticationError,
    SSHConnectionError,
    TransientBusyError,
    TransientError,
    TransientLockError,
    TransientNetworkError,
    TransientSpaceError,
    TransientTimeoutError,
    classify_error,
    classify_ssh_error,
    classify_transfer_error,
)
from .retry import (
    DEFAULT_NETWORK_POLICY,
    DEFAULT_QUICK_POLICY,
    DEFAULT_TRANSFER_POLICY,
    RetryAttempt,
    RetryContext,
    RetryPolicy,
    RetryResult,
    retry_call,
    with_retry,
    with_retry_async,
)

# Chunked transfer system
from .chunked_transfer import (
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

# Operation state persistence
from .state import (
    OperationContext,
    OperationManager,
    OperationRecord,
    OperationState,
    TargetState,
    TransferCheckpoint,
    TransferState,
)

__all__ = [
    # Operations
    "send_snapshot",
    "sync_snapshots",
    "plan_transfers",
    "delete_corrupt_snapshots",
    "run_task",
    "execute_parallel",
    # Errors
    "BackupError",
    "ChunkChecksumError",
    "ChunkTransferError",
    "ErrorCategory",
    "InsufficientSpaceError",
    "PermanentConfigError",
    "PermanentCorruptedError",
    "PermanentError",
    "PermanentNotFoundError",
    "PermanentPermissionError",
    "PermanentUnsupportedError",
    "SnapshotTransferError",
    "SSHAuthenticationError",
    "SSHConnectionError",
    "TransientBusyError",
    "TransientError",
    "TransientLockError",
    "TransientNetworkError",
    "TransientSpaceError",
    "TransientTimeoutError",
    "classify_error",
    "classify_ssh_error",
    "classify_transfer_error",
    # Retry
    "DEFAULT_NETWORK_POLICY",
    "DEFAULT_QUICK_POLICY",
    "DEFAULT_TRANSFER_POLICY",
    "RetryAttempt",
    "RetryContext",
    "RetryPolicy",
    "RetryResult",
    "retry_call",
    "with_retry",
    "with_retry_async",
    # Chunked transfer
    "ChunkedStreamReader",
    "ChunkedStreamWriter",
    "ChunkedTransferManager",
    "ChunkInfo",
    "ChunkStatus",
    "TransferConfig",
    "TransferManifest",
    "TransferStatus",
    "estimate_chunk_count",
    # State persistence
    "OperationContext",
    "OperationManager",
    "OperationRecord",
    "OperationState",
    "TargetState",
    "TransferCheckpoint",
    "TransferState",
]
