"""Error classification hierarchy for btrfs-backup-ng.

This module provides a structured error classification system that enables
intelligent retry decisions and actionable error messages. Errors are
categorized by their nature (transient vs permanent) and their source.

Usage:
    from btrfs_backup_ng.core.errors import (
        TransientNetworkError,
        PermanentPermissionError,
        classify_error,
    )

    try:
        do_transfer()
    except BackupError as e:
        if e.is_retryable:
            # retry with backoff
        else:
            # fail fast with actionable message
"""

from enum import Enum
from typing import Optional


class ErrorCategory(Enum):
    """Classification of error types for retry decisions."""

    # Transient errors - worth retrying
    TRANSIENT_NETWORK = "transient_network"
    TRANSIENT_SPACE = "transient_space"
    TRANSIENT_TIMEOUT = "transient_timeout"
    TRANSIENT_BUSY = "transient_busy"
    TRANSIENT_LOCK = "transient_lock"

    # Permanent errors - don't retry
    PERMANENT_PERMISSION = "permanent_permission"
    PERMANENT_CORRUPTED = "permanent_corrupted"
    PERMANENT_CONFIG = "permanent_config"
    PERMANENT_NOT_FOUND = "permanent_not_found"
    PERMANENT_UNSUPPORTED = "permanent_unsupported"

    @property
    def is_transient(self) -> bool:
        """Return True if this category represents transient errors."""
        return self.name.startswith("TRANSIENT_")


class BackupError(Exception):
    """Base class for all btrfs-backup-ng errors with classification.

    Attributes:
        category: The error category for retry decisions
        is_retryable: Whether this error should trigger a retry
        suggested_action: Human-readable suggestion for resolution
        original_error: The underlying exception, if any
    """

    category: ErrorCategory = ErrorCategory.PERMANENT_CONFIG
    default_suggested_action: str = "Check the error message and configuration"

    def __init__(
        self,
        message: str,
        *,
        suggested_action: Optional[str] = None,
        original_error: Optional[Exception] = None,
        context: Optional[dict] = None,
    ):
        super().__init__(message)
        self.message = message
        self.suggested_action = suggested_action or self.default_suggested_action
        self.original_error = original_error
        self.context = context or {}

    @property
    def is_retryable(self) -> bool:
        """Return True if this error should trigger a retry."""
        return self.category.is_transient

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.message!r}, category={self.category.value})"


# =============================================================================
# Transient Errors - Worth Retrying
# =============================================================================


class TransientError(BackupError):
    """Base class for transient errors that should be retried."""

    pass


class TransientNetworkError(TransientError):
    """Network-related errors that are likely temporary.

    Examples:
        - SSH connection dropped
        - Connection timeout
        - DNS resolution temporary failure
        - Network unreachable (might be temporary)
    """

    category = ErrorCategory.TRANSIENT_NETWORK
    default_suggested_action = (
        "Check network connectivity and retry. "
        "The operation will be retried automatically."
    )


class TransientTimeoutError(TransientError):
    """Operation timed out but might succeed on retry.

    Examples:
        - Command execution timeout
        - Transfer timeout (large snapshot, slow network)
        - SSH keepalive timeout
    """

    category = ErrorCategory.TRANSIENT_TIMEOUT
    default_suggested_action = (
        "The operation timed out. This might be due to a large transfer "
        "or slow network. Consider increasing timeout values in config."
    )


class TransientSpaceError(TransientError):
    """Temporary space issues that might resolve.

    Examples:
        - Destination temporarily low on space (other cleanup in progress)
        - Quota temporarily exceeded
    """

    category = ErrorCategory.TRANSIENT_SPACE
    default_suggested_action = (
        "Destination is low on space. Free up space or wait for "
        "cleanup operations to complete."
    )


class TransientBusyError(TransientError):
    """Resource is busy but might become available.

    Examples:
        - Subvolume is in use
        - Filesystem is busy
        - Another operation in progress
    """

    category = ErrorCategory.TRANSIENT_BUSY
    default_suggested_action = (
        "Resource is currently busy. Wait for other operations to complete "
        "or close applications using the filesystem."
    )


class TransientLockError(TransientError):
    """Lock contention that should resolve.

    Examples:
        - Another backup operation has the lock
        - Stale lock that will be cleaned up
    """

    category = ErrorCategory.TRANSIENT_LOCK
    default_suggested_action = (
        "Another operation holds the lock. Wait for it to complete "
        "or use 'btrfs-backup-ng cleanup' to release stale locks."
    )


# =============================================================================
# Permanent Errors - Don't Retry
# =============================================================================


class PermanentError(BackupError):
    """Base class for permanent errors that should not be retried."""

    pass


class PermanentPermissionError(PermanentError):
    """Permission denied errors that require user intervention.

    Examples:
        - sudo required but not available
        - SSH key not authorized
        - Filesystem permissions insufficient
    """

    category = ErrorCategory.PERMANENT_PERMISSION
    default_suggested_action = (
        "Permission denied. Check that you have the required privileges. "
        "For btrfs operations, root/sudo access is typically required."
    )


class PermanentCorruptedError(PermanentError):
    """Data corruption detected.

    Examples:
        - Snapshot checksum mismatch
        - Send stream corruption
        - Filesystem corruption detected
    """

    category = ErrorCategory.PERMANENT_CORRUPTED
    default_suggested_action = (
        "Data corruption detected. Run 'btrfs check' on the filesystem. "
        "You may need to delete the corrupted snapshot and resync."
    )


class PermanentConfigError(PermanentError):
    """Configuration error that needs to be fixed.

    Examples:
        - Invalid configuration file
        - Missing required configuration
        - Invalid path specified
    """

    category = ErrorCategory.PERMANENT_CONFIG
    default_suggested_action = (
        "Configuration error detected. Check your configuration file "
        "and ensure all paths and settings are correct."
    )


class PermanentNotFoundError(PermanentError):
    """Required resource not found.

    Examples:
        - Source snapshot doesn't exist
        - Destination path doesn't exist
        - Parent snapshot for incremental not found
    """

    category = ErrorCategory.PERMANENT_NOT_FOUND
    default_suggested_action = (
        "Required resource not found. Verify that paths exist "
        "and snapshots are present."
    )


class PermanentUnsupportedError(PermanentError):
    """Operation not supported in current environment.

    Examples:
        - Feature requires newer btrfs-progs version
        - Filesystem doesn't support required feature
        - Endpoint type doesn't support operation
    """

    category = ErrorCategory.PERMANENT_UNSUPPORTED
    default_suggested_action = (
        "This operation is not supported in your environment. "
        "Check btrfs-progs version and filesystem capabilities."
    )


# =============================================================================
# Specific Error Types
# =============================================================================


class SnapshotTransferError(BackupError):
    """Error during snapshot transfer.

    This can be either transient or permanent depending on the cause.
    Use classify_transfer_error() to get the right subclass.
    """

    category = ErrorCategory.TRANSIENT_NETWORK  # Default to retryable
    default_suggested_action = "Snapshot transfer failed. Check logs for details."

    def __init__(
        self,
        message: str,
        *,
        snapshot_name: Optional[str] = None,
        source: Optional[str] = None,
        destination: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(message, **kwargs)
        self.snapshot_name = snapshot_name
        self.source = source
        self.destination = destination
        self.context.update(
            {
                "snapshot_name": snapshot_name,
                "source": source,
                "destination": destination,
            }
        )


class ChunkTransferError(TransientNetworkError):
    """Error during chunked transfer - resumable.

    Attributes:
        transfer_id: The transfer operation ID
        chunk_sequence: The chunk that failed
        bytes_transferred: Total bytes successfully transferred before failure
    """

    default_suggested_action = (
        "Chunk transfer failed. The operation can be resumed from the last "
        "successful chunk using 'btrfs-backup-ng resume'."
    )

    def __init__(
        self,
        message: str,
        *,
        transfer_id: Optional[str] = None,
        chunk_sequence: Optional[int] = None,
        bytes_transferred: int = 0,
        **kwargs,
    ):
        super().__init__(message, **kwargs)
        self.transfer_id = transfer_id
        self.chunk_sequence = chunk_sequence
        self.bytes_transferred = bytes_transferred
        self.context.update(
            {
                "transfer_id": transfer_id,
                "chunk_sequence": chunk_sequence,
                "bytes_transferred": bytes_transferred,
            }
        )


class ChunkChecksumError(PermanentCorruptedError):
    """Chunk checksum verification failed."""

    default_suggested_action = (
        "Chunk checksum mismatch detected. The chunk will be re-transferred. "
        "If this persists, check for network or storage issues."
    )

    def __init__(
        self,
        chunk_sequence: int,
        expected_checksum: str,
        actual_checksum: str,
        **kwargs,
    ):
        message = (
            f"Chunk {chunk_sequence} checksum mismatch: "
            f"expected {expected_checksum[:16]}..., got {actual_checksum[:16]}..."
        )
        super().__init__(message, **kwargs)
        self.chunk_sequence = chunk_sequence
        self.expected_checksum = expected_checksum
        self.actual_checksum = actual_checksum


class InsufficientSpaceError(PermanentError):
    """Destination has insufficient space for the transfer.

    While space issues can sometimes be transient, we treat insufficient
    space as permanent because it requires user intervention to resolve.
    """

    category = ErrorCategory.TRANSIENT_SPACE  # Still marked transient for retry
    default_suggested_action = (
        "Destination has insufficient space. Free up space by deleting old "
        "snapshots or expanding the filesystem."
    )

    def __init__(
        self,
        message: str,
        *,
        required_bytes: Optional[int] = None,
        available_bytes: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(message, **kwargs)
        self.required_bytes = required_bytes
        self.available_bytes = available_bytes


class SSHConnectionError(TransientNetworkError):
    """SSH connection failed."""

    default_suggested_action = (
        "SSH connection failed. Check that the remote host is reachable, "
        "SSH service is running, and credentials are correct."
    )


class SSHAuthenticationError(PermanentPermissionError):
    """SSH authentication failed."""

    default_suggested_action = (
        "SSH authentication failed. Verify your SSH key is authorized on "
        "the remote host and has correct permissions."
    )


# =============================================================================
# Error Classification Utilities
# =============================================================================

# Patterns for classifying errors from stderr/exception messages
_TRANSIENT_PATTERNS = [
    # Network issues
    ("connection refused", TransientNetworkError),
    ("connection reset", TransientNetworkError),
    ("connection timed out", TransientTimeoutError),
    ("network is unreachable", TransientNetworkError),
    ("no route to host", TransientNetworkError),
    ("ssh_exchange_identification", TransientNetworkError),
    ("broken pipe", TransientNetworkError),
    ("connection closed", TransientNetworkError),
    # Timeouts
    ("timed out", TransientTimeoutError),
    ("timeout", TransientTimeoutError),
    # Busy/lock issues
    ("resource busy", TransientBusyError),
    ("device or resource busy", TransientBusyError),
    ("text file busy", TransientBusyError),
    ("lock", TransientLockError),
    # Space issues (temporary)
    ("no space left", TransientSpaceError),
    ("disk quota exceeded", TransientSpaceError),
]

_PERMANENT_PATTERNS = [
    # Permission issues
    ("permission denied", PermanentPermissionError),
    ("operation not permitted", PermanentPermissionError),
    ("access denied", PermanentPermissionError),
    ("authentication failed", SSHAuthenticationError),
    ("publickey denied", SSHAuthenticationError),
    # Not found
    ("no such file or directory", PermanentNotFoundError),
    ("not found", PermanentNotFoundError),
    ("does not exist", PermanentNotFoundError),
    # Corruption
    ("checksum", PermanentCorruptedError),
    ("corrupt", PermanentCorruptedError),
    ("invalid stream", PermanentCorruptedError),
    # Unsupported
    ("not supported", PermanentUnsupportedError),
    ("unsupported", PermanentUnsupportedError),
    ("invalid argument", PermanentConfigError),
]


def classify_error(
    error: Exception,
    stderr: Optional[str] = None,
    default_class: type[BackupError] = BackupError,
) -> BackupError:
    """Classify an exception into the appropriate BackupError subclass.

    Args:
        error: The original exception
        stderr: Optional stderr output to help classification
        default_class: Class to use if no pattern matches

    Returns:
        A BackupError subclass instance wrapping the original error
    """
    # If already a BackupError, return as-is
    if isinstance(error, BackupError):
        return error

    # Combine error message and stderr for pattern matching
    text = str(error).lower()
    if stderr:
        text = f"{text} {stderr.lower()}"

    # Check transient patterns first (more likely in network operations)
    for pattern, error_class in _TRANSIENT_PATTERNS:
        if pattern in text:
            return error_class(
                str(error),
                original_error=error,
            )

    # Check permanent patterns
    for pattern, error_class in _PERMANENT_PATTERNS:
        if pattern in text:
            return error_class(
                str(error),
                original_error=error,
            )

    # Default classification
    return default_class(
        str(error),
        original_error=error,
    )


def classify_ssh_error(
    error: Exception,
    stderr: Optional[str] = None,
    exit_code: Optional[int] = None,
) -> BackupError:
    """Classify an SSH-related error.

    Args:
        error: The original exception
        stderr: Optional stderr output
        exit_code: Optional exit code from SSH command

    Returns:
        Appropriate BackupError subclass
    """
    # SSH-specific exit codes
    if exit_code is not None:
        if exit_code == 255:
            # SSH connection error
            return classify_error(error, stderr, SSHConnectionError)
        if exit_code == 1:
            # General error - could be permission or other
            return classify_error(error, stderr, BackupError)

    return classify_error(error, stderr, TransientNetworkError)


def classify_transfer_error(
    error: Exception,
    stderr: Optional[str] = None,
    snapshot_name: Optional[str] = None,
    source: Optional[str] = None,
    destination: Optional[str] = None,
) -> SnapshotTransferError:
    """Classify a transfer error with snapshot context.

    Args:
        error: The original exception
        stderr: Optional stderr output
        snapshot_name: Name of the snapshot being transferred
        source: Source location
        destination: Destination location

    Returns:
        SnapshotTransferError with appropriate classification
    """
    classified = classify_error(error, stderr)

    return SnapshotTransferError(
        str(error),
        snapshot_name=snapshot_name,
        source=source,
        destination=destination,
        original_error=error,
        suggested_action=classified.suggested_action,
    )
