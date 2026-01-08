"""Tests for the error classification module."""

import pytest

from btrfs_backup_ng.core.errors import (
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


class TestErrorCategory:
    """Tests for ErrorCategory enum."""

    def test_transient_categories(self):
        """Transient categories should be marked as transient."""
        assert ErrorCategory.TRANSIENT_NETWORK.is_transient
        assert ErrorCategory.TRANSIENT_SPACE.is_transient
        assert ErrorCategory.TRANSIENT_TIMEOUT.is_transient
        assert ErrorCategory.TRANSIENT_BUSY.is_transient
        assert ErrorCategory.TRANSIENT_LOCK.is_transient

    def test_permanent_categories(self):
        """Permanent categories should not be marked as transient."""
        assert not ErrorCategory.PERMANENT_PERMISSION.is_transient
        assert not ErrorCategory.PERMANENT_CORRUPTED.is_transient
        assert not ErrorCategory.PERMANENT_CONFIG.is_transient
        assert not ErrorCategory.PERMANENT_NOT_FOUND.is_transient
        assert not ErrorCategory.PERMANENT_UNSUPPORTED.is_transient


class TestBackupError:
    """Tests for BackupError base class."""

    def test_basic_creation(self):
        """Test basic error creation."""
        error = BackupError("Test error")
        assert str(error) == "Test error"
        assert error.message == "Test error"
        assert error.is_retryable is False  # Default category is permanent

    def test_with_suggested_action(self):
        """Test error with custom suggested action."""
        error = BackupError(
            "Test error",
            suggested_action="Do something about it",
        )
        assert error.suggested_action == "Do something about it"

    def test_with_original_error(self):
        """Test error wrapping another exception."""
        original = ValueError("Original error")
        error = BackupError("Wrapped error", original_error=original)
        assert error.original_error is original

    def test_with_context(self):
        """Test error with context dictionary."""
        error = BackupError(
            "Test error",
            context={"key": "value", "count": 42},
        )
        assert error.context["key"] == "value"
        assert error.context["count"] == 42

    def test_repr(self):
        """Test error repr."""
        error = BackupError("Test error")
        repr_str = repr(error)
        assert "BackupError" in repr_str
        assert "Test error" in repr_str


class TestTransientErrors:
    """Tests for transient error classes."""

    def test_transient_network_error(self):
        """TransientNetworkError should be retryable."""
        error = TransientNetworkError("Connection dropped")
        assert error.is_retryable
        assert error.category == ErrorCategory.TRANSIENT_NETWORK
        assert isinstance(error, TransientError)

    def test_transient_timeout_error(self):
        """TransientTimeoutError should be retryable."""
        error = TransientTimeoutError("Operation timed out")
        assert error.is_retryable
        assert error.category == ErrorCategory.TRANSIENT_TIMEOUT

    def test_transient_space_error(self):
        """TransientSpaceError should be retryable."""
        error = TransientSpaceError("Low disk space")
        assert error.is_retryable
        assert error.category == ErrorCategory.TRANSIENT_SPACE

    def test_transient_busy_error(self):
        """TransientBusyError should be retryable."""
        error = TransientBusyError("Resource busy")
        assert error.is_retryable
        assert error.category == ErrorCategory.TRANSIENT_BUSY

    def test_transient_lock_error(self):
        """TransientLockError should be retryable."""
        error = TransientLockError("Lock held by another process")
        assert error.is_retryable
        assert error.category == ErrorCategory.TRANSIENT_LOCK


class TestPermanentErrors:
    """Tests for permanent error classes."""

    def test_permanent_permission_error(self):
        """PermanentPermissionError should not be retryable."""
        error = PermanentPermissionError("Permission denied")
        assert not error.is_retryable
        assert error.category == ErrorCategory.PERMANENT_PERMISSION
        assert isinstance(error, PermanentError)

    def test_permanent_corrupted_error(self):
        """PermanentCorruptedError should not be retryable."""
        error = PermanentCorruptedError("Data corruption detected")
        assert not error.is_retryable
        assert error.category == ErrorCategory.PERMANENT_CORRUPTED

    def test_permanent_config_error(self):
        """PermanentConfigError should not be retryable."""
        error = PermanentConfigError("Invalid configuration")
        assert not error.is_retryable
        assert error.category == ErrorCategory.PERMANENT_CONFIG

    def test_permanent_not_found_error(self):
        """PermanentNotFoundError should not be retryable."""
        error = PermanentNotFoundError("File not found")
        assert not error.is_retryable
        assert error.category == ErrorCategory.PERMANENT_NOT_FOUND

    def test_permanent_unsupported_error(self):
        """PermanentUnsupportedError should not be retryable."""
        error = PermanentUnsupportedError("Feature not supported")
        assert not error.is_retryable
        assert error.category == ErrorCategory.PERMANENT_UNSUPPORTED


class TestSpecificErrors:
    """Tests for specific error types."""

    def test_snapshot_transfer_error(self):
        """Test SnapshotTransferError with snapshot context."""
        error = SnapshotTransferError(
            "Transfer failed",
            snapshot_name="root-20240101",
            source="/mnt/btrfs/.snapshots",
            destination="ssh://backup/snapshots",
        )
        assert error.snapshot_name == "root-20240101"
        assert error.source == "/mnt/btrfs/.snapshots"
        assert error.destination == "ssh://backup/snapshots"
        assert "snapshot_name" in error.context

    def test_chunk_transfer_error(self):
        """Test ChunkTransferError with chunk context."""
        error = ChunkTransferError(
            "Chunk transfer failed",
            transfer_id="abc123",
            chunk_sequence=42,
            bytes_transferred=1024000,
        )
        assert error.transfer_id == "abc123"
        assert error.chunk_sequence == 42
        assert error.bytes_transferred == 1024000
        assert error.is_retryable  # Should be retryable

    def test_chunk_checksum_error(self):
        """Test ChunkChecksumError."""
        error = ChunkChecksumError(
            chunk_sequence=5,
            expected_checksum="abc123def456",
            actual_checksum="xyz789uvw012",
        )
        assert error.chunk_sequence == 5
        assert "abc123" in str(error)  # Truncated checksum in message
        assert not error.is_retryable  # Checksum errors are permanent

    def test_insufficient_space_error(self):
        """Test InsufficientSpaceError with size context."""
        error = InsufficientSpaceError(
            "Not enough space",
            required_bytes=1024 * 1024 * 1024,
            available_bytes=512 * 1024 * 1024,
        )
        assert error.required_bytes == 1024 * 1024 * 1024
        assert error.available_bytes == 512 * 1024 * 1024

    def test_ssh_connection_error(self):
        """Test SSHConnectionError."""
        error = SSHConnectionError("Connection refused")
        assert error.is_retryable
        assert isinstance(error, TransientNetworkError)

    def test_ssh_authentication_error(self):
        """Test SSHAuthenticationError."""
        error = SSHAuthenticationError("Key not authorized")
        assert not error.is_retryable
        assert isinstance(error, PermanentPermissionError)


class TestClassifyError:
    """Tests for error classification functions."""

    def test_classify_already_backup_error(self):
        """BackupError subclasses should be returned as-is."""
        original = TransientNetworkError("Already classified")
        classified = classify_error(original)
        assert classified is original

    def test_classify_network_errors(self):
        """Network-related errors should be classified as transient network."""
        patterns = [
            "Connection refused",
            "connection reset by peer",
            "Network is unreachable",
            "No route to host",
            "Broken pipe",
            "Connection closed unexpectedly",
        ]
        for pattern in patterns:
            error = Exception(pattern)
            classified = classify_error(error)
            assert isinstance(
                classified, TransientNetworkError
            ), f"'{pattern}' should be TransientNetworkError"

    def test_classify_timeout_errors(self):
        """Timeout errors should be classified as transient timeout."""
        patterns = [
            "Connection timed out",
            "Operation timeout",
            "Read timed out",
        ]
        for pattern in patterns:
            error = Exception(pattern)
            classified = classify_error(error)
            assert isinstance(
                classified, TransientTimeoutError
            ), f"'{pattern}' should be TransientTimeoutError"

    def test_classify_permission_errors(self):
        """Permission errors should be classified as permanent permission."""
        patterns = [
            "Permission denied",
            "Operation not permitted",
            "Access denied",
        ]
        for pattern in patterns:
            error = Exception(pattern)
            classified = classify_error(error)
            assert isinstance(
                classified, PermanentPermissionError
            ), f"'{pattern}' should be PermanentPermissionError"

    def test_classify_not_found_errors(self):
        """Not found errors should be classified as permanent not found."""
        patterns = [
            "No such file or directory",
            "File not found",
            "Snapshot does not exist",
        ]
        for pattern in patterns:
            error = Exception(pattern)
            classified = classify_error(error)
            assert isinstance(
                classified, PermanentNotFoundError
            ), f"'{pattern}' should be PermanentNotFoundError"

    def test_classify_busy_errors(self):
        """Busy errors should be classified as transient busy."""
        patterns = [
            "Resource busy",
            "Device or resource busy",
            "Text file busy",
        ]
        for pattern in patterns:
            error = Exception(pattern)
            classified = classify_error(error)
            assert isinstance(
                classified, TransientBusyError
            ), f"'{pattern}' should be TransientBusyError"

    def test_classify_space_errors(self):
        """Space errors should be classified as transient space."""
        patterns = [
            "No space left on device",
            "Disk quota exceeded",
        ]
        for pattern in patterns:
            error = Exception(pattern)
            classified = classify_error(error)
            assert isinstance(
                classified, TransientSpaceError
            ), f"'{pattern}' should be TransientSpaceError"

    def test_classify_with_stderr(self):
        """Classification should consider stderr in addition to error message."""
        error = Exception("Command failed")
        classified = classify_error(error, stderr="Permission denied")
        assert isinstance(classified, PermanentPermissionError)

    def test_classify_unknown_error(self):
        """Unknown errors should use the default class."""
        error = Exception("Something mysterious happened")
        classified = classify_error(error)
        assert isinstance(classified, BackupError)


class TestClassifySSHError:
    """Tests for SSH-specific error classification."""

    def test_exit_code_255(self):
        """Exit code 255 indicates SSH connection error."""
        error = Exception("SSH failed")
        classified = classify_ssh_error(error, exit_code=255)
        assert isinstance(classified, (SSHConnectionError, TransientNetworkError))

    def test_authentication_failure(self):
        """Authentication failures should be permanent."""
        error = Exception("Authentication failed")
        classified = classify_ssh_error(error, stderr="publickey denied")
        assert isinstance(classified, SSHAuthenticationError)


class TestClassifyTransferError:
    """Tests for transfer-specific error classification."""

    def test_transfer_error_with_context(self):
        """Transfer errors should include snapshot context."""
        error = Exception("Transfer failed")
        classified = classify_transfer_error(
            error,
            snapshot_name="test-snapshot",
            source="/source",
            destination="/dest",
        )
        assert isinstance(classified, SnapshotTransferError)
        assert classified.snapshot_name == "test-snapshot"
        assert classified.source == "/source"
        assert classified.destination == "/dest"
