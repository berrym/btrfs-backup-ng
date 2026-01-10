"""Tests for transaction logging module."""

import json
import tempfile
import threading
import time
from pathlib import Path

import pytest

from btrfs_backup_ng import transaction
from btrfs_backup_ng.transaction import (
    TransactionContext,
    get_transaction_stats,
    log_transaction,
    read_transaction_log,
    set_transaction_log,
)


@pytest.fixture
def temp_log_file():
    """Create a temporary log file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        log_path = Path(f.name)
    yield log_path
    # Cleanup
    if log_path.exists():
        log_path.unlink()
    # Reset global state
    set_transaction_log(None)


@pytest.fixture
def temp_log_dir():
    """Create a temporary directory for log files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)
    # Reset global state
    set_transaction_log(None)


class TestSetTransactionLog:
    """Tests for set_transaction_log function."""

    def test_set_log_path(self, temp_log_dir):
        """Test setting a valid log path."""
        log_path = temp_log_dir / "transactions.jsonl"
        set_transaction_log(log_path)
        assert transaction._transaction_log_path == log_path

    def test_set_log_path_creates_parent_dirs(self, temp_log_dir):
        """Test that parent directories are created."""
        log_path = temp_log_dir / "subdir" / "nested" / "transactions.jsonl"
        set_transaction_log(log_path)
        assert log_path.parent.exists()

    def test_set_log_path_to_none(self, temp_log_dir):
        """Test disabling transaction logging."""
        log_path = temp_log_dir / "transactions.jsonl"
        set_transaction_log(log_path)
        assert transaction._transaction_log_path is not None

        set_transaction_log(None)
        assert transaction._transaction_log_path is None

    def test_set_log_path_with_string(self, temp_log_dir):
        """Test setting log path with string instead of Path."""
        log_path = str(temp_log_dir / "transactions.jsonl")
        set_transaction_log(log_path)
        assert transaction._transaction_log_path == Path(log_path)


class TestLogTransaction:
    """Tests for log_transaction function."""

    def test_log_when_disabled(self):
        """Test that logging does nothing when disabled."""
        set_transaction_log(None)
        # Should not raise
        log_transaction(action="test", status="completed")

    def test_log_basic_transaction(self, temp_log_file):
        """Test logging a basic transaction."""
        set_transaction_log(temp_log_file)
        log_transaction(action="snapshot", status="completed")

        with open(temp_log_file) as f:
            record = json.loads(f.readline())

        assert record["action"] == "snapshot"
        assert record["status"] == "completed"
        assert "timestamp" in record
        assert "pid" in record

    def test_log_transaction_with_all_fields(self, temp_log_file):
        """Test logging a transaction with all optional fields."""
        set_transaction_log(temp_log_file)
        log_transaction(
            action="transfer",
            status="completed",
            source="/home",
            destination="/backup",
            snapshot="home-20240101",
            parent="home-20231231",
            size_bytes=1024000,
            duration_seconds=5.5678,
            details={"compression": "zstd"},
        )

        with open(temp_log_file) as f:
            record = json.loads(f.readline())

        assert record["action"] == "transfer"
        assert record["status"] == "completed"
        assert record["source"] == "/home"
        assert record["destination"] == "/backup"
        assert record["snapshot"] == "home-20240101"
        assert record["parent"] == "home-20231231"
        assert record["size_bytes"] == 1024000
        assert record["duration_seconds"] == 5.568  # Rounded to 3 decimals
        assert record["details"] == {"compression": "zstd"}

    def test_log_transaction_with_error(self, temp_log_file):
        """Test logging a failed transaction with error message."""
        set_transaction_log(temp_log_file)
        log_transaction(
            action="transfer",
            status="failed",
            snapshot="home-20240101",
            error="Connection refused",
        )

        with open(temp_log_file) as f:
            record = json.loads(f.readline())

        assert record["status"] == "failed"
        assert record["error"] == "Connection refused"

    def test_log_multiple_transactions(self, temp_log_file):
        """Test logging multiple transactions."""
        set_transaction_log(temp_log_file)

        log_transaction(action="snapshot", status="started")
        log_transaction(action="snapshot", status="completed")
        log_transaction(action="transfer", status="started")
        log_transaction(action="transfer", status="completed")

        with open(temp_log_file) as f:
            lines = f.readlines()

        assert len(lines) == 4

    def test_log_handles_write_error(self, temp_log_dir):
        """Test that write errors are handled gracefully."""
        log_path = temp_log_dir / "transactions.jsonl"
        set_transaction_log(log_path)

        # Create file and make it read-only
        log_path.touch()
        log_path.chmod(0o444)

        try:
            # Should not raise, just log warning
            log_transaction(action="test", status="completed")
        finally:
            log_path.chmod(0o644)

    def test_log_thread_safety(self, temp_log_file):
        """Test that logging is thread-safe."""
        set_transaction_log(temp_log_file)
        num_threads = 10
        logs_per_thread = 50

        def log_many():
            for i in range(logs_per_thread):
                log_transaction(
                    action="test",
                    status="completed",
                    details={"thread": threading.current_thread().name, "index": i},
                )

        threads = [threading.Thread(target=log_many) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with open(temp_log_file) as f:
            lines = f.readlines()

        assert len(lines) == num_threads * logs_per_thread
        # Verify each line is valid JSON
        for line in lines:
            json.loads(line)


class TestTransactionContext:
    """Tests for TransactionContext class."""

    def test_context_basic_success(self, temp_log_file):
        """Test basic successful transaction context."""
        set_transaction_log(temp_log_file)

        with TransactionContext("snapshot", source="/home"):
            pass

        records = read_transaction_log(temp_log_file)
        assert len(records) == 2
        assert records[0]["status"] == "completed"
        assert records[1]["status"] == "started"

    def test_context_with_exception(self, temp_log_file):
        """Test transaction context with exception."""
        set_transaction_log(temp_log_file)

        with pytest.raises(ValueError):
            with TransactionContext("transfer", source="/home"):
                raise ValueError("Test error")

        records = read_transaction_log(temp_log_file)
        assert len(records) == 2
        assert records[0]["status"] == "failed"
        assert records[0]["error"] == "Test error"

    def test_context_set_snapshot(self, temp_log_file):
        """Test setting snapshot name during context."""
        set_transaction_log(temp_log_file)

        with TransactionContext("snapshot", source="/home") as tx:
            tx.set_snapshot("home-20240101")

        records = read_transaction_log(temp_log_file)
        assert records[0]["snapshot"] == "home-20240101"

    def test_context_set_parent(self, temp_log_file):
        """Test setting parent snapshot name."""
        set_transaction_log(temp_log_file)

        with TransactionContext("transfer") as tx:
            tx.set_parent("home-20231231")

        records = read_transaction_log(temp_log_file)
        assert records[0]["parent"] == "home-20231231"

    def test_context_set_size(self, temp_log_file):
        """Test setting transfer size."""
        set_transaction_log(temp_log_file)

        with TransactionContext("transfer") as tx:
            tx.set_size(1024000)

        records = read_transaction_log(temp_log_file)
        assert records[0]["size_bytes"] == 1024000

    def test_context_add_detail(self, temp_log_file):
        """Test adding details to transaction."""
        set_transaction_log(temp_log_file)

        with TransactionContext("transfer") as tx:
            tx.add_detail("compression", "zstd")
            tx.add_detail("rate_limit", "10M")

        records = read_transaction_log(temp_log_file)
        assert records[0]["details"]["compression"] == "zstd"
        assert records[0]["details"]["rate_limit"] == "10M"

    def test_context_fail_method(self, temp_log_file):
        """Test the fail method."""
        set_transaction_log(temp_log_file)

        with TransactionContext("transfer") as tx:
            tx.fail("Manual failure")
            # The fail method just sets _error, doesn't raise
            # The actual failure status requires an exception

        records = read_transaction_log(temp_log_file)
        # Without exception, status is still completed
        assert records[0]["status"] == "completed"

    def test_context_measures_duration(self, temp_log_file):
        """Test that duration is measured correctly."""
        set_transaction_log(temp_log_file)

        with TransactionContext("transfer"):
            time.sleep(0.1)

        records = read_transaction_log(temp_log_file)
        duration = records[0]["duration_seconds"]
        assert duration >= 0.1
        assert duration < 0.5  # Reasonable upper bound

    def test_context_full_parameters(self, temp_log_file):
        """Test context with all constructor parameters."""
        set_transaction_log(temp_log_file)

        with TransactionContext(
            action="transfer",
            source="/home",
            destination="/backup",
            snapshot="home-20240101",
            parent="home-20231231",
        ):
            pass

        records = read_transaction_log(temp_log_file)
        assert records[0]["action"] == "transfer"
        assert records[0]["source"] == "/home"
        assert records[0]["destination"] == "/backup"
        assert records[0]["snapshot"] == "home-20240101"
        assert records[0]["parent"] == "home-20231231"


class TestReadTransactionLog:
    """Tests for read_transaction_log function."""

    def test_read_empty_log(self, temp_log_file):
        """Test reading an empty log file."""
        temp_log_file.touch()
        records = read_transaction_log(temp_log_file)
        assert records == []

    def test_read_nonexistent_log(self, temp_log_dir):
        """Test reading a nonexistent log file."""
        records = read_transaction_log(temp_log_dir / "nonexistent.jsonl")
        assert records == []

    def test_read_when_path_is_none(self):
        """Test reading when no path is set."""
        set_transaction_log(None)
        records = read_transaction_log()
        assert records == []

    def test_read_with_limit(self, temp_log_file):
        """Test reading with a limit."""
        set_transaction_log(temp_log_file)

        for i in range(10):
            log_transaction(action="test", status="completed", details={"index": i})

        records = read_transaction_log(temp_log_file, limit=3)
        assert len(records) == 3
        # Most recent first
        assert records[0]["details"]["index"] == 9

    def test_read_with_action_filter(self, temp_log_file):
        """Test filtering by action."""
        set_transaction_log(temp_log_file)

        log_transaction(action="snapshot", status="completed")
        log_transaction(action="transfer", status="completed")
        log_transaction(action="snapshot", status="completed")
        log_transaction(action="delete", status="completed")

        records = read_transaction_log(temp_log_file, action_filter="snapshot")
        assert len(records) == 2
        for r in records:
            assert r["action"] == "snapshot"

    def test_read_with_status_filter(self, temp_log_file):
        """Test filtering by status."""
        set_transaction_log(temp_log_file)

        log_transaction(action="transfer", status="started")
        log_transaction(action="transfer", status="completed")
        log_transaction(action="transfer", status="failed")
        log_transaction(action="transfer", status="completed")

        records = read_transaction_log(temp_log_file, status_filter="completed")
        assert len(records) == 2
        for r in records:
            assert r["status"] == "completed"

    def test_read_with_both_filters(self, temp_log_file):
        """Test filtering by both action and status."""
        set_transaction_log(temp_log_file)

        log_transaction(action="transfer", status="completed")
        log_transaction(action="transfer", status="failed")
        log_transaction(action="snapshot", status="completed")
        log_transaction(action="snapshot", status="failed")

        records = read_transaction_log(
            temp_log_file, action_filter="transfer", status_filter="failed"
        )
        assert len(records) == 1
        assert records[0]["action"] == "transfer"
        assert records[0]["status"] == "failed"

    def test_read_uses_global_path(self, temp_log_file):
        """Test that read_transaction_log uses global path when not specified."""
        set_transaction_log(temp_log_file)
        log_transaction(action="test", status="completed")

        records = read_transaction_log()  # No path specified
        assert len(records) == 1

    def test_read_handles_malformed_json(self, temp_log_file):
        """Test that malformed JSON lines are skipped."""
        with open(temp_log_file, "w") as f:
            f.write('{"action": "test", "status": "completed"}\n')
            f.write("not valid json\n")
            f.write('{"action": "test2", "status": "completed"}\n')

        records = read_transaction_log(temp_log_file)
        assert len(records) == 2

    def test_read_handles_blank_lines(self, temp_log_file):
        """Test that blank lines are skipped."""
        with open(temp_log_file, "w") as f:
            f.write('{"action": "test", "status": "completed"}\n')
            f.write("\n")
            f.write("   \n")
            f.write('{"action": "test2", "status": "completed"}\n')

        records = read_transaction_log(temp_log_file)
        assert len(records) == 2

    def test_read_returns_most_recent_first(self, temp_log_file):
        """Test that records are returned most recent first."""
        set_transaction_log(temp_log_file)

        log_transaction(action="first", status="completed")
        log_transaction(action="second", status="completed")
        log_transaction(action="third", status="completed")

        records = read_transaction_log(temp_log_file)
        assert records[0]["action"] == "third"
        assert records[1]["action"] == "second"
        assert records[2]["action"] == "first"

    def test_read_handles_oserror(self, temp_log_dir):
        """Test that OSError during read returns empty list."""
        # Create a directory instead of a file - opening it will raise OSError
        dir_path = temp_log_dir / "is_a_directory.jsonl"
        dir_path.mkdir()
        records = read_transaction_log(dir_path)
        assert records == []


class TestGetTransactionStats:
    """Tests for get_transaction_stats function."""

    def test_stats_empty_log(self, temp_log_file):
        """Test stats for empty log."""
        temp_log_file.touch()
        stats = get_transaction_stats(temp_log_file)

        assert stats["total_records"] == 0
        assert stats["transfers"]["completed"] == 0
        assert stats["transfers"]["failed"] == 0
        assert stats["snapshots"]["completed"] == 0
        assert stats["snapshots"]["failed"] == 0
        assert stats["deletes"]["completed"] == 0
        assert stats["deletes"]["failed"] == 0
        assert stats["total_bytes_transferred"] == 0

    def test_stats_nonexistent_log(self, temp_log_dir):
        """Test stats for nonexistent log."""
        stats = get_transaction_stats(temp_log_dir / "nonexistent.jsonl")
        assert stats["total_records"] == 0

    def test_stats_with_transfers(self, temp_log_file):
        """Test stats with transfer records."""
        set_transaction_log(temp_log_file)

        log_transaction(action="transfer", status="completed", size_bytes=1000)
        log_transaction(action="transfer", status="completed", size_bytes=2000)
        log_transaction(action="transfer", status="failed")

        stats = get_transaction_stats(temp_log_file)
        assert stats["transfers"]["completed"] == 2
        assert stats["transfers"]["failed"] == 1
        assert stats["total_bytes_transferred"] == 3000

    def test_stats_with_snapshots(self, temp_log_file):
        """Test stats with snapshot records."""
        set_transaction_log(temp_log_file)

        log_transaction(action="snapshot", status="completed")
        log_transaction(action="snapshot", status="completed")
        log_transaction(action="snapshot", status="failed")

        stats = get_transaction_stats(temp_log_file)
        assert stats["snapshots"]["completed"] == 2
        assert stats["snapshots"]["failed"] == 1

    def test_stats_with_deletes(self, temp_log_file):
        """Test stats with delete records."""
        set_transaction_log(temp_log_file)

        log_transaction(action="delete", status="completed")
        log_transaction(action="prune", status="completed")
        log_transaction(action="delete", status="failed")

        stats = get_transaction_stats(temp_log_file)
        assert stats["deletes"]["completed"] == 2
        assert stats["deletes"]["failed"] == 1

    def test_stats_mixed_actions(self, temp_log_file):
        """Test stats with mixed action types."""
        set_transaction_log(temp_log_file)

        log_transaction(action="snapshot", status="completed")
        log_transaction(action="transfer", status="completed", size_bytes=5000)
        log_transaction(action="transfer", status="completed", size_bytes=3000)
        log_transaction(action="transfer", status="failed")
        log_transaction(action="delete", status="completed")
        log_transaction(action="snapshot", status="failed")

        stats = get_transaction_stats(temp_log_file)
        assert stats["total_records"] == 6
        assert stats["snapshots"]["completed"] == 1
        assert stats["snapshots"]["failed"] == 1
        assert stats["transfers"]["completed"] == 2
        assert stats["transfers"]["failed"] == 1
        assert stats["deletes"]["completed"] == 1
        assert stats["deletes"]["failed"] == 0
        assert stats["total_bytes_transferred"] == 8000

    def test_stats_uses_global_path(self, temp_log_file):
        """Test that get_transaction_stats uses global path when not specified."""
        set_transaction_log(temp_log_file)
        log_transaction(action="transfer", status="completed", size_bytes=1000)

        stats = get_transaction_stats()  # No path specified
        assert stats["total_records"] == 1

    def test_stats_ignores_started_status(self, temp_log_file):
        """Test that 'started' status records are not counted."""
        set_transaction_log(temp_log_file)

        log_transaction(action="transfer", status="started")
        log_transaction(action="transfer", status="completed", size_bytes=1000)
        log_transaction(action="snapshot", status="started")
        log_transaction(action="snapshot", status="completed")

        stats = get_transaction_stats(temp_log_file)
        assert stats["total_records"] == 4
        assert stats["transfers"]["completed"] == 1
        assert stats["transfers"]["failed"] == 0
        assert stats["snapshots"]["completed"] == 1
        assert stats["snapshots"]["failed"] == 0

    def test_stats_handles_missing_size_bytes(self, temp_log_file):
        """Test that missing size_bytes is handled."""
        set_transaction_log(temp_log_file)

        log_transaction(action="transfer", status="completed", size_bytes=1000)
        log_transaction(action="transfer", status="completed")  # No size

        stats = get_transaction_stats(temp_log_file)
        assert stats["total_bytes_transferred"] == 1000
