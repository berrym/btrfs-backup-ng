"""Tests for transaction logging."""

import json
import os
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.transaction import (
    TransactionContext,
    get_transaction_stats,
    log_transaction,
    read_transaction_log,
    set_transaction_log,
)


class TestSetTransactionLog:
    """Tests for set_transaction_log function."""

    def test_set_path(self, tmp_path):
        """Test setting transaction log path."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        # Log something to verify it works
        log_transaction(action="test", status="completed")

        assert log_path.exists()

        # Cleanup
        set_transaction_log(None)

    def test_set_none_disables_logging(self, tmp_path):
        """Test setting None disables logging."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)
        set_transaction_log(None)

        # This should not create file
        log_transaction(action="test", status="completed")

        assert not log_path.exists()

    def test_creates_parent_directories(self, tmp_path):
        """Test creates parent directories if needed."""
        log_path = tmp_path / "deep" / "nested" / "dir" / "transactions.log"
        set_transaction_log(log_path)

        assert log_path.parent.exists()

        # Cleanup
        set_transaction_log(None)

    def test_accepts_string_path(self, tmp_path):
        """Test accepts string path."""
        log_path = str(tmp_path / "transactions.log")
        set_transaction_log(log_path)

        log_transaction(action="test", status="completed")

        assert Path(log_path).exists()

        # Cleanup
        set_transaction_log(None)


class TestLogTransaction:
    """Tests for log_transaction function."""

    def test_logs_basic_transaction(self, tmp_path):
        """Test logging a basic transaction."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(action="backup", status="completed")

        content = log_path.read_text()
        record = json.loads(content.strip())

        assert record["action"] == "backup"
        assert record["status"] == "completed"
        assert "timestamp" in record
        assert "pid" in record

        set_transaction_log(None)

    def test_logs_all_fields(self, tmp_path):
        """Test logging transaction with all optional fields."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(
            action="transfer",
            status="completed",
            source="/home",
            destination="/backup",
            snapshot="home-20260101",
            parent="home-20251231",
            size_bytes=1024000,
            duration_seconds=15.5,
            error=None,
            details={"compression": "zstd"},
        )

        content = log_path.read_text()
        record = json.loads(content.strip())

        assert record["action"] == "transfer"
        assert record["status"] == "completed"
        assert record["source"] == "/home"
        assert record["destination"] == "/backup"
        assert record["snapshot"] == "home-20260101"
        assert record["parent"] == "home-20251231"
        assert record["size_bytes"] == 1024000
        assert record["duration_seconds"] == 15.5
        assert record["details"] == {"compression": "zstd"}
        assert "error" not in record  # None values not included

        set_transaction_log(None)

    def test_logs_error_field(self, tmp_path):
        """Test logging transaction with error."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(
            action="transfer",
            status="failed",
            error="Connection refused",
        )

        content = log_path.read_text()
        record = json.loads(content.strip())

        assert record["error"] == "Connection refused"

        set_transaction_log(None)

    def test_appends_to_log(self, tmp_path):
        """Test transactions are appended to log."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(action="first", status="completed")
        log_transaction(action="second", status="completed")
        log_transaction(action="third", status="completed")

        lines = log_path.read_text().strip().split("\n")
        assert len(lines) == 3

        records = [json.loads(line) for line in lines]
        assert records[0]["action"] == "first"
        assert records[1]["action"] == "second"
        assert records[2]["action"] == "third"

        set_transaction_log(None)

    def test_does_nothing_when_disabled(self, tmp_path):
        """Test does nothing when logging is disabled."""
        set_transaction_log(None)

        # Should not raise
        log_transaction(action="test", status="completed")

    def test_rounds_duration(self, tmp_path):
        """Test duration is rounded to 3 decimal places."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(
            action="test",
            status="completed",
            duration_seconds=1.23456789,
        )

        content = log_path.read_text()
        record = json.loads(content.strip())

        assert record["duration_seconds"] == 1.235

        set_transaction_log(None)

    @patch("builtins.open", side_effect=OSError("Disk full"))
    def test_handles_write_error(self, mock_open, tmp_path):
        """Test handles write errors gracefully."""
        log_path = tmp_path / "transactions.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # Set path but mock will cause write to fail
        import btrfs_backup_ng.transaction as txn_module

        txn_module._transaction_log_path = log_path

        # Should not raise, just log warning
        log_transaction(action="test", status="completed")

        # Cleanup
        set_transaction_log(None)


class TestTransactionContext:
    """Tests for TransactionContext context manager."""

    def test_logs_start_and_completion(self, tmp_path):
        """Test logs started and completed transactions."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        with TransactionContext("backup", source="/home"):
            pass  # Simulate work

        lines = log_path.read_text().strip().split("\n")
        assert len(lines) == 2

        started = json.loads(lines[0])
        completed = json.loads(lines[1])

        assert started["action"] == "backup"
        assert started["status"] == "started"
        assert started["source"] == "/home"

        assert completed["action"] == "backup"
        assert completed["status"] == "completed"
        assert "duration_seconds" in completed

        set_transaction_log(None)

    def test_logs_failure_on_exception(self, tmp_path):
        """Test logs failed status when exception occurs."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        with pytest.raises(ValueError):
            with TransactionContext("backup") as tx:
                raise ValueError("Something went wrong")

        lines = log_path.read_text().strip().split("\n")
        assert len(lines) == 2

        failed = json.loads(lines[1])
        assert failed["status"] == "failed"
        assert "Something went wrong" in failed["error"]

        set_transaction_log(None)

    def test_set_snapshot(self, tmp_path):
        """Test setting snapshot name after context creation."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        with TransactionContext("backup") as tx:
            tx.set_snapshot("home-20260101")

        lines = log_path.read_text().strip().split("\n")
        completed = json.loads(lines[1])

        assert completed["snapshot"] == "home-20260101"

        set_transaction_log(None)

    def test_set_parent(self, tmp_path):
        """Test setting parent snapshot name."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        with TransactionContext("transfer") as tx:
            tx.set_parent("home-20251231")

        lines = log_path.read_text().strip().split("\n")
        completed = json.loads(lines[1])

        assert completed["parent"] == "home-20251231"

        set_transaction_log(None)

    def test_set_size(self, tmp_path):
        """Test setting transfer size."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        with TransactionContext("transfer") as tx:
            tx.set_size(1024000)

        lines = log_path.read_text().strip().split("\n")
        completed = json.loads(lines[1])

        assert completed["size_bytes"] == 1024000

        set_transaction_log(None)

    def test_add_detail(self, tmp_path):
        """Test adding custom details."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        with TransactionContext("transfer") as tx:
            tx.add_detail("compression", "zstd")
            tx.add_detail("level", 3)

        lines = log_path.read_text().strip().split("\n")
        completed = json.loads(lines[1])

        assert completed["details"]["compression"] == "zstd"
        assert completed["details"]["level"] == 3

        set_transaction_log(None)

    def test_fail_method(self, tmp_path):
        """Test fail method sets error message."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        with TransactionContext("backup") as tx:
            tx.fail("Manual failure")

        # fail() just sets the error message, actual failure logged on exception
        lines = log_path.read_text().strip().split("\n")
        completed = json.loads(lines[1])

        # Without exception, still completes
        assert completed["status"] == "completed"

        set_transaction_log(None)

    def test_measures_duration(self, tmp_path):
        """Test duration is measured accurately."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        with TransactionContext("backup"):
            time.sleep(0.1)  # Sleep 100ms

        lines = log_path.read_text().strip().split("\n")
        completed = json.loads(lines[1])

        # Should be at least 0.1 seconds
        assert completed["duration_seconds"] >= 0.1

        set_transaction_log(None)

    def test_returns_self_from_enter(self, tmp_path):
        """Test __enter__ returns self."""
        set_transaction_log(tmp_path / "transactions.log")

        ctx = TransactionContext("test")
        result = ctx.__enter__()

        assert result is ctx

        ctx.__exit__(None, None, None)
        set_transaction_log(None)

    def test_does_not_suppress_exceptions(self, tmp_path):
        """Test exceptions are not suppressed."""
        set_transaction_log(tmp_path / "transactions.log")

        with pytest.raises(RuntimeError):
            with TransactionContext("test"):
                raise RuntimeError("Test error")

        set_transaction_log(None)


class TestReadTransactionLog:
    """Tests for read_transaction_log function."""

    def test_reads_empty_log(self, tmp_path):
        """Test reading empty log returns empty list."""
        log_path = tmp_path / "transactions.log"
        log_path.touch()

        result = read_transaction_log(log_path)

        assert result == []

    def test_reads_nonexistent_log(self, tmp_path):
        """Test reading nonexistent log returns empty list."""
        log_path = tmp_path / "nonexistent.log"

        result = read_transaction_log(log_path)

        assert result == []

    def test_reads_log_entries(self, tmp_path):
        """Test reads and parses log entries."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(action="backup", status="completed")
        log_transaction(action="transfer", status="completed")

        set_transaction_log(None)

        result = read_transaction_log(log_path)

        assert len(result) == 2
        # Most recent first
        assert result[0]["action"] == "transfer"
        assert result[1]["action"] == "backup"

    def test_limit_parameter(self, tmp_path):
        """Test limit parameter."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        for i in range(10):
            log_transaction(action=f"action-{i}", status="completed")

        set_transaction_log(None)

        result = read_transaction_log(log_path, limit=3)

        assert len(result) == 3
        # Most recent first
        assert result[0]["action"] == "action-9"

    def test_action_filter(self, tmp_path):
        """Test filtering by action."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(action="backup", status="completed")
        log_transaction(action="transfer", status="completed")
        log_transaction(action="backup", status="completed")

        set_transaction_log(None)

        result = read_transaction_log(log_path, action_filter="backup")

        assert len(result) == 2
        assert all(r["action"] == "backup" for r in result)

    def test_status_filter(self, tmp_path):
        """Test filtering by status."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(action="backup", status="started")
        log_transaction(action="backup", status="completed")
        log_transaction(action="backup", status="failed")

        set_transaction_log(None)

        result = read_transaction_log(log_path, status_filter="failed")

        assert len(result) == 1
        assert result[0]["status"] == "failed"

    def test_uses_current_log_path(self, tmp_path):
        """Test uses current log path when path not specified."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(action="test", status="completed")

        result = read_transaction_log()  # No path specified

        assert len(result) == 1

        set_transaction_log(None)

    def test_skips_invalid_json(self, tmp_path):
        """Test skips invalid JSON lines."""
        log_path = tmp_path / "transactions.log"
        log_path.write_text(
            '{"action": "valid", "status": "completed"}\n'
            "not valid json\n"
            '{"action": "also_valid", "status": "completed"}\n'
        )

        result = read_transaction_log(log_path)

        assert len(result) == 2

    def test_skips_empty_lines(self, tmp_path):
        """Test skips empty lines."""
        log_path = tmp_path / "transactions.log"
        log_path.write_text(
            '{"action": "test", "status": "completed"}\n'
            "\n"
            "\n"
            '{"action": "test2", "status": "completed"}\n'
        )

        result = read_transaction_log(log_path)

        assert len(result) == 2


class TestGetTransactionStats:
    """Tests for get_transaction_stats function."""

    def test_empty_log_stats(self, tmp_path):
        """Test stats for empty log."""
        log_path = tmp_path / "empty.log"
        log_path.touch()

        stats = get_transaction_stats(log_path)

        assert stats["total_records"] == 0
        assert stats["transfers"]["completed"] == 0
        assert stats["transfers"]["failed"] == 0
        assert stats["snapshots"]["completed"] == 0
        assert stats["snapshots"]["failed"] == 0
        assert stats["deletes"]["completed"] == 0
        assert stats["deletes"]["failed"] == 0
        assert stats["total_bytes_transferred"] == 0

    def test_counts_transfers(self, tmp_path):
        """Test counting transfer records."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(action="transfer", status="completed", size_bytes=1000)
        log_transaction(action="transfer", status="completed", size_bytes=2000)
        log_transaction(action="transfer", status="failed")

        set_transaction_log(None)

        stats = get_transaction_stats(log_path)

        assert stats["transfers"]["completed"] == 2
        assert stats["transfers"]["failed"] == 1
        assert stats["total_bytes_transferred"] == 3000

    def test_counts_snapshots(self, tmp_path):
        """Test counting snapshot records."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(action="snapshot", status="completed")
        log_transaction(action="snapshot", status="completed")
        log_transaction(action="snapshot", status="failed")

        set_transaction_log(None)

        stats = get_transaction_stats(log_path)

        assert stats["snapshots"]["completed"] == 2
        assert stats["snapshots"]["failed"] == 1

    def test_counts_deletes_and_prunes(self, tmp_path):
        """Test counting delete and prune records."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(action="delete", status="completed")
        log_transaction(action="prune", status="completed")
        log_transaction(action="delete", status="failed")

        set_transaction_log(None)

        stats = get_transaction_stats(log_path)

        assert stats["deletes"]["completed"] == 2
        assert stats["deletes"]["failed"] == 1

    def test_total_records(self, tmp_path):
        """Test total records count."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        for i in range(5):
            log_transaction(action="test", status="completed")

        set_transaction_log(None)

        stats = get_transaction_stats(log_path)

        assert stats["total_records"] == 5

    def test_uses_current_log_path(self, tmp_path):
        """Test uses current log path when not specified."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        log_transaction(action="test", status="completed")

        stats = get_transaction_stats()  # No path specified

        assert stats["total_records"] == 1

        set_transaction_log(None)


class TestThreadSafety:
    """Tests for thread safety of transaction logging."""

    def test_concurrent_logging(self, tmp_path):
        """Test concurrent logging from multiple threads."""
        log_path = tmp_path / "transactions.log"
        set_transaction_log(log_path)

        num_threads = 10
        transactions_per_thread = 100

        def log_transactions(thread_id):
            for i in range(transactions_per_thread):
                log_transaction(
                    action=f"action-{thread_id}-{i}",
                    status="completed",
                )

        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=log_transactions, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        set_transaction_log(None)

        # Verify all transactions were logged
        result = read_transaction_log(log_path)
        assert len(result) == num_threads * transactions_per_thread
