"""Transaction logging for audit and recovery.

Provides structured JSON logging of backup operations for:
- Audit trails
- Debugging failed operations
- Recovery assistance
- Statistics and reporting
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Transaction log file path (can be overridden by config)
_transaction_log_path: Path | None = None
_transaction_lock = threading.Lock()


def set_transaction_log(path: str | Path | None) -> None:
    """Set the transaction log file path.

    Args:
        path: Path to transaction log file, or None to disable
    """
    global _transaction_log_path

    if path is None:
        _transaction_log_path = None
        return

    _transaction_log_path = Path(path)
    # Create parent directories if needed
    _transaction_log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Transaction logging enabled: %s", _transaction_log_path)


def log_transaction(
    action: str,
    status: str,
    source: str | None = None,
    destination: str | None = None,
    snapshot: str | None = None,
    parent: str | None = None,
    size_bytes: int | None = None,
    duration_seconds: float | None = None,
    error: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """Log a transaction record.

    Args:
        action: The action performed (snapshot, transfer, delete, prune)
        status: Status of the action (started, completed, failed)
        source: Source path or endpoint
        destination: Destination path or endpoint
        snapshot: Snapshot name
        parent: Parent snapshot name (for incremental transfers)
        size_bytes: Size of transferred data in bytes
        duration_seconds: Duration of the operation
        error: Error message if failed
        details: Additional details as a dict
    """
    if _transaction_log_path is None:
        return

    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "status": status,
    }

    if source:
        record["source"] = source
    if destination:
        record["destination"] = destination
    if snapshot:
        record["snapshot"] = snapshot
    if parent:
        record["parent"] = parent
    if size_bytes is not None:
        record["size_bytes"] = size_bytes
    if duration_seconds is not None:
        record["duration_seconds"] = round(duration_seconds, 3)
    if error:
        record["error"] = error
    if details:
        record["details"] = details

    # Add process info for debugging
    record["pid"] = os.getpid()

    try:
        with _transaction_lock:
            with open(_transaction_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
    except OSError as e:
        logger.warning("Failed to write transaction log: %s", e)


class TransactionContext:
    """Context manager for timing and logging operations.

    Usage:
        with TransactionContext("transfer", source="/home", destination="/backup") as tx:
            # do transfer
            tx.set_snapshot("home-20240101")
            tx.set_size(1024000)
        # automatically logs completion or failure
    """

    def __init__(
        self,
        action: str,
        source: str | None = None,
        destination: str | None = None,
        snapshot: str | None = None,
        parent: str | None = None,
    ):
        self.action = action
        self.source = source
        self.destination = destination
        self.snapshot = snapshot
        self.parent = parent
        self.size_bytes: int | None = None
        self.details: dict[str, Any] = {}
        self._start_time: float | None = None
        self._error: str | None = None

    def __enter__(self) -> "TransactionContext":
        import time

        self._start_time = time.monotonic()
        log_transaction(
            action=self.action,
            status="started",
            source=self.source,
            destination=self.destination,
            snapshot=self.snapshot,
            parent=self.parent,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        import time

        duration = None
        if self._start_time is not None:
            duration = time.monotonic() - self._start_time

        if exc_val is not None:
            self._error = str(exc_val)
            log_transaction(
                action=self.action,
                status="failed",
                source=self.source,
                destination=self.destination,
                snapshot=self.snapshot,
                parent=self.parent,
                size_bytes=self.size_bytes,
                duration_seconds=duration,
                error=self._error,
                details=self.details if self.details else None,
            )
        else:
            log_transaction(
                action=self.action,
                status="completed",
                source=self.source,
                destination=self.destination,
                snapshot=self.snapshot,
                parent=self.parent,
                size_bytes=self.size_bytes,
                duration_seconds=duration,
                details=self.details if self.details else None,
            )

        # Don't suppress exceptions
        return False

    def set_snapshot(self, name: str) -> None:
        """Set the snapshot name after context creation."""
        self.snapshot = name

    def set_parent(self, name: str) -> None:
        """Set the parent snapshot name."""
        self.parent = name

    def set_size(self, size_bytes: int) -> None:
        """Set the transfer size in bytes."""
        self.size_bytes = size_bytes

    def add_detail(self, key: str, value: Any) -> None:
        """Add a detail to the transaction record."""
        self.details[key] = value

    def fail(self, error: str) -> None:
        """Mark the transaction as failed with an error message."""
        self._error = error


def read_transaction_log(
    path: str | Path | None = None,
    limit: int | None = None,
    action_filter: str | None = None,
    status_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Read and parse the transaction log.

    Args:
        path: Path to transaction log (uses current log path if None)
        limit: Maximum number of records to return (most recent first)
        action_filter: Only return records with this action
        status_filter: Only return records with this status

    Returns:
        List of transaction records as dicts
    """
    log_path = Path(path) if path else _transaction_log_path
    if log_path is None or not log_path.exists():
        return []

    records = []
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    if action_filter and record.get("action") != action_filter:
                        continue
                    if status_filter and record.get("status") != status_filter:
                        continue
                    records.append(record)
                except json.JSONDecodeError:
                    continue
    except OSError:
        return []

    # Return most recent first
    records.reverse()

    if limit:
        records = records[:limit]

    return records


def get_transaction_stats(path: str | Path | None = None) -> dict[str, Any]:
    """Get statistics from the transaction log.

    Args:
        path: Path to transaction log (uses current log path if None)

    Returns:
        Dict with statistics
    """
    records = read_transaction_log(path)

    if not records:
        return {
            "total_records": 0,
            "transfers": {"completed": 0, "failed": 0},
            "snapshots": {"completed": 0, "failed": 0},
            "deletes": {"completed": 0, "failed": 0},
            "total_bytes_transferred": 0,
        }

    stats = {
        "total_records": len(records),
        "transfers": {"completed": 0, "failed": 0},
        "snapshots": {"completed": 0, "failed": 0},
        "deletes": {"completed": 0, "failed": 0},
        "total_bytes_transferred": 0,
    }

    for record in records:
        action = record.get("action", "")
        status = record.get("status", "")

        if action == "transfer":
            if status == "completed":
                stats["transfers"]["completed"] += 1
                if record.get("size_bytes"):
                    stats["total_bytes_transferred"] += record["size_bytes"]
            elif status == "failed":
                stats["transfers"]["failed"] += 1
        elif action == "snapshot":
            if status == "completed":
                stats["snapshots"]["completed"] += 1
            elif status == "failed":
                stats["snapshots"]["failed"] += 1
        elif action in ("delete", "prune"):
            if status == "completed":
                stats["deletes"]["completed"] += 1
            elif status == "failed":
                stats["deletes"]["failed"] += 1

    return stats
