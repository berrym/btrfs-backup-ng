"""Transfers command: Manage chunked and resumable transfers.

This module provides CLI commands for managing chunked transfers,
including listing, resuming, and cleaning up incomplete transfers.
"""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..__logger__ import create_logger
from ..core.chunked_transfer import (
    ChunkedTransferManager,
    TransferConfig,
    TransferManifest,
    TransferStatus,
)
from ..core.state import OperationManager, OperationState
from .common import get_log_level

logger = logging.getLogger(__name__)


def _format_bytes(size_bytes: int | float) -> str:
    """Format bytes as human-readable string."""
    size: float = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def _format_age(iso_timestamp: str) -> str:
    """Format timestamp as relative age."""
    try:
        dt = datetime.fromisoformat(iso_timestamp)
        now = datetime.now()
        delta = now - dt

        if delta.days > 0:
            return f"{delta.days}d ago"
        elif delta.seconds >= 3600:
            return f"{delta.seconds // 3600}h ago"
        elif delta.seconds >= 60:
            return f"{delta.seconds // 60}m ago"
        else:
            return "just now"
    except (ValueError, TypeError):
        return "unknown"


def _get_status_symbol(status: TransferStatus) -> str:
    """Get a symbol for transfer status."""
    symbols = {
        TransferStatus.INITIALIZING: "...",
        TransferStatus.CHUNKING: ">>>",
        TransferStatus.TRANSFERRING: "->",
        TransferStatus.REASSEMBLING: "<-",
        TransferStatus.VERIFYING: "?",
        TransferStatus.COMPLETED: "OK",
        TransferStatus.FAILED: "X",
        TransferStatus.PAUSED: "||",
    }
    return symbols.get(status, "?")


def execute_transfers(args: argparse.Namespace) -> int:
    """Execute the transfers command.

    Manages chunked and resumable transfers.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    action = getattr(args, "transfers_action", None)

    if action == "list" or action is None:
        return _list_transfers(args)
    elif action == "show":
        return _show_transfer(args)
    elif action == "resume":
        return _resume_transfer(args)
    elif action == "pause":
        return _pause_transfer(args)
    elif action == "cleanup":
        return _cleanup_transfers(args)
    elif action == "operations":
        return _list_operations(args)
    else:
        print(f"Unknown action: {action}")
        return 1


def _list_transfers(args: argparse.Namespace) -> int:
    """List all transfers."""
    manager = ChunkedTransferManager()

    # Get all incomplete transfers
    incomplete = manager.get_incomplete_transfers()

    output_json = getattr(args, "json", False)

    if output_json:
        data = [t.to_dict() for t in incomplete]
        print(json.dumps(data, indent=2))
        return 0

    if not incomplete:
        print("No incomplete transfers found.")
        print("")
        print("Incomplete transfers are stored in:")
        print(f"  {manager.config.cache_dir}")
        return 0

    print("Incomplete Transfers")
    print("=" * 70)
    print(
        f"{'ID':<10} {'Status':<12} {'Snapshot':<25} {'Progress':<12} {'Age':<10}"
    )
    print("-" * 70)

    for transfer in incomplete:
        progress = f"{transfer.completed_chunks}/{transfer.chunk_count}"
        if transfer.chunk_count > 0:
            pct = transfer.progress_percent
            progress = f"{progress} ({pct:.0f}%)"

        age = _format_age(transfer.created_at)
        status_str = transfer.status.value

        print(
            f"{transfer.transfer_id:<10} "
            f"{status_str:<12} "
            f"{transfer.snapshot_name[:25]:<25} "
            f"{progress:<12} "
            f"{age:<10}"
        )

    print("-" * 70)
    print(f"Total: {len(incomplete)} incomplete transfer(s)")
    print("")
    print("Commands:")
    print("  btrfs-backup-ng transfers show <ID>    - Show transfer details")
    print("  btrfs-backup-ng transfers resume <ID>  - Resume a transfer")
    print("  btrfs-backup-ng transfers cleanup      - Clean up old transfers")

    return 0


def _show_transfer(args: argparse.Namespace) -> int:
    """Show details of a specific transfer."""
    transfer_id = getattr(args, "transfer_id", None)
    if not transfer_id:
        print("Error: Transfer ID required")
        return 1

    manager = ChunkedTransferManager()
    manifest = manager.get_transfer(transfer_id)

    if manifest is None:
        print(f"Transfer not found: {transfer_id}")
        return 1

    output_json = getattr(args, "json", False)

    if output_json:
        print(json.dumps(manifest.to_dict(), indent=2))
        return 0

    print(f"Transfer: {manifest.transfer_id}")
    print("=" * 60)
    print(f"Status:      {_get_status_symbol(manifest.status)} {manifest.status.value}")
    print(f"Snapshot:    {manifest.snapshot_name}")
    print(f"Path:        {manifest.snapshot_path}")
    if manifest.parent_name:
        print(f"Parent:      {manifest.parent_name}")
    print(f"Destination: {manifest.destination}")
    print("")

    print("Transfer Details:")
    print(f"  Total Size:    {_format_bytes(manifest.total_size or 0)}")
    print(f"  Chunk Size:    {_format_bytes(manifest.chunk_size)}")
    print(f"  Chunk Count:   {manifest.chunk_count}")
    print(f"  Transferred:   {manifest.completed_chunks}/{manifest.chunk_count}")
    print(f"  Progress:      {manifest.progress_percent:.1f}%")
    print(f"  Bytes Done:    {_format_bytes(manifest.bytes_transferred)}")
    print("")

    print("Timestamps:")
    print(f"  Created:   {manifest.created_at}")
    print(f"  Updated:   {manifest.updated_at}")
    if manifest.completed_at:
        print(f"  Completed: {manifest.completed_at}")
    print("")

    if manifest.resume_count > 0:
        print(f"Resume Count: {manifest.resume_count}")

    if manifest.error_message:
        print(f"Last Error: {manifest.error_message}")
        print("")

    # Show chunk details if verbose
    if getattr(args, "verbose", 0) >= 1:
        print("Chunks:")
        print(f"  {'#':<6} {'Status':<12} {'Size':<12} {'Checksum':<20}")
        print("  " + "-" * 52)
        for chunk in manifest.chunks[:20]:  # Limit to first 20
            print(
                f"  {chunk.sequence:<6} "
                f"{chunk.status.value:<12} "
                f"{_format_bytes(chunk.size):<12} "
                f"{chunk.checksum[:20] if chunk.checksum else 'N/A':<20}"
            )
        if len(manifest.chunks) > 20:
            print(f"  ... and {len(manifest.chunks) - 20} more chunks")

    if manifest.is_resumable:
        resume_point = manifest.get_resume_point()
        print("")
        print(f"This transfer can be resumed from chunk {resume_point}")
        print(f"Run: btrfs-backup-ng transfers resume {manifest.transfer_id}")

    return 0


def _resume_transfer(args: argparse.Namespace) -> int:
    """Resume a paused or failed transfer."""
    transfer_id = getattr(args, "transfer_id", None)
    if not transfer_id:
        print("Error: Transfer ID required")
        return 1

    manager = ChunkedTransferManager()
    manifest = manager.get_transfer(transfer_id)

    if manifest is None:
        print(f"Transfer not found: {transfer_id}")
        return 1

    if not manifest.is_resumable:
        print(f"Transfer {transfer_id} is not resumable (status: {manifest.status.value})")
        return 1

    dry_run = getattr(args, "dry_run", False)

    if dry_run:
        print(f"Would resume transfer {transfer_id}")
        print(f"  Snapshot: {manifest.snapshot_name}")
        print(f"  Resume from chunk: {manifest.get_resume_point()}/{manifest.chunk_count}")
        return 0

    # Mark as ready to resume
    updated = manager.resume_transfer(transfer_id)
    if updated:
        print(f"Transfer {transfer_id} marked for resume")
        print(f"Resume point: chunk {updated.get_resume_point()}/{updated.chunk_count}")
        print("")
        print("To execute the resume, run your backup command with --use-chunked:")
        print("  btrfs-backup-ng run --use-chunked")
        print("")
        print("Or use the transfer command with the resume ID:")
        print(f"  btrfs-backup-ng transfer --resume-id {transfer_id}")
        return 0
    else:
        print(f"Failed to mark transfer for resume")
        return 1


def _pause_transfer(args: argparse.Namespace) -> int:
    """Pause an active transfer."""
    transfer_id = getattr(args, "transfer_id", None)
    if not transfer_id:
        print("Error: Transfer ID required")
        return 1

    manager = ChunkedTransferManager()
    manifest = manager.get_transfer(transfer_id)

    if manifest is None:
        print(f"Transfer not found: {transfer_id}")
        return 1

    if manifest.status != TransferStatus.TRANSFERRING:
        print(f"Transfer {transfer_id} is not actively transferring")
        return 1

    manager.pause_transfer(manifest)
    print(f"Transfer {transfer_id} paused at chunk {manifest.completed_chunks}/{manifest.chunk_count}")

    return 0


def _cleanup_transfers(args: argparse.Namespace) -> int:
    """Clean up old or completed transfers."""
    manager = ChunkedTransferManager()

    force = getattr(args, "force", False)
    max_age_hours = getattr(args, "max_age", 48)
    transfer_id = getattr(args, "transfer_id", None)
    dry_run = getattr(args, "dry_run", False)

    if transfer_id:
        # Clean up specific transfer
        manifest = manager.get_transfer(transfer_id)
        if manifest is None:
            print(f"Transfer not found: {transfer_id}")
            return 1

        if dry_run:
            print(f"Would clean up transfer: {transfer_id}")
            print(f"  Snapshot: {manifest.snapshot_name}")
            print(f"  Status: {manifest.status.value}")
            return 0

        if manager.cleanup_transfer(transfer_id, force=force):
            print(f"Cleaned up transfer: {transfer_id}")
            return 0
        else:
            print(f"Failed to clean up transfer (use --force for active transfers)")
            return 1

    # Clean up stale transfers
    if dry_run:
        stale = []
        import time
        cutoff = time.time() - (max_age_hours * 3600)

        for transfer in manager.get_incomplete_transfers():
            created = datetime.fromisoformat(transfer.created_at)
            if created.timestamp() < cutoff:
                stale.append(transfer)

        if stale:
            print(f"Would clean up {len(stale)} stale transfer(s):")
            for t in stale:
                print(f"  {t.transfer_id}: {t.snapshot_name} ({_format_age(t.created_at)})")
        else:
            print("No stale transfers to clean up")
        return 0

    cleaned = manager.cleanup_stale_transfers(max_age_hours=max_age_hours)

    if cleaned > 0:
        print(f"Cleaned up {cleaned} stale transfer(s)")
    else:
        print("No stale transfers to clean up")

    return 0


def _list_operations(args: argparse.Namespace) -> int:
    """List backup operations."""
    op_manager = OperationManager()

    include_archived = getattr(args, "all", False)
    output_json = getattr(args, "json", False)

    operations = op_manager.list_operations(include_archived=include_archived)

    if output_json:
        data = [op.to_dict() for op in operations]
        print(json.dumps(data, indent=2))
        return 0

    if not operations:
        print("No operations found.")
        return 0

    print("Backup Operations")
    print("=" * 80)
    print(
        f"{'ID':<10} {'State':<14} {'Source':<20} {'Progress':<15} {'Updated':<12}"
    )
    print("-" * 80)

    for op in operations:
        progress = f"{op.completed_transfers}/{op.total_transfers}"
        if op.total_transfers > 0:
            pct = op.progress_percent
            progress = f"{progress} ({pct:.0f}%)"

        updated = _format_age(op.updated_at)
        source = op.source_volume[:20] if len(op.source_volume) > 20 else op.source_volume

        print(
            f"{op.operation_id:<10} "
            f"{op.state.value:<14} "
            f"{source:<20} "
            f"{progress:<15} "
            f"{updated:<12}"
        )

    print("-" * 80)
    print(f"Total: {len(operations)} operation(s)")

    # Show resumable operations
    resumable = [op for op in operations if op.is_resumable]
    if resumable:
        print("")
        print(f"{len(resumable)} operation(s) can be resumed")

    return 0
