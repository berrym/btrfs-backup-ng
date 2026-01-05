"""Status command: Show job status and statistics."""

import argparse
import logging
import os
from pathlib import Path

from .. import endpoint
from ..__logger__ import create_logger
from ..config import ConfigError, find_config_file, load_config
from ..transaction import get_transaction_stats, read_transaction_log
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


def _format_duration(seconds: float) -> str:
    """Format duration as human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.1f}h"


def execute_status(args: argparse.Namespace) -> int:
    """Execute the status command.

    Shows backup status, last run times, and health information.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    # Find and load config
    try:
        config_path = find_config_file(getattr(args, "config", None))
        if config_path is None:
            print("No configuration file found.")
            print("Create one with: btrfs-backup-ng config init")
            return 1

        config, _ = load_config(config_path)

    except ConfigError as e:
        logger.error("Configuration error: %s", e)
        return 1

    volumes = config.get_enabled_volumes()

    if not volumes:
        print("No volumes configured")
        return 1

    print("btrfs-backup-ng Status")
    print("=" * 60)
    print(f"Config: {config_path}")
    print(
        f"Volumes: {len(volumes)} configured, {len(config.get_enabled_volumes())} enabled"
    )
    print(
        f"Parallelism: {config.global_config.parallel_volumes} volumes, {config.global_config.parallel_targets} targets"
    )
    print("")

    all_healthy = True

    for volume in volumes:
        print(f"Volume: {volume.path}")

        # Build endpoint kwargs
        endpoint_kwargs = {
            "snap_prefix": volume.snapshot_prefix or f"{os.uname()[1]}-",
            "convert_rw": False,
            "subvolume_sync": False,
            "btrfs_debug": False,
            "fs_checks": True,
        }

        # Check source
        source_status = "unknown"
        source_count = 0
        last_snapshot = None

        try:
            source_path = Path(volume.path).resolve()

            snapshot_dir = Path(volume.snapshot_dir)
            if not snapshot_dir.is_absolute():
                # Relative snapshot_dir: relative to source volume
                full_snapshot_dir = (source_path / snapshot_dir).resolve()
            else:
                # Absolute snapshot_dir: add source name as subdirectory
                full_snapshot_dir = (snapshot_dir / source_path.name).resolve()

            if full_snapshot_dir.exists():
                source_kwargs = dict(endpoint_kwargs)
                source_kwargs["path"] = full_snapshot_dir
                source_kwargs["snapshot_folder"] = str(full_snapshot_dir)

                source_endpoint = endpoint.choose_endpoint(
                    str(source_path),
                    source_kwargs,
                    source=True,
                )
                source_endpoint.prepare()

                snapshots = source_endpoint.list_snapshots()
                source_count = len(snapshots)

                if snapshots:
                    last_snapshot = snapshots[-1]
                    source_status = "ok"
                else:
                    source_status = "no snapshots"
            else:
                source_status = "no snapshot dir"

        except Exception as e:
            source_status = f"error: {e}"
            all_healthy = False

        print(f"  Source: {source_status} ({source_count} snapshots)")
        if last_snapshot:
            print(f"  Latest: {last_snapshot.get_name()}")

        # Check targets
        for target in volume.targets:
            target_status = "unknown"
            target_count = 0

            try:
                dest_kwargs = dict(endpoint_kwargs)
                dest_kwargs["ssh_sudo"] = target.ssh_sudo
                dest_kwargs["ssh_password_fallback"] = target.ssh_password_auth

                dest_endpoint = endpoint.choose_endpoint(
                    target.path,
                    dest_kwargs,
                    source=False,
                )
                dest_endpoint.prepare()

                dest_snapshots = dest_endpoint.list_snapshots()
                target_count = len(dest_snapshots)

                if dest_snapshots:
                    target_status = "ok"
                else:
                    target_status = "no backups"

                # Check sync status
                if source_count > 0 and target_count > 0:
                    if target_count < source_count:
                        pending = source_count - target_count
                        target_status = f"ok ({pending} pending)"

            except Exception as e:
                target_status = f"error: {e}"
                all_healthy = False

            print(f"  Target: {target.path}")
            print(f"    Status: {target_status} ({target_count} backups)")

        print("")

    # Transaction log stats
    if config.global_config.transaction_log:
        print("Transaction History")
        print("-" * 60)
        stats = get_transaction_stats(config.global_config.transaction_log)

        if stats["total_records"] > 0:
            print(f"  Total records: {stats['total_records']}")
            print(
                f"  Transfers: {stats['transfers']['completed']} completed, "
                f"{stats['transfers']['failed']} failed"
            )
            print(
                f"  Total transferred: {_format_bytes(stats['total_bytes_transferred'])}"
            )

            # Show recent transactions if --transactions flag
            if getattr(args, "transactions", False):
                print("")
                print("Recent Transactions:")
                limit = getattr(args, "limit", 10)
                records = read_transaction_log(
                    config.global_config.transaction_log, limit=limit
                )
                for record in records:
                    ts = record.get("timestamp", "")[:19].replace("T", " ")
                    status = record.get("status", "?")
                    action = record.get("action", "?")
                    snapshot = record.get("snapshot", "N/A")

                    status_icon = (
                        "✓"
                        if status == "completed"
                        else "✗"
                        if status == "failed"
                        else "…"
                    )
                    line = f"  {status_icon} {ts} {action:10} {snapshot}"

                    if record.get("duration_seconds"):
                        line += f" ({_format_duration(record['duration_seconds'])})"
                    if record.get("size_bytes"):
                        line += f" [{_format_bytes(record['size_bytes'])}]"
                    if record.get("error"):
                        line += f" ERROR: {record['error'][:50]}"

                    print(line)
        else:
            print("  No transaction history yet")

        print("")

    # Summary
    print("=" * 60)
    if all_healthy:
        print("Overall: All systems operational")
    else:
        print("Overall: Some issues detected")

    return 0 if all_healthy else 1
