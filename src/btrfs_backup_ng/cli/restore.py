"""Restore command: Restore snapshots from backup locations.

Enables pulling snapshots from backup storage (SSH or local) back to local systems
for disaster recovery, migration, or backup verification.
"""

import argparse
import logging
import os
import time
from datetime import datetime
from pathlib import Path

from .. import __util__, endpoint
from ..__logger__ import add_file_handler, create_logger
from ..config import ConfigError, find_config_file, load_config
from ..core.restore import (
    RestoreError,
    list_remote_snapshots,
    restore_snapshots,
    validate_restore_destination,
)
from .common import get_log_level, should_show_progress

logger = logging.getLogger(__name__)


def execute_restore(args: argparse.Namespace) -> int:
    """Execute the restore command.

    Restores snapshots from a backup location to a local destination.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    # Handle --list mode (just list available snapshots)
    if getattr(args, "list", False):
        return _execute_list(args)

    # Get source and destination
    source = getattr(args, "source", None)
    destination = getattr(args, "destination", None)

    if not source:
        print("Error: Source backup location required")
        print("Usage: btrfs-backup-ng restore <source> <destination>")
        print("       btrfs-backup-ng restore --list <source>")
        return 1

    if not destination:
        print("Error: Destination path required")
        print("Usage: btrfs-backup-ng restore <source> <destination>")
        return 1

    # Validate destination
    dest_path = Path(destination).resolve()
    in_place = getattr(args, "in_place", False)
    force = getattr(args, "yes_i_know_what_i_am_doing", False)

    try:
        validate_restore_destination(dest_path, in_place=in_place, force=force)
    except RestoreError as e:
        logger.error("Destination validation failed: %s", e)
        return 1

    # Prepare backup endpoint (source)
    try:
        backup_endpoint = _prepare_backup_endpoint(args, source)
    except Exception as e:
        logger.error("Failed to prepare backup endpoint: %s", e)
        return 1

    # Prepare local endpoint (destination)
    try:
        local_endpoint = _prepare_local_endpoint(dest_path)
    except Exception as e:
        logger.error("Failed to prepare local endpoint: %s", e)
        return 1

    # Parse time if --before specified
    before_time = None
    before_str = getattr(args, "before", None)
    if before_str:
        try:
            before_time = _parse_datetime(before_str)
            logger.info(
                "Restoring snapshot before: %s",
                time.strftime("%Y-%m-%d %H:%M:%S", before_time),
            )
        except ValueError as e:
            logger.error("Invalid date format: %s", e)
            return 1

    # Get options
    dry_run = getattr(args, "dry_run", False)
    snapshot_name = getattr(args, "snapshot", None)
    restore_all = getattr(args, "all", False)
    skip_existing = not getattr(args, "overwrite", False)
    no_incremental = getattr(args, "no_incremental", False)
    interactive = getattr(args, "interactive", False)

    # Interactive mode
    if interactive:
        snapshot_name = _interactive_select(backup_endpoint)
        if snapshot_name is None:
            logger.info("No snapshot selected, aborting")
            return 0

    # Build transfer options
    show_progress = should_show_progress(args)
    options = {
        "compress": getattr(args, "compress", None) or "none",
        "rate_limit": getattr(args, "rate_limit", None),
        "show_progress": show_progress,
    }

    # Execute restore
    logger.info(__util__.log_heading(f"Restore started at {time.ctime()}"))
    logger.info("Source: %s", source)
    logger.info("Destination: %s", dest_path)

    try:
        stats = restore_snapshots(
            backup_endpoint=backup_endpoint,
            local_endpoint=local_endpoint,
            snapshot_name=snapshot_name,
            before_time=before_time,
            restore_all=restore_all,
            skip_existing=skip_existing,
            no_incremental=no_incremental,
            options=options,
            dry_run=dry_run,
        )
    except RestoreError as e:
        logger.error("Restore failed: %s", e)
        return 1
    except Exception as e:
        logger.error("Unexpected error during restore: %s", e)
        logger.debug("Exception details:", exc_info=True)
        return 1

    logger.info(__util__.log_heading(f"Restore finished at {time.ctime()}"))

    # Return appropriate exit code
    if stats["failed"] > 0:
        return 1
    return 0


def _execute_list(args: argparse.Namespace) -> int:
    """List available snapshots at backup location."""
    source = getattr(args, "source", None)
    if not source:
        print("Error: Source backup location required")
        print("Usage: btrfs-backup-ng restore --list <source>")
        return 1

    try:
        backup_endpoint = _prepare_backup_endpoint(args, source)
    except Exception as e:
        logger.error("Failed to prepare backup endpoint: %s", e)
        return 1

    try:
        snapshots = list_remote_snapshots(backup_endpoint)
    except Exception as e:
        logger.error("Failed to list snapshots: %s", e)
        return 1

    if not snapshots:
        print("No snapshots found at backup location")
        return 0

    print(f"Available snapshots at {source}:")
    print("")

    # Format nicely
    for i, snap in enumerate(snapshots, 1):
        name = snap.get_name()
        # Try to format the timestamp nicely
        if hasattr(snap, "time_obj") and snap.time_obj:
            time_str = time.strftime("%Y-%m-%d %H:%M:%S", snap.time_obj)
        else:
            time_str = "unknown"
        print(f"  {i:3}. {name:<40} ({time_str})")

    print("")
    print(f"Total: {len(snapshots)} snapshot(s)")

    return 0


def _prepare_backup_endpoint(args: argparse.Namespace, source: str):
    """Prepare the backup endpoint for restore.

    Args:
        args: Command arguments
        source: Source path (local or ssh://)

    Returns:
        Configured endpoint
    """
    # Build endpoint kwargs
    no_fs_checks = getattr(args, "no_fs_checks", False)
    endpoint_kwargs = {
        "snap_prefix": getattr(args, "prefix", "") or "",
        "convert_rw": False,
        "subvolume_sync": False,
        "btrfs_debug": False,
        "fs_checks": not no_fs_checks,
    }

    # SSH options
    if source.startswith("ssh://"):
        endpoint_kwargs["ssh_sudo"] = getattr(args, "ssh_sudo", False)
        endpoint_kwargs["ssh_password_fallback"] = getattr(
            args, "ssh_password_auth", True
        )
        ssh_key = getattr(args, "ssh_key", None)
        if ssh_key:
            endpoint_kwargs["ssh_identity_file"] = ssh_key
    else:
        # For local paths, we need to set 'path' as well since LocalEndpoint
        # always resolves config["path"] during initialization
        endpoint_kwargs["path"] = Path(source).resolve()

    # Create endpoint - for restore, backup location needs to be set as "path"
    # (not "source") because list_snapshots() uses config["path"]
    # The source=False means the path will be stored in config["path"]
    backup_ep = endpoint.choose_endpoint(
        source,
        endpoint_kwargs,
        source=False,
    )
    backup_ep.prepare()

    return backup_ep


def _prepare_local_endpoint(dest_path: Path):
    """Prepare the local endpoint for receiving restored snapshots.

    Args:
        dest_path: Local destination path

    Returns:
        Configured local endpoint
    """
    from ..endpoint.local import LocalEndpoint

    # Ensure directory exists
    dest_path.mkdir(parents=True, exist_ok=True)

    endpoint_kwargs = {
        "path": dest_path,
        "source": None,  # This is the destination for receive
        "snap_prefix": "",
        "convert_rw": False,
        "subvolume_sync": False,
        "btrfs_debug": False,
        "fs_checks": True,
    }

    local_ep = LocalEndpoint(config=endpoint_kwargs)
    local_ep.prepare()

    return local_ep


def _parse_datetime(dt_str: str) -> time.struct_time:
    """Parse a datetime string to struct_time.

    Supports formats:
        - 2026-01-04
        - 2026-01-04 12:00
        - 2026-01-04 12:00:00
        - 2026-01-04T12:00:00

    Args:
        dt_str: Datetime string

    Returns:
        time.struct_time

    Raises:
        ValueError: If format is not recognized
    """
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(dt_str, fmt)
            return dt.timetuple()
        except ValueError:
            continue

    raise ValueError(
        f"Could not parse date '{dt_str}'. "
        f"Use format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"
    )


def _interactive_select(backup_endpoint) -> str | None:
    """Interactively select a snapshot to restore.

    Args:
        backup_endpoint: Endpoint to list snapshots from

    Returns:
        Selected snapshot name, or None if cancelled
    """
    try:
        snapshots = list_remote_snapshots(backup_endpoint)
    except Exception as e:
        logger.error("Failed to list snapshots: %s", e)
        return None

    if not snapshots:
        print("No snapshots available")
        return None

    print("")
    print("Available snapshots:")
    print("")

    for i, snap in enumerate(snapshots, 1):
        name = snap.get_name()
        if hasattr(snap, "time_obj") and snap.time_obj:
            time_str = time.strftime("%Y-%m-%d %H:%M:%S", snap.time_obj)
        else:
            time_str = "unknown"
        print(f"  {i:3}. {name:<40} ({time_str})")

    print("")
    print("  0. Cancel")
    print("")

    while True:
        try:
            choice = input("Select snapshot to restore [0]: ").strip()
            if not choice or choice == "0":
                return None

            idx = int(choice) - 1
            if 0 <= idx < len(snapshots):
                selected = snapshots[idx]
                print(f"\nSelected: {selected.get_name()}")
                confirm = input("Proceed with restore? [y/N]: ").strip().lower()
                if confirm in ("y", "yes"):
                    return selected.get_name()
                else:
                    print("Cancelled")
                    return None
            else:
                print(f"Invalid selection. Enter 1-{len(snapshots)} or 0 to cancel.")

        except ValueError:
            print("Invalid input. Enter a number.")
        except (EOFError, KeyboardInterrupt):
            print("\nCancelled")
            return None
