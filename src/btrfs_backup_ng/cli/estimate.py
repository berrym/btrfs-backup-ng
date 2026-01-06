"""Estimate command: Calculate backup transfer sizes before execution.

Provides pre-transfer size estimates to help with:
- Bandwidth planning
- Time estimation
- Verification of expected transfers
- Space availability checking (with --check-space)
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from .. import endpoint
from ..__logger__ import create_logger
from ..config import ConfigError, find_config_file, load_config
from ..core.estimate import (
    TransferEstimate,
    estimate_transfer,
    format_size,
    print_estimate,
)
from ..core.space import (
    DEFAULT_SAFETY_MARGIN_PERCENT,
    check_space_availability,
    format_space_check,
)
from .common import get_fs_checks_mode, get_log_level

logger = logging.getLogger(__name__)


def execute_estimate(args: argparse.Namespace) -> int:
    """Execute the estimate command.

    Calculates and displays estimated transfer sizes for backup operations.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    # Determine mode: config-driven or direct path
    volume_path = getattr(args, "volume", None)
    source = getattr(args, "source", None)
    destination = getattr(args, "destination", None)

    if volume_path:
        return _estimate_from_config(args, volume_path)
    elif source and destination:
        return _estimate_direct(args, source, destination)
    else:
        print(
            "Error: Specify either --volume (config-driven) or source and destination paths"
        )
        print("Usage:")
        print("  btrfs-backup-ng estimate <source> <destination>")
        print("  btrfs-backup-ng estimate --volume /home")
        return 1


def _estimate_from_config(args: argparse.Namespace, volume_path: str) -> int:
    """Estimate transfer sizes using configuration file.

    Args:
        args: Command arguments
        volume_path: Path of volume from config (e.g., /home)

    Returns:
        Exit code
    """
    # Load config
    config_path = getattr(args, "config", None)
    try:
        if config_path:
            config, _warnings = load_config(config_path)
        else:
            found_path = find_config_file()
            if not found_path:
                print("Error: No configuration file found")
                print("Use --config to specify a config file")
                return 1
            config, _warnings = load_config(found_path)
    except ConfigError as e:
        logger.error("Failed to load config: %s", e)
        return 1

    # Find volume
    volume = None
    for vol in config.get_enabled_volumes():
        if vol.path == volume_path:
            volume = vol
            break

    if not volume:
        print(f"Error: Volume '{volume_path}' not found in configuration")
        print("Available volumes:")
        for vol in config.get_enabled_volumes():
            print(f"  {vol.path}")
        return 1

    if not volume.targets:
        print(f"Error: Volume '{volume_path}' has no backup targets configured")
        return 1

    # Get target index
    target_idx = getattr(args, "target", None) or 0
    if target_idx < 0 or target_idx >= len(volume.targets):
        print(f"Error: Invalid target index {target_idx}")
        return 1

    target = volume.targets[target_idx]
    json_output = getattr(args, "json", False)

    # Prepare source endpoint
    source_path = Path(volume.path)
    snapshot_dir = source_path / volume.snapshot_dir

    try:
        source_ep = endpoint.choose_endpoint(
            str(snapshot_dir),
            {
                "path": snapshot_dir,
                "snap_prefix": volume.snapshot_prefix,
                "fs_checks": "auto",
            },
            source=True,
        )
        source_ep.prepare()
    except Exception as e:
        logger.error("Failed to prepare source endpoint: %s", e)
        return 1

    # Prepare destination endpoint
    try:
        dest_kwargs: dict[str, Any] = {
            "snap_prefix": volume.snapshot_prefix,
            "fs_checks": "auto",
        }
        if target.ssh_sudo:
            dest_kwargs["ssh_sudo"] = True
        if target.ssh_key:
            dest_kwargs["ssh_identity_file"] = target.ssh_key

        dest_ep = endpoint.choose_endpoint(
            target.path,
            dest_kwargs,
            source=False,
        )
        dest_ep.prepare()
    except Exception as e:
        logger.error("Failed to prepare destination endpoint: %s", e)
        return 1

    # Run estimation
    try:
        estimate = estimate_transfer(source_ep, dest_ep)
    except Exception as e:
        logger.error("Estimation failed: %s", e)
        return 1

    # Output results
    if json_output:
        _print_json(estimate, str(snapshot_dir), target.path, dest_ep, args)
    else:
        print_estimate(estimate, str(snapshot_dir), target.path)
        _print_space_check(args, dest_ep, estimate)

    return 0


def _estimate_direct(args: argparse.Namespace, source: str, destination: str) -> int:
    """Estimate transfer sizes for direct source/destination paths.

    Args:
        args: Command arguments
        source: Source path
        destination: Destination path

    Returns:
        Exit code
    """
    json_output = getattr(args, "json", False)
    prefix = getattr(args, "prefix", "") or ""
    ssh_sudo = getattr(args, "ssh_sudo", False)
    ssh_key = getattr(args, "ssh_key", None)

    # Prepare source endpoint
    fs_checks_mode = get_fs_checks_mode(args)

    try:
        source_kwargs = {
            "snap_prefix": prefix,
            "fs_checks": fs_checks_mode,
        }
        if not source.startswith("ssh://"):
            source_kwargs["path"] = Path(source).resolve()

        source_ep = endpoint.choose_endpoint(
            source,
            source_kwargs,
            source=True,
        )
        source_ep.prepare()
    except Exception as e:
        logger.error("Failed to prepare source endpoint: %s", e)
        return 1

    # Prepare destination endpoint
    try:
        dest_kwargs = {
            "snap_prefix": prefix,
            "fs_checks": fs_checks_mode,
        }
        if ssh_sudo:
            dest_kwargs["ssh_sudo"] = True
        if ssh_key:
            dest_kwargs["ssh_identity_file"] = ssh_key
        if not destination.startswith("ssh://"):
            dest_kwargs["path"] = Path(destination).resolve()

        dest_ep = endpoint.choose_endpoint(
            destination,
            dest_kwargs,
            source=False,
        )
        dest_ep.prepare()
    except Exception as e:
        logger.error("Failed to prepare destination endpoint: %s", e)
        return 1

    # Run estimation
    try:
        estimate = estimate_transfer(source_ep, dest_ep)
    except Exception as e:
        logger.error("Estimation failed: %s", e)
        return 1

    # Output results
    if json_output:
        _print_json(estimate, source, destination, dest_ep, args)
    else:
        print_estimate(estimate, source, destination)
        _print_space_check(args, dest_ep, estimate)

    return 0


def _print_space_check(
    args: argparse.Namespace,
    dest_ep: Any,
    estimate: TransferEstimate,
) -> None:
    """Print space availability check if requested.

    Args:
        args: Command arguments (check for --check-space flag)
        dest_ep: Destination endpoint
        estimate: Transfer estimate with size information
    """
    if not getattr(args, "check_space", False):
        return

    if estimate.total_incremental_size == 0:
        print("\nNo data to transfer - skipping space check.")
        return

    safety_margin = getattr(args, "safety_margin", DEFAULT_SAFETY_MARGIN_PERCENT)

    try:
        space_info = dest_ep.get_space_info()
        space_check = check_space_availability(
            space_info,
            estimate.total_incremental_size,
            safety_margin_percent=safety_margin,
        )
        print()
        print(format_space_check(space_check))
    except Exception as e:
        logger.warning("Could not check destination space: %s", e)
        print(f"\nWarning: Could not check destination space: {e}")


def _print_json(
    estimate: TransferEstimate,
    source: str,
    destination: str,
    dest_ep: Any = None,
    args: argparse.Namespace | None = None,
) -> None:
    """Print estimate as JSON.

    Args:
        estimate: The transfer estimate
        source: Source path for display
        destination: Destination path for display
        dest_ep: Optional destination endpoint for space checking
        args: Optional command arguments
    """
    data: dict[str, Any] = {
        "source": source,
        "destination": destination,
        "snapshot_count": estimate.snapshot_count,
        "new_snapshot_count": estimate.new_snapshot_count,
        "skipped_count": estimate.skipped_count,
        "total_transfer_bytes": estimate.total_incremental_size,
        "total_transfer_human": format_size(estimate.total_incremental_size),
        "total_full_bytes": estimate.total_full_size,
        "total_full_human": format_size(estimate.total_full_size),
        "estimation_time_seconds": round(estimate.estimation_time, 2),
        "snapshots": [],
    }

    for snap in estimate.snapshots:
        snap_data = {
            "name": snap.name,
            "full_size_bytes": snap.full_size,
            "full_size_human": format_size(snap.full_size),
            "is_incremental": snap.is_incremental,
            "method": snap.method,
        }
        if snap.is_incremental:
            snap_data["incremental_size_bytes"] = snap.incremental_size
            snap_data["incremental_size_human"] = format_size(snap.incremental_size)
            snap_data["parent"] = snap.parent_name

        data["snapshots"].append(snap_data)

    # Add space check if requested
    if args and getattr(args, "check_space", False) and dest_ep:
        safety_margin = getattr(args, "safety_margin", DEFAULT_SAFETY_MARGIN_PERCENT)
        try:
            space_info = dest_ep.get_space_info()
            space_check = check_space_availability(
                space_info,
                estimate.total_incremental_size,
                safety_margin_percent=safety_margin,
            )
            data["space_check"] = {
                "path": space_info.path,
                "total_bytes": space_info.total_bytes,
                "total_human": format_size(space_info.total_bytes),
                "available_bytes": space_info.available_bytes,
                "available_human": format_size(space_info.available_bytes),
                "quota_enabled": space_info.quota_enabled,
                "quota_limit_bytes": space_info.quota_limit,
                "quota_used_bytes": space_info.quota_used,
                "effective_available_bytes": space_check.effective_limit,
                "effective_available_human": format_size(space_check.effective_limit),
                "required_with_margin_bytes": space_check.required_with_margin,
                "required_with_margin_human": format_size(
                    space_check.required_with_margin
                ),
                "safety_margin_percent": space_check.safety_margin_percent,
                "sufficient": space_check.sufficient,
                "available_after_bytes": space_check.available_after,
                "available_after_human": format_size(space_check.available_after),
                "source": space_info.source,
            }
            if space_check.warning_message:
                data["space_check"]["warning"] = space_check.warning_message
        except Exception as e:
            data["space_check"] = {
                "error": str(e),
                "sufficient": None,
            }

    print(json.dumps(data, indent=2))
