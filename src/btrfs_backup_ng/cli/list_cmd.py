"""List command: Show snapshots and backups across volumes."""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

from .. import endpoint
from ..__logger__ import create_logger
from ..config import ConfigError, find_config_file, load_config
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_list(args: argparse.Namespace) -> int:
    """Execute the list command.

    Lists all snapshots across configured volumes and their targets.

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

        config, warnings = load_config(config_path)

    except ConfigError as e:
        logger.error("Configuration error: %s", e)
        return 1

    # Filter volumes if --volume specified
    volume_filter = getattr(args, "volume", None)
    volumes = config.get_enabled_volumes()

    if volume_filter:
        volumes = [v for v in volumes if v.path in volume_filter]
        if not volumes:
            logger.error("No matching volumes found for: %s", volume_filter)
            return 1

    if not volumes:
        logger.error("No volumes configured")
        return 1

    output_json = getattr(args, "json", False)
    all_data: list[dict[str, Any]] = []

    for volume in volumes:
        volume_data: dict[str, Any] = {
            "path": volume.path,
            "snapshot_prefix": volume.snapshot_prefix,
            "snapshots": [],
            "targets": [],
        }

        # Build endpoint kwargs
        endpoint_kwargs = {
            "snap_prefix": volume.snapshot_prefix or f"{os.uname()[1]}-",
            "convert_rw": False,
            "subvolume_sync": False,
            "btrfs_debug": False,
            "fs_checks": True,
        }

        # Get source snapshots
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
                for snap in snapshots:
                    volume_data["snapshots"].append(
                        {
                            "name": snap.get_name(),
                            "path": str(snap.get_path()),
                        }
                    )

        except Exception as e:
            logger.debug("Error listing source snapshots for %s: %s", volume.path, e)

        # Get target snapshots
        for target in volume.targets:
            target_data: dict[str, Any] = {
                "path": target.path,
                "snapshots": [],
            }

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
                for snap in dest_snapshots:
                    target_data["snapshots"].append(
                        {
                            "name": snap.get_name(),
                        }
                    )

            except Exception as e:
                logger.debug(
                    "Error listing target snapshots for %s: %s", target.path, e
                )
                target_data["error"] = str(e)

            volume_data["targets"].append(target_data)

        all_data.append(volume_data)

    # Output
    if output_json:
        print(json.dumps(all_data, indent=2))
    else:
        _print_text_output(all_data)

    return 0


def _print_text_output(data: list) -> None:
    """Print human-readable output."""
    for volume in data:
        print(f"Volume: {volume['path']}")
        print(f"  Prefix: {volume['snapshot_prefix']}")
        print("")

        snapshots = volume["snapshots"]
        if snapshots:
            print(f"  Source snapshots ({len(snapshots)}):")
            for snap in snapshots[-10:]:  # Show last 10
                print(f"    {snap['name']}")
            if len(snapshots) > 10:
                print(f"    ... and {len(snapshots) - 10} more")
        else:
            print("  Source snapshots: (none)")
        print("")

        for target in volume["targets"]:
            target_snaps = target["snapshots"]
            if "error" in target:
                print(f"  Target: {target['path']} (error: {target['error']})")
            elif target_snaps:
                print(f"  Target: {target['path']} ({len(target_snaps)} snapshots)")
                for snap in target_snaps[-5:]:  # Show last 5
                    print(f"    {snap['name']}")
                if len(target_snaps) > 5:
                    print(f"    ... and {len(target_snaps) - 5} more")
            else:
                print(f"  Target: {target['path']} (no snapshots)")

        print("")
