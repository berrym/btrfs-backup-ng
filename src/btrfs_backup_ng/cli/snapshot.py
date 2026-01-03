"""Snapshot command: Create snapshots only (no transfer)."""

import argparse
import logging
import os
import time
from pathlib import Path

from .. import __util__, endpoint
from ..__logger__ import create_logger
from ..config import ConfigError, find_config_file, load_config
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_snapshot(args: argparse.Namespace) -> int:
    """Execute the snapshot command.

    Creates snapshots for configured volumes without transferring to targets.

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

        logger.info("Loading configuration from: %s", config_path)
        config, warnings = load_config(config_path)

        for warning in warnings:
            logger.warning("Config: %s", warning)

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

    logger.info(__util__.log_heading(f"Creating snapshots at {time.ctime()}"))

    success_count = 0
    fail_count = 0

    for volume in volumes:
        logger.info("Volume: %s", volume.path)

        try:
            # Build endpoint kwargs
            endpoint_kwargs = {
                "snap_prefix": volume.snapshot_prefix or f"{os.uname()[1]}-",
                "convert_rw": False,
                "subvolume_sync": False,
                "btrfs_debug": False,
                "fs_checks": True,
            }

            # Prepare source endpoint
            source_path = Path(volume.path).resolve()

            snapshot_dir = Path(volume.snapshot_dir)
            if not snapshot_dir.is_absolute():
                snapshot_dir = source_path.parent / snapshot_dir
            snapshot_dir = snapshot_dir.resolve()

            relative_source = str(source_path).lstrip(os.sep)
            full_snapshot_dir = snapshot_dir.joinpath(*relative_source.split(os.sep))
            full_snapshot_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

            source_kwargs = dict(endpoint_kwargs)
            source_kwargs["path"] = full_snapshot_dir

            source_endpoint = endpoint.choose_endpoint(
                str(source_path),
                source_kwargs,
                source=True,
            )
            source_endpoint.prepare()

            # Create snapshot
            snapshot = source_endpoint.snapshot()
            logger.info("  Created: %s", snapshot)
            success_count += 1

        except Exception as e:
            logger.error("  Failed: %s", e)
            fail_count += 1

    logger.info(__util__.log_heading(f"Finished at {time.ctime()}"))
    logger.info("Created %d snapshot(s), %d failed", success_count, fail_count)

    return 1 if fail_count > 0 else 0
