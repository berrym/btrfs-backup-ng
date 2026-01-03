"""Prune command: Apply retention policies."""

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


def execute_prune(args: argparse.Namespace) -> int:
    """Execute the prune command.

    Applies retention policies to clean up old snapshots and backups.

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

    volumes = config.get_enabled_volumes()

    if not volumes:
        logger.error("No volumes configured")
        return 1

    dry_run = getattr(args, "dry_run", False)
    if dry_run:
        logger.info("Dry run mode - showing what would be deleted")

    logger.info(__util__.log_heading(f"Pruning snapshots at {time.ctime()}"))

    total_deleted = 0
    total_kept = 0

    for volume in volumes:
        logger.info("Volume: %s", volume.path)

        retention = config.get_effective_retention(volume)

        # For now, use simple count-based retention
        # TODO: Implement time-based retention (Phase 3)
        # Using daily count as the primary retention limit
        keep_count = retention.daily + retention.weekly + retention.monthly

        # Build endpoint kwargs
        endpoint_kwargs = {
            "snap_prefix": volume.snapshot_prefix or f"{os.uname()[1]}-",
            "convert_rw": False,
            "subvolume_sync": False,
            "btrfs_debug": False,
            "fs_checks": True,
        }

        # Prune source snapshots
        try:
            source_path = Path(volume.path).resolve()

            snapshot_dir = Path(volume.snapshot_dir)
            if not snapshot_dir.is_absolute():
                snapshot_dir = source_path.parent / snapshot_dir
            snapshot_dir = snapshot_dir.resolve()

            relative_source = str(source_path).lstrip(os.sep)
            full_snapshot_dir = snapshot_dir.joinpath(*relative_source.split(os.sep))

            if not full_snapshot_dir.exists():
                logger.info("  No snapshot directory found")
                continue

            source_kwargs = dict(endpoint_kwargs)
            source_kwargs["path"] = full_snapshot_dir

            source_endpoint = endpoint.choose_endpoint(
                str(source_path),
                source_kwargs,
                source=True,
            )
            source_endpoint.prepare()

            snapshots = source_endpoint.list_snapshots()
            logger.info(
                "  Source: %d snapshots, keeping %d", len(snapshots), keep_count
            )

            if len(snapshots) > keep_count:
                to_delete = snapshots[:-keep_count]
                logger.info("  Would delete %d old snapshot(s)", len(to_delete))

                if not dry_run:
                    for snap in to_delete:
                        try:
                            source_endpoint.delete_snapshots([snap])
                            logger.info("    Deleted: %s", snap.get_name())
                            total_deleted += 1
                        except Exception as e:
                            logger.error(
                                "    Failed to delete %s: %s", snap.get_name(), e
                            )
                else:
                    for snap in to_delete:
                        logger.info("    Would delete: %s", snap.get_name())
                    total_deleted += len(to_delete)

                total_kept += keep_count
            else:
                total_kept += len(snapshots)

        except Exception as e:
            logger.error("  Error pruning source: %s", e)

        # Prune target backups
        for target in volume.targets:
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
                logger.info(
                    "  Target %s: %d backups, keeping %d",
                    target.path,
                    len(dest_snapshots),
                    keep_count,
                )

                if len(dest_snapshots) > keep_count:
                    to_delete = dest_snapshots[:-keep_count]
                    logger.info("    Would delete %d old backup(s)", len(to_delete))

                    if not dry_run:
                        for snap in to_delete:
                            try:
                                dest_endpoint.delete_snapshots([snap])
                                logger.info("      Deleted: %s", snap.get_name())
                                total_deleted += 1
                            except Exception as e:
                                logger.error(
                                    "      Failed to delete %s: %s", snap.get_name(), e
                                )
                    else:
                        for snap in to_delete:
                            logger.info("      Would delete: %s", snap.get_name())
                        total_deleted += len(to_delete)

                    total_kept += keep_count
                else:
                    total_kept += len(dest_snapshots)

            except Exception as e:
                logger.error("  Error pruning target %s: %s", target.path, e)

    logger.info(__util__.log_heading(f"Finished at {time.ctime()}"))

    if dry_run:
        logger.info("Dry run: would delete %d, keep %d", total_deleted, total_kept)
    else:
        logger.info("Deleted %d snapshot(s), kept %d", total_deleted, total_kept)

    return 0
