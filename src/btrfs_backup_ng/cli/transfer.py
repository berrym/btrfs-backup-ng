"""Transfer command: Transfer existing snapshots to targets."""

import argparse
import logging
import os
import time
from pathlib import Path

from .. import __util__, endpoint
from ..__logger__ import add_file_handler, create_logger
from ..config import ConfigError, find_config_file, load_config
from ..core.operations import sync_snapshots
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_transfer(args: argparse.Namespace) -> int:
    """Execute the transfer command.

    Transfers existing snapshots to targets without creating new ones.

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

    # Enable file logging if configured
    if config.global_config.log_file:
        add_file_handler(config.global_config.log_file)

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

    dry_run = getattr(args, "dry_run", False)

    if dry_run:
        logger.info("Dry run mode - showing what would be done")
        print("")
        for volume in volumes:
            print(f"Volume: {volume.path}")
            if volume.targets:
                print("  Would transfer to:")
                for target in volume.targets:
                    print(f"    -> {target.path}")
            else:
                print("  (no targets configured)")
        return 0

    logger.info(__util__.log_heading(f"Transferring snapshots at {time.ctime()}"))

    success_count = 0
    fail_count = 0

    for volume in volumes:
        logger.info("Volume: %s", volume.path)

        if not volume.targets:
            logger.warning("  No targets configured, skipping")
            continue

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
                # Relative snapshot_dir: relative to source volume
                full_snapshot_dir = (source_path / snapshot_dir).resolve()
            else:
                # Absolute snapshot_dir: add source name as subdirectory
                full_snapshot_dir = (snapshot_dir / source_path.name).resolve()

            if not full_snapshot_dir.exists():
                logger.warning(
                    "  Snapshot directory does not exist: %s", full_snapshot_dir
                )
                continue

            source_kwargs = dict(endpoint_kwargs)
            source_kwargs["path"] = full_snapshot_dir
            source_kwargs["snapshot_folder"] = str(full_snapshot_dir)

            source_endpoint = endpoint.choose_endpoint(
                str(source_path),
                source_kwargs,
                source=True,
            )
            source_endpoint.prepare()

            # Check for existing snapshots
            snapshots = source_endpoint.list_snapshots()
            if not snapshots:
                logger.info("  No snapshots to transfer")
                continue

            logger.info("  Found %d snapshot(s)", len(snapshots))

            # Transfer to each target
            for target in volume.targets:
                try:
                    # Check mount requirement for local targets
                    if target.require_mount and not target.path.startswith("ssh://"):
                        target_path = Path(target.path).resolve()
                        if not __util__.is_mounted(target_path):  # type: ignore[attr-defined]
                            raise __util__.AbortError(
                                f"Target {target.path} is not mounted. "
                                f"Ensure the drive is connected and mounted, or set require_mount = false."
                            )
                        logger.debug("Mount check passed for %s", target.path)

                    dest_kwargs = dict(endpoint_kwargs)
                    dest_kwargs["ssh_sudo"] = target.ssh_sudo
                    dest_kwargs["ssh_password_fallback"] = target.ssh_password_auth

                    if target.ssh_key:
                        dest_kwargs["ssh_identity_file"] = target.ssh_key

                    dest_endpoint = endpoint.choose_endpoint(
                        target.path,
                        dest_kwargs,
                        source=False,
                    )
                    dest_endpoint.prepare()

                    # Build transfer options with compression and throttling
                    # CLI overrides take precedence over config
                    compress_override = getattr(args, "compress", None)
                    rate_limit_override = getattr(args, "rate_limit", None)

                    transfer_options = {
                        "compress": compress_override or target.compress,
                        "rate_limit": rate_limit_override or target.rate_limit,
                        "ssh_sudo": target.ssh_sudo,
                    }

                    sync_snapshots(
                        source_endpoint,
                        dest_endpoint,
                        keep_num_backups=0,
                        no_incremental=not config.global_config.incremental,
                        snapshot=None,  # Transfer all pending
                        options=transfer_options,
                    )
                    success_count += 1

                except Exception as e:
                    logger.error("  Transfer to %s failed: %s", target.path, e)
                    fail_count += 1

        except Exception as e:
            logger.error("  Failed: %s", e)
            fail_count += 1

    logger.info(__util__.log_heading(f"Finished at {time.ctime()}"))

    if fail_count > 0:
        logger.warning(
            "Completed with errors: %d succeeded, %d failed", success_count, fail_count
        )
        return 1
    else:
        logger.info("All transfers completed successfully")
        return 0
