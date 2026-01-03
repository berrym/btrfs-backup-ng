"""Run command: Execute all configured backup jobs."""

import argparse
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .. import __util__, endpoint
from ..__logger__ import create_logger
from ..__logger__ import logger as root_logger
from ..config import Config, ConfigError, VolumeConfig, find_config_file, load_config
from ..core.operations import sync_snapshots
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_run(args: argparse.Namespace) -> int:
    """Execute the run command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Initialize logger
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    # Find and load config
    try:
        config_path = find_config_file(getattr(args, "config", None))
        if config_path is None:
            print("No configuration file found.")
            print("Create one with: btrfs-backup-ng config init")
            print("")
            print("Or use legacy mode: btrfs-backup-ng /source /dest")
            return 1

        logger.info("Loading configuration from: %s", config_path)
        config, warnings = load_config(config_path)

        for warning in warnings:
            logger.warning("Config: %s", warning)

    except ConfigError as e:
        logger.error("Configuration error: %s", e)
        return 1

    if not config.volumes:
        logger.error("No volumes configured")
        return 1

    # Dry run mode
    if getattr(args, "dry_run", False):
        return _dry_run(config)

    # Get parallelism settings
    parallel_volumes = (
        getattr(args, "parallel_volumes", None) or config.global_config.parallel_volumes
    )
    parallel_targets = (
        getattr(args, "parallel_targets", None) or config.global_config.parallel_targets
    )

    logger.info(__util__.log_heading(f"Started at {time.ctime()}"))
    logger.info(
        "Parallel volumes: %d, parallel targets: %d", parallel_volumes, parallel_targets
    )

    # Execute backup for each enabled volume
    enabled_volumes = config.get_enabled_volumes()
    logger.info("Processing %d volume(s)", len(enabled_volumes))

    results = []

    if parallel_volumes > 1 and len(enabled_volumes) > 1:
        # Parallel volume execution
        with ThreadPoolExecutor(max_workers=parallel_volumes) as executor:
            futures = {
                executor.submit(
                    _backup_volume, volume, config, parallel_targets
                ): volume
                for volume in enabled_volumes
            }
            for future in as_completed(futures):
                volume = futures[future]
                try:
                    success = future.result()
                    results.append((volume.path, success))
                except Exception as e:
                    logger.error("Volume %s failed: %s", volume.path, e)
                    results.append((volume.path, False))
    else:
        # Sequential execution
        for volume in enabled_volumes:
            try:
                success = _backup_volume(volume, config, parallel_targets)
                results.append((volume.path, success))
            except Exception as e:
                logger.error("Volume %s failed: %s", volume.path, e)
                results.append((volume.path, False))

    # Summary
    logger.info(__util__.log_heading(f"Finished at {time.ctime()}"))

    success_count = sum(1 for _, success in results if success)
    fail_count = len(results) - success_count

    if fail_count > 0:
        logger.warning(
            "Completed with errors: %d succeeded, %d failed", success_count, fail_count
        )
        return 1
    else:
        logger.info("All %d volume(s) completed successfully", success_count)
        return 0


def _dry_run(config: Config) -> int:
    """Show what would be done without making changes."""
    print("Dry run mode - showing what would be done:")
    print("")

    for volume in config.get_enabled_volumes():
        print(f"Volume: {volume.path}")
        print(f"  Snapshot prefix: {volume.snapshot_prefix}")
        print(f"  Snapshot dir: {volume.snapshot_dir}")

        retention = config.get_effective_retention(volume)
        print(
            f"  Retention: min={retention.min}, hourly={retention.hourly}, daily={retention.daily}"
        )

        if volume.targets:
            print("  Targets:")
            for target in volume.targets:
                sudo_note = " (sudo)" if target.ssh_sudo else ""
                print(f"    -> {target.path}{sudo_note}")
        else:
            print("  Targets: (none)")
        print("")

    return 0


def _backup_volume(volume: VolumeConfig, config: Config, parallel_targets: int) -> bool:
    """Execute backup for a single volume.

    Args:
        volume: Volume configuration
        config: Full configuration
        parallel_targets: Max concurrent target transfers

    Returns:
        True if successful, False otherwise
    """
    logger.info(__util__.log_heading(f"Volume: {volume.path}"))

    # Build endpoint kwargs
    endpoint_kwargs = {
        "snap_prefix": volume.snapshot_prefix or f"{os.uname()[1]}-",
        "convert_rw": False,
        "subvolume_sync": False,
        "btrfs_debug": False,
        "fs_checks": True,
    }

    # Prepare source endpoint
    try:
        source_path = Path(volume.path).resolve()

        # Set up snapshot directory
        snapshot_dir = Path(volume.snapshot_dir)
        if not snapshot_dir.is_absolute():
            snapshot_dir = source_path.parent / snapshot_dir
        snapshot_dir = snapshot_dir.resolve()

        # Create snapshot directory structure
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
        logger.debug("Source endpoint ready: %s", source_endpoint)

    except Exception as e:
        logger.error("Failed to prepare source endpoint for %s: %s", volume.path, e)
        return False

    # Create snapshot
    try:
        logger.info("Creating snapshot...")
        snapshot = source_endpoint.snapshot()
        logger.info("Created snapshot: %s", snapshot)
    except Exception as e:
        logger.error("Failed to create snapshot: %s", e)
        return False

    # Prepare destination endpoints
    if not volume.targets:
        logger.info("No targets configured, snapshot created but not transferred")
        return True

    destination_endpoints = []
    for target in volume.targets:
        try:
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
            destination_endpoints.append(dest_endpoint)
            logger.debug("Destination endpoint ready: %s", dest_endpoint)

        except Exception as e:
            logger.error("Failed to prepare destination %s: %s", target.path, e)

    if not destination_endpoints:
        logger.error("No destination endpoints could be prepared")
        return False

    # Transfer to destinations
    all_success = True

    if parallel_targets > 1 and len(destination_endpoints) > 1:
        # Parallel target transfers
        with ThreadPoolExecutor(max_workers=parallel_targets) as executor:
            futures = {
                executor.submit(
                    _transfer_to_target,
                    source_endpoint,
                    dest,
                    snapshot,
                    config.global_config.incremental,
                ): dest
                for dest in destination_endpoints
            }
            for future in as_completed(futures):
                dest = futures[future]
                try:
                    success = future.result()
                    if not success:
                        all_success = False
                except Exception as e:
                    logger.error("Transfer to %s failed: %s", dest, e)
                    all_success = False
    else:
        # Sequential transfers
        for dest in destination_endpoints:
            try:
                success = _transfer_to_target(
                    source_endpoint,
                    dest,
                    snapshot,
                    config.global_config.incremental,
                )
                if not success:
                    all_success = False
            except Exception as e:
                logger.error("Transfer to %s failed: %s", dest, e)
                all_success = False

    return all_success


def _transfer_to_target(
    source_endpoint,
    destination_endpoint,
    snapshot,
    incremental: bool,
) -> bool:
    """Transfer snapshot to a single target.

    Args:
        source_endpoint: Source endpoint
        destination_endpoint: Destination endpoint
        snapshot: Snapshot to transfer
        incremental: Whether to use incremental transfers

    Returns:
        True if successful
    """
    try:
        sync_snapshots(
            source_endpoint,
            destination_endpoint,
            keep_num_backups=0,
            no_incremental=not incremental,
            snapshot=snapshot,
            options={},
        )
        return True
    except __util__.AbortError as e:
        logger.error("Transfer to %s aborted: %s", destination_endpoint, e)
        return False
    except Exception as e:
        logger.error("Transfer to %s failed: %s", destination_endpoint, e)
        return False
