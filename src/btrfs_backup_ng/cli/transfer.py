"""Transfer command: Transfer existing snapshots to targets."""

import argparse
import logging
import time
from pathlib import Path

from .. import __util__, endpoint
from ..__logger__ import add_file_handler, create_logger
from ..config import ConfigError, find_config_file, load_config
from ..core.operations import sync_snapshots
from .common import (
    apply_config_verbosity,
    btrfs_debug_enabled,
    resolve_snapshot_dir,
    assert_target_mounted,
    get_log_level,
    get_timestamp_format,
    space_options_from_args,
    thread_raw_compression,
    thread_raw_encryption,
    thread_ssh_target_config,
)

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

    except ConfigError as e:
        logger.error("Configuration error: %s", e)
        return 1

    # Enable file logging if configured
    if config.global_config.log_file:
        add_file_handler(config.global_config.log_file)

        # Emitted AFTER the file handler is installed. Logged before it, these
        # went to the console only -- an operator running from cron or systemd
        # with log_file set had a log that silently omitted every config
        # warning, which is the one place they would look afterwards.
    apply_config_verbosity(args, config)
    for warning in warnings:
        logger.warning("Config: %s", warning)

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
    #: Snapshots actually delivered, as opposed to targets that finished without
    #: raising. A target whose plan was empty because it is already up to date
    #: increments success_count and moves nothing.
    moved_count = 0
    #: Optional targets that were unavailable. Their `continue` bypasses both
    #: counters, so without this a run where every target was skipped reported
    #: complete success.
    skipped_count = 0

    for volume in volumes:
        logger.info("Volume: %s", volume.path)

        if not volume.targets:
            logger.warning("  No targets configured, skipping")
            continue

        try:
            # Build endpoint kwargs
            endpoint_kwargs = {
                "snap_prefix": volume.snapshot_prefix,
                "convert_rw": False,
                "subvolume_sync": False,
                "btrfs_debug": btrfs_debug_enabled(args, config),
                "fs_checks": "auto",
                "timestamp_format": get_timestamp_format(config),
            }

            # Prepare source endpoint
            source_path = Path(volume.path).resolve()

            # ONE resolution for every command (resolve_snapshot_dir), so
            # this command reads exactly the directory run/snapshot write. A
            # missing absolute base is refused there with the mount diagnosis
            # instead of the generic warning below.
            # ONE resolution for every command (resolve_snapshot_dir), so
            # this command reads exactly the directory run/snapshot write. A
            # missing absolute base is refused there with the mount diagnosis
            # instead of the generic warning below.
            try:
                full_snapshot_dir = resolve_snapshot_dir(
                    volume.snapshot_dir, source_path
                )
            except __util__.AbortError as e:
                logger.warning("  %s", e)
                continue

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
                    assert_target_mounted(target.path, target.require_mount)

                    dest_kwargs = dict(endpoint_kwargs)
                    thread_ssh_target_config(dest_kwargs, target)

                    thread_raw_encryption(dest_kwargs, target)
                    thread_raw_compression(
                        dest_kwargs, target, getattr(args, "compress", None)
                    )
                    dest_endpoint = endpoint.choose_endpoint(
                        target.path,
                        dest_kwargs,
                        source=False,
                    )
                    endpoint.assert_encryption_applied(target.encrypt, dest_endpoint)
                    endpoint.assert_compression_applied(
                        dest_kwargs["compress"], dest_endpoint
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
                        # Space-check flags (--no-check-space/--force/--safety-margin)
                        # so the destination space preflight can be bypassed.
                        **space_options_from_args(args),
                    }

                    result = sync_snapshots(
                        source_endpoint,
                        dest_endpoint,
                        keep_num_backups=0,
                        no_incremental=not config.global_config.incremental,
                        snapshot=None,  # Transfer all pending
                        options=transfer_options,
                    )
                    success_count += 1
                    moved_count += result.transferred_count
                    if result.transferred_count == 0:
                        logger.info("  %s is already up to date", target.path)

                except Exception as e:
                    if getattr(target, "optional", False):
                        # Declared as allowed to be absent, so its unavailability
                        # is not a transfer failure. Reported either way.
                        logger.warning(
                            "  Skipping optional target %s: %s", target.path, e
                        )
                        skipped_count += 1
                        continue
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
    if skipped_count and success_count == 0:
        # Every target was declared optional and every one was unavailable. The
        # `continue` for an optional target bypasses both counters, so this used
        # to print "All transfers completed successfully" and exit 0 for a run
        # that reached nothing at all.
        logger.warning(
            "No transfers ran: all %d target(s) were skipped as optional",
            skipped_count,
        )
        return 0
    # The snapshot count, not the target count. "All transfers completed
    # successfully" was printed for a run that moved nothing, because the
    # verdict came from fail_count alone and fail_count only ever rises in an
    # except handler.
    logger.info(
        "All transfers completed successfully; %d snapshot(s) transferred",
        moved_count,
    )
    return 0
