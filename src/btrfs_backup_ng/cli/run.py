"""Run command: Execute all configured backup jobs."""

import argparse
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .. import __util__, endpoint
from ..__logger__ import add_file_handler, create_logger
from ..config import (
    Config,
    ConfigError,
    TargetConfig,
    VolumeConfig,
    find_config_file,
    load_config,
)
from ..core.operations import sync_snapshots
from ..notifications import (
    EmailConfig,
    WebhookConfig,
    create_backup_event,
    send_notifications,
)
from ..notifications import (
    NotificationConfig as NotifConfig,
)
from ..transaction import set_transaction_log
from .common import get_log_level, should_show_progress

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

    # Enable file logging if configured
    if config.global_config.log_file:
        add_file_handler(config.global_config.log_file)
        logger.info("File logging enabled: %s", config.global_config.log_file)

    # Enable transaction logging if configured
    if config.global_config.transaction_log:
        set_transaction_log(config.global_config.transaction_log)
        logger.info(
            "Transaction logging enabled: %s", config.global_config.transaction_log
        )

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

    start_time = time.time()
    logger.info(__util__.log_heading(f"Started at {time.ctime()}"))
    logger.info(
        "Parallel volumes: %d, parallel targets: %d", parallel_volumes, parallel_targets
    )

    # Get CLI overrides for compression/throttling
    compress_override = getattr(args, "compress", None)
    rate_limit_override = getattr(args, "rate_limit", None)

    # Determine if progress should be shown
    show_progress = should_show_progress(args)
    if show_progress:
        logger.debug("Progress bars enabled (interactive terminal detected)")
    else:
        logger.debug("Progress bars disabled")

    # Execute backup for each enabled volume
    enabled_volumes = config.get_enabled_volumes()
    logger.info("Processing %d volume(s)", len(enabled_volumes))

    results = []
    transfer_stats = {"completed": 0, "failed": 0}
    error_messages = []

    if parallel_volumes > 1 and len(enabled_volumes) > 1:
        # Parallel volume execution
        with ThreadPoolExecutor(max_workers=parallel_volumes) as executor:
            futures = {
                executor.submit(
                    _backup_volume,
                    volume,
                    config,
                    parallel_targets,
                    compress_override,
                    rate_limit_override,
                    show_progress,
                ): volume
                for volume in enabled_volumes
            }
            for future in as_completed(futures):
                volume = futures[future]
                try:
                    success, vol_stats, vol_errors = future.result()
                    results.append((volume.path, success))
                    transfer_stats["completed"] += vol_stats.get("completed", 0)
                    transfer_stats["failed"] += vol_stats.get("failed", 0)
                    error_messages.extend(vol_errors)
                except Exception as e:
                    logger.error("Volume %s failed: %s", volume.path, e)
                    results.append((volume.path, False))
                    error_messages.append(f"Volume {volume.path}: {e}")
    else:
        # Sequential execution
        for volume in enabled_volumes:
            try:
                success, vol_stats, vol_errors = _backup_volume(
                    volume,
                    config,
                    parallel_targets,
                    compress_override,
                    rate_limit_override,
                    show_progress,
                )
                results.append((volume.path, success))
                transfer_stats["completed"] += vol_stats.get("completed", 0)
                transfer_stats["failed"] += vol_stats.get("failed", 0)
                error_messages.extend(vol_errors)
            except Exception as e:
                logger.error("Volume %s failed: %s", volume.path, e)
                results.append((volume.path, False))
                error_messages.append(f"Volume {volume.path}: {e}")

    # Summary
    end_time = time.time()
    duration = end_time - start_time
    logger.info(__util__.log_heading(f"Finished at {time.ctime()}"))

    success_count = sum(1 for _, success in results if success)
    fail_count = len(results) - success_count

    if fail_count > 0:
        logger.warning(
            "Completed with errors: %d succeeded, %d failed", success_count, fail_count
        )
        exit_code = 1
    else:
        logger.info("All %d volume(s) completed successfully", success_count)
        exit_code = 0

    # Send notifications if configured
    _send_backup_notifications(
        config,
        volumes_processed=len(results),
        volumes_failed=fail_count,
        snapshots_created=success_count,  # One snapshot per successful volume
        transfers_completed=transfer_stats.get("completed", 0),
        transfers_failed=transfer_stats.get("failed", 0),
        duration_seconds=duration,
        errors=error_messages,
    )

    return exit_code


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


def _backup_volume(
    volume: VolumeConfig,
    config: Config,
    parallel_targets: int,
    compress_override: str | None = None,
    rate_limit_override: str | None = None,
    show_progress: bool = False,
) -> tuple[bool, dict[str, int], list[str]]:
    """Execute backup for a single volume.

    Args:
        volume: Volume configuration
        config: Full configuration
        parallel_targets: Max concurrent target transfers
        compress_override: CLI override for compression method
        rate_limit_override: CLI override for bandwidth limit
        show_progress: Whether to show progress bars

    Returns:
        Tuple of (success, transfer_stats, error_messages)
    """
    stats = {"completed": 0, "failed": 0}
    errors = []
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
            # Relative snapshot_dir: relative to source volume
            # e.g., ".btrfs-backup-ng/snapshots" -> source/.btrfs-backup-ng/snapshots
            full_snapshot_dir = (source_path / snapshot_dir).resolve()
        else:
            # Absolute snapshot_dir: use it directly, add source name as subdirectory
            # e.g., "/snapshots" + source "myvolume" -> /snapshots/myvolume
            full_snapshot_dir = (snapshot_dir / source_path.name).resolve()

        full_snapshot_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

        source_kwargs = dict(endpoint_kwargs)
        source_kwargs["path"] = full_snapshot_dir
        # Set snapshot_folder - use absolute path for external snapshot directories
        source_kwargs["snapshot_folder"] = str(full_snapshot_dir)

        source_endpoint = endpoint.choose_endpoint(
            str(source_path),
            source_kwargs,
            source=True,
        )
        source_endpoint.prepare()
        logger.debug("Source endpoint ready: %s", source_endpoint)

    except Exception as e:
        logger.error("Failed to prepare source endpoint for %s: %s", volume.path, e)
        errors.append(f"Source endpoint {volume.path}: {e}")
        return False, stats, errors

    # Create snapshot
    try:
        logger.info("Creating snapshot...")
        snapshot = source_endpoint.snapshot()
        logger.info("Created snapshot: %s", snapshot)
    except Exception as e:
        logger.error("Failed to create snapshot: %s", e)
        errors.append(f"Snapshot creation for {volume.path}: {e}")
        return False, stats, errors

    # Prepare destination endpoints
    if not volume.targets:
        logger.info("No targets configured, snapshot created but not transferred")
        return True, stats, errors

    destination_endpoints = []
    for target in volume.targets:
        try:
            # Check mount requirement for local targets
            if target.require_mount and not target.path.startswith("ssh://"):
                target_path = Path(target.path).resolve()
                if not __util__.is_mounted(target_path):
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
            # Store endpoint with its target config for compression/throttle options
            destination_endpoints.append((dest_endpoint, target))
            logger.debug("Destination endpoint ready: %s", dest_endpoint)

        except Exception as e:
            logger.error("Failed to prepare destination %s: %s", target.path, e)
            errors.append(f"Destination endpoint {target.path}: {e}")

    if not destination_endpoints:
        logger.error("No destination endpoints could be prepared")
        return False, stats, errors

    # Transfer to destinations
    all_success = True

    if parallel_targets > 1 and len(destination_endpoints) > 1:
        # Parallel target transfers
        with ThreadPoolExecutor(max_workers=parallel_targets) as executor:
            futures = {
                executor.submit(
                    _transfer_to_target,
                    source_endpoint,
                    dest_endpoint,
                    target_config,
                    snapshot,
                    config.global_config.incremental,
                    compress_override,
                    rate_limit_override,
                    show_progress,
                ): (dest_endpoint, target_config)
                for dest_endpoint, target_config in destination_endpoints
            }
            for future in as_completed(futures):
                dest, target_cfg = futures[future]
                try:
                    success = future.result()
                    if success:
                        stats["completed"] += 1
                    else:
                        stats["failed"] += 1
                        all_success = False
                except Exception as e:
                    logger.error("Transfer to %s failed: %s", dest, e)
                    stats["failed"] += 1
                    errors.append(f"Transfer to {target_cfg.path}: {e}")
                    all_success = False
    else:
        # Sequential transfers
        for dest_endpoint, target_config in destination_endpoints:
            try:
                success = _transfer_to_target(
                    source_endpoint,
                    dest_endpoint,
                    target_config,
                    snapshot,
                    config.global_config.incremental,
                    compress_override,
                    rate_limit_override,
                    show_progress,
                )
                if success:
                    stats["completed"] += 1
                else:
                    stats["failed"] += 1
                    all_success = False
            except Exception as e:
                logger.error("Transfer to %s failed: %s", dest_endpoint, e)
                stats["failed"] += 1
                errors.append(f"Transfer to {target_config.path}: {e}")
                all_success = False

    return all_success, stats, errors


def _transfer_to_target(
    source_endpoint,
    destination_endpoint,
    target_config: TargetConfig,
    snapshot,
    incremental: bool,
    compress_override: str | None = None,
    rate_limit_override: str | None = None,
    show_progress: bool = False,
) -> bool:
    """Transfer snapshot to a single target.

    Args:
        source_endpoint: Source endpoint
        destination_endpoint: Destination endpoint
        target_config: Target configuration with compression/throttle settings
        snapshot: Snapshot to transfer
        incremental: Whether to use incremental transfers
        compress_override: CLI override for compression method
        rate_limit_override: CLI override for bandwidth limit
        show_progress: Whether to show progress bars

    Returns:
        True if successful
    """
    try:
        # Build transfer options with compression and throttling
        # CLI overrides take precedence over config
        transfer_options = {
            "compress": compress_override or target_config.compress,
            "rate_limit": rate_limit_override or target_config.rate_limit,
            "ssh_sudo": target_config.ssh_sudo,
            "show_progress": show_progress,
        }

        sync_snapshots(
            source_endpoint,
            destination_endpoint,
            keep_num_backups=0,
            no_incremental=not incremental,
            snapshot=snapshot,
            options=transfer_options,
        )
        return True
    except __util__.AbortError as e:
        logger.error("Transfer to %s aborted: %s", destination_endpoint, e)
        return False
    except Exception as e:
        logger.error("Transfer to %s failed: %s", destination_endpoint, e)
        return False


def _send_backup_notifications(
    config: Config,
    volumes_processed: int,
    volumes_failed: int,
    snapshots_created: int,
    transfers_completed: int,
    transfers_failed: int,
    duration_seconds: float,
    errors: list[str],
) -> None:
    """Send backup completion notifications if configured.

    Args:
        config: Full configuration with notification settings
        volumes_processed: Total volumes processed
        volumes_failed: Number of failed volumes
        snapshots_created: Number of snapshots created
        transfers_completed: Number of successful transfers
        transfers_failed: Number of failed transfers
        duration_seconds: Total backup duration
        errors: List of error messages
    """
    notif_config = config.global_config.notifications
    if not notif_config.is_enabled():
        return

    # Determine overall status
    if volumes_failed == 0 and transfers_failed == 0:
        status = "success"
    elif volumes_failed == volumes_processed:
        status = "failure"
    else:
        status = "partial"

    # Create notification event
    event = create_backup_event(
        status=status,
        volumes_processed=volumes_processed,
        volumes_failed=volumes_failed,
        snapshots_created=snapshots_created,
        transfers_completed=transfers_completed,
        transfers_failed=transfers_failed,
        duration_seconds=duration_seconds,
        errors=errors,
    )

    # Convert config schema to notification module types
    email_config = EmailConfig(
        enabled=notif_config.email.enabled,
        smtp_host=notif_config.email.smtp_host,
        smtp_port=notif_config.email.smtp_port,
        smtp_user=notif_config.email.smtp_user,
        smtp_password=notif_config.email.smtp_password,
        smtp_tls=notif_config.email.smtp_tls,
        from_addr=notif_config.email.from_addr,
        to_addrs=notif_config.email.to_addrs,
        on_success=notif_config.email.on_success,
        on_failure=notif_config.email.on_failure,
    )

    webhook_config = WebhookConfig(
        enabled=notif_config.webhook.enabled,
        url=notif_config.webhook.url,
        method=notif_config.webhook.method,
        headers=notif_config.webhook.headers,
        on_success=notif_config.webhook.on_success,
        on_failure=notif_config.webhook.on_failure,
        timeout=notif_config.webhook.timeout,
    )

    notif = NotifConfig(email=email_config, webhook=webhook_config)

    # Send notifications
    results = send_notifications(notif, event)

    for method, success in results.items():
        if success:
            logger.info("Sent %s notification", method)
        else:
            logger.warning("Failed to send %s notification", method)
