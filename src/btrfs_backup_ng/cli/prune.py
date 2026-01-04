"""Prune command: Apply retention policies."""

import argparse
import logging
import os
import time
from pathlib import Path

from .. import __util__, endpoint
from ..__logger__ import add_file_handler, create_logger
from ..config import Config, ConfigError, find_config_file, load_config
from ..notifications import (
    EmailConfig,
    WebhookConfig,
    create_prune_event,
    send_notifications,
)
from ..notifications import (
    NotificationConfig as NotifConfig,
)
from ..retention import apply_retention, format_retention_summary
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_prune(args: argparse.Namespace) -> int:
    """Execute the prune command.

    Applies time-based retention policies to clean up old snapshots and backups.

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

    volumes = config.get_enabled_volumes()

    if not volumes:
        logger.error("No volumes configured")
        return 1

    dry_run = getattr(args, "dry_run", False)
    if dry_run:
        logger.info("Dry run mode - showing what would be deleted")

    start_time = time.time()
    logger.info(__util__.log_heading(f"Pruning snapshots at {time.ctime()}"))

    total_deleted = 0
    total_kept = 0
    volumes_processed = 0
    volumes_failed = 0
    error_messages = []

    for volume in volumes:
        logger.info("Volume: %s", volume.path)
        volume_had_errors = False
        volumes_processed += 1

        retention = config.get_effective_retention(volume)
        logger.info(
            "  Retention: min=%s, hourly=%d, daily=%d, weekly=%d, monthly=%d",
            retention.min,
            retention.hourly,
            retention.daily,
            retention.weekly,
            retention.monthly,
        )

        # Build endpoint kwargs
        endpoint_kwargs = {
            "snap_prefix": volume.snapshot_prefix or f"{os.uname()[1]}-",
            "convert_rw": False,
            "subvolume_sync": False,
            "btrfs_debug": False,
            "fs_checks": True,
        }

        prefix = volume.snapshot_prefix or f"{os.uname()[1]}-"

        # Prune source snapshots
        try:
            source_path = Path(volume.path).resolve()

            snapshot_dir = Path(volume.snapshot_dir)
            if not snapshot_dir.is_absolute():
                # Relative snapshot_dir: relative to source volume
                full_snapshot_dir = (source_path / snapshot_dir).resolve()
            else:
                # Absolute snapshot_dir: add source name as subdirectory
                full_snapshot_dir = (snapshot_dir / source_path.name).resolve()

            if not full_snapshot_dir.exists():
                logger.info("  No snapshot directory found")
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

            snapshots = source_endpoint.list_snapshots()
            logger.info("  Source: %d snapshots", len(snapshots))

            if snapshots:
                # Apply time-based retention
                to_keep, to_delete = apply_retention(
                    snapshots,
                    retention,
                    get_name=lambda s: s.get_name(),
                    prefix=prefix,
                )

                logger.info("  Keeping %d, deleting %d", len(to_keep), len(to_delete))

                if to_delete:
                    if dry_run:
                        for snap in to_delete:
                            logger.info("    Would delete: %s", snap.get_name())
                        total_deleted += len(to_delete)
                    else:
                        for snap in to_delete:
                            try:
                                source_endpoint.delete_snapshots([snap])
                                logger.info("    Deleted: %s", snap.get_name())
                                total_deleted += 1
                            except Exception as e:
                                logger.error(
                                    "    Failed to delete %s: %s", snap.get_name(), e
                                )
                                error_messages.append(f"Delete {snap.get_name()}: {e}")
                                volume_had_errors = True

                total_kept += len(to_keep)

        except Exception as e:
            logger.error("  Error pruning source: %s", e)
            error_messages.append(f"Source {volume.path}: {e}")
            volume_had_errors = True

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
                logger.info("  Target %s: %d backups", target.path, len(dest_snapshots))

                if dest_snapshots:
                    # Apply time-based retention
                    to_keep, to_delete = apply_retention(
                        dest_snapshots,
                        retention,
                        get_name=lambda s: s.get_name(),
                        prefix=prefix,
                    )

                    logger.info(
                        "    Keeping %d, deleting %d", len(to_keep), len(to_delete)
                    )

                    if to_delete:
                        if dry_run:
                            for snap in to_delete:
                                logger.info("      Would delete: %s", snap.get_name())
                            total_deleted += len(to_delete)
                        else:
                            for snap in to_delete:
                                try:
                                    dest_endpoint.delete_snapshots([snap])
                                    logger.info("      Deleted: %s", snap.get_name())
                                    total_deleted += 1
                                except Exception as e:
                                    logger.error(
                                        "      Failed to delete %s: %s",
                                        snap.get_name(),
                                        e,
                                    )
                                    error_messages.append(
                                        f"Delete {snap.get_name()}: {e}"
                                    )
                                    volume_had_errors = True

                    total_kept += len(to_keep)

            except Exception as e:
                logger.error("  Error pruning target %s: %s", target.path, e)
                error_messages.append(f"Target {target.path}: {e}")
                volume_had_errors = True

        # Track volume failure
        if volume_had_errors:
            volumes_failed += 1

    end_time = time.time()
    duration = end_time - start_time
    logger.info(__util__.log_heading(f"Finished at {time.ctime()}"))

    if dry_run:
        logger.info("Dry run: would delete %d, keep %d", total_deleted, total_kept)
    else:
        logger.info("Deleted %d snapshot(s), kept %d", total_deleted, total_kept)

    # Send notifications if configured (not for dry runs)
    if not dry_run:
        _send_prune_notifications(
            config,
            volumes_processed=volumes_processed,
            volumes_failed=volumes_failed,
            snapshots_pruned=total_deleted,
            duration_seconds=duration,
            errors=error_messages,
        )

    if error_messages:
        logger.warning("Encountered %d error(s)", len(error_messages))
        return 1

    return 0


def _send_prune_notifications(
    config: Config,
    volumes_processed: int,
    volumes_failed: int,
    snapshots_pruned: int,
    duration_seconds: float,
    errors: list[str],
) -> None:
    """Send prune completion notifications if configured."""
    notif_config = config.global_config.notifications
    if not notif_config.is_enabled():
        return

    # Determine overall status
    if volumes_failed == 0:
        status = "success"
    elif volumes_failed == volumes_processed:
        status = "failure"
    else:
        status = "partial"

    # Create notification event
    event = create_prune_event(
        status=status,
        volumes_processed=volumes_processed,
        volumes_failed=volumes_failed,
        snapshots_pruned=snapshots_pruned,
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
