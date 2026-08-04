"""Prune command: Apply retention policies."""

import argparse
import logging
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

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
from ..retention import apply_retention, parse_duration
from .common import get_log_level, get_timestamp_format, thread_ssh_target_config

logger = logging.getLogger(__name__)


def is_degenerate_policy(retention: Any) -> bool:
    """A retention policy that would keep only the LATEST snapshot: no periodic buckets AND a
    near-zero ``min`` (<= 1 day). Such a policy is almost always a misconfiguration (fat-fingered
    or all-zeroed config) that would prune essentially all history, so both the ``prune`` command
    and the ``run`` pipeline refuse it (``prune`` unless ``--force``; ``run`` always, since an
    intentional degenerate prune is an explicit ``prune --force``). A legitimate short-window
    policy (e.g. ``min="30d"`` with zeroed buckets) is NOT degenerate -- it keeps a time window."""
    if any(
        c > 0
        for c in (
            retention.hourly,
            retention.daily,
            retention.weekly,
            retention.monthly,
            retention.yearly,
        )
    ):
        return False
    try:
        return parse_duration(retention.min) <= timedelta(days=1)
    except ValueError:
        return False  # invalid min is rejected at config load / fails closed in apply_retention


def plan_endpoint_retention(
    endpoint_obj: Any, retention: Any, prefix: str, timestamp_format: str | None
) -> tuple[list, list]:
    """The shared, deterministic retention PLAN for one endpoint: list its snapshots, apply the
    time-based policy, then protect incremental parents. Returns ``(to_keep, to_delete)``. Raises
    ``RetentionError`` on an invalid ``min`` (fail-closed -- the caller prunes nothing and errors).
    Both the ``prune`` command and the ``run`` pipeline use this, so they can never diverge."""
    snapshots = endpoint_obj.list_snapshots()
    if not snapshots:
        return [], []
    to_keep, to_delete = apply_retention(
        snapshots,
        retention,
        get_name=lambda s: s.get_name(),
        prefix=prefix,
        timestamp_format=timestamp_format,
    )
    # Never prune a snapshot a kept one still needs as an incremental parent (no-op for btrfs;
    # protects raw stream chains from becoming unrestorable).
    return endpoint_obj.protect_incremental_parents(to_keep, to_delete)


def execute_retention_deletes(
    endpoint_obj: Any, to_delete: list
) -> tuple[int, list[str]]:
    """Delete ``to_delete`` on one endpoint, passing the full batch as the delete-session (so the
    chain guard never mistakes a whole-chain delete for orphaning). Returns
    ``(deleted_count, error_messages)`` -- a per-snapshot failure is recorded, not raised, so one
    bad delete does not abort the rest."""
    if not to_delete:
        return 0, []
    delete_session = {s.get_name() for s in to_delete}
    deleted = 0
    errors: list[str] = []
    for snap in to_delete:
        try:
            endpoint_obj.delete_snapshots([snap], delete_session=delete_session)
            deleted += 1
        except Exception as e:  # noqa: BLE001 - record and continue
            errors.append(f"Delete {snap.get_name()}: {e}")
    return deleted, errors


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
    error_messages: list[str] = []
    force = getattr(args, "force", False)
    # PLAN pass: collect every deletion as (endpoint, [snapshots], label) WITHOUT touching
    # anything, so the whole prune is shown and confirmed once before any delete happens.
    plan: list[tuple[Any, list, str]] = []

    for volume in volumes:
        logger.info("Volume: %s", volume.path)
        volume_had_errors = False
        volumes_processed += 1

        retention = config.get_effective_retention(volume)
        logger.info(
            "  Retention: min=%s, hourly=%d, daily=%d, weekly=%d, monthly=%d, yearly=%d",
            retention.min,
            retention.hourly,
            retention.daily,
            retention.weekly,
            retention.monthly,
            retention.yearly,
        )

        # Degenerate-policy guardrail: refuse to prune a volume whose policy keeps only the
        # latest snapshot, unless --force -- enforced even non-interactively (the fat-fingered-
        # config-nukes-all-history backstop). Does not change pruning semantics, only gates them.
        if is_degenerate_policy(retention) and not force:
            logger.error(
                "  Refusing to prune %s: retention keeps ONLY the latest snapshot "
                "(all buckets 0, min=%s). Re-run with --force if this is intended.",
                volume.path,
                retention.min,
            )
            error_messages.append(
                f"Refused (degenerate policy, no --force): {volume.path}"
            )
            volumes_failed += 1
            continue

        # Build endpoint kwargs
        endpoint_kwargs = {
            "snap_prefix": volume.snapshot_prefix,
            "convert_rw": False,
            "subvolume_sync": False,
            "btrfs_debug": False,
            "fs_checks": "auto",
            "timestamp_format": get_timestamp_format(config),
        }

        prefix = volume.snapshot_prefix

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

            to_keep, to_delete = plan_endpoint_retention(
                source_endpoint, retention, prefix, get_timestamp_format(config)
            )
            logger.info("  Keeping %d, deleting %d", len(to_keep), len(to_delete))
            total_kept += len(to_keep)
            if to_delete:
                plan.append((source_endpoint, to_delete, f"source {volume.path}"))

        except Exception as e:
            logger.error("  Error pruning source: %s", e)
            error_messages.append(f"Source {volume.path}: {e}")
            volume_had_errors = True

        # Prune target backups
        for target in volume.targets:
            try:
                dest_kwargs = dict(endpoint_kwargs)
                thread_ssh_target_config(dest_kwargs, target)

                dest_endpoint = endpoint.choose_endpoint(
                    target.path,
                    dest_kwargs,
                    source=False,
                )
                dest_endpoint.prepare()

                to_keep, to_delete = plan_endpoint_retention(
                    dest_endpoint, retention, prefix, get_timestamp_format(config)
                )
                logger.info("    Keeping %d, deleting %d", len(to_keep), len(to_delete))
                total_kept += len(to_keep)
                if to_delete:
                    plan.append((dest_endpoint, to_delete, f"target {target.path}"))

            except Exception as e:
                logger.error("  Error pruning target %s: %s", target.path, e)
                error_messages.append(f"Target {target.path}: {e}")
                volume_had_errors = True

        # Track volume failure
        if volume_had_errors:
            volumes_failed += 1

    # ---- AGGREGATE + CONFIRM + EXECUTE ----
    total_to_delete = sum(len(td) for _, td, _ in plan)
    if dry_run:
        for _ep, to_delete, label in plan:
            for snap in to_delete:
                logger.info("  Would delete (%s): %s", label, snap.get_name())
        total_deleted = total_to_delete
    elif total_to_delete == 0:
        logger.info("Nothing to prune")
    else:
        # A single confirmation before ANY deletion. An interactive TTY prompts unless --yes;
        # a non-interactive run (cron) proceeds without prompting -- a degenerate policy was
        # already refused above unless --force, so cron cannot silently mass-delete.
        proceed = getattr(args, "yes", False) or not sys.stdin.isatty()
        if not proceed:
            print(f"About to delete {total_to_delete} snapshot(s)/backup(s):")
            for _ep, to_delete, label in plan:
                print(f"  {label} -- {len(to_delete)}:")
                for snap in to_delete:
                    print(f"    - {snap.get_name()}")
            print(f"Proceed with deleting {total_to_delete}? [y/N] ", end="")
            proceed = input().strip().lower() in ("y", "yes")
        if not proceed:
            logger.info("Aborted; nothing deleted.")
        else:
            for ep, to_delete, label in plan:
                deleted, errs = execute_retention_deletes(ep, to_delete)
                total_deleted += deleted
                logger.info("  Deleted %d (%s)", deleted, label)
                for err in errs:
                    logger.error("  %s", err)
                error_messages.extend(errs)

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
    status: Literal["success", "failure", "partial"]
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
