"""Prune command: Apply retention policies."""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
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
from ..retention import RetentionError, apply_retention, parse_duration
from .common import (
    assert_target_mounted,
    get_log_level,
    get_timestamp_format,
    thread_ssh_target_config,
)

logger = logging.getLogger(__name__)


def is_degenerate_policy(retention: Any) -> bool:
    """A retention policy that would keep only the LATEST snapshot: no periodic buckets AND a
    near-zero ``min`` (<= 1 day). Such a policy is almost always a misconfiguration (fat-fingered
    or all-zeroed config) that would prune essentially all history, so both the ``prune`` command
    and the ``run`` pipeline refuse it (``prune`` unless ``--force``; ``run`` always, since an
    intentional degenerate prune is an explicit ``prune --force``). A legitimate short-window
    policy (e.g. ``min="30d"`` with zeroed buckets) is NOT degenerate -- it keeps a time window."""
    # A count policy keeps a definite number of snapshots, so it is a real policy
    # even with every bucket at zero -- refusing it would block exactly the
    # configuration the count form exists for.
    if getattr(retention, "keep", 0) > 0:
        return False
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


def snapper_backup_timestamp(backup: dict) -> Any:
    """The creation time of a snapper DESTINATION backup, or None if unknown.

    Snapper backups are numbered slots -- ``.snapshots/558`` -- so there is no
    timestamp in the name to parse. The date lives in the slot's ``info.xml``
    (or, for raw targets, in the sidecar), which the enumeration has already
    read into ``metadata``.

    None means the same thing an unparseable name means to ``apply_retention``:
    quarantine the backup and keep it. A backup whose date cannot be established
    must never be selected for deletion.
    """
    metadata = backup.get("metadata")
    date = getattr(metadata, "date", None)
    return date if isinstance(date, datetime) else None


def plan_snapper_retention(
    backup_path: str,
    retention: Any,
    endpoint_options: dict | None = None,
    now: Any = None,
) -> tuple[list, list]:
    """The retention PLAN for snapper backups at one destination: ``(keep, delete)``.

    The snapper twin of ``plan_endpoint_retention``, and deliberately built on the
    same ``apply_retention`` engine so the two can never drift on what "keep 7
    daily" means. Two things differ, and only two:

    * Enumeration goes through ``list_snapper_backups``, the same scheme-aware
      function ``snapper restore --list`` uses, so what retention reasons about
      is exactly what a restore would find -- local, ssh://, raw:// and
      raw+ssh:// alike.
    * Timestamps come from ``info.xml`` rather than the name, because the name is
      a slot number. Without that, every backup fails name parsing, is
      quarantined, and is kept -- retention would report success having deleted
      nothing, which is a worse answer than not running.

    This plans only. Nothing here deletes, and the caller decides what to do with
    the plan.

    Raises ``RetentionError`` on an invalid ``min`` (fail-closed: the caller
    prunes nothing), and propagates an enumeration failure rather than treating a
    location it could not read as an empty one.
    """
    from ..core.restore import list_snapper_backups

    backups = list_snapper_backups(backup_path, endpoint_options)
    if not backups:
        return [], []
    return apply_retention(
        backups,
        retention,
        get_name=lambda b: f"slot {b.get('number')}",
        now=now,
        get_timestamp=snapper_backup_timestamp,
    )


def _delete_snapper_slot_btrfs(endpoint: Any, slot_dir: str, remote: bool) -> None:
    """Remove one ``.snapshots/{n}`` slot from a btrfs destination.

    Two artifacts, two privilege regimes, and the order matters:

    1. ``btrfs subvolume delete {slot}/snapshot`` reclaims the SPACE, which is
       the entire point of retention. btrfs is the one binary the documented
       sudoers elevates, so this works even against a root-owned destination.
    2. Removing the slot directory (which holds info.xml) is NOT a btrfs
       operation, so ``--ssh-sudo`` does not cover it and ``sudo rm`` is refused
       under the documented policy -- measured. It succeeds when the connecting
       user owns the slot, which is the normal case.

    The subvolume goes first: if step 1 fails there is still data in the slot and
    removing info.xml would strand it. If step 2 fails after step 1 succeeded,
    the space is already reclaimed and an empty slot directory remains -- that is
    reported, not raised, because a leftover directory is litter while a failed
    retention is a full disk. The enumeration skips a slot with no published
    snapshot, so the leftover cannot present as a restorable backup.
    """
    subvolume = f"{slot_dir}/snapshot"
    if remote:
        result = endpoint._exec_remote_command(
            ["btrfs", "subvolume", "delete", subvolume],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            stderr = (result.stderr or b"").decode(errors="replace").strip()
            raise RetentionError(f"could not delete {subvolume}: {stderr}")
        cleanup = endpoint._exec_remote_command(
            ["rm", "-rf", slot_dir],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if cleanup.returncode != 0:
            stderr = (cleanup.stderr or b"").decode(errors="replace").strip()
            logger.warning(
                "Deleted the backup at %s but could not remove the now-empty slot "
                "directory (%s). The space is reclaimed; the empty directory "
                "remains and is ignored by enumeration. Removing it needs write "
                "access to the parent -- --ssh-sudo elevates only btrfs.",
                subvolume,
                stderr or f"rm exited {cleanup.returncode}",
            )
        return

    argv = ["btrfs", "subvolume", "delete", subvolume]
    if os.geteuid() != 0:
        argv = ["sudo", "-n", *argv]
    result = subprocess.run(argv, capture_output=True)
    if result.returncode != 0:
        stderr = (result.stderr or b"").decode(errors="replace").strip()
        raise RetentionError(f"could not delete {subvolume}: {stderr}")
    try:
        __util__.privileged_rmtree(slot_dir)
    except Exception as e:  # noqa: BLE001 - the space is already reclaimed
        logger.warning(
            "Deleted the backup at %s but could not remove the now-empty slot "
            "directory: %s. The space is reclaimed and enumeration ignores the "
            "leftover.",
            subvolume,
            e,
        )


def delete_snapper_backups(
    backup_path: str, backups: list, endpoint_options: dict | None = None
) -> tuple[int, list[str]]:
    """Delete snapper destination backups. Returns ``(deleted_count, errors)``.

    The snapper twin of ``execute_retention_deletes``, and like it a per-backup
    failure is recorded rather than raised, so one bad delete does not abort the
    rest of the pass.

    Raw targets are routed through ``RawEndpoint.delete_snapshots``, deliberately
    rather than removing files here: that path already holds the per-target lock
    (so a delete cannot race a concurrent backup) and enforces the chain guard
    that refuses to orphan an incremental child. A second implementation would
    have neither. btrfs targets have no such existing primitive, which is what
    ``_delete_snapper_slot_btrfs`` supplies.
    """
    if not backups:
        return 0, []

    from ..core.target import TargetKind, parse_target
    from ..endpoint import choose_endpoint

    scheme = parse_target(backup_path)
    config: dict[str, Any] = {"path": backup_path, "snap_prefix": ""}
    if endpoint_options:
        config.update(endpoint_options)
    endpoint_obj = choose_endpoint(backup_path, config)

    deleted = 0
    errors: list[str] = []

    if scheme.is_raw:
        by_name = {s.get_name(): s for s in endpoint_obj.list_snapshots()}
        wanted = []
        for backup in backups:
            name = backup.get("backup_name")
            if name in by_name:
                wanted.append(by_name[name])
            else:
                errors.append(
                    f"Delete slot {backup.get('number')}: no raw stream named "
                    f"{name!r} at the destination"
                )
        if wanted:
            try:
                endpoint_obj.delete_snapshots(
                    wanted, delete_session={s.get_name() for s in wanted}
                )
                deleted += len(wanted)
            except Exception as e:  # noqa: BLE001 - record, do not abort
                errors.append(f"Delete raw snapper backups: {e}")
        return deleted, errors

    base = str(endpoint_obj.config["path"]).rstrip("/")
    remote = scheme.kind is TargetKind.SSH
    for backup in backups:
        slot_dir = f"{base}/.snapshots/{backup.get('number')}"
        try:
            _delete_snapper_slot_btrfs(endpoint_obj, slot_dir, remote)
            deleted += 1
        except Exception as e:  # noqa: BLE001 - record and continue
            errors.append(f"Delete slot {backup.get('number')}: {e}")
    return deleted, errors


def format_snapper_retention_plan(
    backup_path: str, to_keep: list, to_delete: list
) -> str:
    """Render a snapper retention plan for a human, newest first.

    Reports the KEEP side too, not only the deletions. A plan that lists what it
    would remove and stays silent about what survives cannot be checked by the
    person approving it.
    """
    lines = [f"Snapper retention plan for {backup_path}:", ""]

    def render(title: str, backups: list) -> None:
        lines.append(f"  {title}: {len(backups)}")
        for backup in sorted(
            backups,
            key=lambda b: snapper_backup_timestamp(b) or datetime.min,
            reverse=True,
        ):
            stamp = snapper_backup_timestamp(backup)
            when = stamp.strftime("%Y-%m-%d %H:%M:%S") if stamp else "date unknown"
            metadata = backup.get("metadata")
            description = getattr(metadata, "description", "") or ""
            suffix = f"  {description}" if description else ""
            lines.append(f"    slot {backup.get('number')}  {when}{suffix}")
        lines.append("")

    render("Keep", to_keep)
    render("Would delete", to_delete)
    return "\n".join(lines).rstrip() + "\n"


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


def _log_retention(label: str, retention: Any) -> None:
    """Log one endpoint's resolved policy.

    A count-based policy has no meaningful bucket values, so printing the buckets
    for one reports a policy the endpoint is not being pruned under.
    """
    if getattr(retention, "keep", 0) > 0:
        logger.info(
            "%s: keep=%d (at least), min=%s", label, retention.keep, retention.min
        )
    else:
        logger.info(
            "%s: min=%s, hourly=%d, daily=%d, weekly=%d, monthly=%d, yearly=%d",
            label,
            retention.min,
            retention.hourly,
            retention.daily,
            retention.weekly,
            retention.monthly,
            retention.yearly,
        )


def _refuse_degenerate(
    label: str, retention: Any, force: bool, error_messages: list[str]
) -> bool:
    """Return True if this endpoint must not be pruned under ``retention``.

    The guardrail is per endpoint rather than per volume: the policies now differ
    between the source and each target, so a degenerate one must stop that
    endpoint alone instead of cancelling the whole volume.
    """
    if not is_degenerate_policy(retention) or force:
        return False
    logger.error(
        "  Refusing to prune %s: retention keeps ONLY the latest snapshot "
        "(all buckets 0, no keep count, min=%s). Re-run with --force if this is "
        "intended.",
        label,
        retention.min,
    )
    error_messages.append(f"Refused (degenerate policy, no --force): {label}")
    return True


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

        # Retention is resolved per endpoint, the same way the run pipeline
        # resolves it: source_retention for the source, each target's own policy
        # for that target. Resolving a single policy per volume here made `prune`
        # delete snapshots that `run` keeps, from the same config and with no
        # warning -- the two commands share plan_endpoint_retention precisely so
        # they cannot disagree about what a policy means.
        source_retention = config.get_source_retention(volume)
        _log_retention("  Retention (source)", source_retention)

        # Degenerate-policy guardrail: refuse to prune an endpoint whose policy keeps only the
        # latest snapshot, unless --force -- enforced even non-interactively (the fat-fingered-
        # config-nukes-all-history backstop). Does not change pruning semantics, only gates them.
        source_refused = _refuse_degenerate(
            f"source {volume.path}", source_retention, force, error_messages
        )

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

        # A refused source policy stops the source only. The targets below have
        # their own policies and are still pruned under them.
        if source_refused:
            volume_had_errors = True
        else:
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
                    # Skip the SOURCE only. `continue` here skipped this
                    # volume's TARGETS as well, so a volume whose snapshot
                    # directory had been removed left its backups unpruned
                    # forever while `prune` still reported success.
                    logger.info("  No snapshot directory found; skipping source")
                else:
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
                        source_endpoint,
                        source_retention,
                        prefix,
                        get_timestamp_format(config),
                    )
                    logger.info(
                        "  Keeping %d, deleting %d", len(to_keep), len(to_delete)
                    )
                    total_kept += len(to_keep)
                    if to_delete:
                        plan.append(
                            (source_endpoint, to_delete, f"source {volume.path}")
                        )

            except Exception as e:
                logger.error("  Error pruning source: %s", e)
                error_messages.append(f"Source {volume.path}: {e}")
                volume_had_errors = True

        # Prune target backups
        for target in volume.targets:
            target_retention = config.get_target_retention(volume, target)
            _log_retention(f"    Retention (target {target.path})", target_retention)
            if _refuse_degenerate(
                f"target {target.path}", target_retention, force, error_messages
            ):
                volume_had_errors = True
                continue
            try:
                # The same mount gate the other commands use. Without it `prune`
                # was the one command that would happily operate on the empty
                # mount-point directory of an absent drive -- listing the real
                # backups as gone and, on a target whose policy is count-based,
                # deleting whatever a previous unguarded run had written to the
                # root filesystem there. A refused target is skipped, not fatal:
                # the other targets and the source still prune.
                assert_target_mounted(target.path, target.require_mount)

                dest_kwargs = dict(endpoint_kwargs)
                thread_ssh_target_config(dest_kwargs, target)

                dest_endpoint = endpoint.choose_endpoint(
                    target.path,
                    dest_kwargs,
                    source=False,
                )
                dest_endpoint.prepare()

                to_keep, to_delete = plan_endpoint_retention(
                    dest_endpoint,
                    target_retention,
                    prefix,
                    get_timestamp_format(config),
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
