"""Core restore operations: restore snapshots from backup locations.

Enables pulling snapshots from backup storage (SSH or local) back to local systems
for disaster recovery, migration, or backup verification.
"""

import logging
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from .. import __util__
from ..__util__ import Snapshot
from ..transaction import log_transaction
from .operations import send_snapshot

logger = logging.getLogger(__name__)


class RestoreError(Exception):
    """Error during restore operation."""

    pass


def get_restore_chain(
    target_snapshot: Snapshot,
    all_backup_snapshots: list[Snapshot],
    existing_local: list[Snapshot],
) -> list[Snapshot]:
    """Determine which snapshots need to be restored to get target_snapshot.

    For incremental restore to work, we need the complete parent chain.
    This function walks backward from the target to find all required parents.

    Args:
        target_snapshot: The snapshot the user wants to restore
        all_backup_snapshots: All snapshots available at backup location
        existing_local: Snapshots that already exist at restore destination

    Returns:
        List of snapshots in order (oldest first) that must be restored.
        If a parent exists locally, we can use it as incremental base.
    """
    # Get names of existing local snapshots for comparison
    existing_names = {s.get_name() for s in existing_local}

    chain: list[Snapshot] = []
    current: Snapshot | None = target_snapshot

    while current is not None:
        current_name = current.get_name()

        # If this snapshot already exists locally, we can stop
        # It can serve as the incremental base
        if current_name in existing_names:
            logger.debug(
                "Found existing local snapshot %s - can use as incremental base",
                current_name,
            )
            break

        # Add to chain (will be reversed at end)
        chain.insert(0, current)  # Prepend to get oldest-first order

        # Find parent: the most recent snapshot that is OLDER than current
        # We only want strictly older snapshots to avoid infinite loops
        parent = _find_older_parent(current, all_backup_snapshots)
        if parent is None:
            logger.debug(
                "Snapshot %s has no older parent - will be restored in full mode",
                current_name,
            )
        current = parent

    return chain


def _find_older_parent(snapshot, all_snapshots: list):
    """Find the most recent snapshot that is strictly older than the given snapshot.

    Unlike Snapshot.find_parent(), this only returns older snapshots and never
    falls back to returning a newer snapshot. This prevents infinite loops
    when building restore chains.

    Args:
        snapshot: The snapshot to find a parent for
        all_snapshots: All available snapshots to search

    Returns:
        The most recent snapshot older than `snapshot`, or None if none exists.
    """
    candidates = []
    for s in all_snapshots:
        # Only consider snapshots that are strictly older
        if s < snapshot:
            candidates.append(s)

    if not candidates:
        return None

    # Return the most recent (last in sorted order) of the older snapshots
    return max(candidates, key=lambda s: s.time_obj if hasattr(s, "time_obj") else 0)


def find_snapshot_by_name(name: str, snapshots: list):
    """Find a snapshot by name in a list of snapshots.

    Args:
        name: Snapshot name to find
        snapshots: List of Snapshot objects

    Returns:
        Snapshot object if found, None otherwise
    """
    for snap in snapshots:
        if snap.get_name() == name:
            return snap
    return None


def find_snapshot_before_time(
    target_time: time.struct_time,
    snapshots: list,
):
    """Find the most recent snapshot before a given time.

    Args:
        target_time: Time to search before
        snapshots: List of Snapshot objects (should be sorted)

    Returns:
        Most recent Snapshot before target_time, or None
    """
    candidates = []
    for snap in snapshots:
        if hasattr(snap, "time_obj") and snap.time_obj is not None:
            if snap.time_obj <= target_time:
                candidates.append(snap)

    if not candidates:
        return None

    # Return most recent (last in sorted order)
    return max(candidates, key=lambda s: s.time_obj)


def validate_restore_destination(
    path: Path,
    in_place: bool = False,
    force: bool = False,
) -> None:
    """Validate that destination is suitable for restore.

    Args:
        path: Destination path
        in_place: Whether this is an in-place restore (dangerous)
        force: Whether to bypass safety checks

    Raises:
        RestoreError: If destination is invalid or unsafe
    """
    path = Path(path).resolve()

    # Check path exists or can be created
    if not path.exists():
        try:
            path.mkdir(parents=True, exist_ok=True)
            logger.info("Created restore destination: %s", path)
        except OSError as e:
            raise RestoreError(f"Cannot create destination directory {path}: {e}")

    # Must be on btrfs filesystem
    if not __util__.is_btrfs(path):  # type: ignore[attr-defined]
        raise RestoreError(
            f"Destination {path} is not on a btrfs filesystem. "
            "btrfs receive requires a btrfs filesystem."
        )

    # In-place restore requires explicit confirmation
    if in_place and not force:
        raise RestoreError(
            f"In-place restore to {path} is dangerous. "
            "Use --yes-i-know-what-i-am-doing to proceed."
        )


def check_snapshot_collision(
    snapshot_name: str,
    destination_endpoint,
) -> bool:
    """Check if a snapshot with this name already exists at destination.

    Args:
        snapshot_name: Name to check
        destination_endpoint: Destination endpoint

    Returns:
        True if collision exists, False otherwise
    """
    try:
        existing = destination_endpoint.list_snapshots(flush_cache=True)
        for snap in existing:
            if snap.get_name() == snapshot_name:
                return True
        return False
    except Exception as e:
        logger.warning("Could not check for collision: %s", e)
        return False


def verify_restored_snapshot(
    destination_endpoint,
    expected_name: str,
) -> bool:
    """Verify that a snapshot was correctly restored.

    Args:
        destination_endpoint: Endpoint where snapshot was restored
        expected_name: Expected snapshot name

    Returns:
        True if verified successfully

    Raises:
        RestoreError: If verification fails
    """
    try:
        # Check directly if the snapshot path exists and is a subvolume
        # We don't rely on list_snapshots() because it filters by prefix,
        # and the restored snapshot may have a different prefix than the destination
        snapshot_path = Path(destination_endpoint.config["path"]) / expected_name

        if not snapshot_path.exists():
            raise RestoreError(
                f"Snapshot {expected_name} not found after restore. "
                "The restore may have failed silently."
            )

        # Verify it's a valid subvolume
        if not __util__.is_subvolume(snapshot_path):  # type: ignore[attr-defined]
            raise RestoreError(
                f"{snapshot_path} exists but is not a valid btrfs subvolume. "
                "The restore may have failed."
            )

        logger.debug("Verified restored snapshot: %s", expected_name)
        return True

    except RestoreError:
        raise
    except Exception as e:
        raise RestoreError(f"Verification failed: {e}")


def restore_snapshot(
    backup_endpoint,
    local_endpoint,
    snapshot,
    parent=None,
    options: dict | None = None,
    session_id: str | None = None,
) -> None:
    """Restore a single snapshot from backup to local.

    This is the core restore operation - it's essentially send_snapshot
    with source and destination swapped.

    Args:
        backup_endpoint: Endpoint where backup is stored (source for restore)
        local_endpoint: Local endpoint to receive snapshot (destination)
        snapshot: Snapshot to restore
        parent: Optional parent for incremental restore
        options: Transfer options (compress, rate_limit, show_progress)
        session_id: Unique session ID for locking
    """
    if options is None:
        options = {}

    if session_id is None:
        session_id = str(uuid.uuid4())[:8]

    snapshot_name = snapshot.get_name()
    parent_name = parent.get_name() if parent else None

    logger.info("Restoring %s ...", snapshot_name)
    if parent:
        logger.info("  Using parent: %s (incremental)", parent_name)
    else:
        logger.info("  No parent available (full restore)")

    # Set lock on backup to prevent deletion during restore
    lock_id = f"restore:{session_id}"
    backup_endpoint.set_lock(snapshot, lock_id, True)
    if parent:
        backup_endpoint.set_lock(parent, lock_id, True, parent=True)

    restore_start = time.monotonic()

    # Log transaction start
    source_path = str(backup_endpoint.config.get("path", ""))
    dest_path = str(local_endpoint.config.get("path", ""))

    log_transaction(
        action="restore",
        status="started",
        source=source_path,
        destination=dest_path,
        snapshot=snapshot_name,
        parent=parent_name,
    )

    try:
        # Use send_snapshot with swapped endpoints
        # backup_endpoint is the source (has send method)
        # local_endpoint is the destination (has receive method)
        send_snapshot(
            snapshot,
            local_endpoint,
            parent=parent,
            options=options,
        )

        # Verify the restore
        verify_restored_snapshot(local_endpoint, snapshot_name)

        duration = time.monotonic() - restore_start
        log_transaction(
            action="restore",
            status="completed",
            source=source_path,
            destination=dest_path,
            snapshot=snapshot_name,
            parent=parent_name,
            duration_seconds=duration,
        )

        logger.info("Restored %s successfully (%.1fs)", snapshot_name, duration)

    except Exception as e:
        duration = time.monotonic() - restore_start
        log_transaction(
            action="restore",
            status="failed",
            source=source_path,
            destination=dest_path,
            snapshot=snapshot_name,
            parent=parent_name,
            duration_seconds=duration,
            error=str(e),
        )
        logger.error("Failed to restore %s: %s", snapshot_name, e)
        raise RestoreError(f"Restore failed for {snapshot_name}: {e}")

    finally:
        # Release locks
        backup_endpoint.set_lock(snapshot, lock_id, False)
        if parent:
            backup_endpoint.set_lock(parent, lock_id, False, parent=True)


def restore_snapshots(
    backup_endpoint,
    local_endpoint,
    snapshot_name: str | None = None,
    before_time: time.struct_time | None = None,
    restore_all: bool = False,
    skip_existing: bool = True,
    no_incremental: bool = False,
    options: dict | None = None,
    dry_run: bool = False,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> dict:
    """Restore snapshots from backup location to local system.

    This is the main entry point for restore operations.

    Args:
        backup_endpoint: Endpoint where backups are stored
        local_endpoint: Local endpoint to restore to
        snapshot_name: Specific snapshot to restore (None = latest)
        before_time: Restore snapshot closest to this time
        restore_all: Restore all snapshots
        skip_existing: Skip snapshots that already exist locally
        no_incremental: Force full transfers (no incremental)
        options: Transfer options dict
        dry_run: Show what would be done without doing it
        on_progress: Callback for progress updates (current, total, name)

    Returns:
        Dict with restore statistics:
        {
            'restored': int,
            'skipped': int,
            'failed': int,
            'errors': list[str],
        }
    """
    if options is None:
        options = {}

    session_id = str(uuid.uuid4())[:8]
    stats: dict[str, Any] = {"restored": 0, "skipped": 0, "failed": 0, "errors": []}

    # List snapshots at backup location
    logger.info("Listing snapshots at backup location...")
    backup_snapshots = backup_endpoint.list_snapshots()

    if not backup_snapshots:
        logger.warning("No snapshots found at backup location")
        return stats

    logger.info("Found %d snapshot(s) at backup location", len(backup_snapshots))

    # List existing local snapshots
    local_snapshots = local_endpoint.list_snapshots()
    local_names = {s.get_name() for s in local_snapshots}
    logger.debug("Found %d existing local snapshot(s)", len(local_snapshots))

    # Determine which snapshots to restore
    if restore_all:
        # Restore all snapshots
        targets = backup_snapshots
        logger.info("Restoring all %d snapshots", len(targets))
    elif snapshot_name:
        # Restore specific snapshot
        target = find_snapshot_by_name(snapshot_name, backup_snapshots)
        if target is None:
            raise RestoreError(
                f"Snapshot '{snapshot_name}' not found at backup location. "
                f"Available: {[s.get_name() for s in backup_snapshots[:5]]}..."
            )
        targets = [target]
        logger.info("Restoring specific snapshot: %s", snapshot_name)
    elif before_time:
        # Restore snapshot before specific time
        target = find_snapshot_before_time(before_time, backup_snapshots)
        if target is None:
            raise RestoreError(
                "No snapshot found before the specified time. "
                f"Oldest available: {backup_snapshots[0].get_name() if backup_snapshots else 'none'}"
            )
        targets = [target]
        logger.info("Restoring snapshot before time: %s", target.get_name())
    else:
        # Restore latest snapshot
        target = backup_snapshots[-1]  # Snapshots are sorted, last is newest
        targets = [target]
        logger.info("Restoring latest snapshot: %s", target.get_name())

    # Build restore chain(s) for all targets
    all_to_restore = []
    for target in targets:
        chain = get_restore_chain(target, backup_snapshots, local_snapshots)
        for snap in chain:
            if snap not in all_to_restore:
                all_to_restore.append(snap)

    # Sort by time (oldest first for proper parent chain)
    all_to_restore.sort(key=lambda s: s.time_obj if s.time_obj else 0)

    # Filter out existing if skip_existing
    if skip_existing:
        to_restore = []
        for snap in all_to_restore:
            if snap.get_name() in local_names:
                logger.info("Skipping existing: %s", snap.get_name())
                stats["skipped"] += 1
            else:
                to_restore.append(snap)
    else:
        to_restore = all_to_restore

    if not to_restore:
        logger.info("No snapshots need to be restored")
        return stats

    # Show restore plan
    logger.info("")
    logger.info("Restore plan:")
    logger.info("  Target(s): %s", ", ".join(t.get_name() for t in targets))
    logger.info("  Chain: %s", " -> ".join(s.get_name() for s in to_restore))
    logger.info("  Total: %d snapshot(s) to restore", len(to_restore))
    logger.info("")

    if dry_run:
        logger.info("Dry run - no changes made")
        for i, snap in enumerate(to_restore, 1):
            parent = snap.find_parent(
                [s for s in to_restore if s != snap] + local_snapshots
            )
            mode = "incremental" if parent else "full"
            parent_info = f" from {parent.get_name()}" if parent else ""
            logger.info(
                "  [%d/%d] Would restore: %s (%s%s)",
                i,
                len(to_restore),
                snap.get_name(),
                mode,
                parent_info,
            )
        return stats

    # Execute restores
    restored_snapshots = list(local_snapshots)  # Track what we've restored

    for i, snap in enumerate(to_restore, 1):
        snap_name = snap.get_name()

        if on_progress:
            on_progress(i, len(to_restore), snap_name)

        # Find parent (from already-restored or existing local)
        if no_incremental:
            parent = None
        else:
            parent = snap.find_parent(restored_snapshots)

        mode = "incremental" if parent else "full"
        parent_info = f" from {parent.get_name()}" if parent else ""

        logger.info(
            "[%d/%d] Restoring %s (%s%s)",
            i,
            len(to_restore),
            snap_name,
            mode,
            parent_info,
        )

        try:
            restore_snapshot(
                backup_endpoint,
                local_endpoint,
                snap,
                parent=parent,
                options=options,
                session_id=session_id,
            )
            stats["restored"] += 1
            restored_snapshots.append(snap)

        except (RestoreError, __util__.AbortError) as e:
            logger.error("Failed to restore %s: %s", snap_name, e)
            stats["failed"] += 1
            stats["errors"].append(f"{snap_name}: {e}")

            # If this was a parent for later snapshots, we have a problem
            # Future restores in this chain will fail
            logger.warning(
                "Subsequent incremental restores may fail due to missing parent"
            )

    # Summary
    logger.info("")
    logger.info("Restore complete:")
    logger.info("  Restored: %d", stats["restored"])
    logger.info("  Skipped: %d", stats["skipped"])
    logger.info("  Failed: %d", stats["failed"])

    if stats["errors"]:
        logger.warning("Errors:")
        for err in stats["errors"]:
            logger.warning("  %s", err)

    return stats


def list_remote_snapshots(
    backup_endpoint,
    prefix_filter: str | None = None,
) -> list:
    """List snapshots available at a backup location.

    Args:
        backup_endpoint: Endpoint where backups are stored
        prefix_filter: Optional prefix to filter snapshots

    Returns:
        List of Snapshot objects
    """
    snapshots = backup_endpoint.list_snapshots()

    if prefix_filter:
        snapshots = [s for s in snapshots if s.get_name().startswith(prefix_filter)]

    return snapshots
