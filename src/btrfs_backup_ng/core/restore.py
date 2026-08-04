"""Core restore operations: restore snapshots from backup locations.

Enables pulling snapshots from backup storage (SSH or local) back to local systems
for disaster recovery, migration, or backup verification.
"""

import json
import logging
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from .. import __util__
from ..__util__ import Snapshot
from ..transaction import log_transaction
from . import progress as progress_utils
from .operations import _list_snapper_backups_at_destination, send_snapshot

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

        # Verify the restore (can be skipped for snapper restores that rename the subvolume)
        if not options.get("skip_verify", False):
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
            # Correspondence-based parent (received_uuid for btrfs, name for raw)
            parent, _local_match = find_parent_by_correspondence(
                backup_snapshots, local_snapshots, snap, backup_endpoint
            )
            if not parent:
                # Degrade to time-based parent finding when no correspondent exists
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

        # Find parent for incremental restore
        # First try UUID-based matching (most reliable for cross-filesystem)
        # Then fall back to name/time-based matching
        parent = None
        if not no_incremental:
            # Correspondence-based parent (received_uuid for btrfs, name for raw)
            parent, _local_match = find_parent_by_correspondence(
                backup_snapshots, restored_snapshots, snap, backup_endpoint
            )
            if parent:
                logger.debug("Found corresponding parent: %s", parent.get_name())
            else:
                # Degrade to time-based parent finding when no correspondent exists
                parent = snap.find_parent(restored_snapshots)
                if parent:
                    logger.debug("Found time-based parent: %s", parent.get_name())

            # The ``btrfs send -p <parent>`` diff is computed ON THE BACKUP SIDE (the
            # REMOTE host for an ssh:// source, the local backup dir otherwise). A parent
            # picked from the locally-restored copies carries a path that is meaningless
            # there -- e.g. a local ``/mnt/restore/snap-0`` handed to a REMOTE send, which
            # always fails. Remap the chosen parent to the BACKUP snapshot of the same
            # name (which carries the correct send-side path). If it is not present on the
            # backup side, drop to a full send rather than send a bogus parent path. The
            # parent is still guaranteed present LOCALLY because it was chosen from the
            # already-restored set, so ``btrfs receive`` can apply the incremental.
            if parent is not None:
                parent = next(
                    (b for b in backup_snapshots if b.get_name() == parent.get_name()),
                    None,
                )

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


def find_parent_by_correspondence(
    backup_snapshots: list,
    local_snapshots: list,
    target_backup,
    backup_endpoint,
) -> tuple:
    """Find a backup to use as the incremental parent for restoring ``target_backup``.

    A valid restore parent must be present on BOTH sides: on the backup side (its path feeds
    ``btrfs send -p``) AND locally (so ``btrfs receive`` can apply the diff onto it). That is
    exactly ``backup_endpoint.correspondent_of(local_snap)`` -- the ONE polymorphic
    correspondence primitive the backup planner uses (R4): ``received_uuid == uuid`` for
    btrfs, name for raw. So restore no longer re-implements uuid matching with its own
    ``btrfs subvolume show`` parsing (which blocked on a password via bare ``sudo`` and, on a
    raw target, silently failed because a stream file is not a subvolume -- forcing raw
    restore onto the time-based fallback). Correspondence gives btrfs restore the identical
    result (Phase 1 proved the equivalence) and raw restore proper name correspondence.

    Args:
        backup_snapshots: All snapshots at the backup location.
        local_snapshots: Snapshots that exist locally (already-restored copies).
        target_backup: The backup being restored (never its own parent).
        backup_endpoint: Endpoint where backups are stored; queried via correspondent_of.

    Returns:
        Tuple of (parent_backup, matching_local_snapshot) or (None, None). Never raises
        (the correspondent_of contract). When None, the caller degrades to the time-based
        ``Snapshot.find_parent`` fallback.
    """
    # A backup is a valid parent iff a local snapshot corresponds to it (present on both
    # sides). Map each locally-present backup (by name) to its local correspondent.
    local_by_backup_name: dict = {}
    for local_snap in local_snapshots:
        backup = backup_endpoint.correspondent_of(local_snap)
        if backup is not None:
            local_by_backup_name[backup.get_name()] = local_snap

    if not local_by_backup_name:
        logger.debug(
            "No corresponding local snapshot found for restore parent matching"
        )
        return None, None

    # Preserve the prior selection order: the first backup (in backup_snapshots order,
    # excluding the target) that is present locally becomes the incremental parent.
    for backup in backup_snapshots:
        if backup == target_backup:
            continue
        local_match = local_by_backup_name.get(backup.get_name())
        if local_match is not None:
            logger.debug(
                "Restore parent by correspondence: backup %s <-> local %s",
                backup.get_name(),
                local_match.get_name(),
            )
            return backup, local_match

    return None, None


# =============================================================================
# Snapper-specific Restore Operations
# =============================================================================


def list_snapper_backups(
    backup_path: str,
    endpoint_options: dict | None = None,
) -> list[dict]:
    """List snapper backups at a backup location.

    Looks for backups in the snapper directory structure:
        {backup_path}/.snapshots/{num}/snapshot
        {backup_path}/.snapshots/{num}/info.xml

    Args:
        backup_path: Path to backup location

    Returns:
        List of dicts with backup info:
        [
            {
                'number': 558,
                'snapshot_path': Path to snapshot subvolume,
                'info_xml_path': Path to info.xml,
                'metadata': SnapperMetadata or None,
            },
            ...
        ]
    """
    # R11: raw:// and raw+ssh:// snapper backups are flat btrfs-send streams plus a
    # {name}.snapper-meta.json sidecar -- there is no .snapshots/{num}/info.xml layout
    # to scan. Dispatch those to a sidecar-based enumeration so they can be listed AND
    # restored (Part 2), instead of silently returning [] as the .snapshots scan did.
    if str(backup_path).startswith(("raw://", "raw+ssh://")):
        return _list_raw_snapper_backups(backup_path, endpoint_options)

    from ..snapper.metadata import parse_info_xml

    backup_base = Path(backup_path)
    snapshots_dir = backup_base / ".snapshots"
    backups: list[dict[str, Any]] = []

    if not snapshots_dir.exists():
        return backups

    for item in snapshots_dir.iterdir():
        if item.is_dir() and item.name.isdigit():
            snapshot_path = item / "snapshot"
            info_xml_path = item / "info.xml"

            if snapshot_path.exists():
                backup_info = {
                    "number": int(item.name),
                    "snapshot_path": snapshot_path,
                    "info_xml_path": info_xml_path if info_xml_path.exists() else None,
                }

                # Parse info.xml if available
                if info_xml_path.exists():
                    try:
                        metadata = parse_info_xml(info_xml_path)
                        backup_info["metadata"] = metadata
                    except Exception as e:
                        logger.debug(
                            "Could not parse info.xml for %s: %s", item.name, e
                        )
                        backup_info["metadata"] = None
                else:
                    backup_info["metadata"] = None

                backups.append(backup_info)

    # Sort by number
    backups.sort(key=lambda b: int(str(b["number"])))
    return backups


def _load_snapper_sidecar(endpoint: Any, name: str) -> Any:
    """Load a ``{name}.snapper-meta.json`` sidecar into a BackupMetadata.

    Local ``raw://`` reads the file directly; ``raw+ssh://`` cat's it back over
    ssh (the argv is shlex-quoted inside ``_exec_remote_command``, so the name is
    injection-safe). Both funnel through ``BackupMetadata.from_dict`` so the file
    and stream paths cannot drift.
    """
    from ..snapper.metadata import BackupMetadata, load_backup_metadata

    filename = f"{name}.snapper-meta.json"
    dest_path = Path(endpoint.config["path"])

    if getattr(endpoint, "_is_remote", False):
        remote_path = str(dest_path / filename)
        result = endpoint._exec_remote_command(["cat", remote_path], check=True)
        raw = result.stdout
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        return BackupMetadata.from_dict(json.loads(raw))

    return load_backup_metadata(dest_path / filename)


def _raw_endpoint_config(backup_path: str, endpoint_options: dict | None) -> dict:
    """Build the common_config for a raw:// / raw+ssh:// restore-side endpoint.

    ``endpoint_options`` carries the CLI's ssh options (ssh_sudo / ssh_key /
    ssh_auth_sock / ssh_host_key_policy) so a raw+ssh target WRITTEN with --ssh-sudo
    (root-owned remote dir/streams) is READ BACK with the same options -- otherwise
    the remote ls/cat run without sudo and the backups enumerate as empty. It also
    carries the decryption options (gpg_keyring / openssl_cipher) so an encrypted
    raw snapper backup can be decoded on restore.
    """
    config: dict[str, Any] = {"path": backup_path, "snap_prefix": ""}
    if endpoint_options:
        config.update(endpoint_options)
    return config


def _list_raw_snapper_backups(
    backup_path: str, endpoint_options: dict | None = None
) -> list[dict]:
    """Enumerate snapper backups at a ``raw://`` / ``raw+ssh://`` target.

    Raw snapper backups have no ``.snapshots/{num}/info.xml`` layout, so the
    snapper number and metadata are recovered from each ``{name}.snapper-meta.json``
    sidecar (reusing the proven ``_list_snapper_backups_at_destination`` name scan,
    local and remote). Returns the same dict shape the btrfs path returns
    (``number``/``metadata``/``snapshot_path``/``info_xml_path``) plus ``raw`` and
    ``backup_name`` so Part 2 (materialization) can resolve the stream by name.
    """
    from ..endpoint import choose_endpoint

    endpoint = choose_endpoint(
        backup_path, _raw_endpoint_config(backup_path, endpoint_options)
    )

    backups: list[dict[str, Any]] = []
    for name in _list_snapper_backups_at_destination(endpoint):
        try:
            backup_meta = _load_snapper_sidecar(endpoint, name)
            metadata = backup_meta.to_snapper_metadata()
        except Exception as e:
            logger.warning(
                "Skipping raw snapper backup %r (unreadable sidecar): %s", name, e
            )
            continue

        backups.append(
            {
                "number": backup_meta.snapper_number,
                "snapshot_path": None,
                "info_xml_path": None,
                "metadata": metadata,
                "raw": True,
                "backup_name": name,
            }
        )

    # Sort by number, then by backup_name (which embeds the date) so that duplicate
    # snapper numbers -- which snapper reuses after a prune -- have a STABLE, visible
    # order rather than the arbitrary order of the underlying set.
    backups.sort(key=lambda b: (int(str(b["number"])), str(b["backup_name"])))
    return backups


def _resolve_raw_snapper_backup(
    backup_path: str,
    backup_number: int,
    endpoint_options: dict | None = None,
    backup_name: str | None = None,
) -> tuple[Any, Any, Any]:
    """Resolve a raw snapper backup to (endpoint, RawSnapshot, BackupMetadata).

    Two resolution modes:

    * ``backup_name`` given (EXACT) -- resolve that specific sidecar by its unique
      name. This is what the CLI threads once it has enumerated the exact backup the
      user selected (``--snapshot`` after collision-dedup, ``--backup-name``,
      ``--date``), so restore materializes precisely that backup rather than
      re-guessing by number.
    * ``backup_name`` None (FALLBACK, bare-number API callers) -- match by
      ``snapper_number``; on a collision (snapper reuses numbers after a prune, and
      raw targets accumulate streams), pick the NEWEST by snapper_date and warn.

    The matched name maps to a RawSnapshot by ``.name`` (the shared cross-type backup
    identity). One endpoint is built and reused for the lookup and the later ``send()``.

    Raises RestoreError if the requested backup has no sidecar or its stream is missing.
    """
    from ..endpoint import choose_endpoint

    endpoint = choose_endpoint(
        backup_path, _raw_endpoint_config(backup_path, endpoint_options)
    )

    if backup_name is not None:
        # EXACT: load this specific sidecar by its unique name (no number guessing).
        try:
            match_meta = _load_snapper_sidecar(endpoint, backup_name)
        except Exception as e:
            raise RestoreError(
                f"Snapper backup {backup_name!r} not found at {backup_path}: {e}"
            ) from e
        match_name = backup_name
    else:
        # FALLBACK: collect ALL sidecars matching this snapper number, then pick the
        # newest on a collision. Iterating a sorted list keeps the choice deterministic
        # (the underlying set order is salted -> would otherwise be nondeterministic).
        matches: list[tuple[str, Any]] = []
        for name in sorted(_list_snapper_backups_at_destination(endpoint)):
            try:
                meta = _load_snapper_sidecar(endpoint, name)
            except Exception as e:
                logger.warning(
                    "Skipping raw snapper backup %r (unreadable sidecar): %s", name, e
                )
                continue
            if meta.snapper_number == backup_number:
                matches.append((name, meta))

        if not matches:
            raise RestoreError(
                f"Snapper backup number {backup_number} not found at {backup_path}"
            )

        if len(matches) > 1:
            # Deterministic: newest snapper_date wins (name breaks a date tie).
            matches.sort(key=lambda nm: (nm[1].snapper_date, nm[0]), reverse=True)
            logger.warning(
                "Multiple raw snapper backups share number %d at %s: %s. Restoring the "
                "newest (%s, %s); use --backup-name or --date to restore an older copy.",
                backup_number,
                backup_path,
                ", ".join(f"{n} ({m.snapper_date})" for n, m in matches),
                matches[0][0],
                matches[0][1].snapper_date,
            )

        match_name, match_meta = matches[0]

    raw_snapshot = next(
        (s for s in endpoint.list_snapshots() if s.name == match_name), None
    )
    if raw_snapshot is None:
        raise RestoreError(
            f"Raw stream for snapper backup {match_name!r} not found at {backup_path} "
            "(its .snapper-meta.json sidecar exists but the btrfs-send stream is missing)"
        )

    return endpoint, raw_snapshot, match_meta


def restore_snapper_snapshot(
    backup_path: str,
    backup_number: int,
    snapper_config_name: str,
    parent_backup_number: int | None = None,
    options: dict | None = None,
    dry_run: bool = False,
    endpoint_options: dict | None = None,
    backup_name: str | None = None,
) -> tuple[int, Path]:
    """Restore a snapper backup to local snapper format.

    Restores from:
        {backup_path}/.snapshots/{backup_number}/snapshot
    To local snapper:
        {snapper_snapshots_dir}/{new_number}/snapshot

    Uses Rich progress bar for transfers.

    Args:
        backup_path: Base path of backup (e.g., /backup/home)
        backup_number: Snapshot number to restore from backup
        snapper_config_name: Local snapper config to restore to
        parent_backup_number: Parent snapshot number for incremental restore
        options: Transfer options
        dry_run: Show what would be done without doing it

    Returns:
        Tuple of (new snapshot number, path to restored snapshot)

    Raises:
        RestoreError: If restore fails
    """
    import os
    import shutil

    from ..snapper import SnapperScanner
    from ..snapper.metadata import (
        SnapperMetadata,
        generate_info_xml,
        parse_info_xml,
        renumber_info_xml,
    )

    if options is None:
        options = {}

    show_progress = options.get("show_progress", True)

    # Find the snapper config
    scanner = SnapperScanner()
    local_config = scanner.get_config(snapper_config_name)
    if local_config is None:
        raise RestoreError(f"Local snapper config not found: {snapper_config_name}")

    # Backup paths / source resolution. Raw targets have no .snapshots/{n}/snapshot
    # subvolume -- the source is a stored btrfs-send stream resolved via the sidecars
    # (Seam 1). btrfs targets keep the existing subvolume-path check.
    backup_base = Path(backup_path)
    is_raw = str(backup_path).startswith(("raw://", "raw+ssh://"))

    raw_endpoint = None
    raw_snapshot = None
    raw_backup_meta = None
    backup_snapshot_path: Path | None = None
    backup_info_xml: Path | None = None

    if is_raw:
        raw_endpoint, raw_snapshot, raw_backup_meta = _resolve_raw_snapper_backup(
            backup_path, backup_number, endpoint_options, backup_name
        )
        source_desc = raw_snapshot.name
    else:
        backup_snapshot_dir = backup_base / ".snapshots" / str(backup_number)
        backup_snapshot_path = backup_snapshot_dir / "snapshot"
        backup_info_xml = backup_snapshot_dir / "info.xml"
        if not backup_snapshot_path.exists():
            raise RestoreError(f"Backup snapshot not found: {backup_snapshot_path}")
        source_desc = str(backup_snapshot_path)

    # Get next available snapshot number for restore
    next_num = scanner.get_next_snapshot_number(local_config)

    # Local destination paths
    dest_snapshot_dir = local_config.snapshots_dir / str(next_num)
    dest_snapshot_path = dest_snapshot_dir / "snapshot"

    # Parent path for incremental. Not applicable to raw: the stored stream already
    # encodes whatever it encodes (full, or incremental against a parent matched by
    # received_uuid, which the oldest-first restore loop lands just before this one) --
    # RawEndpoint.send replays it verbatim, there is no btrfs-send `-p` to add here.
    parent_path = None
    if not is_raw and parent_backup_number:
        parent_path = (
            backup_base / ".snapshots" / str(parent_backup_number) / "snapshot"
        )
        if not parent_path.exists():
            logger.warning(
                "Parent snapshot %d not found, falling back to full restore",
                parent_backup_number,
            )
            parent_path = None
    elif is_raw and parent_backup_number:
        logger.debug(
            "Raw source: ignoring parent %d (stream is self-contained)",
            parent_backup_number,
        )

    if parent_path:
        logger.info(
            "Restoring snapshot %d -> %d (incremental from %d) ...",
            backup_number,
            next_num,
            parent_backup_number,
        )
    else:
        logger.info("Restoring snapshot %d -> %d (full) ...", backup_number, next_num)

    if dry_run:
        logger.info("Dry run - would restore as snapshot %d", next_num)
        return next_num, Path("/dev/null")

    transfer_start = time.monotonic()

    log_transaction(
        action="snapper_restore",
        status="started",
        source=source_desc,
        destination=str(dest_snapshot_path),
        snapshot=str(backup_number),
        parent=str(parent_backup_number) if parent_backup_number else None,
    )

    # Label for send-side failures: a raw stream's decode pipeline is not `btrfs send`.
    send_label = "raw stream decode" if is_raw else "btrfs send"

    try:
        # Create destination directory
        if os.geteuid() != 0:
            subprocess.run(
                ["sudo", "mkdir", "-p", str(dest_snapshot_dir)],
                check=True,
                capture_output=True,
            )
        else:
            dest_snapshot_dir.mkdir(parents=True, exist_ok=True)

        # Build btrfs receive command (shared: the raw stream's embedded subvolume is
        # named "snapshot" -- the backup source was .snapshots/{n}/snapshot -- so
        # receive lands dest_snapshot_dir/snapshot exactly like the btrfs path).
        receive_cmd = ["btrfs", "receive", str(dest_snapshot_dir)]
        if os.geteuid() != 0:
            receive_cmd = ["sudo"] + receive_cmd
        logger.debug("Receive command: %s", " ".join(receive_cmd))

        # Seam 2 (send side) + Seam 3 (progress estimate).
        estimated_size = None
        if is_raw:
            # RawEndpoint.send returns a Popen whose stdout is the verified,
            # decrypted/decompressed btrfs-send stream -- a drop-in for the send
            # Popen below. Its integrity check fires INSIDE send() before any bytes
            # are received, so a corrupt stream aborts before touching the slot. No
            # size estimate: raw .size is the on-disk (compressed) size, not the
            # decoded stream size, so a progress total would mislead (spinner only).
            assert raw_endpoint is not None and raw_snapshot is not None
            send_process = raw_endpoint.send(raw_snapshot)
        else:
            send_cmd = ["btrfs", "send"]
            if parent_path:
                send_cmd.extend(["-p", str(parent_path)])
            send_cmd.append(str(backup_snapshot_path))
            if os.geteuid() != 0:
                send_cmd = ["sudo"] + send_cmd
            logger.debug("Send command: %s", " ".join(send_cmd))
            if show_progress and not parent_path:
                estimated_size = progress_utils.estimate_snapshot_size(
                    str(backup_snapshot_path),
                    str(parent_path) if parent_path else None,
                )
            send_process = subprocess.Popen(
                send_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

        # Use Rich progress for local transfers
        use_rich_progress = show_progress and progress_utils.is_interactive()

        if use_rich_progress:
            receive_process = subprocess.Popen(
                receive_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            send_rc, receive_rc = progress_utils.run_transfer_with_progress(
                send_process=send_process,
                receive_process=receive_process,
                snapshot_name=f"snapshot {backup_number}",
                estimated_size=estimated_size,
            )

            if send_rc != 0:
                raise RestoreError(f"{send_label} failed with code {send_rc}")
            if receive_rc != 0:
                raise RestoreError(f"btrfs receive failed with code {receive_rc}")
        else:
            # Simple pipe without progress
            receive_process = subprocess.Popen(
                receive_cmd,
                stdin=send_process.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            if send_process.stdout:
                send_process.stdout.close()

            receive_stdout, receive_stderr = receive_process.communicate()
            send_process.wait()

            if send_process.returncode != 0:
                raise RestoreError(
                    f"{send_label} failed with code {send_process.returncode}"
                )
            if receive_process.returncode != 0:
                raise RestoreError(
                    f"btrfs receive failed: {receive_stderr.decode().strip()}"
                )

        # btrfs receive creates "snapshot" subvolume, which is exactly what we want
        # No rename needed since snapper expects .snapshots/{num}/snapshot

        # Copy or generate info.xml
        dest_info_xml = dest_snapshot_dir / "info.xml"
        if is_raw:
            # Seam 4: build info.xml for the fresh slot. Raw targets have no
            # .snapshots/{n}/info.xml to copy, so prefer RENUMBERING the sidecar's
            # stored original_info_xml (snapper's OWN xml) in place -- only <num>
            # changes; userdata (incl. multi-entry), <uid>, and any element we do not
            # model are preserved VERBATIM. Fall back to regenerating from the parsed
            # fields only when no original was stored or it will not parse (older
            # sidecars that also stored a mis-parsed userdata dict still round-trip
            # correctly this way, since the original xml is snapper's authoritative copy).
            assert raw_backup_meta is not None
            xml_content = None
            if raw_backup_meta.original_info_xml:
                try:
                    xml_content = renumber_info_xml(
                        raw_backup_meta.original_info_xml, next_num
                    )
                except Exception as e:
                    logger.warning(
                        "Could not renumber stored info.xml for backup %d (%s); "
                        "regenerating from sidecar fields",
                        backup_number,
                        e,
                    )
            if xml_content is None:
                metadata = raw_backup_meta.to_snapper_metadata()
                metadata.num = next_num
                xml_content = generate_info_xml(metadata)
        elif backup_info_xml is not None and backup_info_xml.exists():
            # Copy original info.xml but update the number
            try:
                metadata = parse_info_xml(backup_info_xml)
                metadata.num = next_num
                xml_content = generate_info_xml(metadata)
            except Exception as e:
                logger.warning("Could not parse backup info.xml, generating new: %s", e)
                from datetime import datetime

                metadata = SnapperMetadata(
                    type="single",
                    num=next_num,
                    date=datetime.now(),
                    description=f"Restored from backup {backup_number}",
                    cleanup="",
                )
                xml_content = generate_info_xml(metadata)
        else:
            # Generate new info.xml
            from datetime import datetime

            metadata = SnapperMetadata(
                type="single",
                num=next_num,
                date=datetime.now(),
                description=f"Restored from backup {backup_number}",
                cleanup="",
            )
            xml_content = generate_info_xml(metadata)

        # Write info.xml
        #
        # Slot dir is 0755 to match what snapper NATIVELY creates for .snapshots/{N}
        # (verified against snapper 0.13: native slot dirs are 0755, only the parent
        # .snapshots is 0750). 0750 here would be stricter than snapper and can block
        # non-root snapper access under ALLOW_USERS/ALLOW_GROUPS/SYNC_ACL; the parent
        # .snapshots (0750, root+group) still gates who can reach the slot at all.
        if os.geteuid() != 0:
            subprocess.run(
                ["sudo", "tee", str(dest_info_xml)],
                input=xml_content.encode(),
                check=True,
                capture_output=True,
            )
            subprocess.run(
                ["sudo", "chmod", "755", str(dest_snapshot_dir)],
                check=True,
                capture_output=True,
            )
        else:
            dest_info_xml.write_text(xml_content)
            dest_snapshot_dir.chmod(0o755)

        duration = time.monotonic() - transfer_start

        log_transaction(
            action="snapper_restore",
            status="completed",
            source=source_desc,
            destination=str(dest_snapshot_path),
            snapshot=str(backup_number),
            parent=str(parent_backup_number) if parent_backup_number else None,
            duration_seconds=duration,
        )

        logger.info(
            "Restored snapshot %d -> %d successfully (%.1fs)",
            backup_number,
            next_num,
            duration,
        )
        return next_num, dest_snapshot_path

    except Exception as e:
        duration = time.monotonic() - transfer_start
        log_transaction(
            action="snapper_restore",
            status="failed",
            source=source_desc,
            destination=str(dest_snapshot_path),
            snapshot=str(backup_number),
            parent=str(parent_backup_number) if parent_backup_number else None,
            duration_seconds=duration,
            error=str(e),
        )

        # Clean up partial restore
        try:
            if dest_snapshot_path.exists():
                if os.geteuid() != 0:
                    subprocess.run(
                        [
                            "sudo",
                            "btrfs",
                            "property",
                            "set",
                            "-f",
                            str(dest_snapshot_path),
                            "ro",
                            "false",
                        ],
                        capture_output=True,
                    )
                    subprocess.run(
                        [
                            "sudo",
                            "btrfs",
                            "subvolume",
                            "delete",
                            str(dest_snapshot_path),
                        ],
                        capture_output=True,
                    )
                else:
                    subprocess.run(
                        [
                            "btrfs",
                            "property",
                            "set",
                            "-f",
                            str(dest_snapshot_path),
                            "ro",
                            "false",
                        ],
                        capture_output=True,
                    )
                    __util__.delete_subvolume(dest_snapshot_path)  # type: ignore[attr-defined]
            if dest_snapshot_dir.exists():
                if os.geteuid() != 0:
                    subprocess.run(
                        ["sudo", "rm", "-rf", str(dest_snapshot_dir)],
                        capture_output=True,
                    )
                else:
                    shutil.rmtree(dest_snapshot_dir)
        except Exception as cleanup_e:
            logger.warning("Cleanup failed: %s", cleanup_e)

        logger.error("Failed to restore snapshot %d: %s", backup_number, e)
        raise RestoreError(f"Failed to restore snapshot {backup_number}: {e}") from e
