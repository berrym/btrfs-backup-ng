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
from .operations import (
    _list_snapper_backups_at_destination,
    _verify_destination_space,
    send_snapshot,
)
from .space import check_space_availability, format_space_check
from .target import TargetKind, parse_target

logger = logging.getLogger(__name__)


class RestoreError(Exception):
    """Error during restore operation."""

    pass


def get_restore_chain(
    target_snapshot: Snapshot,
    all_backup_snapshots: list[Snapshot],
    existing_local: list[Snapshot],
    overwrite: bool = False,
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

        # If this snapshot already exists locally, we can stop.
        # It can serve as the incremental base.
        #
        # Unless it IS the snapshot the caller asked to replace. --overwrite sets
        # skip_existing False, but that filter runs LATER and this truncation ran
        # first, which is why --overwrite did nothing at all.
        #
        # Exempting only the TARGET, not every ancestor. Suppressing the
        # truncation wholesale meant `--overwrite --snapshot snap-3` returned the
        # entire history -- snap-0, snap-1, snap-2, snap-3 -- and deleted three
        # subvolumes the operator never named, while the consent they gave spoke
        # of one. An ancestor that is already present is exactly what an
        # incremental base is; replacing it is neither asked for nor useful.
        replaceable = overwrite and current_name == target_snapshot.get_name()
        if current_name in existing_names and not replaceable:
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
    if not __util__.is_btrfs(path):
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
        True if a snapshot of that name is already at the destination.

    Raises:
        RestoreError: if the destination could not be read at all.

    A check that could not run used to return False -- "no collision" -- and a
    caller acting on that would receive onto a name that already exists, which
    is the failure this function is meant to prevent. Not knowing is not the
    same as knowing there is nothing there, so it now says which one it is and
    lets the caller decide.
    """
    try:
        existing = destination_endpoint.list_snapshots(flush_cache=True)
    except Exception as e:
        raise RestoreError(
            f"Could not read the destination to check whether {snapshot_name!r} "
            f"is already there ({e}). Refusing to assume it is not."
        ) from e
    return any(snap.get_name() == snapshot_name for snap in existing)


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
        if not __util__.is_subvolume(snapshot_path):
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


class _ReplaceFailed(RestoreError):
    """A failed replacement, carrying whether the old copy was already removed.

    The advisory tells an operator their copy is gone. That must be true when it
    is said, AND said when it is true -- someone not told goes looking for a copy
    that no longer exists.
    """

    def __init__(self, message: str, removed: bool):
        super().__init__(message)
        self.removed = removed


def _replacement_size(snapshot) -> int:
    """Bytes the destination must hold for ``snapshot``, or 0 for unknown.

    ``RawSnapshot.size`` is the size of the STREAM FILE -- compressed and
    possibly encrypted -- not of the subvolume the destination will hold. Using
    it directly turned a check that never ran into a check that runs against a
    number which is systematically too small, so it reports enough room, the
    existing copy is deleted, and the transfer then runs out of space. That is
    worse than not checking: the old behaviour at least did not claim anything.

    A compressed stream has no reliable expansion factor, so no factor is
    invented here. Where the recorded size cannot be trusted to represent the
    destination footprint, this returns 0 -- "unknown" -- and the caller says
    the check was skipped instead of acting on a wrong number.
    """
    size = int(getattr(snapshot, "size", 0) or 0)
    if size <= 0:
        return 0
    compress = getattr(snapshot, "compress", None)
    if compress and str(compress).lower() not in ("none", "no", ""):
        logger.debug(
            "%s is stored compressed (%s), so its %d-byte stream does not give "
            "the space the destination needs; treating the size as unknown.",
            snapshot.get_name(),
            compress,
            size,
        )
        return 0
    if getattr(snapshot, "encrypt", None):
        return 0
    return size


def _replace_at_destination(local_endpoint, snapshot_name: str, estimated_bytes: int):
    """Remove ``snapshot_name`` from the destination so it can be received again.

    There is no atomic swap available here. A received subvolume is read-only
    and cannot be renamed OR moved -- verified against real btrfs, where even a
    move to a different parent under the same name fails with EROFS. The snapper
    path works around this by renaming the *containing directory*, which only
    works because its layout puts each snapshot in its own directory; a restore
    destination holds the subvolume directly, so there is nothing to rename
    around it.

    So the old copy is removed and the new one received in its place, which
    leaves a window where neither exists. That window costs a retry rather than
    data: a restore reads the backup and never writes to it, so the authoritative
    copy is untouched and re-running finishes the job. It would be a different
    proposition during a backup, where the destination may be the only other copy.

    Where the size of the replacement is known, free space is checked BEFORE
    anything is deleted, so a destination that cannot hold it is left exactly as
    it was. ``RawSnapshot`` records a stream size; a btrfs ``Snapshot`` carries
    none, and obtaining one costs a full ``btrfs send`` pass -- so for btrfs the
    check is skipped and SAID to be skipped. Claiming a check that did not run
    is worse than not running it.

    Deleting first also frees the old copy's space, so a like-for-like
    replacement needs little or none: the check matters when the replacement is
    substantially larger than what it replaces.
    """
    if estimated_bytes <= 0:
        logger.info(
            "No size is known for %s, so free space was NOT checked before "
            "replacing it. The receive will fail if the destination fills up.",
            snapshot_name,
        )
    if estimated_bytes > 0:
        try:
            space_info = local_endpoint.get_space_info()
            check = check_space_availability(space_info, estimated_bytes)
        except Exception as e:  # noqa: BLE001 - see below
            # Not knowing is not the same as knowing there is room, but refusing
            # to replace because the free space could not be read would make
            # --overwrite unusable wherever the check is unavailable. It says so
            # and continues; the receive still fails safely if space runs out.
            logger.warning(
                "Could not check free space before replacing %s (%s). Continuing; "
                "the receive will fail if the destination fills up.",
                snapshot_name,
                e,
            )
        else:
            if not check.sufficient:
                raise RestoreError(
                    f"Not enough room to replace {snapshot_name}: "
                    f"{format_space_check(check)}. Nothing has been deleted."
                )

    logger.info("Removing the existing %s to replace it", snapshot_name)
    removed = False
    try:
        existing = [
            s
            for s in local_endpoint.list_snapshots(flush_cache=True)
            if s.get_name() == snapshot_name
        ]
        if existing:
            local_endpoint.delete_snapshots(existing)
            # Recorded BEFORE the confirmation below. Once the delete is issued
            # the copy may be gone whatever happens next, and if the confirming
            # listing itself fails the caller still has to be able to say so.
            removed = True
    except Exception as e:
        raise _ReplaceFailed(
            f"Could not remove the existing {snapshot_name} at the destination "
            f"({e}). It has been left in place.",
            removed=removed,
        ) from e

    # delete_snapshots does NOT raise when it declines: it logs and moves on for
    # a locked snapshot, a refused sudo, an incremental parent it will not orphan.
    # Trusting its return would let the whole snapshot stream out and fail at the
    # very end with "creating subvolume ... failed: File exists" -- the exact
    # outcome this function exists to prevent. So the destination is asked again.
    #
    # flush_cache because Endpoint.delete_snapshots drops the entry from its own
    # memo even when the delete FAILED (endpoint/common.py). A cached read would
    # therefore report the snapshot gone after a failure -- a false clean, and
    # precisely the answer this confirmation exists to catch.
    try:
        still_there = [
            s
            for s in local_endpoint.list_snapshots(flush_cache=True)
            if s.get_name() == snapshot_name
        ]
    except Exception as e:
        # The confirmation could not run, but the delete was already issued, so
        # the copy may be gone. Say so, rather than leaving the operator to infer
        # their destination's state from a listing error.
        raise _ReplaceFailed(
            f"Could not confirm whether {snapshot_name} was removed ({e}). The "
            f"removal was already issued, so the copy may be gone. The backup is "
            f"unchanged; re-running will restore it.",
            removed=removed,
        ) from e
    if still_there:
        raise RestoreError(
            f"{snapshot_name} is still at the destination after trying to remove "
            f"it, so it cannot be replaced. It is most likely locked by another "
            f"operation, held as the incremental parent of a snapshot that is "
            f"staying, or the removal was refused for lack of privilege. Nothing "
            f"has been transferred and the destination is unchanged."
        )
    return removed


def restore_snapshot(
    backup_endpoint,
    local_endpoint,
    snapshot,
    parent=None,
    options: dict | None = None,
    session_id: str | None = None,
    overwrite: bool = False,
    estimated_bytes: int = 0,
    on_replaced: Callable[[str], None] | None = None,
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

    # Refuse before streaming rather than after. `btrfs receive` names the
    # subvolume it creates after the source, so receiving onto a name that is
    # already there fails with "creating subvolume ... failed: File exists" --
    # having transferred the whole snapshot first. The callers filter by name
    # already; this catches what reaches here anyway, which is how the 0.9.5
    # known issue surfaced. If the destination cannot be read at all, that is
    # raised rather than treated as "nothing is there".
    if check_snapshot_collision(snapshot_name, local_endpoint):
        if not overwrite:
            raise RestoreError(
                f"{snapshot_name} is already at the destination. Receiving onto "
                f"a name that exists fails after transferring everything, so "
                f"this stops now. Remove it first, restore to a different path, "
                f"or pass --overwrite to replace it."
            )
        # Decided here, DONE later. Everything between this point and the
        # transfer can fail -- most notably set_lock, which reaches across the
        # network and, since locks became persistent, aborts when it cannot
        # record one. Deleting here would mean an operator loses their copy to a
        # failure that never moved a byte. The removal is deferred to the last
        # moment before the stream, so the window is the transfer and nothing
        # else.
        replace_first = True
    else:
        replace_first = False

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
        # Everything that can fail without moving data has now happened: the lock
        # is held, the parent is chosen, the endpoints are ready. This is the last
        # point before the stream, and therefore the narrowest the window can be
        # made -- a received subvolume cannot be renamed or moved (EROFS on real
        # btrfs, and btrfs refuses to clear the read-only flag while received_uuid
        # is set), so replacing means removing first and no staging is available.
        #
        # INSIDE the try: locks are already held by this point, and a failure here
        # must still reach the finally that releases them. Outside it, a refused
        # delete left a persistent restore lock on the backup snapshot, which now
        # blocks retention from ever pruning it.

        if replace_first:
            # The transfer layer's OWN space check runs first. send_snapshot
            # calls _verify_destination_space and raises InsufficientSpaceError
            # -- but AFTER this deletion, so the operator's copy was destroyed
            # for a shortage the tool already had the means to detect. The
            # estimate can be slow on a large subvolume; that is the right
            # trade against deleting somebody's data.
            if options.get("check_space", True) and not options.get("force", False):
                _verify_destination_space(snapshot, local_endpoint, parent, options)
            really_removed = _replace_at_destination(
                local_endpoint, snapshot_name, estimated_bytes
            )
            if on_replaced is not None and really_removed:
                # The caller tracks what is present at the destination and
                # picks incremental parents from it. That list was taken
                # before this deletion; leaving the removed name in it lets a
                # later snapshot be sent against a parent that is gone.
                on_replaced(snapshot_name)

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
        if isinstance(e, _ReplaceFailed):
            # Re-wrapping loses the flag that says whether the destination copy
            # was already removed, and the caller needs it to decide whether to
            # tell the operator their copy is gone. Raise the original.
            raise
        raise RestoreError(f"Restore failed for {snapshot_name}: {e}")

    finally:
        # Release locks
        backup_endpoint.set_lock(snapshot, lock_id, False)
        if parent:
            backup_endpoint.set_lock(parent, lock_id, False, parent=True)


def _retry_with_inferred_prefix(backup_endpoint: Any) -> list[Any]:
    """List again under the prefix the location actually uses, if there is one.

    Returns the snapshots found, or an empty list when nothing can be inferred
    or the choice is ambiguous. Ambiguity is deliberately NOT resolved by
    picking the most populous prefix: two prefixes at one location usually means
    two different volumes backed up side by side, and restoring the wrong one is
    worse than stopping to ask.

    Inference applies ONLY when no prefix was asked for. An operator who passed
    --prefix named the set they want, and quietly listing a different one hands
    back another volume's snapshots while reporting success -- the exact harm the
    ambiguity rule above exists to prevent, arrived at from the other direction.
    A prefix that was given and matches nothing is a mismatch to report, not a
    guess to make.
    """
    config = getattr(backup_endpoint, "config", None)
    configured = (config or {}).get("snap_prefix", "") or ""
    if configured:
        logger.debug("Not inferring a prefix: %r was asked for explicitly", configured)
        return []

    discover = getattr(backup_endpoint, "prefixes_present", None)
    if not callable(discover):
        return []
    try:
        present = discover() or {}
    except Exception as e:  # noqa: BLE001 - a convenience retry must not mask the real error
        logger.debug("Could not infer a snapshot prefix: %s", e)
        return []

    # An endpoint that does not implement this (or a stand-in that returns
    # something else) must degrade to "cannot infer", not crash the restore.
    if not isinstance(present, dict) or not present:
        return []
    if len(present) > 1:
        logger.error(
            "This location holds snapshots under more than one prefix (%s), so "
            "which set to restore is ambiguous. Re-run with --prefix to choose.",
            ", ".join(f"{p!r} ({n})" for p, n in sorted(present.items())),
        )
        return []

    prefix = next(iter(present))
    logger.info(
        "No snapshots matched the configured prefix; this location uses %r, so "
        "listing again with it. Pass --prefix to select a different set.",
        prefix,
    )
    previous = backup_endpoint.config.get("snap_prefix", "")
    backup_endpoint.config["snap_prefix"] = prefix
    try:
        # flush_cache: the base endpoint memoises its listing, and the caller has
        # already listed once under the old prefix. Without this the retry gets
        # that cached (empty) result back and the whole inference is a no-op.
        found = backup_endpoint.list_snapshots(flush_cache=True)
    except TypeError:
        # An endpoint or stand-in whose list_snapshots takes no flush argument.
        found = backup_endpoint.list_snapshots()
    except Exception as e:  # noqa: BLE001 - restore the prefix, then report normally
        backup_endpoint.config["snap_prefix"] = previous
        logger.debug("Listing under inferred prefix %r failed: %s", prefix, e)
        return []
    if not found:
        backup_endpoint.config["snap_prefix"] = previous
    return found or []


def _choose_parent(backup_snapshots, present, snap, backup_endpoint):
    """The incremental parent for ``snap``, or None for a full send.

    ONE chooser, used by both the dry-run preview and the real execution. They
    each had their own copy, which is how a preview comes to describe a transfer
    the run will not perform -- the same divergence that let --list, --status and
    --interactive disagree about one location.

    ``present`` differs legitimately between the two callers: the preview passes
    what WILL be there, the execution passes what IS. The selection rules must
    not differ, and now cannot.
    """
    # Only snapshots from the SAME volume can be parents. A different prefix is
    # a different volume: no shared history, so no delta for `btrfs send -p` to
    # compute. Filtered BEFORE selection rather than after, because
    # Snapshot.__lt__ raises NotImplementedError("prefixes don't match") and
    # find_parent compares as it walks -- so a single foreign snapshot at the
    # destination aborted the whole restore before any guard downstream could
    # refuse it. That is reachable for raw sources, whose snapshots carry no
    # prefix at all.
    # Only snapshots from the SAME volume can be parents: a different prefix is
    # a different volume, so there is no shared history for `btrfs send -p` to
    # compute a delta against. Filtered BEFORE selection because
    # Snapshot.__lt__ raises NotImplementedError("prefixes don't match") and
    # find_parent compares as it walks, so one foreign snapshot at the
    # destination aborted the whole restore.
    #
    # An UNKNOWN prefix is not a mismatch. RawSnapshot defaults prefix to "" and
    # discover_raw_snapshots does not set it, so an exact-equality filter
    # discarded every raw destination snapshot and silently turned every
    # prefixed raw restore into a full transfer -- the opposite of what this
    # branch exists to do, reported as an ordinary success. Raw correspondence
    # is by NAME anyway. So a blank prefix on either side means "cannot tell",
    # and cannot-tell must not exclude.
    snap_prefix = getattr(snap, "prefix", None)

    def _same_volume(candidate) -> bool:
        other = getattr(candidate, "prefix", None)
        if other and snap_prefix:
            return other == snap_prefix
        if snap_prefix:
            # The candidate does not know its own prefix -- RawSnapshot defaults
            # it to "" and discover_raw_snapshots never sets it. Its NAME still
            # carries it, and raw correspondence is by name anyway, so ask the
            # name rather than accepting anything.
            #
            # Accepting on "cannot tell" alone was too generous: a raw stream
            # from an unrelated volume (database-2024...) was handed back as the
            # parent for a home- snapshot, which is a `btrfs send -p` against a
            # subvolume with no shared history.
            try:
                return str(candidate.get_name()).startswith(snap_prefix)
            except Exception:  # noqa: BLE001 - an unnameable candidate is unusable
                return False
        # Neither side declares a prefix. That is every raw-to-raw restore:
        # RawSnapshot defaults prefix to "" on BOTH the restored snapshot and
        # the candidates, so accepting here handed back a stream from an
        # unrelated volume as the incremental parent.
        #
        # The names still carry the volume, and __util__ already knows how to
        # recover it -- infer_snapshot_prefix splits <prefix><timestamp> using
        # the same parse every listing uses. Cannot-tell on BOTH names is the
        # only case that accepts.
        mine = __util__.infer_snapshot_prefix(str(snap.get_name()))
        try:
            theirs = __util__.infer_snapshot_prefix(str(candidate.get_name()))
        except Exception:  # noqa: BLE001 - an unnameable candidate is unusable
            return False
        if mine is None or theirs is None:
            return True
        return mine == theirs

    present = [p for p in present if _same_volume(p)]

    parent, _local_match = find_parent_by_correspondence(
        backup_snapshots, present, snap, backup_endpoint
    )
    if parent:
        logger.debug("Found corresponding parent: %s", parent.get_name())
    else:
        # Degrade to time-based parent finding when no correspondent exists.
        #
        # This can raise: Snapshot.__lt__ refuses to order across prefixes
        # (NotImplementedError "prefixes don't match") and find_parent compares
        # as it walks. The filter above deliberately keeps snapshots whose prefix
        # is UNKNOWN -- raw snapshots default to "" -- because excluding them
        # silently turned every raw restore into a full transfer. Keeping them
        # means the comparison can still meet a genuine mismatch, and a restore
        # must not abort because two snapshots could not be ordered: no parent is
        # a full send, which always works.
        try:
            parent = snap.find_parent(present)
        except (NotImplementedError, TypeError) as e:
            logger.debug(
                "Could not order the snapshots at the destination against %s "
                "(%s); sending in full rather than guessing at a parent.",
                snap.get_name(),
                e,
            )
            parent = None
        if parent:
            logger.debug("Found time-based parent: %s", parent.get_name())

    # What disqualifies a parent is being UNRELATED, not being newer.
    #
    # `btrfs send -p` computes a delta between two related subvolumes; the
    # direction is not part of its contract, and a newer snapshot from the same
    # chain is a perfectly good -- often much smaller -- parent. An earlier
    # version of this guard rejected any parent that was not strictly older,
    # which turned working incremental restores into full transfers.
    #
    # An unrelated parent is one from a different volume, which here means a
    # different prefix. Comparing across prefixes is also what Snapshot.__lt__
    # refuses to do -- it raises NotImplementedError("prefixes don't match") --
    # so this both prevents an invalid send and stops that exception aborting a
    # restore, which it did for raw sources whose snapshots carry no prefix.
    # Belt and braces: correspondence can return a snapshot from the BACKUP set,
    # which was not filtered above.
    if parent is not None:
        parent_prefix = getattr(parent, "prefix", None)
        if parent_prefix and snap_prefix and parent_prefix != snap_prefix:
            logger.debug(
                "Not using %s as a parent for %s: it belongs to %r, not %r, so "
                "there is no shared history to send a delta against.",
                parent.get_name(),
                snap.get_name(),
                parent_prefix,
                snap_prefix,
            )
            parent = None
    return parent


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
        # Before giving up: a listing filters on `snap_prefix` and then requires
        # the rest of each name to parse as a timestamp, so the wrong prefix --
        # or none, which is the default -- discards every real snapshot and the
        # location reports as empty. The endpoint can already say which prefixes
        # ARE there; it said so in a message telling the operator to re-run with
        # --prefix. Doing that for them is the whole fix, and when it is
        # ambiguous the answer is an error naming the choices, never a guess.
        backup_snapshots = _retry_with_inferred_prefix(backup_endpoint)

    if not backup_snapshots:
        # A restore that restored nothing is a FAILED restore. This returned
        # stats with failed=0, which the CLI turns into exit status 0, so
        # `btrfs-backup-ng restore ... && echo ok` printed ok having recovered
        # nothing at all -- measured against a real remote, where a prefix
        # mismatch produced exactly this. Listing legitimately finding an empty
        # location is still a failure HERE: the caller asked for data back.
        detail = ""
        describe = getattr(backup_endpoint, "describe_empty_listing", None)
        if callable(describe):
            try:
                detail = describe() or ""
            except Exception:  # noqa: BLE001 - a diagnostic must not mask the error
                detail = ""
        message = "No snapshots found at the backup location; nothing was restored"
        logger.error(message)
        if detail:
            for line in str(detail).splitlines():
                logger.error("%s", line)
        stats["failed"] = 1
        stats["errors"].append(message if not detail else f"{message}. {detail}")
        return stats

    logger.info("Found %d snapshot(s) at backup location", len(backup_snapshots))

    # Read the destination under the SAME prefix the source ended up being read
    # under. _prepare_local_endpoint's docstring already required this -- "the
    # local endpoint must parse already-restored subvolumes under the SAME
    # prefix, or it fails to recognize them and the restore chain re-restores an
    # existing parent" -- and it held while the prefix came from --prefix. It
    # stopped holding once the prefix could be INFERRED: inference updates the
    # source endpoint, the destination was built earlier from an empty --prefix,
    # and nothing carried the answer across. The destination then listed as
    # empty, so a snapshot sitting right there was invisible and got re-sent
    # onto its own name: "creating subvolume ... failed: File exists".
    #
    # Copied UNCONDITIONALLY rather than inside the inference branch above.
    # `restore --interactive` lists (and therefore infers) before it ever calls
    # this function, so by the time we get here the source listing succeeds on
    # the first attempt and that branch never runs -- while the prefix it left
    # behind is exactly the one the destination needs. Keying off "did we infer
    # just now" would fix the plain path and quietly leave -i broken.
    #
    # Only a real string is copied. Callers in tests hand in doubles whose
    # config.get returns a mock, and a mock reaching str.startswith raises
    # inside the listing rather than here, where the cause would be obvious.
    _source_prefix = backup_endpoint.config.get("snap_prefix", "")
    if isinstance(_source_prefix, str):
        logger.debug(
            "Reading the destination under the source's prefix %r", _source_prefix
        )
        local_endpoint.config["snap_prefix"] = _source_prefix

    # List existing local snapshots. flush_cache because the prefix may have just
    # changed: a listing memoised under the old one would return the stale empty
    # set, which is the very failure being fixed and produces no error of its own.
    local_snapshots = local_endpoint.list_snapshots(flush_cache=True)
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
        chain = get_restore_chain(
            target, backup_snapshots, local_snapshots, overwrite=not skip_existing
        )
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
        # Say WHY there is nothing to do. "No snapshots need to be restored" is
        # true for several different situations and distinguishes none of them,
        # so an operator who named a snapshot and got it back could not tell
        # whether it was already present, filtered out, or never found. The
        # answer is not a failure -- asking for a snapshot that is already at
        # the destination is a satisfied request, and a restore script that
        # re-runs after success must keep succeeding -- but it has to be said.
        already_present = [t.get_name() for t in targets if t.get_name() in local_names]
        if already_present:
            stats["skipped"] += len(already_present)
            logger.info(
                "Already at the destination, so nothing to do: %s",
                ", ".join(already_present),
            )
        else:
            logger.info("Already at the destination, so nothing to do: everything")
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
            # The same chooser the real run uses, so the preview cannot promise a
            # transfer that will not happen. `present` is what WILL be at the
            # destination by the time this snapshot is reached.
            parent = _choose_parent(
                backup_snapshots,
                [s for s in to_restore if s != snap] + local_snapshots,
                snap,
                backup_endpoint,
            )
            if not skip_existing and snap.get_name() in local_names:
                logger.info(
                    "  [%d/%d] Would DELETE and replace: %s",
                    i,
                    len(to_restore),
                    snap.get_name(),
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
    replaced_names: set[str] = set()

    def _forget_replaced(name: str) -> None:
        """Record a replacement, and drop it from the present-set.

        Two things depend on knowing this actually happened. Parents are chosen
        from ``restored_snapshots``, which was taken before any replacement, so
        leaving a removed name in it lets a later snapshot be sent against a
        parent that is no longer at the destination. And the failure advisory
        tells the operator their copy was deleted -- which must be true when it
        is said.
        """
        replaced_names.add(name)
        restored_snapshots[:] = [s for s in restored_snapshots if s.get_name() != name]

    for i, snap in enumerate(to_restore, 1):
        snap_name = snap.get_name()

        if on_progress:
            on_progress(i, len(to_restore), snap_name)

        # Find parent for incremental restore
        # First try UUID-based matching (most reliable for cross-filesystem)
        # Then fall back to name/time-based matching
        parent = None
        if not no_incremental:
            parent = _choose_parent(
                backup_snapshots, restored_snapshots, snap, backup_endpoint
            )

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
                overwrite=not skip_existing,
                estimated_bytes=_replacement_size(snap),
                on_replaced=_forget_replaced,
            )
            stats["restored"] += 1
            restored_snapshots.append(snap)

        except (RestoreError, __util__.AbortError) as e:
            # A replacement that failed AFTER removing the old copy must still be
            # recorded, or the operator is not told their copy is gone -- the
            # same defect as claiming one that is not.
            if isinstance(e, _ReplaceFailed) and e.removed:
                replaced_names.add(snap_name)
            logger.error("Failed to restore %s: %s", snap_name, e)
            if snap_name in replaced_names:
                # Only when the old copy was ACTUALLY removed. Printing this for
                # every --overwrite failure told operators their copy was gone
                # after failures that never touched it -- including the case
                # where the message just above says it was left in place.
                # Reporting a destruction that did not happen is the same defect
                # as hiding one that did.
                logger.error(
                    "  --overwrite removed the previous %s before this transfer, "
                    "so the destination now holds neither copy. The backup was "
                    "only read from and is unchanged -- run the same command "
                    "again to finish restoring it.",
                    snap_name,
                )
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

    # A btrfs target reached over ssh has the SAME .snapshots/{num} layout as a
    # local one, but Path("ssh://host:/p") is a nonexistent LOCAL path: the scan
    # below stat'd it, missed, and returned [] without ever opening a connection.
    # `snapper restore --list ssh://...` therefore reported "No snapper backups
    # found" -- exit 0 -- for a destination holding backups, which is the README's
    # flagship disaster-recovery walkthrough.
    if str(backup_path).startswith("ssh://"):
        return _list_remote_snapper_backups(backup_path, endpoint_options)

    from ..snapper.metadata import parse_info_xml

    backup_base = Path(backup_path)
    snapshots_dir = backup_base / ".snapshots"
    backups: list[dict[str, Any]] = []

    if not snapshots_dir.exists():
        # Same rule as the ssh:// and raw:// branches: a location that cannot be
        # enumerated is an error, not an empty result. Both callers are
        # restore-side, where the source is something the operator has told us
        # holds backups -- a mistyped path must not come back as "no backups".
        raise RuntimeError(
            f"Cannot list snapper backups at {backup_path}: {snapshots_dir} does "
            "not exist. The location could NOT be enumerated -- this is NOT an "
            "empty target. Check the path is correct and that it holds a snapper "
            "backup layout (.snapshots/<number>/snapshot)."
        )

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


def _restore_endpoint_config(backup_path: str, endpoint_options: dict | None) -> dict:
    """Build the common_config for a restore-side endpoint, whatever its scheme.

    ``endpoint_options`` carries the CLI's ssh options (ssh_sudo / ssh_key /
    ssh_auth_sock / ssh_host_key_policy) so a target WRITTEN with --ssh-sudo
    (root-owned remote directories and streams) is READ BACK with the same
    options -- otherwise the remote commands run unprivileged and the backups
    enumerate as empty. It also carries the decryption options (gpg_keyring /
    openssl_cipher) so an encrypted raw snapper backup can be decoded on restore.

    Named for the direction rather than for raw, because ssh:// btrfs targets
    now build their endpoint through here too and a "raw" name would suggest an
    ssh:// restore goes through raw code, which it does not.
    """
    config: dict[str, Any] = {"path": backup_path, "snap_prefix": ""}
    if endpoint_options:
        config.update(endpoint_options)
    return config


def _list_remote_snapper_backups(
    backup_path: str, endpoint_options: dict | None = None
) -> list[dict]:
    """Enumerate snapper backups at an ``ssh://`` btrfs target.

    Same ``.snapshots/{num}/snapshot`` + ``info.xml`` layout as a local btrfs
    target -- only the filesystem is remote, so the scan runs over the endpoint
    instead of over ``Path``. Returns the dict shape the local path returns.

    ``endpoint_options`` carries the CLI's ssh options for the same reason the raw
    sibling takes them: a destination written with ``--ssh-sudo`` is root-owned,
    and reading it back without them enumerates as empty.

    A listing that FAILS raises. "We could not look" must never be reported as
    "there is nothing there" -- during a restore that is the most dangerous
    moment to be wrong.
    """
    from ..endpoint import choose_endpoint
    from ..snapper.metadata import parse_info_xml_string

    endpoint = choose_endpoint(
        backup_path, _restore_endpoint_config(backup_path, endpoint_options)
    )
    base = f"{str(endpoint.config['path']).rstrip('/')}/.snapshots"

    # -maxdepth/-mindepth 1 -type d keeps this to the numbered slot dirs and does
    # not descend into the received subvolumes. stderr is NOT discarded: it is the
    # only explanation of a failure, and discarding it is what made the raw
    # equivalent of this bug invisible.
    # stdout/stderr must be requested explicitly: SSHEndpoint._exec_remote_command
    # passes kwargs straight to subprocess and captures nothing by default (unlike
    # SSHRawEndpoint). Without this the find output goes to the console and the
    # scan sees an empty result -- an empty listing produced by not looking.
    result = endpoint._exec_remote_command(
        ["find", base, "-mindepth", "1", "-maxdepth", "1", "-type", "d"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout = result.stdout or b""
    stderr = result.stderr or b""
    if isinstance(stdout, bytes):
        stdout = stdout.decode(errors="replace")
    if isinstance(stderr, bytes):
        stderr = stderr.decode(errors="replace")

    if result.returncode != 0:
        # find exits 0 whenever it finished looking, including over an empty or
        # absent-but-readable tree, so non-zero means the scan did not complete.
        # No special case for "No such file or directory". The raw path raises for
        # exactly this condition, and a mistyped path produces it just as readily
        # as a never-written destination -- telling someone "no backups" because
        # the location does not exist is the same lie in a different costume. The
        # two schemes must answer identically; see _check_remote_listing.
        raise RuntimeError(
            f"Cannot list snapper backups at {backup_path}: "
            f"{stderr.strip() or f'find exited {result.returncode}'}. The location "
            "could NOT be enumerated -- this is NOT an empty target. Check the path "
            "is correct and readable by the CONNECTING USER: on an ssh:// target "
            "--ssh-sudo elevates only btrfs, so it does not help here (measured: "
            "SSHEndpoint._build_remote_command passes find/test/cat through "
            "unchanged). Grant the user access instead, e.g. "
            "setfacl -m u:<user>:rx on the .snapshots directory."
        )

    backups: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        slot = line.strip()
        if not slot:
            continue
        name = slot.rsplit("/", 1)[-1]
        if not name.isdigit():
            # .incoming / .stale are this run's transactional temps, never backups.
            continue

        snapshot_path = f"{slot}/snapshot"
        info_xml_path = f"{slot}/info.xml"

        # The slot only counts if the received subvolume is actually there; a
        # publish that never completed must not present as a restorable backup.
        # Same probe the restore uses (_remote_dir_exists), so what lists as
        # restorable is exactly what restore_snapper_snapshot then finds.
        if not _remote_dir_exists(endpoint, snapshot_path):
            logger.debug("Skipping snapper slot %s: no published snapshot", slot)
            continue

        metadata = None
        raw_xml = _read_remote_text(endpoint, info_xml_path)
        if raw_xml is not None:
            try:
                metadata = parse_info_xml_string(raw_xml)
            except Exception as e:
                logger.debug("Could not parse remote info.xml for %s: %s", name, e)
        else:
            # Absent info.xml is a known outcome of an older backup, not an error.
            logger.debug("No info.xml in remote snapper slot %s", slot)

        backups.append(
            {
                "number": int(name),
                "snapshot_path": snapshot_path,
                "info_xml_path": info_xml_path if raw_xml is not None else None,
                "metadata": metadata,
            }
        )

    backups.sort(key=lambda b: b["number"])
    return backups


def _remote_dir_exists(endpoint: Any, path: str) -> bool:
    """True when ``path`` is a directory on the endpoint's remote host.

    ``test -d`` is the probe BOTH the enumeration and the restore use, so a slot
    that lists as restorable is the same thing the restore then reads; two
    different probes could disagree and would eventually be made to.

    Non-zero means "absent OR not reachable by the connecting user" -- ``test``
    cannot separate those, and callers must not phrase it as if it could.

    stdout/stderr are requested explicitly because
    ``SSHEndpoint._exec_remote_command`` captures nothing by default (unlike the
    raw endpoint), and an uncaptured probe writes to the console.
    """
    probe = endpoint._exec_remote_command(
        ["test", "-d", path],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return bool(probe.returncode == 0)


def _read_remote_text(endpoint: Any, path: str) -> str | None:
    """Return the contents of a remote file, or None when it could not be read.

    Absent and unreadable collapse to None on purpose: every caller treats a
    missing info.xml as a known outcome of an older backup rather than an error,
    and neither can be distinguished from ``cat``'s exit status alone.
    """
    result = endpoint._exec_remote_command(
        ["cat", path],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        return None
    data = result.stdout or b""
    if isinstance(data, bytes):
        return data.decode(errors="replace")
    return str(data)


class _RemoteSubvolume:
    """The one thing ``SSHEndpoint.send`` needs from a snapshot: ``get_path()``.

    A restore source is a subvolume at a known remote path, not a snapshot this
    process enumerated, so there is no ``__util__.Snapshot`` to hand over:
    that class derives its path from ``prefix + timestamp``, and a snapper slot
    (``.snapshots/{num}/snapshot``) does not follow that naming at all. ``send``
    calls ``_normalize_path(snapshot.get_path())`` and touches nothing else, so
    this carries exactly that rather than pretending to be a full snapshot --
    a fake Snapshot would answer ``get_name()``/``time_obj`` with fiction.
    """

    __slots__ = ("path",)

    def __init__(self, path: str) -> None:
        self.path = path

    def get_path(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return f"_RemoteSubvolume({self.path!r})"


def _resolve_remote_snapper_backup(
    backup_path: str,
    backup_number: int,
    endpoint_options: dict | None = None,
) -> tuple[Any, str, str | None]:
    """Resolve one snapper backup at an ``ssh://`` btrfs target, for restore.

    The remote layout is identical to a local btrfs target's --
    ``{path}/.snapshots/{num}/snapshot`` beside ``info.xml`` -- so this is the
    remote twin of the local branch's existence check, run over the endpoint
    because ``Path("ssh://host:/p")`` is a nonexistent LOCAL path (it collapses
    to ``ssh:/host:/p``, which is what the failure used to name).

    Returns ``(endpoint, remote_snapshot_path, info_xml_text_or_None)``.
    """
    from ..endpoint import choose_endpoint

    endpoint = choose_endpoint(
        backup_path, _restore_endpoint_config(backup_path, endpoint_options)
    )
    slot = f"{str(endpoint.config['path']).rstrip('/')}/.snapshots/{backup_number}"
    snapshot_path = f"{slot}/snapshot"

    if not _remote_dir_exists(endpoint, snapshot_path):
        raise RestoreError(
            f"Backup snapshot not readable: {snapshot_path}. The probe failed, "
            "which means the snapshot is absent OR the connecting user cannot "
            "reach it -- those are indistinguishable from here, so check both. "
            "On an ssh:// target --ssh-sudo elevates only btrfs and so does not "
            "grant access to this path; grant the user access instead, e.g. "
            "setfacl -m u:<user>:rx on the .snapshots directory."
        )

    info_xml = _read_remote_text(endpoint, f"{slot}/info.xml")
    if info_xml is None:
        logger.debug("No readable info.xml in remote snapper slot %s", slot)
    return endpoint, snapshot_path, info_xml


def _assert_raw_location_exists(endpoint: Any, backup_path: str) -> None:
    """Raise unless the raw backup location is actually present and readable.

    Restore-side only. See the call site for why this is not folded into
    _list_snapper_backups_at_destination, which the backup side needs to tolerate.
    """
    dest_path = str(endpoint.config["path"])
    if getattr(endpoint, "_is_remote", False):
        probe = endpoint._exec_remote_command(
            ["test", "-d", dest_path],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        missing = probe.returncode != 0
    else:
        missing = not Path(dest_path).is_dir()
    if missing:
        raise RuntimeError(
            f"Cannot list snapper backups at {backup_path}: {dest_path} is not a "
            "readable directory. The location could NOT be enumerated -- this is "
            "NOT an empty target. Check the path is correct and readable by the "
            "connecting user. Unlike ssh://, a raw+ssh target written with "
            "--ssh-sudo IS read back with it -- SSHRawEndpoint elevates every "
            "remote command, not just btrfs -- so pass the same option here."
        )


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
        backup_path, _restore_endpoint_config(backup_path, endpoint_options)
    )

    # _list_snapper_backups_at_destination is shared with the BACKUP side, where a
    # missing destination legitimately means "first run, nothing here yet" and is
    # tolerated. On the RESTORE side the same absence means the operator gave us a
    # path that does not hold backups, and answering "no backups" is the lie this
    # series exists to eliminate. The shared primitive keeps its semantics; the
    # distinction belongs here, where the caller's intent is known -- matching the
    # local and ssh:// branches, which raise for the same condition.
    _assert_raw_location_exists(endpoint, backup_path)

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
        backup_path, _restore_endpoint_config(backup_path, endpoint_options)
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

    from ..snapper import SnapperScanner
    from ..snapper.metadata import (
        SnapperMetadata,
        generate_info_xml,
        parse_info_xml,
        parse_info_xml_string,
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

    # Backup paths / source resolution (Seam 1). Three layouts, one per kind of
    # target:
    #   raw        -- no .snapshots/{n}/snapshot subvolume at all; the source is a
    #                 stored btrfs-send stream resolved via the sidecars.
    #   ssh://     -- the SAME layout as a local btrfs target, on the far side of a
    #                 connection, so it must be probed over the endpoint.
    #   local      -- the existing subvolume-path check, unchanged.
    #
    # The ssh:// branch is why this dispatch exists: Path("ssh://host:/p") is a
    # nonexistent LOCAL path (it collapses to "ssh:/host:/p"), so every ssh://
    # restore failed the .exists() check below and reported that mangled path as
    # a missing snapshot -- while `snapper restore --list` on the same target
    # listed the backups perfectly well.
    #
    # Classification comes from core.target, the single scheme authority, not from
    # another local startswith(): call sites each deciding "is this remote?" for
    # themselves, and disagreeing, is the family this belongs to.
    scheme = parse_target(backup_path)
    backup_base = Path(backup_path)
    is_raw = scheme.is_raw
    is_ssh = scheme.kind is TargetKind.SSH

    raw_endpoint = None
    raw_snapshot = None
    raw_backup_meta = None
    ssh_endpoint = None
    remote_snapshot_path: str | None = None
    remote_info_xml: str | None = None
    backup_snapshot_path: Path | None = None
    backup_info_xml: Path | None = None

    if is_raw:
        raw_endpoint, raw_snapshot, raw_backup_meta = _resolve_raw_snapper_backup(
            backup_path, backup_number, endpoint_options, backup_name
        )
        source_desc = raw_snapshot.name
    elif is_ssh:
        (
            ssh_endpoint,
            remote_snapshot_path,
            remote_info_xml,
        ) = _resolve_remote_snapper_backup(backup_path, backup_number, endpoint_options)
        source_desc = f"{backup_path} snapshot {backup_number}"
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
    remote_parent_path: str | None = None
    if is_ssh and parent_backup_number:
        # The parent lives on the remote too, so it is probed there. Same
        # fall-back-to-full rule as the local branch: an absent parent must
        # degrade the restore, never fail it.
        assert ssh_endpoint is not None
        candidate = (
            f"{str(ssh_endpoint.config['path']).rstrip('/')}"
            f"/.snapshots/{parent_backup_number}/snapshot"
        )
        if _remote_dir_exists(ssh_endpoint, candidate):
            remote_parent_path = candidate
        else:
            logger.warning(
                "Parent snapshot %d not found on the remote, "
                "falling back to full restore",
                parent_backup_number,
            )
    elif not is_raw and parent_backup_number:
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

    if parent_path or remote_parent_path:
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

    # Label for send-side failures: a raw stream's decode pipeline is not `btrfs
    # send`, and an ssh:// send failed on the OTHER machine -- saying which end
    # broke is most of the diagnosis.
    if is_raw:
        send_label = "raw stream decode"
    elif is_ssh:
        send_label = "remote btrfs send"
    else:
        send_label = "btrfs send"

    try:
        # Create destination directory. Direct first, sudo only if that is
        # refused: a snapper destination under SYNC_ACL is frequently writable by
        # the backup user, and the documented sudoers (NOPASSWD: /usr/bin/btrfs)
        # does not cover mkdir at all -- so the unconditional shell-out failed a
        # restore that needed no privilege whatsoever.
        __util__.privileged_mkdir(dest_snapshot_dir, allow_prompt=True)

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
        elif is_ssh:
            # SSHEndpoint.send runs `btrfs send` ON THE REMOTE and returns a Popen
            # whose stdout is the stream -- the same shape RawEndpoint.send returns,
            # so the receive below is untouched by which end the bytes came from.
            #
            # No size estimate: the endpoint's _estimate_snapshot_size runs
            # `btrfs subvolume show` LOCALLY (it exists for the backup direction,
            # where the source IS local), so aiming it at a remote path measures
            # nothing and returns None after two failed subprocesses. A missing
            # total shows a spinner; a wrong total misinforms, and the raw branch
            # above already set that precedent deliberately.
            assert ssh_endpoint is not None and remote_snapshot_path is not None
            send_process = ssh_endpoint.send(
                _RemoteSubvolume(remote_snapshot_path),
                parent=(
                    _RemoteSubvolume(remote_parent_path) if remote_parent_path else None
                ),
            )
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
        elif is_ssh:
            # Seam 4 over ssh. The remote info.xml was already read during
            # resolution, so no second connection is opened here. Prefer
            # RENUMBERING it: only <num> changes, and everything snapper wrote --
            # multi-entry userdata, <uid>, elements this project does not model --
            # survives verbatim. Parse-and-regenerate is the fallback because it
            # can only preserve the fields modelled here, and a synthesized
            # description is the last resort rather than the first.
            xml_content = None
            if remote_info_xml:
                try:
                    xml_content = renumber_info_xml(remote_info_xml, next_num)
                except Exception as e:
                    logger.warning(
                        "Could not renumber remote info.xml for backup %d (%s); "
                        "regenerating from its parsed fields",
                        backup_number,
                        e,
                    )
                if xml_content is None:
                    try:
                        metadata = parse_info_xml_string(remote_info_xml)
                        metadata.num = next_num
                        xml_content = generate_info_xml(metadata)
                    except Exception as e:
                        logger.warning(
                            "Could not parse remote info.xml for backup %d: %s",
                            backup_number,
                            e,
                        )
            if xml_content is None:
                from datetime import datetime

                metadata = SnapperMetadata(
                    type="single",
                    num=next_num,
                    date=datetime.now(),
                    description=f"Restored from backup {backup_number}",
                    cleanup="",
                )
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
        __util__.privileged_write_bytes(dest_info_xml, xml_content, allow_prompt=True)
        __util__.privileged_chmod(dest_snapshot_dir, 0o755, allow_prompt=True)

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
                    __util__.delete_subvolume(dest_snapshot_path)
            if dest_snapshot_dir.exists():
                __util__.privileged_rmtree(dest_snapshot_dir, allow_prompt=True)
        except Exception as cleanup_e:
            logger.warning("Cleanup failed: %s", cleanup_e)

        logger.error("Failed to restore snapshot %d: %s", backup_number, e)
        raise RestoreError(f"Failed to restore snapshot {backup_number}: {e}") from e
