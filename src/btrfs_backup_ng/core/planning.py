"""Transfer planning: decide what to transfer, in what order, and with which parent.

This is the single authority for the backup transfer plan. Presence on the destination and
incremental-parent validity are decided STRICTLY by ``destination_endpoint.correspondent_of``
-- the btrfs ``received_uuid``/``uuid`` correspondence (or name, for raw targets, where the
override makes name the native identity), NEVER the on-disk name, which can collide (a
re-created snapshot reuses the name but has a new uuid). There is deliberately no name-based
fallback for btrfs: identity comes from uuids that enumeration sudo-escalates to read (see
``Endpoint._load_subvolume_ids_into``), so a missing uuid is an enrichment problem to fix at
the source, not a reason to dilute the planner back into name matching. The executor
(``core.operations._execute_transfers``) only runs the plan this module produces.
"""

import logging

logger = logging.getLogger(__name__)


def snapshots_present_on(source_snapshots, destination_endpoint):
    """Return the set of source-snapshot *names* already present on the destination.

    Presence is decided purely by correspondence -- ``received_uuid == uuid`` for btrfs,
    name for raw -- via the polymorphic ``correspondent_of`` (which never raises; a listing
    failure yields None -> absent). This is the shared presence authority used by both the
    transfer planner and the R3 lock reconcile, so the two can never disagree. A re-created
    snapshot (same name, new uuid) is correctly absent, never a name coincidence.
    """
    return {
        s.get_name()
        for s in source_snapshots
        if destination_endpoint.correspondent_of(s) is not None
    }


def plan_transfer_sequence(
    source_snapshots,
    destination_endpoint,
    *,
    no_incremental=False,
    keep_num_backups=0,
    only=None,
):
    """Build the ordered transfer plan ``[(snapshot, parent_or_None)]``.

    Args:
        source_snapshots: All snapshots at the source (each carrying its btrfs uuid).
        destination_endpoint: The destination; queried via ``correspondent_of``.
        no_incremental: If True, every snapshot is a full send (``parent=None``).
        keep_num_backups: If > 0, only consider the latest N source snapshots.
        only: If given, plan just this one snapshot (single-snapshot transfer mode).

    A snapshot whose correspondent is already present on the destination is skipped. For
    each snapshot to transfer, the parent is the newest source snapshot OLDER than it that is
    (or, within this run, will be) present on the destination by correspondence (uuid for
    btrfs, name for raw), so ``btrfs receive`` can resolve the ``send -p``; if none qualifies
    the snapshot is sent in full. Correspondence is the only presence/parent authority -- a
    snapshot the destination cannot verifiably resolve is never used as a parent.

    Within-run chaining: because the plan executes oldest-first and each transferred snapshot
    then corresponds on the destination, a snapshot may parent off an EARLIER-in-this-run
    transfer (not only snapshots already on the destination at plan time). This keeps a fresh
    multi-snapshot run (e.g. an initial snapper-history backup) a tight incremental chain
    instead of all-full sends. If an earlier transfer fails at execution, its dependent
    incremental fails too and is surfaced (R1) -- never silently mis-applied.
    """
    present = snapshots_present_on(source_snapshots, destination_endpoint)

    if only is not None:
        candidates = [only]
    elif keep_num_backups > 0:
        candidates = source_snapshots[-keep_num_backups:]
    else:
        candidates = list(source_snapshots)

    to_transfer = sorted(
        (s for s in candidates if s.get_name() not in present),
        key=lambda s: s.time_obj,
    )

    # A parent is valid if its correspondent is present on the destination -- either already
    # (``present``) or projected to be, because it is transferred earlier in THIS run.
    # ``to_transfer`` is oldest-first, so an earlier item is on the destination by the time a
    # later one sends against it.
    projected_present = set(present)

    plan = []
    for snap in to_transfer:
        parent = None
        if not no_incremental:
            older_newest_first = sorted(
                (o for o in source_snapshots if o.time_obj < snap.time_obj),
                key=lambda o: o.time_obj,
                reverse=True,
            )
            for candidate in older_newest_first:
                # A valid incremental parent must have a verified correspondent on the
                # destination (uuid for btrfs, name for raw) -- never a bare name match --
                # counting in-run transfers already planned before this one.
                if candidate.get_name() in projected_present:
                    parent = candidate
                    break
        plan.append((snap, parent))
        # This snapshot will correspond on the destination once transferred, so later
        # snapshots in this run may use it as an incremental parent.
        projected_present.add(snap.get_name())

    logger.debug(
        "Planned %d transfer(s) (%d incremental)",
        len(plan),
        sum(1 for _, p in plan if p is not None),
    )
    return plan
