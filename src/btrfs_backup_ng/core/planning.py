"""Transfer planning: decide what to transfer, in what order, and with which parent.

This is the single authority for the transfer plan, in both directions: a backup
run plans with it, and so does a restore (a transfer with the roles swapped).
Presence on the destination and incremental-parent validity are decided STRICTLY
by ``destination_endpoint.correspondent_of`` -- the btrfs
``received_uuid``/``stream_uuid`` correspondence (or name, for raw targets, where
the override makes name the native identity), NEVER the on-disk name, which can
collide (a re-created snapshot reuses the name but has a new uuid). There is
deliberately no name-based fallback for btrfs: identity comes from uuids that
enumeration sudo-escalates to read (see ``Endpoint._load_subvolume_ids_into``),
so a missing uuid is an enrichment problem to fix at the source, not a reason to
dilute the planner back into name matching. The executor
(``core.operations._execute_transfers``) only runs the plan this module produces.
"""

import logging
from typing import Any, Optional

from .. import __util__

logger = logging.getLogger(__name__)


class PlanningError(__util__.AbortError):
    """A plan that cannot be honoured, refused before a byte moves.

    Raised when a selection names a snapshot whose required parent is neither
    at the destination (by correspondence) nor obtainable from the source. The
    alternative -- streaming the increment and letting the receive fail on the
    missing parent -- transfers everything first and leaves a partial behind.
    """


def snapshots_present_on(source_snapshots, destination_endpoint):
    """Return the set of source-snapshot *names* already present on the destination.

    Presence is decided purely by correspondence -- ``received_uuid == stream_uuid`` for
    btrfs, name for raw -- via the polymorphic ``correspondent_of`` (which never raises; a listing
    failure yields None -> absent). This is the shared presence authority used by both the
    transfer planner and the persistent-lock reconcile, so the two can never disagree. A
    re-created snapshot (same name, new uuid) is correctly absent, never a name coincidence.
    """
    # ONE listing for all of them. Asking per snapshot meant a remote
    # `btrfs subvolume list` per source snapshot on an ssh:// destination --
    # about three seconds each, so a 44-snapshot source spent over two minutes
    # here before transferring anything (issue #106). Semantics are unchanged:
    # correspondents_of applies the same per-endpoint rule (received_uuid for
    # btrfs, name for raw) and never raises.
    # Called directly rather than probed for. Every endpoint inherits
    # correspondents_of from Endpoint (raw overrides it with name semantics), so
    # a getattr/callable probe would only ever be accommodating a test double --
    # and it silently mis-fires on one: a bare MagicMock auto-creates the
    # attribute, so the probe took the batch path and called a Mock.
    return set(destination_endpoint.correspondents_of(list(source_snapshots)))


def _as_selection(only: Any) -> Optional[list]:
    """``only`` as a list of snapshots: None for "everything", a one-element list
    for the single snapshot every existing caller passes, the list itself for
    a selection."""
    if only is None:
        return None
    if isinstance(only, (list, tuple, set, frozenset)):
        return list(only)
    return [only]


def expand_required_chain(selection, source_snapshots, present, source_endpoint):
    """The selection plus every snapshot the source says it depends on.

    For each selected snapshot that is not already at the destination, the
    source is asked ``required_parent_of`` and the answer is added, then asked
    about in turn, until the chain reaches a snapshot that IS at the
    destination (by correspondence), needs nothing, or is already selected.
    The chain is drawn from ``source_snapshots`` -- the source's own listing,
    matched by name -- so every added member is something the executor can
    actually send from there.

    Raises ``PlanningError`` when a requirement cannot be met: the source says
    a snapshot needs a parent it does not hold (a raw store whose sidecar names
    a stream that is gone), and nothing at the destination corresponds to the
    snapshot itself. That is refused HERE, before streaming, because the
    receive would otherwise fail after transferring the whole increment.
    """
    by_name = {s.get_name(): s for s in source_snapshots}
    selected_names = {s.get_name() for s in selection}
    expanded = list(selection)
    for snap in list(selection):
        if snap.get_name() in present:
            continue
        current = snap
        while True:
            try:
                required = source_endpoint.required_parent_of(current)
            except __util__.AbortError as e:
                raise PlanningError(
                    f"Refusing to plan {snap.get_name()}: {e} Nothing was transferred."
                ) from e
            if required is None:
                break
            required_name = required.get_name()
            if required_name in present or required_name in selected_names:
                break
            member = by_name.get(required_name)
            if member is None:
                # The source names a requirement it does not itself list.
                # Nothing here can send it, and following the answer any
                # further would be walking objects the listing never produced.
                raise PlanningError(
                    f"Refusing to plan {snap.get_name()}: it requires "
                    f"{required_name}, which the source names but does not "
                    f"list, so it cannot be sent from there. Nothing was "
                    f"transferred."
                )
            expanded.append(member)
            selected_names.add(required_name)
            logger.info(
                "Restoring %s requires %s first; adding it to the plan.",
                current.get_name(),
                required_name,
            )
            current = member
    return expanded


def require_parents_present(selection, present, source_endpoint, *, skip_present):
    """Refuse a selection whose requirements are not met, adding nothing.

    The check-only sibling of :func:`expand_required_chain`, for a caller
    that plans exactly what was selected: each selected snapshot's required
    parent (``required_parent_of``) must already be at the destination by
    correspondence or be selected itself. A snapshot that is present and
    will be skipped needs nothing. A requirement the source names but cannot
    meet raises the same way the expanding form does.

    Raises ``PlanningError``: a stored raw increment whose parent is neither
    at the destination nor selected would otherwise stream all of itself and
    fail in the receive.
    """
    selected_names = {s.get_name() for s in selection}
    for snap in selection:
        name = snap.get_name()
        if skip_present and name in present:
            continue
        try:
            required = source_endpoint.required_parent_of(snap)
        except __util__.AbortError as e:
            raise PlanningError(
                f"Refusing to plan {name}: {e} Nothing was transferred."
            ) from e
        if required is None:
            continue
        required_name = required.get_name()
        if required_name in present or required_name in selected_names:
            continue
        raise PlanningError(
            f"Refusing to plan {name}: it requires {required_name}, which is "
            f"neither at the destination nor selected, and a stored increment "
            f"applies only onto its parent. Restore {required_name} first, or "
            f"select both. Nothing was transferred."
        )


def plan_transfer_sequence(
    source_snapshots,
    destination_endpoint,
    *,
    no_incremental=False,
    keep_num_backups=0,
    only=None,
    source_endpoint=None,
    expand_selection=True,
    skip_present=True,
):
    """Build the ordered transfer plan ``[(snapshot, parent_or_None)]``.

    Args:
        source_snapshots: All snapshots at the source (each carrying its btrfs uuid).
        destination_endpoint: The destination; queried via ``correspondent_of``.
        no_incremental: If True, every snapshot is a full send (``parent=None``).
        keep_num_backups: If > 0, only consider the latest N source snapshots.
        only: If given, plan just this snapshot -- or, given a list, just these
            (a selection). Anything else at the source is neither transferred
            nor, unless ``source_endpoint`` says it is required, added.
        source_endpoint: When given, a selection is expanded to the chain the
            source says it depends on (``required_parent_of``: the time-ordered
            predecessor for a btrfs source, the sidecar's parent for a raw
            store), stopping at whatever the destination already holds. A
            requirement the source cannot meet is refused with
            ``PlanningError`` before anything is transferred. Without it the
            selection is planned exactly as given (the backup run's behaviour).
        expand_selection: With ``source_endpoint``, False plans the selection
            exactly as given and only CHECKS it: a selected snapshot whose
            required parent is neither at the destination nor selected is
            refused (``require_parents_present``). A snapper restore plans
            this way -- the operator named the snapshots to bring, and a
            stored raw increment without its parent is refused rather than
            streamed.
        skip_present: False plans a selected snapshot even when its copy is
            already at the destination. The snapper layout lands every
            restore in a fresh numbered slot, as snapper keeps every
            snapshot, and what is present still serves as the incremental
            parent. The default skips what is present.

    A snapshot whose correspondent is already present on the destination is skipped. For
    each snapshot to transfer, the parent is the newest source snapshot ordered BEFORE it --
    by creation time, then source-enumeration position to break same-second ties (see
    ``_order_key``) -- that is (or, within this run, will be) present on the destination by
    correspondence (uuid for btrfs, name for raw), so ``btrfs receive`` can resolve the
    ``send -p``; if none qualifies the snapshot is sent in full. Correspondence is the only
    presence/parent authority -- a snapshot the destination cannot verifiably resolve is never
    used as a parent.

    Within-run chaining: because the plan executes oldest-first and each transferred snapshot
    then corresponds on the destination, a snapshot may parent off an EARLIER-in-this-run
    transfer (not only snapshots already on the destination at plan time). This keeps a fresh
    multi-snapshot run (e.g. an initial snapper-history backup) a tight incremental chain
    instead of all-full sends. If an earlier transfer fails at execution, its dependent
    incremental fails too and is surfaced -- never silently mis-applied.
    """
    present = snapshots_present_on(source_snapshots, destination_endpoint)

    # A snapshot whose name yields no timestamp cannot be ordered against the
    # others, so it takes no part in candidacy, parent search, or the
    # keep_num_backups budget -- and NEVER silently: each exclusion is
    # reported, because "not transferred" must be a fact the operator was
    # told, not one they discover during a restore.
    dated = []
    for snap in source_snapshots:
        if getattr(snap, "time_obj", None) is None:
            logger.info(
                "Not planning a transfer for %s: its name yields no "
                "timestamp, so it cannot be ordered against the other "
                "snapshots. It remains listed at the source.",
                snap.get_name(),
            )
        else:
            dated.append(snap)

    selection = _as_selection(only)
    undated_full: list = []
    if selection is not None:
        if source_endpoint is not None:
            if expand_selection:
                selection = expand_required_chain(
                    selection, source_snapshots, present, source_endpoint
                )
            else:
                require_parents_present(
                    selection, present, source_endpoint, skip_present=skip_present
                )
        candidates = []
        for chosen in selection:
            if getattr(chosen, "time_obj", None) is None:
                # An EXPLICIT single-snapshot request is honoured even without a
                # timestamp: no ordering is needed for a full send, which always
                # works. Presence still short-circuits it. Sent FIRST, ahead of
                # the dated members: in practice such a member is an old base
                # that a chain was built on, and if that guess is ever wrong
                # btrfs receive fails loudly on the missing parent rather than
                # applying a delta to the wrong subvolume.
                if skip_present and chosen.get_name() in present:
                    continue
                logger.info(
                    "Transferring %s as a full send: its name yields no "
                    "timestamp to choose an incremental parent by.",
                    chosen.get_name(),
                )
                undated_full.append((chosen, None))
            else:
                candidates.append(chosen)
    elif keep_num_backups > 0:
        candidates = dated[-keep_num_backups:]
    else:
        candidates = list(dated)

    # A TOTAL order over snapshots: primary = creation time, secondary = position in the source
    # enumeration (snapper number / btrfs subvol-id order, which is creation order -- and unlike
    # a ``.number`` attribute, every snapshot type has a list position). The secondary breaks
    # SAME-SECOND ties (e.g. a fast pre/post pair snapper's 1s-resolution date collapses to one
    # timestamp): without it, ``o.time_obj < snap.time_obj`` excludes an equal-timestamp earlier
    # snapshot from being a parent, so each same-second snapshot falls back to a full send. For
    # distinct timestamps the secondary never engages, so ordering is unchanged.
    order_pos = {s.get_name(): i for i, s in enumerate(source_snapshots)}

    def _order_key(s):
        return (s.time_obj, order_pos.get(s.get_name(), 0))

    seen: set = set()
    to_transfer = []
    for s in sorted(candidates, key=_order_key):
        if (skip_present and s.get_name() in present) or s.get_name() in seen:
            continue
        seen.add(s.get_name())
        to_transfer.append(s)

    # A parent is valid if its correspondent is present on the destination -- either already
    # (``present``) or projected to be, because it is transferred earlier in THIS run.
    # ``to_transfer`` is oldest-first, so an earlier item is on the destination by the time a
    # later one sends against it.
    projected_present = set(present)

    plan = list(undated_full)
    for snap in to_transfer:
        parent = None
        if not no_incremental:
            older_newest_first = sorted(
                (o for o in dated if _order_key(o) < _order_key(snap)),
                key=_order_key,
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
