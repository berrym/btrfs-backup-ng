"""R4 Phase 2: the single UUID-first TransferPlanner.

``plan_transfer_sequence`` is the sole authority for what to transfer, in what order, and
with which parent -- decided via ``correspondent_of`` (uuid for btrfs, name for raw), so a
``send -p`` is only ever emitted for a parent the destination actually holds. A re-created
snapshot (same name, new uuid) is therefore correctly "not present" and sent in full,
instead of being matched to a stale same-named copy.
"""

from __future__ import annotations

import time

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core.planning import plan_transfer_sequence, snapshots_present_on
from btrfs_backup_ng.endpoint.local import LocalEndpoint


def _snap(stamp, uuid="", received_uuid=""):
    s = __util__.Snapshot(
        "/snaps",
        "home-",
        None,
        time_obj=time.strptime(stamp, "%Y%m%d-%H%M%S"),
        time_format="%Y%m%d-%H%M%S",
    )
    s.uuid = uuid
    s.received_uuid = received_uuid
    return s


def _snap_named(name, stamp, uuid="", received_uuid=""):
    """A snapshot with an EXPLICIT name decoupled from its timestamp -- the real snapper shape
    (``{config}-{number}-{date}``), so two snapshots can share a same-second ``time_obj`` yet
    keep distinct identities/uuids (which the vanilla name==timestamp ``_snap`` cannot express)."""
    s = _snap(stamp, uuid=uuid, received_uuid=received_uuid)
    s.get_name = lambda: name  # type: ignore[method-assign]
    return s


def _dest(tmp_path, dest_snaps):
    """A real btrfs (Local) destination endpoint whose listing we control, so the real
    Endpoint.correspondent_of (received_uuid == source.uuid) drives the planner."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    ep = LocalEndpoint(
        config={
            "path": tmp_path,
            "source": None,
            "snapshot_folder": ".snapshots",
            "snap_prefix": "home-",
        }
    )
    ep.list_snapshots = lambda flush_cache=False: list(dest_snaps)  # type: ignore[method-assign]
    return ep


# --------------------------------------------------------------------------- #
# Skip detection + parent selection by correspondence
# --------------------------------------------------------------------------- #
def test_absent_snapshot_is_planned_present_is_skipped(tmp_path):
    s1 = _snap("20240101-000000", uuid="U1")
    s2 = _snap("20240102-000000", uuid="U2")
    # dest holds the received copy of s1 (received_uuid == s1.uuid), not s2.
    dcopy1 = _snap("20240101-000000", uuid="Dx", received_uuid="U1")
    dest = _dest(tmp_path, [dcopy1])

    plan = plan_transfer_sequence([s1, s2], dest)
    # s1 present -> skipped; s2 absent -> planned, with s1 (corresponding) as parent.
    assert len(plan) == 1
    assert plan[0][0] is s2
    assert plan[0][1] is s1


def test_parent_is_newest_older_corresponding_source(tmp_path):
    s1 = _snap("20240101-000000", uuid="U1")
    s2 = _snap("20240102-000000", uuid="U2")
    s3 = _snap("20240103-000000", uuid="U3")
    # dest holds copies of s1 AND s2.
    dest = _dest(
        tmp_path,
        [
            _snap("20240101-000000", uuid="Da", received_uuid="U1"),
            _snap("20240102-000000", uuid="Db", received_uuid="U2"),
        ],
    )
    plan = plan_transfer_sequence([s1, s2, s3], dest)
    # only s3 to transfer; parent = s2 (newest older corresponding), NOT s1.
    assert [s.get_name() for s, _ in plan] == ["home-20240103-000000"]
    assert plan[0][1] is s2


def test_full_send_when_no_older_corresponding_parent(tmp_path):
    """Dropped reverse-parent heuristic: if no OLDER corresponding parent exists, full-send
    (parent=None) -- even when a NEWER corresponding snapshot is on the destination.
    Mutation guard: restoring the oldest-present fallback assigns the newer snap as parent."""
    s_old = _snap("20240101-000000", uuid="U-OLD")
    s_new = _snap("20240103-000000", uuid="U-NEW")
    # dest only holds the NEWER snapshot's copy.
    dest = _dest(tmp_path, [_snap("20240103-000000", uuid="Dn", received_uuid="U-NEW")])
    plan = plan_transfer_sequence([s_old, s_new], dest)
    # s_new is present (skipped); s_old is absent with no older corresponding parent -> full.
    assert [s.get_name() for s, _ in plan] == ["home-20240101-000000"]
    assert plan[0][1] is None


def test_recreated_snapshot_same_name_new_uuid_full_send(tmp_path):
    """THE R4 WIN (presence): a re-created source snapshot (same name, new uuid) is NOT
    present -- its uuid does not correspond to the dest's stale same-named copy -- so it is
    planned (not silently skipped). With no older snapshot it is a clean FULL send. Mutation
    guard: name-based presence would skip it. (The parent clause is guarded separately, in
    test_recreated_snapshot_parents_on_older_corresponding_not_stale_copy.)"""
    recreated = _snap("20240101-000000", uuid="NEW-UUID")
    stale_copy = _snap("20240101-000000", uuid="Dx", received_uuid="OLD-UUID")
    dest = _dest(tmp_path, [stale_copy])
    plan = plan_transfer_sequence([recreated], dest)
    assert len(plan) == 1
    assert plan[0][0] is recreated
    assert plan[0][1] is None  # full send, NOT send -p against the stale copy


def test_parent_skips_name_present_but_noncorresponding_older_snapshot(tmp_path):
    """Parent selection is by CORRESPONDENCE, not name: an older snapshot that is present on
    the destination BY NAME but whose dest copy does NOT correspond (received_uuid differs --
    e.g. it too was re-created) is NOT a valid ``send -p`` parent and must be skipped in favor
    of an older snapshot that truly corresponds. Mutation guard: a name-based parent wrongly
    picks the non-corresponding s_mid (its unresolvable ``send -p`` would fail on receive)."""
    s_oldest = _snap("20240101-000000", uuid="U-OLDEST")  # corresponds on dest
    s_mid = _snap(
        "20240102-000000", uuid="U-MID-NEW"
    )  # name on dest, but NON-corresponding
    newest = _snap("20240103-000000", uuid="U-NEW")  # absent -> to transfer
    dest = _dest(
        tmp_path,
        [
            _snap(
                "20240101-000000", uuid="Da", received_uuid="U-OLDEST"
            ),  # corresponds
            _snap(
                "20240102-000000", uuid="Db", received_uuid="U-MID-OLD"
            ),  # NON-matching
        ],
    )
    # only=newest so s_mid is NOT transferred this run (and so not projected as an in-run
    # parent) -- isolating the guard: a name-present-but-non-corresponding older snapshot,
    # not itself in this run, must never be chosen as a parent.
    plan = plan_transfer_sequence([s_oldest, s_mid, newest], dest, only=newest)
    assert [s.get_name() for s, _ in plan] == ["home-20240103-000000"]
    # newest's parent must be s_oldest (verified) -- NEVER the newer name-present-but-
    # non-corresponding s_mid, whose send -p the destination could not resolve.
    assert plan[0][1] is s_oldest


def test_within_run_chaining_over_re_sent_noncorresponding_older(tmp_path):
    """Full-list counterpart to the only= guard: when the name-present-but-non-corresponding
    older snapshot (s_mid) IS transferred this run, its stale dest copy is corrected and it
    becomes a valid IN-RUN parent -- so newest chains off s_mid (uuid U-MID-NEW), not the
    older s_oldest. (This is safe: s_mid is executed before newest.)"""
    s_oldest = _snap("20240101-000000", uuid="U-OLDEST")
    s_mid = _snap("20240102-000000", uuid="U-MID-NEW")
    newest = _snap("20240103-000000", uuid="U-NEW")
    dest = _dest(
        tmp_path,
        [
            _snap("20240101-000000", uuid="Da", received_uuid="U-OLDEST"),
            _snap("20240102-000000", uuid="Db", received_uuid="U-MID-OLD"),  # stale
        ],
    )
    plan = plan_transfer_sequence([s_oldest, s_mid, newest], dest)
    by = {s.get_name(): p for s, p in plan}
    assert set(by) == {
        "home-20240102-000000",
        "home-20240103-000000",
    }  # s_oldest skipped
    assert by["home-20240102-000000"] is s_oldest  # s_mid re-sent, parents on s_oldest
    assert by["home-20240103-000000"] is s_mid  # newest chains off the IN-RUN s_mid


# --------------------------------------------------------------------------- #
# Fallbacks: no_incremental, unknown uuid, empty
# --------------------------------------------------------------------------- #
def test_no_incremental_forces_full_sends(tmp_path):
    s1 = _snap("20240101-000000", uuid="U1")
    s2 = _snap("20240102-000000", uuid="U2")
    dest = _dest(tmp_path, [_snap("20240101-000000", uuid="Da", received_uuid="U1")])
    plan = plan_transfer_sequence([s1, s2], dest, no_incremental=True)
    assert [s.get_name() for s, _ in plan] == ["home-20240102-000000"]
    assert plan[0][1] is None  # no parent despite s1 corresponding


def test_empty_uuid_source_is_not_present_and_planned_full(tmp_path):
    """NO name fallback (R4 purity): a source snapshot whose uuid is unknown cannot be
    verified present -- correspondent_of returns None for an empty uuid -- so it is planned
    as a FULL send, never skipped by a name coincidence with a same-named dest copy. An empty
    uuid is an enrichment problem to fix at the source (sudo-escalated `subvolume show`), not
    a reason to dilute the planner into name matching. (Single snapshot, so within-run
    chaining is not in play.) Mutation guard: a name-based presence fallback re-skips it."""
    s = _snap("20240101-000000", uuid="")  # unknown identity
    # The dest lists a SAME-NAMED snapshot: a name fallback would (wrongly) skip s.
    dest = _dest(
        tmp_path, [_snap("20240101-000000", uuid="Dz", received_uuid="anything")]
    )
    present = snapshots_present_on([s], dest)
    assert present == set()  # strictly correspondence; no name fallback
    plan = plan_transfer_sequence([s], dest)
    assert [n.get_name() for n, _ in plan] == ["home-20240101-000000"]
    assert plan[0][1] is None  # full send (no older snapshot to parent off)


def test_ordering_oldest_first_with_within_run_chaining(tmp_path):
    """Plan is oldest-first, and a fresh multi-snapshot run forms a tight within-run
    incremental CHAIN: the oldest is a full send, each later one parents off the
    immediately-earlier in-run transfer (safe -- executed oldest-first, so the parent is on
    the destination by the time its child sends). Mutation guard: dropping the in-run
    projection makes every snapshot a full send."""
    s3 = _snap("20240103-000000", uuid="U3")
    s1 = _snap("20240101-000000", uuid="U1")
    s2 = _snap("20240102-000000", uuid="U2")
    dest = _dest(tmp_path, [])  # empty dest
    plan = plan_transfer_sequence([s3, s1, s2], dest)
    assert [s.get_name() for s, _ in plan] == [
        "home-20240101-000000",
        "home-20240102-000000",
        "home-20240103-000000",
    ]
    assert plan[0][1] is None  # oldest: full send
    assert plan[1][1] is s1  # chains off the in-run s1
    assert plan[2][1] is s2  # chains off the in-run s2


def test_within_run_chaining_off_earlier_in_run_transfer(tmp_path):
    """A snapshot parents off an EARLIER-in-this-run transfer, not only the destination's
    plan-time state: the dest already holds s1; s2 and s3 transfer this run -> s2 parents the
    on-dest s1, and s3 parents the IN-RUN s2. Mutation guard: dropping the in-run projection
    makes s3 parent s1 (stale, larger diff) instead of s2."""
    s1 = _snap("20240101-000000", uuid="U1")
    s2 = _snap("20240102-000000", uuid="U2")
    s3 = _snap("20240103-000000", uuid="U3")
    dest = _dest(tmp_path, [_snap("20240101-000000", uuid="Da", received_uuid="U1")])
    plan = plan_transfer_sequence([s1, s2, s3], dest)
    by = {s.get_name(): p for s, p in plan}
    assert set(by) == {"home-20240102-000000", "home-20240103-000000"}  # s1 skipped
    assert by["home-20240102-000000"] is s1  # s2 parents the already-on-dest s1
    assert (
        by["home-20240103-000000"] is s2
    )  # s3 parents the IN-RUN s2 (within-run chain)


# --------------------------------------------------------------------------- #
# Same-second tiebreak: equal timestamps still chain (deterministic total order)
# --------------------------------------------------------------------------- #
def test_same_second_snapshots_chain_incrementally(tmp_path):
    """Two snapshots created in the SAME SECOND (equal ``time_obj``, distinct names/uuids -- e.g.
    a fast pre/post pair, snapper's date having 1s resolution) must still chain: ordered by the
    positional tiebreak, the later one parents off the earlier in-run transfer instead of BOTH
    falling back to full sends. Mutation guard: selecting parents by ``time_obj <`` alone (no
    positional tiebreak) excludes the equal-second earlier snapshot -> plan[1] parent is None
    (a redundant full send)."""
    s1 = _snap_named("cfg-1-20240101-000000", "20240101-000000", uuid="U1")
    s2 = _snap_named("cfg-2-20240101-000000", "20240101-000000", uuid="U2")
    dest = _dest(tmp_path, [])  # fresh dest
    plan = plan_transfer_sequence([s1, s2], dest)
    assert [s.get_name() for s, _ in plan] == [
        "cfg-1-20240101-000000",
        "cfg-2-20240101-000000",
    ]
    assert plan[0][1] is None  # oldest by (time, position): full send
    assert plan[1][1] is s1  # SAME-SECOND child chains off s1 -- the fix


def test_same_second_present_snapshot_is_used_as_parent(tmp_path):
    """A same-second earlier snapshot ALREADY on the destination is a valid parent for the later
    same-second snapshot -- the positional tiebreak makes it 'ordered before', so ``send -p`` can
    resolve. Mutation guard: a ``time_obj``-only comparison drops it -> full send."""
    s1 = _snap_named("cfg-1-20240101-000000", "20240101-000000", uuid="U1")
    s2 = _snap_named("cfg-2-20240101-000000", "20240101-000000", uuid="U2")
    dest = _dest(
        tmp_path,
        [
            _snap_named(
                "cfg-1-20240101-000000",
                "20240101-000000",
                uuid="Da",
                received_uuid="U1",
            )
        ],
    )
    plan = plan_transfer_sequence([s1, s2], dest)
    # s1 present -> skipped; s2 planned with the on-dest same-second s1 as parent.
    assert [s.get_name() for s, _ in plan] == ["cfg-2-20240101-000000"]
    assert plan[0][1] is s1


def test_same_second_pair_embedded_in_longer_chain(tmp_path):
    """A same-second pair inside a longer fresh run chains cleanly end-to-end: the oldest is
    full, the two same-second snaps chain (second off first), and a later snap chains off the
    second. Guards that the tiebreak threads a whole chain, not just an isolated pair."""
    s0 = _snap_named("cfg-0-20240101-000000", "20240101-000000", uuid="U0")
    s1 = _snap_named("cfg-1-20240101-000010", "20240101-000010", uuid="U1")
    s2 = _snap_named(
        "cfg-2-20240101-000010", "20240101-000010", uuid="U2"
    )  # same sec as s1
    s3 = _snap_named("cfg-3-20240101-000020", "20240101-000020", uuid="U3")
    dest = _dest(tmp_path, [])
    plan = plan_transfer_sequence([s0, s1, s2, s3], dest)
    parents = {s.get_name(): (p.get_name() if p else None) for s, p in plan}
    assert [s.get_name() for s, _ in plan] == [
        "cfg-0-20240101-000000",
        "cfg-1-20240101-000010",
        "cfg-2-20240101-000010",
        "cfg-3-20240101-000020",
    ]
    assert parents["cfg-0-20240101-000000"] is None
    assert parents["cfg-1-20240101-000010"] == "cfg-0-20240101-000000"
    assert (
        parents["cfg-2-20240101-000010"] == "cfg-1-20240101-000010"
    )  # same-second chain
    assert parents["cfg-3-20240101-000020"] == "cfg-2-20240101-000010"


def test_only_single_snapshot_mode(tmp_path):
    s1 = _snap("20240101-000000", uuid="U1")
    s2 = _snap("20240102-000000", uuid="U2")
    dest = _dest(tmp_path, [_snap("20240101-000000", uuid="Da", received_uuid="U1")])
    plan = plan_transfer_sequence([s1, s2], dest, only=s2)
    assert [s.get_name() for s, _ in plan] == ["home-20240102-000000"]
    assert plan[0][1] is s1  # incremental against the corresponding s1


def test_no_incremental_suppresses_within_run_chaining(tmp_path):
    """``no_incremental`` forces ALL full sends even across a multi-snapshot in-run batch --
    the in-run projection must never sneak in an incremental parent. Mutation guard: chaining
    under no_incremental."""
    s1 = _snap("20240101-000000", uuid="U1")
    s2 = _snap("20240102-000000", uuid="U2")
    s3 = _snap("20240103-000000", uuid="U3")
    dest = _dest(tmp_path, [])
    plan = plan_transfer_sequence([s1, s2, s3], dest, no_incremental=True)
    assert [s.get_name() for s, _ in plan] == [
        "home-20240101-000000",
        "home-20240102-000000",
        "home-20240103-000000",
    ]
    assert all(p is None for _, p in plan)


def test_keep_num_backups_chain_does_not_parent_outside_window(tmp_path):
    """With ``keep_num_backups``, an in-window snapshot must NOT parent off an OUT-of-window
    older snapshot (which is not transferred this run, so not on the destination). The first
    kept snapshot is a full send; later kept ones chain within the window. Mutation guard:
    parenting off the out-of-window U2 would emit an unresolvable send -p."""
    snaps = [_snap(f"2024010{i}-000000", uuid=f"U{i}") for i in range(1, 5)]  # U1..U4
    dest = _dest(tmp_path, [])  # empty
    plan = plan_transfer_sequence(snaps, dest, keep_num_backups=2)
    # candidates = [U3, U4]. U3 is full (out-of-window U2 is not transferred -> not present);
    # U4 chains off the in-run U3.
    assert [s.get_name() for s, _ in plan] == [
        "home-20240103-000000",
        "home-20240104-000000",
    ]
    assert plan[0][1] is None  # U3 full -- never parents off the out-of-window U2
    assert plan[1][1] is snaps[2]  # U4 chains off the in-run U3


def test_keep_num_backups_limits_candidates(tmp_path):
    snaps = [_snap(f"2024010{i}-000000", uuid=f"U{i}") for i in range(1, 5)]
    dest = _dest(tmp_path, [])
    plan = plan_transfer_sequence(snaps, dest, keep_num_backups=2)
    # only the latest 2 considered.
    assert [s.get_name() for s, _ in plan] == [
        "home-20240103-000000",
        "home-20240104-000000",
    ]


# --------------------------------------------------------------------------- #
# snapshots_present_on (shared presence authority)
# --------------------------------------------------------------------------- #
def test_snapshots_present_on_is_pure_correspondence(tmp_path):
    """Presence is strictly received_uuid==uuid (btrfs), never name. A same-named dest
    snapshot whose received_uuid does not match, and an empty-uuid source, are both ABSENT.
    Mutation guard: a name fallback would mark the empty-uuid or wrong-received_uuid ones
    present."""
    s_corresponds = _snap("20240101-000000", uuid="U1")
    s_empty_uuid = _snap(
        "20240102-000000", uuid=""
    )  # unknown -> absent despite name match
    s_wrong_recv = _snap(
        "20240103-000000", uuid="U3"
    )  # dest same name, non-matching recv
    dest = _dest(
        tmp_path,
        [
            _snap("20240101-000000", uuid="Da", received_uuid="U1"),
            _snap("20240102-000000", uuid="Db", received_uuid="X"),
            _snap("20240103-000000", uuid="Dc", received_uuid="OTHER"),
        ],
    )
    present = snapshots_present_on([s_corresponds, s_empty_uuid, s_wrong_recv], dest)
    assert present == {"home-20240101-000000"}  # only the true uuid correspondent


# --------------------------------------------------------------------------- #
# Raw destination: correspondence is name-based (polymorphic dispatch)
# --------------------------------------------------------------------------- #
def test_raw_destination_planner_uses_name_correspondence(tmp_path):
    """Against a RAW target, correspondent_of is name semantics (a raw stream has no
    received_uuid), so the planner skips a same-named backup and picks an older same-named
    backup as the incremental parent -- even though the source and raw-backup uuids differ.
    Exercises the polymorphic dispatch the planner relies on for raw (the btrfs LocalEndpoint
    tests can never cover this name path)."""
    from btrfs_backup_ng.endpoint.raw import RawEndpoint
    from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

    raw = RawEndpoint(config={"path": str(tmp_path)})
    backup1 = RawSnapshot(
        name="home-20240101-000000",
        stream_path=tmp_path / "home-20240101-000000.btrfs",
        uuid="RAW1",
    )
    raw.list_snapshots = lambda flush_cache=False: [backup1]  # type: ignore[method-assign]

    s1 = _snap(
        "20240101-000000", uuid="SRC1"
    )  # different uuid, same name -> present by name
    s2 = _snap("20240102-000000", uuid="SRC2")  # absent -> planned
    present = snapshots_present_on([s1, s2], raw)
    assert present == {"home-20240101-000000"}  # name correspondence, uuid ignored
    plan = plan_transfer_sequence([s1, s2], raw)
    assert [s.get_name() for s, _ in plan] == ["home-20240102-000000"]
    assert (
        plan[0][1] is s1
    )  # incremental parent = s1 by raw name correspondence, not None


def test_presence_and_plan_survive_destination_listing_failure(tmp_path):
    """correspondent_of never raises: a destination whose list_snapshots raises yields None
    (absent), so snapshots_present_on returns an empty set and the planner produces a
    full-send plan -- never a crash and never a silent skip. Mutation guard: letting the
    listing exception propagate fails this."""
    s1 = _snap("20240101-000000", uuid="U1")
    dest = _dest(tmp_path, [])

    def boom(flush_cache=False):
        raise RuntimeError("transient listing failure")

    dest.list_snapshots = boom  # type: ignore[method-assign]
    present = snapshots_present_on([s1], dest)
    assert present == set()
    plan = plan_transfer_sequence([s1], dest)
    assert [s.get_name() for s, _ in plan] == ["home-20240101-000000"]
    assert plan[0][1] is None
