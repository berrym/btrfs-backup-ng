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
    plan = plan_transfer_sequence([s_oldest, s_mid, newest], dest)
    # s_oldest present (skipped); s_mid absent (U-MID-NEW != U-MID-OLD) and newest absent.
    # newest's parent must be s_oldest (verified) -- NEVER the newer name-present-but-
    # non-corresponding s_mid, whose send -p the destination could not resolve.
    by = {s.get_name(): p for s, p in plan}
    assert set(by) == {"home-20240102-000000", "home-20240103-000000"}
    assert (
        by["home-20240103-000000"] is s_oldest
    )  # skipped s_mid (name-only), used s_oldest
    assert by["home-20240102-000000"] is s_oldest  # s_mid parents on s_oldest too


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
    as a FULL send, never skipped by a name coincidence and never given an unverifiable
    incremental parent. An empty uuid is an enrichment problem to fix at the source
    (sudo-escalated `subvolume show`), not a reason to dilute the planner into name matching.
    Mutation guard: any name-based presence/parent fallback re-skips these."""
    s1 = _snap("20240101-000000", uuid="")  # unknown identity
    s2 = _snap("20240102-000000", uuid="")
    # The dest even lists a SAME-NAMED snapshot for s1: a name fallback would (wrongly) skip
    # it. Pure correspondence does not.
    dest = _dest(
        tmp_path, [_snap("20240101-000000", uuid="Dz", received_uuid="anything")]
    )
    present = snapshots_present_on([s1, s2], dest)
    assert present == set()  # strictly correspondence; no name fallback
    plan = plan_transfer_sequence([s1, s2], dest)
    assert [s.get_name() for s, _ in plan] == [
        "home-20240101-000000",
        "home-20240102-000000",
    ]
    assert all(p is None for _, p in plan)  # no name-based parent


def test_ordering_is_oldest_first(tmp_path):
    s3 = _snap("20240103-000000", uuid="U3")
    s1 = _snap("20240101-000000", uuid="U1")
    s2 = _snap("20240102-000000", uuid="U2")
    dest = _dest(tmp_path, [])  # empty -> all full sends
    plan = plan_transfer_sequence([s3, s1, s2], dest)
    assert [s.get_name() for s, _ in plan] == [
        "home-20240101-000000",
        "home-20240102-000000",
        "home-20240103-000000",
    ]
    assert all(p is None for _, p in plan)


def test_only_single_snapshot_mode(tmp_path):
    s1 = _snap("20240101-000000", uuid="U1")
    s2 = _snap("20240102-000000", uuid="U2")
    dest = _dest(tmp_path, [_snap("20240101-000000", uuid="Da", received_uuid="U1")])
    plan = plan_transfer_sequence([s1, s2], dest, only=s2)
    assert [s.get_name() for s, _ in plan] == ["home-20240102-000000"]
    assert plan[0][1] is s1  # incremental against the corresponding s1


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
