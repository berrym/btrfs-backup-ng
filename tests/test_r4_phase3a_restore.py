"""R4 Phase 3a: restore's incremental-parent selection converged onto correspondent_of.

``restore.find_parent_by_correspondence`` replaces the bespoke ``btrfs subvolume show``
uuid parser with the ONE polymorphic correspondence primitive: ``received_uuid == uuid`` for
btrfs, name for raw. A valid restore parent is a backup present on BOTH sides -- on the
backup (its path feeds ``send -p``) and locally (so ``receive`` can apply the diff). These
tests drive the REAL ``correspondent_of`` (via real Local/Raw endpoints) so a name-based or
wrong-direction regression is caught.
"""

from __future__ import annotations

import logging
import time

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import restore as restore_mod
from btrfs_backup_ng.core.restore import find_parent_by_correspondence
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


def _btrfs_backup_ep(tmp_path, backups):
    """A real btrfs (Local) backup endpoint whose listing we control, so the real
    Endpoint.correspondent_of (received_uuid == source.uuid) drives parent selection."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    ep = LocalEndpoint(
        config={
            "path": tmp_path,
            "source": None,
            "snapshot_folder": ".snapshots",
            "snap_prefix": "home-",
        }
    )
    ep.list_snapshots = lambda flush_cache=False: list(backups)  # type: ignore[method-assign]
    return ep


# --------------------------------------------------------------------------- #
# btrfs restore: parent by received_uuid == local uuid (== old find_parent_by_uuid)
# --------------------------------------------------------------------------- #
def test_btrfs_parent_by_received_uuid(tmp_path):
    b0 = _snap("20240101-000000", uuid="B0", received_uuid="U0")
    b1 = _snap("20240102-000000", uuid="B1", received_uuid="U1")
    backup_ep = _btrfs_backup_ep(tmp_path, [b0, b1])
    # A locally-restored copy of b0: its uuid is what b0 was received from.
    l0 = _snap("20240101-000000", uuid="U0")

    parent, local_match = find_parent_by_correspondence([b0, b1], [l0], b1, backup_ep)
    assert parent is b0  # b0.received_uuid == l0.uuid
    assert local_match is l0


def test_target_is_never_its_own_parent(tmp_path):
    b0 = _snap("20240101-000000", uuid="B0", received_uuid="U0")
    backup_ep = _btrfs_backup_ep(tmp_path, [b0])
    l0 = _snap("20240101-000000", uuid="U0")  # corresponds to b0

    # Restoring b0 itself -> b0 is excluded as its own parent -> no parent.
    parent, local_match = find_parent_by_correspondence([b0], [l0], b0, backup_ep)
    assert parent is None
    assert local_match is None


def test_no_correspondent_returns_none_for_time_fallback(tmp_path):
    """No local correspondent -> (None, None) so the caller degrades to the time-based
    Snapshot.find_parent fallback. Mutation guard: returning a non-corresponding backup fails."""
    b0 = _snap("20240101-000000", uuid="B0", received_uuid="U0")
    b1 = _snap("20240102-000000", uuid="B1", received_uuid="U1")
    backup_ep = _btrfs_backup_ep(tmp_path, [b0, b1])
    l_other = _snap("20240103-000000", uuid="NOMATCH")  # corresponds to nothing

    parent, local_match = find_parent_by_correspondence(
        [b0, b1], [l_other], b1, backup_ep
    )
    assert parent is None
    assert local_match is None


def test_recreated_local_new_uuid_is_not_matched(tmp_path):
    """A re-created local snapshot (same name, NEW uuid) does not correspond to the backup's
    stale received_uuid, so it is NOT selected as a parent -- the R4 win, carried into restore.
    Mutation guard: a name-based match would wrongly pick b0."""
    b0 = _snap("20240101-000000", uuid="B0", received_uuid="U0-OLD")
    b1 = _snap("20240102-000000", uuid="B1", received_uuid="U1")
    backup_ep = _btrfs_backup_ep(tmp_path, [b0, b1])
    recreated_local = _snap("20240101-000000", uuid="U0-NEW")  # same name, new uuid

    parent, local_match = find_parent_by_correspondence(
        [b0, b1], [recreated_local], b1, backup_ep
    )
    assert parent is None  # U0-NEW != b0.received_uuid U0-OLD
    assert local_match is None


def test_first_backup_present_locally_is_chosen(tmp_path):
    b0 = _snap("20240101-000000", uuid="B0", received_uuid="U0")
    b1 = _snap("20240102-000000", uuid="B1", received_uuid="U1")
    b2 = _snap("20240103-000000", uuid="B2", received_uuid="U2")  # target
    backup_ep = _btrfs_backup_ep(tmp_path, [b0, b1, b2])
    l0 = _snap("20240101-000000", uuid="U0")  # corresponds to b0
    l1 = _snap("20240102-000000", uuid="U1")  # corresponds to b1

    parent, local_match = find_parent_by_correspondence(
        [b0, b1, b2], [l0, l1], b2, backup_ep
    )
    assert parent is b0  # first backup (in order) present locally
    assert local_match is l0


# --------------------------------------------------------------------------- #
# raw restore: parent by NAME (the P3a behavioral gain -- was always time-fallback)
# --------------------------------------------------------------------------- #
def test_raw_parent_by_name_correspondence(tmp_path):
    """On a RAW backup target, correspondent_of is name-based, so restore now gets proper
    name correspondence instead of always degrading to time-based (the old bespoke uuid
    parser silently failed -- a stream file is not a subvolume). Mutation guard: a
    uuid-only matcher returns None here (raw streams carry no received_uuid)."""
    from btrfs_backup_ng.endpoint.raw import RawEndpoint
    from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

    rb0 = RawSnapshot(
        name="home-20240101-000000",
        stream_path=tmp_path / "home-20240101-000000.btrfs",
        uuid="RB0",
    )
    rb1 = RawSnapshot(
        name="home-20240102-000000",
        stream_path=tmp_path / "home-20240102-000000.btrfs",
        uuid="RB1",
    )
    raw_ep = RawEndpoint(config={"path": str(tmp_path)})
    raw_ep.list_snapshots = lambda flush_cache=False: [rb0, rb1]  # type: ignore[method-assign]

    # A locally-restored copy whose NAME matches rb0 (raw ignores uuid).
    l0 = _snap("20240101-000000", uuid="irrelevant-for-raw")

    parent, local_match = find_parent_by_correspondence([rb0, rb1], [l0], rb1, raw_ep)
    assert parent is rb0  # matched by NAME (raw name semantics), not uuid
    assert local_match is l0


def test_empty_uuid_local_yields_no_correspondent(tmp_path):
    """An empty-uuid local snapshot (enrichment miss) has no verifiable identity ->
    correspondent_of returns None -> (None, None) so restore degrades to the time-based
    fallback. Mutation guard: a name-based match would wrongly pair them."""
    b0 = _snap("20240101-000000", uuid="B0", received_uuid="")
    backup_ep = _btrfs_backup_ep(tmp_path, [b0])
    l_empty = _snap("20240101-000000", uuid="")  # same name, but no identity

    parent, local_match = find_parent_by_correspondence([b0], [l_empty], b0, backup_ep)
    assert parent is None
    assert local_match is None


# --------------------------------------------------------------------------- #
# End-to-end through restore_snapshots: the correspondence path actually fires
# --------------------------------------------------------------------------- #
def test_restore_snapshots_takes_correspondence_path_end_to_end(
    tmp_path, monkeypatch, caplog
):
    """Drive restore_snapshots end-to-end with the REAL correspondent_of (the parent finder
    is NOT mocked). A target whose parent is present locally by correspondence takes the
    correspondence path and is handed the BACKUP-SIDE parent object. Kills a call-site
    arg-swap (swap -> correspondence returns None -> the path never fires) that the robust
    time-based fallback would otherwise mask."""
    b0 = _snap("20240101-000000", uuid="B0", received_uuid="U0")
    b1 = _snap("20240102-000000", uuid="B1", received_uuid="U1")
    backup_ep = _btrfs_backup_ep(tmp_path / "backup", [b0, b1])
    # A pre-existing local snapshot corresponding to b0 (its uuid == b0.received_uuid).
    l0 = _snap("20240101-000000", uuid="U0")
    local_ep = _btrfs_backup_ep(tmp_path / "local", [l0])

    captured: dict = {}

    def fake_restore_snapshot(be, le, snap, parent=None, **k):
        captured[snap.get_name()] = parent

    monkeypatch.setattr(restore_mod, "restore_snapshot", fake_restore_snapshot)

    with caplog.at_level(logging.DEBUG, logger="btrfs_backup_ng.core.restore"):
        restore_mod.restore_snapshots(
            backup_endpoint=backup_ep,
            local_endpoint=local_ep,
            restore_all=True,
            skip_existing=False,
            no_incremental=False,
        )

    # The correspondence path fired (not the time fallback)...
    assert "Restore parent by correspondence" in caplog.text
    # ...and b1 was handed the BACKUP-SIDE parent b0 (present locally as l0 by uuid).
    assert captured.get("home-20240102-000000") is b0


def test_restore_snapshots_dry_run_uses_correspondence(tmp_path, caplog):
    """Dry-run restore selects the parent by correspondence too (covers the dry-run call site);
    no transfers happen. Mutation guard for the dry-run arg order."""
    b0 = _snap("20240101-000000", uuid="B0", received_uuid="U0")
    b1 = _snap("20240102-000000", uuid="B1", received_uuid="U1")
    backup_ep = _btrfs_backup_ep(tmp_path / "backup", [b0, b1])
    l0 = _snap("20240101-000000", uuid="U0")  # pre-existing local corresponding to b0
    local_ep = _btrfs_backup_ep(tmp_path / "local", [l0])

    with caplog.at_level(logging.DEBUG, logger="btrfs_backup_ng.core.restore"):
        stats = restore_mod.restore_snapshots(
            backup_endpoint=backup_ep,
            local_endpoint=local_ep,
            restore_all=True,
            skip_existing=False,
            dry_run=True,
        )

    assert "Restore parent by correspondence" in caplog.text
    assert stats["restored"] == 0  # dry run: nothing actually restored


def test_never_raises_when_backup_listing_fails(tmp_path):
    """correspondent_of never raises (a listing failure -> None), so find_parent_by_correspondence
    degrades to (None, None) -> time-based fallback, never a crash."""
    b0 = _snap("20240101-000000", uuid="B0", received_uuid="U0")
    backup_ep = _btrfs_backup_ep(tmp_path, [b0])

    def boom(flush_cache=False):
        raise RuntimeError("transient backup listing failure")

    backup_ep.list_snapshots = boom  # type: ignore[method-assign]
    l0 = _snap("20240101-000000", uuid="U0")

    parent, local_match = find_parent_by_correspondence([b0], [l0], b0, backup_ep)
    assert parent is None
    assert local_match is None
