"""R3: retention locks must persist across runs (source-side lock persistence).

A transfer locks the source snapshot it needs (and its incremental parent) by the
destination's id, so retention cannot prune a snapshot a failed or pending transfer still
depends on. ``set_lock`` wrote those locks to ``<path>/.btrfs-backup-ng.locks`` but nothing
ever read them back onto ``snapshot.locks``. So on the *next* run every snapshot looked
unlocked and ``delete_old_snapshots`` / ``delete_snapshots`` -- whose lock-skipping guards
were correct but inert -- could delete a snapshot the next incremental send required.

R3 has three parts, each pinned below:

1. Read-back: ``Endpoint.list_snapshots`` loads persisted locks onto the snapshots it
   returns (``_load_locks_into``), so the retention guards actually see the locks.
2. Presence-based reconcile: ``sync_snapshots`` clears a destination's lock only for a
   source snapshot CONFIRMED present on that destination; a snapshot not yet on the
   destination keeps its lock (a prior transfer failed / is pending). No time-boxing, so a
   long outage can never drop a still-needed lock.
3. Atomic write: ``_write_locks`` writes a temp file, fsyncs, and ``os.replace``s it over
   the target, so a crash mid-write cannot leave a half-written lock file (which would be
   misread as "no locks"). O_NOFOLLOW|O_EXCL refuse a planted symlink at the temp path.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.core.operations as ops
from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.local import LocalEndpoint


def _local(path, source="/src"):
    path.mkdir(parents=True, exist_ok=True)
    return LocalEndpoint(
        config={
            "path": path,
            "source": source,
            "snapshot_folder": ".snapshots",
            "snap_prefix": "home-",
        }
    )


def _snap_named(ep, path, stamp):
    """A Snapshot whose name is ``home-<stamp>`` (stamp = ``%Y%m%d-%H%M%S``)."""
    return __util__.Snapshot(
        path,
        "home-",
        ep,
        time_obj=time.strptime(stamp, "%Y%m%d-%H%M%S"),
        time_format="%Y%m%d-%H%M%S",
    )


def _fake_snap(name, locks=None, parent_locks=None):
    s = MagicMock()
    s.locks = set(locks or ())
    s.parent_locks = set(parent_locks or ())
    s.get_name.return_value = name
    return s


def _endpoints(source_snaps, dest_snaps=()):
    src = MagicMock()
    src.list_snapshots.return_value = list(source_snaps)
    src.get_id.return_value = "dest-1"
    dst = MagicMock()
    dst.get_id.return_value = "dest-1"
    dst.list_snapshots.return_value = list(dest_snaps)
    return src, dst


# --------------------------------------------------------------------------- #
# 1. Read-back
# --------------------------------------------------------------------------- #
def test_load_locks_into_maps_persisted_locks(tmp_path):
    """``_load_locks_into`` copies the on-disk lock entry onto the matching snapshot by
    name; snapshots with no entry stay unlocked. Mutation guard: deleting the
    ``snap.locks = ...`` / ``snap.parent_locks = ...`` assignments fails this."""
    ep = _local(tmp_path)
    a = _snap_named(ep, tmp_path, "20240101-000000")
    b = _snap_named(ep, tmp_path, "20240102-000000")
    ep._write_locks({a.get_name(): {"locks": ["dx"], "parent_locks": ["dy"]}})

    ep._load_locks_into([a, b])

    assert a.locks == {"dx"}
    assert a.parent_locks == {"dy"}
    assert b.locks == set()
    assert b.parent_locks == set()


def test_list_snapshots_loads_persisted_locks(tmp_path):
    """``list_snapshots`` wires the read-back in, so callers (retention) see the locks.
    Mutation guard: removing the ``_load_locks_into(snapshots)`` call in list_snapshots
    makes the loaded snapshot come back unlocked and fails this."""
    ep = _local(tmp_path)
    (tmp_path / "home-20240101-000000").mkdir()
    (tmp_path / "home-20240102-000000").mkdir()
    ep._write_locks({"home-20240101-000000": {"locks": ["dest-1"]}})

    listed = {s.get_name(): s for s in ep.list_snapshots(flush_cache=True)}

    assert listed["home-20240101-000000"].locks == {"dest-1"}
    assert listed["home-20240102-000000"].locks == set()


def test_lock_persists_across_endpoint_instances(tmp_path):
    """The R3 core: a lock set in one run is visible in the next (a fresh endpoint =
    a new process). Mutation guard: no read-back -> the second instance sees it
    unlocked."""
    ep1 = _local(tmp_path)
    (tmp_path / "home-20240101-000000").mkdir()
    s = {x.get_name(): x for x in ep1.list_snapshots()}["home-20240101-000000"]
    ep1.set_lock(s, "dest-1", True)

    ep2 = _local(tmp_path)  # simulate a new run
    s2 = {x.get_name(): x for x in ep2.list_snapshots()}["home-20240101-000000"]
    assert "dest-1" in s2.locks


def test_corrupt_lock_file_treated_as_unlocked_not_abort(tmp_path):
    """A damaged lock file must degrade to "no locks" (warn) rather than abort the whole
    listing. Mutation guard: dropping the try/except in _load_locks_into lets
    _read_locks' AbortError propagate and fails this (raises instead of returning)."""
    ep = _local(tmp_path)
    (tmp_path / "home-20240101-000000").mkdir()
    (tmp_path / ep.config["lock_file_name"]).write_text("{ not valid json")

    snaps = ep.list_snapshots(flush_cache=True)

    assert len(snaps) == 1
    assert snaps[0].locks == set()


# --------------------------------------------------------------------------- #
# 2. Presence-based reconcile (in sync_snapshots)
# --------------------------------------------------------------------------- #
def test_reconcile_clears_lock_when_present_on_destination(monkeypatch):
    """A snapshot confirmed present on the destination has done its job -> clear the
    lock. Mutation guard: making the reconcile a no-op (never clear) fails this."""
    monkeypatch.setattr(
        "btrfs_backup_ng.core.planning.plan_transfers", lambda *a, **k: []
    )
    snap = _fake_snap("snap-1", locks=["dest-1"], parent_locks=["dest-1"])
    present = _fake_snap("snap-1")  # same NAME on the destination
    src, dst = _endpoints([snap], dest_snaps=[present])

    ops.sync_snapshots(src, dst)

    src.set_lock.assert_any_call(snap, "dest-1", False)
    src.set_lock.assert_any_call(snap, "dest-1", False, parent=True)


def test_reconcile_keeps_lock_when_absent_from_destination(monkeypatch):
    """A snapshot NOT yet on the destination (a failed / pending transfer) keeps its
    lock across runs, so retention cannot prune it. Mutation guard: reverting to the
    unconditional blanket-clear (no presence check) clears this lock and fails the
    test."""
    monkeypatch.setattr(
        "btrfs_backup_ng.core.planning.plan_transfers", lambda *a, **k: []
    )
    snap = _fake_snap("snap-1", locks=["dest-1"])
    other = _fake_snap("snap-2")  # destination has a DIFFERENT snapshot
    src, dst = _endpoints([snap], dest_snaps=[other])

    ops.sync_snapshots(src, dst)

    clears = [
        c for c in src.set_lock.call_args_list if c.args[:3] == (snap, "dest-1", False)
    ]
    assert clears == []


# --------------------------------------------------------------------------- #
# 3. End-to-end: a persisted lock survives retention across runs
# --------------------------------------------------------------------------- #
def test_persisted_lock_survives_retention_across_runs(tmp_path, monkeypatch):
    """The whole point of R3: lock the oldest snapshot in run 1; in run 2 a fresh
    endpoint must NOT prune it even though keep-count would otherwise. Mutation guard:
    removing the read-back makes the locked snapshot look unlocked -> it gets deleted ->
    this fails."""
    names = [
        "home-20240101-000000",  # oldest -- will be locked
        "home-20240102-000000",
        "home-20240103-000000",  # newest
    ]

    # Run 1: create snapshots and lock the oldest for dest-1.
    ep1 = _local(tmp_path)
    for n in names:
        (tmp_path / n).mkdir()
    listed = {s.get_name(): s for s in ep1.list_snapshots()}
    ep1.set_lock(listed["home-20240101-000000"], "dest-1", True)

    # Run 2: brand-new endpoint reads the lock back from disk.
    ep2 = _local(tmp_path)
    deleted: list[str] = []
    monkeypatch.setattr(
        ep2,
        "_exec_command",
        lambda spec, **k: deleted.append(str(spec["command"][3][0])),
    )

    ep2.delete_old_snapshots(keep=1)

    # The locked oldest must survive; the older UNLOCKED one is the one pruned.
    assert not any("home-20240101-000000" in d for d in deleted)
    assert any("home-20240102-000000" in d for d in deleted)


def test_delete_snapshots_skips_persisted_lock(tmp_path, monkeypatch):
    """An explicit delete of a locked snapshot is skipped once the lock is read back.
    Mutation guard: no read-back -> the snapshot is unlocked -> it gets deleted."""
    ep1 = _local(tmp_path)
    (tmp_path / "home-20240101-000000").mkdir()
    locked = {s.get_name(): s for s in ep1.list_snapshots()}["home-20240101-000000"]
    ep1.set_lock(locked, "dest-1", True)

    ep2 = _local(tmp_path)
    deleted: list[str] = []
    monkeypatch.setattr(
        ep2,
        "_exec_command",
        lambda spec, **k: deleted.append(str(spec["command"][3][0])),
    )
    target = {s.get_name(): s for s in ep2.list_snapshots()}["home-20240101-000000"]

    ep2.delete_snapshots([target])

    assert deleted == []  # locked -> skipped, no btrfs delete issued


# --------------------------------------------------------------------------- #
# 4. Atomic write
# --------------------------------------------------------------------------- #
def test_write_locks_is_atomic_and_leaves_no_temp(tmp_path):
    """A successful write commits the final file and leaves no temp behind."""
    ep = _local(tmp_path)
    ep._write_locks({"home-x": {"locks": ["d1"]}})

    assert (tmp_path / ep.config["lock_file_name"]).exists()
    assert not (tmp_path / (ep.config["lock_file_name"] + ".tmp")).exists()
    assert ep._read_locks() == {"home-x": {"locks": ["d1"]}}


def test_write_locks_replaces_stale_temp(tmp_path):
    """A leftover temp file from a prior crash must not block the next write (O_EXCL
    would raise). Mutation guard: dropping the stale-temp unlink makes the O_EXCL create
    raise FileExistsError -> AbortError -> this fails."""
    ep = _local(tmp_path)
    stale = tmp_path / (ep.config["lock_file_name"] + ".tmp")
    stale.write_text("stale garbage from a prior crash")

    ep._write_locks({"home-x": {"locks": ["d1"]}})

    assert ep._read_locks() == {"home-x": {"locks": ["d1"]}}
    assert not stale.exists()


# --------------------------------------------------------------------------- #
# 5. Concurrency: set_lock is a read-modify-write against the on-disk file
# --------------------------------------------------------------------------- #
def test_set_lock_preserves_concurrent_locks_on_other_snapshots(tmp_path):
    """set_lock must not clobber a lock another run persisted for a DIFFERENT snapshot.
    It reads the authoritative on-disk state and updates only its own snapshot's entry.
    Mutation guard: reverting set_lock to rebuild lock_dict from the (stale) snapshot
    cache drops the concurrently-written lock and fails the first assertion."""
    ep = _local(tmp_path)
    (tmp_path / "home-20240102-000000").mkdir()
    snap_y = {s.get_name(): s for s in ep.list_snapshots()}["home-20240102-000000"]

    # Simulate another run/process persisting a lock for a snapshot this run's cache
    # does not know about.
    ep._write_locks({"home-20240101-000000": {"locks": ["dest-A"]}})

    ep.set_lock(snap_y, "dest-B", True)

    persisted = ep._read_locks()
    assert persisted.get("home-20240101-000000", {}).get("locks") == ["dest-A"]
    assert "dest-B" in persisted.get("home-20240102-000000", {}).get("locks", [])


def test_set_lock_aborts_on_corrupt_lock_file_without_clobber(tmp_path):
    """A corrupt lock file must abort set_lock (loud) rather than overwrite it -- an
    overwrite would silently discard locks we could not read. Mutation guard: making
    set_lock read defensively (swallow the error and write anyway) fails this."""
    ep = _local(tmp_path)
    corrupt = "{ this is not valid json"
    (tmp_path / ep.config["lock_file_name"]).write_text(corrupt)
    snap = _snap_named(ep, tmp_path, "20240102-000000")

    with pytest.raises(__util__.AbortError):
        ep.set_lock(snap, "dest-1", True)

    # The unreadable file is preserved intact for operator repair.
    assert (tmp_path / ep.config["lock_file_name"]).read_text() == corrupt


# --------------------------------------------------------------------------- #
# 6. Fail-safe: a corrupt lock file must halt retention, not silently prune
# --------------------------------------------------------------------------- #
def test_retention_refuses_to_delete_on_corrupt_lock_file(tmp_path, monkeypatch):
    """delete_old_snapshots must NOT prune when the lock file is corrupt: it cannot tell
    which snapshots are protected, so deleting risks losing a still-needed one. Mutation
    guard: removing the _locks_read_failed guard in delete_old_snapshots prunes the
    excess and fails this."""
    ep = _local(tmp_path)
    for n in [
        "home-20240101-000000",
        "home-20240102-000000",
        "home-20240103-000000",
    ]:
        (tmp_path / n).mkdir()
    (tmp_path / ep.config["lock_file_name"]).write_text("{ corrupt lock file")

    deleted: list[str] = []
    monkeypatch.setattr(
        ep,
        "_exec_command",
        lambda spec, **k: deleted.append(str(spec["command"][3][0])),
    )

    ep.delete_old_snapshots(keep=1)

    assert deleted == []  # fail-safe: nothing pruned while locks are unknowable


def test_delete_snapshots_refuses_on_corrupt_lock_file(tmp_path, monkeypatch):
    """The explicit delete path is guarded too. Mutation guard: removing the
    _locks_read_failed guard in delete_snapshots deletes the target and fails this."""
    ep = _local(tmp_path)
    (tmp_path / "home-20240101-000000").mkdir()
    (tmp_path / ep.config["lock_file_name"]).write_text("{ corrupt")
    target = ep.list_snapshots()[0]  # sets the corrupt-lock flag

    deleted: list[str] = []
    monkeypatch.setattr(ep, "_exec_command", lambda spec, **k: deleted.append("x"))

    ep.delete_snapshots([target])

    assert deleted == []


def test_write_locks_does_not_clobber_through_symlinked_temp(tmp_path):
    """A symlink planted at the temp path must not be followed to clobber its target.
    Mutation guard: opening the temp with a plain ``open(tmp, 'w')`` (no O_NOFOLLOW /
    no unlink) follows the symlink and overwrites the victim -> this fails."""
    ep = _local(tmp_path)
    victim = tmp_path / "victim"
    victim.write_text("precious")
    planted = tmp_path / (ep.config["lock_file_name"] + ".tmp")
    planted.symlink_to(victim)

    ep._write_locks({"home-x": {"locks": ["d1"]}})

    assert victim.read_text() == "precious"  # untouched
    assert ep._read_locks() == {"home-x": {"locks": ["d1"]}}
