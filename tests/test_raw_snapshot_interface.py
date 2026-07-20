"""RawSnapshot satisfies the __util__.Snapshot interface (0.8.5 PR2).

Before this, raw backups AttributeError'd in restore/verify/prune: those paths
call get_name()/get_path()/time_obj/find_parent and compare/sort snapshots, none
of which a plain dataclass RawSnapshot supported. Identity is by NAME so a raw
backup equals the btrfs snapshot it was made from.

Written to FAIL if the interface is removed (no get_name -> AttributeError; no
__lt__ -> sorting/find_parent break; field-wise equality -> find_parent's
already-present check breaks).
"""

import datetime
import time
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

UTC = datetime.timezone.utc


def _raw(name, day):
    return RawSnapshot(
        name=name,
        stream_path=Path(f"/b/{name}.btrfs"),
        created=datetime.datetime(2024, 1, day, tzinfo=UTC),
    )


def test_basic_interface():
    s = _raw("root.20240115", 15)
    assert s.get_name() == "root.20240115"
    assert s.get_path() == Path("/b/root.20240115.btrfs")
    assert repr(s) == "root.20240115"
    assert isinstance(s.locks, set)
    assert isinstance(s.parent_locks, set)


def test_time_obj_is_struct_time_like_btrfs_snapshot():
    """time_obj must be a struct_time (not a datetime): restore listing calls
    time.strftime(fmt, snap.time_obj) and `restore --before` compares
    snap.time_obj <= struct_time. A datetime raises TypeError on both, breaking
    the exact restore paths raw snapshots are meant to support."""
    s = _raw("root.20240115", 15)
    assert isinstance(s.time_obj, time.struct_time)
    # The two operations the CLI restore paths actually perform:
    assert time.strftime("%Y-%m-%d", s.time_obj) == "2024-01-15"
    later = time.strptime("2024-06-01", "%Y-%m-%d")
    assert s.time_obj <= later  # `restore --before` comparison must not raise


def test_ordering_and_sort():
    a, b, c = _raw("a", 1), _raw("b", 2), _raw("c", 3)
    assert a < b < c
    assert c > a
    assert sorted([c, a, b]) == [a, b, c]


def test_name_based_equality():
    # Same name, different time/fields -> the same snapshot.
    assert _raw("same", 1) == _raw("same", 9)
    assert _raw("x", 1) != _raw("y", 1)
    # Not equal to unrelated objects (NotImplemented -> False), no crash.
    assert _raw("x", 1) != 42


def test_cross_type_equality_with_btrfs_snapshot():
    """A raw backup equals the btrfs Snapshot it came from (shared name), which
    is exactly how incremental parent-matching finds a source snapshot already
    present at a raw destination. (Cross-type *ordering* is not supported --
    btrfs time_obj is a struct_time, raw's is a datetime -- but the live paths
    only ever compare like with like; equality is by name.)"""
    fmt = "%Y%m%d-%H%M%S"
    btrfs = __util__.Snapshot(
        "/src",
        "root.",
        None,
        time_obj=time.strptime("20240115-120000", fmt),
        time_format=fmt,
    )
    raw = RawSnapshot(name=btrfs.get_name(), stream_path=Path("/b/x.btrfs"))
    assert raw == btrfs
    # The live path: `source_snap in destination_list` -> element(raw).__eq__.
    assert btrfs in [raw]


def test_unhashable_like_snapshot():
    # Matches __util__.Snapshot (unhashable); keeps set-based paths consistent.
    with pytest.raises(TypeError):
        hash(_raw("x", 1))


def test_find_parent_picks_most_recent_older():
    a, b, c = _raw("a", 1), _raw("b", 2), _raw("c", 3)
    present = [a, b]
    assert c.find_parent(present) is b
    # `a` is already present (equal by name) -> no parent needed.
    assert a.find_parent(present) is None
    # Nothing present -> no parent.
    assert c.find_parent([]) is None


def test_set_lock_is_in_memory_only_and_never_raises(tmp_path):
    """Restore locks/unlocks the backup snapshot. The raw override must mutate
    only the in-memory lock set -- no `source` requirement, no local lock-file
    write (which would fail for a remote raw+ssh target and abort the restore).
    Persisting locks across runs is deferred (R3)."""
    for ep in (
        RawEndpoint(config={"path": str(tmp_path)}),
        SSHRawEndpoint(config={"path": "/remote/backup", "hostname": "nas"}),
    ):
        snap = _raw("root.20240115", 15)
        ep.set_lock(snap, "restore:abc", True)
        assert snap.locks == {"restore:abc"}
        ep.set_lock(snap, "restore:abc", False)
        assert snap.locks == set()
        ep.set_lock(snap, "xfer:1", True, parent=True)
        assert snap.parent_locks == {"xfer:1"}
        # No lock file was written next to a local raw target.
        assert not (tmp_path / ".btrfs-backup-ng.locks").exists()


def test_list_snapshots_threads_endpoint_locally(tmp_path):
    """Every snapshot from RawEndpoint.list_snapshots must carry endpoint=self so
    restore/verify can read the stream back."""
    (tmp_path / "snap.20240115.btrfs").write_bytes(b"stream")
    ep = RawEndpoint(config={"path": str(tmp_path)})
    snaps = ep.list_snapshots()
    assert len(snaps) == 1
    assert snaps[0].endpoint is ep
