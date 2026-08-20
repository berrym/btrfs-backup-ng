"""A Snapshot compared against a non-Snapshot must answer, not raise.

`__eq__` read `other.prefix` and `other.time_obj` unconditionally, so any
comparison against something that was not a Snapshot raised AttributeError:

    >>> snapshot == None
    AttributeError: 'NoneType' object has no attribute 'prefix'

`x == None` is an ordinary thing to write, and `x in some_list` calls __eq__
against every element, so a single foreign object in a list turned a membership
test into a crash.

This surfaced from annotating __util__ rather than from a failing test: `__eq__`
must accept `object` (Python compares anything against anything), and once the
signature said so, the body's assumption was visible. Returning NotImplemented
is the documented contract -- Python then falls back to identity, giving False.
"""

from __future__ import annotations

import pytest

from btrfs_backup_ng.__util__ import Snapshot


def _snapshot(prefix="home-", time_obj=None):
    snap = Snapshot("/snapshots", prefix, None)
    if time_obj is not None:
        snap.time_obj = time_obj
    return snap


class TestComparingAgainstAForeignType:
    @pytest.mark.parametrize(
        "other", [None, "a string", 42, 3.5, object(), [], {}, set()]
    )
    def test_it_returns_false_rather_than_raising(self, other):
        assert (_snapshot() == other) is False

    @pytest.mark.parametrize("other", [None, "a string", 42])
    def test_inequality_works_too(self, other):
        assert (_snapshot() != other) is True

    def test_membership_in_a_mixed_list_does_not_crash(self):
        """`in` calls __eq__ against every element until one matches."""
        snap = _snapshot()
        assert snap not in [None, "x", 7]
        assert snap in [None, "x", snap]


class TestComparingAgainstAnotherSnapshot:
    def test_same_prefix_and_time_are_equal(self):
        a = _snapshot()
        b = _snapshot(time_obj=a.time_obj)
        assert a == b

    def test_a_different_prefix_is_not_equal(self):
        a = _snapshot(prefix="home-")
        b = _snapshot(prefix="root-", time_obj=a.time_obj)
        assert a != b

    def test_a_different_time_is_not_equal(self):
        import time

        a = _snapshot(time_obj=time.gmtime(1000))
        b = _snapshot(time_obj=time.gmtime(2000))
        assert a != b

    def test_a_snapshot_equals_itself(self):
        snap = _snapshot()
        assert snap == snap


class TestComparisonWithARawSnapshot:
    """Returning NotImplemented changed cross-type equality, deliberately.

    `RawSnapshot.__eq__` matches by name so a raw backup equals the btrfs
    snapshot it came from. Before, `Snapshot.__eq__` compared attributes without
    checking the type, so `snapshot == raw` was False while `raw == snapshot`
    was True -- equality that disagreed with itself depending on which side you
    wrote first. Deferring to the reflected operation makes the two agree.

    Nothing in the codebase routes a delete decision through this: the planner
    matches by uuid (btrfs) or by name (raw) explicitly, never with `==`. These
    pin the behaviour so a future change to either class has to be deliberate.
    """

    def _pair(self):
        import time as _time

        from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

        snap = Snapshot("/snaps", "src-", None, time_obj=_time.localtime())
        name = snap.get_name()
        raw = RawSnapshot(name=name, stream_path=f"/backups/{name}.btrfs")
        return snap, raw

    def test_equality_is_symmetric(self):
        snap, raw = self._pair()
        assert (snap == raw) == (raw == snap)

    def test_the_same_backup_compares_equal_from_either_side(self):
        snap, raw = self._pair()
        assert snap == raw
        assert raw == snap

    def test_a_different_name_is_not_equal(self):
        from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

        snap, _ = self._pair()
        other = RawSnapshot(name="src-19990101-000000", stream_path="/b/x.btrfs")
        assert snap != other
        assert other != snap

    def test_an_unrelated_object_is_still_not_equal(self):
        """Deferring must not turn 'no opinion' into 'equal'."""
        snap, _ = self._pair()
        assert snap != object()
        assert snap is not None
        assert snap != "src-"
