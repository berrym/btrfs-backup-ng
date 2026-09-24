"""What ``run``'s catch-up selection must never leave out, and never add.

The selector sends a target that is behind only what the target's prune
keeps. Two inputs the equivalence argument did not cover:

- a missing snapshot that carries THIS destination's lock is a transfer that
  did not finish. Only a completed transfer releases the lock, and the
  reconcile at the start of a sync clears locks only for snapshots present
  on the destination. Left out of the catch-up, the snapshot kept its lock
  for ever and the source prune skipped it every run.
- an undated subvolume in the snapshot directory is kept by retention as
  unparseable, so the selector kept it in the selection; and a selection is
  an explicit request to the planner, which then sent it as a full send,
  first -- something the plan without a selection never does.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import run as run_cli
from btrfs_backup_ng.config.schema import RetentionConfig

PREFIX = "home-"
FMT = "%Y%m%d-%H%M%S"
NOW = datetime(2026, 9, 23, 12, 0, 0)
POLICY = RetentionConfig(min="0s", hourly=0, daily=2, weekly=0, monthly=0, yearly=0)


def _snapshot(tmp_path, when: datetime | None, name: str | None = None):
    if name is None:
        name = PREFIX + when.strftime(FMT)  # type: ignore[union-attr]
    time_obj = when.timetuple() if when is not None else None
    return __util__.Snapshot(tmp_path, PREFIX, None, time_obj=time_obj, name=name)


def _selector(destination_id="dest"):
    config = MagicMock()
    config.get_target_retention.return_value = POLICY
    config.global_config.timestamp_format = FMT
    destination = MagicMock()
    destination.get_id.return_value = destination_id
    destination.list_snapshots.return_value = []
    select = run_cli._catch_up_selector(
        SimpleNamespace(snapshot_prefix=PREFIX),
        config,
        SimpleNamespace(path="/t"),
        destination,
    )
    assert select is not None
    return select


def _names(snaps) -> set[str]:
    return {s.get_name() for s in snaps}


class TestALockedSnapshotIsAlwaysSent:
    def test_a_snapshot_this_destination_locked_is_kept_in_the_selection(
        self, tmp_path
    ):
        # Two snapshots on the same old day: the older is the day's
        # representative, the newer would be left out -- unless locked.
        victim = _snapshot(tmp_path, NOW - timedelta(days=5, hours=2))
        older = _snapshot(tmp_path, NOW - timedelta(days=5, hours=6))
        newest = _snapshot(tmp_path, NOW - timedelta(hours=1))
        victim.locks.add("dest")
        chosen = _selector()([older, victim, newest], set())
        assert victim in chosen, "a locked snapshot left out keeps its lock for ever"
        assert _names(chosen) == _names([older, victim, newest])

    def test_a_parent_lock_counts_too(self, tmp_path):
        victim = _snapshot(tmp_path, NOW - timedelta(days=5, hours=2))
        older = _snapshot(tmp_path, NOW - timedelta(days=5, hours=6))
        newest = _snapshot(tmp_path, NOW - timedelta(hours=1))
        victim.parent_locks.add("dest")
        chosen = _selector()([older, victim, newest], set())
        assert victim in chosen

    def test_another_destinations_lock_does_not_count(self, tmp_path):
        victim = _snapshot(tmp_path, NOW - timedelta(days=5, hours=2))
        older = _snapshot(tmp_path, NOW - timedelta(days=5, hours=6))
        newest = _snapshot(tmp_path, NOW - timedelta(hours=1))
        victim.locks.add("some-other-target")
        chosen = _selector()([older, victim, newest], set())
        assert victim not in chosen
        assert _names(chosen) == _names([older, newest])


class TestAnUndatedSubvolumeIsNotTurnedIntoARequest:
    def test_it_is_left_out_of_the_selection_and_not_reported_as_pruned(
        self, tmp_path, caplog
    ):
        undated = _snapshot(tmp_path, None, name=PREFIX + "manual-copy")
        a = _snapshot(tmp_path, NOW - timedelta(days=3))
        b = _snapshot(tmp_path, NOW - timedelta(hours=1))
        with caplog.at_level("INFO"):
            chosen = _selector()([undated, a, b], set())
        assert undated not in chosen
        assert _names(chosen) == _names([a, b])
        # Not "left out because the prune would delete it": the prune keeps it.
        assert not any(
            "Not sending" in r.getMessage() and "manual-copy" in r.getMessage()
            for r in caplog.records
        )

    def test_the_plan_without_a_selection_agrees(self, tmp_path):
        """The planner leaves an undated snapshot out of an unselected plan;
        the selection must not bring it back in."""
        from btrfs_backup_ng.core.planning import plan_transfer_sequence

        undated = _snapshot(tmp_path, None, name=PREFIX + "manual-copy")
        a = _snapshot(tmp_path, NOW - timedelta(days=3))
        b = _snapshot(tmp_path, NOW - timedelta(hours=1))
        for s in (undated, a, b):
            s.uuid = "u-" + s.get_name()
        destination = MagicMock()
        destination.correspondents_of.return_value = {}
        destination.list_snapshots.return_value = []
        without = plan_transfer_sequence([undated, a, b], destination)
        chosen = _selector()([undated, a, b], set())
        with_selection = plan_transfer_sequence(
            [undated, a, b], destination, only=chosen
        )
        assert [s.get_name() for s, _ in without] == [
            s.get_name() for s, _ in with_selection
        ]
        assert "home-manual-copy" not in {s.get_name() for s, _ in with_selection}
