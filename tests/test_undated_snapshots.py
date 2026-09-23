"""Timestamp-less snapshots: derived where honest, excluded where not, never silent.

Phase C makes a prefix-matching subvolume whose name yields no timestamp a
listed, restorable snapshot. That object must then be HARMLESS everywhere age
matters: ordering, parent selection, transfer planning, count budgets and
estimates all exclude it -- and each exclusion is reported. These tests pin
the derivation rules and every exclusion point.
"""

from __future__ import annotations

import logging
import time
from unittest.mock import MagicMock

from btrfs_backup_ng.__util__ import DATE_FORMAT, Snapshot, derive_snapshot_time


def _dated(name, stamp, prefix="home-"):
    return Snapshot(
        "/snaps", prefix, None, time_obj=time.strptime(stamp, DATE_FORMAT), name=name
    )


def _undated(name, prefix="home-"):
    return Snapshot("/snaps", prefix, None, name=name)


class TestDeriveSnapshotTime:
    """derive_snapshot_time: as-written first, one _N strip second, None last.

    The as-written-first ordering is documented but NOT claimed load-bearing:
    with the two candidate formats tried today both orders agree on every
    input (measured on the parked branch, brute-forced 7 formats x 14 names),
    so no order-swapping mutation can be scored -- an equivalent mutation is a
    fact about the code, not a weakness of these tests."""

    def test_a_plain_name_parses_as_written(self):
        time_obj, as_written = derive_snapshot_time("20240115-143022")
        assert time.strftime(DATE_FORMAT, time_obj) == "20240115-143022"
        assert as_written is True

    def test_a_trailing_ordinal_is_stripped_for_the_time_only(self):
        time_obj, as_written = derive_snapshot_time("20240115-143022_3")
        assert time.strftime(DATE_FORMAT, time_obj) == "20240115-143022"
        assert as_written is False

    def test_btrbk_daily_counter_names_derive_their_day(self):
        """btrbk short format plus its collision counter -- the migration
        shape that made whole pools read as empty."""
        time_obj, as_written = derive_snapshot_time("20260904_1", "%Y%m%d")
        assert time.strftime("%Y%m%d", time_obj) == "20260904"
        assert as_written is False

    def test_a_full_parse_is_never_mistaken_for_a_counter(self):
        """Under %Y%m%d_%H%M%S the whole of 20260904_120000 is a timestamp:
        noon, no ordinal -- never midnight with ordinal 120000."""
        time_obj, as_written = derive_snapshot_time("20260904_120000", "%Y%m%d_%H%M%S")
        assert time.strftime("%H%M%S", time_obj) == "120000"
        assert as_written is True

    def test_an_unparseable_name_yields_none_not_an_exception(self):
        assert derive_snapshot_time("imported-base") == (None, False)

    def test_a_ten_digit_tail_is_not_an_ordinal(self):
        assert derive_snapshot_time("20240115-143022_1234567890") == (None, False)


class TestUndatedOrdering:
    def test_dated_sorts_before_undated(self):
        dated = _dated("home-20240115-143022", "20240115-143022")
        undated = _undated("home-imported-base")
        assert dated < undated
        assert not undated < dated

    def test_two_undated_order_deterministically_by_name(self):
        a = _undated("home-aaa")
        b = _undated("home-bbb")
        assert a < b
        assert not b < a

    def test_a_mixed_listing_sorts_without_raising(self):
        snaps = [
            _undated("home-zzz"),
            _dated("home-20240116-000000", "20240116-000000"),
            _dated("home-20240115-000000", "20240115-000000"),
        ]
        snaps.sort()
        assert [s.get_name() for s in snaps] == [
            "home-20240115-000000",
            "home-20240116-000000",
            "home-zzz",
        ]

    def test_an_observed_name_without_a_time_stays_timeless(self):
        """The constructor must not invent "now" for an observed snapshot --
        a fictitious age would feed ordering and retention a fact the
        filesystem does not hold."""
        assert _undated("home-imported-base").time_obj is None

    def test_a_created_snapshot_still_defaults_to_now(self):
        snap = Snapshot("/snaps", "home-", None)
        assert snap.time_obj is not None


class TestUndatedFindParent:
    def test_an_undated_snapshot_gets_no_parent(self):
        undated = _undated("home-imported-base")
        present = [_dated("home-20240115-143022", "20240115-143022")]
        assert undated.find_parent(present) is None

    def test_undated_candidates_are_never_parents(self):
        snap = _dated("home-20240116-000000", "20240116-000000")
        old = _dated("home-20240115-000000", "20240115-000000")
        present = [_undated("home-imported-base"), old]
        assert snap.find_parent(present) is old

    def test_all_undated_presents_mean_a_full_send(self):
        snap = _dated("home-20240116-000000", "20240116-000000")
        present = [_undated("home-imported-base")]
        assert snap.find_parent(present) is None

    def test_presence_by_name_still_wins_for_an_undated_snapshot(self):
        """Already-transferred detection is name identity and needs no time."""
        undated = _undated("home-imported-base")
        twin = _undated("home-imported-base")
        assert undated.find_parent([twin]) is None


class TestPlannerExclusion:
    def _dest(self, present_names=()):
        dest = MagicMock()
        dest.correspondents_of.return_value = {n: object() for n in present_names}
        return dest

    def test_the_plan_is_identical_with_and_without_an_undated_source(self, caplog):
        from btrfs_backup_ng.core.planning import plan_transfer_sequence

        d1 = _dated("home-20240115-000000", "20240115-000000")
        d2 = _dated("home-20240116-000000", "20240116-000000")
        undated = _undated("home-imported-base")
        d1.uuid = "u1"
        d2.uuid = "u2"

        with caplog.at_level(logging.INFO, logger="btrfs_backup_ng.core.planning"):
            with_undated = plan_transfer_sequence([d1, undated, d2], self._dest())
        without = plan_transfer_sequence([d1, d2], self._dest())

        assert with_undated == without
        assert any(
            "home-imported-base" in r.getMessage()
            and "yields no timestamp" in r.getMessage()
            for r in caplog.records
        ), "the exclusion must be reported, never silent"

    def test_undated_never_occupies_a_keep_num_slot(self):
        """keep_num_backups budgets over DATED snapshots. Undated sorts last,
        so slicing the raw list would hand it a slot and silently drop the
        oldest dated snapshot from candidacy -- this catches exactly that."""
        from btrfs_backup_ng.core.planning import plan_transfer_sequence

        d1 = _dated("home-20240115-000000", "20240115-000000")
        d2 = _dated("home-20240116-000000", "20240116-000000")
        undated = _undated("home-imported-base")
        plan = plan_transfer_sequence(
            [d1, d2, undated], self._dest(), keep_num_backups=2, no_incremental=True
        )
        assert [snap.get_name() for snap, _parent in plan] == [
            "home-20240115-000000",
            "home-20240116-000000",
        ]

    def test_an_explicit_undated_request_is_a_full_send(self, caplog):
        from btrfs_backup_ng.core.planning import plan_transfer_sequence

        undated = _undated("home-imported-base")
        with caplog.at_level(logging.INFO, logger="btrfs_backup_ng.core.planning"):
            plan = plan_transfer_sequence([undated], self._dest(), only=undated)
        assert plan == [(undated, None)]
        assert any("full send" in r.getMessage() for r in caplog.records)

    def test_an_explicit_undated_request_already_present_is_skipped(self):
        from btrfs_backup_ng.core.planning import plan_transfer_sequence

        undated = _undated("home-imported-base")
        dest = self._dest(present_names=["home-imported-base"])
        assert plan_transfer_sequence([undated], dest, only=undated) == []


class TestEstimateExclusion:
    def test_the_estimate_skips_and_reports_the_undated(self, tmp_path, caplog):
        from btrfs_backup_ng.core.estimate import estimate_transfer

        source = MagicMock()
        source.config = {"path": tmp_path, "ssh_sudo": False}
        dated = _dated("home-20240115-000000", "20240115-000000")
        undated = _undated("home-imported-base")
        source.list_snapshots.return_value = [dated, undated]
        dest = MagicMock()
        dest.list_snapshots.return_value = []

        with caplog.at_level(logging.INFO, logger="btrfs_backup_ng.core.estimate"):
            estimate = estimate_transfer(source, dest)

        assert [s.name for s in estimate.snapshots] == ["home-20240115-000000"]
        assert any("no derivable timestamp" in r.getMessage() for r in caplog.records)


class TestRestoreGuards:
    def test_an_undated_restore_target_requires_no_predecessor(self, tmp_path):
        """A restore's chain comes from ``required_parent_of``: a snapshot with
        no derivable time has no honest "older", so it needs nothing and a
        wrong parent can never be sent."""
        from btrfs_backup_ng.endpoint.local import LocalEndpoint

        undated = _undated("home-imported-base")
        dated = _dated("home-20240115-000000", "20240115-000000")
        ep = LocalEndpoint(config={"path": str(tmp_path), "snap_prefix": "home-"})
        ep.list_snapshots = lambda flush_cache=False: [dated, undated]  # type: ignore[method-assign]
        assert ep.required_parent_of(undated) is None

    def test_verify_gives_an_undated_snapshot_no_parent(self):
        from btrfs_backup_ng.core.verify import _find_parent_snapshot

        undated = _undated("home-imported-base")
        pool = [_dated("home-20240115-000000", "20240115-000000")]
        assert _find_parent_snapshot(undated, pool) is None

    def test_a_selected_undated_base_is_planned_first(self, tmp_path):
        """The planner's order for a restore selection: an undated member
        first (in practice an old base a chain was built on), then the dated
        ones oldest-first. If that guess is ever wrong btrfs receive fails
        loudly on the missing parent rather than applying a delta to the
        wrong subvolume."""
        from btrfs_backup_ng.core.planning import plan_transfer_sequence
        from btrfs_backup_ng.endpoint.local import LocalEndpoint

        snaps = [
            _dated("home-20240116-000000", "20240116-000000"),
            _undated("home-imported-base"),
            _dated("home-20240115-000000", "20240115-000000"),
        ]
        dest = LocalEndpoint(config={"path": str(tmp_path), "snap_prefix": "home-"})
        dest.list_snapshots = lambda flush_cache=False: []  # type: ignore[method-assign]
        plan = plan_transfer_sequence(snaps, dest, only=list(snaps))
        assert [s.get_name() for s, _ in plan] == [
            "home-imported-base",
            "home-20240115-000000",
            "home-20240116-000000",
        ]

    def test_a_time_bound_reports_what_it_cannot_judge(self, caplog):
        from btrfs_backup_ng.core.restore import find_snapshot_before_time

        target = time.strptime("20240117-000000", DATE_FORMAT)
        dated = _dated("home-20240115-000000", "20240115-000000")
        undated = _undated("home-imported-base")
        with caplog.at_level(logging.INFO, logger="btrfs_backup_ng.core.restore"):
            found = find_snapshot_before_time(target, [dated, undated])
        assert found is dated
        assert any(
            "cannot be matched against a time bound" in r.getMessage()
            for r in caplog.records
        )
