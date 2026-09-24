"""Two snapper snapshots taken within one second are ordered by their number.

snapper dates have one-second resolution and no collision counter, so
snapshots taken in quick succession share a timestamp. Retention sorted
newest-first by timestamp alone and let the sort's stability break the tie --
which, under ``reverse=True``, keeps INPUT order: the first of a tie (the
lowest number, the OLDEST) came out as "latest", and the last (the NEWEST)
as the bucket's oldest member or as nothing at all. Measured on real btrfs
with four snapshots taken in two seconds (dates 38, 38, 39, 39) under
``daily = 1``: the plan kept 2 and 3 and deleted 1 and 4 -- the newest
snapshot on the machine among the deleted, and the same wrong answer from
the prune and from ``run``'s catch-up selection, which asks the prune.

The snapper number is creation order, and it now breaks the tie, in every
snapper retention path. Nothing here depends on the zone the suite runs in:
the dates are explicit, and the cases are repeated with the process pinned
west of UTC, at UTC and east of UTC, across a day boundary.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from btrfs_backup_ng.cli import run as run_cli
from btrfs_backup_ng.cli.prune import (
    plan_snapper_retention,
    plan_snapper_retention_of,
)
from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.retention import apply_retention

DAILY_ONE = RetentionConfig(min="0s", hourly=0, daily=1, weekly=0, monthly=0, yearly=0)
HOURLY = RetentionConfig(min="0s", hourly=1, daily=0, weekly=0, monthly=0, yearly=0)


def _backup(number: int, when: datetime) -> dict:
    return {
        "number": number,
        "snapshot_path": f"/bk/.snapshots/{number}/snapshot",
        "metadata": SimpleNamespace(date=when, description="", num=number),
    }


def _source(number: int, when: datetime):
    return SimpleNamespace(number=number, date=when)


def _numbers(items) -> list[int]:
    return sorted(i["number"] if isinstance(i, dict) else i.number for i in items)


@pytest.fixture(params=["Pacific/Honolulu", "UTC", "Pacific/Chatham"])
def zone(request, monkeypatch):
    """The process pinned west of UTC, at UTC and east of UTC, and put back."""
    monkeypatch.setenv("TZ", request.param)
    time.tzset()
    yield request.param
    monkeypatch.undo()
    time.tzset()


class TestTheTieGoesToTheHigherNumber:
    def test_the_measured_case(self, zone):
        """Dates 38, 38, 39, 39; daily = 1; now a moment later. Keep the day's
        first (1) and the newest (4); delete 2 and 3."""
        base = datetime(2026, 9, 24, 1, 2, 38)
        items = [
            _source(1, base),
            _source(2, base),
            _source(3, base + timedelta(seconds=1)),
            _source(4, base + timedelta(seconds=1)),
        ]
        keep, delete = plan_snapper_retention_of(
            items, DAILY_ONE, now=base + timedelta(seconds=2)
        )
        assert _numbers(keep) == [1, 4]
        assert _numbers(delete) == [2, 3]

    def test_across_a_day_boundary(self, zone):
        """Two at 23:59:59 and two at 00:00:00: the newest day holds 3 and 4,
        its first is 3 and the newest overall is 4."""
        before = datetime(2026, 9, 23, 23, 59, 59)
        after = datetime(2026, 9, 24, 0, 0, 0)
        items = [
            _source(1, before),
            _source(2, before),
            _source(3, after),
            _source(4, after),
        ]
        keep, delete = plan_snapper_retention_of(
            items, DAILY_ONE, now=after + timedelta(seconds=5)
        )
        assert _numbers(keep) == [3, 4]
        assert _numbers(delete) == [1, 2]

    def test_input_order_does_not_decide(self, zone):
        """The same items handed over newest-first give the same answer."""
        base = datetime(2026, 9, 24, 1, 2, 38)
        items = [
            _source(4, base + timedelta(seconds=1)),
            _source(3, base + timedelta(seconds=1)),
            _source(2, base),
            _source(1, base),
        ]
        keep, _ = plan_snapper_retention_of(
            items, DAILY_ONE, now=base + timedelta(seconds=2)
        )
        assert _numbers(keep) == [1, 4]

    def test_held_backups_and_missing_snapshots_tie_by_number_too(self, zone):
        """The catch-up asks about the destination's backups (dicts) plus the
        source's missing snapshots together; a tie between a held slot and a
        missing snapshot is still decided by the number, not by which list
        came first."""
        base = datetime(2026, 9, 24, 1, 2, 38)
        held = [_backup(1, base), _backup(3, base + timedelta(seconds=1))]
        missing = [_source(2, base), _source(4, base + timedelta(seconds=1))]
        keep, delete = plan_snapper_retention_of(
            held + missing, DAILY_ONE, now=base + timedelta(seconds=2)
        )
        assert _numbers(keep) == [1, 4]
        assert _numbers(delete) == [2, 3]

    def test_all_four_in_one_second(self, zone):
        base = datetime(2026, 9, 24, 1, 2, 38)
        items = [_source(n, base) for n in (1, 2, 3, 4)]
        keep, _ = plan_snapper_retention_of(
            items, HOURLY, now=base + timedelta(seconds=9)
        )
        assert _numbers(keep) == [1, 4]


class TestEverySnapperRetentionPathAgrees:
    def test_the_prune_plan(self, zone):
        base = datetime(2026, 9, 24, 1, 2, 38)
        backups = [
            _backup(1, base),
            _backup(2, base),
            _backup(3, base + timedelta(seconds=1)),
            _backup(4, base + timedelta(seconds=1)),
        ]
        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups", return_value=backups
        ):
            keep, delete = plan_snapper_retention(
                "/bk", DAILY_ONE, now=base + timedelta(seconds=2)
            )
        assert _numbers(keep) == [1, 4]
        assert _numbers(delete) == [2, 3]

    def test_runs_catch_up_selection(self, zone):
        base = datetime(2026, 9, 24, 1, 2, 38)
        missing = [
            _source(1, base),
            _source(2, base),
            _source(3, base + timedelta(seconds=1)),
            _source(4, base + timedelta(seconds=1)),
        ]
        config = SimpleNamespace(get_target_retention=lambda v, t: DAILY_ONE)
        select = run_cli._snapper_catch_up_selector(
            SimpleNamespace(), config, SimpleNamespace(path="/bk"), {}
        )
        with (
            patch("btrfs_backup_ng.core.restore.list_snapper_backups", return_value=[]),
            patch(
                "btrfs_backup_ng.retention.datetime",
                SimpleNamespace(
                    now=lambda: base + timedelta(seconds=2), min=datetime.min
                ),
            ),
        ):
            chosen = select(missing)
        assert _numbers(chosen) == [1, 4]


class TestTheEngineParameter:
    def test_get_order_breaks_a_tie_the_name_cannot(self):
        """A caller that dates items itself and gives no order gets input
        order; one that gives creation order gets the newest kept."""
        base = datetime(2026, 9, 24, 1, 2, 38)
        items = [_source(n, base) for n in (1, 2, 3)]
        policy = RetentionConfig(
            min="0s", hourly=0, daily=0, weekly=0, monthly=0, keep=1
        )
        keep, _ = apply_retention(
            items,
            policy,
            get_name=lambda s: str(s.number),
            get_timestamp=lambda s: s.date,
            get_order=lambda s: s.number,
            now=base + timedelta(seconds=1),
        )
        assert _numbers(keep) == [3]
