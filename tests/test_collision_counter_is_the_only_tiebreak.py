"""Two snapshots sharing a timestamp order by their collision counter -- and
only by a real one.

The counter is the ``_N`` btrbk and this tool append when a name recurs under
a coarse ``timestamp_format``. It was read off the END of the whole name, so
any name ending in digits carried one: the ``%H`` of ``%Y%m%d_%H``, the
``%S`` of ``%Y%m%d_%H%M%S``, the whole date when the PREFIX ends in ``_``. On
a timestamp tie the bare name (counter 14, or 120000, or 20260923) then
sorted NEWER than its ``_1``, ``_2`` siblings, so the first snapshot created
was "latest" and the last one created was the one retention deleted -- under
an hourly format with a 15-minute timer, the two newest snapshots on the
machine, every hour. The counter is now read from the part of the name after
the timestamp, and only when the name parsed with a trailing ``_N`` removed.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.retention import _date_and_counter, apply_retention

DAILY = RetentionConfig(min="0s", hourly=0, daily=7, weekly=0, monthly=0, yearly=0)
HOURLY = RetentionConfig(min="0s", hourly=48, daily=0, weekly=0, monthly=0, yearly=0)


class TestTheCounterIsReadOffTheRemainder:
    @pytest.mark.parametrize(
        "name,prefix,fmt,expected",
        [
            ("home-20260923", "home-", "%Y%m%d", 0),
            ("home-20260923_1", "home-", "%Y%m%d", 1),
            ("home-20260923_12", "home-", "%Y%m%d", 12),
            # A prefix ending in "_": the date is not a counter.
            ("home_20260923", "home_", "%Y%m%d", 0),
            ("home_20260923_2", "home_", "%Y%m%d", 2),
            # A format ending in digits after an underscore: the hour is not a counter.
            ("home-20260923_14", "home-", "%Y%m%d_%H", 0),
            ("home-20260923_14_3", "home-", "%Y%m%d_%H", 3),
            ("home-20260923_120000", "home-", "%Y%m%d_%H%M%S", 0),
            ("home-20260923_120000_2", "home-", "%Y%m%d_%H%M%S", 2),
            ("home-20260923-143022", "home-", None, 0),
            ("home-20260923-143022_7", "home-", None, 7),
        ],
    )
    def test_retention_reads_it(self, name, prefix, fmt, expected):
        when, counter = _date_and_counter(name, prefix, fmt)
        assert when is not None
        assert counter == expected

    def test_a_name_that_parsed_as_written_carries_no_counter(self):
        parsed = __util__.derive_snapshot_time("20260923", "%Y%m%d")[0]
        assert __util__.collision_counter("20260923_14", parsed, True) == 0
        assert __util__.collision_counter("20260923_14", parsed, False) == 14
        assert __util__.collision_counter("notadate_14", None, False) == 0


def _keep_delete(names, policy, prefix, fmt, now):
    keep, delete = apply_retention(
        names, policy, prefix=prefix, timestamp_format=fmt, now=now
    )
    return set(keep), set(delete)


class TestTheNewestIsNeverTheOneDeleted:
    def test_a_prefix_ending_in_underscore(self):
        """Three runs in one day under %Y%m%d: the day and its _2 stay, _1 goes
        -- the same answer as with a prefix ending in '-'."""
        names = ["home_20260923", "home_20260923_1", "home_20260923_2"]
        keep, delete = _keep_delete(
            names, DAILY, "home_", "%Y%m%d", datetime(2026, 9, 23, 23, 0)
        )
        assert keep == {"home_20260923", "home_20260923_2"}
        assert delete == {"home_20260923_1"}

    def test_an_hourly_format_with_a_quarter_hour_timer(self):
        names = [
            "home-20260923_13",
            "home-20260923_14",
            "home-20260923_14_1",
            "home-20260923_14_2",
            "home-20260923_14_3",
        ]
        keep, delete = _keep_delete(
            names, HOURLY, "home-", "%Y%m%d_%H", datetime(2026, 9, 23, 14, 50)
        )
        # The oldest of hour 14 (its bucket representative) and the newest
        # snapshot of all (_3) stay; the two in between go.
        assert keep == {"home-20260923_13", "home-20260923_14", "home-20260923_14_3"}
        assert delete == {"home-20260923_14_1", "home-20260923_14_2"}

    def test_a_same_second_collision(self):
        names = [
            "home-20260923_120000",
            "home-20260923_120000_1",
            "home-20260923_120000_2",
        ]
        keep, delete = _keep_delete(
            names, DAILY, "home-", "%Y%m%d_%H%M%S", datetime(2026, 9, 23, 23, 0)
        )
        assert keep == {"home-20260923_120000", "home-20260923_120000_2"}
        assert delete == {"home-20260923_120000_1"}

    def test_the_documented_case_still_holds(self):
        names = ["home-20260923", "home-20260923_1", "home-20260923_2"]
        keep, delete = _keep_delete(
            names, DAILY, "home-", "%Y%m%d", datetime(2026, 9, 23, 23, 0)
        )
        assert keep == {"home-20260923", "home-20260923_2"}
        assert delete == {"home-20260923_1"}


class TestTheListingOrdersTheSameWay:
    def _snapshot(self, tmp_path, prefix, name, fmt):
        from types import SimpleNamespace

        rest = name[len(prefix) :]
        time_obj, as_written = __util__.derive_snapshot_time(rest, fmt)
        endpoint = SimpleNamespace(config={"timestamp_format": fmt})
        snap = __util__.Snapshot(
            tmp_path, prefix, endpoint, time_obj=time_obj, name=name
        )
        snap.newly_visible = not as_written
        return snap

    def test_the_bare_name_sorts_before_its_counters(self, tmp_path):
        a, b, c = (
            self._snapshot(tmp_path, "home_", n, "%Y%m%d")
            for n in ("home_20260923_2", "home_20260923", "home_20260923_1")
        )
        assert [s.get_name() for s in sorted([a, b, c])] == [
            "home_20260923",
            "home_20260923_1",
            "home_20260923_2",
        ]

    def test_an_hour_field_is_not_a_counter_in_the_listing_either(self, tmp_path):
        x, x1 = (
            self._snapshot(tmp_path, "home-", n, "%Y%m%d_%H")
            for n in ("home-20260923_14", "home-20260923_14_1")
        )
        assert sorted([x1, x]) == [x, x1]
        assert x.collision_counter() == 0 and x1.collision_counter() == 1
