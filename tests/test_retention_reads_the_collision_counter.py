"""Retention reads a trailing ``_N`` the way every listing does.

A second snapshot in one period under a coarse ``timestamp_format`` (or a
scheduler firing twice, or a pool migrated from btrbk) is named with a
collision counter: ``home-20260923``, ``home-20260923_1``, ``home-20260923_2``.
Listings, transfers and restores dated those by the timestamp before the
counter, and said retention now managed them -- but retention had its own
parser, found no timestamp in ``20260923_1``, and kept every such snapshot
forever, on the source and on every target. It now dates them the same way,
after the name as written has failed, and snapshots sharing a timestamp order
by their counter, so "latest" is the last one created.
"""

from __future__ import annotations

import itertools
from datetime import datetime

import pytest

from btrfs_backup_ng.config import RetentionConfig
from btrfs_backup_ng.retention import apply_retention, extract_timestamp

DAY = RetentionConfig(min="0s", hourly=0, daily=7, weekly=0, monthly=0, yearly=0)


class TestDating:
    @pytest.mark.parametrize("suffix", ["_1", "_2", "_12", "_999"])
    def test_a_counter_is_dated_by_the_timestamp_before_it(self, suffix):
        assert extract_timestamp(
            f"home-20260923{suffix}", "home-", "%Y%m%d"
        ) == datetime(2026, 9, 23)

    def test_a_timestamp_ending_in_digits_keeps_its_meaning(self):
        # As written first: under a format that has the underscore this is
        # noon, never midnight with ordinal 120000 -- to retention exactly as
        # to the listing.
        assert extract_timestamp(
            "home-20260904_120000", "home-", "%Y%m%d_%H%M%S"
        ) == datetime(2026, 9, 4, 12, 0, 0)
        assert extract_timestamp("home-20240115-143022", "home-") == datetime(
            2024, 1, 15, 14, 30, 22
        )

    def test_retention_reads_a_digit_suffix_as_the_listing_does(self):
        """Under ``%Y%m%d`` the name ``20260904_120000`` does not parse as
        written, so both the listing and retention read it as the 4th with a
        collision counter. Retention used to search the name for eight digits,
        a separator and six more, and called this noon while ``list`` showed
        midnight -- two dates for one snapshot."""
        from btrfs_backup_ng import __util__

        time_obj, _ = __util__.derive_snapshot_time("20260904_120000", "%Y%m%d")
        assert time_obj is not None
        listing_says = datetime(*time_obj[:6])
        assert listing_says == datetime(2026, 9, 4)
        assert (
            extract_timestamp("home-20260904_120000", "home-", "%Y%m%d") == listing_says
        )

    def test_the_default_format_with_a_counter(self):
        assert extract_timestamp("home-20260923-143022_2", "home-") == datetime(
            2026, 9, 23, 14, 30, 22
        )

    def test_a_name_with_no_timestamp_is_still_none(self):
        assert extract_timestamp("home-notadate_3", "home-", "%Y%m%d") is None


class _S:
    def __init__(self, name):
        self.name = name

    def get_name(self):
        return self.name


def _run(names, policy=DAY):
    keep, delete = apply_retention(
        [_S(n) for n in names],
        policy,
        get_name=lambda s: s.get_name(),
        prefix="home-",
        timestamp_format="%Y%m%d",
        now=datetime(2026, 9, 23, 23, 0, 0),
    )
    return {s.name for s in keep}, {s.name for s in delete}


class TestRetention:
    SAME_DAY = ["home-20260923", "home-20260923_1", "home-20260923_2"]

    @pytest.mark.parametrize("order", list(itertools.permutations(SAME_DAY)))
    def test_the_latest_and_the_day_s_first_are_kept_whatever_the_order(self, order):
        keep, delete = _run(list(order))
        assert keep == {"home-20260923", "home-20260923_2"}
        assert delete == {"home-20260923_1"}

    def test_counter_named_snapshots_are_no_longer_kept_forever(self):
        names = [f"home-202609{d:02d}" for d in range(1, 21)]
        names += [f"home-202609{d:02d}_1" for d in range(1, 21)]
        keep, delete = _run(names)
        # Seven daily buckets keep each day's FIRST snapshot, plus the latest
        # overall; every other counter-named snapshot is deleted -- before,
        # all twenty were kept for ever.
        assert keep == {f"home-202609{d:02d}" for d in range(14, 21)} | {
            "home-20260920_1"
        }
        assert {n for n in delete if n.endswith("_1")} == {
            f"home-202609{d:02d}_1" for d in range(1, 20)
        }

    def test_the_counter_orders_numerically_not_as_text(self):
        # _10 is newer than _9; as strings "_10" < "_9".
        keep, _ = _run(["home-20260923", "home-20260923_9", "home-20260923_10"])
        assert "home-20260923_10" in keep
