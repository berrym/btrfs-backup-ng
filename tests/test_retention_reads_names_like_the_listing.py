"""Retention dates a snapshot exactly as the listing does.

Every listing derives a snapshot's time from its name with one rule
(``__util__.derive_snapshot_time``): the configured format, then the default
format, then either with one trailing collision counter removed. Retention
used to keep a second parser with a list of guessed formats and an unanchored
search for digits, and the two disagreed: under ``timestamp_format =
"%Y%m%d"`` the name ``20260921_1000000`` is the 21st to the listing and
10:00:00 to retention, because the search matched six of the seven counter
digits. Retention now reads through the listing's rule, and this drives both
over a broad set of generated names and formats to hold them equal.
"""

from __future__ import annotations

import random
import time
import zlib
from datetime import datetime, timedelta, timezone

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.retention import extract_timestamp

FORMATS = [
    __util__.DATE_FORMAT,
    "%Y%m%d",
    "%Y%m%dT%H%M",
    "%Y%m%dT%H%M%S",
    "%Y-%m-%d_%H%M%S",
    "%Y-%m-%d",
    "%Y%m%d%H%M%S",
    "%Y%m%dT%H%M%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "snap_%Y%m%d_%H%M%S",
]
PREFIXES = ["", "home-", "home.", "p_", "root-snapshots-"]
ORDINALS = [None, 1, 2, 12, 999, 100000, 1000000, 999999999]


def _listing_time(rest: str, fmt: str | None) -> datetime | None:
    """What a listing records for the part of the name after the prefix."""
    time_obj, _ = __util__.derive_snapshot_time(rest, fmt)
    if time_obj is None:
        return None
    when = datetime(*time_obj[:6])
    if time_obj.tm_gmtoff is not None:
        when = when.replace(tzinfo=timezone(timedelta(seconds=time_obj.tm_gmtoff)))
        when = when.astimezone().replace(tzinfo=None)
    return when


def _moments(seed: int, count: int) -> list[datetime]:
    rng = random.Random(seed)
    base = datetime(2020, 1, 1)
    return [
        base + timedelta(seconds=rng.randint(0, 6 * 365 * 24 * 3600))
        for _ in range(count)
    ]


class TestTheTwoAgree:
    @pytest.mark.parametrize("fmt", FORMATS, ids=lambda f: f.replace("%", ""))
    @pytest.mark.parametrize("prefix", PREFIXES, ids=lambda p: p or "bare")
    def test_over_generated_names(self, fmt, prefix):
        for moment in _moments(zlib.crc32(f"{fmt}|{prefix}".encode()), 40):
            if "%z" in fmt:
                moment = moment.replace(tzinfo=timezone(timedelta(hours=2)))
            stamp = moment.strftime(fmt)
            for ordinal in ORDINALS:
                rest = stamp if ordinal is None else f"{stamp}_{ordinal}"
                name = prefix + rest
                assert extract_timestamp(name, prefix, fmt) == _listing_time(
                    rest, fmt
                ), (
                    name,
                    fmt,
                )

    @pytest.mark.parametrize("fmt", FORMATS, ids=lambda f: f.replace("%", ""))
    def test_under_another_configured_format(self, fmt):
        """A name made under one format, read under another: still one answer."""
        for other in FORMATS:
            for moment in _moments(zlib.crc32(f"{fmt}|{other}".encode()), 8):
                if "%z" in fmt:
                    moment = moment.replace(tzinfo=timezone(timedelta(hours=-5)))
                rest = moment.strftime(fmt)
                assert extract_timestamp("h-" + rest, "h-", other) == _listing_time(
                    rest, other
                ), (rest, other)

    def test_a_dated_listing_is_never_undated_here(self):
        """Whatever the listing dates, retention dates the same; whatever it
        leaves undated, retention leaves undated -- a snapshot ``list`` calls
        "unknown" is never deleted by a date retention alone believes in."""
        for fmt in FORMATS:
            for name in ("home-notadate_3", "home-2024", "home-", "home-x_1"):
                assert extract_timestamp(name, "home-", fmt) is None
                assert _listing_time(name[len("home-") :], fmt) is None


class TestTheSevenDigitCounter:
    def test_it_is_the_day(self):
        listing = _listing_time("20260921_1000000", "%Y%m%d")
        assert listing == datetime(2026, 9, 21)
        assert extract_timestamp("20260921_1000000", "", "%Y%m%d") == listing

    def test_a_six_digit_counter_is_the_day_too(self):
        """Six digits after the underscore looked like a time to the old search
        and is a counter to the listing."""
        listing = _listing_time("20260921_100000", "%Y%m%d")
        assert listing == datetime(2026, 9, 21)
        assert extract_timestamp("home-20260921_100000", "home-", "%Y%m%d") == listing


class TestNothingIsGuessed:
    def test_no_format_outside_the_rule_is_tried(self):
        """Formats the old parser guessed at parse in neither place now."""
        for rest in ("20240115_143022", "20240115143022", "2024-01-15T14:30:22"):
            assert _listing_time(rest, None) is None
            assert extract_timestamp("p-" + rest, "p-") is None

    def test_digits_inside_a_name_are_not_a_timestamp(self):
        assert extract_timestamp("p-x20240115-143022y", "p-") is None

    def test_offset_names_come_back_naive_and_ordered(self):
        fmt = "%Y%m%dT%H%M%S%z"
        east = extract_timestamp("h.20260101T120000+0100", "h.", fmt)
        west = extract_timestamp("h.20260101T120000-0500", "h.", fmt)
        assert east is not None and west is not None
        assert east.tzinfo is None and west.tzinfo is None
        assert (west - east).total_seconds() == 6 * 3600
        assert east == _listing_time("20260101T120000+0100", fmt)

    def test_struct_time_without_offset_has_no_gmtoff(self):
        """The conversion relies on ``tm_gmtoff`` being None without ``%z``."""
        assert time.strptime("20240115-143022", __util__.DATE_FORMAT).tm_gmtoff is None
