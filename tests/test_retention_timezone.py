"""Retention must not meet a timezone-aware timestamp it cannot compare.

A `timestamp_format` containing `%z` makes strptime return an AWARE datetime,
while every comparison in `retention` is against a naive `datetime.now()`. The
result was an uncaught `TypeError: can't compare offset-naive and offset-aware
datetimes` -- inside a destructive path.

This is reachable from a shipped feature: `config import` maps btrbk's
`long-iso` to `%Y%m%dT%H%M%S%z` and writes it verbatim, so importing such a
config produced one whose prune could never run.
"""

from datetime import datetime

import pytest

from btrfs_backup_ng.retention import extract_timestamp

OFFSET_FMT = "%Y%m%dT%H%M%S%z"


@pytest.mark.parametrize(
    "name",
    [
        "home.20260101T120000+0100",
        "home.20260101T120000-0500",
        "home.20260101T120000+0000",
    ],
)
def test_an_offset_timestamp_comes_back_naive(name):
    ts = extract_timestamp(name, "home.", OFFSET_FMT)

    assert ts is not None
    assert ts.tzinfo is None, "an aware datetime reaches the retention math"


def test_it_can_be_compared_with_now():
    """The exact comparison apply_retention makes against future_cutoff."""
    ts = extract_timestamp("home.20260101T120000+0100", "home.", OFFSET_FMT)

    ts > datetime.now()  # must not raise


def test_the_instant_is_preserved_not_just_stripped():
    """Stripping tzinfo would make 12:00+0100 and 12:00-0500 the same moment.
    They are six hours apart, and retention orders snapshots by these."""
    east = extract_timestamp("home.20260101T120000+0100", "home.", OFFSET_FMT)
    west = extract_timestamp("home.20260101T120000-0500", "home.", OFFSET_FMT)

    assert east < west, "two zones collapsed to the same local time"
    assert (west - east).total_seconds() == 6 * 3600


def test_the_config_import_format_is_the_one_that_was_broken():
    """Guard the actual reachability, not a hypothetical format."""
    from btrfs_backup_ng import btrbk_import

    fmt = btrbk_import.BTRBK_TIMESTAMP_FORMATS["long-iso"]
    assert "%z" in fmt

    ts = extract_timestamp("home.20260101T120000+0100", "home.", fmt)
    assert ts is not None and ts.tzinfo is None


@pytest.mark.parametrize(
    "name,fmt",
    [("home.20260101-120000", "%Y%m%d-%H%M%S"), ("home.20260101-120000", None)],
)
def test_naive_formats_are_unchanged(name, fmt):
    ts = extract_timestamp(name, "home.", fmt)

    assert ts is not None and ts.tzinfo is None
    assert ts.hour == 12
