"""A snapshot name carrying a UTC offset must regenerate byte-identically.

`timestamp_format` is a free-form strftime string, and `%z` is a standard
directive -- `config import` emits `%Y%m%dT%H%M%S%z` for btrbk's `long-iso`, so
this is reachable from a shipped feature, not a hypothetical.

`time.strftime` renders `%z` from `tm_zone`, which `time.strptime` leaves as
None. So a name parsed FROM disk lost its offset on re-render, and since
`Snapshot.get_name()` regenerates the on-disk name and `get_path()` builds a
path from it, every caller resolving a path from a Snapshot -- delete, lock,
send, verify -- was pointed at a name that does not exist.
"""

import time

import pytest

from btrfs_backup_ng.__util__ import DATE_FORMAT, Snapshot, date_to_str

OFFSET_FMT = "%Y%m%dT%H%M%S%z"


@pytest.mark.parametrize(
    "name",
    ["20260919T011855-0400", "20260101T120000+0100", "20260615T235959+0000"],
)
def test_an_offset_survives_the_round_trip(name):
    assert date_to_str(time.strptime(name, OFFSET_FMT), OFFSET_FMT) == name


def test_the_path_a_snapshot_reports_is_the_one_it_came_from(tmp_path):
    """The consequence that matters: prune deletes by get_path()."""
    name = "20260919T011855-0400"
    snap = Snapshot(
        tmp_path,
        "home.",
        None,
        time_obj=time.strptime(name, OFFSET_FMT),
        time_format=OFFSET_FMT,
    )
    on_disk = tmp_path / f"home.{name}"
    on_disk.mkdir()

    assert snap.get_name() == f"home.{name}"
    assert snap.get_path() == on_disk
    assert snap.get_path().exists(), "the snapshot resolves to a path that is not there"


def test_a_new_snapshot_is_named_with_its_offset():
    """Snapshot.__init__ used to round-trip its time through DATE_FORMAT, which
    has no %z, so strptime returned tm_gmtoff=None and a freshly created
    snapshot was named without the offset its own format asked for."""
    snap = Snapshot("/snaps", "home.", None, time_format=OFFSET_FMT)
    rendered = snap.get_name()

    assert rendered[-5] in "+-", f"no offset in {rendered!r}"
    assert time.strptime(rendered[len("home.") :], OFFSET_FMT)


def test_the_default_format_is_untouched():
    """Every existing pool uses DATE_FORMAT, which has no %z; those names must
    render exactly as before."""
    snap = Snapshot("/snaps", "home.", None)
    name = snap.get_name()

    assert name.startswith("home.")
    assert time.strptime(name[len("home.") :], DATE_FORMAT)
    assert "+" not in name and name.count("-") == 1


def test_a_leap_second_is_named_rather_than_refused():
    """datetime rejects tm_sec == 60 where strftime accepts it. Naming must not
    fail over rendering an offset."""
    leap = time.struct_time((2016, 12, 31, 23, 59, 60, 5, 366, 0, "UTC", 0))

    assert date_to_str(leap, OFFSET_FMT).startswith("20161231T235960")
