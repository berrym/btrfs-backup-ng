"""A raw snapshot's time_obj must mean what every consumer reads it as: LOCAL.

``__util__.Snapshot.time_obj`` is a struct_time produced by parsing the
snapshot's NAME, which this project writes in local time. ``restore --before``
builds its target from a local-time string the operator typed, and the restore
listing renders time_obj with strftime beside that same name.

``RawSnapshot.created`` is UTC-aware at every site that sets it, and
``datetime.timetuple()`` on an aware datetime yields its OWN fields with the
offset simply discarded. Returning it directly therefore handed UTC fields to
code that reads them as local: on any non-UTC host, ``restore --before`` picked
the wrong backup for every raw target, and ``restore --list`` printed a time
disagreeing with the name on the same row by the host's UTC offset.

Every test here pins a non-UTC zone deliberately. On a UTC host the bug is
invisible, which is how it survived.
"""

from __future__ import annotations

import datetime as dt
import time
from pathlib import Path

import pytest

from btrfs_backup_ng.core.restore import find_snapshot_before_time
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

#: UTC-5 in winter, and never UTC.
ZONE = "America/New_York"


@pytest.fixture
def eastern(monkeypatch):
    """The process in US Eastern for one test, and back to whatever it was.

    The teardown used to delete TZ and call tzset: when the suite runs under
    an explicit zone (TZ=UTC is the gate), that left the process in the
    machine's own zone for every test after this file, which is exactly the
    setting the gate exists to rule out. ``undo`` puts TZ back to its previous
    state -- set or absent -- and tzset makes the process follow it.
    """
    monkeypatch.setenv("TZ", ZONE)
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


def test_the_zone_is_put_back_after_the_fixture(monkeypatch):
    """Drives the fixture by hand: the process zone after it is the one
    before it, with TZ set (the gate) and with TZ absent."""
    import os

    for previous in ("UTC", None):
        if previous is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", previous)
        time.tzset()
        before = time.tzname
        inner = pytest.MonkeyPatch()
        gen = eastern.__wrapped__(inner)
        next(gen)
        assert os.environ.get("TZ") == ZONE
        with pytest.raises(StopIteration):
            next(gen)
        assert os.environ.get("TZ") == previous
        assert time.tzname == before


def _raw(name, created):
    return RawSnapshot(name=name, stream_path=Path(f"/b/{name}.btrfs"), created=created)


def test_an_aware_utc_created_renders_as_local(eastern):
    """03:30 UTC on the 15th is 22:30 local on the 14th."""
    snap = _raw(
        "home.20240114-223000",
        dt.datetime(2024, 1, 15, 3, 30, tzinfo=dt.timezone.utc),
    )
    assert time.strftime("%Y-%m-%d %H:%M:%S", snap.time_obj) == "2024-01-14 22:30:00"


def test_the_rendered_time_agrees_with_the_name_beside_it(eastern):
    """restore --list prints both on one row; they disagreed by the UTC offset."""
    local = dt.datetime(2024, 1, 14, 22, 30)
    snap = _raw(
        f"home.{local.strftime('%Y%m%d-%H%M%S')}",
        local.astimezone().astimezone(dt.timezone.utc),
    )
    rendered = time.strftime("%Y%m%d-%H%M%S", snap.time_obj)
    assert snap.name.endswith(rendered), (
        f"name says {snap.name}, listing renders {rendered}"
    )


def test_a_naive_created_is_read_as_utc(eastern):
    """What it has always meant here; a legacy sidecar must not shift."""
    naive = _raw("home.x", dt.datetime(2024, 1, 15, 3, 30))
    aware = _raw("home.x", dt.datetime(2024, 1, 15, 3, 30, tzinfo=dt.timezone.utc))
    assert naive.time_obj == aware.time_obj


class TestRestoreBeforeSelectsTheRightBackup:
    """The consequence that costs an operator data during a recovery."""

    @staticmethod
    def _snapshots():
        # Three backups, 21:00 / 22:00 / 23:00 local on 2024-01-14.
        out = []
        for hour in (21, 22, 23):
            local = dt.datetime(2024, 1, 14, hour, 0)
            out.append(
                _raw(
                    f"home.20240114-{hour:02d}0000",
                    local.astimezone().astimezone(dt.timezone.utc),
                )
            )
        return out

    def test_the_backup_just_before_the_cutoff_is_chosen(self, eastern):
        target = time.strptime("2024-01-14 22:30:00", "%Y-%m-%d %H:%M:%S")

        chosen = find_snapshot_before_time(target, self._snapshots())

        assert chosen is not None, (
            "no backup was found before a cutoff that three of them precede"
        )
        assert chosen.name == "home.20240114-220000", (
            f"--before 22:30 selected {chosen.name}; with UTC fields read as "
            "local every backup looks five hours later than it is"
        )

    def test_a_cutoff_before_every_backup_selects_none(self, eastern):
        target = time.strptime("2024-01-14 20:00:00", "%Y-%m-%d %H:%M:%S")
        assert find_snapshot_before_time(target, self._snapshots()) is None

    def test_a_cutoff_after_every_backup_selects_the_newest(self, eastern):
        target = time.strptime("2024-01-15 06:00:00", "%Y-%m-%d %H:%M:%S")
        chosen = find_snapshot_before_time(target, self._snapshots())
        assert chosen is not None and chosen.name == "home.20240114-230000"


def test_ordering_against_a_btrfs_snapshot_is_still_well_defined(eastern):
    """The reason time_obj is a struct_time at all: raw and btrfs snapshots are
    sorted together. Both must now be in the same frame as well as the same type."""
    from btrfs_backup_ng import __util__

    local = dt.datetime(2024, 1, 14, 22, 30)
    raw = _raw("home.20240114-223000", local.astimezone().astimezone(dt.timezone.utc))
    btrfs = __util__.Snapshot(Path("/src"), "home.", None, time_obj=local.timetuple())
    assert raw.time_obj[:6] == btrfs.time_obj[:6], (
        "the same instant sorts differently depending on snapshot type"
    )
