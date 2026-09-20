"""A listing REMEMBERS the on-disk names it observed; nothing re-renders them.

strptime is more lenient than strftime: '2026-9-8_020304' parses under
%Y-%m-%d_%H%M%S, but re-rendering pads it to '2026-09-08_020304'. A snapshot
whose name was rebuilt from its timestamp therefore resolved to a path that is
not on disk -- and prune deletes, locks and verifies by that path. These tests
drive the REAL local listing over a real directory and pin that every name a
listing returns is the string the filesystem holds.
"""

from __future__ import annotations

import subprocess
import time

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.local import LocalEndpoint

FMT = "%Y-%m-%d_%H%M%S"
OBSERVED = "home.2026-9-8_020304"  # parses under FMT, does NOT round-trip
CANONICAL = "home.2026-09-08_020305"  # parses and round-trips


@pytest.fixture(autouse=True)
def _no_btrfs(monkeypatch):
    """Simulate a non-btrfs environment: uuid enrichment misses, best-effort.

    This patches the ENVIRONMENT (there is no btrfs on a tmpdir), not the unit
    under test -- the unit is the listing's name threading.
    """

    def fail_run(argv, *a, **k):
        return subprocess.CompletedProcess(argv, 1, stdout="", stderr="not btrfs")

    monkeypatch.setattr("btrfs_backup_ng.endpoint.common.subprocess.run", fail_run)


def _endpoint(tmp_path):
    return LocalEndpoint(
        config={
            "path": tmp_path,
            "source": "/src",
            "snapshot_folder": ".snapshots",
            "snap_prefix": "home.",
            "timestamp_format": FMT,
        }
    )


def test_fixture_name_does_not_round_trip():
    """Self-check FIRST: if strptime ever stops accepting single-digit fields,
    every test built on OBSERVED silently stops testing the remembered name.
    This makes that failure loud instead."""
    time_obj, matched = __util__.parse_snapshot_time(OBSERVED[len("home.") :], FMT)
    assert "home." + time.strftime(matched, time_obj) != OBSERVED


def test_a_listing_returns_the_names_it_observed(tmp_path):
    """Every listed name is the on-disk string, and every listed path exists.
    Mutation guards: a get_name() that re-renders, or a listing that drops the
    observed name, yields 'home.2026-09-08_020304' here -- a path that is not
    on disk."""
    (tmp_path / OBSERVED).mkdir()
    (tmp_path / CANONICAL).mkdir()
    snaps = _endpoint(tmp_path).list_snapshots()

    assert {s.get_name() for s in snaps} == {OBSERVED, CANONICAL}
    for snap in snaps:
        assert snap.get_path().exists(), (
            f"{snap.get_name()!r} resolves to {snap.get_path()!r}, which is not on disk"
        )


def test_add_snapshot_carries_the_observed_name_into_the_cache(tmp_path):
    """add_snapshot rewrites the snapshot onto this endpoint; the rewrite must
    carry the NAME, not rebuild one from (time_obj, format)."""
    (tmp_path / CANONICAL).mkdir()
    ep = _endpoint(tmp_path)
    ep.list_snapshots()  # populate the cache so add_snapshot appends

    time_obj, _ = __util__.parse_snapshot_time(OBSERVED[len("home.") :], FMT)
    incoming = __util__.Snapshot(
        tmp_path, "home.", ep, time_obj=time_obj, name=OBSERVED
    )
    ep.add_snapshot(incoming)

    names = {s.get_name() for s in ep.list_snapshots()}
    assert OBSERVED in names, "the rewrite dropped the observed name"


def test_a_lock_set_under_an_observed_name_survives_a_fresh_listing(tmp_path):
    """Locks are keyed by get_name(). With the observed name as the key, a
    lock written for a non-round-tripping name reattaches after the cache is
    flushed and the directory is listed again from scratch."""
    (tmp_path / OBSERVED).mkdir()
    ep = _endpoint(tmp_path)
    snap = ep.list_snapshots()[0]
    ep.set_lock(snap, "test-lock", True)

    fresh = ep.list_snapshots(flush_cache=True)[0]
    assert fresh.get_name() == OBSERVED
    assert "test-lock" in fresh.locks


def test_the_ssh_listing_remembers_the_observed_name():
    """The ssh twin of the local listing: _parse_snapshot_list must hand the
    snapshot the exact name from the `btrfs subvolume list` line, not one
    rebuilt from its parsed timestamp."""
    from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

    dest = "/backups/home"
    output = f"ID 258 gen 5 top level 5 path backups/home/{OBSERVED}\n"
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {
        "path": dest,
        "hostname": "nas",
        "username": "backup",
        "snap_prefix": "home.",
        "timestamp_format": FMT,
    }
    ep.hostname = "nas"

    parsed = ep._parse_snapshot_list(output, dest)

    assert [s.get_name() for s in parsed] == [OBSERVED]


def test_snapper_metadata_is_named_after_the_name_the_backup_used(tmp_path):
    """_write_snapper_metadata names the sidecar after the backup_name the
    caller passes -- the ONE string rendered when the transfer wrapper was
    built. Discriminating fixture: the snapshot's own get_backup_name()
    returns a DIFFERENT string, so a mutation that re-renders inside the
    function names the file after the wrong string and fails here."""
    from datetime import datetime
    from unittest.mock import MagicMock

    from btrfs_backup_ng.core.operations import _write_snapper_metadata
    from btrfs_backup_ng.snapper.metadata import SnapperMetadata
    from btrfs_backup_ng.snapper.snapshot import SnapperSnapshot

    info_xml = tmp_path / "info.xml"
    info_xml.write_text("<snapshot/>")
    snap = SnapperSnapshot(
        config_name="root",
        number=7,
        metadata=SnapperMetadata(
            type="single", num=7, date=datetime(2024, 1, 15, 14, 30, 22)
        ),
        subvolume_path=tmp_path,
        info_xml_path=info_xml,
    )
    dest = MagicMock()
    dest.config = {"path": str(tmp_path)}
    dest._is_remote = False
    transferred_as = "root-7-THE-TRANSFERRED-NAME"
    assert snap.get_backup_name() != transferred_as  # the fixture discriminates

    _write_snapper_metadata(snap, dest, transferred_as)

    assert (tmp_path / f"{transferred_as}.snapper-meta.json").exists()
