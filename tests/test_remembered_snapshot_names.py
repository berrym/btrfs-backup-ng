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
from pathlib import Path

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


# --------------------------------------------------------------------------- #
# Phase C: the listing separation -- what something IS decides whether it is a
# snapshot; whether its name parses decides only whether it has a timestamp.
# --------------------------------------------------------------------------- #

ORDINAL = "home.2026-09-08_020306_1"  # dated via one stripped _N
WEIRD = "home.imported-base"  # a subvolume with no derivable timestamp


@pytest.fixture()
def endpoint_log():
    """Capture records from the endpoint modules' shared logger.

    That logger (``btrfs_backup_ng.__logger__.logger``) is a standalone
    ``logging.Logger`` outside the manager tree: it does not propagate to
    root, so ``caplog`` sees nothing from it whether a message is emitted or
    not -- an assertion built on caplog here cannot fail. A handler attached
    directly to it can.
    """
    import logging

    from btrfs_backup_ng.__logger__ import logger as pkg_logger

    class _Capture(logging.Handler):
        def __init__(self):
            super().__init__(level=logging.DEBUG)
            self.records = []

        def emit(self, record):
            self.records.append(record)

    handler = _Capture()
    previous_level = pkg_logger.level
    pkg_logger.addHandler(handler)
    pkg_logger.setLevel(logging.INFO)
    try:
        yield handler
    finally:
        pkg_logger.removeHandler(handler)
        pkg_logger.setLevel(previous_level)


def _mixed_pool(tmp_path, monkeypatch):
    """A directory holding every kind of entry, with subvolume-ness injected
    (tmpdirs cannot hold real subvolumes; the inode probe is the ENVIRONMENT,
    not the unit under test -- tier2 runs the same scenario on real btrfs)."""
    for name in (CANONICAL, ORDINAL, WEIRD, "lost+found"):
        (tmp_path / name).mkdir()
    (tmp_path / "README.md").write_text("not a snapshot")
    subvols = {CANONICAL, ORDINAL, WEIRD}
    monkeypatch.setattr(
        "btrfs_backup_ng.__util__.is_subvolume",
        lambda path: Path(path).name in subvols,
    )
    return _endpoint(tmp_path)


def test_a_subvolume_is_a_snapshot_and_clutter_is_not(tmp_path, monkeypatch):
    """The pincer, unit-level. List-everything fails on README.md/lost+found
    being absent; list-nothing-new fails on ORDINAL/WEIRD being present. The
    empty-prefix landmine (tests/test_explicit_empty_prefix.py) and the
    diagnosis landmine (tests/test_empty_listing_diagnosis.py) run unmodified
    beside this."""
    ep = _mixed_pool(tmp_path, monkeypatch)
    snaps = {s.get_name(): s for s in ep.list_snapshots()}

    assert set(snaps) == {CANONICAL, ORDINAL, WEIRD}
    assert "README.md" not in snaps
    assert "lost+found" not in snaps

    # ORDINAL: dated by stripping one trailing _N, and marked newly visible.
    assert snaps[ORDINAL].time_obj is not None
    assert time.strftime(FMT, snaps[ORDINAL].time_obj) == "2026-09-08_020306"
    assert snaps[ORDINAL].newly_visible

    # WEIRD: a snapshot with no extractable timestamp -- listed, marked.
    assert snaps[WEIRD].time_obj is None
    assert snaps[WEIRD].newly_visible

    # CANONICAL: visible to every release, not marked.
    assert not snaps[CANONICAL].newly_visible


def test_with_an_empty_prefix_only_the_inode_check_separates_clutter(
    tmp_path, monkeypatch
):
    """The true pincer tooth. With the supported empty prefix (issue #6) EVERY
    entry matches the prefix filter, so is_subvolume() is the ONLY thing
    keeping README.md and lost+found out of the listing. A gate that lists
    everything fails here; a gate that skips every unparseable name fails on
    'importbase' being absent. (The prefixed variant above cannot catch the
    first failure -- its clutter never reaches the gate.)"""
    (tmp_path / "2026-09-08_020310").mkdir()  # bare timestamp: parses
    (tmp_path / "importbase").mkdir()  # subvolume, no timestamp
    (tmp_path / "lost+found").mkdir()  # plain directory
    (tmp_path / "README.md").write_text("not a snapshot")
    subvols = {"2026-09-08_020310", "importbase"}
    monkeypatch.setattr(
        "btrfs_backup_ng.__util__.is_subvolume",
        lambda path: Path(path).name in subvols,
    )
    ep = LocalEndpoint(
        config={
            "path": tmp_path,
            "source": "/src",
            "snapshot_folder": ".snapshots",
            "snap_prefix": "",
            "timestamp_format": FMT,
        }
    )
    names = {s.get_name() for s in ep.list_snapshots()}
    assert names == {"2026-09-08_020310", "importbase"}
    assert "README.md" not in names
    assert "lost+found" not in names


def test_a_timestamp_less_snapshot_sorts_last(tmp_path, monkeypatch):
    """Unknown age is never treated as old: "oldest" is where count-based
    deletion slices from."""
    ep = _mixed_pool(tmp_path, monkeypatch)
    names = [s.get_name() for s in ep.list_snapshots()]
    assert names[-1] == WEIRD


def test_newly_visible_snapshots_are_announced(tmp_path, monkeypatch, endpoint_log):
    """Reported, never silent: the first run that can see a foreign pool says
    so at INFO, naming the snapshots, BEFORE any deletion surface does.
    Mutation guard: removing the announcement leaves this log empty."""
    ep = _mixed_pool(tmp_path, monkeypatch)
    ep.list_snapshots()
    announcement = [
        r.getMessage()
        for r in endpoint_log.records
        if "not visible to earlier" in r.getMessage()
    ]
    assert len(announcement) == 1
    assert ORDINAL in announcement[0]
    assert WEIRD in announcement[0]
    assert CANONICAL not in announcement[0]


def test_an_all_canonical_pool_is_not_announced(tmp_path, monkeypatch, endpoint_log):
    """The announcement is for pools that CHANGED meaning under this release;
    an ordinary pool stays quiet. Fixture self-check: the same capture is
    proven able to see announcements by the positive test above -- this
    absence assertion is not blind."""
    (tmp_path / CANONICAL).mkdir()
    ep = _endpoint(tmp_path)
    ep.list_snapshots()
    assert not [
        r for r in endpoint_log.records if "not visible to earlier" in r.getMessage()
    ]


def test_a_lock_on_a_timestamp_less_snapshot_survives_relisting(tmp_path, monkeypatch):
    """Locks key by name, and a name needs no timestamp to be a stable key."""
    ep = _mixed_pool(tmp_path, monkeypatch)
    weird = next(s for s in ep.list_snapshots() if s.get_name() == WEIRD)
    ep.set_lock(weird, "test-lock", True)
    fresh = next(
        s for s in ep.list_snapshots(flush_cache=True) if s.get_name() == WEIRD
    )
    assert "test-lock" in fresh.locks


def test_the_ssh_listing_separates_the_same_way():
    """The ssh twin: every candidate line IS a subvolume, so an unparseable
    prefix-matching name is a timestamp-less snapshot, and a stripped _N
    yields a dated one -- both marked newly visible."""
    from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

    dest = "/backups/home"
    output = (
        f"ID 258 gen 5 top level 5 path backups/home/{OBSERVED}\n"
        f"ID 259 gen 6 top level 5 path backups/home/{ORDINAL}\n"
        f"ID 260 gen 7 top level 5 path backups/home/{WEIRD}\n"
    )
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {
        "path": dest,
        "hostname": "nas",
        "username": "backup",
        "snap_prefix": "home.",
        "timestamp_format": FMT,
    }
    ep.hostname = "nas"

    parsed = {s.get_name(): s for s in ep._parse_snapshot_list(output, dest)}

    assert set(parsed) == {OBSERVED, ORDINAL, WEIRD}
    assert not parsed[OBSERVED].newly_visible
    assert parsed[ORDINAL].time_obj is not None
    assert parsed[ORDINAL].newly_visible
    assert parsed[WEIRD].time_obj is None
    assert parsed[WEIRD].newly_visible


def test_count_based_retention_neither_counts_nor_deletes_the_undated(
    tmp_path, monkeypatch, endpoint_log
):
    """delete_old_snapshots keeps "the newest N". A timestamp-less snapshot
    sorts LAST, so without the partition it would occupy a keep slot and push
    a real, dated snapshot into the delete slice -- deleting MORE real
    backups than the operator asked to lose. Mutation guard: dropping the
    partition deletes two dated snapshots here instead of one, and the
    exclusion INFO disappears."""
    for name in (
        "home.2026-09-06_020306",
        "home.2026-09-07_020306",
        CANONICAL,
        WEIRD,
    ):
        (tmp_path / name).mkdir()
    subvols = {WEIRD}
    monkeypatch.setattr(
        "btrfs_backup_ng.__util__.is_subvolume",
        lambda path: Path(path).name in subvols,
    )
    ep = _endpoint(tmp_path)
    deleted_batches = []
    monkeypatch.setattr(
        ep,
        "delete_snapshots",
        lambda snaps, **kw: (
            deleted_batches.append([s.get_name() for s in snaps])
            or __import__(
                "btrfs_backup_ng.endpoint.common", fromlist=["DeletionResult"]
            ).DeletionResult()
        ),
    )
    ep.delete_old_snapshots(keep=2)

    assert deleted_batches == [["home.2026-09-06_020306"]]
    assert any(
        "no derivable timestamp" in r.getMessage() and WEIRD in r.getMessage()
        for r in endpoint_log.records
    )


def test_prune_marks_a_newly_visible_deletion(caplog):
    """execute_retention_deletes names each deleted snapshot that earlier
    releases could not list. Mutation guard: dropping the loop silences the
    INFO line and this fails."""
    import logging
    from unittest.mock import MagicMock

    from btrfs_backup_ng.cli.prune import execute_retention_deletes
    from btrfs_backup_ng.endpoint.common import DeletionResult

    snap = MagicMock()
    snap.get_name.return_value = WEIRD
    snap.newly_visible = True
    outcome = DeletionResult()
    outcome.deleted.append(snap)
    endpoint = MagicMock()
    endpoint.delete_snapshots.return_value = outcome

    with caplog.at_level(logging.INFO, logger="btrfs_backup_ng.cli.prune"):
        deleted, errors = execute_retention_deletes(endpoint, [snap])

    assert deleted == outcome.deleted_count
    assert errors == []
    assert any(
        "not visible to earlier" in r.getMessage() and WEIRD in r.getMessage()
        for r in caplog.records
    )


def test_the_confirmation_marker_discriminates():
    """newly_visible_mark is the string every deletion surface appends; it
    must be non-empty exactly for marked snapshots."""
    from unittest.mock import MagicMock

    from btrfs_backup_ng.cli.prune import newly_visible_mark

    marked = MagicMock()
    marked.newly_visible = True
    plain = MagicMock()
    plain.newly_visible = False
    assert "not visible to earlier releases" in newly_visible_mark(marked)
    assert newly_visible_mark(plain) == ""


# --------------------------------------------------------------------------- #
# Phase D: creation-side _N suffixing -- a collision creates a sibling, and
# the refusal survives only as the exhaustion diagnosis.
# --------------------------------------------------------------------------- #


def _creation_endpoint(tmp_path, monkeypatch, timestamp_format=None):
    """An endpoint whose ``snapshot()`` runs for real up to the btrfs command,
    which is replaced by a fake that records the EMITTED command and creates
    the destination directory -- the environment substituted, never the
    allocation logic under test. The clock is frozen so names are
    deterministic."""
    src = tmp_path / "src"
    src.mkdir()
    snaps = tmp_path / "snaps"
    snaps.mkdir()
    fixed = __util__.str_to_date("2026-01-02 03:04:05", fmt="%Y-%m-%d %H:%M:%S")
    monkeypatch.setattr(time, "localtime", lambda *args: fixed)
    config = {
        "source": str(src),
        "path": str(snaps),
        "snapshot_folder": str(snaps),
        "snap_prefix": "t-",
    }
    if timestamp_format is not None:
        config["timestamp_format"] = timestamp_format
    ep = LocalEndpoint(config=config)
    emitted = []

    def fake_exec(options, **kwargs):
        command = options.get("command")
        args = [a[0] if isinstance(a, tuple) else a for a in command]
        emitted.append(args)
        if "snapshot" in args:
            Path(str(args[-1])).mkdir()

    monkeypatch.setattr(ep, "_exec_command", fake_exec)
    monkeypatch.setattr(ep, "_remount", lambda *a, **k: None)
    return ep, snaps, emitted


def test_a_collision_creates_the_suffixed_sibling(tmp_path, monkeypatch):
    """The btrbk-compatible counter replaces the refusal: with t-X on disk,
    snapshot() creates t-X_1. Asserted on the FILESYSTEM and on the emitted
    command -- "no exception" alone would let a silent no-op pass. Mutation
    guards: an empty allocator range falls through to the refusal and this
    dies on AbortError; dropping the newly_visible assignment dies on the
    flag assert."""
    ep, snaps, emitted = _creation_endpoint(tmp_path, monkeypatch)
    base = "t-20260102-030405"
    (snaps / base).mkdir()  # the colliding snapshot

    snap = ep.snapshot()

    assert snap.get_name() == f"{base}_1"
    assert (snaps / f"{base}_1").is_dir(), "the sibling is not on disk"
    assert (snaps / base).is_dir(), "the original was disturbed"
    create = next(args for args in emitted if "snapshot" in args)
    assert str(create[-1]).endswith(f"{base}_1"), (
        "the emitted btrfs command targets a different name than the one returned"
    )
    assert snap.get_path() == snaps / f"{base}_1"
    assert snap.time_obj is not None, "the sibling lost its creation time"
    assert snap.newly_visible, "prune surfaces would not mark it this run"


def test_the_lowest_free_suffix_wins(tmp_path, monkeypatch):
    """btrbk semantics: with X, X_1 and X_3 taken, the next is X_2 -- the
    lowest free, not max+1 and not a blind retry of _1. Mutation guard: an
    allocator pinned to _1 attempts a name that exists and dies here."""
    ep, snaps, _ = _creation_endpoint(tmp_path, monkeypatch)
    base = "t-20260102-030405"
    for name in (base, f"{base}_1", f"{base}_3"):
        (snaps / name).mkdir()

    snap = ep.snapshot()

    assert snap.get_name() == f"{base}_2"
    assert (snaps / f"{base}_2").is_dir()
    assert (snaps / f"{base}_3").is_dir(), "an unrelated sibling was disturbed"


def test_the_dst_fold_pair_lands_as_base_and_suffix(tmp_path, monkeypatch):
    """America/New_York fall-back, 2026-11-01: 01:30 EDT and 01:30 EST are an
    hour apart and render the SAME default-format name. The pair must land as
    X and X_1 -- before this change the second snapshot of the fold was
    refused with advice that could not work for another hour."""
    ep, snaps, _ = _creation_endpoint(tmp_path, monkeypatch)
    edt = time.struct_time((2026, 11, 1, 1, 30, 0, 6, 305, 1))
    est = time.struct_time((2026, 11, 1, 1, 30, 0, 6, 305, 0))
    assert time.strftime(__util__.DATE_FORMAT, edt) == time.strftime(
        __util__.DATE_FORMAT, est
    ), "fixture self-check: the fold pair must render identically"

    monkeypatch.setattr(time, "localtime", lambda *args: edt)
    first = ep.snapshot()
    monkeypatch.setattr(time, "localtime", lambda *args: est)
    second = ep.snapshot()

    assert first.get_name() == "t-20261101-013000"
    assert second.get_name() == "t-20261101-013000_1"
    assert (snaps / first.get_name()).is_dir()
    assert (snaps / second.get_name()).is_dir()


def test_btrfs_correspondence_never_matches_by_name_even_for_suffixes(tmp_path):
    """The boundary Phase D must not move: a suffixed name changes nothing
    about correspondence, which stays received_uuid == stream_uuid for btrfs. A
    same-named destination entry without the uuid is NOT a correspondent; a
    differently-named entry with it IS."""
    from btrfs_backup_ng.endpoint.common import Endpoint

    ep = Endpoint.__new__(Endpoint)
    ep.config = {"path": tmp_path, "snap_prefix": "t-"}

    source = __util__.Snapshot(tmp_path, "t-", None, name="t-20260102-030405_1")
    source.uuid = "u-source"

    name_twin = __util__.Snapshot(tmp_path, "t-", None, name="t-20260102-030405_1")
    name_twin.received_uuid = ""
    real_copy = __util__.Snapshot(tmp_path, "t-", None, name="t-other-name")
    real_copy.received_uuid = "u-source"

    ep.list_snapshots = lambda flush_cache=False: [name_twin]  # type: ignore[method-assign]
    assert ep.correspondent_of(source) is None

    ep.list_snapshots = lambda flush_cache=False: [real_copy]  # type: ignore[method-assign]
    assert ep.correspondent_of(source) is real_copy
