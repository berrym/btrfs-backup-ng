"""A native restore is a transfer through the engine: select, plan, execute.

``restore_snapshots`` keeps its signature and its stats dict; underneath it
the planner decides presence and parents by correspondence, the executor
moves the bytes under ``restore:<session>`` pins that a failure releases, and
the plain layout answers where a copy lands and what is already there. These
tests drive the facade end to end over a real ``LocalEndpoint`` destination
(its listing controlled, its identity probe answered) and a source fake that
behaves like a listing: snapshots that carry the identity their stream
carries, a predecessor chain, pins recorded.

What was pinned before -- a restore-only chain builder, a name skip, a
collision check by name, a verifier that checked existence -- is gone, so
the tests pin the behaviour those were supposed to give and now must:
present by correspondence is skipped, a same-name stranger is refused before
streaming, the chain comes along, a failure leaves no pin, and the preview
is the run.
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path

import pytest

import btrfs_backup_ng.core.layout as layout_mod
import btrfs_backup_ng.core.operations as ops
import btrfs_backup_ng.core.restore as core_restore
from btrfs_backup_ng import __util__
from btrfs_backup_ng.core.restore import RestoreError, restore_snapshots
from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict

PREFIX = "home-"
STAMPS = ("20240101-120000", "20240102-120000", "20240103-120000")


def _snap(stamp, uuid, received_uuid="", endpoint=None, location="/backup"):
    """A snapshot as a listing produces one: the backup copy carries the
    original's uuid as received_uuid, which is what its stream carries on."""
    s = __util__.Snapshot(
        location,
        PREFIX,
        endpoint,
        time_obj=time.strptime(stamp, "%Y%m%d-%H%M%S"),
        name=f"{PREFIX}{stamp}",
    )
    s.uuid = uuid
    s.received_uuid = received_uuid
    return s


class _Source:
    """A backup location: three received copies whose streams carry O1..O3."""

    _is_remote = False

    def __init__(self, snapshots):
        self.config = {"path": "/backup", "snap_prefix": PREFIX}
        self._snaps = list(snapshots)
        for s in self._snaps:
            s.endpoint = self
        self.held: set[tuple[str, bool]] = set()
        self.lock_calls: list[tuple[str, str, bool, bool]] = []

    def list_snapshots(self, flush_cache=False):
        return list(self._snaps)

    def set_lock(self, snapshot, lock_id, lock_state, parent=False):
        self.lock_calls.append((snapshot.get_name(), lock_id, lock_state, parent))
        key = (snapshot.get_name(), parent)
        if lock_state:
            self.held.add(key)
        else:
            self.held.discard(key)

    def get_id(self):
        return "backup"

    def required_parent_of(self, snapshot):
        # The engine's own rule, so the facade is tested against it and not
        # against a copy that could drift.
        return Endpoint.required_parent_of(self, snapshot)  # type: ignore[arg-type]

    def describe_empty_listing(self):
        return None


def _source():
    return _Source(
        [
            _snap(STAMPS[0], "B1", received_uuid="O1"),
            _snap(STAMPS[1], "B2", received_uuid="O2"),
            _snap(STAMPS[2], "B3", received_uuid="O3"),
        ]
    )


@pytest.fixture
def rig(tmp_path, monkeypatch):
    """A real local destination endpoint over tmp_path, with the parts that
    need btrfs answered: the listing, the inode-shape probe, the identity."""
    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    dest = LocalEndpoint(
        config={"path": str(dest_dir), "snap_prefix": "", "fs_checks": "skip"}
    )
    copies: list = []
    identities: dict[str, dict | None] = {}
    dest.list_snapshots = lambda flush_cache=False: list(copies)  # type: ignore[method-assign]
    dest.subvolume_identity = lambda path: identities.get(str(path))  # type: ignore[method-assign]
    dest.receive = lambda *a, **k: None  # type: ignore[method-assign]
    monkeypatch.setattr(layout_mod, "_received_subvolume_shape", lambda p: None)

    sent: list[dict] = []
    cleaned: list[str] = []
    failing: set[str] = set()

    def fake_send(snapshot, destination_endpoint, parent=None, options=None, **kw):
        # What the engine's transfer does that the facade can observe: the
        # receive starts (the layout's marker hook), the copy lands.
        destination_endpoint.receive(None, str(snapshot))
        sent.append(
            {
                "snapshot": snapshot,
                "parent": parent,
                "options": dict(options or {}),
                "marker_present": bool(
                    list(layout_mod.marker_dir(dest_dir).glob("*.json"))
                ),
            }
        )
        landing = dest_dir / snapshot.get_name()
        landing.mkdir(exist_ok=True)
        if snapshot.get_name() in failing:
            raise __util__.SnapshotTransferError(f"receive of {snapshot} died")
        identities[str(landing)] = {"uuid": "N", "received_uuid": snapshot.stream_uuid}

    def fake_cleanup(endpoint, name, *, created_by_this_run):
        cleaned.append(name)
        if created_by_this_run:
            (dest_dir / name).rmdir()

    monkeypatch.setattr(ops, "send_snapshot", fake_send)
    monkeypatch.setattr(ops, "_cleanup_partial_local_subvolume", fake_cleanup)
    monkeypatch.setattr(
        ops, "artifact_verdict", lambda ep, s: StructureVerdict("ok", "verified")
    )
    return {
        "dest": dest,
        "dest_dir": dest_dir,
        "copies": copies,
        "identities": identities,
        "sent": sent,
        "cleaned": cleaned,
        "failing": failing,
    }


def _present(rig, source_snap, own_uuid="D"):
    """Put the received copy of ``source_snap`` at the destination, the way a
    real listing would report it: its received_uuid is what the stream
    carried, and its landing directory exists."""
    copy = _snap(
        source_snap.get_name()[len(PREFIX) :],
        own_uuid,
        received_uuid=source_snap.stream_uuid,
        endpoint=rig["dest"],
        location=str(rig["dest_dir"]),
    )
    rig["copies"].append(copy)
    landing = rig["dest_dir"] / copy.get_name()
    landing.mkdir(exist_ok=True)
    rig["identities"][str(landing)] = {
        "uuid": own_uuid,
        "received_uuid": copy.received_uuid,
    }
    return copy


def _plan_lines(records):
    return [
        r.getMessage().strip()
        for r in records
        if re.match(r"^\s+\[\d+/\d+\] ", r.getMessage())
    ]


class TestTheLatestBringsItsChainOntoEmptyMedia:
    def test_the_chain_is_sent_oldest_first_with_the_backup_side_parents(self, rig):
        source = _source()
        stats = restore_snapshots(source, rig["dest"])
        names = [d["snapshot"].get_name() for d in rig["sent"]]
        assert names == [f"{PREFIX}{s}" for s in STAMPS]
        parents = [d["parent"] for d in rig["sent"]]
        assert parents[0] is None
        assert parents[1] is source.list_snapshots()[0], (
            "the parent handed to the send is not the backup-side snapshot"
        )
        assert parents[2] is source.list_snapshots()[1]
        assert stats == {"restored": 3, "skipped": 0, "failed": 0, "errors": []}

    def test_the_selection_by_name_is_expanded_too(self, rig):
        source = _source()
        restore_snapshots(source, rig["dest"], snapshot_name=f"{PREFIX}{STAMPS[1]}")
        assert [d["snapshot"].get_name() for d in rig["sent"]] == [
            f"{PREFIX}{STAMPS[0]}",
            f"{PREFIX}{STAMPS[1]}",
        ]

    def test_the_oldest_alone_lands_one_subvolume(self, rig):
        source = _source()
        stats = restore_snapshots(
            source, rig["dest"], snapshot_name=f"{PREFIX}{STAMPS[0]}"
        )
        assert [d["snapshot"].get_name() for d in rig["sent"]] == [
            f"{PREFIX}{STAMPS[0]}"
        ]
        assert stats["restored"] == 1

    def test_no_incremental_sends_the_chain_in_full(self, rig):
        restore_snapshots(_source(), rig["dest"], no_incremental=True)
        assert len(rig["sent"]) == 3
        assert all(d["parent"] is None for d in rig["sent"])

    def test_options_reach_every_transfer(self, rig):
        options = {"compress": "zstd", "rate_limit": "10M", "show_progress": False}
        restore_snapshots(_source(), rig["dest"], options=options)
        assert all(d["options"] == options for d in rig["sent"])


class TestPresenceIsCorrespondenceNotName:
    def test_a_rerun_after_success_skips_by_the_streams_identity(self, rig, caplog):
        """The copy's received_uuid is O3 -- the original's uuid, not the
        backup copy's own -- and that is what matches. Nothing is sent, the
        request is reported satisfied, and it is not a failure."""
        source = _source()
        for s in source.list_snapshots():
            _present(rig, s)
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            stats = restore_snapshots(source, rig["dest"])
        assert rig["sent"] == []
        assert stats == {"restored": 0, "skipped": 1, "failed": 0, "errors": []}
        assert any(
            "Already at the destination" in r.getMessage()
            and f"{PREFIX}{STAMPS[2]}" in r.getMessage()
            for r in caplog.records
        )

    def test_all_present_under_all_counts_every_target_as_skipped(self, rig):
        source = _source()
        for s in source.list_snapshots():
            _present(rig, s)
        stats = restore_snapshots(source, rig["dest"], restore_all=True)
        assert stats["skipped"] == 3 and stats["restored"] == 0

    def test_a_present_ancestor_is_the_parent_and_is_not_resent(self, rig):
        """The destination holds the copy of the first; the second is sent
        against it and the first is not sent again."""
        source = _source()
        first, second, _third = source.list_snapshots()
        _present(rig, first)
        restore_snapshots(source, rig["dest"], snapshot_name=second.get_name())
        assert [d["snapshot"].get_name() for d in rig["sent"]] == [second.get_name()]
        assert rig["sent"][0]["parent"] is first

    def test_a_present_middle_ancestor_stops_the_chain(self, rig):
        source = _source()
        _first, second, third = source.list_snapshots()
        _present(rig, second)
        restore_snapshots(source, rig["dest"])
        assert [d["snapshot"].get_name() for d in rig["sent"]] == [third.get_name()]
        assert rig["sent"][0]["parent"] is second


class TestASameNameStrangerIsRefusedBeforeStreaming:
    def _stranger(self, rig, name, identity):
        landing = rig["dest_dir"] / name
        landing.mkdir()
        rig["identities"][str(landing)] = identity
        return landing

    def test_a_subvolume_without_received_uuid_is_refused_and_left(self, rig):
        """The interrupted-restore shape (a subvolume, rw, no received_uuid):
        it used to be listed by name, logged "Skipping existing", exit 0."""
        source = _source()
        landing = self._stranger(
            rig, f"{PREFIX}{STAMPS[2]}", {"uuid": "X", "received_uuid": ""}
        )
        with pytest.raises(RestoreError) as info:
            restore_snapshots(source, rig["dest"])
        message = str(info.value)
        assert str(landing) in message
        assert "no received_uuid" in message and "interrupted restore" in message
        assert "Nothing was transferred" in message
        assert rig["sent"] == [], "the run streamed before refusing"
        assert landing.is_dir(), "the stranger was deleted"
        assert source.held == set(), "a pin was taken for a refused run"

    def test_a_copy_of_something_else_is_refused(self, rig):
        source = _source()
        self._stranger(
            rig, f"{PREFIX}{STAMPS[0]}", {"uuid": "X", "received_uuid": "SOMEONE-ELSE"}
        )
        with pytest.raises(RestoreError, match="different snapshot"):
            restore_snapshots(source, rig["dest"])
        assert rig["sent"] == []

    def test_an_identity_that_cannot_be_read_is_not_assumed_absent(self, rig):
        source = _source()
        self._stranger(rig, f"{PREFIX}{STAMPS[0]}", None)
        with pytest.raises(RestoreError, match="could not be read"):
            restore_snapshots(source, rig["dest"])
        assert rig["sent"] == []

    def test_a_plain_directory_is_refused(self, rig, monkeypatch):
        source = _source()
        landing = rig["dest_dir"] / f"{PREFIX}{STAMPS[0]}"
        landing.mkdir()
        monkeypatch.setattr(
            layout_mod,
            "_received_subvolume_shape",
            lambda p: "a plain directory (inode 4711), not a btrfs subvolume",
        )
        with pytest.raises(RestoreError, match="plain directory"):
            restore_snapshots(source, rig["dest"])
        assert rig["sent"] == []

    def test_the_whole_plan_is_checked_before_anything_moves(self, rig):
        """A collision on the THIRD entry refuses the run before the first."""
        source = _source()
        self._stranger(rig, f"{PREFIX}{STAMPS[2]}", {"uuid": "X", "received_uuid": ""})
        with pytest.raises(RestoreError):
            restore_snapshots(source, rig["dest"])
        assert rig["sent"] == []

    def test_the_dry_run_refuses_what_the_run_refuses(self, rig):
        source = _source()
        self._stranger(rig, f"{PREFIX}{STAMPS[0]}", {"uuid": "X", "received_uuid": ""})
        with pytest.raises(RestoreError, match="no received_uuid"):
            restore_snapshots(source, rig["dest"], dry_run=True)


class TestTheRunHoldsRestorePinsAndReleasesThemOnFailure:
    def test_pins_are_taken_under_the_restore_session_id(self, rig):
        source = _source()
        restore_snapshots(source, rig["dest"], snapshot_name=f"{PREFIX}{STAMPS[0]}")
        ids = {lock_id for _, lock_id, _, _ in source.lock_calls}
        assert len(ids) == 1
        (lock_id,) = ids
        assert lock_id.startswith("restore:") and len(lock_id) > len("restore:")

    def test_a_clean_run_leaves_nothing_pinned(self, rig):
        source = _source()
        restore_snapshots(source, rig["dest"])
        assert source.held == set()
        assert any(state is True for _, _, state, _ in source.lock_calls)

    def test_a_failed_receive_releases_the_snapshot_and_parent_pins(self, rig):
        """A backup keeps a failed snapshot pinned so retention cannot prune
        it; a restore must NOT, or a persistent pin on a remote target blocks
        its prune until someone runs --unlock."""
        source = _source()
        rig["failing"].add(f"{PREFIX}{STAMPS[1]}")
        stats = restore_snapshots(source, rig["dest"])
        assert stats["failed"] >= 1
        assert source.held == set(), f"pins left after the failure: {source.held}"
        released_parent = [
            c for c in source.lock_calls if c[0] == f"{PREFIX}{STAMPS[0]}" and c[3]
        ]
        assert any(state is False for _, _, state, _ in released_parent)

    def test_a_failure_is_counted_named_and_its_dependants_not_streamed(self, rig):
        source = _source()
        rig["failing"].add(f"{PREFIX}{STAMPS[0]}")
        stats = restore_snapshots(source, rig["dest"])
        assert stats["restored"] == 0 and stats["failed"] == 3
        assert any(f"{PREFIX}{STAMPS[0]}" in e and "died" in e for e in stats["errors"])
        assert [d["snapshot"].get_name() for d in rig["sent"]] == [
            f"{PREFIX}{STAMPS[0]}"
        ]
        assert rig["cleaned"] == [f"{PREFIX}{STAMPS[0]}"]


class TestThePreviewIsTheRun:
    @pytest.mark.parametrize(
        ("selection", "present_first", "no_incremental"),
        [
            ({}, False, False),
            ({}, False, True),
            ({"restore_all": True}, True, False),
            ({"snapshot_name": f"{PREFIX}{STAMPS[1]}"}, True, False),
        ],
    )
    def test_the_dry_run_prints_exactly_the_plan_the_run_executes(
        self, rig, caplog, selection, present_first, no_incremental
    ):
        source = _source()
        if present_first:
            _present(rig, source.list_snapshots()[0])
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            preview_stats = restore_snapshots(
                source,
                rig["dest"],
                dry_run=True,
                no_incremental=no_incremental,
                **selection,
            )
        previewed = _plan_lines(caplog.records)
        assert previewed, "the preview announced nothing"
        assert rig["sent"] == [], "the dry run transferred"
        assert preview_stats["restored"] == 0
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            restore_snapshots(
                source, rig["dest"], no_incremental=no_incremental, **selection
            )
        assert _plan_lines(caplog.records) == previewed
        executed = [
            (d["snapshot"].get_name(), d["parent"].get_name() if d["parent"] else None)
            for d in rig["sent"]
        ]
        described = []
        for line in previewed:
            m = re.match(r"\[\d+/\d+\] (\S+) \((full|incremental from (\S+))\)", line)
            assert m, line
            described.append((m.group(1), m.group(3)))
        assert described == executed, "the lines describe a different transfer"

    def test_the_first_previews_as_full(self, rig, caplog):
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            restore_snapshots(_source(), rig["dest"], dry_run=True)
        first = _plan_lines(caplog.records)[0]
        assert first.endswith("(full)"), first


class TestTheRunMarker:
    def test_a_marker_names_the_receive_while_it_runs_and_is_gone_after(self, rig):
        restore_snapshots(_source(), rig["dest"], snapshot_name=f"{PREFIX}{STAMPS[0]}")
        assert rig["sent"][0]["marker_present"], "no marker during the receive"
        assert list(layout_mod.marker_dir(rig["dest_dir"]).glob("*.json")) == []

    def test_a_marker_records_the_landing_path_and_this_process(self, rig, monkeypatch):
        seen: dict = {}

        def peek(snapshot, destination_endpoint, parent=None, options=None, **kw):
            destination_endpoint.receive(None, str(snapshot))
            (marker,) = layout_mod.marker_dir(rig["dest_dir"]).glob("*.json")
            seen.update(json.loads(marker.read_text()))
            (rig["dest_dir"] / snapshot.get_name()).mkdir(exist_ok=True)

        monkeypatch.setattr(ops, "send_snapshot", peek)
        restore_snapshots(_source(), rig["dest"], snapshot_name=f"{PREFIX}{STAMPS[0]}")
        assert seen["snapshot"] == f"{PREFIX}{STAMPS[0]}"
        assert seen["path"] == str(rig["dest_dir"] / f"{PREFIX}{STAMPS[0]}")
        assert seen["pid"] > 0 and seen["session"] and seen["format"] == 1

    def test_a_failed_receive_whose_partial_was_removed_leaves_no_marker(self, rig):
        rig["failing"].add(f"{PREFIX}{STAMPS[0]}")
        restore_snapshots(_source(), rig["dest"], snapshot_name=f"{PREFIX}{STAMPS[0]}")
        assert list(layout_mod.marker_dir(rig["dest_dir"]).glob("*.json")) == []

    def test_a_failed_receive_whose_partial_remains_keeps_its_marker(
        self, rig, monkeypatch
    ):
        """The cleanup could not remove it (or did not own it): the marker
        stays so --cleanup can still identify the subvolume as this run's."""
        rig["failing"].add(f"{PREFIX}{STAMPS[0]}")
        monkeypatch.setattr(
            ops, "_cleanup_partial_local_subvolume", lambda *a, **k: None
        )
        restore_snapshots(_source(), rig["dest"], snapshot_name=f"{PREFIX}{STAMPS[0]}")
        markers = list(layout_mod.marker_dir(rig["dest_dir"]).glob("*.json"))
        assert len(markers) == 1
        assert json.loads(markers[0].read_text())["snapshot"] == f"{PREFIX}{STAMPS[0]}"

    def test_progress_is_reported_per_receive(self, rig):
        calls: list = []
        restore_snapshots(
            _source(),
            rig["dest"],
            on_progress=lambda i, n, name: calls.append((i, n, name)),
        )
        assert calls == [(i + 1, 3, f"{PREFIX}{s}") for i, s in enumerate(STAMPS)]


class TestSelectionErrorsAreRaisedBeforeAnythingMoves:
    def test_a_named_snapshot_that_is_not_there(self, rig):
        with pytest.raises(RestoreError, match="not found"):
            restore_snapshots(_source(), rig["dest"], snapshot_name="home-nope")
        assert rig["sent"] == []

    def test_no_snapshot_before_the_time(self, rig):
        early = time.strptime("20230101-000000", "%Y%m%d-%H%M%S")
        with pytest.raises(RestoreError, match="No snapshot found before"):
            restore_snapshots(_source(), rig["dest"], before_time=early)

    def test_before_time_picks_the_newest_not_after_it(self, rig):
        bound = time.strptime("20240102-130000", "%Y%m%d-%H%M%S")
        restore_snapshots(_source(), rig["dest"], before_time=bound)
        assert [d["snapshot"].get_name() for d in rig["sent"]] == [
            f"{PREFIX}{STAMPS[0]}",
            f"{PREFIX}{STAMPS[1]}",
        ]

    def test_an_empty_location_is_a_failed_restore_with_the_diagnosis(self, rig):
        source = _Source([])
        source.describe_empty_listing = lambda: "prefixes present: 'srv-'"  # type: ignore[method-assign]
        source.prefixes_present = lambda: {}  # type: ignore[attr-defined]
        stats = restore_snapshots(source, rig["dest"])
        assert stats["failed"] == 1 and stats["restored"] == 0
        assert any("srv-" in e for e in stats["errors"])
        assert rig["sent"] == []


class TestARawSourceRestoresItsStoredChain:
    def _raw_source(self, tmp_path, with_parent=True):
        from datetime import datetime, timezone

        from btrfs_backup_ng.endpoint.raw import RawEndpoint
        from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

        def raw(stamp, source_uuid, parent_name=None):
            return RawSnapshot(
                name=f"{PREFIX}{stamp}",
                stream_path=tmp_path / "raw" / f"{PREFIX}{stamp}.btrfs.zst",
                parent_name=parent_name,
                source_uuid=source_uuid,
                created=datetime.strptime(stamp, "%Y%m%d-%H%M%S").replace(
                    tzinfo=timezone.utc
                ),
            )

        base = raw(STAMPS[0], "O1")
        inc = raw(STAMPS[1], "O2", parent_name=base.name)
        store = RawEndpoint(
            config={"path": str(tmp_path / "raw"), "snap_prefix": PREFIX}
        )
        listing = [base, inc] if with_parent else [inc]
        for s in listing:
            s.endpoint = store
        store.list_snapshots = lambda flush_cache=False: list(listing)  # type: ignore[method-assign]
        return store, listing

    def test_one_increment_onto_empty_media_brings_its_parent_first(
        self, rig, tmp_path
    ):
        store, (base, inc) = self._raw_source(tmp_path)
        stats = restore_snapshots(store, rig["dest"], snapshot_name=inc.name)
        assert [d["snapshot"].get_name() for d in rig["sent"]] == [base.name, inc.name]
        assert stats["restored"] == 2

    def test_an_increment_whose_parent_is_gone_is_refused_before_streaming(
        self, rig, tmp_path
    ):
        store, (inc,) = self._raw_source(tmp_path, with_parent=False)
        with pytest.raises(RestoreError, match="Nothing was transferred") as info:
            restore_snapshots(store, rig["dest"], snapshot_name=inc.name)
        assert f"{PREFIX}{STAMPS[0]}" in str(info.value)
        assert rig["sent"] == []
        assert not (rig["dest_dir"] / inc.name).exists()


class TestTheDestinationIsReadUnderTheSourcesPrefix:
    def test_the_prefix_is_copied_and_the_listing_flushed(self, rig):
        source = _source()
        flushed: list = []
        real = rig["dest"].list_snapshots

        def listing(flush_cache=False):
            flushed.append(flush_cache)
            return real(flush_cache=flush_cache)

        rig["dest"].list_snapshots = listing  # type: ignore[method-assign]
        restore_snapshots(source, rig["dest"], dry_run=True)
        assert rig["dest"].config["snap_prefix"] == PREFIX
        assert flushed and flushed[0] is True


def test_the_removed_planners_are_gone_not_neutralised():
    """Replaced by the planner, the executor, the verdict and the layout; a
    half-present helper is worse than either state."""
    import inspect

    source = inspect.getsource(core_restore)
    for gone in (
        "def get_restore_chain",
        "def _find_older_parent",
        "def _choose_parent",
        "def find_parent_by_correspondence",
        "def check_snapshot_collision",
        "def verify_restored_snapshot",
        "def restore_snapshot(",
        "skip_existing",
        "def _receive_order_key",
    ):
        assert gone not in source, f"{gone} survived"
    assert "plan_transfer_sequence(" in source and "_execute_transfers(" in source
    assert "PlainLayout(" in source
    cli_source = Path(core_restore.__file__).parent.parent / "cli" / "restore.py"
    assert ".partial" not in cli_source.read_text()
