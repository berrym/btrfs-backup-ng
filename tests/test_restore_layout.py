"""The plain layout: what is at a landing name, and what a run marker means.

Two questions the layout answers for the restore, each pinned on its own:

* ``describe_entry`` -- given a name a receive is about to create, what is
  already there? Nothing (None), or a description that names the kind of
  stranger. Presence by correspondence has already excluded this backup's
  own copy, so every description is a refusal reason; the one thing that
  must never come back is "nothing" for something that is there.
* ``read_markers`` -- which of the markers left under the destination
  authorise a deletion? Only a stale one naming a subvolume with no
  received_uuid, directly under the destination. Everything else is a
  report.

The inode-shape probe and the identity probe are the two btrfs facts a
tmp_path cannot supply; they are answered by the test while the paths, the
files and the JSON are real.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

import btrfs_backup_ng.core.layout as layout_mod
from btrfs_backup_ng.core.layout import (
    PlainLayout,
    describe_entry,
    marker_dir,
    read_markers,
)
from btrfs_backup_ng.endpoint.local import LocalEndpoint


class _Probe:
    """An endpoint that answers the identity probe from a table."""

    def __init__(self, dest: Path, identities: dict | None = None):
        self.config = {"path": str(dest)}
        self.identities = identities or {}

    def subvolume_identity(self, path):
        return self.identities.get(str(path))


@pytest.fixture
def subvolumes(monkeypatch):
    """Every directory reads as a subvolume root unless the test says otherwise."""
    monkeypatch.setattr(layout_mod, "_received_subvolume_shape", lambda p: None)


class TestWhatIsAtTheLandingName:
    def test_nothing_there_is_none(self, tmp_path, subvolumes):
        assert describe_entry(_Probe(tmp_path), tmp_path / "home-1", "S") is None

    def test_a_subvolume_without_received_uuid_is_an_interrupted_restore(
        self, tmp_path, subvolumes
    ):
        path = tmp_path / "home-1"
        path.mkdir()
        probe = _Probe(tmp_path, {str(path): {"uuid": "x", "received_uuid": ""}})
        what = describe_entry(probe, path, "S")
        assert what is not None
        assert "no received_uuid" in what and "interrupted restore" in what

    def test_a_copy_of_another_snapshot_names_both_identities(
        self, tmp_path, subvolumes
    ):
        path = tmp_path / "home-1"
        path.mkdir()
        probe = _Probe(tmp_path, {str(path): {"uuid": "x", "received_uuid": "OTHER"}})
        what = describe_entry(probe, path, "S")
        assert what is not None
        assert "different snapshot" in what and "OTHER" in what and "S" in what

    def test_an_unreadable_identity_is_said_not_assumed(self, tmp_path, subvolumes):
        path = tmp_path / "home-1"
        path.mkdir()
        what = describe_entry(_Probe(tmp_path), path, "S")
        assert what is not None and "could not be read" in what

    def test_a_received_copy_against_an_unknown_stream_identity(
        self, tmp_path, subvolumes
    ):
        """A legacy raw sidecar has no source_uuid: the entry may well be this
        backup's copy, and nothing can confirm it, so it is a refusal too."""
        path = tmp_path / "home-1"
        path.mkdir()
        probe = _Probe(tmp_path, {str(path): {"uuid": "x", "received_uuid": "R"}})
        what = describe_entry(probe, path, "")
        assert what is not None and "identity is unknown" in what

    def test_a_plain_directory_or_file_is_described_without_a_probe(
        self, tmp_path, monkeypatch
    ):
        path = tmp_path / "home-1"
        path.mkdir()
        monkeypatch.setattr(
            layout_mod, "_received_subvolume_shape", lambda p: "a plain directory"
        )
        asked = []
        probe = _Probe(tmp_path)
        probe.subvolume_identity = lambda p: asked.append(p)  # type: ignore[method-assign]
        assert describe_entry(probe, path, "S") == "is a plain directory"
        assert asked == [], "privilege was asked for what an inode already settles"

    def test_a_dangling_symlink_is_something_not_nothing(self, tmp_path, subvolumes):
        link = tmp_path / "home-1"
        link.symlink_to(tmp_path / "gone")
        what = describe_entry(_Probe(tmp_path), link, "S")
        assert what is not None


def _dead_pid() -> int:
    proc = subprocess.Popen(["true"])
    proc.wait()
    return proc.pid


def _marker(
    dest: Path, name: str, *, pid: int, path: Path | None = None, token="s-001"
):
    directory = marker_dir(dest)
    directory.mkdir(parents=True, exist_ok=True)
    record = {
        "format": 1,
        "session": "s",
        "pid": pid,
        "snapshot": name,
        "path": str(path if path is not None else dest / name),
        "started": "2026-01-01T00:00:00+00:00",
    }
    marker = directory / f"{token}.json"
    marker.write_text(json.dumps(record))
    return marker


class TestWhatAMarkerAuthorises:
    def test_no_marker_directory_means_no_markers(self, tmp_path):
        assert read_markers(tmp_path, _Probe(tmp_path)) == []

    def test_a_stale_marker_over_a_bare_subvolume_is_abandoned(
        self, tmp_path, subvolumes
    ):
        (tmp_path / "home-1").mkdir()
        _marker(tmp_path, "home-1", pid=_dead_pid())
        probe = _Probe(
            tmp_path, {str(tmp_path / "home-1"): {"uuid": "x", "received_uuid": ""}}
        )
        (entry,) = read_markers(tmp_path, probe)
        assert entry.state == "abandoned"
        assert entry.path == tmp_path / "home-1"

    def test_a_live_process_means_in_progress(self, tmp_path, subvolumes):
        (tmp_path / "home-1").mkdir()
        _marker(tmp_path, "home-1", pid=os.getpid())
        (entry,) = read_markers(tmp_path, _Probe(tmp_path))
        assert entry.state == "in-progress"

    def test_a_complete_copy_under_a_stale_marker_is_complete(
        self, tmp_path, subvolumes
    ):
        (tmp_path / "home-1").mkdir()
        _marker(tmp_path, "home-1", pid=_dead_pid())
        probe = _Probe(
            tmp_path, {str(tmp_path / "home-1"): {"uuid": "x", "received_uuid": "R"}}
        )
        (entry,) = read_markers(tmp_path, probe)
        assert entry.state == "complete" and "R" in entry.detail

    def test_an_unreadable_identity_is_unreadable_never_abandoned(
        self, tmp_path, subvolumes
    ):
        (tmp_path / "home-1").mkdir()
        _marker(tmp_path, "home-1", pid=_dead_pid())
        (entry,) = read_markers(tmp_path, _Probe(tmp_path))
        assert entry.state == "unreadable"

    def test_nothing_at_the_path_is_missing(self, tmp_path, subvolumes):
        _marker(tmp_path, "home-1", pid=_dead_pid())
        (entry,) = read_markers(tmp_path, _Probe(tmp_path))
        assert entry.state == "missing"

    def test_a_marker_naming_a_path_elsewhere_is_foreign(self, tmp_path, subvolumes):
        """The marker file is under the destination, but the path it names is
        not directly under it: whatever wrote that, it does not authorise
        deleting anything outside the destination."""
        elsewhere = tmp_path / "other" / "home-1"
        elsewhere.mkdir(parents=True)
        _marker(tmp_path, "home-1", pid=_dead_pid(), path=elsewhere)
        (entry,) = read_markers(tmp_path, _Probe(tmp_path))
        assert entry.state == "foreign"

    def test_a_marker_whose_name_disagrees_with_its_path_is_foreign(
        self, tmp_path, subvolumes
    ):
        (tmp_path / "home-2").mkdir()
        _marker(tmp_path, "home-1", pid=_dead_pid(), path=tmp_path / "home-2")
        (entry,) = read_markers(tmp_path, _Probe(tmp_path))
        assert entry.state == "foreign"

    def test_a_plain_directory_under_a_marker_is_not_a_subvolume(
        self, tmp_path, monkeypatch
    ):
        (tmp_path / "home-1").mkdir()
        _marker(tmp_path, "home-1", pid=_dead_pid())
        monkeypatch.setattr(
            layout_mod, "_received_subvolume_shape", lambda p: "a plain directory"
        )
        (entry,) = read_markers(tmp_path, _Probe(tmp_path))
        assert entry.state == "not-a-subvolume"

    def test_a_malformed_marker_is_reported_not_skipped(self, tmp_path, subvolumes):
        directory = marker_dir(tmp_path)
        directory.mkdir(parents=True)
        (directory / "junk.json").write_text("{not json")
        (entry,) = read_markers(tmp_path, _Probe(tmp_path))
        assert entry.state == "malformed"


class TestTheLayoutWritesAndClearsMarkers:
    def _layout(self, tmp_path, monkeypatch):
        dest = tmp_path / "dest"
        dest.mkdir()
        ep = LocalEndpoint(
            config={"path": str(dest), "snap_prefix": "", "fs_checks": "skip"}
        )
        monkeypatch.setattr(layout_mod, "_received_subvolume_shape", lambda p: None)
        return PlainLayout(ep, "abcd1234"), dest

    def test_a_receive_writes_an_atomic_marker_naming_its_landing(
        self, tmp_path, monkeypatch
    ):
        layout, dest = self._layout(tmp_path, monkeypatch)
        snap = type(
            "S",
            (),
            {
                "get_name": lambda self: "home-1",
                "get_path": lambda self: "/b/home-1",
                "__str__": lambda self: "home-1",
            },
        )()
        layout.begin([(snap, None)], None)
        layout.receive_endpoint._layout._receive_started("home-1")
        (marker,) = marker_dir(dest).glob("*.json")
        assert not marker.with_name(marker.name + ".tmp").exists(), "temp left behind"
        record = json.loads(marker.read_text())
        assert record["snapshot"] == "home-1"
        assert record["path"] == str(dest / "home-1")
        assert record["pid"] == os.getpid()
        assert record["session"] == "abcd1234"

    def test_a_verified_copy_clears_its_marker(self, tmp_path, monkeypatch):
        layout, dest = self._layout(tmp_path, monkeypatch)
        snap = type(
            "S",
            (),
            {
                "get_name": lambda self: "home-1",
                "get_path": lambda self: "/b/home-1",
                "__str__": lambda self: "home-1",
            },
        )()
        layout.begin([(snap, None)], None)
        layout._receive_started("home-1")
        layout._receive_verified(snap)
        assert list(marker_dir(dest).glob("*.json")) == []

    def test_finish_clears_a_failed_entry_only_when_its_partial_is_gone(
        self, tmp_path, monkeypatch
    ):
        layout, dest = self._layout(tmp_path, monkeypatch)
        gone = type(
            "S",
            (),
            {
                "get_name": lambda self: "home-1",
                "get_path": lambda self: "/b/home-1",
                "__str__": lambda self: "home-1",
            },
        )()
        kept = type(
            "S",
            (),
            {
                "get_name": lambda self: "home-2",
                "get_path": lambda self: "/b/home-2",
                "__str__": lambda self: "home-2",
            },
        )()
        layout.begin([(gone, None), (kept, None)], None)
        layout._receive_started("home-1")
        layout._receive_started("home-2")
        (dest / "home-2").mkdir()  # the partial the cleanup could not remove

        class _Result:
            failed = [(gone, "x"), (kept, "y")]

        layout.finish(_Result())
        remaining = [
            json.loads(m.read_text())["snapshot"]
            for m in marker_dir(dest).glob("*.json")
        ]
        assert remaining == ["home-2"]

    def test_the_receive_endpoint_is_the_endpoint_to_everything_else(
        self, tmp_path, monkeypatch
    ):
        layout, dest = self._layout(tmp_path, monkeypatch)
        receiver = layout.receive_endpoint
        assert receiver.config is layout.endpoint.config
        assert receiver.get_id() == layout.endpoint.get_id()
        assert receiver._is_remote is False
        assert repr(receiver) == repr(layout.endpoint)
        with pytest.raises(AttributeError):
            receiver.no_such_attribute  # noqa: B018 - the probe IS the test
