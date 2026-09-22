"""A restore that does nothing must say why, and a check that cannot run must not answer.

Two findings from the 0.9.6 review, both the same shape: an operator is given a
result that does not distinguish between different situations.

* `restore --snapshot NAME` where NAME is already at the destination reported
  "No snapshots need to be restored" -- the same sentence used when nothing
  matched, when everything was filtered, and when the location was empty. The
  outcome is a satisfied request, not a failure, so the exit code stays 0; but
  which of those happened has to be said.
* the collision check answered "no collision" when it could not read the
  destination at all. A caller acting on that receives onto a name that may
  already exist, which is what the check exists to prevent. The check now
  examines the landing path itself and describes what it cannot read.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.core import restore as core_restore
from btrfs_backup_ng.core.restore import RestoreError


def _parses(text):
    return bool(re.fullmatch(r"\d{8}T\d{6}", text))


class _Snap:
    def __init__(self, name, t=0):
        self.name, self.time_obj = name, t
        self.locks, self.parent_locks = set(), set()

    def get_name(self):
        return self.name

    def get_path(self):
        return f"/backup/{self.name}"

    def find_parent(self, others):
        return None

    def __lt__(self, other):
        return self.time_obj < other.time_obj

    def __str__(self):
        return self.name


class _Endpoint:
    def __init__(self, names, prefix=""):
        self._names = list(names)
        self.config = {"path": "/backup", "snap_prefix": prefix}

    def list_snapshots(self, flush_cache=False):
        p = self.config.get("snap_prefix", "") or ""
        return [
            _Snap(n, i)
            for i, n in enumerate(self._names)
            if n.startswith(p) and _parses(n[len(p) :])
        ]

    def prefixes_present(self):
        return {"home-": len(self._names)}

    def correspondent_of(self, snapshot):
        for snap in self.list_snapshots():
            if snap.get_name() == snapshot.get_name():
                return snap
        return None

    def correspondents_of(self, snapshots):
        found = {}
        for snap in snapshots:
            match = self.correspondent_of(snap)
            if match is not None:
                found[snap.get_name()] = match
        return found

    def required_parent_of(self, snapshot):
        older = [s for s in self.list_snapshots() if s.time_obj < snapshot.time_obj]
        return max(older, key=lambda s: s.time_obj) if older else None

    def subvolume_identity(self, path):
        return None

    def set_lock(self, *a, **kw):
        pass

    def get_id(self):
        return "endpoint"


class TestItSaysWhyThereIsNothingToDo:
    def test_an_already_present_snapshot_is_named(self, caplog):
        backup = _Endpoint(["home-20240101T120000", "home-20240102T120000"])
        destination = _Endpoint(["home-20240102T120000"])

        with caplog.at_level("INFO"):
            stats = core_restore.restore_snapshots(
                backup, destination, snapshot_name="home-20240102T120000"
            )

        messages = " ".join(r.getMessage() for r in caplog.records)
        assert "Already at the destination" in messages, (
            "the operator was not told the snapshot was already there"
        )
        assert "home-20240102T120000" in messages, "the message does not name it"
        assert "No snapshots need to be restored" not in messages, (
            "the generic sentence was used for a case it does not describe"
        )
        assert stats["skipped"] == 1
        assert stats["failed"] == 0, "an already-satisfied request is not a failure"

    def test_a_genuinely_empty_result_still_says_so(self, caplog):
        """The generic sentence is still right when nothing matched."""
        backup = _Endpoint(["home-20240101T120000"])
        destination = _Endpoint([])

        with caplog.at_level("INFO"):
            core_restore.restore_snapshots(
                backup, destination, snapshot_name="home-20240101T120000", dry_run=True
            )
        messages = " ".join(r.getMessage() for r in caplog.records)
        assert "Already at the destination" not in messages


class TestACheckThatCannotRunDoesNotAnswer:
    """A same-name entry whose identity cannot be read is a refusal, never
    "not there". The collision question used to be asked by NAME against a
    listing, and answered "no collision" when the listing failed. It is now
    asked of the landing path itself, and an entry that cannot be examined
    is described as exactly that."""

    def test_an_unreadable_identity_refuses_rather_than_assuming_absent(
        self, tmp_path, monkeypatch
    ):
        import btrfs_backup_ng.core.layout as layout_mod
        from btrfs_backup_ng.core.layout import describe_entry

        monkeypatch.setattr(layout_mod, "_received_subvolume_shape", lambda p: None)
        entry = tmp_path / "home-20240101T120000"
        entry.mkdir()
        probe = MagicMock()
        probe.subvolume_identity.return_value = None
        what = describe_entry(probe, entry, "S")
        assert what is not None, "an unexaminable entry was read as absent"
        assert "could not be read" in what

    def test_a_readable_entry_still_answers_both_ways(self, tmp_path, monkeypatch):
        import btrfs_backup_ng.core.layout as layout_mod
        from btrfs_backup_ng.core.layout import describe_entry

        monkeypatch.setattr(layout_mod, "_received_subvolume_shape", lambda p: None)
        probe = MagicMock()
        probe.subvolume_identity.return_value = {"uuid": "x", "received_uuid": ""}
        present = tmp_path / "home-20240101T120000"
        present.mkdir()
        assert describe_entry(probe, present, "S") is not None
        assert describe_entry(probe, tmp_path / "home-20240102T120000", "S") is None

    def test_the_restore_refuses_before_streaming(self, tmp_path, monkeypatch):
        """The point of wiring it: refuse up front, not after the transfer."""
        import btrfs_backup_ng.core.layout as layout_mod
        import btrfs_backup_ng.core.operations as ops

        monkeypatch.setattr(layout_mod, "_received_subvolume_shape", lambda p: None)
        streamed = []
        monkeypatch.setattr(ops, "send_snapshot", lambda *a, **k: streamed.append(1))
        backup = _Endpoint(["home-20240101T120000"], prefix="home-")
        destination = _Endpoint([], prefix="home-")
        destination.config["path"] = str(tmp_path)
        (tmp_path / "home-20240101T120000").mkdir()
        with pytest.raises(RestoreError, match="not this backup's copy"):
            core_restore.restore_snapshots(
                backup, destination, snapshot_name="home-20240101T120000"
            )
        assert streamed == []
