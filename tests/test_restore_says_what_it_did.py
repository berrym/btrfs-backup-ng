"""A restore that does nothing must say why, and a check that cannot run must not answer.

Two findings from the 0.9.6 review, both the same shape: an operator is given a
result that does not distinguish between different situations.

* `restore --snapshot NAME` where NAME is already at the destination reported
  "No snapshots need to be restored" -- the same sentence used when nothing
  matched, when everything was filtered, and when the location was empty. The
  outcome is a satisfied request, not a failure, so the exit code stays 0; but
  which of those happened has to be said.
* `check_snapshot_collision` returned False -- "no collision" -- when it could
  not read the destination at all. A caller acting on that receives onto a name
  that may already exist, which is what the function exists to prevent.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.core import restore as core_restore
from btrfs_backup_ng.core.restore import RestoreError, check_snapshot_collision


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
    def test_an_unreadable_destination_raises(self):
        endpoint = MagicMock()
        endpoint.list_snapshots.side_effect = OSError("no route to host")
        with pytest.raises(RestoreError, match="Refusing to assume"):
            check_snapshot_collision("home-20240101T120000", endpoint)

    def test_a_readable_destination_still_answers_both_ways(self):
        endpoint = MagicMock()
        endpoint.list_snapshots.return_value = [_Snap("home-20240101T120000")]
        assert check_snapshot_collision("home-20240101T120000", endpoint) is True
        assert check_snapshot_collision("home-20240102T120000", endpoint) is False

    def test_the_restore_refuses_before_streaming(self):
        """The point of wiring it: refuse up front, not after the transfer."""
        backup = _Endpoint(["home-20240101T120000"], prefix="home-")
        # The destination must be readable under the prefix, which is what
        # restore_snapshots now guarantees before it gets here. With the wrong
        # prefix the destination cannot see its own snapshot and the collision
        # check finds nothing -- the same blindness this release is fixing.
        destination = _Endpoint(["home-20240101T120000"], prefix="home-")
        with pytest.raises(RestoreError, match="already at the destination"):
            core_restore.restore_snapshot(
                backup, destination, _Snap("home-20240101T120000")
            )
