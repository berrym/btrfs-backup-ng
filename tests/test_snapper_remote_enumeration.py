"""Snapper backups on an ``ssh://`` btrfs target must actually be enumerated.

`list_snapper_backups()` dispatched raw:// / raw+ssh:// to a sidecar scan and sent
everything else -- including ssh:// -- down a LOCAL filesystem scan:

    Path("ssh://user@host:/p") / ".snapshots"   ->  ssh:/user@host:/p/.snapshots
    .exists() is False                          ->  []

so `snapper restore --list ssh://...` reported "No snapper backups found" and
exited 0 without ever opening a connection. That is the README's flagship
disaster-recovery walkthrough, and it was answering a question it never asked.

Verified against a real destination on 192.168.0.70 produced by an actual
`snapper backup`: two slots enumerate with number, type, date and description
parsed from the remote info.xml.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core.restore import list_snapper_backups

INFO_XML = """<?xml version="1.0"?>
<snapshot>
  <type>single</type>
  <num>{num}</num>
  <date>2026-08-18 00:57:01</date>
  <description>{desc}</description>
</snapshot>
"""


class FakeRemote:
    """An SSHEndpoint stand-in that answers the exact argv the code sends.

    Deliberately strict: an unexpected command fails the test rather than
    returning a benign default, so a silently-changed remote call cannot pass.
    """

    _is_remote = True

    def __init__(self, path="/remote/dest", slots=(1, 2), *, info_for=None, fail=None):
        self.config = {"path": path}
        self.slots = list(slots)
        self.info_for = self.slots if info_for is None else list(info_for)
        self.fail = fail or {}
        self.calls = []

    def _exec_remote_command(self, command, **kwargs):
        self.calls.append(list(command))
        verb = command[0]
        if verb in self.fail:
            rc, err = self.fail[verb]
            return MagicMock(returncode=rc, stdout=b"", stderr=err.encode())
        if verb == "find":
            base = command[1]
            out = "\n".join(f"{base}/{n}" for n in self.slots)
            return MagicMock(returncode=0, stdout=out.encode(), stderr=b"")
        if verb == "test":
            return MagicMock(returncode=0, stdout=b"", stderr=b"")
        if verb == "cat":
            num = int(Path(command[1]).parent.name)
            if num not in self.info_for:
                return MagicMock(returncode=1, stdout=b"", stderr=b"No such file")
            body = INFO_XML.format(num=num, desc=f"snapshot {num}")
            return MagicMock(returncode=0, stdout=body.encode(), stderr=b"")
        raise AssertionError(f"unexpected remote command: {command}")


def _list(endpoint, url="ssh://host:/remote/dest"):
    with patch("btrfs_backup_ng.endpoint.choose_endpoint", return_value=endpoint):
        return list_snapper_backups(url)


class TestItActuallyConnects:
    def test_a_remote_destination_enumerates(self):
        ep = FakeRemote()
        backups = _list(ep)
        assert [b["number"] for b in backups] == [1, 2]

    def test_it_issues_a_remote_find_rather_than_stat_ing_locally(self):
        ep = FakeRemote()
        _list(ep)
        assert any(c[0] == "find" for c in ep.calls), (
            "no remote command was issued; the scan is still local"
        )

    def test_metadata_comes_from_the_remote_info_xml(self):
        ep = FakeRemote()
        backups = _list(ep)
        meta = backups[0]["metadata"]
        assert meta is not None, "info.xml was fetched but not parsed"
        assert meta.description == "snapshot 1"

    def test_results_are_ordered_by_snapshot_number(self):
        ep = FakeRemote(slots=(10, 2, 7))
        assert [b["number"] for b in _list(ep)] == [2, 7, 10]

    def test_paths_point_at_the_remote_layout(self):
        ep = FakeRemote()
        b = _list(ep)[0]
        assert b["snapshot_path"] == "/remote/dest/.snapshots/1/snapshot"
        assert b["info_xml_path"] == "/remote/dest/.snapshots/1/info.xml"


class TestPartialAndTransactionalStates:
    def test_transactional_temp_slots_are_not_backups(self):
        """.incoming / .stale are this run's temps, never restorable backups."""
        ep = FakeRemote(slots=(1,))
        ep.slots = [1]
        original = ep._exec_remote_command

        def with_temps(command, **kwargs):
            if command[0] == "find":
                base = command[1]
                out = "\n".join([f"{base}/1", f"{base}/2.incoming", f"{base}/3.stale"])
                ep.calls.append(list(command))
                return MagicMock(returncode=0, stdout=out.encode(), stderr=b"")
            return original(command, **kwargs)

        ep._exec_remote_command = with_temps
        assert [b["number"] for b in _list(ep)] == [1]

    def test_a_slot_without_a_published_snapshot_is_skipped(self):
        """A publish that never completed must not look restorable."""
        ep = FakeRemote(slots=(1, 2), fail={"test": (1, "")})
        assert _list(ep) == []

    def test_a_backup_without_info_xml_still_enumerates(self):
        """Older backups predate info.xml placement; they are still restorable."""
        ep = FakeRemote(slots=(1, 2), info_for=[1])
        backups = _list(ep)
        assert [b["number"] for b in backups] == [1, 2]
        missing = next(b for b in backups if b["number"] == 2)
        assert missing["metadata"] is None
        assert missing["info_xml_path"] is None


class TestAFailedScanIsNotAnEmptyResult:
    @pytest.mark.parametrize(
        "stderr",
        [
            "find: '/remote/dest/.snapshots': Permission denied",
            "find: '/remote/dest/.snapshots': No such file or directory",
            "sh: find: command not found",
            "",
        ],
    )
    def test_a_failed_find_raises(self, stderr):
        ep = FakeRemote(fail={"find": (1, stderr)})
        with pytest.raises(RuntimeError, match="NOT an empty target"):
            _list(ep)

    def test_the_error_repeats_what_find_said(self):
        ep = FakeRemote(fail={"find": (1, "find: '/x': Permission denied")})
        with pytest.raises(RuntimeError) as excinfo:
            _list(ep)
        assert "Permission denied" in str(excinfo.value)

    def test_a_missing_location_raises_rather_than_reporting_none(self):
        """A mistyped path is not an empty backup set."""
        ep = FakeRemote(fail={"find": (1, "No such file or directory")})
        with pytest.raises(RuntimeError, match="NOT an empty target"):
            _list(ep)

    def test_an_empty_but_readable_location_is_genuinely_empty(self):
        """Exit 0 with no slots is the one case that really is empty."""
        ep = FakeRemote(slots=())
        assert _list(ep) == []


class TestOutputIsCaptured:
    """SSHEndpoint._exec_remote_command captures nothing by default.

    Unlike SSHRawEndpoint's, it forwards kwargs straight to subprocess. The first
    version of this enumerator relied on the default, so find's output went to the
    console and the scan reported "no backups" from an empty variable -- an empty
    listing produced by not looking.
    """

    def test_every_remote_call_requests_pipes(self):
        ep = FakeRemote()
        seen = []

        original = ep._exec_remote_command

        def recording(command, **kwargs):
            seen.append(kwargs)
            return original(command, **kwargs)

        ep._exec_remote_command = recording
        _list(ep)
        assert seen, "no remote calls were made"
        for kwargs in seen:
            assert kwargs.get("stdout") is subprocess.PIPE, kwargs
            assert kwargs.get("stderr") is subprocess.PIPE, kwargs
