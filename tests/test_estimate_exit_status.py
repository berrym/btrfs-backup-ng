"""`estimate` exits with a status that says what its output says.

It returned 1 only when nothing ran, and 0 once the estimate had run,
whatever it found: every row "unknown", a space check that could not be
made, or a destination the check found short, all exited 0 (#123). A script
guarding a transfer reads the status, not the output. Now 2 means the
estimate ran but is not a measurement (a snapshot that would transfer has no
size, or --check-space could not be made), 3 means the check ran on measured
sizes and the destination is short, and 0 means complete and fitting, the
convention `config validate` set for "ran, but cannot answer".

The command is driven end to end with stand-in endpoints and a fixed
estimate, in the text and the JSON form, and the two printers directly.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.cli import estimate as estimate_cli
from btrfs_backup_ng.cli.estimate import (
    EXIT_INCOMPLETE,
    EXIT_INSUFFICIENT,
    _print_json,
    _print_space_check,
    execute_estimate,
)
from btrfs_backup_ng.core.estimate import SnapshotEstimate, TransferEstimate
from btrfs_backup_ng.core.space import SpaceInfo

GIB = 1024**3


def _estimate(*sizes):
    """A transfer of full snapshots with these sizes; None is unmeasured."""
    estimate = TransferEstimate()
    for i, size in enumerate(sizes):
        estimate.add_snapshot(SnapshotEstimate(name=f"home.{i}", full_size=size))
    estimate.new_snapshot_count = len(sizes)
    return estimate


def _destination(available=100 * GIB, failing=False):
    dest = MagicMock()
    if failing:
        dest.get_space_info.side_effect = OSError("statvfs failed")
    else:
        dest.get_space_info.return_value = SpaceInfo(
            path="/d", total_bytes=200 * GIB, used_bytes=0, available_bytes=available
        )
    return dest


def _args(**overrides):
    base = dict(check_space=False, safety_margin=10, json=False)
    base.update(overrides)
    return SimpleNamespace(**base)


class TestTheTextPrinterDecides:
    def test_a_complete_estimate_without_a_check(self, capsys):
        assert _print_space_check(_args(), _destination(), _estimate(GIB)) == 0

    def test_an_unmeasured_snapshot_without_a_check(self):
        assert (
            _print_space_check(_args(), _destination(), _estimate(GIB, None))
            == EXIT_INCOMPLETE
        )

    def test_a_check_that_fits(self, capsys):
        rc = _print_space_check(_args(check_space=True), _destination(), _estimate(GIB))
        assert rc == 0
        assert "OK - Sufficient" in capsys.readouterr().out

    def test_a_check_that_finds_the_destination_short(self, capsys):
        rc = _print_space_check(
            _args(check_space=True), _destination(available=GIB), _estimate(5 * GIB)
        )
        assert rc == EXIT_INSUFFICIENT
        assert "INSUFFICIENT" in capsys.readouterr().out

    def test_a_check_with_nothing_measured(self, capsys):
        rc = _print_space_check(
            _args(check_space=True), _destination(), _estimate(None)
        )
        assert rc == EXIT_INCOMPLETE
        assert "Space was NOT verified" in capsys.readouterr().out

    def test_a_check_on_a_partial_sum_says_so(self, capsys):
        """One measured, one not: the check runs on the partial total, says so,
        and the status is incomplete even though the partial total fits."""
        rc = _print_space_check(
            _args(check_space=True), _destination(), _estimate(GIB, None)
        )
        out = capsys.readouterr().out
        assert rc == EXIT_INCOMPLETE
        assert "OK - Sufficient" in out
        assert "partial sum" in out and "NOT verified" in out

    def test_a_check_that_could_not_be_made(self, capsys):
        rc = _print_space_check(
            _args(check_space=True), _destination(failing=True), _estimate(GIB)
        )
        assert rc == EXIT_INCOMPLETE
        assert "Could not check destination space" in capsys.readouterr().out

    def test_nothing_to_transfer_is_complete(self, capsys):
        assert (
            _print_space_check(_args(check_space=True), _destination(), _estimate())
            == 0
        )
        assert "No data to transfer" in capsys.readouterr().out


class TestTheJsonPrinterDecides:
    def _run(self, capsys, estimate, dest, **args):
        rc = _print_json(estimate, "/s", "/d", dest, _args(**args))
        return rc, json.loads(capsys.readouterr().out)

    def test_a_complete_estimate(self, capsys):
        rc, data = self._run(capsys, _estimate(GIB), _destination())
        assert rc == 0 and data["unmeasured_count"] == 0

    def test_an_unmeasured_snapshot(self, capsys):
        rc, data = self._run(capsys, _estimate(GIB, None), _destination())
        assert rc == EXIT_INCOMPLETE and data["unmeasured_count"] == 1

    def test_a_check_that_fits(self, capsys):
        rc, data = self._run(capsys, _estimate(GIB), _destination(), check_space=True)
        assert rc == 0 and data["space_check"]["sufficient"] is True

    def test_a_check_that_finds_the_destination_short(self, capsys):
        rc, data = self._run(
            capsys, _estimate(5 * GIB), _destination(available=GIB), check_space=True
        )
        assert rc == EXIT_INSUFFICIENT and data["space_check"]["sufficient"] is False

    def test_a_check_that_could_not_be_made(self, capsys):
        rc, data = self._run(
            capsys, _estimate(GIB), _destination(failing=True), check_space=True
        )
        assert rc == EXIT_INCOMPLETE and data["space_check"]["sufficient"] is None

    def test_an_unmeasured_snapshot_under_a_check_that_fits(self, capsys):
        rc, data = self._run(
            capsys, _estimate(GIB, None), _destination(), check_space=True
        )
        assert rc == EXIT_INCOMPLETE
        assert data["space_check"]["sufficient"] is True


class TestTheCommandEndToEnd:
    """execute_estimate with stand-in endpoints and a fixed estimate, both
    forms, every status."""

    def _run(self, monkeypatch, capsys, estimate, dest, *, as_json, check):
        monkeypatch.setattr(estimate_cli, "create_logger", lambda *a, **k: None)
        monkeypatch.setattr(
            estimate_cli.endpoint, "choose_endpoint", lambda *a, **k: dest
        )
        monkeypatch.setattr(estimate_cli, "estimate_transfer", lambda s, d: estimate)
        args = SimpleNamespace(
            volume=None,
            source="/src",
            destination="/dst",
            json=as_json,
            check_space=check,
            safety_margin=10,
            prefix="",
            ssh_sudo=False,
            ssh_key=None,
            ssh_auth_sock=None,
            ssh_host_key_policy=None,
            config=None,
            timestamp_format=None,
            verbose=False,
            quiet=False,
            debug=False,
            log_level=None,
            fs_checks=None,
            no_fs_checks=False,
        )
        rc = execute_estimate(args)
        return rc, capsys.readouterr().out

    @pytest.mark.parametrize("as_json", [False, True], ids=["text", "json"])
    def test_every_status(self, monkeypatch, capsys, as_json):
        cases = [
            (_estimate(GIB), _destination(), False, 0),
            (_estimate(GIB, None), _destination(), False, EXIT_INCOMPLETE),
            (_estimate(GIB), _destination(), True, 0),
            (_estimate(5 * GIB), _destination(available=GIB), True, EXIT_INSUFFICIENT),
            (_estimate(None), _destination(), True, EXIT_INCOMPLETE),
            (_estimate(GIB), _destination(failing=True), True, EXIT_INCOMPLETE),
            (_estimate(), _destination(), True, 0),
        ]
        for i, (estimate, dest, check, expected) in enumerate(cases):
            rc, out = self._run(
                monkeypatch, capsys, estimate, dest, as_json=as_json, check=check
            )
            assert rc == expected, (i, rc, out)

    def test_nothing_ran_is_still_1(self, monkeypatch):
        monkeypatch.setattr(estimate_cli, "create_logger", lambda *a, **k: None)

        def refuse(*a, **k):
            raise RuntimeError("no such endpoint")

        monkeypatch.setattr(estimate_cli.endpoint, "choose_endpoint", refuse)
        args = SimpleNamespace(
            volume=None,
            source="/src",
            destination="/dst",
            json=False,
            check_space=True,
            safety_margin=10,
            prefix="",
            ssh_sudo=False,
            ssh_key=None,
            ssh_auth_sock=None,
            ssh_host_key_policy=None,
            config=None,
            timestamp_format=None,
            verbose=False,
            quiet=False,
            debug=False,
            log_level=None,
            fs_checks=None,
            no_fs_checks=False,
        )
        assert execute_estimate(args) == 1
