"""An SSH listing that FAILED must never be reported as an empty location.

`btrfs subvolume list` needs root on the remote. When the connecting user lacks
it, the command dies with

    ERROR: can't perform the search: Operation not permitted

and SSHEndpoint.list_snapshots() used to log a warning and return []. Every
caller then behaved as though the location held no backups: `restore --list`
printed "No snapshots found" and exited 0. Measured against a real host holding
subvolumes -- and during a restore, being told your backups do not exist is the
most dangerous possible moment for that to be wrong.

This is the same defect class already closed for raw+ssh targets in
_check_remote_listing; these tests pin it for the ssh endpoint.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

PERMISSION_DENIED = b"ERROR: can't perform the search: Operation not permitted"


def _endpoint(**config):
    base = {"path": "/backups", "hostname": "nas", "username": "backup"}
    base.update(config)
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = base
    ep.hostname = base["hostname"]
    ep._run_diagnostics = MagicMock()
    ep._is_master_active = MagicMock(return_value=False)
    ep._prime_remote_sudo = MagicMock(return_value=True)
    return ep


def _result(returncode, stdout=b"", stderr=b""):
    return MagicMock(returncode=returncode, stdout=stdout, stderr=stderr)


class TestAFailedListingIsNotAnEmptyLocation:
    @pytest.mark.parametrize(
        "stderr",
        [
            PERMISSION_DENIED,
            b"ERROR: not a btrfs filesystem: /backups",
            b"ERROR: cannot access '/backups': No such file or directory",
            b"sudo: a password is required",
            b"",  # no diagnostic at all -- still a failure
        ],
    )
    def test_a_nonzero_listing_raises(self, stderr):
        ep = _endpoint()
        with patch.object(
            ep, "_exec_remote_command", return_value=_result(1, b"", stderr)
        ):
            with pytest.raises(RuntimeError, match="NOT an empty target"):
                ep.list_snapshots()

    def test_the_error_repeats_what_btrfs_said(self):
        ep = _endpoint()
        with patch.object(
            ep, "_exec_remote_command", return_value=_result(1, b"", PERMISSION_DENIED)
        ):
            with pytest.raises(RuntimeError) as excinfo:
                ep.list_snapshots()
        message = str(excinfo.value)
        assert "Operation not permitted" in message
        assert "nas:/backups" in message
        # and it must say what to do about it
        assert "ssh_sudo" in message

    def test_an_exception_mid_listing_also_raises(self):
        """A transport blowup is equally unknowable, so equally not 'empty'."""
        ep = _endpoint()
        with patch.object(
            ep, "_exec_remote_command", side_effect=OSError("connection reset")
        ):
            with pytest.raises(RuntimeError, match="NOT an empty target"):
                ep.list_snapshots()

    def test_a_successful_empty_listing_is_still_empty(self):
        """Exit 0 with no output is the one case that IS an empty location."""
        ep = _endpoint()
        with patch.object(
            ep, "_exec_remote_command", return_value=_result(0, b"", b"")
        ):
            assert ep.list_snapshots() == []

    def test_a_successful_listing_still_parses(self):
        """Guard against over-correcting: real output must survive.

        The pipeline is parse -> scope-to-destination, so a snapshot that IS at
        the destination has to come back. The scoping probe is stubbed True here;
        the probe's own behaviour is pinned in test_ssh_listing_scope.py."""
        ep = _endpoint()
        snap = MagicMock()
        snap.get_name.return_value = "home-20240101-000000"
        with patch.object(
            ep, "_exec_remote_command", return_value=_result(0, b"x", b"")
        ):
            with patch.object(ep, "_parse_snapshot_list", return_value=[snap]) as parse:
                with patch.object(ep, "_subvolume_exists_at", return_value=True):
                    assert ep.list_snapshots() == [snap]
        assert parse.called


class TestTheSudoPathBehavesTheSameWay:
    def test_elevated_failure_also_raises(self):
        ep = _endpoint(ssh_sudo=True)
        ep._is_master_active = MagicMock(return_value=True)
        with patch.object(
            ep, "_exec_remote_command", return_value=_result(0, b"", b"")
        ):
            with patch.object(
                ep,
                "_exec_remote_command_with_retry",
                return_value=_result(1, b"", PERMISSION_DENIED),
            ):
                with pytest.raises(RuntimeError, match="NOT an empty target"):
                    ep.list_snapshots()


class TestTheErrorIsNotReWrapped:
    """The raise must escape the surrounding `except Exception`.

    Without an explicit re-raise the guard's own RuntimeError is caught by the
    broad handler below it, which re-wraps the message inside a second copy of
    itself and fires _run_diagnostics -- extra remote round-trips on a host we
    have just established we cannot read.
    """

    def test_the_message_is_not_nested(self):
        ep = _endpoint()
        with patch.object(
            ep, "_exec_remote_command", return_value=_result(1, b"", PERMISSION_DENIED)
        ):
            with pytest.raises(RuntimeError) as excinfo:
                ep.list_snapshots()
        message = str(excinfo.value)
        assert message.count("Cannot list snapshots at") == 1, (
            f"the error was re-wrapped by the broad handler: {message}"
        )

    def test_diagnostics_run_once_not_twice(self):
        ep = _endpoint()
        with patch.object(
            ep, "_exec_remote_command", return_value=_result(1, b"", PERMISSION_DENIED)
        ):
            with pytest.raises(RuntimeError):
                ep.list_snapshots()
        assert ep._run_diagnostics.call_count <= 1, (
            "diagnostics fired more than once; the raise is being caught and "
            "re-handled by the broad except"
        )
