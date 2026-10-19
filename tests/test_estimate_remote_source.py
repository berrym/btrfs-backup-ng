"""An estimate of an ssh:// source measures the snapshots where they live.

`estimate_transfer` built each snapshot's path as config["path"] / name and
ran the local size commands on it, whatever the source endpoint was. For a
volume whose source is ssh://host/path that is a path on the remote host, so
every local measurement failed and every row read "unknown" (#124). The
engine's space check solved the same problem by asking the endpoint that
holds the snapshot (measure_transfer_size); the estimate now asks it too.

The remote measurement itself ran `btrfs filesystem du` as the remote user
once. A snapshot made by root holds directories only root can walk, so on a
sudo-elevated target it measured nothing. It now runs `sudo -n` first where
the target elevates and then as the user, the two attempts the local ladder
makes, and a Total of 0 is no answer.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.core import estimate as estimate_mod
from btrfs_backup_ng.core.estimate import SizeEstimate, estimate_transfer
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint


def _snapshot(name: str, when: int, path: str = "/remote/snaps"):
    snap = MagicMock()
    snap.get_name.return_value = name
    snap.time_obj = when
    snap.get_path.return_value = f"{path}/{name}"
    return snap


class _RemoteSource:
    """What an ssh:// source endpoint offers the estimate."""

    _is_remote = True
    hostname = "nas"

    def __init__(self, snapshots, sizes):
        self.config = {"path": "/remote/snaps", "ssh_sudo": True}
        self._snapshots = snapshots
        self._sizes = sizes
        self.measured: list[str] = []

    def list_snapshots(self):
        return list(self._snapshots)

    def measure_transfer_size(self, snapshot, parent=None):
        self.measured.append(snapshot.get_name())
        size = self._sizes.get(snapshot.get_name())
        return None if size is None else SizeEstimate(size, "filesystem_du")


def _destination(present=()):
    dest = MagicMock()
    dest.list_snapshots.return_value = [_snapshot(n, 0) for n in present]
    return dest


class TestARemoteSourceIsMeasuredWhereItLives:
    @pytest.fixture(autouse=True)
    def _no_local_ladder(self, monkeypatch):
        def forbidden(*a, **k):
            raise AssertionError("the local ladder ran on a remote path")

        monkeypatch.setattr(estimate_mod, "estimate_snapshot_full_size", forbidden)
        monkeypatch.setattr(estimate_mod, "estimate_incremental_size", forbidden)

    def test_each_snapshot_is_asked_of_the_endpoint(self):
        snaps = [_snapshot("home.1", 1), _snapshot("home.2", 2)]
        source = _RemoteSource(snaps, {"home.1": 4096, "home.2": 8192})

        estimate = estimate_transfer(source, _destination())

        assert source.measured == ["home.1", "home.2"]
        assert [(s.name, s.full_size, s.method) for s in estimate.snapshots] == [
            ("home.1", 4096, "filesystem_du"),
            ("home.2", 8192, "filesystem_du"),
        ]
        assert estimate.total_incremental_size == 4096 + 8192
        assert estimate.unmeasured_count == 0

    def test_incremental_sizes_are_not_estimated_and_it_is_said(self, caplog):
        snaps = [_snapshot("home.1", 1), _snapshot("home.2", 2)]
        source = _RemoteSource(snaps, {"home.1": 4096, "home.2": 8192})

        with caplog.at_level(logging.INFO, logger=estimate_mod.logger.name):
            estimate = estimate_transfer(source, _destination())

        assert all(not s.is_incremental for s in estimate.snapshots)
        assert any(
            "incremental sizes are not estimated for a remote source" in r.getMessage()
            and "nas" in r.getMessage()
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    def test_a_snapshot_the_remote_cannot_measure_is_unmeasured(self):
        source = _RemoteSource([_snapshot("home.1", 1)], {})

        estimate = estimate_transfer(source, _destination())

        (row,) = estimate.snapshots
        assert (row.full_size, row.method) == (None, "failed")
        assert estimate.unmeasured_count == 1

    def test_snapshots_already_at_the_destination_are_not_measured(self):
        source = _RemoteSource([_snapshot("home.1", 1)], {"home.1": 1})

        estimate = estimate_transfer(source, _destination(present=["home.1"]))

        assert source.measured == [] and estimate.skipped_count == 1


class TestALocalSourceStillUsesTheLocalLadder:
    def test_a_mock_endpoint_is_not_taken_for_a_remote_one(self, monkeypatch, tmp_path):
        """A MagicMock answers every attribute with a truthy object; only an
        endpoint that says `_is_remote = True` is measured remotely."""
        asked = []
        monkeypatch.setattr(
            estimate_mod,
            "estimate_snapshot_full_size",
            lambda p, use_sudo=False: asked.append(str(p)) or (10, "du"),
        )
        source = MagicMock()
        source.config = {"path": tmp_path, "ssh_sudo": False}
        source.list_snapshots.return_value = [_snapshot("home.1", 1)]

        estimate = estimate_transfer(source, _destination())

        assert asked == [str(tmp_path / "home.1")]
        assert estimate.snapshots[0].full_size == 10


class TestTheRemoteMeasurementElevatesFirst:
    def _endpoint(self, ssh_sudo: bool):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": "/remote/snaps", "ssh_sudo": ssh_sudo}
        ep.hostname = "nas"
        return ep

    @staticmethod
    def _answers(table):
        """A remote whose answer depends on whether sudo led the command."""
        asked = []

        def run(argv, **kwargs):
            asked.append(list(argv))
            elevated = argv[:2] == ["sudo", "-n"]
            rc, out = table[elevated]
            return SimpleNamespace(returncode=rc, stdout=out.encode(), stderr=b"")

        return run, asked

    TOTAL = (
        "Total   Exclusive  Set shared  Filename\n123456   100   23   /remote/snaps/s\n"
    )

    def test_sudo_first_and_its_answer_is_taken(self, monkeypatch):
        run, asked = self._answers({True: (0, self.TOTAL), False: (0, "unexpected")})
        ep = self._endpoint(ssh_sudo=True)
        monkeypatch.setattr(ep, "_exec_remote_command", run)
        monkeypatch.setattr(ep, "_normalize_path", lambda p: p)

        assert ep.estimate_transfer_size(_snapshot("s", 1)) == 123456
        assert [a[:2] for a in asked] == [["sudo", "-n"]]

    def test_a_refused_sudo_is_followed_by_the_user(self, monkeypatch):
        run, asked = self._answers({True: (1, ""), False: (0, self.TOTAL)})
        ep = self._endpoint(ssh_sudo=True)
        monkeypatch.setattr(ep, "_exec_remote_command", run)
        monkeypatch.setattr(ep, "_normalize_path", lambda p: p)

        assert ep.estimate_transfer_size(_snapshot("s", 1)) == 123456
        assert [a[:2] for a in asked] == [["sudo", "-n"], ["btrfs", "filesystem"]]

    def test_a_total_of_zero_under_sudo_is_no_answer(self, monkeypatch):
        zero = "Total   Exclusive  Set shared  Filename\n0   0   0   /remote/snaps/s\n"
        run, asked = self._answers({True: (0, zero), False: (0, self.TOTAL)})
        ep = self._endpoint(ssh_sudo=True)
        monkeypatch.setattr(ep, "_exec_remote_command", run)
        monkeypatch.setattr(ep, "_normalize_path", lambda p: p)

        assert ep.estimate_transfer_size(_snapshot("s", 1)) == 123456
        assert len(asked) == 2

    def test_without_elevation_only_the_user_is_asked(self, monkeypatch):
        run, asked = self._answers({True: (0, self.TOTAL), False: (0, self.TOTAL)})
        ep = self._endpoint(ssh_sudo=False)
        monkeypatch.setattr(ep, "_exec_remote_command", run)
        monkeypatch.setattr(ep, "_normalize_path", lambda p: p)

        assert ep.estimate_transfer_size(_snapshot("s", 1)) == 123456
        assert asked == [
            ["btrfs", "filesystem", "du", "-s", "--raw", "/remote/snaps/s"]
        ]

    def test_no_answer_from_either_is_none(self, monkeypatch):
        run, asked = self._answers({True: (1, ""), False: (1, "")})
        ep = self._endpoint(ssh_sudo=True)
        monkeypatch.setattr(ep, "_exec_remote_command", run)
        monkeypatch.setattr(ep, "_normalize_path", lambda p: p)

        assert ep.estimate_transfer_size(_snapshot("s", 1)) is None
        assert len(asked) == 2

    def test_an_incremental_is_not_measured(self, monkeypatch):
        run, asked = self._answers({True: (0, self.TOTAL), False: (0, self.TOTAL)})
        ep = self._endpoint(ssh_sudo=True)
        monkeypatch.setattr(ep, "_exec_remote_command", run)
        assert (
            ep.estimate_transfer_size(_snapshot("s", 1), parent=_snapshot("p", 0))
            is None
        )
        assert asked == []


class TestTheCommandGivesARemoteSourceItsPath:
    """`estimate ssh://host:/snapshots DEST`: choose_endpoint keeps an ssh://
    source's URL path under "source" and leaves "path", which the listing
    reads, unset, so the estimate listed '' on the remote and failed. The
    command now hands the snapshot directory and the ssh options to the
    source endpoint as it does to the destination."""

    def test_the_source_endpoint_gets_the_path_and_the_ssh_options(self, monkeypatch):
        from btrfs_backup_ng.cli import estimate as estimate_cli

        built = []

        def choose(spec, kwargs, source=False):
            built.append((spec, dict(kwargs), source))
            ep = MagicMock()
            ep.config = dict(kwargs)
            return ep

        monkeypatch.setattr(estimate_cli, "create_logger", lambda *a, **k: None)
        monkeypatch.setattr(estimate_cli.endpoint, "choose_endpoint", choose)
        monkeypatch.setattr(
            estimate_cli,
            "estimate_transfer",
            lambda s, d: estimate_mod.TransferEstimate(),
        )
        args = SimpleNamespace(
            volume=None,
            source="ssh://mberry@nas:/home/mberry/snaps",
            destination="/var/tmp/dest",
            json=False,
            check_space=False,
            safety_margin=10,
            prefix="vol-",
            ssh_sudo=True,
            ssh_key="/home/me/.ssh/id",
            ssh_auth_sock="/run/agent",
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

        assert estimate_cli.execute_estimate(args) == 0

        (spec, kwargs, is_source), _dest = built
        assert (spec, is_source) == ("ssh://mberry@nas:/home/mberry/snaps", True)
        assert kwargs["path"] == "/home/mberry/snaps"
        assert kwargs["ssh_sudo"] is True
        assert kwargs["ssh_identity_file"] == "/home/me/.ssh/id"
        assert kwargs["ssh_auth_sock"] == "/run/agent"
