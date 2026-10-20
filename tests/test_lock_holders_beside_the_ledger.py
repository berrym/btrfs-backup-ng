"""Who holds each pin in a lock file is recorded beside the file.

A pin in the lock file is the protection: it survives the process that took
it, so an interrupted backup keeps the source from pruning what a later run
still has to send. Nothing in the file said who took a pin or whether they
were still there, so doctor could not tell a leftover from a pin at work.
The directory store an ssh:// location keeps had the answer for years: one
record per pin with hostname, pid and a heartbeat. The same record is now
written beside the lock file, in ``<lock file>.holders``, by the same
machinery, and read by doctor and ``restore --status``. The file's format is
untouched, so every reader of it, and every older version, is unaffected.

The record is evidence, not protection: it outlives the process as the pin
does, its beat stops with the process, and a record that cannot be written
is a warning, never a failed pin.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__, lifecycle
from btrfs_backup_ng.cli import restore as restore_cli
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.sshutil.lock import holders_beside

LOCK_FILE = ".btrfs-backup-ng.locks"
HOLDERS = LOCK_FILE + ".holders"
S0 = "home-20260101-000000"


@pytest.fixture(autouse=True)
def _no_btrfs_identity(monkeypatch):
    monkeypatch.setattr(LocalEndpoint, "subvolume_identity", lambda self, path: None)


def _location(path: Path) -> LocalEndpoint:
    (path / S0).mkdir(parents=True, exist_ok=True)
    return LocalEndpoint(
        config={"path": path, "source": "/src", "snap_prefix": "home-"}
    )


def _snap(ep: LocalEndpoint, name: str = S0):
    return {s.get_name(): s for s in ep.list_snapshots(flush_cache=True)}[name]


def _ledger(path: Path) -> dict:
    raw = __util__.read_locks((path / LOCK_FILE).read_text())
    return {n: {k: set(v) for k, v in e.items()} for n, e in raw.items()}


def _records(path: Path) -> dict:
    """{lock name: {holder id: (pid, hostname)}} as read beside the file."""
    return {
        name: {h.lock_id: (h.info.get("pid"), h.info.get("hostname")) for h, _ in rs}
        for name, rs in holders_beside(path / LOCK_FILE).shared_records().items()
    }


class TestARecordIsWrittenBesideTheFile:
    def test_a_pin_and_a_parent_pin_each_get_a_record(self, tmp_path):
        ep = _location(tmp_path)
        ep.set_lock(_snap(ep), "restore:abc", True)
        ep.set_lock(_snap(ep), "/dest/one", True, parent=True)

        assert _ledger(tmp_path) == {
            S0: {"locks": {"restore:abc"}, "parent_locks": {"/dest/one"}}
        }
        assert _records(tmp_path) == {
            f"snap-{S0}": {
                "restore:abc": (os.getpid(), os.uname().nodename),
                "p:/dest/one": (os.getpid(), os.uname().nodename),
            }
        }

    def test_a_release_removes_the_record_and_only_that_one(self, tmp_path):
        ep = _location(tmp_path)
        ep.set_lock(_snap(ep), "restore:abc", True)
        ep.set_lock(_snap(ep), "/dest/one", True, parent=True)
        ep.set_lock(_snap(ep), "restore:abc", False)

        assert _ledger(tmp_path) == {S0: {"parent_locks": {"/dest/one"}}}
        assert set(_records(tmp_path)[f"snap-{S0}"]) == {"p:/dest/one"}

    def test_the_holders_directory_is_not_a_snapshot(self, tmp_path):
        ep = _location(tmp_path)
        ep.set_lock(_snap(ep), "restore:abc", True)
        assert (tmp_path / HOLDERS).is_dir()
        assert [s.get_name() for s in ep.list_snapshots(flush_cache=True)] == [S0]


class TestTheRecordOutlivesTheProcessAsThePinDoes:
    def test_the_exit_cleanups_leave_the_record(self, tmp_path):
        """The exit drain runs every cleanup a lock manager registered; the
        record of a pin in a lock file is not registered, so it stays, as the
        pin does. Only the cleanups of this manager are run here: the drain
        itself also stops every child process and sets the stop flag."""
        ep = _location(tmp_path)
        ep.set_lock(_snap(ep), "restore:abc", True)
        manager = ep._holders_manager()

        lifecycle.run_cleanups(
            select=lambda key: (
                isinstance(key, tuple) and key[:2] == ("remote-lock", id(manager))
            )
        )

        assert _ledger(tmp_path) == {S0: {"locks": {"restore:abc"}}}
        assert set(_records(tmp_path)[f"snap-{S0}"]) == {"restore:abc"}


class TestTheRecordIsEvidenceNotProtection:
    def test_a_record_that_cannot_be_written_is_a_warning_and_the_pin_stands(
        self, tmp_path, shared_log, monkeypatch
    ):
        ep = _location(tmp_path)

        def unusable(*a, **k):
            raise RuntimeError("the holders directory cannot be written")

        monkeypatch.setattr(
            ep._holders_manager(), "acquire_shared_persistent", unusable
        )
        ep.set_lock(_snap(ep), "restore:abc", True)

        assert _ledger(tmp_path) == {S0: {"locks": {"restore:abc"}}}
        (warning,) = shared_log.messages(logging.WARNING)
        assert "holder record" in warning and "could not be written" in warning
        assert _records(tmp_path) == {}


class TestUnlockReleasesTheRecordsOfThePinsItRemoves:
    def test_unlock_all(self, tmp_path, monkeypatch):
        ep = _location(tmp_path)
        ep.set_lock(_snap(ep), "restore:abc", True)
        ep.set_lock(_snap(ep), "/dest/one", True)
        args = type(
            "Args", (), {"source": str(tmp_path), "fs_checks": "skip", "prefix": ""}
        )()
        monkeypatch.setattr(restore_cli, "_prepare_backup_endpoint", lambda a, s: ep)

        assert restore_cli._execute_unlock(args, "all") == 0

        assert _ledger(tmp_path) == {S0: {"locks": {"/dest/one"}}}
        assert set(_records(tmp_path)[f"snap-{S0}"]) == {"/dest/one"}


class TestStatusShowsWhoHoldsEachPin:
    def test_a_live_holder_and_a_gone_one(self, tmp_path, monkeypatch, capsys):
        ep = _location(tmp_path)
        ep.set_lock(_snap(ep), "restore:live", True)
        ep.set_lock(_snap(ep), "/dest/one", True)
        gone = [
            p
            for p in (tmp_path / HOLDERS).rglob("holders/*")
            if "dest" in p.read_text()
        ]
        old = os.path.getmtime(gone[0]) - 1000
        os.utime(gone[0], (old, old))
        args = type(
            "Args", (), {"source": str(tmp_path), "fs_checks": "skip", "prefix": ""}
        )()
        monkeypatch.setattr(restore_cli, "_prepare_backup_endpoint", lambda a, s: ep)

        assert restore_cli._execute_status(args) == 0

        out = capsys.readouterr().out
        assert f"session live (pid {os.getpid()} on {os.uname().nodename}, beat" in out
        assert "/dest/one (holder gone: pid" in out and "beat stopped" in out
