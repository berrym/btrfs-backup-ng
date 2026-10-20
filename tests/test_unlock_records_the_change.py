"""``restore --unlock`` releases the pins it read, as a change to the state at
the moment of writing.

It read the lock state, removed the restore pins from its copy and wrote the
copy back whole: ``_write_locks`` for a lock file, which reconciled a
directory store to the copy. Neither write took the guard ``set_lock`` takes,
and neither recorded a change, so a pin a run took between the read and the
write was erased and a pin the run released came back. ``--unlock`` is run
while something else may well be running, since it exists to clean up after
an interrupted operation.

Now the command decides from what it read which pins to release, and the
endpoint releases exactly those: each lock-file entry changed under the guard
through ``_update_lock_entry``, each directory-store pin by its own holder
record. What the command reports is what was released. The doctor's
stale-lock fix is covered by its own issue.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from filelock import FileLock

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import restore as restore_cli
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
from btrfs_backup_ng.sshutil.lock import (
    LOCK_DIR_NAME,
    RemoteLockManager,
    read_persisted_locks,
)

LOCK_FILE = ".btrfs-backup-ng.locks"


def _endpoint(path: Path) -> LocalEndpoint:
    return LocalEndpoint(config={"path": path, "snap_prefix": "home-"})


def _snap(name: str):
    return SimpleNamespace(locks=set(), parent_locks=set(), get_name=lambda: name)


def _file(path: Path) -> dict:
    raw = __util__.read_locks((path / LOCK_FILE).read_text())
    return {n: {k: set(v) for k, v in e.items()} for n, e in raw.items()}


def _args(path: Path) -> argparse.Namespace:
    return argparse.Namespace(source=str(path), fs_checks="skip", prefix="")


def _unlock(path: Path, lock_id: str, ep=None) -> int:
    ep = _endpoint(path) if ep is None else ep
    with patch.object(restore_cli, "_prepare_backup_endpoint", lambda a, s: ep):
        return restore_cli._execute_unlock(_args(path), lock_id)


def _unlock_with_a_run_in_between(path: Path, lock_id: str, run) -> int:
    """``--unlock``, with ``run(path)`` changing the lock state after the
    command has read it and before it writes, as another process would."""
    ep = _endpoint(path)
    real_read = ep._read_locks
    calls = {"n": 0}

    def read_then_the_run_acts():
        calls["n"] += 1
        state = real_read()
        if calls["n"] == 1:
            run(path)
        return state

    ep._read_locks = read_then_the_run_acts  # type: ignore[method-assign]
    return _unlock(path, lock_id, ep)


class TestUnlockKeepsWhatARunDoesMeanwhile:
    def test_a_pin_taken_meanwhile_is_kept(self, tmp_path):
        (tmp_path / LOCK_FILE).write_text(
            __util__.write_locks({"s1": {"locks": ["restore:abc", "backup:xyz"]}})
        )

        def run(path):
            other = _endpoint(path)
            other.set_lock(_snap("s1"), "/dest/one", True)
            other.set_lock(_snap("s2"), "/dest/one", True, parent=True)

        assert _unlock_with_a_run_in_between(tmp_path, "all", run) == 0
        assert _file(tmp_path) == {
            "s1": {"locks": {"backup:xyz", "/dest/one"}},
            "s2": {"parent_locks": {"/dest/one"}},
        }

    def test_a_pin_released_meanwhile_does_not_come_back(self, tmp_path):
        (tmp_path / LOCK_FILE).write_text(
            __util__.write_locks(
                {
                    "s1": {"locks": ["restore:abc"]},
                    "s2": {"locks": ["/dest/one"], "parent_locks": ["restore:abc"]},
                }
            )
        )

        def run(path):
            _endpoint(path).set_lock(_snap("s2"), "/dest/one", False)

        assert _unlock_with_a_run_in_between(tmp_path, "abc", run) == 0
        assert _file(tmp_path) == {}

    def test_what_it_reports_is_what_it_removed(self, tmp_path, capsys):
        (tmp_path / LOCK_FILE).write_text(
            __util__.write_locks(
                {
                    "s1": {"locks": ["restore:a", "restore:b"]},
                    "s2": {"parent_locks": ["restore:a"], "locks": ["backup:k"]},
                    "s3": {"locks": ["backup:m"]},
                }
            )
        )

        def run(path):
            _endpoint(path).set_lock(_snap("s1"), "restore:b", False)

        assert _unlock_with_a_run_in_between(tmp_path, "all", run) == 0
        out = capsys.readouterr().out
        assert "Unlocked 2 lock(s)." in out
        assert "Remaining locks: 2 snapshot(s)" in out
        assert _file(tmp_path) == {
            "s2": {"locks": {"backup:k"}},
            "s3": {"locks": {"backup:m"}},
        }

    def test_it_waits_for_the_guard(self, tmp_path):
        (tmp_path / LOCK_FILE).write_text(
            __util__.write_locks({"s1": {"locks": ["restore:abc"]}})
        )
        rc: list = []
        guard = FileLock(str(tmp_path / LOCK_FILE) + ".guard")
        with guard:
            worker = threading.Thread(
                target=lambda: rc.append(_unlock(tmp_path, "all"))
            )
            worker.start()
            worker.join(0.5)
            assert worker.is_alive(), "it wrote without the guard"
            assert _file(tmp_path) == {"s1": {"locks": {"restore:abc"}}}
        worker.join(10)
        assert rc == [0]
        assert _file(tmp_path) == {}

    def test_a_write_that_fails_part_way_says_how_far_it_got(self, tmp_path, caplog):
        """One entry at a time means a failure can come after some entries are
        done: those stay done, the rest stay as they were, and the error says
        how many were unlocked."""
        (tmp_path / LOCK_FILE).write_text(
            __util__.write_locks(
                {"s1": {"locks": ["restore:a"]}, "s2": {"locks": ["restore:a"]}}
            )
        )
        ep = _endpoint(tmp_path)
        real_write = ep._write_locks
        writes = {"n": 0}

        def second_write_fails(lock_dict):
            writes["n"] += 1
            if writes["n"] == 2:
                raise __util__.AbortError("Cannot write the lock file: disk full")
            real_write(lock_dict)

        ep._write_locks = second_write_fails  # type: ignore[method-assign]
        with caplog.at_level("ERROR"):
            rc = _unlock(tmp_path, "all", ep)

        assert rc == 1
        assert _file(tmp_path) == {"s2": {"locks": {"restore:a"}}}
        assert "1 lock(s) unlocked before this" in caplog.text


class TestUnlockRemovesOnlyWhatItRead:
    """A restore that began after --unlock read the file is live: it was never
    shown as a leftover, so it is kept, whether or not its snapshot's entry had
    a restore pin when --unlock read."""

    def test_on_an_entry_without_a_restore_pin_when_read(self, tmp_path):
        (tmp_path / LOCK_FILE).write_text(
            __util__.write_locks(
                {"s1": {"locks": ["restore:old"]}, "s3": {"locks": ["backup:m"]}}
            )
        )

        def run(path):
            _endpoint(path).set_lock(_snap("s3"), "restore:new", True)

        assert _unlock_with_a_run_in_between(tmp_path, "all", run) == 0
        assert _file(tmp_path) == {"s3": {"locks": {"backup:m", "restore:new"}}}

    def test_on_an_entry_with_a_restore_pin_when_read(self, tmp_path):
        (tmp_path / LOCK_FILE).write_text(
            __util__.write_locks({"s1": {"locks": ["restore:old"]}})
        )

        def run(path):
            _endpoint(path).set_lock(_snap("s1"), "restore:new", True)

        assert _unlock_with_a_run_in_between(tmp_path, "all", run) == 0
        assert _file(tmp_path) == {"s1": {"locks": {"restore:new"}}}


def _manager(target: Path) -> RemoteLockManager:
    """The lock scripts an ssh:// endpoint runs on its remote, run here."""

    def run(script):
        proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    return RemoteLockManager(run, str(target), hostname="test")


def _store(tmp_path: Path) -> Path:
    """A location that carries the directory store a local endpoint honours."""
    location = tmp_path / "location"
    location.mkdir()
    (location / LOCK_DIR_NAME).mkdir()
    return location


def _garbled_holder(target: Path, snapshot: str) -> Path:
    """A holder record whose payload cannot be parsed: reported as ``unknown``."""
    holders = target / LOCK_DIR_NAME / f"snap-{snapshot}.lock" / "holders"
    holders.mkdir(parents=True)
    record = holders / "garbled"
    record.write_text("{{{")
    return record


class TestADirectoryStoreIsReleasedPerHolder:
    """A location whose pins live in a directory store -- a local location
    carrying one, raw://, and the remote ones -- is released by holder record,
    and only the records that were read. No lock file and no guard file appear
    there."""

    @pytest.mark.parametrize(
        "cls", [LocalEndpoint, RawEndpoint, SSHEndpoint, SSHRawEndpoint]
    )
    def test_one_session_is_cleared_and_nothing_else_is_written(self, cls, tmp_path):
        target = tmp_path / "location"
        target.mkdir()
        manager = _manager(target)
        manager.acquire_shared("snap-s1", "restore:a")
        manager.acquire_shared("snap-s1", "restore:b")
        ep = cls.__new__(cls)
        ep.config = {"path": target, "lock_file_name": LOCK_FILE}
        if cls is not LocalEndpoint:
            ep._lock_manager = lambda: manager  # type: ignore[method-assign]
            ep._lock_target_path = lambda: str(target)  # type: ignore[method-assign]
        before = sorted(p.name for p in target.iterdir())

        assert _unlock(target, "a", ep) == 0
        assert read_persisted_locks(_manager(target)) == {
            "s1": {"locks": ["restore:b"]}
        }
        assert sorted(p.name for p in target.iterdir()) == before

    def test_a_pin_taken_meanwhile_is_kept(self, tmp_path, capsys):
        location = _store(tmp_path)
        leftover = _manager(location)
        leftover.acquire_shared("snap-s1", "restore:a")
        leftover.acquire_shared("snap-s2", "p:restore:a")

        def run(path):
            live = _manager(path)
            live.acquire_shared("snap-s1", "restore:new")
            live.acquire_shared("snap-s2", "/dest/one")
            live.acquire_shared("snap-s3", "restore:late")

        assert _unlock_with_a_run_in_between(location, "all", run) == 0
        out = capsys.readouterr().out
        assert "Unlocked 2 lock(s)." in out
        assert "Remaining locks: 2 snapshot(s)" in out
        assert read_persisted_locks(_manager(location)) == {
            "s1": {"locks": ["restore:new"]},
            "s2": {"locks": ["/dest/one"]},
            "s3": {"locks": ["restore:late"]},
        }

    def test_a_pin_released_meanwhile_is_not_counted(self, tmp_path, capsys):
        location = _store(tmp_path)
        leftover = _manager(location)
        leftover.acquire_shared("snap-s1", "restore:a")
        leftover.acquire_shared("snap-s1", "restore:b")

        def run(path):
            leftover.release_shared("snap-s1", "restore:b")

        assert _unlock_with_a_run_in_between(location, "all", run) == 0
        out = capsys.readouterr().out
        assert "Unlocked 1 lock(s)." in out
        assert "All locks cleared." in out
        assert read_persisted_locks(_manager(location)) == {}

    def test_a_record_that_cannot_be_read_is_cleared_by_all_only(self, tmp_path):
        location = _store(tmp_path)
        record = _garbled_holder(location, "x")
        _manager(location).acquire_shared("snap-x", "restore:a")
        read = read_persisted_locks(_manager(location))
        assert set(read["x"]["locks"]) == {"unknown", "restore:a"}

        assert _unlock(location, "a") == 0
        assert record.exists(), "a named session is not the unreadable record"
        assert read_persisted_locks(_manager(location)) == {"x": {"locks": ["unknown"]}}

        assert _unlock(location, "all") == 0
        assert not record.exists()
        assert read_persisted_locks(_manager(location)) == {}

    def test_a_record_that_cannot_be_listed_is_not_reported_as_released(
        self, tmp_path, caplog, capsys
    ):
        if os.geteuid() == 0:
            pytest.skip("root lists any directory")
        location = _store(tmp_path)
        _manager(location).acquire_shared("snap-s1", "restore:a")
        (holders,) = (location / LOCK_DIR_NAME).glob("snap-s1-*.lock/holders")
        holders.chmod(0)
        try:
            with caplog.at_level("WARNING"):
                rc = _unlock(location, "all")
            out = capsys.readouterr().out
        finally:
            holders.chmod(0o755)

        assert rc == 1
        assert "Unlocked" not in out
        assert "could not be listed" in caplog.text
        assert read_persisted_locks(_manager(location)) == {
            "s1": {"locks": ["restore:a"]}
        }

    def test_a_running_operations_target_lock_is_left_alone(self, tmp_path):
        location = _store(tmp_path)
        manager = _manager(location)
        manager.acquire_persistent("target", "prune")
        manager.acquire_shared("snap-a", "restore:1")

        assert _unlock(location, "all") == 0
        assert _manager(location).live_lock_names() == {"target"}
