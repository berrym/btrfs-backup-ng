"""A shared pin's heartbeat writes the holder's record again, whole.

The beat was a plain ``touch`` of the holder's file. A record removed from
under a live holder -- ``restore --unlock all`` run while the restore was in
progress, a sweep after a long stall -- came back at the next beat as an empty
file, which the listing reports as ``unknown``: the snapshot stayed protected
but the pin had lost its identity. When that holder was the only one, the
release had tidied the directories away with the record, the ``touch`` found
no directory to write in, and nothing came back at all: the restore ran on
with no pin, and a prune on the target was free to delete what it was reading.

The beat now runs the script the acquire ran, which creates the directories
below the target and writes the complete record. The exclusive lock's beat is
unchanged: a holder whose lock was broken must not write itself into the next
holder's lock, and ``test_remote_locks`` holds that.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest

from btrfs_backup_ng.sshutil.lock import (
    LOCK_DIR_NAME,
    RemoteLockManager,
    read_persisted_locks,
)


def _manager(target: Path, **kw) -> RemoteLockManager:
    def run(script):
        proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    return RemoteLockManager(run, str(target), hostname="test", **kw)


def _location(tmp_path: Path) -> Path:
    location = tmp_path / "location"
    location.mkdir()
    (location / LOCK_DIR_NAME).mkdir()
    return location


def _holder_of(target: Path, name: str, lock_id: str):
    (holder,) = [h for h in _manager(target).live_locks()[name] if h.lock_id == lock_id]
    return holder


def _record_of(target: Path, name: str, lock_id: str) -> Path:
    holder = _holder_of(target, name, lock_id)
    return (
        target
        / LOCK_DIR_NAME
        / f"{holder.dir_name}.lock"
        / "holders"
        / holder.file_name
    )


def _wait_for(condition, seconds: float = 2.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


class TestARecordRemovedFromUnderALiveHolderComesBackWhole:
    def test_beside_another_holder_it_returns_with_its_identity(self, tmp_path):
        location = _location(tmp_path)
        live = _manager(location, heartbeat_interval=0.05)
        live.acquire_shared_persistent("snap-s1", "restore:live")
        _manager(location).acquire_shared("snap-s1", "restore:leftover")
        try:
            _manager(location).release_holder(
                _holder_of(location, "snap-s1", "restore:live")
            )
            assert _wait_for(
                lambda: (
                    "restore:live"
                    in read_persisted_locks(_manager(location))
                    .get("s1", {})
                    .get("locks", [])
                )
            ), read_persisted_locks(_manager(location))
            assert sorted(read_persisted_locks(_manager(location))["s1"]["locks"]) == [
                "restore:leftover",
                "restore:live",
            ]
        finally:
            live.release_shared("snap-s1", "restore:live")

    def test_alone_it_returns_with_its_directories(self, tmp_path):
        location = _location(tmp_path)
        live = _manager(location, heartbeat_interval=0.05)
        live.acquire_shared_persistent("snap-s2", "restore:alone")
        lock_dir = _record_of(location, "snap-s2", "restore:alone").parent.parent
        try:
            _manager(location).release_holder(
                _holder_of(location, "snap-s2", "restore:alone")
            )
            assert _wait_for(
                lambda: (
                    read_persisted_locks(_manager(location))
                    == {"s2": {"locks": ["restore:alone"]}}
                )
            ), read_persisted_locks(_manager(location))
            assert lock_dir.is_dir()
        finally:
            live.release_shared("snap-s2", "restore:alone")

    def test_the_record_that_returns_is_the_complete_one(self, tmp_path):
        location = _location(tmp_path)
        live = _manager(location, heartbeat_interval=0.05)
        live.acquire_shared_persistent("snap-s3", "restore:r", operation="restore")
        record = _record_of(location, "snap-s3", "restore:r")
        before = record.read_text()
        try:
            record.unlink()
            assert _wait_for(record.exists)
            assert record.read_text() == before
        finally:
            live.release_shared("snap-s3", "restore:r")


class TestAReleasedPinStaysReleased:
    def test_nothing_comes_back_after_release_shared(self, tmp_path):
        location = _location(tmp_path)
        live = _manager(location, heartbeat_interval=0.05)
        live.acquire_shared_persistent("snap-s4", "restore:done")
        live.release_shared("snap-s4", "restore:done")
        time.sleep(0.3)
        assert read_persisted_locks(_manager(location)) == {}
        assert not list((location / LOCK_DIR_NAME).iterdir())


def _elevating_manager(target: Path, unprivileged, elevated: list, **kw):
    """A manager whose unprivileged runner is ``unprivileged`` and whose
    elevated runner is the plain shell, recording every script it is given."""

    def run_elevated(script):
        elevated.append(script)
        proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    return RemoteLockManager(
        unprivileged, str(target), hostname="test", run_elevated=run_elevated, **kw
    )


def _unprivileged_over(target: Path, store: Path):
    """A runner that cannot write below ``store``, as an account that does not
    own a root-owned lock directory cannot: every directory under the store is
    read-only while its script runs, and ``touch`` is refused, since the owner
    of a file may always set its times and the account is not the owner."""
    shims = target.parent / "shims"
    shims.mkdir(exist_ok=True)
    (shims / "touch").write_text(
        "#!/bin/sh\necho 'touch: Permission denied' >&2\nexit 1\n"
    )
    (shims / "touch").chmod(0o755)

    def run(script):
        dirs = [store] + [d for d in store.rglob("*") if d.is_dir()]
        for d in dirs:
            d.chmod(0o555)
        try:
            proc = subprocess.run(
                ["sh", "-c", script],
                capture_output=True,
                text=True,
                env={**os.environ, "PATH": f"{shims}:{os.environ['PATH']}"},
            )
        finally:
            for d in dirs:
                d.chmod(0o755)
        return proc.returncode, proc.stdout, proc.stderr

    return run


class TestABeatElevatesAsTheAcquireDid:
    def test_a_shared_pin_on_a_root_owned_store_is_kept_alive(self, tmp_path):
        if os.geteuid() == 0:
            pytest.skip("root writes anywhere")
        location = _location(tmp_path)
        store = location / LOCK_DIR_NAME
        elevated: list = []
        live = _elevating_manager(
            location,
            _unprivileged_over(location, store),
            elevated,
            heartbeat_interval=0.05,
        )
        live.acquire_shared_persistent("snap-s5", "restore:r")
        assert len(elevated) == 1, "the acquire elevated once"
        record = _record_of(location, "snap-s5", "restore:r")
        try:
            record.unlink()
            assert _wait_for(record.exists)
            assert len(elevated) >= 2, "a beat that could not write ran elevated"
            assert elevated[-1] == elevated[0], "the same script, elevated"
        finally:
            live._stop_heartbeat("snap-s5\x00restore:r")

    def test_an_exclusive_lock_on_a_root_owned_store_is_kept_alive(self, tmp_path):
        if os.geteuid() == 0:
            pytest.skip("root writes anywhere")
        location = _location(tmp_path)
        store = location / LOCK_DIR_NAME
        elevated: list = []
        holder = _elevating_manager(
            location,
            _unprivileged_over(location, store),
            elevated,
            heartbeat_interval=0.05,
        )
        holder.acquire_persistent("target", "prune")
        (lock_dir,) = store.glob("target*.lock")
        heartbeat = lock_dir / "heartbeat"
        old = time.time() - 100
        os.utime(heartbeat, (old, old))
        try:
            assert _wait_for(lambda: heartbeat.stat().st_mtime > old + 50)
            assert any("touch -c" in script for script in elevated), (
                "the beat that could not touch ran elevated"
            )
        finally:
            holder._stop_heartbeat("target")

    def test_a_beat_that_can_write_does_not_elevate(self, tmp_path):
        location = _location(tmp_path)
        elevated: list = []
        live = _elevating_manager(
            location, _manager(location)._run, elevated, heartbeat_interval=0.05
        )
        live.acquire_shared_persistent("snap-s6", "restore:r")
        time.sleep(0.3)
        live.release_shared("snap-s6", "restore:r")
        assert elevated == []


class TestARemovalElevatesAsTheAcquireDid:
    """A release, a release by record and a sweep each run unprivileged
    first and again elevated when the verdict says the record could not be
    removed, as the acquire does; on a root-owned store every removal left
    its record behind, so pins and locks accumulated and the next prune
    waited out the stale window after every operation."""

    def _root_owned(self, tmp_path):
        if os.geteuid() == 0:
            pytest.skip("root writes anywhere")
        location = _location(tmp_path)
        elevated: list = []
        manager = _elevating_manager(
            location,
            _unprivileged_over(location, location / LOCK_DIR_NAME),
            elevated,
            heartbeat_interval=0.05,
        )
        return location, manager, elevated

    def test_a_shared_release_removes_its_record(self, tmp_path):
        location, manager, elevated = self._root_owned(tmp_path)
        manager.acquire_shared_persistent("snap-s7", "restore:r")
        taken = len(elevated)
        manager.release_shared("snap-s7", "restore:r")
        assert read_persisted_locks(_manager(location)) == {}
        assert not list((location / LOCK_DIR_NAME).iterdir())
        assert any("rm -f" in s for s in elevated[taken:]), "the removal ran elevated"

    def test_an_exclusive_release_removes_its_record(self, tmp_path, caplog):
        location, manager, elevated = self._root_owned(tmp_path)
        manager.acquire_persistent("target", "prune")
        taken = len(elevated)
        with caplog.at_level("WARNING"):
            manager.release("target")
        assert "Could not release" not in caplog.text
        assert not list((location / LOCK_DIR_NAME).iterdir())
        assert any("RELEASED" in s for s in elevated[taken:]), (
            "the removal ran elevated"
        )
        assert not manager.holds("target")

    def test_a_release_by_record_removes_it(self, tmp_path):
        location, manager, elevated = self._root_owned(tmp_path)
        manager.acquire_shared("snap-s8", "restore:leftover")
        taken = len(elevated)
        (holder,) = _manager(location).live_locks()["snap-s8"]
        manager.release_holder(holder)
        assert read_persisted_locks(_manager(location)) == {}
        assert any("rm -f" in s for s in elevated[taken:]), "the removal ran elevated"

    def test_a_sweep_removes_an_abandoned_record(self, tmp_path):
        location, manager, elevated = self._root_owned(tmp_path)
        _manager(location).acquire_shared("snap-s9", "restore:dead")
        record = _record_of(location, "snap-s9", "restore:dead")
        old = time.time() - 3600
        os.utime(record, (old, old))
        swept = _elevating_manager(
            location,
            _unprivileged_over(location, location / LOCK_DIR_NAME),
            elevated,
            stale_after=1,
        ).sweep_dead_holders()
        assert swept == 1
        assert not record.exists()
        assert any("rm -f" in s for s in elevated), "the sweep ran elevated"

    def test_a_removal_that_can_write_does_not_elevate(self, tmp_path):
        location = _location(tmp_path)
        elevated: list = []
        manager = _elevating_manager(location, _manager(location)._run, elevated)
        manager.acquire_shared("snap-s10", "restore:r")
        manager.release_shared("snap-s10", "restore:r")
        manager.acquire_persistent("target", "prune")
        manager.release("target")
        manager.sweep_dead_holders()
        assert elevated == []

    def test_a_broken_lock_is_still_left_alone(self, tmp_path, caplog):
        """NOTOWNER is not a verdict to elevate on: the record is gone, not
        unwritable, and elevating would not change that."""
        location, manager, elevated = self._root_owned(tmp_path)
        manager.acquire_persistent("target", "prune")
        taken = len(elevated)
        (lock_dir,) = (location / LOCK_DIR_NAME).glob("target*.lock")
        for f in lock_dir.iterdir():
            f.unlink()
        with caplog.at_level("WARNING"):
            manager.release("target")
        assert "no longer held by this process" in caplog.text
        assert elevated[taken:] == []
