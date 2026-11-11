"""Breaking a dead lock has one breaker (#158).

Twelve contenders at one dead lock could leave no winner. The traces showed
two contenders breaking the same dead instance: one removed its record, and
a second, listing before the first had removed the heartbeat, found a
directory holding only a heartbeat, aged it by the heartbeat's old mtime,
and removed that. Two breakers, two rmdirs; the second removed a claimant's
fresh, still-empty directory, a later claimant made the path again, the
first wrote its record into the second's directory, and the record count
decided: one winner and a NOLOCK error, or no winner at all.

A directory holding only a heartbeat is now aged by the directory, which the
record's unlink has just refreshed, so a lister in the breaker's gap says
BUSY and the dead instance has one breaker. The same shape left by a release
that died between its two removals has an old directory mtime and is still
broken. Shell protocol and Python port alike.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest

from btrfs_backup_ng.sshutil.lock import (
    RemoteLockBusy,
    RemoteLockManager,
    local_manager,
)

DEAD = time.time() - 10_000


def _shell_manager(sandbox: Path, **kw) -> RemoteLockManager:
    def run(script: str):
        proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    return RemoteLockManager(run, str(sandbox), hostname="testhost", **kw)


def _manager(sandbox: Path, transport: str) -> RemoteLockManager:
    """The shell protocol or the Python port, over the same store."""
    return _shell_manager(sandbox) if transport == "shell" else local_manager(sandbox)


def _heartbeat_only(lock_dir: Path, *, directory_age: float) -> None:
    """The shape a breaker or a release leaves between its two removals: a
    heartbeat whose own mtime is long past, in a directory whose mtime is
    whatever the last removal made it."""
    lock_dir.mkdir(parents=True)
    hb = lock_dir / "heartbeat"
    hb.touch()
    os.utime(hb, (DEAD, DEAD))
    when = time.time() - directory_age
    os.utime(lock_dir, (when, when))


@pytest.mark.parametrize("transport", ["shell", "python"])
class TestAHeartbeatOnlyDirectoryIsAgedByTheDirectory:
    def test_a_fresh_directory_is_busy_however_old_its_heartbeat(
        self, tmp_path, transport
    ):
        """The breaker's gap: the record is gone, the heartbeat is still there
        with the dead holder's old mtime, and the directory was touched by
        the unlink an instant ago. Aged by the heartbeat this was a second
        dead instance to break; it is a break in progress."""
        manager = _manager(tmp_path, transport)
        lock_dir = Path(manager._lock_dir("target"))
        _heartbeat_only(lock_dir, directory_age=0)
        with pytest.raises(RemoteLockBusy):
            manager.acquire_once("target", "lister")
        assert sorted(p.name for p in lock_dir.iterdir()) == ["heartbeat"], (
            "a lister in the gap must touch nothing"
        )

    def test_an_old_directory_is_broken_through_its_heartbeat(
        self, tmp_path, transport
    ):
        """A release that died between its two removals leaves the same
        shape, with a directory mtime as old as the death. That one is dead
        and is broken."""
        manager = _manager(tmp_path, transport)
        lock_dir = Path(manager._lock_dir("target"))
        _heartbeat_only(lock_dir, directory_age=10_000)
        assert manager.acquire_once("target", "breaker") == "stale-broken"
        owners = [p.name for p in lock_dir.glob("owner.*")]
        assert len(owners) == 1
        assert (lock_dir / "heartbeat").exists()
