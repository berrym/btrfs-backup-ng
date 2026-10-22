"""A shared acquire that loses the release-vs-acquire race retries instead of
failing the operation.

`release_shared` tidies the empty lock/holders directories with `rmdir`; a
concurrent `acquire_shared` whose record script already created those
directories can find them gone when it writes, yielding NOLOCKDIR/FAILED. That
is a transient lost race, not an unwritable store -- the exclusive acquire
recovers from the same race by re-mkdir'ing within its own script. The shared
acquire now re-runs its record script a bounded number of times.
"""

import threading

import pytest

from btrfs_backup_ng.sshutil.lock import (
    RemoteLockManager,
    RemoteLockUnavailable,
    SHARED_ACQUIRE_ATTEMPTS,
)


def _sh_run(script):
    import subprocess

    p = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


def test_a_transient_failed_verdict_is_retried_and_succeeds():
    """First attempt loses the race (FAILED), the next wins -> 'acquired'."""
    calls = {"n": 0}

    def run(script):
        calls["n"] += 1
        if calls["n"] == 1:
            return 0, "FAILED\n", ""  # directories vanished mid-write
        return 0, "ACQUIRED\n", ""

    m = RemoteLockManager(run, "/tmp/x", hostname="h")
    assert m.acquire_shared("snap-s", "restore:1", "restore") == "acquired"
    assert calls["n"] == 2  # retried exactly once, then won


def test_a_transient_nolockdir_is_retried_and_succeeds():
    """A NOLOCKDIR from a vanished holders dir is also a retryable race."""
    calls = {"n": 0}

    def run(script):
        calls["n"] += 1
        if calls["n"] == 1:
            return 0, "NOLOCKDIR\n", ""
        return 0, "ACQUIRED\n", ""

    m = RemoteLockManager(run, "/tmp/x", hostname="h")  # no run_elevated
    assert m.acquire_shared("snap-s", "restore:1", "restore") == "acquired"
    assert calls["n"] == 2


def test_a_persistent_failure_still_raises_after_the_bounded_retries():
    """A genuinely unwritable store fails -- after exactly N cheap attempts."""
    calls = {"n": 0}

    def run(script):
        calls["n"] += 1
        return 0, "FAILED\n", ""

    m = RemoteLockManager(run, "/tmp/x", hostname="h")
    with pytest.raises(RemoteLockUnavailable):
        m.acquire_shared("snap-s", "restore:1", "restore")
    assert calls["n"] == SHARED_ACQUIRE_ATTEMPTS


def test_an_unsafe_verdict_is_raised_at_once_never_retried():
    """UNSAFE is a safety refusal, not a race: no retry."""
    calls = {"n": 0}

    def run(script):
        calls["n"] += 1
        return 0, "UNSAFE SYMLINK /tmp/x/.btrfs-backup-ng.locks\n", ""

    m = RemoteLockManager(run, "/tmp/x", hostname="h")
    with pytest.raises(RemoteLockUnavailable):
        m.acquire_shared("snap-s", "restore:1", "restore")
    assert calls["n"] == 1


def test_real_churn_has_no_spurious_failures(tmp_path):
    """Three holders churning acquire/release of one snapshot on a real store:
    with the retry, the release-vs-acquire race no longer aborts an acquire."""
    failures = []

    def churn(lock_id):
        m = RemoteLockManager(_sh_run, str(tmp_path), hostname="h")
        for _ in range(300):
            try:
                m.acquire_shared("snap-s", lock_id, "restore")
            except RemoteLockUnavailable as e:
                failures.append((lock_id, str(e)))
                continue
            m.release_shared("snap-s", lock_id)

    ts = [threading.Thread(target=churn, args=(f"restore:{k}",)) for k in range(3)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()

    assert failures == [], f"spurious shared-acquire failures: {failures[:3]}"
