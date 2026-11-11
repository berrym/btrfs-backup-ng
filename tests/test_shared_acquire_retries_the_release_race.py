"""A shared acquire that loses the release-vs-acquire race retries instead of
failing the operation, for as long as its time budget allows.

`release_shared` tidies the empty lock/holders directories with `rmdir`; a
concurrent `acquire_shared` whose record script already created those
directories can find them gone when it writes, yielding FAILED. That is a
transient lost race, not an unwritable store (NOLOCKDIR, which a release never
causes and which is reported after two rounds in a row) -- the exclusive
acquire recovers from the same race by re-mkdir'ing within its own script. The
shared acquire re-runs its record script until it wins or its budget is spent.

The budget is time, not a count. A count of 3 failed on the ppc64le Koji
builders (0.9.19), and the count of 20 that replaced it was reached on a
two-CPU machine under CPU starvation (#160): a contender descheduled for tens
of milliseconds loses the race as many times in a row as the machine is slow,
and an acquire that reaches a count refuses a restore or a transfer on a
perfectly writable store.
"""

import os
import subprocess
import threading
import time
from pathlib import Path

import pytest

from btrfs_backup_ng.sshutil import lock as lock_module
from btrfs_backup_ng.sshutil.lock import (
    SHARED_ACQUIRE_BUDGET,
    RemoteLockManager,
    RemoteLockUnavailable,
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


def test_an_unwritable_store_is_reported_after_two_answers_not_retried():
    """NOLOCKDIR twice in a row is a store this account cannot write. A
    release never removes the store, so it is not the race: no budget is
    spent on it, and the message says what is wrong with the path."""
    calls = {"n": 0}

    def run(script):
        calls["n"] += 1
        return 0, "NOLOCKDIR\n", ""

    m = RemoteLockManager(run, "/tmp/x", hostname="h")  # no run_elevated
    started = time.monotonic()
    with pytest.raises(RemoteLockUnavailable) as info:
        m.acquire_shared("snap-s", "restore:1", "restore")
    assert calls["n"] == 2
    assert time.monotonic() - started < 1.0
    assert "could not be created or written to" in str(info.value)
    assert "attempts" not in str(info.value)


def test_a_single_nolockdir_is_given_one_more_attempt():
    """The script's two tests of one directory are not atomic, and a release
    can remove it between them, so one NOLOCKDIR can be the race in disguise:
    it is tried once more, and the next ACQUIRED stands."""
    calls = {"n": 0}

    def run(script):
        calls["n"] += 1
        if calls["n"] == 1:
            return 0, "NOLOCKDIR\n", ""
        return 0, "ACQUIRED\n", ""

    m = RemoteLockManager(run, "/tmp/x", hostname="h")
    assert m.acquire_shared("snap-s", "restore:1", "restore") == "acquired"
    assert calls["n"] == 2


def test_an_unwritable_store_is_reported_after_two_elevated_answers():
    """Where elevation is available each round ends with an elevated attempt,
    and two rounds answering NOLOCKDIR in a row are the store, not a race to
    retry for the budget."""
    calls = {"plain": 0, "elevated": 0}

    def run(script):
        calls["plain"] += 1
        return 0, "NOLOCKDIR\n", ""

    def run_elevated(script):
        calls["elevated"] += 1
        return 0, "NOLOCKDIR\n", ""

    m = RemoteLockManager(run, "/tmp/x", hostname="h", run_elevated=run_elevated)
    with pytest.raises(RemoteLockUnavailable):
        m.acquire_shared("snap-s", "restore:1", "restore")
    assert calls == {"plain": 2, "elevated": 2}


def test_two_nolockdirs_apart_are_not_an_unwritable_store():
    """Twice in a row, not twice ever: a false NOLOCKDIR (the race caught
    between the script's tests), dozens of lost races, and another false one
    is still contention, and the acquire that then wins stands."""
    answers = iter(["NOLOCKDIR", "FAILED", "FAILED", "NOLOCKDIR", "ACQUIRED"])
    calls = {"n": 0}

    def run(script):
        calls["n"] += 1
        return 0, next(answers) + "\n", ""

    m = RemoteLockManager(run, "/tmp/x", hostname="h")
    assert m.acquire_shared("snap-s", "restore:1", "restore") == "acquired"
    assert calls["n"] == 5


def test_a_root_owned_lock_directory_is_judged_by_the_elevated_answer():
    """The ssh-sudo shape: the plain attempt says NOLOCKDIR legitimately every
    round and the elevated one does the work. The plain answers do not count
    towards an unwritable store; the elevated attempt losing the race once is
    retried, and its win stands."""
    elevated = iter(["FAILED", "ACQUIRED"])
    calls = {"plain": 0, "elevated": 0}

    def run(script):
        calls["plain"] += 1
        return 0, "NOLOCKDIR\n", ""

    def run_elevated(script):
        calls["elevated"] += 1
        return 0, next(elevated) + "\n", ""

    m = RemoteLockManager(run, "/tmp/x", hostname="h", run_elevated=run_elevated)
    assert m.acquire_shared("snap-s", "restore:1", "restore") == "acquired"
    assert calls == {"plain": 2, "elevated": 2}


def _lock_dirs(tmp_path):
    m = RemoteLockManager(_sh_run, str(tmp_path), hostname="h")
    return m, Path(m._lock_dir("snap-s")), Path(m._holders_dir("snap-s"))


def test_shell_lock_directories_removed_under_the_record_script_are_the_race(
    tmp_path, monkeypatch
):
    """The record script's walk makes the lock and holders directories; a
    concurrent release removes them before the record is written. The
    verdict is FAILED, the race, not NOLOCKDIR: the store is there and
    writable. Forced by a mkdir shim that removes them as soon as the walk
    has made the holders directory."""
    m, lock_dir, holders = _lock_dirs(tmp_path)
    real = subprocess.run(
        ["sh", "-c", "command -v mkdir"], capture_output=True, text=True
    ).stdout.strip()
    shims = tmp_path / "shims"
    shims.mkdir()
    (shims / "mkdir").write_text(
        "#!/bin/sh\n"
        f'{real} "$@" || exit $?\n'
        f'case "$*" in *holders) rmdir "{holders}" "{lock_dir}";; esac\n'
    )
    (shims / "mkdir").chmod(0o755)
    monkeypatch.setenv("PATH", f"{shims}:{os.environ['PATH']}")

    _rc, out, _err = _sh_run(m._shared_record_script("snap-s", "restore:1", "restore"))

    assert out.split()[0] == "FAILED", out


@pytest.mark.skipif(os.geteuid() == 0, reason="root writes anywhere")
def test_shell_an_unwritable_lock_directory_is_not_the_race(tmp_path):
    """A lock directory this account cannot write under a writable store (one
    a root-owned prune made) is NOLOCKDIR, which the acquire raises at once
    or elevates for; it is not retried for the budget."""
    m, lock_dir, holders = _lock_dirs(tmp_path)
    lock_dir.mkdir(parents=True)
    lock_dir.chmod(0o555)
    try:
        _rc, out, _err = _sh_run(
            m._shared_record_script("snap-s", "restore:1", "restore")
        )
    finally:
        lock_dir.chmod(0o755)
    assert out.split()[0] == "NOLOCKDIR", out


def test_python_a_directory_gone_from_under_the_walk_is_the_race(tmp_path, monkeypatch):
    """The descriptor port: the lock directory the walk opened is removed
    before the holders directory can be made beneath it (ENOENT from the
    kernel) -- FAILED, the race; anything else wrong with the store stays
    NOLOCKDIR."""
    import btrfs_backup_ng.__util__ as util
    from btrfs_backup_ng.sshutil.lock import HOLDERS_DIR_NAME, local_manager

    m = local_manager(str(tmp_path))
    real_open = util.open_directory_nofollow

    def holders_parent_gone(name, **kw):
        if name == HOLDERS_DIR_NAME:
            raise FileNotFoundError(2, "No such file or directory", name)
        return real_open(name, **kw)

    monkeypatch.setattr(util, "open_directory_nofollow", holders_parent_gone)
    assert m._record_shared_here("snap-s", "restore:1", "restore") == "FAILED"

    def holders_refused(name, **kw):
        if name == HOLDERS_DIR_NAME:
            raise PermissionError(13, "Permission denied", name)
        return real_open(name, **kw)

    monkeypatch.setattr(util, "open_directory_nofollow", holders_refused)
    from btrfs_backup_ng.sshutil.lock import _StoreVerdict

    with pytest.raises(_StoreVerdict) as info:
        m._record_shared_here("snap-s", "restore:1", "restore")
    assert info.value.line == "NOLOCKDIR"


def test_a_persistent_failure_raises_once_the_budget_is_spent(monkeypatch):
    """A race lost on every attempt (FAILED throughout) fails after the
    budget, not a count, and the message says how long it tried."""
    monkeypatch.setattr(lock_module, "SHARED_ACQUIRE_BUDGET", 0.3)
    calls = {"n": 0}

    def run(script):
        calls["n"] += 1
        return 0, "FAILED\n", ""

    m = RemoteLockManager(run, "/tmp/x", hostname="h")
    started = time.monotonic()
    with pytest.raises(RemoteLockUnavailable) as info:
        m.acquire_shared("snap-s", "restore:1", "restore")
    elapsed = time.monotonic() - started
    assert 0.3 <= elapsed < 3.0, elapsed  # the budget, one back-off, and a slow CI host
    assert calls["n"] >= 2, calls
    assert f"after {calls['n']} attempts over 0.3 seconds" in str(info.value)
    assert "extreme contention" in str(info.value)
    assert "not writable" not in str(info.value)


def test_a_persistent_failure_does_not_wait_past_the_budget(monkeypatch):
    """The last back-off is cut at the deadline: a race lost throughout is
    reported when the budget ends, not one back-off later."""
    monkeypatch.setattr(lock_module, "SHARED_ACQUIRE_BUDGET", 0.05)
    sleeps = []
    monkeypatch.setattr(lock_module.time, "sleep", lambda s: sleeps.append(s))

    def run(script):
        return 0, "FAILED\n", ""

    m = RemoteLockManager(run, "/tmp/x", hostname="h")
    with pytest.raises(RemoteLockUnavailable):
        m.acquire_shared("snap-s", "restore:1", "restore")
    # With sleep patched out the loop spins until the clock passes the
    # deadline; no single back-off asked for may reach past it, so none is
    # longer than the whole budget, and the growing back-off is cut short.
    assert sleeps, "no back-off between attempts"
    assert max(sleeps) <= 0.05 + 1e-6, max(sleeps)


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


def test_real_churn_stays_well_under_the_budget(tmp_path):
    """Three holders churning acquire/release of one snapshot on a real store.

    Two assertions. The first -- no acquire failed -- is the behaviour, but it
    goes flaky on a slow or loaded machine: a budget near real contention can
    still pass it on a fast idle host and only fail under load. The second does
    not depend on the host's speed in the same way: it times each acquire and
    requires the budget to sit a wide margin above the slowest one. It is a
    floor, not a measurement of the budget's margin on a loaded host: idle,
    the slowest acquire of 900 takes about a quarter of a second, so only a
    budget within a few times that fails it here; on a fast machine pinned to
    two CPUs with four busy loops beside it, the slowest took between 1.3 and
    3.2 seconds over three measurements (#160), which the constant covers.
    """
    failures = []
    worst = {"attempts": 0, "seconds": 0.0}
    worst_lock = threading.Lock()

    def churn(lock_id):
        m = RemoteLockManager(_sh_run, str(tmp_path), hostname="h")
        attempts = {"n": 0}
        inner = m._record_shared

        def counting(*a, **k):
            attempts["n"] += 1
            return inner(*a, **k)

        m._record_shared = counting  # count attempts per acquire
        for _ in range(300):
            attempts["n"] = 0
            started = time.monotonic()
            try:
                m.acquire_shared("snap-s", lock_id, "restore")
            except RemoteLockUnavailable as e:
                failures.append((lock_id, str(e)))
                continue
            finally:
                with worst_lock:
                    worst["attempts"] = max(worst["attempts"], attempts["n"])
                    worst["seconds"] = max(worst["seconds"], time.monotonic() - started)
            m.release_shared("snap-s", lock_id)

    ts = [threading.Thread(target=churn, args=(f"restore:{k}",)) for k in range(3)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()

    assert failures == [], f"spurious shared-acquire failures: {failures[:3]}"
    # Host-independent guard: the slowest acquire must stay a wide margin below
    # the budget.
    assert worst["seconds"] <= SHARED_ACQUIRE_BUDGET / 2, (
        f"slowest acquire took {worst['seconds']:.2f}s ({worst['attempts']} "
        f"attempts) against a budget of {SHARED_ACQUIRE_BUDGET:g}s; the budget "
        f"is too close to real contention"
    )
