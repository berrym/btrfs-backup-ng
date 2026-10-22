"""Two check-then-act races in the shared lock store corrupt a live holder's
state. Both are closed here.

Finding 3: record_pin does holds_shared() then acquire_shared_persistent() ->
_start_heartbeat(), and _held/_threads were touched without _state_lock, so two
threads pinning the same key both started a heartbeat and the second orphaned
the first -- which kept rewriting the record after release (a resurrected pin).

Finding 5: sweep_dead_holders listed dead records then removed them by path with
no mtime re-check, so a holder whose heartbeat refreshed its record between the
listing and the removal had its FRESH record swept.
"""

import subprocess
import threading
import time

from btrfs_backup_ng.sshutil.lock import (
    DEAD_HOLDER_MULTIPLE,
    RemoteLockManager,
    read_persisted_locks,
    record_pin,
)


def _run(script):
    p = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


class _Snap:
    def __init__(self, name="s"):
        self._name = name

    def get_name(self):
        return self._name


# ---------------------------------------------------------------- Finding 3


def test_two_pins_of_one_key_start_one_heartbeat_and_do_not_resurrect(tmp_path):
    m = RemoteLockManager(_run, str(tmp_path), hostname="h", heartbeat_interval=0.2)
    snap = _Snap()
    barrier = threading.Barrier(2)

    def pin():
        barrier.wait()
        record_pin(m, snap, "dest", True)

    ts = [threading.Thread(target=pin) for _ in range(2)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()

    hb = [t for t in threading.enumerate() if t.name == "lock-hb"]
    assert len(hb) == 1, f"expected one heartbeat thread, found {len(hb)}"

    record_pin(m, snap, "dest", False)  # release
    time.sleep(0.6)  # several heartbeat intervals

    # The released pin must stay gone -- no orphaned beat rewriting it.
    records = read_persisted_locks(RemoteLockManager(_run, str(tmp_path)))
    assert records == {}, f"pin resurrected after release: {records}"
    assert [t for t in threading.enumerate() if t.name == "lock-hb"] == []


def test_a_duplicate_start_heartbeat_is_dropped_not_orphaning(tmp_path):
    """Starting a heartbeat for a key already held is a no-op, not an overwrite."""
    m = RemoteLockManager(_run, str(tmp_path), hostname="h", heartbeat_interval=5)
    m.acquire_shared_persistent("snap-s", "id", "op")
    first = dict(m._threads)
    m._start_heartbeat(m._held_key("snap-s", "id"), shared=("snap-s", "id", "op"))
    assert m._threads == first, "a second start replaced the live heartbeat thread"
    m.release_shared("snap-s", "id")


# ---------------------------------------------------------------- Finding 5


def test_sweep_keeps_a_record_refreshed_since_the_listing(tmp_path):
    holder = RemoteLockManager(_run, str(tmp_path), hostname="h")
    holder.acquire_shared("snap-s", "restore:1", "restore")
    rec = holder._holder_file("snap-s", "restore:1")
    # Age it past the dead threshold so the listing judges it dead.
    subprocess.run(
        ["touch", "-d", f"-{holder._stale_after * DEAD_HOLDER_MULTIPLE + 20} sec", rec],
        check=True,
    )

    class Sweeper(RemoteLockManager):
        def _scan_holders(self):
            live, dead = super()._scan_holders()
            # Between the listing and the removal, the stalled holder's beat lands.
            _run(holder._shared_record_script("snap-s", "restore:1", "restore"))
            return live, dead

    sw = Sweeper(_run, str(tmp_path), hostname="h")
    swept = sw.sweep_dead_holders()

    assert swept == 0, "swept a record that was refreshed since the listing"
    live = list(RemoteLockManager(_run, str(tmp_path)).live_locks())
    assert "snap-s" in live, f"a live holder's fresh record was removed: live={live}"
    holder.release_shared("snap-s", "restore:1")


def test_sweep_still_removes_a_genuinely_abandoned_record(tmp_path):
    """The sweep must still clear a record that stays old (really abandoned)."""
    holder = RemoteLockManager(_run, str(tmp_path), hostname="h")
    holder.acquire_shared("snap-s", "restore:1", "restore")
    rec = holder._holder_file("snap-s", "restore:1")
    subprocess.run(
        ["touch", "-d", f"-{holder._stale_after * DEAD_HOLDER_MULTIPLE + 20} sec", rec],
        check=True,
    )

    swept = RemoteLockManager(_run, str(tmp_path), hostname="h").sweep_dead_holders()

    assert swept == 1
    assert list(RemoteLockManager(_run, str(tmp_path)).live_locks()) == []
