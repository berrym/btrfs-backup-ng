"""A pin written to the lock file changes that pin and nothing else.

``run`` sends to its targets in parallel, and every target's thread lists the
source for itself (``sync_snapshots`` lists with ``flush_cache=True``). Each
thread therefore holds its own copy of every source snapshot, with the lock
sets the file had when that thread listed. ``set_lock`` took the guard and
read the file, but then wrote the calling thread's whole copy of the
snapshot's entry. So a pin another thread had released since came back, and
a pin another thread had taken since was erased: after an interrupt, three
targets each logged "Keeping ... locked" and the file kept one of the three
pins; with real threads through the real executor, 101 of 200 runs in which
every transfer succeeded left pins behind.

Now the file records the change -- one id added to or removed from one key
of one snapshot's entry, applied to the entry as the file holds it at that
moment. These tests drive the real ``LocalEndpoint`` against a real lock file;
only the byte mover and the post-receive check are stood in.
"""

from __future__ import annotations

import os
import random
import threading
import time
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict

S0, S1 = "home-20260101-000000", "home-20260102-000000"
LOCK_FILE = ".btrfs-backup-ng.locks"


@pytest.fixture(autouse=True)
def _no_btrfs_identity(monkeypatch):
    """The listing asks btrfs for each snapshot's uuid; these are plain
    directories, so it has none to give. Answered here without running sudo."""
    monkeypatch.setattr(LocalEndpoint, "subvolume_identity", lambda self, path: None)


def _location(path: Path, leftover: dict | None = None) -> LocalEndpoint:
    path.mkdir(parents=True, exist_ok=True)
    for name in (S0, S1):
        (path / name).mkdir(exist_ok=True)
    ep = LocalEndpoint(config={"path": path, "source": "/src", "snap_prefix": "home-"})
    if leftover is not None:
        ep._write_locks(leftover)
    return ep


def _listing(ep: LocalEndpoint) -> dict:
    """What one target's thread gets: its own objects, from its own listing."""
    return {s.get_name(): s for s in ep.list_snapshots(flush_cache=True)}


def _file(path: Path) -> dict:
    """The lock file, with each id list as a set (the file's order is not kept)."""
    raw = __util__.read_locks((path / LOCK_FILE).read_text())
    return {
        name: {key: set(ids) for key, ids in entry.items()}
        for name, entry in raw.items()
    }


class TestTwoObjectsOneSnapshot:
    """The measurement behind #122: two objects for one snapshot, as two
    parallel targets hold, each pinning through its own."""

    def test_both_pins_survive_and_a_release_drops_only_its_own(self, tmp_path):
        ep = _location(tmp_path, {})
        a = __util__.Snapshot(tmp_path, "home-", ep, name=S1)
        b = __util__.Snapshot(tmp_path, "home-", ep, name=S1)
        outcome: dict = {}

        def pin(snap, lock_id):
            try:
                ep.set_lock(snap, lock_id, True)
                outcome[lock_id] = "pinned"
            except Exception as e:  # noqa: BLE001 - reported by the assertion
                outcome[lock_id] = repr(e)

        threads = [
            threading.Thread(target=pin, args=(a, "target-1")),
            threading.Thread(target=pin, args=(b, "target-2")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert outcome == {"target-1": "pinned", "target-2": "pinned"}
        assert _file(tmp_path) == {S1: {"locks": {"target-1", "target-2"}}}
        ep.set_lock(b, "target-2", False)
        assert _file(tmp_path) == {S1: {"locks": {"target-1"}}}
        assert a.locks == {"target-1", "target-2"} or a.locks == {"target-1"}


class TestTwoThreadsOneSource:
    """Two listings of one source are what two parallel targets hold."""

    def test_a_released_pin_does_not_come_back(self, tmp_path):
        """An interrupted run left target ``ssh``'s pins. The ``local`` thread
        lists first, so its copy carries them; ``ssh`` then finds its snapshot
        delivered and releases them. ``local``'s own pin and release must not
        write ``ssh``'s back."""
        ep = _location(
            tmp_path, {S1: {"locks": ["ssh"]}, S0: {"parent_locks": ["ssh"]}}
        )
        local = _listing(ep)
        ssh = _listing(ep)
        ep.set_lock(ssh[S1], "ssh", False)
        ep.set_lock(ssh[S0], "ssh", False, parent=True)
        ep.set_lock(local[S1], "local", True)
        ep.set_lock(local[S0], "local", True, parent=True)
        assert _file(tmp_path) == {
            S1: {"locks": {"local"}},
            S0: {"parent_locks": {"local"}},
        }
        ep.set_lock(local[S1], "local", False)
        ep.set_lock(local[S0], "local", False, parent=True)
        assert _file(tmp_path) == {}

    def test_a_pin_taken_by_another_thread_is_not_erased(self, tmp_path):
        """Clean start; both threads list before either pins. An interrupt now
        keeps both pins (a backup keeps its pin when its transfer fails), so
        both must be in the file."""
        ep = _location(tmp_path, {})
        ssh = _listing(ep)
        local = _listing(ep)
        ep.set_lock(ssh[S1], "ssh", True)
        ep.set_lock(ssh[S0], "ssh", True, parent=True)
        ep.set_lock(local[S1], "local", True)
        ep.set_lock(local[S0], "local", True, parent=True)
        assert _file(tmp_path) == {
            S1: {"locks": {"ssh", "local"}},
            S0: {"parent_locks": {"ssh", "local"}},
        }

    def test_staggered_listings_end_with_nothing_held(self, tmp_path):
        """The second thread lists after the first pinned, so its copy holds
        the first thread's pin; releasing its own must not keep that one."""
        ep = _location(tmp_path, {})
        ssh = _listing(ep)
        ep.set_lock(ssh[S1], "ssh", True)
        local = _listing(ep)
        ep.set_lock(local[S1], "local", True)
        assert _file(tmp_path) == {S1: {"locks": {"ssh", "local"}}}
        ep.set_lock(ssh[S1], "ssh", False)
        assert _file(tmp_path) == {S1: {"locks": {"local"}}}
        ep.set_lock(local[S1], "local", False)
        assert _file(tmp_path) == {}

    def test_the_callers_copy_still_records_its_own_change(self, tmp_path):
        """The in-memory sets of the object passed in keep the change, as the
        transfer and the prune of this run read them."""
        ep = _location(tmp_path, {})
        snap = _listing(ep)[S1]
        ep.set_lock(snap, "t1", True)
        ep.set_lock(snap, "t2", True, parent=True)
        assert snap.locks == {"t1"} and snap.parent_locks == {"t2"}
        ep.set_lock(snap, "t1", False)
        assert snap.locks == set() and snap.parent_locks == {"t2"}


class TestTheChangeAndNothingElse:
    def test_the_other_key_other_holders_and_other_snapshots_pass_through(
        self, tmp_path
    ):
        leftover = {
            S1: {"locks": ["a", "b"], "parent_locks": ["p"]},
            S0: {"locks": ["z"]},
        }
        ep = _location(tmp_path, leftover)
        # A copy that knows nothing of what the file holds.
        snap = __util__.Snapshot(tmp_path, "home-", ep, name=S1)
        ep.set_lock(snap, "c", True, parent=True)
        assert _file(tmp_path) == {
            S1: {"locks": {"a", "b"}, "parent_locks": {"p", "c"}},
            S0: {"locks": {"z"}},
        }
        ep.set_lock(snap, "c", False, parent=True)
        ep.set_lock(snap, "a", False)
        assert _file(tmp_path) == {
            S1: {"locks": {"b"}, "parent_locks": {"p"}},
            S0: {"locks": {"z"}},
        }

    @pytest.mark.parametrize(
        "lock_id, state",
        [("never-held", False), ("held", True)],
        ids=["release-of-an-id-not-held", "pin-of-an-id-already-held"],
    )
    def test_nothing_is_written_when_nothing_changes(self, tmp_path, lock_id, state):
        ep = _location(tmp_path, {S1: {"locks": ["held"]}})
        before = os.stat(tmp_path / LOCK_FILE)
        ep.set_lock(__util__.Snapshot(tmp_path, "home-", ep, name=S1), lock_id, state)
        after = os.stat(tmp_path / LOCK_FILE)
        # The writer replaces the file, so any write gives it a new inode.
        assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)


# --------------------------------------------------------------------------- #
# Real threads through the real executor
# --------------------------------------------------------------------------- #
class _Destination:
    """Answers presence by name; records what it receives."""

    _is_remote = False

    def __init__(self, ident: str, present: set, path: Path) -> None:
        self.ident = ident
        self.present = set(present)
        self.config = {"path": str(path)}

    def get_id(self) -> str:
        return self.ident

    def correspondents_of(self, snaps):
        return {s.get_name(): s for s in snaps if s.get_name() in self.present}

    def correspondent_of(self, snap):
        return snap if snap.get_name() in self.present else None

    def list_snapshots(self, flush_cache: bool = False) -> list:
        return []

    def add_snapshot(self, snap, rewrite: bool = True) -> None:
        self.present.add(snap.get_name())

    def __str__(self) -> str:
        return self.ident


def _run_two_targets(tmp_path, monkeypatch, iterations, leftover, fail):
    """``iterations`` runs of two targets in parallel threads over ONE shared
    source endpoint, as ``run`` shares it. Returns the lock file after each
    run and every send during which the sender's own pin was not in the file."""
    missing_own_pin: list = []

    def send(snapshot, destination, parent=None, options=None, **kwargs):
        time.sleep(random.uniform(0, 0.004))
        held = __util__.read_locks(
            (Path(snapshot.endpoint.config["path"]) / LOCK_FILE).read_text()
        )
        if destination.get_id() not in held.get(snapshot.get_name(), {}).get(
            "locks", []
        ):
            missing_own_pin.append((destination.get_id(), snapshot.get_name()))
        time.sleep(random.uniform(0, 0.004))
        if fail:
            raise __util__.SnapshotTransferError("stand-in: the transfer died")

    monkeypatch.setattr(ops, "send_snapshot", send)
    monkeypatch.setattr(
        ops, "artifact_verdict", lambda d, s: StructureVerdict("ok", "stand-in")
    )
    finals = []
    for i in range(iterations):
        root = tmp_path / f"run{i}"
        source = _location(root / "snaps", leftover)
        source.list_snapshots(flush_cache=True)
        targets = [_Destination(ident, {S0}, root / ident) for ident in ("d1", "d2")]
        errors: list = []

        def work(target, source=source, errors=errors):
            try:
                ops.sync_snapshots(source, target)
            except __util__.SnapshotTransferError as e:
                if not fail:
                    errors.append(repr(e))
            except BaseException as e:  # noqa: BLE001 - reported below
                errors.append(repr(e))

        threads = [threading.Thread(target=work, args=(t,)) for t in targets]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        finals.append(_file(root / "snaps"))
    return finals, missing_own_pin


class TestRealThreadsThroughTheRealExecutor:
    """50 runs each. Before the fix, 101 of 200 runs from an interrupted run's
    leftovers ended with pins held although every transfer succeeded."""

    RUNS = 50

    @pytest.mark.parametrize(
        "leftover",
        [
            {S1: {"locks": ["d1"]}, S0: {"parent_locks": ["d1"]}},
            {},
        ],
        ids=["an-interrupted-runs-leftovers", "a-clean-start"],
    )
    def test_every_transfer_succeeds_and_nothing_is_left_held(
        self, tmp_path, monkeypatch, leftover
    ):
        finals, missing_own_pin = _run_two_targets(
            tmp_path, monkeypatch, self.RUNS, leftover, fail=False
        )
        leaks = [f for f in finals if f]
        assert leaks == [], f"{len(leaks)}/{self.RUNS} runs left pins: {leaks[:3]}"
        assert missing_own_pin == []

    def test_every_target_fails_and_every_targets_pin_is_kept(
        self, tmp_path, monkeypatch
    ):
        """A failed backup transfer keeps its pin, so the next run can still
        send the snapshot; with two targets failing, both pins."""
        finals, missing_own_pin = _run_two_targets(
            tmp_path, monkeypatch, self.RUNS, {}, fail=True
        )
        wrong = [
            f
            for f in finals
            if f != {S1: {"locks": {"d1", "d2"}}, S0: {"parent_locks": {"d1", "d2"}}}
        ]
        assert wrong == [], f"{len(wrong)}/{self.RUNS} runs lost a pin: {wrong[:3]}"
        assert missing_own_pin == []
