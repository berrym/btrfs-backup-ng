"""A holder whose slot lease was broken leaves the slot to the run that holds it.

The snapper receive into ``.snapshots/<n>`` runs under the slot's receiving
lock, a lease: the holder's ``owner.<token>`` record, refreshed by a beat, and
broken by the next run once it is older than the stale threshold. Every slot
action after the acquire -- removing a leftover as the slot opens, publishing
the slot, abandoning it -- acted on the slot by path and existence, never by
ownership. A holder suspended past the threshold (laptop sleep, a swap storm,
a long ``sudo -n``) was stale-broken by the next run, which opened the same
slot, removed the first run's in-flight receive as a crashed leftover, and
received into it; the first run, resuming, removed the second run's receive in
turn, or published it as its own copy: a read-write subvolume with no
received_uuid, invisible to listing and re-sent whole (#143).

The lease is now asked of the store, immediately before each such action. A
holder whose record is gone or superseded owns nothing in the slot: it
publishes nothing, removes nothing, and fails its own transfer with the reason.
A store that cannot be asked is a lease that cannot be confirmed, refused the
same way. And a slot found occupied as it opens is judged by when it was last
written, against the store's own clock and the same threshold a lease is: a
recent write may be a receive still arriving and is left alone.

No real suspension is needed: the store is driven directly, as the next run
would drive it -- the holder's record aged past the threshold, the lock taken
through a manager of its own.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.core.layout import SnapperLayout
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
from btrfs_backup_ng.sshutil.lock import (
    OWNER_PREFIX,
    RECEIVING_DIR_NAME,
    RECEIVING_LOCK_PREFIX,
    LocalLockManager,
    RemoteLockManager,
    local_manager,
)


def _real_shell(ep, script):
    r = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return r.returncode, r.stdout


@pytest.fixture
def real_shell(monkeypatch):
    monkeypatch.setattr(ops, "_snapper_run_shell", _real_shell)


def _endpoint(base: Path) -> LocalEndpoint:
    base.mkdir(parents=True, exist_ok=True)
    return LocalEndpoint(
        config={"path": str(base), "snap_prefix": "", "fs_checks": "skip"}
    )


def _slot_lock_name(layout: SnapperLayout, number: int) -> str:
    return f"{RECEIVING_LOCK_PREFIX}{layout.slot_dir(number)}"


def _age(path: Path, seconds: int) -> None:
    """Set ``path``'s last write ``seconds`` ago, by the clock the store's
    probe reads: the filesystem's own."""
    then = time.time() - seconds
    os.utime(path, (then, then), follow_symlinks=False)


def _stale_break(base: Path, holder, name: str):
    """What the next run does to a holder whose beat lapsed: finds the record
    older than the threshold, removes it and takes the lock -- through a
    manager of its own, as another process would. Returns that manager."""
    lock_dir = Path(holder._lock_dir(name))
    for entry in lock_dir.iterdir():
        _age(entry, 2 * holder.stale_after + 60)
    _age(lock_dir, 2 * holder.stale_after + 60)
    successor = local_manager(str(base), dir_name=RECEIVING_DIR_NAME)
    assert successor.acquire_once(name, "receive") == "stale-broken"
    return successor


def _received(layout: SnapperLayout, number: int, marker: str) -> Path:
    """What a ``btrfs receive`` into the open slot leaves: ``<n>.incoming/snapshot``."""
    landing = Path(layout.incoming_dir(number)) / "snapshot"
    landing.mkdir(exist_ok=True)
    (landing / "marker").write_text(marker)
    return landing


class _Taken:
    """Slot 7 opened by this run (A), then stale-broken and received into by
    the next run (B), which now holds the lease and has its partial in the
    slot. A is about to act."""

    def __init__(self, tmp_path) -> None:
        self.base = tmp_path / "target"
        self.endpoint = _endpoint(self.base)
        self.layout = SnapperLayout(self.endpoint)
        self.layout.open_slot(7)
        self.name = _slot_lock_name(self.layout, 7)
        self.holder = self.endpoint._receive_lock_manager()
        assert self.holder.is_owner(self.name) is True
        self.successor = _stale_break(self.base, self.holder, self.name)
        self.partial = _received(self.layout, 7, "B's receive, still arriving")

    def slot_as_b_left_it(self) -> bool:
        return (
            self.partial.exists()
            and (self.partial / "marker").read_text() == "B's receive, still arriving"
            and not Path(self.layout.slot_dir(7)).exists()
        )

    def close(self) -> None:
        self.successor.release(self.name)


class TestTheLeaseIsAskedOfTheStore:
    def test_a_holder_owns_its_lease_while_its_record_stands(self, tmp_path):
        store = local_manager(str(tmp_path), dir_name=RECEIVING_DIR_NAME)
        name = f"{RECEIVING_LOCK_PREFIX}{tmp_path}/.snapshots/7"
        assert store.is_owner(name) is False, "a lease never taken is not owned"
        store.acquire_once(name, "receive")
        try:
            assert store.is_owner(name) is True
        finally:
            store.release(name)
        assert store.is_owner(name) is False

    def test_a_record_removed_or_superseded_is_a_lease_lost(self, tmp_path):
        store = local_manager(str(tmp_path), dir_name=RECEIVING_DIR_NAME)
        name = f"{RECEIVING_LOCK_PREFIX}{tmp_path}/.snapshots/7"
        store.acquire_once(name, "receive")
        lock_dir = Path(store._lock_dir(name))
        (own,) = [p for p in lock_dir.iterdir() if p.name.startswith(OWNER_PREFIX)]
        try:
            # Another record beside this holder's: the lock was broken and
            # taken while this record was being judged.
            (lock_dir / f"{OWNER_PREFIX}another").write_text("{}")
            assert store.is_owner(name) is False
            (lock_dir / f"{OWNER_PREFIX}another").unlink()
            assert store.is_owner(name) is True
            own.unlink()
            assert store.is_owner(name) is False
        finally:
            store.release(name)

    def test_a_store_that_cannot_be_asked_confirms_nothing(self, tmp_path):
        store = local_manager(str(tmp_path), dir_name=RECEIVING_DIR_NAME)
        name = f"{RECEIVING_LOCK_PREFIX}{tmp_path}/.snapshots/7"
        store.acquire_once(name, "receive")
        lock_dir = Path(store._lock_dir(name))
        lock_dir.chmod(0)
        try:
            if os.access(lock_dir, os.R_OK):
                pytest.skip("running with privileges that read anything")
            assert store.is_owner(name) is None
        finally:
            lock_dir.chmod(0o755)
            store.release(name)

    def test_the_remote_store_answers_the_same_three_ways(self, tmp_path):
        def sh(script):
            r = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
            return r.returncode, r.stdout, r.stderr

        store = RemoteLockManager(sh, str(tmp_path), hostname="nas")
        name = f"{RECEIVING_LOCK_PREFIX}/backup/.snapshots/7"
        store.acquire_once(name, "receive")
        lock_dir = Path(store._lock_dir(name))
        try:
            assert store.is_owner(name) is True
            (lock_dir / f"{OWNER_PREFIX}another").write_text("{}")
            assert store.is_owner(name) is False
            (lock_dir / f"{OWNER_PREFIX}another").unlink()
            store._run = lambda script: (255, "", "ssh: connection closed")  # type: ignore[method-assign]
            assert store.is_owner(name) is None
        finally:
            store._run = sh  # type: ignore[method-assign]
            store.release(name)


class TestAStaleBrokenHolderPublishesNothing:
    def test_the_publish_is_refused_and_the_successors_slot_is_left_alone(
        self, tmp_path, real_shell
    ):
        taken = _Taken(tmp_path)
        try:
            with pytest.raises(__util__.ReceiveLeaseLost) as e:
                taken.layout.publish(info_xml=b"<info/>")
            assert "lost the receive lease on slot 7" in str(e.value)
            assert "refusing to publish it" in str(e.value)
            assert taken.slot_as_b_left_it(), "the stale-broken run touched B's slot"
            assert not (taken.partial.parent / "info.xml").exists(), (
                "the stale-broken run wrote its info.xml into B's slot"
            )
            assert taken.layout.open_number is None
        finally:
            taken.close()

    def test_the_abandon_removes_nothing(self, tmp_path, real_shell):
        taken = _Taken(tmp_path)
        try:
            taken.layout.abandon()
            assert taken.slot_as_b_left_it(), "the stale-broken run removed B's receive"
            assert taken.layout.open_number is None
        finally:
            taken.close()

    def test_a_lease_that_cannot_be_confirmed_is_refused_the_same_way(
        self, tmp_path, real_shell, monkeypatch
    ):
        base = tmp_path / "target"
        layout = SnapperLayout(_endpoint(base))
        layout.open_slot(7)
        landing = _received(layout, 7, "this run's own copy")
        monkeypatch.setattr(
            LocalLockManager, "_check_owner_here", lambda *a: "UNREADABLE"
        )
        with pytest.raises(__util__.ReceiveLeaseLost, match="could not be confirmed"):
            layout.publish()
        assert landing.exists(), "a copy was removed on a lease that was never judged"
        assert not Path(layout.slot_dir(7)).exists()

    def test_a_holder_whose_lease_stands_publishes_as_before(
        self, tmp_path, real_shell
    ):
        base = tmp_path / "target"
        layout = SnapperLayout(_endpoint(base))
        layout.open_slot(7)
        _received(layout, 7, "this run's own copy")
        assert layout.publish(info_xml=b"<info/>") == 7
        assert (Path(layout.slot_dir(7)) / "snapshot" / "marker").read_text() == (
            "this run's own copy"
        )
        assert (Path(layout.slot_dir(7)) / "info.xml").read_bytes() == b"<info/>"

    def test_a_slot_without_a_lease_of_this_process_is_not_fenced(
        self, tmp_path, real_shell, monkeypatch
    ):
        """A stand-in lock, or the operator's opt-out: no lease was taken,
        so there is none to lose, and the layout works as it did."""
        import contextlib

        monkeypatch.setattr(
            ops, "_receiving_lock", lambda *a, **k: contextlib.nullcontext()
        )
        layout = SnapperLayout(_endpoint(tmp_path / "target"))
        layout.open_slot(7)
        _received(layout, 7, "copy")
        assert layout.publish() == 7


class TestTheBackupFailsLoudlyAndTouchesNothing:
    def test_the_snapper_backup_reports_the_lost_lease(
        self, tmp_path, real_shell, monkeypatch
    ):
        """The run is suspended during its send; the next run breaks the lease
        and receives into the slot; this run resumes and reaches its publish."""
        monkeypatch.setattr(ops, "log_transaction", lambda **k: None)
        monkeypatch.setattr(ops, "_write_snapper_metadata", lambda *a, **k: None)
        monkeypatch.setattr(
            ops,
            "_create_snapper_snapshot_wrapper",
            lambda snap, ep=None: SimpleNamespace(
                get_name=lambda: f"home-{snap.number}"
            ),
        )
        base = tmp_path / "target"
        endpoint = _endpoint(base)
        state: dict = {}

        def suspended_during_the_send(source, destination, parent=None, options=None):
            layout_dir = Path(destination.config["path"])  # the open slot
            name = f"{RECEIVING_LOCK_PREFIX}{base}/.snapshots/7"
            holder = destination._receive_lock_manager()
            state["successor"] = _stale_break(base, holder, name)
            state["name"] = name
            landing = layout_dir / "snapshot"
            landing.mkdir()
            (landing / "marker").write_text("B")
            state["landing"] = landing

        monkeypatch.setattr(ops, "send_snapshot", suspended_during_the_send)
        snapper = SimpleNamespace(
            number=7,
            subvolume_path=tmp_path / "src" / "7" / "snapshot",
            info_xml_path=tmp_path / "src" / "7" / "info.xml",
        )
        try:
            with pytest.raises(__util__.SnapshotTransferError) as e:
                ops.send_snapper_snapshot(snapper, endpoint, options={})
            assert "lost the receive lease on slot 7" in str(e.value)
            assert (state["landing"] / "marker").read_text() == "B"
            assert not (base / ".snapshots" / "7").exists(), "B's partial was published"
            assert str(endpoint.config["path"]) == str(base)
        finally:
            state["successor"].release(state["name"])


class TestASlotStillBeingReceivedIntoIsNotCleared:
    """The cleanup at the door: a leftover is judged by when it was last
    written, by the store's clock, against the lease threshold."""

    def _occupied(
        self, tmp_path, *, with_subvolume: bool = True
    ) -> tuple[SnapperLayout, Path]:
        incoming = tmp_path / "target" / ".snapshots" / "7.incoming"
        written = incoming / "snapshot" if with_subvolume else incoming
        written.mkdir(parents=True)
        if with_subvolume:
            (written / "half").write_text("of a receive")
        return SnapperLayout(_endpoint(tmp_path / "target")), written

    def test_a_recently_written_slot_is_refused_and_kept(self, tmp_path, real_shell):
        layout, written = self._occupied(tmp_path)
        with pytest.raises(
            __util__.SnapshotTransferError, match="may still be in progress"
        ):
            layout.open_slot(7)
        assert (written / "half").exists(), "a receive still arriving was removed"
        assert layout.open_number is None
        store = layout.endpoint._receive_lock_manager()
        assert store.is_owner(_slot_lock_name(layout, 7)) is False, (
            "the slot lock was kept"
        )

    def test_a_slot_written_past_the_threshold_is_a_leftover_and_goes(
        self, tmp_path, real_shell
    ):
        layout, written = self._occupied(tmp_path)
        store = layout.endpoint._receive_lock_manager()
        _age(written, store.stale_after + 30)
        layout.open_slot(7)
        assert not (written / "half").exists()
        assert Path(layout.incoming_dir(7)).is_dir()

    def test_the_bare_incoming_directory_is_judged_the_same_way(
        self, tmp_path, real_shell
    ):
        layout, incoming = self._occupied(tmp_path, with_subvolume=False)
        with pytest.raises(
            __util__.SnapshotTransferError, match="may still be in progress"
        ):
            layout.open_slot(7)
        assert incoming.is_dir()
        _age(incoming, layout.endpoint._receive_lock_manager().stale_after + 30)
        layout.open_slot(7)
        assert layout.open_number == 7

    def test_a_clock_that_cannot_be_read_keeps_the_slot(
        self, tmp_path, real_shell, monkeypatch
    ):
        layout, written = self._occupied(tmp_path)
        _age(written, 3600)
        monkeypatch.setattr(LocalLockManager, "_probe_now", lambda self, root_fd: None)
        with pytest.raises(__util__.SnapshotTransferError, match="could not be read"):
            layout.open_slot(7)
        assert (written / "half").exists()

    def test_the_age_is_the_stores_own_clock(self, tmp_path):
        store = local_manager(str(tmp_path), dir_name=RECEIVING_DIR_NAME)
        name = f"{RECEIVING_LOCK_PREFIX}{tmp_path}/.snapshots/7"
        store.acquire_once(name, "receive")  # the store exists once a lock has
        try:
            fresh = tmp_path / "fresh"
            fresh.write_text("")
            just_now = store.age_of(str(fresh))
            assert just_now is not None and 0 <= just_now <= 5
            _age(fresh, 400)
            a_while_ago = store.age_of(str(fresh))
            assert a_while_ago is not None and 395 <= a_while_ago <= 405
            assert store.age_of(str(tmp_path / "absent")) is None
        finally:
            store.release(name)

    def test_the_remote_store_reads_the_age_the_same_way(self, tmp_path):
        def sh(script):
            r = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
            return r.returncode, r.stdout, r.stderr

        store = RemoteLockManager(sh, str(tmp_path), hostname="nas")
        name = f"{RECEIVING_LOCK_PREFIX}/backup/.snapshots/7"
        store.acquire_once(name, "receive")
        try:
            aged = tmp_path / "aged"
            aged.write_text("")
            _age(aged, 400)
            a_while_ago = store.age_of(str(aged))
            assert a_while_ago is not None and 395 <= a_while_ago <= 405
            assert store.age_of(str(tmp_path / "absent")) is None
        finally:
            store.release(name)


class _Snap:
    def __init__(self, name: str) -> None:
        self.name = name
        self.locks: set = set()
        self.parent_locks: set = set()

    def get_name(self) -> str:
        return self.name

    def get_path(self) -> str:
        return f"/src/snapshots/{self.name}"

    def __str__(self) -> str:
        return self.name


class TestTheSshPartialIsLeftToTheRunThatHoldsTheLock:
    """The ssh destination removes the partial its failed receive left, inside
    its receive lock. A holder whose lock was broken while it was stalled
    finds another run's receive at that path, and leaves it."""

    def _ssh(self, tmp_path):
        def run(script):
            proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
            return proc.returncode, proc.stdout, proc.stderr

        manager = RemoteLockManager(run, str(tmp_path), hostname="testhost")
        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"path": str(tmp_path)}
        endpoint._is_remote = True
        endpoint.hostname = "testhost"
        endpoint._build_lock_manager = lambda: manager  # type: ignore[method-assign]
        endpoint._normalize_path = lambda p: str(p)  # type: ignore[method-assign]
        endpoint.get_id = lambda: "d"  # type: ignore[method-assign]
        endpoint.add_snapshot = lambda snapshot, rewrite=True: None  # type: ignore[method-assign]
        endpoint.list_snapshots = lambda flush_cache=False: []  # type: ignore[method-assign]
        endpoint._require_remote_destination = lambda path: True  # type: ignore[method-assign]
        endpoint._exec_remote_command = (  # type: ignore[method-assign]
            lambda *a, **k: SimpleNamespace(returncode=1)
        )
        endpoint.artifact_exists = lambda d, n: False  # type: ignore[method-assign]
        removed: list[tuple[str, bool]] = []
        endpoint._cleanup_partial_subvolume = (  # type: ignore[method-assign]
            lambda d, n, *, created_by_this_run: removed.append(
                (n, created_by_this_run)
            )
        )
        return endpoint, manager, removed

    def test_a_failed_receive_whose_lock_was_broken_leaves_the_partial(
        self, monkeypatch, tmp_path
    ):
        endpoint, manager, removed = self._ssh(tmp_path)
        state: dict = {}

        def suspended_during_the_send(*a, **k):
            (name,) = list(manager._tokens)
            lock_dir = Path(manager._lock_dir(name))
            for entry in lock_dir.iterdir():
                _age(entry, 2 * manager.stale_after + 60)
            _age(lock_dir, 2 * manager.stale_after + 60)
            successor = RemoteLockManager(manager._run, str(tmp_path), hostname="other")
            assert successor.acquire_once(name, "receive") == "stale-broken"
            state["successor"], state["name"] = successor, name
            raise __util__.SnapshotTransferError("the receive failed on resuming")

        monkeypatch.setattr(ops, "send_snapshot", suspended_during_the_send)
        try:
            result = ops._execute_transfers(
                MagicMock(), endpoint, [(_Snap("s1"), None)], {}
            )
            assert result.failed
            assert removed == [], "the stale-broken run removed the successor's receive"
        finally:
            state["successor"].release(state["name"])

    def test_a_failed_receive_whose_lock_stands_still_removes_its_partial(
        self, monkeypatch, tmp_path
    ):
        endpoint, manager, removed = self._ssh(tmp_path)
        monkeypatch.setattr(
            ops,
            "send_snapshot",
            MagicMock(side_effect=__util__.SnapshotTransferError("boom")),
        )
        result = ops._execute_transfers(
            MagicMock(), endpoint, [(_Snap("s1"), None)], {}
        )
        assert result.failed
        assert removed == [("s1", True)]
        assert manager.live_lock_names() == set()
