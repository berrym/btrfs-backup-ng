"""A destination's right to create a copy is held from before the authorship
probe to after the commit, on every transport.

Two transfers of one snapshot into one local btrfs destination both decided,
before either receive ran, that they were the one creating it: the probe that
records authorship ran outside any lock, so both read "absent, so ours". One
won and committed a verified copy; the other's receive failed with "File
exists" and -- believing the artifact its own partial -- its cleanup deleted
the winner's copy. The destination ended empty after a run reported success.

ssh:// targets never had this: their receive lock is taken before the probe
and held through the cleanup. The local target had no lock at all. One lock
body now serves every transport (``sshutil.lock.hold_receiving_lock``), each
endpoint supplying only the store it lives in, and the transfer loop takes it
before it asks whether the copy pre-exists. The second transfer is refused
before its stream starts, as that snapshot's failure rather than the run's.
"""

from __future__ import annotations

import ast
import inspect
import shutil
import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import layout as layout_mod
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
from btrfs_backup_ng.sshutil.lock import (
    LOCK_DIR_NAME,
    RECEIVING_DIR_NAME,
    RemoteLockManager,
)


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


def _destination(sandbox: Path) -> Endpoint:
    """A base endpoint over ``sandbox``: a local btrfs target, with the two
    calls the transfer loop makes after a copy is verified answered."""
    endpoint = Endpoint.__new__(Endpoint)
    endpoint.config = {"path": str(sandbox)}
    endpoint._is_remote = False
    endpoint.get_id = lambda: "d"  # type: ignore[method-assign]
    endpoint.add_snapshot = lambda snapshot, rewrite=True: None  # type: ignore[method-assign]
    endpoint.list_snapshots = lambda flush_cache=False: []  # type: ignore[method-assign]
    return endpoint


class TestTheSecondTransferOfOneSnapshotIsRefusedBeforeItsStreamStarts:
    """The data-loss interleaving, forced: A has passed its authorship probe
    and is receiving; B transfers the same snapshot to completion; A resumes.
    Without the lock, A's receive fails with "File exists" and A's cleanup --
    told the artifact is its own -- deletes B's committed copy. With it, B is
    refused at once, A completes, and the copy is there afterwards."""

    def _race(self, monkeypatch, tmp_path):
        snapshot = _Snap("s1")
        dest_a, dest_b = _destination(tmp_path), _destination(tmp_path)
        a_in_receive = threading.Event()
        release_a = threading.Event()
        sends: dict[str, int] = {"a": 0, "b": 0}
        roles = {id(dest_a): "a", id(dest_b): "b"}

        def send(snap, destination, parent=None, options=None):
            role = roles[id(destination)]
            sends[role] += 1
            if role == "a":
                a_in_receive.set()
                assert release_a.wait(10), "the test never resumed A"
            artifact = Path(destination.config["path"]) / snap.get_name()
            if artifact.exists():
                # What btrfs receive says when the subvolume is already there.
                raise __util__.SnapshotTransferError(
                    f"creating subvolume {snap.get_name()} failed: File exists"
                )
            artifact.mkdir()

        def cleanup(endpoint, name, *, created_by_this_run):
            # The real cleanup is `btrfs subvolume delete`; what matters here
            # is whether it is told the artifact is this run's to remove.
            if created_by_this_run:
                shutil.rmtree(Path(endpoint.config["path"]) / name, ignore_errors=True)

        monkeypatch.setattr(ops, "send_snapshot", send)
        monkeypatch.setattr(ops, "_cleanup_partial_local_subvolume", cleanup)
        monkeypatch.setattr(
            ops, "artifact_verdict", lambda *a: StructureVerdict("ok", "stand-in")
        )
        source = MagicMock()
        results: dict[str, ops.TransferResult] = {}

        def run_a() -> None:
            results["a"] = ops._execute_transfers(
                source, dest_a, [(snapshot, None)], {}
            )

        thread = threading.Thread(target=run_a)
        thread.start()
        try:
            assert a_in_receive.wait(10), "A never reached its receive"
            results["b"] = ops._execute_transfers(
                source, dest_b, [(snapshot, None)], {}
            )
        finally:
            release_a.set()
            thread.join(10)
        return snapshot, results, sends, tmp_path / "s1"

    def test_the_copy_survives_and_exactly_one_transfer_succeeds(
        self, monkeypatch, tmp_path
    ):
        snapshot, results, sends, artifact = self._race(monkeypatch, tmp_path)
        assert artifact.is_dir(), (
            "the destination holds no copy although a transfer reported success"
        )
        assert results["a"].transferred == [snapshot], results["a"].failed
        assert results["b"].transferred == []

    def test_the_loser_is_refused_before_it_sends_anything(self, monkeypatch, tmp_path):
        snapshot, results, sends, _artifact = self._race(monkeypatch, tmp_path)
        assert sends["b"] == 0, "B's stream started while A held the copy"
        [(failed, error)] = results["b"].failed
        assert failed is snapshot
        assert "Already being received" in str(error), error

    def test_the_refusal_is_the_snapshots_failure_not_the_runs(
        self, monkeypatch, tmp_path
    ):
        """A busy copy fails that one transfer and the loop goes on; it must
        not escape as the abort an unlockable store is."""
        _snapshot, results, _sends, _artifact = self._race(monkeypatch, tmp_path)
        assert isinstance(results["b"], ops.TransferResult)


class TestTheLockIsTakenBeforeTheAuthorshipProbe:
    """Order is the whole fix: a probe made before the lock answers for a
    moment that may already have passed by the time the lock is held."""

    def test_the_probe_runs_while_the_lock_is_held(self, monkeypatch, tmp_path):
        held_during_probe: list[bool] = []
        dest = _destination(tmp_path)
        manager = dest._receive_lock_manager()
        real_probe = ops.destination_artifact_exists

        def probe(endpoint, name):
            held_during_probe.append(bool(manager.live_lock_names()))
            return real_probe(endpoint, name)

        monkeypatch.setattr(ops, "destination_artifact_exists", probe)
        monkeypatch.setattr(ops, "send_snapshot", lambda *a, **k: None)
        monkeypatch.setattr(
            ops, "artifact_verdict", lambda *a: StructureVerdict("ok", "stand-in")
        )
        ops._execute_transfers(MagicMock(), dest, [(_Snap("s1"), None)], {})
        assert held_during_probe == [True]

    def test_the_lock_is_released_afterwards(self, monkeypatch, tmp_path):
        dest = _destination(tmp_path)
        monkeypatch.setattr(ops, "send_snapshot", lambda *a, **k: None)
        monkeypatch.setattr(
            ops, "artifact_verdict", lambda *a: StructureVerdict("ok", "stand-in")
        )
        ops._execute_transfers(MagicMock(), dest, [(_Snap("s1"), None)], {})
        assert dest._receive_lock_manager().live_lock_names() == set()

    def test_the_lock_is_released_after_a_failed_transfer_too(
        self, monkeypatch, tmp_path
    ):
        dest = _destination(tmp_path)
        monkeypatch.setattr(
            ops,
            "send_snapshot",
            MagicMock(side_effect=__util__.SnapshotTransferError("boom")),
        )
        monkeypatch.setattr(
            ops, "_cleanup_partial_local_subvolume", lambda *a, **k: None
        )
        result = ops._execute_transfers(MagicMock(), dest, [(_Snap("s1"), None)], {})
        assert result.failed
        assert dest._receive_lock_manager().live_lock_names() == set()

    def test_a_missing_destination_is_reported_as_itself_not_as_a_lock_failure(
        self, monkeypatch, tmp_path
    ):
        """Against a missing target the lock would fail first and report a
        store that could not be created. The target is checked before it."""
        dest = _destination(tmp_path / "unmounted")
        monkeypatch.setattr(ops, "send_snapshot", lambda *a, **k: None)
        result = ops._execute_transfers(MagicMock(), dest, [(_Snap("s1"), None)], {})
        [(_snapshot, error)] = result.failed
        assert "does not exist" in str(error), error
        assert "lock" not in str(error).lower(), error


class TestAnSshDestinationStillCleansItsPartialInsideItsLock:
    """The transfer loop now holds the ssh destination's lock from outside the
    transfer, and the lock the transfer takes deeper down is re-entrant. The
    outer frame is therefore the one whose in-lock cleanup -- the partial
    subvolume this run received, removed by ``undo_on_failure`` with the
    receive's processes stopped first -- must see a failed or interrupted
    transfer. A lock released by ``ExitStack.close()`` tells its context
    managers the block succeeded, and the partial stays behind."""

    def _ssh(self, tmp_path):
        def run(script):
            proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
            return proc.returncode, proc.stdout, proc.stderr

        manager = RemoteLockManager(run, str(tmp_path), hostname="testhost")
        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"path": str(tmp_path)}
        endpoint._is_remote = True
        endpoint.hostname = "testhost"  # the loop's report names the destination
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

    def test_a_failed_transfer_removes_the_partial(self, monkeypatch, tmp_path):
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
        assert removed == [("s1", True)], "the ssh lock never saw the failure"
        assert manager.live_lock_names() == set()

    def test_an_interrupted_transfer_removes_it_before_the_lock_goes(
        self, monkeypatch, tmp_path
    ):
        endpoint, manager, removed = self._ssh(tmp_path)
        at_release: list[bool] = []
        real_release = manager.release

        def release(name):
            at_release.append(bool(removed))
            real_release(name)

        manager.release = release  # type: ignore[method-assign]

        def interrupted(*a, **k):
            raise KeyboardInterrupt

        monkeypatch.setattr(ops, "send_snapshot", interrupted)
        with pytest.raises(KeyboardInterrupt):
            ops._execute_transfers(MagicMock(), endpoint, [(_Snap("s1"), None)], {})
        assert removed == [("s1", True)], "the ssh lock never saw the interrupt"
        assert at_release == [True], "the lock was released before the partial went"


class TestTheLocalLockLeavesThePinStoreAlone:
    def test_a_transfer_creates_the_receive_store_and_not_the_pin_store(
        self, monkeypatch, tmp_path
    ):
        dest = _destination(tmp_path)
        dest.config["lock_file_name"] = LOCK_DIR_NAME
        monkeypatch.setattr(ops, "send_snapshot", lambda *a, **k: None)
        monkeypatch.setattr(
            ops, "artifact_verdict", lambda *a: StructureVerdict("ok", "stand-in")
        )
        ops._execute_transfers(MagicMock(), dest, [(_Snap("s1"), None)], {})
        assert (tmp_path / RECEIVING_DIR_NAME).is_dir()
        assert not (tmp_path / LOCK_DIR_NAME).exists()
        assert dest._lock_store_manager() is None


class TestTheSnapperLayoutsReceiverLeavesLockingToTheSlot:
    def test_its_receive_lock_holds_nothing(self, tmp_path):
        """The slot is chosen as the receive starts; before that there is no
        slot to lock, and the layout takes the real lock in open_slot."""
        endpoint = _destination(tmp_path)
        layout = layout_mod.SnapperLayout(endpoint)
        receiver = layout.receive_endpoint
        with receiver.receiving_lock(f"{tmp_path}/snapshot"):
            assert not (tmp_path / RECEIVING_DIR_NAME).exists()
            assert not (tmp_path / LOCK_DIR_NAME).exists()

    def test_the_layout_itself_takes_it_as_the_slot_opens(self):
        assert "_receiving_lock(" in inspect.getsource(
            layout_mod.SnapperLayout.open_slot
        )


class TestEveryReceiveFunnelIsLocked:
    """A receive can begin in exactly three places, and each takes the
    destination lock. A fourth that does not would reopen the race this
    module closes; this names the three so a new one has to be classified."""

    LOCKED = {
        "_execute_transfers": ("_receiving_lock(",),
        # btrfs: the layout's open_slot takes the slot lock for the receive
        "send_snapper_snapshot": ("open_slot(",),
        # raw: no slot to open, so the stream's own lock is taken here
        "_send_snapper_snapshot_to_raw_store": ("_receiving_lock(",),
    }

    def _callers_of_send_snapshot(self) -> dict[str, str]:
        tree = ast.parse(inspect.getsource(ops))
        callers: dict[str, str] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            for call in ast.walk(node):
                if (
                    isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Name)
                    and call.func.id == "send_snapshot"
                ):
                    callers[node.name] = (
                        ast.get_source_segment(inspect.getsource(ops), node) or ""
                    )
        return callers

    def test_every_caller_of_send_snapshot_is_a_known_locked_funnel(self):
        callers = self._callers_of_send_snapshot()
        unknown = set(callers) - set(self.LOCKED)
        assert not unknown, (
            f"a receive funnel is not classified: {sorted(unknown)}. Take the "
            f"destination lock there, or say here why the slot holds it."
        )
        assert set(callers) == set(self.LOCKED), (
            f"a classified funnel no longer calls send_snapshot: "
            f"{sorted(set(self.LOCKED) - set(callers))}"
        )

    def test_each_funnel_takes_the_lock(self):
        callers = self._callers_of_send_snapshot()
        for name, markers in self.LOCKED.items():
            for marker in markers:
                assert marker in callers[name], f"{name} does not contain {marker!r}"


class TestTheRawSnapperBackupIsLocked:
    def test_the_raw_branch_locks_the_stream_by_its_stored_name(
        self, monkeypatch, tmp_path
    ):
        """A snapper backup into a raw store runs through neither the transfer
        loop nor a slot; it had no lock site at all."""
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        dest = RawEndpoint(config={"path": str(tmp_path)})
        locked: list[str] = []
        real = ops._receiving_lock

        def spy(endpoint, destination, lock_root=""):
            locked.append(destination)
            return real(endpoint, destination, lock_root)

        monkeypatch.setattr(ops, "_receiving_lock", spy)
        monkeypatch.setattr(ops, "send_snapshot", lambda *a, **k: None)
        monkeypatch.setattr(ops, "_write_snapper_metadata", lambda *a, **k: None)
        monkeypatch.setattr(
            ops,
            "_create_snapper_snapshot_wrapper",
            lambda s, d=None: _Snap(f"root-{s.number}-20240101"),
        )
        snapper = MagicMock()
        snapper.number = 7
        snapper.subvolume_path = "/.snapshots/7/snapshot"
        ops.send_snapper_snapshot(snapper, dest, writer_lock_held=True)
        assert locked == [f"{tmp_path}/root-7-20240101"]
