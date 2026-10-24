"""An incremental receive pins, on the destination, the parent it chains against.

A transfer of S3 incremental from S2 applies against the destination's copy of
S2. The source side pinned S2 so the SOURCE could not prune it mid-send; the
destination side pinned nothing, so a prune of the destination -- a second
config, another process, another machine -- was free to delete its copy of S2
while S3's stream was in flight. The receive then failed after sending
everything, or a raw store committed a stream whose recorded parent was gone
(#135).

The transfer loop, the snapper backup into a raw store and the snapper backup
into a btrfs target now record a reader pin on the destination's copy of the
parent for the span of the receive, through the destination's own ``set_lock``
(``parent=True``), under ``receiving:<child>``. Every delete path re-reads the
destination's pins at delete time and skips a pinned parent, so the deletion is
a skip, never a race. The pin is released on every exit. A parent that is not
on the destination any more fails the transfer before a byte is sent.

Each test here runs the prune from a SECOND endpoint over the same location,
with its own empty in-memory lock sets -- what a prune in another process sees
-- so what protects the parent is the pin on disk, not an object in memory.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import prune as prune_cli
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot, StructureVerdict


def _real_shell(ep, script):
    r = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return r.returncode, r.stdout


@pytest.fixture
def real_shell(monkeypatch):
    monkeypatch.setattr(ops, "_snapper_run_shell", _real_shell)


def _endpoint(base: Path, **config) -> LocalEndpoint:
    base.mkdir(parents=True, exist_ok=True)
    return LocalEndpoint(
        config={"path": str(base), "snap_prefix": "", "fs_checks": "skip", **config}
    )


class _Snap:
    def __init__(self, location: Path, name: str) -> None:
        self.location = location
        self.name = name
        self.locks: set = set()
        self.parent_locks: set = set()

    def get_name(self) -> str:
        return self.name

    def get_path(self) -> Path:
        return self.location / self.name

    def __str__(self) -> str:
        return self.name


class _Source:
    """The source side of a transfer: its pins are the other half of the story."""

    def __init__(self) -> None:
        self.pins: list[tuple[str, bool, bool]] = []

    def set_lock(self, snapshot, lock_id, state, parent=False) -> None:
        self.pins.append((snapshot.get_name(), state, parent))


class TestTheTransferLoopPinsTheParentOnTheDestination:
    """A local btrfs destination; its pins live in the lock file at its path."""

    def _destination(self, tmp_path, monkeypatch):
        dest = _endpoint(tmp_path / "dest")
        here = Path(dest.config["path"])
        # The destination's copy of the parent, found by name, as the planner
        # counts an in-run parent present; the child is not there yet.
        (here / "home.1").mkdir()
        monkeypatch.setattr(dest, "list_snapshots", lambda flush_cache=False: [])
        monkeypatch.setattr(dest, "add_snapshot", lambda s, rewrite=True: None)
        monkeypatch.setattr(
            ops, "artifact_verdict", lambda ep, s: StructureVerdict("ok", "")
        )
        return dest, here

    def _pruner(self, tmp_path, monkeypatch):
        """A prune of the destination in another process."""
        pruner = _endpoint(tmp_path / "dest")
        issued: list = []
        monkeypatch.setattr(
            pruner, "_exec_command", lambda spec: issued.append(spec["command"])
        )
        return pruner, issued

    def _plan(self):
        src = Path("/mnt/snapshots")
        return _Snap(src, "home.2"), _Snap(src, "home.1")

    def test_a_prune_during_the_receive_keeps_the_parent_and_deletes_it_after(
        self, tmp_path, monkeypatch
    ):
        dest, here = self._destination(tmp_path, monkeypatch)
        pruner, issued = self._pruner(tmp_path, monkeypatch)
        child, parent = self._plan()
        during: dict = {}

        def send(snapshot, destination_endpoint, parent=None, options=None):
            during["pins"] = dest._read_locks()
            during["prune"] = pruner.delete_snapshots([_Snap(here, "home.1")])

        monkeypatch.setattr(ops, "send_snapshot", send)
        result = ops._execute_transfers(_Source(), dest, [(child, parent)], {})

        assert result.transferred == [child], result.failed
        assert during["pins"]["home.1"]["parent_locks"] == ["receiving:home.2"]
        assert during["prune"].deleted == []
        assert [reason for _s, reason in during["prune"].skipped] == [
            "held by a retention lock"
        ]
        assert issued == [], "the parent was deleted under the receive"

        assert dest._read_locks().get("home.1", {}).get("parent_locks", []) == []
        after = pruner.delete_snapshots([_Snap(here, "home.1")])
        assert len(after.deleted) == 1 and len(issued) == 1

    @pytest.mark.parametrize(
        "ending",
        [__util__.SnapshotTransferError("the receive died"), KeyboardInterrupt()],
        ids=["failed", "interrupted"],
    )
    def test_the_pin_is_released_however_the_receive_ends(
        self, tmp_path, monkeypatch, ending
    ):
        dest, here = self._destination(tmp_path, monkeypatch)
        child, parent = self._plan()
        seen: dict = {}

        def send(snapshot, destination_endpoint, parent=None, options=None):
            seen["pins"] = dest._read_locks()
            raise ending

        monkeypatch.setattr(ops, "send_snapshot", send)
        if isinstance(ending, KeyboardInterrupt):
            with pytest.raises(KeyboardInterrupt):
                ops._execute_transfers(_Source(), dest, [(child, parent)], {})
        else:
            result = ops._execute_transfers(_Source(), dest, [(child, parent)], {})
            assert [s for s, _ in result.failed] == [child]

        assert seen["pins"]["home.1"]["parent_locks"] == ["receiving:home.2"]
        assert dest._read_locks().get("home.1", {}).get("parent_locks", []) == [], (
            "a destination pin outlived the receive it protected"
        )

    def test_a_parent_that_is_gone_fails_the_transfer_before_it_sends(
        self, tmp_path, monkeypatch
    ):
        dest, here = self._destination(tmp_path, monkeypatch)
        (here / "home.1").rmdir()  # pruned between the plan and the send
        child, parent = self._plan()
        sent = MagicMock()
        monkeypatch.setattr(ops, "send_snapshot", sent)

        result = ops._execute_transfers(_Source(), dest, [(child, parent)], {})

        sent.assert_not_called()
        assert [s for s, _ in result.failed] == [child]
        assert "is not on the destination" in str(result.failed[0][1])
        assert dest._read_locks() == {}

    def test_a_full_transfer_pins_nothing_on_the_destination(
        self, tmp_path, monkeypatch
    ):
        dest, here = self._destination(tmp_path, monkeypatch)
        pinned = MagicMock()
        monkeypatch.setattr(dest, "set_lock", pinned)
        monkeypatch.setattr(ops, "send_snapshot", lambda *a, **k: None)
        child, _ = self._plan()
        result = ops._execute_transfers(_Source(), dest, [(child, None)], {})
        assert result.transferred == [child]
        pinned.assert_not_called()


class TestASnapperBackupIntoARawStorePinsTheParentStream:
    """The raw store answers for the parent by name from its own listing -- a
    snapper stream is stored under its backup name -- for real."""

    def _stream(self, location: Path, name: str) -> RawSnapshot:
        stream = location / f"{name}.btrfs"
        stream.write_bytes(b"the parent stream")
        (location / f"{name}.btrfs.meta").write_text("{}")
        return RawSnapshot(name=name, stream_path=stream)

    def test_a_prune_during_the_receive_keeps_the_parent_stream(
        self, tmp_path, monkeypatch
    ):
        location = tmp_path / "raw"
        location.mkdir()
        store = RawEndpoint(config={"path": str(location), "snap_prefix": ""})
        pruner = RawEndpoint(config={"path": str(location), "snap_prefix": ""})
        parent = self._stream(location, "home-4-20240104-120000")
        child = _Snap(Path("/.snapshots/5"), "home-5-20240105-120000")
        parent_wrapper = _Snap(Path("/.snapshots/4"), "home-4-20240104-120000")
        during: dict = {}

        def send(snapshot, destination_endpoint, parent=None, options=None):
            theirs = RawSnapshot(
                name="home-4-20240104-120000",
                stream_path=location / "home-4-20240104-120000.btrfs",
            )
            during["prune"] = pruner.delete_snapshots([theirs])

        monkeypatch.setattr(ops, "send_snapshot", send)
        ops._send_snapper_snapshot_to_raw_store(
            child, store, parent_wrapper, {}, str(location)
        )

        assert during["prune"].deleted == []
        assert [reason for _s, reason in during["prune"].skipped] == [
            "locked by another process at this location"
        ]
        assert parent.stream_path.exists(), "the parent stream was deleted mid-receive"

        theirs = RawSnapshot(
            name="home-4-20240104-120000",
            stream_path=location / "home-4-20240104-120000.btrfs",
        )
        after = pruner.delete_snapshots([theirs])
        assert len(after.deleted) == 1 and not parent.stream_path.exists()

    def test_a_parent_stream_that_is_gone_fails_before_the_send(
        self, tmp_path, monkeypatch
    ):
        location = tmp_path / "raw"
        location.mkdir()
        store = RawEndpoint(config={"path": str(location), "snap_prefix": ""})
        sent = MagicMock()
        monkeypatch.setattr(ops, "send_snapshot", sent)
        child = _Snap(Path("/.snapshots/5"), "home-5-20240105-120000")
        missing = _Snap(Path("/.snapshots/4"), "home-4-20240104-120000")
        with pytest.raises(__util__.SnapshotTransferError, match="is not on the"):
            ops._send_snapper_snapshot_to_raw_store(
                child, store, missing, {}, str(location)
            )
        sent.assert_not_called()


class TestASnapperBackupIntoABtrfsTargetPinsTheParentSlot:
    """The slot has no listed snapshot to resolve; the pin is taken under the
    slot's own name, ``snapshot-<n>``, the key prune reads at delete time, and
    before the slot is opened, so it lands at the target and not in the slot."""

    def _snapper_snapshot(self, tmp_path, number):
        src = tmp_path / "src" / ".snapshots" / str(number)
        src.mkdir(parents=True)
        (src / "info.xml").write_bytes(b"<snapshot/>")
        return SimpleNamespace(
            number=number,
            subvolume_path=src / "snapshot",
            info_xml_path=src / "info.xml",
        )

    @pytest.fixture
    def quiet(self, monkeypatch):
        monkeypatch.setattr(ops, "log_transaction", lambda **k: None)
        monkeypatch.setattr(ops, "_write_snapper_metadata", lambda *a, **k: None)

        def wrapper(snap, ep=None):
            w = MagicMock()
            w.get_name.return_value = f"home-{snap.number}"
            return w

        monkeypatch.setattr(ops, "_create_snapper_snapshot_wrapper", wrapper)

    def _prune(self, monkeypatch, base: Path, number: int):
        deleted: list[str] = []
        monkeypatch.setattr(
            prune_cli,
            "_delete_snapper_slot_btrfs",
            lambda ep, slot_dir, remote: deleted.append(slot_dir),
        )
        monkeypatch.setattr(
            "btrfs_backup_ng.endpoint.choose_endpoint", lambda p, c: _endpoint(base)
        )
        return deleted, prune_cli.delete_snapper_backups(
            str(base), [{"number": number}], {}
        )

    def test_a_prune_during_the_receive_keeps_the_parent_slot(
        self, tmp_path, real_shell, quiet, monkeypatch, caplog
    ):
        base = tmp_path / "target"
        ep = _endpoint(base)
        (base / ".snapshots" / "4" / "snapshot").mkdir(parents=True)
        during: dict = {}

        def send(snapshot, destination_endpoint, parent=None, options=None):
            (Path(destination_endpoint.config["path"]) / "snapshot").mkdir()
            # The open slot points this endpoint's path at .incoming; the pins
            # are read where a prune reads them, at the target.
            during["pins"] = _endpoint(base)._read_locks()
            with caplog.at_level(logging.INFO):
                during["prune"] = self._prune(monkeypatch, base, 4)

        monkeypatch.setattr(ops, "send_snapshot", send)
        ops.send_snapper_snapshot(
            self._snapper_snapshot(tmp_path, 5),
            ep,
            parent_snapper_snapshot=self._snapper_snapshot(tmp_path, 4),
        )

        assert during["pins"] == {"snapshot-4": {"parent_locks": ["receiving:home-5"]}}
        deleted, (count, errors) = during["prune"]
        assert (count, errors, deleted) == (0, [], [])
        assert "Kept slot 4" in caplog.text
        assert "pinned by receiving:home-5" in caplog.text
        assert (base / ".snapshots" / "4").is_dir()
        assert (base / ".snapshots" / "5" / "snapshot").is_dir()

        # Released at the target's path, where it was written, once the slot
        # was published and the endpoint pointed back at the target.
        assert ep._read_locks().get("snapshot-4", {}).get("parent_locks", []) == []
        deleted, (count, errors) = self._prune(monkeypatch, base, 4)
        assert (count, errors) == (1, []) and deleted == [f"{base}/.snapshots/4"]

    def test_the_pin_is_written_at_the_target_not_in_the_slot(
        self, tmp_path, real_shell, quiet, monkeypatch
    ):
        base = tmp_path / "target"
        ep = _endpoint(base)
        (base / ".snapshots" / "4" / "snapshot").mkdir(parents=True)
        lock_file = str(ep.config["lock_file_name"])
        seen: dict = {}

        def send(snapshot, destination_endpoint, parent=None, options=None):
            slot = Path(destination_endpoint.config["path"])
            (slot / "snapshot").mkdir()
            # Observed while the slot is open: a pin written through the
            # repointed endpoint would sit in the slot and vanish with it.
            seen["in_the_slot"] = (slot / lock_file).exists()
            seen["at_the_target"] = _endpoint(base)._read_locks()
            raise __util__.SnapshotTransferError("the receive died")

        monkeypatch.setattr(ops, "send_snapshot", send)
        with pytest.raises(__util__.SnapshotTransferError):
            ops.send_snapper_snapshot(
                self._snapper_snapshot(tmp_path, 5),
                ep,
                parent_snapper_snapshot=self._snapper_snapshot(tmp_path, 4),
            )
        assert seen["in_the_slot"] is False, "a pin was written inside the slot"
        assert seen["at_the_target"] == {
            "snapshot-4": {"parent_locks": ["receiving:home-5"]}
        }
        assert ep._read_locks().get("snapshot-4", {}).get("parent_locks", []) == []

    def test_a_parent_slot_that_is_gone_fails_before_the_slot_is_opened(
        self, tmp_path, real_shell, quiet, monkeypatch
    ):
        base = tmp_path / "target"
        ep = _endpoint(base)
        sent = MagicMock()
        monkeypatch.setattr(ops, "send_snapshot", sent)
        with pytest.raises(
            __util__.SnapshotTransferError, match="snapshot-4 is not on"
        ):
            ops.send_snapper_snapshot(
                self._snapper_snapshot(tmp_path, 5),
                ep,
                parent_snapper_snapshot=self._snapper_snapshot(tmp_path, 4),
            )
        sent.assert_not_called()
        for slot in ("5", "5.incoming"):
            assert not (base / ".snapshots" / slot).exists(), (
                "a slot was opened for a doomed send"
            )
