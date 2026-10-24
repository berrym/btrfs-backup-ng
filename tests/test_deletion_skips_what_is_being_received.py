"""A deletion takes the artifact's receiving lock, so it skips what is being received.

``prune`` deleted a snapper destination slot by its number under no lock and
without asking whether anything was arriving in it. When snapper recycled a
number and a concurrent ``run`` republished that slot with a new backup, prune
deleted the new backup, believing it had removed the old occupant its plan had
chosen; run reported the snapshot transferred and prune reported it deleted,
and the destination held neither (#137). The same hole was open on every
transport's delete: a local or ssh ``delete_snapshots`` and a raw store's
stream removal consulted retention pins only, never the lock a transfer holds
on the artifact it is creating.

Every deletion primitive now takes the artifact's receiving lock -- the lock a
transfer holds from before its authorship probe through its commit -- before
touching the artifact, and takes it BEFORE any look at the path, because during
a receive the path exists and is the partial. A held lock is a skip ("being
received by ..."), never a race; a lock that cannot be taken is a failure, not
a guess, unless the operator passed ``skip_remote_lock``.

The deletion keys the lock exactly as the receive does. Each test below holds
the lock through ``operations._received_artifact_of`` -- the transfer loop's own
derivation, from a SOURCE snapshot -- and then deletes the DESTINATION's listing
of that artifact. If the two derivations ever drift apart the lock no longer
collides, the deletion proceeds, and these tests fail.
"""

from __future__ import annotations

import logging
import shlex
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from btrfs_backup_ng.cli import prune as prune_cli
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.core.layout import SnapperLayout
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
from btrfs_backup_ng.sshutil.lock import (
    RECEIVING_LOCK_PREFIX,
    RemoteLockManager,
    RemoteLockUnavailable,
)


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


def _received(layout: SnapperLayout, number: int) -> None:
    """What a ``btrfs receive`` into the open slot leaves."""
    landing = Path(layout.incoming_dir(number)) / "snapshot"
    landing.mkdir()
    (landing / "marker").write_text("NEW")


class _Snap:
    """A snapshot as the delete paths see one: named, located, unpinned."""

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


def _local_run(script: str) -> tuple[int, str, str]:
    proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr


class TestASnapperSlotBeingReceivedIsNotPruned:
    """The run holds the slot's lock from ``open_slot`` through the publish;
    prune asks for the same lock, keyed on the same slot directory, and keeps
    the slot while it is held."""

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
        outcome = prune_cli.delete_snapper_backups(str(base), [{"number": number}], {})
        return deleted, outcome

    def test_the_slot_a_run_is_filling_is_kept_and_deleted_once_published(
        self, tmp_path, real_shell, monkeypatch, caplog
    ):
        base = tmp_path / "target"
        layout = SnapperLayout(_endpoint(base))
        layout.open_slot(7)

        with caplog.at_level(logging.INFO):
            deleted, (count, errors) = self._prune(monkeypatch, base, 7)

        assert (count, errors) == (0, []), "the slot was pruned out from under the run"
        assert deleted == []
        assert "Kept slot 7" in caplog.text and "being received by" in caplog.text

        _received(layout, 7)
        layout.publish()
        deleted, (count, errors) = self._prune(monkeypatch, base, 7)
        assert (count, errors) == (1, [])
        assert deleted == [f"{base}/.snapshots/7"]

    def test_a_slot_nobody_is_filling_is_pruned_at_once(
        self, tmp_path, real_shell, monkeypatch
    ):
        """The lock is per slot: a receive into slot 7 does not keep slot 3."""
        base = tmp_path / "target"
        layout = SnapperLayout(_endpoint(base))
        layout.open_slot(7)
        deleted, (count, errors) = self._prune(monkeypatch, base, 3)
        assert (count, errors) == (1, [])
        assert deleted == [f"{base}/.snapshots/3"]
        layout.abandon()


class TestALocalDeletionSkipsWhatIsBeingReceived:
    def _destination(self, tmp_path, monkeypatch, **config):
        dest = _endpoint(tmp_path / "dest", **config)
        issued: list = []
        monkeypatch.setattr(
            dest, "_exec_command", lambda spec: issued.append(spec["command"])
        )
        return dest, issued

    def test_the_subvolume_is_skipped_while_received_and_deleted_after(
        self, tmp_path, monkeypatch
    ):
        dest, issued = self._destination(tmp_path, monkeypatch)
        receiver = _endpoint(tmp_path / "dest")  # another process's transfer
        source = _Snap(Path("/mnt/snapshots"), "home.20240101T120000")
        listed = _Snap(Path(dest.config["path"]), "home.20240101T120000")

        with receiver.receiving_lock(ops._received_artifact_of(receiver, source)):
            result = dest.delete_snapshots([listed])

        assert issued == [], "a delete was issued for a subvolume being received"
        assert result.deleted == [] and result.failed == []
        assert len(result.skipped) == 1
        snapshot, reason = result.skipped[0]
        assert snapshot is listed and reason.startswith("being received by ")

        result = dest.delete_snapshots([listed])
        assert result.deleted == [listed]
        # The command is in the endpoint's own form: (argument, is_path) pairs.
        assert issued and issued[0][-1] == (str(listed.get_path()), True)

    def test_other_subvolumes_are_still_deleted(self, tmp_path, monkeypatch):
        """Per artifact, not per location: the lock on one copy keeps only it."""
        dest, issued = self._destination(tmp_path, monkeypatch)
        receiver = _endpoint(tmp_path / "dest")
        source = _Snap(Path("/mnt/snapshots"), "home.20240101T120000")
        here = Path(dest.config["path"])
        received = _Snap(here, "home.20240101T120000")
        other = _Snap(here, "var.20240101T120000")

        with receiver.receiving_lock(ops._received_artifact_of(receiver, source)):
            result = dest.delete_snapshots([received, other])

        assert result.deleted == [other]
        assert [s for s, _ in result.skipped] == [received]
        assert [c[-1][0] for c in issued] == [str(other.get_path())]


class _UnusableStore:
    """A lock store that cannot be operated: not held, not takeable."""

    def holds(self, name: str) -> bool:
        return False

    def acquire_once(self, name: str, owner: str) -> None:
        raise RemoteLockUnavailable("the lock directory cannot be created")


class TestAnArtifactWhoseLockCannotBeTakenIsNotDeleted:
    """A deletion that cannot know whether a receive is under way does not
    guess -- unless the operator said ``skip_remote_lock``."""

    def _destination(self, tmp_path, monkeypatch, **config):
        dest = _endpoint(tmp_path / "dest", **config)
        issued: list = []
        monkeypatch.setattr(
            dest, "_exec_command", lambda spec: issued.append(spec["command"])
        )
        monkeypatch.setattr(dest, "_receive_lock_manager", lambda: _UnusableStore())
        return dest, issued

    def test_the_deletion_fails_and_says_why(self, tmp_path, monkeypatch):
        dest, issued = self._destination(tmp_path, monkeypatch)
        listed = _Snap(Path(dest.config["path"]), "home.20240101T120000")
        result = dest.delete_snapshots([listed])
        assert issued == []
        assert result.deleted == [] and result.skipped == []
        assert len(result.failed) == 1
        snapshot, error = result.failed[0]
        assert snapshot is listed
        assert "receiving lock could not be taken" in str(error)
        assert "cannot be created" in str(error), "the store's own reason was lost"

    def test_skip_remote_lock_lets_it_through_unprotected(
        self, tmp_path, monkeypatch, caplog
    ):
        dest, issued = self._destination(tmp_path, monkeypatch, skip_remote_lock=True)
        listed = _Snap(Path(dest.config["path"]), "home.20240101T120000")
        with caplog.at_level(logging.WARNING):
            result = dest.delete_snapshots([listed])
        assert result.deleted == [listed]
        assert len(issued) == 1
        assert "WITHOUT protection" in caplog.text


class TestAnSshDeletionSkipsWhatIsBeingReceived:
    """The ssh endpoint's delete checks the path is a subvolume and then
    deletes it, each a remote command; both must run under the lock, so the
    stub records whether this endpoint held the artifact's lock at each one."""

    def _ssh(self, tmp_path):
        manager = RemoteLockManager(_local_run, str(tmp_path), hostname="testhost")
        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"path": str(tmp_path)}
        endpoint._is_remote = True
        endpoint.hostname = "testhost"
        endpoint._build_lock_manager = lambda: manager  # type: ignore[method-assign]
        endpoint._normalize_path = lambda p: str(p)  # type: ignore[method-assign]
        issued: list[tuple[list, bool]] = []

        def exec_remote(cmd, **kwargs):
            held = manager.holds(f"{RECEIVING_LOCK_PREFIX}{cmd[-1]}")
            issued.append((list(cmd), held))
            return SimpleNamespace(returncode=0, stderr=b"")

        endpoint._exec_remote_command = exec_remote  # type: ignore[method-assign]
        return endpoint, issued

    def test_the_subvolume_is_skipped_while_received_and_deleted_under_the_lock(
        self, tmp_path
    ):
        endpoint, issued = self._ssh(tmp_path)
        receiver, _ = self._ssh(tmp_path)
        source = _Snap(Path("/mnt/snapshots"), "home.20240101T120000")
        listed = _Snap(tmp_path, "home.20240101T120000")

        with receiver._hold_receiving_lock(ops._received_artifact_of(receiver, source)):
            result = endpoint.delete_snapshots([listed])

        assert issued == [], "a remote command ran against a subvolume being received"
        assert len(result.skipped) == 1 and result.skipped[0][0] is listed
        assert result.skipped[0][1].startswith("being received by testhost")

        result = endpoint.delete_snapshots([listed])
        assert result.deleted == [listed]
        verbs = [(cmd[:3], held) for cmd, held in issued]
        assert verbs == [
            (["btrfs", "subvolume", "show"], True),
            (["btrfs", "subvolume", "delete"], True),
        ], "the existence check or the delete ran outside the lock"


class TestARawDeletionSkipsWhatIsBeingReceived:
    def _stream(self, location: Path, name: str) -> RawSnapshot:
        snap = RawSnapshot(name=name, stream_path=location / f"{name}.btrfs")
        snap.stream_path.write_bytes(b"a stream a transfer is still writing")
        return snap

    def test_a_local_stream_is_skipped_while_received_and_deleted_after(self, tmp_path):
        location = tmp_path / "raw"
        location.mkdir()
        store = RawEndpoint(config={"path": str(location), "snap_prefix": ""})
        receiver = RawEndpoint(config={"path": str(location), "snap_prefix": ""})
        source = _Snap(Path("/mnt/snapshots"), "home.20240101T120000")
        stream = self._stream(location, "home.20240101T120000")

        with receiver.receiving_lock(ops._received_artifact_of(receiver, source)):
            result = store.delete_snapshots([stream])

        assert stream.stream_path.exists(), "the stream being written was deleted"
        assert result.deleted == [] and result.failed == []
        assert len(result.skipped) == 1
        assert result.skipped[0][1].startswith("being received by ")

        result = store.delete_snapshots([stream])
        assert result.deleted == [stream]
        assert not stream.stream_path.exists()

    def _remote_store(self, tmp_path, log: Path) -> SSHRawEndpoint:
        location = tmp_path / "raw"
        location.mkdir(exist_ok=True)
        endpoint = SSHRawEndpoint(config={"path": str(location), "hostname": "nas"})
        manager = RemoteLockManager(_local_run, str(location), hostname="nas")
        endpoint._build_lock_manager = lambda: manager  # type: ignore[method-assign]
        # The "remote" shell: whatever would be run over ssh is appended to a
        # log instead, so the test reads what was asked of the remote.
        endpoint._build_ssh_command = lambda: [  # type: ignore[method-assign]
            "sh",
            "-c",
            f"printf '%s\\n' \"$1\" >> {shlex.quote(str(log))}",
            "--",
        ]
        return endpoint

    def test_a_remote_stream_is_skipped_while_received_and_removed_after(
        self, tmp_path
    ):
        log = tmp_path / "remote-commands"
        store = self._remote_store(tmp_path, log)
        receiver = self._remote_store(tmp_path, log)
        location = Path(store.config["path"])
        source = _Snap(Path("/mnt/snapshots"), "home.20240101T120000")
        stream = RawSnapshot(
            name="home.20240101T120000",
            stream_path=location / "home.20240101T120000.btrfs",
        )

        with receiver.receiving_lock(ops._received_artifact_of(receiver, source)):
            result = store.delete_snapshots([stream])

        # The chain guard lists the remote first; only an rm would be the defect.
        asked = log.read_text().splitlines() if log.exists() else []
        assert not [c for c in asked if c.startswith("rm ")], (
            f"a remote rm ran under the receive: {asked}"
        )
        assert len(result.skipped) == 1
        assert result.skipped[0][1].startswith("being received by nas")

        result = store.delete_snapshots([stream])
        assert result.deleted == [stream]
        removed = [c for c in log.read_text().splitlines() if c.startswith("rm ")]
        assert len(removed) == 1 and str(stream.stream_path) in removed[0]
