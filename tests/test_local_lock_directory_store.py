"""One lock store per location: a local endpoint honours the directory store.

An ssh:// target keeps its persistent locks -- the pins a restore holds, the
locks a receive holds -- in a directory named ``.btrfs-backup-ng.locks`` under
the target (``sshutil/lock.py``). A local endpoint keeps a JSON FILE of the
same name. A local endpoint over an ssh:// mirror (a transfer onward from it,
a prune of it on the host itself) therefore met the directory where it
expected its file, and refused: proceeding with an empty lock set would have
let a local prune delete what a remote restore holds. Renaming either store
was rejected for the same reason -- the two would be blind to each other.

Now, when the location carries the directory, the local endpoint USES it: the
same scripts the ssh endpoint runs on the remote, run here through ``sh``. The
pin a restore takes over ssh:// is the pin a local prune sees, and the pin a
local run takes is the pin an ssh:// prune sees. Nothing is mocked: the
"remote" side is a ``RemoteLockManager`` driving the real scripts against the
same directory, as the ssh endpoint would.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.sshutil.lock import (
    LOCK_DIR_NAME,
    RemoteLockManager,
    RemoteLockUnavailable,
    blocked_by_remote_lock,
    read_persisted_locks,
)

PREFIX = "home-"
FIRST = "home-20260101-120000"
SECOND = "home-20260102-120000"


def _ssh_side(target: Path) -> RemoteLockManager:
    """What the ssh:// endpoint runs on the remote: the same scripts, here."""

    def run(script: str):
        proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    return RemoteLockManager(run, str(target), hostname="remote-restorer")


def _location(tmp_path: Path, *names: str) -> Path:
    location = tmp_path / "mirror"
    location.mkdir()
    for name in names:
        (location / name).mkdir()
    return location


def _endpoint(location: Path, **extra) -> LocalEndpoint:
    """``extra`` travels as keyword arguments, the way the CLI threads a
    target's options (``skip_remote_lock`` among them) into every endpoint."""
    return LocalEndpoint(
        config={"path": str(location), "snap_prefix": PREFIX, "fs_checks": "skip"},
        **extra,
    )


def _record_deletes(monkeypatch, endpoint: LocalEndpoint) -> list:
    """The delete command, captured instead of run: tmp_path is not btrfs."""
    issued: list = []
    monkeypatch.setattr(
        endpoint, "_exec_command", lambda options, **kw: issued.append(options)
    )
    return issued


class TestARemotePinIsHonouredLocally:
    def test_the_directory_store_is_read_in_the_lock_file_shape(self, tmp_path):
        location = _location(tmp_path, FIRST)
        _ssh_side(location).acquire_shared(f"snap-{FIRST}", "restore:20260101-abc")
        assert (location / LOCK_DIR_NAME).is_dir()

        assert _endpoint(location)._read_locks() == {
            FIRST: {"locks": ["restore:20260101-abc"]}
        }

    def test_the_listing_carries_the_remote_pin(self, tmp_path):
        location = _location(tmp_path, FIRST, SECOND)
        _ssh_side(location).acquire_shared(f"snap-{FIRST}", "restore:20260101-abc")

        by_name = {s.get_name(): s for s in _endpoint(location).list_snapshots()}
        assert by_name[FIRST].locks == {"restore:20260101-abc"}
        assert by_name[SECOND].locks == set()

    def test_a_local_prune_refuses_to_delete_what_a_remote_restore_holds(
        self, tmp_path, monkeypatch
    ):
        """The defect the refusal stood in for. With the store ignored, the
        pinned snapshot lists as unlocked and the delete goes through."""
        location = _location(tmp_path, FIRST, SECOND)
        remote = _ssh_side(location)
        remote.acquire_shared(f"snap-{FIRST}", "restore:20260101-abc")
        ep = _endpoint(location)
        issued = _record_deletes(monkeypatch, ep)
        snapshots = ep.list_snapshots()

        result = ep.delete_snapshots(snapshots)

        assert [s.get_name() for s, _ in result.skipped] == [FIRST]
        assert [s.get_name() for s in result.deleted] == [SECOND]
        assert len(issued) == 1, "exactly the unpinned snapshot was deleted"
        assert FIRST not in " ".join(str(c) for c in issued[0]["command"])

        # The control: once the remote restore lets go, the same prune deletes it.
        remote.release_shared(f"snap-{FIRST}", "restore:20260101-abc")
        ep2 = _endpoint(location)
        issued2 = _record_deletes(monkeypatch, ep2)
        result2 = ep2.delete_snapshots(ep2.list_snapshots())
        assert [s.get_name() for s in result2.deleted] == [FIRST, SECOND]
        assert len(issued2) == 2

    def test_a_pin_taken_after_the_listing_still_blocks_the_delete(
        self, tmp_path, monkeypatch
    ):
        """The store is asked again at delete time. A prune lists, then a
        restore elsewhere pins, then the prune deletes: the listing's lock
        sets are stale and only a fresh query sees the pin."""
        location = _location(tmp_path, FIRST)
        ep = _endpoint(location)
        issued = _record_deletes(monkeypatch, ep)
        snapshots = ep.list_snapshots()
        assert snapshots[0].locks == set()

        _ssh_side(location).acquire_shared(f"snap-{FIRST}", "restore:late")
        result = ep.delete_snapshots(snapshots)

        assert [s.get_name() for s, _ in result.skipped] == [FIRST]
        assert issued == []

    def test_an_unanswerable_store_deletes_nothing(self, tmp_path, monkeypatch):
        location = _location(tmp_path, FIRST)
        _ssh_side(location).acquire_shared(f"snap-{SECOND}", "x")  # store exists
        ep = _endpoint(location)
        issued = _record_deletes(monkeypatch, ep)
        snapshots = ep.list_snapshots()

        def cannot_ask(manager, snaps):
            raise RemoteLockUnavailable("sh is gone")

        monkeypatch.setattr(
            "btrfs_backup_ng.sshutil.lock.blocked_by_remote_lock", cannot_ask
        )
        result = ep.delete_snapshots(snapshots)
        assert result.failed_count == 1 and result.deleted == []
        assert issued == []

    def test_a_pin_with_an_unreadable_payload_still_blocks(self, tmp_path, monkeypatch):
        """ "Something holds this and we cannot say what" is a lock."""
        location = _location(tmp_path, FIRST)
        _ssh_side(location).acquire_shared(f"snap-{FIRST}", "restore:x")
        holders = next((location / LOCK_DIR_NAME).glob("*/holders/*"))
        holders.write_text("not json")
        ep = _endpoint(location)
        issued = _record_deletes(monkeypatch, ep)
        result = ep.delete_snapshots(ep.list_snapshots())
        assert [s.get_name() for s, _ in result.skipped] == [FIRST]
        assert issued == []


class TestALocalPinIsVisibleToTheSshSide:
    def test_set_lock_writes_a_holder_the_remote_guard_sees(self, tmp_path):
        location = _location(tmp_path, FIRST)
        (location / LOCK_DIR_NAME).mkdir()  # the location is an ssh:// target
        ep = _endpoint(location)
        (snapshot,) = ep.list_snapshots()

        ep.set_lock(snapshot, "/other/target", True)

        remote = _ssh_side(location)
        assert blocked_by_remote_lock(remote, [snapshot]) == {FIRST}
        assert read_persisted_locks(remote) == {FIRST: {"locks": ["/other/target"]}}
        assert not (location / LOCK_DIR_NAME).is_file()
        assert not list(location.glob("*.guard")), "no file-store guard was made"

        ep.set_lock(snapshot, "/other/target", False)
        assert blocked_by_remote_lock(remote, [snapshot]) == set()

    def test_a_parent_pin_is_keyed_apart(self, tmp_path):
        location = _location(tmp_path, FIRST)
        (location / LOCK_DIR_NAME).mkdir()
        ep = _endpoint(location)
        (snapshot,) = ep.list_snapshots()
        ep.set_lock(snapshot, "t", True, parent=True)
        assert read_persisted_locks(_ssh_side(location)) == {
            FIRST: {"parent_locks": ["t"]}
        }
        ep.set_lock(snapshot, "t", False, parent=True)
        assert read_persisted_locks(_ssh_side(location)) == {}

    def test_unlock_reconciles_the_store(self, tmp_path):
        """``restore --unlock`` writes the state it wants through _write_locks."""
        location = _location(tmp_path, FIRST)
        remote = _ssh_side(location)
        remote.acquire_shared(f"snap-{FIRST}", "restore:a")
        remote.acquire_shared(f"snap-{FIRST}", "restore:b")
        ep = _endpoint(location)
        ep._write_locks({FIRST: {"locks": ["restore:b"]}})
        assert read_persisted_locks(remote) == {FIRST: {"locks": ["restore:b"]}}
        ep._write_locks({})
        assert read_persisted_locks(remote) == {}

    def test_an_unwritable_store_stops_the_run_unless_opted_out(
        self, tmp_path, monkeypatch
    ):
        location = _location(tmp_path, FIRST)
        (location / LOCK_DIR_NAME).mkdir()
        ep = _endpoint(location)
        (snapshot,) = ep.list_snapshots()

        def unusable(*a, **k):
            raise RemoteLockUnavailable("read-only location")

        manager = ep._lock_store_manager()
        monkeypatch.setattr(manager, "acquire_shared_persistent", unusable)
        with pytest.raises(__util__.AbortError, match="skip-remote-lock"):
            ep.set_lock(snapshot, "t", True)
        assert "t" in snapshot.locks, "the in-memory pin is still kept"

        opted = _endpoint(location, skip_remote_lock=True)
        (snap2,) = opted.list_snapshots()
        manager2 = opted._lock_store_manager()
        monkeypatch.setattr(manager2, "acquire_shared_persistent", unusable)
        opted.set_lock(snap2, "t", True)  # must not raise


class TestALocationWithoutTheDirectoryIsUnchanged:
    def test_the_lock_file_is_written_and_read_as_before(self, tmp_path):
        location = _location(tmp_path, FIRST)
        ep = _endpoint(location)
        (snapshot,) = ep.list_snapshots()
        ep.set_lock(snapshot, "t", True)
        lock_file = location / LOCK_DIR_NAME
        assert lock_file.is_file()
        assert json.loads(lock_file.read_text()) == {FIRST: {"locks": ["t"]}}
        assert ep._lock_store_manager() is None
        assert _endpoint(location)._read_locks() == {FIRST: {"locks": ["t"]}}

    def test_a_non_regular_non_directory_entry_is_still_refused(self, tmp_path):
        location = _location(tmp_path, FIRST)
        (location / LOCK_DIR_NAME).symlink_to(tmp_path / "nowhere")
        with pytest.raises(__util__.AbortError, match="not a regular file"):
            _endpoint(location)._read_locks()

    def test_the_refusal_for_a_directory_is_gone(self, tmp_path):
        location = _location(tmp_path)
        (location / LOCK_DIR_NAME).mkdir()
        assert _endpoint(location)._read_locks() == {}
