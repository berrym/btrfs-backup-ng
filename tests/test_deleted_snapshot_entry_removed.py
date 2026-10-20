"""A deleted snapshot's entry does not stay in the lock file.

``delete_snapshots`` reads the lock file again before it deletes, so a
snapshot it deletes holds no pin at that moment. A pin taken after that read
and before or during the deletion, by another target's thread or another
process, landed in an entry for a snapshot that no longer existed. Nothing
lists a deleted snapshot again, so nothing ever released that pin, and the
entry stayed in the file: ``restore --status`` and ``doctor`` reported a lock
held on a snapshot that did not exist, and ``--unlock`` was the only way out.
The same held for an entry with nothing in it, as a hand edit can leave.

Now the deleting endpoint removes the snapshot's entry after a successful
deletion, through the guarded by-change writer, with a warning when the
entry held a pin taken after the read and quietly when it held nothing. The
deletion stands either way.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.local import LocalEndpoint

S0, S1 = "home-20260101-000000", "home-20260102-000000"
LOCK_FILE = ".btrfs-backup-ng.locks"


@pytest.fixture(autouse=True)
def _no_btrfs_identity(monkeypatch):
    monkeypatch.setattr(LocalEndpoint, "subvolume_identity", lambda self, path: None)


def _location(path: Path, leftover: dict) -> LocalEndpoint:
    path.mkdir(parents=True, exist_ok=True)
    for name in (S0, S1):
        (path / name).mkdir(exist_ok=True)
    ep = LocalEndpoint(config={"path": path, "source": "/src", "snap_prefix": "home-"})
    (path / LOCK_FILE).write_text(__util__.write_locks(leftover))
    return ep


def _file(path: Path) -> dict:
    raw = __util__.read_locks((path / LOCK_FILE).read_text())
    return {n: {k: set(v) for k, v in e.items()} for n, e in raw.items()}


def _snap(ep: LocalEndpoint, name: str):
    return {s.get_name(): s for s in ep.list_snapshots(flush_cache=True)}[name]


def _deleting(ep: LocalEndpoint, deleted: list, fail: bool = False):
    """Stand in for ``btrfs subvolume delete``: record, or refuse."""

    def exec_command(options, **kwargs):
        if fail:
            raise RuntimeError("btrfs subvolume delete refused")
        deleted.append(options["command"])

    ep._exec_command = exec_command  # type: ignore[method-assign]


def _delete_with_a_pin_taken_after_the_read(ep: LocalEndpoint, path: Path, pins):
    """``ep.delete_snapshots``, with ``pins`` taken through another endpoint
    after the delete-time read of the lock file and before the deletion."""
    real_reload = ep._load_locks_into

    def reload_then_the_other_pins(snapshots):
        real_reload(snapshots)
        other = LocalEndpoint(config={"path": path, "snap_prefix": "home-"})
        for name, lock_id, parent in pins:
            other.set_lock(_snap(other, name), lock_id, True, parent=parent)

    def delete(snapshots):
        ep._load_locks_into = reload_then_the_other_pins  # type: ignore[method-assign]
        try:
            return ep.delete_snapshots(snapshots)
        finally:
            ep._load_locks_into = real_reload  # type: ignore[method-assign]

    return delete


class TestAPinTakenAfterTheReadDoesNotOutliveTheSnapshot:
    def test_the_entry_is_removed_and_the_pin_is_reported(self, tmp_path, shared_log):
        ep = _location(tmp_path, {S1: {"locks": ["/dest/one"]}})
        commands: list = []
        _deleting(ep, commands)
        snapshot = _snap(ep, S0)
        delete = _delete_with_a_pin_taken_after_the_read(
            ep, tmp_path, [(S0, "/dest/two", False), (S0, "/dest/three", True)]
        )
        result = delete([snapshot])

        assert [s.get_name() for s in result.deleted] == [S0]
        assert len(commands) == 1
        assert _file(tmp_path) == {S1: {"locks": {"/dest/one"}}}
        (warning,) = shared_log.messages(logging.WARNING)
        assert warning.startswith(f"Deleted {S0}")
        assert "/dest/two (locks)" in warning
        assert "/dest/three (parent_locks)" in warning

    def test_another_snapshots_pin_taken_meanwhile_is_kept(self, tmp_path):
        ep = _location(tmp_path, {})
        _deleting(ep, [])
        snapshot = _snap(ep, S0)
        delete = _delete_with_a_pin_taken_after_the_read(
            ep, tmp_path, [(S1, "/dest/two", False)]
        )
        delete([snapshot])
        assert _file(tmp_path) == {S1: {"locks": {"/dest/two"}}}


class TestAnEntryWithNothingInItIsRemovedQuietly:
    def test_quietly(self, tmp_path, shared_log):
        (tmp_path / S0).mkdir(parents=True)
        (tmp_path / S1).mkdir()
        ep = LocalEndpoint(
            config={"path": tmp_path, "source": "/src", "snap_prefix": "home-"}
        )
        (tmp_path / LOCK_FILE).write_text(
            '{"%s": {"locks": []}, "%s": {"locks": ["/dest/one"]}}' % (S0, S1)
        )
        _deleting(ep, [])
        result = ep.delete_snapshots([_snap(ep, S0)])
        assert [s.get_name() for s in result.deleted] == [S0]
        assert _file(tmp_path) == {S1: {"locks": {"/dest/one"}}}
        assert shared_log.messages(logging.WARNING) == []

    def test_a_snapshot_without_an_entry_leaves_the_file_as_it_is(self, tmp_path):
        ep = _location(tmp_path, {S1: {"locks": ["/dest/one"]}})
        _deleting(ep, [])
        before = (tmp_path / LOCK_FILE).stat().st_mtime_ns
        ep.delete_snapshots([_snap(ep, S0)])
        assert (tmp_path / LOCK_FILE).stat().st_mtime_ns == before
        assert _file(tmp_path) == {S1: {"locks": {"/dest/one"}}}


class TestTheDeletionStands:
    def test_a_failed_deletion_keeps_the_entry(self, tmp_path):
        ep = _location(tmp_path, {})
        _deleting(ep, [], fail=True)
        snapshot = _snap(ep, S0)
        delete = _delete_with_a_pin_taken_after_the_read(
            ep, tmp_path, [(S0, "/dest/two", False)]
        )
        result = delete([snapshot])
        assert result.deleted == []
        assert _file(tmp_path) == {S0: {"locks": {"/dest/two"}}}

    def test_an_entry_that_cannot_be_removed_is_a_warning(self, tmp_path, shared_log):
        ep = _location(tmp_path, {})
        _deleting(ep, [])
        snapshot = _snap(ep, S0)
        delete = _delete_with_a_pin_taken_after_the_read(
            ep, tmp_path, [(S0, "/dest/two", False)]
        )
        real_write = ep._write_locks

        def write_fails(lock_dict):
            raise __util__.AbortError("Cannot write the lock file: disk full")

        ep._write_locks = write_fails  # type: ignore[method-assign]
        result = delete([snapshot])
        ep._write_locks = real_write  # type: ignore[method-assign]

        assert [s.get_name() for s in result.deleted] == [S0]
        (warning,) = shared_log.messages(logging.WARNING)
        assert "could not be removed" in warning
        assert "restore --unlock" in warning
        assert _file(tmp_path) == {S0: {"locks": {"/dest/two"}}}
