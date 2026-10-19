"""The delete guard decides from the lock file as it is when it deletes.

``delete_snapshots`` skipped a snapshot whose in-memory lock sets were not
empty, and those sets were a copy taken at some listing -- in ``run``, the
listing of whichever target's thread filled the cache last, since the prune
lists without flushing. It never read the file again. So it deleted a
snapshot the file pinned when the copy predated the pin, and kept one that
only a stale copy still pinned. And a snapshot it did delete kept its entry
in the file for ever, since nothing lists a deleted snapshot again.

Now, for the lock-file store, a batch reads the file again first and each
snapshot's sets become what the file says; pins this process holds only in
memory (the file could not record them and the operation went on) are
added back.
"""

from __future__ import annotations

import errno
import os
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.local import LocalEndpoint

S0, S1 = "home-20260101-000000", "home-20260102-000000"
LOCK_FILE = ".btrfs-backup-ng.locks"


@pytest.fixture(autouse=True)
def _no_btrfs_identity(monkeypatch):
    monkeypatch.setattr(LocalEndpoint, "subvolume_identity", lambda self, path: None)


def _endpoint(path: Path, **config) -> LocalEndpoint:
    """An endpoint whose btrfs delete removes the directory standing in for
    the snapshot, and records it."""
    ep = LocalEndpoint(config={"path": path, "source": "/src", "snap_prefix": "home-"})
    ep.config.update(config)
    ep.deleted = []  # type: ignore[attr-defined]

    def exec_command(options, **kwargs):
        target = options["command"][-1]
        target = target[0] if isinstance(target, tuple) else target
        ep.deleted.append(Path(target).name)  # type: ignore[attr-defined]
        os.rmdir(target)

    ep._exec_command = exec_command  # type: ignore[method-assign]
    return ep


def _location(tmp_path: Path, leftover: dict | None = None) -> Path:
    for name in (S0, S1):
        (tmp_path / name).mkdir()
    if leftover is not None:
        (tmp_path / LOCK_FILE).write_text(__util__.write_locks(leftover))
    return tmp_path


def _listing(ep: LocalEndpoint) -> dict:
    return {s.get_name(): s for s in ep.list_snapshots(flush_cache=True)}


def _file(path: Path) -> dict:
    raw = __util__.read_locks((path / LOCK_FILE).read_text())
    return {n: {k: set(v) for k, v in e.items()} for n, e in raw.items()}


class TestTheFileDecides:
    def test_a_pin_taken_after_the_listing_is_honoured(self, tmp_path):
        """The object was listed before another process pinned it."""
        path = _location(tmp_path, {})
        ep = _endpoint(path)
        stale = _listing(ep)
        other = _endpoint(path)
        other.set_lock(_listing(other)[S1], "restore:s1", True)
        assert stale[S1].locks == set()

        result = ep.delete_snapshots([stale[S1]])

        assert ep.deleted == []  # type: ignore[attr-defined]
        assert result.skipped_count == 1 and result.deleted_count == 0
        assert (path / S1).is_dir()
        assert _file(path) == {S1: {"locks": {"restore:s1"}}}

    def test_a_pin_released_after_the_listing_does_not_keep_it(self, tmp_path):
        """Only a stale copy still pins it: the file no longer does."""
        path = _location(tmp_path, {S0: {"parent_locks": ["ssh"]}})
        ep = _endpoint(path)
        stale = _listing(ep)
        other = _endpoint(path)
        other.set_lock(_listing(other)[S0], "ssh", False, parent=True)
        assert stale[S0].parent_locks == {"ssh"}

        result = ep.delete_snapshots([stale[S0]])

        assert ep.deleted == [S0]  # type: ignore[attr-defined]
        assert result.deleted_count == 1
        assert not (path / S0).exists()

    def test_a_snapshot_with_no_entry_reads_as_unpinned(self, tmp_path):
        """The listing's loader, used to refresh, empties the sets of a
        snapshot the file has no entry for, rather than leaving the copy's."""
        path = _location(tmp_path, {})
        ep = _endpoint(path)
        snap = _listing(ep)[S1]
        snap.locks.add("gone-long-ago")
        ep._load_locks_into([snap])
        assert snap.locks == set() and snap.parent_locks == set()


class TestAnUnreadableFileRefusesTheBatch:
    def test_unreadable_at_the_listing_refuses_even_if_it_reads_now(self, tmp_path):
        path = _location(tmp_path, None)
        (path / LOCK_FILE).write_text("{ not json")
        ep = _endpoint(path)
        snaps = list(_listing(ep).values())
        (path / LOCK_FILE).write_text("{}")  # repaired after the listing

        result = ep.delete_snapshots(snaps)

        assert ep.deleted == []  # type: ignore[attr-defined]
        assert result.failed_count == 2 and not result.ok

    def test_unreadable_now_refuses_and_leaves_the_file_as_it_is(self, tmp_path):
        path = _location(tmp_path, {})
        ep = _endpoint(path)
        snaps = list(_listing(ep).values())
        (path / LOCK_FILE).write_text("{ not json")

        result = ep.delete_snapshots(snaps)

        assert ep.deleted == []  # type: ignore[attr-defined]
        assert result.failed_count == 2 and not result.ok
        assert (path / LOCK_FILE).read_text() == "{ not json"

    def test_unreadable_now_refuses_only_that_batch(self, tmp_path):
        """A file that cannot be read at delete time refuses that batch; once
        it reads again, the next batch from the same listing goes ahead."""
        path = _location(tmp_path, {})
        ep = _endpoint(path)
        snaps = _listing(ep)
        (path / LOCK_FILE).write_text("{ not json")
        assert ep.delete_snapshots([snaps[S0]]).failed_count == 1
        (path / LOCK_FILE).write_text("{}")  # repaired

        result = ep.delete_snapshots([snaps[S1]])

        assert ep.deleted == [S1]  # type: ignore[attr-defined]
        assert result.deleted_count == 1


class TestAPinHeldInMemoryOnly:
    """``--skip-remote-lock``: the lock file could not record the pin and the
    operation went on. The pin still holds for this process."""

    def _refuse_writes(self, monkeypatch):
        def refuse(*a, **k):
            raise OSError(errno.EACCES, "cannot write")

        monkeypatch.setattr(__util__, "atomic_write_bytes", refuse)

    def test_it_is_honoured_by_a_delete_of_a_fresh_listing(self, tmp_path, monkeypatch):
        path = _location(tmp_path, {})
        ep = _endpoint(path, skip_remote_lock=True)
        self._refuse_writes(monkeypatch)
        ep.set_lock(_listing(ep)[S1], "restore:s1", True)
        assert _file(path) == {}  # nothing was recorded
        fresh = _listing(ep)[S1]  # a copy that has never seen the pin

        result = ep.delete_snapshots([fresh])

        assert ep.deleted == []  # type: ignore[attr-defined]
        assert result.skipped_count == 1

    def test_its_release_lets_the_delete_go_ahead(self, tmp_path, monkeypatch):
        path = _location(tmp_path, {})
        ep = _endpoint(path, skip_remote_lock=True)
        with monkeypatch.context() as refused:
            self._refuse_writes(refused)
            ep.set_lock(_listing(ep)[S1], "restore:s1", True, parent=True)
            ep.set_lock(_listing(ep)[S1], "restore:s1", False, parent=True)

        result = ep.delete_snapshots([_listing(ep)[S1]])

        assert ep.deleted == [S1]  # type: ignore[attr-defined]
        assert result.deleted_count == 1

    def test_a_release_that_could_not_be_written_is_not_held_in_memory(
        self, tmp_path, monkeypatch
    ):
        """A release this process could not write is not a pin it holds: once
        the file no longer holds the pin, the delete goes ahead."""
        path = _location(tmp_path, {})
        ep = _endpoint(path, skip_remote_lock=True)
        snap = _listing(ep)[S1]
        ep.set_lock(snap, "restore:s1", True)  # recorded in the file
        with monkeypatch.context() as refused:
            self._refuse_writes(refused)
            ep.set_lock(snap, "restore:s1", False)  # the release is not recorded
        _endpoint(path)._update_lock_entry(S1, lambda entry: {})  # cleared by hand

        result = ep.delete_snapshots([_listing(ep)[S1]])

        assert ep.deleted == [S1]  # type: ignore[attr-defined]
        assert result.deleted_count == 1
