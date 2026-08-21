"""`restore --status` and `--unlock` must not invent a clean lock state.

Three separate ways this command claimed a target was unlocked without knowing:

1. It rebuilt the lock path itself as ``config["path"] / name`` instead of
   asking the endpoint. SSH endpoints keep ``path`` as a str, so that raised
   "unsupported operand type(s) for /: 'str' and 'str'", which --status caught,
   logged as a warning, and followed with "No active locks found." -- measured
   against a real host holding real backups.

2. ssh://, raw:// and raw+ssh:// endpoints overrode set_lock to mutate only the
   in-memory lock set; no lock was ever recorded for them. "No active locks
   found" was then true and useless, because it reads as "we looked and it is
   clean" when there was never anything to look at.

   ssh:// and raw+ssh:// now record locks ON THE TARGET (sshutil.lock), so for
   them the answer is real: --status reports what actually holds the target and
   --unlock clears it. Local raw:// still keeps its locks in memory only, and
   still says so. The invariant these tests defend is unchanged -- persists_locks
   must never claim more than set_lock actually does -- only the set of endpoints
   on each side of it has moved.

3. Endpoint._read_locks answered {} for any path that was not a regular file, so
   a directory or a symlink where the lock file belongs also reported zero locks.
   Retention trusts that answer and prunes on it.
"""

from __future__ import annotations

import argparse
import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import restore as restore_cli
from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.shell import ShellEndpoint
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

LOCK_NAME = ".btrfs-backup-ng.locks"


def _local_manager(target):
    """A RemoteLockManager whose "remote" is this machine.

    The manager only ever runs POSIX shell against the target, so pointing its
    runner at a local `sh` exercises the real acquire/release protocol -- the
    scripts, the atomic mkdir, the heartbeat -- instead of a mock of its output.
    A mock here would pass whatever the protocol did.
    """
    import subprocess

    from btrfs_backup_ng.sshutil.lock import RemoteLockManager

    def run(script):
        proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    return RemoteLockManager(run, str(target), hostname="test")


def _endpoint(cls, path):
    ep = cls.__new__(cls)
    ep.config = {"path": path, "lock_file_name": LOCK_NAME}
    return ep


def _args(source):
    return argparse.Namespace(source=str(source), fs_checks="skip", prefix="")


class TestTheCapabilityMatchesTheBehaviour:
    """persists_locks is only worth having if it cannot drift from set_lock."""

    @pytest.mark.parametrize(
        "cls",
        [
            Endpoint,
            LocalEndpoint,
            ShellEndpoint,
            SSHEndpoint,
            RawEndpoint,
            SSHRawEndpoint,
        ],
    )
    def test_the_flag_predicts_whether_a_lock_is_recorded(self, cls, tmp_path):
        """The flag must predict a DURABLE record, wherever that record lives.

        Local endpoints write a lock file next to the backups; ssh:// and
        raw+ssh:// write a lock directory on the remote target. Both are
        persistence, and the flag has to mean the same thing for both -- so the
        remote endpoints are driven through a lock manager backed by a real
        directory here, and the assertion is "something durable appeared",
        not "a local file appeared".
        """
        target = tmp_path / cls.__name__
        target.mkdir()
        ep = _endpoint(cls, target)
        snapshot = SimpleNamespace(
            locks=set(), parent_locks=set(), get_name=lambda: "snap-1"
        )

        if cls in (SSHEndpoint, SSHRawEndpoint):
            ep._lock_manager = lambda: _local_manager(target)  # type: ignore[method-assign]
            ep._lock_target_path = lambda: str(target)  # type: ignore[method-assign]

        ep.set_lock(snapshot, "restore:s1", True)

        recorded = (target / LOCK_NAME).is_file() or bool(
            list((target / LOCK_NAME).glob("*.lock"))
            if (target / LOCK_NAME).is_dir()
            else []
        )
        assert recorded == cls.persists_locks, (
            f"{cls.__name__}.persists_locks={cls.persists_locks} but set_lock "
            f"{'recorded' if recorded else 'did not record'} a durable lock"
        )

    def test_the_lock_is_held_in_memory_either_way(self, tmp_path):
        """Persisting or not, the run's own logic reads the in-memory set.

        Transfer and prune within a single run consult it directly rather than
        making a remote round trip per query, so it has to be maintained even
        where a durable record is also written.
        """
        for cls in (SSHEndpoint, RawEndpoint):  # one persists, one does not
            target = tmp_path / cls.__name__
            target.mkdir()
            snapshot = SimpleNamespace(
                locks=set(), parent_locks=set(), get_name=lambda: "snap-1"
            )
            endpoint = _endpoint(cls, target)
            if cls is SSHEndpoint:
                endpoint._lock_manager = lambda t=target: _local_manager(t)
            endpoint.set_lock(snapshot, "restore:s1", True)
            assert "restore:s1" in snapshot.locks


class TestATargetThatNeverPersistsLocks:
    """Only local raw:// is still in this category; ssh:// and raw+ssh:// left it."""

    @pytest.mark.parametrize("cls", [RawEndpoint])
    def test_status_says_so_instead_of_reporting_zero_locks(
        self, cls, tmp_path, capsys
    ):
        with patch.object(
            restore_cli,
            "_prepare_backup_endpoint",
            lambda a, s: _endpoint(cls, tmp_path),
        ):
            rc = restore_cli._execute_status(_args(tmp_path))
        out = capsys.readouterr().out
        assert rc == 0
        assert "does not persist locks" in out
        assert "No active locks found" not in out, "a false all-clear"

    @pytest.mark.parametrize("cls", [RawEndpoint])
    def test_unlock_says_so_instead_of_nothing_to_unlock(self, cls, tmp_path, capsys):
        with patch.object(
            restore_cli,
            "_prepare_backup_endpoint",
            lambda a, s: _endpoint(cls, tmp_path),
        ):
            rc = restore_cli._execute_unlock(_args(tmp_path), "all")
        out = capsys.readouterr().out
        assert rc == 0
        assert "does not persist locks" in out
        assert "No lock file found" not in out

    def test_a_remote_str_path_does_not_raise_a_typeerror(self, tmp_path, capsys):
        """The production shape: an SSH endpoint's config['path'] is a str, which
        is what the CLI's own `path / name` could not handle."""
        ep = _endpoint(SSHEndpoint, str(tmp_path))  # str, as SSH endpoints keep it
        ep._lock_manager = lambda: _local_manager(tmp_path)
        with patch.object(restore_cli, "_prepare_backup_endpoint", lambda a, s: ep):
            with patch.object(restore_cli, "list_remote_snapshots", lambda e: []):
                rc = restore_cli._execute_status(_args("ssh://nas:/backups/home"))
        out = capsys.readouterr().out
        assert rc == 0
        assert "unsupported operand" not in out


class TestATargetThatDoesPersistLocks:
    def _local(self, tmp_path):
        return _endpoint(LocalEndpoint, tmp_path)

    def test_an_absent_lock_file_genuinely_means_no_locks(self, tmp_path, capsys):
        with patch.object(
            restore_cli, "_prepare_backup_endpoint", lambda a, s: self._local(tmp_path)
        ):
            with patch.object(restore_cli, "list_remote_snapshots", lambda ep: []):
                rc = restore_cli._execute_status(_args(tmp_path))
        assert rc == 0
        assert "No active locks found" in capsys.readouterr().out

    def test_existing_locks_are_reported(self, tmp_path, capsys):
        (tmp_path / LOCK_NAME).write_text(
            __util__.write_locks({"snap-1": {"locks": ["restore:abc"]}})
        )
        with patch.object(
            restore_cli, "_prepare_backup_endpoint", lambda a, s: self._local(tmp_path)
        ):
            with patch.object(restore_cli, "list_remote_snapshots", lambda ep: []):
                rc = restore_cli._execute_status(_args(tmp_path))
        out = capsys.readouterr().out
        assert rc == 0
        assert "Active Locks" in out and "abc" in out

    def test_an_unreadable_lock_file_is_an_error_not_a_clean_report(
        self, tmp_path, capsys
    ):
        (tmp_path / LOCK_NAME).write_text("not json {{{")
        with patch.object(
            restore_cli, "_prepare_backup_endpoint", lambda a, s: self._local(tmp_path)
        ):
            rc = restore_cli._execute_status(_args(tmp_path))
        out = capsys.readouterr().out
        assert rc == 1
        assert "No active locks found" not in out
        assert "NOT a report of zero locks" in out

    def test_the_error_does_not_trail_off_after_a_colon(self, tmp_path, capsys):
        """AbortError stringifies to '', so the message must name the type."""
        (tmp_path / LOCK_NAME).write_text("not json {{{")
        with patch.object(
            restore_cli, "_prepare_backup_endpoint", lambda a, s: self._local(tmp_path)
        ):
            restore_cli._execute_status(_args(tmp_path))
        out = capsys.readouterr().out
        assert "could not be read ()" not in out
        assert "AbortError" in out

    def test_unlock_does_not_overwrite_locks_it_could_not_read(self, tmp_path, capsys):
        """Rewriting an unparseable lock file as {} destroys the very state that
        stops retention pruning a locked snapshot."""
        corrupt = "not json {{{"
        (tmp_path / LOCK_NAME).write_text(corrupt)
        with patch.object(
            restore_cli, "_prepare_backup_endpoint", lambda a, s: self._local(tmp_path)
        ):
            rc = restore_cli._execute_unlock(_args(tmp_path), "all")
        assert rc == 1
        assert (tmp_path / LOCK_NAME).read_text() == corrupt

    def test_unlock_removes_the_lock_and_leaves_others(self, tmp_path):
        (tmp_path / LOCK_NAME).write_text(
            __util__.write_locks({"snap-1": {"locks": ["restore:abc", "backup:xyz"]}})
        )
        with patch.object(
            restore_cli, "_prepare_backup_endpoint", lambda a, s: self._local(tmp_path)
        ):
            rc = restore_cli._execute_unlock(_args(tmp_path), "all")
        assert rc == 0
        remaining = __util__.read_locks((tmp_path / LOCK_NAME).read_text())
        assert remaining == {"snap-1": {"locks": ["backup:xyz"]}}


class TestReadingLocksDistinguishesAbsentFromUnreadable:
    def test_absent_is_no_locks(self, tmp_path):
        assert _endpoint(LocalEndpoint, tmp_path)._read_locks() == {}

    def test_a_directory_where_the_lock_file_belongs_raises(self, tmp_path):
        (tmp_path / LOCK_NAME).mkdir()
        with pytest.raises(__util__.AbortError):
            _endpoint(LocalEndpoint, tmp_path)._read_locks()

    def test_a_symlink_raises_because_the_writer_refuses_to_follow_one(self, tmp_path):
        """_write_locks goes through atomic_write_bytes, which opens O_NOFOLLOW.
        A reader that followed a symlink would return what the writer never
        wrote -- in a directory untrusted users may be able to write to."""
        real = tmp_path / "elsewhere"
        real.write_text(__util__.write_locks({}))
        os.symlink(real, tmp_path / LOCK_NAME)
        with pytest.raises(__util__.AbortError):
            _endpoint(LocalEndpoint, tmp_path)._read_locks()

    def test_a_regular_file_parses(self, tmp_path):
        (tmp_path / LOCK_NAME).write_text(
            __util__.write_locks({"snap-1": {"locks": ["restore:abc"]}})
        )
        assert _endpoint(LocalEndpoint, tmp_path)._read_locks() == {
            "snap-1": {"locks": ["restore:abc"]}
        }
