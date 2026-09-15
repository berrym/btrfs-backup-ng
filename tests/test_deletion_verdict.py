"""A deletion must report what it actually did.

Every delete path returned None, and none of them raise. A corrupt lock file
refused the whole batch; a locked snapshot was skipped; a chain-guarded raw
stream was refused; a busy target was passed over; a remote whose lock directory
was unusable deleted nothing; an already-absent stream was a no-op; and a plain
``btrfs subvolume delete`` failure was logged. The caller saw the same None for
all of it, so ``prune`` credited a deletion to every call that returned --

    Deleted 3 snapshot(s), kept 5

-- exited 0, and sent a success notification, having removed nothing. Measured on
this tree before the fix, for both the per-snapshot failure and the whole-batch
refusal.

The distinction these tests insist on is between a SKIP and a FAILURE. Refusing
to delete a snapshot a restore is reading, or one another stream still needs as
its incremental parent, is the guard working; it is reported and does not fail
the run. A deletion that was asked for and did not happen is a failure, and the
exit code has to say so.
"""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli.prune import execute_retention_deletes
from btrfs_backup_ng.endpoint import raw as raw_mod
from btrfs_backup_ng.endpoint.common import DeletionResult, Endpoint
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint


def _local(tmp_path, count=3):
    ep = LocalEndpoint(config={"path": str(tmp_path), "snap_prefix": "root."})
    snaps = [
        __util__.Snapshot(tmp_path, "root.", ep, time_obj=time.localtime(t))
        for t in (1700000000, 1700086400, 1700172800)[:count]
    ]
    return ep, snaps


def _ssh(**config):
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"ssh_sudo": False, "path": "/mnt/backup", **config}
    ep.hostname = "host"
    ep._normalize_path = MagicMock(side_effect=lambda p: str(p))
    ep._lock_target_path = MagicMock(return_value=None)
    ep._SSHEndpoint__cached_snapshots = None
    return ep


def _raw_snap(tmp_path, name="root.20240101T120000"):
    return RawSnapshot(name=name, stream_path=tmp_path / f"{name}.btrfs")


class TestTheBaseEndpoint:
    def test_a_failed_btrfs_delete_is_a_failure_not_a_deletion(self, tmp_path):
        ep, snaps = _local(tmp_path)
        with patch.object(
            LocalEndpoint,
            "_exec_command",
            side_effect=RuntimeError("Directory not empty"),
        ):
            result = ep.delete_snapshots(snaps)

        assert result.deleted_count == 0
        assert result.failed_count == 3
        assert not result.ok

    def test_an_unreadable_lock_file_fails_the_whole_batch(self, tmp_path):
        """Nothing is attempted, so nothing may be reported as deleted."""
        ep, snaps = _local(tmp_path)
        ep._locks_read_failed = True

        result = ep.delete_snapshots(snaps)

        assert result.deleted_count == 0
        assert result.failed_count == 3
        assert not result.ok
        assert all("unreadable" in str(reason) for _s, reason in result.failed)

    def test_a_retention_lock_is_a_skip_and_does_not_fail_the_run(self, tmp_path):
        ep, snaps = _local(tmp_path)
        snaps[0].locks.add("restore")
        with patch.object(LocalEndpoint, "_exec_command", return_value=None):
            result = ep.delete_snapshots(snaps)

        assert result.deleted_count == 2
        assert result.skipped_count == 1
        assert result.ok, "a lock doing its job is not a failure"

    def test_a_failed_delete_does_not_evict_the_snapshot_from_the_cache(self, tmp_path):
        """The eviction sat outside the try, so a snapshot whose deletion FAILED
        was dropped from the cache and every later list_snapshots() in the
        process reported it gone -- the listing agreeing with a deletion that did
        not happen."""
        ep, snaps = _local(tmp_path)
        ep._Endpoint__cached_snapshots = list(snaps)
        with patch.object(
            LocalEndpoint, "_exec_command", side_effect=RuntimeError("Device busy")
        ):
            ep.delete_snapshots(snaps)

        assert ep._Endpoint__cached_snapshots == snaps, (
            "a failed deletion removed the snapshot from the listing cache"
        )


class TestTheSSHEndpoint:
    def test_an_unusable_remote_lock_fails_the_whole_batch(self):
        from btrfs_backup_ng.sshutil.lock import RemoteLockUnavailable

        ep = _ssh()
        ep._lock_target_path = MagicMock(return_value="/mnt/backup")
        ep._lock_manager = MagicMock()
        snaps = [
            SimpleNamespace(locks=set(), parent_locks=set(), get_path=lambda: "/a")
        ]

        with patch(
            "btrfs_backup_ng.sshutil.lock.blocked_by_remote_lock",
            side_effect=RemoteLockUnavailable("lock dir not writable"),
        ):
            result = ep.delete_snapshots(snaps)

        assert result.deleted_count == 0
        assert result.failed_count == 1
        assert not result.ok

    @staticmethod
    def _with_remote(show_rc, delete_rc, stderr=b""):
        ep = _ssh()

        def fake_exec(cmd, **kwargs):
            cmd = [str(c) for c in cmd]
            if cmd[:3] == ["btrfs", "subvolume", "show"]:
                return SimpleNamespace(returncode=show_rc, stdout=b"", stderr=b"")
            return SimpleNamespace(returncode=delete_rc, stdout=b"", stderr=stderr)

        ep._exec_remote_command = MagicMock(side_effect=fake_exec)
        ep._exec_remote_command_with_retry = MagicMock(side_effect=fake_exec)
        return ep

    def test_a_deleted_remote_snapshot_is_reported_deleted(self):
        ep = self._with_remote(show_rc=0, delete_rc=0)
        snap = SimpleNamespace(
            locks=set(), parent_locks=set(), get_path=lambda: "/mnt/backup/s"
        )
        result = ep.delete_snapshots([snap])
        assert result.deleted_count == 1 and result.ok

    def test_a_path_that_is_not_a_subvolume_is_a_skip_not_a_deletion(self):
        ep = self._with_remote(show_rc=1, delete_rc=0)
        snap = SimpleNamespace(
            locks=set(), parent_locks=set(), get_path=lambda: "/mnt/backup/s"
        )
        result = ep.delete_snapshots([snap])
        assert result.deleted_count == 0
        assert result.skipped_count == 1

    def test_a_failed_remote_delete_is_a_failure(self):
        ep = self._with_remote(show_rc=0, delete_rc=1, stderr=b"Directory not empty")
        snap = SimpleNamespace(
            locks=set(), parent_locks=set(), get_path=lambda: "/mnt/backup/s"
        )
        result = ep.delete_snapshots([snap])
        assert result.deleted_count == 0
        assert result.failed_count == 1
        assert not result.ok

    def test_an_already_absent_remote_snapshot_is_a_skip(self):
        """The goal state is reached, so it is not a failure -- but it is not a
        deletion this run performed either."""
        ep = self._with_remote(
            show_rc=0, delete_rc=1, stderr=b"ERROR: No such file or directory"
        )
        snap = SimpleNamespace(
            locks=set(), parent_locks=set(), get_path=lambda: "/mnt/backup/s"
        )
        result = ep.delete_snapshots([snap])
        assert result.deleted_count == 0
        assert result.skipped_count == 1
        assert result.ok


class TestTheRawEndpoint:
    def test_a_deleted_stream_is_reported_deleted(self, tmp_path):
        ep = RawEndpoint(config={"path": str(tmp_path)})
        snap = _raw_snap(tmp_path)
        snap.stream_path.write_bytes(b"stream")

        result = ep.delete_snapshots([snap])

        assert result.deleted_count == 1 and result.ok
        assert not snap.stream_path.exists()

    def test_an_already_absent_stream_is_a_skip_and_is_logged(self, tmp_path, capsys):
        """Previously silent: a batch whose every stream was already gone produced
        no log line at all and was reported as a completed prune of that many.

        Read off stderr rather than caplog: this project's logger does not
        propagate to the root, so caplog.text is empty here whether the message
        is emitted or not -- a check that cannot fail.
        """
        ep = RawEndpoint(config={"path": str(tmp_path)})
        snap = _raw_snap(tmp_path)
        assert not snap.stream_path.exists()

        result = ep.delete_snapshots([snap])

        assert result.deleted_count == 0
        assert result.skipped_count == 1
        assert "already absent" in capsys.readouterr().err

    def test_a_chain_referenced_parent_is_a_skip(self, tmp_path):
        ep = RawEndpoint(config={"path": str(tmp_path)})
        parent = _raw_snap(tmp_path, "root.20240101T120000")
        parent.stream_path.write_bytes(b"parent")
        with patch.object(
            RawEndpoint, "_chain_referenced_parents", return_value={parent.get_name()}
        ):
            result = ep.delete_snapshots([parent])

        assert result.deleted_count == 0
        assert result.skipped_count == 1
        assert parent.stream_path.exists(), "the guard must actually keep the file"

    def test_a_busy_target_fails_the_whole_batch(self, tmp_path):
        ep = RawEndpoint(config={"path": str(tmp_path)})
        snap = _raw_snap(tmp_path)
        snap.stream_path.write_bytes(b"stream")
        with patch.object(
            RawEndpoint, "target_lock", side_effect=RuntimeError("target busy")
        ):
            result = ep.delete_snapshots([snap])

        assert result.deleted_count == 0
        assert result.failed_count == 1
        assert not result.ok
        assert snap.stream_path.exists()

    def test_an_unlink_failure_is_a_failure(self, tmp_path):
        ep = RawEndpoint(config={"path": str(tmp_path)})
        snap = _raw_snap(tmp_path)
        snap.stream_path.write_bytes(b"stream")
        with patch.object(Path, "unlink", side_effect=OSError("Read-only filesystem")):
            result = ep.delete_snapshots([snap])

        assert result.deleted_count == 0
        assert result.failed_count == 1
        assert not result.ok


class TestTheRawSSHEndpoint:
    @staticmethod
    def _endpoint(rc, stderr=b""):
        ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
        ep._lock_target_path = MagicMock(return_value=None)
        return ep

    def test_a_deleted_remote_stream_is_reported_deleted(self):
        ep = self._endpoint(0)
        snap = RawSnapshot(name="root.1", stream_path=Path("/backup/root.1.btrfs"))
        with patch.object(
            raw_mod.subprocess,
            "run",
            return_value=SimpleNamespace(returncode=0, stdout=b"", stderr=b""),
        ):
            result = ep._delete_snapshots_locked([snap])
        assert result.deleted_count == 1 and result.ok

    def test_a_failed_remote_rm_is_a_failure(self):
        import subprocess as _sp

        ep = self._endpoint(1)
        snap = RawSnapshot(name="root.1", stream_path=Path("/backup/root.1.btrfs"))
        with patch.object(
            raw_mod.subprocess,
            "run",
            side_effect=_sp.CalledProcessError(
                1, "rm", b"", b"rm: Read-only file system"
            ),
        ):
            result = ep._delete_snapshots_locked([snap])

        assert result.deleted_count == 0
        assert result.failed_count == 1
        assert not result.ok


class TestPruneReportsTheRealCount:
    def test_a_pass_that_deleted_nothing_does_not_report_deletions(self, tmp_path):
        ep, snaps = _local(tmp_path)
        with patch.object(
            LocalEndpoint,
            "_exec_command",
            side_effect=RuntimeError("Directory not empty"),
        ):
            deleted, errors = execute_retention_deletes(ep, snaps)

        assert deleted == 0, "prune reported deletions that did not happen"
        assert len(errors) == 3, "prune would exit 0 with no errors recorded"

    def test_skips_are_reported_without_failing_the_run(self, tmp_path):
        ep, snaps = _local(tmp_path)
        for snap in snaps:
            snap.locks.add("restore")
        with patch.object(LocalEndpoint, "_exec_command", return_value=None):
            deleted, errors = execute_retention_deletes(ep, snaps)

        assert deleted == 0
        assert errors == [], "a retention lock doing its job must not fail the prune"

    def test_a_partial_batch_reports_both_halves(self, tmp_path):
        ep, snaps = _local(tmp_path)
        calls = {"n": 0}

        def flaky(*a, **k):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("Device or resource busy")

        with patch.object(LocalEndpoint, "_exec_command", side_effect=flaky):
            deleted, errors = execute_retention_deletes(ep, snaps)

        assert deleted == 2 and len(errors) == 1


class TestNoImplementationMayReportNothing:
    """The risk in widening a return type is an override that does not follow.

    Every one of these methods used to return None; a single one left behind
    degrades silently back to the old behaviour for exactly one target scheme,
    which is the hardest kind of half-applied fix to notice.
    """

    IMPLEMENTATIONS = [
        (Endpoint, "delete_snapshots"),
        (Endpoint, "delete_snapshot"),
        (Endpoint, "delete_old_snapshots"),
        (SSHEndpoint, "delete_snapshots"),
        (SSHEndpoint, "delete_old_snapshots"),
        (RawEndpoint, "delete_snapshots"),
        (RawEndpoint, "delete_snapshot"),
        (RawEndpoint, "delete_old_snapshots"),
        (RawEndpoint, "_delete_snapshots_locked"),
        (SSHRawEndpoint, "_delete_snapshots_locked"),
    ]

    @pytest.mark.parametrize(
        ("cls", "name"),
        IMPLEMENTATIONS,
        ids=[f"{c.__name__}.{n}" for c, n in IMPLEMENTATIONS],
    )
    def test_the_annotation_promises_a_verdict(self, cls, name):
        func = cls.__dict__[name]
        annotation = getattr(func, "__annotations__", {}).get("return")
        assert annotation is not None, f"{cls.__name__}.{name} has no return annotation"
        assert "DeletionResult" in str(annotation), (
            f"{cls.__name__}.{name} returns {annotation!r}, not a DeletionResult"
        )

    @pytest.mark.parametrize(
        ("cls", "name"),
        IMPLEMENTATIONS,
        ids=[f"{c.__name__}.{n}" for c, n in IMPLEMENTATIONS],
    )
    def test_no_code_path_falls_off_the_end(self, cls, name):
        """An annotation is a promise; a bare `return` breaks it silently.

        mypy does catch this, but only for a `return` with no value -- and these
        methods are full of early exits that were bare returns until now.
        """
        import ast
        import inspect
        import textwrap

        tree = ast.parse(textwrap.dedent(inspect.getsource(cls.__dict__[name])))
        bare = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Return) and node.value is None
        ]
        assert not bare, (
            f"{cls.__name__}.{name} returns nothing at relative line(s) {bare}; "
            "an early exit that reports no verdict is the original defect"
        )


def test_the_result_type_separates_skips_from_failures():
    """The whole point of the type. Collapse the two and either a working guard
    fails the run, or a failed deletion passes it."""
    result = DeletionResult()
    result.deleted.append("a")
    result.skip("b", "held by a retention lock")

    assert result.ok and result.deleted_count == 1 and result.skipped_count == 1

    result.fail("c", "Directory not empty")
    assert not result.ok
    assert result.attempted == 3
