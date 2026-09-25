"""A receive that does not complete removes the partial it created.

An interrupted receive leaves a partial subvolume at the snapshot's own
name. Only transfer errors used to remove it; Ctrl-C, SIGTERM, SIGHUP and
unexpected exceptions left it, and every later run then refused to remove
something that was there before that run started -- so that snapshot failed
on every run, and the incremental chain behind it stopped, until someone
deleted it by hand.

The partial is removed only if THIS run created it (the authorship rule every
cleanup here follows), after the processes writing it have stopped, and --
for an ssh:// destination -- before the receive lock on that path is
released, so no other transfer can have started creating the path by then.
"""

from __future__ import annotations

import signal
import subprocess
import sys
import textwrap
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__, lifecycle
from btrfs_backup_ng.core import operations as ops


class TestUndoOnFailure:
    def test_it_runs_when_the_block_fails_and_not_when_it_completes(self):
        ran = []
        with lifecycle.undo_on_failure(lambda: ran.append("undo")):
            pass
        assert ran == []
        with pytest.raises(KeyboardInterrupt):
            with lifecycle.undo_on_failure(lambda: ran.append("undo")):
                raise KeyboardInterrupt
        assert ran == ["undo"]

    def test_a_failure_the_caller_handles_itself_is_left_to_it(self):
        ran = []
        with pytest.raises(ValueError):
            with lifecycle.undo_on_failure(
                lambda: ran.append("undo"), unless=(ValueError,)
            ):
                raise ValueError
        assert ran == []

    def test_it_is_owed_to_the_exit_drain_only_while_the_block_runs(self):
        before = set(lifecycle._CLEANUPS)
        with lifecycle.undo_on_failure(lambda: None):
            owed = [k for k in lifecycle._CLEANUPS if k not in before]
            assert len(owed) == 1
            assert lifecycle._CLEANUPS[owed[0]][0] == lifecycle.STAGE_PARTIALS
        assert set(lifecycle._CLEANUPS) == before

    def test_the_drain_runs_it_after_the_writers_and_before_the_locks(self):
        assert (
            lifecycle.STAGE_PARTIALS
            < lifecycle.STAGE_LOCKS
            < lifecycle.STAGE_CONNECTIONS
        )


def _defaults() -> None:
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, signal.SIG_DFL)


def test_a_fatal_signal_undoes_after_the_writer_and_before_the_lock(tmp_path):
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """
                import signal, subprocess
                from btrfs_backup_ng import lifecycle
                lifecycle.install_signal_handlers()
                order = []
                def note(what):
                    order.append(what)
                    open("order", "w").write(",".join(order))
                lifecycle.register("lock", lambda: note("lock"), lifecycle.STAGE_LOCKS)
                with lifecycle.undo_on_failure(
                    lambda: note(f"undo(writer alive={child.poll() is None})")
                ):
                    with lifecycle.process_scope():
                        child = lifecycle.track(subprocess.Popen(["sleep", "30"]))
                        signal.raise_signal(signal.SIGTERM)
                """
            ),
        ],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        preexec_fn=_defaults,
        timeout=60,
    )
    assert proc.returncode == -signal.SIGTERM, proc.stderr
    assert (tmp_path / "order").read_text() == "undo(writer alive=False),lock"


class TestTheTransferEngine:
    """Local and raw destinations: the partial is removed around the transfer."""

    @staticmethod
    def _drive(monkeypatch, tmp_path, failure, *, artifact_present=False):
        if artifact_present:
            (tmp_path / "s1").mkdir()
        src, dst = MagicMock(), MagicMock()
        dst.get_id.return_value = "d"
        dst._is_remote = False
        dst.config = {"path": str(tmp_path)}
        snap = MagicMock()
        snap.get_name.return_value = "s1"
        seen: list = []
        writer: list = []

        def transfer(*_a, **_k):
            writer.append(lifecycle.track(subprocess.Popen(["sleep", "30"])))
            raise failure

        monkeypatch.setattr(ops, "_send_snapshot", transfer)
        monkeypatch.setattr(
            ops,
            "_cleanup_partial_local_subvolume",
            lambda ep, name, *, created_by_this_run: seen.append(
                (name, created_by_this_run, writer[0].poll() is None)
            ),
        )
        try:
            ops._execute_transfers(src, dst, [(snap, None)], {})
        except BaseException as exc:  # noqa: BLE001 - returned for the assertion
            return seen, exc
        return seen, None

    def test_ctrl_c_removes_this_run_s_partial_after_its_writer_stopped(
        self, monkeypatch, tmp_path
    ):
        seen, raised = self._drive(monkeypatch, tmp_path, KeyboardInterrupt())
        assert isinstance(raised, KeyboardInterrupt)
        assert seen == [("s1", True, False)]

    def test_an_unexpected_error_removes_it_too(self, monkeypatch, tmp_path):
        seen, raised = self._drive(monkeypatch, tmp_path, OSError("pipe"))
        assert isinstance(raised, OSError)
        assert seen == [("s1", True, False)]

    def test_what_was_there_before_the_run_is_not_this_run_s(
        self, monkeypatch, tmp_path
    ):
        seen, _raised = self._drive(
            monkeypatch, tmp_path, KeyboardInterrupt(), artifact_present=True
        )
        assert seen == [("s1", False, False)]

    def test_a_transfer_error_is_cleaned_once(self, monkeypatch, tmp_path):
        """The transfer-error path already removes it; it must not be asked
        twice (a partial from before the run would be reported twice)."""
        seen, raised = self._drive(
            monkeypatch, tmp_path, __util__.SnapshotTransferError("boom")
        )
        assert raised is None
        assert seen == [("s1", True, False)]


class TestTheSshReceiveLock:
    """ssh:// undoes its receive inside the lock on the path it creates."""

    @staticmethod
    def _endpoint(tmp_path, *, preexisted):
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
        from btrfs_backup_ng.sshutil.lock import RemoteLockManager

        def run(script):
            p = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
            return p.returncode, p.stdout, p.stderr

        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"path": str(tmp_path)}
        manager = RemoteLockManager(run, str(tmp_path), hostname="h")
        endpoint._build_lock_manager = lambda: manager
        endpoint.artifact_exists = lambda d, n: preexisted
        order: list = []
        endpoint._cleanup_partial_subvolume = lambda d, n, *, created_by_this_run: (
            order.append(("remove", d, n, created_by_this_run))
        )
        real_release = manager.release
        manager.release = lambda name: (order.append(("release",)), real_release(name))
        return endpoint, order

    def test_an_interrupted_receive_is_undone_before_its_lock_goes(self, tmp_path):
        endpoint, order = self._endpoint(tmp_path, preexisted=False)
        writer: list = []

        def remove(d, n, *, created_by_this_run):
            order.append(
                ("remove", d, n, created_by_this_run, writer[0].poll() is None)
            )

        endpoint._cleanup_partial_subvolume = remove
        with pytest.raises(KeyboardInterrupt):
            with endpoint.receiving_lock(f"{tmp_path}/home.20240101T120000"):
                writer.append(lifecycle.track(subprocess.Popen(["sleep", "30"])))
                raise KeyboardInterrupt
        assert order == [
            ("remove", str(tmp_path), "home.20240101T120000", True, False),
            ("release",),
        ]

    def test_a_receive_that_completes_is_left_alone(self, tmp_path):
        endpoint, order = self._endpoint(tmp_path, preexisted=False)
        with endpoint.receiving_lock(f"{tmp_path}/home.20240101T120000"):
            pass
        assert order == [("release",)]

    def test_what_was_at_the_path_before_is_not_this_run_s(self, tmp_path):
        endpoint, order = self._endpoint(tmp_path, preexisted=True)
        with pytest.raises(RuntimeError):
            with endpoint.receiving_lock(f"{tmp_path}/home.20240101T120000"):
                raise RuntimeError
        assert order[0] == ("remove", str(tmp_path), "home.20240101T120000", False)


def test_the_real_ssh_cleaner_refuses_what_it_did_not_create(tmp_path):
    """The removal the lock runs is the endpoint's existing exact-path
    cleaner, with its authorship rule intact: nothing is even asked of the
    target for a path that was there before the run."""
    from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

    endpoint = SSHEndpoint.__new__(SSHEndpoint)
    endpoint.config = {"path": str(tmp_path)}
    endpoint.hostname = "h"
    endpoint._exec_remote_command = MagicMock()
    endpoint._exec_remote_command_with_retry = MagicMock()
    endpoint._cleanup_partial_subvolume(str(tmp_path), "x", created_by_this_run=False)
    endpoint._exec_remote_command.assert_not_called()
    endpoint._exec_remote_command_with_retry.assert_not_called()
