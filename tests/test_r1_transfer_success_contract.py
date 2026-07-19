"""Enforcement tests for the R1 transfer-success contract.

The contract (see the R1 design): a snapshot transfer succeeds *iff* every process
in the pipeline exited 0 AND a post-completion check confirms a well-formed
received subvolume. Existence is checked only AFTER the processes exit 0, never
during the transfer and never as a substitute for the exit code.

These tests are mutation-verified: reverting any one fix (re-adding the ``test -d``
fallback, downgrading a nonzero receive to a warning, restoring the mid-flight
existence short-circuit, or dropping ``pipefail``) makes the corresponding test
fail. They guard the SSH point-of-truth layer (commit A1).
"""

from __future__ import annotations

import io
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import btrfs_backup_ng.endpoint.ssh as ssh_mod
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint


def _bare_endpoint(config: dict | None = None) -> SSHEndpoint:
    """An SSHEndpoint with no SSH connection, for unit-testing pure logic."""
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"ssh_sudo": False, **(config or {})}
    return ep


def _proc(returncode: int | None) -> MagicMock:
    """A fake process. returncode None == still running (poll returns None)."""
    p = MagicMock()
    p.poll.return_value = returncode
    p.returncode = returncode
    p.stderr = None
    return p


def _proc_alive_then(returncode: int) -> MagicMock:
    """A fake process that reports alive (poll -> None) on the FIRST poll and
    finished (poll -> returncode) on every poll thereafter.

    Robust to the exact number of poll() calls the code makes (unlike a fixed
    side_effect list, which raises StopIteration if the call count shifts). The
    code only reads ``.returncode`` after a poll has reported the process
    finished, so a static returncode value is faithful at every read site.
    """
    p = MagicMock()
    state = {"polled": False}

    def poll():
        if not state["polled"]:
            state["polled"] = True
            return None
        return returncode

    p.poll.side_effect = poll
    p.returncode = returncode
    p.stderr = None
    return p


class TestVerifyRejectsPlainDirectory:
    """`_verify_snapshot_exists` requires a real subvolume, not just a directory.

    A failed/interrupted `btrfs receive` can leave a bare directory at the exact
    destination path. The removed `test -d` fallback would have accepted it as a
    valid backup. Only a successful `btrfs subvolume show` counts.
    """

    @staticmethod
    def _endpoint(subvolume_paths: set[str], dir_paths: set[str]) -> SSHEndpoint:
        ep = _bare_endpoint()

        def fake_exec(cmd, **kwargs):
            target = cmd[-1]
            if cmd[:3] == ["btrfs", "subvolume", "show"]:
                rc = 0 if target in subvolume_paths else 1
            elif cmd[:2] == ["test", "-d"]:
                rc = 0 if target in dir_paths else 1
            else:
                rc = 1
            return SimpleNamespace(returncode=rc, stdout=b"", stderr=b"")

        ep._exec_remote_command = MagicMock(side_effect=fake_exec)  # type: ignore[method-assign]
        ep._exec_remote_command_with_retry = MagicMock(side_effect=fake_exec)  # type: ignore[method-assign]
        return ep

    def test_real_subvolume_verifies(self):
        ep = self._endpoint(subvolume_paths={"/d/5/snapshot"}, dir_paths=set())
        assert ep._verify_snapshot_exists("/d/5", "snapshot") is True

    def test_plain_directory_is_not_accepted(self):
        # `btrfs subvolume show` fails (not a subvolume) but the directory exists.
        # The old `test -d` fallback reported success here; the contract rejects it.
        ep = self._endpoint(subvolume_paths=set(), dir_paths={"/d/5/snapshot"})
        assert ep._verify_snapshot_exists("/d/5", "snapshot") is False


class TestSimpleMonitorReceiveExitAuthoritative:
    """`_simple_transfer_monitor`: a nonzero `btrfs receive` exit is a failure."""

    def _run(self, send_rc, receive_rc, verify_result):
        ep = _bare_endpoint()
        ep._verify_snapshot_exists = MagicMock(return_value=verify_result)  # type: ignore[method-assign]
        ep._log_simple_process_error = MagicMock()  # type: ignore[method-assign]
        processes = {
            "send": _proc(send_rc),
            "receive": _proc(receive_rc),
            "buffer": None,
        }
        return ep._simple_transfer_monitor(
            processes=processes,
            start_time=time.time(),
            dest_path="/d/5",
            snapshot_name="snapshot",
            max_wait_time=3600,
            received_name="snapshot",
        )

    def test_nonzero_receive_fails_even_when_subvolume_exists(self):
        # Both processes finished; receive exited nonzero. Even though existence
        # verification would pass, the transfer MUST be reported as failed.
        assert self._run(send_rc=0, receive_rc=1, verify_result=True) is False

    def test_nonzero_send_fails_even_when_subvolume_exists(self):
        # The contract requires EVERY process to exit 0, not just receive.
        assert self._run(send_rc=1, receive_rc=0, verify_result=True) is False

    def test_success_requires_both_exit_zero_and_verification(self):
        # Guards against a false-negative: a genuinely good transfer still passes.
        assert self._run(send_rc=0, receive_rc=0, verify_result=True) is True

    def test_zero_exit_but_missing_subvolume_fails(self):
        assert self._run(send_rc=0, receive_rc=0, verify_result=False) is False

    def test_timeout_with_live_processes_fails(self):
        # start_time in the past forces the wait loop to exit immediately with the
        # processes still "alive" (poll() -> None): a timeout, which must fail
        # rather than fall through to an existence check.
        ep = _bare_endpoint()
        ep._verify_snapshot_exists = MagicMock(return_value=True)  # type: ignore[method-assign]
        processes = {"send": _proc(None), "receive": _proc(None), "buffer": None}
        result = ep._simple_transfer_monitor(
            processes=processes,
            start_time=time.time() - 10_000,
            dest_path="/d/5",
            snapshot_name="snapshot",
            max_wait_time=1,
            received_name="snapshot",
        )
        assert result is False


class TestEnhancedMonitorReceiveExitAuthoritative:
    """`_monitor_transfer_progress`: nonzero receive fails; no mid-flight success."""

    def _run(self, send, receive, verify_result, start_time=None):
        ep = _bare_endpoint()
        ep._verify_snapshot_exists = MagicMock(return_value=verify_result)  # type: ignore[method-assign]
        ep._log_process_error = MagicMock()  # type: ignore[method-assign]
        processes = {"send": send, "receive": receive, "buffer": None}
        return ep._monitor_transfer_progress(
            processes=processes,
            start_time=start_time if start_time is not None else time.time(),
            dest_path="/d/5",
            snapshot_name="snapshot",
            max_wait_time=3600,
            received_name="snapshot",
        )

    def test_nonzero_receive_fails_even_when_subvolume_exists(self):
        assert self._run(_proc(0), _proc(1), verify_result=True) is False

    def test_nonzero_send_fails_even_when_subvolume_exists(self):
        assert self._run(_proc(1), _proc(0), verify_result=True) is False

    def test_success_requires_both_exit_zero_and_verification(self):
        # Green-path guard against a false-negative in the enhanced monitor.
        assert self._run(_proc(0), _proc(0), verify_result=True) is True

    def test_zero_exit_but_missing_subvolume_fails(self):
        assert self._run(_proc(0), _proc(0), verify_result=False) is False

    def test_timeout_with_live_processes_fails(self):
        # Processes still running past max_wait_time: a timeout, which must fail
        # rather than fall through to an existence check.
        ep = _bare_endpoint()
        ep._verify_snapshot_exists = MagicMock(return_value=True)  # type: ignore[method-assign]
        ep._log_process_error = MagicMock()  # type: ignore[method-assign]
        processes = {"send": _proc(None), "receive": _proc(None), "buffer": None}
        result = ep._monitor_transfer_progress(
            processes=processes,
            start_time=time.time() - 10_000,
            dest_path="/d/5",
            snapshot_name="snapshot",
            max_wait_time=1,
            received_name="snapshot",
        )
        assert result is False

    def test_no_mid_flight_success_when_receive_later_fails(self):
        # Force the periodic-verification branch to fire on the first loop
        # iteration (start_time set 31s in the past so the 30s interval elapses),
        # while the subvolume already "exists". The receive process is alive on
        # that first iteration and then exits NONZERO. The old code returned True
        # from the periodic existence check (racing the still-running receive); the
        # contract requires waiting for the authoritative exit code -> failure.
        send = _proc(0)  # finished cleanly throughout
        receive = _proc_alive_then(1)  # alive on first poll, then exits nonzero
        result = self._run(
            send, receive, verify_result=True, start_time=time.time() - 31
        )
        assert result is False


class TestShellPipeline:
    """`_do_shell_pipeline_transfer`: pipefail + exit-code authority + safe cleanup.

    Without pipefail the shell reports only the last stage's (ssh/receive) exit
    code, masking a `btrfs send` or `pv` failure -> a truncated/empty backup
    reported as success. And because a zero pipeline exit means the receive
    succeeded, an inconclusive verification there must NOT delete the artifact.
    """

    @staticmethod
    def _run(monkeypatch, *, pipeline_rc, verify_result):
        ep = _bare_endpoint({"username": "u", "port": None, "ssh_sudo": True})
        ep.hostname = "host"
        ep.ssh_manager = SimpleNamespace(control_path="/tmp/cp")
        ep._estimate_snapshot_size = MagicMock(return_value=None)  # type: ignore[method-assign]
        ep._check_command_exists = MagicMock(return_value=False)  # type: ignore[method-assign]
        ep._verify_snapshot_exists = MagicMock(return_value=verify_result)  # type: ignore[method-assign]
        ep._cleanup_partial_subvolume = MagicMock()  # type: ignore[method-assign]

        captured: dict = {}

        class FakeStderr:
            def read(self, _n=-1):
                return b""

        def fake_popen(cmd, **kwargs):
            captured["cmd"] = cmd
            captured["executable"] = kwargs.get("executable")
            proc = MagicMock()
            proc.stderr = FakeStderr()
            proc.wait.return_value = pipeline_rc
            proc.returncode = pipeline_rc
            return proc

        monkeypatch.setattr(ssh_mod.shutil, "which", lambda name: "/bin/bash")
        monkeypatch.setattr(ssh_mod.subprocess, "Popen", fake_popen)

        ok = ep._do_shell_pipeline_transfer(
            source_path="/src/snapshot",
            dest_path="/d/5",
            snapshot_name="snapshot",
        )
        return ok, ep._cleanup_partial_subvolume, captured

    def test_pipeline_launched_with_pipefail_under_bash(self, monkeypatch):
        ok, _cleanup, captured = self._run(
            monkeypatch, pipeline_rc=0, verify_result=True
        )
        assert ok is True
        assert captured["cmd"].startswith("set -o pipefail;")
        assert captured["executable"] == "/bin/bash"

    def test_pipeline_failure_fails_and_cleans_up(self, monkeypatch):
        # Nonzero pipeline exit (a real send/pv/receive failure) -> failure, and
        # the partial artifact IS removed.
        ok, cleanup, _captured = self._run(
            monkeypatch, pipeline_rc=1, verify_result=True
        )
        assert ok is False
        cleanup.assert_called_once()

    def test_pipeline_success_but_inconclusive_verify_keeps_artifact(self, monkeypatch):
        # Pipeline exited 0 (receive succeeded) but verification failed: report
        # failure but do NOT delete -- the received subvolume may be a good backup
        # and only the verify step failed. This is the false-negative guard.
        ok, cleanup, _captured = self._run(
            monkeypatch, pipeline_rc=0, verify_result=False
        )
        assert ok is False
        cleanup.assert_not_called()


class TestCleanupPartialSubvolume:
    """Failed transfers remove their partial artifact at the exact path only."""

    def _endpoint(self):
        ep = _bare_endpoint()
        calls: list[list[str]] = []

        def fake_exec(cmd, **kwargs):
            calls.append(list(cmd))
            # `test -e` succeeds (something is present); delete succeeds.
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

        ep._exec_remote_command = MagicMock(side_effect=fake_exec)  # type: ignore[method-assign]
        ep._exec_remote_command_with_retry = MagicMock(side_effect=fake_exec)  # type: ignore[method-assign]
        return ep, calls

    def test_deletes_exact_path_when_present(self):
        ep, calls = self._endpoint()
        ep._cleanup_partial_subvolume("/d/5", "snapshot")
        delete_cmds = [c for c in calls if c[:3] == ["btrfs", "subvolume", "delete"]]
        assert delete_cmds, "expected a subvolume delete of the partial"
        assert delete_cmds[0][-1] == "/d/5/snapshot"

    def test_no_delete_when_nothing_present(self):
        ep = _bare_endpoint()
        calls: list[list[str]] = []

        def fake_exec(cmd, **kwargs):
            calls.append(list(cmd))
            # `test -e` fails -> nothing at the path -> no delete attempted.
            return SimpleNamespace(returncode=1, stdout=b"", stderr=b"")

        ep._exec_remote_command = MagicMock(side_effect=fake_exec)  # type: ignore[method-assign]
        ep._exec_remote_command_with_retry = MagicMock(side_effect=fake_exec)  # type: ignore[method-assign]
        ep._cleanup_partial_subvolume("/d/5", "snapshot")
        assert not [c for c in calls if c[:3] == ["btrfs", "subvolume", "delete"]]


class TestDirectTransferCleanupGating:
    """`_try_direct_transfer` deletes a partial ONLY when a process actually failed.

    This pins the false-negative guard: if the receive exited 0 but the monitor
    returned failure because verification was inconclusive (e.g. a transient
    timeout on `btrfs subvolume show`), the just-received subvolume must be LEFT
    IN PLACE — deleting it would destroy a successful backup. Cleanup runs only
    when the receive (or send) genuinely failed / was killed.
    """

    @staticmethod
    def _run(monkeypatch, *, monitor_result, send_rc, recv_rc):
        ep = _bare_endpoint({"path": "/d/5", "simple_progress": True})
        ep.hostname = "host"
        ep._run_diagnostics = MagicMock(  # type: ignore[method-assign]
            return_value={
                "ssh_connection": True,
                "btrfs_command": True,
                "write_permissions": True,
                "btrfs_filesystem": True,
                "passwordless_sudo": True,
            }
        )
        ep._find_buffer_program = MagicMock(return_value=(None, None))  # type: ignore[method-assign]
        send = _proc(send_rc)
        receive = _proc(recv_rc)
        ep._btrfs_receive = MagicMock(return_value=receive)  # type: ignore[method-assign]
        ep._simple_transfer_monitor = MagicMock(return_value=monitor_result)  # type: ignore[method-assign]
        ep._cleanup_partial_subvolume = MagicMock()  # type: ignore[method-assign]

        monkeypatch.setattr(ssh_mod.subprocess, "Popen", lambda *a, **k: send)
        monkeypatch.setattr(ssh_mod.os.path, "exists", lambda p: True)

        result = ep._try_direct_transfer(
            source_path="/src/snapshot",
            dest_path="/d/5",
            snapshot_name="snapshot",
        )
        return result, ep._cleanup_partial_subvolume

    def test_receive_failure_triggers_cleanup(self, monkeypatch):
        result, cleanup = self._run(
            monkeypatch, monitor_result=False, send_rc=0, recv_rc=1
        )
        assert result is False
        cleanup.assert_called_once()

    def test_inconclusive_verify_with_good_receive_keeps_backup(self, monkeypatch):
        # THE false-negative guard: receive exited 0, monitor returned failure
        # (verification inconclusive) -> the good subvolume must NOT be deleted.
        result, cleanup = self._run(
            monkeypatch, monitor_result=False, send_rc=0, recv_rc=0
        )
        assert result is False
        cleanup.assert_not_called()

    def test_success_returns_true_without_cleanup(self, monkeypatch):
        result, cleanup = self._run(
            monkeypatch, monitor_result=True, send_rc=0, recv_rc=0
        )
        assert result is True
        cleanup.assert_not_called()


class TestReceiveChunkedCleanupGating:
    """`receive_chunked` cleans a partial only when the receive genuinely failed.

    Same false-negative discipline as the direct SSH path: a nonzero receive is
    cleaned, but a receive that exited 0 followed by an inconclusive verification
    must NOT delete the received subvolume.
    """

    @staticmethod
    def _run(monkeypatch, *, return_code, verify_result):
        ep = _bare_endpoint({"path": "/d/5"})
        ep._normalize_path = lambda p: p  # type: ignore[method-assign]
        ep.ssh_manager = SimpleNamespace(  # type: ignore[attr-defined]
            get_ssh_base_cmd=lambda force_tty=False: ["ssh", "host"]
        )
        ep._verify_snapshot_exists = MagicMock(return_value=verify_result)  # type: ignore[method-assign]
        ep._cleanup_partial_subvolume = MagicMock()  # type: ignore[method-assign]

        recv = MagicMock()
        recv.stdin = MagicMock()
        recv.stderr = io.BytesIO(b"err")
        recv.poll.return_value = None  # alive during the streaming loop
        recv.wait.return_value = return_code
        recv.returncode = return_code
        monkeypatch.setattr(ssh_mod.subprocess, "Popen", lambda *a, **k: recv)

        manifest = SimpleNamespace(
            snapshot_name="snap",
            snapshot_path="/src/snapshot",
            chunk_count=1,
        )
        reader = MagicMock()
        reader.read_chunks.return_value = iter([b"data"])

        result = ep.receive_chunked(reader, manifest, show_progress=False, timeout=3600)
        return result, ep._cleanup_partial_subvolume

    def test_nonzero_receive_cleans_up(self, monkeypatch):
        result, cleanup = self._run(monkeypatch, return_code=1, verify_result=True)
        assert result is False
        cleanup.assert_called_once()

    def test_inconclusive_verify_after_good_receive_keeps_backup(self, monkeypatch):
        # receive exited 0 but verification failed -> inconclusive -> keep artifact.
        result, cleanup = self._run(monkeypatch, return_code=0, verify_result=False)
        assert result is False
        cleanup.assert_not_called()

    def test_success_returns_true_without_cleanup(self, monkeypatch):
        result, cleanup = self._run(monkeypatch, return_code=0, verify_result=True)
        assert result is True
        cleanup.assert_not_called()
