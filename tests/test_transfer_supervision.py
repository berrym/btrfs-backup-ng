"""Transfer supervision must never hang, and must terminate a stuck producer.

The send/receive supervisor waits on the RECEIVE (the sink) first, not the send. Waiting
on the send first blocked for the full timeout when the receive exited early (e.g. a
remote ``ssh btrfs send`` whose local ``btrfs receive`` failed with "subvolume already
exists" and never sent SIGPIPE) -- the hang that broke incremental top-up restores over
ssh. When the receive fails, the send's consumer is gone, so the send is TERMINATED
rather than waited on. This applies to every storage engine, not just ssh.
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import btrfs_backup_ng.core.operations as ops


def _send(returncode=0):
    p = MagicMock()
    p.stdout = MagicMock()
    p.wait.return_value = returncode
    p.returncode = returncode
    return p


def _receive(returncode):
    p = MagicMock()
    p.wait.return_value = returncode
    p.returncode = returncode
    return p


def _dest(recv):
    d = MagicMock()
    d.receive.return_value = recv
    d.config = {}
    return d


def test_failed_receive_terminates_send_without_waiting_on_it(monkeypatch):
    """When the receive fails, the send MUST be terminated, and the supervisor must NOT
    block on ``send.wait`` (which would hang on a producer whose consumer has exited).
    Mutation guard: reverting to ``send.wait`` first trips the send.wait sentinel."""
    send = _send()
    send.returncode = -15  # as if SIGTERM'd
    # A regression to send-first-wait would call this and blow up.
    send.wait.side_effect = AssertionError(
        "send.wait must not be called when the receive has already failed"
    )
    receive = _receive(1)  # receive FAILS fast

    terminated = []
    monkeypatch.setattr(ops, "_terminate_process", lambda p, **k: terminated.append(p))

    codes = ops._do_process_transfer(
        send,
        _dest(receive),
        None,
        is_ssh_endpoint=True,
        compress="none",
        show_progress=False,
    )
    assert send in terminated  # the stuck/pointless send was torn down
    assert 1 in codes  # the receive failure is surfaced (not masked as success)


def test_successful_transfer_returns_zero_codes(monkeypatch):
    """The happy path (receive ok, send ok within the grace) returns all-zero codes."""
    monkeypatch.setattr(ops, "_terminate_process", lambda *a, **k: None)
    codes = ops._do_process_transfer(
        _send(0),
        _dest(_receive(0)),
        None,
        is_ssh_endpoint=False,
        compress="none",
        show_progress=False,
    )
    assert codes and all(c == 0 for c in codes)


def test_lingering_send_after_ok_receive_is_terminated_and_fails(monkeypatch):
    """Receive SUCCEEDS but the send does not exit within the reap grace (a genuinely
    stuck producer -- the same hang class, displaced to the reap step). The supervisor
    must terminate the send and surface a NONZERO code (the -1 fallback), never block for
    the full timeout nor falsely report success. Mutation guard: dropping the reap
    try/except (block forever), no-oping the terminate, or returning 0 instead of -1 all
    fail this."""
    send = _send()
    send.wait.side_effect = subprocess.TimeoutExpired(cmd="send", timeout=30)
    send.returncode = None  # never exited on its own
    receive = _receive(0)  # receive SUCCEEDED (data is on disk)

    terminated = []
    monkeypatch.setattr(ops, "_terminate_process", lambda p, **k: terminated.append(p))

    codes = ops._do_process_transfer(
        send,
        _dest(receive),
        None,
        is_ssh_endpoint=True,
        compress="none",
        show_progress=False,
    )
    assert send in terminated  # the stuck send was torn down, not waited on forever
    assert any(
        c != 0 for c in codes
    )  # reported as FAILED (the -1 fallback), not success


def test_terminate_process_is_best_effort_on_a_finished_process():
    """_terminate_process must be safe/idempotent on an already-exited process."""
    p = MagicMock()
    p.wait.return_value = 0
    ops._terminate_process(p)  # must not raise
    p.terminate.assert_called()


def test_terminate_process_kills_when_terminate_times_out():
    """If SIGTERM does not stop it within the grace period, SIGKILL follows."""
    import subprocess

    p = MagicMock()
    p.wait.side_effect = [subprocess.TimeoutExpired(cmd="x", timeout=1), 0]
    ops._terminate_process(p, grace=0.01)
    p.terminate.assert_called()
    p.kill.assert_called()
