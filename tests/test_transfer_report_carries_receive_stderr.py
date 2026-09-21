"""A failed receive's own words reach the transfer report.

Before this, a local restore from a corrupt stream reported "btrfs
send/receive failed with return codes: [-13, 1]" and nothing else: the base
send and receive sent stderr to DEVNULL, and the receive was started INSIDE
_do_process_transfer -- which returns only exit codes -- so even a pipe could
not have reached the report; _log_process_errors was handed None. With the
endpoints draining stderr into a tail and _do_process_transfer handing the
receive back through ``processes``, the report says why: on real btrfs,
"ERROR: crc32 mismatch in command".

These tests use real processes for the send and the receive and the real
engine functions between them.
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.core.operations as ops
from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import transfer as T


def _send(script: str) -> subprocess.Popen:
    p = subprocess.Popen(
        ["sh", "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    T.tail_stderr(p)
    return p


def _receiving_endpoint(script: str) -> MagicMock:
    """A destination whose receive() runs ``script`` with the stream on stdin,
    stderr drained into a tail exactly as the real endpoints do."""

    def receive(stdin, snapshot_name="", parent_name=None):
        p = subprocess.Popen(["sh", "-c", script], stdin=stdin, stderr=subprocess.PIPE)
        T.tail_stderr(p)
        return p

    d = MagicMock()
    d.receive.side_effect = receive
    d.config = {}
    d._is_remote = False
    return d


class TestDoProcessTransferHandsTheReceiveBack:
    def test_the_started_receive_and_its_words_are_available(self):
        started: dict = {}
        codes = ops._do_process_transfer(
            _send("echo stream"),
            _receiving_endpoint(
                "cat >/dev/null; echo 'ERROR: crc32 mismatch in command' >&2; exit 1"
            ),
            None,
            is_ssh_endpoint=False,
            compress="none",
            show_progress=False,
            processes=started,
        )
        assert 1 in codes
        assert "receive" in started
        assert "crc32 mismatch" in T.stderr_text(started["receive"])

    def test_without_the_out_parameter_nothing_changes(self):
        codes = ops._do_process_transfer(
            _send("echo stream"),
            _receiving_endpoint("cat >/dev/null; exit 0"),
            None,
            is_ssh_endpoint=False,
            compress="none",
            show_progress=False,
        )
        assert codes[-1] == 0


class TestTheReportSaysWhy:
    def test_log_process_errors_returns_the_tail(self):
        send = _send("echo 'At subvol x' >&2; exit 1")
        recv = subprocess.Popen(
            ["sh", "-c", "echo 'ERROR: unexpected header' >&2; exit 1"],
            stderr=subprocess.PIPE,
        )
        T.tail_stderr(recv)
        send.wait(timeout=10)
        recv.wait(timeout=10)
        send_err, recv_err = ops._log_process_errors(send, recv)
        assert "At subvol x" in send_err
        assert "unexpected header" in recv_err

    def test_send_snapshot_raises_with_the_receive_words(self, monkeypatch):
        """End to end through send_snapshot: the exception a restore catches
        carries the receive's diagnosis, so the user sees it."""
        snapshot = MagicMock()
        snapshot.get_name.return_value = "snap-1"
        snapshot.endpoint.send.side_effect = lambda *a, **k: _send("echo stream")
        snapshot.endpoint.config = {}
        dest = _receiving_endpoint(
            "cat >/dev/null; echo 'ERROR: crc32 mismatch in command' >&2; exit 1"
        )
        dest.config = {"path": "/nowhere"}
        monkeypatch.setattr(ops, "_ensure_destination_exists", lambda *a, **k: None)
        monkeypatch.setattr(
            ops,
            "_receiving_lock",
            lambda *a, **k: __import__("contextlib").nullcontext(),
        )
        with pytest.raises(__util__.SnapshotTransferError) as e:
            ops.send_snapshot(snapshot, dest, options={"compress": "none"})
        assert "crc32 mismatch" in str(e.value), str(e.value)
