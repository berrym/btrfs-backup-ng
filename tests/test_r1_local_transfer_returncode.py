"""Enforcement: the local (non-chunked) standard transfer path must return the
ACTUAL send/receive exit codes, so a failed `btrfs receive` propagates upward.

If _do_process_transfer returned hardcoded success, send_snapshot's
`any(rc != 0)` check would never fire on the local path and a failed local
receive would be reported as a successful backup -- the same false-success class
R1 fixed for SSH. The previous tests only asserted receive() was called with the
right args and never checked the returned codes, so that regression was
invisible.
"""

from __future__ import annotations

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


class TestDoProcessTransferReturnCodes:
    def test_nonzero_receive_code_is_returned(self):
        codes = ops._do_process_transfer(
            _send(0),
            _dest(_receive(1)),
            None,
            is_ssh_endpoint=False,
            compress="none",
            show_progress=False,
        )
        assert 1 in codes, (
            f"a failed receive must surface its nonzero code, got {codes}"
        )

    def test_nonzero_send_code_is_returned(self):
        codes = ops._do_process_transfer(
            _send(2),
            _dest(_receive(0)),
            None,
            is_ssh_endpoint=False,
            compress="none",
            show_progress=False,
        )
        assert 2 in codes, f"a failed send must surface its nonzero code, got {codes}"

    def test_all_zero_on_success(self):
        codes = ops._do_process_transfer(
            _send(0),
            _dest(_receive(0)),
            None,
            is_ssh_endpoint=False,
            compress="none",
            show_progress=False,
        )
        assert codes and all(c == 0 for c in codes), codes
