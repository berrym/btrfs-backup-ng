"""A transfer killed by our own limit must say so.

Reported by mjg in #93. Both monitors logged "Transfer timed out" and returned
without recording a reason anywhere the run summary or the transaction log would
find it. The operator saw a generic failure, so the obvious suspect was an ssh
idle or keepalive timeout -- which also commonly defaults to an hour. He went
through his sshd config before discovering the limit was ours.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

from btrfs_backup_ng.endpoint.ssh import SSHEndpoint


class TestATimeoutSaysWhoseItWas:
    def _endpoint(self):
        return SSHEndpoint(hostname="nas", path="/backup")

    def test_the_reason_names_the_limit_and_denies_being_ssh(self):
        reason = self._endpoint()._timeout_failure_reason(3600)
        assert "3600" in reason
        assert "btrfs-backup-ng" in reason
        # The whole point: it must rule out the thing he actually went hunting.
        assert "NOT an ssh" in reason
        assert "keepalive" in reason

    def test_the_reason_says_the_backup_is_incomplete(self):
        """Killed mid-stream is not a partial success."""
        assert "incomplete" in self._endpoint()._timeout_failure_reason(60)

    def _never_exits(self):
        proc = MagicMock()
        proc.poll.return_value = None  # still running, forever
        proc.returncode = None
        return proc

    def test_the_monitor_records_the_reason_not_just_a_log_line(self):
        """_last_transfer_error is the channel the run summary and transaction
        log read. The timeout path returned without setting it, so the reason
        existed only in a log line nobody correlates with the failure."""
        endpoint = self._endpoint()
        endpoint._last_transfer_error = None
        processes = {"send": self._never_exits(), "receive": self._never_exits()}

        result = endpoint._monitor_transfer_progress(
            processes,
            start_time=time.time(),
            dest_path="/backup",
            snapshot_name="snap",
            max_wait_time=0,  # already past the deadline
        )
        assert result is False
        assert endpoint._last_transfer_error, "the timeout recorded no reason"
        assert "NOT an ssh" in endpoint._last_transfer_error

    def test_the_simple_monitor_records_it_too(self):
        """Two monitors, the same silence -- fixing one would have left the other."""
        endpoint = self._endpoint()
        endpoint._last_transfer_error = None
        result = endpoint._simple_transfer_monitor(
            {"send": self._never_exits(), "receive": self._never_exits()},
            start_time=time.time(),
            dest_path="/backup",
            snapshot_name="snap",
            max_wait_time=0,
        )
        assert result is False
        assert endpoint._last_transfer_error, "the timeout recorded no reason"
        assert "NOT an ssh" in endpoint._last_transfer_error
