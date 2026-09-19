"""The post-send tail must not be able to hang a run forever (issue #107).

Once the local send exits, no bytes move on this side, so the stall check is
deliberately disarmed: the remote is applying what it already received and
killing it there would destroy a transfer about to succeed. Both monitor loops
described that tail as "covered by the wall clock" -- but `transfer_timeout`
defaults to 0, which means NO wall clock. On a default configuration nothing
bounded the tail at all.
"""

import inspect

from btrfs_backup_ng.core.transfer import UNMEASURABLE_FALLBACK_TIMEOUT
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

FALLBACK = UNMEASURABLE_FALLBACK_TIMEOUT


def _endpoint():
    return SSHEndpoint.__new__(SSHEndpoint)


def test_a_wedged_tail_is_given_up_on_when_no_wall_clock_is_set():
    """transfer_timeout = 0 is the default, and used to mean 'wait forever'."""
    assert _endpoint()._unbounded_tail_expired(
        send_exit_time=0.0, max_wait_time=0, now=FALLBACK + 1
    )


def test_a_tail_within_the_fallback_is_left_alone():
    """A remote legitimately applying a large stream must not be cut off."""
    assert not _endpoint()._unbounded_tail_expired(
        send_exit_time=0.0, max_wait_time=0, now=FALLBACK - 1
    )


def test_a_send_still_running_is_not_a_tail():
    """Before the send exits the stall check governs; this must not fire."""
    assert not _endpoint()._unbounded_tail_expired(
        send_exit_time=None, max_wait_time=0, now=FALLBACK * 10
    )


def test_a_configured_wall_clock_keeps_precedence():
    """An operator who set transfer_timeout gets their deadline, not ours."""
    assert not _endpoint()._unbounded_tail_expired(
        send_exit_time=0.0, max_wait_time=60, now=FALLBACK * 10
    )


def test_both_monitor_loops_bound_their_tail():
    """The defect existed twice; a fix to one loop only is not a fix."""
    for func in (
        SSHEndpoint._monitor_transfer_progress,
        SSHEndpoint._simple_transfer_monitor,
    ):
        source = inspect.getsource(func)
        # The CALL, not the name: a comment mentioning the helper satisfies a
        # bare substring check while the bound itself is absent.
        assert "self._unbounded_tail_expired(" in source, (
            f"{func.__name__} does not bound its post-send tail, so a remote "
            "wedged mid-apply hangs the run forever"
        )


def test_the_error_names_the_limit_as_ours():
    """An operator chasing a hang must not be sent after an ssh timeout."""
    message = _endpoint()._tail_timeout_error(FALLBACK)
    assert "NOT an ssh timeout" in message
    assert "transfer_timeout" in message
