"""The tail bound must fire from inside the monitor loop, not just evaluate.

Hardware exposed the gap this closes. `_unbounded_tail_expired` was tested as a
predicate and passed, but on a real transfer the branch never ran: a small
payload finishes send and receive in the same iteration, so the loop breaks at
`all_finished` first. That is correct -- there is no tail to bound -- but it
means the predicate being right proved nothing about the loop calling it.

These drive the real loop with a send that has exited while the receive is
still running, which is exactly the shape of a remote wedged mid-apply.
"""

import time

import pytest

from btrfs_backup_ng.endpoint import ssh as ssh_mod
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint


class _Proc:
    """A process stand-in whose liveness the loop polls."""

    def __init__(self, alive, returncode=0):
        self._alive = alive
        self.returncode = returncode
        self.pid = 4242
        self.stderr = None
        self.stdout = None
        self.terminated = False

    def poll(self):
        return None if self._alive else self.returncode

    def terminate(self):
        self.terminated = True
        self._alive = False

    def kill(self):
        self.terminate()

    def wait(self, timeout=None):
        self._alive = False
        return self.returncode


@pytest.fixture
def endpoint(monkeypatch):
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"transfer_stall_timeout": 0}  # unmeasurable: no stall regime
    ep._last_transfer_error = None
    # A short fallback so the test does not wait a real day.
    monkeypatch.setattr(ssh_mod, "UNMEASURABLE_FALLBACK_TIMEOUT", 1)
    return ep


#: Both monitors are live: _try_direct_transfer picks between them on the
#: `simple_progress` config key, which defaults to True.
MONITORS = ["_simple_transfer_monitor", "_monitor_transfer_progress"]


def _run(endpoint, *, send_alive, receive_alive, started_ago, monitor=MONITORS[0]):
    processes = {
        "send": _Proc(alive=send_alive),
        "receive": _Proc(alive=receive_alive),
        "buffer": None,
        "compress": None,
    }
    result = getattr(endpoint, monitor)(
        processes=processes,
        start_time=time.time() - started_ago,
        dest_path="/backup/dest",
        snapshot_name="home.20260101",
        max_wait_time=0,  # the default: NO wall clock
        received_name="home.20260101",
    )
    return result, processes


@pytest.mark.parametrize("monitor", MONITORS)
def test_a_wedged_tail_is_terminated_by_the_loop(endpoint, monitor):
    """Send finished, receive still applying, no wall clock: must not hang."""
    started = time.monotonic()
    result, processes = _run(
        endpoint, send_alive=False, receive_alive=True, started_ago=0, monitor=monitor
    )
    elapsed = time.monotonic() - started

    assert result is False, "a wedged tail was reported as a successful transfer"
    assert elapsed < 30, f"the loop did not give up promptly ({elapsed:.0f}s)"
    assert processes["receive"].terminated, "the wedged receive was left running"
    assert "NOT an ssh timeout" in (endpoint._last_transfer_error or "")


@pytest.mark.parametrize("monitor", MONITORS)
def test_a_completed_transfer_does_not_trip_the_bound(endpoint, monitor):
    """Both finished: the loop must exit via all_finished, not via the tail."""
    result, processes = _run(
        endpoint, send_alive=False, receive_alive=False, started_ago=0, monitor=monitor
    )

    assert not processes["receive"].terminated, (
        "a completed transfer was terminated by the tail bound"
    )
    assert result is not False or endpoint._last_transfer_error is None
