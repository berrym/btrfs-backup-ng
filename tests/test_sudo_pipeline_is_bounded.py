"""The ssh_sudo transfer path must not wait forever.

`_do_shell_pipeline_transfer` runs whenever ssh_sudo is set without passwordless
sudo. It ended in a bare `proc.wait()`: no stall check, no wall clock, no bound
of any kind, so a remote that wedged mid-apply hung the client indefinitely --
while the README promised a fallback limit would apply instead.
"""

import inspect
import subprocess
import sys
import time

import pytest

from btrfs_backup_ng.core.transfer import wait_with_progress
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint


def test_the_sudo_pipeline_wait_is_bounded():
    """Wiring guard: the bound was absent here for this path's whole existence."""
    source = inspect.getsource(SSHEndpoint._do_shell_pipeline_transfer)
    assert "wait_with_progress" in source, (
        "_do_shell_pipeline_transfer does not bound its wait, so a wedged "
        "remote hangs the client forever"
    )


def test_a_process_that_stops_moving_is_given_up_on():
    """The mechanism itself, against a real process that produces nothing."""
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(120)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        started = time.monotonic()
        with pytest.raises(subprocess.TimeoutExpired):
            wait_with_progress(
                proc,
                stall_pids=[proc.pid],
                stall_timeout=1,
                wall_timeout=2,
                poll_interval=0.1,
                description="stalled test process",
            )
        assert time.monotonic() - started < 30, "it did not give up promptly"
    finally:
        proc.kill()
        proc.wait()


def test_a_process_that_finishes_is_not_killed_by_the_bound():
    """A working transfer must not be ended by the guard that catches a dead one."""
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(0.3)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert (
            wait_with_progress(
                proc,
                stall_pids=[proc.pid],
                stall_timeout=1,
                wall_timeout=10,
                poll_interval=0.1,
                description="healthy test process",
            )
            == 0
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
