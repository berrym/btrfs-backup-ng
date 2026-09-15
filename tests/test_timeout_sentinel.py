"""``0`` means "no limit" -- including where it reaches a stdlib timeout.

DEFAULT_TRANSFER_TIMEOUT is 0 and config/schema.py ships transfer_timeout = 0.
Every other consumer reads that as "unbounded": wait_with_progress spells it
``wall_timeout <= 0`` and substitutes a generous fallback. Popen.wait does not --
``timeout=0`` is a non-blocking poll that raises TimeoutExpired immediately.

wait_for_pipeline passed the sentinel straight through, so its handler SIGKILLed
the process and recorded -1, and core/operations.py scores any non-zero code as a
failed transfer. On the shipped default, a compress/throttle/progress stage that
had not already been reaped when the send finished was killed with no grace and a
transfer whose data was fully received was reported failed.

It survived because the compressor has usually drained by the time the send exits,
which is precisely why a passing test suite could not see it.
"""

from __future__ import annotations

import subprocess
import time

import pytest

from btrfs_backup_ng.core.transfer import (
    DEFAULT_TRANSFER_TIMEOUT,
    wait_for_pipeline,
)


class _SlowProc:
    """A process that needs a moment, like a compressor still draining."""

    def __init__(self, seconds=0.25, rc=0):
        self._ready_at = time.monotonic() + seconds
        self._rc = rc
        self.killed = False
        self.stderr = None

    def wait(self, timeout=None):
        remaining = self._ready_at - time.monotonic()
        if remaining > 0:
            if timeout is not None and timeout < remaining:
                raise subprocess.TimeoutExpired(cmd="slow", timeout=timeout)
            time.sleep(remaining)
        return self._rc

    def kill(self):
        self.killed = True


def test_the_shipped_default_is_the_no_limit_sentinel():
    """If this changes, the rest of this module is testing the wrong thing."""
    from btrfs_backup_ng.config.schema import GlobalConfig

    assert DEFAULT_TRANSFER_TIMEOUT == 0
    assert GlobalConfig().transfer_timeout == 0


def test_a_draining_stage_is_not_killed_on_the_default():
    proc = _SlowProc(seconds=0.25, rc=0)

    codes = wait_for_pipeline([("compress", proc)])

    assert codes == [0], f"a stage that exits 0 was scored {codes}"
    assert not proc.killed, "the stage was SIGKILLed while still draining"


@pytest.mark.parametrize("sentinel", [0, -1])
def test_every_non_positive_value_means_unlimited(sentinel):
    """Negative values reach Popen.wait as a negative timeout, which is also an
    immediate expiry rather than 'wait forever'."""
    proc = _SlowProc(seconds=0.2, rc=0)

    assert wait_for_pipeline([("compress", proc)], timeout=sentinel) == [0]
    assert not proc.killed


def test_a_real_limit_is_still_enforced():
    """The sentinel fix must not disable a deadline the operator asked for."""
    proc = _SlowProc(seconds=5, rc=0)

    codes = wait_for_pipeline([("compress", proc)], timeout=1)

    assert codes == [-1]
    assert proc.killed, "a stage past its configured wall limit was not killed"


def test_a_genuinely_failing_stage_still_reports_its_code():
    proc = _SlowProc(seconds=0, rc=3)
    assert wait_for_pipeline([("compress", proc)]) == [3]


def test_the_scoring_rule_this_protects():
    """Pins why -1 matters: operations.py fails a transfer on ANY non-zero code,
    so one wrongly-killed pipeline stage discards a fully received backup."""
    import ast
    import inspect

    from btrfs_backup_ng.core import operations as ops

    source = inspect.getsource(ops)
    assert "if any(rc != 0 for rc in return_codes):" in source
    ast.parse(source)
