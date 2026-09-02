"""A skip condition must not cost the suite minutes to decide.

`tests/test_raw_endpoint_integration.py` gated two tests on a `skipif` that
shelled out to `ssh localhost true`. It set `ConnectTimeout=1` but gave
`subprocess.run` no timeout, and `ConnectTimeout` only bounds the TCP connect --
an ssh that connects and then stalls is unbounded. Measured on a machine where
localhost ssh stalls: those two tests accounted for 230 seconds of a 400-second
run, spent deciding to skip.

Worse, a boolean `skipif` condition is evaluated when the module is IMPORTED, so
every pytest invocation paid it, including ones that deselected those tests
entirely. This is the same defect the tier3 conftest had; two instances make it a
class worth guarding.

The probe now lives in conftest, is cached, has a subprocess timeout, and is
reached through a FIXTURE so it runs at test setup rather than at import.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

TESTS = Path(__file__).parent


def _skipif_blocks(path: Path) -> list[str]:
    """Every skipif condition in a file, as source text."""
    source = path.read_text()
    return [
        m.group(1) for m in re.finditer(r"skipif\((.{0,800}?)\)\s*\n", source, re.S)
    ]


class TestNoSkipConditionCanHang:
    @pytest.mark.parametrize(
        "path", sorted(TESTS.rglob("test_*.py")), ids=lambda p: p.name
    )
    def test_a_subprocess_in_a_skip_condition_has_a_timeout(self, path):
        """A subprocess with no timeout inside a skipif blocks collection for as
        long as the command takes -- unbounded, on every run."""
        offenders = [
            block
            for block in _skipif_blocks(path)
            if "subprocess" in block and "timeout=" not in block
        ]
        assert not offenders, (
            f"{path.name} has a skipif that runs a subprocess with no timeout:\n"
            f"{offenders[0][:200]}\n"
            f"Use the cached, bounded probe in conftest (see requires_ssh_localhost)."
        )

    @pytest.mark.parametrize(
        "path", sorted(TESTS.rglob("test_*.py")), ids=lambda p: p.name
    )
    def test_no_skip_condition_reaches_the_network(self, path):
        """Network access in a skip condition runs at import, so it is paid even
        by runs that never select the test."""
        offenders = [
            block
            for block in _skipif_blocks(path)
            if re.search(r'"ssh"|localhost|socket\.|urlopen', block)
        ]
        assert not offenders, (
            f"{path.name} decides a skip by touching the network at import "
            f"time:\n{offenders[0][:200]}\nUse a fixture so it runs at setup."
        )


def _conftest():
    """The shared conftest, imported the way this repo already does cross-module
    test imports -- `conftest` is not importable by name on its own."""
    import sys

    sys.path.insert(0, str(TESTS))
    import conftest

    return conftest


class TestTheSharedProbeIsSafe:
    def test_it_is_bounded_and_cached(self):
        conftest = _conftest()

        assert hasattr(conftest.ssh_localhost_works, "cache_clear"), (
            "the probe is not cached, so every caller pays the ssh round trip"
        )
        # Read the whole function, not a fixed slice -- the first version of
        # this test took 900 characters and the docstring pushed `timeout=` past
        # the window, so it failed against correct code.
        import inspect

        block = inspect.getsource(conftest.ssh_localhost_works.__wrapped__)
        assert "timeout=" in block, "the probe can hang without a subprocess timeout"

    def test_a_missing_ssh_is_not_an_error(self, monkeypatch):
        """Anything that goes wrong means 'cannot use localhost ssh', not a
        crash during collection."""
        import subprocess

        conftest = _conftest()

        conftest.ssh_localhost_works.cache_clear()
        monkeypatch.setattr(
            subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError)
        )
        try:
            assert conftest.ssh_localhost_works() is False
        finally:
            conftest.ssh_localhost_works.cache_clear()

    def test_a_stalling_ssh_is_not_an_error(self, monkeypatch):
        import subprocess

        conftest = _conftest()

        conftest.ssh_localhost_works.cache_clear()

        def _timeout(*a, **k):
            raise subprocess.TimeoutExpired(cmd="ssh", timeout=8)

        monkeypatch.setattr(subprocess, "run", _timeout)
        try:
            assert conftest.ssh_localhost_works() is False
        finally:
            conftest.ssh_localhost_works.cache_clear()
