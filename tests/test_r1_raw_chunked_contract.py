"""Enforcement tests for the R1 transfer-success contract: raw + chunked paths.

Companion to test_r1_transfer_success_contract.py (the SSH point-of-truth layer).
These cover the non-SSH-direct transfer paths (commits A2a/A2b): the raw stream
pipeline must fail the whole pipeline when an upstream stage fails (pipefail), and
chunked transfers must not report success before `btrfs receive` confirms.
"""

from __future__ import annotations

import shlex
import shutil
import subprocess

import pytest

import btrfs_backup_ng.endpoint.raw as raw_mod
from btrfs_backup_ng.endpoint.raw import (
    RawEndpoint,
    SSHRawEndpoint,
    _popen_pipeline_pipefail,
)

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None, reason="bash required for pipefail behavior"
)


class TestRawPipelinePipefail:
    """A raw stream pipeline fails when ANY stage fails, not just the last.

    Without pipefail, `false | cat > file` exits 0 (the redirect succeeds),
    masking the upstream failure and writing a truncated/empty stream file that
    is then reported as a successful backup.
    """

    def test_pipefail_surfaces_upstream_failure(self, tmp_path):
        out = tmp_path / "out"
        proc = _popen_pipeline_pipefail(
            f"false | cat > {shlex.quote(str(out))}",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        proc.wait()
        assert proc.returncode != 0

    def test_successful_pipeline_returns_zero_and_writes(self, tmp_path):
        out = tmp_path / "out"
        proc = _popen_pipeline_pipefail(
            f"printf hello | cat > {shlex.quote(str(out))}",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        proc.wait()
        assert proc.returncode == 0
        assert out.read_bytes() == b"hello"


class TestExecutePipelineRoutesThroughPipefail:
    """Both multi-stage `_execute_pipeline` implementations use the pipefail helper.

    A multi-stage pipeline exists whenever compression and/or encryption is
    configured on a raw target -- the live, vulnerable case.
    """

    @staticmethod
    def _spy(monkeypatch):
        captured: dict = {}

        def spy(shell_cmd, **kwargs):
            captured["shell_cmd"] = shell_cmd
            allowed = {
                k: v for k, v in kwargs.items() if k in ("stdin", "stdout", "stderr")
            }
            return subprocess.Popen("true", shell=True, **allowed)

        monkeypatch.setattr(raw_mod, "_popen_pipeline_pipefail", spy)
        return captured

    def test_local_multistage_uses_pipefail_helper(self, monkeypatch, tmp_path):
        captured = self._spy(monkeypatch)
        ep = RawEndpoint.__new__(RawEndpoint)
        ep._pending_metadata = {  # type: ignore[attr-defined]
            "stream_path": tmp_path / "out",
            "part_path": tmp_path / "out.part",
        }
        proc = ep._execute_pipeline([["gzip"], ["cat"]], subprocess.DEVNULL)
        proc.wait()
        assert "gzip" in captured.get("shell_cmd", "")

    def test_ssh_multistage_uses_pipefail_helper(self, monkeypatch, tmp_path):
        captured = self._spy(monkeypatch)
        ep = SSHRawEndpoint.__new__(SSHRawEndpoint)
        ep._pending_metadata = {  # type: ignore[attr-defined]
            "stream_path": tmp_path / "out",
            "part_path": tmp_path / "out.part",
        }
        ep.ssh_sudo = False  # type: ignore[attr-defined]
        ep._build_ssh_command = lambda: ["ssh", "host"]  # type: ignore[method-assign]
        proc = ep._execute_pipeline([["gzip"], ["cat"]], subprocess.DEVNULL)
        proc.wait()
        assert "gzip" in captured.get("shell_cmd", "")
