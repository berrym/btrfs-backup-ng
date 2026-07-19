"""Enforcement tests for R2 — no cleanup / poisoned re-runs.

A failed/killed transfer must not leave a partial artifact that the next run's
skip-detection mistakes for a completed backup. Cleanup is scoped to the EXACT
artifact path and runs only on the failure path, so a good backup is never
deleted (the R1 false-negative guard, extended to the local/standard path).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import btrfs_backup_ng.core.operations as ops
from btrfs_backup_ng import __util__


def _fake_snap(name: str) -> MagicMock:
    s = MagicMock()
    s.locks = {}
    s.parent_locks = {}
    s.get_name.return_value = name
    return s


class TestCleanupPartialLocalSubvolume:
    def _local_endpoint(self, path):
        ep = MagicMock()
        ep._is_remote = False
        ep.config = {"path": str(path)}
        return ep

    def test_deletes_exact_local_path_when_present(self, monkeypatch, tmp_path):
        (tmp_path / "snap-1").mkdir()  # the partial artifact this failed run left
        ep = self._local_endpoint(tmp_path)
        snap = _fake_snap("snap-1")

        calls: list[list[str]] = []

        def fake_run(cmd, **kwargs):
            calls.append(list(cmd))
            return SimpleNamespace(returncode=0)

        monkeypatch.setattr(ops.subprocess, "run", fake_run)
        ops._cleanup_partial_local_subvolume(ep, snap)

        deletes = [c for c in calls if c[-3:-1] == ["subvolume", "delete"]]
        assert deletes, "expected a btrfs subvolume delete of the partial"
        assert deletes[0][-1] == str(tmp_path / "snap-1")

    def test_noop_when_nothing_present(self, monkeypatch, tmp_path):
        ep = self._local_endpoint(tmp_path)
        snap = _fake_snap("absent")
        called: list[int] = []
        monkeypatch.setattr(ops.subprocess, "run", lambda *a, **k: called.append(1))
        ops._cleanup_partial_local_subvolume(ep, snap)
        assert not called

    def test_skips_remote_endpoint(self, monkeypatch, tmp_path):
        (tmp_path / "snap-1").mkdir()
        ep = MagicMock()
        ep._is_remote = True  # SSH cleans its own partials during the transfer
        ep.config = {"path": str(tmp_path)}
        snap = _fake_snap("snap-1")
        called: list[int] = []
        monkeypatch.setattr(ops.subprocess, "run", lambda *a, **k: called.append(1))
        ops._cleanup_partial_local_subvolume(ep, snap)
        assert not called

    def test_skips_raw_endpoint(self, monkeypatch, tmp_path):
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        (tmp_path / "snap-1").mkdir()
        ep = RawEndpoint.__new__(RawEndpoint)
        ep._is_remote = False  # local raw: handled by the raw cleanup path, not here
        ep.config = {"path": str(tmp_path)}
        snap = _fake_snap("snap-1")
        called: list[int] = []
        monkeypatch.setattr(ops.subprocess, "run", lambda *a, **k: called.append(1))
        ops._cleanup_partial_local_subvolume(ep, snap)
        assert not called


class TestExecuteTransfersCleansPartialOnFailure:
    def test_failure_triggers_local_cleanup(self, monkeypatch):
        snap = _fake_snap("s1")
        src = MagicMock()
        dst = MagicMock()
        dst.get_id.return_value = "d"
        dst._is_remote = False

        monkeypatch.setattr(
            ops,
            "send_snapshot",
            MagicMock(side_effect=__util__.SnapshotTransferError("boom")),
        )
        spy = MagicMock()
        monkeypatch.setattr(ops, "_cleanup_partial_local_subvolume", spy)

        ops._execute_transfers(src, dst, [snap], [], [snap], True, {})

        spy.assert_called_once()
        # cleanup targets the exact snapshot that failed
        assert spy.call_args[0][1] is snap

    def test_success_does_not_trigger_cleanup(self, monkeypatch):
        snap = _fake_snap("s1")
        src = MagicMock()
        dst = MagicMock()
        dst.get_id.return_value = "d"
        dst._is_remote = False

        monkeypatch.setattr(ops, "send_snapshot", MagicMock(return_value=None))
        spy = MagicMock()
        monkeypatch.setattr(ops, "_cleanup_partial_local_subvolume", spy)

        ops._execute_transfers(src, dst, [snap], [], [snap], True, {})
        spy.assert_not_called()
