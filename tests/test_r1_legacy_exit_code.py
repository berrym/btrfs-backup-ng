"""Enforcement tests for the R1 transfer-success contract: legacy CLI (commit C).

The legacy path (`run_task` / `legacy_main`, reachable via the dispatcher) caught
per-destination transfer failures, logged them, and returned success -- so a
failed backup exited 0. With the orchestration now raising on failure, run_task
must record the failure and legacy_main must exit non-zero.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import btrfs_backup_ng._legacy_main as legacy
from btrfs_backup_ng import __util__


def _base_options():
    return {
        "source": "/data",
        "no_snapshot": True,
        "num_backups": 0,
        "no_incremental": False,
    }


def _patch_run_task_deps(monkeypatch):
    monkeypatch.setattr(legacy, "log_initial_settings", lambda o: None)
    monkeypatch.setattr(legacy, "prepare_source_endpoint", lambda o: MagicMock())
    monkeypatch.setattr(
        legacy, "prepare_destination_endpoints", lambda o, s: [MagicMock()]
    )
    monkeypatch.setattr(legacy, "cleanup_snapshots", lambda *a, **k: None)
    monkeypatch.setattr(legacy.time, "sleep", lambda *a, **k: None)


class TestRunTaskSignalsFailure:
    def test_returns_false_when_transfer_fails(self, monkeypatch):
        _patch_run_task_deps(monkeypatch)
        monkeypatch.setattr(
            legacy,
            "sync_snapshots",
            MagicMock(side_effect=__util__.SnapshotTransferError("boom")),
        )
        assert legacy.run_task(_base_options()) is False

    def test_returns_true_on_success(self, monkeypatch):
        _patch_run_task_deps(monkeypatch)
        monkeypatch.setattr(legacy, "sync_snapshots", MagicMock(return_value=None))
        assert legacy.run_task(_base_options()) is True


class TestLegacyMainExitCode:
    def _patch_parse(self, monkeypatch):
        monkeypatch.setattr(
            legacy, "parse_options", lambda gp, argv: {"verbosity": "INFO"}
        )
        monkeypatch.setattr(legacy, "create_logger", lambda *a, **k: None)

    def test_exit_1_when_a_task_fails(self, monkeypatch):
        self._patch_parse(monkeypatch)
        monkeypatch.setattr(legacy, "run_task", lambda o: False)
        assert legacy.legacy_main(["/data", "/backup"]) == 1

    def test_exit_0_when_all_tasks_succeed(self, monkeypatch):
        self._patch_parse(monkeypatch)
        monkeypatch.setattr(legacy, "run_task", lambda o: True)
        assert legacy.legacy_main(["/data", "/backup"]) == 0
