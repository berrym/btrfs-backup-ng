"""A snapper run's summary and notification count what the run did.

The native pipeline scores a target with two counters: ``completed`` counts
the target once when its transfer succeeded, and is what the notification
reports as "transfers completed"; ``snapshots_transferred`` adds the number of
snapshots that target received, and is what the summary line reports. The
snapper pipeline (``_backup_snapper_volume``) added the snapshot count to
``completed``, since snapper support arrived, and never touched
``snapshots_transferred``, since 0.9.7 gave the native path that counter and
switched the summary from counting targets to counting snapshots. A snapper
volume with one target and two snapshots therefore ended its run with

    INFO  Transferred 2 snapshot(s) to <target>
    INFO  All 1 volume(s) completed successfully; 0 snapshot(s) transferred

and a notification saying two transfers had completed.
"""

from __future__ import annotations

import argparse
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import run as run_cli

SNAPSHOTS = 2


def _target(path: str) -> SimpleNamespace:
    return SimpleNamespace(
        path=path,
        require_mount=False,
        compress=None,
        rate_limit=None,
        encrypt=None,
    )


def _snapper_volume(target_path: str) -> SimpleNamespace:
    return SimpleNamespace(
        path="/home",
        snapper=SimpleNamespace(config_name="root"),
        targets=[_target(target_path)],
        is_snapper_source=lambda: True,
    )


@pytest.fixture
def snapper_run(monkeypatch, tmp_path):
    """A run over one snapper volume whose single target receives SNAPSHOTS.

    Everything below the volume function is faked: the scanner, the transfer
    (which reports how many snapshots it delivered, as the real one does), the
    endpoint checks and the prune. Returns the volume and a config the run
    command accepts.
    """
    monkeypatch.setattr(
        "btrfs_backup_ng.core.operations.sync_snapper_snapshots",
        lambda *a, **k: SNAPSHOTS,
    )
    monkeypatch.setattr(
        "btrfs_backup_ng.snapper.SnapperScanner", lambda *a, **k: object()
    )
    monkeypatch.setattr(
        run_cli, "snapper_destination_options", lambda *a: {"compress": "none"}
    )
    monkeypatch.setattr(run_cli, "_snapper_catch_up_selector", lambda *a: None)
    monkeypatch.setattr(run_cli, "_prune_snapper_after_transfer", lambda *a: True)
    monkeypatch.setattr(run_cli.endpoint, "choose_endpoint", lambda *a: MagicMock())
    for check in ("assert_encryption_applied", "assert_compression_applied"):
        monkeypatch.setattr(run_cli.endpoint, check, lambda *a: None)

    volume = _snapper_volume(str(tmp_path))
    config = SimpleNamespace(
        global_config=SimpleNamespace(
            parallel_volumes=1,
            parallel_targets=1,
            transfer_timeout=0,
            btrfs_debug=False,
        ),
        get_enabled_volumes=lambda: [volume],
    )
    return volume, config


def _args() -> argparse.Namespace:
    return argparse.Namespace(
        parallel_volumes=1,
        parallel_targets=1,
        compress=None,
        rate_limit=None,
        newest_only=False,
        no_progress=True,
    )


class TestASnapperVolumeIsScoredLikeANativeOne:
    def test_the_target_counts_once_and_the_snapshots_by_number(self, snapper_run):
        volume, config = snapper_run

        ok, stats, errors = run_cli._backup_snapper_volume(volume, config)

        assert ok and not errors
        assert stats["completed"] == 1, "one target succeeded"
        assert stats["snapshots_transferred"] == SNAPSHOTS
        assert stats["failed"] == 0

    def test_the_summary_line_reports_the_snapshots(
        self, snapper_run, monkeypatch, caplog
    ):
        _volume, config = snapper_run
        monkeypatch.setattr(run_cli, "_send_backup_notifications", lambda *a, **k: None)
        with caplog.at_level(logging.INFO, logger="btrfs_backup_ng.cli.run"):
            exit_code = run_cli._run_configured_backups(_args(), config)

        assert exit_code == 0
        assert (
            f"All 1 volume(s) completed successfully; {SNAPSHOTS} snapshot(s) "
            "transferred"
        ) in caplog.text

    def test_the_notification_reports_the_target_not_the_snapshots(
        self, snapper_run, monkeypatch
    ):
        _volume, config = snapper_run
        sent: dict = {}
        monkeypatch.setattr(
            run_cli, "_send_backup_notifications", lambda *a, **k: sent.update(k)
        )

        assert run_cli._run_configured_backups(_args(), config) == 0

        assert sent["transfers_completed"] == 1, "one target, not two snapshots"
        assert sent["transfers_failed"] == 0
        assert sent["volumes_processed"] == 1 and sent["volumes_failed"] == 0
        assert sent["snapshots_created"] == 0, "snapper took the snapshots, not us"

    def test_each_target_scores_its_own_outcome(
        self, snapper_run, monkeypatch, tmp_path
    ):
        """Two targets receive the snapshots, a third fails before it does.

        A target is counted when, and only when, its transfer succeeded, and
        the snapshots are summed across targets: with one target, a counter
        assigned instead of added, or a target scored before its transfer ran,
        would read the same.
        """
        volume, config = snapper_run
        failing = str(tmp_path / "failing")
        volume.targets = [
            _target(str(tmp_path / "first")),
            _target(str(tmp_path / "second")),
            _target(failing),
        ]
        monkeypatch.setattr(
            run_cli.endpoint,
            "choose_endpoint",
            lambda path, cfg: SimpleNamespace(path=path, prepare=lambda: None),
        )

        def sync(scanner, config_name, destination_endpoint, **kwargs):
            if destination_endpoint.path == failing:
                raise __util__.AbortError("btrfs receive failed")
            return SNAPSHOTS

        monkeypatch.setattr(
            "btrfs_backup_ng.core.operations.sync_snapper_snapshots", sync
        )
        sent: dict = {}
        monkeypatch.setattr(
            run_cli, "_send_backup_notifications", lambda *a, **k: sent.update(k)
        )

        ok, stats, errors = run_cli._backup_snapper_volume(volume, config)

        assert not ok and len(errors) == 1 and failing in errors[0]
        assert stats["completed"] == 2
        assert stats["failed"] == 1
        assert stats["snapshots_transferred"] == 2 * SNAPSHOTS

        assert run_cli._run_configured_backups(_args(), config) == 1
        assert sent["transfers_completed"] == 2 and sent["transfers_failed"] == 1
