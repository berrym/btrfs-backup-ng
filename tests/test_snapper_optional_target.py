"""A snapper volume's optional target may be away without failing the run.

The README promises `optional = true` for `run`, `transfer` and `prune`
alike: a target that cannot be prepared is reported and skipped instead of
failing the run. The native path of `run` and both prune paths honoured it;
the snapper path of `run` wrapped the mount check, the endpoint open and the
transfer in one try, and every exception was a failure. A snapper volume
with a monthly drive marked optional was a red run whenever the drive was
away, which is the outcome the option exists to prevent.

Preparation is now apart from the transfer, as the native path has it. An
optional target whose preparation fails is warned about and skipped, in
neither counter; a transfer that fails after preparation is a failure on
both paths, optional or not.
"""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import run as run_cli

SNAPSHOTS = 2
AWAY = "/mnt/monthly"


def _target(path: str, optional: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        path=path,
        require_mount=path,
        optional=optional,
        compress=None,
        rate_limit=None,
        encrypt=None,
    )


def _volume(*targets: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(
        path="/home",
        snapper=SimpleNamespace(config_name="root"),
        targets=list(targets),
        is_snapper_source=lambda: True,
    )


@pytest.fixture
def rig(monkeypatch):
    """The snapper pipeline with the mount check real in shape: the target at
    AWAY is not mounted and the check refuses it; every other target is
    present and receives SNAPSHOTS."""

    def mounted(path, require_mount):
        if path == AWAY:
            raise __util__.AbortError(f"{path} is not mounted")

    monkeypatch.setattr(run_cli, "assert_target_mounted", mounted)
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
    monkeypatch.setattr(
        run_cli.endpoint,
        "choose_endpoint",
        lambda path, cfg: SimpleNamespace(path=path, prepare=lambda: None),
    )
    for check in ("assert_encryption_applied", "assert_compression_applied"):
        monkeypatch.setattr(run_cli.endpoint, check, lambda *a: None)
    return SimpleNamespace(
        global_config=SimpleNamespace(transfer_timeout=0), get_target_retention=None
    )


class TestAnOptionalTargetThatIsAwayIsSkipped:
    def test_the_run_succeeds_and_counts_only_the_present_target(self, rig, caplog):
        volume = _volume(_target("/mnt/always"), _target(AWAY, optional=True))

        with caplog.at_level(logging.WARNING, logger="btrfs_backup_ng.cli.run"):
            ok, stats, errors = run_cli._backup_snapper_volume(volume, rig)

        assert ok and errors == []
        assert stats["completed"] == 1 and stats["failed"] == 0
        assert stats["snapshots_transferred"] == SNAPSHOTS
        skips = [r for r in caplog.records if "Skipping optional target" in r.message]
        assert len(skips) == 1 and AWAY in skips[0].message
        assert skips[0].levelno == logging.WARNING, "said out loud, not at debug"
        assert not any("No transfers ran" in r.message for r in caplog.records), (
            "one target did run"
        )

    def test_the_same_target_without_optional_fails_the_run(self, rig, caplog):
        """The mutation guard for the skip: the option is what makes the
        difference, not the absence."""
        volume = _volume(_target("/mnt/always"), _target(AWAY))

        with caplog.at_level(logging.ERROR, logger="btrfs_backup_ng.cli.run"):
            ok, stats, errors = run_cli._backup_snapper_volume(volume, rig)

        assert not ok
        assert stats["completed"] == 1 and stats["failed"] == 1
        assert errors == [f"Destination endpoint {AWAY}: {AWAY} is not mounted"]
        assert any("Failed to prepare destination" in r.message for r in caplog.records)
        assert not any("Skipping optional" in r.message for r in caplog.records)

    def test_every_target_away_is_said_so_and_is_not_a_failure(self, rig, caplog):
        volume = _volume(_target(AWAY, optional=True))

        with caplog.at_level(logging.WARNING, logger="btrfs_backup_ng.cli.run"):
            ok, stats, errors = run_cli._backup_snapper_volume(volume, rig)

        assert ok and errors == []
        assert stats == {
            "completed": 0,
            "failed": 0,
            "snapshots_created": 0,
            "snapshots_transferred": 0,
        }
        assert any(
            "No transfers ran: all 1 target(s) were skipped as optional" in r.message
            for r in caplog.records
        )

    def test_a_present_optional_target_whose_transfer_fails_is_a_failure(
        self, rig, monkeypatch, caplog
    ):
        """`optional` covers preparation, as on the native path. A drive that
        is there and then fails to receive is a failed backup, whatever the
        target was declared."""
        failing = "/mnt/present-but-failing"

        def sync(scanner, config_name, destination_endpoint, **kwargs):
            if destination_endpoint.path == failing:
                raise __util__.AbortError("btrfs receive failed")
            return SNAPSHOTS

        monkeypatch.setattr(
            "btrfs_backup_ng.core.operations.sync_snapper_snapshots", sync
        )
        volume = _volume(_target("/mnt/always"), _target(failing, optional=True))

        with caplog.at_level(logging.WARNING, logger="btrfs_backup_ng.cli.run"):
            ok, stats, errors = run_cli._backup_snapper_volume(volume, rig)

        assert not ok
        assert stats["completed"] == 1 and stats["failed"] == 1
        assert errors == [f"Transfer to {failing}: btrfs receive failed"]
        assert not any("Skipping optional" in r.message for r in caplog.records)

    @pytest.mark.parametrize("optional", [True, False])
    def test_a_host_that_is_down_is_met_at_preparation(
        self, rig, monkeypatch, caplog, optional
    ):
        """An ssh destination opens its connection when it is prepared. The
        snapper loop never prepared its destination, so a host that was down
        failed inside the transfer, where `optional` could not reach it."""
        down = "ssh://backup@down.example/srv/backups"

        def choose(path, cfg):
            def prepare():
                if path == down:
                    raise ConnectionError(
                        f"Could not establish SSH connection to {path}"
                    )

            return SimpleNamespace(path=path, prepare=prepare)

        monkeypatch.setattr(run_cli.endpoint, "choose_endpoint", choose)
        volume = _volume(_target("/mnt/always"), _target(down, optional=optional))

        with caplog.at_level(logging.WARNING, logger="btrfs_backup_ng.cli.run"):
            ok, stats, errors = run_cli._backup_snapper_volume(volume, rig)

        assert stats["completed"] == 1 and stats["snapshots_transferred"] == SNAPSHOTS
        if optional:
            assert ok and errors == [] and stats["failed"] == 0
            assert any("Skipping optional target" in r.message for r in caplog.records)
        else:
            assert not ok and stats["failed"] == 1
            assert errors == [
                f"Destination endpoint {down}: Could not establish SSH connection to {down}"
            ]
            assert any(
                "Failed to prepare destination" in r.message for r in caplog.records
            )

    def test_a_skipped_target_is_not_pruned(self, rig, monkeypatch):
        """Retention runs only over targets whose transfer succeeded; a target
        that was never reached is not among them."""
        pruned: list = []
        monkeypatch.setattr(
            run_cli,
            "_prune_snapper_after_transfer",
            lambda volume, config, succeeded, errors: pruned.extend(succeeded) or True,
        )
        volume = _volume(_target("/mnt/always"), _target(AWAY, optional=True))

        ok, _stats, _errors = run_cli._backup_snapper_volume(volume, rig)

        assert ok
        assert [t.path for t, _cfg in pruned] == ["/mnt/always"]


class TestTheNativePathStillSkipsAnOptionalTarget:
    """The native path's skip moved onto the shared reporter; its verdict is
    unchanged: skipped targets are in neither counter and do not fail the
    volume, the present target is transferred."""

    def test_an_optional_target_whose_mount_check_fails_is_skipped(
        self, monkeypatch, caplog
    ):
        def mounted(path, require_mount):
            if path == AWAY:
                raise __util__.AbortError(f"{path} is not mounted")

        monkeypatch.setattr(run_cli, "assert_target_mounted", mounted)
        monkeypatch.setattr(run_cli, "thread_ssh_target_config", lambda *a: None)
        monkeypatch.setattr(run_cli, "thread_raw_encryption", lambda *a: None)
        monkeypatch.setattr(
            run_cli,
            "thread_raw_compression",
            lambda kwargs, *a, **k: kwargs.__setitem__("compress", "none"),
        )
        monkeypatch.setattr(
            run_cli.endpoint, "choose_endpoint", lambda *a, **k: MagicMock()
        )
        for check in ("assert_encryption_applied", "assert_compression_applied"):
            monkeypatch.setattr(run_cli.endpoint, check, lambda *a: None)
        monkeypatch.setattr(run_cli, "create_snapshot_dir", lambda *a, **k: Path("/s"))
        monkeypatch.setattr(run_cli, "_catch_up_selector", lambda *a, **k: None)
        monkeypatch.setattr(run_cli, "_prune_after_transfer", lambda *a, **k: True)
        monkeypatch.setattr(
            run_cli,
            "_transfer_to_target",
            lambda *a, **k: SimpleNamespace(transferred_count=SNAPSHOTS),
        )

        volume = SimpleNamespace(
            path="/home",
            snapshot_prefix="home-",
            snapshot_dir=".snapshots",
            targets=[_target("/mnt/always"), _target(AWAY, optional=True)],
            is_snapper_source=lambda: False,
        )
        config = SimpleNamespace(
            global_config=SimpleNamespace(
                transfer_timeout=0,
                transfer_stall_timeout=0,
                incremental=True,
                timestamp_format=None,
                snapshot_dir=".snapshots",
            ),
            get_target_retention=lambda *a: None,
        )
        with caplog.at_level(logging.WARNING, logger="btrfs_backup_ng.cli.run"):
            ok, stats, errors = run_cli._backup_volume(volume, config, 1)

        assert ok and errors == []
        assert stats["completed"] == 1 and stats["failed"] == 0
        assert any(
            "Skipping optional target" in r.message and AWAY in r.message
            for r in caplog.records
        )
