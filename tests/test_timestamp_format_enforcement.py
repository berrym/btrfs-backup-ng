"""Enforcement tests: every command threads the configured timestamp_format.

Each test exercises a real command code path and captures the configuration that
reaches the endpoint (or the retention/naming call), then asserts a DISTINCTIVE
CUSTOM format value -- ``%Y%m%dT%H%M%S`` -- is present rather than the built-in
default ``%Y%m%d-%H%M%S``.

Asserting the custom value specifically is what makes these tests ENFORCING: if a
future change stops threading ``timestamp_format`` (dropping the line, or reverting
a ``get_backup_name(fmt)`` call to ``get_backup_name()``), the captured value falls
back to the default and the test fails.
"""

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.config.schema import (
    Config,
    GlobalConfig,
    TargetConfig,
    VolumeConfig,
)

# Distinctive value that is NOT the built-in default "%Y%m%d-%H%M%S".
CUSTOM_FMT = "%Y%m%dT%H%M%S"
DEFAULT_FMT = "%Y%m%d-%H%M%S"


def _custom_config(volumes=None) -> Config:
    """Build a Config whose [global] timestamp_format is the custom value."""
    return Config(
        global_config=GlobalConfig(timestamp_format=CUSTOM_FMT),
        volumes=volumes or [],
    )


def _native_volume() -> VolumeConfig:
    """A native (non-snapper) volume with one local target."""
    return VolumeConfig(
        path="/data",
        snapshot_prefix="data-",
        snapshot_dir=".snapshots",
        targets=[TargetConfig(path="/mnt/backup")],
    )


# ---------------------------------------------------------------------------
# 1. verify
# ---------------------------------------------------------------------------


class TestVerifyThreadsTimestampFormat:
    """cli/verify.execute() puts timestamp_format into endpoint_kwargs."""

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_endpoint_kwargs_carry_custom_format(self, mock_choose, mock_verify):
        from btrfs_backup_ng.core.verify import VerifyLevel, VerifyReport

        mock_choose.return_value = MagicMock()
        report = VerifyReport(level=VerifyLevel.METADATA, location="/backup")
        report.completed_at = report.started_at
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=True,
            json=False,
            temp_dir=None,
            no_cleanup=False,
            timestamp_format=CUSTOM_FMT,
        )

        from btrfs_backup_ng.cli.verify import execute

        execute(args)

        endpoint_kwargs = mock_choose.call_args[0][1]
        assert endpoint_kwargs["timestamp_format"] == CUSTOM_FMT


# ---------------------------------------------------------------------------
# 2. restore: _prepare_backup_endpoint + main restore (local + backup)
# ---------------------------------------------------------------------------


class TestRestoreThreadsTimestampFormat:
    """cli/restore threads timestamp_format into backup and local endpoints."""

    @patch("btrfs_backup_ng.cli.restore.endpoint.choose_endpoint")
    def test_backup_endpoint_config_carries_custom_format(self, mock_choose, tmp_path):
        mock_choose.return_value = MagicMock()

        args = MagicMock()
        args.prefix = ""
        args.fs_checks = "auto"
        args.timestamp_format = CUSTOM_FMT

        from btrfs_backup_ng.cli.restore import _prepare_backup_endpoint

        _prepare_backup_endpoint(args, str(tmp_path))

        endpoint_kwargs = mock_choose.call_args[0][1]
        assert endpoint_kwargs["timestamp_format"] == CUSTOM_FMT

    @patch("btrfs_backup_ng.cli.restore.restore_snapshots")
    @patch("btrfs_backup_ng.cli.restore._prepare_local_endpoint")
    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    @patch("btrfs_backup_ng.cli.restore.validate_restore_destination")
    def test_local_endpoint_receives_custom_format(
        self, mock_validate, mock_backup, mock_local, mock_restore, tmp_path
    ):
        """The main restore path resolves the same custom format and passes it as
        the second positional arg to _prepare_local_endpoint."""
        mock_backup.return_value = MagicMock()
        mock_local.return_value = MagicMock()
        mock_restore.return_value = {"failed": 0}

        args = MagicMock()
        args.source = "/backup"
        args.destination = str(tmp_path / "dest")
        args.in_place = False
        args.yes_i_know_what_i_am_doing = False
        args.timestamp_format = CUSTOM_FMT
        args.before = None
        args.dry_run = True
        args.snapshot = None
        args.all = False
        args.overwrite = False
        args.no_incremental = False
        args.interactive = False
        args.compress = None
        args.rate_limit = None

        from btrfs_backup_ng.cli.restore import _execute_main_restore

        _execute_main_restore(args)

        # _prepare_local_endpoint(dest_path, timestamp_format)
        assert mock_local.call_args[0][1] == CUSTOM_FMT


# ---------------------------------------------------------------------------
# 3. estimate: _estimate_direct (BOTH source and dest)
# ---------------------------------------------------------------------------


class TestEstimateDirectThreadsTimestampFormat:
    """cli/estimate._estimate_direct threads it into source AND dest kwargs."""

    @patch("btrfs_backup_ng.cli.estimate.print_estimate")
    @patch("btrfs_backup_ng.cli.estimate.estimate_transfer")
    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    def test_both_endpoints_carry_custom_format(
        self, mock_endpoint, mock_estimate, mock_print, tmp_path
    ):
        mock_endpoint.choose_endpoint.return_value = MagicMock()
        mock_estimate.return_value = MagicMock()

        args = MagicMock()
        args.json = False
        args.prefix = ""
        args.ssh_sudo = False
        args.ssh_key = None
        args.no_fs_checks = False
        args.fs_checks = "auto"
        args.check_space = False
        args.timestamp_format = CUSTOM_FMT

        from btrfs_backup_ng.cli.estimate import _estimate_direct

        _estimate_direct(args, str(tmp_path / "source"), str(tmp_path / "dest"))

        calls = mock_endpoint.choose_endpoint.call_args_list
        assert len(calls) == 2
        source_kwargs = calls[0][0][1]
        dest_kwargs = calls[1][0][1]
        assert source_kwargs["timestamp_format"] == CUSTOM_FMT
        assert dest_kwargs["timestamp_format"] == CUSTOM_FMT


# ---------------------------------------------------------------------------
# 4. prune: endpoint kwargs AND apply_retention(timestamp_format=...)
# ---------------------------------------------------------------------------


class TestPruneThreadsTimestampFormat:
    """cli/prune threads it into endpoint kwargs and the apply_retention call."""

    @patch("btrfs_backup_ng.cli.prune.apply_retention")
    @patch("btrfs_backup_ng.cli.prune.endpoint.choose_endpoint")
    @patch("btrfs_backup_ng.cli.prune.load_config")
    @patch("btrfs_backup_ng.cli.prune.find_config_file")
    def test_endpoint_and_retention_carry_custom_format(
        self, mock_find, mock_load, mock_choose, mock_retention, tmp_path
    ):
        # Real snapshot dir so the source-prune branch runs.
        snap_dir = tmp_path / ".snapshots"
        snap_dir.mkdir()
        volume = VolumeConfig(
            path=str(tmp_path),
            snapshot_prefix="data-",
            snapshot_dir=".snapshots",
            targets=[TargetConfig(path="/mnt/backup")],
        )
        config = _custom_config([volume])

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (config, [])

        # An endpoint that reports one snapshot so apply_retention is invoked.
        ep = MagicMock()
        ep.list_snapshots.return_value = [MagicMock()]
        mock_choose.return_value = ep
        mock_retention.return_value = ([], [])

        args = MagicMock()
        args.config = None
        args.dry_run = True

        from btrfs_backup_ng.cli.prune import execute_prune

        execute_prune(args)

        # EVERY endpoint (source and target) carries the format.
        assert mock_choose.call_args_list  # sanity: at least one endpoint built
        for call in mock_choose.call_args_list:
            assert call[0][1]["timestamp_format"] == CUSTOM_FMT

        # EVERY apply_retention call (source and target) uses the configured format.
        assert mock_retention.call_args_list  # sanity: retention actually applied
        for call in mock_retention.call_args_list:
            assert call.kwargs["timestamp_format"] == CUSTOM_FMT


# ---------------------------------------------------------------------------
# 5. run: native volume endpoint kwargs AND snapper-volume endpoint config
# ---------------------------------------------------------------------------


class TestRunThreadsTimestampFormat:
    """cli/run threads it into native and snapper-volume endpoint configs."""

    @patch("btrfs_backup_ng.cli.run.endpoint.choose_endpoint")
    @patch("btrfs_backup_ng.cli.run.load_config")
    @patch("btrfs_backup_ng.cli.run.find_config_file")
    def test_native_volume_endpoint_carries_custom_format(
        self, mock_find, mock_load, mock_choose, tmp_path
    ):
        volume = VolumeConfig(
            path=str(tmp_path / "data"),
            snapshot_prefix="data-",
            snapshot_dir=".snapshots",
            targets=[],  # No targets: snapshot created, no transfer -> success.
        )
        config = _custom_config([volume])

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (config, [])

        source_ep = MagicMock()
        source_ep.snapshot.return_value = MagicMock()
        mock_choose.return_value = source_ep

        args = MagicMock()
        args.config = None
        args.dry_run = False
        args.parallel_volumes = 1
        args.parallel_targets = 1
        args.compress = None
        args.rate_limit = None
        args.progress = False
        args.no_progress = True
        args.quiet = True

        from btrfs_backup_ng.cli.run import execute_run

        execute_run(args)

        source_kwargs = mock_choose.call_args_list[0][0][1]
        assert source_kwargs["timestamp_format"] == CUSTOM_FMT

    @patch("btrfs_backup_ng.core.operations.sync_snapper_snapshots")
    @patch("btrfs_backup_ng.cli.run.endpoint.choose_endpoint")
    def test_snapper_volume_endpoint_config_carries_custom_format(
        self, mock_choose, mock_sync
    ):
        """_backup_snapper_volume builds the destination endpoint config with the
        configured timestamp_format."""
        from btrfs_backup_ng.snapper.scanner import SnapperConfig

        volume = VolumeConfig(path="/", snapshot_prefix="root-")
        # Make it a snapper source with one target.
        volume.source = "snapper"  # type: ignore[attr-defined]
        volume.snapper = MagicMock(config_name="root")  # type: ignore[attr-defined]
        volume.targets = [TargetConfig(path="/mnt/backup")]
        config = _custom_config([volume])

        mock_choose.return_value = MagicMock(_is_remote=False)
        mock_sync.return_value = 1

        with patch("btrfs_backup_ng.snapper.SnapperScanner") as mock_scanner_cls:
            scanner = MagicMock()
            scanner.find_config_for_path.return_value = SnapperConfig(
                name="root", subvolume=Path("/")
            )
            mock_scanner_cls.return_value = scanner

            from btrfs_backup_ng.cli.run import _backup_snapper_volume

            _backup_snapper_volume(volume, config)

        snapper_endpoint_config = mock_choose.call_args[0][1]
        assert snapper_endpoint_config["timestamp_format"] == CUSTOM_FMT


# ---------------------------------------------------------------------------
# 6. snapshot / list / status / transfer: endpoint kwargs
# ---------------------------------------------------------------------------


class TestConfigDrivenCommandsThreadTimestampFormat:
    """snapshot, list, status, transfer each build endpoint_kwargs with it."""

    @patch("btrfs_backup_ng.cli.snapshot.endpoint.choose_endpoint")
    @patch("btrfs_backup_ng.cli.snapshot.load_config")
    @patch("btrfs_backup_ng.cli.snapshot.find_config_file")
    def test_snapshot_endpoint_carries_custom_format(
        self, mock_find, mock_load, mock_choose, tmp_path
    ):
        volume = VolumeConfig(
            path=str(tmp_path / "data"),
            snapshot_prefix="data-",
            snapshot_dir=".snapshots",
        )
        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (_custom_config([volume]), [])

        ep = MagicMock()
        ep.snapshot.return_value = MagicMock()
        mock_choose.return_value = ep

        args = MagicMock()
        args.config = None
        args.volume = None
        args.dry_run = False

        from btrfs_backup_ng.cli.snapshot import execute_snapshot

        execute_snapshot(args)

        endpoint_kwargs = mock_choose.call_args_list[0][0][1]
        assert endpoint_kwargs["timestamp_format"] == CUSTOM_FMT

    @patch("btrfs_backup_ng.cli.list_cmd.endpoint.choose_endpoint")
    @patch("btrfs_backup_ng.cli.list_cmd.load_config")
    @patch("btrfs_backup_ng.cli.list_cmd.find_config_file")
    def test_list_endpoint_carries_custom_format(
        self, mock_find, mock_load, mock_choose, tmp_path
    ):
        # Real snapshot dir so the source-listing branch builds an endpoint.
        data = tmp_path / "data"
        (data / ".snapshots").mkdir(parents=True)
        volume = VolumeConfig(
            path=str(data),
            snapshot_prefix="data-",
            snapshot_dir=".snapshots",
        )
        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (_custom_config([volume]), [])

        ep = MagicMock()
        ep.list_snapshots.return_value = []
        mock_choose.return_value = ep

        args = MagicMock()
        args.config = None
        args.volume = None
        args.json = False

        from btrfs_backup_ng.cli.list_cmd import execute_list

        execute_list(args)

        endpoint_kwargs = mock_choose.call_args_list[0][0][1]
        assert endpoint_kwargs["timestamp_format"] == CUSTOM_FMT

    @patch("btrfs_backup_ng.cli.status.endpoint.choose_endpoint")
    @patch("btrfs_backup_ng.cli.status.load_config")
    @patch("btrfs_backup_ng.cli.status.find_config_file")
    def test_status_endpoint_carries_custom_format(
        self, mock_find, mock_load, mock_choose, tmp_path
    ):
        data = tmp_path / "data"
        (data / ".snapshots").mkdir(parents=True)
        volume = VolumeConfig(
            path=str(data),
            snapshot_prefix="data-",
            snapshot_dir=".snapshots",
        )
        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (_custom_config([volume]), [])

        ep = MagicMock()
        ep.list_snapshots.return_value = []
        mock_choose.return_value = ep

        args = MagicMock()
        args.config = None
        args.transactions = False

        from btrfs_backup_ng.cli.status import execute_status

        execute_status(args)

        endpoint_kwargs = mock_choose.call_args_list[0][0][1]
        assert endpoint_kwargs["timestamp_format"] == CUSTOM_FMT

    @patch("btrfs_backup_ng.cli.transfer.sync_snapshots")
    @patch("btrfs_backup_ng.cli.transfer.endpoint.choose_endpoint")
    @patch("btrfs_backup_ng.cli.transfer.load_config")
    @patch("btrfs_backup_ng.cli.transfer.find_config_file")
    def test_transfer_endpoint_carries_custom_format(
        self, mock_find, mock_load, mock_choose, mock_sync, tmp_path
    ):
        data = tmp_path / "data"
        (data / ".snapshots").mkdir(parents=True)
        volume = VolumeConfig(
            path=str(data),
            snapshot_prefix="data-",
            snapshot_dir=".snapshots",
            targets=[TargetConfig(path="/mnt/backup")],
        )
        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (_custom_config([volume]), [])

        ep = MagicMock()
        ep.list_snapshots.return_value = [MagicMock()]
        mock_choose.return_value = ep

        args = MagicMock()
        args.config = None
        args.volume = None
        args.dry_run = False
        args.compress = None
        args.rate_limit = None

        from btrfs_backup_ng.cli.transfer import execute_transfer

        execute_transfer(args)

        endpoint_kwargs = mock_choose.call_args_list[0][0][1]
        assert endpoint_kwargs["timestamp_format"] == CUSTOM_FMT


# ---------------------------------------------------------------------------
# 7. snapper_cmd: backup, status, list
# ---------------------------------------------------------------------------


class TestSnapperBackupThreadsTimestampFormat:
    """snapper_cmd._handle_backup builds endpoint config with the format."""

    @patch("btrfs_backup_ng.endpoint.choose_endpoint")
    @patch("btrfs_backup_ng.cli.snapper_cmd.SnapperScanner")
    def test_endpoint_config_carries_custom_format(
        self, mock_scanner_cls, mock_choose, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        scanner = MagicMock()
        scanner.get_config.return_value = MagicMock()
        mock_scanner_cls.return_value = scanner
        mock_choose.return_value = MagicMock(_is_remote=False)

        args = argparse.Namespace(
            config="root",
            target="/mnt/backup",
            snapshot=None,
            type=None,
            dry_run=True,
            compress=None,
            rate_limit=None,
            verbose=False,
            quiet=False,
            log_level=None,
            min_age="0",
            timestamp_format=CUSTOM_FMT,
        )

        with patch(
            "btrfs_backup_ng.core.operations.get_snapper_snapshots_for_backup",
            return_value=[],
        ):
            from btrfs_backup_ng.cli.snapper_cmd import _handle_backup

            _handle_backup(args)

        endpoint_config = mock_choose.call_args[0][1]
        assert endpoint_config["timestamp_format"] == CUSTOM_FMT


class TestSnapperStatusThreadsTimestampFormat:
    """snapper_cmd._handle_status threads it into the endpoint config AND uses
    get_backup_name(status_fmt) to compute local snapshot names."""

    @patch("btrfs_backup_ng.core.operations._list_snapper_backups_at_destination")
    @patch("btrfs_backup_ng.endpoint.choose_endpoint")
    @patch("btrfs_backup_ng.cli.snapper_cmd.SnapperScanner")
    def test_endpoint_config_and_backup_name_use_custom_format(
        self, mock_scanner_cls, mock_choose, mock_list
    ):
        snap = MagicMock()
        snap.get_backup_name.return_value = "root-1-20240101T120000"

        config = MagicMock()
        config.name = "root"

        scanner = MagicMock()
        scanner.list_configs.return_value = [config]
        scanner.get_snapshots.return_value = [snap]
        mock_scanner_cls.return_value = scanner
        mock_choose.return_value = MagicMock()
        mock_list.return_value = set()

        args = argparse.Namespace(
            json=False,
            config=None,
            target="/mnt/backup",
            timestamp_format=CUSTOM_FMT,
        )

        from btrfs_backup_ng.cli.snapper_cmd import _handle_status

        _handle_status(args)

        endpoint_config = mock_choose.call_args[0][1]
        assert endpoint_config["timestamp_format"] == CUSTOM_FMT
        # Local names are recomputed via get_backup_name(status_fmt).
        snap.get_backup_name.assert_called_with(CUSTOM_FMT)


class TestSnapperListThreadsTimestampFormat:
    """snapper_cmd._handle_list resolves the format for displayed backup names."""

    @patch("btrfs_backup_ng.cli.snapper_cmd.SnapperScanner")
    def test_backup_name_uses_custom_format(self, mock_scanner_cls):
        snap = MagicMock()
        snap.number = 1
        snap.snapshot_type = "single"
        snap.description = "timeline"
        snap.cleanup = "timeline"
        snap.pre_num = None
        snap.get_backup_name.return_value = "root-1-20240101T120000"
        from datetime import datetime

        snap.date = datetime(2024, 1, 1, 12, 0, 0)

        config = MagicMock()
        config.name = "root"
        config.subvolume = Path("/")

        scanner = MagicMock()
        scanner.list_configs.return_value = [config]
        scanner.get_snapshots.return_value = [snap]
        mock_scanner_cls.return_value = scanner

        args = argparse.Namespace(
            json=True,
            config=None,
            type=None,
            timestamp_format=CUSTOM_FMT,
        )

        from btrfs_backup_ng.cli.snapper_cmd import _handle_list

        _handle_list(args)

        # The resolved custom format is what get_backup_name is invoked with.
        snap.get_backup_name.assert_called_with(CUSTOM_FMT)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
