"""End-to-end CLI integration tests.

Tests the complete CLI flow from argument parsing through execution.
"""

import argparse
import sys
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from btrfs_backup_ng.cli.dispatcher import (
    create_subcommand_parser,
    is_legacy_mode,
    main,
    run_legacy_mode,
    run_subcommand,
    show_migration_notice,
)


class TestCLIArgumentParsing:
    """Test CLI argument parsing flows."""

    def test_parse_run_command(self):
        """Test parsing run command with options."""
        parser = create_subcommand_parser()
        args = parser.parse_args(
            ["run", "--dry-run", "--parallel-volumes", "2", "--compress", "zstd"]
        )

        assert args.command == "run"
        assert args.dry_run is True
        assert args.parallel_volumes == 2
        assert args.compress == "zstd"

    def test_parse_snapshot_command(self):
        """Test parsing snapshot command."""
        parser = create_subcommand_parser()
        args = parser.parse_args(["snapshot", "--volume", "/home", "--dry-run"])

        assert args.command == "snapshot"
        assert args.volume == ["/home"]
        assert args.dry_run is True

    def test_parse_transfer_command(self):
        """Test parsing transfer command."""
        parser = create_subcommand_parser()
        args = parser.parse_args(
            ["transfer", "--rate-limit", "10M", "--compress", "lz4"]
        )

        assert args.command == "transfer"
        assert args.rate_limit == "10M"
        assert args.compress == "lz4"

    def test_parse_prune_command(self):
        """Test parsing prune command."""
        parser = create_subcommand_parser()
        args = parser.parse_args(["prune", "--dry-run"])

        assert args.command == "prune"
        assert args.dry_run is True

    def test_parse_list_command(self):
        """Test parsing list command."""
        parser = create_subcommand_parser()
        args = parser.parse_args(["list", "--json", "--volume", "/home"])

        assert args.command == "list"
        assert args.json is True
        assert args.volume == ["/home"]

    def test_parse_status_command(self):
        """Test parsing status command."""
        parser = create_subcommand_parser()
        args = parser.parse_args(["status"])

        assert args.command == "status"

    def test_parse_config_validate(self):
        """Test parsing config validate subcommand."""
        parser = create_subcommand_parser()
        args = parser.parse_args(["config", "validate"])

        assert args.command == "config"
        assert args.config_action == "validate"

    def test_parse_config_init(self):
        """Test parsing config init subcommand."""
        parser = create_subcommand_parser()
        args = parser.parse_args(["config", "init", "-o", "config.toml"])

        assert args.command == "config"
        assert args.config_action == "init"
        assert args.output == "config.toml"

    def test_parse_config_import(self):
        """Test parsing config import subcommand."""
        parser = create_subcommand_parser()
        args = parser.parse_args(
            ["config", "import", "/etc/btrbk/btrbk.conf", "-o", "output.toml"]
        )

        assert args.command == "config"
        assert args.config_action == "import"
        assert args.btrbk_config == "/etc/btrbk/btrbk.conf"
        assert args.output == "output.toml"

    def test_parse_install_command(self):
        """Test parsing install command."""
        parser = create_subcommand_parser()
        args = parser.parse_args(["install", "--timer", "hourly", "--user"])

        assert args.command == "install"
        assert args.timer == "hourly"
        assert args.user is True

    def test_parse_install_oncalendar(self):
        """Test parsing install with custom oncalendar."""
        parser = create_subcommand_parser()
        args = parser.parse_args(["install", "--oncalendar", "*:0/15"])

        assert args.command == "install"
        assert args.oncalendar == "*:0/15"

    def test_parse_version_flag(self):
        """Test version flag parsing."""
        parser = create_subcommand_parser()
        args = parser.parse_args(["--version"])

        assert args.version is True

    def test_parse_config_file_option(self):
        """Test config file option parsing."""
        parser = create_subcommand_parser()
        args = parser.parse_args(["-c", "/etc/btrfs-backup-ng/config.toml", "run"])

        assert args.config == "/etc/btrfs-backup-ng/config.toml"
        assert args.command == "run"

    def test_parse_verbosity_options(self):
        """Test verbosity options."""
        parser = create_subcommand_parser()

        # Verbose flag (boolean)
        args = parser.parse_args(["-v", "run"])
        assert args.verbose is True

        # Debug flag
        args = parser.parse_args(["--debug", "run"])
        assert args.debug is True

        # Quiet
        args = parser.parse_args(["-q", "run"])
        assert args.quiet is True


class TestLegacyModeDetection:
    """Test legacy mode detection logic."""

    @pytest.mark.parametrize(
        "argv,expected",
        [
            # Legacy mode cases
            (["/home", "/backup"], True),
            (["./source", "/backup"], True),
            (["../parent/source", "/backup"], True),
            (["/mnt/data/.snapshots", "ssh://user@host:/backup"], True),
            # New mode cases
            (["run"], False),
            (["snapshot"], False),
            (["config", "validate"], False),
            (["-h"], False),
            (["--help"], False),
            (["-V"], False),
            (["--version"], False),
            ([], False),
            # Edge cases
            (["ssh://user@host:/path"], False),  # URL scheme, not path
            (["-v", "run"], False),
        ],
    )
    def test_legacy_mode_detection(self, argv, expected):
        """Test various argument patterns for legacy detection."""
        assert is_legacy_mode(argv) == expected


class TestMainEntryPoint:
    """Test main CLI entry point."""

    @patch("btrfs_backup_ng.cli.dispatcher.run_legacy_mode")
    def test_main_legacy_mode(self, mock_legacy):
        """Test main routes to legacy mode correctly."""
        mock_legacy.return_value = 0

        result = main(["/home", "/backup"])

        mock_legacy.assert_called_once_with(["/home", "/backup"])
        assert result == 0

    @patch("btrfs_backup_ng.cli.dispatcher.run_subcommand")
    def test_main_subcommand_mode(self, mock_subcommand):
        """Test main routes to subcommand mode."""
        mock_subcommand.return_value = 0

        result = main(["run"])

        mock_subcommand.assert_called_once()
        assert result == 0

    @patch("btrfs_backup_ng.cli.dispatcher.run_subcommand")
    def test_main_version_flag(self, mock_subcommand):
        """Test main handles version flag."""
        mock_subcommand.return_value = 0

        main(["--version"])

        mock_subcommand.assert_called_once()
        args = mock_subcommand.call_args[0][0]
        assert args.version is True


class TestSubcommandExecution:
    """Test subcommand execution flow."""

    @patch("btrfs_backup_ng.cli.run.execute_run")
    def test_run_command_execution(self, mock_execute):
        """Test run command is executed correctly."""
        mock_execute.return_value = 0

        parser = create_subcommand_parser()
        args = parser.parse_args(["run", "--dry-run"])

        result = run_subcommand(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.snapshot.execute_snapshot")
    def test_snapshot_command_execution(self, mock_execute):
        """Test snapshot command is executed correctly."""
        mock_execute.return_value = 0

        parser = create_subcommand_parser()
        args = parser.parse_args(["snapshot"])

        result = run_subcommand(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.transfer.execute_transfer")
    def test_transfer_command_execution(self, mock_execute):
        """Test transfer command is executed correctly."""
        mock_execute.return_value = 0

        parser = create_subcommand_parser()
        args = parser.parse_args(["transfer"])

        result = run_subcommand(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.prune.execute_prune")
    def test_prune_command_execution(self, mock_execute):
        """Test prune command is executed correctly."""
        mock_execute.return_value = 0

        parser = create_subcommand_parser()
        args = parser.parse_args(["prune"])

        result = run_subcommand(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.list_cmd.execute_list")
    def test_list_command_execution(self, mock_execute):
        """Test list command is executed correctly."""
        mock_execute.return_value = 0

        parser = create_subcommand_parser()
        args = parser.parse_args(["list"])

        result = run_subcommand(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.status.execute_status")
    def test_status_command_execution(self, mock_execute):
        """Test status command is executed correctly."""
        mock_execute.return_value = 0

        parser = create_subcommand_parser()
        args = parser.parse_args(["status"])

        result = run_subcommand(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.config_cmd.execute_config")
    def test_config_command_execution(self, mock_execute):
        """Test config command is executed correctly."""
        mock_execute.return_value = 0

        parser = create_subcommand_parser()
        args = parser.parse_args(["config", "validate"])

        result = run_subcommand(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.install.execute_install")
    def test_install_command_execution(self, mock_execute):
        """Test install command is executed correctly."""
        mock_execute.return_value = 0

        parser = create_subcommand_parser()
        args = parser.parse_args(["install", "--timer", "daily"])

        result = run_subcommand(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.install.execute_uninstall")
    def test_uninstall_command_execution(self, mock_execute):
        """Test uninstall command is executed correctly."""
        mock_execute.return_value = 0

        parser = create_subcommand_parser()
        args = parser.parse_args(["uninstall"])

        result = run_subcommand(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    def test_no_command_returns_error(self):
        """Test no command specified returns error."""
        parser = create_subcommand_parser()
        args = parser.parse_args([])

        with patch("builtins.print"):
            result = run_subcommand(args)

        assert result == 1

    def test_version_prints_and_exits(self):
        """Test version flag prints version and exits."""
        parser = create_subcommand_parser()
        args = parser.parse_args(["--version"])

        with patch("builtins.print") as mock_print:
            result = run_subcommand(args)

        assert result == 0
        mock_print.assert_called_once()
        assert "btrfs-backup-ng" in mock_print.call_args[0][0]


class TestLegacyModeExecution:
    """Test legacy mode execution flow."""

    @patch("btrfs_backup_ng._legacy_main.legacy_main")
    @patch("btrfs_backup_ng.cli.dispatcher.show_migration_notice")
    def test_legacy_mode_calls_legacy_main(self, mock_notice, mock_legacy):
        """Test legacy mode calls the legacy main function."""
        mock_legacy.return_value = 0

        result = run_legacy_mode(["/source", "/dest"])

        mock_notice.assert_called_once()
        mock_legacy.assert_called_once_with(["/source", "/dest"])
        assert result == 0

    @patch("btrfs_backup_ng._legacy_main.legacy_main")
    @patch("btrfs_backup_ng.cli.dispatcher.show_migration_notice")
    def test_legacy_mode_returns_exit_code(self, mock_notice, mock_legacy):
        """Test legacy mode returns correct exit code."""
        mock_legacy.return_value = 1

        result = run_legacy_mode(["/source", "/dest"])

        assert result == 1


class TestMigrationNotice:
    """Test migration notice display."""

    def test_notice_creates_marker_file(self, tmp_path):
        """Test migration notice creates marker file."""
        notice_file = tmp_path / ".migration-notice-shown"

        with patch.object(Path, "home", return_value=tmp_path):
            with patch("builtins.print"):
                # Create the config directory structure
                config_dir = tmp_path / ".config" / "btrfs-backup-ng"
                config_dir.mkdir(parents=True, exist_ok=True)

                show_migration_notice()

        # Marker should be created
        marker = tmp_path / ".config" / "btrfs-backup-ng" / ".migration-notice-shown"
        assert marker.exists()

    def test_notice_not_shown_twice(self, tmp_path):
        """Test migration notice is only shown once."""
        # Create marker file
        config_dir = tmp_path / ".config" / "btrfs-backup-ng"
        config_dir.mkdir(parents=True, exist_ok=True)
        marker = config_dir / ".migration-notice-shown"
        marker.touch()

        with patch.object(Path, "home", return_value=tmp_path):
            with patch("builtins.print") as mock_print:
                show_migration_notice()

        # Print should not be called if marker exists
        mock_print.assert_not_called()


class TestCLIErrorHandling:
    """Test CLI error handling scenarios."""

    def test_unknown_command(self):
        """Test unknown command returns error."""
        parser = create_subcommand_parser()
        args = argparse.Namespace(command="unknown_cmd", version=False)

        with patch("builtins.print"):
            result = run_subcommand(args)

        assert result == 1

    def test_invalid_compression_method(self):
        """Test invalid compression method is rejected."""
        parser = create_subcommand_parser()

        with pytest.raises(SystemExit):
            parser.parse_args(["run", "--compress", "invalid"])

    def test_invalid_timer_option(self):
        """Test invalid timer option is rejected."""
        parser = create_subcommand_parser()

        with pytest.raises(SystemExit):
            parser.parse_args(["install", "--timer", "minutely"])


class TestCLIWithConfig:
    """Test CLI commands with configuration files."""

    @patch("btrfs_backup_ng.cli.config_cmd.execute_config")
    def test_config_validate_with_file(self, mock_execute, tmp_path):
        """Test config validate with explicit config file."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            """
[[volumes]]
path = "/home"

[[volumes.targets]]
path = "/backup"
"""
        )

        mock_execute.return_value = 0

        parser = create_subcommand_parser()
        args = parser.parse_args(["-c", str(config_file), "config", "validate"])

        result = run_subcommand(args)

        assert result == 0
        assert args.config == str(config_file)


class TestCLIIntegrationScenarios:
    """Test complete CLI scenarios."""

    @patch("btrfs_backup_ng.cli.run.execute_run")
    def test_full_run_workflow(self, mock_run, tmp_path):
        """Test complete run command workflow."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            """
[global]
snapshot_dir = ".snapshots"
parallel_volumes = 2

[[volumes]]
path = "/home"

[[volumes.targets]]
path = "/backup/home"
"""
        )

        mock_run.return_value = 0

        result = main(
            [
                "-c",
                str(config_file),
                "run",
                "--dry-run",
                "--parallel-volumes",
                "4",
            ]
        )

        assert result == 0
        mock_run.assert_called_once()

        # Verify args passed to execute_run
        args = mock_run.call_args[0][0]
        assert args.dry_run is True
        assert args.parallel_volumes == 4
        assert args.config == str(config_file)

    @patch("btrfs_backup_ng.cli.prune.execute_prune")
    def test_prune_dry_run_workflow(self, mock_prune):
        """Test prune dry-run workflow."""
        mock_prune.return_value = 0

        result = main(["prune", "--dry-run"])

        assert result == 0
        args = mock_prune.call_args[0][0]
        assert args.dry_run is True

    @patch("btrfs_backup_ng.cli.list_cmd.execute_list")
    def test_list_json_output_workflow(self, mock_list):
        """Test list with JSON output workflow."""
        mock_list.return_value = 0

        result = main(["list", "--json", "--volume", "/home", "--volume", "/var"])

        assert result == 0
        args = mock_list.call_args[0][0]
        assert args.json is True
        assert args.volume == ["/home", "/var"]
