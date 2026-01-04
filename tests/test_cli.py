"""Tests for CLI dispatcher and commands."""

import argparse
from unittest.mock import patch

import pytest

from btrfs_backup_ng.cli.dispatcher import (
    SUBCOMMANDS,
    create_subcommand_parser,
    is_legacy_mode,
)


class TestIsLegacyMode:
    """Tests for is_legacy_mode function."""

    def test_empty_argv_not_legacy(self):
        """Test that empty argv is not legacy mode."""
        assert is_legacy_mode([]) is False

    def test_subcommand_not_legacy(self):
        """Test that known subcommands are not legacy mode."""
        for cmd in [
            "run",
            "snapshot",
            "transfer",
            "prune",
            "list",
            "status",
            "config",
            "install",
        ]:
            assert is_legacy_mode([cmd]) is False, f"{cmd} should not be legacy"

    def test_absolute_path_is_legacy(self):
        """Test that absolute paths trigger legacy mode."""
        assert is_legacy_mode(["/home/user"]) is True
        assert is_legacy_mode(["/mnt/backup"]) is True
        assert is_legacy_mode(["/"]) is True

    def test_relative_path_is_legacy(self):
        """Test that relative paths trigger legacy mode."""
        assert is_legacy_mode(["./source"]) is True
        assert is_legacy_mode(["../backup"]) is True

    def test_path_with_slash_is_legacy(self):
        """Test that paths with slashes are legacy mode."""
        assert is_legacy_mode(["source/subdir"]) is True

    def test_ssh_url_not_legacy(self):
        """Test that SSH URLs are not legacy mode (they're handled as arguments)."""
        # When first arg is ssh://, it's not a source path for legacy mode
        # But actually ssh:// as first arg would be unusual - typically source comes first
        # The function checks for :// to exclude URLs
        assert is_legacy_mode(["ssh://user@host:/path"]) is False

    def test_help_flags_not_legacy(self):
        """Test that help flags are not legacy mode."""
        assert is_legacy_mode(["-h"]) is False
        assert is_legacy_mode(["--help"]) is False
        assert is_legacy_mode(["-V"]) is False
        assert is_legacy_mode(["--version"]) is False

    def test_option_flags_not_legacy(self):
        """Test that option flags are not legacy mode."""
        assert is_legacy_mode(["-v"]) is False
        assert is_legacy_mode(["--verbose"]) is False
        assert is_legacy_mode(["-c", "config.toml"]) is False


class TestSubcommands:
    """Tests for SUBCOMMANDS constant."""

    def test_expected_subcommands(self):
        """Test that expected subcommands exist."""
        expected = {
            "run",
            "snapshot",
            "transfer",
            "prune",
            "list",
            "status",
            "config",
            "install",
            "uninstall",
        }
        for cmd in expected:
            assert cmd in SUBCOMMANDS, f"{cmd} not in SUBCOMMANDS"

    def test_subcommands_is_frozenset(self):
        """Test that SUBCOMMANDS is immutable."""
        assert isinstance(SUBCOMMANDS, frozenset)


class TestCreateSubcommandParser:
    """Tests for create_subcommand_parser function."""

    def test_parser_creation(self):
        """Test that parser is created successfully."""
        parser = create_subcommand_parser()
        assert parser is not None
        assert isinstance(parser, argparse.ArgumentParser)

    def test_parser_has_subparsers(self):
        """Test that parser has subcommand parsers."""
        parser = create_subcommand_parser()

        # Parse a known subcommand
        args = parser.parse_args(["run"])
        assert args.command == "run"

    def test_run_command_options(self):
        """Test run command has expected options."""
        parser = create_subcommand_parser()

        args = parser.parse_args(["run", "--dry-run"])
        assert args.dry_run is True

        args = parser.parse_args(["run", "--parallel-volumes", "4"])
        assert args.parallel_volumes == 4

        args = parser.parse_args(["run", "--compress", "zstd"])
        assert args.compress == "zstd"

        args = parser.parse_args(["run", "--rate-limit", "10M"])
        assert args.rate_limit == "10M"

    def test_snapshot_command_options(self):
        """Test snapshot command has expected options."""
        parser = create_subcommand_parser()

        args = parser.parse_args(["snapshot", "--dry-run"])
        assert args.dry_run is True

        args = parser.parse_args(["snapshot", "--volume", "/home"])
        assert args.volume == ["/home"]

    def test_transfer_command_options(self):
        """Test transfer command has expected options."""
        parser = create_subcommand_parser()

        args = parser.parse_args(["transfer", "--compress", "lz4"])
        assert args.compress == "lz4"

        args = parser.parse_args(["transfer", "--rate-limit", "50M"])
        assert args.rate_limit == "50M"

    def test_prune_command_options(self):
        """Test prune command has expected options."""
        parser = create_subcommand_parser()

        args = parser.parse_args(["prune", "--dry-run"])
        assert args.dry_run is True

    def test_list_command_options(self):
        """Test list command has expected options."""
        parser = create_subcommand_parser()

        args = parser.parse_args(["list", "--json"])
        assert args.json is True

        args = parser.parse_args(["list", "--volume", "/home", "--volume", "/var"])
        assert args.volume == ["/home", "/var"]

    def test_config_subcommands(self):
        """Test config subcommands."""
        parser = create_subcommand_parser()

        args = parser.parse_args(["config", "validate"])
        assert args.command == "config"
        assert args.config_action == "validate"

        args = parser.parse_args(["config", "init", "-o", "output.toml"])
        assert args.config_action == "init"
        assert args.output == "output.toml"

        args = parser.parse_args(["config", "import", "btrbk.conf"])
        assert args.config_action == "import"
        assert args.btrbk_config == "btrbk.conf"

    def test_install_command_options(self):
        """Test install command has expected options."""
        parser = create_subcommand_parser()

        args = parser.parse_args(["install", "--timer", "hourly"])
        assert args.timer == "hourly"

        args = parser.parse_args(["install", "--oncalendar", "*:0/15"])
        assert args.oncalendar == "*:0/15"

        args = parser.parse_args(["install", "--user"])
        assert args.user is True

    def test_global_options(self):
        """Test global options work with subcommands."""
        parser = create_subcommand_parser()

        args = parser.parse_args(["-v", "run"])
        assert args.verbose == 1

        args = parser.parse_args(["-c", "myconfig.toml", "run"])
        assert args.config == "myconfig.toml"

    def test_compression_choices(self):
        """Test that compression has valid choices."""
        parser = create_subcommand_parser()

        # Valid choices should work
        for method in ["none", "gzip", "zstd", "lz4", "pigz", "lzop"]:
            args = parser.parse_args(["run", "--compress", method])
            assert args.compress == method

        # Invalid choice should fail
        with pytest.raises(SystemExit):
            parser.parse_args(["run", "--compress", "invalid"])

    def test_timer_choices(self):
        """Test that timer has valid choices."""
        parser = create_subcommand_parser()

        for preset in ["hourly", "daily", "weekly"]:
            args = parser.parse_args(["install", "--timer", preset])
            assert args.timer == preset

        with pytest.raises(SystemExit):
            parser.parse_args(["install", "--timer", "invalid"])


class TestCLIIntegration:
    """Integration tests for CLI."""

    @patch("btrfs_backup_ng.cli.dispatcher.run_legacy_mode")
    def test_legacy_mode_called(self, mock_legacy):
        """Test that legacy mode is called for path arguments."""
        from btrfs_backup_ng.cli.dispatcher import main

        mock_legacy.return_value = 0
        main(["/home", "/backup"])

        mock_legacy.assert_called_once()

    @patch("btrfs_backup_ng.cli.dispatcher.run_subcommand")
    def test_subcommand_mode_called(self, mock_subcommand):
        """Test that subcommand mode is called for known commands."""
        from btrfs_backup_ng.cli.dispatcher import main

        mock_subcommand.return_value = 0
        main(["status"])

        mock_subcommand.assert_called_once()


class TestRunSubcommand:
    """Tests for run_subcommand function."""

    def test_version_flag(self, capsys):
        """Test that --version prints version and exits."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        args = argparse.Namespace(version=True, command=None)
        result = run_subcommand(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "btrfs-backup-ng" in captured.out

    def test_no_command(self, capsys):
        """Test that missing command prints error."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        args = argparse.Namespace(version=False, command=None)
        result = run_subcommand(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "No command specified" in captured.out

    def test_unknown_command(self, capsys):
        """Test that unknown command prints error."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        args = argparse.Namespace(version=False, command="unknown_cmd")
        result = run_subcommand(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Unknown command" in captured.out

    @patch("btrfs_backup_ng.cli.dispatcher.cmd_run")
    def test_routes_to_run(self, mock_cmd):
        """Test that 'run' routes to cmd_run."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        mock_cmd.return_value = 0
        args = argparse.Namespace(version=False, command="run")
        run_subcommand(args)

        mock_cmd.assert_called_once_with(args)

    @patch("btrfs_backup_ng.cli.dispatcher.cmd_snapshot")
    def test_routes_to_snapshot(self, mock_cmd):
        """Test that 'snapshot' routes to cmd_snapshot."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        mock_cmd.return_value = 0
        args = argparse.Namespace(version=False, command="snapshot")
        run_subcommand(args)

        mock_cmd.assert_called_once_with(args)

    @patch("btrfs_backup_ng.cli.dispatcher.cmd_transfer")
    def test_routes_to_transfer(self, mock_cmd):
        """Test that 'transfer' routes to cmd_transfer."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        mock_cmd.return_value = 0
        args = argparse.Namespace(version=False, command="transfer")
        run_subcommand(args)

        mock_cmd.assert_called_once_with(args)

    @patch("btrfs_backup_ng.cli.dispatcher.cmd_prune")
    def test_routes_to_prune(self, mock_cmd):
        """Test that 'prune' routes to cmd_prune."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        mock_cmd.return_value = 0
        args = argparse.Namespace(version=False, command="prune")
        run_subcommand(args)

        mock_cmd.assert_called_once_with(args)

    @patch("btrfs_backup_ng.cli.dispatcher.cmd_list")
    def test_routes_to_list(self, mock_cmd):
        """Test that 'list' routes to cmd_list."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        mock_cmd.return_value = 0
        args = argparse.Namespace(version=False, command="list")
        run_subcommand(args)

        mock_cmd.assert_called_once_with(args)

    @patch("btrfs_backup_ng.cli.dispatcher.cmd_status")
    def test_routes_to_status(self, mock_cmd):
        """Test that 'status' routes to cmd_status."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        mock_cmd.return_value = 0
        args = argparse.Namespace(version=False, command="status")
        run_subcommand(args)

        mock_cmd.assert_called_once_with(args)

    @patch("btrfs_backup_ng.cli.dispatcher.cmd_config")
    def test_routes_to_config(self, mock_cmd):
        """Test that 'config' routes to cmd_config."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        mock_cmd.return_value = 0
        args = argparse.Namespace(version=False, command="config")
        run_subcommand(args)

        mock_cmd.assert_called_once_with(args)

    @patch("btrfs_backup_ng.cli.dispatcher.cmd_install")
    def test_routes_to_install(self, mock_cmd):
        """Test that 'install' routes to cmd_install."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        mock_cmd.return_value = 0
        args = argparse.Namespace(version=False, command="install")
        run_subcommand(args)

        mock_cmd.assert_called_once_with(args)

    @patch("btrfs_backup_ng.cli.dispatcher.cmd_uninstall")
    def test_routes_to_uninstall(self, mock_cmd):
        """Test that 'uninstall' routes to cmd_uninstall."""
        from btrfs_backup_ng.cli.dispatcher import run_subcommand

        mock_cmd.return_value = 0
        args = argparse.Namespace(version=False, command="uninstall")
        run_subcommand(args)

        mock_cmd.assert_called_once_with(args)


class TestShowMigrationNotice:
    """Tests for show_migration_notice function."""

    def test_creates_notice_file(self, tmp_path, monkeypatch):
        """Test that notice file is created."""
        from btrfs_backup_ng.cli.dispatcher import show_migration_notice

        # Patch home directory
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)

        show_migration_notice()

        notice_file = (
            tmp_path / ".config" / "btrfs-backup-ng" / ".migration-notice-shown"
        )
        assert notice_file.exists()

    def test_shows_notice_once(self, tmp_path, monkeypatch, capsys):
        """Test that notice is only shown once."""
        from btrfs_backup_ng.cli.dispatcher import show_migration_notice

        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)

        # First call - should show notice
        show_migration_notice()
        first_output = capsys.readouterr().out

        # Second call - should not show notice
        show_migration_notice()
        second_output = capsys.readouterr().out

        assert "TOML" in first_output
        assert second_output == ""

    def test_handles_write_error(self, tmp_path, monkeypatch):
        """Test that write errors are handled gracefully."""
        from btrfs_backup_ng.cli.dispatcher import show_migration_notice

        # Create a directory where the notice file should be
        # to cause a write error
        notice_path = (
            tmp_path / ".config" / "btrfs-backup-ng" / ".migration-notice-shown"
        )
        notice_path.mkdir(parents=True)

        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)

        # Should not raise
        show_migration_notice()


class TestMainFunction:
    """Tests for main entry point."""

    def test_default_argv(self):
        """Test that main uses sys.argv by default."""
        from btrfs_backup_ng.cli.dispatcher import main

        with patch("sys.argv", ["prog", "status"]):
            with patch("btrfs_backup_ng.cli.dispatcher.run_subcommand") as mock:
                mock.return_value = 0
                # This would use sys.argv[1:] = ["status"]
                # But we pass explicit argv to avoid side effects
                main(["status"])
                mock.assert_called_once()

    def test_returns_exit_code(self):
        """Test that main returns exit code from subcommand."""
        from btrfs_backup_ng.cli.dispatcher import main

        with patch("btrfs_backup_ng.cli.dispatcher.run_subcommand") as mock:
            mock.return_value = 42
            result = main(["status"])
            assert result == 42

    def test_main_with_none_argv(self):
        """Test main with None argv uses sys.argv."""
        from btrfs_backup_ng.cli.dispatcher import main

        with patch("sys.argv", ["prog", "status"]):
            with patch("btrfs_backup_ng.cli.dispatcher.run_subcommand") as mock:
                mock.return_value = 0
                main(None)
                mock.assert_called_once()


class TestRunLegacyMode:
    """Tests for run_legacy_mode function."""

    @patch("btrfs_backup_ng.cli.dispatcher.show_migration_notice")
    @patch("btrfs_backup_ng._legacy_main.legacy_main")
    def test_calls_legacy_main(self, mock_legacy, mock_notice):
        """Test that run_legacy_mode calls legacy_main."""
        from btrfs_backup_ng.cli.dispatcher import run_legacy_mode

        mock_legacy.return_value = 0
        result = run_legacy_mode(["/source", "/dest"])

        mock_notice.assert_called_once()
        mock_legacy.assert_called_once_with(["/source", "/dest"])
        assert result == 0

    @patch("btrfs_backup_ng.cli.dispatcher.show_migration_notice")
    @patch("btrfs_backup_ng._legacy_main.legacy_main")
    def test_returns_legacy_exit_code(self, mock_legacy, mock_notice):
        """Test that run_legacy_mode returns legacy_main's exit code."""
        from btrfs_backup_ng.cli.dispatcher import run_legacy_mode

        mock_legacy.return_value = 5
        result = run_legacy_mode(["/source"])

        assert result == 5


class TestCommandHandlers:
    """Tests for individual command handler functions."""

    @patch("btrfs_backup_ng.cli.run.execute_run")
    def test_cmd_run(self, mock_execute):
        """Test cmd_run calls execute_run."""
        from btrfs_backup_ng.cli.dispatcher import cmd_run

        mock_execute.return_value = 0
        args = argparse.Namespace()
        result = cmd_run(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.snapshot.execute_snapshot")
    def test_cmd_snapshot(self, mock_execute):
        """Test cmd_snapshot calls execute_snapshot."""
        from btrfs_backup_ng.cli.dispatcher import cmd_snapshot

        mock_execute.return_value = 0
        args = argparse.Namespace()
        result = cmd_snapshot(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.transfer.execute_transfer")
    def test_cmd_transfer(self, mock_execute):
        """Test cmd_transfer calls execute_transfer."""
        from btrfs_backup_ng.cli.dispatcher import cmd_transfer

        mock_execute.return_value = 0
        args = argparse.Namespace()
        result = cmd_transfer(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.prune.execute_prune")
    def test_cmd_prune(self, mock_execute):
        """Test cmd_prune calls execute_prune."""
        from btrfs_backup_ng.cli.dispatcher import cmd_prune

        mock_execute.return_value = 0
        args = argparse.Namespace()
        result = cmd_prune(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.list_cmd.execute_list")
    def test_cmd_list(self, mock_execute):
        """Test cmd_list calls execute_list."""
        from btrfs_backup_ng.cli.dispatcher import cmd_list

        mock_execute.return_value = 0
        args = argparse.Namespace()
        result = cmd_list(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.status.execute_status")
    def test_cmd_status(self, mock_execute):
        """Test cmd_status calls execute_status."""
        from btrfs_backup_ng.cli.dispatcher import cmd_status

        mock_execute.return_value = 0
        args = argparse.Namespace()
        result = cmd_status(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.config_cmd.execute_config")
    def test_cmd_config(self, mock_execute):
        """Test cmd_config calls execute_config."""
        from btrfs_backup_ng.cli.dispatcher import cmd_config

        mock_execute.return_value = 0
        args = argparse.Namespace()
        result = cmd_config(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.install.execute_install")
    def test_cmd_install(self, mock_execute):
        """Test cmd_install calls execute_install."""
        from btrfs_backup_ng.cli.dispatcher import cmd_install

        mock_execute.return_value = 0
        args = argparse.Namespace()
        result = cmd_install(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0

    @patch("btrfs_backup_ng.cli.install.execute_uninstall")
    def test_cmd_uninstall(self, mock_execute):
        """Test cmd_uninstall calls execute_uninstall."""
        from btrfs_backup_ng.cli.dispatcher import cmd_uninstall

        mock_execute.return_value = 0
        args = argparse.Namespace()
        result = cmd_uninstall(args)

        mock_execute.assert_called_once_with(args)
        assert result == 0
