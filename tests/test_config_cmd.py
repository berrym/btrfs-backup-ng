"""Tests for config command functionality."""

import argparse
import sys
from unittest import mock

import pytest

from btrfs_backup_ng.cli.config_cmd import (
    _generate_config_from_wizard,
    _init_config,
    _prompt,
    _prompt_bool,
    _prompt_choice,
    _prompt_int,
    _run_interactive_wizard,
    execute_config,
)


class TestPrompt:
    """Tests for _prompt function."""

    def test_returns_user_input(self):
        with mock.patch("builtins.input", return_value="test_value"):
            result = _prompt("Enter value")
        assert result == "test_value"

    def test_returns_default_on_empty_input(self):
        with mock.patch("builtins.input", return_value=""):
            result = _prompt("Enter value", default="default")
        assert result == "default"

    def test_strips_whitespace(self):
        with mock.patch("builtins.input", return_value="  value  "):
            result = _prompt("Enter value")
        assert result == "value"

    def test_raises_keyboard_interrupt_on_eof(self):
        with mock.patch("builtins.input", side_effect=EOFError):
            with pytest.raises(KeyboardInterrupt):
                _prompt("Enter value")

    def test_raises_keyboard_interrupt_on_ctrl_c(self):
        with mock.patch("builtins.input", side_effect=KeyboardInterrupt):
            with pytest.raises(KeyboardInterrupt):
                _prompt("Enter value")


class TestPromptBool:
    """Tests for _prompt_bool function."""

    def test_returns_true_for_yes(self):
        for val in ["y", "Y", "yes", "YES", "Yes", "true", "1"]:
            with mock.patch("builtins.input", return_value=val):
                result = _prompt_bool("Confirm?")
            assert result is True

    def test_returns_false_for_no(self):
        for val in ["n", "N", "no", "NO", "No", "false", "0"]:
            with mock.patch("builtins.input", return_value=val):
                result = _prompt_bool("Confirm?")
            assert result is False

    def test_returns_default_on_empty(self):
        with mock.patch("builtins.input", return_value=""):
            result = _prompt_bool("Confirm?", default=True)
        assert result is True

        with mock.patch("builtins.input", return_value=""):
            result = _prompt_bool("Confirm?", default=False)
        assert result is False

    def test_raises_keyboard_interrupt_on_eof(self):
        with mock.patch("builtins.input", side_effect=EOFError):
            with pytest.raises(KeyboardInterrupt):
                _prompt_bool("Confirm?")


class TestPromptChoice:
    """Tests for _prompt_choice function."""

    def test_returns_choice_by_number(self):
        with mock.patch("builtins.input", return_value="2"):
            result = _prompt_choice("Choose", ["a", "b", "c"])
        assert result == "b"

    def test_returns_choice_by_value(self):
        with mock.patch("builtins.input", return_value="b"):
            result = _prompt_choice("Choose", ["a", "b", "c"])
        assert result == "b"

    def test_returns_default_on_empty(self):
        with mock.patch("builtins.input", return_value=""):
            result = _prompt_choice("Choose", ["a", "b", "c"], default="b")
        assert result == "b"

    def test_rejects_invalid_then_accepts_valid(self):
        inputs = iter(["99", "invalid", "2"])
        with mock.patch("builtins.input", side_effect=lambda _: next(inputs)):
            result = _prompt_choice("Choose", ["a", "b", "c"])
        assert result == "b"


class TestPromptInt:
    """Tests for _prompt_int function."""

    def test_returns_valid_int(self):
        with mock.patch("builtins.input", return_value="5"):
            result = _prompt_int("Enter number", default=10)
        assert result == 5

    def test_returns_default_on_empty(self):
        with mock.patch("builtins.input", return_value=""):
            result = _prompt_int("Enter number", default=10)
        assert result == 10

    def test_rejects_out_of_range(self):
        inputs = iter(["999", "5"])
        with mock.patch("builtins.input", side_effect=lambda _: next(inputs)):
            result = _prompt_int("Enter number", default=10, min_val=0, max_val=100)
        assert result == 5

    def test_rejects_non_numeric(self):
        inputs = iter(["abc", "5"])
        with mock.patch("builtins.input", side_effect=lambda _: next(inputs)):
            result = _prompt_int("Enter number", default=10)
        assert result == 5


class TestGenerateConfigFromWizard:
    """Tests for _generate_config_from_wizard function."""

    def test_generates_basic_config(self):
        config_data = {
            "snapshot_dir": ".snapshots",
            "timestamp_format": "%Y%m%d-%H%M%S",
            "incremental": True,
            "parallel_volumes": 2,
            "parallel_targets": 3,
            "retention": {
                "min": "1d",
                "hourly": 24,
                "daily": 7,
                "weekly": 4,
                "monthly": 12,
                "yearly": 0,
            },
            "volumes": [
                {
                    "path": "/home",
                    "snapshot_prefix": "home",
                    "targets": [{"path": "/mnt/backup/home"}],
                }
            ],
        }

        result = _generate_config_from_wizard(config_data)

        assert "[global]" in result
        assert 'snapshot_dir = ".snapshots"' in result
        assert "incremental = true" in result
        assert "[global.retention]" in result
        assert "hourly = 24" in result
        assert "[[volumes]]" in result
        assert 'path = "/home"' in result
        assert "[[volumes.targets]]" in result
        assert 'path = "/mnt/backup/home"' in result

    def test_generates_config_with_log_file(self):
        config_data = {
            "snapshot_dir": ".snapshots",
            "timestamp_format": "%Y%m%d-%H%M%S",
            "incremental": True,
            "log_file": "/var/log/backup.log",
            "parallel_volumes": 2,
            "parallel_targets": 3,
            "retention": {
                "min": "1d",
                "hourly": 24,
                "daily": 7,
                "weekly": 4,
                "monthly": 12,
                "yearly": 0,
            },
            "volumes": [],
        }

        result = _generate_config_from_wizard(config_data)
        assert 'log_file = "/var/log/backup.log"' in result

    def test_generates_config_with_ssh_target(self):
        config_data = {
            "snapshot_dir": ".snapshots",
            "timestamp_format": "%Y%m%d-%H%M%S",
            "incremental": True,
            "parallel_volumes": 2,
            "parallel_targets": 3,
            "retention": {
                "min": "1d",
                "hourly": 24,
                "daily": 7,
                "weekly": 4,
                "monthly": 12,
                "yearly": 0,
            },
            "volumes": [
                {
                    "path": "/home",
                    "snapshot_prefix": "home",
                    "targets": [
                        {"path": "ssh://user@server:/backups", "ssh_sudo": True}
                    ],
                }
            ],
        }

        result = _generate_config_from_wizard(config_data)
        assert 'path = "ssh://user@server:/backups"' in result
        assert "ssh_sudo = true" in result

    def test_generates_config_with_require_mount(self):
        config_data = {
            "snapshot_dir": ".snapshots",
            "timestamp_format": "%Y%m%d-%H%M%S",
            "incremental": True,
            "parallel_volumes": 2,
            "parallel_targets": 3,
            "retention": {
                "min": "1d",
                "hourly": 24,
                "daily": 7,
                "weekly": 4,
                "monthly": 12,
                "yearly": 0,
            },
            "volumes": [
                {
                    "path": "/home",
                    "snapshot_prefix": "home",
                    "targets": [{"path": "/mnt/usb-backup", "require_mount": True}],
                }
            ],
        }

        result = _generate_config_from_wizard(config_data)
        assert "require_mount = true" in result

    def test_generates_config_with_email(self):
        config_data = {
            "snapshot_dir": ".snapshots",
            "timestamp_format": "%Y%m%d-%H%M%S",
            "incremental": True,
            "parallel_volumes": 2,
            "parallel_targets": 3,
            "retention": {
                "min": "1d",
                "hourly": 24,
                "daily": 7,
                "weekly": 4,
                "monthly": 12,
                "yearly": 0,
            },
            "email": {
                "enabled": True,
                "smtp_host": "smtp.example.com",
                "smtp_port": 587,
                "smtp_tls": "starttls",
                "smtp_user": "user",
                "smtp_password": "pass",
                "from_addr": "from@example.com",
                "to_addrs": ["to@example.com"],
                "on_success": False,
                "on_failure": True,
            },
            "volumes": [],
        }

        result = _generate_config_from_wizard(config_data)
        assert "[global.notifications.email]" in result
        assert "enabled = true" in result
        assert 'smtp_host = "smtp.example.com"' in result

    def test_generates_config_with_webhook(self):
        config_data = {
            "snapshot_dir": ".snapshots",
            "timestamp_format": "%Y%m%d-%H%M%S",
            "incremental": True,
            "parallel_volumes": 2,
            "parallel_targets": 3,
            "retention": {
                "min": "1d",
                "hourly": 24,
                "daily": 7,
                "weekly": 4,
                "monthly": 12,
                "yearly": 0,
            },
            "webhook": {
                "enabled": True,
                "url": "https://hooks.example.com/webhook",
                "method": "POST",
                "on_success": False,
                "on_failure": True,
            },
            "volumes": [],
        }

        result = _generate_config_from_wizard(config_data)
        assert "[global.notifications.webhook]" in result
        assert 'url = "https://hooks.example.com/webhook"' in result


class TestInitConfig:
    """Tests for _init_config function."""

    def test_non_interactive_outputs_to_stdout(self, capsys):
        args = argparse.Namespace(interactive=False, output=None)
        result = _init_config(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "[global]" in captured.out

    def test_non_interactive_writes_to_file(self, tmp_path):
        output_file = tmp_path / "config.toml"
        args = argparse.Namespace(interactive=False, output=str(output_file))
        result = _init_config(args)
        assert result == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert "[global]" in content

    def test_interactive_requires_tty(self, capsys):
        args = argparse.Namespace(interactive=True, output=None)
        with mock.patch.object(sys.stdin, "isatty", return_value=False):
            result = _init_config(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "requires a terminal" in captured.out

    def test_interactive_cancelled_by_user(self, capsys):
        args = argparse.Namespace(interactive=True, output=None)
        with mock.patch.object(sys.stdin, "isatty", return_value=True):
            with mock.patch(
                "btrfs_backup_ng.cli.config_cmd._run_interactive_wizard",
                side_effect=KeyboardInterrupt,
            ):
                result = _init_config(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "cancelled" in captured.out

    def test_interactive_overwrites_existing_file_when_confirmed(self, tmp_path):
        output_file = tmp_path / "config.toml"
        output_file.write_text("old content")

        args = argparse.Namespace(interactive=True, output=str(output_file))
        wizard_output = "# new config content\n[global]\n"

        with mock.patch.object(sys.stdin, "isatty", return_value=True):
            with mock.patch(
                "btrfs_backup_ng.cli.config_cmd._run_interactive_wizard",
                return_value=wizard_output,
            ):
                # Mock Rich prompt_bool (new wizard uses Rich prompts)
                with mock.patch(
                    "btrfs_backup_ng.cli.config_cmd.prompt_bool",
                    return_value=True,
                ):
                    result = _init_config(args)

        assert result == 0
        assert "new config content" in output_file.read_text()

    def test_interactive_aborts_when_overwrite_declined(self, tmp_path, capsys):
        output_file = tmp_path / "config.toml"
        output_file.write_text("old content")

        args = argparse.Namespace(interactive=True, output=str(output_file))
        wizard_output = "# new config content\n[global]\n"

        with mock.patch.object(sys.stdin, "isatty", return_value=True):
            with mock.patch(
                "btrfs_backup_ng.cli.config_cmd._run_interactive_wizard",
                return_value=wizard_output,
            ):
                # Mock Rich prompt_bool (new wizard uses Rich prompts)
                with mock.patch(
                    "btrfs_backup_ng.cli.config_cmd.prompt_bool",
                    return_value=False,
                ):
                    result = _init_config(args)

        assert result == 1
        assert "old content" in output_file.read_text()


class TestExecuteConfig:
    """Tests for execute_config function."""

    def test_validate_with_no_config(self, capsys):
        args = argparse.Namespace(
            config=None, config_action="validate", verbose=0, quiet=False
        )
        with mock.patch(
            "btrfs_backup_ng.cli.config_cmd.find_config_file",
            return_value=None,
        ):
            result = execute_config(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "No configuration file found" in captured.out

    def test_init_action(self, capsys):
        args = argparse.Namespace(
            config_action="init",
            interactive=False,
            output=None,
            verbose=0,
            quiet=False,
        )
        result = execute_config(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "[global]" in captured.out

    def test_unknown_action(self, capsys):
        args = argparse.Namespace(config_action=None, verbose=0, quiet=False)
        result = execute_config(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "Usage:" in captured.out


class TestInteractiveWizard:
    """Integration tests for the interactive wizard flow.

    Note: The wizard now uses Rich-based prompts from wizard_utils.
    Tests mock the wizard_utils functions directly.
    """

    def test_full_wizard_flow(self):
        """Test complete wizard flow with mocked Rich prompts."""
        # Mock the Rich prompt functions
        prompt_returns = iter(
            [
                ".snapshots",  # snapshot_dir
                "%Y%m%d-%H%M%S",  # timestamp_format
                "",  # log_file (empty)
                "",  # transaction_log (empty)
                "1d",  # min retention
                "/home",  # volume path
                "home-",  # snapshot prefix
                "/mnt/backup",  # target path
            ]
        )
        prompt_int_returns = iter([2, 3, 24, 7, 4, 12, 0])  # parallel and retention
        prompt_bool_returns = iter(
            [
                True,  # incremental
                False,  # email notifications
                False,  # webhook notifications
                False,  # require_mount
                False,  # add another target
                False,  # add another volume
            ]
        )

        with mock.patch(
            "btrfs_backup_ng.cli.config_cmd.prompt",
            side_effect=lambda *a, **kw: next(prompt_returns),
        ):
            with mock.patch(
                "btrfs_backup_ng.cli.config_cmd.prompt_int",
                side_effect=lambda *a, **kw: next(prompt_int_returns),
            ):
                with mock.patch(
                    "btrfs_backup_ng.cli.config_cmd.prompt_bool",
                    side_effect=lambda *a, **kw: next(prompt_bool_returns),
                ):
                    result = _run_interactive_wizard()

        assert "[global]" in result
        assert 'path = "/home"' in result
        assert 'path = "/mnt/backup"' in result


class TestMigrateSystemd:
    """Tests for the migrate-systemd command."""

    def test_migrate_systemd_dry_run(self, capsys):
        """Test migrate-systemd with dry-run flag."""
        from btrfs_backup_ng.cli.config_cmd import execute_config

        args = argparse.Namespace(
            config_action="migrate-systemd",
            dry_run=True,
        )

        with mock.patch(
            "btrfs_backup_ng.systemd_utils.get_migration_summary"
        ) as mock_summary:
            mock_summary.return_value = {
                "btrbk_units": [
                    {
                        "name": "btrbk.timer",
                        "enabled": True,
                        "active": False,
                        "path": None,
                    }
                ],
                "backup_ng_units": [],
                "btrbk_active": True,
                "backup_ng_active": False,
                "migration_needed": True,
            }
            with mock.patch(
                "btrfs_backup_ng.systemd_utils.migrate_from_btrbk"
            ) as mock_migrate:
                mock_migrate.return_value = (
                    True,
                    ["Would disable btrbk.timer (dry-run)"],
                )
                result = execute_config(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Systemd Migration" in captured.out

    def test_migrate_systemd_no_migration_needed(self, capsys):
        """Test migrate-systemd when no migration is needed."""
        from btrfs_backup_ng.cli.config_cmd import execute_config

        args = argparse.Namespace(
            config_action="migrate-systemd",
            dry_run=False,
        )

        with mock.patch(
            "btrfs_backup_ng.systemd_utils.get_migration_summary"
        ) as mock_summary:
            mock_summary.return_value = {
                "btrbk_units": [
                    {
                        "name": "btrbk.timer",
                        "enabled": False,
                        "active": False,
                        "path": None,
                    }
                ],
                "backup_ng_units": [],
                "btrbk_active": False,
                "backup_ng_active": False,
                "migration_needed": False,
            }
            result = execute_config(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "No migration needed" in captured.out

    def test_migrate_systemd_success(self, capsys):
        """Test successful systemd migration."""
        from btrfs_backup_ng.cli.config_cmd import execute_config

        args = argparse.Namespace(
            config_action="migrate-systemd",
            dry_run=False,
        )

        with mock.patch(
            "btrfs_backup_ng.systemd_utils.get_migration_summary"
        ) as mock_summary:
            mock_summary.return_value = {
                "btrbk_units": [
                    {
                        "name": "btrbk.timer",
                        "enabled": True,
                        "active": True,
                        "path": None,
                    }
                ],
                "backup_ng_units": [],
                "btrbk_active": True,
                "backup_ng_active": False,
                "migration_needed": True,
            }
            with mock.patch(
                "btrfs_backup_ng.systemd_utils.migrate_from_btrbk"
            ) as mock_migrate:
                mock_migrate.return_value = (
                    True,
                    ["Stopped btrbk.timer", "Disabled btrbk.timer"],
                )
                result = execute_config(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Systemd migration complete" in captured.out

    def test_migrate_systemd_with_errors(self, capsys):
        """Test systemd migration with errors."""
        from btrfs_backup_ng.cli.config_cmd import execute_config

        args = argparse.Namespace(
            config_action="migrate-systemd",
            dry_run=False,
        )

        with mock.patch(
            "btrfs_backup_ng.systemd_utils.get_migration_summary"
        ) as mock_summary:
            mock_summary.return_value = {
                "btrbk_units": [
                    {
                        "name": "btrbk.timer",
                        "enabled": True,
                        "active": True,
                        "path": None,
                    }
                ],
                "backup_ng_units": [],
                "btrbk_active": True,
                "backup_ng_active": False,
                "migration_needed": True,
            }
            with mock.patch(
                "btrfs_backup_ng.systemd_utils.migrate_from_btrbk"
            ) as mock_migrate:
                mock_migrate.return_value = (False, ["  Error: Permission denied"])
                result = execute_config(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "errors" in captured.out.lower()

    def test_wizard_with_ssh_target(self):
        """Test wizard with SSH target that prompts for sudo."""
        prompt_returns = iter(
            [
                ".snapshots",
                "%Y%m%d-%H%M%S",
                "",
                "",
                "1d",
                "/home",
                "home-",
                "ssh://user@server:/backups",  # SSH target
            ]
        )
        prompt_int_returns = iter([2, 3, 24, 7, 4, 12, 0])
        prompt_bool_returns = iter(
            [
                True,  # incremental
                False,  # email notifications
                False,  # webhook notifications
                True,  # ssh_sudo
                False,  # add another target
                False,  # add another volume
            ]
        )

        with mock.patch(
            "btrfs_backup_ng.cli.config_cmd.prompt",
            side_effect=lambda *a, **kw: next(prompt_returns),
        ):
            with mock.patch(
                "btrfs_backup_ng.cli.config_cmd.prompt_int",
                side_effect=lambda *a, **kw: next(prompt_int_returns),
            ):
                with mock.patch(
                    "btrfs_backup_ng.cli.config_cmd.prompt_bool",
                    side_effect=lambda *a, **kw: next(prompt_bool_returns),
                ):
                    result = _run_interactive_wizard()

        assert 'path = "ssh://user@server:/backups"' in result
        assert "ssh_sudo = true" in result

    def test_wizard_requires_at_least_one_volume(self):
        """Test that wizard requires at least one volume."""
        prompt_returns = iter(
            [
                ".snapshots",
                "%Y%m%d-%H%M%S",
                "",
                "",
                "1d",
                "",  # Try empty volume path first
                "/home",  # Then provide valid path
                "home-",
                "/mnt/backup",
            ]
        )
        prompt_int_returns = iter([2, 3, 24, 7, 4, 12, 0])
        prompt_bool_returns = iter(
            [
                True,
                False,
                False,
                False,  # require_mount
                False,  # add another target
                False,  # add another volume
            ]
        )

        with mock.patch(
            "btrfs_backup_ng.cli.config_cmd.prompt",
            side_effect=lambda *a, **kw: next(prompt_returns),
        ):
            with mock.patch(
                "btrfs_backup_ng.cli.config_cmd.prompt_int",
                side_effect=lambda *a, **kw: next(prompt_int_returns),
            ):
                with mock.patch(
                    "btrfs_backup_ng.cli.config_cmd.prompt_bool",
                    side_effect=lambda *a, **kw: next(prompt_bool_returns),
                ):
                    result = _run_interactive_wizard()

        # Should have completed with the second volume attempt
        assert 'path = "/home"' in result

    def test_wizard_requires_at_least_one_target(self):
        """Test that wizard requires at least one target per volume."""
        prompt_returns = iter(
            [
                ".snapshots",
                "%Y%m%d-%H%M%S",
                "",
                "",
                "1d",
                "/home",
                "home-",
                "",  # Try empty target first
                "/mnt/backup",  # Then provide valid target
            ]
        )
        prompt_int_returns = iter([2, 3, 24, 7, 4, 12, 0])
        prompt_bool_returns = iter(
            [
                True,
                False,
                False,
                False,  # require_mount
                False,  # add another target
                False,  # add another volume
            ]
        )

        with mock.patch(
            "btrfs_backup_ng.cli.config_cmd.prompt",
            side_effect=lambda *a, **kw: next(prompt_returns),
        ):
            with mock.patch(
                "btrfs_backup_ng.cli.config_cmd.prompt_int",
                side_effect=lambda *a, **kw: next(prompt_int_returns),
            ):
                with mock.patch(
                    "btrfs_backup_ng.cli.config_cmd.prompt_bool",
                    side_effect=lambda *a, **kw: next(prompt_bool_returns),
                ):
                    result = _run_interactive_wizard()

        assert 'path = "/mnt/backup"' in result
