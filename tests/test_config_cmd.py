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
                    with mock.patch(
                        "btrfs_backup_ng.cli.config_cmd.prompt_snapshot_prefix",
                        side_effect=["home-"],  # snapshot prefix
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
                    with mock.patch(
                        "btrfs_backup_ng.cli.config_cmd.prompt_snapshot_prefix",
                        side_effect=["home-"],  # snapshot prefix
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
                    with mock.patch(
                        "btrfs_backup_ng.cli.config_cmd.prompt_snapshot_prefix",
                        side_effect=["home-"],  # snapshot prefix
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
                    with mock.patch(
                        "btrfs_backup_ng.cli.config_cmd.prompt_snapshot_prefix",
                        side_effect=["home-"],  # snapshot prefix
                    ):
                        result = _run_interactive_wizard()

        assert 'path = "/mnt/backup"' in result


class TestPromptTargetEncryption:
    """0.9.2 Phase 2B-ii: the wizard's raw-target encryption prompt."""

    def test_non_raw_targets_are_not_prompted(self):
        """Encryption is raw-only; ssh:// and local targets must skip it entirely
        (return {} without asking a question)."""
        from btrfs_backup_ng.cli.config_cmd import _prompt_target_encryption

        with mock.patch("btrfs_backup_ng.cli.config_cmd.prompt_choice") as mchoice:
            assert _prompt_target_encryption("ssh://h/p") == {}
            assert _prompt_target_encryption("/mnt/backup") == {}
            mchoice.assert_not_called()

    def test_raw_none_returns_empty(self):
        from btrfs_backup_ng.cli.config_cmd import _prompt_target_encryption

        with mock.patch(
            "btrfs_backup_ng.cli.config_cmd.prompt_choice", return_value="none"
        ):
            assert _prompt_target_encryption("raw:///mnt/b") == {}

    def test_raw_gpg_collects_required_recipient(self):
        from btrfs_backup_ng.cli.config_cmd import _prompt_target_encryption

        with (
            mock.patch(
                "btrfs_backup_ng.cli.config_cmd.prompt_choice", return_value="gpg"
            ),
            mock.patch(
                "btrfs_backup_ng.cli.config_cmd.prompt",
                side_effect=["me@example.com", ""],  # recipient, keyring (blank)
            ),
        ):
            result = _prompt_target_encryption("raw+ssh://h/b")
        assert result == {"encrypt": "gpg", "gpg_recipient": "me@example.com"}

    def test_raw_gpg_reprompts_until_recipient_given(self):
        """A blank gpg recipient must re-prompt (the loader requires it)."""
        from btrfs_backup_ng.cli.config_cmd import _prompt_target_encryption

        with (
            mock.patch(
                "btrfs_backup_ng.cli.config_cmd.prompt_choice", return_value="gpg"
            ),
            mock.patch(
                "btrfs_backup_ng.cli.config_cmd.prompt",
                side_effect=["", "   ", "KEYID", "/kr.gpg"],
            ),
        ):
            result = _prompt_target_encryption("raw:///mnt/b")
        assert result["encrypt"] == "gpg"
        assert result["gpg_recipient"] == "KEYID"
        assert result["gpg_keyring"] == "/kr.gpg"

    def test_raw_openssl_optional_cipher(self):
        from btrfs_backup_ng.cli.config_cmd import _prompt_target_encryption

        # explicit cipher
        with (
            mock.patch(
                "btrfs_backup_ng.cli.config_cmd.prompt_choice",
                return_value="openssl_enc",
            ),
            mock.patch(
                "btrfs_backup_ng.cli.config_cmd.prompt", return_value="aes-128-cbc"
            ),
        ):
            assert _prompt_target_encryption("raw:///mnt/b") == {
                "encrypt": "openssl_enc",
                "openssl_cipher": "aes-128-cbc",
            }
        # blank cipher -> field omitted (endpoint default applies)
        with (
            mock.patch(
                "btrfs_backup_ng.cli.config_cmd.prompt_choice",
                return_value="openssl_enc",
            ),
            mock.patch("btrfs_backup_ng.cli.config_cmd.prompt", return_value=""),
        ):
            assert _prompt_target_encryption("raw:///mnt/b") == {
                "encrypt": "openssl_enc"
            }


class TestGenerateConfigEncryption:
    """0.9.2 Phase 2B-ii: wizard-emitted encrypted raw configs serialize & LOAD."""

    @staticmethod
    def _config_with_target(target):
        return {
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
                {"path": "/home", "snapshot_prefix": "home", "targets": [target]}
            ],
        }

    def test_gpg_target_serializes_and_loads(self, tmp_path):
        from btrfs_backup_ng.cli.config_cmd import _generate_config_from_wizard
        from btrfs_backup_ng.config.loader import load_config

        toml = _generate_config_from_wizard(
            self._config_with_target(
                {
                    "path": "raw+ssh://user@host/backups",
                    "ssh_sudo": True,
                    "encrypt": "gpg",
                    "gpg_recipient": "me@example.com",
                    "gpg_keyring": "/etc/keys/backup.gpg",
                }
            )
        )
        assert 'encrypt = "gpg"' in toml
        assert 'gpg_recipient = "me@example.com"' in toml
        p = tmp_path / "c.toml"
        p.write_text(toml)
        cfg, warns = load_config(p)
        t = cfg.volumes[0].targets[0]
        assert t.encrypt == "gpg"
        assert t.gpg_recipient == "me@example.com"
        assert t.gpg_keyring == "/etc/keys/backup.gpg"
        assert not [w for w in warns if "Unknown config key" in w]

    def test_openssl_target_serializes_and_loads(self, tmp_path):
        from btrfs_backup_ng.cli.config_cmd import _generate_config_from_wizard
        from btrfs_backup_ng.config.loader import load_config

        toml = _generate_config_from_wizard(
            self._config_with_target(
                {
                    "path": "raw:///mnt/backup",
                    "encrypt": "openssl_enc",
                    "openssl_cipher": "aes-256-cbc",
                }
            )
        )
        p = tmp_path / "c.toml"
        p.write_text(toml)
        cfg, _ = load_config(p)
        t = cfg.volumes[0].targets[0]
        assert t.encrypt == "openssl_enc"
        assert t.openssl_cipher == "aes-256-cbc"

    def test_non_encrypted_target_emits_no_encrypt_key(self):
        from btrfs_backup_ng.cli.config_cmd import _generate_config_from_wizard

        toml = _generate_config_from_wizard(
            self._config_with_target({"path": "ssh://h/p", "ssh_sudo": True})
        )
        assert "encrypt" not in toml
        assert "gpg_recipient" not in toml

    def test_serializer_skips_invalid_gpg_block_without_recipient(self, tmp_path):
        """Defense-in-depth: the serializer enforces the loader's rule (encrypt=gpg
        REQUIRES gpg_recipient) at emit time -- rather than writing an unloadable
        `encrypt = "gpg"` with no recipient, it omits the encryption block, so the
        generated config always loads. (The wizard already requires the recipient
        interactively; this guards a malformed input dict.)"""
        from btrfs_backup_ng.cli.config_cmd import _generate_config_from_wizard
        from btrfs_backup_ng.config.loader import load_config

        toml = _generate_config_from_wizard(
            self._config_with_target({"path": "raw:///mnt/b", "encrypt": "gpg"})
        )
        assert 'encrypt = "gpg"' not in toml
        p = tmp_path / "c.toml"
        p.write_text(toml)
        cfg, _ = load_config(p)  # loads cleanly (no invalid block emitted)
        assert cfg.volumes[0].targets[0].encrypt == "none"

    def test_serializer_enforces_raw_only_invariant(self, tmp_path):
        """The serializer must not emit encryption keys for a NON-raw target even if
        the dict carries them (raw-only invariant enforced at emit time)."""
        from btrfs_backup_ng.cli.config_cmd import _generate_config_from_wizard

        toml = _generate_config_from_wizard(
            self._config_with_target(
                {"path": "ssh://h/p", "encrypt": "gpg", "gpg_recipient": "x@y"}
            )
        )
        assert "encrypt" not in toml
        assert "gpg_recipient" not in toml

    def test_backslash_keyring_path_round_trips_losslessly(self, tmp_path):
        """A keyring path with a backslash must survive serialization -> load
        unchanged (previously `\\t` silently became a TAB, or broke parsing)."""
        from btrfs_backup_ng.cli.config_cmd import _generate_config_from_wizard
        from btrfs_backup_ng.config.loader import load_config

        keyring = "/keys\\thekey.gpg"
        toml = _generate_config_from_wizard(
            self._config_with_target(
                {
                    "path": "raw:///mnt/b",
                    "encrypt": "gpg",
                    "gpg_recipient": "me@example.com",
                    "gpg_keyring": keyring,
                }
            )
        )
        p = tmp_path / "c.toml"
        p.write_text(toml)
        cfg, _ = load_config(p)
        assert cfg.volumes[0].targets[0].gpg_keyring == keyring


class TestGenerateConfigEscaping:
    """R9 follow-up: EVERY string field in _generate_config_from_wizard is emitted
    through _toml_str, so a backslash / double-quote / control char round-trips
    losslessly instead of producing an unparseable or silently-corrupted config."""

    # backslash-t (silent-corruption vector: \t is a valid TOML escape -> TAB) +
    # a double-quote (breaks the basic string).
    NASTY = 'x\\ty"z'

    def _full_config(self, n):
        return {
            "snapshot_dir": "/snaps" + n,
            "timestamp_format": "%Y" + n,
            "incremental": True,
            "log_file": "/log" + n,
            "transaction_log": "/tx" + n,
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
                "smtp_host": "h" + n,
                "smtp_port": 587,
                "smtp_tls": "starttls",
                "smtp_user": "u" + n,
                "smtp_password": "p" + n,
                "from_addr": "a" + n,
                "to_addrs": ["to" + n],
                "on_success": False,
                "on_failure": True,
            },
            "webhook": {
                "enabled": True,
                "url": "https://x" + n,
                "method": "POST",
                "on_success": False,
                "on_failure": True,
            },
            "volumes": [
                {
                    "path": "/vol" + n,
                    "source": "snapper",
                    "snapper": {
                        "config_name": "root" + n,
                        "include_types": ["single"],
                        "min_age": "1h",
                    },
                    "targets": [{"path": "/tgt" + n}],
                }
            ],
        }

    def test_nasty_values_round_trip_losslessly(self, tmp_path):
        from btrfs_backup_ng.cli.config_cmd import _generate_config_from_wizard
        from btrfs_backup_ng.config.loader import load_config

        n = self.NASTY
        toml = _generate_config_from_wizard(self._full_config(n))
        p = tmp_path / "c.toml"
        p.write_text(toml)
        cfg, warns = load_config(p)  # must not raise (unescaped would break parsing)

        g = cfg.global_config
        assert g.snapshot_dir == "/snaps" + n
        assert g.timestamp_format == "%Y" + n
        assert g.log_file == "/log" + n
        assert g.transaction_log == "/tx" + n
        assert g.notifications.email.smtp_host == "h" + n
        assert g.notifications.email.smtp_user == "u" + n
        assert g.notifications.email.smtp_password == "p" + n
        assert g.notifications.email.from_addr == "a" + n
        assert g.notifications.email.to_addrs == ["to" + n]
        assert g.notifications.webhook.url == "https://x" + n
        v = cfg.volumes[0]
        assert v.path == "/vol" + n
        assert v.snapper.config_name == "root" + n
        assert v.targets[0].path == "/tgt" + n
        assert not [w for w in warns if "Unknown config key" in w]

    def test_backslash_path_not_silently_corrupted(self, tmp_path):
        """The specific silent-corruption case: a path with \\t must NOT become a
        literal tab on load."""
        from btrfs_backup_ng.cli.config_cmd import _generate_config_from_wizard
        from btrfs_backup_ng.config.loader import load_config

        cfg_in = self._full_config("")
        cfg_in["volumes"][0]["path"] = "/mnt/keys\\there"  # backslash-t
        toml = _generate_config_from_wizard(cfg_in)
        p = tmp_path / "c.toml"
        p.write_text(toml)
        cfg, _ = load_config(p)
        assert cfg.volumes[0].path == "/mnt/keys\\there"
        assert "\t" not in cfg.volumes[0].path

    def test_clean_config_still_valid_and_faithful(self, tmp_path):
        """No regression: a clean config loads with exactly its values."""
        from btrfs_backup_ng.cli.config_cmd import _generate_config_from_wizard
        from btrfs_backup_ng.config.loader import load_config

        toml = _generate_config_from_wizard(self._full_config(""))
        p = tmp_path / "c.toml"
        p.write_text(toml)
        cfg, warns = load_config(p)
        assert cfg.global_config.snapshot_dir == "/snaps"
        assert cfg.volumes[0].targets[0].path == "/tgt"
        assert cfg.volumes[0].snapper.config_name == "root"
        assert not [w for w in warns if "Unknown config key" in w]
