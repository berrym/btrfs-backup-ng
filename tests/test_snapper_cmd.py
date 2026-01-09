"""Tests for snapper CLI commands."""

import argparse
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.cli.snapper_cmd import (
    _generate_snapper_toml,
    _handle_generate_config,
    execute_snapper,
)
from btrfs_backup_ng.snapper.scanner import SnapperConfig, SnapperNotFoundError


@pytest.fixture
def mock_snapper_configs():
    """Create mock snapper configurations."""
    root_config = MagicMock(spec=SnapperConfig)
    root_config.name = "root"
    root_config.subvolume = Path("/")
    root_config.fstype = "btrfs"
    root_config.snapshots_dir = Path("/.snapshots")
    root_config.allow_users = []
    root_config.is_valid.return_value = True

    home_config = MagicMock(spec=SnapperConfig)
    home_config.name = "home"
    home_config.subvolume = Path("/home")
    home_config.fstype = "btrfs"
    home_config.snapshots_dir = Path("/home/.snapshots")
    home_config.allow_users = ["user1"]
    home_config.is_valid.return_value = True

    return [root_config, home_config]


class TestGenerateSnapperToml:
    """Tests for TOML generation."""

    def test_basic_volume(self):
        """Test generating TOML for a basic volume."""
        volumes = [
            {
                "path": "/",
                "source": "snapper",
                "snapper": {
                    "config_name": "root",
                    "include_types": ["single"],
                    "min_age": "1h",
                },
            }
        ]

        lines = _generate_snapper_toml(volumes, None)
        content = "\n".join(lines)

        assert 'path = "/"' in content
        assert 'source = "snapper"' in content
        # Snapper volumes don't use snapshot_prefix - they use numbered directories
        assert "snapshot_prefix" not in content
        assert 'config_name = "root"' in content
        assert 'include_types = ["single"]' in content
        assert 'min_age = "1h"' in content
        # Should have commented placeholder target
        assert "# [[volumes.targets]]" in content

    def test_volume_with_target(self):
        """Test generating TOML with a target specified."""
        volumes = [
            {
                "path": "/home",
                "source": "snapper",
                "snapshot_prefix": "home-",
                "snapper": {
                    "config_name": "home",
                    "include_types": ["single", "pre"],
                    "min_age": "30m",
                },
                "targets": [{"path": "ssh://backup@server:/backups"}],
            }
        ]

        lines = _generate_snapper_toml(volumes, "ssh://backup@server:/backups")
        content = "\n".join(lines)

        assert 'path = "/home"' in content
        assert 'include_types = ["single", "pre"]' in content
        assert "[[volumes.targets]]" in content
        assert 'path = "ssh://backup@server:/backups"' in content
        # Should NOT have commented placeholder
        assert "# [[volumes.targets]]" not in content

    def test_volume_with_ssh_sudo(self):
        """Test generating TOML with SSH sudo enabled."""
        volumes = [
            {
                "path": "/",
                "source": "snapper",
                "snapshot_prefix": "root-",
                "snapper": {
                    "config_name": "root",
                    "include_types": ["single"],
                    "min_age": "1h",
                },
                "targets": [{"path": "ssh://backup@server:/backups", "ssh_sudo": True}],
            }
        ]

        lines = _generate_snapper_toml(volumes, "ssh://backup@server:/backups")
        content = "\n".join(lines)

        assert "ssh_sudo = true" in content

    def test_multiple_volumes(self):
        """Test generating TOML for multiple volumes."""
        volumes = [
            {
                "path": "/",
                "source": "snapper",
                "snapshot_prefix": "root-",
                "snapper": {
                    "config_name": "root",
                    "include_types": ["single"],
                    "min_age": "1h",
                },
            },
            {
                "path": "/home",
                "source": "snapper",
                "snapshot_prefix": "home-",
                "snapper": {
                    "config_name": "home",
                    "include_types": ["single"],
                    "min_age": "1h",
                },
            },
        ]

        lines = _generate_snapper_toml(volumes, None)
        content = "\n".join(lines)

        # Should have two volume sections
        assert content.count("[[volumes]]") == 2
        assert 'config_name = "root"' in content
        assert 'config_name = "home"' in content

    def test_header_comments(self):
        """Test that TOML includes helpful header comments."""
        volumes = [
            {
                "path": "/",
                "source": "snapper",
                "snapshot_prefix": "root-",
                "snapper": {
                    "config_name": "root",
                    "include_types": ["single"],
                    "min_age": "1h",
                },
            }
        ]

        lines = _generate_snapper_toml(volumes, None)
        content = "\n".join(lines)

        assert "# Snapper volume configuration" in content
        assert "# Generated by: btrfs-backup-ng snapper generate-config" in content


class TestHandleGenerateConfig:
    """Tests for the generate-config command handler."""

    def test_snapper_not_found(self, capsys):
        """Test handling when snapper is not installed."""
        args = argparse.Namespace(
            config=None,
            target=None,
            output=None,
            append=None,
            type=None,
            min_age="1h",
            ssh_sudo=False,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner_cls.side_effect = SnapperNotFoundError("snapper not found")
            result = _handle_generate_config(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Snapper not found" in captured.out

    def test_snapper_not_found_json(self, capsys):
        """Test JSON output when snapper is not installed."""
        args = argparse.Namespace(
            config=None,
            target=None,
            output=None,
            append=None,
            type=None,
            min_age="1h",
            ssh_sudo=False,
            json=True,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner_cls.side_effect = SnapperNotFoundError("snapper not found")
            result = _handle_generate_config(args)

        assert result == 1
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert "error" in output

    def test_no_configs_found(self, capsys):
        """Test handling when no snapper configs exist."""
        args = argparse.Namespace(
            config=None,
            target=None,
            output=None,
            append=None,
            type=None,
            min_age="1h",
            ssh_sudo=False,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = []
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_generate_config(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "No snapper configurations found" in captured.out

    def test_generate_all_configs(self, capsys, mock_snapper_configs):
        """Test generating config for all detected snapper configs."""
        args = argparse.Namespace(
            config=None,
            target=None,
            output=None,
            append=None,
            type=None,
            min_age="1h",
            ssh_sudo=False,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_generate_config(args)

        assert result == 0
        captured = capsys.readouterr()
        assert 'config_name = "root"' in captured.out
        assert 'config_name = "home"' in captured.out

    def test_generate_specific_config(self, capsys, mock_snapper_configs):
        """Test generating config for a specific snapper config."""
        args = argparse.Namespace(
            config=["root"],
            target=None,
            output=None,
            append=None,
            type=None,
            min_age="1h",
            ssh_sudo=False,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_generate_config(args)

        assert result == 0
        captured = capsys.readouterr()
        assert 'config_name = "root"' in captured.out
        assert 'config_name = "home"' not in captured.out

    def test_generate_with_target(self, capsys, mock_snapper_configs):
        """Test generating config with a backup target."""
        args = argparse.Namespace(
            config=["root"],
            target="ssh://backup@server:/backups",
            output=None,
            append=None,
            type=None,
            min_age="1h",
            ssh_sudo=False,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_generate_config(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "[[volumes.targets]]" in captured.out
        # Config name is appended to target path for organization
        assert 'path = "ssh://backup@server:/backups/root"' in captured.out

    def test_generate_with_ssh_sudo(self, capsys, mock_snapper_configs):
        """Test generating config with SSH sudo enabled."""
        args = argparse.Namespace(
            config=["root"],
            target="ssh://backup@server:/backups",
            output=None,
            append=None,
            type=None,
            min_age="1h",
            ssh_sudo=True,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_generate_config(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "ssh_sudo = true" in captured.out

    def test_generate_with_custom_types(self, capsys, mock_snapper_configs):
        """Test generating config with custom snapshot types."""
        args = argparse.Namespace(
            config=["root"],
            target=None,
            output=None,
            append=None,
            type=["single", "pre", "post"],
            min_age="30m",
            ssh_sudo=False,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_generate_config(args)

        assert result == 0
        captured = capsys.readouterr()
        assert 'include_types = ["single", "pre", "post"]' in captured.out
        assert 'min_age = "30m"' in captured.out

    def test_generate_json_output(self, capsys, mock_snapper_configs):
        """Test generating JSON output instead of TOML."""
        args = argparse.Namespace(
            config=["root"],
            target="ssh://backup@server:/backups",
            output=None,
            append=None,
            type=None,
            min_age="1h",
            ssh_sudo=True,
            json=True,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_generate_config(args)

        assert result == 0
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert "volumes" in output
        assert len(output["volumes"]) == 1
        assert output["volumes"][0]["path"] == "/"
        assert output["volumes"][0]["source"] == "snapper"
        assert output["volumes"][0]["snapper"]["config_name"] == "root"
        assert output["volumes"][0]["targets"][0]["ssh_sudo"] is True

    def test_generate_to_file(self, tmp_path, mock_snapper_configs):
        """Test writing config to a file."""
        output_file = tmp_path / "snapper.toml"
        args = argparse.Namespace(
            config=["root"],
            target=None,
            output=str(output_file),
            append=None,
            type=None,
            min_age="1h",
            ssh_sudo=False,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_generate_config(args)

        assert result == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert 'config_name = "root"' in content

    def test_missing_config_warning(self, capsys, mock_snapper_configs):
        """Test warning when requested config doesn't exist."""
        args = argparse.Namespace(
            config=["root", "nonexistent"],
            target=None,
            output=None,
            append=None,
            type=None,
            min_age="1h",
            ssh_sudo=False,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_generate_config(args)

        assert result == 0  # Still succeeds with found configs
        captured = capsys.readouterr()
        assert "nonexistent" in captured.out
        assert "not found" in captured.out


class TestAppendToConfig:
    """Tests for appending to existing config files."""

    def test_append_to_existing(self, tmp_path, mock_snapper_configs):
        """Test appending snapper config to existing file."""
        existing_config = tmp_path / "config.toml"
        existing_config.write_text(
            """[global]
snapshot_dir = ".snapshots"

[[volumes]]
path = "/data"
snapshot_prefix = "data-"

[[volumes.targets]]
path = "/mnt/backup"
"""
        )

        args = argparse.Namespace(
            config=["root"],
            target=None,
            output=None,
            append=str(existing_config),
            type=None,
            min_age="1h",
            ssh_sudo=False,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_generate_config(args)

        assert result == 0
        content = existing_config.read_text()
        # Original content preserved
        assert 'path = "/data"' in content
        assert 'snapshot_prefix = "data-"' in content
        # New content appended
        assert 'config_name = "root"' in content
        assert "# --- Snapper volumes (auto-generated) ---" in content

    def test_append_nonexistent_file(self, tmp_path, capsys, mock_snapper_configs):
        """Test error when appending to nonexistent file."""
        nonexistent = tmp_path / "nonexistent.toml"

        args = argparse.Namespace(
            config=["root"],
            target=None,
            output=None,
            append=str(nonexistent),
            type=None,
            min_age="1h",
            ssh_sudo=False,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_generate_config(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "not found" in captured.out


class TestExecuteSnapper:
    """Tests for the main snapper command dispatcher."""

    def test_no_action(self, capsys):
        """Test error when no action specified."""
        args = argparse.Namespace(snapper_action=None)
        result = execute_snapper(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "No snapper action specified" in captured.out

    def test_unknown_action(self, capsys):
        """Test error for unknown action."""
        args = argparse.Namespace(snapper_action="unknown")
        result = execute_snapper(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "Unknown snapper action" in captured.out

    def test_dispatch_generate_config(self, capsys, mock_snapper_configs):
        """Test dispatching to generate-config handler."""
        args = argparse.Namespace(
            snapper_action="generate-config",
            config=None,
            target=None,
            output=None,
            append=None,
            type=None,
            min_age="1h",
            ssh_sudo=False,
            json=False,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = execute_snapper(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "[[volumes]]" in captured.out
