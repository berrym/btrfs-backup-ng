"""Tests for snapper CLI commands."""

import argparse
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.cli.snapper_cmd import (
    _generate_snapper_toml,
    _handle_detect,
    _handle_generate_config,
    _handle_list,
    _handle_status,
    execute_snapper,
)
from btrfs_backup_ng.snapper.scanner import SnapperConfig, SnapperNotFoundError
from btrfs_backup_ng.snapper.snapshot import SnapperSnapshot


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


@pytest.fixture
def mock_snapper_snapshots():
    """Create mock snapper snapshots."""
    snap1 = MagicMock(spec=SnapperSnapshot)
    snap1.number = 559
    snap1.snapshot_type = "single"
    snap1.date = datetime(2026, 1, 8, 14, 30, 0)
    snap1.description = "timeline"
    snap1.cleanup = "timeline"
    snap1.pre_num = None
    snap1.get_backup_name.return_value = "559"

    snap2 = MagicMock(spec=SnapperSnapshot)
    snap2.number = 560
    snap2.snapshot_type = "pre"
    snap2.date = datetime(2026, 1, 8, 15, 0, 0)
    snap2.description = "dnf install vim"
    snap2.cleanup = "number"
    snap2.pre_num = None
    snap2.get_backup_name.return_value = "560"

    snap3 = MagicMock(spec=SnapperSnapshot)
    snap3.number = 561
    snap3.snapshot_type = "post"
    snap3.date = datetime(2026, 1, 8, 15, 1, 0)
    snap3.description = "dnf install vim"
    snap3.cleanup = "number"
    snap3.pre_num = 560
    snap3.get_backup_name.return_value = "561"

    return [snap1, snap2, snap3]


class TestHandleDetect:
    """Tests for the detect command handler."""

    def test_snapper_not_found(self, capsys):
        """Test handling when snapper is not installed."""
        args = argparse.Namespace(json=False)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner_cls.side_effect = SnapperNotFoundError("snapper not found")
            result = _handle_detect(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Snapper not found" in captured.out

    def test_snapper_not_found_json(self, capsys):
        """Test JSON output when snapper is not installed."""
        args = argparse.Namespace(json=True)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner_cls.side_effect = SnapperNotFoundError("snapper not found")
            result = _handle_detect(args)

        assert result == 1
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert "error" in output
        assert output["configs"] == []

    def test_no_configs_found(self, capsys):
        """Test when no snapper configs exist."""
        args = argparse.Namespace(json=False)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = []
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_detect(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "No snapper configurations found" in captured.out

    def test_detect_configs(self, capsys, mock_snapper_configs):
        """Test detecting snapper configurations."""
        args = argparse.Namespace(json=False)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_detect(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Found 2 snapper configuration(s)" in captured.out
        assert "root:" in captured.out
        assert "home:" in captured.out
        assert "Subvolume:" in captured.out
        assert "Status:" in captured.out
        assert "OK" in captured.out

    def test_detect_configs_with_users(self, capsys, mock_snapper_configs):
        """Test detecting configs that have allowed users."""
        args = argparse.Namespace(json=False)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_detect(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Allowed users: user1" in captured.out

    def test_detect_json_output(self, capsys, mock_snapper_configs):
        """Test JSON output for detect command."""
        args = argparse.Namespace(json=True)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_detect(args)

        assert result == 0
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert "configs" in output
        assert len(output["configs"]) == 2
        assert output["configs"][0]["name"] == "root"
        assert output["configs"][0]["valid"] is True
        assert output["configs"][1]["name"] == "home"


class TestHandleList:
    """Tests for the list command handler."""

    def test_snapper_not_found(self, capsys):
        """Test handling when snapper is not installed."""
        args = argparse.Namespace(json=False, config=None, type=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner_cls.side_effect = SnapperNotFoundError("snapper not found")
            result = _handle_list(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Snapper not found" in captured.out

    def test_snapper_not_found_json(self, capsys):
        """Test JSON output when snapper is not installed."""
        args = argparse.Namespace(json=True, config=None, type=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner_cls.side_effect = SnapperNotFoundError("snapper not found")
            result = _handle_list(args)

        assert result == 1
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert "error" in output

    def test_no_configs_found(self, capsys):
        """Test when no snapper configs exist."""
        args = argparse.Namespace(json=False, config=None, type=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = []
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_list(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "No snapper configurations found" in captured.out

    def test_config_not_found(self, capsys, mock_snapper_configs):
        """Test when specified config doesn't exist."""
        args = argparse.Namespace(json=False, config="nonexistent", type=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.get_config.return_value = None
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_list(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_list_snapshots(self, capsys, mock_snapper_configs, mock_snapper_snapshots):
        """Test listing snapshots for all configs."""
        args = argparse.Namespace(json=False, config=None, type=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner.get_snapshots.return_value = mock_snapper_snapshots
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_list(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Config: root" in captured.out
        assert "559" in captured.out
        assert "single" in captured.out
        assert "timeline" in captured.out

    def test_list_specific_config(
        self, capsys, mock_snapper_configs, mock_snapper_snapshots
    ):
        """Test listing snapshots for a specific config."""
        args = argparse.Namespace(json=False, config="root", type=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.get_config.return_value = mock_snapper_configs[0]
            mock_scanner.get_snapshots.return_value = mock_snapper_snapshots
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_list(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Config: root" in captured.out

    def test_list_with_type_filter(
        self, capsys, mock_snapper_configs, mock_snapper_snapshots
    ):
        """Test listing snapshots with type filter."""
        args = argparse.Namespace(json=False, config=None, type=["single"])

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner.get_snapshots.return_value = [mock_snapper_snapshots[0]]
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_list(args)

        assert result == 0
        # get_snapshots should be called with include_types
        mock_scanner.get_snapshots.assert_called()

    def test_list_no_snapshots(self, capsys, mock_snapper_configs):
        """Test listing when no snapshots exist."""
        args = argparse.Namespace(json=False, config=None, type=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner.get_snapshots.return_value = []
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_list(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "No snapshots found" in captured.out

    def test_list_json_output(
        self, capsys, mock_snapper_configs, mock_snapper_snapshots
    ):
        """Test JSON output for list command."""
        args = argparse.Namespace(json=True, config=None, type=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner.get_snapshots.return_value = mock_snapper_snapshots
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_list(args)

        assert result == 0
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert "configs" in output
        assert len(output["configs"]) == 2
        assert "snapshots" in output["configs"][0]

    def test_list_handles_snapshot_exception(self, capsys, mock_snapper_configs):
        """Test graceful handling of snapshot retrieval errors."""
        args = argparse.Namespace(json=False, config=None, type=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner.get_snapshots.side_effect = Exception("Permission denied")
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_list(args)

        # Should still succeed, just with empty snapshots
        assert result == 0


class TestHandleStatus:
    """Tests for the status command handler."""

    def test_snapper_not_found(self, capsys):
        """Test handling when snapper is not installed."""
        args = argparse.Namespace(json=False, config=None, target=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner_cls.side_effect = SnapperNotFoundError("snapper not found")
            result = _handle_status(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Snapper not found" in captured.out

    def test_no_configs_found(self, capsys):
        """Test when no snapper configs exist."""
        args = argparse.Namespace(json=False, config=None, target=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = []
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_status(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "No snapper configurations found" in captured.out

    def test_status_local_only(
        self, capsys, mock_snapper_configs, mock_snapper_snapshots
    ):
        """Test status without target (local snapshot counts)."""
        args = argparse.Namespace(json=False, config=None, target=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner.get_snapshots.return_value = mock_snapper_snapshots
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_status(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Snapper snapshot status" in captured.out
        assert "Total snapshots:" in captured.out

    def test_status_json_output(
        self, capsys, mock_snapper_configs, mock_snapper_snapshots
    ):
        """Test JSON output for status command."""
        args = argparse.Namespace(json=True, config=None, target=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner.get_snapshots.return_value = mock_snapper_snapshots
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_status(args)

        assert result == 0
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert "status" in output

    def test_status_specific_config(
        self, capsys, mock_snapper_configs, mock_snapper_snapshots
    ):
        """Test status for a specific config."""
        args = argparse.Namespace(json=False, config="root", target=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.get_config.return_value = mock_snapper_configs[0]
            mock_scanner.get_snapshots.return_value = mock_snapper_snapshots
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_status(args)

        assert result == 0

    def test_status_config_not_found(self, capsys):
        """Test status when specified config doesn't exist."""
        args = argparse.Namespace(json=False, config="nonexistent", target=None)

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.get_config.return_value = None
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_status(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "not found" in captured.out

    def test_status_with_target(
        self, capsys, mock_snapper_configs, mock_snapper_snapshots
    ):
        """Test status with a backup target."""
        args = argparse.Namespace(json=False, config=None, target="/mnt/backup")

        with (
            patch("btrfs_backup_ng.cli.snapper_cmd.SnapperScanner") as mock_scanner_cls,
            patch(
                "btrfs_backup_ng.core.operations._list_snapper_backups_at_destination"
            ) as mock_list_backups,
            patch("btrfs_backup_ng.endpoint.choose_endpoint") as mock_choose,
        ):
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner.get_snapshots.return_value = mock_snapper_snapshots
            mock_scanner_cls.return_value = mock_scanner
            mock_list_backups.return_value = {"559", "560"}
            mock_choose.return_value = MagicMock()
            result = _handle_status(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Backup status" in captured.out
        assert "Backed up:" in captured.out
        assert "Pending:" in captured.out

    def test_status_target_access_error(self, capsys, mock_snapper_configs):
        """Test status when target is inaccessible."""
        args = argparse.Namespace(json=False, config=None, target="/mnt/backup")

        with (
            patch("btrfs_backup_ng.cli.snapper_cmd.SnapperScanner") as mock_scanner_cls,
            patch("btrfs_backup_ng.endpoint.choose_endpoint") as mock_choose,
        ):
            mock_scanner = MagicMock()
            mock_scanner.list_configs.return_value = mock_snapper_configs
            mock_scanner_cls.return_value = mock_scanner
            mock_choose.side_effect = Exception("Cannot access target")
            result = _handle_status(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Cannot access target" in captured.out


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


class TestHandleBackup:
    """Tests for the backup command handler."""

    def test_snapper_not_found(self, capsys):
        """Test handling when snapper is not installed."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_backup

        args = argparse.Namespace(
            config="root",
            target="/mnt/backup",
            snapshot=None,
            type=None,
            dry_run=False,
            compress=None,
            rate_limit=None,
            verbose=False,
            quiet=False,
            log_level=None,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner_cls.side_effect = SnapperNotFoundError("snapper not found")
            result = _handle_backup(args)

        assert result == 1

    def test_config_not_found(self, capsys):
        """Test handling when specified config doesn't exist."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_backup

        args = argparse.Namespace(
            config="nonexistent",
            target="/mnt/backup",
            snapshot=None,
            type=None,
            dry_run=False,
            compress=None,
            rate_limit=None,
            verbose=False,
            quiet=False,
            log_level=None,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.get_config.return_value = None
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_backup(args)

        assert result == 1

    def test_dry_run_all_snapshots(
        self, capsys, mock_snapper_configs, mock_snapper_snapshots
    ):
        """Test dry run mode for all snapshots."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_backup

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
        )

        with (
            patch("btrfs_backup_ng.cli.snapper_cmd.SnapperScanner") as mock_scanner_cls,
            patch(
                "btrfs_backup_ng.core.operations.get_snapper_snapshots_for_backup"
            ) as mock_get_snaps,
        ):
            mock_scanner = MagicMock()
            mock_scanner.get_config.return_value = mock_snapper_configs[0]
            mock_scanner_cls.return_value = mock_scanner
            mock_get_snaps.return_value = mock_snapper_snapshots
            result = _handle_backup(args)

        assert result == 0

    def test_dry_run_specific_snapshot(
        self, capsys, mock_snapper_configs, mock_snapper_snapshots
    ):
        """Test dry run mode for a specific snapshot."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_backup

        args = argparse.Namespace(
            config="root",
            target="/mnt/backup",
            snapshot=559,
            type=None,
            dry_run=True,
            compress=None,
            rate_limit=None,
            verbose=False,
            quiet=False,
            log_level=None,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.get_config.return_value = mock_snapper_configs[0]
            mock_scanner.get_snapshot.return_value = mock_snapper_snapshots[0]
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_backup(args)

        assert result == 0

    def test_snapshot_not_found(self, capsys, mock_snapper_configs):
        """Test when specified snapshot doesn't exist."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_backup

        args = argparse.Namespace(
            config="root",
            target="/mnt/backup",
            snapshot=999,
            type=None,
            dry_run=False,
            compress=None,
            rate_limit=None,
            verbose=False,
            quiet=False,
            log_level=None,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.get_config.return_value = mock_snapper_configs[0]
            mock_scanner.get_snapshot.return_value = None
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_backup(args)

        assert result == 1


class TestHandleRestore:
    """Tests for the restore command handler."""

    def test_list_mode(self, capsys):
        """Test listing available backups."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        args = argparse.Namespace(
            source="/mnt/backup",
            config="root",
            snapshot=None,
            list=True,
            dry_run=False,
            json=False,
            verbose=False,
            quiet=False,
            log_level=None,
        )

        mock_backup = {
            "number": 559,
            "metadata": MagicMock(
                type="single", date=datetime(2026, 1, 8), description="test"
            ),
        }

        with patch("btrfs_backup_ng.core.restore.list_snapper_backups") as mock_list:
            mock_list.return_value = [mock_backup]
            result = _handle_restore(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "559" in captured.out

    def test_list_mode_json(self, capsys):
        """Test JSON output for listing backups."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        args = argparse.Namespace(
            source="/mnt/backup",
            config="root",
            snapshot=None,
            list=True,
            dry_run=False,
            json=True,
            verbose=False,
            quiet=False,
            log_level=None,
        )

        mock_backup = {
            "number": 559,
            "metadata": MagicMock(
                type="single", date=datetime(2026, 1, 8), description="test"
            ),
        }

        with patch("btrfs_backup_ng.core.restore.list_snapper_backups") as mock_list:
            mock_list.return_value = [mock_backup]
            result = _handle_restore(args)

        assert result == 0
        captured = capsys.readouterr()
        output = json.loads(captured.out)
        assert "backups" in output

    def test_list_mode_no_backups(self, capsys):
        """Test listing when no backups exist."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        args = argparse.Namespace(
            source="/mnt/backup",
            config="root",
            snapshot=None,
            list=True,
            dry_run=False,
            json=False,
            verbose=False,
            quiet=False,
            log_level=None,
        )

        with patch("btrfs_backup_ng.core.restore.list_snapper_backups") as mock_list:
            mock_list.return_value = []
            result = _handle_restore(args)

        assert result == 0

    def test_list_mode_error(self, capsys):
        """Test error handling for list mode."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        args = argparse.Namespace(
            source="/mnt/backup",
            config="root",
            snapshot=None,
            list=True,
            dry_run=False,
            json=False,
            verbose=False,
            quiet=False,
            log_level=None,
        )

        with patch("btrfs_backup_ng.core.restore.list_snapper_backups") as mock_list:
            mock_list.side_effect = Exception("Access denied")
            result = _handle_restore(args)

        assert result == 1

    def test_snapper_not_found(self, capsys):
        """Test handling when snapper is not installed."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        args = argparse.Namespace(
            source="/mnt/backup",
            config="root",
            snapshot=[559],
            list=False,
            dry_run=False,
            json=False,
            verbose=False,
            quiet=False,
            log_level=None,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner_cls.side_effect = SnapperNotFoundError("snapper not found")
            result = _handle_restore(args)

        assert result == 1

    def test_config_not_found(self, capsys):
        """Test handling when local snapper config doesn't exist."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        args = argparse.Namespace(
            source="/mnt/backup",
            config="nonexistent",
            snapshot=[559],
            list=False,
            dry_run=False,
            json=False,
            verbose=False,
            quiet=False,
            log_level=None,
        )

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.get_config.return_value = None
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_restore(args)

        assert result == 1

    def test_no_snapshot_specified(self, capsys, mock_snapper_configs):
        """Test error when no snapshot or --all specified."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        args = argparse.Namespace(
            source="/mnt/backup",
            config="root",
            snapshot=None,
            list=False,
            dry_run=False,
            json=False,
            verbose=False,
            quiet=False,
            log_level=None,
        )
        # Ensure 'all' attribute doesn't exist or is False
        args.all = False

        with patch(
            "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner"
        ) as mock_scanner_cls:
            mock_scanner = MagicMock()
            mock_scanner.get_config.return_value = mock_snapper_configs[0]
            mock_scanner_cls.return_value = mock_scanner
            result = _handle_restore(args)

        assert result == 1


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
