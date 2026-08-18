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

    def _single_backup_args(self, num):
        return argparse.Namespace(
            config="root",
            target="/mnt/backup",
            snapshot=num,
            type=None,
            dry_run=False,
            compress=None,
            rate_limit=None,
            verbose=False,
            quiet=False,
            log_level=None,
        )

    def _run_single_backup(self, args, all_snaps, target_snap, fake_plan):
        """Drive _handle_backup's single-snapshot path with the correspondence planner mocked;
        return (result, sent) where ``sent`` records send_snapper_snapshot's parent arg."""
        from btrfs_backup_ng.cli.snapper_cmd import _handle_backup

        mock_scanner = MagicMock()
        mock_scanner.get_config.return_value = MagicMock()
        mock_scanner.get_snapshot.return_value = target_snap
        mock_scanner.get_snapshots.return_value = all_snaps

        def _wrap(s, dest=None):
            w = MagicMock()
            w.get_name.return_value = f"cfg-{s.number}"
            return w

        sent = []

        def _send(snap, dest, parent_snapper_snapshot=None, options=None):
            sent.append((snap, parent_snapper_snapshot))

        with (
            patch(
                "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner",
                return_value=mock_scanner,
            ),
            patch(
                "btrfs_backup_ng.endpoint.choose_endpoint",
                return_value=MagicMock(_is_remote=False),
            ),
            patch("btrfs_backup_ng.endpoint.assert_encryption_applied"),
            patch("btrfs_backup_ng.endpoint.assert_compression_applied"),
            patch(
                "btrfs_backup_ng.core.operations._create_snapper_snapshot_wrapper",
                side_effect=_wrap,
            ),
            patch(
                "btrfs_backup_ng.core.operations._snapper_dest_view",
                return_value=MagicMock(),
            ),
            patch(
                "btrfs_backup_ng.core.planning.plan_transfer_sequence",
                side_effect=fake_plan,
            ),
            patch(
                "btrfs_backup_ng.core.operations.send_snapper_snapshot",
                side_effect=_send,
            ),
        ):
            result = _handle_backup(args)
        return result, sent

    def test_single_backup_skips_when_already_present(self):
        """FINDING #9: `snapper backup <cfg> --snapshot N` decides skip by CORRESPONDENCE via the
        shared planner (same authority as a full sync). When the planner reports the target is
        already present (empty plan), the handler returns 0 and NEVER calls send. Mutation guard:
        a number-based or unconditional send would call send here."""
        target = MagicMock(number=5)

        def fake_plan(wrappers, view, only=None, **k):
            return []  # already present on the destination

        result, sent = self._run_single_backup(
            self._single_backup_args(5), [target], target, fake_plan
        )
        assert result == 0
        assert sent == []  # correspondence skip -> nothing sent

    def test_single_backup_sends_with_correspondence_parent(self):
        """FINDING #9: an ABSENT single snapshot is sent with the planner's correspondence-chosen
        parent threaded into send_snapper_snapshot -- so a single-snapshot backup gets a valid
        incremental parent (or a recycled number is handled right), just like a full sync.
        Mutation guard: dropping the parent wiring sends it as a full (parent=None)."""
        parent = MagicMock(number=4)
        target = MagicMock(number=5)

        def fake_plan(wrappers, view, only=None, **k):
            # Planner selects an older wrapper as the incremental parent of the target.
            parent_w = next(w for w in wrappers if w is not only)
            return [(only, parent_w)]

        result, sent = self._run_single_backup(
            self._single_backup_args(5), [parent, target], target, fake_plan
        )
        assert result == 0
        assert len(sent) == 1
        sent_snap, sent_parent = sent[0]
        assert sent_snap is target
        assert sent_parent is parent  # correspondence-selected parent threaded through


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


class TestSnapperEndpointRouting:
    """Snapper backup routes through the endpoint layer (issue #1)."""

    def test_send_snapper_btrfs_receives_into_incoming_temp_slot(self):
        """btrfs dispatch receives into the transactional .snapshots/{num}.incoming slot (NOT
        the final slot), then publishes; the endpoint's base path is restored afterward."""
        from btrfs_backup_ng.core import operations

        ep = MagicMock()
        ep.config = {"path": "/backup/home"}
        ep._is_remote = False

        snap = MagicMock()
        snap.number = 5
        snap.get_backup_name.return_value = "home-5-20240115-120000"
        snap.subvolume_path = Path("/.snapshots/5/snapshot")

        captured = {}

        def fake_send_snapshot(src, dest, parent=None, options=None):
            captured["path"] = dest.config["path"]

        with (
            patch.object(
                operations, "_create_snapper_snapshot_wrapper", return_value=MagicMock()
            ),
            patch.object(operations, "send_snapshot", side_effect=fake_send_snapshot),
            patch.object(operations, "_snapper_run_shell", return_value=(0, "")),
            patch.object(operations, "_snapper_publish_slot") as pub,
            patch.object(operations, "_place_info_xml"),
            patch.object(operations, "_write_snapper_metadata"),
        ):
            operations.send_snapper_snapshot(snap, ep)

        # Received into the .incoming temp slot, never the final one.
        assert captured["path"] == "/backup/home/.snapshots/5.incoming"
        pub.assert_called_once_with(ep, 5)  # then published atomically
        # The endpoint's base path is restored after the transfer.
        assert ep.config["path"] == "/backup/home"

    def test_send_snapper_raw_forces_no_compression(self, tmp_path):
        """Raw dispatch sends on the base endpoint with compression disabled."""
        from btrfs_backup_ng.core import operations
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        ep = RawEndpoint(config={"path": str(tmp_path)})
        snap = MagicMock()
        snap.number = 5
        snap.get_backup_name.return_value = "home-5-20240115-120000"
        snap.subvolume_path = Path("/.snapshots/5/snapshot")

        captured = {}

        def fake_send_snapshot(src, dest, parent=None, options=None):
            captured["options"] = options
            captured["dest"] = dest

        with (
            patch.object(
                operations, "_create_snapper_snapshot_wrapper", return_value=MagicMock()
            ),
            patch.object(operations, "send_snapshot", side_effect=fake_send_snapshot),
            patch.object(operations, "_write_snapper_metadata"),
        ):
            operations.send_snapper_snapshot(snap, ep, options={"compress": "zstd"})

        assert captured["options"]["compress"] == "none"
        assert captured["dest"] is ep

    def test_handle_backup_routes_ssh_through_choose_endpoint(
        self, tmp_path, monkeypatch
    ):
        """`snapper backup <cfg> ssh://...` parses the URL via choose_endpoint and
        passes an endpoint (not a raw string) downstream -- no local 'ssh:' dir."""
        from btrfs_backup_ng.cli import snapper_cmd

        args = argparse.Namespace(
            config="root",
            target="ssh://backup@host:/share/Backups",
            snapshot=None,
            type=None,
            dry_run=False,
            compress=None,
            rate_limit=None,
            verbose=False,
            quiet=False,
            log_level=None,
            min_age="0",
        )
        monkeypatch.chdir(tmp_path)

        mock_scanner = MagicMock()
        mock_scanner.get_config.return_value = MagicMock()

        with (
            patch(
                "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner",
                return_value=mock_scanner,
            ),
            patch("btrfs_backup_ng.endpoint.choose_endpoint") as mock_choose,
            patch(
                "btrfs_backup_ng.core.operations.sync_snapper_snapshots",
                return_value=0,
            ) as mock_sync,
        ):
            mock_choose.return_value = MagicMock(_is_remote=True)
            snapper_cmd._handle_backup(args)

        mock_choose.assert_called_once()
        assert mock_choose.call_args[0][0] == "ssh://backup@host:/share/Backups"
        # sync received the endpoint object, not the raw target string.
        assert mock_sync.call_args[0][2] is mock_choose.return_value
        # Regression: the ssh URL is not turned into a local directory.
        assert not (tmp_path / "ssh:").exists()

    def test_cleanup_targets_only_incoming_temp_never_published_backup(self, tmp_path):
        """DATA-LOSS GUARD: cleanup after a failed transfer touches ONLY the transactional
        .snapshots/{num}.incoming temp -- NEVER the published .snapshots/{num}/snapshot backup.
        It deletes the temp's read-only received subvolume via btrfs subvolume delete."""
        from btrfs_backup_ng.core import operations

        base = tmp_path / "backup"
        ep = MagicMock()
        ep.config = {"path": str(base)}
        ep._is_remote = False

        calls = []

        def record(cmd, *a, **k):
            calls.append(list(cmd))
            return MagicMock(returncode=0, stdout="")

        with (
            patch("btrfs_backup_ng.core.operations.subprocess.run", side_effect=record),
            patch("btrfs_backup_ng.core.operations.os.geteuid", return_value=0),
        ):
            operations._cleanup_snapper_backup(ep, 7, is_raw=False)

        # One privileged shell pass.
        assert len(calls) == 1
        assert calls[0][:2] == ["sh", "-c"]
        script = calls[0][2]
        incoming = str(base / ".snapshots" / "7.incoming")
        published = str(base / ".snapshots" / "7" / "snapshot")
        assert "btrfs subvolume delete" in script
        assert incoming in script  # only the temp is targeted
        assert published not in script  # the published backup is NEVER touched

    def test_handle_backup_threads_ssh_options(self, tmp_path, monkeypatch):
        """--ssh-sudo / --ssh-key reach the endpoint config for ssh targets."""
        from btrfs_backup_ng.cli import snapper_cmd

        args = argparse.Namespace(
            config="root",
            target="ssh://backup@host:/b",
            snapshot=None,
            type=None,
            dry_run=False,
            compress=None,
            rate_limit=None,
            verbose=False,
            quiet=False,
            log_level=None,
            min_age="0",
            ssh_sudo=True,
            ssh_key="/home/u/.ssh/id",
        )
        monkeypatch.chdir(tmp_path)

        mock_scanner = MagicMock()
        mock_scanner.get_config.return_value = MagicMock()

        with (
            patch(
                "btrfs_backup_ng.cli.snapper_cmd.SnapperScanner",
                return_value=mock_scanner,
            ),
            patch("btrfs_backup_ng.endpoint.choose_endpoint") as mock_choose,
            patch(
                "btrfs_backup_ng.core.operations.sync_snapper_snapshots",
                return_value=0,
            ),
        ):
            mock_choose.return_value = MagicMock(_is_remote=True)
            snapper_cmd._handle_backup(args)

        cfg = mock_choose.call_args[0][1]
        assert cfg.get("ssh_sudo") is True
        assert cfg.get("ssh_identity_file") == "/home/u/.ssh/id"


class TestRestoreSnapperdCacheHint:
    """R11: restore prints a snapperd-rescan hint (a)+(c) decision."""

    def _args(self, dry_run=False):
        return argparse.Namespace(
            source="/mnt/backup",
            config="root",
            snapshot=[559],
            all=False,
            list=False,
            dry_run=dry_run,
            json=False,
            verbose=False,
            quiet=False,
            log_level=None,
        )

    def _run(self, args, mock_snapper_configs, capsys):
        from pathlib import Path

        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        with (
            patch("btrfs_backup_ng.cli.snapper_cmd.SnapperScanner") as scanner_cls,
            patch("btrfs_backup_ng.core.restore.list_snapper_backups") as mock_list,
            patch(
                "btrfs_backup_ng.core.restore.restore_snapper_snapshot"
            ) as mock_restore,
        ):
            scanner = MagicMock()
            scanner.get_config.return_value = mock_snapper_configs[0]
            scanner_cls.return_value = scanner
            mock_list.return_value = [{"number": 559, "metadata": MagicMock()}]
            mock_restore.return_value = (1, Path("/x/.snapshots/1/snapshot"))
            result = _handle_restore(args)
        captured = capsys.readouterr()
        # Rich wraps log lines; normalize whitespace before substring checks.
        text = " ".join((captured.out + captured.err).split())
        return result, text

    def test_hint_printed_after_successful_restore(self, capsys, mock_snapper_configs):
        result, text = self._run(self._args(), mock_snapper_configs, capsys)
        assert result == 0
        assert "daemon picks up the restored" in text
        assert "snapper -c root list" in text

    def test_no_hint_on_dry_run(self, capsys, mock_snapper_configs):
        result, text = self._run(self._args(dry_run=True), mock_snapper_configs, capsys)
        assert result == 0
        assert "daemon picks up the restored" not in text


class TestRestoreNameDateSelection:
    """R11b: --backup-name / --date selection + collision dedup + name threading."""

    def _mk_backups(self):
        from datetime import datetime

        from btrfs_backup_ng.snapper.metadata import SnapperMetadata

        def mk(num, name, date):
            return {
                "number": num,
                "metadata": SnapperMetadata(
                    type="single",
                    num=num,
                    date=datetime.fromisoformat(date),
                    description="d",
                ),
                "raw": True,
                "backup_name": name,
            }

        # two backups share number 100 (reused after prune); 3 is unique
        return [
            mk(3, "root-3-x", "2024-01-01T00:00:00"),
            mk(100, "root-100-old", "2024-01-01T00:00:00"),
            mk(100, "root-100-new", "2024-06-01T00:00:00"),
        ]

    def _args(self, **kw):
        base = dict(
            source="raw:///b",
            config="root",
            snapshot=None,
            backup_name=None,
            all=False,
            date=None,
            list=False,
            json=False,
            dry_run=False,
            verbose=False,
            quiet=False,
            log_level=None,
        )
        base.update(kw)
        return argparse.Namespace(**base)

    def _run(self, args, mock_snapper_configs):
        from pathlib import Path

        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        with (
            patch("btrfs_backup_ng.cli.snapper_cmd.SnapperScanner") as scanner_cls,
            patch(
                "btrfs_backup_ng.core.restore.list_snapper_backups",
                return_value=self._mk_backups(),
            ),
            patch(
                "btrfs_backup_ng.core.restore.restore_snapper_snapshot"
            ) as mock_restore,
        ):
            scanner = MagicMock()
            scanner.get_config.return_value = mock_snapper_configs[0]
            scanner_cls.return_value = scanner
            mock_restore.return_value = (1, Path("/x/snapshot"))
            result = _handle_restore(args)
        return result, mock_restore

    @staticmethod
    def _restored_names(mock_restore):
        return [c.kwargs.get("backup_name") for c in mock_restore.call_args_list]

    def test_backup_name_restores_exact_older(self, mock_snapper_configs):
        result, mr = self._run(
            self._args(backup_name=["root-100-old"]), mock_snapper_configs
        )
        assert result == 0
        assert self._restored_names(mr) == ["root-100-old"]

    def test_snapshot_collision_restores_newest_only_and_warns(
        self, capsys, mock_snapper_configs
    ):
        result, mr = self._run(self._args(snapshot=[100]), mock_snapper_configs)
        assert result == 0
        assert self._restored_names(mr) == ["root-100-new"]  # newest only, once
        cap = capsys.readouterr()
        text = " ".join((cap.out + cap.err).split())
        assert "Multiple backups share number 100" in text
        assert "--backup-name or --date" in text

    def test_snapshot_with_date_picks_dated_copy(self, mock_snapper_configs):
        result, mr = self._run(
            self._args(snapshot=[100], date="2024-01-01"), mock_snapper_configs
        )
        assert result == 0
        assert self._restored_names(mr) == ["root-100-old"]  # the 2024-01-01 copy

    def test_all_restores_each_backup_once(self, mock_snapper_configs):
        result, mr = self._run(self._args(all=True), mock_snapper_configs)
        assert result == 0
        assert sorted(self._restored_names(mr)) == [
            "root-100-new",
            "root-100-old",
            "root-3-x",
        ]

    def test_backup_name_not_found_errors_without_restoring(self, mock_snapper_configs):
        result, mr = self._run(
            self._args(backup_name=["root-does-not-exist"]), mock_snapper_configs
        )
        assert result == 1
        assert mr.call_count == 0

    def test_date_with_no_match_errors(self, mock_snapper_configs):
        result, mr = self._run(
            self._args(snapshot=[100], date="1999-12-31"), mock_snapper_configs
        )
        assert result == 1
        assert mr.call_count == 0

    def test_no_selection_errors(self, mock_snapper_configs):
        result, mr = self._run(self._args(), mock_snapper_configs)
        assert result == 1
        assert mr.call_count == 0

    def test_list_table_shows_name_column_and_tip(self, capsys, mock_snapper_configs):
        self._run(self._args(list=True), mock_snapper_configs)
        out = capsys.readouterr().out
        assert "NAME" in out
        assert "root-100-old" in out and "root-100-new" in out
        assert "--backup-name" in out  # the disambiguation tip

    def test_list_json_includes_backup_name(self, capsys, mock_snapper_configs):
        self._run(self._args(list=True, json=True), mock_snapper_configs)
        data = json.loads(capsys.readouterr().out)
        names = [e["backup_name"] for e in data["backups"]]
        assert "root-100-old" in names and "root-100-new" in names

    def test_overlapping_selectors_restore_each_once(self, mock_snapper_configs):
        """The _add() dedup guard: overlapping --backup-name + --snapshot restore once.

        Kills a mutation that removes the guard (which would double-restore the newest).
        """
        result, mr = self._run(
            self._args(backup_name=["root-100-new"], snapshot=[100]),
            mock_snapper_configs,
        )
        assert result == 0
        names = self._restored_names(mr)
        assert names == ["root-100-new"]  # exactly once, not twice
        assert len(names) == len(set(names))

    def test_all_plus_snapshot_collision_no_false_warning(
        self, capsys, mock_snapper_configs
    ):
        """--all --snapshot N restores each once and does NOT emit the newest-only warn."""
        result, mr = self._run(
            self._args(all=True, snapshot=[100]), mock_snapper_configs
        )
        assert result == 0
        assert sorted(self._restored_names(mr)) == [
            "root-100-new",
            "root-100-old",
            "root-3-x",
        ]
        cap = capsys.readouterr()
        text = " ".join((cap.out + cap.err).split())
        assert "Multiple backups share number" not in text  # older IS restored

    def test_date_alone_errors_mentioning_date(self, capsys, mock_snapper_configs):
        """--date with no selector explains it is a filter (not a selector)."""
        result, mr = self._run(self._args(date="2024-01-01"), mock_snapper_configs)
        assert result == 1
        assert mr.call_count == 0
        cap = capsys.readouterr()
        text = " ".join((cap.out + cap.err).split())
        # message names --date and points at combining it
        assert "--date" in text

    def test_iso_t_separator_date_matches(self, mock_snapper_configs):
        """--date with an ISO 'T' separator resolves (normalized to a space)."""
        result, mr = self._run(
            self._args(snapshot=[100], date="2024-06-01T00:00:00"),
            mock_snapper_configs,
        )
        assert result == 0
        assert self._restored_names(mr) == ["root-100-new"]  # the 2024-06-01 copy


class TestBackupSelectionHelpers:
    """R11b: the date-match and recency-key helpers."""

    def _b(self, name, date):
        from datetime import datetime

        from btrfs_backup_ng.snapper.metadata import SnapperMetadata

        return {
            "number": 1,
            "backup_name": name,
            "metadata": SnapperMetadata(
                type="single", num=1, date=datetime.fromisoformat(date)
            ),
        }

    def test_date_matches_prefix(self):
        from btrfs_backup_ng.cli.snapper_cmd import _backup_date_matches

        b = self._b("x", "2024-01-01T12:30:00")
        assert _backup_date_matches(b, "2024-01-01")
        assert _backup_date_matches(b, "2024-01-01 12:30:00")
        assert not _backup_date_matches(b, "2024-01-02")

    def test_date_matches_handles_missing_metadata(self):
        from btrfs_backup_ng.cli.snapper_cmd import _backup_date_matches

        assert not _backup_date_matches({"number": 1}, "2024-01-01")

    def test_recency_key_orders_newest_last(self):
        from btrfs_backup_ng.cli.snapper_cmd import _backup_recency_key

        old = self._b("old", "2024-01-01T00:00:00")
        new = self._b("new", "2024-06-01T00:00:00")
        assert max([old, new], key=_backup_recency_key)["backup_name"] == "new"


class TestRestoreConfigOptionalWithList:
    """0.9.2: CONFIG is optional with --list, required for an actual restore."""

    def _args(self, **kw):
        base = dict(
            source="raw:///b",
            config=None,
            snapshot=None,
            backup_name=None,
            all=False,
            date=None,
            list=False,
            json=False,
            dry_run=False,
            verbose=False,
            quiet=False,
            log_level=None,
        )
        base.update(kw)
        return argparse.Namespace(**base)

    def test_list_without_config_lists(self, capsys):
        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups", return_value=[]
        ) as mock_list:
            result = _handle_restore(self._args(list=True))  # config=None
        assert result == 0
        mock_list.assert_called_once()  # listing proceeded without a CONFIG

    def test_restore_without_config_errors(self, capsys):
        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        result = _handle_restore(self._args(snapshot=[1]))  # config=None, not --list
        assert result == 1
        cap = capsys.readouterr()
        assert "CONFIG is required" in " ".join((cap.out + cap.err).split())


class TestSnapperRestoreDecryptOptions:
    """snapper restore threads --gpg-keyring/--openssl-cipher into endpoint_options.

    These carry through _restore_endpoint_config into the raw endpoint that decodes an
    encrypted raw snapper backup. Threaded only when supplied so the sidecar-
    recorded cipher stays authoritative.
    """

    @staticmethod
    def _args(**over):
        base = dict(
            source="raw://backup",
            config="root",
            snapshot=None,
            list=True,
            dry_run=False,
            json=False,
            verbose=False,
            quiet=False,
            log_level=None,
            ssh_sudo=False,
            ssh_key=None,
            ssh_auth_sock=None,
            ssh_host_key_policy=None,
            gpg_keyring=None,
            openssl_cipher=None,
        )
        base.update(over)
        return argparse.Namespace(**base)

    def test_keyring_and_cipher_threaded(self):
        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        args = self._args(gpg_keyring="/kr.gpg", openssl_cipher="aes-256-cbc")
        with patch("btrfs_backup_ng.core.restore.list_snapper_backups") as mock_list:
            mock_list.return_value = []
            _handle_restore(args)
        opts = mock_list.call_args.args[1]
        assert opts["gpg_keyring"] == "/kr.gpg"
        assert opts["openssl_cipher"] == "aes-256-cbc"

    def test_absent_flags_not_threaded(self):
        from btrfs_backup_ng.cli.snapper_cmd import _handle_restore

        args = self._args()
        with patch("btrfs_backup_ng.core.restore.list_snapper_backups") as mock_list:
            mock_list.return_value = []
            _handle_restore(args)
        opts = mock_list.call_args.args[1]
        assert "gpg_keyring" not in opts
        assert "openssl_cipher" not in opts
