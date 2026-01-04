"""Integration tests for configuration loading and execution flow.

Tests the complete path from loading a config file to planning operations.
"""

import pytest

from btrfs_backup_ng.config.loader import load_config


class TestConfigLoadingFlow:
    """Test complete config loading and validation flow."""

    def test_load_and_validate_minimal_config(self, tmp_path):
        """Test loading a minimal valid configuration."""
        config_content = """
[[volumes]]
path = "/home"

[[volumes.targets]]
path = "/mnt/backup/home"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        config, warnings = load_config(config_file)

        assert config is not None
        assert len(config.volumes) == 1
        assert config.volumes[0].path == "/home"
        assert len(config.volumes[0].targets) == 1

    def test_load_full_config_with_all_options(self, tmp_path):
        """Test loading a comprehensive configuration."""
        config_content = """
[global]
snapshot_dir = ".snapshots"
timestamp_format = "%Y%m%d-%H%M%S"
incremental = true
parallel_volumes = 2
parallel_targets = 3

[global.retention]
min = "2d"
hourly = 48
daily = 14
weekly = 8
monthly = 24

[[volumes]]
path = "/home"
snapshot_prefix = "home-"
enabled = true

[volumes.retention]
daily = 30

[[volumes.targets]]
path = "/mnt/backup/home"
compress = "zstd"

[[volumes.targets]]
path = "ssh://backup@server:/backups/home"
ssh_sudo = true
compress = "lz4"
rate_limit = "50M"

[[volumes]]
path = "/var/log"
snapshot_prefix = "logs-"
enabled = true

[[volumes.targets]]
path = "/mnt/backup/logs"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        config, warnings = load_config(config_file)

        # Verify global settings
        assert config.global_config.snapshot_dir == ".snapshots"
        assert config.global_config.incremental is True
        assert config.global_config.parallel_volumes == 2
        assert config.global_config.parallel_targets == 3

        # Verify global retention
        assert config.global_config.retention.min == "2d"
        assert config.global_config.retention.hourly == 48
        assert config.global_config.retention.daily == 14

        # Verify volumes
        assert len(config.volumes) == 2

        # First volume
        home_vol = config.volumes[0]
        assert home_vol.path == "/home"
        assert home_vol.snapshot_prefix == "home-"
        assert len(home_vol.targets) == 2
        assert home_vol.retention.daily == 30  # Override

        # SSH target settings
        ssh_target = home_vol.targets[1]
        assert "ssh://" in ssh_target.path
        assert ssh_target.ssh_sudo is True
        assert ssh_target.compress == "lz4"
        assert ssh_target.rate_limit == "50M"

    def test_config_warnings_for_issues(self, tmp_path):
        """Test that appropriate warnings are generated."""
        config_content = """
[[volumes]]
path = "/home"
enabled = false

[[volumes.targets]]
path = "/mnt/backup/home"

[[volumes]]
path = "/data"
# No targets defined
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        config, warnings = load_config(config_file)

        # Should have warnings about disabled volume and missing targets
        assert len(warnings) > 0

    def test_get_enabled_volumes(self, tmp_path):
        """Test filtering to only enabled volumes."""
        config_content = """
[[volumes]]
path = "/home"
enabled = true

[[volumes.targets]]
path = "/backup/home"

[[volumes]]
path = "/var"
enabled = false

[[volumes.targets]]
path = "/backup/var"

[[volumes]]
path = "/data"
# enabled defaults to true

[[volumes.targets]]
path = "/backup/data"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        config, warnings = load_config(config_file)
        enabled = config.get_enabled_volumes()

        assert len(enabled) == 2
        paths = [v.path for v in enabled]
        assert "/home" in paths
        assert "/data" in paths
        assert "/var" not in paths

    def test_effective_retention_inheritance(self, tmp_path):
        """Test that retention settings are properly inherited."""
        config_content = """
[global.retention]
min = "1d"
hourly = 24
daily = 7
weekly = 4
monthly = 12

[[volumes]]
path = "/home"

[[volumes.targets]]
path = "/backup/home"

[[volumes]]
path = "/data"

[volumes.retention]
daily = 30
weekly = 8

[[volumes.targets]]
path = "/backup/data"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        config, warnings = load_config(config_file)

        # First volume should use global retention
        home_retention = config.get_effective_retention(config.volumes[0])
        assert home_retention.daily == 7
        assert home_retention.weekly == 4

        # Second volume should use overridden values
        data_retention = config.get_effective_retention(config.volumes[1])
        assert data_retention.daily == 30
        assert data_retention.weekly == 8
        # But inherit non-overridden values
        assert data_retention.hourly == 24
        assert data_retention.monthly == 12


class TestConfigToOperationPlanning:
    """Test translating config to operation plans."""

    def test_plan_snapshot_operations(self, tmp_path):
        """Test planning snapshot operations from config."""
        config_content = """
[global]
snapshot_dir = ".snapshots"

[[volumes]]
path = "/home"
snapshot_prefix = "home-"

[[volumes.targets]]
path = "/mnt/backup/home"

[[volumes]]
path = "/var/log"
snapshot_prefix = "logs-"

[[volumes.targets]]
path = "/mnt/backup/logs"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        config, _ = load_config(config_file)
        enabled = config.get_enabled_volumes()

        # Verify we can plan operations for each volume
        for volume in enabled:
            assert volume.path is not None
            assert volume.snapshot_prefix is not None
            assert len(volume.targets) > 0

            for target in volume.targets:
                assert target.path is not None

    def test_plan_with_ssh_targets(self, tmp_path):
        """Test planning operations with SSH targets."""
        config_content = """
[[volumes]]
path = "/home"
snapshot_prefix = "home-"

[[volumes.targets]]
path = "ssh://backup@server:/backups/home"
ssh_sudo = true
ssh_key = "/root/.ssh/backup_key"
compress = "zstd"
rate_limit = "10M"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        config, _ = load_config(config_file)
        target = config.volumes[0].targets[0]

        # Verify SSH-specific settings are preserved
        assert target.path.startswith("ssh://")
        assert target.ssh_sudo is True
        assert target.ssh_key == "/root/.ssh/backup_key"
        assert target.compress == "zstd"
        assert target.rate_limit == "10M"


class TestConfigErrorHandling:
    """Test error handling in config loading."""

    def test_missing_volume_path_error(self, tmp_path):
        """Test error when volume path is missing."""
        config_content = """
[[volumes]]
snapshot_prefix = "test-"

[[volumes.targets]]
path = "/backup"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        from btrfs_backup_ng.config.loader import ConfigError

        with pytest.raises(ConfigError, match="path"):
            load_config(config_file)

    def test_missing_target_path_error(self, tmp_path):
        """Test error when target path is missing."""
        config_content = """
[[volumes]]
path = "/home"

[[volumes.targets]]
compress = "zstd"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        from btrfs_backup_ng.config.loader import ConfigError

        with pytest.raises(ConfigError, match="path"):
            load_config(config_file)

    def test_invalid_compression_method_error(self, tmp_path):
        """Test error for invalid compression method."""
        config_content = """
[[volumes]]
path = "/home"

[[volumes.targets]]
path = "/backup"
compress = "invalid_compression"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        from btrfs_backup_ng.config.loader import ConfigError

        with pytest.raises(ConfigError, match="[Cc]ompression"):
            load_config(config_file)

    def test_invalid_toml_syntax_error(self, tmp_path):
        """Test error for invalid TOML syntax."""
        config_content = """
[[volumes]
path = "/home"  # Missing closing bracket
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        from btrfs_backup_ng.config.loader import ConfigError

        with pytest.raises(ConfigError, match="TOML"):
            load_config(config_file)
