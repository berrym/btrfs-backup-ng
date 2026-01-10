"""Tests for config schema dataclasses."""

from btrfs_backup_ng.config.schema import (
    Config,
    EmailNotificationConfig,
    GlobalConfig,
    NotificationConfig,
    RetentionConfig,
    TargetConfig,
    VolumeConfig,
    WebhookNotificationConfig,
)


class TestRetentionConfig:
    """Tests for RetentionConfig dataclass."""

    def test_default_values(self):
        """Test default retention values."""
        retention = RetentionConfig()
        assert retention.min == "1d"
        assert retention.hourly == 24
        assert retention.daily == 7
        assert retention.weekly == 4
        assert retention.monthly == 12
        assert retention.yearly == 0

    def test_custom_values(self):
        """Test custom retention values."""
        retention = RetentionConfig(
            min="2d",
            hourly=48,
            daily=14,
            weekly=8,
            monthly=6,
            yearly=2,
        )
        assert retention.min == "2d"
        assert retention.hourly == 48
        assert retention.daily == 14
        assert retention.weekly == 8
        assert retention.monthly == 6
        assert retention.yearly == 2


class TestNotificationConfig:
    """Tests for NotificationConfig dataclass."""

    def test_is_enabled_default(self):
        """Test is_enabled returns False by default."""
        config = NotificationConfig()
        assert config.is_enabled() is False

    def test_is_enabled_with_email(self):
        """Test is_enabled returns True when email is enabled."""
        config = NotificationConfig(
            email=EmailNotificationConfig(enabled=True, to_addrs=["test@example.com"])
        )
        assert config.is_enabled() is True

    def test_is_enabled_with_webhook(self):
        """Test is_enabled returns True when webhook is enabled."""
        config = NotificationConfig(
            webhook=WebhookNotificationConfig(enabled=True, url="http://example.com")
        )
        assert config.is_enabled() is True

    def test_is_enabled_with_both(self):
        """Test is_enabled returns True when both are enabled."""
        config = NotificationConfig(
            email=EmailNotificationConfig(enabled=True, to_addrs=["test@example.com"]),
            webhook=WebhookNotificationConfig(enabled=True, url="http://example.com"),
        )
        assert config.is_enabled() is True


class TestTargetConfig:
    """Tests for TargetConfig dataclass."""

    def test_required_path(self):
        """Test that path is required."""
        target = TargetConfig(path="/mnt/backup")
        assert target.path == "/mnt/backup"

    def test_default_values(self):
        """Test default target values."""
        target = TargetConfig(path="/mnt/backup")
        assert target.ssh_sudo is False
        assert target.ssh_port == 22
        assert target.ssh_key is None
        assert target.ssh_password_auth is True
        assert target.compress == "none"
        assert target.rate_limit is None

    def test_ssh_target(self):
        """Test SSH target configuration."""
        target = TargetConfig(
            path="ssh://backup@server:/backups",
            ssh_sudo=True,
            ssh_port=2222,
            ssh_key="~/.ssh/backup_key",
            compress="zstd",
            rate_limit="10M",
        )
        assert target.path == "ssh://backup@server:/backups"
        assert target.ssh_sudo is True
        assert target.ssh_port == 2222
        assert target.ssh_key == "~/.ssh/backup_key"
        assert target.compress == "zstd"
        assert target.rate_limit == "10M"


class TestVolumeConfig:
    """Tests for VolumeConfig dataclass."""

    def test_required_path(self):
        """Test that path is required."""
        volume = VolumeConfig(path="/home")
        assert volume.path == "/home"

    def test_default_values(self):
        """Test default volume values."""
        volume = VolumeConfig(path="/home")
        assert volume.snapshot_dir == ".snapshots"
        assert volume.targets == []
        assert volume.retention is None
        assert volume.enabled is True

    def test_auto_prefix_from_path(self):
        """Test automatic snapshot prefix generation (includes trailing dash)."""
        volume = VolumeConfig(path="/home")
        assert volume.snapshot_prefix == "home-"

        volume = VolumeConfig(path="/var/log")
        assert volume.snapshot_prefix == "var-log-"

        volume = VolumeConfig(path="/")
        assert volume.snapshot_prefix == "root-"

    def test_custom_prefix(self):
        """Test custom snapshot prefix."""
        volume = VolumeConfig(path="/home", snapshot_prefix="my-prefix-")
        assert volume.snapshot_prefix == "my-prefix-"

    def test_with_targets(self):
        """Test volume with targets."""
        targets = [
            TargetConfig(path="/mnt/backup"),
            TargetConfig(path="ssh://server:/backup", ssh_sudo=True),
        ]
        volume = VolumeConfig(path="/home", targets=targets)
        assert len(volume.targets) == 2
        assert volume.targets[0].path == "/mnt/backup"
        assert volume.targets[1].ssh_sudo is True

    def test_with_retention_override(self):
        """Test volume with retention override."""
        retention = RetentionConfig(daily=14, weekly=8)
        volume = VolumeConfig(path="/home", retention=retention)
        assert volume.retention is not None
        assert volume.retention.daily == 14
        assert volume.retention.weekly == 8


class TestGlobalConfig:
    """Tests for GlobalConfig dataclass."""

    def test_default_values(self):
        """Test default global config values."""
        config = GlobalConfig()
        assert config.snapshot_dir == ".snapshots"
        assert config.timestamp_format == "%Y%m%d-%H%M%S"
        assert config.incremental is True
        assert config.log_file is None
        assert config.parallel_volumes == 2
        assert config.parallel_targets == 3
        assert config.quiet is False
        assert config.verbose is False

    def test_default_retention(self):
        """Test default retention in global config."""
        config = GlobalConfig()
        assert config.retention.min == "1d"
        assert config.retention.daily == 7

    def test_custom_values(self):
        """Test custom global config values."""
        config = GlobalConfig(
            snapshot_dir="/snapshots",
            incremental=False,
            log_file="/var/log/backup.log",
            parallel_volumes=4,
        )
        assert config.snapshot_dir == "/snapshots"
        assert config.incremental is False
        assert config.log_file == "/var/log/backup.log"
        assert config.parallel_volumes == 4


class TestConfig:
    """Tests for root Config dataclass."""

    def test_default_values(self):
        """Test default config values."""
        config = Config()
        assert config.global_config is not None
        assert config.volumes == []

    def test_get_enabled_volumes(self):
        """Test get_enabled_volumes method."""
        volumes = [
            VolumeConfig(path="/home", enabled=True),
            VolumeConfig(path="/var", enabled=False),
            VolumeConfig(path="/data", enabled=True),
        ]
        config = Config(volumes=volumes)

        enabled = config.get_enabled_volumes()
        assert len(enabled) == 2
        assert enabled[0].path == "/home"
        assert enabled[1].path == "/data"

    def test_get_effective_retention_global(self):
        """Test effective retention uses global when volume has none."""
        global_config = GlobalConfig(retention=RetentionConfig(daily=7, weekly=4))
        volume = VolumeConfig(path="/home")  # No retention override
        config = Config(global_config=global_config, volumes=[volume])

        effective = config.get_effective_retention(volume)
        assert effective.daily == 7
        assert effective.weekly == 4

    def test_get_effective_retention_override(self):
        """Test effective retention uses volume override."""
        global_config = GlobalConfig(retention=RetentionConfig(daily=7, weekly=4))
        volume_retention = RetentionConfig(daily=14, weekly=8)
        volume = VolumeConfig(path="/home", retention=volume_retention)
        config = Config(global_config=global_config, volumes=[volume])

        effective = config.get_effective_retention(volume)
        assert effective.daily == 14
        assert effective.weekly == 8
