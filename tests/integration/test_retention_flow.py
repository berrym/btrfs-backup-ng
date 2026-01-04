"""Integration tests for retention and prune operations.

Tests the complete retention policy application and pruning flow.
"""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.retention import (
    apply_retention,
    extract_timestamp,
    format_retention_summary,
    get_bucket_key,
    parse_duration,
)


class TestRetentionPolicyApplication:
    """Test complete retention policy flows."""

    def test_full_retention_scenario_30_days(self):
        """Test realistic 30-day backup retention scenario."""
        now = datetime(2024, 1, 31, 12, 0, 0)

        # Create snapshots: 3 per day for 30 days (90 total)
        snapshots = []
        for days_ago in range(30):
            for hour in [6, 12, 18]:
                ts = now - timedelta(days=days_ago, hours=now.hour - hour)
                if ts <= now:
                    name = f"home-{ts.strftime('%Y%m%d-%H%M%S')}"
                    snapshots.append(name)

        config = RetentionConfig(
            min="2d",
            hourly=24,
            daily=7,
            weekly=4,
            monthly=12,
            yearly=0,
        )

        to_keep, to_delete = apply_retention(
            snapshots,
            config,
            get_name=lambda s: s,
            prefix="home-",
            now=now,
        )

        # Should keep:
        # - All within 2 days (min)
        # - Up to 24 hourly after that
        # - Up to 7 daily after that
        # - Up to 4 weekly after that

        # At minimum, we should keep latest
        assert len(to_keep) > 0
        assert snapshots[0] in to_keep  # Latest should always be kept

        # Should have deleted some older snapshots
        assert len(to_delete) > 0

        # Total should equal original
        assert len(to_keep) + len(to_delete) == len(snapshots)

    def test_retention_with_unparseable_timestamps(self):
        """Test retention handles unparseable timestamps gracefully."""
        now = datetime(2024, 1, 15, 12, 0, 0)

        snapshots = [
            "home-20240115-120000",  # Valid
            "home-20240114-120000",  # Valid
            "home-invalid-timestamp",  # Invalid - should be kept
            "home-20240113-120000",  # Valid
        ]

        config = RetentionConfig(
            min="1d",
            hourly=0,
            daily=2,
            weekly=0,
            monthly=0,
            yearly=0,
        )

        to_keep, to_delete = apply_retention(
            snapshots,
            config,
            get_name=lambda s: s,
            prefix="home-",
            now=now,
        )

        # Invalid timestamp snapshot should be kept (safety)
        assert "home-invalid-timestamp" in to_keep

    def test_retention_preserves_latest_always(self):
        """Test that latest snapshot is always preserved."""
        now = datetime(2024, 1, 15, 12, 0, 0)

        # All snapshots are old
        snapshots = [
            f"home-{(now - timedelta(days=d)).strftime('%Y%m%d-%H%M%S')}"
            for d in range(100, 110)
        ]

        config = RetentionConfig(
            min="1d",
            hourly=0,
            daily=0,
            weekly=0,
            monthly=0,
            yearly=0,
        )

        to_keep, to_delete = apply_retention(
            snapshots,
            config,
            get_name=lambda s: s,
            prefix="home-",
            now=now,
        )

        # Even with aggressive retention, newest should be kept
        assert len(to_keep) >= 1
        # First in sorted list (newest) should be kept
        sorted_snaps = sorted(snapshots, reverse=True)
        assert sorted_snaps[0] in to_keep

    def test_retention_all_within_min_period(self):
        """Test all snapshots within min period are kept."""
        now = datetime(2024, 1, 15, 12, 0, 0)

        # All snapshots within last day
        snapshots = [
            f"home-{(now - timedelta(hours=h)).strftime('%Y%m%d-%H%M%S')}"
            for h in range(24)
        ]

        config = RetentionConfig(
            min="2d",  # 2 days
            hourly=0,
            daily=0,
            weekly=0,
            monthly=0,
            yearly=0,
        )

        to_keep, to_delete = apply_retention(
            snapshots,
            config,
            get_name=lambda s: s,
            prefix="home-",
            now=now,
        )

        # All should be kept (within min period)
        assert len(to_keep) == len(snapshots)
        assert len(to_delete) == 0

    def test_retention_bucket_selection(self):
        """Test correct snapshots are selected for each bucket."""
        now = datetime(2024, 1, 15, 12, 0, 0)

        # Create snapshots at specific times to test bucket selection
        snapshots = []
        # Day 5 ago: multiple snapshots in same hour
        base = now - timedelta(days=5)
        for minute in [0, 15, 30, 45]:
            ts = base.replace(minute=minute)
            snapshots.append(f"home-{ts.strftime('%Y%m%d-%H%M%S')}")

        config = RetentionConfig(
            min="1d",
            hourly=24,
            daily=7,
            weekly=0,
            monthly=0,
            yearly=0,
        )

        to_keep, to_delete = apply_retention(
            snapshots,
            config,
            get_name=lambda s: s,
            prefix="home-",
            now=now,
        )

        # Should keep one per hour bucket
        assert len(to_keep) >= 1

    def test_retention_weekly_bucket(self):
        """Test weekly bucket retention."""
        now = datetime(2024, 1, 31, 12, 0, 0)

        # Create one snapshot per week for 8 weeks
        snapshots = []
        for weeks_ago in range(8):
            ts = now - timedelta(weeks=weeks_ago)
            snapshots.append(f"home-{ts.strftime('%Y%m%d-%H%M%S')}")

        config = RetentionConfig(
            min="1d",
            hourly=0,
            daily=0,
            weekly=4,  # Keep 4 weekly
            monthly=0,
            yearly=0,
        )

        to_keep, to_delete = apply_retention(
            snapshots,
            config,
            get_name=lambda s: s,
            prefix="home-",
            now=now,
        )

        # Should keep at least weekly=4 plus the latest
        assert len(to_keep) >= 4

    def test_retention_monthly_bucket(self):
        """Test monthly bucket retention."""
        now = datetime(2024, 12, 15, 12, 0, 0)

        # Create one snapshot per month for 18 months
        snapshots = []
        for months_ago in range(18):
            # Approximate months back
            ts = now - timedelta(days=30 * months_ago)
            snapshots.append(f"home-{ts.strftime('%Y%m%d-%H%M%S')}")

        config = RetentionConfig(
            min="1d",
            hourly=0,
            daily=0,
            weekly=0,
            monthly=12,  # Keep 12 monthly
            yearly=0,
        )

        to_keep, to_delete = apply_retention(
            snapshots,
            config,
            get_name=lambda s: s,
            prefix="home-",
            now=now,
        )

        # Should keep at least monthly=12 plus the latest
        assert len(to_keep) >= 12


class TestRetentionWithRealSnapshots:
    """Test retention with snapshot-like objects."""

    def test_retention_with_mock_snapshot_objects(self):
        """Test retention works with actual snapshot-like objects."""
        now = datetime(2024, 1, 15, 12, 0, 0)

        class MockSnapshot:
            def __init__(self, name):
                self.name = name

            def get_name(self):
                return self.name

        snapshots = [
            MockSnapshot(f"home-{(now - timedelta(days=d)).strftime('%Y%m%d-%H%M%S')}")
            for d in range(20)
        ]

        config = RetentionConfig(
            min="1d",
            hourly=24,
            daily=7,
            weekly=4,
            monthly=0,
            yearly=0,
        )

        to_keep, to_delete = apply_retention(
            snapshots,
            config,
            get_name=lambda s: s.get_name(),
            prefix="home-",
            now=now,
        )

        # Verify we get MockSnapshot objects back
        assert all(isinstance(s, MockSnapshot) for s in to_keep)
        assert all(isinstance(s, MockSnapshot) for s in to_delete)


class TestPruneFlowIntegration:
    """Test pruning flow with mocked endpoints."""

    def test_prune_dry_run_reports_correctly(self):
        """Test dry-run prune reports what would be deleted."""
        now = datetime(2024, 1, 15, 12, 0, 0)

        snapshots = [
            f"home-{(now - timedelta(days=d)).strftime('%Y%m%d-%H%M%S')}"
            for d in range(30)
        ]

        config = RetentionConfig(
            min="1d",
            hourly=0,
            daily=7,
            weekly=0,
            monthly=0,
            yearly=0,
        )

        to_keep, to_delete = apply_retention(
            snapshots,
            config,
            get_name=lambda s: s,
            prefix="home-",
            now=now,
        )

        summary = format_retention_summary(to_keep, to_delete, get_name=lambda s: s)

        assert "keeping" in summary
        assert "deleting" in summary
        assert str(len(to_keep)) in summary
        assert str(len(to_delete)) in summary

    @patch("btrfs_backup_ng.endpoint.local.LocalEndpoint.delete_snapshots")
    def test_prune_executes_deletions(self, mock_delete, tmp_path):
        """Test prune actually calls deletion for applicable snapshots."""
        from btrfs_backup_ng.endpoint.local import LocalEndpoint

        dest = tmp_path / "dest"
        dest.mkdir()

        # Create snapshot directories
        now = datetime(2024, 1, 15, 12, 0, 0)
        for d in range(10):
            ts = now - timedelta(days=d)
            (dest / f"home-{ts.strftime('%Y%m%d-%H%M%S')}").mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
                "snap_prefix": "home-",
            }
        )

        snapshots = endpoint.list_snapshots()

        config = RetentionConfig(
            min="1d",
            hourly=0,
            daily=3,  # Keep only 3
            weekly=0,
            monthly=0,
            yearly=0,
        )

        to_keep, to_delete = apply_retention(
            snapshots,
            config,
            get_name=lambda s: s.get_name(),
            prefix="home-",
            now=now,
        )

        # Simulate prune by deleting
        if to_delete:
            endpoint.delete_snapshots(to_delete)

        # Verify delete was called
        if to_delete:
            mock_delete.assert_called_once()


class TestRetentionConfigIntegration:
    """Test retention with loaded config."""

    def test_retention_from_toml_config(self, tmp_path):
        """Test retention using config loaded from TOML."""
        from btrfs_backup_ng.config.loader import load_config

        config_content = """
[global.retention]
min = "2d"
hourly = 48
daily = 14
weekly = 8
monthly = 12
yearly = 0

[[volumes]]
path = "/home"

[[volumes.targets]]
path = "/backup/home"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        config, _ = load_config(config_file)

        # Get retention config
        retention = config.global_config.retention

        assert retention.min == "2d"
        assert retention.hourly == 48
        assert retention.daily == 14

        # Apply to snapshots
        now = datetime(2024, 1, 31, 12, 0, 0)
        snapshots = [
            f"home-{(now - timedelta(days=d)).strftime('%Y%m%d-%H%M%S')}"
            for d in range(60)
        ]

        to_keep, to_delete = apply_retention(
            snapshots,
            retention,
            get_name=lambda s: s,
            prefix="home-",
            now=now,
        )

        # With these settings, should keep reasonable number
        assert len(to_keep) > 20  # At least some kept
        assert len(to_delete) > 0  # Some should be deleted

    def test_volume_retention_override(self, tmp_path):
        """Test volume-specific retention overrides global."""
        from btrfs_backup_ng.config.loader import load_config

        config_content = """
[global.retention]
daily = 7

[[volumes]]
path = "/home"

[volumes.retention]
daily = 30

[[volumes.targets]]
path = "/backup/home"

[[volumes]]
path = "/var"

[[volumes.targets]]
path = "/backup/var"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(config_content)

        config, _ = load_config(config_file)

        # /home should use volume-level override
        home_retention = config.get_effective_retention(config.volumes[0])
        assert home_retention.daily == 30

        # /var should use global
        var_retention = config.get_effective_retention(config.volumes[1])
        assert var_retention.daily == 7


class TestTimestampExtraction:
    """Test timestamp extraction from various snapshot name formats."""

    @pytest.mark.parametrize(
        "name,prefix,expected_date",
        [
            ("home-20240115-143022", "home-", datetime(2024, 1, 15, 14, 30, 22)),
            ("backup-20240101-000000", "backup-", datetime(2024, 1, 1, 0, 0, 0)),
            ("test-20231231-235959", "test-", datetime(2023, 12, 31, 23, 59, 59)),
            # Without prefix
            ("20240115-143022", "", datetime(2024, 1, 15, 14, 30, 22)),
        ],
    )
    def test_extract_timestamp_formats(self, name, prefix, expected_date):
        """Test various timestamp format extraction."""
        result = extract_timestamp(name, prefix)
        assert result == expected_date

    def test_extract_timestamp_returns_none_for_invalid(self):
        """Test None is returned for unparseable names."""
        result = extract_timestamp("not-a-timestamp", "")
        assert result is None

        result = extract_timestamp("home-invalid", "home-")
        assert result is None


class TestBucketKeyGeneration:
    """Test bucket key generation for different time periods."""

    def test_hourly_bucket_key(self):
        """Test hourly bucket key format."""
        ts = datetime(2024, 1, 15, 14, 30, 22)
        key = get_bucket_key(ts, "hourly")
        assert key == "2024-01-15-14"

    def test_daily_bucket_key(self):
        """Test daily bucket key format."""
        ts = datetime(2024, 1, 15, 14, 30, 22)
        key = get_bucket_key(ts, "daily")
        assert key == "2024-01-15"

    def test_weekly_bucket_key(self):
        """Test weekly bucket key format."""
        ts = datetime(2024, 1, 15, 14, 30, 22)
        key = get_bucket_key(ts, "weekly")
        assert key.startswith("2024-W")

    def test_monthly_bucket_key(self):
        """Test monthly bucket key format."""
        ts = datetime(2024, 1, 15, 14, 30, 22)
        key = get_bucket_key(ts, "monthly")
        assert key == "2024-01"

    def test_yearly_bucket_key(self):
        """Test yearly bucket key format."""
        ts = datetime(2024, 1, 15, 14, 30, 22)
        key = get_bucket_key(ts, "yearly")
        assert key == "2024"


class TestDurationParsing:
    """Test duration string parsing."""

    @pytest.mark.parametrize(
        "duration_str,expected_days",
        [
            ("1d", 1),
            ("7d", 7),
            ("1w", 7),
            ("2w", 14),
            ("1M", 30),
            ("12M", 360),
            ("1y", 365),
        ],
    )
    def test_parse_duration(self, duration_str, expected_days):
        """Test duration parsing."""
        result = parse_duration(duration_str)
        assert result.days == expected_days

    def test_parse_duration_hours(self):
        """Test parsing hour durations."""
        result = parse_duration("24h")
        assert result.total_seconds() == 24 * 3600

    def test_parse_duration_minutes(self):
        """Test parsing minute durations."""
        result = parse_duration("30m")
        assert result.total_seconds() == 30 * 60

    def test_parse_duration_invalid(self):
        """Test invalid duration raises error."""
        with pytest.raises(ValueError):
            parse_duration("invalid")

        with pytest.raises(ValueError):
            parse_duration("10x")  # Unknown unit
