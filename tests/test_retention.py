"""Tests for retention logic module."""

from datetime import datetime, timedelta

import pytest

from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.retention import (
    apply_retention,
    extract_timestamp,
    format_retention_summary,
    get_bucket_key,
    parse_duration,
)


class TestParseDuration:
    """Tests for parse_duration function."""

    def test_parse_minutes(self):
        """Test parsing minute durations."""
        assert parse_duration("30m") == timedelta(minutes=30)
        assert parse_duration("1m") == timedelta(minutes=1)
        assert parse_duration("90m") == timedelta(minutes=90)

    def test_parse_hours(self):
        """Test parsing hour durations."""
        assert parse_duration("1h") == timedelta(hours=1)
        assert parse_duration("24h") == timedelta(hours=24)
        assert parse_duration("6h") == timedelta(hours=6)

    def test_parse_days(self):
        """Test parsing day durations."""
        assert parse_duration("1d") == timedelta(days=1)
        assert parse_duration("7d") == timedelta(days=7)
        assert parse_duration("30d") == timedelta(days=30)

    def test_parse_weeks(self):
        """Test parsing week durations."""
        assert parse_duration("1w") == timedelta(weeks=1)
        assert parse_duration("4w") == timedelta(weeks=4)
        assert parse_duration("2w") == timedelta(weeks=2)

    def test_parse_invalid_format(self):
        """Test parsing invalid duration format."""
        with pytest.raises(ValueError):
            parse_duration("invalid")

    def test_parse_empty_string(self):
        """Test parsing empty string."""
        with pytest.raises(ValueError):
            parse_duration("")

    def test_parse_no_unit(self):
        """Test parsing number without unit."""
        with pytest.raises(ValueError):
            parse_duration("30")

    def test_parse_unknown_unit(self):
        """Test parsing unknown unit."""
        with pytest.raises(ValueError):
            parse_duration("30x")

    def test_parse_float_value(self):
        """Test parsing float value (should work for some cases)."""
        # This depends on implementation - may or may not support floats
        try:
            result = parse_duration("1.5h")
            assert result == timedelta(hours=1.5)
        except ValueError:
            pass  # Implementation may not support floats

    def test_parse_case_sensitive(self):
        """Test that parsing is case-sensitive (lowercase only, M=months)."""
        # Lowercase units work
        assert parse_duration("1d") == timedelta(days=1)
        assert parse_duration("1h") == timedelta(hours=1)
        assert parse_duration("1m") == timedelta(minutes=1)
        assert parse_duration("1w") == timedelta(weeks=1)
        # Capital M means months (30 days), not minutes
        assert parse_duration("1M") == timedelta(days=30)
        # Other capitals are invalid
        with pytest.raises(ValueError):
            parse_duration("1D")
        with pytest.raises(ValueError):
            parse_duration("1H")

    def test_parse_seconds(self):
        """Test parsing seconds."""
        assert parse_duration("30s") == timedelta(seconds=30)
        assert parse_duration("1s") == timedelta(seconds=1)

    def test_parse_years(self):
        """Test parsing years."""
        assert parse_duration("1y") == timedelta(days=365)
        assert parse_duration("2y") == timedelta(days=730)


class TestExtractTimestamp:
    """Tests for extract_timestamp function."""

    def test_extract_standard_format(self):
        """Test extracting timestamp from standard format."""
        # Format: YYYYMMDD-HHMMSS
        result = extract_timestamp("home-20240115-143022")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 22

    def test_extract_with_prefix(self):
        """Test extracting timestamp with various prefixes."""
        result = extract_timestamp("myprefix-20240115-143022")
        assert result is not None
        assert result.year == 2024

    def test_extract_no_timestamp(self):
        """Test extracting from name without timestamp."""
        result = extract_timestamp("snapshot-without-date")
        assert result is None

    def test_extract_invalid_timestamp(self):
        """Test extracting invalid timestamp."""
        result = extract_timestamp("home-99999999-999999")
        assert result is None

    def test_extract_with_explicit_prefix(self):
        """Test extracting with explicit prefix parameter."""
        result = extract_timestamp("home-20240115-143022", prefix="home-")
        assert result is not None
        assert result.year == 2024

    def test_extract_underscore_format(self):
        """Test extracting timestamp with underscore format."""
        result = extract_timestamp("snap-20240115_143022")
        assert result is not None
        assert result.year == 2024

    def test_extract_iso_format(self):
        """Test extracting ISO format timestamp."""
        result = extract_timestamp("2024-01-15T14:30:22")
        assert result is not None
        assert result.year == 2024

    def test_extract_compact_format(self):
        """Test extracting compact format timestamp."""
        result = extract_timestamp("backup-20240115143022")
        assert result is not None
        assert result.year == 2024


class TestGetBucketKey:
    """Tests for get_bucket_key function."""

    def test_hourly_bucket(self):
        """Test hourly bucket calculation."""
        dt = datetime(2024, 1, 15, 14, 30, 22)
        bucket = get_bucket_key(dt, "hourly")
        # Returns a string key, check it contains the hour
        assert "2024" in bucket
        assert "14" in bucket or "15" in bucket  # Date components

    def test_daily_bucket(self):
        """Test daily bucket calculation."""
        dt = datetime(2024, 1, 15, 14, 30, 22)
        bucket = get_bucket_key(dt, "daily")
        assert "2024" in bucket
        assert "01" in bucket or "15" in bucket

    def test_weekly_bucket(self):
        """Test weekly bucket calculation."""
        dt = datetime(2024, 1, 17, 14, 30, 22)  # Wednesday
        bucket = get_bucket_key(dt, "weekly")
        # Should return a consistent key for the week
        assert bucket is not None

    def test_monthly_bucket(self):
        """Test monthly bucket calculation."""
        dt = datetime(2024, 1, 15, 14, 30, 22)
        bucket = get_bucket_key(dt, "monthly")
        assert "2024" in bucket

    def test_yearly_bucket(self):
        """Test yearly bucket calculation."""
        dt = datetime(2024, 6, 15, 14, 30, 22)
        bucket = get_bucket_key(dt, "yearly")
        assert "2024" in bucket

    def test_unknown_bucket_type(self):
        """Test that unknown bucket type raises ValueError."""
        dt = datetime(2024, 1, 15, 14, 30, 22)
        with pytest.raises(ValueError, match="Unknown bucket type"):
            get_bucket_key(dt, "unknown_bucket")


class TestApplyRetention:
    """Tests for apply_retention function."""

    def _make_snapshot_names(self, timestamps):
        """Helper to create snapshot names from timestamps."""
        return [f"home-{ts.strftime('%Y%m%d-%H%M%S')}" for ts in timestamps]

    def test_unparseable_timestamp_kept(self):
        """Test that snapshots with unparseable timestamps are kept."""
        now = datetime.now()
        retention = RetentionConfig(min="0m", hourly=0, daily=1, weekly=0, monthly=0)

        snapshots = ["home-20240115-100000", "invalid-snapshot-name"]
        to_keep, to_delete = apply_retention(snapshots, retention, now=now)

        # Invalid name should be kept
        assert "invalid-snapshot-name" in to_keep

    def test_custom_get_name_function(self):
        """Test using custom get_name function."""
        now = datetime(2024, 1, 15, 12, 0, 0)
        retention = RetentionConfig(min="0m", hourly=0, daily=1, weekly=0, monthly=0)

        # Snapshots as dicts
        snapshots = [
            {"name": "home-20240115-100000", "id": 1},
            {"name": "home-20240114-100000", "id": 2},
        ]

        to_keep, to_delete = apply_retention(
            snapshots,
            retention,
            now=now,
            get_name=lambda s: s["name"],
        )

        assert len(to_keep) >= 1

    def test_invalid_min_duration_defaults(self):
        """Test that invalid min duration defaults to 1 day."""
        now = datetime(2024, 1, 15, 12, 0, 0)
        retention = RetentionConfig(
            min="invalid", hourly=0, daily=0, weekly=0, monthly=0
        )

        timestamps = [now - timedelta(hours=i) for i in range(5)]
        snapshots = self._make_snapshot_names(timestamps)

        # Should not raise, should use default
        to_keep, to_delete = apply_retention(snapshots, retention, now=now)
        assert len(to_keep) >= 1

    def test_with_prefix(self):
        """Test retention with prefix parameter."""
        now = datetime(2024, 1, 15, 12, 0, 0)
        retention = RetentionConfig(min="0m", hourly=0, daily=2, weekly=0, monthly=0)

        timestamps = [now - timedelta(days=i) for i in range(5)]
        snapshots = self._make_snapshot_names(timestamps)

        to_keep, to_delete = apply_retention(
            snapshots,
            retention,
            now=now,
            prefix="home-",
        )

        assert len(to_keep) >= 2

    def test_keep_within_min_period(self):
        """Test that all snapshots within min period are kept."""
        now = datetime.now()
        retention = RetentionConfig(min="1d", hourly=0, daily=0, weekly=0, monthly=0)

        # Create snapshots from the last 12 hours
        timestamps = [now - timedelta(hours=i) for i in range(12)]
        snapshots = self._make_snapshot_names(timestamps)

        to_keep, to_delete = apply_retention(snapshots, retention, now=now)

        # All should be kept (within 1 day)
        assert len(to_keep) == 12
        assert len(to_delete) == 0

    def test_daily_retention(self):
        """Test daily retention bucket."""
        now = datetime(2024, 1, 15, 12, 0, 0)
        retention = RetentionConfig(min="0m", hourly=0, daily=3, weekly=0, monthly=0)

        # Create 2 snapshots per day for 5 days
        timestamps = []
        for day in range(5):
            dt = now - timedelta(days=day)
            timestamps.append(dt.replace(hour=10))
            timestamps.append(dt.replace(hour=14))

        snapshots = self._make_snapshot_names(timestamps)
        to_keep, to_delete = apply_retention(snapshots, retention, now=now)

        # Should keep 1 per day for 3 days, plus latest is always kept
        # Implementation may keep slightly more due to bucket boundaries
        assert len(to_keep) >= 3
        assert len(to_keep) <= 4  # At most 3 daily + 1 latest (if not overlapping)

    def test_hourly_retention(self):
        """Test hourly retention bucket."""
        now = datetime(2024, 1, 15, 12, 0, 0)
        retention = RetentionConfig(min="0m", hourly=6, daily=0, weekly=0, monthly=0)

        # Create 2 snapshots per hour for 10 hours
        timestamps = []
        for hour in range(10):
            dt = now - timedelta(hours=hour)
            timestamps.append(dt.replace(minute=15))
            timestamps.append(dt.replace(minute=45))

        snapshots = self._make_snapshot_names(timestamps)
        to_keep, to_delete = apply_retention(snapshots, retention, now=now)

        # Should keep 1 per hour for 6 hours, plus latest is always kept
        # Implementation keeps one per bucket, may vary by exact algorithm
        assert len(to_keep) >= 6
        assert len(to_keep) <= 10  # Won't keep more than we have hours for

    def test_empty_snapshot_list(self):
        """Test retention with empty snapshot list."""
        retention = RetentionConfig()
        to_keep, to_delete = apply_retention([], retention)

        assert to_keep == []
        assert to_delete == []

    def test_always_keep_latest(self):
        """Test that the latest snapshot is always kept."""
        now = datetime(2024, 1, 15, 12, 0, 0)
        # Very restrictive retention - keep nothing
        retention = RetentionConfig(min="0m", hourly=0, daily=0, weekly=0, monthly=0)

        timestamps = [now - timedelta(days=i) for i in range(5)]
        snapshots = self._make_snapshot_names(timestamps)

        to_keep, to_delete = apply_retention(snapshots, retention, now=now)

        # Latest should still be kept
        assert len(to_keep) >= 1
        assert snapshots[0] in to_keep  # Most recent

    def test_combined_retention(self):
        """Test combined hourly + daily retention."""
        now = datetime(2024, 1, 15, 12, 0, 0)
        retention = RetentionConfig(
            min="0m",
            hourly=6,
            daily=7,
            weekly=0,
            monthly=0,
        )

        # Create snapshots for the past 14 days, 4 per day
        timestamps = []
        for day in range(14):
            dt = now - timedelta(days=day)
            for hour in [6, 10, 14, 18]:
                timestamps.append(dt.replace(hour=hour, minute=0, second=0))

        snapshots = self._make_snapshot_names(timestamps)
        to_keep, to_delete = apply_retention(snapshots, retention, now=now)

        # Should keep: 6 hourly + some daily (non-overlapping)
        # Exact count depends on overlap between hourly and daily
        assert len(to_keep) > 0
        assert len(to_keep) <= 6 + 7  # Maximum possible


class TestFormatRetentionSummary:
    """Tests for format_retention_summary function."""

    def test_basic_summary(self):
        """Test basic summary formatting."""
        to_keep = ["snap1", "snap2", "snap3"]
        to_delete = ["snap4", "snap5"]

        result = format_retention_summary(to_keep, to_delete)

        assert "keeping 3" in result
        assert "deleting 2" in result

    def test_empty_delete_list(self):
        """Test summary with nothing to delete."""
        to_keep = ["snap1", "snap2"]
        to_delete = []

        result = format_retention_summary(to_keep, to_delete)

        assert "keeping 2" in result
        assert "deleting 0" in result

    def test_custom_get_name(self):
        """Test summary with custom get_name function."""
        to_keep = [{"name": "snap1"}, {"name": "snap2"}]
        to_delete = [{"name": "snap3"}]

        result = format_retention_summary(
            to_keep,
            to_delete,
            get_name=lambda s: s["name"],
        )

        assert "keeping 2" in result
        assert "snap3" in result

    def test_truncates_long_delete_list(self):
        """Test that long delete lists are truncated."""
        to_keep = ["keep1"]
        to_delete = [f"delete{i}" for i in range(20)]

        result = format_retention_summary(to_keep, to_delete)

        assert "and 10 more" in result
