"""Tests for retention logic module."""

from datetime import datetime, timedelta

import pytest

from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.retention import (
    CLOCK_SKEW_TOLERANCE,
    RetentionError,
    apply_retention,
    extract_timestamp,
    format_retention_summary,
    get_bucket_key,
    parse_duration,
    subtract_duration,
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

    def test_preferred_format_parses_custom_names(self):
        """A custom timestamp_format not in the built-in list is honored when
        passed as preferred_fmt, so retention buckets it instead of keeping it
        forever."""
        # Custom format with a 'T' separator and no seconds -> not in the fallbacks.
        name = "home-2024-01-15T1430"
        assert extract_timestamp(name, prefix="home-") is None
        result = extract_timestamp(name, prefix="home-", preferred_fmt="%Y-%m-%dT%H%M")
        assert result is not None
        assert (result.year, result.month, result.day, result.hour, result.minute) == (
            2024,
            1,
            15,
            14,
            30,
        )

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

    def test_weekly_bucket_is_iso_week_no_year_boundary_split(self):
        """R10c: all seven days of an ISO week share ONE weekly key, even across a year boundary
        (2021-12-27 Mon .. 2022-01-02 Sun == ISO 2021-W52). Mutation guard: calendar %Y+%W splits
        this week into '2021-W52' and '2022-W00'."""
        keys = {
            get_bucket_key(datetime(2021, 12, 27) + timedelta(days=d), "weekly")
            for d in range(7)  # Mon 2021-12-27 .. Sun 2022-01-02
        }
        assert keys == {"2021-W52"}

    def test_weekly_bucket_jan1_maps_to_prior_iso_year(self):
        """R10c: 2023-01-01 (a Sunday) belongs to ISO week 2022-W52, not '2023-W00'. Mutation
        guard: %Y+%W yields '2023-W00'."""
        assert get_bucket_key(datetime(2023, 1, 1), "weekly") == "2022-W52"

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

    def test_invalid_min_raises_and_deletes_nothing(self):
        """R10a: an invalid min fails LOUD and CLOSED -- apply_retention raises RetentionError
        (so the caller prunes nothing) instead of silently defaulting to 1d and deleting.
        Mutation guard: the old silent-1d fallback returns a non-empty to_delete."""
        now = datetime(2024, 1, 15, 12, 0, 0)
        retention = RetentionConfig(
            min="invalid", hourly=0, daily=0, weekly=0, monthly=0
        )
        timestamps = [now - timedelta(days=i) for i in range(5)]
        snapshots = self._make_snapshot_names(timestamps)

        with pytest.raises(RetentionError):
            apply_retention(snapshots, retention, now=now)

    def test_unparseable_does_not_steal_latest(self):
        """R10a CRITICAL: an unparseable-named entry must NOT consume the 'keep latest' slot --
        the real newest snapshot is still kept, the junk is quarantined (kept). Mutation guard:
        the old code assigns the unparseable timestamp=now, making IT 'latest' and DELETING the
        real newest."""
        now = datetime(2024, 1, 15, 12, 0, 0)
        retention = RetentionConfig(min="0s", hourly=0, daily=0, weekly=0, monthly=0)
        real_new = "home-20240115-100000"  # 2h before now -> newest real
        real_old = "home-20240110-100000"
        to_keep, to_delete = apply_retention(
            ["garbage-name-xyz", real_new, real_old], retention, now=now
        )
        assert real_new in to_keep  # the real newest keeps its 'latest' guarantee
        assert real_new not in to_delete
        assert "garbage-name-xyz" in to_keep  # quarantined, always kept
        assert "garbage-name-xyz" not in to_delete

    def test_future_dated_does_not_steal_latest(self):
        """R10a HIGH: a future-dated snapshot (beyond the skew tolerance) is quarantined (kept)
        and never consumes 'latest'; the real newest valid snapshot survives. Mutation guard:
        without the future partition it sorts first, steals latest, and the real newest is
        deleted."""
        now = datetime(2024, 1, 15, 12, 0, 0)
        retention = RetentionConfig(min="0s", hourly=0, daily=0, weekly=0, monthly=0)
        future = "home-20240115-180000"  # +6h, well beyond the 5m tolerance
        real_new = "home-20240115-100000"
        real_old = "home-20240110-100000"
        to_keep, to_delete = apply_retention(
            [future, real_new, real_old], retention, now=now
        )
        assert future in to_keep  # quarantined, kept
        assert future not in to_delete
        assert real_new in to_keep  # real newest still 'latest'
        assert real_new not in to_delete

    def test_skew_within_tolerance_is_valid_and_can_be_latest(self):
        """R10a: a snapshot a couple minutes in the future (benign NTP jitter, within
        CLOCK_SKEW_TOLERANCE) is treated as VALID (clamped to now), so it is the 'latest' and
        retention still functions -- it is NOT quarantined. Mutation guard: dropping the
        tolerance (quarantining every t>now) makes the OLDER snapshot 'latest', so nothing is
        deleted and this test's `older in to_delete` fails."""
        # This test fixes the skew at +2min; assert the default tolerance actually covers it.
        assert CLOCK_SKEW_TOLERANCE >= timedelta(minutes=2)
        now = datetime(2024, 1, 15, 12, 0, 0)
        skew = "home-20240115-120200"  # +2min, within tolerance
        older = "home-20240115-100000"
        retention = RetentionConfig(min="0s", hourly=0, daily=0, weekly=0, monthly=0)
        to_keep, to_delete = apply_retention([skew, older], retention, now=now)
        assert skew in to_keep  # kept as the latest valid (clamped to now)
        assert older in to_delete  # retention still runs: older is pruned

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


class TestSubtractDuration:
    """R10c: calendar-aware min cutoff for month/year units (stdlib, no dep)."""

    def test_fixed_units_unchanged(self):
        """s/m/h/d/w stay fixed timedeltas (identical to the old behavior). Mutation guard:
        routing these through month math changes them."""
        now = datetime(2024, 3, 15, 12, 0, 0)
        assert subtract_duration(now, "7d") == now - timedelta(days=7)
        assert subtract_duration(now, "2h") == now - timedelta(hours=2)
        assert subtract_duration(now, "3w") == now - timedelta(weeks=3)

    def test_calendar_month_full_31_day_month(self):
        """min='1M' subtracts a CALENDAR month, not a fixed 30 days: 2024-03-31 - 1M -> 2024-02-29
        (not 2024-03-01). Mutation guard: fixed 30d yields 2024-03-01."""
        assert subtract_duration(datetime(2024, 3, 31, 12), "1M") == datetime(
            2024, 2, 29, 12
        )

    def test_calendar_month_day_clamp_non_leap(self):
        """Day clamps to the target month's last day: 2023-03-31 - 1M -> 2023-02-28."""
        assert subtract_duration(datetime(2023, 3, 31), "1M") == datetime(2023, 2, 28)

    def test_12M_equals_1y_calendar(self):
        """Calendar math makes 12M == 1y (both one calendar year), fixing the old 360d != 365d
        mismatch. Mutation guard: the fixed 30d/365d approximation makes them differ."""
        now = datetime(2024, 7, 15, 8, 30, 0)
        assert subtract_duration(now, "12M") == subtract_duration(now, "1y")
        assert subtract_duration(now, "1y") == datetime(2023, 7, 15, 8, 30, 0)

    def test_year_leap_day_clamp(self):
        """1y from a leap day clamps: 2024-02-29 - 1y -> 2023-02-28."""
        assert subtract_duration(datetime(2024, 2, 29), "1y") == datetime(2023, 2, 28)

    def test_invalid_duration_raises_valueerror(self):
        """An invalid duration raises ValueError (callers map it to RetentionError/ConfigError --
        fail closed)."""
        with pytest.raises(ValueError):
            subtract_duration(datetime(2024, 1, 1), "not-a-duration")

    def test_calendar_min_keeps_snapshot_a_30day_cutoff_would_prune(self):
        """Integration: with now on a 31-day month's last day and min='1M', a snapshot 31 days old
        is KEPT (calendar month back), where the old fixed-30d cutoff would have PRUNED it.
        Mutation guard: reverting to `now - parse_duration` deletes the 31-day-old snapshot."""
        now = datetime(2024, 1, 31, 12, 0, 0)
        newest = "home-20240131-000000"  # holds the always-keep-latest slot
        old = "home-20231231-120000"  # 31 days before now; calendar 1M cutoff is 2023-12-31 12:00
        retention = RetentionConfig(min="1M", hourly=0, daily=0, weekly=0, monthly=0)
        to_keep, to_delete = apply_retention([newest, old], retention, now=now)
        # 'old' is kept ONLY by the within-min rule -> proves the calendar cutoff reaches 31 days.
        assert old in to_keep and old not in to_delete
