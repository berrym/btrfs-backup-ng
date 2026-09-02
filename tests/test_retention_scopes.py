"""Retention can differ per scope, and can be expressed as a count.

Requested in #103. Before this there was ONE policy per volume, applied
identically to the source and to every target, so neither of these could be
said:

  * keep many recent snapshots on the box, but the long tail on the targets
  * a rarely-connected archive target and an always-on space-limited one want
    different policies from each other

Count-based retention ("keep the latest N") is not new to the project -- the
original btrfs-backup had it as --num-snapshots and --num-backups, separately for
source and destination, and the legacy CLI still does. The subcommand CLI dropped
it; this brings it back as config rather than flags.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from btrfs_backup_ng.cli.prune import is_degenerate_policy
from btrfs_backup_ng.config.loader import ConfigError, load_config
from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.retention import apply_retention

NOW = datetime(2026, 9, 2, 12, 0)
FMT = "%Y%m%d-%H%M%S"


def _daily(count):
    """`count` daily snapshots, newest first."""
    return [f"s-{(NOW - timedelta(days=n)).strftime(FMT)}" for n in range(count)]


def _run(snaps, config):
    return apply_retention(snaps, config, prefix="s-", now=NOW, timestamp_format=FMT)


class TestCountBasedRetention:
    @pytest.mark.parametrize("keep", [1, 5, 10, 39])
    def test_it_keeps_that_many_when_no_floor_applies(self, keep):
        """`min = "0s"` means no time floor, so the count is the whole answer."""
        to_keep, to_delete = _run(_daily(40), RetentionConfig(keep=keep, min="0s"))
        assert len(to_keep) == keep
        assert len(to_delete) == 40 - keep

    def test_min_still_protects_recent_snapshots(self):
        """THE adversarial finding. `min` is documented as "keep ALL snapshots
        for at least this long" -- a safety floor. Ignoring it in count mode made
        a setting sitting in the config do nothing: measured, keep=1 with min=1d
        deleted five snapshots from the last hour.

        Both are floors, so they compose: keep = N means at least the newest N,
        and at least everything inside min."""
        from datetime import timedelta

        recent = [f"s-{(NOW - timedelta(hours=h)).strftime(FMT)}" for h in range(6)]
        older = [f"s-{(NOW - timedelta(days=d)).strftime(FMT)}" for d in range(2, 20)]

        to_keep, to_delete = _run(recent + older, RetentionConfig(keep=1, min="1d"))

        assert not [x for x in to_delete if x in recent], (
            "count mode deleted snapshots that min was supposed to protect"
        )
        assert len(to_keep) == len(recent)

    def test_it_keeps_the_NEWEST_ones(self):
        snaps = _daily(40)
        to_keep, _ = _run(snaps, RetentionConfig(keep=10))
        assert sorted(to_keep, reverse=True) == sorted(snaps[:10], reverse=True)

    def test_asking_for_more_than_exist_deletes_nothing(self):
        to_keep, to_delete = _run(_daily(5), RetentionConfig(keep=100))
        assert len(to_keep) == 5
        assert to_delete == []

    def test_it_ignores_the_time_BUCKETS(self):
        """keep replaces hourly/daily/weekly/monthly/yearly. `min` is different:
        it is a floor, not a bucket, and it still applies -- see
        test_min_still_protects_recent_snapshots."""
        with_buckets = RetentionConfig(keep=3, min="0s", daily=7, monthly=12)
        to_keep, _ = _run(_daily(40), with_buckets)
        assert len(to_keep) == 3, (
            "the buckets still influenced the outcome; keep must replace them"
        )

    def test_unorderable_snapshots_are_still_kept(self):
        """A count must never be the reason a snapshot with no usable timestamp
        is deleted -- that quarantine rule holds in every mode."""
        snaps = _daily(5) + ["s-not-a-timestamp", "README"]
        to_keep, to_delete = _run(snaps, RetentionConfig(keep=2))
        assert "s-not-a-timestamp" in to_keep
        assert "README" in to_keep
        assert len(to_delete) == 3

    def test_keep_zero_means_use_the_buckets(self):
        by_buckets, _ = _run(_daily(40), RetentionConfig(min="1d", daily=7))
        explicit_zero, _ = _run(_daily(40), RetentionConfig(min="1d", daily=7, keep=0))
        assert len(by_buckets) == len(explicit_zero)


class TestACountPolicyIsNotDegenerate:
    """The degeneracy guard refuses an all-zero-bucket policy because it would
    keep only the latest snapshot. A count policy keeps a definite number, so
    refusing it would block exactly the config the count form exists for."""

    def test_zero_buckets_with_a_count_is_allowed(self):
        assert not is_degenerate_policy(
            RetentionConfig(
                min="0s", hourly=0, daily=0, weekly=0, monthly=0, yearly=0, keep=30
            )
        )

    def test_zero_buckets_without_a_count_is_still_refused(self):
        assert is_degenerate_policy(
            RetentionConfig(min="0s", hourly=0, daily=0, weekly=0, monthly=0, yearly=0)
        )


def _write(tmp_path, body):
    cfg = tmp_path / "c.toml"
    cfg.write_text(body)
    return load_config(str(cfg))


class TestPerScopeResolution:
    BODY = """
[global.retention]
min = "1d"
daily = 7

[[volumes]]
path = "/data"

[volumes.source_retention]
min = "6h"
hourly = 48

[[volumes.targets]]
path = "/mnt/archive"

[volumes.targets.retention]
keep = 30

[[volumes.targets]]
path = "/mnt/fast"
"""

    def test_the_source_uses_source_retention(self, tmp_path):
        config, _ = _write(tmp_path, self.BODY)
        volume = config.volumes[0]
        assert config.get_source_retention(volume).min == "6h"
        assert config.get_source_retention(volume).hourly == 48

    def test_a_target_uses_its_own_policy(self, tmp_path):
        config, _ = _write(tmp_path, self.BODY)
        volume = config.volumes[0]
        archive = volume.targets[0]
        assert config.get_target_retention(volume, archive).keep == 30

    def test_a_target_without_one_falls_back(self, tmp_path):
        config, _ = _write(tmp_path, self.BODY)
        volume = config.volumes[0]
        fast = volume.targets[1]
        resolved = config.get_target_retention(volume, fast)
        assert resolved.daily == 7 and resolved.keep == 0

    def test_a_config_with_no_new_keys_is_unchanged(self, tmp_path):
        """The whole point: existing configs must resolve to one policy
        everywhere, exactly as before."""
        config, _ = _write(
            tmp_path,
            '[global.retention]\nmin = "2d"\ndaily = 3\n\n[[volumes]]\npath = "/d"\n\n'
            '[[volumes.targets]]\npath = "/mnt/a"\n',
        )
        volume = config.volumes[0]
        source = config.get_source_retention(volume)
        target = config.get_target_retention(volume, volume.targets[0])
        assert source.min == target.min == "2d"
        assert source.daily == target.daily == 3

    def test_volume_retention_still_covers_both_when_it_is_the_only_override(
        self, tmp_path
    ):
        config, _ = _write(
            tmp_path,
            '[global.retention]\nmin = "9d"\n\n[[volumes]]\npath = "/d"\n\n'
            '[volumes.retention]\nmin = "3d"\n\n[[volumes.targets]]\npath = "/mnt/a"\n',
        )
        volume = config.volumes[0]
        assert config.get_source_retention(volume).min == "3d"
        assert config.get_target_retention(volume, volume.targets[0]).min == "3d"


class TestBadCountsAndContradictions:
    @pytest.mark.parametrize("value", ["-1", '"30"', "1.5", "true"])
    def test_a_non_count_is_refused_at_load(self, tmp_path, value):
        with pytest.raises(ConfigError, match="retention 'keep'"):
            _write(
                tmp_path,
                f'[global.retention]\nkeep = {value}\n\n[[volumes]]\npath = "/d"\n',
            )

    def test_setting_both_keep_and_buckets_warns(self, tmp_path):
        _config, warnings = _write(
            tmp_path,
            '[[volumes]]\npath = "/d"\n\n[volumes.retention]\nkeep = 30\ndaily = 7\n',
        )
        assert any("keep replaces the time buckets" in w for w in warnings), warnings
        assert any("daily" in w for w in warnings)

    def test_keep_alone_does_not_warn(self, tmp_path):
        _config, warnings = _write(
            tmp_path, '[[volumes]]\npath = "/d"\n\n[volumes.retention]\nkeep = 30\n'
        )
        assert not [w for w in warnings if "keep replaces" in w]
