"""``min = "all"``: a retention scope that keeps every snapshot.

btrbk's ``snapshot_preserve_min all`` -- and its default -- means nothing in
that scope is ever deleted by retention, whatever the schedule says. This
project's importer needs to express it (a btrbk configuration that sets no
minimum keeps everything), so ``min`` accepts ``"all"``. These tests take it
through every reader of ``min``: the loader, scope inheritance, the engine
under buckets and under a count, the degenerate-policy guard, the prune plan,
``run``'s catch-up selection and the snapper plan.
"""

from __future__ import annotations

import datetime as _dt
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.cli import run as run_cli
from btrfs_backup_ng.cli.prune import (
    is_degenerate_policy,
    plan_endpoint_retention,
    plan_retention_of,
    plan_snapper_retention_of,
)
from btrfs_backup_ng.config.loader import load_config
from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.retention import (
    MIN_KEEP_ALL,
    apply_retention,
    keeps_everything,
    subtract_duration,
)

NOW = _dt.datetime(2026, 9, 23, 12, 0, 0)
PREFIX = "home-"
FMT = "%Y%m%d-%H%M%S"


def _names(days: int) -> list[str]:
    """One snapshot a day for ``days`` days, the oldest years back."""
    return [
        PREFIX + (NOW - _dt.timedelta(days=d * 37)).strftime(FMT) for d in range(days)
    ]


class TestTheValue:
    @pytest.mark.parametrize("spelling", ["all", "ALL", " all "])
    def test_it_is_recognised_in_any_case(self, spelling):
        assert keeps_everything(spelling)

    @pytest.mark.parametrize("other", ["1d", "0s", "", "allx", None, 7])
    def test_nothing_else_is(self, other):
        assert not keeps_everything(other)

    def test_the_cutoff_is_before_every_possible_snapshot(self):
        assert subtract_duration(NOW, MIN_KEEP_ALL) == _dt.datetime.min


class TestTheLoader:
    def test_it_loads_at_every_scope(self, tmp_path):
        cfg = tmp_path / "c.toml"
        cfg.write_text(
            '[global.retention]\nmin = "all"\n\n'
            '[[volumes]]\npath = "/home"\n\n'
            '[volumes.source_retention]\nmin = "all"\n\n'
            '[[volumes.targets]]\npath = "/mnt/a"\n\n'
            '[volumes.targets.retention]\nmin = "all"\n'
        )
        config, warnings = load_config(cfg)
        assert not [w for w in warnings if "Unknown" in w]
        volume = config.volumes[0]
        assert config.global_config.retention.min == "all"
        assert config.get_source_retention(volume).min == "all"
        assert config.get_target_retention(volume, volume.targets[0]).min == "all"

    def test_a_narrower_scope_inherits_it(self, tmp_path):
        """A volume that sets only daily keeps the global min = "all"."""
        cfg = tmp_path / "c.toml"
        cfg.write_text(
            '[global.retention]\nmin = "all"\n\n'
            '[[volumes]]\npath = "/home"\n\n[volumes.retention]\ndaily = 3\n\n'
            '[[volumes.targets]]\npath = "/mnt/a"\n'
        )
        config, _ = load_config(cfg)
        resolved = config.get_effective_retention(config.volumes[0])
        assert (resolved.min, resolved.daily) == ("all", 3)


class TestTheEngine:
    def test_buckets_cannot_select_anything_to_delete(self):
        policy = RetentionConfig(min="all", hourly=0, daily=1, weekly=0, monthly=0)
        keep, delete = apply_retention(_names(40), policy, prefix=PREFIX, now=NOW)
        assert delete == []
        assert len(keep) == 40

    def test_zero_buckets_delete_nothing_either(self):
        policy = RetentionConfig(min="all", hourly=0, daily=0, weekly=0, monthly=0)
        _keep, delete = apply_retention(_names(40), policy, prefix=PREFIX, now=NOW)
        assert delete == []

    def test_a_count_cannot_select_anything_to_delete(self):
        """``min`` is a floor and composes with ``keep``: both can only keep more."""
        policy = RetentionConfig(min="all", keep=2)
        _keep, delete = apply_retention(_names(40), policy, prefix=PREFIX, now=NOW)
        assert delete == []

    def test_the_reason_names_the_setting(self):
        """Mutation guard: a min that is silently read as a duration raises
        here (RetentionError), a min read as "keep nothing" deletes."""
        policy = RetentionConfig(min="all", hourly=0, daily=0, weekly=0, monthly=0)
        keep, _ = apply_retention(_names(3), policy, prefix=PREFIX, now=NOW)
        assert len(keep) == 3


class TestTheGuardsAndPlans:
    def test_it_is_not_degenerate(self):
        """All-zero buckets with a min of a day or less keep only the latest and
        are refused; all-zero buckets with min = "all" keep everything."""
        assert not is_degenerate_policy(
            RetentionConfig(min="all", hourly=0, daily=0, weekly=0, monthly=0)
        )
        assert is_degenerate_policy(
            RetentionConfig(min="1d", hourly=0, daily=0, weekly=0, monthly=0)
        )

    def test_the_prune_plan_deletes_nothing(self):
        endpoint = MagicMock()
        endpoint.list_snapshots.return_value = [
            SimpleNamespace(get_name=lambda n=n: n) for n in _names(30)
        ]
        endpoint.protect_incremental_parents.side_effect = lambda k, d: (k, d)
        policy = RetentionConfig(min="all", hourly=0, daily=1, weekly=0, monthly=0)
        keep, delete = plan_endpoint_retention(endpoint, policy, PREFIX, FMT)
        assert delete == [] and len(keep) == 30

    def test_run_sends_everything_a_target_is_missing(self):
        """The catch-up selector leaves out what the prune would delete; under
        min = "all" the prune deletes nothing, so nothing is left out."""
        policy = RetentionConfig(min="all", hourly=0, daily=1, weekly=0, monthly=0)
        config = MagicMock()
        config.get_target_retention.return_value = policy
        config.global_config.timestamp_format = FMT
        volume = SimpleNamespace(snapshot_prefix=PREFIX)
        destination = MagicMock()
        destination.list_snapshots.return_value = []
        select = run_cli._catch_up_selector(
            volume, config, SimpleNamespace(path="/t"), destination
        )
        assert select is not None
        missing = [SimpleNamespace(get_name=lambda n=n: n) for n in _names(30)]
        chosen = select(missing, set())
        assert chosen is not None and len(chosen) == 30

    def test_plan_retention_of_agrees(self):
        policy = RetentionConfig(min="all", hourly=0, daily=1, weekly=0, monthly=0)
        snaps = [SimpleNamespace(get_name=lambda n=n: n) for n in _names(30)]
        keep, delete = plan_retention_of(snaps, policy, PREFIX, FMT)
        assert delete == [] and len(keep) == 30

    def test_the_snapper_plan_deletes_nothing(self):
        policy = RetentionConfig(min="all", hourly=0, daily=1, weekly=0, monthly=0)
        backups = [
            {
                "number": n,
                "metadata": SimpleNamespace(date=NOW - _dt.timedelta(days=n * 20)),
            }
            for n in range(1, 20)
        ]
        keep, delete = plan_snapper_retention_of(backups, policy, now=NOW)
        assert delete == [] and len(keep) == 19

    def test_the_run_summary_shows_it(self):
        policy = RetentionConfig(min="all", hourly=0, daily=1, weekly=0, monthly=0)
        assert "min=all" in run_cli._format_retention(policy)
