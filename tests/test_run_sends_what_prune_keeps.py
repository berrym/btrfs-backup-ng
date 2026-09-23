"""`run` does not send a target what the target's own prune deletes straight after.

`run` transfers everything a target is missing and then prunes the target with
its own policy. A target that has been away is missing a backlog, and some of
it -- snapshots that are neither the newest nor the oldest of their time
bucket under the target's policy -- was sent only to be deleted by the prune a
moment later (issue #104). `run` now asks the prune's own retention decision
first and sends only what it keeps. The property that makes that safe: the
target ends up holding exactly what it would have held had everything been
sent and pruned.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

import pytest

from btrfs_backup_ng.cli import run as run_cli
from btrfs_backup_ng.cli.prune import plan_retention_of
from btrfs_backup_ng.config.schema import (
    Config,
    GlobalConfig,
    RetentionConfig,
    TargetConfig,
    VolumeConfig,
)

PREFIX = "home-"
FMT = "%Y%m%d-%H%M%S"
NOW = datetime.now()


class Snap:
    def __init__(self, when: datetime):
        self.name = PREFIX + when.strftime(FMT)

    def get_name(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return self.name


def _hourly(start_hours_ago: int, count: int) -> list[Snap]:
    base = NOW - timedelta(hours=start_hours_ago)
    return [Snap(base + timedelta(hours=i)) for i in range(count)]


def _names(snaps) -> set[str]:
    return {s.get_name() for s in snaps}


POLICIES = [
    RetentionConfig(min="0s", hourly=0, daily=3, weekly=0, monthly=0, yearly=0),
    RetentionConfig(min="0s", hourly=6, daily=7, weekly=4, monthly=0, yearly=0),
    RetentionConfig(min="2h", hourly=0, daily=2, weekly=1, monthly=0, yearly=0),
    RetentionConfig(min="0s", hourly=0, daily=0, weekly=0, monthly=0, yearly=0, keep=5),
    RetentionConfig(min="1d", hourly=24, daily=7, weekly=4, monthly=12, yearly=0),
]


class TestTheTargetEndsUpTheSame:
    @pytest.mark.parametrize("policy", POLICIES, ids=range(len(POLICIES)))
    @pytest.mark.parametrize("seed", range(60))
    def test_send_what_prune_keeps_then_prune_equals_send_all_then_prune(
        self, policy, seed
    ):
        rng = random.Random(seed)
        history = _hourly(rng.randint(24, 24 * 40), rng.randint(20, 400))
        rng.shuffle(history)
        cut = rng.randint(0, len(history) // 2)
        history.sort(key=lambda s: s.name)
        held, missing = history[:cut], history[cut:]  # the target is behind

        # Today: everything missing is sent, then the prune runs.
        everything, _ = plan_retention_of(held + missing, policy, PREFIX, FMT)
        # Now: only what the prune keeps is sent, then the prune runs.
        keep, _ = plan_retention_of(held + missing, policy, PREFIX, FMT)
        sent = [s for s in missing if s in keep]
        after, _ = plan_retention_of(held + sent, policy, PREFIX, FMT)

        assert _names(after) == _names(everything)


def _volume(retention: RetentionConfig) -> tuple[VolumeConfig, Config, TargetConfig]:
    target = TargetConfig(path="/backup", retention=retention)
    volume = VolumeConfig(path="/home", snapshot_prefix=PREFIX, targets=[target])
    config = Config(global_config=GlobalConfig(timestamp_format=FMT), volumes=[volume])
    return volume, config, target


class _Target:
    def __init__(self, held, fail=False):
        self.held = held
        self.fail = fail

    def list_snapshots(self, flush_cache=False):
        if self.fail:
            raise OSError("unreachable")
        return list(self.held)


class TestTheSelector:
    DAILY = RetentionConfig(min="0s", hourly=0, daily=3, weekly=0, monthly=0, yearly=0)

    def _select(self, held, missing, retention=None, **target_kw):
        volume, config, target = _volume(retention or self.DAILY)
        select = run_cli._catch_up_selector(
            volume, config, target, _Target(held, **target_kw)
        )
        return select, (select(held + missing, _names(held)) if select else None)

    def test_a_backlog_is_cut_to_what_the_target_keeps(self, caplog):
        held = _hourly(24 * 10, 2)
        missing = _hourly(24 * 5, 24 * 5)
        with caplog.at_level("INFO"):
            _, chosen = self._select(held, missing)
        assert chosen is not None
        assert 0 < len(chosen) < len(missing)
        assert "retention would delete them straight after" in caplog.text

    def test_the_newest_missing_snapshot_is_always_sent(self):
        held = _hourly(24 * 10, 2)
        missing = _hourly(24 * 5, 24 * 5)
        _, chosen = self._select(held, missing)
        assert max(missing, key=lambda s: s.name) in chosen

    def test_a_policy_the_prune_refuses_sends_everything(self):
        degenerate = RetentionConfig(
            min="0s", hourly=0, daily=0, weekly=0, monthly=0, yearly=0
        )
        select, _ = self._select([], _hourly(48, 10), retention=degenerate)
        assert select is None

    def test_a_target_that_cannot_be_listed_sends_everything(self):
        _, chosen = self._select([], _hourly(48, 10), fail=True)
        assert chosen is None

    def test_colliding_names_send_everything(self):
        # The target holds a snapshot under a missing one's NAME that is not
        # its copy (a re-created snapshot): presence is by identity, so it is
        # missing, and a name-keyed decision about the pair cannot be trusted.
        snaps = _hourly(48, 10)
        impostor = Snap(NOW - timedelta(hours=48))
        volume, config, target = _volume(self.DAILY)
        select = run_cli._catch_up_selector(volume, config, target, _Target([impostor]))
        assert select(snaps, set()) is None

    def test_one_missing_snapshot_is_simply_sent(self):
        _, chosen = self._select(_hourly(48, 3), _hourly(1, 1))
        assert chosen is None


class TestTheWiring:
    def test_sync_snapshots_plans_what_select_returns(self, monkeypatch):
        from btrfs_backup_ng.core import operations, planning

        seen = {}

        def fake_plan(source_snapshots, destination, **kw):
            seen["only"] = kw.get("only")
            return []

        monkeypatch.setattr(planning, "plan_transfer_sequence", fake_plan)
        monkeypatch.setattr(planning, "snapshots_present_on", lambda s, d: set())
        snaps = _hourly(10, 5)

        class Source:
            def list_snapshots(self, flush_cache=False):
                return snaps

        class Dest:
            def get_id(self):
                return "dest"

        chosen = snaps[-2:]
        operations.sync_snapshots(Source(), Dest(), select=lambda s, p: chosen)
        assert seen["only"] == chosen

    def test_without_select_everything_is_planned(self, monkeypatch):
        from btrfs_backup_ng.core import operations, planning

        seen = {}
        monkeypatch.setattr(
            planning, "plan_transfer_sequence", lambda s, d, **kw: seen.update(kw) or []
        )
        monkeypatch.setattr(planning, "snapshots_present_on", lambda s, d: set())

        class Source:
            def list_snapshots(self, flush_cache=False):
                return []

        class Dest:
            def get_id(self):
                return "dest"

        operations.sync_snapshots(Source(), Dest())
        assert seen["only"] is None

    def _record(self, monkeypatch):
        from btrfs_backup_ng.core.operations import TransferResult

        got = {}

        def fake_sync(*a, **kw):
            got.update(kw)
            return TransferResult()

        monkeypatch.setattr(run_cli, "sync_snapshots", fake_sync)
        return got

    def test_run_hands_its_selector_to_the_transfer(self, monkeypatch):
        got = self._record(monkeypatch)
        marker = object()
        run_cli._transfer_to_target(
            object(), object(), TargetConfig(path="/b"), "s", True, select=marker
        )
        assert got["select"] is marker

    def test_newest_only_ignores_the_selector(self, monkeypatch):
        got = self._record(monkeypatch)
        run_cli._transfer_to_target(
            object(),
            object(),
            TargetConfig(path="/b"),
            "s",
            True,
            newest_only=True,
            select=object(),
        )
        assert got["select"] is None

    def test_both_transfer_paths_build_the_selector(self):
        import inspect

        source = inspect.getsource(run_cli._backup_volume)
        assert source.count("_catch_up_selector(") == 2, (
            "run has a sequential and a parallel transfer path; one of them "
            "does not build the catch-up selector"
        )
