"""``run`` does not send a snapper target what its own prune deletes straight after.

The native pipeline gained this in 0.9.10: a target that is behind is sent
only what the target's retention keeps, and ends up holding exactly what it
would have held had everything been sent and pruned. The snapper pipeline
(``_backup_snapper_volume`` -> ``sync_snapper_snapshots`` ->
``_prune_snapper_after_transfer``) still sent every eligible snapper snapshot
and pruned afterwards. It now asks the same retention decision its prune
makes (``plan_snapper_retention_of``, over the destination's backups plus
the snapshots it is missing) and sends only what that keeps.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import btrfs_backup_ng.core.operations as ops
from btrfs_backup_ng.cli import run as run_cli
from btrfs_backup_ng.cli.prune import plan_snapper_retention_of
from btrfs_backup_ng.config.schema import RetentionConfig

NOW = datetime(2026, 9, 23, 12, 0, 0)


def _backup(number: int, when: datetime) -> dict:
    return {
        "number": number,
        "snapshot_path": f"/bk/.snapshots/{number}/snapshot",
        "metadata": SimpleNamespace(date=when, description="", num=number),
    }


def _source(number: int, when: datetime):
    return SimpleNamespace(number=number, date=when)


def _numbers(items) -> set[int]:
    return {i["number"] if isinstance(i, dict) else i.number for i in items}


POLICIES = [
    RetentionConfig(min="0s", hourly=0, daily=3, weekly=0, monthly=0, yearly=0),
    RetentionConfig(min="0s", hourly=6, daily=7, weekly=4, monthly=0, yearly=0),
    RetentionConfig(min="2h", hourly=0, daily=2, weekly=1, monthly=0, yearly=0),
    RetentionConfig(min="0s", hourly=0, daily=0, weekly=0, monthly=0, yearly=0, keep=5),
    RetentionConfig(min="1d", hourly=24, daily=7, weekly=4, monthly=12, yearly=0),
]


class TestTheTargetEndsUpTheSame:
    @pytest.mark.parametrize("policy", POLICIES, ids=range(len(POLICIES)))
    @pytest.mark.parametrize("seed", range(40))
    def test_send_what_prune_keeps_then_prune_equals_send_all_then_prune(
        self, policy, seed
    ):
        rng = random.Random(seed)
        start = NOW - timedelta(hours=rng.randint(24, 24 * 40))
        moments = sorted(
            start + timedelta(hours=i) for i in range(rng.randint(20, 300))
        )
        cut = rng.randint(0, len(moments) // 2)
        # The destination holds the older part as numbered backups; the newer
        # part exists only at the source (the destination was away).
        held = [_backup(n, when) for n, when in enumerate(moments[:cut], 1)]
        missing = [_source(n, when) for n, when in enumerate(moments[cut:], cut + 1)]

        # Today: everything missing is sent, then the prune runs.
        everything_kept, _ = plan_snapper_retention_of(held + missing, policy, now=NOW)
        # Now: only what the prune keeps is sent, then the prune runs.
        kept_ids = {id(s) for s in everything_kept}
        chosen = [s for s in missing if id(s) in kept_ids]
        after_transfer = held + [_backup(s.number, s.date) for s in chosen]
        final_kept, final_deleted = plan_snapper_retention_of(
            after_transfer, policy, now=NOW
        )
        assert _numbers(final_kept) == _numbers(everything_kept)
        # Whatever that prune still deletes was held before the transfer and
        # would have been deleted either way; nothing sent is deleted.
        assert _numbers(final_deleted) <= _numbers(held)


class TestTheSelector:
    def _selector(self, policy, held, degenerate=False):
        config = SimpleNamespace(get_target_retention=lambda v, t: policy)
        target = SimpleNamespace(path="/bk")
        patcher = patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups", return_value=held
        )
        return (
            run_cli._snapper_catch_up_selector(SimpleNamespace(), config, target, {}),
            patcher,
        )

    def test_it_leaves_out_what_the_prune_would_delete(self, caplog):
        policy = RetentionConfig(min="0s", hourly=0, daily=2, weekly=0, monthly=0)
        held = [_backup(1, NOW - timedelta(days=5))]
        missing = [
            _source(2, NOW - timedelta(days=1, hours=3)),
            _source(3, NOW - timedelta(days=1, hours=1)),  # same day as 2: deleted
            _source(4, NOW - timedelta(hours=2)),
        ]
        select, patcher = self._selector(policy, held)
        assert select is not None
        with patcher, caplog.at_level("INFO"):
            chosen = select(missing)
        assert _numbers(chosen) == {2, 4}
        assert "Not sending 1 of 3 missing snapper snapshot(s)" in caplog.text
        assert "(3)" in caplog.text

    def test_a_degenerate_policy_sends_everything(self):
        policy = RetentionConfig(min="1h", hourly=0, daily=0, weekly=0, monthly=0)
        select, _ = self._selector(policy, [])
        assert select is None

    def test_fewer_than_two_missing_sends_everything(self):
        policy = RetentionConfig(min="0s", hourly=0, daily=2, weekly=0, monthly=0)
        select, patcher = self._selector(policy, [])
        with patcher:
            assert select([_source(1, NOW)]) is None

    def test_an_enumeration_failure_sends_everything(self):
        """Leaving out a snapshot the prune would have kept loses history, so
        an undecidable case sends all."""
        policy = RetentionConfig(min="0s", hourly=0, daily=2, weekly=0, monthly=0)
        config = SimpleNamespace(get_target_retention=lambda v, t: policy)
        select = run_cli._snapper_catch_up_selector(
            SimpleNamespace(), config, SimpleNamespace(path="/bk"), {}
        )
        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups",
            side_effect=RuntimeError("unreachable"),
        ):
            assert select([_source(1, NOW), _source(2, NOW)]) is None

    def test_a_destination_with_no_layout_yet_holds_nothing(self, tmp_path):
        """The first backup to a fresh destination is a catch-up like any
        other. The enumeration refuses to call an absent .snapshots "empty"
        (a restore-side rule), and taken at its word the selector sent
        everything -- measured on real btrfs: four same-day snapshots all
        sent, two pruned a moment later."""
        policy = RetentionConfig(min="0s", hourly=0, daily=1, weekly=0, monthly=0)
        config = SimpleNamespace(get_target_retention=lambda v, t: policy)
        destination = SimpleNamespace(config={"path": str(tmp_path)}, _is_remote=False)
        select = run_cli._snapper_catch_up_selector(
            SimpleNamespace(),
            config,
            SimpleNamespace(path=str(tmp_path)),
            {},
            destination,
        )
        missing = [
            _source(1, NOW - timedelta(hours=4)),
            _source(2, NOW - timedelta(hours=3)),
            _source(3, NOW - timedelta(hours=2)),
            _source(4, NOW - timedelta(hours=1)),
        ]
        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups",
            side_effect=RuntimeError("does not exist"),
        ):
            chosen = select(missing)
        assert _numbers(chosen) == {1, 4}

    def test_a_destination_whose_layout_cannot_be_read_sends_everything(self, tmp_path):
        policy = RetentionConfig(min="0s", hourly=0, daily=1, weekly=0, monthly=0)
        config = SimpleNamespace(get_target_retention=lambda v, t: policy)
        (tmp_path / ".snapshots").mkdir()
        destination = SimpleNamespace(config={"path": str(tmp_path)}, _is_remote=False)
        select = run_cli._snapper_catch_up_selector(
            SimpleNamespace(),
            config,
            SimpleNamespace(path=str(tmp_path)),
            {},
            destination,
        )
        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups",
            side_effect=RuntimeError("permission denied"),
        ):
            assert (
                select([_source(1, NOW), _source(2, NOW - timedelta(days=2))]) is None
            )

    def test_the_endpoint_options_reach_the_enumeration(self):
        policy = RetentionConfig(min="0s", hourly=0, daily=2, weekly=0, monthly=0)
        config = SimpleNamespace(get_target_retention=lambda v, t: policy)
        options = {"ssh_sudo": True}
        select = run_cli._snapper_catch_up_selector(
            SimpleNamespace(), config, SimpleNamespace(path="ssh://h:/bk"), options
        )
        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups", return_value=[]
        ) as listing:
            select([_source(1, NOW), _source(2, NOW - timedelta(days=3))])
        listing.assert_called_once_with("ssh://h:/bk", options)


class TestSyncHonoursTheSelection:
    def _sync(self, monkeypatch, select):
        snaps = [
            SimpleNamespace(number=n, date=NOW - timedelta(days=n)) for n in (1, 2, 3)
        ]
        monkeypatch.setattr(
            ops, "get_snapper_snapshots_for_backup", lambda *a, **k: list(snaps)
        )

        def _wrap(s, dest=None):
            w = MagicMock()
            w.get_name.return_value = f"b{s.number}"
            return w

        monkeypatch.setattr(ops, "_create_snapper_snapshot_wrapper", _wrap)
        monkeypatch.setattr(ops, "_snapper_dest_view", lambda dest: MagicMock())
        # Nothing is present at the destination.
        monkeypatch.setattr(
            "btrfs_backup_ng.core.planning.snapshots_present_on", lambda w, v: set()
        )
        planned: dict = {}

        def fake_plan(wrappers, view, only=None, **k):
            planned["only"] = only
            chosen = wrappers if only is None else only
            return [(w, None) for w in chosen]

        monkeypatch.setattr(
            "btrfs_backup_ng.core.planning.plan_transfer_sequence", fake_plan
        )
        sent: list[int] = []
        monkeypatch.setattr(
            ops,
            "send_snapper_snapshot",
            lambda snap, dest, parent_snapper_snapshot=None, options=None, **kw: (
                sent.append(snap.number)
            ),
        )
        count = ops.sync_snapper_snapshots(
            MagicMock(), "root", MagicMock(), select=select
        )
        return count, sent, planned, snaps

    def test_a_selection_plans_and_sends_only_its_members(self, monkeypatch):
        seen: dict = {}

        def select(missing):
            seen["missing"] = list(missing)
            return [missing[0], missing[2]]

        count, sent, planned, snaps = self._sync(monkeypatch, select)
        assert [s.number for s in seen["missing"]] == [1, 2, 3]
        assert sent == [1, 3] and count == 2
        assert [w.get_name() for w in planned["only"]] == ["b1", "b3"]

    def test_none_plans_everything(self, monkeypatch):
        count, sent, planned, _ = self._sync(monkeypatch, lambda missing: None)
        assert sent == [1, 2, 3] and count == 3
        assert planned["only"] is None

    def test_no_selector_is_the_old_behaviour(self, monkeypatch):
        count, sent, planned, _ = self._sync(monkeypatch, None)
        assert sent == [1, 2, 3] and planned["only"] is None


class TestRunWiresIt:
    def test_the_snapper_pipeline_passes_a_selector(self, monkeypatch, tmp_path):
        """``run`` prunes straight after, so it selects; ``snapper backup`` (no
        prune) keeps sending everything."""
        captured: dict = {}

        def fake_sync(scanner, config_name, destination_endpoint, **kwargs):
            captured.update(kwargs)
            return 0

        monkeypatch.setattr(
            "btrfs_backup_ng.core.operations.sync_snapper_snapshots", fake_sync
        )
        monkeypatch.setattr(run_cli, "_prune_snapper_after_transfer", lambda *a: True)
        monkeypatch.setattr(run_cli, "assert_target_mounted", lambda *a, **k: None)
        monkeypatch.setattr(
            run_cli.endpoint, "choose_endpoint", lambda path, cfg: MagicMock()
        )
        monkeypatch.setattr(
            run_cli.endpoint, "assert_encryption_applied", lambda *a, **k: None
        )
        monkeypatch.setattr(
            run_cli.endpoint, "assert_compression_applied", lambda *a, **k: None
        )
        scanner = MagicMock()
        monkeypatch.setattr("btrfs_backup_ng.snapper.SnapperScanner", lambda: scanner)
        policy = RetentionConfig(min="0s", hourly=0, daily=2, weekly=0, monthly=0)
        target = SimpleNamespace(
            path=str(tmp_path),
            require_mount=False,
            compress=None,
            rate_limit=None,
            encrypt=None,
            retention=None,
            ssh_port=22,
            ssh_sudo=False,
            skip_remote_lock=False,
            ssh_host_key_policy="accept-new",
            ssh_password_auth=True,
            ssh_key=None,
            ssh_auth_sock=None,
            gpg_recipient=None,
            gpg_keyring=None,
            openssl_cipher=None,
        )
        volume = SimpleNamespace(
            path="/home",
            snapper=SimpleNamespace(
                config_name="root",
                include_types=["single"],
                exclude_cleanup=[],
                min_age="0s",
            ),
            targets=[target],
        )
        config = SimpleNamespace(
            global_config=SimpleNamespace(
                transfer_timeout=0, transfer_stall_timeout=0, timestamp_format=None
            ),
            get_target_retention=lambda v, t: policy,
        )
        ok, _stats, errors = run_cli._backup_snapper_volume(volume, config)
        assert ok and not errors
        assert callable(captured.get("select")), "run did not pass a selector"
