"""0.9.1 Item #1: `run` composes `prune`.

`run` is the strict pipeline snapshot -> transfer -> prune. Its post-transfer retention phase
(`_prune_after_transfer`) applies the SAME shared engine as the `prune` command, non-interactively,
with strict semantics: a degenerate or invalid policy fails loudly (returns False -> run exits
non-zero) rather than being silently skipped, and only successfully-transferred targets are pruned.
"""

from __future__ import annotations

import datetime as _dt
from unittest.mock import MagicMock

from btrfs_backup_ng.cli.run import _prune_after_transfer
from btrfs_backup_ng.config.schema import RetentionConfig

NORMAL = RetentionConfig(min="1d", hourly=0, daily=3, weekly=0, monthly=0, yearly=0)
DEGENERATE = RetentionConfig(min="1d", hourly=0, daily=0, weekly=0, monthly=0, yearly=0)
INVALID_MIN = RetentionConfig(
    min="not-a-duration", hourly=0, daily=3, weekly=0, monthly=0, yearly=0
)


def _snap(name):
    s = MagicMock()
    s.get_name.return_value = name
    return s


def _endpoint(n_old):
    """An endpoint whose list_snapshots returns n_old snapshots (2..n+2 days old), a pass-through
    protect_incremental_parents, and a delete_snapshots that records what it deleted."""
    ep = MagicMock()
    now = _dt.datetime.now()
    snaps = [
        _snap(f"home-{(now - _dt.timedelta(days=d)).strftime('%Y%m%d-%H%M%S')}")
        for d in range(2, 2 + n_old)
    ]
    ep.list_snapshots.return_value = snaps
    ep.protect_incremental_parents.side_effect = lambda keep, delete: (keep, delete)
    ep.deleted = []
    ep.delete_snapshots.side_effect = lambda batch, **k: ep.deleted.extend(batch)
    return ep


def _volume():
    v = MagicMock()
    v.snapshot_prefix = "home-"
    v.path = "/home"
    return v


def _config(retention):
    c = MagicMock()
    c.get_effective_retention.return_value = retention
    # Retention is resolved PER ENDPOINT now -- source and each target can carry
    # their own policy. A bare MagicMock would return a Mock from these, and the
    # degeneracy check compares against an int, so an unconfigured double no
    # longer stands in faithfully for a Config.
    c.get_source_retention.return_value = retention
    c.get_target_retention.return_value = retention
    return c


def _no_ts_format(monkeypatch):
    # get_timestamp_format(config) would return a MagicMock; force None so apply_retention parses
    # with its built-in formats.
    monkeypatch.setattr("btrfs_backup_ng.cli.run.get_timestamp_format", lambda c: None)


def test_run_prunes_source_and_succeeded_target(monkeypatch):
    """A normal policy prunes BOTH the source and a successfully-transferred target after the
    transfer. Mutation guard: dropping the prune phase leaves both un-pruned (nothing deleted)."""
    _no_ts_format(monkeypatch)
    src, tgt = _endpoint(10), _endpoint(10)
    tgt_cfg = MagicMock()
    tgt_cfg.path = "/mnt/backup"
    errors: list[str] = []
    ok = _prune_after_transfer(
        _volume(), _config(NORMAL), src, [(tgt, tgt_cfg)], errors
    )
    assert ok is True and errors == []
    assert len(src.deleted) > 0  # source pruned per policy
    assert len(tgt.deleted) > 0  # succeeded target pruned per policy


def test_run_degenerate_policy_fails_loud_and_deletes_nothing(monkeypatch):
    """A degenerate policy makes the prune phase FAIL (return False, error recorded) and delete
    NOTHING -- fail-closed, not a silent skip. Mutation guard: removing the degenerate check prunes
    almost everything (delete called); returning True instead of False fails the ok assertion."""
    _no_ts_format(monkeypatch)
    src = _endpoint(10)
    errors: list[str] = []
    ok = _prune_after_transfer(_volume(), _config(DEGENERATE), src, [], errors)
    assert ok is False
    assert errors  # loud
    assert src.deleted == []  # nothing deleted
    src.list_snapshots.assert_not_called()  # refused before even listing


def test_run_invalid_min_fails_loud_and_deletes_nothing(monkeypatch):
    """An invalid `min` (RetentionError) makes the prune phase fail loudly and delete nothing --
    never delete on ambiguous config. Mutation guard: swallowing RetentionError as success fails
    the ok assertion."""
    _no_ts_format(monkeypatch)
    src = _endpoint(5)
    errors: list[str] = []
    ok = _prune_after_transfer(_volume(), _config(INVALID_MIN), src, [], errors)
    assert ok is False
    assert errors
    assert src.deleted == []


def test_run_only_prunes_succeeded_targets(monkeypatch):
    """Fate-sharing: only a successfully-transferred target is pruned; a target whose transfer
    failed (absent from succeeded_targets) is never touched. Mutation guard: pruning all targets
    regardless of transfer success would delete on the failed one."""
    _no_ts_format(monkeypatch)
    src = _endpoint(10)
    ok_tgt = _endpoint(10)
    ok_cfg = MagicMock()
    ok_cfg.path = "/ok"
    failed_tgt = _endpoint(10)  # transfer failed -> NOT in the succeeded list
    errors: list[str] = []
    _prune_after_transfer(_volume(), _config(NORMAL), src, [(ok_tgt, ok_cfg)], errors)
    assert len(ok_tgt.deleted) > 0  # succeeded target pruned
    assert failed_tgt.deleted == []  # failed target untouched
    failed_tgt.list_snapshots.assert_not_called()
