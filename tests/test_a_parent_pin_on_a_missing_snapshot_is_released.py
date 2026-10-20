"""A parent pin on a snapshot the destination no longer holds is released.

A transfer pins its incremental parent on the source, under the
destination's id, because the destination holds that parent as the base of
the ``send -p``. An interrupted run keeps its pins. If the destination's own
prune then deletes the parent there, the pin can protect nothing: a snapshot
the destination does not hold cannot be the base of any incremental to it.
But the reconcile at the start of a sync released pins only on snapshots
present on the destination, and ``run``'s catch-up selection counted the
parent pin as a transfer that had not finished. So every run sent that
snapshot again in full, the target's prune deleted it again, and the source
kept it for ever.

Driven through the real ``sync_snapshots`` and the real catch-up selector of
``run``, over a real ``LocalEndpoint`` and its lock file.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import run as run_cli
from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict

PREFIX = "home-"
FMT = "%Y%m%d-%H%M%S"
NOW = datetime.now()
S0 = PREFIX + (NOW - timedelta(days=10)).strftime(FMT)  # deleted from the target
S1 = PREFIX + (NOW - timedelta(hours=2)).strftime(FMT)  # on the target
S2 = PREFIX + (NOW - timedelta(hours=1)).strftime(FMT)  # new since
TARGET = "/backups/home"
KEEP_TWO = RetentionConfig(min="0s", hourly=0, daily=0, weekly=0, monthly=0, keep=2)


@pytest.fixture(autouse=True)
def _no_btrfs_identity(monkeypatch):
    monkeypatch.setattr(LocalEndpoint, "subvolume_identity", lambda self, path: None)


class _Target:
    """Holds snapshots by name; its listing is what the catch-up asks."""

    _is_remote = True  # the post-transfer checks have nothing local to look at
    config = {"path": TARGET}

    def __init__(self, held):
        self.held = set(held)

    def get_id(self):
        return TARGET

    def correspondents_of(self, snaps):
        return {s.get_name(): s for s in snaps if s.get_name() in self.held}

    def correspondent_of(self, snap):
        return snap if snap.get_name() in self.held else None

    def list_snapshots(self, flush_cache=False):
        return [
            __util__.Snapshot(
                TARGET,
                PREFIX,
                None,
                time_obj=datetime.strptime(n[len(PREFIX) :], FMT).timetuple(),
                name=n,
            )
            for n in sorted(self.held)
        ]

    def add_snapshot(self, snap, rewrite=True):
        self.held.add(snap.get_name())

    def __str__(self):
        return TARGET


def _selector(target):
    config = MagicMock()
    config.get_target_retention.return_value = KEEP_TWO
    config.global_config.timestamp_format = FMT
    select = run_cli._catch_up_selector(
        SimpleNamespace(snapshot_prefix=PREFIX),
        config,
        SimpleNamespace(path=TARGET),
        target,
    )
    assert select is not None
    return select


def _source(tmp_path, leftover: dict) -> LocalEndpoint:
    for name in (S0, S1, S2):
        (tmp_path / name).mkdir()
    source = LocalEndpoint(
        config={"path": tmp_path, "source": "/src", "snap_prefix": PREFIX}
    )
    source._write_locks(leftover)
    return source


def test_the_next_sync_releases_it_and_does_not_send_the_snapshot(
    tmp_path, monkeypatch
):
    # What an interrupted run left: its snapshot has since arrived (a later
    # run), its parent was deleted from the target by the target's prune.
    source = _source(tmp_path, {S0: {"parent_locks": [TARGET]}})
    sent: list = []
    monkeypatch.setattr(
        ops,
        "send_snapshot",
        lambda snapshot, dest, parent=None, options=None, **k: sent.append(
            (snapshot.get_name(), parent.get_name() if parent else None)
        ),
    )
    monkeypatch.setattr(
        ops, "artifact_verdict", lambda d, s: StructureVerdict("ok", "stand-in")
    )
    target = _Target({S1})

    ops.sync_snapshots(source, target, select=_selector(target))

    assert sent == [(S2, S1)], "only the new snapshot, against the base it holds"
    assert source._read_locks() == {}, "the parent pin was not released"


def test_a_pin_on_a_missing_snapshot_is_kept(tmp_path, monkeypatch):
    """The rule it must not break: a transfer's own pin on a snapshot the
    destination lacks is an unfinished transfer, kept until it arrives."""
    source = _source(tmp_path, {S0: {"locks": [TARGET]}})
    monkeypatch.setattr(
        ops,
        "send_snapshot",
        lambda *a, **k: (_ for _ in ()).throw(
            __util__.SnapshotTransferError("stand-in: the transfer died")
        ),
    )
    target = _Target({S1})

    with pytest.raises(__util__.SnapshotTransferError):
        ops.sync_snapshots(source, target, select=_selector(target))

    assert TARGET in source._read_locks().get(S0, {}).get("locks", [])


def test_another_destinations_parent_pin_is_kept(tmp_path, monkeypatch):
    """Another destination may still hold the snapshot as its base."""
    source = _source(tmp_path, {S0: {"parent_locks": ["/elsewhere"]}})
    monkeypatch.setattr(ops, "send_snapshot", lambda *a, **k: None)
    monkeypatch.setattr(
        ops, "artifact_verdict", lambda d, s: StructureVerdict("ok", "stand-in")
    )
    target = _Target({S1})

    ops.sync_snapshots(source, target, select=_selector(target))

    assert source._read_locks() == {S0: {"parent_locks": ["/elsewhere"]}}
