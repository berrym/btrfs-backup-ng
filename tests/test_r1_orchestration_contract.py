"""Enforcement tests for the R1 transfer-success contract: orchestration (commit B).

The orchestration layer must never swallow a per-snapshot transfer failure. A
sync with any failed transfer raises SnapshotTransferError carrying a
TransferResult (transferred vs failed) as ``err.result`` so callers report a
non-zero exit and accurate counts instead of inferring success from the absence
of an exception.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.core.operations as ops
from btrfs_backup_ng import __util__


def _fake_snap(name: str) -> MagicMock:
    s = MagicMock()
    s.locks = {}
    s.parent_locks = {}
    s.get_name.return_value = name
    return s


def _endpoints(source_snaps, dest_snaps=()):
    src = MagicMock()
    src.list_snapshots.return_value = list(source_snaps)
    src.get_id.return_value = "dest-id"
    dst = MagicMock()
    dst.get_id.return_value = "dest-id"
    dst.list_snapshots.return_value = list(dest_snaps)
    return src, dst


class TestExecuteTransfersResult:
    def test_records_transferred_and_failed(self, monkeypatch):
        s1 = _fake_snap("s1")
        s2 = _fake_snap("s2")
        src, dst = _endpoints([s1, s2])

        def fake_send(snap, *a, **k):
            if snap is s2:
                raise __util__.SnapshotTransferError("boom")

        monkeypatch.setattr(ops, "send_snapshot", fake_send)

        result = ops._execute_transfers(src, dst, [s1, s2], [], [s1, s2], True, {})
        assert result.transferred_count == 1
        assert result.failed_count == 1
        assert result.attempted == 2
        assert not result.ok


class TestSyncSnapshotsFailLoud:
    def test_failed_transfer_raises_with_result(self, monkeypatch):
        snap = _fake_snap("snap-1")
        src, dst = _endpoints([snap])
        monkeypatch.setattr(
            ops,
            "send_snapshot",
            MagicMock(side_effect=__util__.SnapshotTransferError("boom")),
        )

        with pytest.raises(__util__.SnapshotTransferError) as ei:
            ops.sync_snapshots(src, dst, snapshot=snap, no_incremental=True)

        # The result breakdown must ride on the exception.
        assert hasattr(ei.value, "result")
        assert ei.value.result.failed_count == 1
        assert ei.value.result.transferred_count == 0

    def test_successful_transfer_returns_ok_result(self, monkeypatch):
        snap = _fake_snap("snap-1")
        src, dst = _endpoints([snap])
        monkeypatch.setattr(ops, "send_snapshot", MagicMock(return_value=None))

        result = ops.sync_snapshots(src, dst, snapshot=snap, no_incremental=True)
        assert result.ok
        assert result.transferred_count == 1

    def test_nothing_to_transfer_returns_ok_result(self, monkeypatch):
        snap = _fake_snap("snap-1")
        # snapshot already at destination -> nothing planned.
        src, dst = _endpoints([snap], dest_snaps=[snap])
        result = ops.sync_snapshots(src, dst, snapshot=snap, no_incremental=True)
        assert result.ok
        assert result.transferred_count == 0


class TestSyncSnapperSnapshotsFailLoud:
    def _patch_common(self, monkeypatch, snaps):
        monkeypatch.setattr(
            ops, "get_snapper_snapshots_for_backup", lambda *a, **k: list(snaps)
        )
        monkeypatch.setattr(ops, "_list_backed_up_snapper_numbers", lambda ep: set())

    def test_failed_snapshot_raises_with_result(self, monkeypatch):
        snap = MagicMock()
        snap.number = 1
        self._patch_common(monkeypatch, [snap])
        monkeypatch.setattr(
            ops,
            "send_snapper_snapshot",
            MagicMock(side_effect=__util__.SnapshotTransferError("boom")),
        )

        with pytest.raises(__util__.SnapshotTransferError) as ei:
            ops.sync_snapper_snapshots(MagicMock(), "root", MagicMock())

        assert ei.value.result.failed_count == 1
        assert ei.value.result.transferred_count == 0

    def test_success_returns_transferred_count(self, monkeypatch):
        snap = MagicMock()
        snap.number = 1
        self._patch_common(monkeypatch, [snap])
        monkeypatch.setattr(ops, "send_snapper_snapshot", MagicMock(return_value=None))

        count = ops.sync_snapper_snapshots(MagicMock(), "root", MagicMock())
        assert count == 1

    def test_partial_failure_raises_and_reports_both_counts(self, monkeypatch):
        good = MagicMock()
        good.number = 1
        bad = MagicMock()
        bad.number = 2
        self._patch_common(monkeypatch, [good, bad])

        def fake_send(snap, *a, **k):
            if snap is bad:
                raise __util__.SnapshotTransferError("boom")

        monkeypatch.setattr(ops, "send_snapper_snapshot", fake_send)

        with pytest.raises(__util__.SnapshotTransferError) as ei:
            ops.sync_snapper_snapshots(MagicMock(), "root", MagicMock())

        # The good snapshot still counts as transferred; the bad one as failed.
        assert ei.value.result.transferred_count == 1
        assert ei.value.result.failed_count == 1
