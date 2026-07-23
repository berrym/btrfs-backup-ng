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

_TIME = [0]


def _fake_snap(name: str) -> MagicMock:
    s = MagicMock()
    s.locks = {}
    s.parent_locks = {}
    s.get_name.return_value = name
    s.uuid = "uuid-" + name
    s.received_uuid = ""
    _TIME[0] += 1
    s.time_obj = (
        2024,
        1,
        _TIME[0],
        0,
        0,
        0,
        0,
        0,
        0,
    )  # comparable, for planner ordering
    return s


def _endpoints(source_snaps, dest_snaps=()):
    src = MagicMock()
    src.list_snapshots.return_value = list(source_snaps)
    src.get_id.return_value = "dest-id"
    dst = MagicMock()
    dst.get_id.return_value = "dest-id"
    dst.list_snapshots.return_value = list(dest_snaps)
    # Presence via the correspondence primitive: present iff a same-named dest snap exists.
    _dest_by_name = {s.get_name(): s for s in dest_snaps}
    dst.correspondent_of.side_effect = lambda s: _dest_by_name.get(s.get_name())
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

        result = ops._execute_transfers(src, dst, [(s1, None), (s2, None)], {})
        assert result.transferred_count == 1
        assert result.failed_count == 1
        assert result.attempted == 2
        assert not result.ok

    def test_failed_in_run_parent_short_circuits_dependents(self, monkeypatch):
        """Within-run chaining safety: if an in-run parent transfer FAILS, its dependent
        incrementals must NOT be attempted or committed -- a raw target would otherwise write
        a valid-looking but UNRESTORABLE stream (a false success). They are recorded as
        failures (R1). Mutation guard: removing the short-circuit lets the dependents transfer
        against a missing parent."""
        s1 = _fake_snap("s1")
        s2 = _fake_snap("s2")
        s3 = _fake_snap("s3")
        src, dst = _endpoints([s1, s2, s3])

        attempted = []

        def fake_send(snap, *a, **k):
            attempted.append(snap)
            if snap is s1:
                raise __util__.SnapshotTransferError("boom")

        monkeypatch.setattr(ops, "send_snapshot", fake_send)

        # Chained plan: s2 parents s1, s3 parents s2. s1 fails -> s2, s3 must short-circuit.
        result = ops._execute_transfers(src, dst, [(s1, None), (s2, s1), (s3, s2)], {})

        assert result.transferred_count == 0
        assert result.failed_count == 3
        # Only s1 was actually attempted; s2 and s3 were short-circuited (parent missing).
        assert attempted == [s1]

    def test_parent_lock_lifecycle(self, monkeypatch):
        """The executor locks the incremental PARENT (parent=True) before the send and
        releases it after a verified success -- the R3 lock lifecycle that keeps retention
        from pruning a parent a pending transfer still needs. Only covered end-to-end
        elsewhere; this pins it directly. Mutation guard: dropping either parent set_lock
        call fails this."""
        parent = _fake_snap("p")
        child = _fake_snap("c")
        src, dst = _endpoints([parent, child])
        monkeypatch.setattr(ops, "send_snapshot", MagicMock(return_value=None))

        result = ops._execute_transfers(src, dst, [(child, parent)], {})

        # Parent locked (parent=True) before send, released (parent=True) on success.
        src.set_lock.assert_any_call(parent, "dest-id", True, parent=True)
        src.set_lock.assert_any_call(parent, "dest-id", False, parent=True)
        # The transferred child is locked then released.
        src.set_lock.assert_any_call(child, "dest-id", True)
        src.set_lock.assert_any_call(child, "dest-id", False)
        assert result.transferred_count == 1


class TestSourceEnrichmentOrdering:
    def test_sync_lists_source_with_flush_for_enriched_snapshots(self, monkeypatch):
        """sync_snapshots must enumerate the source with ``flush_cache=True`` so the
        correspondence-based planner/reconcile always see freshly ENRICHED (uuid-carrying)
        snapshots -- never a stale cache that a prior list + unenriched ``add_snapshot()``
        left with empty uuids (which would silently degrade every transfer to a full send).
        Mutation guard: a plain ``list_snapshots()`` (no flush) fails this."""
        snap = _fake_snap("snap-1")
        src, dst = _endpoints(
            [snap], dest_snaps=[snap]
        )  # present -> nothing to transfer
        monkeypatch.setattr(ops, "send_snapshot", MagicMock(return_value=None))

        ops.sync_snapshots(src, dst, snapshot=snap, no_incremental=True)

        src.list_snapshots.assert_any_call(flush_cache=True)


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

        # Wrap each snapper snapshot as a lightweight named stand-in, and plan every one as a
        # full send (no chaining), so the fail-loud loop is exercised in isolation from the
        # wrapper enrichment / correspondence enumeration.
        def _wrap(s, dest=None):
            w = MagicMock()
            w.get_name.return_value = f"backup-{s.number}"
            return w

        monkeypatch.setattr(ops, "_create_snapper_snapshot_wrapper", _wrap)
        monkeypatch.setattr(ops, "_snapper_dest_view", lambda dest: MagicMock())
        monkeypatch.setattr(
            "btrfs_backup_ng.core.planning.plan_transfer_sequence",
            lambda wrappers, view, **k: [(w, None) for w in wrappers],
        )

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
