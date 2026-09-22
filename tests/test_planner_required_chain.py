"""A selection expands to the chain its source says it depends on, or is refused.

The planner has always planned ``only=`` exactly as given. A restore of one
snapshot needs more than that: a stored raw increment can only be received onto
its parent, and a btrfs restore has always brought the chain the snapshot
descends from. Both are one question asked of the SOURCE --
``required_parent_of`` -- and one rule in the planner: follow the answers until
the chain reaches something the destination already holds, and refuse before
streaming when a requirement cannot be met from the source.

The two kinds of answer are pinned separately. For a btrfs endpoint the
predecessor is POLICY (a full send always works); for a raw store the sidecar's
parent is a FACT, and a parent the store no longer holds is a refusal, not
"needs nothing".
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.core.operations as ops
from btrfs_backup_ng import __util__
from btrfs_backup_ng.core.planning import PlanningError, plan_transfer_sequence
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def _snap(stamp, uuid="", received_uuid="", name=None):
    s = __util__.Snapshot(
        "/snaps",
        "home-",
        None,
        time_obj=time.strptime(stamp, "%Y%m%d-%H%M%S"),
        name=name,
    )
    s.uuid = uuid
    s.received_uuid = received_uuid
    return s


def _local(tmp_path, listing):
    tmp_path.mkdir(parents=True, exist_ok=True)
    ep = LocalEndpoint(
        config={"path": tmp_path, "source": None, "snap_prefix": "home-"}
    )
    ep.list_snapshots = lambda flush_cache=False: list(listing)  # type: ignore[method-assign]
    return ep


def _raw(name, stamp, parent_name=None, source_uuid=""):
    return RawSnapshot(
        name=name,
        stream_path=Path(f"/raw/{name}.btrfs.zst"),
        parent_name=parent_name,
        source_uuid=source_uuid,
        created=datetime.strptime(stamp, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc),
    )


def _raw_store(tmp_path, listing):
    ep = RawEndpoint(config={"path": str(tmp_path)})
    ep.list_snapshots = lambda flush_cache=False: list(listing)  # type: ignore[method-assign]
    return ep


# --------------------------------------------------------------------------- #
# the btrfs answer: the time-ordered predecessor, as policy
# --------------------------------------------------------------------------- #
class TestABtrfsSourceNamesItsPredecessor:
    def test_the_predecessor_is_the_newest_older_snapshot(self, tmp_path):
        s1, s2, s3 = (
            _snap("20240101-000000", uuid="U1"),
            _snap("20240102-000000", uuid="U2"),
            _snap("20240103-000000", uuid="U3"),
        )
        source = _local(tmp_path, [s1, s2, s3])
        assert source.required_parent_of(s3) is s2
        assert source.required_parent_of(s2) is s1

    def test_the_oldest_needs_nothing(self, tmp_path):
        s1, s2 = _snap("20240101-000000"), _snap("20240102-000000")
        assert _local(tmp_path, [s1, s2]).required_parent_of(s1) is None

    def test_a_same_second_pair_orders_by_listing_position(self, tmp_path):
        """snapper's one-second dates collapse a fast pre/post pair onto one
        timestamp; the listing order (creation order) is the tie-break, so
        the post snapshot depends on the pre, never the other way round."""
        pre = _snap("20240101-000000", name="home-1-20240101-000000")
        post = _snap("20240101-000000", name="home-2-20240101-000000")
        source = _local(tmp_path, [pre, post])
        assert source.required_parent_of(post) is pre
        assert source.required_parent_of(pre) is None

    def test_an_undated_snapshot_has_no_predecessor_and_is_nobodys(self, tmp_path):
        base = __util__.Snapshot("/snaps", "home-", None, name="home-imported")
        base.time_obj = None
        s1 = _snap("20240101-000000")
        source = _local(tmp_path, [base, s1])
        assert source.required_parent_of(base) is None
        assert source.required_parent_of(s1) is None

    def test_a_snapshot_the_source_does_not_list_needs_nothing(self, tmp_path):
        stranger = _snap("20240105-000000")
        assert (
            _local(tmp_path, [_snap("20240101-000000")]).required_parent_of(stranger)
            is None
        )

    def test_a_listing_failure_offers_no_chain_rather_than_raising(self, tmp_path):
        source = _local(tmp_path, [])

        def boom(flush_cache=False):
            raise OSError("gone")

        source.list_snapshots = boom  # type: ignore[method-assign]
        assert source.required_parent_of(_snap("20240102-000000")) is None


# --------------------------------------------------------------------------- #
# the raw answer: the sidecar's parent, as fact
# --------------------------------------------------------------------------- #
class TestARawStoreNamesTheSidecarsParent:
    def test_an_increment_needs_the_stream_its_sidecar_names(self, tmp_path):
        base = _raw("home-20240101-000000", "20240101-000000")
        inc = _raw("home-20240102-000000", "20240102-000000", parent_name=base.name)
        assert _raw_store(tmp_path, [base, inc]).required_parent_of(inc) is base

    def test_a_full_stream_needs_nothing(self, tmp_path):
        base = _raw("home-20240101-000000", "20240101-000000")
        assert _raw_store(tmp_path, [base]).required_parent_of(base) is None

    def test_the_predecessor_is_not_assumed_for_a_full_stream(self, tmp_path):
        """Two full streams in a row: the newer does not depend on the older
        merely for being newer. The sidecar, not the clock, is the authority."""
        a = _raw("home-20240101-000000", "20240101-000000")
        b = _raw("home-20240102-000000", "20240102-000000")
        assert _raw_store(tmp_path, [a, b]).required_parent_of(b) is None

    def test_a_parent_the_store_no_longer_holds_is_a_refusal_not_nothing(
        self, tmp_path
    ):
        inc = _raw(
            "home-20240102-000000", "20240102-000000", parent_name="home-20240101"
        )
        with pytest.raises(__util__.AbortError, match="home-20240101"):
            _raw_store(tmp_path, [inc]).required_parent_of(inc)


# --------------------------------------------------------------------------- #
# the planner: expansion and refusal
# --------------------------------------------------------------------------- #
class TestTheSelectionExpandsToItsChain:
    def _source(self, tmp_path):
        s1 = _snap("20240101-000000", uuid="U1")
        s2 = _snap("20240102-000000", uuid="U2")
        s3 = _snap("20240103-000000", uuid="U3")
        return _local(tmp_path / "src", [s1, s2, s3]), (s1, s2, s3)

    def test_one_snapshot_onto_empty_media_brings_the_whole_chain(self, tmp_path):
        source, (s1, s2, s3) = self._source(tmp_path)
        dest = _local(tmp_path / "dst", [])
        plan = plan_transfer_sequence(
            [s1, s2, s3], dest, only=s3, source_endpoint=source
        )
        assert [(s.get_name(), p.get_name() if p else None) for s, p in plan] == [
            (s1.get_name(), None),
            (s2.get_name(), s1.get_name()),
            (s3.get_name(), s2.get_name()),
        ]

    def test_the_chain_stops_at_what_the_destination_holds(self, tmp_path):
        """The destination corresponds to s1 (by the stream identity, not the
        name), so s1 is not re-sent and s2 parents off it."""
        source, (s1, s2, s3) = self._source(tmp_path)
        copy1 = _snap("20240101-000000", uuid="Dx", received_uuid="U1")
        dest = _local(tmp_path / "dst", [copy1])
        plan = plan_transfer_sequence(
            [s1, s2, s3], dest, only=s3, source_endpoint=source
        )
        assert [(s.get_name(), p.get_name() if p else None) for s, p in plan] == [
            (s2.get_name(), s1.get_name()),
            (s3.get_name(), s2.get_name()),
        ]

    def test_a_present_middle_ancestor_ends_the_walk(self, tmp_path):
        """The destination holds s2. s3 parents off it and s1, older than
        what is already there, is NOT re-sent: the walk stops at the first
        present ancestor rather than continuing past it to the oldest."""
        source, (s1, s2, s3) = self._source(tmp_path)
        copy2 = _snap("20240102-000000", uuid="Dy", received_uuid="U2")
        dest = _local(tmp_path / "dst", [copy2])
        plan = plan_transfer_sequence(
            [s1, s2, s3], dest, only=s3, source_endpoint=source
        )
        assert [(s.get_name(), p.get_name() if p else None) for s, p in plan] == [
            (s3.get_name(), s2.get_name()),
        ], "history older than what the destination holds was re-sent"

    def test_a_present_selection_is_skipped_without_expanding(self, tmp_path):
        source, (s1, s2, s3) = self._source(tmp_path)
        copy3 = _snap("20240103-000000", uuid="Dz", received_uuid="U3")
        dest = _local(tmp_path / "dst", [copy3])
        assert (
            plan_transfer_sequence([s1, s2, s3], dest, only=s3, source_endpoint=source)
            == []
        )

    def test_without_a_source_endpoint_the_selection_is_planned_as_given(
        self, tmp_path
    ):
        """The backup run's contract is unchanged: ``only`` is that snapshot."""
        _source, (s1, s2, s3) = self._source(tmp_path)
        dest = _local(tmp_path / "dst", [])
        plan = plan_transfer_sequence([s1, s2, s3], dest, only=s3)
        assert [s.get_name() for s, _ in plan] == [s3.get_name()]
        assert plan[0][1] is None

    def test_a_list_selects_several(self, tmp_path):
        source, (s1, s2, s3) = self._source(tmp_path)
        dest = _local(tmp_path / "dst", [])
        plan = plan_transfer_sequence(
            [s1, s2, s3], dest, only=[s3, s1], source_endpoint=source
        )
        assert [s.get_name() for s, _ in plan] == [
            s1.get_name(),
            s2.get_name(),
            s3.get_name(),
        ], "the selection was not expanded, ordered oldest-first and deduplicated"

    def test_no_incremental_still_brings_the_chain_as_full_sends(self, tmp_path):
        """The flag means no ``-p``; it does not mean the history is dropped."""
        source, (s1, s2, s3) = self._source(tmp_path)
        dest = _local(tmp_path / "dst", [])
        plan = plan_transfer_sequence(
            [s1, s2, s3], dest, only=s3, source_endpoint=source, no_incremental=True
        )
        assert [s.get_name() for s, _ in plan] == [
            s1.get_name(),
            s2.get_name(),
            s3.get_name(),
        ]
        assert all(p is None for _, p in plan)

    def test_the_expansion_is_announced(self, tmp_path, caplog):
        source, (s1, s2, s3) = self._source(tmp_path)
        dest = _local(tmp_path / "dst", [])
        with caplog.at_level(logging.INFO, logger="btrfs_backup_ng.core.planning"):
            plan_transfer_sequence([s1, s2, s3], dest, only=s3, source_endpoint=source)
        assert any(
            "requires" in r.getMessage() and s2.get_name() in r.getMessage()
            for r in caplog.records
        ), "the operator was not told the plan grew"


class TestARawIncrementIsRefusedWithoutItsParent:
    def test_the_chain_is_expanded_when_the_parent_is_at_the_store(self, tmp_path):
        base = _raw("home-20240101-000000", "20240101-000000", source_uuid="A")
        inc = _raw(
            "home-20240102-000000",
            "20240102-000000",
            parent_name=base.name,
            source_uuid="B",
        )
        store = _raw_store(tmp_path / "raw", [base, inc])
        dest = _local(tmp_path / "dst", [])
        plan = plan_transfer_sequence(
            [base, inc], dest, only=inc, source_endpoint=store
        )
        assert [s.get_name() for s, _ in plan] == [base.name, inc.name]

    def test_a_missing_parent_refuses_before_anything_is_planned(self, tmp_path):
        inc = _raw(
            "home-20240102-000000",
            "20240102-000000",
            parent_name="home-20240101-000000",
        )
        store = _raw_store(tmp_path / "raw", [inc])
        dest = _local(tmp_path / "dst", [])
        with pytest.raises(PlanningError, match="home-20240101-000000") as info:
            plan_transfer_sequence([inc], dest, only=inc, source_endpoint=store)
        assert "Nothing was transferred" in str(info.value)

    def test_a_requirement_the_source_does_not_list_is_refused_not_followed(
        self, tmp_path
    ):
        """A source answering with an object its listing never produced (a
        double, a drifted override) must not be walked: it is refused by
        name, before anything is planned."""
        s1 = _snap("20240101-000000", uuid="U1")
        s2 = _snap("20240102-000000", uuid="U2")
        source = _local(tmp_path / "src", [s1, s2])
        ghost = _snap("20231201-000000", uuid="G")
        source.required_parent_of = lambda snapshot: ghost  # type: ignore[method-assign]
        dest = _local(tmp_path / "dst", [])
        with pytest.raises(PlanningError, match=ghost.get_name()):
            plan_transfer_sequence([s1, s2], dest, only=s2, source_endpoint=source)

    def test_an_increment_already_at_the_destination_is_not_refused(self, tmp_path):
        """Presence wins: nothing needs sending, so nothing needs a parent."""
        inc = _raw(
            "home-20240102-000000",
            "20240102-000000",
            parent_name="home-20240101-000000",
            source_uuid="B",
        )
        store = _raw_store(tmp_path / "raw", [inc])
        copy = _snap("20240102-000000", uuid="Dy", received_uuid="B")
        dest = _local(tmp_path / "dst", [copy])
        assert (
            plan_transfer_sequence([inc], dest, only=inc, source_endpoint=store) == []
        )


# --------------------------------------------------------------------------- #
# the verdict looks where a raw stream is RECEIVED, not where it is stored
# --------------------------------------------------------------------------- #
class TestTheReceivedName:
    def test_a_raw_stream_is_received_under_its_snapshot_name(self):
        inc = _raw("home-20240102-000000", "20240102-000000")
        assert inc.get_path().name == "home-20240102-000000.btrfs.zst"
        assert ops.received_name_of(inc) == "home-20240102-000000"

    def test_a_subvolume_is_received_under_its_basename(self):
        assert ops.received_name_of(_snap("20240102-000000")) == "home-20240102-000000"

    def test_the_verdict_on_a_raw_source_probes_the_subvolume_name(
        self, tmp_path, monkeypatch
    ):
        """Probing ``DEST/<name>.btrfs.zst`` finds nothing and would call a
        correct restore ``invalid`` -- and the executor would then delete it."""
        dest = _local(tmp_path / "dst", [])
        probed = []

        def shape(path):
            probed.append(path)
            return None

        monkeypatch.setattr(ops, "_received_subvolume_shape", shape)
        monkeypatch.setattr(
            dest, "subvolume_identity", lambda p: {"uuid": "x", "received_uuid": "B"}
        )
        inc = _raw("home-20240102-000000", "20240102-000000", source_uuid="B")
        verdict = ops.artifact_verdict(dest, inc)
        assert verdict.status == "ok", verdict.message
        assert probed == [str(tmp_path / "dst" / "home-20240102-000000")]


# --------------------------------------------------------------------------- #
# a source that refuses to send fails THAT transfer, not the run
# --------------------------------------------------------------------------- #
class TestARefusingSendIsThisTransfersFailure:
    def _snapshot(self, error):
        snapshot = MagicMock()
        snapshot.get_name.return_value = "s1"
        snapshot.get_path.return_value = "/raw/s1.btrfs"
        snapshot.__str__ = lambda self: "s1"  # type: ignore[assignment]
        snapshot.stream_uuid = ""
        snapshot.endpoint.send.side_effect = error
        return snapshot

    def _dest(self, tmp_path):
        dest = MagicMock()
        dest.config = {"path": str(tmp_path)}
        dest._is_remote = False
        dest.get_id.return_value = "dest"
        return dest

    def test_an_abort_from_send_becomes_a_transfer_error(self, tmp_path):
        snapshot = self._snapshot(__util__.AbortError("the stored stream is CORRUPT"))
        with pytest.raises(__util__.SnapshotTransferError, match="CORRUPT"):
            ops.send_snapshot(
                snapshot, self._dest(tmp_path), options={"check_space": False}
            )

    def test_the_executor_records_it_and_releases_the_restore_pin(
        self, tmp_path, monkeypatch
    ):
        """Escaping the executor left the pin taken and every later snapshot
        unattempted. Now it is one failed entry, and with release_on_failure
        the pin comes off, as a restore requires."""
        snapshot = self._snapshot(__util__.AbortError("the stored stream is CORRUPT"))
        source = MagicMock()
        monkeypatch.setattr(ops, "destination_artifact_exists", lambda ep, n: False)
        monkeypatch.setattr(
            ops, "_cleanup_partial_local_subvolume", lambda *a, **k: None
        )
        result = ops._execute_transfers(
            source,
            self._dest(tmp_path),
            [(snapshot, None)],
            {"check_space": False},
            lock_id="restore:t",
            release_on_failure=True,
        )
        assert result.failed_count == 1 and result.transferred_count == 0
        assert "CORRUPT" in str(result.failed[0][1])
        source.set_lock.assert_any_call(snapshot, "restore:t", False)
