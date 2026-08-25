"""`--overwrite` replaces what is at the destination, or refuses before deleting.

It previously did nothing at all. `--overwrite` sets `skip_existing=False`, but
`get_restore_chain` stopped walking at any snapshot already present -- so the
snapshot it was meant to replace never reached the filter that would have kept
it. Measured: a restore with `--overwrite` against a full destination sent
nothing and exited 0, having replaced nothing.

Replacing means deleting first. A received subvolume is read-only and cannot be
renamed or even moved (EROFS, verified on real btrfs), so there is no atomic
swap and the destination briefly holds neither copy. That costs a retry rather
than data, because a restore only ever reads the backup -- but it destroys
something the operator has, so it is gated and space is checked before anything
is removed.
"""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.cli import restore as restore_cli
from btrfs_backup_ng.core import restore as core_restore
from btrfs_backup_ng.core.restore import RestoreError, get_restore_chain
from btrfs_backup_ng.core.space import SpaceInfo


def _snaps(names):
    from tests.test_restore import make_snapshots

    return make_snapshots([(n, t) for n, t in names])


class TestTheChainNoLongerStopsAtWhatWillBeReplaced:
    def test_the_requested_snapshot_stays_in_the_chain_when_overwriting(self):
        """The TARGET is kept even though it is already present."""
        snapshots = _snaps(
            [("snap-1", "20260101-100000"), ("snap-2", "20260101-110000")]
        )
        chain = get_restore_chain(
            snapshots[1], snapshots, list(snapshots), overwrite=True
        )
        assert [s.get_name() for s in chain] == ["snap-2"], (
            "the snapshot to be replaced was dropped from the chain, which is "
            "why --overwrite did nothing"
        )

    def test_ancestors_already_present_are_NOT_replaced(self):
        """The defect this branch nearly shipped.

        Suppressing the truncation for every ancestor rather than only the
        target made `--overwrite --snapshot snap-3` return the entire history
        and delete three subvolumes the operator never named -- while the
        consent they gave spoke of one. Four snapshots, because with two the
        whole history and "the target plus its parent" are the same list, which
        is how the first version of this test passed while the defect was live.
        """
        snapshots = _snaps(
            [
                ("snap-0", "20260101-090000"),
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),
            ]
        )
        present = snapshots[:3]  # 0, 1, 2 already at the destination

        chain = get_restore_chain(snapshots[3], snapshots, present, overwrite=True)
        assert [s.get_name() for s in chain] == ["snap-3"], (
            f"ancestors the operator never named were pulled in: "
            f"{[s.get_name() for s in chain]}"
        )

        # And when the TARGET is itself already present, still only the target.
        chain = get_restore_chain(snapshots[2], snapshots, present, overwrite=True)
        assert [s.get_name() for s in chain] == ["snap-2"], (
            f"replacing snap-2 pulled in its ancestors: {[s.get_name() for s in chain]}"
        )

    def test_without_overwrite_it_still_truncates(self):
        """The default must not change: an existing snapshot is a parent."""
        snapshots = _snaps(
            [("snap-1", "20260101-100000"), ("snap-2", "20260101-110000")]
        )
        chain = get_restore_chain(snapshots[1], snapshots, [snapshots[0]])
        assert [s.get_name() for s in chain] == ["snap-2"]


def _real_snapshots(stamps, prefix="snap-"):
    """Real __util__.Snapshot objects, not the test double.

    MockSnapshot.find_parent returns only strictly-older candidates; the real
    one falls back to the oldest present when nothing older exists, which is the
    whole case under test here. A test built on the double cannot reach it.
    """
    import time as _time
    from pathlib import Path

    from btrfs_backup_ng.__util__ import Snapshot

    return [
        Snapshot(
            Path("/backup"), prefix, None, time_obj=_time.strptime(t, "%Y%m%d-%H%M%S")
        )
        for t in stamps
    ]


class _Destination:
    """A destination that actually forgets what it deletes.

    A MagicMock keeps returning the snapshot after delete_snapshots is called,
    which hides two things: that the code now re-lists to confirm the removal,
    and WHICH snapshots were removed. A mutant deleting every snapshot at the
    destination passed the suite while the fake could not tell the difference.
    """

    def __init__(self, names, space=None):
        self._names = list(names)
        self.config = {"path": "/dest", "snap_prefix": ""}
        self.deleted: list[str] = []
        self._space = space or SpaceInfo(
            path="/dest",
            total_bytes=10**12,
            used_bytes=0,
            available_bytes=10**12,
        )

    def list_snapshots(self, flush_cache=False):
        return _snaps([(n, "20260101-100000") for n in self._names])

    def delete_snapshots(self, snapshots, **kw):
        for s in snapshots:
            name = s.get_name()
            self.deleted.append(name)
            if name in self._names:
                self._names.remove(name)

    def get_space_info(self, path=None):
        if isinstance(self._space, Exception):
            raise self._space
        return self._space


class TestSpaceIsCheckedBeforeAnythingIsDeleted:
    def test_an_undersized_destination_is_left_untouched(self):
        """The real check_space_availability runs -- not a stub of it."""
        tiny = SpaceInfo(
            path="/dest", total_bytes=1000, used_bytes=1000, available_bytes=0
        )
        endpoint = _Destination(["snap-1"], space=tiny)
        with pytest.raises(RestoreError, match="Nothing has been deleted"):
            core_restore._replace_at_destination(endpoint, "snap-1", 10**9)
        assert endpoint.deleted == [], "something was deleted despite no room"

    def test_a_sized_destination_proceeds_to_delete(self):
        endpoint = _Destination(["snap-1"])
        core_restore._replace_at_destination(endpoint, "snap-1", 10)
        assert endpoint.deleted == ["snap-1"]

    def test_only_the_named_snapshot_is_deleted(self):
        """Blast radius. A mutant removing everything at the destination used to
        pass, because the fake could not report WHAT it deleted."""
        endpoint = _Destination(["snap-1", "snap-2", "unrelated-9"])
        core_restore._replace_at_destination(endpoint, "snap-1", 10)
        assert endpoint.deleted == ["snap-1"], (
            f"snapshots the operator never named were deleted: {endpoint.deleted}"
        )
        assert sorted(endpoint._names) == ["snap-2", "unrelated-9"]

    def test_an_unreadable_free_space_does_not_block_the_replace(self, caplog):
        """Refusing wherever the check is unavailable would make the flag
        unusable; the receive still fails safely if the disk fills."""
        endpoint = _Destination(["snap-1"], space=OSError("cannot stat"))
        with caplog.at_level("WARNING"):
            core_restore._replace_at_destination(endpoint, "snap-1", 10)
        assert "Could not check free space" in caplog.text
        assert endpoint.deleted == ["snap-1"]

    def test_an_unknown_size_says_the_check_was_skipped(self, caplog):
        """estimated_bytes is 0 for every btrfs snapshot, because Snapshot
        carries no size. Claiming a check that did not run is worse than not
        running it."""
        endpoint = _Destination(["snap-1"])
        with caplog.at_level("INFO"):
            core_restore._replace_at_destination(endpoint, "snap-1", 0)
        assert "free space was NOT checked" in caplog.text

    def test_a_delete_that_did_nothing_is_caught(self):
        """delete_snapshots does not raise when it declines -- a locked
        snapshot, a refused sudo. Trusting it would stream the whole snapshot
        and fail at the end with 'File exists'."""

        class _Stubborn(_Destination):
            def delete_snapshots(self, snapshots, **kw):
                self.deleted.append(snapshots[0].get_name())  # logs, removes nothing

        endpoint = _Stubborn(["snap-1"])
        with pytest.raises(RestoreError, match="still at the destination"):
            core_restore._replace_at_destination(endpoint, "snap-1", 10)

    def test_a_failed_delete_says_the_old_copy_is_still_there(self):
        class _Refusing(_Destination):
            def delete_snapshots(self, snapshots, **kw):
                raise OSError("permission denied")

        endpoint = _Refusing(["snap-1"])
        with pytest.raises(RestoreError, match="left in place"):
            core_restore._replace_at_destination(endpoint, "snap-1", 0)


class TestTheWindowIsAsNarrowAsItCanBe:
    """Nothing that can fail without moving data may happen after the delete.

    The window cannot be closed -- a received subvolume cannot be renamed or
    moved, and btrfs refuses to clear its read-only flag while received_uuid is
    set -- so the only thing left is to make it as short as possible. It was
    much wider than necessary: the delete ran before set_lock, which reaches
    across the network and, since locks became persistent, ABORTS when it cannot
    record one. An operator could lose their copy to a failure that never moved
    a byte.
    """

    def _snapshot(self):
        from tests.test_restore import make_snapshots

        return make_snapshots([("snap-1", "20260101-100000")])[0]

    def test_the_lock_is_taken_before_anything_is_deleted(self):
        order: list[str] = []

        backup = MagicMock()
        backup.config = {"path": "/backup"}
        backup.set_lock.side_effect = lambda *a, **k: order.append("lock")

        # A destination that really forgets what it deletes: the code re-lists
        # to confirm the removal, so a mock still returning the snapshot fails
        # for the wrong reason.
        destination = _Destination(["snap-1"])
        _really_delete = destination.delete_snapshots

        def _tracked(snapshots, **kw):
            order.append("delete")
            _really_delete(snapshots, **kw)

        destination.delete_snapshots = _tracked

        # The post-restore verification checks the real destination path, which a
        # mock does not have; the ordering is what is under test here.
        with patch.object(
            core_restore, "send_snapshot", lambda *a, **k: order.append("send")
        ):
            with patch.object(core_restore, "log_transaction", lambda **k: None):
                with patch.object(
                    core_restore, "verify_restored_snapshot", lambda *a, **k: True
                ):
                    core_restore.restore_snapshot(
                        backup, destination, self._snapshot(), overwrite=True
                    )

        assert order.index("lock") < order.index("delete"), (
            f"the destination was deleted before the lock was taken: {order}"
        )
        assert order.index("delete") < order.index("send"), (
            f"the delete must still precede the send: {order}"
        )

    def test_a_lock_failure_leaves_the_destination_intact(self):
        """The failure this ordering exists for: set_lock aborts, and the
        operator still has their copy."""
        from btrfs_backup_ng import __util__

        backup = MagicMock()
        backup.config = {"path": "/backup"}
        backup.set_lock.side_effect = __util__.AbortError("could not record lock")

        destination = MagicMock()
        destination.config = {"path": "/dest"}
        destination.list_snapshots.return_value = [self._snapshot()]

        with pytest.raises(__util__.AbortError):
            core_restore.restore_snapshot(
                backup, destination, self._snapshot(), overwrite=True
            )
        destination.delete_snapshots.assert_not_called()


class TestTheAdvisoryTellsTheTruth:
    """It must not report a deletion that did not happen."""

    def _snapshot(self):
        from tests.test_restore import make_snapshots

        return make_snapshots([("snap-1", "20260101-100000")])[0]

    def test_no_deletion_claim_when_the_delete_never_ran(self, caplog):
        """set_lock aborts before anything is removed. The operator must not be
        told their copy is gone -- especially not right after being told it was
        left in place."""
        from btrfs_backup_ng import __util__

        backup = MagicMock()
        backup.config = {"path": "/backup", "snap_prefix": ""}
        backup.list_snapshots.return_value = [self._snapshot()]
        backup.correspondent_of.return_value = None
        backup.set_lock.side_effect = __util__.AbortError("no lock")

        destination = _Destination(["snap-1"])
        with caplog.at_level("ERROR"):
            core_restore.restore_snapshots(
                backup, destination, restore_all=True, skip_existing=False
            )
        assert destination.deleted == []
        assert "removed the previous" not in caplog.text, (
            "the operator was told their copy was deleted when it was not"
        )

    def test_the_claim_IS_made_when_a_copy_really_went(self, caplog):
        """The inverse: a genuine loss must still be reported."""
        backup = MagicMock()
        backup.config = {"path": "/backup", "snap_prefix": ""}
        backup.list_snapshots.return_value = [self._snapshot()]
        backup.correspondent_of.return_value = None

        destination = _Destination(["snap-1"])
        with patch.object(
            core_restore, "send_snapshot", side_effect=OSError("link dropped")
        ):
            with caplog.at_level("ERROR"):
                core_restore.restore_snapshots(
                    backup, destination, restore_all=True, skip_existing=False
                )
        assert destination.deleted == ["snap-1"]
        assert "removed the previous" in caplog.text, (
            "a real loss of the local copy was not reported"
        )


class TestTheGateLetsYouThroughWhenYouSayTheWords:
    """The gate's POSITIVE case. Without this, a gate that ignores the
    acknowledgement -- refusing --overwrite forever -- passes a green suite,
    and the branch's headline feature is dead on the CLI."""

    def _args(self, tmp_path, **kw):
        base = dict(
            source=str(tmp_path),
            destination=str(tmp_path),
            overwrite=True,
            in_place=False,
            yes_i_know_what_i_am_doing=True,
            dry_run=False,
            fs_checks="skip",
            prefix="",
        )
        base.update(kw)
        return argparse.Namespace(**base)

    def test_overwrite_with_the_acknowledgement_proceeds(self, tmp_path, caplog):
        seen = {}

        def _fake_restore(*a, **kw):
            seen["skip_existing"] = kw.get("skip_existing")
            return {"restored": 1, "skipped": 0, "failed": 0, "errors": []}

        with patch.object(
            restore_cli, "validate_restore_destination", lambda *a, **k: None
        ):
            with patch.object(restore_cli, "_prepare_backup_endpoint", MagicMock()):
                with patch.object(restore_cli, "_prepare_local_endpoint", MagicMock()):
                    with patch.object(restore_cli, "restore_snapshots", _fake_restore):
                        with caplog.at_level("ERROR"):
                            rc = restore_cli._execute_main_restore(self._args(tmp_path))

        assert rc == 0, "the acknowledgement was given and the run was still refused"
        assert "yes-i-know-what-i-am-doing" not in caplog.text
        assert seen.get("skip_existing") is False, (
            "--overwrite did not reach the restore as skip_existing=False, so the "
            "flag is recognised and then ignored"
        )

    def test_a_dry_run_is_allowed_without_the_acknowledgement(self, tmp_path):
        """FIX 5: an operator must be able to SEE what --overwrite would destroy
        before consenting to it. Gating the preview refuses the one command they
        would use to decide."""
        with patch.object(
            restore_cli, "validate_restore_destination", lambda *a, **k: None
        ):
            with patch.object(restore_cli, "_prepare_backup_endpoint", MagicMock()):
                with patch.object(restore_cli, "_prepare_local_endpoint", MagicMock()):
                    with patch.object(
                        restore_cli,
                        "restore_snapshots",
                        lambda *a, **k: {
                            "restored": 0,
                            "skipped": 0,
                            "failed": 0,
                            "errors": [],
                        },
                    ):
                        rc = restore_cli._execute_main_restore(
                            self._args(
                                tmp_path,
                                yes_i_know_what_i_am_doing=False,
                                dry_run=True,
                            )
                        )
        assert rc == 0, "the preview was refused, so the deletion cannot be inspected"

    def test_the_preview_names_what_would_be_deleted(self, caplog):
        """The other half of FIX 5: being allowed to preview is worthless if the
        preview does not say which subvolumes go."""
        backup = MagicMock()
        backup.config = {"path": "/backup", "snap_prefix": ""}
        present = _snaps([("snap-1", "20260101-100000")])
        backup.list_snapshots.return_value = present
        backup.correspondent_of.return_value = None

        destination = _Destination(["snap-1"])
        with caplog.at_level("INFO"):
            core_restore.restore_snapshots(
                backup, destination, restore_all=True, skip_existing=False, dry_run=True
            )
        assert "Would DELETE and replace: snap-1" in caplog.text, (
            "the preview did not name the snapshot it would destroy"
        )
        assert destination.deleted == [], "a dry run deleted something"


class TestReverseIncrementalsAreAllowedAgain:
    """Only UNRELATED parents are refused, not merely newer ones."""

    def test_a_newer_related_parent_is_used(self):
        """btrfs send -p accepts a newer related snapshot, and it is usually a
        far smaller transfer than a full send. Banning it outright turned
        working incremental restores into full ones."""
        older, newer = _real_snapshots(["20260101-100000", "20260102-100000"])
        backup = MagicMock()
        backup.correspondent_of.return_value = None

        parent = core_restore._choose_parent([older, newer], [newer], older, backup)
        assert parent is not None and parent.get_name() == newer.get_name(), (
            "a valid reverse incremental was refused"
        )

    def test_a_parent_from_another_volume_is_refused(self):
        """A different prefix means a different volume: no shared history, so no
        delta to send. Comparing across prefixes is also what Snapshot.__lt__
        refuses to do, raising NotImplementedError."""
        (mine,) = _real_snapshots(["20260101-100000"], prefix="home-")
        (theirs,) = _real_snapshots(["20260102-100000"], prefix="other-")

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        assert (
            core_restore._choose_parent([mine, theirs], [theirs], mine, backup) is None
        )

    def test_choosing_a_parent_never_raises_across_prefixes(self):
        """Snapshot.__lt__ raises NotImplementedError across prefixes, which used
        to abort the whole restore for raw sources."""
        (mine,) = _real_snapshots(["20260101-100000"], prefix="home-")
        (theirs,) = _real_snapshots(["20260102-100000"], prefix="")

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        core_restore._choose_parent([mine, theirs], [theirs], mine, backup)


class TestSpaceIsCheckedBeforeTheDeleteNotAfter:
    """The transfer layer already had a real space check; it just ran too late.

    send_snapshot calls _verify_destination_space and raises
    InsufficientSpaceError -- but AFTER the deletion, so the operator's copy was
    destroyed for a shortage the tool had the means to detect beforehand. The
    information was collected and dropped, which is this project's recurring
    defect in its destructive direction.
    """

    def _snapshot(self):
        from tests.test_restore import make_snapshots

        return make_snapshots([("snap-1", "20260101-100000")])[0]

    def _run(self, order, space_effect=None):
        snap = self._snapshot()
        backup = MagicMock()
        backup.config = {"path": "/backup"}
        backup.set_lock.side_effect = lambda s, i, st, **k: order.append(f"lock={st}")

        destination = _Destination(["snap-1"])
        _really = destination.delete_snapshots

        def _tracked(snapshots, **kw):
            order.append("DELETE")
            _really(snapshots, **kw)

        destination.delete_snapshots = _tracked

        def _space(*a, **k):
            order.append("space-check")
            if space_effect:
                raise space_effect

        with patch.object(core_restore, "_verify_destination_space", _space):
            with patch.object(core_restore, "log_transaction", lambda **k: None):
                with patch.object(core_restore, "send_snapshot", lambda *a, **k: None):
                    with patch.object(
                        core_restore, "verify_restored_snapshot", lambda *a, **k: True
                    ):
                        try:
                            core_restore.restore_snapshot(
                                backup, destination, snap, overwrite=True
                            )
                        except Exception as exc:
                            order.append(f"raised:{type(exc).__name__}")
        return destination

    def test_the_space_check_runs_before_the_delete(self):
        order: list[str] = []
        self._run(order)
        assert order.index("space-check") < order.index("DELETE"), (
            f"the copy was deleted before free space was checked: {order}"
        )

    def test_a_shortage_leaves_the_copy_in_place(self):
        """The whole point: no room means nothing is destroyed."""
        order: list[str] = []
        destination = self._run(order, space_effect=RestoreError("no room"))
        assert "DELETE" not in order, (
            f"the copy was deleted despite a known space shortage: {order}"
        )
        assert destination.deleted == []

    def test_the_lock_is_released_even_when_the_replace_fails(self):
        """The replace sits inside the try, so the finally still runs. Outside
        it, a refused delete stranded a persistent restore lock on the backup
        snapshot, which now blocks retention from ever pruning it."""
        order: list[str] = []
        self._run(order, space_effect=RestoreError("no room"))
        assert "lock=True" in order and "lock=False" in order, (
            f"the backup lock was taken and never released: {order}"
        )
        assert order.index("lock=False") > order.index("lock=True")


class TestRawSourcesKeepTheirIncrementals:
    """FIX 4's prefix filter silently disabled incremental restore for raw.

    RawSnapshot defaults ``prefix`` to "" and discover_raw_snapshots never sets
    it, while a btrfs source Snapshot carries e.g. 'home-'. An exact-equality
    filter therefore discarded every raw destination snapshot and turned every
    prefixed raw restore into a full transfer -- reported as an ordinary success.
    That inverts this branch's purpose on the very path it exists to fix.
    """

    def test_a_raw_destination_snapshot_can_still_be_a_parent(self):
        import time as _time
        from pathlib import Path

        from btrfs_backup_ng.__util__ import Snapshot
        from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

        source = Snapshot(
            Path("/backup"),
            "home-",
            None,
            time_obj=_time.strptime("20240102-120000", "%Y%m%d-%H%M%S"),
        )
        present = RawSnapshot(
            name="home-20240101-120000", stream_path=Path("/backup/x.btrfs")
        )
        assert present.prefix == "", "fixture no longer models the defect"

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        parent = core_restore._choose_parent([source], [present], source, backup)
        assert parent is not None, (
            "a raw destination snapshot was discarded, so the restore silently "
            "degraded to a full transfer"
        )

    def test_an_unrelated_raw_snapshot_is_still_refused(self):
        """Tolerating an UNKNOWN prefix must not become tolerating anything.

        Raw snapshots all report prefix "", so accepting on "cannot tell" alone
        handed back a stream from an entirely different volume as the parent --
        a `btrfs send -p` against a subvolume with no shared history. The name
        still carries the volume, and raw correspondence is by name anyway.
        """
        import time as _time
        from pathlib import Path

        from btrfs_backup_ng.__util__ import Snapshot
        from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

        source = Snapshot(
            Path("/backup"),
            "home-",
            None,
            time_obj=_time.strptime("20240102-120000", "%Y%m%d-%H%M%S"),
        )
        foreign = RawSnapshot(
            name="database-20240101-120000", stream_path=Path("/backup/db.btrfs")
        )
        assert foreign.prefix == "", "fixture no longer models a raw snapshot"

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        assert (
            core_restore._choose_parent([source], [foreign], source, backup) is None
        ), "a stream from another volume was offered as an incremental parent"

    def test_a_genuine_mismatch_is_still_refused(self):
        """Tolerating UNKNOWN must not tolerate KNOWN-and-different."""
        (mine,) = _real_snapshots(["20240101-120000"], prefix="home-")
        (theirs,) = _real_snapshots(["20240102-120000"], prefix="other-")
        backup = MagicMock()
        backup.correspondent_of.return_value = None
        assert (
            core_restore._choose_parent([mine, theirs], [theirs], mine, backup) is None
        )

    def test_an_unorderable_pair_degrades_instead_of_aborting(self):
        """Keeping unknown-prefix snapshots means find_parent can still meet a
        genuine mismatch and raise. A restore must not abort for that: no parent
        is a full send, which always works."""
        (mine,) = _real_snapshots(["20240101-120000"], prefix="home-")
        (odd,) = _real_snapshots(["20240102-120000"], prefix="")
        backup = MagicMock()
        backup.correspondent_of.return_value = None
        core_restore._choose_parent([mine, odd], [odd], mine, backup)


class TestTheSpaceEstimateIsNotAnUnderestimate:
    """FIX 3 turned a never-running check into one running on a wrong number.

    RawSnapshot.size is the STREAM file -- compressed, possibly encrypted -- not
    the subvolume the destination must hold. Acting on it reports enough room,
    deletes the existing copy, and then runs out of space. That is worse than
    not checking, which at least claimed nothing.
    """

    def _raw(self, **kw):
        from pathlib import Path

        from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

        return RawSnapshot(
            name="home-20240101-120000",
            stream_path=Path("/b/x.btrfs"),
            **kw,
        )

    def test_a_plain_stream_size_is_used(self):
        assert core_restore._replacement_size(self._raw(size=4096)) == 4096

    def test_a_compressed_stream_size_is_treated_as_unknown(self):
        assert (
            core_restore._replacement_size(self._raw(size=4096, compress="zstd")) == 0
        )

    def test_a_btrfs_snapshot_has_no_size_and_is_unknown(self):
        """The wiring FIX 3 repaired: this reads `size`, not the non-existent
        `size_bytes`. Reverting that identifier must not pass unnoticed."""
        (snap,) = _real_snapshots(["20240101-120000"])
        assert not hasattr(snap, "size_bytes"), "the old wiring read this attribute"
        assert core_restore._replacement_size(snap) == 0

    def test_the_size_actually_reaches_the_space_check(self):
        """Pins the wiring end to end: a raw snapshot with a real size must make
        the check run, and an undersized destination must refuse."""
        tiny = SpaceInfo(
            path="/dest", total_bytes=1000, used_bytes=1000, available_bytes=0
        )
        endpoint = _Destination(["home-20240101-120000"], space=tiny)
        with pytest.raises(RestoreError, match="Nothing has been deleted"):
            core_restore._replace_at_destination(
                endpoint,
                "home-20240101-120000",
                core_restore._replacement_size(self._raw(size=10**9)),
            )
        assert endpoint.deleted == []


class TestTheReplaceReportsItsRealOutcome:
    """`really_removed` is what the advisory keys off. Its contract is pinned
    here, because a guard nothing exercises is a guard that quietly stops
    working."""

    def test_it_returns_True_when_something_was_deleted(self):
        endpoint = _Destination(["snap-1"])
        assert core_restore._replace_at_destination(endpoint, "snap-1", 0) is True

    def test_it_returns_False_when_there_was_nothing_to_delete(self):
        """The race the guard exists for: the snapshot vanished between the
        collision check and the replace. Nothing was removed, so nothing may be
        reported as removed."""
        endpoint = _Destination([])
        assert core_restore._replace_at_destination(endpoint, "snap-1", 0) is False
        assert endpoint.deleted == []

    def test_a_failed_confirmation_still_reports_the_deletion(self, caplog):
        """The dangerous direction: the delete happened, the confirming listing
        failed, and the operator is NOT told their copy is gone. They go looking
        for something that no longer exists."""

        class _BlindAfterDelete(_Destination):
            def __init__(self, names):
                super().__init__(names)
                self._calls = 0

            def list_snapshots(self, flush_cache=False):
                self._calls += 1
                if self._calls > 1 and self.deleted:
                    raise OSError("destination went away")
                return super().list_snapshots(flush_cache=flush_cache)

        endpoint = _BlindAfterDelete(["snap-1"])
        with pytest.raises(core_restore._ReplaceFailed) as caught:
            core_restore._replace_at_destination(endpoint, "snap-1", 0)
        assert caught.value.removed is True, (
            "the deletion happened but the error does not carry that, so the "
            "operator will not be told their copy is gone"
        )

    def test_the_advisory_fires_for_a_failed_but_deleted_replace(self, caplog):
        """End to end: _ReplaceFailed(removed=True) must reach the advisory."""

        class _BlindAfterDelete(_Destination):
            def __init__(self, names):
                super().__init__(names)
                self._calls = 0

            def list_snapshots(self, flush_cache=False):
                self._calls += 1
                if self._calls > 2 and self.deleted:
                    raise OSError("destination went away")
                return super().list_snapshots(flush_cache=flush_cache)

        backup = MagicMock()
        backup.config = {"path": "/backup", "snap_prefix": ""}
        backup.list_snapshots.return_value = _snaps([("snap-1", "20260101-100000")])
        backup.correspondent_of.return_value = None

        destination = _BlindAfterDelete(["snap-1"])
        with caplog.at_level("ERROR"):
            core_restore.restore_snapshots(
                backup, destination, restore_all=True, skip_existing=False
            )
        assert destination.deleted == ["snap-1"]
        assert "removed the previous" in caplog.text, (
            "a real loss of the operator's copy went unreported"
        )


class TestARaceThatRemovesNothingIsNotReportedAsARemoval:
    """The guard `on_replaced ... and really_removed` covers a real race.

    Another process -- a concurrent prune, exactly what the persistent-lock work
    exists for -- can delete the snapshot between the collision check and the
    replace. Nothing was then removed BY US, and recording it as a replacement
    would make the failure advisory claim we destroyed the operator's copy when
    we did not.
    """

    def test_a_snapshot_that_vanished_first_is_not_recorded_as_replaced(self):
        class _VanishesAfterTheCheck(_Destination):
            """Present for the collision check, gone by the time we delete."""

            def __init__(self):
                super().__init__(["snap-1"])
                self._seen = 0

            def list_snapshots(self, flush_cache=False):
                self._seen += 1
                if self._seen > 1:
                    self._names = []  # somebody else removed it
                return super().list_snapshots(flush_cache=flush_cache)

        endpoint = _VanishesAfterTheCheck()
        recorded: list[str] = []

        from tests.test_restore import make_snapshots

        snap = make_snapshots([("snap-1", "20260101-100000")])[0]
        backup = MagicMock()
        backup.config = {"path": "/backup"}

        with patch.object(core_restore, "log_transaction", lambda **k: None):
            with patch.object(core_restore, "send_snapshot", lambda *a, **k: None):
                with patch.object(
                    core_restore, "verify_restored_snapshot", lambda *a, **k: True
                ):
                    with patch.object(
                        core_restore, "_verify_destination_space", lambda *a, **k: None
                    ):
                        core_restore.restore_snapshot(
                            backup,
                            endpoint,
                            snap,
                            overwrite=True,
                            on_replaced=recorded.append,
                        )

        assert endpoint.deleted == [], "we deleted something that was already gone"
        assert recorded == [], (
            "a replacement was recorded although nothing was removed; the failure "
            "advisory would then claim we destroyed the operator's copy"
        )


class TestAFailedReplaceDoesNotPoisonTheRestOfTheChain:
    """`_forget_replaced` has to prune the present-set, not just record a name.

    Parents are chosen from `restored_snapshots`, taken BEFORE any replacement.
    If a replace deletes the old copy and the transfer then fails, the name is
    still in that list, so the NEXT snapshot is sent incrementally against a
    subvolume that is no longer there -- `btrfs receive: cannot find parent
    subvolume`. One failed snapshot becomes a failed run.

    The pruning was written, then lost in editing while both comments claiming
    it happens survived. Nothing failed: the whole suite stayed green either
    way, which is why this test exists.
    """

    def test_the_next_snapshot_falls_back_to_a_full_send(self):
        from tests.test_restore import make_snapshots

        snaps = make_snapshots(
            [("snap-2", "20260102-100000"), ("snap-3", "20260103-100000")]
        )
        backup = MagicMock()
        backup.config = {"path": "/backup", "snap_prefix": ""}
        backup.list_snapshots.return_value = snaps
        backup.correspondent_of.return_value = None

        destination = _Destination(["snap-2"])
        sends: list = []

        def _send(snapshot, endpoint, parent=None, **kw):
            sends.append((snapshot.get_name(), parent.get_name() if parent else None))
            if snapshot.get_name() == "snap-2":
                raise OSError("link dropped mid-transfer")

        with patch.object(core_restore, "send_snapshot", _send):
            with patch.object(core_restore, "log_transaction", lambda **k: None):
                with patch.object(
                    core_restore, "verify_restored_snapshot", lambda *a, **k: True
                ):
                    with patch.object(
                        core_restore, "_verify_destination_space", lambda *a, **k: None
                    ):
                        core_restore.restore_snapshots(
                            backup,
                            destination,
                            restore_all=True,
                            skip_existing=False,
                        )

        assert destination.deleted == ["snap-2"], "the fixture no longer replaces"
        later = [s for s in sends if s[0] == "snap-3"]
        assert later, "snap-3 was never attempted, so nothing is proven"
        assert later[0][1] != "snap-2", (
            f"snap-3 was sent against snap-2, which the failed replace deleted: {sends}"
        )


def _raw_snapshots(stamps, prefix="home-"):
    """RawSnapshots as discovery produces them: named, and really dated.

    Constructed bare, ``created`` defaults to now() for every instance, so two
    of them compare EQUAL and any ordering test passes or fails by accident.
    """
    from datetime import datetime, timezone
    from pathlib import Path

    from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

    out = []
    for t in stamps:
        dt = datetime.strptime(t, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
        out.append(
            RawSnapshot(
                name=f"{prefix}{t}",
                stream_path=Path(f"/backup/{prefix}{t}.btrfs"),
                created=dt,
            )
        )
    return out


class TestARawSnapshotIsRestorable:
    """The actual raw restore path: a RawSnapshot as the thing BEING restored.

    The earlier raw tests put a RawSnapshot in the `present` list only, i.e. as
    a parent CANDIDATE, with a btrfs Snapshot as the target. That is the mirror
    image of a raw restore and it exercises a configuration that does not occur.
    B1 exists because "every prefixed raw restore" silently became a full
    transfer, so the restored snapshot has to be the raw one.
    """

    def test_a_raw_restore_still_finds_its_incremental_parent(self):
        older, newer = _raw_snapshots(["20240101-120000", "20240102-120000"])
        assert older < newer, "fixture does not order -- created was not set"
        assert newer.prefix == "", "fixture no longer models a raw snapshot"

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        parent = core_restore._choose_parent([older, newer], [older], newer, backup)
        assert parent is not None and parent.get_name() == older.get_name(), (
            "a raw restore degraded to a full transfer, which is the regression "
            "B1 was written to remove"
        )

    def test_a_raw_restore_refuses_a_parent_from_another_volume(self):
        (target,) = _raw_snapshots(["20240102-120000"], prefix="home-")
        (foreign,) = _raw_snapshots(["20240101-120000"], prefix="database-")

        backup = MagicMock()
        backup.correspondent_of.return_value = None
        parent = core_restore._choose_parent(
            [target, foreign], [foreign], target, backup
        )
        assert parent is None, (
            f"a stream from another volume was offered as the parent for a raw "
            f"restore: {parent.get_name() if parent else None}"
        )


class TestUnorderableSnapshotsDegradeInsteadOfAborting:
    """B2's swallow is load-bearing on the raw path and was untested.

    Keeping unknown-prefix snapshots (B1) means find_parent can still meet a
    genuine mismatch. Snapshot.__lt__ raises NotImplementedError across
    prefixes; a comparison against something with no usable time raises
    TypeError. Neither may abort a restore: no parent is a full send, which
    always works.
    """

    def test_a_prefix_mismatch_degrades_to_a_full_send(self):
        (mine,) = _real_snapshots(["20240101-120000"], prefix="home-")
        (odd,) = _real_snapshots(["20240102-120000"], prefix="")
        backup = MagicMock()
        backup.correspondent_of.return_value = None
        assert core_restore._choose_parent([mine, odd], [odd], mine, backup) is None, (
            "an unorderable pair produced a parent instead of a full send"
        )

    def test_a_comparison_that_raises_TypeError_degrades_too(self):
        """The TypeError arm is reachable independently of NotImplementedError."""

        class _Unorderable:
            prefix = ""

            def get_name(self):
                return "home-20240101-120000"

            def __lt__(self, other):
                raise TypeError("cannot order this")

            def __gt__(self, other):
                raise TypeError("cannot order this")

        (mine,) = _real_snapshots(["20240102-120000"], prefix="home-")
        backup = MagicMock()
        backup.correspondent_of.return_value = None
        # Must not raise; the restore continues as a full send.
        core_restore._choose_parent([mine], [_Unorderable()], mine, backup)


class TestTheSizeWiringIsRealNotDecorative:
    """`estimated_bytes=_replacement_size(snap)` must be the value that arrives.

    The earlier test called _replacement_size itself and separately called
    _replace_at_destination with the result -- proving both functions work while
    proving nothing about the call between them. Replacing the argument with 0
    left the whole suite green, which is exactly the defect FIX 3 repaired (the
    old code read a non-existent `size_bytes`, so the value was always 0).
    """

    def test_the_computed_size_reaches_the_replace(self):
        from tests.test_restore import make_snapshots

        seen = {}

        def _spy(endpoint, name, estimated_bytes):
            seen["bytes"] = estimated_bytes
            return True

        snap = make_snapshots([("snap-1", "20260101-100000")])[0]
        snap.size = 4096  # a plain, uncompressed raw stream
        snap.compress = None
        snap.encrypt = None

        backup = MagicMock()
        backup.config = {"path": "/backup", "snap_prefix": ""}
        backup.list_snapshots.return_value = [snap]
        backup.correspondent_of.return_value = None
        destination = _Destination(["snap-1"])

        with patch.object(core_restore, "_replace_at_destination", _spy):
            with patch.object(core_restore, "log_transaction", lambda **k: None):
                with patch.object(core_restore, "send_snapshot", lambda *a, **k: None):
                    with patch.object(
                        core_restore, "verify_restored_snapshot", lambda *a, **k: True
                    ):
                        with patch.object(
                            core_restore,
                            "_verify_destination_space",
                            lambda *a, **k: None,
                        ):
                            core_restore.restore_snapshots(
                                backup,
                                destination,
                                restore_all=True,
                                skip_existing=False,
                            )

        assert seen.get("bytes") == 4096, (
            f"the computed size never reached _replace_at_destination "
            f"(got {seen.get('bytes')!r}); the wiring is decorative"
        )


class TestTheSpaceGuardRunsForReal:
    """No stub between check_space_availability and the delete.

    The ordering tests patch _verify_destination_space out, so they prove the
    ORDER without proving the guard bites. This drives the real
    check_space_availability against a real SpaceInfo.
    """

    def test_a_real_shortage_stops_a_real_delete(self):
        tiny = SpaceInfo(
            path="/dest", total_bytes=10**6, used_bytes=10**6, available_bytes=0
        )
        endpoint = _Destination(["snap-1"], space=tiny)
        with pytest.raises(RestoreError, match="Nothing has been deleted"):
            core_restore._replace_at_destination(endpoint, "snap-1", 10**9)
        assert endpoint.deleted == [], "a delete happened despite no free space"

    def test_real_headroom_allows_the_delete(self):
        """The inverse: a guard that always refuses is as broken as one that
        never does."""
        roomy = SpaceInfo(
            path="/dest",
            total_bytes=10**12,
            used_bytes=0,
            available_bytes=10**12,
        )
        endpoint = _Destination(["snap-1"], space=roomy)
        assert core_restore._replace_at_destination(endpoint, "snap-1", 10**6) is True
        assert endpoint.deleted == ["snap-1"]


class TestThePreDeleteListingMustAlsoFlush:
    """Both listings in _replace_at_destination flush, and both matter.

    The PRE-delete listing decides whether there is anything to delete, and so
    decides `removed`. Served from a stale memo it can report nothing present,
    return False, and leave the operator's copy in place while the caller
    believes no replacement was needed.
    """

    def test_a_stale_memo_cannot_decide_there_is_nothing_to_delete(self):
        class _Memoising(_Destination):
            def __init__(self, names):
                super().__init__(names)
                self._memo: list | None = None

            def list_snapshots(self, flush_cache=False):
                if self._memo is not None and not flush_cache:
                    return self._memo
                self._memo = super().list_snapshots()
                return self._memo

        endpoint = _Memoising([])
        endpoint.list_snapshots()  # memoise an EMPTY destination
        endpoint._names = ["snap-1"]  # ...which then gains the snapshot

        removed = core_restore._replace_at_destination(endpoint, "snap-1", 0)
        assert removed is True, (
            "the pre-delete listing was served from a stale memo, so the "
            "snapshot present at the destination was never deleted"
        )
        assert endpoint.deleted == ["snap-1"]


class TestTheConfirmationIsNotAllowedToBeCached:
    """FIX 2 exists because delete_snapshots does not raise when it declines.

    Endpoint.delete_snapshots drops the entry from its OWN memo even when the
    delete failed, so a cached read reports the snapshot gone -- a false clean,
    exactly the answer the confirmation exists to catch. The fake used elsewhere
    ignores flush_cache, so losing it there changes nothing and the guard is
    invisible to the suite.
    """

    def test_a_cached_listing_cannot_satisfy_the_confirmation(self):
        class _Memoising(_Destination):
            """Models the real memo, including dropping the entry on a FAILED
            delete -- which is what makes a cached read lie."""

            def __init__(self, names):
                super().__init__(names)
                self._memo = None

            def list_snapshots(self, flush_cache=False):
                if self._memo is not None and not flush_cache:
                    return self._memo
                self._memo = super().list_snapshots()
                return self._memo

            def delete_snapshots(self, snapshots, **kw):
                # declines silently, but forgets the entry anyway
                for s in snapshots:
                    self.deleted.append(s.get_name())
                self._memo = [
                    m
                    for m in (self._memo or [])
                    if m.get_name() != snapshots[0].get_name()
                ]

        endpoint = _Memoising(["snap-1"])
        endpoint.list_snapshots()  # prime the memo
        with pytest.raises(RestoreError, match="still at the destination"):
            core_restore._replace_at_destination(endpoint, "snap-1", 0)


class TestTheSafetyGate:
    def _args(self, tmp_path, **kw):
        return argparse.Namespace(
            source=str(tmp_path),
            destination=str(tmp_path),
            overwrite=True,
            in_place=False,
            yes_i_know_what_i_am_doing=False,
            fs_checks="skip",
            prefix="",
            **kw,
        )

    def test_overwrite_alone_is_refused(self, tmp_path, caplog):
        # The destination is validated first (a tmp dir is not btrfs), which is
        # a more fundamental complaint than a missing flag. Stub it so the gate
        # itself is what is under test.
        with patch.object(
            restore_cli, "validate_restore_destination", lambda *a, **k: None
        ):
            with caplog.at_level("ERROR"):
                rc = restore_cli._execute_main_restore(self._args(tmp_path))
        assert rc == 1
        assert "--yes-i-know-what-i-am-doing" in caplog.text
        assert "deletes the existing snapshot" in caplog.text

    def test_the_refusal_says_the_backup_is_safe(self, tmp_path, caplog):
        """An operator deciding whether to add the flag needs to know what is
        actually at risk -- a local copy, not the backup."""
        with patch.object(
            restore_cli, "validate_restore_destination", lambda *a, **k: None
        ):
            with caplog.at_level("ERROR"):
                restore_cli._execute_main_restore(self._args(tmp_path))
        assert "never modified" in caplog.text
        assert "re-running" in caplog.text.lower()
