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
from btrfs_backup_ng.core.space import SpaceCheck, SpaceInfo


def _snaps(names):
    from tests.test_restore import make_snapshots

    return make_snapshots([(n, t) for n, t in names])


class TestTheChainNoLongerStopsAtWhatWillBeReplaced:
    def test_an_existing_snapshot_stays_in_the_chain_when_overwriting(self):
        snapshots = _snaps(
            [("snap-1", "20260101-100000"), ("snap-2", "20260101-110000")]
        )
        chain = get_restore_chain(
            snapshots[1], snapshots, [snapshots[0]], overwrite=True
        )
        assert [s.get_name() for s in chain] == ["snap-1", "snap-2"], (
            "the snapshot to be replaced was dropped from the chain, which is "
            "why --overwrite did nothing"
        )

    def test_without_overwrite_it_still_truncates(self):
        """The default must not change: an existing snapshot is a parent."""
        snapshots = _snaps(
            [("snap-1", "20260101-100000"), ("snap-2", "20260101-110000")]
        )
        chain = get_restore_chain(snapshots[1], snapshots, [snapshots[0]])
        assert [s.get_name() for s in chain] == ["snap-2"]


class TestSpaceIsCheckedBeforeAnythingIsDeleted:
    def _endpoint(self, available_ok: bool):
        endpoint = MagicMock()
        endpoint.list_snapshots.return_value = []
        endpoint.get_space_info.return_value = SpaceInfo(
            path="/dest", total_bytes=0, used_bytes=0, available_bytes=0
        )
        return endpoint

    def test_an_undersized_destination_is_left_untouched(self):
        endpoint = self._endpoint(False)
        check = SpaceCheck(
            space_info=endpoint.get_space_info.return_value,
            estimated_size=10**9,
            sufficient=False,
        )
        with patch.object(
            core_restore, "check_space_availability", lambda *a, **k: check
        ):
            with pytest.raises(RestoreError, match="Nothing has been deleted"):
                core_restore._replace_at_destination(endpoint, "snap-1", 10**9)
        endpoint.delete_snapshots.assert_not_called()

    def test_a_sized_destination_proceeds_to_delete(self):
        endpoint = self._endpoint(True)
        existing = _snaps([("snap-1", "20260101-100000")])
        endpoint.list_snapshots.return_value = existing
        check = SpaceCheck(
            space_info=endpoint.get_space_info.return_value,
            estimated_size=10,
            sufficient=True,
        )
        with patch.object(
            core_restore, "check_space_availability", lambda *a, **k: check
        ):
            core_restore._replace_at_destination(endpoint, "snap-1", 10)
        endpoint.delete_snapshots.assert_called_once()

    def test_an_unreadable_free_space_does_not_block_the_replace(self, caplog):
        """Refusing wherever the check is unavailable would make the flag
        unusable; the receive still fails safely if the disk fills."""
        endpoint = self._endpoint(True)
        endpoint.get_space_info.side_effect = OSError("cannot stat")
        endpoint.list_snapshots.return_value = _snaps([("snap-1", "20260101-100000")])
        with caplog.at_level("WARNING"):
            core_restore._replace_at_destination(endpoint, "snap-1", 10)
        assert "Could not check free space" in caplog.text
        endpoint.delete_snapshots.assert_called_once()

    def test_a_failed_delete_says_the_old_copy_is_still_there(self):
        endpoint = self._endpoint(True)
        endpoint.list_snapshots.return_value = _snaps([("snap-1", "20260101-100000")])
        endpoint.delete_snapshots.side_effect = OSError("permission denied")
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

        destination = MagicMock()
        destination.config = {"path": "/dest"}
        destination.list_snapshots.return_value = [self._snapshot()]
        destination.delete_snapshots.side_effect = lambda *a, **k: order.append(
            "delete"
        )

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
