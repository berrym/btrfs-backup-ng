"""A prune deletes a snapper slot only while it still holds the backup the plan chose.

A snapper destination slot is named by its number, and snapper reuses numbers.
A retention plan that names slot 1 therefore does not name a backup: between
the plan and the delete, a run can republish slot 1 with a new copy -- the
reproduction attached to #137 parks ``prune`` at its confirmation prompt while
``run`` does exactly that -- and a delete by number then removes the new copy
believing it the old one. The slot's receiving lock closes the overlapping
case; it cannot close this one, because by the time the prune resumes the run
has finished and the lock is free.

So the plan records what each slot held when it chose it, the ``received_uuid``
of the copy (``with_slot_identities``), and the delete asks the slot again,
under its lock, immediately before acting (``_occupant_differs``). A slot whose
occupant changed is kept, and so is one whose identity cannot be read, or whose
plan recorded none: "the same backup" is the one answer that must not be
invented.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import prune as prune_cli
from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.endpoint.local import LocalEndpoint

NOW = datetime(2026, 10, 25, 12, 0, 0)


def _endpoint(base: Path) -> LocalEndpoint:
    base.mkdir(parents=True, exist_ok=True)
    return LocalEndpoint(
        config={"path": str(base), "snap_prefix": "", "fs_checks": "skip"}
    )


class _Destination:
    """A local btrfs destination as the delete sees it: a lock store under the
    target, slots whose occupants the test controls, and a recorder in place
    of ``btrfs subvolume delete``."""

    def __init__(self, tmp_path: Path, monkeypatch) -> None:
        self.base = tmp_path / "target"
        self.endpoint = _endpoint(self.base)
        self.occupants: dict[int, str | None] = {}
        self.deleted: list[str] = []
        self.asked_under_lock: list[bool] = []
        monkeypatch.setattr(
            prune_cli,
            "_delete_snapper_slot_btrfs",
            lambda ep, slot_dir, remote: self.deleted.append(slot_dir),
        )
        monkeypatch.setattr(
            "btrfs_backup_ng.endpoint.choose_endpoint", lambda p, c: self.endpoint
        )
        monkeypatch.setattr(
            LocalEndpoint,
            "subvolume_identity",
            lambda endpoint_self, path: self._identity(path),
        )

    def _identity(self, path: str):
        slot_dir, _snapshot = path.rsplit("/", 1)
        # Is the slot's receiving lock held while this is asked? Another
        # process -- its own endpoint, its own lock manager, the same store
        # -- is refused the lock if so.
        other = _endpoint(self.base)
        try:
            with ops._receiving_lock(other, slot_dir, str(self.base)):
                self.asked_under_lock.append(False)
        except __util__.ReceivingLockBusy:
            self.asked_under_lock.append(True)
        current = self.occupants.get(int(slot_dir.rsplit("/", 1)[-1]))
        if current is None:
            return None
        return {"uuid": "", "received_uuid": current}

    def slot(self, number: int) -> str:
        return f"{self.base}/.snapshots/{number}"

    def prune(self, *items: dict):
        return prune_cli.delete_snapper_backups(str(self.base), list(items), {})


class TestTheSlotIsAskedAgainUnderItsLock:
    def test_a_republished_slot_is_kept_and_the_change_is_named(
        self, tmp_path, monkeypatch, caplog
    ):
        dest = _Destination(tmp_path, monkeypatch)
        dest.occupants[1] = "new-copy"

        with caplog.at_level(logging.INFO):
            count, errors = dest.prune({"number": 1, "received_uuid": "old-copy"})

        assert (count, errors) == (0, []), "the republished backup was deleted"
        assert dest.deleted == []
        assert "Kept slot 1" in caplog.text
        assert "the plan chose received_uuid old-copy" in caplog.text
        assert "now holds new-copy" in caplog.text

    def test_a_slot_still_holding_the_planned_copy_is_deleted(
        self, tmp_path, monkeypatch
    ):
        dest = _Destination(tmp_path, monkeypatch)
        dest.occupants[1] = "old-copy"

        count, errors = dest.prune({"number": 1, "received_uuid": "old-copy"})

        assert (count, errors) == (1, [])
        assert dest.deleted == [dest.slot(1)]

    def test_the_slot_is_asked_while_its_lock_is_held(self, tmp_path, monkeypatch):
        """Asked outside the lock, a run could republish the slot between the
        answer and the delete, and the question would have settled nothing."""
        dest = _Destination(tmp_path, monkeypatch)
        dest.occupants[1] = "old-copy"

        dest.prune({"number": 1, "received_uuid": "old-copy"})

        assert dest.asked_under_lock == [True]

    def test_a_slot_whose_identity_cannot_be_read_now_is_kept(
        self, tmp_path, monkeypatch, caplog
    ):
        dest = _Destination(tmp_path, monkeypatch)
        dest.occupants[1] = None

        with caplog.at_level(logging.INFO):
            count, errors = dest.prune({"number": 1, "received_uuid": "old-copy"})

        assert (count, errors, dest.deleted) == (0, [], [])
        assert "Kept slot 1" in caplog.text
        assert "cannot be read now" in caplog.text
        assert "old-copy" in caplog.text

    def test_a_plan_that_recorded_no_identity_keeps_the_slot(
        self, tmp_path, monkeypatch, caplog
    ):
        """An item that names a number and nothing else is what every caller
        used to hand the delete. It is kept now, with the reason, rather
        than deleted on the strength of the number."""
        dest = _Destination(tmp_path, monkeypatch)
        dest.occupants[1] = "old-copy"

        with caplog.at_level(logging.INFO):
            count, errors = dest.prune({"number": 1})

        assert (count, errors, dest.deleted) == (0, [], [])
        assert "Kept slot 1" in caplog.text
        assert "was not read when the plan was made" in caplog.text

    def test_each_slot_in_the_pass_is_judged_on_its_own(self, tmp_path, monkeypatch):
        dest = _Destination(tmp_path, monkeypatch)
        dest.occupants[1] = "republished"
        dest.occupants[2] = "copy-2"

        count, errors = dest.prune(
            {"number": 1, "received_uuid": "copy-1"},
            {"number": 2, "received_uuid": "copy-2"},
        )

        assert (count, errors) == (1, [])
        assert dest.deleted == [dest.slot(2)]


def _backup(number: int, days_ago: int) -> dict:
    return {
        "number": number,
        "snapshot_path": f"/backups/.snapshots/{number}/snapshot",
        "info_xml_path": f"/backups/.snapshots/{number}/info.xml",
        "metadata": SimpleNamespace(
            date=NOW - timedelta(days=days_ago), description="", num=number
        ),
    }


_RETENTION = RetentionConfig(hourly=0, daily=7, weekly=0, monthly=0, yearly=0, min="1d")


class TestThePlanRecordsWhatEachSlotHeld:
    @pytest.fixture
    def plain_endpoint(self, monkeypatch):
        endpoint = SimpleNamespace(config={"path": "/backups"})
        monkeypatch.setattr(
            "btrfs_backup_ng.endpoint.choose_endpoint", lambda p, c: endpoint
        )
        return endpoint

    def test_every_slot_the_pass_could_read_carries_its_received_uuid(
        self, monkeypatch, plain_endpoint
    ):
        monkeypatch.setattr(
            ops,
            "_enumerate_snapper_btrfs_backups",
            lambda ep: [
                SimpleNamespace(number=1, received_uuid="copy-1"),
                SimpleNamespace(number=2, received_uuid="copy-2"),
            ],
        )
        listing = [_backup(n, days_ago=n * 20) for n in range(1, 13)]

        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups", return_value=listing
        ):
            keep, delete = prune_cli.plan_snapper_retention(
                "/backups", _RETENTION, {}, now=NOW
            )

        by_number = {b["number"]: b for b in keep + delete}
        assert len(by_number) == 12, "the plan still covers every slot"
        assert by_number[1]["received_uuid"] == "copy-1"
        assert by_number[2]["received_uuid"] == "copy-2"
        assert all("received_uuid" not in by_number[n] for n in range(3, 13)), (
            "a slot the pass could not read records no identity"
        )
        assert delete, "the decision itself is unchanged: old slots are selected"

    def test_the_identity_is_read_through_the_endpoint_the_delete_uses(
        self, monkeypatch, plain_endpoint
    ):
        seen = []
        monkeypatch.setattr(
            ops,
            "_enumerate_snapper_btrfs_backups",
            lambda ep: seen.append(ep) or [],
        )

        prune_cli.with_slot_identities("/backups", [_backup(1, 1)], {})

        assert seen == [plain_endpoint]

    def test_a_raw_destination_is_left_as_it_is(self, monkeypatch):
        def never(ep):
            raise AssertionError("a raw store has no slots to read")

        monkeypatch.setattr(ops, "_enumerate_snapper_btrfs_backups", never)
        listing = [{"number": 1, "backup_name": "root-1-20261025-120000"}]

        assert prune_cli.with_slot_identities("raw:///backups", listing, {}) == listing

    def test_a_failed_identity_pass_fails_the_plan(self, monkeypatch, plain_endpoint):
        """Planning deletions whose identities could not be confirmed would
        keep every slot at delete time anyway; failing the plan says why."""

        def failing(ep):
            raise __util__.SnapshotTransferError("could not list the slots")

        monkeypatch.setattr(ops, "_enumerate_snapper_btrfs_backups", failing)

        with patch(
            "btrfs_backup_ng.core.restore.list_snapper_backups",
            return_value=[_backup(1, 30)],
        ):
            with pytest.raises(__util__.SnapshotTransferError):
                prune_cli.plan_snapper_retention("/backups", _RETENTION, {}, now=NOW)

    def test_an_empty_listing_asks_nothing(self, monkeypatch):
        def never(ep):
            raise AssertionError("nothing to identify")

        monkeypatch.setattr(ops, "_enumerate_snapper_btrfs_backups", never)
        assert prune_cli.with_slot_identities("/backups", [], {}) == []
