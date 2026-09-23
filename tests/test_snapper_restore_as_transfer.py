"""A snapper restore is a transfer through the engine into the snapper layout.

``restore_snapper_snapshots`` selects nothing itself (the command line does),
builds the backup location as a prepared SOURCE, plans against the local
config's slots by correspondence and hands the plan to the executor with the
snapper layout as the destination. What the old function did on its own --
choose the parent from the BACKUP side by number, open its own send and
receive pipes, write info.xml three different ways, take no pin, verify
nothing -- is gone, so these tests pin the behaviour the engine now gives:

- the incremental parent is chosen from what the CONFIG holds, by identity,
  so one increment onto empty media is a full send into slot 1 and the same
  increment onto a config holding its base is an increment from that base;
- every selected backup lands in a fresh slot, present or not;
- the source endpoint is prepared, so an ssh:// source with ``--ssh-sudo``
  gets the passwordless probe before its ``btrfs send``;
- the pins are taken under ``restore:<session>`` and released on failure;
- the verdict judges the copy IN ITS SLOT before the slot is published, and a
  failed receive leaves no numbered slot and no ``.incoming``;
- info.xml is snapper's own, renumbered, from one implementation;
- a stored raw increment without its parent is refused before streaming;
- the preview prints the plan the run executes.

The engine's transfer is replaced by a fake that does what the layout can
observe -- the receive starts (the slot opens), a ``snapshot`` lands in the
slot -- and the slot scripts run for real against plain directories.
"""

from __future__ import annotations

import inspect
import logging
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import btrfs_backup_ng.core.operations as ops
import btrfs_backup_ng.core.restore as core_restore
from btrfs_backup_ng import __util__
from btrfs_backup_ng.core.restore import (
    RestoreError,
    _restored_info_xml,
    list_snapper_backups,
    restore_snapper_snapshots,
)
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot, StructureVerdict
from btrfs_backup_ng.snapper import SnapperConfig, SnapperScanner
from btrfs_backup_ng.snapper.metadata import BackupMetadata, save_backup_metadata

# A real snapper info.xml, with the multi-block userdata form snapper writes
# (one <userdata> element PER entry) and the <uid> element this project does
# not model -- the element that tells renumbering apart from regenerating.
INFO_XML = """<?xml version="1.0"?>
<snapshot>
  <type>pre</type>
  <num>{num}</num>
  <date>2026-08-18 00:5{num}:01</date>
  <uid>0</uid>
  <description>before upgrade {num}</description>
  <cleanup>number</cleanup>
  <userdata>
    <key>reason</key>
    <value>manual</value>
  </userdata>
  <userdata>
    <key>requestor</key>
    <value>operator</value>
  </userdata>
</snapshot>
"""

REMOTE = "ssh://backup@nas:/backups/home"
REMOTE_BASE = "/backups/home"

#: The engine's own verdict, kept from before any fixture replaces it.
_REAL_VERDICT = ops.artifact_verdict


def _real_shell(ep, script):
    r = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return r.returncode, r.stdout


def _make_slot(base: Path, number: int, info_xml: str | None = INFO_XML) -> None:
    slot = base / ".snapshots" / str(number)
    (slot / "snapshot").mkdir(parents=True)
    if info_xml is not None:
        (slot / "info.xml").write_text(info_xml.format(num=number))


def _next_number(config) -> int:
    return SnapperScanner.get_next_snapshot_number(None, config)  # type: ignore[arg-type]


def _slots(local: Path) -> list[int]:
    snaps = local / ".snapshots"
    return sorted(int(p.name) for p in snaps.iterdir() if p.name.isdigit())


def _incomings(local: Path) -> list[str]:
    return sorted(
        p.name for p in (local / ".snapshots").iterdir() if p.name.endswith(".incoming")
    )


def _plan_lines(records):
    return [
        r.getMessage().strip()
        for r in records
        if re.match(r"^\s+\[\d+/\d+\] ", r.getMessage())
    ]


class _Rig(SimpleNamespace):
    def restore(self, *numbers, dry_run=False, source=None, **kw):
        source = source or str(self.backup)
        backups = list_snapper_backups(source, kw.pop("endpoint_options", None))
        selected = [b for b in backups if b["number"] in numbers]
        return restore_snapper_snapshots(
            source,
            backups,
            selected,
            "root",
            options={"check_space": False},
            dry_run=dry_run,
            **kw,
        )


@pytest.fixture
def rig(tmp_path, monkeypatch):
    """A local btrfs backup location holding slots 5 (base) and 6 (increment),
    a local snapper config with an empty ``.snapshots``, and the engine's
    transfer replaced by what the layout can observe."""
    backup = tmp_path / "backup"
    _make_slot(backup, 5)
    _make_slot(backup, 6)
    local = tmp_path / "local"
    (local / ".snapshots").mkdir(parents=True)
    config = SnapperConfig(name="root", subvolume=local)

    scanner = MagicMock()
    scanner.get_config.side_effect = lambda name: config if name == "root" else None
    scanner.get_next_snapshot_number.side_effect = _next_number
    monkeypatch.setattr("btrfs_backup_ng.snapper.SnapperScanner", lambda: scanner)

    # Which slot at which location carries which received_uuid -- the one
    # enumeration both the source listing and the destination view read.
    identities: dict[str, dict[int, str]] = {
        str(backup): {5: "O5", 6: "O6"},
        str(local): {},
    }

    def fake_enumerate(endpoint):
        base = str(endpoint.config["path"]).rstrip("/")
        return [
            ops._SnapperBtrfsBackup(n, u, base=base, endpoint=endpoint)
            for n, u in identities.get(base, {}).items()
        ]

    monkeypatch.setattr(ops, "_enumerate_snapper_btrfs_backups", fake_enumerate)
    monkeypatch.setattr(ops, "_snapper_run_shell", _real_shell)
    # The real receive would run btrfs; the fake transfer below lands the copy.
    monkeypatch.setattr(LocalEndpoint, "receive", lambda self, *a, **k: None)

    sent: list[dict] = []
    failing: set[str] = set()

    def fake_send(snapshot, destination_endpoint, parent=None, options=None, **kw):
        ops._ensure_destination_exists(destination_endpoint)
        destination_endpoint.receive(None, str(snapshot))
        landing = Path(destination_endpoint.config["path"]) / "snapshot"
        landing.mkdir()
        (landing / "payload").write_text(snapshot.get_name())
        sent.append(
            {
                "snapshot": snapshot,
                "parent": parent,
                "landing": landing,
                "options": dict(options or {}),
            }
        )
        if snapshot.get_name() in failing:
            raise __util__.SnapshotTransferError(f"receive of {snapshot} died")

    monkeypatch.setattr(ops, "send_snapshot", fake_send)
    monkeypatch.setattr(
        ops, "artifact_verdict", lambda ep, s: StructureVerdict("ok", "verified")
    )
    return _Rig(
        backup=backup,
        local=local,
        config=config,
        identities=identities,
        sent=sent,
        failing=failing,
        scanner=scanner,
    )


def _present(rig, number: int, received_uuid: str) -> None:
    """A published slot in the config whose copy carries ``received_uuid``."""
    slot = rig.local / ".snapshots" / str(number)
    (slot / "snapshot").mkdir(parents=True)
    (slot / "info.xml").write_text(INFO_XML.format(num=number))
    rig.identities[str(rig.local)][number] = received_uuid


# --------------------------------------------------------------------------- #
# the parent comes from the config, never from the backup side
# --------------------------------------------------------------------------- #


class TestTheParentComesFromTheConfig:
    def test_one_increment_onto_an_empty_config_is_one_full_send_into_slot_one(
        self, rig
    ):
        """The disaster-recovery walkthrough. The backup HOLDS the parent (slot
        5), which is what the old backup-side chooser keyed on; the config
        holds nothing, so the send is full."""
        stats = rig.restore(6)
        assert [(s["snapshot"].number, s["parent"]) for s in rig.sent] == [(6, None)]
        assert _slots(rig.local) == [1]
        assert (rig.local / ".snapshots" / "1" / "snapshot" / "payload").exists()
        assert stats["restored"] == 1 and stats["failed"] == 0
        assert stats["slots"] == [(6, 1)]

    def test_the_selection_is_not_expanded_to_the_chain(self, rig):
        rig.restore(6)
        assert [s["snapshot"].number for s in rig.sent] == [6]

    def test_a_parent_the_config_holds_is_used_incrementally(self, rig):
        """The config already holds a copy of backup 5 (slot 3, whatever its
        number): by identity, not by number, it is the parent."""
        _present(rig, 3, "O5")
        rig.restore(6)
        (entry,) = rig.sent
        assert entry["parent"] is not None and entry["parent"].number == 5
        assert _slots(rig.local) == [3, 4]

    def test_a_copy_under_the_same_number_with_a_different_identity_is_no_parent(
        self, rig
    ):
        """Numbers recycle: slot 5 in the config is a DIFFERENT snapshot."""
        _present(rig, 5, "SOMETHING-ELSE")
        rig.restore(6)
        (entry,) = rig.sent
        assert entry["parent"] is None

    def test_all_onto_empty_media_is_a_chain_rebuilt_from_the_restored_base(self, rig):
        stats = rig.restore(5, 6)
        assert [
            (s["snapshot"].number, getattr(s["parent"], "number", None))
            for s in rig.sent
        ] == [(5, None), (6, 5)]
        assert _slots(rig.local) == [1, 2]
        assert stats["slots"] == [(5, 1), (6, 2)]

    def test_every_selected_backup_lands_in_a_new_slot_even_when_present(self, rig):
        """Snapper keeps every snapshot; a restore is never skipped as
        "already there". A rerun adds slots, and the increment is still an
        increment -- from the copy already in the config."""
        rig.restore(5, 6)
        rig.identities[str(rig.local)] = {1: "O5", 2: "O6"}
        rig.sent.clear()
        stats = rig.restore(5, 6)
        assert _slots(rig.local) == [1, 2, 3, 4]
        assert stats["restored"] == 2
        assert [
            (s["snapshot"].number, getattr(s["parent"], "number", None))
            for s in rig.sent
        ] == [(5, None), (6, 5)]


# --------------------------------------------------------------------------- #
# the slot: verdict in place, published once verified, abandoned on failure
# --------------------------------------------------------------------------- #


class TestTheSlot:
    def test_the_copy_lands_in_the_incoming_slot_and_is_published_afterwards(self, rig):
        rig.restore(6)
        (entry,) = rig.sent
        assert entry["landing"] == rig.local / ".snapshots" / "1.incoming" / "snapshot"
        assert (rig.local / ".snapshots" / "1" / "snapshot" / "payload").exists()
        assert _incomings(rig.local) == []

    def test_the_verdict_judges_the_copy_in_its_slot(self, rig, monkeypatch):
        """The real verdict, with the identity answered per path: it must ask
        about ``1.incoming/snapshot`` -- the copy that was received -- and
        only then is the slot published."""
        asked: list[str] = []
        monkeypatch.setattr(ops, "artifact_verdict", _REAL_VERDICT)
        monkeypatch.setattr(ops, "_received_subvolume_shape", lambda p: None)
        monkeypatch.setattr(
            LocalEndpoint,
            "subvolume_identity",
            lambda self, path: (
                asked.append(str(path)) or {"uuid": "N", "received_uuid": "O6"}
            ),
        )
        rig.restore(6)
        assert asked == [str(rig.local / ".snapshots" / "1.incoming" / "snapshot")]
        assert _slots(rig.local) == [1]

    def test_an_invalid_copy_is_not_published_and_leaves_nothing(
        self, rig, monkeypatch
    ):
        monkeypatch.setattr(ops, "artifact_verdict", _REAL_VERDICT)
        monkeypatch.setattr(ops, "_received_subvolume_shape", lambda p: None)
        monkeypatch.setattr(
            LocalEndpoint,
            "subvolume_identity",
            lambda self, path: {"uuid": "N", "received_uuid": ""},
        )
        stats = rig.restore(6)
        assert stats["failed"] == 1 and stats["restored"] == 0
        assert "not valid" in stats["errors"][0]
        assert _slots(rig.local) == []
        assert _incomings(rig.local) == []

    def test_a_failed_receive_leaves_no_numbered_slot_and_no_incoming(self, rig):
        rig.failing.add("snapshot-6")
        stats = rig.restore(6)
        assert stats == {
            "restored": 0,
            "failed": 1,
            "errors": ["snapshot 6: receive of snapshot 6 died"],
            "slots": [],
        }
        assert _slots(rig.local) == []
        assert _incomings(rig.local) == []

    def test_a_failure_in_the_middle_of_all_leaves_only_the_published_slots(self, rig):
        """The base fails; the increment that depended on it is not streamed
        (the executor's in-run rule), and nothing is left but what succeeded."""
        rig.failing.add("snapshot-5")
        stats = rig.restore(5, 6)
        assert [s["snapshot"].number for s in rig.sent] == [5]
        assert stats["failed"] == 2 and stats["restored"] == 0
        assert _slots(rig.local) == [] and _incomings(rig.local) == []

    def test_a_broken_slot_primitive_fails_the_restore(self, rig, monkeypatch):
        """The other half of the shared-lifecycle proof (the backup half is in
        test_snapper_layout.py): the slot the receive lands in is made by the
        one primitive both directions run, and breaking it fails a restore."""

        def broken_prepare(endpoint, number):
            raise __util__.SnapshotTransferError("prepare broken")

        monkeypatch.setattr(ops, "_snapper_prepare_slot", broken_prepare)
        stats = rig.restore(6)
        assert stats["failed"] == 1
        assert "prepare broken" in stats["errors"][0]
        assert _slots(rig.local) == [] and _incomings(rig.local) == []

    def test_a_rename_that_cannot_proceed_fails_the_restore_and_leaves_nothing(
        self, rig, monkeypatch
    ):
        def unsupported(src, dst):
            raise OSError(22, "no RENAME_NOREPLACE here")

        monkeypatch.setattr(__util__, "rename_noreplace", unsupported)
        stats = rig.restore(6)
        assert stats["failed"] == 1
        assert "could not publish" in stats["errors"][0]
        assert _slots(rig.local) == [] and _incomings(rig.local) == []

    def test_the_slot_number_is_taken_fresh_for_each_receive(self, rig, monkeypatch):
        """A slot snapper made between two receives is not overwritten: the
        number is asked for as each receive starts and again as each copy
        is published, never assigned for the whole plan up front."""
        engine_send = ops.send_snapshot

        def send_then_interlope(snapshot, destination_endpoint, **kw):
            engine_send(snapshot, destination_endpoint, **kw)
            if snapshot.number == 5:
                # Something else takes the next number while the first copy
                # is still in its temp.
                (rig.local / ".snapshots" / "2" / "snapshot").mkdir(parents=True)

        monkeypatch.setattr(ops, "send_snapshot", send_then_interlope)
        stats = rig.restore(5, 6)
        assert [e["landing"].parent.name for e in rig.sent] == [
            "1.incoming",
            "4.incoming",
        ]
        assert stats["slots"] == [(5, 3), (6, 4)]
        assert _slots(rig.local) == [2, 3, 4]
        assert list((rig.local / ".snapshots" / "2").iterdir()) == [
            rig.local / ".snapshots" / "2" / "snapshot"
        ]


# --------------------------------------------------------------------------- #
# the destination is a LIVE snapper config: numbers move under the restore
# --------------------------------------------------------------------------- #


class TestSnapperTakesTheNumberWhileTheReceiveIsInFlight:
    """``<n>.incoming`` is invisible to snapper, so its timeline or an
    operator's ``snapper create`` can take slot n during a restore. That
    snapshot must survive; the copy lands under the next free number, with
    its info.xml saying so."""

    def _interloper_during_receive(self, rig, monkeypatch, make):
        engine_send = ops.send_snapshot

        def send_then_snapper(snapshot, destination_endpoint, **kw):
            engine_send(snapshot, destination_endpoint, **kw)
            make(rig.local / ".snapshots" / "1")

        monkeypatch.setattr(ops, "send_snapshot", send_then_snapper)

    def test_a_snapshot_snapper_made_at_n_survives_and_the_copy_lands_at_n_plus_one(
        self, rig, monkeypatch, caplog
    ):
        def snapper_creates(slot):
            (slot / "snapshot").mkdir(parents=True)
            (slot / "snapshot" / "marker").write_text("snapper's")
            (slot / "info.xml").write_text("<snapshot><num>1</num></snapshot>")

        self._interloper_during_receive(rig, monkeypatch, snapper_creates)
        with caplog.at_level(logging.INFO):
            stats = rig.restore(6)
        assert (rig.local / ".snapshots" / "1" / "snapshot" / "marker").read_text() == (
            "snapper's"
        )
        assert (rig.local / ".snapshots" / "1" / "info.xml").read_text() == (
            "<snapshot><num>1</num></snapshot>"
        )
        assert (rig.local / ".snapshots" / "2" / "snapshot" / "payload").exists()
        assert (
            "<num>2</num>" in (rig.local / ".snapshots" / "2" / "info.xml").read_text()
        )
        assert stats["slots"] == [(6, 2)]
        assert _incomings(rig.local) == []
        assert not (rig.local / ".snapshots" / "1.stale").exists()
        assert "Restored snapshot 6 as local snapshot 2 (full)" in caplog.text
        assert (
            "Slot 1 was taken while the copy was being received (snapper, or "
            "another writer); it is left as it is and the copy is published as "
            "slot 2." in caplog.text
        )

    def test_an_empty_directory_snapper_just_made_is_not_taken_over(self, rig):
        """snapper's ``mkdir N`` precedes its ``info.xml``, and an empty ``N/``
        is exactly what ``os.rename`` replaces. The directory appears in the
        one window a re-read cannot see: after the publish has chosen N and
        before it renames. Mutation guard for the no-replace rename."""
        snaps = rig.local / ".snapshots"

        def next_then_snapper_mkdir(config):
            n = _next_number(config)
            # Only at publish (the temp exists) and only once: snapper's
            # mkdir lands between the choice of n and the rename.
            if (snaps / f"{n}.incoming").is_dir() and not (snaps / str(n)).exists():
                (snaps / str(n)).mkdir()
            return n

        rig.scanner.get_next_snapshot_number.side_effect = next_then_snapper_mkdir
        stats = rig.restore(6)
        assert (snaps / "1").is_dir()
        assert list((snaps / "1").iterdir()) == [], (
            "the empty directory snapper made was replaced by the restored copy"
        )
        assert stats["slots"] == [(6, 2)]
        assert (snaps / "2" / "snapshot" / "payload").exists()
        assert "<num>2</num>" in (snaps / "2" / "info.xml").read_text()

    def test_the_plan_says_the_number_is_decided_at_publish(self, rig, caplog):
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            rig.restore(6, dry_run=True)
        assert "the next free slot is 1" in caplog.text
        assert "free when it is published" in caplog.text


class TestOneRestoreAtATimeIntoAConfig:
    def test_a_second_restore_while_one_runs_is_refused_and_damages_nothing(
        self, rig, monkeypatch
    ):
        """Without the lock the second restore picks the same number and its
        open_slot removes the first's in-flight temp. Mutation guard for the
        restore lock."""
        engine_send = ops.send_snapshot
        second: dict = {}

        def send_then_second_restore(snapshot, destination_endpoint, **kw):
            engine_send(snapshot, destination_endpoint, **kw)
            landing = Path(destination_endpoint.config["path"]) / "snapshot"
            if "error" not in second:
                monkeypatch.setattr(ops, "send_snapshot", engine_send)
                try:
                    rig.restore(5)
                except RestoreError as e:
                    second["error"] = str(e)
                else:
                    second["error"] = None
                second["first_temp_survived"] = landing.exists()

        monkeypatch.setattr(ops, "send_snapshot", send_then_second_restore)
        stats = rig.restore(6)
        assert second["error"] is not None, "the second restore was not refused"
        assert "another operation holds the lock" in second["error"]
        assert "Nothing was restored" in second["error"]
        assert second["first_temp_survived"], (
            "the second restore removed the first's temp"
        )
        assert stats["slots"] == [(6, 1)]
        assert _slots(rig.local) == [1] and _incomings(rig.local) == []

    def test_the_lock_is_released_when_the_restore_ends(self, rig):
        rig.restore(6)
        rig.restore(5)
        assert _slots(rig.local) == [1, 2]

    def test_temps_of_restores_that_died_are_swept_first(self, rig):
        for name in ("1.incoming", "7.incoming"):
            (rig.local / ".snapshots" / name / "snapshot").mkdir(parents=True)
            (rig.local / ".snapshots" / name / "snapshot" / "half").write_text("x")
        stats = rig.restore(6)
        assert _incomings(rig.local) == []
        assert stats["slots"] == [(6, 1)]

    def test_a_dry_run_sweeps_nothing(self, rig):
        (rig.local / ".snapshots" / "7.incoming").mkdir(parents=True)
        rig.restore(6, dry_run=True)
        assert _incomings(rig.local) == ["7.incoming"]

    def test_a_config_without_snapshots_dir_is_refused(self, rig):
        import shutil

        shutil.rmtree(rig.local / ".snapshots")
        backups = list_snapper_backups(str(rig.backup))
        with pytest.raises(RestoreError, match="has no .*snapshots"):
            restore_snapper_snapshots(str(rig.backup), backups, backups[:1], "root")


# --------------------------------------------------------------------------- #
# the source is a prepared endpoint, and the backup is pinned on it
# --------------------------------------------------------------------------- #


class FakeRemote:
    """An ssh:// btrfs location: answers the listing's probes from a table,
    records whether it was prepared and what was pinned."""

    _is_remote = True

    def __init__(self, slots=(5, 6), info_xml=INFO_XML):
        self.config = {"path": REMOTE_BASE, "snap_prefix": ""}
        self.slots = list(slots)
        self.info_xml = info_xml
        self.prepared = 0
        self.lock_calls: list[tuple[str, str, bool, bool]] = []
        self.held: set[tuple[str, bool]] = set()

    def prepare(self):
        self.prepared += 1

    def set_lock(self, snapshot, lock_id, lock_state, parent=False):
        self.lock_calls.append((snapshot.get_name(), lock_id, lock_state, parent))
        key = (snapshot.get_name(), parent)
        if lock_state:
            self.held.add(key)
        else:
            self.held.discard(key)

    def _exec_remote_command(self, command, **kwargs):
        if command[0] == "find":
            out = "\n".join(f"{REMOTE_BASE}/.snapshots/{n}" for n in self.slots)
            return MagicMock(returncode=0, stdout=out.encode(), stderr=b"")
        if command[0] == "test":
            present = any(
                command[-1] == f"{REMOTE_BASE}/.snapshots/{n}/snapshot"
                for n in self.slots
            )
            return MagicMock(returncode=0 if present else 1, stdout=b"", stderr=b"")
        if command[0] == "cat":
            if self.info_xml is None:
                return MagicMock(returncode=1, stdout=b"", stderr=b"no such file")
            number = int(command[-1].split("/")[-2])
            return MagicMock(
                returncode=0,
                stdout=self.info_xml.format(num=number).encode(),
                stderr=b"",
            )
        raise AssertionError(f"unexpected remote command: {command}")


@pytest.fixture
def remote(rig, monkeypatch):
    fake = FakeRemote()
    rig.identities[REMOTE_BASE] = {5: "O5", 6: "O6"}
    monkeypatch.setattr(
        "btrfs_backup_ng.endpoint.choose_endpoint", lambda *a, **k: fake
    )
    return fake


class TestTheSourceEndpoint:
    def test_the_source_is_prepared_before_anything_is_sent(self, rig, remote):
        """prepare() is what runs the passwordless-sudo probe on an ssh://
        source; without it --ssh-sudo emits `sudo -S` and prompts. Mutation
        guard: drop the prepare() call and this fails."""
        order = []
        remote.prepare = lambda: order.append("prepare")  # type: ignore[method-assign]
        original = ops.send_snapshot

        def observing_send(*a, **k):
            order.append("send")
            return original(*a, **k)

        with patch.object(ops, "send_snapshot", observing_send):
            rig.restore(6, source=REMOTE)
        assert order == ["prepare", "send"]

    def test_the_source_snapshot_is_the_remote_slot_sent_by_its_own_endpoint(
        self, rig, remote
    ):
        rig.restore(6, source=REMOTE)
        (entry,) = rig.sent
        snapshot = entry["snapshot"]
        assert snapshot.endpoint is remote
        assert snapshot.get_path() == f"{REMOTE_BASE}/.snapshots/6/snapshot"
        assert snapshot.stream_uuid == "O6"

    def test_a_dry_run_prepares_and_lists_but_sends_nothing(self, rig, remote):
        rig.restore(6, source=REMOTE, dry_run=True)
        assert remote.prepared == 1
        assert rig.sent == []
        assert _slots(rig.local) == [] and _incomings(rig.local) == []


class TestPins:
    def test_the_backup_is_pinned_under_the_restore_session_and_released_after(
        self, rig, remote
    ):
        rig.restore(6, source=REMOTE)
        taken = [c for c in remote.lock_calls if c[2]]
        assert taken and all(c[1].startswith("restore:") for c in taken)
        assert remote.held == set()

    def test_the_parent_is_pinned_too_while_the_increment_is_sent(self, rig, remote):
        _present(rig, 3, "O5")
        rig.restore(6, source=REMOTE)
        assert ("snapshot-5", "restore:", True) in {
            (c[0], c[1][:8], c[3]) for c in remote.lock_calls if c[2]
        }
        assert remote.held == set()

    def test_a_failed_restore_releases_its_pin(self, rig, remote):
        """Mutation guard: release_on_failure=False leaves the pin held."""
        rig.failing.add("snapshot-6")
        stats = rig.restore(6, source=REMOTE)
        assert stats["failed"] == 1
        assert remote.held == set(), "a failed restore left a pin on the backup"


# --------------------------------------------------------------------------- #
# info.xml: snapper's own, renumbered, from one implementation
# --------------------------------------------------------------------------- #


class TestInfoXml:
    def test_the_slots_info_xml_is_the_backups_renumbered_verbatim(self, rig):
        rig.restore(6)
        xml = (rig.local / ".snapshots" / "1" / "info.xml").read_text()
        assert "<num>1</num>" in xml and "<num>6</num>" not in xml
        assert "<uid>0</uid>" in xml, "regenerating from parsed fields drops <uid>"
        assert "<key>reason</key>" in xml and "<key>requestor</key>" in xml
        assert "<description>before upgrade 6</description>" in xml

    def test_a_remote_backups_info_xml_is_renumbered_verbatim_too(self, rig, remote):
        rig.restore(6, source=REMOTE)
        xml = (rig.local / ".snapshots" / "1" / "info.xml").read_text()
        assert "<num>1</num>" in xml and "<uid>0</uid>" in xml

    def test_renumbering_changes_only_num(self):
        out = _restored_info_xml({"number": 6, "info_xml": INFO_XML.format(num=6)}, 42)
        text = out.decode()
        assert "<num>42</num>" in text and "<num>6</num>" not in text
        assert "<uid>0</uid>" in text
        assert text.count("<userdata>") == 2

    def test_unparseable_text_falls_back_to_the_parsed_fields(self, caplog):
        from btrfs_backup_ng.snapper.metadata import SnapperMetadata

        meta = SnapperMetadata(
            type="single",
            num=6,
            date=datetime(2026, 1, 1, 12, 0, 0),
            description="fallback",
            userdata={"reason": "manual"},
        )
        with caplog.at_level(logging.WARNING):
            out = _restored_info_xml(
                {"number": 6, "info_xml": "<snapshot><num>6</num>", "metadata": meta},
                42,
            ).decode()
        assert "<num>42</num>" in out and "fallback" in out
        assert "<key>reason</key>" in out
        assert meta.num == 6, "the listed metadata was mutated"
        assert "regenerating" in caplog.text

    def test_no_text_and_no_fields_gives_a_minimal_record_naming_the_backup(self):
        out = _restored_info_xml({"number": 6, "info_xml": None, "metadata": None}, 42)
        text = out.decode()
        assert "<num>42</num>" in text
        assert "Restored from backup 6" in text

    def test_a_backup_without_info_xml_still_restores_with_a_minimal_one(self, rig):
        _make_slot(rig.backup, 7, info_xml=None)
        rig.identities[str(rig.backup)][7] = "O7"
        rig.restore(7)
        xml = (rig.local / ".snapshots" / "1" / "info.xml").read_text()
        assert "<num>1</num>" in xml and "Restored from backup 7" in xml


# --------------------------------------------------------------------------- #
# the preview is the run
# --------------------------------------------------------------------------- #


class TestThePreviewIsTheRun:
    def test_the_dry_run_prints_the_plan_the_run_executes_and_changes_nothing(
        self, rig, caplog
    ):
        _present(rig, 3, "O5")
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            preview = rig.restore(5, 6, dry_run=True)
        previewed = _plan_lines(caplog.records)
        assert previewed == [
            "[1/2] snapshot 5 (full)",
            "[2/2] snapshot 6 (incremental from 5)",
        ]
        assert preview["restored"] == 0 and rig.sent == []
        assert _slots(rig.local) == [3] and _incomings(rig.local) == []

        caplog.clear()
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            rig.restore(5, 6)
        assert _plan_lines(caplog.records) == previewed

    def test_each_published_slot_is_reported_with_how_it_was_received(
        self, rig, caplog
    ):
        _present(rig, 3, "O5")
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            rig.restore(5, 6)
        reported = [
            r.getMessage()
            for r in caplog.records
            if r.getMessage().startswith("Restored ")
        ]
        assert reported == [
            "Restored snapshot 5 as local snapshot 4 (full)",
            "Restored snapshot 6 as local snapshot 5 (incremental from 5)",
        ]

    def test_the_plan_says_where_the_slots_start(self, rig, caplog):
        _present(rig, 3, "O5")
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            rig.restore(6, dry_run=True)
        assert "the next free slot is 4" in caplog.text


# --------------------------------------------------------------------------- #
# a raw store as the source
# --------------------------------------------------------------------------- #


def _raw_snapshot(name, stamp, *, parent_name=None, source_uuid=""):
    return RawSnapshot(
        name=name,
        stream_path=Path(f"/raw/{name}.btrfs"),
        parent_name=parent_name,
        source_uuid=source_uuid,
        created=datetime.strptime(stamp, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc),
    )


def _sidecar(store: Path, name: str, number: int, date: str, info_xml: str = ""):
    save_backup_metadata(
        store / f"{name}.snapper-meta.json",
        BackupMetadata(
            snapper_config="root",
            snapper_number=number,
            snapper_type="single",
            snapper_description=f"raw {number}",
            snapper_cleanup="number",
            snapper_pre_num=None,
            snapper_userdata={},
            snapper_date=date,
            original_info_xml=info_xml,
        ),
    )


@pytest.fixture
def raw_store(rig, tmp_path, monkeypatch):
    """A raw:// store holding a base and an increment, as sidecars and as the
    RawSnapshot objects its listing produces."""
    store = tmp_path / "raw"
    store.mkdir()
    base = _raw_snapshot("root-5-20260101-000000", "20260101-000000", source_uuid="O5")
    inc = _raw_snapshot(
        "root-6-20260102-000000",
        "20260102-000000",
        parent_name=base.name,
        source_uuid="O6",
    )
    _sidecar(store, base.name, 5, "2026-01-01 00:00:00", INFO_XML.format(num=5))
    _sidecar(store, inc.name, 6, "2026-01-02 00:00:00", INFO_XML.format(num=6))
    endpoint = RawEndpoint(config={"path": str(store)})
    endpoint.list_snapshots = lambda flush_cache=False: [base, inc]  # type: ignore[method-assign]
    for s in (base, inc):
        s.endpoint = endpoint
    endpoint.prepare = lambda: None  # type: ignore[method-assign]
    monkeypatch.setattr(
        "btrfs_backup_ng.endpoint.choose_endpoint", lambda *a, **k: endpoint
    )
    return SimpleNamespace(store=store, url=f"raw://{store}", base=base, inc=inc)


class TestARawStoreAsTheSource:
    def test_a_stored_increment_without_its_parent_is_refused_before_streaming(
        self, rig, raw_store
    ):
        with pytest.raises(RestoreError) as info:
            rig.restore(6, source=raw_store.url)
        message = str(info.value)
        assert raw_store.base.name in message and "Nothing was transferred" in message
        assert rig.sent == []
        assert _slots(rig.local) == [] and _incomings(rig.local) == []

    def test_selecting_the_parent_too_is_honoured(self, rig, raw_store):
        stats = rig.restore(5, 6, source=raw_store.url)
        assert [s["snapshot"].name for s in rig.sent] == [
            raw_store.base.name,
            raw_store.inc.name,
        ]
        assert stats["slots"] == [(5, 1), (6, 2)]

    def test_a_parent_the_config_already_holds_satisfies_the_increment(
        self, rig, raw_store
    ):
        _present(rig, 3, "O5")
        stats = rig.restore(6, source=raw_store.url)
        assert [s["snapshot"].name for s in rig.sent] == [raw_store.inc.name]
        assert stats["slots"] == [(6, 4)]

    def test_the_slots_info_xml_comes_from_the_sidecars_stored_xml(
        self, rig, raw_store
    ):
        rig.restore(5, source=raw_store.url)
        xml = (rig.local / ".snapshots" / "1" / "info.xml").read_text()
        assert "<num>1</num>" in xml and "<uid>0</uid>" in xml

    def test_the_plan_says_what_a_stored_stream_is(self, rig, raw_store, caplog):
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            rig.restore(5, 6, source=raw_store.url, dry_run=True)
        assert _plan_lines(caplog.records) == [
            f"[1/2] snapshot 5 ({raw_store.base.name}) (stored full stream)",
            f"[2/2] snapshot 6 ({raw_store.inc.name}) (stored increment of {raw_store.base.name})",
        ]

    def test_a_reused_number_restores_exactly_the_copy_selected(self, rig, raw_store):
        """Two backups share number 6; the command line selects by name and
        the facade restores that copy, never the other."""
        older = raw_store.inc
        newer = _raw_snapshot(
            "root-6-20260301-000000",
            "20260301-000000",
            parent_name=raw_store.base.name,
            source_uuid="O6b",
        )
        newer.endpoint = older.endpoint
        _sidecar(raw_store.store, newer.name, 6, "2026-03-01 00:00:00")
        older.endpoint.list_snapshots = lambda flush_cache=False: [  # type: ignore[method-assign]
            raw_store.base,
            older,
            newer,
        ]
        _present(rig, 1, "O5")
        backups = list_snapper_backups(raw_store.url)
        selected = [b for b in backups if b.get("backup_name") == older.name]
        restore_snapper_snapshots(
            raw_store.url, backups, selected, "root", options={"check_space": False}
        )
        assert [s["snapshot"].name for s in rig.sent] == [older.name]

    def test_the_verdict_judges_the_slot_for_a_stored_stream_too(
        self, rig, raw_store, monkeypatch
    ):
        """A snapper stream was sent from ``.snapshots/<n>/snapshot``, so the
        receive creates ``snapshot`` whatever the store named the stream. The
        verdict must look there, not for ``<slot>/<stream name>``: measured
        on real btrfs, that condemned a copy that was right there."""
        asked: list[str] = []
        monkeypatch.setattr(ops, "artifact_verdict", _REAL_VERDICT)
        monkeypatch.setattr(ops, "_received_subvolume_shape", lambda p: None)
        monkeypatch.setattr(
            LocalEndpoint,
            "subvolume_identity",
            lambda self, path: (
                asked.append(str(path)) or {"uuid": "N", "received_uuid": "O5"}
            ),
        )
        stats = rig.restore(5, source=raw_store.url)
        assert asked == [str(rig.local / ".snapshots" / "1.incoming" / "snapshot")]
        assert stats["restored"] == 1 and _slots(rig.local) == [1]

    def test_a_sidecar_whose_stream_is_gone_is_refused(self, rig, raw_store):
        raw_store.inc.endpoint.list_snapshots = lambda flush_cache=False: [
            raw_store.base
        ]  # type: ignore[method-assign]
        with pytest.raises(RestoreError, match="stream is missing"):
            rig.restore(6, source=raw_store.url)
        assert rig.sent == []


# --------------------------------------------------------------------------- #
# refusals before anything moves
# --------------------------------------------------------------------------- #


class TestRefusals:
    def test_an_unknown_config(self, rig):
        backups = list_snapper_backups(str(rig.backup))
        with pytest.raises(RestoreError, match="Local snapper config not found"):
            restore_snapper_snapshots(str(rig.backup), backups, backups[:1], "nope")

    def test_a_config_whose_subvolume_is_not_there(self, rig):
        rig.config.subvolume = rig.local / "gone"
        backups = list_snapper_backups(str(rig.backup))
        with pytest.raises(RestoreError, match="is not there"):
            restore_snapper_snapshots(str(rig.backup), backups, backups[:1], "root")

    def test_an_empty_selection(self, rig):
        backups = list_snapper_backups(str(rig.backup))
        with pytest.raises(RestoreError, match="Nothing was selected"):
            restore_snapper_snapshots(str(rig.backup), backups, [], "root")

    def test_a_selection_the_listing_does_not_hold(self, rig):
        backups = list_snapper_backups(str(rig.backup))
        with pytest.raises(RestoreError, match="not among the backups"):
            restore_snapper_snapshots(
                str(rig.backup), backups, [{"number": 99}], "root"
            )
        assert rig.sent == []


# --------------------------------------------------------------------------- #
# the slot object through the real ssh send
# --------------------------------------------------------------------------- #


class TestTheSlotThroughTheRealRemoteSend:
    """``_SnapperBtrfsBackup`` must satisfy its real consumer, ``SSHEndpoint.send``."""

    def _endpoint(self, **config):
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        base = {"path": REMOTE_BASE, "hostname": "nas", "username": "backup"}
        base.update(config)
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = base
        ep.hostname = base["hostname"]
        ep.ssh_manager = MagicMock()
        ep.ssh_manager.get_ssh_base_cmd.return_value = ["ssh", "backup@nas"]
        return ep

    def _slot(self, ep, number):
        return ops._SnapperBtrfsBackup(
            number, f"O{number}", base=REMOTE_BASE, endpoint=ep
        )

    def _sent_command(self, ep, snapshot, parent=None):
        with patch("btrfs_backup_ng.endpoint.ssh.subprocess.Popen") as popen:
            ep.send(snapshot, parent=parent)
        return popen.call_args[0][0][-1]

    def test_it_yields_a_remote_btrfs_send_of_the_slots_subvolume(self):
        ep = self._endpoint()
        assert self._sent_command(ep, self._slot(ep, 2)) == (
            f"btrfs send {REMOTE_BASE}/.snapshots/2/snapshot"
        )

    def test_a_parent_slot_becomes_dash_p(self):
        ep = self._endpoint()
        assert self._sent_command(ep, self._slot(ep, 2), parent=self._slot(ep, 1)) == (
            f"btrfs send -p {REMOTE_BASE}/.snapshots/1/snapshot {REMOTE_BASE}/.snapshots/2/snapshot"
        )

    def test_ssh_sudo_elevates_the_remote_send_without_a_prompt_once_probed(self):
        ep = self._endpoint(ssh_sudo=True, passwordless_sudo_available=True)
        assert self._sent_command(ep, self._slot(ep, 2)) == (
            f"sudo -n btrfs send {REMOTE_BASE}/.snapshots/2/snapshot"
        )

    def test_a_base_with_spaces_cannot_be_split_by_the_remote_shell(self):
        ep = self._endpoint()
        slot = ops._SnapperBtrfsBackup(2, "O2", base="/backups/my home", endpoint=ep)
        assert self._sent_command(ep, slot) == (
            "btrfs send '/backups/my home/.snapshots/2/snapshot'"
        )

    def test_the_slot_is_one_path_component_to_the_engine(self):
        """Joined under a destination path it names nothing real there, so the
        executor's pre-existence probe and its partial cleanup are inert for
        this layout; identity is the received_uuid, never the name."""
        slot = self._slot(self._endpoint(), 2)
        assert "/" not in slot.get_name()
        assert slot.stream_uuid == "O2"
        assert slot.locks == set() and slot.parent_locks == set()


def test_the_removed_paths_are_gone_not_neutralised():
    from btrfs_backup_ng.cli import snapper_cmd

    for gone in (
        "restore_snapper_snapshot",
        "_RemoteSubvolume",
        "_resolve_remote_snapper_backup",
        "_resolve_raw_snapper_backup",
    ):
        assert not hasattr(core_restore, gone), f"{gone} is still there"
    handler = inspect.getsource(snapper_cmd._handle_restore)
    assert "backup_numbers" not in handler, "the backup-side parent loop is back"
    assert "subprocess.Popen" not in inspect.getsource(core_restore)
