"""Tier 2: the snapper layout in both directions, on real btrfs.

Three loopback filesystems: a snapper-shaped source on the first
(``data/.snapshots/<n>/snapshot`` beside ``info.xml``, as snapper lays a
config out), its backup on the second (``send_snapper_snapshot`` into
``.snapshots/<n>``), and on the third the config a restore lands in, holding
nothing. The unit suite proves the layout's decisions against plain
directories and a faked transfer; this proves them where they matter -- real
``btrfs send | btrfs receive``, real ``btrfs subvolume show``:

- a backup publishes slots with their info.xml and no ``.incoming`` behind;
- one increment restored onto an empty config is a full receive into slot 1
  whose Received UUID is the ORIGINAL's uuid, two hops away;
- with the base then restored too, the increment restored again is received
  AGAINST the restored base -- the parent came from the config, not from the
  backup side -- and every restore is a new slot;
- the slot's info.xml is snapper's own, renumbered, ``<uid>`` and all;
- a stream that fails in the receive leaves no numbered slot and no
  ``.incoming``, and no pin on the backup;
- a raw store's increment is refused before streaming without its parent
  and restores its chain when the parent is selected too.

No snapper binary is needed: the config is a ``SnapperConfig`` over a
subvolume of the third filesystem, which is what snapper itself would hand
the scanner.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.core.operations as ops
from btrfs_backup_ng.core.restore import (
    RestoreError,
    list_snapper_backups,
    restore_snapper_snapshots,
)
from btrfs_backup_ng.core.transfer import tail_stderr
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.snapper import SnapperConfig, SnapperScanner
from btrfs_backup_ng.snapper.metadata import parse_info_xml
from btrfs_backup_ng.snapper.snapshot import SnapperSnapshot

from .conftest import create_snapshot, requires_btrfs

INFO_XML = """<?xml version="1.0"?>
<snapshot>
  <type>single</type>
  <num>{num}</num>
  <date>2026-01-0{num} 12:00:00</date>
  <uid>0</uid>
  <description>tier2 {num}</description>
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


def _show(path: Path) -> dict[str, str]:
    out = subprocess.run(
        ["btrfs", "subvolume", "show", str(path)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    fields: dict[str, str] = {}
    for line in out.splitlines():
        key, sep, value = line.strip().partition(":")
        if sep:
            fields[key.strip()] = value.strip()
    return fields


def _snapper_source(first: Path) -> tuple[Path, list[SnapperSnapshot], bytes]:
    """A snapper-shaped config on the first filesystem: two numbered
    snapshots of ``data``, the second holding a genuine delta."""
    data = first / "data"
    subprocess.run(
        ["btrfs", "subvolume", "create", str(data)], check=True, capture_output=True
    )
    (data / "payload.bin").write_bytes(os.urandom(256 * 1024))
    snapshots = []
    delta = b""
    for number in (1, 2):
        if number == 2:
            delta = os.urandom(64 * 1024)
            (data / "extra.bin").write_bytes(delta)
        slot = data / ".snapshots" / str(number)
        slot.mkdir(parents=True)
        create_snapshot(data, slot / "snapshot", readonly=True)
        (slot / "info.xml").write_text(INFO_XML.format(num=number))
        snapshots.append(
            SnapperSnapshot(
                config_name="t2",
                number=number,
                metadata=parse_info_xml(slot / "info.xml"),
                subvolume_path=slot / "snapshot",
                info_xml_path=slot / "info.xml",
            )
        )
    return data, snapshots, delta


def _target(path: Path) -> LocalEndpoint:
    path.mkdir(exist_ok=True)
    ep = LocalEndpoint(
        config={"path": str(path), "snap_prefix": "", "fs_checks": "skip"}
    )
    ep.prepare()
    return ep


def _backup(snapshots, target) -> None:
    ops.send_snapper_snapshot(snapshots[0], target)
    ops.send_snapper_snapshot(
        snapshots[1], target, parent_snapper_snapshot=snapshots[0]
    )


def _config(third: Path, monkeypatch) -> SnapperConfig:
    """The config a restore lands in: a subvolume of the third filesystem with
    an empty ``.snapshots``, handed to the restore the way the scanner would."""
    subvol = third / "recover"
    subprocess.run(
        ["btrfs", "subvolume", "create", str(subvol)], check=True, capture_output=True
    )
    (subvol / ".snapshots").mkdir()
    config = SnapperConfig(name="rec", subvolume=subvol)
    scanner = MagicMock()
    scanner.get_config.side_effect = lambda name: config if name == "rec" else None
    scanner.get_next_snapshot_number.side_effect = (
        lambda cfg: SnapperScanner.get_next_snapshot_number(None, cfg)  # type: ignore[arg-type]
    )
    monkeypatch.setattr("btrfs_backup_ng.snapper.SnapperScanner", lambda: scanner)
    return config


def _slots(config: SnapperConfig) -> list[int]:
    return sorted(
        int(p.name) for p in config.snapshots_dir.iterdir() if p.name.isdigit()
    )


def _temps(config: SnapperConfig) -> list[str]:
    return sorted(
        p.name for p in config.snapshots_dir.iterdir() if p.name.endswith(".incoming")
    )


def _restore(source: str, config: SnapperConfig, *numbers, **kw):
    backups = list_snapper_backups(source)
    selected = [b for b in backups if b["number"] in numbers]
    return restore_snapper_snapshots(
        source, backups, selected, config.name, options={"check_space": False}, **kw
    )


@pytest.mark.tier2
@requires_btrfs
class TestBackupThenRestoreThroughTheLayout:
    def test_the_backup_publishes_slots_with_their_info_xml(self, btrfs_three_volumes):
        first, second, _third = btrfs_three_volumes
        _data, snapshots, _delta = _snapper_source(first)
        _backup(snapshots, _target(second / "backup"))
        base = second / "backup" / ".snapshots"
        # The writer lock file lives beside the slots and is not a slot.
        entries = sorted(p.name for p in base.iterdir() if not p.name.startswith("."))
        assert entries == ["1", "2"], "a temp slot outlived its publish"
        for number in (1, 2):
            copy = _show(base / str(number) / "snapshot")
            assert (
                copy["Received UUID"]
                == _show(snapshots[number - 1].subvolume_path)["UUID"]
            )
            assert (base / str(number) / "info.xml").read_text() == INFO_XML.format(
                num=number
            )
        assert (
            _show(base / "2" / "snapshot")["Parent UUID"]
            == (_show(base / "1" / "snapshot")["UUID"])
        ), "the second backup was not received against the first"

    def test_one_increment_onto_an_empty_config_is_one_full_receive_into_slot_one(
        self, btrfs_three_volumes, monkeypatch
    ):
        first, second, third = btrfs_three_volumes
        _data, snapshots, delta = _snapper_source(first)
        _backup(snapshots, _target(second / "backup"))
        config = _config(third, monkeypatch)

        stats = _restore(str(second / "backup"), config, 2)

        assert stats["restored"] == 1 and stats["failed"] == 0, stats
        assert stats["slots"] == [(2, 1)]
        assert _slots(config) == [1] and _temps(config) == []
        restored = config.snapshots_dir / "1" / "snapshot"
        copy = _show(restored)
        assert copy["Parent UUID"] == "-", "the increment was sent against a parent"
        assert copy["Received UUID"] == _show(snapshots[1].subvolume_path)["UUID"], (
            "two-hop identity: the restored copy records the ORIGINAL's uuid"
        )
        assert (restored / "extra.bin").read_bytes() == delta
        xml = (config.snapshots_dir / "1" / "info.xml").read_text()
        assert "<num>1</num>" in xml and "<num>2</num>" not in xml
        assert "<uid>0</uid>" in xml and "<key>requestor</key>" in xml
        assert "<description>tier2 2</description>" in xml

    def test_the_parent_comes_from_the_config_and_every_restore_is_a_new_slot(
        self, btrfs_three_volumes, monkeypatch
    ):
        first, second, third = btrfs_three_volumes
        _data, snapshots, delta = _snapper_source(first)
        _backup(snapshots, _target(second / "backup"))
        config = _config(third, monkeypatch)
        source = str(second / "backup")

        _restore(source, config, 2)  # slot 1: the increment, in full
        _restore(source, config, 1)  # slot 2: the base
        stats = _restore(source, config, 2)  # slot 3: the increment, again

        assert stats["slots"] == [(2, 3)]
        assert _slots(config) == [1, 2, 3] and _temps(config) == []
        base, again = (
            _show(config.snapshots_dir / "2" / "snapshot"),
            _show(config.snapshots_dir / "3" / "snapshot"),
        )
        assert again["Parent UUID"] == base["UUID"], (
            "the increment was not received against the base the config holds"
        )
        assert (
            config.snapshots_dir / "3" / "snapshot" / "extra.bin"
        ).read_bytes() == delta
        assert (config.snapshots_dir / "3" / "info.xml").read_text().count(
            "<num>3</num>"
        ) == 1

    def test_all_onto_an_empty_config_is_a_chain_and_the_preview_is_the_run(
        self, btrfs_three_volumes, monkeypatch, caplog
    ):
        import logging
        import re

        import btrfs_backup_ng.core.restore as core_restore

        first, second, third = btrfs_three_volumes
        _data, snapshots, _delta = _snapper_source(first)
        _backup(snapshots, _target(second / "backup"))
        config = _config(third, monkeypatch)
        source = str(second / "backup")

        def plan_lines():
            return [
                r.getMessage().strip()
                for r in caplog.records
                if re.match(r"^\s+\[\d+/\d+\] ", r.getMessage())
            ]

        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            _restore(source, config, 1, 2, dry_run=True)
        previewed = plan_lines()
        assert previewed == [
            "[1/2] snapshot 1 (full)",
            "[2/2] snapshot 2 (incremental from 1)",
        ]
        assert _slots(config) == [] and _temps(config) == []

        caplog.clear()
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            stats = _restore(source, config, 1, 2)
        assert plan_lines() == previewed
        assert stats["slots"] == [(1, 1), (2, 2)]
        assert (
            _show(config.snapshots_dir / "2" / "snapshot")["Parent UUID"]
            == (_show(config.snapshots_dir / "1" / "snapshot")["UUID"])
        )


@pytest.mark.tier2
@requires_btrfs
class TestAFailedReceiveLeavesNothing:
    def test_a_stream_the_receive_rejects_leaves_no_slot_no_temp_and_no_pin(
        self, btrfs_three_volumes, monkeypatch
    ):
        first, second, third = btrfs_three_volumes
        _data, snapshots, _delta = _snapper_source(first)
        _backup(snapshots, _target(second / "backup"))
        config = _config(third, monkeypatch)
        backup = second / "backup"

        pinned_during: list[dict] = []

        def garbage_send(self, snapshot, parent=None, clones=None):
            # What the backup holds while the stream is in flight.
            probe = LocalEndpoint(
                config={"path": str(backup), "snap_prefix": "", "fs_checks": "skip"}
            )
            pinned_during.append(probe._read_locks())
            proc = subprocess.Popen(
                ["sh", "-c", "head -c 65536 /dev/urandom"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            tail_stderr(proc)
            return proc

        monkeypatch.setattr(LocalEndpoint, "send", garbage_send)
        stats = _restore(str(backup), config, 2)

        assert stats["restored"] == 0 and stats["failed"] == 1, stats
        assert "snapshot 2" in stats["errors"][0]
        assert _slots(config) == [], "a failed receive left a numbered slot"
        assert _temps(config) == [], "a failed receive left its .incoming"
        assert pinned_during and any(
            lock.startswith("restore:")
            for entry in pinned_during[0].values()
            for lock in entry.get("locks", [])
        ), pinned_during
        probe = LocalEndpoint(
            config={"path": str(backup), "snap_prefix": "", "fs_checks": "skip"}
        )
        assert probe._read_locks() == {}, "a failed restore left its pin on the backup"


@pytest.mark.tier2
@requires_btrfs
class TestARawStoreAsTheSource:
    def test_an_increment_alone_is_refused_and_the_chain_restores_when_selected(
        self, btrfs_three_volumes, monkeypatch, tmp_path
    ):
        first, _second, third = btrfs_three_volumes
        _data, snapshots, delta = _snapper_source(first)
        store_dir = tmp_path / "raw"
        store_dir.mkdir()
        store = RawEndpoint(config={"path": str(store_dir), "snap_prefix": ""})
        store.prepare()
        _backup(snapshots, store)
        config = _config(third, monkeypatch)
        url = f"raw://{store_dir}"

        with pytest.raises(RestoreError, match="Nothing was transferred"):
            _restore(url, config, 2)
        assert _slots(config) == [] and _temps(config) == []

        stats = _restore(url, config, 1, 2)
        assert stats["slots"] == [(1, 1), (2, 2)], stats
        base, inc = (
            _show(config.snapshots_dir / "1" / "snapshot"),
            _show(config.snapshots_dir / "2" / "snapshot"),
        )
        assert base["Received UUID"] == _show(snapshots[0].subvolume_path)["UUID"]
        assert inc["Parent UUID"] == base["UUID"]
        assert (
            config.snapshots_dir / "2" / "snapshot" / "extra.bin"
        ).read_bytes() == delta
        xml = (config.snapshots_dir / "2" / "info.xml").read_text()
        assert "<num>2</num>" in xml and "<uid>0</uid>" in xml


@pytest.mark.tier2
@requires_btrfs
class TestTheConfigIsLiveWhileTheRestoreRuns:
    def test_a_snapshot_snapper_creates_at_n_during_the_receive_survives(
        self, btrfs_three_volumes, monkeypatch, caplog
    ):
        """What ``snapper create`` does while the receive is in flight: a
        read-only snapshot of the config's subvolume at ``.snapshots/1/snapshot``
        beside an info.xml. The restore must leave it and land at 2."""
        import logging

        from btrfs_backup_ng.core.layout import SnapperLayout

        first, second, third = btrfs_three_volumes
        _data, snapshots, delta = _snapper_source(first)
        _backup(snapshots, _target(second / "backup"))
        config = _config(third, monkeypatch)
        real_started = SnapperLayout._receive_started

        def started_then_snapper(self, snapshot_name):
            number = real_started(self, snapshot_name)
            slot = config.snapshots_dir / str(number)
            slot.mkdir()
            create_snapshot(config.subvolume, slot / "snapshot", readonly=True)
            (slot / "snapshot-by").write_text("snapper")
            (slot / "info.xml").write_text(INFO_XML.format(num=number))
            return number

        monkeypatch.setattr(SnapperLayout, "_receive_started", started_then_snapper)
        with caplog.at_level(logging.INFO):
            stats = _restore(str(second / "backup"), config, 2)

        assert stats["restored"] == 1 and stats["slots"] == [(2, 2)], stats
        assert _slots(config) == [1, 2] and _temps(config) == []
        theirs = config.snapshots_dir / "1"
        assert (theirs / "snapshot-by").read_text() == "snapper"
        assert (theirs / "info.xml").read_text() == INFO_XML.format(num=1)
        assert (theirs / "snapshot").is_dir(), "snapper's snapshot was deleted"
        assert _show(theirs / "snapshot")["Received UUID"] == "-"
        ours = config.snapshots_dir / "2"
        assert (ours / "snapshot" / "extra.bin").read_bytes() == delta
        assert "<num>2</num>" in (ours / "info.xml").read_text()
        assert (
            _show(ours / "snapshot")["Received UUID"]
            == (_show(snapshots[1].subvolume_path)["UUID"])
        )
        assert "Slot 1 was taken while the copy was being received" in caplog.text
        assert "Restored snapshot 2 as local snapshot 2 (full)" in caplog.text

    def test_two_restores_into_one_config_the_second_is_refused_and_the_first_completes(
        self, btrfs_three_volumes, monkeypatch
    ):
        import threading

        from btrfs_backup_ng.core.layout import SnapperLayout

        first, second, third = btrfs_three_volumes
        _data, snapshots, _delta = _snapper_source(first)
        _backup(snapshots, _target(second / "backup"))
        config = _config(third, monkeypatch)
        source = str(second / "backup")
        real_started = SnapperLayout._receive_started
        opened = threading.Event()
        second_done = threading.Event()
        outcome: dict = {}

        def started_then_wait(self, snapshot_name):
            number = real_started(self, snapshot_name)
            opened.set()
            assert second_done.wait(60), "the second restore never finished"
            return number

        monkeypatch.setattr(SnapperLayout, "_receive_started", started_then_wait)

        def run_first():
            outcome["first"] = _restore(source, config, 2)

        runner = threading.Thread(target=run_first)
        runner.start()
        assert opened.wait(60), "the first restore never opened its slot"
        assert _temps(config) == ["1.incoming"]
        try:
            _restore(source, config, 1)
            outcome["second"] = None
        except RestoreError as e:
            outcome["second"] = str(e)
        finally:
            second_done.set()
        runner.join(120)
        assert not runner.is_alive()

        assert outcome["second"] is not None, "the second restore was not refused"
        assert "another operation holds the lock" in outcome["second"]
        assert outcome["first"]["restored"] == 1, outcome["first"]
        assert _slots(config) == [1] and _temps(config) == []
        assert (config.snapshots_dir / "1" / "snapshot" / "payload.bin").exists()


@pytest.mark.tier2
@requires_btrfs
class TestTheEdgesHoldOnRealBtrfs:
    """The snapper edges pinned by unit tests, run against real subvolumes:
    one writer at a time into a target, a pinned slot that no prune deletes,
    a raw pin another process can see, and a read-only medium as a source."""

    def test_a_second_backup_into_a_busy_target_is_refused_and_disturbs_nothing(
        self, btrfs_three_volumes
    ):
        from btrfs_backup_ng import __util__
        from btrfs_backup_ng.core.layout import SnapperLayout

        first, second, _third = btrfs_three_volumes
        _data, snapshots, _delta = _snapper_source(first)
        target = _target(second / "backup")
        ops.send_snapper_snapshot(snapshots[0], target)
        holder = SnapperLayout(_target(second / "backup"))
        with holder.writer_lock("a backup already running"):
            with pytest.raises(
                __util__.SnapshotTransferError, match="another operation holds the lock"
            ):
                ops.send_snapper_snapshot(
                    snapshots[1], target, parent_snapper_snapshot=snapshots[0]
                )
        snapshots_dir = second / "backup" / ".snapshots"
        published = sorted(
            p.name for p in snapshots_dir.iterdir() if not p.name.startswith(".")
        )
        assert published == ["1"], published
        assert (snapshots_dir / SnapperLayout.WRITER_LOCK_NAME).is_file()
        # Released: the same send goes through afterwards.
        ops.send_snapper_snapshot(
            snapshots[1], target, parent_snapper_snapshot=snapshots[0]
        )
        assert (second / "backup" / ".snapshots" / "2" / "snapshot").is_dir()

    def test_a_slot_pinned_by_a_restore_survives_the_prune(self, btrfs_three_volumes):
        from types import SimpleNamespace

        from btrfs_backup_ng.cli.prune import delete_snapper_backups

        first, second, _third = btrfs_three_volumes
        _data, snapshots, _delta = _snapper_source(first)
        backup = second / "backup"
        _backup(snapshots, _target(backup))
        # What a restore in another process does while it reads slot 2.
        reader = LocalEndpoint(
            config={"path": str(backup), "snap_prefix": "", "fs_checks": "skip"}
        )
        reader.set_lock(
            SimpleNamespace(
                locks=set(), parent_locks=set(), get_name=lambda: "snapshot-2"
            ),
            "restore:s1",
            True,
        )
        backups = list_snapper_backups(str(backup))
        assert [b["number"] for b in backups] == [1, 2]
        deleted, errors = delete_snapper_backups(str(backup), backups, {})
        assert (deleted, errors) == (1, [])
        assert not (backup / ".snapshots" / "1").exists()
        assert (backup / ".snapshots" / "2" / "snapshot").is_dir(), (
            "the prune deleted the slot a restore was reading"
        )

    def test_a_raw_pin_is_honoured_by_a_prune_in_another_process(
        self, btrfs_three_volumes
    ):
        first, second, _third = btrfs_three_volumes
        _data, snapshots, _delta = _snapper_source(first)
        store = second / "raw"
        store.mkdir()
        raw = RawEndpoint(config={"path": str(store), "snap_prefix": ""})
        raw.prepare()
        ops.send_snapper_snapshot(snapshots[0], raw)
        streams = raw.list_snapshots()
        assert len(streams) == 1
        raw.set_lock(streams[0], "restore:s1", True)
        pruner = RawEndpoint(config={"path": str(store), "snap_prefix": ""})
        pruner.prepare()
        theirs = pruner.list_snapshots()
        result = pruner.delete_snapshots(theirs, delete_session={theirs[0].get_name()})
        assert result.deleted_count == 0
        assert streams[0].stream_path.exists()
        raw.set_lock(streams[0], "restore:s1", False)
        result = pruner.delete_snapshots(theirs, delete_session={theirs[0].get_name()})
        assert result.deleted_count == 1
        assert not streams[0].stream_path.exists()

    def test_a_read_only_medium_restores_without_writing_to_it(
        self, btrfs_three_volumes, monkeypatch
    ):
        first, second, third = btrfs_three_volumes
        _data, snapshots, delta = _snapper_source(first)
        backup = second / "backup"
        _backup(snapshots, _target(backup))
        config = _config(third, monkeypatch)
        subprocess.run(
            ["mount", "-o", "remount,ro", str(second)], check=True, capture_output=True
        )
        try:
            before = sorted(p.name for p in backup.iterdir())
            stats = _restore(str(backup), config, 2)
            assert stats["restored"] == 1, stats
            assert _slots(config) == [1]
            landed = config.snapshots_dir / "1" / "snapshot" / "extra.bin"
            assert landed.read_bytes() == delta
            assert sorted(p.name for p in backup.iterdir()) == before, (
                "the restore wrote to the read-only medium"
            )
        finally:
            subprocess.run(
                ["mount", "-o", "remount,rw", str(second)],
                check=True,
                capture_output=True,
            )
