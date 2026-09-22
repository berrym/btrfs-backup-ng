"""Tier 2: the native restore as an engine transfer, on real btrfs.

Three loopback filesystems: originals on the first, their backup copies on
the second (a real ``btrfs send | btrfs receive``), recovery media on the
third that holds nothing. The unit suite proves the facade's decisions
against fakes; this proves them where they matter -- ``restore_snapshots``
over real ``LocalEndpoint``s, real receives, real ``btrfs subvolume show``:

- the latest snapshot brings its chain, the increment received against the
  restored base, and a rerun finds everything present by the stream's
  identity (the ORIGINAL's uuid, two hops away) and sends nothing;
- the preview prints exactly the plan the run then executes;
- an interrupted restore -- a subvolume under the name with no received_uuid,
  the shape a killed receive leaves -- is refused on rerun and left in place;
- the run marker exists while a receive is in flight and is gone after; a
  stale one over a bare subvolume is what ``--cleanup`` deletes, while an
  unmarked bare subvolume beside it is left alone;
- a raw store's increment restored alone brings its parent first;
- a restore that fails leaves no pin on the backup.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import types
from pathlib import Path

import pytest

import btrfs_backup_ng.core.operations as ops
import btrfs_backup_ng.core.restore as core_restore
from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import restore as restore_cli
from btrfs_backup_ng.core.layout import marker_dir
from btrfs_backup_ng.core.restore import RestoreError, restore_snapshots
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint

from .conftest import create_snapshot, requires_btrfs, send_snapshot

PREFIX = "home-"
FIRST = "home-20260101-120000"
SECOND = "home-20260102-120000"


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


def _endpoint(path: Path) -> LocalEndpoint:
    ep = LocalEndpoint(
        config={"path": str(path), "snap_prefix": PREFIX, "fs_checks": "skip"}
    )
    ep.prepare()
    return ep


def _subvolumes(path: Path) -> list[str]:
    return sorted(
        p.name for p in path.iterdir() if p.is_dir() and p.stat().st_ino == 256
    )


def _chain(first: Path, second: Path) -> tuple[Path, Path]:
    """O1, O2 on the first filesystem; S1, S2 their copies on the second,
    S2 received incrementally against S1."""
    data = first / "data"
    subprocess.run(
        ["btrfs", "subvolume", "create", str(data)], check=True, capture_output=True
    )
    (data / "payload.bin").write_bytes(os.urandom(256 * 1024))
    o1 = create_snapshot(data, first / FIRST, readonly=True)
    (data / "extra.bin").write_bytes(os.urandom(64 * 1024))
    o2 = create_snapshot(data, first / SECOND, readonly=True)
    send_snapshot(o1, second)
    send_snapshot(o2, second, parent=o1)
    return o1, o2


def _plan_lines(records):
    return [
        r.getMessage().strip()
        for r in records
        if re.match(r"^\s+\[\d+/\d+\] ", r.getMessage())
    ]


@pytest.mark.tier2
@requires_btrfs
class TestTheChainOntoEmptyMedia:
    def test_latest_brings_the_chain_and_a_rerun_sends_nothing(
        self, btrfs_three_volumes, caplog
    ):
        first, second, third = btrfs_three_volumes
        o1, o2 = _chain(first, second)
        backup = _endpoint(second)
        media = third / "restore"
        media.mkdir()

        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            preview = restore_snapshots(backup, _endpoint(media), dry_run=True)
        previewed = _plan_lines(caplog.records)
        assert preview["restored"] == 0 and _subvolumes(media) == []
        assert previewed == [
            f"[1/2] {FIRST} (full)",
            f"[2/2] {SECOND} (incremental from {FIRST})",
        ]

        caplog.clear()
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            stats = restore_snapshots(
                backup, _endpoint(media), options={"check_space": False}
            )
        assert stats == {"restored": 2, "skipped": 0, "failed": 0, "errors": []}
        assert _plan_lines(caplog.records) == previewed, (
            "the run's plan differs from the preview"
        )
        assert _subvolumes(media) == [FIRST, SECOND]
        base, inc = _show(media / FIRST), _show(media / SECOND)
        assert base["Received UUID"] == _show(o1)["UUID"], "two-hop identity"
        assert inc["Received UUID"] == _show(o2)["UUID"]
        assert inc["Parent UUID"] == base["UUID"], (
            "the increment was not received against the restored base"
        )
        assert (media / SECOND / "extra.bin").read_bytes() == (
            o2 / "extra.bin"
        ).read_bytes()
        assert list(marker_dir(media).glob("*.json")) == [], (
            "a marker outlived its receive"
        )

        # A rerun: everything present BY CORRESPONDENCE with the original's
        # uuid, which is what the backup copy's stream carries. Nothing is
        # sent, and the request is satisfied.
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=core_restore.logger.name):
            again = restore_snapshots(backup, _endpoint(media))
        assert again == {"restored": 0, "skipped": 1, "failed": 0, "errors": []}
        assert any(
            "Already at the destination" in r.getMessage() for r in caplog.records
        )
        assert _show(media / SECOND)["UUID"] == inc["UUID"], "the copy was re-created"

    def test_the_base_alone_lands_one_subvolume_then_latest_chains_onto_it(
        self, btrfs_three_volumes
    ):
        first, second, third = btrfs_three_volumes
        _chain(first, second)
        backup = _endpoint(second)
        media = third / "restore"
        media.mkdir()

        stats = restore_snapshots(
            backup,
            _endpoint(media),
            snapshot_name=FIRST,
            options={"check_space": False},
        )
        assert stats["restored"] == 1 and _subvolumes(media) == [FIRST]

        stats = restore_snapshots(
            backup, _endpoint(media), options={"check_space": False}
        )
        assert stats["restored"] == 1 and _subvolumes(media) == [FIRST, SECOND]
        assert _show(media / SECOND)["Parent UUID"] == _show(media / FIRST)["UUID"]


@pytest.mark.tier2
@requires_btrfs
class TestAnInterruptedRestoreIsRefusedNotSkipped:
    def test_a_bare_subvolume_under_the_name_refuses_the_rerun_and_stays(
        self, btrfs_three_volumes
    ):
        """A receive killed mid-stream leaves a SUBVOLUME under the right name
        with no received_uuid (probe 3 of the design). By name it looks
        restored; it is not, and the rerun must say so rather than exit 0."""
        first, second, third = btrfs_three_volumes
        _chain(first, second)
        backup = _endpoint(second)
        media = third / "restore"
        media.mkdir()
        partial = media / SECOND
        subprocess.run(
            ["btrfs", "subvolume", "create", str(partial)],
            check=True,
            capture_output=True,
        )
        (partial / "half.bin").write_bytes(b"x" * 4096)
        assert _show(partial)["Received UUID"] == "-"

        with pytest.raises(RestoreError) as info:
            restore_snapshots(backup, _endpoint(media), options={"check_space": False})
        message = str(info.value)
        assert str(partial) in message and "no received_uuid" in message
        assert "Nothing was transferred" in message
        assert _subvolumes(media) == [SECOND], "something was received anyway"
        assert (partial / "half.bin").exists(), "the stranger was deleted"


@pytest.mark.tier2
@requires_btrfs
class TestTheRunMarkerOnRealBtrfs:
    def test_the_marker_lives_exactly_as_long_as_the_receive(
        self, btrfs_three_volumes, monkeypatch
    ):
        first, second, third = btrfs_three_volumes
        _chain(first, second)
        media = third / "restore"
        media.mkdir()
        seen: list[list[dict]] = []
        real_send = ops.send_snapshot

        def spy(snapshot, destination_endpoint, **kw):
            result = real_send(snapshot, destination_endpoint, **kw)
            # The receive has exited; the executor has not yet verified the
            # copy, so its marker must still be down.
            seen.append(
                [json.loads(m.read_text()) for m in marker_dir(media).glob("*.json")]
            )
            return result

        monkeypatch.setattr(ops, "send_snapshot", spy)
        stats = restore_snapshots(
            _endpoint(second), _endpoint(media), options={"check_space": False}
        )
        assert stats["restored"] == 2
        assert [[m["snapshot"] for m in during] for during in seen] == [
            [FIRST],
            [SECOND],
        ]
        assert all(
            m["path"] == str(media / m["snapshot"]) for during in seen for m in during
        )
        assert list(marker_dir(media).glob("*.json")) == []

    def test_cleanup_deletes_only_what_a_stale_marker_names(
        self, btrfs_three_volumes, monkeypatch, capsys
    ):
        """Two bare subvolumes: one named by a stale marker (the interrupted
        restore), one under no marker (the operator's). The real ``btrfs
        subvolume delete`` removes the first and nothing else."""
        _first, _second, third = btrfs_three_volumes
        media = third / "restore"
        media.mkdir()
        _endpoint(media)  # the bookkeeping tree a real restore destination has
        abandoned = media / FIRST
        theirs = media / "operators-own"
        for path in (abandoned, theirs):
            subprocess.run(
                ["btrfs", "subvolume", "create", str(path)],
                check=True,
                capture_output=True,
            )
        (abandoned / "half.bin").write_bytes(b"x" * 4096)
        dead = subprocess.Popen(["true"])
        dead.wait()
        directory = marker_dir(media)
        directory.mkdir(parents=True, exist_ok=True)
        marker = directory / "deadbeef-001.json"
        marker.write_text(
            json.dumps(
                {
                    "format": 1,
                    "session": "deadbeef",
                    "pid": dead.pid,
                    "snapshot": FIRST,
                    "path": str(abandoned),
                    "started": "2026-01-01T00:00:00+00:00",
                }
            )
        )
        monkeypatch.setattr("builtins.input", lambda *_: "y")

        rc = restore_cli._execute_cleanup(
            types.SimpleNamespace(destination=str(media), dry_run=False)
        )

        out = capsys.readouterr().out
        assert rc == 0, out
        assert not abandoned.exists(), "the interrupted restore was not removed"
        assert not marker.exists()
        assert theirs.is_dir() and theirs.stat().st_ino == 256, (
            "the operator's subvolume was deleted"
        )
        assert "operators-own" in out and "not identifiable as ours" in out
        assert "1 deleted, 0 failed" in out


@pytest.mark.tier2
@requires_btrfs
class TestARawStoreRestoresItsChain:
    def test_one_increment_alone_brings_its_parent_first(
        self, btrfs_three_volumes, tmp_path
    ):
        first, _second, third = btrfs_three_volumes
        data = first / "data"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(data)], check=True, capture_output=True
        )
        (data / "payload.bin").write_bytes(os.urandom(256 * 1024))
        o1 = create_snapshot(data, first / FIRST, readonly=True)
        (data / "extra.bin").write_bytes(os.urandom(64 * 1024))
        o2 = create_snapshot(data, first / SECOND, readonly=True)
        store_dir = tmp_path / "raw"
        store_dir.mkdir()
        store = RawEndpoint(config={"path": str(store_dir), "snap_prefix": PREFIX})
        store.prepare()
        source = _endpoint(first)
        s1, s2 = (
            next(s for s in source.list_snapshots() if s.get_name() == n)
            for n in (FIRST, SECOND)
        )
        result = ops._execute_transfers(
            source, store, [(s1, None), (s2, s1)], {"check_space": False}
        )
        assert result.transferred_count == 2, result.failed
        stored = {
            s.get_name(): s
            for s in RawEndpoint(
                config={"path": str(store_dir), "snap_prefix": PREFIX}
            ).list_snapshots()
        }
        assert stored[SECOND].parent_name == FIRST, (
            "the sidecar does not name the parent"
        )
        assert stored[SECOND].stream_uuid == _show(o2)["UUID"]

        media = third / "restore"
        media.mkdir()
        raw_source = RawEndpoint(config={"path": str(store_dir), "snap_prefix": PREFIX})
        raw_source.prepare()
        stats = restore_snapshots(
            raw_source,
            _endpoint(media),
            snapshot_name=SECOND,
            options={"check_space": False},
        )
        assert stats["restored"] == 2, stats
        assert _subvolumes(media) == [FIRST, SECOND]
        base, inc = _show(media / FIRST), _show(media / SECOND)
        assert base["Received UUID"] == _show(o1)["UUID"]
        assert inc["Parent UUID"] == base["UUID"]
        assert (media / SECOND / "extra.bin").read_bytes() == (
            o2 / "extra.bin"
        ).read_bytes()


@pytest.mark.tier2
@requires_btrfs
class TestAFailedRestoreLeavesNoPin:
    def test_the_backups_lock_file_holds_no_restore_pin_after_a_failure(
        self, btrfs_three_volumes, monkeypatch
    ):
        first, second, third = btrfs_three_volumes
        _chain(first, second)
        backup = _endpoint(second)
        media = third / "restore"
        media.mkdir()
        pinned_during: list[dict] = []
        real_send = ops.send_snapshot

        def dies(snapshot, destination_endpoint, **kw):
            pinned_during.append(backup._read_locks())
            raise __util__.SnapshotTransferError("the link died")

        monkeypatch.setattr(ops, "send_snapshot", dies)
        stats = restore_snapshots(
            backup, _endpoint(media), options={"check_space": False}
        )
        monkeypatch.setattr(ops, "send_snapshot", real_send)
        assert stats["restored"] == 0 and stats["failed"] == 2
        # The pin WAS taken (the lock file named the snapshot under restore:)...
        assert pinned_during and any(
            lock.startswith("restore:")
            for entry in pinned_during[0].values()
            for lock in entry.get("locks", [])
        ), pinned_during
        # ...and nothing is left of it.
        assert backup._read_locks() == {}, "a failed restore left its pin on the backup"
        assert _subvolumes(media) == []
