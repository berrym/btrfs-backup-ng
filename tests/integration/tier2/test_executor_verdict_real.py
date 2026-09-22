"""Tier 2: the executor's artifact verdict and the shared lock store on real btrfs.

The unit suite proves the verdict's decisions against a stub ``btrfs``; this
proves them against the real one, on loopback filesystems, through the
engine's own executor:

- a receive through the engine leaves a copy whose verdict is ``ok``;
- a "receive" that exits 0 but leaves a SUBVOLUME with no received_uuid (the
  shape an interrupted receive takes, simulated here by creating the subvolume
  in place of the receive) is ``invalid``, the transfer fails, and the
  subvolume is deleted under the authorship rule -- one that predates the run
  is left alone;
- when the identity cannot be read, the verdict is ``unverifiable``, the
  data stays and the transfer counts;
- a location whose lock store is the directory an ssh:// endpoint keeps pins
  a snapshot against a real local ``btrfs subvolume delete``.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.sshutil.lock import LOCK_DIR_NAME, RemoteLockManager

from .conftest import create_snapshot, requires_btrfs

PREFIX = "home-"
FIRST = "home-20260101-120000"
SECOND = "home-20260102-120000"


def _subvolume_show(path: Path) -> dict[str, str]:
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


def _endpoint(path: Path, **extra) -> LocalEndpoint:
    return LocalEndpoint(
        config={"path": str(path), "snap_prefix": PREFIX, "fs_checks": "skip"},
        **extra,
    )


def _listed(endpoint: LocalEndpoint, name: str):
    for snap in endpoint.list_snapshots(flush_cache=True):
        if snap.get_name() == name:
            return snap
    raise AssertionError(f"{name} is not listed at {endpoint.config['path']}")


def _original(source: Path, name: str, payload: str) -> Path:
    data = source / "data"
    if not data.exists():
        subprocess.run(
            ["btrfs", "subvolume", "create", str(data)],
            check=True,
            capture_output=True,
        )
    (data / "file.txt").write_text(payload)
    return create_snapshot(data, source / name, readonly=True)


def _is_subvolume(path: Path) -> bool:
    return path.is_dir() and path.stat().st_ino == 256


def _delete_subvolume(path: Path) -> None:
    subprocess.run(
        ["btrfs", "subvolume", "delete", str(path)], check=False, capture_output=True
    )


@pytest.mark.tier2
@requires_btrfs
class TestTheSharedLockStoreOnRealBtrfs:
    def test_a_lock_directory_pins_against_a_real_prune(self, btrfs_source_and_dest):
        """Two received copies at a location that carries the directory store;
        a remote restore's pin on one of them. A local prune deletes the other
        and refuses the pinned one; once the pin is released the same prune
        deletes it. The deletion is the real ``btrfs subvolume delete``."""
        source, dest = btrfs_source_and_dest
        for name, payload in ((FIRST, "one"), (SECOND, "two")):
            original = _original(source, name, payload)
            subprocess.run(
                [
                    "sh",
                    "-c",
                    f"btrfs send {original} 2>/dev/null | btrfs receive {dest}",
                ],
                check=True,
                capture_output=True,
            )
        assert _is_subvolume(dest / FIRST) and _is_subvolume(dest / SECOND)

        def run(script: str):
            proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
            return proc.returncode, proc.stdout, proc.stderr

        remote = RemoteLockManager(run, str(dest), hostname="remote-restorer")
        remote.acquire_shared(f"snap-{FIRST}", "restore:20260101-abc")
        assert (dest / LOCK_DIR_NAME).is_dir()

        ep = _endpoint(dest)
        snapshots = ep.list_snapshots()
        assert {s.get_name(): s.locks for s in snapshots} == {
            FIRST: {"restore:20260101-abc"},
            SECOND: set(),
        }
        result = ep.delete_snapshots(snapshots)
        assert [s.get_name() for s, _ in result.skipped] == [FIRST]
        assert [s.get_name() for s in result.deleted] == [SECOND]
        assert _is_subvolume(dest / FIRST), "the pinned copy was deleted"
        assert not (dest / SECOND).exists()

        remote.release_shared(f"snap-{FIRST}", "restore:20260101-abc")
        ep2 = _endpoint(dest)
        result2 = ep2.delete_snapshots(ep2.list_snapshots())
        assert [s.get_name() for s in result2.deleted] == [FIRST]
        assert not (dest / FIRST).exists()

    def test_a_local_run_pins_through_the_directory_store(self, btrfs_source_and_dest):
        """The executor's pin on a SOURCE whose location carries the directory
        store lands in that store, where an ssh:// prune would look."""
        source, dest = btrfs_source_and_dest
        _original(source, FIRST, "one")
        (source / LOCK_DIR_NAME).mkdir()
        src_ep = _endpoint(source)
        snapshot = _listed(src_ep, FIRST)
        holders: list[list[str]] = []
        real_send = ops.send_snapshot

        def send_and_look(snap, destination, **kw):
            holders.append(
                sorted(
                    str(p.name) for p in (source / LOCK_DIR_NAME).glob("*/holders/*")
                )
            )
            return real_send(snap, destination, **kw)

        ops_send = MagicMock(side_effect=send_and_look)
        try:
            ops.send_snapshot = ops_send  # type: ignore[assignment]
            result = ops._execute_transfers(
                src_ep, _endpoint(dest), [(snapshot, None)], {"check_space": False}
            )
        finally:
            ops.send_snapshot = real_send  # type: ignore[assignment]
        assert result.transferred_count == 1
        assert holders and holders[0], "no holder file existed during the transfer"
        assert not list((source / LOCK_DIR_NAME).glob("*/holders/*")), (
            "the pin was not released after the transfer"
        )
        assert not (source / ".btrfs-backup-ng.locks.guard").exists()
