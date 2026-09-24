"""Tier 2: a raw store on a medium mounted read-only restores, byte-exact,
with nothing written to the medium.

The disaster-recovery case: the backups are on a drive that is mounted
read-only (or can only be mounted so), and the restore reads them back. It
is driven through the real command line, as an operator would, because the
regression it pins was found there: making ``raw://`` pins durable made the
restore refuse with "Could not lock ... pass --skip-remote-lock" where the
release before restored. Every pin writer now decides read-only by one rule
-- the location's filesystem is mounted read-only, per the kernel's mount
table and a failed write -- says so at INFO, and goes on without the pin,
which a read-only location does not need.

Needs root and btrfs (loopback filesystems); skips otherwise.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import requires_btrfs

pytestmark = [pytest.mark.tier2, requires_btrfs]


def _cli(*argv: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "COLUMNS": "200"}
    env.pop("BTRFS_BACKUP_LOG_LEVEL", None)
    return subprocess.run(
        [sys.executable, "-m", "btrfs_backup_ng", *argv],
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
    )


def _listing(root: Path) -> list[tuple[str, int, int]]:
    """Every path under ``root`` with its size and mtime: what a write would change."""
    out = []
    for path in sorted(root.rglob("*")):
        st = path.lstat()
        out.append((str(path.relative_to(root)), st.st_size, st.st_mtime_ns))
    return out


def _remount(mount: Path, mode: str) -> None:
    subprocess.run(
        ["mount", "-o", f"remount,{mode}", str(mount)], check=True, capture_output=True
    )


class TestARawStoreOnAReadOnlyMedium:
    def test_the_restore_is_byte_exact_and_writes_nothing(
        self, btrfs_source_and_dest, tmp_path
    ):
        source, medium = btrfs_source_and_dest
        # A subvolume with recognisable content, snapshotted once.
        data = source / "vol"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(data)], check=True, capture_output=True
        )
        payload = os.urandom(512 * 1024)
        (data / "payload.bin").write_bytes(payload)

        # The raw store lives on the second filesystem; `run` takes the
        # snapshot and writes a real raw backup into it, through the command
        # line.
        store = medium / "store"
        store.mkdir()
        config = tmp_path / "config.toml"
        config.write_text(
            "[global]\n"
            'snapshot_dir = ".snapshots"\n'
            "\n[global.retention]\n"
            'min = "all"\n'
            f'\n[[volumes]]\npath = "{data}"\nsnapshot_prefix = "vol-"\n'
            f'\n[[volumes.targets]]\npath = "raw://{store}"\n'
        )
        backed_up = _cli("-c", str(config), "run")
        assert backed_up.returncode == 0, backed_up.stdout + backed_up.stderr
        streams = sorted(p.name for p in store.iterdir() if p.name.endswith(".btrfs"))
        assert streams, list(store.iterdir())

        # The medium becomes read-only, as a disaster-recovery drive would be.
        _remount(medium, "ro")
        try:
            before = _listing(medium)
            restore_to = source / "restored"
            restore_to.mkdir()
            restored = _cli("restore", f"raw://{store}", str(restore_to), "--all")
            text = restored.stdout + restored.stderr
            assert restored.returncode == 0, text
            assert "Refusing to continue unprotected" not in text
            assert "mounted read-only" in text, text
            assert _listing(medium) == before, (
                "the restore wrote to the read-only medium"
            )
            assert not (store / ".btrfs-backup-ng.locks").exists()
            copies = [p for p in restore_to.iterdir() if p.name.startswith("vol-")]
            assert len(copies) == 1, list(restore_to.iterdir())
            landed = (copies[0] / "payload.bin").read_bytes()
            assert (
                hashlib.sha256(landed).hexdigest()
                == hashlib.sha256(payload).hexdigest()
            )
        finally:
            _remount(medium, "rw")

    def test_the_same_medium_writable_is_pinned_and_released(
        self, btrfs_source_and_dest, tmp_path
    ):
        """The rule bites only on a read-only mount: the same store on a
        writable medium records the restore's pin and releases it after."""
        source, medium = btrfs_source_and_dest
        data = source / "vol"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(data)], check=True, capture_output=True
        )
        (data / "payload.bin").write_bytes(b"x" * 4096)
        store = medium / "store"
        store.mkdir()
        config = tmp_path / "config.toml"
        config.write_text(
            '[global]\nsnapshot_dir = ".snapshots"\n\n[global.retention]\nmin = "all"\n'
            f'\n[[volumes]]\npath = "{data}"\nsnapshot_prefix = "vol-"\n'
            f'\n[[volumes.targets]]\npath = "raw://{store}"\n'
        )
        backed_up = _cli("-c", str(config), "run")
        assert backed_up.returncode == 0, backed_up.stdout + backed_up.stderr
        restore_to = source / "restored"
        restore_to.mkdir()
        restored = _cli("restore", f"raw://{store}", str(restore_to), "--all")
        text = restored.stdout + restored.stderr
        assert restored.returncode == 0, text
        assert "mounted read-only" not in text
        assert (store / ".btrfs-backup-ng.locks").is_dir(), "no pin was ever recorded"
        status = _cli("restore", "--status", f"raw://{store}")
        assert "No active locks" in status.stdout or "restore:" not in status.stdout, (
            status.stdout
        )
