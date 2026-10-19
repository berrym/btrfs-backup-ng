"""Tier 2: the source's pins across parallel targets, through the real CLI.

One volume, three local targets sent to in parallel (``parallel_targets =
3``), on real btrfs. Every transfer pins the snapshot it sends, and its
incremental parent, on the source, in the source's lock file. A run
interrupted while all three are sending keeps every one of those pins: a
backup keeps its pin when its transfer fails, so the next run can still send
the snapshot. Before this was fixed, each target's thread wrote its own copy
of a snapshot's pins to the file whole, and the file kept only the pins of
whichever thread wrote last -- measured on real hardware after exactly this
interrupt, with "Keeping ... locked" logged by all three. The next normal run
then delivers everything, and must leave no pin behind and prune the source
by its policy.

The interrupt is a terminal's Ctrl-C: SIGINT to the whole process group, so
the transfers' own processes die with it (the program's signal dispositions
are reset to the defaults before it starts). Transfers are rate-limited with
``pv`` so all three are still sending when it arrives.

Needs root, btrfs (loopback filesystems) and ``pv``; skips otherwise.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__

from .conftest import requires_btrfs

pytestmark = [
    pytest.mark.tier2,
    requires_btrfs,
    pytest.mark.skipif(shutil.which("pv") is None, reason="needs pv to rate-limit"),
]

PREFIX = "vol-"
LOCK_FILE = ".btrfs-backup-ng.locks"


def _defaults() -> None:
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, signal.SIG_DFL)


def _env(tmp_path: Path) -> dict:
    runtime = tmp_path / "runtime"
    runtime.mkdir(exist_ok=True, mode=0o700)
    env = {**os.environ, "COLUMNS": "200", "XDG_RUNTIME_DIR": str(runtime)}
    env.pop("BTRFS_BACKUP_LOG_LEVEL", None)
    return env


def _run(tmp_path: Path, config: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "btrfs_backup_ng", "-c", str(config), "run"],
        capture_output=True,
        text=True,
        env=_env(tmp_path),
        timeout=300,
    )


def _config(path: Path, data: Path, targets: list[Path], rate: str | None) -> Path:
    text = (
        '[global]\nsnapshot_dir = ".snapshots"\nparallel_targets = 3\n'
        '\n[global.retention]\nmin = "0s"\nkeep = 2\n'
        f'\n[[volumes]]\npath = "{data}"\nsnapshot_prefix = "{PREFIX}"\n'
    )
    for target in targets:
        text += f'\n[[volumes.targets]]\npath = "{target}"\n'
        if rate:
            text += f'rate_limit = "{rate}"\n'
    path.write_text(text)
    return path


def _snapshots(where: Path) -> list[str]:
    return sorted(p.name for p in where.iterdir() if p.name.startswith(PREFIX))


def _locks(snapshot_dir: Path) -> dict:
    raw = __util__.read_locks((snapshot_dir / LOCK_FILE).read_text())
    return {n: {k: set(v) for k, v in e.items()} for n, e in raw.items()}


def test_an_interrupt_keeps_every_targets_pins_and_the_next_run_clears_them(
    btrfs_source_and_dest, tmp_path
):
    source, dest = btrfs_source_and_dest
    data = source / "vol"
    subprocess.run(
        ["btrfs", "subvolume", "create", str(data)], check=True, capture_output=True
    )
    (data / "seed.bin").write_bytes(os.urandom(256 * 1024))
    targets = []
    for label in ("d1", "d2", "d3"):
        (dest / label).mkdir()
        targets.append((dest / label).resolve())
    ids = {str(t) for t in targets}
    fast = _config(tmp_path / "fast.toml", data, targets, None)
    slow = _config(tmp_path / "slow.toml", data, targets, "1M")
    snapshot_dir = data / ".snapshots"

    # A first run gives every target a base, so the next sends are
    # incremental and pin a parent too.
    seeded = _run(tmp_path, fast)
    assert seeded.returncode == 0, seeded.stdout + seeded.stderr
    (s0,) = _snapshots(snapshot_dir)

    time.sleep(1.1)  # the next snapshot's name is a later second
    (data / "payload.bin").write_bytes(os.urandom(24 * 1024 * 1024))
    log = tmp_path / "interrupted.log"
    with open(log, "w") as out:
        proc = subprocess.Popen(
            [sys.executable, "-m", "btrfs_backup_ng", "-c", str(slow), "run"],
            stdout=out,
            stderr=subprocess.STDOUT,
            text=True,
            env=_env(tmp_path),
            preexec_fn=_defaults,
            start_new_session=True,
        )
        try:
            # All three are sending once each target holds the snapshot its
            # receive is writing.
            deadline = time.monotonic() + 60
            while not all(len(_snapshots(t)) == 2 for t in targets):
                assert proc.poll() is None, log.read_text()
                assert time.monotonic() < deadline, log.read_text()
                time.sleep(0.1)
            time.sleep(1.0)
            assert proc.poll() is None, "the transfers finished before the interrupt"
            os.killpg(proc.pid, signal.SIGINT)
            proc.wait(timeout=120)
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
    text = log.read_text()
    assert proc.returncode == 130, text
    (s1,) = [n for n in _snapshots(snapshot_dir) if n != s0]
    assert text.count("Keeping ") >= 3, text

    # Every target's pins, not the last writer's.
    assert _locks(snapshot_dir) == {
        s1: {"locks": ids},
        s0: {"parent_locks": ids},
    }, text

    time.sleep(1.1)
    finished = _run(tmp_path, fast)
    out = finished.stdout + finished.stderr
    assert finished.returncode == 0, out
    assert _locks(snapshot_dir) == {}, out
    # keep = 2: the source and every target hold the newest two.
    kept = _snapshots(snapshot_dir)
    assert len(kept) == 2 and s0 not in kept and s1 in kept, (kept, out)
    for target in targets:
        assert _snapshots(target) == kept, (target, out)
