"""A claimant that withdraws tries once more; a round is not lost to a lying mkdir.

The round the GitHub runners failed (#158): their ``mkdir`` is the uutils one,
which exits 0 when its own call has failed with "File exists" but a directory
stands at the path by then. Two contenders racing for the path a break has just
freed are both told they made it; both write a record, both count two, both
withdraw, and the round had no winner. The protocol now tries once more after
a withdrawal, and that try usually finds the path free.

The shell protocol is run with a ``mkdir`` that lies as uutils does, only when
the directory appeared between its own check and its call; the other claimant's
record is planted and withdrawn by shims at the two commands that bracket this
claimant's own count: its ``mkdir`` (the other wrote first) and its ``rm`` of
its own record (the other withdrew too). The Python port cannot run a
different ``mkdir``, so the same two moments are patched in.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

import pytest

from btrfs_backup_ng.sshutil.lock import (
    OWNER_PREFIX,
    LocalLockManager,
    RemoteLockBusy,
    RemoteLockManager,
    local_manager,
)

DEAD = time.time() - 10_000

#: The uutils coreutils mkdir, as traced on the runners: a stat of the path,
#: the real mkdir(2), and exit 0 anyway when the stat found nothing and a
#: directory stands at the path afterwards. {real} is the real mkdir; {planted}
#: runs after a real success at the lock path (the store's ``-m 755`` mkdir is
#: left alone).
LYING_MKDIR = """#!/bin/sh
for bbng_last; do :; done
bbng_absent=0; [ -e "$bbng_last" ] || bbng_absent=1
if {real} "$@" 2>/dev/null; then case "$1" in -m) ;; *) {planted};; esac; exit 0; fi
[ "$bbng_absent" = 1 ] && [ -d "$bbng_last" ] && exit 0
exit 1
"""


def _real(command: str) -> str:
    """The path of the real command the shims fall through to: /usr/bin on
    most systems, /bin under busybox."""
    return subprocess.run(
        ["sh", "-c", f"command -v {command}"], capture_output=True, text=True
    ).stdout.strip()


def _shell_manager(sandbox: Path) -> RemoteLockManager:
    def run(script: str):
        proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr

    return RemoteLockManager(run, str(sandbox), hostname="testhost")


def _records(lock_dir: Path) -> list[str]:
    return sorted(p.name for p in lock_dir.glob(f"{OWNER_PREFIX}*"))


def _dead_lock(lock_dir: Path) -> None:
    """A lock whose holder is long gone: the claimant under test breaks it,
    so the round it then wins is a broken one."""
    lock_dir.mkdir(parents=True)
    for name in ("owner.deadbeef", "heartbeat"):
        (lock_dir / name).write_text('{"name": "target", "operation": "dead"}')
    for path in (lock_dir / "owner.deadbeef", lock_dir / "heartbeat", lock_dir):
        os.utime(path, (DEAD, DEAD))


def _shell_shims(
    tmp_path: Path, intruder: Path, *, plant_once: bool, other_withdraws: bool
) -> Path:
    """The other claimant, as shims: its record appears when this claimant's
    mkdir of the lock path returns (once, or on every try), and goes when this
    claimant removes its own record (it counted two as well)."""
    shims = tmp_path / "shims"
    shims.mkdir()
    planted = tmp_path / "planted"
    tries = tmp_path / "tries"
    plant = f'echo other > "{intruder}"'
    if plant_once:
        plant = f'[ -e "{planted}" ] || {{ {plant}; : > "{planted}"; }}'
    (shims / "mkdir").write_text(
        LYING_MKDIR.format(
            real=_real("mkdir"), planted=f'echo try >> "{tries}"; {plant}'
        )
    )
    if other_withdraws:
        (shims / "rm").write_text(
            "#!/bin/sh\n"
            f'case "$*" in *{OWNER_PREFIX}*) {_real("rm")} -f "{intruder}";; esac\n'
            f'exec {_real("rm")} "$@"\n'
        )
    for shim in shims.iterdir():
        shim.chmod(0o755)
    return shims


def _python_other_claimant(
    monkeypatch, intruder_name: str, *, plant_once: bool, other_withdraws: bool
):
    """The other claimant, as patches on the Python port: its record appears
    when this claimant's mkdir of the lock path succeeds, and goes right after
    this claimant has counted it. Returns the list of successful mkdirs."""
    made: list[int] = []
    real_mkdir = LocalLockManager._mkdir

    def mkdir_then_other_writes(root_fd, lock_name, lock_path, **kw):
        ok = real_mkdir(root_fd, lock_name, lock_path, **kw)
        if ok:
            made.append(1)
            if not plant_once or len(made) == 1:
                (Path(lock_path) / intruder_name).write_text("other")
        return ok

    real_is_regular = LocalLockManager._is_regular

    def counted_then_other_withdraws(dir_fd, name):
        regular = real_is_regular(dir_fd, name)
        if name == intruder_name and regular:
            os.unlink(intruder_name, dir_fd=dir_fd)
        return regular

    monkeypatch.setattr(
        LocalLockManager, "_mkdir", staticmethod(mkdir_then_other_writes)
    )
    if other_withdraws:
        monkeypatch.setattr(
            LocalLockManager, "_is_regular", staticmethod(counted_then_other_withdraws)
        )
    return made


class TestAClaimantThatWithdrawsTriesOnceMore:
    def test_shell_breaker_whose_first_try_withdraws_wins_the_second(
        self, tmp_path, monkeypatch
    ):
        sandbox = tmp_path / "target"
        sandbox.mkdir()
        manager = _shell_manager(sandbox)
        lock_dir = Path(manager._lock_dir("target"))
        _dead_lock(lock_dir)
        intruder = lock_dir / f"{OWNER_PREFIX}intruder"
        shims = _shell_shims(tmp_path, intruder, plant_once=True, other_withdraws=True)
        monkeypatch.setenv("PATH", f"{shims}:{os.environ['PATH']}")

        mode = manager.acquire_once("target", "op")

        # The break happened on the first try; the second try reports it.
        assert mode == "stale-broken"
        assert (tmp_path / "tries").read_text().count("try") == 2
        records = _records(lock_dir)
        assert len(records) == 1, records
        assert json.loads((lock_dir / records[0]).read_text())["operation"] == "op"
        assert (lock_dir / "heartbeat").exists()
        assert not intruder.exists()

    def test_shell_claimant_whose_both_tries_withdraw_is_busy(
        self, tmp_path, monkeypatch
    ):
        """Bounded: the other claimant wrote and withdrew on both tries, so
        this one withdrew twice -- BUSY, not a third try, and no directory."""
        sandbox = tmp_path / "target"
        sandbox.mkdir()
        manager = _shell_manager(sandbox)
        lock_dir = Path(manager._lock_dir("target"))
        intruder = lock_dir / f"{OWNER_PREFIX}intruder"
        shims = _shell_shims(tmp_path, intruder, plant_once=False, other_withdraws=True)
        monkeypatch.setenv("PATH", f"{shims}:{os.environ['PATH']}")

        with pytest.raises(RemoteLockBusy):
            manager.acquire_once("target", "op")

        assert (tmp_path / "tries").read_text().count("try") == 2
        assert not lock_dir.exists()

    def test_shell_second_try_does_not_break_the_other_claimants_fresh_lock(
        self, tmp_path, monkeypatch
    ):
        """The other claimant's record stays: it won. This claimant's second
        try finds the directory held and says BUSY, making nothing."""
        sandbox = tmp_path / "target"
        sandbox.mkdir()
        manager = _shell_manager(sandbox)
        lock_dir = Path(manager._lock_dir("target"))
        intruder = lock_dir / f"{OWNER_PREFIX}intruder"
        shims = _shell_shims(
            tmp_path, intruder, plant_once=False, other_withdraws=False
        )
        monkeypatch.setenv("PATH", f"{shims}:{os.environ['PATH']}")

        with pytest.raises(RemoteLockBusy):
            manager.acquire_once("target", "op")

        assert (tmp_path / "tries").read_text().count("try") == 1
        assert _records(lock_dir) == [intruder.name]

    def test_python_breaker_whose_first_try_withdraws_wins_the_second(
        self, tmp_path, monkeypatch
    ):
        sandbox = tmp_path / "target"
        sandbox.mkdir()
        manager = local_manager(str(sandbox))
        assert isinstance(manager, LocalLockManager)
        lock_dir = Path(manager._lock_dir("target"))
        _dead_lock(lock_dir)
        intruder_name = f"{OWNER_PREFIX}intruder"
        made = _python_other_claimant(
            monkeypatch, intruder_name, plant_once=True, other_withdraws=True
        )

        mode = manager.acquire_once("target", "op")

        assert mode == "stale-broken"
        assert len(made) == 2, "the second try made the directory afresh"
        records = _records(lock_dir)
        assert len(records) == 1 and records[0] != intruder_name, records
        assert json.loads((lock_dir / records[0]).read_text())["operation"] == "op"

    def test_python_claimant_whose_both_tries_withdraw_is_busy(
        self, tmp_path, monkeypatch
    ):
        sandbox = tmp_path / "target"
        sandbox.mkdir()
        manager = local_manager(str(sandbox))
        lock_dir = Path(manager._lock_dir("target"))
        intruder_name = f"{OWNER_PREFIX}intruder"
        made = _python_other_claimant(
            monkeypatch, intruder_name, plant_once=False, other_withdraws=True
        )

        with pytest.raises(RemoteLockBusy):
            manager.acquire_once("target", "op")

        assert made == [1, 1], made
        assert not lock_dir.exists()

    def test_python_second_try_does_not_break_the_other_claimants_fresh_lock(
        self, tmp_path, monkeypatch
    ):
        sandbox = tmp_path / "target"
        sandbox.mkdir()
        manager = local_manager(str(sandbox))
        lock_dir = Path(manager._lock_dir("target"))
        intruder_name = f"{OWNER_PREFIX}intruder"
        made = _python_other_claimant(
            monkeypatch, intruder_name, plant_once=False, other_withdraws=False
        )

        with pytest.raises(RemoteLockBusy):
            manager.acquire_once("target", "op")

        # The other's record stayed, so the first try's rmdir left the
        # directory standing; the second try found it held and said BUSY
        # without making anything.
        assert made == [1], made
        assert _records(lock_dir) == [intruder_name]
