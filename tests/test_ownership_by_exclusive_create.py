"""Who owns a lock directory is decided by the exclusive create of its heartbeat,
not by mkdir's exit status.

The uutils coreutils ``mkdir``, Ubuntu's default from 25.10 and the one on
GitHub's Ubuntu runners, exits 0 when its own call has failed with "File
exists" but a directory stands at the path by then (#158). Two contenders
racing for the path a break has just freed are both told they made it. The
claimant now creates the ``heartbeat`` first, under ``set -C`` in the shell
and with ``O_EXCL`` in the Python port, which succeeds for exactly one of
them; the other writes nothing, removes nothing, and is BUSY.

The interleaving is forced: the slower claimant's ``mkdir`` is a shim that,
finding the path absent, first runs the faster claimant's whole acquisition,
then makes the real ``mkdir`` call, which fails because the faster made the
directory, and exits 0 anyway, as uutils does. The Python port cannot run a
different ``mkdir``, so the same moment is patched in.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from btrfs_backup_ng.sshutil.lock import (
    OWNER_PREFIX,
    LocalLockManager,
    RemoteLockBusy,
    RemoteLockManager,
    local_manager,
)


def _real(command: str) -> str:
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


class TestTheHeartbeatDecidesOwnership:
    def test_shell_claimant_told_it_made_another_claimants_directory_is_busy(
        self, tmp_path, monkeypatch
    ):
        sandbox = tmp_path / "target"
        sandbox.mkdir()
        manager = _shell_manager(sandbox)
        lock_dir = Path(manager._lock_dir("target"))
        faster = tmp_path / "faster.sh"
        faster.write_text(
            manager._acquire_script(
                "target", '{"name": "target", "operation": "faster"}', "faster"
            )
        )
        faster_out = tmp_path / "faster.out"
        lock_mkdirs = tmp_path / "lock-mkdirs"
        removals = tmp_path / "removals"
        shims = tmp_path / "shims"
        shims.mkdir()
        # uutils, as traced on the runners: a stat, the real mkdir(2), and exit
        # 0 anyway when the stat found nothing and a directory stands there
        # afterwards. Here the directory appears because the faster claimant's
        # whole acquisition runs in that gap. BBNG_FASTER keeps the faster's
        # own mkdir truthful and the shim from recursing.
        (shims / "mkdir").write_text(
            "#!/bin/sh\n"
            "for bbng_last; do :; done\n"
            f'[ -n "$BBNG_FASTER" ] || case "$bbng_last" in *.lock) echo try >> "{lock_mkdirs}";; esac\n'
            'bbng_absent=0; [ -e "$bbng_last" ] || bbng_absent=1\n'
            'if [ -z "$BBNG_FASTER" ] && [ "$bbng_absent" = 1 ]; then '
            f'case "$bbng_last" in *.lock) BBNG_FASTER=1 sh "{faster}" > "{faster_out}" 2>&1;; esac; fi\n'
            f'if {_real("mkdir")} "$@" 2>/dev/null; then exit 0; fi\n'
            '[ "$bbng_absent" = 1 ] && [ -d "$bbng_last" ] && exit 0\n'
            "exit 1\n"
        )
        (shims / "rm").write_text(
            f'#!/bin/sh\necho "$*" >> "{removals}"\nexec {_real("rm")} "$@"\n'
        )
        for shim in shims.iterdir():
            shim.chmod(0o755)
        monkeypatch.setenv("PATH", f"{shims}:{os.environ['PATH']}")

        with pytest.raises(RemoteLockBusy):
            manager.acquire_once("target", "slower")

        assert faster_out.read_text().split()[0] == "ACQUIRED"
        # The slower was told it made the directory once, wrote nothing into it
        # and removed nothing from it; the faster holds it with its heartbeat.
        assert lock_mkdirs.read_text().count("try") == 1
        assert not removals.exists() or OWNER_PREFIX not in removals.read_text()
        records = _records(lock_dir)
        assert len(records) == 1, records
        assert json.loads((lock_dir / records[0]).read_text())["operation"] == "faster"
        assert (lock_dir / "heartbeat").exists()

    def test_python_claimant_told_it_made_another_claimants_directory_is_busy(
        self, tmp_path, monkeypatch
    ):
        sandbox = tmp_path / "target"
        sandbox.mkdir()
        slower = local_manager(str(sandbox))
        faster = local_manager(str(sandbox))
        assert isinstance(slower, LocalLockManager)
        lock_dir = Path(slower._lock_dir("target"))
        real_mkdir = LocalLockManager._mkdir
        lied: list[str] = []
        counted: list[str] = []
        real_is_regular = LocalLockManager._is_regular

        def mkdir_lying_once(root_fd, lock_name, lock_path, **kw):
            if lock_name.endswith(".lock") and not lied and not lock_dir.exists():
                lied.append(lock_name)
                # the faster claimant's whole acquisition lands in the gap
                monkeypatch.setattr(
                    LocalLockManager, "_mkdir", staticmethod(real_mkdir)
                )
                assert faster.acquire_once("target", "faster") == "acquired"
                monkeypatch.setattr(
                    LocalLockManager, "_mkdir", staticmethod(mkdir_lying_once)
                )
                assert real_mkdir(root_fd, lock_name, lock_path, **kw) is False
                return True  # as uutils reports it
            return real_mkdir(root_fd, lock_name, lock_path, **kw)

        def counting_is_regular(dir_fd, name):
            counted.append(name)
            return real_is_regular(dir_fd, name)

        monkeypatch.setattr(LocalLockManager, "_mkdir", staticmethod(mkdir_lying_once))
        monkeypatch.setattr(
            LocalLockManager, "_is_regular", staticmethod(counting_is_regular)
        )

        with pytest.raises(RemoteLockBusy):
            slower.acquire_once("target", "slower")

        assert lied == [lock_dir.name]
        records = _records(lock_dir)
        assert len(records) == 1, records
        assert json.loads((lock_dir / records[0]).read_text())["operation"] == "faster"
        assert (lock_dir / "heartbeat").exists()
        # The slower never got as far as a record: the only record anyone
        # counted is the faster's own.
        assert {n for n in counted if n.startswith(OWNER_PREFIX)} == set(records)
