"""Answer the remote-lock protocol for tests that fake an ssh transport.

``sshutil.lock`` works by running POSIX shell on the target: ``mkdir`` to take a
lock, ``stat`` to judge a heartbeat, ``mv`` to break a dead one. A test that
patches ``subprocess.run`` to return a canned object gives those scripts no
answer, and the manager -- correctly -- refuses to believe it holds a lock it
could not confirm. Every such test then looks like a locking bug.

The fix is not to fake the protocol's output, which would pass whatever the
protocol did. It is to run the real scripts, locally, against a sandbox
directory standing in for the remote target.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from btrfs_backup_ng.sshutil.lock import LOCK_DIR_NAME

#: Captured at import, before any test patches ``subprocess.run``. These tests
#: patch it globally, so calling the module attribute here would re-enter the
#: test's own fake -- which recurses until the interpreter stops it, and reports
#: as a lock failure rather than as the harness bug it is.
_real_run = subprocess.run


class _Result:
    def __init__(self, returncode: int, stdout: bytes, stderr: bytes) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def is_lock_script(cmd) -> bool:
    """Whether this remote command is part of the lock protocol."""
    return LOCK_DIR_NAME in " ".join(str(c) for c in cmd)


def run_lock_script(cmd, sandbox: Path, remote_root: str = "/backup") -> _Result:
    """Execute the lock script in ``cmd`` against ``sandbox``.

    The script is the last element of an ssh argv. Remote absolute paths are
    remapped onto the sandbox so the real scripts can run unmodified.
    """
    script = str(cmd[-1]).replace(remote_root, str(sandbox))
    proc = _real_run(["sh", "-c", script], capture_output=True)
    return _Result(proc.returncode, proc.stdout, proc.stderr)


POSIX_SHELLS = ("sh", "dash", "bash")


def available_shells() -> list:
    """Which POSIX shells this machine can actually test against."""
    import shutil as _shutil

    return [name for name in POSIX_SHELLS if _shutil.which(name)]


def lock_aware(fake_run, sandbox: Path, remote_root: str = "/backup"):
    """Wrap a test's fake ``subprocess.run`` so lock scripts are really run."""

    def dispatch(cmd, *args, **kwargs):
        if is_lock_script(cmd):
            return run_lock_script(cmd, sandbox, remote_root)
        return fake_run(cmd, *args, **kwargs)

    return dispatch
