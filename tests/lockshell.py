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

#: Captured at import, before any test patches these. Tests patch them globally,
#: so calling the module attribute here would re-enter the test's own fake --
#: which recurses until the interpreter stops it, and reports as a lock failure
#: rather than as the harness bug it is.
_real_popen = subprocess.Popen


class _Result:
    def __init__(self, returncode: int, stdout: bytes, stderr: bytes) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def is_lock_script(cmd) -> bool:
    """Whether this remote command is part of the lock protocol."""
    return LOCK_DIR_NAME in " ".join(str(c) for c in cmd)


def _exec(script: str, stdin_bytes: bytes | None = None) -> _Result:
    """Run one remapped script, reaching the OS past any patch a test installed.

    NOT via ``subprocess.run``: run() calls ``subprocess.Popen`` by module
    global, so a test that patches Popen has its fake re-enter here and remap an
    already-remapped path a second time. That produced a sandbox path nested
    inside itself and reported as "mv: cannot stat" -- a harness fault wearing
    the costume of the product bug under test.
    """
    proc = _real_popen(
        ["sh", "-c", script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = proc.communicate(stdin_bytes)
    return _Result(proc.returncode, out, err)


def run_lock_script(cmd, sandbox: Path, remote_root: str = "/backup") -> _Result:
    """Execute the lock script in ``cmd`` against ``sandbox``.

    The script is the last element of an ssh argv. Remote absolute paths are
    remapped onto the sandbox so the real scripts can run unmodified.
    """
    return _exec(str(cmd[-1]).replace(remote_root, str(sandbox)))


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


def local_remote(sandbox: Path, remote_root: str = "/backup"):
    """Stand in for the WHOLE far end, not just its lock protocol.

    ``lock_aware`` runs the lock scripts for real and leaves everything else to
    the test's canned answers, which is right when the test is about the lock.
    It is not enough to prove a commit: asserting that the endpoint emitted
    ``sync && mv -f ...`` proves the string, not that a stream ends up published
    under its final name with a sidecar beside it.

    So every remote script is run, locally, with remote absolute paths remapped
    onto ``sandbox``. The commands are the endpoint's own -- no re-implementation
    of what they were supposed to do -- and the assertions are made against the
    resulting directory, which is the only thing a backup target is judged by.
    """

    def run(cmd, *args, **kwargs):
        script = str(cmd[-1]).replace(remote_root, str(sandbox))
        return _exec(script, kwargs.get("input"))

    return run


def local_remote_popen(sandbox: Path, remote_root: str = "/backup"):
    """``local_remote`` for the receive pipeline, which is opened not run."""

    def popen(cmd, *args, **kwargs):
        raw = cmd if isinstance(cmd, str) else str(cmd[-1])
        script = raw.replace(remote_root, str(sandbox))
        passthrough = {
            k: v for k, v in kwargs.items() if k in ("stdin", "stdout", "stderr")
        }
        return _real_popen(["sh", "-c", script], **passthrough)

    return popen
