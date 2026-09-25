"""Remote commands survive a csh or tcsh login shell.

Every command sent to a target is parsed by the target account's LOGIN shell
before ``sh`` ever sees it: sshd runs ``$SHELL -c '<command>'``. csh and tcsh
begin a history substitution at any ``!`` that is not followed by a blank, a
tab, ``=`` or ``(`` -- inside single quotes too -- and reject a quoted string
that spans a line. Either fails the whole command with "Event not found." or
"Unmatched '." before it runs.

A digits check written as ``*[!0-9]*`` did exactly that: every lock script
failed on such an account, and the snapper enumeration returned nothing, which
read as "no backups".
"""

from __future__ import annotations

import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

from btrfs_backup_ng.sshutil.lock import (
    RemoteLockBusy,
    RemoteLockManager,
    csh_unsafe,
    read_only_probe_script,
)


def test_the_check_itself_sees_the_pattern_that_broke():
    assert csh_unsafe("case $x in ''|*[!0-9]*) x=;; esac") == ["!0"]
    assert csh_unsafe("[ ! -d /x ] && [ a != b ] && ! (true)") == []
    assert csh_unsafe("echo a\necho b") == ["\\n"]


def _recording(sandbox: Path, shell_argv):
    """A runner executing each script as a login shell would hand it on, and
    remembering every script it was given."""
    seen: list[str] = []

    def run(script: str):
        seen.append(script)
        proc = subprocess.run(
            [*shell_argv, f"sh -c {shlex.quote(script)}"],
            capture_output=True,
            text=True,
        )
        return proc.returncode, proc.stdout, proc.stderr

    return run, seen


def _exercise(run, sandbox: Path) -> dict:
    """Every operation of the lock protocol, as a run and a prune use it."""
    first = RemoteLockManager(run, str(sandbox), hostname="h1")
    second = RemoteLockManager(run, str(sandbox), hostname="h2")
    outcome: dict = {}
    outcome["acquire"] = first.acquire_once("target", "prune")
    try:
        second.acquire_once("target", "other")
        outcome["contended"] = "acquired"
    except RemoteLockBusy:
        outcome["contended"] = "busy"
    outcome["is_locked"] = (first.is_locked("target") or {}).get("hostname")
    first.release("target")
    outcome["after_release"] = first.is_locked("target")
    first.acquire_shared("snap-a", "restore:1")
    outcome["pins"] = sorted(first.live_lock_names())
    outcome["swept"] = first.sweep_dead_holders()
    first.release_shared("snap-a", "restore:1")
    outcome["pins_after"] = sorted(first.live_lock_names())
    outcome["read_only"] = first.location_is_read_only()
    return outcome


EXPECTED = {
    "acquire": "acquired",
    "contended": "busy",
    "is_locked": "h1",
    "after_release": None,
    "pins": ["snap-a"],
    "swept": 0,
    "pins_after": [],
    "read_only": False,
}


def test_every_lock_script_is_csh_safe(tmp_path):
    run, seen = _recording(tmp_path, ["sh", "-c"])
    assert _exercise(run, tmp_path) == EXPECTED
    seen.append(read_only_probe_script(str(tmp_path)))
    unsafe = {script[:80]: csh_unsafe(script) for script in seen if csh_unsafe(script)}
    assert unsafe == {}


@pytest.mark.parametrize("login", ["tcsh", "csh"])
def test_the_whole_protocol_works_under_a_csh_login_shell(tmp_path, login):
    """The real shell, where one is installed. The static checks above are
    what enforces this everywhere; this shows they are checking the right
    thing."""
    if shutil.which(login) is None:
        pytest.skip(f"{login} is not installed here")
    run, _seen = _recording(tmp_path, [login, "-f", "-c"])
    assert _exercise(run, tmp_path) == EXPECTED
