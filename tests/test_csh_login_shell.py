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

import ast
import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

import btrfs_backup_ng
from btrfs_backup_ng.sshutil.lock import (
    RemoteLockBusy,
    RemoteLockManager,
    csh_unsafe,
    read_only_probe_script,
)

SRC = Path(btrfs_backup_ng.__file__).parent

#: String literals that contain a csh-unsafe ``!`` and are never sent to a
#: shell: console markup and messages. (file relative to the package, text).
NOT_SHELL = {
    ("cli/wizard_utils.py", "  [yellow]![/yellow] "),
    ("cli/config_cmd.py", "[green]Systemd migration complete![/green]"),
    ("cli/config_cmd.py", "  [yellow]![/yellow] "),
    (
        "cli/dispatcher.py",
        "TIP: btrfs-backup-ng now supports TOML configuration files!",
    ),
    ("core/operations.py", " complete!"),
    ("sshutil/lock.py", "!"),  # the csh check itself
}

#: Files whose strings starting with these are HTML/markup, never a command.
NOT_SHELL_PREFIXES = {("notifications.py", "\n        <!DOCTYPE html>")}


def _unsafe_literals() -> list[tuple[str, int, str]]:
    found = []
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docstrings = {
            id(body[0].value)
            for body in (
                getattr(n, "body", None)
                for n in ast.walk(tree)
                if isinstance(
                    n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
                )
            )
            if body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                text = node.value
                if id(node) in docstrings or (rel, text) in NOT_SHELL:
                    continue
                if any(rel == f and text.startswith(p) for f, p in NOT_SHELL_PREFIXES):
                    continue
                if any(
                    c == "!" and text[i + 1 : i + 2] not in (" ", "\t", "\n", "=", "(")
                    for i, c in enumerate(text)
                ):
                    found.append((rel, node.lineno, text[:60]))
    return found


def test_no_string_in_the_program_carries_a_history_expanding_bang():
    """Checked over every string literal, not only the scripts known today:
    the next remote command is written by someone who has never heard of csh."""
    assert _unsafe_literals() == []


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
        if os.environ.get("BBNG_REQUIRE_LOGIN_SHELLS"):
            pytest.fail(
                f"{login} is not installed, and BBNG_REQUIRE_LOGIN_SHELLS says this "
                f"run must prove the protocol under it rather than skip"
            )
        pytest.skip(f"{login} is not installed here")
    run, _seen = _recording(tmp_path, [login, "-f", "-c"])
    assert _exercise(run, tmp_path) == EXPECTED
