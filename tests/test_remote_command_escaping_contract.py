"""One layer of escaping, owned by the remote-exec producer.

Every remote command in this codebase is handed to an ``_exec_remote_command``
as a plain argv list. That producer escapes each element exactly once, because
ssh space-joins the remote argv and the remote login shell re-splits it::

    SSHEndpoint._exec_remote_command     endpoint/ssh.py
    SSHRawEndpoint._exec_remote_command  endpoint/raw.py

Both build ``" ".join(shlex.quote(str(c)) for c in command)``.

The contract therefore has two halves, and confusing them is what broke:

* an argv ELEMENT is passed raw -- the producer quotes it;
* a word interpolated INSIDE a script string is quoted by the caller, because
  the producer only sees the finished script as one element.

``_snapper_run_shell`` quoted the script itself as well as being quoted by the
producer. Double-escaped, the whole script collapsed to one literal word and the
remote shell tried to run it as a command name -- ``rc 127`` on every privileged
snapper call over ssh, which broke publish, enumeration and cleanup. Reproduced
over real ssh::

    sh -c ''"'"'echo "1 UUID-A"; echo "2 UUID-B"'"'"''
    sh: line 1: echo "1 UUID-A"; echo "2 UUID-B": command not found   # rc=127

These tests execute the produced command through a real shell rather than
asserting on its text, since only execution distinguishes correct escaping from
double escaping.
"""

from __future__ import annotations

import shlex
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.core import operations

# Scripts that a shell must receive intact. Each is a multi-word command line
# with the metacharacters the real snapper scripts use.
SCRIPTS = [
    pytest.param("echo hello", id="single-word-args"),
    pytest.param('echo "1 UUID-A"; echo "2 UUID-B"', id="semicolon-and-quotes"),
    pytest.param("cd /tmp 2>/dev/null || exit 0; echo ok", id="redirect-and-or"),
    pytest.param("for n in a b; do echo $n; done", id="loop-with-expansion"),
    pytest.param("set -e; [ -e /tmp ] && echo present", id="test-and-and"),
    pytest.param("echo 'single'; echo \"double\"", id="both-quote-kinds"),
    pytest.param("printf '%s\\n' 'a b'", id="printf-embedded-space"),
    pytest.param("echo back`echo tick`", id="backtick"),
]


def remote_producer(argv) -> str:
    """Exactly what both _exec_remote_command implementations build."""
    return " ".join(shlex.quote(str(c)) for c in argv)


def run_through_remote_shell(argv):
    """Execute a produced command the way the remote login shell would.

    ssh space-joins its command arguments and hands the result to the remote
    shell, so running the joined string under ``sh -c`` is faithful.
    """
    joined = remote_producer(argv)
    return subprocess.run(
        ["/bin/sh", "-c", joined], capture_output=True, text=True, timeout=30
    )


class TestProducerOwnsEscaping:
    """A raw argv element survives; a pre-quoted one does not."""

    @pytest.mark.parametrize("script", SCRIPTS)
    def test_raw_script_element_executes(self, script):
        proc = run_through_remote_shell(["sh", "-c", script])
        assert proc.returncode == 0, (
            f"raw script failed: rc={proc.returncode} stderr={proc.stderr.strip()!r} "
            f"produced={remote_producer(['sh', '-c', script])!r}"
        )

    @pytest.mark.parametrize("script", SCRIPTS)
    def test_pre_quoted_script_element_is_the_bug(self, script):
        """Double escaping must be observable, not silently different.

        Pins the failure mode: the shell treats the whole script as a command
        name. If this ever starts passing, the producer stopped escaping and the
        raw-element contract above is no longer being enforced by anything.
        """
        proc = run_through_remote_shell(["sh", "-c", shlex.quote(script)])
        assert proc.returncode != 0, (
            "a double-escaped script executed successfully, so the producer is no "
            "longer escaping its elements and the contract is unenforced"
        )
        # The shell reports the entire script as the command name it could not
        # run -- "command not found", or "No such file or directory" when the
        # script happens to begin with a path-like token. Either way the whole
        # script text is echoed back, which is the signature of double escaping.
        assert script in proc.stderr, proc.stderr


class TestWordsInsideAScriptAreCallerQuoted:
    """The other half: a path inside script TEXT is the caller's to quote."""

    @pytest.mark.parametrize(
        "path",
        ["/mnt/my backup", "/mnt/bob's", "/mnt/a b'c$(x)", "/mnt/back;up"],
    )
    def test_interpolated_path_must_be_quoted_by_the_caller(self, path, tmp_path):
        target = tmp_path / "probe"
        target.write_text("ok\n")
        # A caller building script text quotes the word it interpolates.
        script = f"cat {shlex.quote(str(target))}"
        proc = run_through_remote_shell(["sh", "-c", script])
        assert proc.returncode == 0 and proc.stdout == "ok\n", proc.stderr

    def test_unquoted_interpolation_breaks(self, tmp_path):
        spaced = tmp_path / "two words"
        spaced.write_text("ok\n")
        script = f"cat {spaced}"  # deliberately unquoted
        proc = run_through_remote_shell(["sh", "-c", script])
        assert proc.returncode != 0, "an unquoted spaced path should not resolve"


class TestSnapperRunShellHonoursTheContract:
    """_snapper_run_shell is the caller that broke it; pin its argv."""

    @staticmethod
    def _capture_argv(script: str):
        captured = {}

        def fake_exec(command, **kwargs):
            captured.setdefault("commands", []).append([str(c) for c in command])
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

        endpoint = SimpleNamespace(
            _is_remote=True,
            _exec_remote_command=MagicMock(side_effect=fake_exec),
            _prime_remote_sudo=MagicMock(),
            config={"path": "/mnt/backup"},
        )
        operations._snapper_run_shell(endpoint, script)
        return captured["commands"]

    @pytest.mark.parametrize("script", SCRIPTS)
    def test_script_is_passed_raw(self, script):
        commands = self._capture_argv(script)
        sh_calls = [c for c in commands if "sh" in c and "-c" in c]
        assert sh_calls, commands
        argv = sh_calls[0]
        passed = argv[argv.index("-c") + 1]
        assert passed == script, (
            f"script was pre-escaped before the producer: {passed!r} != {script!r}"
        )
        assert passed != shlex.quote(script)

    @pytest.mark.parametrize("script", SCRIPTS)
    def test_captured_argv_executes_through_a_real_shell(self, script):
        """End to end: what _snapper_run_shell sends must actually run.

        The `sudo -n` prefix is dropped for execution -- privilege is not what is
        under test here, escaping is -- but the argv is otherwise the real one.
        """
        commands = self._capture_argv(script)
        argv = next(c for c in commands if "sh" in c and "-c" in c)
        argv = argv[argv.index("sh") :]  # strip the sudo prefix
        proc = run_through_remote_shell(argv)
        assert proc.returncode == 0, (
            f"rc={proc.returncode} stderr={proc.stderr.strip()!r} "
            f"produced={remote_producer(argv)!r}"
        )


def test_local_branch_passes_the_script_raw_too(monkeypatch):
    """The local path has no shell in between, so it also takes the raw script."""
    seen = {}

    def fake_run(argv, **kwargs):
        seen["argv"] = [str(a) for a in argv]
        return SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(operations.subprocess, "run", fake_run)
    monkeypatch.setattr(operations.os, "geteuid", lambda: 0)

    endpoint = SimpleNamespace(_is_remote=False, config={"path": "/mnt/backup"})
    script = 'echo "a b"; echo c'
    operations._snapper_run_shell(endpoint, script)

    assert seen["argv"][-1] == script, seen["argv"]
