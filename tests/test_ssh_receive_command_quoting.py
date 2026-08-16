"""The remote receive command must survive real shell parsing intact.

``_build_receive_command`` returns a string that ssh hands to the REMOTE login
shell verbatim. Asserting on substrings of that string cannot tell a correct
command from a broken one, so these tests execute it through a real ``/bin/sh``
with a stub ``btrfs`` on PATH and assert on the argv ``btrfs receive`` actually
received.

The pre-fix producer wrapped the script in literal single quotes
(``f"sh -c '" ... f"'"``) while callers passed an already-``shlex.quote``d
destination, so the outer quotes cancelled the caller's escaping:

* ``/mnt/my backup``   -> btrfs received ``/mnt/my`` (silently wrong subvolume)
* ``/mnt/b;touch ...`` -> injected command ran, and the compound's exit status
                          was the injected command's, masking a failed receive
* ``/mnt/bob's``       -> remote syntax error, receive never ran

Mutation-verified: restoring the literal-quote wrapper, and separately dropping
the producer's internal ``shlex.quote``, each fail the large majority of the
cases below. Re-adding ``shlex.quote`` at any one of the four call sites fails
exactly that site's TestCallerContract case.
"""

from __future__ import annotations

import os
import shlex
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import btrfs_backup_ng.endpoint.ssh as ssh_mod
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint, _build_receive_command

# Space + single-quote + command substitution: mangled by any quoting mistake.
NASTY_DEST = "/mnt/a b'c$(x)"


class _StopAfterBuild(Exception):
    """Raised from the patched producer to halt a call site before it does I/O."""


# Destinations that a correct producer must deliver to btrfs unchanged.
HOSTILE_DESTS = [
    pytest.param("/mnt/backup", id="clean"),
    pytest.param("/mnt/my backup", id="space"),
    pytest.param("/mnt/bob's", id="single-quote"),
    pytest.param('/mnt/say "hi"', id="double-quote"),
    pytest.param("/mnt/a b'c$(x)", id="space-quote-cmdsub"),
    pytest.param("/mnt/back;up", id="semicolon"),
    pytest.param("/mnt/back|up", id="pipe"),
    pytest.param("/mnt/back$IFSup", id="dollar-var"),
    pytest.param("/mnt/back`id`up", id="backtick"),
    pytest.param("/mnt/back\\up", id="backslash"),
    pytest.param("/mnt/back&up", id="ampersand"),
    pytest.param("/mnt/back\nup", id="newline"),
]


@pytest.fixture
def remote_shell(tmp_path):
    """Run a produced command the way the remote login shell would.

    ssh space-joins its command arguments and passes the result to the remote
    shell, so executing the produced string under ``sh -c`` is faithful for the
    single-argument form every call site uses.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    argv_out = tmp_path / "btrfs-argv"
    canary = tmp_path / "INJECTED"

    # Records the argv it was invoked with, so the test can assert on the real
    # post-parse arguments rather than on the command string. NUL-separated so a
    # destination containing a newline round-trips unambiguously.
    btrfs_stub = bin_dir / "btrfs"
    btrfs_stub.write_text(
        '#!/bin/sh\nprintf "%s\\000" "$@" > "$BTRFS_ARGV_OUT"\nexit 0\n'
    )
    btrfs_stub.chmod(0o755)

    # Drops sudo's own flags and execs the rest, so the sudo variants exercise
    # the same argv assertion as the non-sudo one.
    sudo_stub = bin_dir / "sudo"
    sudo_stub.write_text(
        '#!/bin/sh\nwhile [ "$1" = "-n" ] || [ "$1" = "-S" ]; do shift; done\nexec "$@"\n'
    )
    sudo_stub.chmod(0o755)

    def run(remote_cmd: str):
        env = dict(
            os.environ,
            PATH=f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            BTRFS_ARGV_OUT=str(argv_out),
        )
        if argv_out.exists():
            argv_out.unlink()
        proc = subprocess.run(
            ["/bin/sh", "-c", remote_cmd],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        argv = (
            argv_out.read_bytes().decode().split("\0")[:-1]
            if argv_out.exists()
            else None
        )
        return argv, proc

    run.canary = canary  # type: ignore[attr-defined]
    return run


@pytest.mark.parametrize("dest", HOSTILE_DESTS)
@pytest.mark.parametrize(
    ("use_sudo", "password_on_stdin"),
    [(False, False), (True, False), (True, True)],
    ids=["no-sudo", "sudo-n", "sudo-S"],
)
def test_destination_reaches_btrfs_receive_unchanged(
    remote_shell, dest, use_sudo, password_on_stdin
):
    """btrfs receive must be invoked with exactly the destination we asked for."""
    cmd = _build_receive_command(
        dest, use_sudo=use_sudo, password_on_stdin=password_on_stdin
    )
    argv, proc = remote_shell(cmd)

    assert argv is not None, (
        f"btrfs was never executed (rc={proc.returncode}); "
        f"stderr={proc.stderr.strip()!r}; produced={cmd!r}"
    )
    assert argv == ["receive", dest], (
        f"destination was mangled by shell parsing: {argv!r}; produced={cmd!r}"
    )
    assert proc.returncode == 0, f"stderr={proc.stderr.strip()!r}"


def test_metacharacters_in_destination_cannot_inject_a_command(remote_shell):
    """A `;` in the destination must not run a second command.

    This is the security core of the bug: the injected command's exit status
    became the compound's, so a failing `btrfs receive` reported success.

    The argv assertion below already fails for the specific pre-fix bug. The
    canary is kept for the class the argv assertion cannot see: a producer that
    passes the correct destination to btrfs AND still runs a trailing command.
    """
    canary = remote_shell.canary
    dest = f"/mnt/backup;touch {canary}"

    cmd = _build_receive_command(dest)
    argv, proc = remote_shell(cmd)

    assert not canary.exists(), f"command injection executed; produced={cmd!r}"
    assert argv == ["receive", dest]
    assert proc.returncode == 0


def test_double_escaping_is_visible_not_silent(remote_shell):
    """A pre-quoted path reaches btrfs with its quote characters intact.

    This pins the producer half of the contract only: double-escaping must fail
    loudly (wrong, obviously-quoted path) rather than silently target a
    truncated path the way the pre-fix producer did. The caller half -- that no
    call site pre-quotes -- is guarded by TestCallerContract below.
    """
    dest = "/mnt/my backup"
    cmd = _build_receive_command(shlex.quote(dest))
    argv, _ = remote_shell(cmd)

    assert argv == ["receive", shlex.quote(dest)]
    assert argv != ["receive", dest]


class TestCallerContract:
    """Every call site must hand the producer a RAW, unquoted destination.

    The producer escapes exactly once. A caller that re-introduces
    ``shlex.quote`` double-escapes, and btrfs receives the literal quote
    characters -- the mirror image of the bug this change fixed. Before these
    tests only ``receive_chunked`` was guarded (tests/test_r9_quoting.py), so
    re-adding a quote at the other three sites passed the whole suite.
    """

    def _endpoint(self):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": NASTY_DEST, "username": "u", "port": 22, "ssh_sudo": True}
        ep.hostname = "host"
        ep.ssh_manager = MagicMock()
        ep.ssh_manager.control_path = "/tmp/cp"
        ep.ssh_manager.get_ssh_base_cmd.return_value = ["ssh", "host"]
        ep._normalize_path = MagicMock(side_effect=lambda p: p)
        ep._estimate_snapshot_size = MagicMock(return_value=None)
        ep._check_command_exists = MagicMock(return_value=False)
        ep._get_ssh_password = MagicMock(return_value="pw")
        ep._cached_sudo_password = "pw"
        return ep

    @staticmethod
    def _capture(fn):
        """Run fn with the producer patched to record its dest and bail out."""
        captured = {}

        def fake_build(dest_path, **kwargs):
            captured["dest"] = dest_path
            raise _StopAfterBuild

        with patch.object(ssh_mod, "_build_receive_command", side_effect=fake_build):
            with pytest.raises(_StopAfterBuild):
                fn()
        return captured["dest"]

    def test_btrfs_receive_passes_raw_destination(self):
        ep = self._endpoint()
        dest = self._capture(lambda: ep._btrfs_receive(NASTY_DEST, None))
        assert dest == NASTY_DEST
        assert dest != shlex.quote(NASTY_DEST)

    def test_paramiko_transfer_passes_raw_destination(self):
        ep = self._endpoint()
        dest = self._capture(
            lambda: ep._do_paramiko_transfer(
                source_path="/src/snap",
                dest_path=NASTY_DEST,
                snapshot_name="snap",
                parent_path=None,
                sudo_password="pw",
            )
        )
        assert dest == NASTY_DEST
        assert dest != shlex.quote(NASTY_DEST)

    def test_shell_pipeline_transfer_passes_raw_destination(self):
        ep = self._endpoint()
        dest = self._capture(
            lambda: ep._do_shell_pipeline_transfer(
                source_path="/src/snap",
                dest_path=NASTY_DEST,
                snapshot_name="snap",
            )
        )
        assert dest == NASTY_DEST
        assert dest != shlex.quote(NASTY_DEST)

    def test_receive_chunked_passes_raw_destination(self):
        ep = self._endpoint()
        manifest = SimpleNamespace(
            snapshot_name="snap", chunk_count=1, snapshot_path="/src/snap"
        )
        dest = self._capture(
            lambda: ep.receive_chunked(chunk_reader=None, manifest=manifest)
        )
        assert dest == NASTY_DEST
        assert dest != shlex.quote(NASTY_DEST)


def test_signal_handling_semantics_preserved(remote_shell):
    """The PIPE trap and exec must survive the quoting change."""
    cmd = _build_receive_command("/mnt/backup")
    assert 'trap "" PIPE' in cmd
    assert "exec btrfs receive" in cmd
    # And it still actually runs.
    argv, proc = remote_shell(cmd)
    assert argv == ["receive", "/mnt/backup"]
    assert proc.returncode == 0
