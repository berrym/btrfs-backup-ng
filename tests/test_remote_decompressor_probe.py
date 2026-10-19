"""A compressed ssh:// transfer asks the remote for its decompressor before
sending anything, and the receive's status stays the verdict.

With `compress` set, the remote runs `<decompressor> | btrfs receive`. A
remote without the program still fails the transfer, because the receive is
handed a stream cut short, but only after the whole stream has crossed the
wire, with the shell's "not found" line inside the receive's error output
(#120). The pre-transfer diagnostics now probe for the program as they probe
for btrfs, and the transfer is refused before a byte is sent. `doctor`
already asked the same question (`compression_programs`).

The pipeline's status is not changed: it is `btrfs receive`'s, and that is
the data verdict. A decompressor that is missing or dies early cuts the
stream short and the receive fails; one that fails after a complete stream
leaves a complete backup. Measured here under sh and dash, in both sudo
forms, by running the string the product emits.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core.transfer import COMPRESSION_PROGRAMS
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint, _build_receive_command

MESSAGE = "has no zstd to decompress the stream"


def _endpoint(compress: str | None):
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"path": "/dest", "ssh_sudo": False}
    if compress is not None:
        ep.config["compress"] = compress
    ep.hostname = "nas"
    ep._diagnostics_cache = {}
    ep._diagnostics_cache_timeout = 300
    return ep


def _remote(missing: set[str]):
    """A remote that answers every probe well except `command -v <missing>`."""
    asked: list[list[str]] = []

    def run(argv, *args, **kwargs):
        asked.append(list(argv))
        rc = 1 if argv[:2] == ["command", "-v"] and argv[2] in missing else 0
        return MagicMock(returncode=rc, stdout=b"btrfs\n", stderr=b"")

    return run, asked


class TestTheProbe:
    def test_asked_only_when_the_transfer_is_compressed(self):
        for compress, expected in (
            (None, []),
            ("none", []),
            ("zstd", [["command", "-v", "zstd"]]),
        ):
            run, asked = _remote(set())
            with patch.object(SSHEndpoint, "_exec_remote_command", side_effect=run):
                results = _endpoint(compress)._run_diagnostics("/dest")
            probes = [
                a for a in asked if a[:2] == ["command", "-v"] and a[2] != "btrfs"
            ]
            assert probes == expected, (compress, probes)
            assert results["decompressor"] is True

    @pytest.mark.parametrize("method", sorted(COMPRESSION_PROGRAMS))
    def test_the_program_asked_for_is_the_methods_decompressor(self, method):
        """`lzo` runs lzop, `pigz` its own program: the table decides, never
        the method's name."""
        run, asked = _remote(set())
        with patch.object(SSHEndpoint, "_exec_remote_command", side_effect=run):
            _endpoint(method)._run_diagnostics("/dest")
        program = COMPRESSION_PROGRAMS[method]["decompress"][0]
        assert ["command", "-v", program] in asked

    def test_a_missing_program_is_an_error_naming_it_and_the_host(self, shared_log):
        run, _asked = _remote({"zstd"})
        with patch.object(SSHEndpoint, "_exec_remote_command", side_effect=run):
            results = _endpoint("zstd")._run_diagnostics("/dest")
        assert results["decompressor"] is False
        errors = shared_log.messages(logging.ERROR)
        assert any(
            "The remote nas has no zstd to decompress the stream. Install zstd on "
            "nas, or set compress to a method it has, or to none." == m
            for m in errors
        ), errors

    def test_a_probe_that_could_not_run_is_not_a_pass(self):
        def run(argv, *args, **kwargs):
            if argv[:2] == ["command", "-v"] and argv[2] == "zstd":
                raise OSError("ssh died")
            return MagicMock(returncode=0, stdout=b"", stderr=b"")

        with patch.object(SSHEndpoint, "_exec_remote_command", side_effect=run):
            results = _endpoint("zstd")._run_diagnostics("/dest")
        assert results["decompressor"] is False

    def test_the_answer_is_cached_with_the_other_probes(self):
        run, asked = _remote({"zstd"})
        ep = _endpoint("zstd")
        with patch.object(SSHEndpoint, "_exec_remote_command", side_effect=run):
            first = ep._run_diagnostics("/dest")
            count = len(asked)
            second = ep._run_diagnostics("/dest")
        assert first["decompressor"] is False and second["decompressor"] is False
        assert len(asked) == count, "the cache was bypassed"


class TestTheTransferIsRefusedBeforeAnyByte:
    """Both transfer routes gate on the diagnostics: the direct pipe and the
    chunked transfer. A refused decompressor starts no process."""

    def _ready(self, ep, monkeypatch, decompressor: bool):
        ep.artifact_exists = MagicMock(return_value=False)
        ep._run_diagnostics = MagicMock(
            return_value={
                "ssh_connection": True,
                "btrfs_command": True,
                "decompressor": decompressor,
                "passwordless_sudo": True,
                "write_permissions": True,
                "btrfs_filesystem": True,
            }
        )
        ep._find_buffer_program = MagicMock(return_value=(None, None))
        ep.ssh_manager = MagicMock(_cached_ssh_password=None, control_path="/tmp/cp")
        ep._cached_sudo_password = None
        started: list = []
        monkeypatch.setattr(
            "btrfs_backup_ng.endpoint.ssh.subprocess.Popen",
            lambda *a, **k: started.append(a) or MagicMock(),
        )
        return started

    def test_the_direct_pipe_refuses(self, tmp_path, monkeypatch):
        ep = _endpoint("zstd")
        monkeypatch.setattr(SSHEndpoint, "_sudo_password_cache", {})
        started = self._ready(ep, monkeypatch, decompressor=False)
        assert (
            ep._try_direct_transfer(
                source_path=str(tmp_path), dest_path="/dest", snapshot_name="s"
            )
            is False
        )
        assert started == [], (
            "a process was started although the remote cannot decompress"
        )

    def test_the_direct_pipe_proceeds_when_the_program_is_there(
        self, tmp_path, monkeypatch
    ):
        """The gate must not refuse what it has no reason to refuse."""
        ep = _endpoint("zstd")
        monkeypatch.setattr(SSHEndpoint, "_sudo_password_cache", {})
        started = self._ready(ep, monkeypatch, decompressor=True)
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        # The stand-in Popen returns a MagicMock the transfer cannot drive, so
        # it reports failure; what matters is that it was started.
        ep._try_direct_transfer(
            source_path=str(tmp_path), dest_path="/dest", snapshot_name="s"
        )
        assert started, "the transfer never started"

    def test_an_older_diagnostics_dict_without_the_key_is_not_a_refusal(
        self, tmp_path, monkeypatch
    ):
        ep = _endpoint(None)
        monkeypatch.setattr(SSHEndpoint, "_sudo_password_cache", {})
        started = self._ready(ep, monkeypatch, decompressor=True)
        del ep._run_diagnostics.return_value["decompressor"]
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        ep._try_direct_transfer(
            source_path=str(tmp_path), dest_path="/dest", snapshot_name="s"
        )
        assert started


# --------------------------------------------------------------------------- #
# The pipeline's verdict, measured by running the emitted string
# --------------------------------------------------------------------------- #
def _tools(tmp_path: Path, *, with_decompressor: bool) -> dict:
    """A PATH holding the tools the emitted string runs, stand-ins for btrfs and
    sudo, and zstd only when asked for."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for tool in ("sh", "dd", "cat", "wc", "head"):
        found = shutil.which(tool)
        assert found, tool
        os.symlink(found, bin_dir / tool)
    if with_decompressor:
        os.symlink(shutil.which("zstd"), bin_dir / "zstd")
    (bin_dir / "sudo").write_text(
        "#!/bin/sh\n"
        'while [ $# -gt 0 ]; do case "$1" in -n) shift;; -S) shift; IFS= read -r _pw;; '
        '*) break;; esac; done\nexec "$@"\n'
    )
    (bin_dir / "btrfs").write_text(
        "#!/bin/sh\n"
        'case "$1" in --version) exit 0;; esac\n'
        "n=$(wc -c)\n"
        'if [ "$n" -lt 100 ]; then echo "ERROR: unexpected EOF in stream" >&2; exit 1; fi\n'
        'echo "received $n bytes" >&2\n'
    )
    for name in ("sudo", "btrfs"):
        (bin_dir / name).chmod(0o755)
    return {**os.environ, "PATH": str(bin_dir)}


def _compressed(size: int = 4096) -> bytes:
    return subprocess.run(
        ["zstd", "-q"], input=b"\0" * size, capture_output=True, check=True
    ).stdout


#: The shells the string is run under: /bin/sh, and dash where installed, the
#: shell of Debian-family remotes. Resolved from the real PATH, since the
#: environment handed to the command holds only the tools it may use.
SHELLS = [
    "sh",
    pytest.param("dash", marks=pytest.mark.requires_tool("dash")),
]


def _shell(name: str) -> str:
    found = shutil.which(name)
    assert found, name
    return found


@pytest.mark.requires_tool("zstd")
class TestTheReceivesStatusIsTheVerdict:
    @pytest.mark.parametrize("shell", SHELLS)
    @pytest.mark.parametrize("password", [False, True], ids=["sudo-n", "sudo-S"])
    def test_a_missing_decompressor_fails_the_receive_and_is_named(
        self, tmp_path, shell, password
    ):
        env = _tools(tmp_path, with_decompressor=False)
        cmd = _build_receive_command(
            "/backups", use_sudo=True, password_on_stdin=password, decompress="zstd"
        )
        stream = (b"0003pw\n" if password else b"") + _compressed()
        result = subprocess.run(
            [_shell(shell), "-c", cmd], input=stream, capture_output=True, env=env
        )
        assert result.returncode != 0
        assert b"zstd" in result.stderr, result.stderr
        assert b"unexpected EOF" in result.stderr, result.stderr

    @pytest.mark.parametrize("shell", SHELLS)
    @pytest.mark.parametrize("password", [False, True], ids=["sudo-n", "sudo-S"])
    def test_a_present_decompressor_delivers_the_stream(
        self, tmp_path, shell, password
    ):
        env = _tools(tmp_path, with_decompressor=True)
        cmd = _build_receive_command(
            "/backups", use_sudo=True, password_on_stdin=password, decompress="zstd"
        )
        stream = (b"0003pw\n" if password else b"") + _compressed()
        result = subprocess.run(
            [_shell(shell), "-c", cmd], input=stream, capture_output=True, env=env
        )
        assert result.returncode == 0, result.stderr
        assert b"received 4096 bytes" in result.stderr
