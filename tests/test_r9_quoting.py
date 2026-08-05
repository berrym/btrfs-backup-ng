"""R9 defensive shell-quoting sweep: paths/names interpolated into a REMOTE shell
context must be shlex.quote'd so a space / single-quote / shell metacharacter can
neither break the command nor inject into the remote shell.

Mutation-verified: reverting any one quote (the chunked-receive dest, the central
``_exec_remote_command`` element quoting, or a diagnose.py path) makes the matching
test fail. Clean paths are unchanged (no regression).
"""

from __future__ import annotations

import shlex
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import btrfs_backup_ng.endpoint.ssh as ssh_mod
import btrfs_backup_ng.sshutil.diagnose as diag
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

NASTY = "/mnt/a b'c$(x)"  # space + single-quote + command-substitution
CLEAN = "/mnt/backup/home"


class TestReceiveChunkedQuotesDest:
    """HIGH: receive_chunked passes shlex.quote(dest_path) to _build_receive_command
    (parity with the other three callers)."""

    def _capture_dest(self, configured_path):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": configured_path, "ssh_sudo": False}
        ep._normalize_path = MagicMock(side_effect=lambda p: p)  # type: ignore[method-assign]
        manifest = SimpleNamespace(
            snapshot_name="snap", chunk_count=1, snapshot_path="/src/snap"
        )
        captured: dict = {}

        def fake_build(dest_path, **kw):
            captured["dest_path"] = dest_path
            raise RuntimeError("stop-after-build")

        with patch.object(ssh_mod, "_build_receive_command", side_effect=fake_build):
            with pytest.raises(RuntimeError, match="stop-after-build"):
                ep.receive_chunked(chunk_reader=None, manifest=manifest)
        return captured["dest_path"]

    def test_nasty_path_is_quoted(self):
        assert self._capture_dest(NASTY) == shlex.quote(NASTY)

    def test_clean_path_unchanged(self):
        # shlex.quote is a no-op for a clean path -> no regression.
        assert self._capture_dest(CLEAN) == CLEAN


class TestExecRemoteCommandQuotesElements:
    """Systemic MEDIUM: _exec_remote_command quotes each remote_cmd element (ssh
    space-joins them and the REMOTE shell re-parses)."""

    def _capture_ssh_cmd(self, built_remote_cmd):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"ssh_sudo": False}
        ep._normalize_path = MagicMock(side_effect=lambda a: a)  # type: ignore[method-assign]
        ep._build_remote_command = MagicMock(return_value=built_remote_cmd)  # type: ignore[method-assign]
        ep.ssh_manager = MagicMock()
        ep.ssh_manager.get_ssh_base_cmd.return_value = ["ssh", "host"]
        captured: dict = {}

        def fake_run(cmd, *a, **k):
            captured["cmd"] = cmd
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

        with patch.object(ssh_mod.subprocess, "run", side_effect=fake_run):
            ep._exec_remote_command(["btrfs", "subvolume", "show", "/x"])
        return captured["cmd"]

    def test_path_element_with_space_is_quoted(self):
        ssh_cmd = self._capture_ssh_cmd(["btrfs", "subvolume", "show", "/mnt/a b"])
        assert ssh_cmd == [
            "ssh",
            "host",
            "--",
            "btrfs",
            "subvolume",
            "show",
            "'/mnt/a b'",
        ]

    def test_metachars_are_neutralized(self):
        ssh_cmd = self._capture_ssh_cmd(["rm", "-rf", NASTY])
        assert ssh_cmd[-1] == shlex.quote(NASTY)
        # the dangerous $()/;/quote is inside single quotes now
        assert "$(x)" not in " ".join(ssh_cmd).replace(shlex.quote(NASTY), "")

    def test_clean_elements_unchanged(self):
        ssh_cmd = self._capture_ssh_cmd(["btrfs", "subvolume", "show", CLEAN])
        assert ssh_cmd == ["ssh", "host", "--", "btrfs", "subvolume", "show", CLEAN]


class TestDiagnoseQuotesPaths:
    """MEDIUM: sshutil/diagnose.py builds remote-shell strings; paths must be
    shlex.quote'd, not naively wrapped in bare single quotes."""

    def _run_and_capture(self, func, *args):
        """Call a diagnose.* function with run_command mocked, capturing every
        appended remote-shell string (cmd[-1] of each call)."""
        strings: list[str] = []

        def fake_run_command(cmd, timeout=30):
            strings.append(cmd[-1])
            return (0, "btrfs", "")  # returncode 0 keeps functions progressing

        with patch.object(diag, "run_command", side_effect=fake_run_command):
            func(*args)
        return strings

    def test_write_permissions_quotes_path(self):
        strings = self._run_and_capture(
            diag.test_write_permissions, "host", NASTY, None, None
        )
        joined = "\n".join(strings)
        # The raw single-quote-wrapped form must be gone; the shlex form present.
        assert f"'{NASTY}'" not in joined
        assert shlex.quote(NASTY) in joined

    def test_btrfs_filesystem_quotes_path(self):
        strings = self._run_and_capture(
            diag.test_btrfs_filesystem, "host", NASTY, None, None
        )
        joined = "\n".join(strings)
        assert shlex.quote(NASTY) in joined
        assert f"'{NASTY}'" not in joined

    def test_btrfs_receive_quotes_sudo_path(self):
        strings = self._run_and_capture(
            diag.test_btrfs_receive, "host", NASTY, None, None
        )
        joined = "\n".join(strings)
        # test_path is derived (NASTY + ".test_receive"); assert it's shlex-quoted
        # in the sudo btrfs subvolume command (injection-as-remote-root vector).
        assert any(
            "sudo -n btrfs subvolume create" in s
            and shlex.quote(NASTY + ".test_receive") in s
            for s in strings
        ), joined

    def test_clean_path_not_extra_quoted(self):
        strings = self._run_and_capture(
            diag.test_write_permissions, "host", CLEAN, None, None
        )
        # A clean path appears bare (shlex.quote is a no-op) -> no regression.
        assert any(CLEAN in s and f"'{CLEAN}'" not in s for s in strings)
