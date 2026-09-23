"""A slot's info.xml must be written without needlessly elevating.

The writer assumed "not root => must sudo". Under the sudoers policy this
project documents -- NOPASSWD limited to /usr/bin/btrfs -- `sudo cp` is refused
outright, so every non-root backup lost its info.xml. Measured on a real host:

    WARNING  Failed to place info.xml: Command '['sudo', 'cp', ...]'
             returned non-zero exit status 1

with the destination slot owned by the running user, where a plain copy would
have worked. The consequence is not cosmetic: list_snapper_backups() builds its
`metadata` field from info.xml, so the backup enumerates without a description,
type or userdata, and snapper does not list a restored slot without one.

``_write_info_xml`` is the ONE writer: the backup direction hands it the
source's info.xml verbatim, the restore direction the backup's renumbered.
"""

from __future__ import annotations

import os
import subprocess
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import operations


def _sealed(path):
    """A real directory the current user genuinely cannot write into.

    Better than patching the write to raise: these tests exist because the code
    guessed at permission from the uid instead of attempting the operation, and
    a fake failure would let that guess back in.
    """
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o500)
    return path


_needs_non_root = pytest.mark.skipif(
    os.geteuid() == 0,
    reason="pins NON-root behaviour; root writes regardless of mode bits",
)


CONTENT = b"<?xml version='1.0'?><snapshot><num>1</num></snapshot>\n"


def _endpoint(dest):
    return SimpleNamespace(config={"path": str(dest)}, _is_remote=False)


def _write(dest, content=CONTENT):
    operations._write_info_xml(_endpoint(dest), str(dest), content)


class TestAWritableDestinationNeedsNoSudo:
    def test_plain_copy_is_used_and_sudo_is_never_invoked(self, tmp_path):
        dest = tmp_path / "slot"
        dest.mkdir()
        with patch.object(operations.subprocess, "run") as run:
            _write(dest)
        assert (dest / "info.xml").exists(), "info.xml was not placed"
        assert not run.called, "sudo was invoked for a destination we can write"

    def test_the_content_is_written_as_given(self, tmp_path):
        dest = tmp_path / "slot"
        dest.mkdir()
        _write(dest, b"<snapshot><num>42</num><uid>0</uid></snapshot>")
        assert (dest / "info.xml").read_bytes() == (
            b"<snapshot><num>42</num><uid>0</uid></snapshot>"
        )

    def test_it_works_as_a_non_root_uid(self, tmp_path):
        """The old code keyed on geteuid() alone; ownership is what matters."""
        dest = tmp_path / "slot"
        dest.mkdir()
        with patch("os.geteuid", return_value=1000):
            with patch.object(operations.subprocess, "run") as run:
                _write(dest)
        assert (dest / "info.xml").exists()
        assert not run.called


class TestElevationIsAFallbackNotADefault:
    @_needs_non_root
    def test_sudo_is_tried_only_when_the_plain_write_fails(self, tmp_path):
        dest = _sealed(tmp_path / "slot")
        with patch.object(__util__.subprocess, "run") as run:
            _write(dest)
        assert run.called, "no fallback was attempted for an unwritable destination"
        argv = run.call_args[0][0]
        assert argv[:3] == ["sudo", "-n", "tee"], argv

    @_needs_non_root
    def test_the_fallback_is_non_interactive(self, tmp_path):
        """A backup runs headless; an interactive sudo can only hang."""
        dest = _sealed(tmp_path / "slot")
        with patch.object(__util__.subprocess, "run") as run:
            _write(dest)
        assert "-n" in run.call_args[0][0]

    @_needs_non_root
    def test_root_does_not_shell_out_at_all(self, tmp_path):
        dest = _sealed(tmp_path / "slot")
        with patch("os.geteuid", return_value=0):
            with patch.object(__util__.subprocess, "run") as run:
                _write(dest)
        assert not run.called, "root cannot fix a failure by sudo-ing to root"


class TestFailureIsReportedUsefully:
    @_needs_non_root
    def test_a_total_failure_warns_and_says_what_was_lost(self, tmp_path, caplog):
        """Soft-fail is deliberate -- the backup data is already published -- but
        the operator must be able to trace a later metadata-less listing to here."""
        dest = _sealed(tmp_path / "slot")
        recorded = []
        refused = subprocess.CompletedProcess(
            ["sudo"], 1, b"", b"sudo: a password is required\n"
        )
        with patch.object(__util__.subprocess, "run", return_value=refused):
            with patch.object(
                operations.logger,
                "warning",
                lambda m, *a, **k: recorded.append((m, a)),
            ):
                _write(dest)
        assert recorded, "a total failure produced no warning"
        text = recorded[0][0] % recorded[0][1] if recorded[0][1] else recorded[0][0]
        assert "snapshot itself is intact" in text
        assert "metadata" in text


class TestTheBackupDirectionReadsTheSourcesOwnFile:
    def test_a_missing_source_file_is_none_not_an_error(self, tmp_path):
        """Not every snapper snapshot has an info.xml; that is not an error."""
        snap = SimpleNamespace(info_xml_path=tmp_path / "absent.xml")
        assert operations._snapper_info_xml_bytes(snap) is None

    def test_the_file_is_read_verbatim(self, tmp_path):
        src = tmp_path / "info.xml"
        src.write_bytes(CONTENT)
        assert operations._snapper_info_xml_bytes(
            SimpleNamespace(info_xml_path=src)
        ) == (CONTENT)
