"""info.xml must be placed without needlessly elevating.

_place_info_xml assumed "not root => must sudo". Under the sudoers policy this
project documents -- NOPASSWD limited to /usr/bin/btrfs -- `sudo cp` is refused
outright, so every non-root backup lost its info.xml. Measured on a real host:

    WARNING  Failed to place info.xml: Command '['sudo', 'cp', ...]'
             returned non-zero exit status 1

with the destination slot owned by the running user, where a plain copy would
have worked. The consequence is not cosmetic: list_snapper_backups() builds its
`metadata` field from info.xml, so the backup enumerates without a description,
type or userdata.
"""

from __future__ import annotations

import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import patch


from btrfs_backup_ng.core import operations


def _snapshot(tmp_path):
    src = tmp_path / "info.xml"
    src.write_text("<?xml version='1.0'?><snapshot><num>1</num></snapshot>\n")
    return SimpleNamespace(info_xml_path=src)


def _endpoint(dest):
    return SimpleNamespace(config={"path": str(dest)}, _is_remote=False)


class TestAWritableDestinationNeedsNoSudo:
    def test_plain_copy_is_used_and_sudo_is_never_invoked(self, tmp_path):
        dest = tmp_path / "slot"
        dest.mkdir()
        with patch.object(operations.subprocess, "run") as run:
            operations._place_info_xml(_snapshot(tmp_path), _endpoint(dest))
        assert (dest / "info.xml").exists(), "info.xml was not placed"
        assert not run.called, "sudo was invoked for a destination we can write"

    def test_the_content_is_the_source_content(self, tmp_path):
        dest = tmp_path / "slot"
        dest.mkdir()
        snap = _snapshot(tmp_path)
        operations._place_info_xml(snap, _endpoint(dest))
        assert (dest / "info.xml").read_text() == snap.info_xml_path.read_text()

    def test_it_works_as_a_non_root_uid(self, tmp_path):
        """The old code keyed on geteuid() alone; ownership is what matters."""
        dest = tmp_path / "slot"
        dest.mkdir()
        with patch("os.geteuid", return_value=1000):
            with patch.object(operations.subprocess, "run") as run:
                operations._place_info_xml(_snapshot(tmp_path), _endpoint(dest))
        assert (dest / "info.xml").exists()
        assert not run.called


class TestElevationIsAFallbackNotADefault:
    def test_sudo_is_tried_only_when_the_plain_copy_fails(self, tmp_path):
        dest = tmp_path / "slot"
        dest.mkdir()
        with patch.object(shutil, "copy2", side_effect=PermissionError("denied")):
            with patch("os.geteuid", return_value=1000):
                with patch.object(operations.subprocess, "run") as run:
                    operations._place_info_xml(_snapshot(tmp_path), _endpoint(dest))
        assert run.called, "no fallback was attempted for an unwritable destination"
        argv = run.call_args[0][0]
        assert argv[:3] == ["sudo", "-n", "cp"], argv

    def test_the_fallback_is_non_interactive(self, tmp_path):
        """A backup runs headless; an interactive sudo can only hang."""
        dest = tmp_path / "slot"
        dest.mkdir()
        with patch.object(shutil, "copy2", side_effect=PermissionError("x")):
            with patch("os.geteuid", return_value=1000):
                with patch.object(operations.subprocess, "run") as run:
                    operations._place_info_xml(_snapshot(tmp_path), _endpoint(dest))
        assert "-n" in run.call_args[0][0]

    def test_root_does_not_shell_out_at_all(self, tmp_path):
        dest = tmp_path / "slot"
        dest.mkdir()
        with patch.object(shutil, "copy2", side_effect=PermissionError("x")):
            with patch("os.geteuid", return_value=0):
                with patch.object(operations.subprocess, "run") as run:
                    operations._place_info_xml(_snapshot(tmp_path), _endpoint(dest))
        assert not run.called, "root cannot fix a failure by sudo-ing to root"


class TestFailureIsReportedUsefully:
    def test_a_total_failure_warns_and_says_what_was_lost(self, tmp_path, caplog):
        """Soft-fail is deliberate -- the backup data is already published -- but
        the operator must be able to trace a later metadata-less listing to here."""
        dest = tmp_path / "slot"
        dest.mkdir()
        recorded = []
        with patch.object(shutil, "copy2", side_effect=PermissionError("denied")):
            with patch("os.geteuid", return_value=1000):
                with patch.object(
                    operations.subprocess,
                    "run",
                    side_effect=subprocess.CalledProcessError(1, "sudo"),
                ):
                    with patch.object(
                        operations.logger,
                        "warning",
                        lambda m, *a, **k: recorded.append((m, a)),
                    ):
                        operations._place_info_xml(_snapshot(tmp_path), _endpoint(dest))
        assert recorded, "a total failure produced no warning"
        text = recorded[0][0] % recorded[0][1] if recorded[0][1] else recorded[0][0]
        assert "backup itself is intact" in text
        assert "metadata" in text

    def test_a_missing_source_is_silently_skipped(self, tmp_path):
        """Not every snapper snapshot has an info.xml; that is not an error."""
        dest = tmp_path / "slot"
        dest.mkdir()
        snap = SimpleNamespace(info_xml_path=tmp_path / "absent.xml")
        operations._place_info_xml(snap, _endpoint(dest))
        assert not (dest / "info.xml").exists()
