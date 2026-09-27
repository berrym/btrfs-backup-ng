"""One answer to "can this volume be backed up from here", for validate AND doctor.

Measured before this change: `config validate` and `doctor` each asked only
whether a source exists, is a directory and is on btrfs. A plain directory on
btrfs passed both -- validate printed "All enabled volumes look usable from
this machine." and exited 0, doctor printed "Volume path valid" -- and `run`
then failed, because `btrfs subvolume snapshot` refuses anything that is not a
subvolume. An unreadable source made validate stop with a raw "Permission
denied" and exit 1, the status that means the FILE is wrong.

These tests use real paths whose answer does not depend on the machine: a
missing path, a regular file, /proc (never btrfs), an unreadable directory.
The answers that need btrfs -- a subvolume passes, a plain directory is refused
and names the subvolume holding it -- are proven on real btrfs in
tests/integration/tier2/test_source_check_real.py.
"""

from __future__ import annotations

import argparse
import os
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.__util__ as util
from btrfs_backup_ng.__util__ import backup_source_problems
from btrfs_backup_ng.cli.config_cmd import _validate_config
from btrfs_backup_ng.core.doctor import (
    DiagnosticCategory,
    DiagnosticSeverity,
    Doctor,
)

needs_non_root = pytest.mark.skipif(
    os.geteuid() == 0, reason="root reads through a mode-000 directory"
)


@pytest.fixture
def unreadable_source(tmp_path):
    """A source below a directory this account cannot search."""
    locked = tmp_path / "locked"
    source = locked / "source"
    source.mkdir(parents=True)
    locked.chmod(0o000)
    try:
        yield source
    finally:
        locked.chmod(0o755)


def _validate(tmp_path, source, capsys):
    config = tmp_path / "config.toml"
    config.write_text(
        f'[[volumes]]\npath = "{source}"\n\n'
        f'[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n'
    )
    rc = _validate_config(argparse.Namespace(config=str(config)))
    return rc, capsys.readouterr().out


def _doctor_volume_findings(source, snapper=False):
    volume = MagicMock()
    volume.path = str(source)
    volume.is_snapper_source.return_value = snapper
    config = MagicMock()
    config.get_enabled_volumes.return_value = [volume]
    return Doctor(config=config)._check_volume_paths()


class TestTheSharedAnswer:
    def test_a_missing_source(self, tmp_path):
        problems = backup_source_problems(tmp_path / "nope")
        assert len(problems) == 1
        assert "does not exist" in problems[0]

    def test_a_file_where_a_subvolume_belongs(self, tmp_path):
        target = tmp_path / "afile"
        target.write_text("x")
        problems = backup_source_problems(target)
        assert len(problems) == 1
        assert "is not a directory" in problems[0]

    def test_a_directory_on_a_filesystem_that_is_not_btrfs(self):
        problems = backup_source_problems("/proc")
        assert len(problems) == 1
        assert "is not on a btrfs filesystem" in problems[0]

    def test_a_plain_directory_is_never_usable(self, tmp_path):
        """Whatever filesystem tmp_path is on, a directory made with mkdir is
        not a subvolume root, so it is refused: as not on btrfs, or as a
        directory inside a subvolume."""
        source = tmp_path / "plain"
        source.mkdir()
        problems = backup_source_problems(source)
        assert len(problems) == 1
        assert "btrfs" in problems[0]

    @needs_non_root
    def test_an_unreadable_source_is_not_reported_missing(self, unreadable_source):
        problems = backup_source_problems(unreadable_source)
        assert len(problems) == 1
        assert "cannot be checked from this account" in problems[0]
        assert "does not exist" not in problems[0]

    def test_a_probe_failure_is_raised_to_the_caller(self, tmp_path, monkeypatch):
        """Reading the mount table failing is not an answer about the source;
        each caller decides what it means (below)."""
        monkeypatch.setattr(
            util, "is_btrfs", lambda p: (_ for _ in ()).throw(OSError("boom"))
        )
        with pytest.raises(util.SourceProbeError, match="boom"):
            backup_source_problems(tmp_path)

    def test_a_relative_source_is_reported_as_the_path_it_names(
        self, tmp_path, monkeypatch
    ):
        """`run` resolves a relative source against the working directory; the
        message names that path, not the bare relative one."""
        monkeypatch.chdir(tmp_path)
        problems = backup_source_problems("nope")
        assert problems == [f"Source {tmp_path / 'nope'} does not exist."]


class TestValidateUsesIt:
    @needs_non_root
    def test_an_unreadable_source_exits_2_with_the_reason(
        self, tmp_path, unreadable_source, capsys
    ):
        """Before: a raw 'Permission denied' and exit 1, the status that says
        the file itself must be edited."""
        rc, out = _validate(tmp_path, unreadable_source, capsys)
        assert rc == 2, out
        assert "cannot be checked from this account" in out
        assert "Errno" not in out

    def test_a_directory_not_on_btrfs_exits_2(self, tmp_path, capsys):
        rc, out = _validate(tmp_path, "/proc", capsys)
        assert rc == 2, out
        assert "Source /proc is not on a btrfs filesystem." in out

    def test_a_probe_failure_is_no_verdict(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setattr(
            util, "is_btrfs", lambda p: (_ for _ in ()).throw(OSError("boom"))
        )
        source = tmp_path / "src"
        source.mkdir()
        rc, out = _validate(tmp_path, source, capsys)
        assert rc == 0, out

    def test_a_fault_in_the_check_is_not_a_pass(self, tmp_path, capsys, monkeypatch):
        """Only a failed mount-table probe is 'no verdict'. Any other exception
        is a fault in the check and must not come out as 'usable', exit 0."""

        def broken(path, **kw):
            raise RuntimeError("a bug in the check")

        monkeypatch.setattr(util, "backup_source_problems", broken)
        with pytest.raises(RuntimeError, match="a bug in the check"):
            _validate(tmp_path, tmp_path, capsys)


class TestDoctorUsesIt:
    def test_each_problem_is_an_error_with_the_shared_message(self, tmp_path):
        findings = _doctor_volume_findings("/proc")
        assert [(f.severity, f.message) for f in findings] == [
            (DiagnosticSeverity.ERROR, backup_source_problems("/proc")[0])
        ]

    def test_no_problem_is_ok(self, monkeypatch):
        monkeypatch.setattr(util, "backup_source_problems", lambda p, **kw: [])
        findings = _doctor_volume_findings("/anywhere")
        assert [f.severity for f in findings] == [DiagnosticSeverity.OK]

    @needs_non_root
    def test_an_unreadable_source_is_an_error_with_the_reason(self, unreadable_source):
        findings = _doctor_volume_findings(unreadable_source)
        assert [f.severity for f in findings] == [DiagnosticSeverity.ERROR]
        assert "cannot be checked from this account" in findings[0].message

    def test_a_probe_failure_is_a_failed_check_not_a_pass(self, monkeypatch):
        """doctor never says 'valid' for a source it could not check."""
        monkeypatch.setattr(
            util, "is_btrfs", lambda p: (_ for _ in ()).throw(OSError("boom"))
        )
        volume = MagicMock()
        volume.path = "/"
        volume.is_snapper_source.return_value = False
        config = MagicMock()
        config.get_enabled_volumes.return_value = [volume]
        report = Doctor(config=config).run_diagnostics(
            categories={DiagnosticCategory.CONFIG}
        )
        mine = [f for f in report.findings if f.check_name == "volume_paths"]
        assert [f.severity for f in mine] == [DiagnosticSeverity.ERROR]
        assert "boom" in mine[0].message


class TestSnapperSources:
    """snapper takes a snapper volume's snapshots, and `run` never snapshots its
    path (with `config_name = "auto"`, any path at or below a snapper subvolume
    selects that config). So a snapper source is not required to be a
    subvolume root -- proven on real btrfs in tier2 -- but is still checked as
    far as being a readable directory on btrfs."""

    def test_the_checks_before_the_subvolume_one_still_apply(self, tmp_path):
        afile = tmp_path / "afile"
        afile.write_text("x")
        assert (
            "does not exist"
            in backup_source_problems(tmp_path / "nope", snapper=True)[0]
        )
        assert "is not a directory" in backup_source_problems(afile, snapper=True)[0]
        assert (
            "is not on a btrfs filesystem"
            in backup_source_problems("/proc", snapper=True)[0]
        )

    @needs_non_root
    def test_an_unreadable_snapper_source(self, unreadable_source):
        problems = backup_source_problems(unreadable_source, snapper=True)
        assert "cannot be checked from this account" in problems[0]

    @pytest.mark.parametrize("source,snapper", [("snapper", True), ("native", False)])
    def test_validate_asks_with_the_volume_source_kind(
        self, tmp_path, capsys, monkeypatch, source, snapper
    ):
        asked = []
        monkeypatch.setattr(
            util,
            "backup_source_problems",
            lambda p, **kw: asked.append(kw) or [],
        )
        config = tmp_path / "config.toml"
        body = f'[[volumes]]\npath = "/data"\nsource = "{source}"\n\n'
        if source == "snapper":
            body += '[volumes.snapper]\nconfig_name = "data"\n\n'
        body += f'[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n'
        config.write_text(body)
        assert _validate_config(argparse.Namespace(config=str(config))) == 0
        assert asked == [{"snapper": snapper}]

    @pytest.mark.parametrize("snapper", [True, False])
    def test_doctor_asks_with_the_volume_source_kind(self, monkeypatch, snapper):
        asked = []
        monkeypatch.setattr(
            util,
            "backup_source_problems",
            lambda p, **kw: asked.append(kw) or [],
        )
        _doctor_volume_findings("/data", snapper=snapper)
        assert asked == [{"snapper": snapper}]


class TestTheTwoCommandsAgree:
    @pytest.mark.parametrize("kind", ["missing", "file", "not-btrfs", "plain"])
    def test_doctor_reports_exactly_what_validate_prints(self, tmp_path, kind, capsys):
        source = {
            "missing": tmp_path / "nope",
            "file": tmp_path / "afile",
            "not-btrfs": "/proc",
            "plain": tmp_path / "plain",
        }[kind]
        if kind == "file":
            source.write_text("x")
        if kind == "plain":
            source.mkdir()
        rc, out = _validate(tmp_path, source, capsys)
        doctor_messages = [f.message for f in _doctor_volume_findings(source)]
        assert rc == 2
        assert len(doctor_messages) == 1
        assert f"  - {doctor_messages[0]}" in out.splitlines()
