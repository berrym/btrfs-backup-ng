"""Tier 2: what counts as a usable backup source, on real btrfs.

`config validate` and `doctor` used to call any directory on btrfs usable, and
`run` then failed: `btrfs subvolume snapshot` refuses anything that is not a
subvolume. Both now ask `__util__.backup_source_problems`. The unit tests cover
the answers that need no btrfs; these cover the ones that do -- a subvolume, the
top-level root and a snapshot pass; a plain directory is refused and names the
subvolume that holds it, the nearest one when subvolumes nest.
"""

import argparse
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.__util__ import backup_source_problems
from btrfs_backup_ng.cli.config_cmd import _validate_config
from btrfs_backup_ng.core.doctor import DiagnosticSeverity, Doctor

from .conftest import create_snapshot, requires_btrfs


def _subvolume(path: Path) -> Path:
    subprocess.run(
        ["btrfs", "subvolume", "create", str(path)], check=True, capture_output=True
    )
    return path


def _validate(
    tmp_path: Path, source: Path, capsys, snapper: bool = False
) -> tuple[int, str]:
    config = tmp_path / "config.toml"
    body = f'[[volumes]]\npath = "{source}"\n'
    if snapper:
        body += 'source = "snapper"\n\n[volumes.snapper]\nconfig_name = "home"\n'
    body += f'\n[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n'
    config.write_text(body)
    rc = _validate_config(argparse.Namespace(config=str(config)))
    return rc, capsys.readouterr().out


def _doctor(source: Path, snapper: bool = False):
    volume = MagicMock()
    volume.path = str(source)
    volume.is_snapper_source.return_value = snapper
    config = MagicMock()
    config.get_enabled_volumes.return_value = [volume]
    return Doctor(config=config)._check_volume_paths()


@pytest.mark.tier2
@requires_btrfs
class TestUsableSources:
    def test_a_subvolume(self, btrfs_volume: Path, tmp_path: Path, capsys):
        source = _subvolume(btrfs_volume / "home")
        assert backup_source_problems(source) == []
        rc, out = _validate(tmp_path, source, capsys)
        assert rc == 0, out
        assert "All enabled volumes look usable from this machine." in out
        assert [f.severity for f in _doctor(source)] == [DiagnosticSeverity.OK]

    def test_the_top_level_root(self, btrfs_volume: Path):
        assert backup_source_problems(btrfs_volume) == []

    def test_a_snapshot(self, btrfs_volume: Path):
        source = _subvolume(btrfs_volume / "home")
        snap = create_snapshot(source, btrfs_volume / "home.snap", readonly=True)
        assert backup_source_problems(snap) == []


@pytest.mark.tier2
@requires_btrfs
class TestAPlainDirectoryIsRefusedAndNamesItsSubvolume:
    def test_a_directory_in_the_top_level_names_the_mount_root(
        self, btrfs_volume: Path
    ):
        source = btrfs_volume / "plain"
        source.mkdir()
        problems = backup_source_problems(source)
        assert len(problems) == 1
        assert (
            f"is a directory inside the btrfs subvolume {btrfs_volume.resolve()},"
            in problems[0]
        ), problems

    def test_a_deep_directory_names_the_subvolume_holding_it(self, btrfs_volume: Path):
        holder = _subvolume(btrfs_volume / "data")
        source = holder / "a" / "b"
        source.mkdir(parents=True)
        problems = backup_source_problems(source)
        assert f"inside the btrfs subvolume {holder.resolve()}," in problems[0]
        assert f"Point the volume at {holder.resolve()}," in problems[0]

    def test_nested_subvolumes_name_the_nearest(self, btrfs_volume: Path):
        outer = _subvolume(btrfs_volume / "outer")
        inner = _subvolume(outer / "inner")
        source = inner / "dir"
        source.mkdir()
        problems = backup_source_problems(source)
        assert f"inside the btrfs subvolume {inner.resolve()}," in problems[0]

    def test_a_bind_mount_does_not_borrow_the_subvolume_it_sits_in(
        self, btrfs_volume: Path
    ):
        """A plain directory of subvolume A bind-mounted inside subvolume B:
        its parent directories belong to B, a different device, and naming B
        would send the operator to the wrong subvolume. With no holder that can
        be read, the message says so without naming one."""
        a = _subvolume(btrfs_volume / "a")
        (a / "x").mkdir()
        b = _subvolume(btrfs_volume / "b")
        source = b / "mnt"
        source.mkdir()
        subprocess.run(["mount", "--bind", str(a / "x"), str(source)], check=True)
        try:
            problems = backup_source_problems(source)
        finally:
            subprocess.run(["umount", str(source)], check=True)
        assert len(problems) == 1
        assert "inside the btrfs subvolume" not in problems[0], problems
        assert "is a directory, not a btrfs subvolume" in problems[0]

    def test_validate_exits_2_and_doctor_reports_the_same_error(
        self, btrfs_volume: Path, tmp_path: Path, capsys
    ):
        holder = _subvolume(btrfs_volume / "home")
        source = holder / "user"
        source.mkdir()
        rc, out = _validate(tmp_path, source, capsys)
        assert rc == 2, out
        assert "look usable" not in out
        findings = _doctor(source)
        assert [f.severity for f in findings] == [DiagnosticSeverity.ERROR]
        assert f"  - {findings[0].message}" in out.splitlines()
        assert f"inside the btrfs subvolume {holder.resolve()}," in out


@pytest.mark.tier2
@requires_btrfs
class TestASnapperSourceNeedNotBeASubvolumeRoot:
    """snapper takes a snapper volume's snapshots and `run` never snapshots its
    path: with `config_name = "auto"` any path at or below a snapper subvolume
    selects that config. The subvolume-root requirement is for native sources."""

    def test_a_directory_inside_a_subvolume_passes_for_snapper_only(
        self, btrfs_volume: Path
    ):
        holder = _subvolume(btrfs_volume / "home")
        source = holder / "user"
        source.mkdir()
        assert backup_source_problems(source, snapper=True) == []
        assert backup_source_problems(source)  # native: refused

    def test_validate_and_doctor_accept_it(
        self, btrfs_volume: Path, tmp_path: Path, capsys
    ):
        holder = _subvolume(btrfs_volume / "home")
        source = holder / "user"
        source.mkdir()
        rc, out = _validate(tmp_path, source, capsys, snapper=True)
        assert rc == 0, out
        assert "All enabled volumes look usable from this machine." in out
        findings = _doctor(source, snapper=True)
        assert [f.severity for f in findings] == [DiagnosticSeverity.OK]
