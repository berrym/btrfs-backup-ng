"""validate and doctor ask what `run` depends on: the snapper config and the
snapshot_dir, not the source alone.

Measured before this change: a snapper volume naming a config this machine
does not have, or lying under no config with `config_name = "auto"`, passed
`config validate` and `doctor` and failed `run` with "cannot detect snapper
config" (#112); a native volume whose absolute `snapshot_dir` was absent or
on another filesystem passed both and failed `run`, which refuses to create a
base and whose `btrfs subvolume snapshot` cannot cross filesystems (#113).
And snapper's configs were read as files, so on a host where they are mode
0640 (Fedora writes them so) an unprivileged validate or doctor did not see
them at all, although `snapper --jsonout list-configs` lists them for any user.

The answers that need real btrfs -- a snapshot_dir on the source's filesystem
passes, one on another btrfs is refused naming both devices -- are proven in
tests/integration/tier2/test_source_check_real.py. Here the mount table and
the kernel's list of btrfs filesystems are files the test writes, and snapper
is a stand-in that prints the JSON the real one prints.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.__util__ as util
import btrfs_backup_ng.snapper.scanner as scanner_mod
from btrfs_backup_ng.__util__ import (
    backup_volume_problems,
    filesystem_identity,
    mount_holding,
    snapshot_dir_problems,
)
from btrfs_backup_ng.cli.config_cmd import _validate_config
from btrfs_backup_ng.core.doctor import DiagnosticSeverity, Doctor
from btrfs_backup_ng.snapper.scanner import (
    SnapperNotFoundError,
    SnapperScanner,
    snapper_config_problems,
)

needs_non_root = pytest.mark.skipif(
    os.geteuid() == 0, reason="root reads through a mode-000 directory"
)


# --------------------------------------------------------------------------- #
# a mount table and a btrfs device list the test writes
# --------------------------------------------------------------------------- #
@pytest.fixture
def disks(tmp_path, monkeypatch):
    """Four mounts: fs A (btrfs, /dev/vda) with a second mount of the same
    device below it, fs B (btrfs, /dev/vdb), and an ext4 one. The kernel's
    btrfs list says vda and vdc are members of one filesystem."""
    a = tmp_path / "a"
    a_sub = a / "sub"
    b = tmp_path / "b"
    ext = tmp_path / "ext"
    for d in (a_sub, b, ext):
        d.mkdir(parents=True)
    mounts = tmp_path / "mounts"
    mounts.write_text(
        f"/dev/vda {a} btrfs rw 0 0\n"
        f"/dev/vda {a_sub} btrfs rw,subvol=/sub 0 0\n"
        f"/dev/vdb {b} btrfs rw 0 0\n"
        f"/dev/vdd {ext} ext4 rw 0 0\n"
    )
    sysfs = tmp_path / "sysfs"
    for dev in ("vda", "vdc"):
        (sysfs / "fs-a-uuid" / "devices" / dev).mkdir(parents=True)
    (sysfs / "fs-b-uuid" / "devices" / "vdb").mkdir(parents=True)
    (sysfs / "features").mkdir()
    monkeypatch.setattr(util, "MOUNTS_FILE", str(mounts))
    monkeypatch.setattr(util, "SYSFS_BTRFS", sysfs)
    monkeypatch.setattr(util, "DISK_BY_UUID", tmp_path / "no-by-uuid")
    return argparse.Namespace(a=a, a_sub=a_sub, b=b, ext=ext, sysfs=sysfs)


class TestTheFilesystemBehindAMount:
    def test_the_longest_mount_point_holds_the_path(self, disks):
        assert mount_holding(disks.a / "x") == ("/dev/vda", str(disks.a), "btrfs")
        assert mount_holding(disks.a_sub / "y") == (
            "/dev/vda",
            str(disks.a_sub),
            "btrfs",
        )
        assert mount_holding(disks.ext)[2] == "ext4"

    def test_members_of_one_btrfs_are_one_filesystem(self, disks):
        assert filesystem_identity("/dev/vda") == "btrfs:fs-a-uuid"
        assert filesystem_identity("/dev/vdc") == "btrfs:fs-a-uuid"
        assert filesystem_identity("/dev/vdb") == "btrfs:fs-b-uuid"

    def test_a_device_the_kernel_does_not_list_falls_back_to_by_uuid(
        self, disks, tmp_path, monkeypatch
    ):
        by_uuid = tmp_path / "by-uuid"
        by_uuid.mkdir()
        target = tmp_path / "vdd"
        target.write_text("")
        os.symlink(target, by_uuid / "1234-ABCD")
        monkeypatch.setattr(util, "DISK_BY_UUID", by_uuid)
        assert filesystem_identity(str(target)) == "UUID=1234-ABCD"
        assert filesystem_identity("/dev/vdz") == "/dev/vdz"


# --------------------------------------------------------------------------- #
# snapshot_dir (#113)
# --------------------------------------------------------------------------- #
class TestSnapshotDir:
    def test_a_relative_one_under_the_source_is_not_checked(self, disks):
        source = disks.a / "home"
        source.mkdir()
        assert snapshot_dir_problems(".snapshots", source) == []
        assert snapshot_dir_problems("deeper/than/that", source) == []

    def test_a_relative_one_climbing_out_must_exist(self, disks):
        source = disks.a / "home"
        source.mkdir()
        (problem,) = snapshot_dir_problems("../elsewhere", source)
        assert f"snapshot_dir {disks.a / 'elsewhere'} does not exist" in problem
        assert "`run` refuses to create it" in problem

    def test_an_absolute_one_must_exist(self, disks):
        (problem,) = snapshot_dir_problems(str(disks.a / "snaps"), disks.a / "home")
        assert "does not exist" in problem and "Nothing was created" in problem

    def test_a_file_where_a_directory_belongs(self, disks):
        afile = disks.a / "afile"
        afile.write_text("x")
        assert snapshot_dir_problems(str(afile), disks.a / "home") == [
            f"snapshot_dir {afile} is not a directory."
        ]

    @needs_non_root
    def test_an_unreadable_one_is_not_reported_missing(self, disks):
        locked = disks.a / "locked"
        snaps = locked / "snaps"
        snaps.mkdir(parents=True)
        locked.chmod(0o000)
        try:
            (problem,) = snapshot_dir_problems(str(snaps), disks.a / "home")
        finally:
            locked.chmod(0o755)
        assert "cannot be checked from this account" in problem

    def test_one_that_is_not_on_btrfs(self, disks):
        (problem,) = snapshot_dir_problems(str(disks.ext), disks.a / "home")
        assert f"snapshot_dir {disks.ext} is not on a btrfs filesystem" in problem

    def test_one_on_another_btrfs_names_both_devices(self, disks):
        source = disks.a / "home"
        source.mkdir()
        (problem,) = snapshot_dir_problems(str(disks.b), source)
        assert f"snapshot_dir {disks.b} is on a different filesystem (/dev/vdb)" in (
            problem
        )
        assert f"from the source {source} (/dev/vda)" in problem
        assert "cannot cross filesystems" in problem

    def test_one_on_the_same_filesystem_passes(self, disks):
        source = disks.a / "home"
        source.mkdir()
        assert snapshot_dir_problems(str(disks.a_sub), source) == []
        assert snapshot_dir_problems(str(disks.a), source) == []

    def test_another_member_of_the_same_filesystem_passes(self, disks, tmp_path):
        """fs A mounted again through its other member device, vdc."""
        c = tmp_path / "c"
        c.mkdir()
        mounts = Path(util.MOUNTS_FILE)
        mounts.write_text(mounts.read_text() + f"/dev/vdc {c} btrfs rw 0 0\n")
        source = disks.a / "home"
        source.mkdir()
        assert snapshot_dir_problems(str(c), source) == []


# --------------------------------------------------------------------------- #
# the snapper config (#112)
# --------------------------------------------------------------------------- #
def _configs(tmp_path, **subvolumes):
    configs = tmp_path / "configs"
    configs.mkdir(exist_ok=True)
    for name, subvolume in subvolumes.items():
        Path(subvolume).mkdir(parents=True, exist_ok=True)
        (configs / name).write_text(f'SUBVOLUME="{subvolume}"\nFSTYPE="btrfs"\n')
    return configs


def _file_scanner(configs):
    return SnapperScanner(configs_dir=configs, use_snapper_command=False)


class TestSnapperConfig:
    def test_a_named_config_that_exists(self, tmp_path):
        configs = _configs(tmp_path, home=tmp_path / "home")
        assert (
            snapper_config_problems(tmp_path / "home", "home", _file_scanner(configs))
            == []
        )

    def test_a_named_config_that_does_not_exist_lists_what_does(self, tmp_path):
        configs = _configs(tmp_path, root=tmp_path / "r", home=tmp_path / "h")
        (problem,) = snapper_config_problems(
            tmp_path / "h", "data", _file_scanner(configs)
        )
        assert "names the snapper config 'data', which does not exist" in problem
        assert f"snapper manages home ({tmp_path / 'h'}), root ({tmp_path / 'r'})" in (
            problem
        )

    def test_auto_with_a_config_holding_the_path(self, tmp_path):
        configs = _configs(tmp_path, home=tmp_path / "home")
        scanner = _file_scanner(configs)
        assert snapper_config_problems(tmp_path / "home", "auto", scanner) == []
        assert snapper_config_problems(tmp_path / "home" / "me", "auto", scanner) == []

    def test_auto_with_no_config_holding_the_path(self, tmp_path):
        configs = _configs(tmp_path, home=tmp_path / "home")
        (problem,) = snapper_config_problems(
            tmp_path / "srv", "auto", _file_scanner(configs)
        )
        assert "is under no snapper config's subvolume" in problem
        assert 'config_name is "auto"' in problem
        assert f"snapper manages home ({tmp_path / 'home'})" in problem

    def test_no_configs_at_all(self, tmp_path):
        configs = _configs(tmp_path)
        (problem,) = snapper_config_problems(
            tmp_path / "x", "auto", _file_scanner(configs)
        )
        assert "snapper has no configs on this machine" in problem

    def test_snapper_not_set_up(self, tmp_path):
        scanner = _file_scanner(tmp_path / "nowhere")
        (problem,) = snapper_config_problems(tmp_path / "x", "root", scanner)
        assert "snapper is not set up on this machine" in problem
        assert "Snapper configs directory not found" in problem


# --------------------------------------------------------------------------- #
# the scanner asks snapper itself
# --------------------------------------------------------------------------- #
@pytest.fixture
def snapper_stand_in(tmp_path, monkeypatch):
    """A `snapper` first on PATH that answers `--version` and prints the JSON
    in ``listing`` for `--jsonout list-configs`, recording every call; the
    system configs directory is a directory under tmp_path."""
    bin_dir = tmp_path / "snapper-bin"
    bin_dir.mkdir()
    listing = tmp_path / "listing.json"
    calls = tmp_path / "snapper-calls"
    calls.write_text("")
    script = bin_dir / "snapper"
    script.write_text(
        "#!/bin/sh\n"
        f'echo "$*" >> {calls}\n'
        'case "$1" in\n'
        "  --version) echo 'snapper 0.0 (stand-in)'; exit 0 ;;\n"
        "esac\n"
        f"[ -f {listing} ] && cat {listing} && exit 0\n"
        "exit 1\n"
    )
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    configs = tmp_path / "etc-snapper-configs"
    configs.mkdir()
    monkeypatch.setattr(scanner_mod, "SNAPPER_CONFIGS_DIR", configs)

    def answer(*entries):
        listing.write_text(
            json.dumps(
                {"configs": [{"config": n, "subvolume": str(s)} for n, s in entries]}
            )
        )

    return argparse.Namespace(
        configs=configs, answer=answer, calls=lambda: calls.read_text().splitlines()
    )


class TestTheScannerAsksSnapper:
    def test_configs_root_alone_can_read_are_listed(self, snapper_stand_in, tmp_path):
        """No file for `home` at all, as a 0640 file reads to a user."""
        snapper_stand_in.answer(("home", tmp_path / "home"), ("root", "/"))
        scanner = SnapperScanner(configs_dir=snapper_stand_in.configs)

        configs = scanner.list_configs()

        assert [(c.name, c.subvolume) for c in configs] == [
            ("home", tmp_path / "home"),
            ("root", Path("/")),
        ]
        assert scanner.get_config("home").subvolume == tmp_path / "home"
        assert scanner.get_config("data") is None
        assert scanner.find_config_for_path(tmp_path / "home" / "me").name == "home"

    def test_a_readable_file_still_supplies_the_other_fields(
        self, snapper_stand_in, tmp_path
    ):
        snapper_stand_in.answer(("home", tmp_path / "home"))
        (snapper_stand_in.configs / "home").write_text(
            f'SUBVOLUME="{tmp_path / "home"}"\nSPACE_LIMIT="0.3"\n'
        )
        (config,) = SnapperScanner(configs_dir=snapper_stand_in.configs).list_configs()
        assert (config.subvolume, config.space_limit) == (tmp_path / "home", 0.3)

    def test_the_files_are_read_when_the_command_fails(
        self, snapper_stand_in, tmp_path
    ):
        # no listing file: the stand-in exits 1
        (snapper_stand_in.configs / "root").write_text('SUBVOLUME="/"\n')
        (config,) = SnapperScanner(configs_dir=snapper_stand_in.configs).list_configs()
        assert config.name == "root"
        assert any(
            c.startswith("--jsonout list-configs") for c in snapper_stand_in.calls()
        )

    def test_the_files_are_read_where_snapper_is_absent(self, tmp_path, monkeypatch):
        empty = tmp_path / "empty-bin"
        empty.mkdir()
        monkeypatch.setenv("PATH", str(empty))
        configs = _configs(tmp_path, root=tmp_path / "r")
        monkeypatch.setattr(scanner_mod, "SNAPPER_CONFIGS_DIR", configs)
        (config,) = SnapperScanner(configs_dir=configs).list_configs()
        assert config.name == "root"

    def test_another_configs_directory_is_read_as_files_alone(
        self, snapper_stand_in, tmp_path
    ):
        snapper_stand_in.answer(("home", tmp_path / "home"))
        other = _configs(tmp_path, root=tmp_path / "r")
        (config,) = SnapperScanner(configs_dir=other).list_configs()
        assert config.name == "root"
        assert not any("list-configs" in c for c in snapper_stand_in.calls())

    def test_a_missing_directory_without_snapper_is_still_not_found(
        self, tmp_path, monkeypatch
    ):
        empty = tmp_path / "empty-bin"
        empty.mkdir()
        monkeypatch.setenv("PATH", str(empty))
        monkeypatch.setattr(scanner_mod, "SNAPPER_CONFIGS_DIR", tmp_path / "none")
        with pytest.raises(SnapperNotFoundError):
            SnapperScanner(configs_dir=tmp_path / "none").list_configs()

    @pytest.mark.requires_tool("snapper")
    def test_the_real_snapper_lists_what_it_lists(self):
        probe = subprocess.run(
            ["snapper", "--jsonout", "list-configs"], capture_output=True, text=True
        )
        if probe.returncode != 0:
            pytest.skip(f"snapper answers no listing here: {probe.stderr.strip()}")
        expected = sorted(
            (c["config"], Path(c["subvolume"]))
            for c in json.loads(probe.stdout)["configs"]
        )
        listed = SnapperScanner().list_configs()
        assert sorted((c.name, c.subvolume) for c in listed) == expected


# --------------------------------------------------------------------------- #
# the one question, and the two commands asking it
# --------------------------------------------------------------------------- #
class TestTheOneQuestion:
    def test_a_source_problem_ends_it(self, monkeypatch):
        monkeypatch.setattr(util, "backup_source_problems", lambda p, **kw: ["bad"])
        monkeypatch.setattr(
            util, "snapshot_dir_problems", lambda *a: pytest.fail("asked anyway")
        )
        assert backup_volume_problems("/x", snapshot_dir="/y") == ["bad"]
        assert backup_volume_problems("/x", snapper=True) == ["bad"]

    def test_a_native_volume_asks_about_its_snapshot_dir(self, monkeypatch):
        monkeypatch.setattr(util, "backup_source_problems", lambda p, **kw: [])
        asked = []
        monkeypatch.setattr(
            util, "snapshot_dir_problems", lambda d, s: asked.append((d, s)) or ["no"]
        )
        assert backup_volume_problems("/x", snapshot_dir="/snaps") == ["no"]
        assert asked == [("/snaps", "/x")]

    def test_a_snapper_volume_asks_about_its_config_not_its_snapshot_dir(
        self, monkeypatch
    ):
        monkeypatch.setattr(util, "backup_source_problems", lambda p, **kw: [])
        monkeypatch.setattr(
            util, "snapshot_dir_problems", lambda *a: pytest.fail("asked anyway")
        )
        asked = []
        monkeypatch.setattr(
            scanner_mod,
            "snapper_config_problems",
            lambda p, name, scanner=None: asked.append((p, name)) or ["no"],
        )
        assert backup_volume_problems(
            "/x", snapper=True, snapper_config_name="home", snapshot_dir="/s"
        ) == ["no"]
        assert backup_volume_problems("/x", snapper=True) == ["no"]
        assert asked == [("/x", "home"), ("/x", "auto")]


def _write(tmp_path, body):
    path = tmp_path / "config.toml"
    path.write_text(body)
    return path


def _doctor(volume):
    config = MagicMock()
    config.get_enabled_volumes.return_value = [volume]
    return Doctor(config=config)._check_volume_paths()


class TestValidateAndDoctor:
    @pytest.fixture(autouse=True)
    def _usable_source(self, monkeypatch):
        """The source itself passes; what these tests add is the rest."""
        monkeypatch.setattr(util, "backup_source_problems", lambda p, **kw: [])

    def test_a_snapper_volume_naming_a_missing_config(
        self, snapper_stand_in, tmp_path, capsys
    ):
        snapper_stand_in.answer(("root", "/"))
        config = _write(
            tmp_path,
            f'[[volumes]]\npath = "{tmp_path}"\nsource = "snapper"\n\n'
            '[volumes.snapper]\nconfig_name = "data"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n',
        )

        rc = _validate_config(argparse.Namespace(config=str(config)))

        out = capsys.readouterr().out
        assert rc == 2, out
        assert "These volumes cannot be backed up from this machine:" in out
        assert "names the snapper config 'data', which does not exist" in out
        assert "snapper manages root (/)" in out

    def test_a_snapper_volume_under_no_config(self, snapper_stand_in, tmp_path, capsys):
        snapper_stand_in.answer(("home", tmp_path / "home"))
        config = _write(
            tmp_path,
            f'[[volumes]]\npath = "{tmp_path / "srv"}"\nsource = "snapper"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n',
        )

        rc = _validate_config(argparse.Namespace(config=str(config)))

        out = capsys.readouterr().out
        assert rc == 2, out
        assert "is under no snapper config's subvolume" in out

    def test_a_snapper_volume_that_run_can_find(
        self, snapper_stand_in, tmp_path, capsys
    ):
        snapper_stand_in.answer(("home", tmp_path / "home"))
        config = _write(
            tmp_path,
            f'[[volumes]]\npath = "{tmp_path / "home" / "me"}"\nsource = "snapper"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n',
        )

        rc = _validate_config(argparse.Namespace(config=str(config)))

        out = capsys.readouterr().out
        assert rc == 0, out
        assert "All enabled volumes look usable from this machine." in out

    def test_a_native_volume_with_an_absent_snapshot_dir(self, tmp_path, capsys):
        config = _write(
            tmp_path,
            f'[[volumes]]\npath = "{tmp_path}"\n'
            f'snapshot_dir = "{tmp_path / "snaps"}"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n',
        )

        rc = _validate_config(argparse.Namespace(config=str(config)))

        out = capsys.readouterr().out
        assert rc == 2, out
        assert f"snapshot_dir {tmp_path / 'snaps'} does not exist" in out

    def test_a_native_volume_with_a_snapshot_dir_on_another_filesystem(
        self, disks, tmp_path, capsys
    ):
        source = disks.a / "home"
        source.mkdir()
        config = _write(
            tmp_path,
            f'[[volumes]]\npath = "{source}"\nsnapshot_dir = "{disks.b}"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n',
        )

        rc = _validate_config(argparse.Namespace(config=str(config)))

        out = capsys.readouterr().out
        assert rc == 2, out
        assert "is on a different filesystem (/dev/vdb)" in out

    def test_the_global_snapshot_dir_is_the_one_checked(self, tmp_path, capsys):
        config = _write(
            tmp_path,
            f'[global]\nsnapshot_dir = "{tmp_path / "global-snaps"}"\n\n'
            f'[[volumes]]\npath = "{tmp_path}"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n',
        )
        rc = _validate_config(argparse.Namespace(config=str(config)))
        out = capsys.readouterr().out
        assert rc == 2, out
        assert f"snapshot_dir {tmp_path / 'global-snaps'} does not exist" in out

    def test_doctor_reports_exactly_what_validate_prints(
        self, snapper_stand_in, disks, tmp_path, capsys
    ):
        snapper_stand_in.answer(("root", "/"))
        source = disks.a / "home"
        source.mkdir()
        config = _write(
            tmp_path,
            f'[[volumes]]\npath = "{source}"\nsnapshot_dir = "{disks.b}"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n\n'
            f'[[volumes]]\npath = "{tmp_path / "srv"}"\nsource = "snapper"\n\n'
            '[volumes.snapper]\nconfig_name = "data"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n',
        )
        rc = _validate_config(argparse.Namespace(config=str(config)))
        out = capsys.readouterr().out
        assert rc == 2, out

        native = MagicMock()
        native.path = str(source)
        native.is_snapper_source.return_value = False
        native.snapper = None
        native.snapshot_dir = str(disks.b)
        snapper = MagicMock()
        snapper.path = str(tmp_path / "srv")
        snapper.is_snapper_source.return_value = True
        snapper.snapper = MagicMock(config_name="data")
        snapper.snapshot_dir = ".snapshots"
        findings = _doctor(native) + _doctor(snapper)

        assert [f.severity for f in findings] == [DiagnosticSeverity.ERROR] * 2
        for finding in findings:
            assert f"  - {finding.message}" in out.splitlines(), finding.message

    def test_doctor_says_ok_when_run_could_proceed(self, snapper_stand_in, tmp_path):
        snapper_stand_in.answer(("home", tmp_path / "home"))
        volume = MagicMock()
        volume.path = str(tmp_path / "home")
        volume.is_snapper_source.return_value = True
        volume.snapper = MagicMock(config_name="auto")
        volume.snapshot_dir = ".snapshots"
        assert [f.severity for f in _doctor(volume)] == [DiagnosticSeverity.OK]
