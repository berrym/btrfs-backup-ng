"""Size estimates where sudo refuses, is absent, or is not needed.

Every local size measurement ran its commands under `sudo -n` when not root
and never as the user, so where sudo refused or was not installed the
estimate failed, although `btrfs filesystem du` and `du` need no privilege on
a tree the user can read (#117). The ssh endpoint's copy ran a plain `sudo`,
root included. These drive the one ladder the three copies became, with the
real tools against real directories: the sudo stand-in refusing as `sudo -n`
does for a user with no rule, absent from PATH, or agreeing. Cases that need
btrfs run under `BBNG_TEST_RIG` and skip without it.

The expected figure is computed by running the tool as the test's user: on
btrfs `btrfs filesystem du` answers and is preferred, elsewhere it fails and
`du -sb` answers, so each test states what it expects from what the machine
can measure rather than assuming one of the two.
"""

from __future__ import annotations

import fcntl
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import estimate as estimate_mod
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.core import progress
from btrfs_backup_ng.core.estimate import (
    SizeEstimate,
    estimate_snapshot_full_size,
    estimate_transfer,
    measure_snapshot_full_size,
    print_estimate,
)
from btrfs_backup_ng.core.space import SpaceInfo
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

FICLONE = 0x40049409


def _du(path: Path) -> int:
    out = subprocess.run(
        ["du", "-sb", str(path)], capture_output=True, text=True, check=True
    ).stdout
    return int(out.split()[0])


def _filesystem_du(path: Path) -> int | None:
    """The Total `btrfs filesystem du -s --raw` gives this user, or None."""
    result = subprocess.run(
        ["btrfs", "filesystem", "du", "-s", "--raw", str(path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return int(result.stdout.strip().splitlines()[1].split()[0])


def _expected(path: Path) -> tuple[int, str]:
    """What the ladder should answer for a tree this user can read."""
    total = _filesystem_du(path)
    if total:
        return total, "filesystem_du"
    return _du(path), "du"


def _fill(root: Path) -> Path:
    (root / "sub").mkdir(parents=True)
    (root / "a.bin").write_bytes(b"a" * 300_000)
    (root / "sub" / "b.bin").write_bytes(b"b" * 50_000)
    return root


@pytest.fixture
def readable(tmp_path: Path) -> Path:
    return _fill(tmp_path / "snap")


@pytest.fixture
def btrfs_dir():
    rig = os.environ.get("BBNG_TEST_RIG")
    if not rig:
        pytest.skip("BBNG_TEST_RIG names no directory on btrfs")
    with tempfile.TemporaryDirectory(dir=rig, prefix="bbng-117-") as name:
        path = Path(name)
        probe = subprocess.run(
            ["btrfs", "filesystem", "du", "-s", "--raw", name],
            capture_output=True,
            text=True,
        )
        if probe.returncode != 0:
            pytest.skip(f"{rig} is not on btrfs: {probe.stderr.strip()}")
        yield path


def _write_synced(path: Path, data: bytes, *, apparent_size: int | None = None):
    with open(path, "wb") as fh:
        if apparent_size is not None:
            fh.truncate(apparent_size)
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


class TestWhereSudoRefuses:
    @pytest.mark.requires_tool("du")
    def test_a_readable_tree_is_measured_by_the_user_after_sudo_refuses(
        self, readable, sudo_refuses
    ):
        expected = _expected(readable)

        assert estimate_snapshot_full_size(readable, use_sudo=True) == expected

        refused = [line.split()[:3] for line in sudo_refuses()]
        assert ["btrfs", "filesystem", "du"] in refused, "sudo was not asked first"
        if expected[1] == "du":
            assert ["du", "-sb", str(readable)] in refused, "sudo was not asked first"

    @pytest.mark.requires_tool("du")
    def test_the_progress_estimate_is_the_same_measurement(
        self, readable, sudo_refuses
    ):
        size, method = _expected(readable)

        assert progress.estimate_snapshot_size(str(readable)) == size
        assert progress.measure_snapshot(str(readable)) == SizeEstimate(size, method)
        assert measure_snapshot_full_size(readable) == SizeEstimate(size, method)

    def test_an_incremental_transfer_is_still_not_sized(self, readable, sudo_refuses):
        parent = readable.parent
        assert progress.estimate_snapshot_size(str(readable), str(parent)) is None
        assert progress.measure_snapshot(str(readable), str(parent)) is None
        assert sudo_refuses() == []

    @pytest.mark.requires_tool("du")
    def test_without_a_sudo_to_run_the_user_measures_directly(
        self, readable, sudo_refuses, tmp_path, monkeypatch
    ):
        expected = _expected(readable)
        only = tmp_path / "no-sudo-here"
        only.mkdir()
        for tool in ("du", "btrfs"):
            found = shutil.which(tool)
            if found:
                os.symlink(found, only / tool)
        monkeypatch.setenv("PATH", str(only))

        assert estimate_snapshot_full_size(readable, use_sudo=True) == expected
        assert sudo_refuses() == [], "sudo was asked although it is not installed"

    @pytest.mark.requires_tool("du")
    def test_root_measures_directly_and_asks_no_sudo(
        self, readable, sudo_refuses, monkeypatch
    ):
        monkeypatch.setattr(os, "geteuid", lambda: 0)

        assert estimate_snapshot_full_size(readable, use_sudo=True) == _expected(
            readable
        )
        assert sudo_refuses() == [], "root asked sudo"

    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads any directory")
    @pytest.mark.requires_tool("du")
    def test_a_tree_this_user_cannot_read_in_full_gets_no_size(
        self, readable, sudo_refuses
    ):
        """A partial count is not a size: du exits non-zero, and nothing is
        guessed; `btrfs filesystem du` exits 0 with a Total of 0, which is no
        answer either."""
        hidden = readable / "hidden"
        hidden.mkdir()
        (hidden / "c.bin").write_bytes(b"c" * 10_000)
        hidden.chmod(0o000)
        try:
            assert estimate_snapshot_full_size(readable, use_sudo=True) == (
                None,
                "failed",
            )
            assert progress.estimate_snapshot_size(str(readable)) is None
        finally:
            hidden.chmod(0o700)


class TestWhereSudoAnswers:
    @pytest.mark.requires_tool("du")
    def test_an_answer_under_sudo_is_not_measured_again(
        self, readable, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(os, "geteuid", lambda: 1000)
        expected = _expected(readable)  # measured before the counting shim is installed
        real_du = shutil.which("du")
        shim_dir = tmp_path / "counting"
        shim_dir.mkdir()
        runs = tmp_path / "du-runs"
        runs.write_text("")
        shim = shim_dir / "du"
        shim.write_text(f'#!/bin/sh\necho "$*" >> {runs}\nexec {real_du} "$@"\n')
        shim.chmod(0o755)
        monkeypatch.setenv("PATH", f"{shim_dir}{os.pathsep}{os.environ['PATH']}")

        size, method = estimate_snapshot_full_size(readable)

        assert (size, method) == expected
        if method == "du":
            assert runs.read_text().splitlines() == [f"-sb {readable}"], (
                "du was run again after it had answered"
            )
        else:
            assert runs.read_text() == "", "du was run although btrfs had answered"

    def test_a_timeout_is_not_retried_as_the_user(self, readable, monkeypatch):
        monkeypatch.setattr(os, "geteuid", lambda: 1000)
        calls: list[list[str]] = []

        def run(argv, **kwargs):
            calls.append(argv)
            if argv[-3:] == ["du", "-sb", str(readable)]:
                raise subprocess.TimeoutExpired(argv, kwargs.get("timeout"))
            return subprocess.CompletedProcess(argv, 1, "", "no")

        monkeypatch.setattr(estimate_mod.subprocess, "run", run)

        assert estimate_snapshot_full_size(readable) == (None, "failed")
        du_runs = [argv for argv in calls if "-sb" in argv]
        assert du_runs == [["sudo", "-n", "du", "-sb", str(readable)]], du_runs


class TestOnBtrfs:
    @pytest.mark.requires_tool("btrfs")
    @pytest.mark.requires_tool("du")
    def test_a_readable_tree_is_measured_by_btrfs_not_du(self, btrfs_dir, sudo_refuses):
        root = _fill(btrfs_dir / "snap")
        for file in (root / "a.bin", root / "sub" / "b.bin"):
            _write_synced(file, file.read_bytes())
        total = _filesystem_du(root)
        assert total, "the premise: the user's own btrfs filesystem du answers"

        assert estimate_snapshot_full_size(root, use_sudo=True) == (
            total,
            "filesystem_du",
        )
        assert ["du", "-sb", str(root)] not in [
            line.split()[:3] for line in sudo_refuses()
        ], "du was asked although btrfs had answered"

    @pytest.mark.requires_tool("btrfs")
    @pytest.mark.requires_tool("du")
    def test_a_tree_of_inline_files_is_measured_by_du(self, btrfs_dir, sudo_refuses):
        """`btrfs filesystem du` reports a Total of 0 for data small enough to
        live inline in metadata, with exit status 0; that is no answer."""
        root = btrfs_dir / "small"
        root.mkdir()
        for i in range(200):
            _write_synced(root / f"f{i:03d}", b"x" * 100)
        if _filesystem_du(root) != 0:
            pytest.skip("this filesystem does not inline small files")

        assert estimate_snapshot_full_size(root, use_sudo=True) == (_du(root), "du")
        assert progress.estimate_snapshot_size(str(root)) == _du(root)

    @pytest.mark.requires_tool("btrfs")
    @pytest.mark.requires_tool("du")
    def test_a_sparse_file_is_measured_below_its_apparent_size(
        self, btrfs_dir, sudo_refuses
    ):
        root = btrfs_dir / "sparse"
        root.mkdir()
        _write_synced(root / "disk.img", b"x" * 2**20, apparent_size=64 * 2**20)

        size, method = estimate_snapshot_full_size(root, use_sudo=True)

        assert method == "filesystem_du"
        assert size < _du(root), "the hole was counted"

    @pytest.mark.requires_tool("btrfs")
    @pytest.mark.requires_tool("du")
    def test_reflinked_copies_count_once_per_name(self, btrfs_dir, sudo_refuses):
        """What `btrfs filesystem du` reports is referenced data per file, so
        three reflinks of one extent count three times: the overcount the
        space check already tolerates on a measurement, pinned so a change in
        what the figure means is noticed."""
        root = btrfs_dir / "reflinks"
        root.mkdir()
        payload = os.urandom(4 * 2**20)
        _write_synced(root / "a.bin", payload)
        for name in ("b.bin", "c.bin"):
            with open(root / "a.bin", "rb") as src, open(root / name, "wb") as dst:
                fcntl.ioctl(dst.fileno(), FICLONE, src.fileno())
                os.fsync(dst.fileno())

        assert estimate_snapshot_full_size(root, use_sudo=True) == (
            3 * len(payload),
            "filesystem_du",
        )


class TestTheSpaceCheckKnowsWhatItWasGiven:
    """`_verify_destination_space` decides on the bound of the figure: a
    measurement that does not fit is refused, an upper bound that does not
    fit proves nothing and is warned about, a floor that does not fit proves
    the transfer does not fit."""

    def _check(self, measured, caplog, *, available):
        snapshot = MagicMock()
        snapshot.endpoint.measure_transfer_size.return_value = measured
        dest = MagicMock()
        dest.get_space_info.return_value = SpaceInfo(
            path="/dest", total_bytes=10**12, used_bytes=0, available_bytes=available
        )
        with caplog.at_level(logging.INFO, logger=ops.logger.name):
            ops._verify_destination_space(snapshot, dest, None, {})
        return [r.getMessage() for r in caplog.records]

    def test_a_measurement_that_does_not_fit_is_refused(self, caplog):
        with pytest.raises(__util__.InsufficientSpaceError):
            self._check(SizeEstimate(10**9, "filesystem_du"), caplog, available=10**6)

    def test_an_upper_bound_that_does_not_fit_warns_and_proceeds(self, caplog):
        messages = self._check(SizeEstimate(10**9, "du"), caplog, available=10**6)

        assert any("upper bound" in m for m in messages), messages
        assert any("may still fit" in m and "du" in m for m in messages), messages
        assert not any("Could not estimate" in m for m in messages), messages

    def test_an_upper_bound_that_fits_passes_quietly(self, caplog):
        messages = self._check(SizeEstimate(10**3, "du"), caplog, available=10**9)

        assert not any("upper bound" in m for m in messages), messages
        assert not any("Could not estimate" in m for m in messages), messages

    def test_a_floor_that_does_not_fit_is_refused(self, caplog):
        with pytest.raises(__util__.InsufficientSpaceError):
            self._check(SizeEstimate(10**9, "send_no_data"), caplog, available=10**6)

    def test_a_floor_that_fits_says_space_was_not_verified(self, caplog):
        messages = self._check(
            SizeEstimate(10**3, "send_no_data"), caplog, available=10**9
        )

        assert any("floor" in m and "not verified" in m for m in messages), messages

    def test_no_estimate_still_proceeds_with_the_warning(self, caplog):
        messages = self._check(None, caplog, available=10**6)

        assert any("Could not estimate" in m for m in messages), messages

    def test_the_report_names_the_bound(self):
        from btrfs_backup_ng.core.space import (
            check_space_availability,
            format_space_check,
        )

        info = SpaceInfo(
            path="/dest", total_bytes=10**12, used_bytes=0, available_bytes=10**6
        )
        upper = format_space_check(
            check_space_availability(info, 10**9, estimate_bound="upper")
        )
        plain = format_space_check(check_space_availability(info, 10**9))

        assert "upper bound" in upper and "INSUFFICIENT" in upper
        assert "upper bound" not in plain and "floor" not in plain


class TestTheEstimateCommand:
    @pytest.mark.requires_tool("du")
    def test_a_size_is_printed_not_unknown(self, readable, sudo_refuses, capsys):
        source = MagicMock()
        source.config = {"path": readable.parent, "ssh_sudo": False}
        snapshot = MagicMock()
        snapshot.get_name.return_value = readable.name
        snapshot.time_obj = 1
        source.list_snapshots.return_value = [snapshot]
        dest = MagicMock()
        dest.list_snapshots.return_value = []
        size, method = _expected(readable)

        estimate = estimate_transfer(source, dest)
        print_estimate(estimate)

        (row,) = estimate.snapshots
        assert (row.full_size, row.method) == (size, method)
        out = capsys.readouterr().out
        assert "unknown" not in out
        if method == "du":
            assert estimate.total_bound == "upper"
            assert "AT MOST" in out and "<= " in out
        else:
            assert estimate.total_bound == "measurement"
            assert "AT MOST" not in out

    def test_a_floor_and_a_ceiling_together_keep_the_floor(self):
        from btrfs_backup_ng.core.estimate import SnapshotEstimate, TransferEstimate

        estimate = TransferEstimate()
        estimate.add_snapshot(SnapshotEstimate("a", full_size=100, method="du"))
        assert estimate.total_bound == "upper"
        estimate.add_snapshot(
            SnapshotEstimate(
                "b",
                full_size=100,
                incremental_size=5,
                is_incremental=True,
                method="send_no_data",
            )
        )
        assert estimate.total_bound == "lower"
        assert not estimate.total_is_upper_bound


class TestTheSshEndpointsLocalMeasurement:
    @pytest.fixture
    def endpoint(self):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": "/remote/dest", "username": "u", "ssh_sudo": True}
        ep.hostname = "nas"
        return ep

    @pytest.mark.requires_tool("du")
    def test_it_is_the_shared_ladder_and_never_a_prompting_sudo(
        self, endpoint, readable, sudo_refuses, tmp_path, monkeypatch
    ):
        stand_in = shutil.which("sudo")
        guard_dir = tmp_path / "guard"
        guard_dir.mkdir()
        plain = tmp_path / "plain-sudo-calls"
        plain.write_text("")
        guard = guard_dir / "sudo"
        guard.write_text(
            "#!/bin/sh\n"
            f'case "$1" in -n) exec {stand_in} "$@" ;; esac\n'
            f'echo "$*" >> {plain}\nexit 1\n'
        )
        guard.chmod(0o755)
        monkeypatch.setenv("PATH", f"{guard_dir}{os.pathsep}{os.environ['PATH']}")
        size, _method = _expected(readable)

        assert endpoint._estimate_snapshot_size(str(readable)) == size
        assert (
            endpoint._estimate_snapshot_size(str(readable), str(readable.parent))
            is None
        )
        assert plain.read_text() == "", "a sudo that may prompt was run"

    def test_root_asks_no_sudo_for_it(
        self, endpoint, readable, sudo_refuses, monkeypatch
    ):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        calls: list[list[str]] = []

        def run(argv, **kwargs):
            calls.append(argv)
            return subprocess.CompletedProcess(argv, 0, "4096\t/p\n", "")

        monkeypatch.setattr(estimate_mod.subprocess, "run", run)

        endpoint._estimate_snapshot_size(str(readable))

        assert calls and all(argv[0] != "sudo" for argv in calls), calls


class TestOneTable:
    def test_the_three_names_parse_alike(self, monkeypatch):
        for text, expected in (
            ("1G", 2**30),
            ("512M", 512 * 2**20),
            ("1.5GiB", int(1.5 * 2**30)),
            ("1000", 1000),
            ("2KB", 2000),
        ):
            assert estimate_mod._parse_size(text) == expected, text
            assert progress._parse_size(text) == expected, text
            assert (
                SSHEndpoint._parse_size_string(SSHEndpoint.__new__(SSHEndpoint), text)
                == expected
            ), text

    def test_subvolume_show_is_never_run(self, tmp_path, monkeypatch):
        """The size ladder starts at ``btrfs filesystem du``. It used to try
        ``btrfs subvolume show`` first for an ``Exclusive`` line that
        btrfs-progs prints as ``Usage exclusive`` and only with quotas, so
        the step never matched; had it matched, the exclusive bytes are not
        what a full send carries, so it would have measured the wrong thing
        (#119). The step is gone: a show-only answer is no answer."""
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        asked: list = []

        def run(argv, **kwargs):
            asked.append(argv[:3])
            if argv[:3] == ["btrfs", "subvolume", "show"]:
                return subprocess.CompletedProcess(
                    argv, 0, "\t  Usage exclusive:\t1.00GiB\n\tExclusive: 1.00GiB\n", ""
                )
            return subprocess.CompletedProcess(argv, 1, "", "")

        monkeypatch.setattr(estimate_mod.subprocess, "run", run)

        assert estimate_snapshot_full_size(tmp_path) == (None, "failed")
        assert ["btrfs", "subvolume", "show"] not in asked
        assert asked[0] == ["btrfs", "filesystem", "du"]

    def test_a_filesystem_du_total_of_zero_is_not_a_size(self, tmp_path, monkeypatch):
        monkeypatch.setattr(os, "geteuid", lambda: 0)

        def run(argv, **kwargs):
            if argv[:3] == ["btrfs", "filesystem", "du"]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    "Total   Exclusive  Set shared  Filename\n0  0  0  /p\n",
                    "",
                )
            if argv[:2] == ["du", "-sb"]:
                return subprocess.CompletedProcess(argv, 0, "4096\t/p\n", "")
            return subprocess.CompletedProcess(argv, 1, "", "")

        monkeypatch.setattr(estimate_mod.subprocess, "run", run)

        assert progress.estimate_snapshot_size(str(tmp_path)) == 4096
        assert estimate_snapshot_full_size(tmp_path) == (4096, "du")

    def test_an_os_error_is_no_answer_not_an_exception(self, tmp_path, monkeypatch):
        """The progress copy let any OSError but ENOENT out to the caller."""
        monkeypatch.setattr(os, "geteuid", lambda: 0)

        def run(argv, **kwargs):
            raise PermissionError(13, "Permission denied", argv[0])

        monkeypatch.setattr(estimate_mod.subprocess, "run", run)

        assert progress.estimate_snapshot_size(str(tmp_path)) is None
        assert estimate_snapshot_full_size(tmp_path) == (None, "failed")
