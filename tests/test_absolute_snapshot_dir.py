"""An absolute snapshot_dir was created wherever it pointed.

`snapshot_dir` is documented as "relative to volume or absolute". The absolute
form is how an operator moves snapshots OFF the root filesystem onto a bigger
disk. The whole tree was built with `parents=True`, so when that disk was not
mounted the directory was created on the root filesystem -- and the snapshots
then SUCCEEDED there, because a btrfs snapshot only needs to share a filesystem
with its source, which on a btrfs root it does.

That is the sharpest form of the defect: the operator moved snapshots away from
root precisely to stop filling it, and an unmounted disk silently put them back,
succeeding all the way. If the source had been on a different filesystem btrfs
would have refused, so the dangerous configuration is the ordinary one.

The configured base must now exist. The per-source directory BELOW it is still
created, so first use on a mounted disk works unchanged, and a relative
snapshot_dir is untouched -- it resolves under the source, which the endpoint
has already established is there.
"""

from __future__ import annotations


import pytest

from btrfs_backup_ng.__util__ import AbortError
from btrfs_backup_ng.cli.common import resolve_snapshot_dir


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "source"
    path.mkdir()
    return path


class TestRelativeIsUnchanged:
    def test_the_default_resolves_under_the_source(self, source):
        assert resolve_snapshot_dir(".snapshots", source) == source / ".snapshots"

    def test_a_nested_relative_dir_resolves_under_the_source(self, source):
        result = resolve_snapshot_dir(".btrfs-backup-ng/snapshots", source)
        assert result == source / ".btrfs-backup-ng" / "snapshots"

    def test_it_does_not_need_to_exist_yet(self, source):
        """First run: the caller creates it, because the source is known to be there."""
        result = resolve_snapshot_dir(".snapshots", source)
        assert not result.exists()


class TestAbsoluteRequiresItsBase:
    def test_an_existing_base_gets_a_per_source_directory(self, tmp_path, source):
        base = tmp_path / "big"
        base.mkdir()

        assert resolve_snapshot_dir(str(base), source) == base / source.name

    def test_the_per_source_directory_need_not_exist_yet(self, tmp_path, source):
        """First use on a mounted disk still works."""
        base = tmp_path / "big"
        base.mkdir()

        result = resolve_snapshot_dir(str(base), source)
        assert not result.exists()

    def test_a_missing_base_is_refused(self, tmp_path, source):
        missing = tmp_path / "mnt" / "big"

        with pytest.raises(AbortError) as excinfo:
            resolve_snapshot_dir(str(missing), source)

        assert str(missing) in str(excinfo.value)
        assert "mounted" in str(excinfo.value).lower()

    def test_a_missing_base_is_not_created(self, tmp_path, source):
        missing = tmp_path / "mnt" / "big"

        with pytest.raises(AbortError):
            resolve_snapshot_dir(str(missing), source)

        assert not missing.exists()
        assert not (tmp_path / "mnt").exists(), "refused, then built its parents"

    def test_a_file_where_the_base_should_be_is_refused(self, tmp_path, source):
        not_a_dir = tmp_path / "big"
        not_a_dir.write_text("not a directory")

        with pytest.raises(AbortError):
            resolve_snapshot_dir(str(not_a_dir), source)


class TestEveryCallSiteUsesIt:
    """Seven commands resolve a volume's snapshot directory; every one must
    resolve it through the shared helper. Five carried byte-identical inline
    copies, and the sixth divergence was not cosmetic: estimate joined by
    hand (`source / configured`), and pathlib resolves that to the absolute
    RIGHT operand, so with an absolute snapshot_dir the one command whose job
    is to predict a transfer enumerated the BASE while the transfer reads
    <base>/<source name>. Presence-scanned here (helper referenced, hand
    branch absent) and pinned behaviourally below."""

    @pytest.mark.parametrize(
        "module",
        ["run", "snapshot", "transfer", "prune", "list_cmd", "status", "estimate"],
    )
    def test_the_cli_resolves_through_the_helper(self, module):
        import importlib
        import inspect

        source = inspect.getsource(
            importlib.import_module(f"btrfs_backup_ng.cli.{module}")
        )
        assert "resolve_snapshot_dir(" in source, (
            f"cli/{module}.py must resolve snapshot_dir through the shared helper"
        )
        assert "snapshot_dir.is_absolute()" not in source, (
            f"cli/{module}.py still branches on absoluteness itself"
        )


class TestEstimateReadsWhatTransferReads:
    def test_estimate_counts_the_snapshots_the_listing_sees(self, tmp_path):
        """The reproduce, pinned: absolute snapshot_dir, two snapshots on
        disk at <base>/<source name>. Before the fix estimate enumerated
        <base> and reported snapshot_count 0 / 0 bytes for a config whose
        `list` showed both snapshots. Mutation guards: reverting the hand
        join, or the helper dropping the per-source component, both put the
        count back to 0."""
        import json
        import subprocess
        import sys

        vol = tmp_path / "vol"
        vol.mkdir()
        store = tmp_path / "store"
        (store / "vol").mkdir(parents=True)
        (store / "vol" / "vol-20260101-000000").mkdir()
        (store / "vol" / "vol-20260102-000000").mkdir()
        target = tmp_path / "target"
        target.mkdir()
        cfg = tmp_path / "cfg.toml"
        cfg.write_text(
            f"""
[[volumes]]
path = "{vol}"
snapshot_dir = "{store}"
snapshot_prefix = "vol-"

[[volumes.targets]]
path = "{target}"
"""
        )

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "btrfs_backup_ng",
                "-c",
                str(cfg),
                "estimate",
                "--volume",
                str(vol),
                "--json",
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr[-500:]
        # Rich log lines share stdout with the report; the JSON block is the
        # last thing printed.
        report = json.loads(result.stdout[result.stdout.rindex("\n{") + 1 :])
        assert report["source"] == str(store / "vol"), (
            "estimate reads a different directory than the transfer will"
        )
        assert report["snapshot_count"] == 2, (
            f"estimate saw {report['snapshot_count']} snapshots where the "
            f"listing sees 2: {report}"
        )
