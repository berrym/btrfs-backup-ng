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


class TestBothCallSitesUseIt:
    """run and snapshot both created the directory; neither may do it unguarded."""

    @pytest.mark.parametrize("module", ["run", "snapshot"])
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
