"""Tier 2: a listing never creates the path it enumerates -- on real btrfs.

The unit tests prove the contract against tmpdirs; this proves it where it
matters, with a real subvolume in the pool: the mid-run vanish (prepare()
passed long ago, the path is gone, the next listing must refuse -- not
rebuild the mount point and report the pool empty) and the source-side
baseline (a volume never snapshotted lists empty without creating anything).
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from btrfs_backup_ng.endpoint.local import LocalEndpoint

from .conftest import (
    create_snapshot,
    delete_subvolume,
    requires_btrfs,
)


@pytest.mark.tier2
@requires_btrfs
class TestAListingNeverCreatesOnRealBtrfs:
    def test_the_mid_run_vanish_refuses_instead_of_recreating(self, btrfs_volume: Path):
        """One real backup lists while the path exists; the path vanishes;
        the re-list must raise the not-an-empty-target contract and must NOT
        recreate the directory. Before the fix the read rebuilt the path and
        returned 0 snapshots, so presence checks saw nothing and the planner
        scheduled full re-sends into the invented directory."""
        source = btrfs_volume / "source"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source)],
            check=True,
            capture_output=True,
        )
        (source / "data.txt").write_text("payload")
        dest = btrfs_volume / "mnt" / "backups"
        dest.mkdir(parents=True)
        create_snapshot(source, dest / "home.20260101-000000", readonly=True)

        endpoint = LocalEndpoint(
            config={"path": str(dest), "source": None, "snap_prefix": "home."}
        )
        try:
            names = [s.get_name() for s in endpoint.list_snapshots()]
            assert names == ["home.20260101-000000"]

            delete_subvolume(dest / "home.20260101-000000")
            shutil.rmtree(btrfs_volume / "mnt")

            with pytest.raises(RuntimeError, match="NOT an empty target"):
                endpoint.list_snapshots(flush_cache=True)
            assert not dest.exists(), "the READ recreated the directory"
        finally:
            if (dest / "home.20260101-000000").exists():
                delete_subvolume(dest / "home.20260101-000000")
            if (btrfs_volume / "mnt").exists():
                shutil.rmtree(btrfs_volume / "mnt")
            delete_subvolume(source)

    def test_a_never_snapshotted_source_lists_empty_without_creating(
        self, btrfs_volume: Path
    ):
        """The source-side baseline: a real subvolume that has never been
        snapshotted has no snapshot directory. Empty is the truth, and the
        directory's creation belongs to snapshot(), never to a read."""
        source = btrfs_volume / "source"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source)],
            check=True,
            capture_output=True,
        )
        missing = btrfs_volume / "source" / ".snapshots"
        endpoint = LocalEndpoint(
            config={
                "path": str(missing),
                "source": str(source),
                "snap_prefix": "home.",
            }
        )
        try:
            assert endpoint.list_snapshots() == []
            assert not missing.exists(), "the listing created the snapshot dir"
        finally:
            delete_subvolume(source)
