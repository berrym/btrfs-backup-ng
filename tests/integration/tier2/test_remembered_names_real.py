"""Tier 2: remembered snapshot names against real btrfs.

strptime accepts single-digit fields the format would zero-pad, so
'home.2026-9-8_020304' parses under %Y-%m-%d_%H%M%S but re-renders as
'home.2026-09-08_020304'. On a regenerating get_name() such a snapshot was
listed while get_path() -- the path prune DELETES by -- pointed at a name not
on disk. These tests pin, on a real filesystem, that the listing returns the
observed names and that a delete removes exactly the entry it named.
"""

import subprocess
from pathlib import Path

import pytest

from btrfs_backup_ng.endpoint.local import LocalEndpoint

from .conftest import (
    create_snapshot,
    delete_subvolume,
    requires_btrfs,
)

FMT = "%Y-%m-%d_%H%M%S"
OBSERVED = "home.2026-9-8_020304"  # parses under FMT, does NOT round-trip
CANONICAL = "home.2026-09-08_020305"
ORDINAL = "home.2026-09-08_020306_1"  # dated by stripping one trailing _N
WEIRD = "home.imported-base"  # a subvolume with no derivable timestamp


@pytest.mark.tier2
@requires_btrfs
class TestRememberedNamesOnRealBtrfs:
    def test_listing_and_delete_resolve_the_observed_entry(self, btrfs_volume: Path):
        """4 facts, in order: both subvolumes list under their on-disk names;
        every listed path exists; deleting the non-round-tripping one removes
        EXACTLY that subvolume; its sibling survives. A regenerating name
        fails at the second assert (phantom path) and could otherwise aim the
        delete at the wrong entry."""
        source = btrfs_volume / "source"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source)],
            check=True,
            capture_output=True,
        )
        (source / "data.txt").write_text("payload")
        snap_dir = btrfs_volume / "snapshots"
        snap_dir.mkdir()
        create_snapshot(source, snap_dir / OBSERVED, readonly=True)
        create_snapshot(source, snap_dir / CANONICAL, readonly=True)

        endpoint = LocalEndpoint(
            config={
                "source": str(source),
                "path": str(snap_dir),
                "snap_prefix": "home.",
                "timestamp_format": FMT,
            }
        )
        try:
            snapshots = endpoint.list_snapshots()
            assert {s.get_name() for s in snapshots} == {OBSERVED, CANONICAL}
            for snap in snapshots:
                assert snap.get_path().exists(), (
                    f"{snap.get_name()!r} resolves to {snap.get_path()!r}, "
                    "which is not on disk"
                )

            target = next(s for s in snapshots if s.get_name() == OBSERVED)
            result = endpoint.delete_snapshots([target])

            assert result.deleted_count == 1
            assert not (snap_dir / OBSERVED).exists(), "the named entry survived"
            assert (snap_dir / CANONICAL).exists(), "the delete took the sibling"
        finally:
            for name in (OBSERVED, CANONICAL):
                if (snap_dir / name).exists():
                    delete_subvolume(snap_dir / name)
            delete_subvolume(source)

    def test_a_foreign_pool_lists_fully_and_deletes_exactly(self, btrfs_volume: Path):
        """The Phase C acceptance bar on real btrfs, where is_subvolume() runs
        for real: a pool of 4 subvolumes -- canonical, non-round-tripping,
        _N-suffixed, and one with no derivable timestamp -- lists as 4, while
        a README file and a plain lost+found directory are excluded by the
        inode check, not by name parsing. Deleting the _N snapshot removes
        exactly that subvolume. Before this release the same pool listed as 2
        (canonical + non-round-tripping) and the other two were invisible to
        every listing and every prune."""
        source = btrfs_volume / "source"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source)],
            check=True,
            capture_output=True,
        )
        (source / "data.txt").write_text("payload")
        snap_dir = btrfs_volume / "snapshots"
        snap_dir.mkdir()
        for name in (CANONICAL, OBSERVED, ORDINAL, WEIRD):
            create_snapshot(source, snap_dir / name, readonly=True)
        (snap_dir / "README.md").write_text("not a snapshot")
        (snap_dir / "lost+found").mkdir()  # a plain directory, not a subvolume

        endpoint = LocalEndpoint(
            config={
                "source": str(source),
                "path": str(snap_dir),
                "snap_prefix": "home.",
                "timestamp_format": FMT,
            }
        )
        try:
            snaps = {s.get_name(): s for s in endpoint.list_snapshots()}
            assert set(snaps) == {CANONICAL, OBSERVED, ORDINAL, WEIRD}
            assert snaps[ORDINAL].time_obj is not None
            assert snaps[WEIRD].time_obj is None
            for snap in snaps.values():
                assert snap.get_path().exists()

            result = endpoint.delete_snapshots([snaps[ORDINAL]])

            assert result.deleted_count == 1
            assert not (snap_dir / ORDINAL).exists()
            for name in (CANONICAL, OBSERVED, WEIRD):
                assert (snap_dir / name).exists(), f"the delete took {name}"
        finally:
            for name in (CANONICAL, OBSERVED, ORDINAL, WEIRD):
                if (snap_dir / name).exists():
                    delete_subvolume(snap_dir / name)
            delete_subvolume(source)


@pytest.mark.tier2
@requires_btrfs
class TestCollisionSuffixOnRealBtrfs:
    def test_same_period_snapshots_become_base_and_suffixes(self, btrfs_volume: Path):
        """The Phase D proof the unit tests cannot give: THE REAL
        endpoint.snapshot() -- real btrfs subvolume snapshot commands, real
        filesystem -- called three times under a daily timestamp_format
        produces X, X_1 and X_2 as three DISTINCT subvolumes, each returned
        name matching its on-disk entry. Before this change the second call
        of the day was refused."""
        source = btrfs_volume / "source"
        subprocess.run(
            ["btrfs", "subvolume", "create", str(source)],
            check=True,
            capture_output=True,
        )
        (source / "data.txt").write_text("payload")
        snap_dir = btrfs_volume / "snapshots"
        snap_dir.mkdir()

        endpoint = LocalEndpoint(
            config={
                "source": str(source),
                "path": str(snap_dir),
                "snapshot_folder": str(snap_dir),
                "snap_prefix": "home.",
                "timestamp_format": "%Y%m%d",
            }
        )
        created = []
        try:
            for _ in range(3):
                snap = endpoint.snapshot()
                created.append(snap.get_name())
                on_disk = snap_dir / snap.get_name()
                assert on_disk.is_dir(), f"{snap.get_name()} returned but not on disk"
                assert on_disk.stat().st_ino == 256, (
                    f"{snap.get_name()} is not a subvolume"
                )

            assert len(set(created)) == 3, f"names not distinct: {created}"
            base = created[0]
            assert created[1] == f"{base}_1"
            assert created[2] == f"{base}_2"
        finally:
            for name in created:
                if (snap_dir / name).exists():
                    delete_subvolume(snap_dir / name)
            delete_subvolume(source)
