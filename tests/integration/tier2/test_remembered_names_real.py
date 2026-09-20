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
