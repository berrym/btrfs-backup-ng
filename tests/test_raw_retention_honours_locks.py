"""A retention lock must not be paid for out of the operator's keep budget.

`Endpoint.delete_old_snapshots` filters locked snapshots out BEFORE applying
`keep`, so `-N 2` means two usable backups. The raw override sliced the full
listing instead, so a stream locked by a running restore occupied one of the
kept slots and the operator was left with one usable backup out of two.
"""

import pytest

from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

NAMES = [f"s.2024010{i}T120000" for i in range(1, 6)]


def _rig(tmp_path, locked=None):
    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    snapshots = []
    for name in NAMES:
        snap = RawSnapshot(name=name, stream_path=tmp_path / f"{name}.btrfs")
        snap.stream_path.write_bytes(b"stream")
        if name == locked:
            snap.locks.add("restore")
        snapshots.append(snap)
    endpoint._cached_snapshots = snapshots
    return endpoint


def _surviving(tmp_path):
    return sorted(p.name[: -len(".btrfs")] for p in tmp_path.glob("*.btrfs"))


@pytest.mark.parametrize("locked", [None, NAMES[-1], NAMES[0], NAMES[2]])
def test_keep_counts_usable_backups_not_total_streams(tmp_path, locked):
    """Whatever is locked, -N 2 leaves two backups the operator can restore."""
    endpoint = _rig(tmp_path, locked=locked)

    endpoint.delete_old_snapshots(keep=2)

    survivors = _surviving(tmp_path)
    usable = [n for n in survivors if n != locked]
    assert len(usable) == 2, (
        f"locked={locked}: kept {survivors}, of which only {len(usable)} are usable"
    )


def test_a_locked_stream_is_never_deleted(tmp_path):
    """The lock still protects the stream itself, it is just extra."""
    endpoint = _rig(tmp_path, locked=NAMES[0])

    endpoint.delete_old_snapshots(keep=2)

    assert NAMES[0] in _surviving(tmp_path)


def test_the_newest_unlocked_streams_are_the_ones_kept(tmp_path):
    """Keeping two usable means the two newest unlocked, not any two."""
    endpoint = _rig(tmp_path, locked=NAMES[-1])

    endpoint.delete_old_snapshots(keep=2)

    assert _surviving(tmp_path) == sorted([NAMES[2], NAMES[3], NAMES[4]])
