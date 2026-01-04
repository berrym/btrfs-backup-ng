"""Tests for restore functionality."""

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core.restore import (
    RestoreError,
    _find_older_parent,
    check_snapshot_collision,
    find_snapshot_before_time,
    find_snapshot_by_name,
    get_restore_chain,
    list_remote_snapshots,
    validate_restore_destination,
)


class MockSnapshot:
    """Mock Snapshot object for testing."""

    def __init__(self, name: str, time_obj=None):
        self._name = name
        self.time_obj = time_obj or time.strptime("20260101-120000", "%Y%m%d-%H%M%S")

    def get_name(self) -> str:
        return self._name

    def __lt__(self, other):
        """Compare by time for sorting."""
        if self.time_obj and other.time_obj:
            return self.time_obj < other.time_obj
        return False

    def __eq__(self, other):
        if isinstance(other, MockSnapshot):
            return self._name == other._name
        return False

    def __hash__(self):
        return hash(self._name)

    def __repr__(self):
        return f"MockSnapshot({self._name!r})"


def make_snapshots(names_and_times: list) -> list:
    """Create list of MockSnapshots from names and time strings.

    Args:
        names_and_times: List of (name, time_str) tuples.
            time_str format: YYYYMMDD-HHMMSS

    Returns:
        List of MockSnapshot objects sorted by time.
    """
    snapshots = []
    for name, time_str in names_and_times:
        t = time.strptime(time_str, "%Y%m%d-%H%M%S")
        snapshots.append(MockSnapshot(name, t))
    return sorted(snapshots, key=lambda s: s.time_obj)


class TestFindOlderParent:
    """Tests for _find_older_parent function."""

    def test_finds_most_recent_older(self):
        """Test that the most recent older snapshot is returned."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),
                ("snap-4", "20260101-130000"),
            ]
        )

        result = _find_older_parent(snapshots[3], snapshots)  # snap-4
        assert result is not None
        assert result.get_name() == "snap-3"

    def test_returns_none_for_oldest(self):
        """Test that None is returned for oldest snapshot."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
            ]
        )

        result = _find_older_parent(snapshots[0], snapshots)  # snap-1
        assert result is None

    def test_skips_same_and_newer(self):
        """Test that same and newer snapshots are skipped."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
            ]
        )

        # Find parent of snap-2, only snap-1 is older
        result = _find_older_parent(snapshots[1], snapshots)
        assert result is not None
        assert result.get_name() == "snap-1"


class TestGetRestoreChain:
    """Tests for get_restore_chain function."""

    def test_single_snapshot_no_existing(self):
        """Test chain for single snapshot with no local copies."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
            ]
        )

        chain = get_restore_chain(snapshots[0], snapshots, [])

        assert len(chain) == 1
        assert chain[0].get_name() == "snap-1"

    def test_builds_full_chain(self):
        """Test building a complete parent chain."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),
                ("snap-4", "20260101-130000"),
            ]
        )

        chain = get_restore_chain(snapshots[3], snapshots, [])  # Want snap-4

        # Should include all 4 snapshots, oldest first
        assert len(chain) == 4
        assert [s.get_name() for s in chain] == ["snap-1", "snap-2", "snap-3", "snap-4"]

    def test_stops_at_existing_local(self):
        """Test chain stops when local snapshot exists."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),
                ("snap-4", "20260101-130000"),
            ]
        )

        # snap-2 already exists locally
        existing = [snapshots[1]]

        chain = get_restore_chain(snapshots[3], snapshots, existing)

        # Should only need snap-3 and snap-4 (snap-2 can be parent)
        assert len(chain) == 2
        assert [s.get_name() for s in chain] == ["snap-3", "snap-4"]

    def test_empty_chain_if_target_exists(self):
        """Test empty chain if target already exists locally."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
            ]
        )

        # Target already exists
        chain = get_restore_chain(snapshots[0], snapshots, [snapshots[0]])

        assert len(chain) == 0


class TestFindSnapshotByName:
    """Tests for find_snapshot_by_name function."""

    def test_finds_existing(self):
        """Test finding an existing snapshot."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
            ]
        )

        result = find_snapshot_by_name("snap-2", snapshots)
        assert result is not None
        assert result.get_name() == "snap-2"

    def test_returns_none_for_missing(self):
        """Test None is returned for non-existent name."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
            ]
        )

        result = find_snapshot_by_name("snap-nonexistent", snapshots)
        assert result is None

    def test_empty_list(self):
        """Test with empty snapshot list."""
        result = find_snapshot_by_name("snap-1", [])
        assert result is None


class TestFindSnapshotBeforeTime:
    """Tests for find_snapshot_before_time function."""

    def test_finds_most_recent_before(self):
        """Test finding snapshot before a given time."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),
            ]
        )

        target_time = time.strptime("20260101-113000", "%Y%m%d-%H%M%S")
        result = find_snapshot_before_time(target_time, snapshots)

        assert result is not None
        assert result.get_name() == "snap-2"

    def test_returns_none_if_all_after(self):
        """Test None returned if all snapshots are after target time."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
            ]
        )

        target_time = time.strptime("20260101-090000", "%Y%m%d-%H%M%S")
        result = find_snapshot_before_time(target_time, snapshots)

        assert result is None

    def test_includes_exact_match(self):
        """Test that exact time match is included."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
            ]
        )

        target_time = time.strptime("20260101-100000", "%Y%m%d-%H%M%S")
        result = find_snapshot_before_time(target_time, snapshots)

        assert result is not None
        assert result.get_name() == "snap-1"


class TestValidateRestoreDestination:
    """Tests for validate_restore_destination function."""

    @patch("btrfs_backup_ng.core.restore.__util__.is_btrfs")
    def test_validates_existing_btrfs(self, mock_is_btrfs, tmp_path):
        """Test validation of existing btrfs directory."""
        mock_is_btrfs.return_value = True

        # Should not raise
        validate_restore_destination(tmp_path)
        mock_is_btrfs.assert_called_once()

    @patch("btrfs_backup_ng.core.restore.__util__.is_btrfs")
    def test_creates_missing_directory(self, mock_is_btrfs, tmp_path):
        """Test that missing directory is created."""
        mock_is_btrfs.return_value = True
        new_path = tmp_path / "new_dir"

        validate_restore_destination(new_path)

        assert new_path.exists()

    @patch("btrfs_backup_ng.core.restore.__util__.is_btrfs")
    def test_rejects_non_btrfs(self, mock_is_btrfs, tmp_path):
        """Test rejection of non-btrfs filesystem."""
        mock_is_btrfs.return_value = False

        with pytest.raises(RestoreError, match="not on a btrfs filesystem"):
            validate_restore_destination(tmp_path)

    @patch("btrfs_backup_ng.core.restore.__util__.is_btrfs")
    def test_in_place_requires_force(self, mock_is_btrfs, tmp_path):
        """Test that in-place restore requires force flag."""
        mock_is_btrfs.return_value = True

        with pytest.raises(RestoreError, match="dangerous"):
            validate_restore_destination(tmp_path, in_place=True, force=False)

    @patch("btrfs_backup_ng.core.restore.__util__.is_btrfs")
    def test_in_place_with_force(self, mock_is_btrfs, tmp_path):
        """Test in-place restore allowed with force flag."""
        mock_is_btrfs.return_value = True

        # Should not raise
        validate_restore_destination(tmp_path, in_place=True, force=True)


class TestCheckSnapshotCollision:
    """Tests for check_snapshot_collision function."""

    def test_detects_collision(self):
        """Test detection of existing snapshot."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [
            MockSnapshot("snap-1"),
            MockSnapshot("snap-2"),
        ]

        result = check_snapshot_collision("snap-1", mock_endpoint)
        assert result is True

    def test_no_collision(self):
        """Test no collision when snapshot doesn't exist."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [
            MockSnapshot("snap-1"),
        ]

        result = check_snapshot_collision("snap-nonexistent", mock_endpoint)
        assert result is False

    def test_handles_error(self):
        """Test graceful handling of errors."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.side_effect = Exception("Network error")

        result = check_snapshot_collision("snap-1", mock_endpoint)
        assert result is False  # Default to no collision on error


class TestListRemoteSnapshots:
    """Tests for list_remote_snapshots function."""

    def test_lists_all(self):
        """Test listing all snapshots."""
        mock_endpoint = MagicMock()
        snapshots = [MockSnapshot("snap-1"), MockSnapshot("snap-2")]
        mock_endpoint.list_snapshots.return_value = snapshots

        result = list_remote_snapshots(mock_endpoint)

        assert len(result) == 2
        mock_endpoint.list_snapshots.assert_called_once()

    def test_filters_by_prefix(self):
        """Test filtering by prefix."""
        mock_endpoint = MagicMock()
        snapshots = [
            MockSnapshot("home-1"),
            MockSnapshot("home-2"),
            MockSnapshot("root-1"),
        ]
        mock_endpoint.list_snapshots.return_value = snapshots

        result = list_remote_snapshots(mock_endpoint, prefix_filter="home-")

        assert len(result) == 2
        assert all(s.get_name().startswith("home-") for s in result)


class TestRestoreChainEdgeCases:
    """Edge case tests for restore chain building."""

    def test_handles_single_snapshot(self):
        """Test chain with just one snapshot available."""
        snap = MockSnapshot("snap-1", time.strptime("20260101-100000", "%Y%m%d-%H%M%S"))

        chain = get_restore_chain(snap, [snap], [])

        assert len(chain) == 1
        assert chain[0].get_name() == "snap-1"

    def test_handles_gaps_in_chain(self):
        """Test chain building with time gaps."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),  # Day 1
                ("snap-3", "20260103-100000"),  # Day 3 (gap)
                ("snap-5", "20260105-100000"),  # Day 5 (gap)
            ]
        )

        chain = get_restore_chain(snapshots[2], snapshots, [])

        # Should still build complete chain
        assert len(chain) == 3
        assert [s.get_name() for s in chain] == ["snap-1", "snap-3", "snap-5"]

    def test_preserves_order_oldest_first(self):
        """Test that chain is always oldest-first."""
        snapshots = make_snapshots(
            [
                ("snap-c", "20260103-100000"),
                ("snap-a", "20260101-100000"),
                ("snap-b", "20260102-100000"),
            ]
        )
        # Sort them properly first
        snapshots.sort(key=lambda s: s.time_obj)

        chain = get_restore_chain(snapshots[-1], snapshots, [])

        # Verify oldest first order
        times = [s.time_obj for s in chain]
        assert times == sorted(times)
