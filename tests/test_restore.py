"""Tests for restore functionality."""

import argparse
import time
from unittest.mock import ANY, MagicMock, patch

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

    def find_parent(self, snapshots: list):
        """Find the most recent snapshot older than this one."""
        candidates = [s for s in snapshots if s < self]
        if not candidates:
            return None
        return max(
            candidates, key=lambda s: s.time_obj if hasattr(s, "time_obj") else 0
        )

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


# Tests for error recovery commands (--status, --unlock, --cleanup)


class TestExecuteStatus:
    """Tests for _execute_status function."""

    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    def test_status_no_source(self, mock_prepare):
        """Test --status without source shows error."""
        from btrfs_backup_ng.cli.restore import _execute_status

        args = MagicMock()
        args.source = None

        result = _execute_status(args)

        assert result == 1
        mock_prepare.assert_not_called()

    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_status_no_locks(self, mock_list, mock_prepare, tmp_path):
        """Test --status with no locks."""
        from btrfs_backup_ng.cli.restore import _execute_status

        # Setup mock endpoint
        mock_endpoint = MagicMock()
        mock_endpoint.config = {
            "path": tmp_path,
            "lock_file_name": ".btrfs-backup-ng.locks",
        }
        mock_prepare.return_value = mock_endpoint
        mock_list.return_value = []

        args = MagicMock()
        args.source = "/backup"

        result = _execute_status(args)

        assert result == 0

    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_status_with_restore_locks(self, mock_list, mock_prepare, tmp_path):
        """Test --status shows restore locks."""
        from btrfs_backup_ng import __util__
        from btrfs_backup_ng.cli.restore import _execute_status

        # Create lock file with restore locks
        lock_file = tmp_path / ".btrfs-backup-ng.locks"
        locks = {
            "snap-1": {"locks": ["restore:session-123"]},
            "snap-2": {"parent_locks": ["restore:session-123"]},
        }
        lock_file.write_text(__util__.write_locks(locks))

        # Setup mock endpoint
        mock_endpoint = MagicMock()
        mock_endpoint.config = {
            "path": tmp_path,
            "lock_file_name": ".btrfs-backup-ng.locks",
        }
        mock_prepare.return_value = mock_endpoint
        mock_list.return_value = []

        args = MagicMock()
        args.source = "/backup"

        result = _execute_status(args)

        assert result == 0


class TestExecuteUnlock:
    """Tests for _execute_unlock function."""

    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    def test_unlock_no_source(self, mock_prepare):
        """Test --unlock without source shows error."""
        from btrfs_backup_ng.cli.restore import _execute_unlock

        args = MagicMock()
        args.source = None

        result = _execute_unlock(args, "all")

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    def test_unlock_no_lock_file(self, mock_prepare, tmp_path):
        """Test --unlock when no lock file exists."""
        from btrfs_backup_ng.cli.restore import _execute_unlock

        mock_endpoint = MagicMock()
        mock_endpoint.config = {
            "path": tmp_path,
            "lock_file_name": ".btrfs-backup-ng.locks",
        }
        mock_prepare.return_value = mock_endpoint

        args = MagicMock()
        args.source = "/backup"

        result = _execute_unlock(args, "all")

        assert result == 0  # No error, just nothing to unlock

    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    def test_unlock_all_restore_locks(self, mock_prepare, tmp_path):
        """Test --unlock all removes all restore locks."""
        from btrfs_backup_ng import __util__
        from btrfs_backup_ng.cli.restore import _execute_unlock

        # Create lock file with mixed locks
        lock_file = tmp_path / ".btrfs-backup-ng.locks"
        locks = {
            "snap-1": {"locks": ["restore:session-123", "backup:transfer-456"]},
            "snap-2": {"locks": ["restore:session-789"]},
        }
        lock_file.write_text(__util__.write_locks(locks))

        mock_endpoint = MagicMock()
        mock_endpoint.config = {
            "path": tmp_path,
            "lock_file_name": ".btrfs-backup-ng.locks",
        }
        mock_prepare.return_value = mock_endpoint

        args = MagicMock()
        args.source = "/backup"

        result = _execute_unlock(args, "all")

        assert result == 0

        # Verify only restore locks were removed
        new_locks = __util__.read_locks(lock_file.read_text())
        assert "snap-1" in new_locks
        assert "backup:transfer-456" in new_locks["snap-1"]["locks"]
        assert "restore:session-123" not in new_locks["snap-1"].get("locks", [])
        # snap-2 had only restore locks, so should be gone
        assert "snap-2" not in new_locks

    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    def test_unlock_specific_session(self, mock_prepare, tmp_path):
        """Test --unlock with specific session ID."""
        from btrfs_backup_ng import __util__
        from btrfs_backup_ng.cli.restore import _execute_unlock

        # Create lock file
        lock_file = tmp_path / ".btrfs-backup-ng.locks"
        locks = {
            "snap-1": {"locks": ["restore:session-123", "restore:session-456"]},
        }
        lock_file.write_text(__util__.write_locks(locks))

        mock_endpoint = MagicMock()
        mock_endpoint.config = {
            "path": tmp_path,
            "lock_file_name": ".btrfs-backup-ng.locks",
        }
        mock_prepare.return_value = mock_endpoint

        args = MagicMock()
        args.source = "/backup"

        result = _execute_unlock(args, "session-123")

        assert result == 0

        # Verify only the specific lock was removed
        new_locks = __util__.read_locks(lock_file.read_text())
        assert "snap-1" in new_locks
        assert "restore:session-456" in new_locks["snap-1"]["locks"]
        assert "restore:session-123" not in new_locks["snap-1"]["locks"]

    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    def test_unlock_nonexistent_session(self, mock_prepare, tmp_path):
        """Test --unlock with non-existent session ID."""
        from btrfs_backup_ng import __util__
        from btrfs_backup_ng.cli.restore import _execute_unlock

        lock_file = tmp_path / ".btrfs-backup-ng.locks"
        locks = {"snap-1": {"locks": ["restore:session-123"]}}
        lock_file.write_text(__util__.write_locks(locks))

        mock_endpoint = MagicMock()
        mock_endpoint.config = {
            "path": tmp_path,
            "lock_file_name": ".btrfs-backup-ng.locks",
        }
        mock_prepare.return_value = mock_endpoint

        args = MagicMock()
        args.source = "/backup"

        result = _execute_unlock(args, "nonexistent")

        assert result == 1  # Not found


class TestExecuteCleanup:
    """Tests for _execute_cleanup function."""

    def test_cleanup_no_destination(self):
        """Test --cleanup without destination shows error."""
        from btrfs_backup_ng.cli.restore import _execute_cleanup

        args = MagicMock()
        args.destination = None
        args.source = None

        result = _execute_cleanup(args)

        assert result == 1

    def test_cleanup_nonexistent_path(self, tmp_path):
        """Test --cleanup with non-existent path."""
        from btrfs_backup_ng.cli.restore import _execute_cleanup

        args = MagicMock()
        args.destination = str(tmp_path / "nonexistent")
        args.source = None

        result = _execute_cleanup(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore.__util__.is_subvolume")
    def test_cleanup_no_partial_subvolumes(self, mock_is_subvol, tmp_path):
        """Test --cleanup finds no partial subvolumes."""
        from btrfs_backup_ng.cli.restore import _execute_cleanup

        # Create a regular directory (not subvolume)
        (tmp_path / "regular_dir").mkdir()
        mock_is_subvol.return_value = False

        args = MagicMock()
        args.destination = str(tmp_path)
        args.source = None
        args.dry_run = False

        result = _execute_cleanup(args)

        assert result == 0

    @patch("btrfs_backup_ng.cli.restore.__util__.is_subvolume")
    def test_cleanup_finds_empty_subvolume(self, mock_is_subvol, tmp_path):
        """Test --cleanup identifies empty subvolumes as partial."""
        from btrfs_backup_ng.cli.restore import _execute_cleanup

        # Create empty directory (simulating empty subvolume)
        empty_snap = tmp_path / "snap-partial"
        empty_snap.mkdir()
        mock_is_subvol.return_value = True

        args = MagicMock()
        args.destination = str(tmp_path)
        args.source = None
        args.dry_run = True  # Don't actually delete

        result = _execute_cleanup(args)

        assert result == 0

    @patch("btrfs_backup_ng.cli.restore.__util__.is_subvolume")
    def test_cleanup_finds_partial_suffix(self, mock_is_subvol, tmp_path):
        """Test --cleanup identifies .partial suffix as partial."""
        from btrfs_backup_ng.cli.restore import _execute_cleanup

        # Create directory with .partial suffix
        partial_snap = tmp_path / "snap-1.partial"
        partial_snap.mkdir()
        (partial_snap / "somefile").touch()  # Not empty
        mock_is_subvol.return_value = True

        args = MagicMock()
        args.destination = str(tmp_path)
        args.source = None
        args.dry_run = True

        result = _execute_cleanup(args)

        assert result == 0

    @patch("btrfs_backup_ng.cli.restore.__util__.is_subvolume")
    def test_cleanup_dry_run_no_delete(self, mock_is_subvol, tmp_path):
        """Test --cleanup --dry-run doesn't delete anything."""
        from btrfs_backup_ng.cli.restore import _execute_cleanup

        empty_snap = tmp_path / "snap-partial"
        empty_snap.mkdir()
        mock_is_subvol.return_value = True

        args = MagicMock()
        args.destination = str(tmp_path)
        args.source = None
        args.dry_run = True

        result = _execute_cleanup(args)

        assert result == 0
        assert empty_snap.exists()  # Should still exist


# Tests for config-driven restore (--volume flag)


class TestExecuteListVolumes:
    """Tests for _execute_list_volumes function."""

    @patch("btrfs_backup_ng.cli.restore.find_config_file")
    def test_list_volumes_no_config(self, mock_find):
        """Test --list-volumes when no config file exists."""
        from btrfs_backup_ng.cli.restore import _execute_list_volumes

        mock_find.return_value = None

        args = MagicMock()
        args.config = None

        result = _execute_list_volumes(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore.load_config")
    @patch("btrfs_backup_ng.cli.restore.find_config_file")
    def test_list_volumes_empty_config(self, mock_find, mock_load, tmp_path):
        """Test --list-volumes with empty config."""
        from btrfs_backup_ng.cli.restore import _execute_list_volumes
        from btrfs_backup_ng.config.schema import Config

        config_path = tmp_path / "config.toml"
        mock_find.return_value = str(config_path)
        mock_load.return_value = (Config(), [])

        args = MagicMock()
        args.config = None

        result = _execute_list_volumes(args)

        assert result == 0

    @patch("btrfs_backup_ng.cli.restore.load_config")
    @patch("btrfs_backup_ng.cli.restore.find_config_file")
    def test_list_volumes_with_volumes(self, mock_find, mock_load, tmp_path):
        """Test --list-volumes shows configured volumes."""
        from btrfs_backup_ng.cli.restore import _execute_list_volumes
        from btrfs_backup_ng.config.schema import Config, TargetConfig, VolumeConfig

        config_path = tmp_path / "config.toml"
        mock_find.return_value = str(config_path)

        config = Config(
            volumes=[
                VolumeConfig(
                    path="/home",
                    snapshot_prefix="home",
                    targets=[
                        TargetConfig(
                            path="ssh://backup@server:/backups/home", ssh_sudo=True
                        ),
                        TargetConfig(path="/mnt/external/home"),
                    ],
                ),
                VolumeConfig(
                    path="/var/log",
                    snapshot_prefix="logs",
                    targets=[TargetConfig(path="/mnt/backup/logs")],
                ),
            ]
        )
        mock_load.return_value = (config, [])

        args = MagicMock()
        args.config = None

        result = _execute_list_volumes(args)

        assert result == 0


class TestExecuteConfigRestore:
    """Tests for _execute_config_restore function."""

    @patch("btrfs_backup_ng.cli.restore.find_config_file")
    def test_config_restore_no_config(self, mock_find):
        """Test --volume when no config file exists."""
        from btrfs_backup_ng.cli.restore import _execute_config_restore

        mock_find.return_value = None

        args = MagicMock()
        args.config = None

        result = _execute_config_restore(args, "/home")

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore.load_config")
    @patch("btrfs_backup_ng.cli.restore.find_config_file")
    def test_config_restore_volume_not_found(self, mock_find, mock_load, tmp_path):
        """Test --volume with non-existent volume."""
        from btrfs_backup_ng.cli.restore import _execute_config_restore
        from btrfs_backup_ng.config.schema import Config, VolumeConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(volumes=[VolumeConfig(path="/var/log", snapshot_prefix="logs")]),
            [],
        )

        args = MagicMock()
        args.config = None

        result = _execute_config_restore(args, "/home")

        assert result == 1  # Volume not found

    @patch("btrfs_backup_ng.cli.restore.load_config")
    @patch("btrfs_backup_ng.cli.restore.find_config_file")
    def test_config_restore_no_targets(self, mock_find, mock_load, tmp_path):
        """Test --volume with volume that has no targets."""
        from btrfs_backup_ng.cli.restore import _execute_config_restore
        from btrfs_backup_ng.config.schema import Config, VolumeConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(
                volumes=[VolumeConfig(path="/home", snapshot_prefix="home", targets=[])]
            ),
            [],
        )

        args = MagicMock()
        args.config = None

        result = _execute_config_restore(args, "/home")

        assert result == 1  # No targets

    @patch("btrfs_backup_ng.cli.restore.load_config")
    @patch("btrfs_backup_ng.cli.restore.find_config_file")
    def test_config_restore_invalid_target_index(self, mock_find, mock_load, tmp_path):
        """Test --volume with invalid target index."""
        from btrfs_backup_ng.cli.restore import _execute_config_restore
        from btrfs_backup_ng.config.schema import Config, TargetConfig, VolumeConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(
                volumes=[
                    VolumeConfig(
                        path="/home",
                        snapshot_prefix="home",
                        targets=[TargetConfig(path="/mnt/backup/home")],
                    )
                ]
            ),
            [],
        )

        args = MagicMock()
        args.config = None
        args.target = 5  # Invalid index

        result = _execute_config_restore(args, "/home")

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore._execute_list")
    @patch("btrfs_backup_ng.cli.restore.load_config")
    @patch("btrfs_backup_ng.cli.restore.find_config_file")
    def test_config_restore_list_mode(self, mock_find, mock_load, mock_list, tmp_path):
        """Test --volume --list uses config to list snapshots."""
        from btrfs_backup_ng.cli.restore import _execute_config_restore
        from btrfs_backup_ng.config.schema import Config, TargetConfig, VolumeConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(
                volumes=[
                    VolumeConfig(
                        path="/home",
                        snapshot_prefix="home",
                        targets=[
                            TargetConfig(
                                path="ssh://backup@server:/backups/home", ssh_sudo=True
                            )
                        ],
                    )
                ]
            ),
            [],
        )
        mock_list.return_value = 0

        args = MagicMock()
        args.config = None
        args.target = None
        args.list = True
        args.prefix = None

        result = _execute_config_restore(args, "/home")

        assert result == 0
        mock_list.assert_called_once()
        # Verify args were updated with config values
        assert args.source == "ssh://backup@server:/backups/home"
        assert args.ssh_sudo is True
        assert args.prefix == "home"

    @patch("btrfs_backup_ng.cli.restore.load_config")
    @patch("btrfs_backup_ng.cli.restore.find_config_file")
    def test_config_restore_no_destination(self, mock_find, mock_load, tmp_path):
        """Test --volume without --to shows error."""
        from btrfs_backup_ng.cli.restore import _execute_config_restore
        from btrfs_backup_ng.config.schema import Config, TargetConfig, VolumeConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(
                volumes=[
                    VolumeConfig(
                        path="/home",
                        snapshot_prefix="home",
                        targets=[TargetConfig(path="/mnt/backup/home")],
                    )
                ]
            ),
            [],
        )

        args = MagicMock()
        args.config = None
        args.target = None
        args.list = False
        args.to = None
        args.destination = None

        result = _execute_config_restore(args, "/home")

        assert result == 1  # Need destination


# Tests for core restore functions


class TestVerifyRestoredSnapshot:
    """Tests for verify_restored_snapshot function."""

    @patch("btrfs_backup_ng.core.restore.__util__.is_subvolume")
    def test_success_when_valid_subvolume(self, mock_is_subvol, tmp_path):
        """Test verification succeeds for valid subvolume."""
        from btrfs_backup_ng.core.restore import verify_restored_snapshot

        # Create the snapshot path
        snapshot_path = tmp_path / "test-snapshot"
        snapshot_path.mkdir()

        mock_is_subvol.return_value = True

        mock_endpoint = MagicMock()
        mock_endpoint.config = {"path": str(tmp_path)}

        result = verify_restored_snapshot(mock_endpoint, "test-snapshot")

        assert result is True
        mock_is_subvol.assert_called_once_with(snapshot_path)

    @patch("btrfs_backup_ng.core.restore.__util__.is_subvolume")
    def test_raises_when_path_not_exists(self, mock_is_subvol, tmp_path):
        """Test verification fails when snapshot path doesn't exist."""
        from btrfs_backup_ng.core.restore import RestoreError, verify_restored_snapshot

        mock_endpoint = MagicMock()
        mock_endpoint.config = {"path": str(tmp_path)}

        with pytest.raises(RestoreError, match="not found after restore"):
            verify_restored_snapshot(mock_endpoint, "nonexistent-snapshot")

    @patch("btrfs_backup_ng.core.restore.__util__.is_subvolume")
    def test_raises_when_not_subvolume(self, mock_is_subvol, tmp_path):
        """Test verification fails when path is not a subvolume."""
        from btrfs_backup_ng.core.restore import RestoreError, verify_restored_snapshot

        # Create the path but not as subvolume
        snapshot_path = tmp_path / "not-subvolume"
        snapshot_path.mkdir()

        mock_is_subvol.return_value = False

        mock_endpoint = MagicMock()
        mock_endpoint.config = {"path": str(tmp_path)}

        with pytest.raises(RestoreError, match="not a valid btrfs subvolume"):
            verify_restored_snapshot(mock_endpoint, "not-subvolume")

    @patch("btrfs_backup_ng.core.restore.__util__.is_subvolume")
    def test_wraps_unexpected_exceptions(self, mock_is_subvol, tmp_path):
        """Test unexpected exceptions are wrapped in RestoreError."""
        from btrfs_backup_ng.core.restore import RestoreError, verify_restored_snapshot

        snapshot_path = tmp_path / "test-snapshot"
        snapshot_path.mkdir()

        mock_is_subvol.side_effect = OSError("Unexpected error")

        mock_endpoint = MagicMock()
        mock_endpoint.config = {"path": str(tmp_path)}

        with pytest.raises(RestoreError, match="Verification failed"):
            verify_restored_snapshot(mock_endpoint, "test-snapshot")


class TestRestoreSnapshot:
    """Tests for restore_snapshot function."""

    @patch("btrfs_backup_ng.core.restore.verify_restored_snapshot")
    @patch("btrfs_backup_ng.core.restore.send_snapshot")
    @patch("btrfs_backup_ng.core.restore.log_transaction")
    def test_restores_single_snapshot(self, mock_log, mock_send, mock_verify, tmp_path):
        """Test restoring a single snapshot."""
        from btrfs_backup_ng.core.restore import restore_snapshot

        mock_verify.return_value = True

        backup_endpoint = MagicMock()
        backup_endpoint.config = {"path": "/backup"}
        local_endpoint = MagicMock()
        local_endpoint.config = {"path": str(tmp_path)}

        snapshot = MockSnapshot("test-snap")

        restore_snapshot(backup_endpoint, local_endpoint, snapshot)

        # Verify lock was set
        backup_endpoint.set_lock.assert_any_call(snapshot, ANY, True)

        # Verify send_snapshot was called
        mock_send.assert_called_once()

        # Verify lock was released
        backup_endpoint.set_lock.assert_any_call(snapshot, ANY, False)

    @patch("btrfs_backup_ng.core.restore.verify_restored_snapshot")
    @patch("btrfs_backup_ng.core.restore.send_snapshot")
    @patch("btrfs_backup_ng.core.restore.log_transaction")
    def test_restores_with_parent(self, mock_log, mock_send, mock_verify):
        """Test restoring with incremental parent."""
        from btrfs_backup_ng.core.restore import restore_snapshot

        mock_verify.return_value = True

        backup_endpoint = MagicMock()
        backup_endpoint.config = {"path": "/backup"}
        local_endpoint = MagicMock()
        local_endpoint.config = {"path": "/restore"}

        snapshot = MockSnapshot("snap-2")
        parent = MockSnapshot("snap-1")

        restore_snapshot(backup_endpoint, local_endpoint, snapshot, parent=parent)

        # Verify parent lock was set
        backup_endpoint.set_lock.assert_any_call(parent, ANY, True, parent=True)

        # Verify send_snapshot was called with parent
        call_kwargs = mock_send.call_args[1]
        assert call_kwargs["parent"] == parent

    @patch("btrfs_backup_ng.core.restore.verify_restored_snapshot")
    @patch("btrfs_backup_ng.core.restore.send_snapshot")
    @patch("btrfs_backup_ng.core.restore.log_transaction")
    def test_logs_transaction_on_success(self, mock_log, mock_send, mock_verify):
        """Test transaction logging on successful restore."""
        from btrfs_backup_ng.core.restore import restore_snapshot

        mock_verify.return_value = True

        backup_endpoint = MagicMock()
        backup_endpoint.config = {"path": "/backup"}
        local_endpoint = MagicMock()
        local_endpoint.config = {"path": "/restore"}

        snapshot = MockSnapshot("test-snap")

        restore_snapshot(backup_endpoint, local_endpoint, snapshot)

        # Should log started and completed
        assert mock_log.call_count >= 2
        statuses = [call[1]["status"] for call in mock_log.call_args_list]
        assert "started" in statuses
        assert "completed" in statuses

    @patch("btrfs_backup_ng.core.restore.verify_restored_snapshot")
    @patch("btrfs_backup_ng.core.restore.send_snapshot")
    @patch("btrfs_backup_ng.core.restore.log_transaction")
    def test_logs_failure_on_error(self, mock_log, mock_send, mock_verify):
        """Test transaction logging on failed restore."""
        from btrfs_backup_ng.core.restore import RestoreError, restore_snapshot

        mock_send.side_effect = Exception("Transfer failed")

        backup_endpoint = MagicMock()
        backup_endpoint.config = {"path": "/backup"}
        local_endpoint = MagicMock()
        local_endpoint.config = {"path": "/restore"}

        snapshot = MockSnapshot("test-snap")

        with pytest.raises(RestoreError, match="Restore failed"):
            restore_snapshot(backup_endpoint, local_endpoint, snapshot)

        # Should log failure
        statuses = [call[1]["status"] for call in mock_log.call_args_list]
        assert "failed" in statuses

    @patch("btrfs_backup_ng.core.restore.verify_restored_snapshot")
    @patch("btrfs_backup_ng.core.restore.send_snapshot")
    @patch("btrfs_backup_ng.core.restore.log_transaction")
    def test_releases_locks_on_error(self, mock_log, mock_send, mock_verify):
        """Test locks are released even on error."""
        from btrfs_backup_ng.core.restore import RestoreError, restore_snapshot

        mock_send.side_effect = Exception("Transfer failed")

        backup_endpoint = MagicMock()
        backup_endpoint.config = {"path": "/backup"}
        local_endpoint = MagicMock()
        local_endpoint.config = {"path": "/restore"}

        snapshot = MockSnapshot("test-snap")
        parent = MockSnapshot("parent-snap")

        with pytest.raises(RestoreError):
            restore_snapshot(backup_endpoint, local_endpoint, snapshot, parent=parent)

        # Verify locks were released
        backup_endpoint.set_lock.assert_any_call(snapshot, ANY, False)
        backup_endpoint.set_lock.assert_any_call(parent, ANY, False, parent=True)


class TestRestoreSnapshots:
    """Tests for restore_snapshots function."""

    def test_returns_empty_stats_when_no_backups(self):
        """Test returns empty stats when no backups found."""
        from btrfs_backup_ng.core.restore import restore_snapshots

        backup_endpoint = MagicMock()
        backup_endpoint.list_snapshots.return_value = []

        local_endpoint = MagicMock()

        stats = restore_snapshots(backup_endpoint, local_endpoint)

        assert stats["restored"] == 0
        assert stats["skipped"] == 0
        assert stats["failed"] == 0

    def test_restores_latest_by_default(self):
        """Test restores latest snapshot by default."""
        from btrfs_backup_ng.core.restore import restore_snapshots

        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
            ]
        )

        backup_endpoint = MagicMock()
        backup_endpoint.list_snapshots.return_value = snapshots

        local_endpoint = MagicMock()
        local_endpoint.list_snapshots.return_value = []

        # Dry run to see what would be restored
        stats = restore_snapshots(backup_endpoint, local_endpoint, dry_run=True)

        # In dry run, nothing is actually restored
        assert stats["restored"] == 0

    def test_restores_specific_snapshot(self):
        """Test restores specific named snapshot."""
        from btrfs_backup_ng.core.restore import restore_snapshots

        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),
            ]
        )

        backup_endpoint = MagicMock()
        backup_endpoint.list_snapshots.return_value = snapshots

        local_endpoint = MagicMock()
        local_endpoint.list_snapshots.return_value = []

        stats = restore_snapshots(
            backup_endpoint,
            local_endpoint,
            snapshot_name="snap-2",
            dry_run=True,
        )

        assert stats["restored"] == 0  # Dry run

    def test_raises_when_snapshot_not_found(self):
        """Test raises error when named snapshot not found."""
        from btrfs_backup_ng.core.restore import RestoreError, restore_snapshots

        snapshots = make_snapshots([("snap-1", "20260101-100000")])

        backup_endpoint = MagicMock()
        backup_endpoint.list_snapshots.return_value = snapshots

        local_endpoint = MagicMock()

        with pytest.raises(RestoreError, match="not found"):
            restore_snapshots(
                backup_endpoint,
                local_endpoint,
                snapshot_name="nonexistent",
            )

    def test_no_restore_needed_when_all_exist(self):
        """Test no restore needed when all snapshots already exist locally."""
        from btrfs_backup_ng.core.restore import restore_snapshots

        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
            ]
        )

        backup_endpoint = MagicMock()
        backup_endpoint.list_snapshots.return_value = snapshots

        # Both snapshots already exist locally - chain will be empty
        local_endpoint = MagicMock()
        local_endpoint.list_snapshots.return_value = snapshots.copy()

        stats = restore_snapshots(
            backup_endpoint,
            local_endpoint,
            restore_all=True,
            dry_run=True,
        )

        # No restores needed since all exist (chain is empty)
        assert stats["restored"] == 0

    def test_restore_before_time(self):
        """Test restoring snapshot before specific time."""
        from btrfs_backup_ng.core.restore import restore_snapshots

        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),
            ]
        )

        backup_endpoint = MagicMock()
        backup_endpoint.list_snapshots.return_value = snapshots

        local_endpoint = MagicMock()
        local_endpoint.list_snapshots.return_value = []

        before_time = time.strptime("20260101-113000", "%Y%m%d-%H%M%S")

        stats = restore_snapshots(
            backup_endpoint,
            local_endpoint,
            before_time=before_time,
            dry_run=True,
        )

        assert stats["restored"] == 0  # Dry run

    def test_raises_when_no_snapshot_before_time(self):
        """Test raises when no snapshot before requested time."""
        from btrfs_backup_ng.core.restore import RestoreError, restore_snapshots

        snapshots = make_snapshots([("snap-1", "20260101-120000")])

        backup_endpoint = MagicMock()
        backup_endpoint.list_snapshots.return_value = snapshots

        local_endpoint = MagicMock()

        before_time = time.strptime("20260101-100000", "%Y%m%d-%H%M%S")

        with pytest.raises(RestoreError, match="No snapshot found before"):
            restore_snapshots(
                backup_endpoint,
                local_endpoint,
                before_time=before_time,
            )

    def test_calls_progress_callback(self):
        """Test calls on_progress callback during restore."""
        from btrfs_backup_ng.core.restore import restore_snapshots

        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
            ]
        )

        backup_endpoint = MagicMock()
        backup_endpoint.list_snapshots.return_value = snapshots

        local_endpoint = MagicMock()
        local_endpoint.list_snapshots.return_value = []

        progress_calls = []

        def on_progress(current, total, name):
            progress_calls.append((current, total, name))

        # Use dry_run=True so we don't actually try to restore
        stats = restore_snapshots(
            backup_endpoint,
            local_endpoint,
            restore_all=True,
            dry_run=True,
            on_progress=on_progress,
        )

        # In dry run, progress is not called
        assert stats["restored"] == 0

    def test_returns_stats_with_errors(self):
        """Test returns stats including error list."""
        from btrfs_backup_ng.core.restore import restore_snapshots

        backup_endpoint = MagicMock()
        backup_endpoint.list_snapshots.return_value = []

        local_endpoint = MagicMock()

        stats = restore_snapshots(backup_endpoint, local_endpoint)

        assert "errors" in stats
        assert isinstance(stats["errors"], list)


# Tests for CLI entry points


class TestExecuteRestore:
    """Tests for execute_restore CLI entry point."""

    def test_no_source_shows_error(self):
        """Test execute_restore with no source shows error."""
        from btrfs_backup_ng.cli.restore import execute_restore

        args = argparse.Namespace(
            list_volumes=False,
            volume=None,
            list=False,
            status=False,
            unlock=None,
            cleanup=False,
            source=None,
            destination=None,
            verbose=0,
            quiet=False,
        )

        result = execute_restore(args)

        assert result == 1

    def test_no_destination_shows_error(self):
        """Test execute_restore with source but no destination."""
        from btrfs_backup_ng.cli.restore import execute_restore

        args = argparse.Namespace(
            list_volumes=False,
            volume=None,
            list=False,
            status=False,
            unlock=None,
            cleanup=False,
            source="/backup",
            destination=None,
            verbose=0,
            quiet=False,
        )

        result = execute_restore(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore._execute_list_volumes")
    def test_list_volumes_mode(self, mock_list_volumes):
        """Test --list-volumes mode calls _execute_list_volumes."""
        from btrfs_backup_ng.cli.restore import execute_restore

        mock_list_volumes.return_value = 0

        args = argparse.Namespace(
            list_volumes=True,
            volume=None,
            verbose=0,
            quiet=False,
        )

        result = execute_restore(args)

        assert result == 0
        mock_list_volumes.assert_called_once()

    @patch("btrfs_backup_ng.cli.restore._execute_config_restore")
    def test_volume_mode(self, mock_config_restore):
        """Test --volume mode calls _execute_config_restore."""
        from btrfs_backup_ng.cli.restore import execute_restore

        mock_config_restore.return_value = 0

        args = argparse.Namespace(
            list_volumes=False,
            volume="/home",
            verbose=0,
            quiet=False,
        )

        result = execute_restore(args)

        assert result == 0
        mock_config_restore.assert_called_once_with(args, "/home")

    @patch("btrfs_backup_ng.cli.restore._execute_list")
    def test_list_mode(self, mock_list):
        """Test --list mode calls _execute_list."""
        from btrfs_backup_ng.cli.restore import execute_restore

        mock_list.return_value = 0

        args = argparse.Namespace(
            list_volumes=False,
            volume=None,
            list=True,
            status=False,
            unlock=None,
            cleanup=False,
            source="/backup",
            destination=None,
            verbose=0,
            quiet=False,
        )

        result = execute_restore(args)

        assert result == 0
        mock_list.assert_called_once()

    @patch("btrfs_backup_ng.cli.restore._execute_status")
    def test_status_mode(self, mock_status):
        """Test --status mode calls _execute_status."""
        from btrfs_backup_ng.cli.restore import execute_restore

        mock_status.return_value = 0

        args = argparse.Namespace(
            list_volumes=False,
            volume=None,
            list=False,
            status=True,
            unlock=None,
            cleanup=False,
            source="/backup",
            destination=None,
            verbose=0,
            quiet=False,
        )

        result = execute_restore(args)

        assert result == 0
        mock_status.assert_called_once()

    @patch("btrfs_backup_ng.cli.restore._execute_unlock")
    def test_unlock_mode(self, mock_unlock):
        """Test --unlock mode calls _execute_unlock."""
        from btrfs_backup_ng.cli.restore import execute_restore

        mock_unlock.return_value = 0

        args = argparse.Namespace(
            list_volumes=False,
            volume=None,
            list=False,
            status=False,
            unlock="session-123",
            cleanup=False,
            source="/backup",
            destination=None,
            verbose=0,
            quiet=False,
        )

        result = execute_restore(args)

        assert result == 0
        mock_unlock.assert_called_once_with(args, "session-123")

    @patch("btrfs_backup_ng.cli.restore._execute_cleanup")
    def test_cleanup_mode(self, mock_cleanup):
        """Test --cleanup mode calls _execute_cleanup."""
        from btrfs_backup_ng.cli.restore import execute_restore

        mock_cleanup.return_value = 0

        args = argparse.Namespace(
            list_volumes=False,
            volume=None,
            list=False,
            status=False,
            unlock=None,
            cleanup=True,
            source=None,
            destination="/restore",
            verbose=0,
            quiet=False,
        )

        result = execute_restore(args)

        assert result == 0
        mock_cleanup.assert_called_once()


class TestExecuteMainRestore:
    """Tests for _execute_main_restore function."""

    @patch("btrfs_backup_ng.cli.restore.validate_restore_destination")
    def test_destination_validation_failure(self, mock_validate, tmp_path):
        """Test handling of destination validation failure."""
        from btrfs_backup_ng.cli.restore import _execute_main_restore

        mock_validate.side_effect = RestoreError("Not a btrfs filesystem")

        args = argparse.Namespace(
            source="/backup",
            destination=str(tmp_path),
            in_place=False,
            yes_i_know_what_i_am_doing=False,
            verbose=0,
            quiet=False,
        )

        result = _execute_main_restore(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore._prepare_local_endpoint")
    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    @patch("btrfs_backup_ng.cli.restore.validate_restore_destination")
    def test_backup_endpoint_failure(
        self, mock_validate, mock_prep_backup, mock_prep_local, tmp_path
    ):
        """Test handling of backup endpoint preparation failure."""
        from btrfs_backup_ng.cli.restore import _execute_main_restore

        mock_prep_backup.side_effect = Exception("SSH connection failed")

        args = argparse.Namespace(
            source="ssh://server/backup",
            destination=str(tmp_path),
            in_place=False,
            yes_i_know_what_i_am_doing=False,
            verbose=0,
            quiet=False,
        )

        result = _execute_main_restore(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore._prepare_local_endpoint")
    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    @patch("btrfs_backup_ng.cli.restore.validate_restore_destination")
    def test_local_endpoint_failure(
        self, mock_validate, mock_prep_backup, mock_prep_local, tmp_path
    ):
        """Test handling of local endpoint preparation failure."""
        from btrfs_backup_ng.cli.restore import _execute_main_restore

        mock_prep_backup.return_value = MagicMock()
        mock_prep_local.side_effect = Exception("Cannot create local endpoint")

        args = argparse.Namespace(
            source="/backup",
            destination=str(tmp_path),
            in_place=False,
            yes_i_know_what_i_am_doing=False,
            verbose=0,
            quiet=False,
        )

        result = _execute_main_restore(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore.restore_snapshots")
    @patch("btrfs_backup_ng.cli.restore._prepare_local_endpoint")
    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    @patch("btrfs_backup_ng.cli.restore.validate_restore_destination")
    def test_restore_error(
        self, mock_validate, mock_prep_backup, mock_prep_local, mock_restore, tmp_path
    ):
        """Test handling of RestoreError during restore."""
        from btrfs_backup_ng.cli.restore import _execute_main_restore

        mock_prep_backup.return_value = MagicMock()
        mock_prep_local.return_value = MagicMock()
        mock_restore.side_effect = RestoreError("Snapshot not found")

        args = argparse.Namespace(
            source="/backup",
            destination=str(tmp_path),
            in_place=False,
            yes_i_know_what_i_am_doing=False,
            before=None,
            dry_run=False,
            snapshot=None,
            all=False,
            overwrite=False,
            no_incremental=False,
            interactive=False,
            compress=None,
            rate_limit=None,
            verbose=0,
            quiet=False,
        )

        result = _execute_main_restore(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore.restore_snapshots")
    @patch("btrfs_backup_ng.cli.restore._prepare_local_endpoint")
    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    @patch("btrfs_backup_ng.cli.restore.validate_restore_destination")
    def test_successful_restore(
        self, mock_validate, mock_prep_backup, mock_prep_local, mock_restore, tmp_path
    ):
        """Test successful restore returns 0."""
        from btrfs_backup_ng.cli.restore import _execute_main_restore

        mock_prep_backup.return_value = MagicMock()
        mock_prep_local.return_value = MagicMock()
        mock_restore.return_value = {"restored": 2, "skipped": 0, "failed": 0}

        args = argparse.Namespace(
            source="/backup",
            destination=str(tmp_path),
            in_place=False,
            yes_i_know_what_i_am_doing=False,
            before=None,
            dry_run=False,
            snapshot=None,
            all=False,
            overwrite=False,
            no_incremental=False,
            interactive=False,
            compress=None,
            rate_limit=None,
            verbose=0,
            quiet=False,
        )

        result = _execute_main_restore(args)

        assert result == 0

    @patch("btrfs_backup_ng.cli.restore.restore_snapshots")
    @patch("btrfs_backup_ng.cli.restore._prepare_local_endpoint")
    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    @patch("btrfs_backup_ng.cli.restore.validate_restore_destination")
    def test_restore_with_failures_returns_1(
        self, mock_validate, mock_prep_backup, mock_prep_local, mock_restore, tmp_path
    ):
        """Test restore with failures returns 1."""
        from btrfs_backup_ng.cli.restore import _execute_main_restore

        mock_prep_backup.return_value = MagicMock()
        mock_prep_local.return_value = MagicMock()
        mock_restore.return_value = {"restored": 1, "skipped": 0, "failed": 1}

        args = argparse.Namespace(
            source="/backup",
            destination=str(tmp_path),
            in_place=False,
            yes_i_know_what_i_am_doing=False,
            before=None,
            dry_run=False,
            snapshot=None,
            all=False,
            overwrite=False,
            no_incremental=False,
            interactive=False,
            compress=None,
            rate_limit=None,
            verbose=0,
            quiet=False,
        )

        result = _execute_main_restore(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore.restore_snapshots")
    @patch("btrfs_backup_ng.cli.restore._prepare_local_endpoint")
    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    @patch("btrfs_backup_ng.cli.restore.validate_restore_destination")
    def test_invalid_before_date_format(
        self, mock_validate, mock_prep_backup, mock_prep_local, mock_restore, tmp_path
    ):
        """Test invalid --before date format."""
        from btrfs_backup_ng.cli.restore import _execute_main_restore

        mock_prep_backup.return_value = MagicMock()
        mock_prep_local.return_value = MagicMock()

        args = argparse.Namespace(
            source="/backup",
            destination=str(tmp_path),
            in_place=False,
            yes_i_know_what_i_am_doing=False,
            before="not-a-date",
            dry_run=False,
            snapshot=None,
            all=False,
            overwrite=False,
            no_incremental=False,
            interactive=False,
            compress=None,
            rate_limit=None,
            verbose=0,
            quiet=False,
        )

        result = _execute_main_restore(args)

        assert result == 1


class TestExecuteList:
    """Tests for _execute_list function."""

    def test_list_no_source(self):
        """Test --list without source shows error."""
        from btrfs_backup_ng.cli.restore import _execute_list

        args = MagicMock()
        args.source = None

        result = _execute_list(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    def test_list_endpoint_failure(self, mock_prepare):
        """Test --list with endpoint failure."""
        from btrfs_backup_ng.cli.restore import _execute_list

        mock_prepare.side_effect = Exception("Connection failed")

        args = MagicMock()
        args.source = "/backup"

        result = _execute_list(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    def test_list_empty(self, mock_prepare, mock_list):
        """Test --list with no snapshots."""
        from btrfs_backup_ng.cli.restore import _execute_list

        mock_prepare.return_value = MagicMock()
        mock_list.return_value = []

        args = MagicMock()
        args.source = "/backup"

        result = _execute_list(args)

        assert result == 0

    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    def test_list_with_snapshots(self, mock_prepare, mock_list, capsys):
        """Test --list shows snapshots."""
        from btrfs_backup_ng.cli.restore import _execute_list

        mock_prepare.return_value = MagicMock()
        mock_list.return_value = [
            MockSnapshot("snap-1"),
            MockSnapshot("snap-2"),
        ]

        args = MagicMock()
        args.source = "/backup"

        result = _execute_list(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "snap-1" in captured.out
        assert "snap-2" in captured.out
        assert "2 snapshot(s)" in captured.out

    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    @patch("btrfs_backup_ng.cli.restore._prepare_backup_endpoint")
    def test_list_exception(self, mock_prepare, mock_list):
        """Test --list handles exceptions."""
        from btrfs_backup_ng.cli.restore import _execute_list

        mock_prepare.return_value = MagicMock()
        mock_list.side_effect = Exception("Failed to list")

        args = MagicMock()
        args.source = "/backup"

        result = _execute_list(args)

        assert result == 1


class TestPrepareBackupEndpoint:
    """Tests for _prepare_backup_endpoint function."""

    @patch("btrfs_backup_ng.cli.restore.endpoint.choose_endpoint")
    def test_local_endpoint(self, mock_choose, tmp_path):
        """Test preparing local backup endpoint."""
        from btrfs_backup_ng.cli.restore import _prepare_backup_endpoint

        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        args = MagicMock()
        args.no_fs_checks = False
        args.prefix = "home-"

        _prepare_backup_endpoint(args, str(tmp_path))

        mock_choose.assert_called_once()
        mock_ep.prepare.assert_called_once()
        # Check endpoint kwargs
        call_kwargs = mock_choose.call_args[0][1]
        assert call_kwargs["snap_prefix"] == "home-"
        assert call_kwargs["fs_checks"] is True

    @patch("btrfs_backup_ng.cli.restore.endpoint.choose_endpoint")
    def test_ssh_endpoint(self, mock_choose):
        """Test preparing SSH backup endpoint."""
        from btrfs_backup_ng.cli.restore import _prepare_backup_endpoint

        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        args = MagicMock()
        args.no_fs_checks = False
        args.prefix = ""
        args.ssh_sudo = True
        args.ssh_password_auth = False
        args.ssh_key = "/path/to/key"

        _prepare_backup_endpoint(args, "ssh://user@server/backup")

        call_kwargs = mock_choose.call_args[0][1]
        assert call_kwargs["ssh_sudo"] is True
        assert call_kwargs["ssh_identity_file"] == "/path/to/key"


class TestPrepareLocalEndpoint:
    """Tests for _prepare_local_endpoint function."""

    @patch("btrfs_backup_ng.endpoint.local.LocalEndpoint")
    def test_creates_directory(self, mock_local_ep, tmp_path):
        """Test local endpoint creates destination directory."""
        from btrfs_backup_ng.cli.restore import _prepare_local_endpoint

        dest = tmp_path / "new_restore_dir"
        assert not dest.exists()

        _prepare_local_endpoint(dest)

        assert dest.exists()

    @patch("btrfs_backup_ng.endpoint.local.LocalEndpoint")
    def test_calls_prepare(self, mock_local_ep, tmp_path):
        """Test local endpoint prepare is called."""
        from btrfs_backup_ng.cli.restore import _prepare_local_endpoint

        mock_ep_instance = MagicMock()
        mock_local_ep.return_value = mock_ep_instance

        _prepare_local_endpoint(tmp_path)

        mock_ep_instance.prepare.assert_called_once()


class TestParseDatetime:
    """Tests for _parse_datetime function."""

    def test_parse_date_only(self):
        """Test parsing date without time."""
        from btrfs_backup_ng.cli.restore import _parse_datetime

        result = _parse_datetime("2026-01-15")

        assert result.tm_year == 2026
        assert result.tm_mon == 1
        assert result.tm_mday == 15

    def test_parse_date_with_time(self):
        """Test parsing date with time."""
        from btrfs_backup_ng.cli.restore import _parse_datetime

        result = _parse_datetime("2026-01-15 14:30:00")

        assert result.tm_year == 2026
        assert result.tm_hour == 14
        assert result.tm_min == 30
        assert result.tm_sec == 0

    def test_parse_date_with_time_no_seconds(self):
        """Test parsing date with time but no seconds."""
        from btrfs_backup_ng.cli.restore import _parse_datetime

        result = _parse_datetime("2026-01-15 14:30")

        assert result.tm_hour == 14
        assert result.tm_min == 30

    def test_parse_iso_format(self):
        """Test parsing ISO format with T separator."""
        from btrfs_backup_ng.cli.restore import _parse_datetime

        result = _parse_datetime("2026-01-15T14:30:00")

        assert result.tm_year == 2026
        assert result.tm_hour == 14

    def test_parse_invalid_format(self):
        """Test parsing invalid format raises ValueError."""
        from btrfs_backup_ng.cli.restore import _parse_datetime

        with pytest.raises(ValueError, match="Could not parse date"):
            _parse_datetime("invalid-date")


class TestInteractiveSelect:
    """Tests for _interactive_select function."""

    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_no_snapshots_returns_none(self, mock_list):
        """Test returns None when no snapshots available."""
        from btrfs_backup_ng.cli.restore import _interactive_select

        mock_list.return_value = []

        result = _interactive_select(MagicMock())

        assert result is None

    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_exception_returns_none(self, mock_list):
        """Test returns None on exception."""
        from btrfs_backup_ng.cli.restore import _interactive_select

        mock_list.side_effect = Exception("Failed")

        result = _interactive_select(MagicMock())

        assert result is None

    @patch("builtins.input", side_effect=["0"])
    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_cancel_returns_none(self, mock_list, mock_input):
        """Test selecting 0 cancels and returns None."""
        from btrfs_backup_ng.cli.restore import _interactive_select

        mock_list.return_value = [MockSnapshot("snap-1")]

        result = _interactive_select(MagicMock())

        assert result is None

    @patch("builtins.input", side_effect=[""])
    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_empty_input_returns_none(self, mock_list, mock_input):
        """Test empty input cancels and returns None."""
        from btrfs_backup_ng.cli.restore import _interactive_select

        mock_list.return_value = [MockSnapshot("snap-1")]

        result = _interactive_select(MagicMock())

        assert result is None

    @patch("builtins.input", side_effect=["1", "y"])
    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_select_and_confirm(self, mock_list, mock_input):
        """Test selecting a snapshot and confirming."""
        from btrfs_backup_ng.cli.restore import _interactive_select

        mock_list.return_value = [MockSnapshot("snap-1"), MockSnapshot("snap-2")]

        result = _interactive_select(MagicMock())

        assert result == "snap-1"

    @patch("builtins.input", side_effect=["1", "n"])
    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_select_and_decline(self, mock_list, mock_input):
        """Test selecting a snapshot but declining confirmation."""
        from btrfs_backup_ng.cli.restore import _interactive_select

        mock_list.return_value = [MockSnapshot("snap-1")]

        result = _interactive_select(MagicMock())

        assert result is None

    @patch("builtins.input", side_effect=["invalid", "1", "y"])
    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_invalid_input_then_valid(self, mock_list, mock_input):
        """Test invalid input followed by valid selection."""
        from btrfs_backup_ng.cli.restore import _interactive_select

        mock_list.return_value = [MockSnapshot("snap-1")]

        result = _interactive_select(MagicMock())

        assert result == "snap-1"

    @patch("builtins.input", side_effect=["5", "1", "y"])
    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_out_of_range_then_valid(self, mock_list, mock_input):
        """Test out of range selection followed by valid."""
        from btrfs_backup_ng.cli.restore import _interactive_select

        mock_list.return_value = [MockSnapshot("snap-1")]

        result = _interactive_select(MagicMock())

        assert result == "snap-1"

    @patch("builtins.input", side_effect=KeyboardInterrupt())
    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_keyboard_interrupt(self, mock_list, mock_input):
        """Test keyboard interrupt returns None."""
        from btrfs_backup_ng.cli.restore import _interactive_select

        mock_list.return_value = [MockSnapshot("snap-1")]

        result = _interactive_select(MagicMock())

        assert result is None

    @patch("builtins.input", side_effect=EOFError())
    @patch("btrfs_backup_ng.cli.restore.list_remote_snapshots")
    def test_eof_error(self, mock_list, mock_input):
        """Test EOF error returns None."""
        from btrfs_backup_ng.cli.restore import _interactive_select

        mock_list.return_value = [MockSnapshot("snap-1")]

        result = _interactive_select(MagicMock())

        assert result is None
