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
        mock_load.return_value = Config()

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
        mock_load.return_value = config

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
        mock_load.return_value = Config(
            volumes=[VolumeConfig(path="/var/log", snapshot_prefix="logs")]
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
        mock_load.return_value = Config(
            volumes=[VolumeConfig(path="/home", snapshot_prefix="home", targets=[])]
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
        mock_load.return_value = Config(
            volumes=[
                VolumeConfig(
                    path="/home",
                    snapshot_prefix="home",
                    targets=[TargetConfig(path="/mnt/backup/home")],
                )
            ]
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
        mock_load.return_value = Config(
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
        mock_load.return_value = Config(
            volumes=[
                VolumeConfig(
                    path="/home",
                    snapshot_prefix="home",
                    targets=[TargetConfig(path="/mnt/backup/home")],
                )
            ]
        )

        args = MagicMock()
        args.config = None
        args.target = None
        args.list = False
        args.to = None
        args.destination = None

        result = _execute_config_restore(args, "/home")

        assert result == 1  # Need destination
