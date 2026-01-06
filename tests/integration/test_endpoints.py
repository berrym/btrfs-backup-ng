"""Integration tests for endpoint operations.

Tests the complete flow of endpoint operations with mocked subprocess calls.
"""

from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.local import LocalEndpoint


class TestLocalEndpointOperations:
    """Test LocalEndpoint with mocked subprocess calls."""

    def test_endpoint_initialization(self, tmp_path):
        """Test endpoint initializes with correct paths."""
        source = tmp_path / "source"
        dest = tmp_path / "dest"
        source.mkdir()
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "source": str(source),
                "path": str(dest),
                "snap_prefix": "test-",
            }
        )

        assert endpoint.config["source"] == source
        assert endpoint.config["path"] == dest
        assert endpoint.config["snap_prefix"] == "test-"

    def test_endpoint_prepare_creates_directories(self, tmp_path):
        """Test endpoint prepare creates required directories."""
        source = tmp_path / "source"
        dest = tmp_path / "dest"
        source.mkdir()
        # dest doesn't exist yet

        with patch("shutil.which", return_value="/usr/bin/btrfs"):
            with patch("btrfs_backup_ng.__util__.is_subvolume", return_value=True):
                with patch("btrfs_backup_ng.__util__.is_btrfs", return_value=True):
                    endpoint = LocalEndpoint(
                        config={
                            "source": str(source),
                            "path": str(dest),
                            "fs_checks": "auto",
                        }
                    )
                    endpoint.prepare()

        assert dest.exists()
        assert (dest / ".btrfs-backup-ng").exists()

    def test_endpoint_get_id(self, tmp_path):
        """Test endpoint ID generation."""
        dest = tmp_path / "dest"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
            }
        )

        assert endpoint.get_id() == str(dest)

    def test_list_snapshots_empty(self, tmp_path):
        """Test listing snapshots when none exist."""
        dest = tmp_path / "dest"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
                "snap_prefix": "test-",
            }
        )

        snapshots = endpoint.list_snapshots()
        assert snapshots == []

    def test_list_snapshots_with_prefix_matching(self, tmp_path):
        """Test listing snapshots filters by prefix."""
        dest = tmp_path / "dest"
        dest.mkdir()

        # Create some snapshot-like directories
        (dest / "test-20240115-120000").mkdir()
        (dest / "test-20240116-120000").mkdir()
        (dest / "other-20240117-120000").mkdir()  # Different prefix

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
                "snap_prefix": "test-",
            }
        )

        snapshots = endpoint.list_snapshots()
        names = [s.get_name() for s in snapshots]

        assert len(snapshots) == 2
        assert "test-20240115-120000" in names
        assert "test-20240116-120000" in names
        assert "other-20240117-120000" not in names

    def test_snapshot_cache_behavior(self, tmp_path):
        """Test snapshot listing cache is used correctly."""
        dest = tmp_path / "dest"
        dest.mkdir()
        (dest / "test-20240115-120000").mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
                "snap_prefix": "test-",
            }
        )

        # First call populates cache
        snapshots1 = endpoint.list_snapshots()
        assert len(snapshots1) == 1

        # Add another snapshot
        (dest / "test-20240116-120000").mkdir()

        # Second call should return cached result
        snapshots2 = endpoint.list_snapshots()
        assert len(snapshots2) == 1  # Still 1 because of cache

        # Flush cache
        snapshots3 = endpoint.list_snapshots(flush_cache=True)
        assert len(snapshots3) == 2


class TestEndpointSnapshotOperations:
    """Test endpoint snapshot creation with mocked btrfs commands."""

    @patch("btrfs_backup_ng.__util__.exec_subprocess")
    @patch("filelock.FileLock")
    def test_snapshot_command_building(self, mock_filelock, mock_exec, tmp_path):
        """Test snapshot command is built correctly."""
        source = tmp_path / "source"
        dest = tmp_path / "dest"
        source.mkdir()
        dest.mkdir()

        mock_filelock.return_value.__enter__ = MagicMock()
        mock_filelock.return_value.__exit__ = MagicMock()
        mock_exec.return_value = b""

        with patch("subprocess.check_output", return_value=b""):
            with patch.object(Endpoint, "_remount"):
                endpoint = LocalEndpoint(
                    config={
                        "source": str(source),
                        "path": str(dest),
                        "snap_prefix": "test-",
                        "snapshot_folder": ".snapshots",
                    }
                )
                endpoint.snapshot(readonly=True, sync=True)

        # Verify btrfs snapshot command was called
        assert mock_exec.called
        calls = mock_exec.call_args_list

        # Should have at least snapshot command
        snapshot_call = None
        sync_call = None
        for c in calls:
            args = c[0][0] if c[0] else []
            if "snapshot" in args:
                snapshot_call = c
            if args == ["sync"]:
                sync_call = c

        assert snapshot_call is not None, "snapshot command not found"
        assert sync_call is not None, "sync command not found"


class TestEndpointSendReceiveOperations:
    """Test send/receive operations with mocked subprocess."""

    @patch("btrfs_backup_ng.__util__.exec_subprocess")
    def test_build_send_command(self, mock_exec, tmp_path):
        """Test send command construction."""
        dest = tmp_path / "dest"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
            }
        )

        # Create a mock snapshot
        mock_snapshot = MagicMock()
        mock_snapshot.get_path.return_value = dest / "test-20240115-120000"

        # Build send command
        cmd = endpoint._build_send_command(mock_snapshot)

        # Verify command structure
        assert ("btrfs", False) in cmd
        assert ("send", False) in cmd
        # Last element should be the snapshot path
        assert cmd[-1][0] == str(dest / "test-20240115-120000")
        assert cmd[-1][1] is True  # Is a path

    @patch("btrfs_backup_ng.__util__.exec_subprocess")
    def test_build_send_command_with_parent(self, mock_exec, tmp_path):
        """Test send command with parent snapshot."""
        dest = tmp_path / "dest"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
            }
        )

        mock_snapshot = MagicMock()
        mock_snapshot.get_path.return_value = dest / "test-20240116-120000"

        mock_parent = MagicMock()
        mock_parent.get_path.return_value = dest / "test-20240115-120000"

        cmd = endpoint._build_send_command(mock_snapshot, parent=mock_parent)

        # Verify parent flag is included
        assert ("-p", False) in cmd
        # Find parent path in command
        parent_found = False
        for arg, is_path in cmd:
            if str(arg) == str(dest / "test-20240115-120000"):
                parent_found = True
                break
        assert parent_found, "Parent path not found in command"

    def test_build_receive_command(self, tmp_path):
        """Test receive command construction."""
        dest = tmp_path / "dest"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
            }
        )

        cmd = endpoint._build_receive_command(dest)

        assert ("btrfs", False) in cmd
        assert ("receive", False) in cmd
        # Last element should be destination
        assert str(dest) in str(cmd[-1][0])


class TestEndpointDeletionOperations:
    """Test snapshot deletion with mocked subprocess."""

    @patch("btrfs_backup_ng.__util__.exec_subprocess")
    def test_build_deletion_commands(self, mock_exec, tmp_path):
        """Test deletion command construction."""
        dest = tmp_path / "dest"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
            }
        )

        mock_snapshot = MagicMock()
        mock_snapshot.get_path.return_value = dest / "test-20240115-120000"
        mock_snapshot.locks = set()
        mock_snapshot.parent_locks = set()

        cmds = endpoint._build_deletion_commands([mock_snapshot])

        assert len(cmds) == 1
        cmd = cmds[0]
        assert ("btrfs", False) in cmd
        assert ("subvolume", False) in cmd
        assert ("delete", False) in cmd

    @patch("btrfs_backup_ng.__util__.exec_subprocess")
    def test_build_deletion_commands_with_convert_rw(self, mock_exec, tmp_path):
        """Test deletion with read-write conversion."""
        dest = tmp_path / "dest"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
                "convert_rw": True,
            }
        )

        mock_snapshot = MagicMock()
        mock_snapshot.get_path.return_value = dest / "test-20240115-120000"

        cmds = endpoint._build_deletion_commands([mock_snapshot])

        # Should have property set command before delete
        assert len(cmds) == 2
        prop_cmd = cmds[0]
        assert ("property", False) in prop_cmd
        assert ("set", False) in prop_cmd

    @patch("btrfs_backup_ng.__util__.exec_subprocess")
    def test_delete_snapshots_skips_locked(self, mock_exec, tmp_path):
        """Test that locked snapshots are not deleted."""
        dest = tmp_path / "dest"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
            }
        )

        mock_snapshot = MagicMock()
        mock_snapshot.get_path.return_value = dest / "test-20240115-120000"
        mock_snapshot.locks = {"some-lock-id"}
        mock_snapshot.parent_locks = set()

        endpoint.delete_snapshots([mock_snapshot])

        # exec_subprocess should not be called for locked snapshots
        mock_exec.assert_not_called()


class TestEndpointLockOperations:
    """Test lock file operations."""

    def test_lock_file_path(self, tmp_path):
        """Test lock file path generation."""
        source = tmp_path / "source"
        dest = tmp_path / "dest"
        source.mkdir()
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "source": str(source),
                "path": str(dest),
                "lock_file_name": ".test-locks",
            }
        )

        lock_path = endpoint._get_lock_file_path()
        assert lock_path == dest / ".test-locks"


class TestEndpointRemotePathHandling:
    """Test path handling for remote-like configurations."""

    def test_normalize_path_remote_flag(self, tmp_path):
        """Test path normalization respects remote flag."""
        endpoint = Endpoint(
            config={
                "path": "/remote/path",
            }
        )

        # Set remote flag
        endpoint._is_remote = True

        # Remote paths should not be resolved
        normalized = endpoint._normalize_path("/remote/path")
        assert normalized == "/remote/path"

    def test_normalize_path_local_absolute(self, tmp_path):
        """Test local absolute paths are handled correctly."""
        dest = tmp_path / "dest"
        dest.mkdir()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
            }
        )

        # Local paths should be resolved
        assert endpoint.config["path"] == dest


class TestEndpointCommandExecution:
    """Test command execution with sudo handling."""

    @patch("btrfs_backup_ng.__util__.exec_subprocess")
    @patch("os.geteuid", return_value=1000)  # Non-root user
    @patch("filelock.FileLock")
    def test_adds_sudo_for_btrfs_commands(
        self, mock_filelock, mock_geteuid, mock_exec, tmp_path
    ):
        """Test sudo is added for btrfs commands when not root."""
        dest = tmp_path / "dest"
        dest.mkdir()

        mock_filelock.return_value.__enter__ = MagicMock()
        mock_filelock.return_value.__exit__ = MagicMock()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
            }
        )

        cmd = [("btrfs", False), ("subvolume", False), ("list", False)]
        endpoint._exec_command({"command": cmd})

        # Verify sudo was prepended
        called_cmd = mock_exec.call_args[0][0]
        assert called_cmd[0] == "sudo"
        assert "btrfs" in called_cmd

    @patch("btrfs_backup_ng.__util__.exec_subprocess")
    @patch("os.geteuid", return_value=0)  # Root user
    @patch("filelock.FileLock")
    def test_no_sudo_for_root_user(
        self, mock_filelock, mock_geteuid, mock_exec, tmp_path
    ):
        """Test sudo is not added when running as root."""
        dest = tmp_path / "dest"
        dest.mkdir()

        mock_filelock.return_value.__enter__ = MagicMock()
        mock_filelock.return_value.__exit__ = MagicMock()

        endpoint = LocalEndpoint(
            config={
                "path": str(dest),
            }
        )

        cmd = [("btrfs", False), ("subvolume", False), ("list", False)]
        endpoint._exec_command({"command": cmd})

        # Verify sudo was NOT prepended
        called_cmd = mock_exec.call_args[0][0]
        assert called_cmd[0] == "btrfs"


class TestEndpointErrorHandling:
    """Test endpoint error handling scenarios."""

    def test_invalid_source_path(self, tmp_path):
        """Test handling of invalid source path."""
        # Path that definitely doesn't exist with weird chars that might fail
        with pytest.raises(ValueError, match="Invalid source path"):
            LocalEndpoint(
                config={
                    "source": "/nonexistent/\x00/path",  # Null char makes it invalid
                    "path": str(tmp_path),
                }
            )

    def test_prepare_without_btrfs_command(self, tmp_path):
        """Test prepare fails when btrfs is not found."""
        dest = tmp_path / "dest"
        dest.mkdir()

        with patch("shutil.which", return_value=None):
            endpoint = LocalEndpoint(
                config={
                    "path": str(dest),
                }
            )

            from btrfs_backup_ng.__util__ import AbortError

            with pytest.raises(AbortError, match="btrfs command not found"):
                endpoint.prepare()

    def test_prepare_fails_fs_check(self, tmp_path):
        """Test prepare fails when fs check fails."""
        source = tmp_path / "source"
        dest = tmp_path / "dest"
        source.mkdir()
        dest.mkdir()

        with patch("shutil.which", return_value="/usr/bin/btrfs"):
            with patch("btrfs_backup_ng.__util__.is_subvolume", return_value=False):
                endpoint = LocalEndpoint(
                    config={
                        "source": str(source),
                        "path": str(dest),
                        "fs_checks": "strict",  # Use strict mode to test error behavior
                    }
                )

                from btrfs_backup_ng.__util__ import AbortError

                with pytest.raises(AbortError, match="not a btrfs subvolume"):
                    endpoint.prepare()

    def test_prepare_auto_mode_warns_but_continues(self, tmp_path, capsys):
        """Test prepare in auto mode warns but doesn't error on fs check failure."""
        source = tmp_path / "source"
        dest = tmp_path / "dest"
        source.mkdir()
        dest.mkdir()

        with patch("shutil.which", return_value="/usr/bin/btrfs"):
            with patch("btrfs_backup_ng.__util__.is_subvolume", return_value=False):
                with patch("btrfs_backup_ng.__util__.is_btrfs", return_value=True):
                    endpoint = LocalEndpoint(
                        config={
                            "source": str(source),
                            "path": str(dest),
                            "fs_checks": "auto",  # Auto mode should warn but continue
                        }
                    )

                    # Should NOT raise, just warn
                    endpoint.prepare()

                    # Check that a warning was logged to stderr
                    captured = capsys.readouterr()
                    assert "btrfs subvolume" in captured.err
                    assert "auto mode" in captured.err

    def test_prepare_skip_mode_no_checks(self, tmp_path, caplog):
        """Test prepare in skip mode doesn't perform fs checks."""
        import logging

        source = tmp_path / "source"
        dest = tmp_path / "dest"
        source.mkdir()
        dest.mkdir()

        with patch("shutil.which", return_value="/usr/bin/btrfs"):
            # is_subvolume returns False but with skip mode it shouldn't matter
            with patch(
                "btrfs_backup_ng.__util__.is_subvolume", return_value=False
            ) as mock_is_subvol:
                with patch("btrfs_backup_ng.__util__.is_btrfs", return_value=True):
                    endpoint = LocalEndpoint(
                        config={
                            "source": str(source),
                            "path": str(dest),
                            "fs_checks": "skip",  # Skip mode should not check
                        }
                    )

                    with caplog.at_level(logging.WARNING):
                        endpoint.prepare()

                    # is_subvolume should not have been called (checks skipped)
                    mock_is_subvol.assert_not_called()

                    # No warnings about subvolume should be logged
                    assert not any(
                        "subvolume" in record.message for record in caplog.records
                    )
