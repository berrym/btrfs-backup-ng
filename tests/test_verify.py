"""Tests for backup verification functionality."""

import subprocess
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core.verify import (
    VerifyError,
    VerifyLevel,
    VerifyReport,
    VerifyResult,
    _find_parent_snapshot,
    _test_send_stream,
    verify_full,
    verify_metadata,
    verify_stream,
)


class MockSnapshot:
    """Mock Snapshot object for testing."""

    def __init__(self, name: str, time_obj=None):
        self._name = name
        self.time_obj = time_obj or time.strptime("20260101-120000", "%Y%m%d-%H%M%S")

    def get_name(self) -> str:
        return self._name

    def __lt__(self, other):
        if self.time_obj and other.time_obj:
            return self.time_obj < other.time_obj
        return False

    def __eq__(self, other):
        if isinstance(other, MockSnapshot):
            return self._name == other._name
        return False

    def __hash__(self):
        return hash(self._name)


def make_snapshots(names_and_times: list) -> list:
    """Create list of MockSnapshots from names and time strings."""
    snapshots = []
    for name, time_str in names_and_times:
        t = time.strptime(time_str, "%Y%m%d-%H%M%S")
        snapshots.append(MockSnapshot(name, t))
    return sorted(snapshots, key=lambda s: s.time_obj)


class TestVerifyResult:
    """Tests for VerifyResult dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        result = VerifyResult(
            snapshot_name="test",
            level=VerifyLevel.METADATA,
            passed=True,
        )
        assert result.snapshot_name == "test"
        assert result.level == VerifyLevel.METADATA
        assert result.passed is True
        assert result.message == ""
        assert result.duration_seconds == 0.0
        assert result.details == {}

    def test_with_details(self):
        """Test result with details."""
        result = VerifyResult(
            snapshot_name="test",
            level=VerifyLevel.STREAM,
            passed=False,
            message="Stream failed",
            details={"error_code": 1},
        )
        assert result.passed is False
        assert result.message == "Stream failed"
        assert result.details["error_code"] == 1


class TestVerifyReport:
    """Tests for VerifyReport dataclass."""

    def test_empty_report(self):
        """Test empty report statistics."""
        report = VerifyReport(
            level=VerifyLevel.METADATA,
            location="/test",
        )
        assert report.passed == 0
        assert report.failed == 0
        assert report.total == 0
        assert report.errors == []

    def test_report_with_results(self):
        """Test report statistics with results."""
        report = VerifyReport(
            level=VerifyLevel.METADATA,
            location="/test",
        )
        report.results = [
            VerifyResult("snap-1", VerifyLevel.METADATA, True),
            VerifyResult("snap-2", VerifyLevel.METADATA, True),
            VerifyResult("snap-3", VerifyLevel.METADATA, False),
        ]

        assert report.passed == 2
        assert report.failed == 1
        assert report.total == 3

    def test_duration_calculation(self):
        """Test duration is calculated correctly."""
        report = VerifyReport(
            level=VerifyLevel.METADATA,
            location="/test",
        )
        report.started_at = time.time() - 5.0
        report.completed_at = time.time()

        assert 4.5 < report.duration < 5.5


class TestFindParentSnapshot:
    """Tests for _find_parent_snapshot function."""

    def test_finds_most_recent_older(self):
        """Test finding the most recent older snapshot."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),
            ]
        )

        parent = _find_parent_snapshot(snapshots[2], snapshots)
        assert parent is not None
        assert parent.get_name() == "snap-2"

    def test_returns_none_for_oldest(self):
        """Test None is returned for oldest snapshot."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
            ]
        )

        parent = _find_parent_snapshot(snapshots[0], snapshots)
        assert parent is None

    def test_excludes_self(self):
        """Test that snapshot is not its own parent."""
        snap = MockSnapshot("snap-1", time.strptime("20260101-100000", "%Y%m%d-%H%M%S"))

        parent = _find_parent_snapshot(snap, [snap])
        assert parent is None


class TestVerifyMetadata:
    """Tests for verify_metadata function."""

    def test_empty_backup_location(self):
        """Test handling of empty backup location."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = []
        mock_endpoint.config = {"path": "/backup"}

        report = verify_metadata(mock_endpoint)

        assert report.total == 0
        assert "No snapshots found" in report.errors[0]

    def test_single_snapshot(self):
        """Test verification of single snapshot."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [
            MockSnapshot("snap-1"),
        ]
        mock_endpoint.config = {"path": "/backup"}

        report = verify_metadata(mock_endpoint)

        assert report.total == 1
        assert report.passed == 1
        assert report.failed == 0

    def test_complete_chain(self):
        """Test verification of complete parent chain."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),
            ]
        )

        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = snapshots
        mock_endpoint.config = {"path": "/backup"}

        report = verify_metadata(mock_endpoint)

        assert report.total == 3
        assert report.passed == 3
        assert report.failed == 0

    def test_specific_snapshot(self):
        """Test verification of specific snapshot only."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),
            ]
        )

        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = snapshots
        mock_endpoint.config = {"path": "/backup"}

        report = verify_metadata(mock_endpoint, snapshot_name="snap-2")

        assert report.total == 1
        assert report.results[0].snapshot_name == "snap-2"

    def test_snapshot_not_found(self):
        """Test handling of non-existent snapshot."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
        mock_endpoint.config = {"path": "/backup"}

        report = verify_metadata(mock_endpoint, snapshot_name="nonexistent")

        assert report.total == 0
        assert "not found" in report.errors[0]

    def test_progress_callback(self):
        """Test that progress callback is called."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
            ]
        )

        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = snapshots
        mock_endpoint.config = {"path": "/backup"}

        progress_calls = []

        def on_progress(current, total, name):
            progress_calls.append((current, total, name))

        verify_metadata(mock_endpoint, on_progress=on_progress)

        assert len(progress_calls) == 2
        assert progress_calls[0] == (1, 2, "snap-1")
        assert progress_calls[1] == (2, 2, "snap-2")

    def test_source_comparison(self):
        """Test comparison with source snapshots."""
        backup_snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
            ]
        )
        source_snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),  # Not in backup
            ]
        )

        backup_ep = MagicMock()
        backup_ep.list_snapshots.return_value = backup_snapshots
        backup_ep.config = {"path": "/backup"}

        source_ep = MagicMock()
        source_ep.list_snapshots.return_value = source_snapshots

        report = verify_metadata(backup_ep, source_endpoint=source_ep)

        # Should report missing snapshot
        assert any("snap-3" in str(e) for e in report.errors)


class TestVerifyLevel:
    """Tests for VerifyLevel enum."""

    def test_level_values(self):
        """Test level enum values."""
        assert VerifyLevel.METADATA.value == "metadata"
        assert VerifyLevel.STREAM.value == "stream"
        assert VerifyLevel.FULL.value == "full"

    def test_level_from_string(self):
        """Test creating level from string."""
        assert VerifyLevel("metadata") == VerifyLevel.METADATA
        assert VerifyLevel("stream") == VerifyLevel.STREAM
        assert VerifyLevel("full") == VerifyLevel.FULL


class TestVerifyStream:
    """Tests for verify_stream function."""

    def test_empty_backup_location(self):
        """Test handling of empty backup location."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = []
        mock_endpoint.config = {"path": "/backup"}

        report = verify_stream(mock_endpoint)

        assert report.total == 0
        assert report.level == VerifyLevel.STREAM
        assert "No snapshots found" in report.errors[0]

    def test_snapshot_not_found(self):
        """Test handling of non-existent snapshot."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
        mock_endpoint.config = {"path": "/backup"}

        report = verify_stream(mock_endpoint, snapshot_name="nonexistent")

        assert report.total == 0
        assert "not found" in report.errors[0]

    @patch("btrfs_backup_ng.core.verify._test_send_stream")
    def test_verify_latest_by_default(self, mock_test_stream):
        """Test that only latest snapshot is verified by default."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
                ("snap-3", "20260101-120000"),
            ]
        )

        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = snapshots
        mock_endpoint.config = {"path": "/backup"}

        report = verify_stream(mock_endpoint)

        assert report.total == 1
        assert report.results[0].snapshot_name == "snap-3"

    @patch("btrfs_backup_ng.core.verify._test_send_stream")
    def test_verify_specific_snapshot(self, mock_test_stream):
        """Test verification of specific snapshot."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
            ]
        )

        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = snapshots
        mock_endpoint.config = {"path": "/backup"}

        report = verify_stream(mock_endpoint, snapshot_name="snap-1")

        assert report.total == 1
        assert report.results[0].snapshot_name == "snap-1"

    @patch("btrfs_backup_ng.core.verify._test_send_stream")
    def test_stream_success(self, mock_test_stream):
        """Test successful stream verification."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
        mock_endpoint.config = {"path": "/backup"}

        report = verify_stream(mock_endpoint)

        assert report.total == 1
        assert report.passed == 1
        assert report.results[0].passed is True
        assert "verified successfully" in report.results[0].message

    @patch("btrfs_backup_ng.core.verify._test_send_stream")
    def test_stream_failure(self, mock_test_stream):
        """Test failed stream verification."""
        mock_test_stream.side_effect = Exception("Stream error")

        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
        mock_endpoint.config = {"path": "/backup"}

        report = verify_stream(mock_endpoint)

        assert report.total == 1
        assert report.failed == 1
        assert report.results[0].passed is False
        assert "failed" in report.results[0].message

    @patch("btrfs_backup_ng.core.verify._test_send_stream")
    def test_progress_callback(self, mock_test_stream):
        """Test that progress callback is called."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
        mock_endpoint.config = {"path": "/backup"}

        progress_calls = []

        def on_progress(current, total, name):
            progress_calls.append((current, total, name))

        verify_stream(mock_endpoint, on_progress=on_progress)

        assert len(progress_calls) == 1
        assert progress_calls[0] == (1, 1, "snap-1")

    @patch("btrfs_backup_ng.core.verify._test_send_stream")
    def test_incremental_detection(self, mock_test_stream):
        """Test that incremental parent is detected."""
        snapshots = make_snapshots(
            [
                ("snap-1", "20260101-100000"),
                ("snap-2", "20260101-110000"),
            ]
        )

        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = snapshots
        mock_endpoint.config = {"path": "/backup"}

        report = verify_stream(mock_endpoint, snapshot_name="snap-2")

        assert report.results[0].details.get("incremental") is True
        assert report.results[0].details.get("parent") == "snap-1"

    def test_exception_handling(self):
        """Test that exceptions are caught and reported."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.side_effect = Exception("Connection failed")
        mock_endpoint.config = {"path": "/backup"}

        report = verify_stream(mock_endpoint)

        assert len(report.errors) > 0
        assert "Connection failed" in report.errors[0]


class TestVerifyFull:
    """Tests for verify_full function."""

    @patch("btrfs_backup_ng.core.verify.__util__.is_btrfs")
    def test_empty_backup_location(self, mock_is_btrfs):
        """Test handling of empty backup location."""
        import tempfile

        mock_is_btrfs.return_value = True

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_endpoint = MagicMock()
            mock_endpoint.list_snapshots.return_value = []
            mock_endpoint.config = {"path": tmpdir}

            report = verify_full(mock_endpoint, temp_dir=Path(tmpdir))

            assert report.level == VerifyLevel.FULL
            assert "No snapshots found" in report.errors[0]

    @patch("btrfs_backup_ng.core.verify.__util__.is_btrfs")
    def test_snapshot_not_found(self, mock_is_btrfs):
        """Test handling of non-existent snapshot."""
        import tempfile

        mock_is_btrfs.return_value = True

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_endpoint = MagicMock()
            mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
            mock_endpoint.config = {"path": tmpdir}

            report = verify_full(
                mock_endpoint, snapshot_name="nonexistent", temp_dir=Path(tmpdir)
            )

            assert "not found" in report.errors[0]

    @patch("btrfs_backup_ng.core.verify.__util__.is_btrfs")
    def test_temp_dir_not_btrfs(self, mock_is_btrfs):
        """Test error when temp dir is not on btrfs."""
        import tempfile

        mock_is_btrfs.return_value = False

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_endpoint = MagicMock()
            mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
            mock_endpoint.config = {"path": tmpdir}

            report = verify_full(mock_endpoint, temp_dir=Path(tmpdir))

            assert "not on btrfs" in report.errors[0]

    def test_remote_without_temp_dir(self):
        """Test error when remote backup without temp_dir."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
        mock_endpoint.config = {"path": "ssh://server:/backups"}

        report = verify_full(mock_endpoint)

        assert "--temp-dir must be specified" in report.errors[0]

    @patch("btrfs_backup_ng.endpoint.LocalEndpoint")
    @patch("btrfs_backup_ng.core.verify._test_restore")
    @patch("btrfs_backup_ng.core.verify.__util__.is_subvolume")
    @patch("btrfs_backup_ng.core.verify.__util__.is_btrfs")
    def test_full_verify_success(
        self,
        mock_is_btrfs,
        mock_is_subvolume,
        mock_test_restore,
        mock_local_endpoint,
    ):
        """Test successful full verification."""
        import tempfile

        mock_is_btrfs.return_value = True
        mock_is_subvolume.return_value = True

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create mock snapshot path
            snap_path = Path(tmpdir) / "snap-1"
            snap_path.mkdir()

            mock_endpoint = MagicMock()
            mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
            mock_endpoint.config = {"path": "/backup"}

            report = verify_full(mock_endpoint, temp_dir=Path(tmpdir), cleanup=False)

            assert report.total == 1
            assert report.passed == 1
            assert report.results[0].passed is True

    @patch("btrfs_backup_ng.endpoint.LocalEndpoint")
    @patch("btrfs_backup_ng.core.verify._test_restore")
    @patch("btrfs_backup_ng.core.verify.__util__.is_btrfs")
    def test_full_verify_restore_fails(
        self,
        mock_is_btrfs,
        mock_test_restore,
        mock_local_endpoint,
    ):
        """Test full verification when restore fails."""
        import tempfile

        mock_is_btrfs.return_value = True
        mock_test_restore.side_effect = Exception("Restore failed")

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_endpoint = MagicMock()
            mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
            mock_endpoint.config = {"path": "/backup"}

            report = verify_full(mock_endpoint, temp_dir=Path(tmpdir), cleanup=False)

            assert report.total == 1
            assert report.failed == 1
            assert "failed" in report.results[0].message.lower()

    @patch("btrfs_backup_ng.endpoint.LocalEndpoint")
    @patch("btrfs_backup_ng.core.verify._test_restore")
    @patch("btrfs_backup_ng.core.verify.__util__.is_btrfs")
    def test_restored_path_not_found(
        self,
        mock_is_btrfs,
        mock_test_restore,
        mock_local_endpoint,
    ):
        """Test error when restored snapshot path doesn't exist."""
        import tempfile

        mock_is_btrfs.return_value = True
        # Don't create the snapshot path - it won't exist

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_endpoint = MagicMock()
            mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
            mock_endpoint.config = {"path": "/backup"}

            report = verify_full(mock_endpoint, temp_dir=Path(tmpdir), cleanup=False)

            assert report.failed == 1
            assert "not found" in report.results[0].message.lower()

    @patch("btrfs_backup_ng.endpoint.LocalEndpoint")
    @patch("btrfs_backup_ng.core.verify._test_restore")
    @patch("btrfs_backup_ng.core.verify.__util__.is_subvolume")
    @patch("btrfs_backup_ng.core.verify.__util__.is_btrfs")
    def test_restored_not_subvolume(
        self,
        mock_is_btrfs,
        mock_is_subvolume,
        mock_test_restore,
        mock_local_endpoint,
    ):
        """Test error when restored path is not a valid subvolume."""
        import tempfile

        mock_is_btrfs.return_value = True
        mock_is_subvolume.return_value = False

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create the path but it won't be a subvolume
            snap_path = Path(tmpdir) / "snap-1"
            snap_path.mkdir()

            mock_endpoint = MagicMock()
            mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
            mock_endpoint.config = {"path": "/backup"}

            report = verify_full(mock_endpoint, temp_dir=Path(tmpdir), cleanup=False)

            assert report.failed == 1
            assert "not a valid subvolume" in report.results[0].message.lower()

    @patch("btrfs_backup_ng.core.verify.__util__.is_btrfs")
    def test_with_custom_temp_dir(self, mock_is_btrfs):
        """Test using custom temp directory."""
        import tempfile

        mock_is_btrfs.return_value = True

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)

            mock_endpoint = MagicMock()
            mock_endpoint.list_snapshots.return_value = []
            mock_endpoint.config = {"path": "/backup"}

            report = verify_full(mock_endpoint, temp_dir=temp_path)

            # Should have gotten past temp_dir setup to "no snapshots" error
            assert "No snapshots found" in report.errors[0]

    @patch("btrfs_backup_ng.core.verify.__util__.is_btrfs")
    def test_creates_temp_dir_if_not_exists(self, mock_is_btrfs):
        """Test that temp_dir is created if it doesn't exist."""
        import tempfile

        mock_is_btrfs.return_value = True

        with tempfile.TemporaryDirectory() as tmpdir:
            new_temp = Path(tmpdir) / "new_subdir"
            assert not new_temp.exists()

            mock_endpoint = MagicMock()
            mock_endpoint.list_snapshots.return_value = []
            mock_endpoint.config = {"path": "/backup"}

            verify_full(mock_endpoint, temp_dir=new_temp)

            assert new_temp.exists()

    @patch("btrfs_backup_ng.endpoint.LocalEndpoint")
    @patch("btrfs_backup_ng.core.verify._test_restore")
    @patch("btrfs_backup_ng.core.verify.__util__.is_subvolume")
    @patch("btrfs_backup_ng.core.verify.__util__.is_btrfs")
    def test_progress_callback(
        self,
        mock_is_btrfs,
        mock_is_subvolume,
        mock_test_restore,
        mock_local_endpoint,
    ):
        """Test that progress callback is called."""
        import tempfile

        mock_is_btrfs.return_value = True
        mock_is_subvolume.return_value = True

        with tempfile.TemporaryDirectory() as tmpdir:
            snap_path = Path(tmpdir) / "snap-1"
            snap_path.mkdir()

            mock_endpoint = MagicMock()
            mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
            mock_endpoint.config = {"path": "/backup"}

            progress_calls = []

            def on_progress(current, total, name):
                progress_calls.append((current, total, name))

            verify_full(
                mock_endpoint,
                temp_dir=Path(tmpdir),
                cleanup=False,
                on_progress=on_progress,
            )

            assert len(progress_calls) == 1
            assert progress_calls[0] == (1, 1, "snap-1")


class TestTestSendStream:
    """Tests for _test_send_stream function."""

    @patch("subprocess.run")
    def test_local_send_success(self, mock_run):
        """Test successful local send stream test."""
        mock_run.return_value = MagicMock(returncode=0)

        mock_endpoint = MagicMock()
        mock_endpoint.config = {"path": "/backup"}
        mock_endpoint.ssh_client = None

        snapshot = MockSnapshot("snap-1")

        # Should not raise
        _test_send_stream(mock_endpoint, snapshot)

        mock_run.assert_called_once()
        call_args = mock_run.call_args
        assert "btrfs" in call_args[0][0]
        assert "--no-data" in call_args[0][0]

    @patch("subprocess.run")
    def test_local_send_with_parent(self, mock_run):
        """Test local send with parent snapshot."""
        mock_run.return_value = MagicMock(returncode=0)

        mock_endpoint = MagicMock()
        mock_endpoint.config = {"path": "/backup"}
        mock_endpoint.ssh_client = None

        snapshot = MockSnapshot("snap-2")
        parent = MockSnapshot("snap-1")

        _test_send_stream(mock_endpoint, snapshot, parent)

        call_args = mock_run.call_args
        assert "-p" in call_args[0][0]

    @patch("subprocess.run")
    def test_local_send_failure(self, mock_run):
        """Test failed local send stream test."""
        mock_run.return_value = MagicMock(
            returncode=1, stderr=b"Send failed: corrupted data"
        )

        mock_endpoint = MagicMock()
        mock_endpoint.config = {"path": "/backup"}
        mock_endpoint.ssh_client = None

        snapshot = MockSnapshot("snap-1")

        with pytest.raises(VerifyError) as exc_info:
            _test_send_stream(mock_endpoint, snapshot)

        assert "failed" in str(exc_info.value).lower()

    def test_ssh_send_success(self):
        """Test successful SSH send stream test."""
        mock_stdout = MagicMock()
        mock_stdout.channel.recv_exit_status.return_value = 0

        mock_stderr = MagicMock()
        mock_stderr.read.return_value = b""

        mock_ssh_client = MagicMock()
        mock_ssh_client.exec_command.return_value = (
            MagicMock(),
            mock_stdout,
            mock_stderr,
        )

        mock_endpoint = MagicMock()
        mock_endpoint.config = {"path": "/backup", "ssh_sudo": False}
        mock_endpoint.ssh_client = mock_ssh_client

        snapshot = MockSnapshot("snap-1")

        # Should not raise
        _test_send_stream(mock_endpoint, snapshot)

        mock_ssh_client.exec_command.assert_called_once()

    def test_ssh_send_with_sudo(self):
        """Test SSH send with sudo."""
        mock_stdout = MagicMock()
        mock_stdout.channel.recv_exit_status.return_value = 0

        mock_stderr = MagicMock()

        mock_ssh_client = MagicMock()
        mock_ssh_client.exec_command.return_value = (
            MagicMock(),
            mock_stdout,
            mock_stderr,
        )

        mock_endpoint = MagicMock()
        mock_endpoint.config = {"path": "/backup", "ssh_sudo": True}
        mock_endpoint.ssh_client = mock_ssh_client

        snapshot = MockSnapshot("snap-1")

        _test_send_stream(mock_endpoint, snapshot)

        call_args = mock_ssh_client.exec_command.call_args
        assert "sudo" in call_args[0][0]

    def test_ssh_send_failure(self):
        """Test failed SSH send stream test."""
        mock_stdout = MagicMock()
        mock_stdout.channel.recv_exit_status.return_value = 1

        mock_stderr = MagicMock()
        mock_stderr.read.return_value = b"Permission denied"

        mock_ssh_client = MagicMock()
        mock_ssh_client.exec_command.return_value = (
            MagicMock(),
            mock_stdout,
            mock_stderr,
        )

        mock_endpoint = MagicMock()
        mock_endpoint.config = {"path": "/backup", "ssh_sudo": False}
        mock_endpoint.ssh_client = mock_ssh_client

        snapshot = MockSnapshot("snap-1")

        with pytest.raises(VerifyError) as exc_info:
            _test_send_stream(mock_endpoint, snapshot)

        assert "failed" in str(exc_info.value).lower()


class TestVerifyError:
    """Tests for VerifyError exception."""

    def test_verify_error_message(self):
        """Test VerifyError exception."""
        error = VerifyError("Test error message")
        assert str(error) == "Test error message"

    def test_verify_error_is_exception(self):
        """Test VerifyError is an Exception."""
        assert issubclass(VerifyError, Exception)


class TestVerifyMetadataExceptionHandling:
    """Tests for exception handling in verify_metadata."""

    def test_list_snapshots_exception(self):
        """Test handling of exception during list_snapshots."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.side_effect = Exception("Connection failed")
        mock_endpoint.config = {"path": "/backup"}

        report = verify_metadata(mock_endpoint)

        assert len(report.errors) > 0
        assert "Connection failed" in report.errors[0]

    def test_source_comparison_exception(self):
        """Test handling of exception during source comparison."""
        backup_snapshots = [MockSnapshot("snap-1")]

        backup_ep = MagicMock()
        backup_ep.list_snapshots.return_value = backup_snapshots
        backup_ep.config = {"path": "/backup"}

        source_ep = MagicMock()
        source_ep.list_snapshots.side_effect = Exception("Source unreachable")

        # Should not raise, just log warning
        report = verify_metadata(backup_ep, source_endpoint=source_ep)

        # The report should still be valid
        assert report.total == 1


class TestVerifyReportDuration:
    """Additional tests for VerifyReport duration."""

    def test_duration_before_completion(self):
        """Test duration calculation before completed_at is set."""
        report = VerifyReport(
            level=VerifyLevel.METADATA,
            location="/test",
        )
        report.started_at = time.time() - 2.0
        # completed_at defaults to 0.0

        # Duration should be calculated from now
        duration = report.duration
        assert duration >= 2.0
