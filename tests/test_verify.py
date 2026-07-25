"""Tests for backup verification functionality."""

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict
from btrfs_backup_ng.core.verify import (
    ParentViability,
    ParentViabilityResult,
    VerifyError,
    VerifyLevel,
    VerifyReport,
    VerifyResult,
    _find_parent_snapshot,
    _test_send_stream,
    check_parent_viability,
    find_viable_parent,
    validate_transfer_chain,
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
        """A single snapshot the endpoint confirms structurally valid -> passes.
        (Plumbing: the count/report; the real structural checks are exercised against
        real filesystem objects in TestVerifyMetadataStructural.)"""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [
            MockSnapshot("snap-1"),
        ]
        mock_endpoint.config = {"path": "/backup"}
        mock_endpoint.verify_structure.return_value = StructureVerdict("ok", "valid")

        report = verify_metadata(mock_endpoint)

        assert report.total == 1
        assert report.passed == 1
        assert report.failed == 0

    def test_all_valid_snapshots_pass(self):
        """N structurally-valid snapshots -> N passed. (Replaces the old 'complete chain'
        test, which only passed because the parent check was a tautology that could never
        fail; real chain-break detection is in TestVerifyMetadataStructural.)"""
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
        mock_endpoint.verify_structure.return_value = StructureVerdict("ok", "valid")

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
        mock_endpoint.verify_structure.return_value = StructureVerdict("ok", "valid")

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
        mock_endpoint.verify_structure.return_value = StructureVerdict("ok", "valid")

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
        backup_ep.verify_structure.return_value = StructureVerdict("ok", "valid")

        source_ep = MagicMock()
        source_ep.list_snapshots.return_value = source_snapshots

        report = verify_metadata(backup_ep, source_endpoint=source_ep)

        # Should report missing snapshot
        assert any("snap-3" in str(e) for e in report.errors)

    def test_mixed_structural_verdicts_counted_correctly(self):
        """A single run with invalid + ok + unverifiable snapshots -> `failed` counts ONLY
        the invalid one; ok AND unverifiable both pass. This guards the aggregation rule
        (is_failure == invalid only): mutating `if structure.is_failure` to
        `if structure.status != "ok"` would wrongly fail the unverifiable one -- and every
        other test would still pass, so only this mixed-report test catches it."""
        snaps = [
            MockSnapshot("s-invalid"),
            MockSnapshot("s-ok"),
            MockSnapshot("s-unver"),
        ]
        verdicts = {
            "s-invalid": StructureVerdict("invalid", "not a subvolume"),
            "s-ok": StructureVerdict("ok", "received subvolume"),
            "s-unver": StructureVerdict("unverifiable", "no received_uuid"),
        }
        ep = MagicMock()
        ep.list_snapshots.return_value = snaps
        ep.config = {"path": "/backup"}
        ep.verify_structure.side_effect = lambda s: verdicts[s.get_name()]

        report = verify_metadata(ep)

        assert report.total == 3
        assert report.failed == 1  # only the invalid one
        assert report.passed == 2  # ok + unverifiable both pass
        by = {r.snapshot_name: r for r in report.results}
        assert by["s-invalid"].passed is False
        assert by["s-invalid"].details["structure"] == "invalid"
        assert by["s-ok"].passed is True
        assert by["s-ok"].details["structure"] == "ok"
        assert by["s-unver"].passed is True  # unverifiable is NOT a failure
        assert by["s-unver"].details["structure"] == "unverifiable"

    def test_ssh_verify_structure_checks_received_uuid(self):
        """SSHEndpoint.verify_structure: a received subvolume (non-empty received_uuid) ->
        ok; a non-received one (empty received_uuid) -> unverifiable, NOT a failure. SSH
        enumerates only real subvolumes, so the received_uuid is the masquerade check.
        (self is unused, so the method is exercised without a live SSH connection.)
        Mutation guard: dropping the received_uuid check makes the non-received case pass
        as ok."""
        from types import SimpleNamespace

        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        received = SSHEndpoint.verify_structure(
            None, SimpleNamespace(received_uuid="1111-2222")
        )
        non_received = SSHEndpoint.verify_structure(
            None, SimpleNamespace(received_uuid="")
        )

        assert received.status == "ok"
        assert non_received.status == "unverifiable"
        assert non_received.is_failure is False


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


# =============================================================================
# Pre-Transfer Parent Validation Tests
# =============================================================================


class TestParentViabilityResult:
    """Tests for ParentViabilityResult dataclass."""

    def test_viable_result(self):
        """Test a viable parent result."""
        result = ParentViabilityResult(
            status=ParentViability.VIABLE,
            parent_name="root-20240101",
            message="Parent is viable",
        )
        assert result.is_viable
        assert not result.should_use_full_send
        assert result.parent_name == "root-20240101"

    def test_missing_result(self):
        """Test a missing parent result."""
        result = ParentViabilityResult(
            status=ParentViability.MISSING,
            parent_name="root-20240101",
            message="Parent not found",
            fallback_to_full=True,
        )
        assert not result.is_viable
        assert result.should_use_full_send

    def test_corrupted_result(self):
        """Test a corrupted parent result."""
        result = ParentViabilityResult(
            status=ParentViability.CORRUPTED,
            parent_name="root-20240101",
            message="Send stream failed",
            fallback_to_full=True,
            details={"error": "checksum mismatch"},
        )
        assert not result.is_viable
        assert result.should_use_full_send
        assert "error" in result.details

    def test_locked_result(self):
        """Test a locked parent result - should not auto-fallback."""
        result = ParentViabilityResult(
            status=ParentViability.LOCKED,
            parent_name="root-20240101",
            message="Parent is locked",
            fallback_to_full=False,
        )
        assert not result.is_viable
        # Locked parents should NOT auto-fallback (wait for lock)
        assert result.should_use_full_send  # still true because not viable


class TestCheckParentViability:
    """Tests for check_parent_viability function."""

    def test_no_parent_returns_viable_with_fallback(self):
        """When no parent is specified, return viable but fallback to full."""
        source_ep = MagicMock()
        dest_ep = MagicMock()
        snapshot = MockSnapshot("root-20240102")

        result = check_parent_viability(
            snapshot, None, source_ep, dest_ep, check_level="quick"
        )

        assert result.status == ParentViability.VIABLE
        assert result.parent_name is None
        assert result.fallback_to_full

    def test_check_level_none_skips_validation(self):
        """check_level='none' skips all validation."""
        source_ep = MagicMock()
        dest_ep = MagicMock()
        snapshot = MockSnapshot("root-20240102")
        parent = MockSnapshot("root-20240101")

        result = check_parent_viability(
            snapshot, parent, source_ep, dest_ep, check_level="none"
        )

        assert result.status == ParentViability.VIABLE
        assert result.parent_name == "root-20240101"
        # Destination should not be called
        dest_ep.list_snapshots.assert_not_called()

    def test_parent_exists_at_destination(self):
        """Parent found at destination passes quick check."""
        source_ep = MagicMock()
        dest_ep = MagicMock()
        dest_ep.list_snapshots.return_value = [
            MockSnapshot("root-20240101"),
            MockSnapshot("root-20240102"),
        ]
        snapshot = MockSnapshot("root-20240103")
        parent = MockSnapshot("root-20240101")

        result = check_parent_viability(
            snapshot, parent, source_ep, dest_ep, check_level="quick"
        )

        assert result.status == ParentViability.VIABLE
        assert result.parent_name == "root-20240101"

    def test_parent_missing_at_destination(self):
        """Parent not found at destination returns MISSING."""
        source_ep = MagicMock()
        dest_ep = MagicMock()
        dest_ep.list_snapshots.return_value = [
            MockSnapshot("root-20240102"),
        ]
        snapshot = MockSnapshot("root-20240103")
        parent = MockSnapshot("root-20240101")  # Not at destination

        result = check_parent_viability(
            snapshot, parent, source_ep, dest_ep, check_level="quick"
        )

        assert result.status == ParentViability.MISSING
        assert result.fallback_to_full

    def test_parent_locked(self):
        """Locked parent returns LOCKED status."""
        source_ep = MagicMock()
        dest_ep = MagicMock()
        dest_ep.list_snapshots.return_value = [MockSnapshot("root-20240101")]

        snapshot = MockSnapshot("root-20240102")
        parent = MockSnapshot("root-20240101")
        parent.locks = {"backup_operation"}  # Parent is locked

        result = check_parent_viability(
            snapshot, parent, source_ep, dest_ep, check_level="quick"
        )

        assert result.status == ParentViability.LOCKED
        assert not result.fallback_to_full  # Don't fallback, wait for lock

    def test_destination_list_failure_continues(self):
        """If listing destination fails, proceed cautiously."""
        source_ep = MagicMock()
        dest_ep = MagicMock()
        dest_ep.list_snapshots.side_effect = Exception("Connection failed")

        snapshot = MockSnapshot("root-20240102")
        parent = MockSnapshot("root-20240101")

        # Should not raise, should log warning and proceed
        result = check_parent_viability(
            snapshot, parent, source_ep, dest_ep, check_level="quick"
        )

        # Should assume viable since we can't verify
        assert result.status == ParentViability.VIABLE

    @patch("btrfs_backup_ng.core.verify._test_send_stream")
    def test_stream_check_success(self, mock_send_stream):
        """Stream-level check succeeds when send stream works."""
        source_ep = MagicMock()
        dest_ep = MagicMock()
        dest_ep.list_snapshots.return_value = [MockSnapshot("root-20240101")]

        snapshot = MockSnapshot("root-20240102")
        parent = MockSnapshot("root-20240101")

        result = check_parent_viability(
            snapshot, parent, source_ep, dest_ep, check_level="stream"
        )

        mock_send_stream.assert_called_once()
        assert result.status == ParentViability.VIABLE

    @patch("btrfs_backup_ng.core.verify._test_send_stream")
    def test_stream_check_failure(self, mock_send_stream):
        """Stream-level check fails when send stream fails."""
        mock_send_stream.side_effect = Exception("Send stream failed")

        source_ep = MagicMock()
        dest_ep = MagicMock()
        dest_ep.list_snapshots.return_value = [MockSnapshot("root-20240101")]

        snapshot = MockSnapshot("root-20240102")
        parent = MockSnapshot("root-20240101")

        result = check_parent_viability(
            snapshot, parent, source_ep, dest_ep, check_level="stream"
        )

        assert result.status == ParentViability.CORRUPTED
        assert result.fallback_to_full


class TestFindViableParent:
    """Tests for find_viable_parent function."""

    def test_no_present_snapshots(self):
        """Empty destination should recommend full send."""
        source_ep = MagicMock()
        dest_ep = MagicMock()
        snapshot = MockSnapshot("root-20240101")

        result = find_viable_parent(
            snapshot, [], source_ep, dest_ep, check_level="quick"
        )

        assert result.status == ParentViability.MISSING
        assert result.fallback_to_full
        assert result.parent_name is None

    def test_finds_best_parent(self):
        """Should find the most recent viable parent."""
        source_ep = MagicMock()
        dest_ep = MagicMock()
        dest_ep.list_snapshots.return_value = [
            MockSnapshot(
                "root-20240101", time.strptime("20240101-120000", "%Y%m%d-%H%M%S")
            ),
            MockSnapshot(
                "root-20240102", time.strptime("20240102-120000", "%Y%m%d-%H%M%S")
            ),
        ]

        present = [
            MockSnapshot(
                "root-20240101", time.strptime("20240101-120000", "%Y%m%d-%H%M%S")
            ),
            MockSnapshot(
                "root-20240102", time.strptime("20240102-120000", "%Y%m%d-%H%M%S")
            ),
        ]
        snapshot = MockSnapshot(
            "root-20240103", time.strptime("20240103-120000", "%Y%m%d-%H%M%S")
        )

        result = find_viable_parent(
            snapshot, present, source_ep, dest_ep, check_level="quick"
        )

        assert result.status == ParentViability.VIABLE
        # Should pick most recent (20240102)
        assert result.parent_name == "root-20240102"

    def test_no_older_candidates(self):
        """No older snapshots should recommend full send."""
        source_ep = MagicMock()
        dest_ep = MagicMock()

        # All present snapshots are newer
        present = [
            MockSnapshot(
                "root-20240105", time.strptime("20240105-120000", "%Y%m%d-%H%M%S")
            ),
        ]
        snapshot = MockSnapshot(
            "root-20240101", time.strptime("20240101-120000", "%Y%m%d-%H%M%S")
        )

        result = find_viable_parent(
            snapshot, present, source_ep, dest_ep, check_level="quick"
        )

        assert result.status == ParentViability.MISSING
        assert result.fallback_to_full

    def test_fallback_to_older_parent(self):
        """If best parent is missing, try older ones."""
        source_ep = MagicMock()
        dest_ep = MagicMock()

        # Only the oldest parent exists at destination
        dest_ep.list_snapshots.return_value = [
            MockSnapshot(
                "root-20240101", time.strptime("20240101-120000", "%Y%m%d-%H%M%S")
            ),
        ]

        present = [
            MockSnapshot(
                "root-20240101", time.strptime("20240101-120000", "%Y%m%d-%H%M%S")
            ),
            MockSnapshot(
                "root-20240102", time.strptime("20240102-120000", "%Y%m%d-%H%M%S")
            ),
        ]
        snapshot = MockSnapshot(
            "root-20240103", time.strptime("20240103-120000", "%Y%m%d-%H%M%S")
        )

        result = find_viable_parent(
            snapshot, present, source_ep, dest_ep, check_level="quick"
        )

        # Should fall back to 20240101 since 20240102 is missing at dest
        assert result.status == ParentViability.VIABLE
        assert result.parent_name == "root-20240101"


class TestValidateTransferChain:
    """Tests for validate_transfer_chain function."""

    def test_validates_multiple_snapshots(self):
        """Should validate parent for each snapshot in order."""
        source_ep = MagicMock()
        dest_ep = MagicMock()
        dest_ep.list_snapshots.return_value = []

        present = []
        to_transfer = [
            MockSnapshot(
                "root-20240101", time.strptime("20240101-120000", "%Y%m%d-%H%M%S")
            ),
            MockSnapshot(
                "root-20240102", time.strptime("20240102-120000", "%Y%m%d-%H%M%S")
            ),
            MockSnapshot(
                "root-20240103", time.strptime("20240103-120000", "%Y%m%d-%H%M%S")
            ),
        ]

        results = validate_transfer_chain(
            to_transfer, present, source_ep, dest_ep, check_level="quick"
        )

        assert len(results) == 3

        # First snapshot has no parent (full send)
        snap1, result1 = results[0]
        assert result1.fallback_to_full

        # Subsequent snapshots can use previous as parent
        # (they would be in will_be_present after simulated transfer)

    def test_tracks_transferred_snapshots(self):
        """Should track what will be present after each transfer."""
        source_ep = MagicMock()
        dest_ep = MagicMock()
        # All snapshots will be "present" at dest after they're transferred
        dest_ep.list_snapshots.return_value = [
            MockSnapshot(
                "root-20240101", time.strptime("20240101-120000", "%Y%m%d-%H%M%S")
            ),
            MockSnapshot(
                "root-20240102", time.strptime("20240102-120000", "%Y%m%d-%H%M%S")
            ),
            MockSnapshot(
                "root-20240103", time.strptime("20240103-120000", "%Y%m%d-%H%M%S")
            ),
        ]

        present = [
            MockSnapshot(
                "root-20240101", time.strptime("20240101-120000", "%Y%m%d-%H%M%S")
            ),
        ]
        to_transfer = [
            MockSnapshot(
                "root-20240102", time.strptime("20240102-120000", "%Y%m%d-%H%M%S")
            ),
            MockSnapshot(
                "root-20240103", time.strptime("20240103-120000", "%Y%m%d-%H%M%S")
            ),
        ]

        results = validate_transfer_chain(
            to_transfer, present, source_ep, dest_ep, check_level="quick"
        )

        # First can use 20240101 as parent
        _, result1 = results[0]
        assert result1.parent_name == "root-20240101"

        # Second can use 20240102 (which will be present after first transfer)
        _, result2 = results[1]
        assert result2.parent_name == "root-20240102"


# =============================================================================
# verify_raw_checksums (R8a): raw-target checksum verification branches
# =============================================================================
from btrfs_backup_ng.core.verify import verify_raw_checksums  # noqa: E402
from btrfs_backup_ng.endpoint.raw_metadata import ChecksumVerdict  # noqa: E402


class _FakeRawEndpoint:
    """Minimal raw endpoint: returns preset snapshots and a preset verdict per name."""

    def __init__(self, verdicts, path="/raw", raises=None):
        # verdicts: dict name -> ChecksumVerdict
        self._verdicts = verdicts
        self.config = {"path": path}
        self._raises = raises

    def list_snapshots(self):
        if self._raises:
            raise self._raises
        return [MockSnapshot(name) for name in self._verdicts]

    def verify_stream_checksum(self, snap):
        return self._verdicts[snap.get_name()]


def _v(status, recorded="a" * 64, computed=None, remote=False):
    return ChecksumVerdict(status, recorded, computed, "sha256", remote)


def test_raw_checksums_empty_target_reports_error():
    """No snapshots -> a reported error (exit 2 upstream), not a silent empty pass."""
    ep = _FakeRawEndpoint({})
    report = verify_raw_checksums(ep, VerifyLevel.STREAM)
    assert report.errors == ["No snapshots found at backup location"]
    assert report.total == 0


def test_raw_checksums_snapshot_filter_selects_one():
    """--snapshot NAME verifies only that stream."""
    ep = _FakeRawEndpoint(
        {"a": _v("ok", computed="a" * 64), "b": _v("corrupt", computed="b" * 64)}
    )
    report = verify_raw_checksums(ep, VerifyLevel.STREAM, snapshot_name="a")
    assert report.total == 1
    assert report.results[0].snapshot_name == "a"
    assert report.results[0].passed is True


def test_raw_checksums_snapshot_filter_missing_reports_error():
    """--snapshot NAME for an absent snapshot -> reported error, no results."""
    ep = _FakeRawEndpoint({"a": _v("ok", computed="a" * 64)})
    report = verify_raw_checksums(ep, VerifyLevel.STREAM, snapshot_name="nope")
    assert report.total == 0
    assert any("nope" in e for e in report.errors)


def test_raw_checksums_invokes_on_progress():
    """The on_progress callback fires once per verified snapshot."""
    ep = _FakeRawEndpoint({"a": _v("ok", computed="a" * 64)})
    seen = []
    verify_raw_checksums(
        ep, VerifyLevel.STREAM, on_progress=lambda i, n, name: seen.append((i, n, name))
    )
    assert seen == [(1, 1, "a")]


def test_raw_checksums_error_status_fails_with_message():
    """An unreadable stream -> error verdict -> failed result with an explanatory message."""
    ep = _FakeRawEndpoint({"a": _v("error", computed=None)})
    report = verify_raw_checksums(ep, VerifyLevel.STREAM)
    r = report.results[0]
    assert r.passed is False
    assert "could not be read" in r.message
    assert report.failed == 1


def test_raw_checksums_unverifiable_is_not_a_failure():
    """No recorded checksum -> unverifiable -> passes (not a failure) with a clear message."""
    ep = _FakeRawEndpoint({"a": ChecksumVerdict("unverifiable", None, None, "sha256")})
    report = verify_raw_checksums(ep, VerifyLevel.STREAM)
    r = report.results[0]
    assert r.passed is True
    assert "Unverifiable" in r.message
    assert report.failed == 0


def test_raw_checksums_remote_ok_carries_trust_caveat():
    """A raw+ssh 'ok' verdict passes AND its message/details carry the consistency-only
    caveat (the digest came from the untrusted remote)."""
    ep = _FakeRawEndpoint({"a": _v("ok", computed="a" * 64, remote=True)})
    report = verify_raw_checksums(ep, VerifyLevel.STREAM)
    r = report.results[0]
    assert r.passed is True
    assert "consistency-only" in r.message
    assert r.details["trust"] == "consistency-only (remote hash)"


def test_raw_checksums_list_failure_is_reported_not_raised():
    """A list_snapshots failure is captured as a report error (exit 2 upstream), never
    propagated as an unhandled exception."""
    ep = _FakeRawEndpoint({}, raises=OSError("mount gone"))
    report = verify_raw_checksums(ep, VerifyLevel.STREAM)
    assert any("mount gone" in e for e in report.errors)
    assert report.total == 0


# =============================================================================
# verify_metadata STRUCTURAL validation (R8b): real filesystem objects, no mocks.
# These exercise the actual endpoint.verify_structure + authoritative parent check
# against real directories / real raw streams -- the audit's whole point was that the
# old mock tests hid a byte-blind, tautological metadata level.
# =============================================================================
from btrfs_backup_ng.core.verify import verify_metadata as _verify_metadata  # noqa: E402
from btrfs_backup_ng.endpoint.local import LocalEndpoint  # noqa: E402
from btrfs_backup_ng.endpoint.raw import RawEndpoint  # noqa: E402


def _build_raw_backup(path, name):
    """Write a real raw backup (stream + authoritative .meta sidecar) at path."""
    ep = RawEndpoint(config={"path": str(path)})
    src = path / (name + ".src")
    src.write_bytes(b"r8b-structural-" * 200)
    with open(src, "rb") as f:
        ep.receive(f, snapshot_name=name).communicate()
    ep.commit_receive()
    src.unlink()


class TestVerifyMetadataStructural:
    """R8b: metadata level validates real structure (F1) and authoritative parent
    continuity (F2), against real filesystem objects."""

    def test_local_plain_directory_fails_as_invalid(self, tmp_path):
        """F1 end-to-end: a real LocalEndpoint over a directory holding plain (non-
        subvolume) directories named like snapshots -> every entry FAILS as 'invalid'.
        Before R8b these passed with a green 'All verifications passed'. Mutation guard:
        revert verify_structure to hardcode exists=True and this reports all passed."""
        backup = tmp_path / "backup"
        backup.mkdir()
        (backup / "home-20260101-120000").mkdir()  # interrupted-receive leftover
        (backup / "home-20260102-120000").mkdir()
        ep = LocalEndpoint(config={"path": str(backup), "snap_prefix": "home-"})

        report = _verify_metadata(ep)

        assert report.total == 2
        assert report.failed == 2 and report.passed == 0
        assert all("not a btrfs subvolume" in r.message for r in report.results)
        assert all(r.details["structure"] == "invalid" for r in report.results)

    def test_local_verify_structure_direct_on_plain_dir(self, tmp_path):
        """The polymorphic check itself: LocalEndpoint.verify_structure on a real plain
        directory -> invalid (a privilege-free inode check, no root needed)."""
        backup = tmp_path / "b"
        backup.mkdir()
        (backup / "home-20260101-120000").mkdir()
        ep = LocalEndpoint(config={"path": str(backup), "snap_prefix": "home-"})
        (snap,) = ep.list_snapshots(flush_cache=True)

        verdict = ep.verify_structure(snap)

        assert verdict.status == "invalid" and verdict.is_failure

    def test_local_is_subvolume_permission_error_is_unverifiable_not_fail(
        self, tmp_path, monkeypatch
    ):
        """The false-negative-safety path: if is_subvolume cannot even stat the path (e.g.
        permission denied), the entry is 'unverifiable' (passed=True), NEVER 'invalid' --
        a good backup must not be failed just because the environment could not check it.
        Mutation guard: removing the OSError catch (letting it raise / marking invalid)
        breaks this."""
        import btrfs_backup_ng.endpoint.common as common_mod

        backup = tmp_path / "b"
        backup.mkdir()
        (backup / "home-20260101-120000").mkdir()
        ep = LocalEndpoint(config={"path": str(backup), "snap_prefix": "home-"})
        (snap,) = ep.list_snapshots(flush_cache=True)

        def _denied(_path):
            raise OSError(13, "Permission denied")

        monkeypatch.setattr(common_mod.__util__, "is_subvolume", _denied)

        verdict = ep.verify_structure(snap)

        assert verdict.status == "unverifiable"
        assert verdict.is_failure is False

    def test_raw_authoritative_sidecar_passes(self, tmp_path):
        """A raw backup with a real native-write sidecar -> structure ok -> passes."""
        _build_raw_backup(tmp_path, "a.20260101T120000")
        ep = RawEndpoint(config={"path": str(tmp_path)})

        report = _verify_metadata(ep)

        assert report.passed == 1 and report.failed == 0
        assert report.results[0].details["structure"] == "ok"

    def test_raw_filename_inferred_is_unverifiable_not_silent_pass(self, tmp_path):
        """A raw stream with NO .meta sidecar (metadata inferred from the filename) is
        'unverifiable' -- reported explicitly, not a silent clean pass and not a failure.
        Mutation guard: treating filename-inferred as 'ok' loses the distinction."""
        _build_raw_backup(tmp_path, "legacy.20260101T120000")
        # Drop the sidecar so discovery falls back to filename inference.
        (tmp_path / "legacy.20260101T120000.btrfs.meta").unlink()
        ep = RawEndpoint(config={"path": str(tmp_path)})

        report = _verify_metadata(ep)

        assert report.total == 1
        r = report.results[0]
        assert r.passed is True  # unverifiable is not a failure
        assert r.details["structure"] == "unverifiable"
        assert "no .meta sidecar" in r.message

    def test_raw_missing_incremental_parent_fails(self, tmp_path):
        """F2 end-to-end: a raw snapshot whose sidecar records a parent_name that is NOT
        present at the target -> FAIL 'missing incremental parent' (an unrestorable chain
        break). This branch was UNREACHABLE before R8b (the tautology); the test proves it
        now fires. Mutation guard: dropping the parent-continuity check makes this pass."""
        import json

        _build_raw_backup(tmp_path, "child.20260102T120000")
        meta = tmp_path / "child.20260102T120000.btrfs.meta"
        doc = json.loads(meta.read_text())
        doc["parent_name"] = (
            "ghost.20250101T000000"  # a parent that is not on the target
        )
        meta.write_text(json.dumps(doc))
        ep = RawEndpoint(config={"path": str(tmp_path)})

        report = _verify_metadata(ep)

        r = report.results[0]
        assert r.passed is False
        assert "missing incremental parent: ghost.20250101T000000" in r.message
        assert r.details["parent_missing"] is True

    def test_raw_present_incremental_parent_passes(self, tmp_path):
        """A valid raw chain (parent_name points at a snapshot that IS present) -> PASS.
        Guards against false-failing a genuinely intact incremental chain."""
        import json

        _build_raw_backup(tmp_path, "base.20260101T120000")
        _build_raw_backup(tmp_path, "child.20260102T120000")
        meta = tmp_path / "child.20260102T120000.btrfs.meta"
        doc = json.loads(meta.read_text())
        doc["parent_name"] = "base.20260101T120000"  # present at the target
        meta.write_text(json.dumps(doc))
        ep = RawEndpoint(config={"path": str(tmp_path)})

        report = _verify_metadata(ep)

        assert report.failed == 0 and report.passed == 2
