"""Tests for backup verification functionality."""

import time
from unittest.mock import MagicMock


from btrfs_backup_ng.core.verify import (
    VerifyLevel,
    VerifyReport,
    VerifyResult,
    _find_parent_snapshot,
    verify_metadata,
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
