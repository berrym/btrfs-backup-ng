"""Tests for backup verification functionality."""

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict
from btrfs_backup_ng.core.verify import (
    VerifyError,
    VerifyLevel,
    VerifyReport,
    VerifyResult,
    _find_parent_snapshot,
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

    def test_verify_latest_by_default(self):
        """Test that only latest snapshot is verified by default. (verify_stream calls
        the endpoint's polymorphic test_send_stream, auto-mocked to succeed here.)"""
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

    def test_verify_specific_snapshot(self):
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

    def test_stream_success(self):
        """Test successful stream verification (endpoint.test_send_stream returns without
        raising)."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
        mock_endpoint.config = {"path": "/backup"}

        report = verify_stream(mock_endpoint)

        assert report.total == 1
        assert report.passed == 1
        assert report.results[0].passed is True
        assert "verified successfully" in report.results[0].message

    def test_stream_failure(self):
        """Test failed stream verification: endpoint.test_send_stream raising -> the
        snapshot fails."""
        mock_endpoint = MagicMock()
        mock_endpoint.list_snapshots.return_value = [MockSnapshot("snap-1")]
        mock_endpoint.config = {"path": "/backup"}
        mock_endpoint.test_send_stream.side_effect = Exception("Stream error")

        report = verify_stream(mock_endpoint)

        assert report.total == 1
        assert report.failed == 1
        assert report.results[0].passed is False
        assert "failed" in report.results[0].message

    def test_progress_callback(self):
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

    def test_incremental_detection(self):
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


class TestEndpointTestSendStream:
    """R8c: the polymorphic endpoint.test_send_stream replaces the deleted module-level
    _test_send_stream (and its dead ssh_client branch). Local runs `btrfs send --no-data`
    locally; SSH runs it ON THE REMOTE via _exec_remote_command -- the exact bug the dead
    branch caused was ssh:// stream verify running btrfs send LOCALLY against a remote
    path; raw rejects (a stored stream is not a subvolume)."""

    def _local_ep(self):
        from btrfs_backup_ng.endpoint.local import LocalEndpoint

        return LocalEndpoint(config={"path": "/backup", "snap_prefix": ""})

    def test_local_runs_btrfs_send_locally(self, monkeypatch):
        """LocalEndpoint.test_send_stream runs `btrfs send --no-data <path>` via a local
        subprocess; returncode 0 -> passes."""
        import btrfs_backup_ng.endpoint.common as common_mod
        from types import SimpleNamespace

        calls = {}

        def fake_run(cmd, **kw):
            calls["cmd"] = cmd
            return SimpleNamespace(returncode=0, stderr=b"")

        monkeypatch.setattr(common_mod.subprocess, "run", fake_run)
        snap = SimpleNamespace(get_path=lambda: Path("/backup/snap-1"))

        self._local_ep().test_send_stream(snap)  # must not raise

        assert calls["cmd"][:3] == ["btrfs", "send", "--no-data"]
        assert calls["cmd"][-1] == "/backup/snap-1"

    def test_local_nonzero_raises(self, monkeypatch):
        """A non-zero `btrfs send` -> VerifyError."""
        import btrfs_backup_ng.endpoint.common as common_mod
        from types import SimpleNamespace

        monkeypatch.setattr(
            common_mod.subprocess,
            "run",
            lambda cmd, **kw: SimpleNamespace(
                returncode=1, stderr=b"unable to resolve"
            ),
        )
        snap = SimpleNamespace(get_path=lambda: Path("/backup/s"))
        with pytest.raises(VerifyError):
            self._local_ep().test_send_stream(snap)

    def test_local_incremental_adds_parent_flag(self, monkeypatch):
        """An incremental test adds `-p <parent>` before the snapshot path."""
        import btrfs_backup_ng.endpoint.common as common_mod
        from types import SimpleNamespace

        calls = {}

        def fake_run(cmd, **kw):
            calls["cmd"] = cmd
            return SimpleNamespace(returncode=0, stderr=b"")

        monkeypatch.setattr(common_mod.subprocess, "run", fake_run)
        snap = SimpleNamespace(get_path=lambda: Path("/backup/child"))
        parent = SimpleNamespace(get_path=lambda: Path("/backup/base"))

        self._local_ep().test_send_stream(snap, parent)

        assert "-p" in calls["cmd"] and "/backup/base" in calls["cmd"]

    def _ssh_ep(self, ssh_sudo=False):
        import btrfs_backup_ng.endpoint.ssh as ssh_mod

        ep = ssh_mod.SSHEndpoint.__new__(ssh_mod.SSHEndpoint)  # avoid a live connection
        ep.config = {"path": "/backups", "ssh_sudo": ssh_sudo}
        ep._normalize_path = lambda pth: str(pth)
        return ep

    def test_ssh_dispatches_to_remote_exec_not_local_subprocess(self, monkeypatch):
        """THE dead-branch fix: SSHEndpoint.test_send_stream runs `btrfs send --no-data`
        via _exec_remote_command (ON THE REMOTE) and NEVER via a local subprocess.run.
        Mutation guard: routing it to a local subprocess (the old fallthrough) trips the
        tripwire below."""
        import btrfs_backup_ng.endpoint.ssh as ssh_mod
        from types import SimpleNamespace

        ep = self._ssh_ep()
        remote = {}

        def fake_exec(cmd, **kw):
            remote["cmd"] = cmd
            return SimpleNamespace(returncode=0, stderr=b"")

        ep._exec_remote_command = fake_exec

        def boom(*a, **k):
            raise AssertionError("must not run btrfs send LOCALLY for an ssh target")

        monkeypatch.setattr(ssh_mod.subprocess, "run", boom)

        snap = SimpleNamespace(get_path=lambda: Path("/backups/snap-1"))
        ep.test_send_stream(snap)  # must not raise, must not run locally

        assert remote["cmd"][:3] == ["btrfs", "send", "--no-data"]
        assert "/backups/snap-1" in remote["cmd"]

    def test_ssh_nonzero_remote_raises(self):
        """A non-zero remote send -> VerifyError (surfacing the remote stderr)."""
        from types import SimpleNamespace

        ep = self._ssh_ep()
        ep._exec_remote_command = lambda cmd, **kw: SimpleNamespace(
            returncode=1, stderr=b"unable to resolve"
        )
        snap = SimpleNamespace(get_path=lambda: Path("/backups/s"))
        with pytest.raises(VerifyError) as e:
            ep.test_send_stream(snap)
        assert "remote" in str(e.value).lower()

    def test_ssh_empty_stderr_gives_informative_message(self):
        """A non-zero remote send with EMPTY stderr -> a clear message (not a crash, not a
        blank), since stderr is bytes b'' (falsy). Guards the robust-decode fix."""
        from types import SimpleNamespace

        ep = self._ssh_ep()
        ep._exec_remote_command = lambda cmd, **kw: SimpleNamespace(
            returncode=3, stderr=b""
        )
        with pytest.raises(VerifyError) as e:
            ep.test_send_stream(SimpleNamespace(get_path=lambda: Path("/backups/s")))
        msg = str(e.value)
        # Informative even with no remote stderr: names the returncode (the old truthiness
        # guard produced a bare "unknown error" with no such context).
        assert "unknown error" in msg and "returned 3" in msg

    def test_ssh_transient_error_propagates_not_wrapped(self):
        """A transient SSH/timeout fault propagates as ITSELF (not disguised as a
        VerifyError 'stream test failed'): a can't-reach-the-remote condition is not a
        corrupt-snapshot verdict. Guards the transient-passthrough fix."""
        import subprocess as _sp
        from types import SimpleNamespace

        ep = self._ssh_ep()

        def _timeout(cmd, **kw):
            raise _sp.TimeoutExpired(cmd, 300)

        ep._exec_remote_command = _timeout
        with pytest.raises(_sp.TimeoutExpired):
            ep.test_send_stream(SimpleNamespace(get_path=lambda: Path("/backups/s")))

    def test_ssh_sudo_uses_retry_variant(self):
        """With ssh_sudo, the auth-aware retry variant is used, not the plain exec."""
        from types import SimpleNamespace

        ep = self._ssh_ep(ssh_sudo=True)
        used = {}
        ep._exec_remote_command_with_retry = lambda cmd, **kw: (
            used.__setitem__("retry", True),
            SimpleNamespace(returncode=0, stderr=b""),
        )[1]
        ep._exec_remote_command = lambda cmd, **kw: (
            used.__setitem__("plain", True),
            SimpleNamespace(returncode=0, stderr=b""),
        )[1]

        ep.test_send_stream(SimpleNamespace(get_path=lambda: Path("/backups/s")))

        assert used.get("retry") is True and "plain" not in used

    def test_raw_rejects_as_not_a_subvolume(self):
        """RawEndpoint.test_send_stream raises (a stored stream is not a subvolume; raw
        integrity is verified by checksum)."""
        from btrfs_backup_ng.endpoint.raw import RawEndpoint
        from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

        ep = RawEndpoint(config={"path": "/raw"})
        snap = RawSnapshot(name="s", stream_path=Path("/raw/s.btrfs"))
        with pytest.raises(VerifyError) as e:
            ep.test_send_stream(snap)
        assert "checksum" in str(e.value).lower()


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

    def test_local_verify_structure_ok_and_unverifiable_by_received_uuid(
        self, tmp_path, monkeypatch
    ):
        """With is_subvolume True (a real subvolume), the received_uuid decides: non-empty
        -> ok (a received backup); empty -> unverifiable (a subvolume, but not a received
        backup). Covers the local ok/unverifiable branches without root; the tier2 suite
        proves the same against a REAL received subvolume. Mutation guard: dropping the
        received_uuid gate makes the empty case report ok."""
        import btrfs_backup_ng.endpoint.common as common_mod
        from types import SimpleNamespace

        backup = tmp_path / "b"
        backup.mkdir()
        ep = LocalEndpoint(config={"path": str(backup), "snap_prefix": "home-"})
        monkeypatch.setattr(common_mod.__util__, "is_subvolume", lambda _p: True)

        received = SimpleNamespace(
            get_path=lambda: backup / "recv", received_uuid="uuid-1234"
        )
        non_received = SimpleNamespace(
            get_path=lambda: backup / "local", received_uuid=""
        )

        assert ep.verify_structure(received).status == "ok"
        verdict = ep.verify_structure(non_received)
        assert verdict.status == "unverifiable" and verdict.is_failure is False

    def test_local_verify_structure_unresolvable_path_is_unverifiable(self, tmp_path):
        """A snapshot whose path cannot be resolved -> unverifiable (not a crash, not an
        invalid): a snapshot with no resolvable path is simply not ours to judge."""
        from types import SimpleNamespace

        ep = LocalEndpoint(config={"path": str(tmp_path), "snap_prefix": "home-"})

        def _boom():
            raise RuntimeError("no path")

        verdict = ep.verify_structure(SimpleNamespace(get_path=_boom))
        assert verdict.status == "unverifiable" and verdict.is_failure is False

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
