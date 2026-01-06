"""Tests for backup size estimation functionality."""

import subprocess
from unittest.mock import MagicMock, patch

from btrfs_backup_ng.core.estimate import (
    SnapshotEstimate,
    TransferEstimate,
    _parse_size,
    estimate_incremental_size,
    estimate_snapshot_full_size,
    estimate_transfer,
    format_size,
    print_estimate,
)


class TestFormatSize:
    """Tests for format_size function."""

    def test_format_bytes(self):
        """Test formatting bytes."""
        assert format_size(500) == "500 B"
        assert format_size(0) == "0 B"

    def test_format_kibibytes(self):
        """Test formatting KiB."""
        assert format_size(1024) == "1.00 KiB"
        assert format_size(2048) == "2.00 KiB"
        assert format_size(1536) == "1.50 KiB"

    def test_format_mebibytes(self):
        """Test formatting MiB."""
        assert format_size(1024**2) == "1.00 MiB"
        assert format_size(5 * 1024**2) == "5.00 MiB"

    def test_format_gibibytes(self):
        """Test formatting GiB."""
        assert format_size(1024**3) == "1.00 GiB"
        assert format_size(2.5 * 1024**3) == "2.50 GiB"

    def test_format_tebibytes(self):
        """Test formatting TiB."""
        assert format_size(1024**4) == "1.00 TiB"

    def test_format_none(self):
        """Test formatting None."""
        assert format_size(None) == "unknown"


class TestParseSize:
    """Tests for _parse_size function."""

    def test_parse_bytes(self):
        """Test parsing bytes."""
        assert _parse_size("100") == 100
        assert _parse_size("100B") == 100

    def test_parse_kibibytes(self):
        """Test parsing KiB."""
        assert _parse_size("1KiB") == 1024
        assert _parse_size("2.5KiB") == 2560

    def test_parse_mebibytes(self):
        """Test parsing MiB."""
        assert _parse_size("1MiB") == 1024**2
        assert _parse_size("1.5MiB") == int(1.5 * 1024**2)

    def test_parse_gibibytes(self):
        """Test parsing GiB."""
        assert _parse_size("1GiB") == 1024**3
        assert _parse_size("2GiB") == 2 * 1024**3

    def test_parse_tebibytes(self):
        """Test parsing TiB."""
        assert _parse_size("1TiB") == 1024**4

    def test_parse_si_units(self):
        """Test parsing SI units (KB, MB, GB, TB)."""
        assert _parse_size("1KB") == 1000
        assert _parse_size("1MB") == 1000**2
        assert _parse_size("1GB") == 1000**3
        assert _parse_size("1TB") == 1000**4

    def test_parse_invalid(self):
        """Test parsing invalid strings."""
        assert _parse_size("invalid") is None
        assert _parse_size("") is None


class TestSnapshotEstimate:
    """Tests for SnapshotEstimate dataclass."""

    def test_basic_estimate(self):
        """Test creating a basic estimate."""
        est = SnapshotEstimate(
            name="snap-1",
            full_size=1024**3,
            method="filesystem_du",
        )

        assert est.name == "snap-1"
        assert est.full_size == 1024**3
        assert est.incremental_size is None
        assert est.is_incremental is False
        assert est.method == "filesystem_du"

    def test_incremental_estimate(self):
        """Test incremental estimate."""
        est = SnapshotEstimate(
            name="snap-2",
            full_size=1024**3,
            incremental_size=50 * 1024**2,
            parent_name="snap-1",
            is_incremental=True,
            method="send_no_data",
        )

        assert est.is_incremental is True
        assert est.incremental_size == 50 * 1024**2
        assert est.parent_name == "snap-1"


class TestTransferEstimate:
    """Tests for TransferEstimate dataclass."""

    def test_empty_estimate(self):
        """Test empty estimate."""
        est = TransferEstimate()

        assert est.snapshot_count == 0
        assert est.new_snapshot_count == 0
        assert est.skipped_count == 0
        assert est.total_full_size == 0
        assert est.total_incremental_size == 0
        assert len(est.snapshots) == 0

    def test_add_full_snapshot(self):
        """Test adding a full snapshot."""
        est = TransferEstimate()
        snap = SnapshotEstimate(
            name="snap-1",
            full_size=1024**3,
        )

        est.add_snapshot(snap)

        assert est.snapshot_count == 1
        assert est.total_full_size == 1024**3
        assert est.total_incremental_size == 1024**3  # Full = incremental for first

    def test_add_incremental_snapshot(self):
        """Test adding an incremental snapshot."""
        est = TransferEstimate()

        # Add full snapshot
        snap1 = SnapshotEstimate(name="snap-1", full_size=1024**3)
        est.add_snapshot(snap1)

        # Add incremental snapshot
        snap2 = SnapshotEstimate(
            name="snap-2",
            full_size=1024**3,
            incremental_size=50 * 1024**2,
            is_incremental=True,
            parent_name="snap-1",
        )
        est.add_snapshot(snap2)

        assert est.snapshot_count == 2
        assert est.total_full_size == 2 * 1024**3
        # Incremental only counts the delta
        assert est.total_incremental_size == 1024**3 + 50 * 1024**2

    def test_multiple_snapshots(self):
        """Test adding multiple snapshots."""
        est = TransferEstimate()

        for i in range(5):
            snap = SnapshotEstimate(
                name=f"snap-{i}",
                full_size=100 * 1024**2,
            )
            est.add_snapshot(snap)

        assert est.snapshot_count == 5
        assert est.total_full_size == 500 * 1024**2


class TestEstimateSnapshotFullSize:
    """Tests for estimate_snapshot_full_size function."""

    @patch("subprocess.run")
    def test_subvolume_show_success(self, mock_run, tmp_path):
        """Test successful estimation via btrfs subvolume show."""
        from btrfs_backup_ng.core.estimate import estimate_snapshot_full_size

        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="""snapshot-1
\tName: \t\t\tsnapshot-1
\tUUID: \t\t\t12345678-1234-1234-1234-123456789abc
\tParent UUID: \t\t-
\tReceived UUID: \t\t-
\tCreation time: \t\t2026-01-01 12:00:00 +0000
\tSubvolume ID: \t\t256
\tGeneration: \t\t1234
\tGen at creation: \t1234
\tParent ID: \t\t5
\tTop level ID: \t\t5
\tFlags: \t\t\treadonly
\tSnapshot(s):
\tExclusive: \t\t2.50GiB
""",
        )

        size, method = estimate_snapshot_full_size(tmp_path / "snap")

        assert size == int(2.5 * 1024**3)
        assert method == "subvolume_show"

    @patch("subprocess.run")
    def test_filesystem_du_fallback(self, mock_run, tmp_path):
        """Test fallback to btrfs filesystem du."""
        from btrfs_backup_ng.core.estimate import estimate_snapshot_full_size

        # First call (subvolume show) fails, second (filesystem du) succeeds
        mock_run.side_effect = [
            MagicMock(returncode=1, stdout="", stderr="error"),
            MagicMock(
                returncode=0,
                stdout="Total   Exclusive  Set shared  Filename\n1073741824  536870912  536870912  /snap\n",
            ),
        ]

        size, method = estimate_snapshot_full_size(tmp_path / "snap")

        assert size == 1073741824
        assert method == "filesystem_du"

    @patch("subprocess.run")
    def test_du_fallback(self, mock_run, tmp_path):
        """Test fallback to regular du."""
        from btrfs_backup_ng.core.estimate import estimate_snapshot_full_size

        # All btrfs commands fail, du succeeds
        mock_run.side_effect = [
            MagicMock(returncode=1, stdout="", stderr="error"),
            MagicMock(returncode=1, stdout="", stderr="error"),
            MagicMock(returncode=0, stdout="1073741824\t/snap\n"),
        ]

        size, method = estimate_snapshot_full_size(tmp_path / "snap")

        assert size == 1073741824
        assert method == "du"

    @patch("subprocess.run")
    def test_all_methods_fail(self, mock_run, tmp_path):
        """Test when all estimation methods fail."""
        from btrfs_backup_ng.core.estimate import estimate_snapshot_full_size

        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")

        size, method = estimate_snapshot_full_size(tmp_path / "snap")

        assert size is None
        assert method == "failed"


class TestEstimateIncrementalSize:
    """Tests for estimate_incremental_size function."""

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_send_no_data_success(self, mock_euid, mock_run, tmp_path):
        """Test successful estimation via btrfs send --no-data."""
        # Return a mock stdout with some bytes to represent stream size
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=b"x" * 50000,  # 50KB mock stream
        )

        size, method = estimate_incremental_size(tmp_path / "snap2", tmp_path / "snap1")

        assert size == 50000
        assert method == "send_no_data"
        # Verify the command was called correctly
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "btrfs" in cmd
        assert "send" in cmd
        assert "--no-data" in cmd
        assert "-p" in cmd

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=1000)
    def test_send_no_data_with_sudo(self, mock_euid, mock_run, tmp_path):
        """Test estimation uses sudo when not root."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=b"x" * 1000,
        )

        size, method = estimate_incremental_size(
            tmp_path / "snap2", tmp_path / "snap1", use_sudo=True
        )

        assert method == "send_no_data"
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "sudo"
        assert cmd[1] == "-n"

    @patch("btrfs_backup_ng.core.estimate.estimate_snapshot_full_size")
    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_fallback_to_size_diff(self, mock_euid, mock_run, mock_full_size, tmp_path):
        """Test fallback to size difference when send --no-data fails."""
        # send --no-data fails
        mock_run.return_value = MagicMock(returncode=1, stdout=b"", stderr=b"error")

        # Full size estimation succeeds
        mock_full_size.side_effect = [
            (1024 * 1024 * 100, "du"),  # snap2: 100 MiB
            (1024 * 1024 * 80, "du"),  # snap1: 80 MiB
        ]

        size, method = estimate_incremental_size(tmp_path / "snap2", tmp_path / "snap1")

        assert size == 1024 * 1024 * 20  # 20 MiB difference
        assert method == "size_diff"

    @patch("btrfs_backup_ng.core.estimate.estimate_snapshot_full_size")
    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_size_diff_negative_returns_zero(
        self, mock_euid, mock_run, mock_full_size, tmp_path
    ):
        """Test that negative size diff returns 0."""
        mock_run.return_value = MagicMock(returncode=1, stdout=b"", stderr=b"error")

        # Parent is larger than snapshot (e.g., files deleted)
        mock_full_size.side_effect = [
            (1024 * 1024 * 50, "du"),  # snap2: 50 MiB
            (1024 * 1024 * 100, "du"),  # snap1: 100 MiB
        ]

        size, method = estimate_incremental_size(tmp_path / "snap2", tmp_path / "snap1")

        assert size == 0  # max(0, negative) = 0
        assert method == "size_diff"

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_timeout_failure(self, mock_euid, mock_run, tmp_path):
        """Test handling of timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired("btrfs", 300)

        size, method = estimate_incremental_size(tmp_path / "snap2", tmp_path / "snap1")

        assert size is None
        assert method == "failed"

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_file_not_found_failure(self, mock_euid, mock_run, tmp_path):
        """Test handling of FileNotFoundError (btrfs not installed)."""
        mock_run.side_effect = FileNotFoundError("btrfs not found")

        size, method = estimate_incremental_size(tmp_path / "snap2", tmp_path / "snap1")

        assert size is None
        assert method == "failed"


class TestEstimateSnapshotFullSizeAdvanced:
    """Additional tests for estimate_snapshot_full_size function."""

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=1000)
    def test_uses_sudo_when_not_root(self, mock_euid, mock_run, tmp_path):
        """Test that sudo is used when not running as root."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Exclusive:\t\t1.00GiB\n",
        )

        estimate_snapshot_full_size(tmp_path / "snap", use_sudo=True)

        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "sudo"
        assert cmd[1] == "-n"

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_no_sudo_when_root(self, mock_euid, mock_run, tmp_path):
        """Test that sudo is not used when running as root."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Exclusive:\t\t1.00GiB\n",
        )

        estimate_snapshot_full_size(tmp_path / "snap")

        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "btrfs"

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_timeout_handling(self, mock_euid, mock_run, tmp_path):
        """Test handling of command timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired("btrfs", 30)

        size, method = estimate_snapshot_full_size(tmp_path / "snap")

        assert size is None
        assert method == "failed"

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_exclusive_size_zero_falls_through(self, mock_euid, mock_run, tmp_path):
        """Test that zero exclusive size falls through to next method."""
        mock_run.side_effect = [
            # subvolume show returns 0 for exclusive
            MagicMock(returncode=0, stdout="Exclusive:\t\t0B\n"),
            # filesystem du succeeds
            MagicMock(returncode=0, stdout="Total\tExclusive\n12345\t12345\n"),
        ]

        size, method = estimate_snapshot_full_size(tmp_path / "snap")

        assert size == 12345
        assert method == "filesystem_du"

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_oserror_handling(self, mock_euid, mock_run, tmp_path):
        """Test handling of OSError."""
        mock_run.side_effect = OSError("Permission denied")

        size, method = estimate_snapshot_full_size(tmp_path / "snap")

        assert size is None
        assert method == "failed"

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_du_value_error_fallthrough(self, mock_euid, mock_run, tmp_path):
        """Test that non-integer du output falls through."""
        mock_run.side_effect = [
            MagicMock(returncode=1, stdout=""),  # subvolume show fails
            MagicMock(returncode=1, stdout=""),  # filesystem du fails
            MagicMock(returncode=0, stdout="not_a_number\t/path\n"),  # du bad output
        ]

        size, method = estimate_snapshot_full_size(tmp_path / "snap")

        assert size is None
        assert method == "failed"


class TestEstimateTransfer:
    """Tests for estimate_transfer function."""

    def test_empty_source(self):
        """Test with no snapshots at source."""
        source = MagicMock()
        source.list_snapshots.return_value = []
        source.config = {"path": "/source", "ssh_sudo": False}

        dest = MagicMock()
        dest.list_snapshots.return_value = []

        result = estimate_transfer(source, dest)

        assert result.snapshot_count == 0
        assert result.new_snapshot_count == 0
        assert result.skipped_count == 0

    def test_all_snapshots_at_destination(self):
        """Test when all snapshots already exist at destination."""
        snap1 = MagicMock()
        snap1.get_name.return_value = "snap-1"
        snap1.time_obj = 1000

        source = MagicMock()
        source.list_snapshots.return_value = [snap1]
        source.config = {"path": "/source", "ssh_sudo": False}

        dest = MagicMock()
        dest.list_snapshots.return_value = [snap1]

        result = estimate_transfer(source, dest)

        assert result.snapshot_count == 0
        assert result.skipped_count == 1

    @patch("btrfs_backup_ng.core.estimate.estimate_snapshot_full_size")
    def test_new_snapshot_full_transfer(self, mock_full_size, tmp_path):
        """Test estimating a new snapshot for full transfer."""
        snap1 = MagicMock()
        snap1.get_name.return_value = "snap-1"
        snap1.time_obj = 1000

        source = MagicMock()
        source.list_snapshots.return_value = [snap1]
        source.config = {"path": tmp_path, "ssh_sudo": False}

        dest = MagicMock()
        dest.list_snapshots.return_value = []

        mock_full_size.return_value = (1024 * 1024 * 100, "filesystem_du")

        result = estimate_transfer(source, dest)

        assert result.snapshot_count == 1
        assert result.new_snapshot_count == 1
        assert result.total_full_size == 1024 * 1024 * 100
        assert len(result.snapshots) == 1
        assert result.snapshots[0].name == "snap-1"

    @patch("btrfs_backup_ng.core.estimate.estimate_incremental_size")
    @patch("btrfs_backup_ng.core.estimate.estimate_snapshot_full_size")
    def test_incremental_transfer_estimation(
        self, mock_full_size, mock_incr_size, tmp_path
    ):
        """Test estimating incremental transfer between snapshots."""
        snap1 = MagicMock()
        snap1.get_name.return_value = "snap-1"
        snap1.time_obj = 1000

        snap2 = MagicMock()
        snap2.get_name.return_value = "snap-2"
        snap2.time_obj = 2000

        # Create real directory so path.exists() works
        (tmp_path / "snap-1").mkdir()

        source = MagicMock()
        source.list_snapshots.return_value = [snap1, snap2]
        source.config = {"path": tmp_path, "ssh_sudo": False}

        dest = MagicMock()
        dest.list_snapshots.return_value = []

        mock_full_size.return_value = (1024 * 1024 * 100, "filesystem_du")
        mock_incr_size.return_value = (1024 * 1024 * 10, "send_no_data")

        result = estimate_transfer(source, dest)

        assert result.snapshot_count == 2
        assert result.new_snapshot_count == 2
        # Second snapshot should be incremental
        assert result.snapshots[1].is_incremental is True
        assert result.snapshots[1].parent_name == "snap-1"

    def test_dest_list_snapshots_fails(self, tmp_path):
        """Test handling when destination list_snapshots raises exception."""
        snap1 = MagicMock()
        snap1.get_name.return_value = "snap-1"
        snap1.time_obj = 1000

        source = MagicMock()
        source.list_snapshots.return_value = [snap1]
        source.config = {"path": tmp_path, "ssh_sudo": False}

        dest = MagicMock()
        dest.list_snapshots.side_effect = Exception("Connection failed")

        # Should not raise, should treat as empty destination
        with patch(
            "btrfs_backup_ng.core.estimate.estimate_snapshot_full_size"
        ) as mock_full:
            mock_full.return_value = (1000, "du")
            result = estimate_transfer(source, dest)

        assert result.new_snapshot_count == 1

    def test_explicit_snapshot_list(self, tmp_path):
        """Test with explicit snapshot list instead of listing from source."""
        snap1 = MagicMock()
        snap1.get_name.return_value = "snap-1"
        snap1.time_obj = 1000

        source = MagicMock()
        source.config = {"path": tmp_path, "ssh_sudo": False}

        dest = MagicMock()
        dest.list_snapshots.return_value = []

        with patch(
            "btrfs_backup_ng.core.estimate.estimate_snapshot_full_size"
        ) as mock_full:
            mock_full.return_value = (5000, "du")
            result = estimate_transfer(source, dest, snapshots=[snap1])

        # Should use provided list, not call list_snapshots
        source.list_snapshots.assert_not_called()
        assert result.snapshot_count == 1

    def test_uses_ssh_sudo_from_config(self, tmp_path):
        """Test that ssh_sudo config is passed to estimation functions."""
        snap1 = MagicMock()
        snap1.get_name.return_value = "snap-1"
        snap1.time_obj = 1000

        source = MagicMock()
        source.list_snapshots.return_value = [snap1]
        source.config = {"path": tmp_path, "ssh_sudo": True}

        dest = MagicMock()
        dest.list_snapshots.return_value = []

        with patch(
            "btrfs_backup_ng.core.estimate.estimate_snapshot_full_size"
        ) as mock_full:
            mock_full.return_value = (1000, "du")
            estimate_transfer(source, dest)

            # Check use_sudo was passed (second positional arg)
            mock_full.assert_called_once()
            # Args are (snap_path, use_sudo)
            assert mock_full.call_args[0][1] is True


class TestPrintEstimate:
    """Tests for print_estimate function."""

    def test_print_empty_estimate(self, capsys):
        """Test printing estimate with no new snapshots."""
        estimate = TransferEstimate(skipped_count=5)

        print_estimate(estimate, "source", "dest")

        captured = capsys.readouterr()
        assert "source" in captured.out
        assert "dest" in captured.out
        assert "already at destination: 5" in captured.out
        assert "No new snapshots to transfer" in captured.out

    def test_print_full_snapshot(self, capsys):
        """Test printing estimate with full snapshot."""
        estimate = TransferEstimate()
        snap = SnapshotEstimate(
            name="snap-2024-01-01",
            full_size=1024 * 1024 * 500,  # 500 MiB
        )
        estimate.add_snapshot(snap)
        estimate.new_snapshot_count = 1

        print_estimate(estimate, "local", "remote")

        captured = capsys.readouterr()
        assert "snap-2024-01-01" in captured.out
        assert "full" in captured.out
        assert "MiB" in captured.out

    def test_print_incremental_snapshot(self, capsys):
        """Test printing estimate with incremental snapshot."""
        estimate = TransferEstimate()
        snap = SnapshotEstimate(
            name="snap-2024-01-02",
            full_size=1024 * 1024 * 500,
            incremental_size=1024 * 1024 * 50,
            parent_name="snap-2024-01-01",
            is_incremental=True,
        )
        estimate.add_snapshot(snap)
        estimate.new_snapshot_count = 1

        print_estimate(estimate, "local", "remote")

        captured = capsys.readouterr()
        assert "snap-2024-01-02" in captured.out
        assert "incremental" in captured.out
        assert "snap-2024-01-01" in captured.out

    def test_print_long_snapshot_name_truncated(self, capsys):
        """Test that long snapshot names are truncated."""
        estimate = TransferEstimate()
        long_name = "snapshot-with-a-very-long-name-that-exceeds-forty-characters"
        snap = SnapshotEstimate(
            name=long_name,
            full_size=1024,
        )
        estimate.add_snapshot(snap)
        estimate.new_snapshot_count = 1

        print_estimate(estimate)

        captured = capsys.readouterr()
        assert ".." in captured.out  # Truncation indicator

    def test_print_multiple_snapshots(self, capsys):
        """Test printing estimate with multiple snapshots."""
        estimate = TransferEstimate()

        snap1 = SnapshotEstimate(name="snap-1", full_size=1024 * 1024)
        snap2 = SnapshotEstimate(
            name="snap-2",
            full_size=1024 * 1024,
            incremental_size=512 * 1024,
            parent_name="snap-1",
            is_incremental=True,
        )

        estimate.add_snapshot(snap1)
        estimate.add_snapshot(snap2)
        estimate.new_snapshot_count = 2
        estimate.estimation_time = 1.5

        print_estimate(estimate)

        captured = capsys.readouterr()
        assert "Snapshots to transfer: 2" in captured.out
        assert "snap-1" in captured.out
        assert "snap-2" in captured.out
        assert "1.50s" in captured.out

    def test_print_totals(self, capsys):
        """Test that totals are printed correctly."""
        estimate = TransferEstimate()
        snap = SnapshotEstimate(
            name="snap-1",
            full_size=1024 * 1024 * 1024,  # 1 GiB
        )
        estimate.add_snapshot(snap)
        estimate.new_snapshot_count = 1

        print_estimate(estimate)

        captured = capsys.readouterr()
        assert "Total data to transfer" in captured.out
        assert "GiB" in captured.out


class TestParseSizeEdgeCases:
    """Additional edge case tests for _parse_size."""

    def test_parse_with_whitespace(self):
        """Test parsing size with whitespace."""
        assert _parse_size("  1GiB  ") == 1024**3
        # Space in middle is actually supported due to strip in value parsing
        assert _parse_size("1 GiB") == 1024**3

    def test_parse_float_value(self):
        """Test parsing float without unit."""
        assert _parse_size("1.5") == 1

    def test_parse_large_tib(self):
        """Test parsing large TiB value."""
        assert _parse_size("10TiB") == 10 * 1024**4

    def test_parse_case_sensitivity(self):
        """Test that parsing is case-sensitive."""
        # Our implementation is case-sensitive
        assert _parse_size("1gib") is None
        assert _parse_size("1GIB") is None


class TestExecuteEstimate:
    """Tests for execute_estimate CLI function."""

    def test_no_args_shows_error(self):
        """Test that missing args shows error."""
        from btrfs_backup_ng.cli.estimate import execute_estimate

        args = MagicMock()
        args.volume = None
        args.source = None
        args.destination = None

        result = execute_estimate(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.estimate.find_config_file")
    def test_volume_no_config(self, mock_find):
        """Test --volume when no config file exists."""
        from btrfs_backup_ng.cli.estimate import execute_estimate

        mock_find.return_value = None

        args = MagicMock()
        args.volume = "/home"
        args.config = None

        result = execute_estimate(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.estimate.load_config")
    @patch("btrfs_backup_ng.cli.estimate.find_config_file")
    def test_volume_not_found(self, mock_find, mock_load, tmp_path):
        """Test --volume with non-existent volume."""
        from btrfs_backup_ng.cli.estimate import execute_estimate
        from btrfs_backup_ng.config.schema import Config, VolumeConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(volumes=[VolumeConfig(path="/var/log", snapshot_prefix="logs")]),
            [],
        )

        args = MagicMock()
        args.volume = "/home"
        args.config = None

        result = execute_estimate(args)

        assert result == 1


class TestEstimateFromConfig:
    """Tests for _estimate_from_config function."""

    @patch("btrfs_backup_ng.cli.estimate.load_config")
    def test_config_load_error(self, mock_load):
        """Test handling of config load error."""
        from btrfs_backup_ng.cli.estimate import _estimate_from_config
        from btrfs_backup_ng.config import ConfigError

        mock_load.side_effect = ConfigError("Invalid config")

        args = MagicMock()
        args.config = "/path/to/config.toml"

        result = _estimate_from_config(args, "/home")

        assert result == 1

    @patch("btrfs_backup_ng.cli.estimate.load_config")
    @patch("btrfs_backup_ng.cli.estimate.find_config_file")
    def test_volume_no_targets(self, mock_find, mock_load, tmp_path):
        """Test volume with no backup targets configured."""
        from btrfs_backup_ng.cli.estimate import _estimate_from_config
        from btrfs_backup_ng.config.schema import Config, VolumeConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(
                volumes=[
                    VolumeConfig(path="/home", snapshot_prefix="home-", targets=[])
                ]
            ),
            [],
        )

        args = MagicMock()
        args.config = None

        result = _estimate_from_config(args, "/home")

        assert result == 1

    @patch("btrfs_backup_ng.cli.estimate.load_config")
    @patch("btrfs_backup_ng.cli.estimate.find_config_file")
    def test_invalid_target_index(self, mock_find, mock_load, tmp_path):
        """Test with invalid target index."""
        from btrfs_backup_ng.cli.estimate import _estimate_from_config
        from btrfs_backup_ng.config.schema import Config, VolumeConfig, TargetConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(
                volumes=[
                    VolumeConfig(
                        path="/home",
                        snapshot_prefix="home-",
                        targets=[TargetConfig(path="/mnt/backup")],
                    )
                ]
            ),
            [],
        )

        args = MagicMock()
        args.config = None
        args.target = 5  # Invalid index

        result = _estimate_from_config(args, "/home")

        assert result == 1

    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    @patch("btrfs_backup_ng.cli.estimate.load_config")
    @patch("btrfs_backup_ng.cli.estimate.find_config_file")
    def test_source_endpoint_prepare_fails(
        self, mock_find, mock_load, mock_endpoint, tmp_path
    ):
        """Test handling when source endpoint preparation fails."""
        from btrfs_backup_ng.cli.estimate import _estimate_from_config
        from btrfs_backup_ng.config.schema import Config, VolumeConfig, TargetConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(
                volumes=[
                    VolumeConfig(
                        path="/home",
                        snapshot_prefix="home-",
                        targets=[TargetConfig(path="/mnt/backup")],
                    )
                ]
            ),
            [],
        )

        mock_ep = MagicMock()
        mock_ep.prepare.side_effect = Exception("Cannot access source")
        mock_endpoint.choose_endpoint.return_value = mock_ep

        args = MagicMock()
        args.config = None
        args.target = None
        args.json = False

        result = _estimate_from_config(args, "/home")

        assert result == 1

    @patch("btrfs_backup_ng.cli.estimate.estimate_transfer")
    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    @patch("btrfs_backup_ng.cli.estimate.load_config")
    @patch("btrfs_backup_ng.cli.estimate.find_config_file")
    def test_dest_endpoint_prepare_fails(
        self, mock_find, mock_load, mock_endpoint, mock_estimate, tmp_path
    ):
        """Test handling when destination endpoint preparation fails."""
        from btrfs_backup_ng.cli.estimate import _estimate_from_config
        from btrfs_backup_ng.config.schema import Config, VolumeConfig, TargetConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(
                volumes=[
                    VolumeConfig(
                        path="/home",
                        snapshot_prefix="home-",
                        targets=[TargetConfig(path="/mnt/backup")],
                    )
                ]
            ),
            [],
        )

        # First call for source succeeds, second for dest fails
        source_ep = MagicMock()
        dest_ep = MagicMock()
        dest_ep.prepare.side_effect = Exception("Cannot access destination")
        mock_endpoint.choose_endpoint.side_effect = [source_ep, dest_ep]

        args = MagicMock()
        args.config = None
        args.target = None
        args.json = False

        result = _estimate_from_config(args, "/home")

        assert result == 1

    @patch("btrfs_backup_ng.cli.estimate.estimate_transfer")
    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    @patch("btrfs_backup_ng.cli.estimate.load_config")
    @patch("btrfs_backup_ng.cli.estimate.find_config_file")
    def test_estimation_fails(
        self, mock_find, mock_load, mock_endpoint, mock_estimate, tmp_path
    ):
        """Test handling when estimation fails."""
        from btrfs_backup_ng.cli.estimate import _estimate_from_config
        from btrfs_backup_ng.config.schema import Config, VolumeConfig, TargetConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(
                volumes=[
                    VolumeConfig(
                        path="/home",
                        snapshot_prefix="home-",
                        targets=[TargetConfig(path="/mnt/backup")],
                    )
                ]
            ),
            [],
        )

        mock_ep = MagicMock()
        mock_endpoint.choose_endpoint.return_value = mock_ep
        mock_estimate.side_effect = Exception("Estimation error")

        args = MagicMock()
        args.config = None
        args.target = None
        args.json = False

        result = _estimate_from_config(args, "/home")

        assert result == 1

    @patch("btrfs_backup_ng.cli.estimate.print_estimate")
    @patch("btrfs_backup_ng.cli.estimate.estimate_transfer")
    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    @patch("btrfs_backup_ng.cli.estimate.load_config")
    @patch("btrfs_backup_ng.cli.estimate.find_config_file")
    def test_successful_estimation(
        self, mock_find, mock_load, mock_endpoint, mock_estimate, mock_print, tmp_path
    ):
        """Test successful config-driven estimation."""
        from btrfs_backup_ng.cli.estimate import _estimate_from_config
        from btrfs_backup_ng.config.schema import Config, VolumeConfig, TargetConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(
                volumes=[
                    VolumeConfig(
                        path="/home",
                        snapshot_prefix="home-",
                        targets=[TargetConfig(path="/mnt/backup")],
                    )
                ]
            ),
            [],
        )

        mock_ep = MagicMock()
        mock_endpoint.choose_endpoint.return_value = mock_ep
        mock_estimate.return_value = TransferEstimate()

        args = MagicMock()
        args.config = None
        args.target = None
        args.json = False

        result = _estimate_from_config(args, "/home")

        assert result == 0
        mock_print.assert_called_once()

    @patch("btrfs_backup_ng.cli.estimate._print_json")
    @patch("btrfs_backup_ng.cli.estimate.estimate_transfer")
    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    @patch("btrfs_backup_ng.cli.estimate.load_config")
    @patch("btrfs_backup_ng.cli.estimate.find_config_file")
    def test_successful_estimation_json_output(
        self, mock_find, mock_load, mock_endpoint, mock_estimate, mock_json, tmp_path
    ):
        """Test successful estimation with JSON output."""
        from btrfs_backup_ng.cli.estimate import _estimate_from_config
        from btrfs_backup_ng.config.schema import Config, VolumeConfig, TargetConfig

        mock_find.return_value = str(tmp_path / "config.toml")
        mock_load.return_value = (
            Config(
                volumes=[
                    VolumeConfig(
                        path="/home",
                        snapshot_prefix="home-",
                        targets=[
                            TargetConfig(
                                path="/mnt/backup",
                                ssh_sudo=True,
                                ssh_key="~/.ssh/id_rsa",
                            )
                        ],
                    )
                ]
            ),
            [],
        )

        mock_ep = MagicMock()
        mock_endpoint.choose_endpoint.return_value = mock_ep
        mock_estimate.return_value = TransferEstimate()

        args = MagicMock()
        args.config = None
        args.target = 0
        args.json = True

        result = _estimate_from_config(args, "/home")

        assert result == 0
        mock_json.assert_called_once()

    @patch("btrfs_backup_ng.cli.estimate.load_config")
    def test_explicit_config_path(self, mock_load, tmp_path):
        """Test using explicit config path."""
        from btrfs_backup_ng.cli.estimate import _estimate_from_config
        from btrfs_backup_ng.config.schema import Config, VolumeConfig

        config_path = str(tmp_path / "custom-config.toml")
        mock_load.return_value = (
            Config(volumes=[VolumeConfig(path="/var/log", snapshot_prefix="logs-")]),
            [],
        )

        args = MagicMock()
        args.config = config_path

        result = _estimate_from_config(args, "/home")

        # Should fail because /home not in config, but load_config should be called with explicit path
        mock_load.assert_called_once_with(config_path)
        assert result == 1


class TestEstimateDirect:
    """Tests for _estimate_direct function."""

    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    def test_source_endpoint_prepare_fails(self, mock_endpoint, tmp_path):
        """Test handling when source endpoint preparation fails."""
        from btrfs_backup_ng.cli.estimate import _estimate_direct

        mock_ep = MagicMock()
        mock_ep.prepare.side_effect = Exception("Cannot access source")
        mock_endpoint.choose_endpoint.return_value = mock_ep

        args = MagicMock()
        args.json = False
        args.prefix = ""
        args.ssh_sudo = False
        args.ssh_key = None
        args.no_fs_checks = False

        result = _estimate_direct(
            args, str(tmp_path / "source"), str(tmp_path / "dest")
        )

        assert result == 1

    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    def test_dest_endpoint_prepare_fails(self, mock_endpoint, tmp_path):
        """Test handling when destination endpoint preparation fails."""
        from btrfs_backup_ng.cli.estimate import _estimate_direct

        source_ep = MagicMock()
        dest_ep = MagicMock()
        dest_ep.prepare.side_effect = Exception("Cannot access destination")
        mock_endpoint.choose_endpoint.side_effect = [source_ep, dest_ep]

        args = MagicMock()
        args.json = False
        args.prefix = ""
        args.ssh_sudo = False
        args.ssh_key = None
        args.no_fs_checks = False

        result = _estimate_direct(
            args, str(tmp_path / "source"), str(tmp_path / "dest")
        )

        assert result == 1

    @patch("btrfs_backup_ng.cli.estimate.estimate_transfer")
    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    def test_estimation_fails(self, mock_endpoint, mock_estimate, tmp_path):
        """Test handling when estimation fails."""
        from btrfs_backup_ng.cli.estimate import _estimate_direct

        mock_ep = MagicMock()
        mock_endpoint.choose_endpoint.return_value = mock_ep
        mock_estimate.side_effect = Exception("Estimation error")

        args = MagicMock()
        args.json = False
        args.prefix = ""
        args.ssh_sudo = False
        args.ssh_key = None
        args.no_fs_checks = False

        result = _estimate_direct(
            args, str(tmp_path / "source"), str(tmp_path / "dest")
        )

        assert result == 1

    @patch("btrfs_backup_ng.cli.estimate.print_estimate")
    @patch("btrfs_backup_ng.cli.estimate.estimate_transfer")
    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    def test_successful_estimation(
        self, mock_endpoint, mock_estimate, mock_print, tmp_path
    ):
        """Test successful direct path estimation."""
        from btrfs_backup_ng.cli.estimate import _estimate_direct

        mock_ep = MagicMock()
        mock_endpoint.choose_endpoint.return_value = mock_ep
        mock_estimate.return_value = TransferEstimate()

        args = MagicMock()
        args.json = False
        args.prefix = "home-"
        args.ssh_sudo = False
        args.ssh_key = None
        args.no_fs_checks = False

        result = _estimate_direct(
            args, str(tmp_path / "source"), str(tmp_path / "dest")
        )

        assert result == 0
        mock_print.assert_called_once()

    @patch("btrfs_backup_ng.cli.estimate._print_json")
    @patch("btrfs_backup_ng.cli.estimate.estimate_transfer")
    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    def test_successful_estimation_json_output(
        self, mock_endpoint, mock_estimate, mock_json, tmp_path
    ):
        """Test successful estimation with JSON output."""
        from btrfs_backup_ng.cli.estimate import _estimate_direct

        mock_ep = MagicMock()
        mock_endpoint.choose_endpoint.return_value = mock_ep
        mock_estimate.return_value = TransferEstimate()

        args = MagicMock()
        args.json = True
        args.prefix = ""
        args.ssh_sudo = False
        args.ssh_key = None
        args.no_fs_checks = False

        result = _estimate_direct(
            args, str(tmp_path / "source"), str(tmp_path / "dest")
        )

        assert result == 0
        mock_json.assert_called_once()

    @patch("btrfs_backup_ng.cli.estimate.print_estimate")
    @patch("btrfs_backup_ng.cli.estimate.estimate_transfer")
    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    def test_ssh_source_path(self, mock_endpoint, mock_estimate, mock_print, tmp_path):
        """Test with SSH source path."""
        from btrfs_backup_ng.cli.estimate import _estimate_direct

        mock_ep = MagicMock()
        mock_endpoint.choose_endpoint.return_value = mock_ep
        mock_estimate.return_value = TransferEstimate()

        args = MagicMock()
        args.json = False
        args.prefix = ""
        args.ssh_sudo = True
        args.ssh_key = "/path/to/key"
        args.no_fs_checks = True

        result = _estimate_direct(args, "ssh://user@host:/path", str(tmp_path / "dest"))

        assert result == 0
        # Check endpoint was created with SSH options
        calls = mock_endpoint.choose_endpoint.call_args_list
        assert len(calls) == 2

    @patch("btrfs_backup_ng.cli.estimate.print_estimate")
    @patch("btrfs_backup_ng.cli.estimate.estimate_transfer")
    @patch("btrfs_backup_ng.cli.estimate.endpoint")
    def test_ssh_dest_path(self, mock_endpoint, mock_estimate, mock_print, tmp_path):
        """Test with SSH destination path."""
        from btrfs_backup_ng.cli.estimate import _estimate_direct

        mock_ep = MagicMock()
        mock_endpoint.choose_endpoint.return_value = mock_ep
        mock_estimate.return_value = TransferEstimate()

        args = MagicMock()
        args.json = False
        args.prefix = ""
        args.ssh_sudo = True
        args.ssh_key = None
        args.no_fs_checks = False

        result = _estimate_direct(
            args, str(tmp_path / "source"), "ssh://backup@server:/backups"
        )

        assert result == 0


class TestPrintJson:
    """Tests for _print_json function."""

    def test_empty_estimate(self, capsys):
        """Test JSON output for empty estimate."""
        import json

        from btrfs_backup_ng.cli.estimate import _print_json

        estimate = TransferEstimate()
        _print_json(estimate, "/source", "/dest")

        captured = capsys.readouterr()
        data = json.loads(captured.out)

        assert data["source"] == "/source"
        assert data["destination"] == "/dest"
        assert data["snapshot_count"] == 0
        assert data["new_snapshot_count"] == 0
        assert data["skipped_count"] == 0
        assert data["snapshots"] == []

    def test_full_snapshot(self, capsys):
        """Test JSON output for full snapshot."""
        import json

        from btrfs_backup_ng.cli.estimate import _print_json

        estimate = TransferEstimate()
        snap = SnapshotEstimate(
            name="snap-1",
            full_size=1024 * 1024 * 100,
            method="filesystem_du",
        )
        estimate.add_snapshot(snap)

        _print_json(estimate, "/source", "/dest")

        captured = capsys.readouterr()
        data = json.loads(captured.out)

        assert data["snapshot_count"] == 1
        assert len(data["snapshots"]) == 1
        assert data["snapshots"][0]["name"] == "snap-1"
        assert data["snapshots"][0]["full_size_bytes"] == 1024 * 1024 * 100
        assert data["snapshots"][0]["is_incremental"] is False
        assert "parent" not in data["snapshots"][0]

    def test_incremental_snapshot(self, capsys):
        """Test JSON output for incremental snapshot."""
        import json

        from btrfs_backup_ng.cli.estimate import _print_json

        estimate = TransferEstimate()
        snap = SnapshotEstimate(
            name="snap-2",
            full_size=1024 * 1024 * 100,
            incremental_size=1024 * 1024 * 10,
            parent_name="snap-1",
            is_incremental=True,
            method="send_no_data",
        )
        estimate.add_snapshot(snap)

        _print_json(estimate, "/source", "/dest")

        captured = capsys.readouterr()
        data = json.loads(captured.out)

        assert data["snapshots"][0]["is_incremental"] is True
        assert data["snapshots"][0]["incremental_size_bytes"] == 1024 * 1024 * 10
        assert data["snapshots"][0]["parent"] == "snap-1"

    def test_multiple_snapshots(self, capsys):
        """Test JSON output for multiple snapshots."""
        import json

        from btrfs_backup_ng.cli.estimate import _print_json

        estimate = TransferEstimate()
        estimate.estimation_time = 2.5

        snap1 = SnapshotEstimate(
            name="snap-1",
            full_size=1024 * 1024 * 100,
        )
        snap2 = SnapshotEstimate(
            name="snap-2",
            full_size=1024 * 1024 * 100,
            incremental_size=1024 * 1024 * 20,
            parent_name="snap-1",
            is_incremental=True,
        )
        estimate.add_snapshot(snap1)
        estimate.add_snapshot(snap2)
        estimate.new_snapshot_count = 2
        estimate.skipped_count = 3

        _print_json(estimate, "ssh://user@host:/source", "/dest")

        captured = capsys.readouterr()
        data = json.loads(captured.out)

        assert data["source"] == "ssh://user@host:/source"
        assert data["snapshot_count"] == 2
        assert data["new_snapshot_count"] == 2
        assert data["skipped_count"] == 3
        assert data["estimation_time_seconds"] == 2.5
        assert len(data["snapshots"]) == 2
        assert data["snapshots"][0]["name"] == "snap-1"
        assert data["snapshots"][1]["name"] == "snap-2"

    def test_human_readable_sizes(self, capsys):
        """Test that human-readable sizes are included."""
        import json

        from btrfs_backup_ng.cli.estimate import _print_json

        estimate = TransferEstimate()
        snap = SnapshotEstimate(
            name="snap-1",
            full_size=1024 * 1024 * 1024,  # 1 GiB
            incremental_size=1024 * 1024 * 50,  # 50 MiB
            parent_name="snap-0",
            is_incremental=True,
        )
        estimate.add_snapshot(snap)

        _print_json(estimate, "/source", "/dest")

        captured = capsys.readouterr()
        data = json.loads(captured.out)

        assert "GiB" in data["snapshots"][0]["full_size_human"]
        assert "MiB" in data["snapshots"][0]["incremental_size_human"]
        assert "total_transfer_human" in data
        assert "total_full_human" in data


class TestExecuteEstimateIntegration:
    """Integration tests for execute_estimate."""

    @patch("btrfs_backup_ng.cli.estimate._estimate_direct")
    def test_direct_mode_called(self, mock_direct):
        """Test that direct estimation is called with source and dest."""
        from btrfs_backup_ng.cli.estimate import execute_estimate

        mock_direct.return_value = 0

        args = MagicMock()
        args.volume = None
        args.source = "/source"
        args.destination = "/dest"

        result = execute_estimate(args)

        assert result == 0
        mock_direct.assert_called_once_with(args, "/source", "/dest")

    @patch("btrfs_backup_ng.cli.estimate._estimate_from_config")
    def test_config_mode_called(self, mock_config):
        """Test that config estimation is called with --volume."""
        from btrfs_backup_ng.cli.estimate import execute_estimate

        mock_config.return_value = 0

        args = MagicMock()
        args.volume = "/home"
        args.source = None
        args.destination = None

        result = execute_estimate(args)

        assert result == 0
        mock_config.assert_called_once_with(args, "/home")

    def test_missing_source_with_dest(self):
        """Test error when only destination is provided."""
        from btrfs_backup_ng.cli.estimate import execute_estimate

        args = MagicMock()
        args.volume = None
        args.source = None
        args.destination = "/dest"

        result = execute_estimate(args)

        assert result == 1

    def test_missing_dest_with_source(self):
        """Test error when only source is provided."""
        from btrfs_backup_ng.cli.estimate import execute_estimate

        args = MagicMock()
        args.volume = None
        args.source = "/source"
        args.destination = None

        result = execute_estimate(args)

        assert result == 1
