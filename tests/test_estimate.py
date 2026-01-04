"""Tests for backup size estimation functionality."""

from unittest.mock import MagicMock, patch


from btrfs_backup_ng.core.estimate import (
    SnapshotEstimate,
    TransferEstimate,
    _parse_size,
    format_size,
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
        mock_load.return_value = Config(
            volumes=[VolumeConfig(path="/var/log", snapshot_prefix="logs")]
        )

        args = MagicMock()
        args.volume = "/home"
        args.config = None

        result = execute_estimate(args)

        assert result == 1
