"""Tests for progress bar support."""

import io
import subprocess
import threading
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core.progress import (
    ProgressReader,
    RichProgressPipe,
    _parse_size,
    _simple_transfer,
    create_rich_progress,
    estimate_snapshot_size,
    is_interactive,
    run_transfer_with_progress,
)


class TestIsInteractive:
    """Tests for is_interactive function."""

    @patch("sys.stdout.isatty")
    def test_returns_true_when_tty(self, mock_isatty):
        """Test returns True when stdout is a TTY."""
        mock_isatty.return_value = True
        assert is_interactive() is True

    @patch("sys.stdout.isatty")
    def test_returns_false_when_not_tty(self, mock_isatty):
        """Test returns False when stdout is not a TTY."""
        mock_isatty.return_value = False
        assert is_interactive() is False


class TestParseSize:
    """Tests for _parse_size function."""

    def test_parse_bytes_suffix(self):
        """Test parsing with B suffix."""
        assert _parse_size("1024B") == 1024
        assert _parse_size("100B") == 100

    def test_parse_plain_number(self):
        """Test parsing plain bytes number."""
        assert _parse_size("12345") == 12345
        assert _parse_size("0") == 0

    def test_parse_with_whitespace(self):
        """Test parsing with surrounding whitespace."""
        assert _parse_size("  12345  ") == 12345
        assert _parse_size("\t100B\n") == 100

    def test_parse_invalid_returns_none(self):
        """Test invalid strings return None."""
        assert _parse_size("invalid") is None
        assert _parse_size("") is None

    def test_parse_float_bytes(self):
        """Test parsing float as bytes."""
        assert _parse_size("1024.5") == 1024

    def test_parse_size_with_decimal(self):
        """Test parsing sizes with decimal values."""
        assert _parse_size("1.5B") == 1
        assert _parse_size("100.5B") == 100

    def test_multipliers_exist(self):
        """Test the function handles numeric strings."""
        assert _parse_size("1000") == 1000
        assert _parse_size("1048576") == 1048576


class TestEstimateSnapshotSize:
    """Tests for estimate_snapshot_size function."""

    @patch("os.path.exists")
    def test_incremental_returns_none(self, mock_exists):
        """Test incremental transfer returns None for indeterminate."""
        mock_exists.return_value = True
        result = estimate_snapshot_size("/snapshot", parent_path="/parent")
        assert result is None

    @patch("os.geteuid")
    @patch("subprocess.run")
    def test_parses_exclusive_size_bytes(self, mock_run, mock_euid):
        """Test parsing exclusive size from btrfs subvolume show (plain bytes)."""
        mock_euid.return_value = 0  # Running as root
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Name: snapshot\nExclusive: 1234567890B\nOther: data\n",
        )

        result = estimate_snapshot_size("/snapshot")

        assert result == 1234567890
        mock_run.assert_called_once()

    @patch("os.geteuid")
    @patch("subprocess.run")
    def test_uses_sudo_when_not_root(self, mock_run, mock_euid):
        """Test uses sudo -n when not running as root."""
        mock_euid.return_value = 1000  # Not root
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Name: snapshot\nExclusive: 104857600B\n",
        )

        result = estimate_snapshot_size("/snapshot")

        assert result == 104857600  # 100 MiB in bytes
        # Verify sudo was used
        call_args = mock_run.call_args[0][0]
        assert call_args[:2] == ["sudo", "-n"]

    @patch("os.geteuid")
    @patch("subprocess.run")
    def test_fallback_to_btrfs_du(self, mock_run, mock_euid):
        """Test fallback to btrfs filesystem du."""
        mock_euid.return_value = 0

        # First call (btrfs subvolume show) fails
        # Second call (btrfs filesystem du) succeeds
        mock_run.side_effect = [
            MagicMock(returncode=1, stderr="Error"),
            MagicMock(
                returncode=0,
                stdout="Total   Exclusive  Set shared  Filename\n12345678   6789000    5000000    /snapshot\n",
            ),
        ]

        result = estimate_snapshot_size("/snapshot")

        assert result == 12345678
        assert mock_run.call_count == 2

    @patch("os.geteuid")
    @patch("subprocess.run")
    def test_fallback_to_du(self, mock_run, mock_euid):
        """Test fallback to regular du command."""
        mock_euid.return_value = 0

        # First two calls fail, third (du) succeeds
        mock_run.side_effect = [
            MagicMock(returncode=1, stderr="Error"),
            MagicMock(returncode=1, stderr="Error"),
            MagicMock(returncode=0, stdout="98765432\t/snapshot\n"),
        ]

        result = estimate_snapshot_size("/snapshot")

        assert result == 98765432
        assert mock_run.call_count == 3

    @patch("os.geteuid")
    @patch("subprocess.run")
    def test_returns_none_when_all_fail(self, mock_run, mock_euid):
        """Test returns None when all estimation methods fail."""
        mock_euid.return_value = 0
        mock_run.return_value = MagicMock(returncode=1, stderr="Error")

        result = estimate_snapshot_size("/snapshot")

        assert result is None

    @patch("os.geteuid")
    @patch("subprocess.run")
    def test_handles_timeout(self, mock_run, mock_euid):
        """Test handles subprocess timeout gracefully."""
        mock_euid.return_value = 0
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="btrfs", timeout=30)

        result = estimate_snapshot_size("/snapshot")

        assert result is None

    @patch("os.geteuid")
    @patch("subprocess.run")
    def test_handles_file_not_found(self, mock_run, mock_euid):
        """Test handles missing btrfs command gracefully."""
        mock_euid.return_value = 0
        mock_run.side_effect = FileNotFoundError("btrfs not found")

        result = estimate_snapshot_size("/snapshot")

        assert result is None


class TestRichProgressPipe:
    """Tests for RichProgressPipe class."""

    def test_read_updates_progress(self):
        """Test that read updates progress bar."""
        source = io.BytesIO(b"Hello, World!")
        mock_progress = MagicMock()
        task_id = "task-1"

        pipe = RichProgressPipe(source, mock_progress, task_id)
        data = pipe.read(5)

        assert data == b"Hello"
        assert pipe.bytes_read == 5
        mock_progress.update.assert_called_once_with(task_id, advance=5)

    def test_read_full_data(self):
        """Test reading all data in chunks."""
        source = io.BytesIO(b"0123456789")
        mock_progress = MagicMock()

        pipe = RichProgressPipe(source, mock_progress, "task-1", chunk_size=4)

        chunks = []
        while True:
            data = pipe.read(4)
            if not data:
                break
            chunks.append(data)

        assert b"".join(chunks) == b"0123456789"
        assert pipe.bytes_read == 10

    def test_read_default_chunk_size(self):
        """Test read with default chunk size."""
        source = io.BytesIO(b"data")
        mock_progress = MagicMock()

        pipe = RichProgressPipe(source, mock_progress, "task-1")
        pipe.read()  # Uses default chunk_size

        assert pipe.bytes_read == 4

    def test_close(self):
        """Test close method."""
        source = MagicMock()
        pipe = RichProgressPipe(source, MagicMock(), "task-1")

        pipe.close()

        assert pipe._closed is True
        source.close.assert_called_once()

    def test_close_idempotent(self):
        """Test close is idempotent."""
        source = MagicMock()
        pipe = RichProgressPipe(source, MagicMock(), "task-1")

        pipe.close()
        pipe.close()  # Second close should not raise

        assert source.close.call_count == 1

    def test_context_manager(self):
        """Test context manager protocol."""
        source = MagicMock()
        mock_progress = MagicMock()

        with RichProgressPipe(source, mock_progress, "task-1") as pipe:
            assert pipe is not None

        source.close.assert_called_once()

    def test_fileno(self):
        """Test fileno returns source's file descriptor."""
        source = MagicMock()
        source.fileno.return_value = 42

        pipe = RichProgressPipe(source, MagicMock(), "task-1")

        assert pipe.fileno() == 42


class TestProgressReader:
    """Tests for ProgressReader thread class."""

    def test_transfers_data_with_progress(self):
        """Test data transfer with progress updates."""
        source = io.BytesIO(b"Hello, World!")
        # Use a mock for dest so we can verify write calls without close issues
        dest = MagicMock()
        written_data = []
        dest.write = lambda data: written_data.append(data)
        mock_progress = MagicMock()

        reader = ProgressReader(source, dest, mock_progress, "task-1", chunk_size=5)
        reader.start()
        reader.join(timeout=5)

        assert b"".join(written_data) == b"Hello, World!"
        assert reader.bytes_transferred == 13
        assert reader.error is None
        assert mock_progress.update.call_count >= 1

    def test_handles_read_error(self):
        """Test handling of read errors."""
        source = MagicMock()
        source.read.side_effect = IOError("Read error")
        dest = io.BytesIO()
        mock_progress = MagicMock()

        reader = ProgressReader(source, dest, mock_progress, "task-1")
        reader.start()
        reader.join(timeout=5)

        assert reader.error is not None
        assert "Read error" in str(reader.error)

    def test_closes_dest_on_completion(self):
        """Test destination is closed after transfer."""
        source = io.BytesIO(b"data")
        dest = MagicMock()

        reader = ProgressReader(source, dest, MagicMock(), "task-1")
        reader.start()
        reader.join(timeout=5)

        dest.close.assert_called_once()

    def test_is_daemon_thread(self):
        """Test reader is a daemon thread."""
        reader = ProgressReader(io.BytesIO(), io.BytesIO(), MagicMock(), "task-1")
        assert reader.daemon is True


class TestCreateRichProgress:
    """Tests for create_rich_progress function."""

    def test_creates_progress_when_rich_available(self):
        """Test creates Progress instance when Rich is available."""
        progress = create_rich_progress()

        # Rich should be available in test environment
        assert progress is not None

    @patch.dict(
        "sys.modules", {"rich": None, "rich.progress": None, "rich.console": None}
    )
    def test_returns_none_when_rich_unavailable(self):
        """Test returns None when Rich import fails."""
        # Clear any cached imports
        import importlib

        import btrfs_backup_ng.core.progress as prog_module

        # Patch the import to fail
        with patch.object(prog_module, "create_rich_progress") as mock_create:
            mock_create.return_value = None
            result = mock_create()
            assert result is None


class TestSimpleTransfer:
    """Tests for _simple_transfer function."""

    def test_transfers_data(self):
        """Test simple transfer pipes data correctly."""
        send_stdout = io.BytesIO(b"test data")
        # Use a mock for stdin so we can verify write calls without close issues
        recv_stdin = MagicMock()
        written_data = []
        recv_stdin.write = lambda data: written_data.append(data)

        send_process = MagicMock()
        send_process.stdout = send_stdout
        send_process.wait.return_value = 0

        recv_process = MagicMock()
        recv_process.stdin = recv_stdin
        recv_process.wait.return_value = 0

        send_rc, recv_rc = _simple_transfer(send_process, recv_process)

        assert send_rc == 0
        assert recv_rc == 0
        assert b"".join(written_data) == b"test data"

    def test_returns_process_codes(self):
        """Test returns correct return codes."""
        send_process = MagicMock()
        send_process.stdout = io.BytesIO(b"")
        send_process.wait.return_value = 1

        recv_process = MagicMock()
        recv_process.stdin = io.BytesIO()
        recv_process.wait.return_value = 2

        send_rc, recv_rc = _simple_transfer(send_process, recv_process)

        assert send_rc == 1
        assert recv_rc == 2


class TestRunTransferWithProgress:
    """Tests for run_transfer_with_progress function."""

    @patch("btrfs_backup_ng.core.progress.create_rich_progress")
    def test_falls_back_to_simple_when_no_rich(self, mock_create):
        """Test falls back to simple transfer when Rich unavailable."""
        mock_create.return_value = None

        send_process = MagicMock()
        send_process.stdout = io.BytesIO(b"data")
        send_process.wait.return_value = 0

        recv_process = MagicMock()
        recv_process.stdin = io.BytesIO()
        recv_process.wait.return_value = 0

        send_rc, recv_rc = run_transfer_with_progress(
            send_process, recv_process, "test-snapshot"
        )

        assert send_rc == 0
        assert recv_rc == 0

    @patch("btrfs_backup_ng.core.progress.ProgressReader")
    @patch("btrfs_backup_ng.core.progress.create_rich_progress")
    def test_uses_progress_reader(self, mock_create, mock_reader_class):
        """Test uses ProgressReader thread for transfer."""
        mock_progress = MagicMock()
        mock_progress.__enter__ = MagicMock(return_value=mock_progress)
        mock_progress.__exit__ = MagicMock(return_value=False)
        mock_progress.add_task.return_value = "task-1"
        mock_create.return_value = mock_progress

        mock_reader = MagicMock()
        mock_reader.error = None
        mock_reader.bytes_transferred = 1000
        mock_reader_class.return_value = mock_reader

        send_process = MagicMock()
        send_process.stdout = io.BytesIO(b"data")
        send_process.wait.return_value = 0

        recv_process = MagicMock()
        recv_process.stdin = io.BytesIO()
        recv_process.wait.return_value = 0

        run_transfer_with_progress(
            send_process, recv_process, "test-snapshot", estimated_size=1000
        )

        mock_reader.start.assert_called_once()
        mock_reader.join.assert_called_once()

    @patch("btrfs_backup_ng.core.progress.ProgressReader")
    @patch("btrfs_backup_ng.core.progress.create_rich_progress")
    def test_logs_reader_error(self, mock_create, mock_reader_class):
        """Test logs error from ProgressReader."""
        mock_progress = MagicMock()
        mock_progress.__enter__ = MagicMock(return_value=mock_progress)
        mock_progress.__exit__ = MagicMock(return_value=False)
        mock_progress.add_task.return_value = "task-1"
        mock_create.return_value = mock_progress

        mock_reader = MagicMock()
        mock_reader.error = IOError("Transfer failed")
        mock_reader.bytes_transferred = 500
        mock_reader_class.return_value = mock_reader

        send_process = MagicMock()
        send_process.stdout = io.BytesIO(b"data")
        send_process.wait.return_value = 0

        recv_process = MagicMock()
        recv_process.stdin = io.BytesIO()
        recv_process.wait.return_value = 0

        # Should not raise, just log
        send_rc, recv_rc = run_transfer_with_progress(
            send_process, recv_process, "test-snapshot"
        )

        assert send_rc == 0
        assert recv_rc == 0

    @patch("btrfs_backup_ng.core.progress.ProgressReader")
    @patch("btrfs_backup_ng.core.progress.create_rich_progress")
    def test_updates_final_progress(self, mock_create, mock_reader_class):
        """Test updates progress to 100% at completion."""
        mock_progress = MagicMock()
        mock_progress.__enter__ = MagicMock(return_value=mock_progress)
        mock_progress.__exit__ = MagicMock(return_value=False)
        mock_progress.add_task.return_value = "task-1"
        mock_create.return_value = mock_progress

        mock_reader = MagicMock()
        mock_reader.error = None
        mock_reader.bytes_transferred = 2000
        mock_reader_class.return_value = mock_reader

        send_process = MagicMock()
        send_process.stdout = io.BytesIO(b"data")
        send_process.wait.return_value = 0

        recv_process = MagicMock()
        recv_process.stdin = io.BytesIO()
        recv_process.wait.return_value = 0

        run_transfer_with_progress(
            send_process, recv_process, "test-snapshot", estimated_size=1000
        )

        # Should update to actual bytes transferred
        mock_progress.update.assert_called_with("task-1", total=2000, completed=2000)
