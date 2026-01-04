"""Tests for transfer utilities (compression, rate limiting)."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core.transfer import (
    COMPRESSION_PROGRAMS,
    build_receive_pipeline,
    build_transfer_pipeline,
    check_compression_available,
    check_pv_available,
    cleanup_pipeline,
    create_compress_process,
    create_decompress_process,
    create_throttle_process,
    get_available_compression_methods,
    parse_rate_limit,
    wait_for_pipeline,
)


class TestParseRateLimit:
    """Tests for parse_rate_limit function."""

    def test_parse_kilobytes(self):
        """Test parsing kilobyte rate limits."""
        assert parse_rate_limit("500K") == 500 * 1024
        assert parse_rate_limit("1K") == 1024
        assert parse_rate_limit("100k") == 100 * 1024  # lowercase

    def test_parse_megabytes(self):
        """Test parsing megabyte rate limits."""
        assert parse_rate_limit("10M") == 10 * 1024 * 1024
        assert parse_rate_limit("1M") == 1024 * 1024
        assert parse_rate_limit("50m") == 50 * 1024 * 1024  # lowercase

    def test_parse_gigabytes(self):
        """Test parsing gigabyte rate limits."""
        assert parse_rate_limit("1G") == 1024 * 1024 * 1024
        assert parse_rate_limit("2g") == 2 * 1024 * 1024 * 1024  # lowercase

    def test_parse_bytes(self):
        """Test parsing raw byte values."""
        assert parse_rate_limit("1024") == 1024
        assert parse_rate_limit("52428800") == 52428800

    def test_parse_none(self):
        """Test parsing None returns None."""
        assert parse_rate_limit(None) is None

    def test_parse_empty_string(self):
        """Test parsing empty string returns None."""
        assert parse_rate_limit("") is None

    def test_parse_invalid(self):
        """Test parsing invalid format returns None."""
        assert parse_rate_limit("invalid") is None
        assert parse_rate_limit("abc123") is None

    def test_parse_whitespace(self):
        """Test parsing with whitespace."""
        assert parse_rate_limit("  10M  ") == 10 * 1024 * 1024

    def test_parse_float_value(self):
        """Test parsing float values."""
        result = parse_rate_limit("1.5M")
        assert result == int(1.5 * 1024 * 1024)


class TestCheckCompressionAvailable:
    """Tests for check_compression_available function."""

    def test_none_always_available(self):
        """Test that 'none' compression is always available."""
        assert check_compression_available("none") is True
        assert check_compression_available("") is True

    def test_unknown_method(self):
        """Test that unknown method returns False."""
        assert check_compression_available("unknown_method") is False

    @patch("shutil.which")
    def test_gzip_available(self, mock_which):
        """Test checking gzip availability."""
        mock_which.return_value = "/usr/bin/gzip"
        assert check_compression_available("gzip") is True

    @patch("shutil.which")
    def test_compression_not_installed(self, mock_which):
        """Test when compression program is not installed."""
        mock_which.return_value = None
        assert check_compression_available("zstd") is False


class TestCheckPvAvailable:
    """Tests for check_pv_available function."""

    @patch("shutil.which")
    def test_pv_available(self, mock_which):
        """Test when pv is available."""
        mock_which.return_value = "/usr/bin/pv"
        assert check_pv_available() is True

    @patch("shutil.which")
    def test_pv_not_available(self, mock_which):
        """Test when pv is not available."""
        mock_which.return_value = None
        assert check_pv_available() is False


class TestGetAvailableCompressionMethods:
    """Tests for get_available_compression_methods function."""

    def test_always_includes_none(self):
        """Test that 'none' is always in the list."""
        methods = get_available_compression_methods()
        assert "none" in methods

    @patch("shutil.which")
    def test_includes_available_methods(self, mock_which):
        """Test that available methods are included."""

        def which_side_effect(cmd):
            if cmd in ["gzip", "zstd"]:
                return f"/usr/bin/{cmd}"
            return None

        mock_which.side_effect = which_side_effect
        methods = get_available_compression_methods()

        assert "none" in methods
        assert "gzip" in methods
        assert "zstd" in methods


class TestCompressionPrograms:
    """Tests for COMPRESSION_PROGRAMS constant."""

    def test_has_required_keys(self):
        """Test that each program has required keys."""
        for name, config in COMPRESSION_PROGRAMS.items():
            assert "compress" in config, f"{name} missing 'compress' key"
            assert "decompress" in config, f"{name} missing 'decompress' key"
            assert "check" in config, f"{name} missing 'check' key"

    def test_compress_is_list(self):
        """Test that compress commands are lists."""
        for name, config in COMPRESSION_PROGRAMS.items():
            assert isinstance(config["compress"], list), f"{name} compress not a list"
            assert isinstance(config["decompress"], list), (
                f"{name} decompress not a list"
            )

    def test_known_methods_exist(self):
        """Test that expected compression methods exist."""
        expected = ["gzip", "zstd", "lz4", "pigz", "lzop"]
        for method in expected:
            assert method in COMPRESSION_PROGRAMS, (
                f"{method} not in COMPRESSION_PROGRAMS"
            )


class TestCreateCompressProcess:
    """Tests for create_compress_process function."""

    def test_none_compression_returns_none(self):
        """Test that 'none' compression returns None."""
        result = create_compress_process("none", stdin=None)
        assert result is None

    def test_empty_compression_returns_none(self):
        """Test that empty compression returns None."""
        result = create_compress_process("", stdin=None)
        assert result is None

    def test_unknown_method_returns_none(self):
        """Test that unknown method returns None."""
        result = create_compress_process("unknown_method", stdin=None)
        assert result is None

    @patch("shutil.which")
    def test_unavailable_program_returns_none(self, mock_which):
        """Test that unavailable program returns None."""
        mock_which.return_value = None
        result = create_compress_process("zstd", stdin=None)
        assert result is None


class TestCreateDecompressProcess:
    """Tests for create_decompress_process function."""

    def test_none_compression_returns_none(self):
        """Test that 'none' compression returns None."""
        result = create_decompress_process("none", stdin=None)
        assert result is None

    def test_empty_compression_returns_none(self):
        """Test that empty compression returns None."""
        result = create_decompress_process("", stdin=None)
        assert result is None


class TestCreateThrottleProcess:
    """Tests for create_throttle_process function."""

    def test_no_rate_limit_returns_none(self):
        """Test that no rate limit returns None."""
        result = create_throttle_process(None, stdin=None)
        assert result is None

    @patch("shutil.which")
    def test_pv_not_available_returns_none(self, mock_which):
        """Test that missing pv returns None."""
        mock_which.return_value = None
        result = create_throttle_process("10M", stdin=None)
        assert result is None


class TestBuildTransferPipeline:
    """Tests for build_transfer_pipeline function."""

    def test_no_compression_no_throttle(self):
        """Test pipeline with no compression or throttling."""
        mock_stdout = MagicMock()

        final_stdout, processes = build_transfer_pipeline(
            send_stdout=mock_stdout,
            compress="none",
            rate_limit=None,
        )

        assert final_stdout == mock_stdout
        assert len(processes) == 0

    @patch("btrfs_backup_ng.core.transfer.check_compression_available")
    @patch("subprocess.Popen")
    def test_with_compression(self, mock_popen, mock_check):
        """Test pipeline with compression."""
        mock_check.return_value = True
        mock_process = MagicMock()
        mock_process.stdout = MagicMock()
        mock_popen.return_value = mock_process

        mock_stdout = MagicMock()

        final_stdout, processes = build_transfer_pipeline(
            send_stdout=mock_stdout,
            compress="gzip",
            rate_limit=None,
        )

        assert len(processes) == 1
        assert processes[0][0] == "compress"


class TestCleanupPipeline:
    """Tests for cleanup_pipeline function."""

    def test_cleanup_empty_list(self):
        """Test cleanup with empty process list."""
        cleanup_pipeline([])  # Should not raise

    def test_cleanup_terminates_running(self):
        """Test that running processes are terminated."""
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None  # Still running

        cleanup_pipeline([("test", mock_proc)])

        mock_proc.terminate.assert_called_once()
        mock_proc.wait.assert_called_once()

    def test_cleanup_skips_finished(self):
        """Test that finished processes are skipped."""
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 0  # Already finished

        cleanup_pipeline([("test", mock_proc)])

        mock_proc.terminate.assert_not_called()


class TestWaitForPipeline:
    """Tests for wait_for_pipeline function."""

    def test_wait_empty_list(self):
        """Test waiting on empty process list."""
        result = wait_for_pipeline([])
        assert result == []

    def test_wait_successful_processes(self):
        """Test waiting on successful processes."""
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0

        result = wait_for_pipeline([("test", mock_proc)])

        assert result == [0]

    def test_wait_failed_process(self):
        """Test waiting on failed process."""
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 1
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.read.return_value = b"error message"

        result = wait_for_pipeline([("test", mock_proc)])

        assert result == [1]

    def test_wait_timeout(self):
        """Test timeout handling."""
        mock_proc = MagicMock()
        mock_proc.wait.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=10)

        result = wait_for_pipeline([("test", mock_proc)], timeout=10)

        assert result == [-1]
        mock_proc.kill.assert_called_once()


class TestCreateDecompressProcessMore:
    """Additional tests for create_decompress_process."""

    def test_unknown_method_returns_none(self):
        """Test that unknown method returns None."""
        result = create_decompress_process("unknown_method", stdin=None)
        assert result is None

    @patch("shutil.which")
    def test_unavailable_program_returns_none(self, mock_which):
        """Test that unavailable program returns None."""
        mock_which.return_value = None
        result = create_decompress_process("zstd", stdin=None)
        assert result is None


class TestCreateThrottleProcessMore:
    """Additional tests for create_throttle_process."""

    def test_invalid_rate_returns_none(self):
        """Test that invalid rate returns None."""
        with patch("shutil.which", return_value="/usr/bin/pv"):
            result = create_throttle_process("invalid", stdin=None)
            assert result is None

    @patch("shutil.which")
    @patch("subprocess.Popen")
    def test_creates_pv_process(self, mock_popen, mock_which):
        """Test that pv process is created with correct args."""
        mock_which.return_value = "/usr/bin/pv"
        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc

        result = create_throttle_process("10M", stdin=MagicMock())

        assert result == mock_proc
        # Verify pv was called
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args
        assert "pv" in call_args[0][0]

    @patch("shutil.which")
    @patch("subprocess.Popen")
    def test_quiet_mode(self, mock_popen, mock_which):
        """Test quiet mode with show_progress=False."""
        mock_which.return_value = "/usr/bin/pv"
        mock_proc = MagicMock()
        mock_popen.return_value = mock_proc

        result = create_throttle_process("10M", stdin=MagicMock(), show_progress=False)

        call_args = mock_popen.call_args
        assert "-q" in call_args[0][0]


class TestBuildTransferPipelineMore:
    """Additional tests for build_transfer_pipeline."""

    @patch("btrfs_backup_ng.core.transfer.check_pv_available")
    @patch("subprocess.Popen")
    def test_with_throttle(self, mock_popen, mock_check):
        """Test pipeline with rate limiting."""
        mock_check.return_value = True
        mock_process = MagicMock()
        mock_process.stdout = MagicMock()
        mock_popen.return_value = mock_process

        mock_stdout = MagicMock()

        final_stdout, processes = build_transfer_pipeline(
            send_stdout=mock_stdout,
            compress="none",
            rate_limit="10M",
        )

        assert len(processes) == 1
        assert processes[0][0] == "throttle"

    @patch("btrfs_backup_ng.core.transfer.check_compression_available")
    @patch("btrfs_backup_ng.core.transfer.check_pv_available")
    @patch("subprocess.Popen")
    def test_with_compression_and_throttle(self, mock_popen, mock_pv, mock_comp):
        """Test pipeline with both compression and rate limiting."""
        mock_comp.return_value = True
        mock_pv.return_value = True
        mock_process = MagicMock()
        mock_process.stdout = MagicMock()
        mock_popen.return_value = mock_process

        mock_stdout = MagicMock()

        final_stdout, processes = build_transfer_pipeline(
            send_stdout=mock_stdout,
            compress="gzip",
            rate_limit="10M",
        )

        assert len(processes) == 2


class TestParseRateLimitMore:
    """Additional tests for parse_rate_limit."""

    def test_lowercase_units(self):
        """Test lowercase unit suffixes."""
        assert parse_rate_limit("10k") == 10 * 1024
        assert parse_rate_limit("10m") == 10 * 1024 * 1024
        assert parse_rate_limit("1g") == 1024 * 1024 * 1024

    def test_zero_value(self):
        """Test zero values."""
        assert parse_rate_limit("0M") == 0
        assert parse_rate_limit("0") == 0

    def test_large_values(self):
        """Test very large values."""
        result = parse_rate_limit("100G")
        assert result == 100 * 1024 * 1024 * 1024

    def test_invalid_number_with_suffix(self):
        """Test invalid number with valid suffix returns None."""
        # Has suffix but invalid number portion
        assert parse_rate_limit("abcM") is None
        assert parse_rate_limit("M") is None
        assert parse_rate_limit("xyzK") is None

    def test_invalid_number_without_suffix(self):
        """Test invalid number without suffix returns None."""
        # No suffix, but not a valid integer
        assert parse_rate_limit("abc") is None
        assert parse_rate_limit("12.34.56") is None


class TestBuildReceivePipeline:
    """Tests for build_receive_pipeline function."""

    def test_no_compression(self):
        """Test receive pipeline with no compression."""
        mock_stdout = MagicMock()

        final_stdout, processes = build_receive_pipeline(
            input_stdout=mock_stdout,
            compress="none",
        )

        assert final_stdout == mock_stdout
        assert len(processes) == 0

    def test_empty_compression(self):
        """Test receive pipeline with empty compression string."""
        mock_stdout = MagicMock()

        final_stdout, processes = build_receive_pipeline(
            input_stdout=mock_stdout,
            compress="",
        )

        assert final_stdout == mock_stdout
        assert len(processes) == 0

    @patch("btrfs_backup_ng.core.transfer.check_compression_available")
    @patch("subprocess.Popen")
    def test_with_decompression(self, mock_popen, mock_check):
        """Test receive pipeline with decompression."""
        mock_check.return_value = True
        mock_process = MagicMock()
        mock_process.stdout = MagicMock()
        mock_popen.return_value = mock_process

        mock_stdout = MagicMock()

        final_stdout, processes = build_receive_pipeline(
            input_stdout=mock_stdout,
            compress="gzip",
        )

        assert len(processes) == 1
        assert processes[0][0] == "decompress"
        assert final_stdout == mock_process.stdout


class TestCreateDecompressProcessActual:
    """Tests for create_decompress_process with actual process creation."""

    @patch("btrfs_backup_ng.core.transfer.check_compression_available")
    @patch("subprocess.Popen")
    def test_creates_process_with_correct_command(self, mock_popen, mock_check):
        """Test that decompression process is created with correct command."""
        mock_check.return_value = True
        mock_process = MagicMock()
        mock_popen.return_value = mock_process

        mock_stdin = MagicMock()
        result = create_decompress_process("gzip", stdin=mock_stdin)

        assert result == mock_process
        mock_popen.assert_called_once()
        # Verify the command includes gzip decompression
        call_args = mock_popen.call_args
        assert "gzip" in call_args[0][0][0] or "gunzip" in str(call_args[0][0])


class TestCreateCompressProcessActual:
    """Tests for create_compress_process with actual process creation."""

    @patch("btrfs_backup_ng.core.transfer.check_compression_available")
    @patch("subprocess.Popen")
    def test_creates_process_with_correct_command(self, mock_popen, mock_check):
        """Test that compression process is created with correct command."""
        mock_check.return_value = True
        mock_process = MagicMock()
        mock_popen.return_value = mock_process

        mock_stdin = MagicMock()
        result = create_compress_process("zstd", stdin=mock_stdin)

        assert result == mock_process
        mock_popen.assert_called_once()


class TestCleanupPipelineExceptions:
    """Tests for cleanup_pipeline exception handling."""

    def test_cleanup_handles_terminate_exception(self):
        """Test that cleanup handles exception during terminate."""
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None  # Still running
        mock_proc.terminate.side_effect = OSError("Cannot terminate")
        mock_proc.kill.return_value = None

        # Should not raise, should try to kill instead
        cleanup_pipeline([("test", mock_proc)])

        mock_proc.terminate.assert_called_once()
        mock_proc.kill.assert_called_once()

    def test_cleanup_handles_kill_exception(self):
        """Test that cleanup handles exception during kill."""
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.terminate.side_effect = OSError("Cannot terminate")
        mock_proc.kill.side_effect = OSError("Cannot kill")

        # Should not raise even if both terminate and kill fail
        cleanup_pipeline([("test", mock_proc)])

    def test_cleanup_handles_wait_timeout(self):
        """Test that cleanup handles wait timeout."""
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.terminate.return_value = None
        mock_proc.wait.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=5)
        mock_proc.kill.return_value = None

        # Should not raise, should try to kill after timeout
        cleanup_pipeline([("test", mock_proc)])

        mock_proc.kill.assert_called_once()
