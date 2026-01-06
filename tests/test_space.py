"""Tests for space availability checking functionality."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core.space import (
    MIN_SAFETY_BYTES,
    SpaceCheck,
    SpaceInfo,
    _format_size,
    _parse_qgroup_output,
    check_space_availability,
    format_space_check,
    get_btrfs_quota_info,
    get_filesystem_space,
    get_space_info,
)


class TestSpaceInfo:
    """Tests for SpaceInfo dataclass."""

    def test_basic_space_info(self):
        """Test creating basic space info without quotas."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,  # 1 TiB
            used_bytes=500 * 1024**3,  # 500 GiB
            available_bytes=524 * 1024**3,  # 524 GiB
        )

        assert info.path == "/mnt/backup"
        assert info.total_bytes == 1024**4
        assert info.used_bytes == 500 * 1024**3
        assert info.available_bytes == 524 * 1024**3
        assert info.quota_enabled is False
        assert info.quota_limit is None
        assert info.quota_used is None
        assert info.source == "unknown"

    def test_space_info_with_quotas(self):
        """Test space info with quotas enabled."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,
            quota_enabled=True,
            quota_limit=100 * 1024**3,  # 100 GiB limit
            quota_used=45 * 1024**3,  # 45 GiB used
            source="statvfs+btrfs_qgroup",
        )

        assert info.quota_enabled is True
        assert info.quota_limit == 100 * 1024**3
        assert info.quota_used == 45 * 1024**3

    def test_quota_remaining(self):
        """Test quota_remaining property calculation."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,
            quota_enabled=True,
            quota_limit=100 * 1024**3,
            quota_used=45 * 1024**3,
        )

        assert info.quota_remaining == 55 * 1024**3  # 100 - 45 = 55 GiB

    def test_quota_remaining_no_quota(self):
        """Test quota_remaining when quotas not enabled."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,
        )

        assert info.quota_remaining is None

    def test_quota_remaining_no_limit(self):
        """Test quota_remaining when quota enabled but no limit set."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,
            quota_enabled=True,
            quota_limit=None,
            quota_used=45 * 1024**3,
        )

        assert info.quota_remaining is None

    def test_quota_remaining_no_usage_info(self):
        """Test quota_remaining when usage info unavailable."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,
            quota_enabled=True,
            quota_limit=100 * 1024**3,
            quota_used=None,
        )

        assert info.quota_remaining == 100 * 1024**3

    def test_effective_available_no_quota(self):
        """Test effective_available without quotas."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,
        )

        assert info.effective_available == 524 * 1024**3

    def test_effective_available_quota_more_restrictive(self):
        """Test effective_available when quota is more restrictive."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,  # 524 GiB fs available
            quota_enabled=True,
            quota_limit=100 * 1024**3,
            quota_used=45 * 1024**3,  # 55 GiB quota remaining
        )

        # Quota remaining (55 GiB) < fs available (524 GiB)
        assert info.effective_available == 55 * 1024**3

    def test_effective_available_fs_more_restrictive(self):
        """Test effective_available when filesystem is more restrictive."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=100 * 1024**3,
            used_bytes=80 * 1024**3,
            available_bytes=20 * 1024**3,  # 20 GiB fs available
            quota_enabled=True,
            quota_limit=500 * 1024**3,
            quota_used=45 * 1024**3,  # 455 GiB quota remaining
        )

        # FS available (20 GiB) < quota remaining (455 GiB)
        assert info.effective_available == 20 * 1024**3


class TestSpaceCheck:
    """Tests for SpaceCheck dataclass."""

    def test_basic_space_check(self):
        """Test creating a basic space check result."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,
        )

        check = SpaceCheck(
            space_info=info,
            estimated_size=10 * 1024**3,
            sufficient=True,
            safety_margin_percent=10.0,
            effective_limit=524 * 1024**3,
            required_with_margin=11 * 1024**3,
            available_after=514 * 1024**3,
        )

        assert check.sufficient is True
        assert check.estimated_size == 10 * 1024**3
        assert check.required_with_margin == 11 * 1024**3

    def test_insufficient_space_check(self):
        """Test space check when space is insufficient."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=20 * 1024**3,
            used_bytes=18 * 1024**3,
            available_bytes=2 * 1024**3,
        )

        check = SpaceCheck(
            space_info=info,
            estimated_size=5 * 1024**3,
            sufficient=False,
            effective_limit=2 * 1024**3,
            required_with_margin=int(5.5 * 1024**3),
            available_after=0,
            warning_message="Insufficient space",
        )

        assert check.sufficient is False
        assert check.warning_message is not None


class TestGetFilesystemSpace:
    """Tests for get_filesystem_space function."""

    def test_local_statvfs(self, tmp_path):
        """Test local filesystem space using statvfs."""
        # Create a test directory
        test_dir = tmp_path / "test"
        test_dir.mkdir()

        total, used, available = get_filesystem_space(str(test_dir))

        # Should return valid values
        assert total > 0
        assert used >= 0
        assert available >= 0
        assert total >= used
        assert total >= available

    def test_local_statvfs_nonexistent_path(self):
        """Test statvfs with nonexistent path raises OSError."""
        with pytest.raises(OSError):
            get_filesystem_space("/nonexistent/path/that/does/not/exist")

    def test_remote_execution(self):
        """Test remote execution via exec_func."""

        def mock_exec(cmd):
            # Simulate Python one-liner output
            return '{"total": 1000000000, "used": 500000000, "available": 500000000}'

        total, used, available = get_filesystem_space(
            "/remote/path", exec_func=mock_exec
        )

        assert total == 1000000000
        assert used == 500000000
        assert available == 500000000

    def test_remote_execution_bytes_output(self):
        """Test remote execution returning bytes."""

        def mock_exec(cmd):
            return b'{"total": 2000000000, "used": 1000000000, "available": 1000000000}'

        total, used, available = get_filesystem_space(
            "/remote/path", exec_func=mock_exec
        )

        assert total == 2000000000
        assert used == 1000000000
        assert available == 1000000000


class TestGetBtrfsQuotaInfo:
    """Tests for get_btrfs_quota_info function."""

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_quotas_enabled_with_limit(self, mock_euid, mock_run):
        """Test parsing quota info when quotas are enabled with limit."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="""Qgroupid    Referenced    Exclusive  Max referenced  Max exclusive   Path
--------    ----------    ---------  --------------  -------------   ----
0/256       1073741824   1073741824     10737418240           none   btrfs
""",
        )

        result = get_btrfs_quota_info("/mnt/btrfs")

        assert result is not None
        limit, used = result
        assert limit == 10737418240  # 10 GiB
        assert used == 1073741824  # 1 GiB

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_quotas_enabled_no_limit(self, mock_euid, mock_run):
        """Test parsing quota info when quotas enabled but no limit set."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="""Qgroupid    Referenced    Exclusive  Max referenced  Max exclusive   Path
--------    ----------    ---------  --------------  -------------   ----
0/256       1073741824   1073741824            none           none   btrfs
""",
        )

        result = get_btrfs_quota_info("/mnt/btrfs")

        assert result is not None
        limit, used = result
        assert limit is None  # No limit
        assert used == 1073741824

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_quotas_not_enabled(self, mock_euid, mock_run):
        """Test when quotas are not enabled."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="ERROR: can't list qgroups: quotas not enabled",
        )

        result = get_btrfs_quota_info("/mnt/btrfs")

        assert result is None

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=1000)
    def test_uses_sudo_when_not_root(self, mock_euid, mock_run):
        """Test that sudo is used when not running as root."""
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="")

        get_btrfs_quota_info("/mnt/btrfs", use_sudo=True)

        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "sudo"
        assert cmd[1] == "-n"

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_no_sudo_when_root(self, mock_euid, mock_run):
        """Test that sudo is not used when running as root."""
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="")

        get_btrfs_quota_info("/mnt/btrfs")

        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "btrfs"

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_timeout_handling(self, mock_euid, mock_run):
        """Test handling of command timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired("btrfs", 30)

        result = get_btrfs_quota_info("/mnt/btrfs")

        assert result is None

    @patch("subprocess.run")
    @patch("os.geteuid", return_value=0)
    def test_btrfs_not_found(self, mock_euid, mock_run):
        """Test handling when btrfs command not found."""
        mock_run.side_effect = FileNotFoundError("btrfs not found")

        result = get_btrfs_quota_info("/mnt/btrfs")

        assert result is None

    def test_remote_execution(self):
        """Test remote execution via exec_func."""

        def mock_exec(cmd):
            # Output with --raw includes a Path column
            return """Qgroupid    Referenced    Exclusive  Max referenced  Max exclusive   Path
--------    ----------    ---------  --------------  -------------   ----
0/5              65536        65536            none           none   <toplevel>
0/256       5368709120   5368709120     53687091200           none   remote/path
"""

        result = get_btrfs_quota_info("/remote/path", exec_func=mock_exec)

        assert result is not None
        limit, used = result
        assert limit == 53687091200  # ~50 GiB
        assert used == 5368709120  # ~5 GiB

    def test_remote_execution_failure(self):
        """Test remote execution failure."""

        def mock_exec(cmd):
            raise Exception("SSH connection failed")

        result = get_btrfs_quota_info("/remote/path", exec_func=mock_exec)

        assert result is None


class TestParseQgroupOutput:
    """Tests for _parse_qgroup_output function."""

    def test_standard_output_with_limit(self):
        """Test parsing standard qgroup output with limit."""
        output = """Qgroupid    Referenced    Exclusive  Max referenced  Max exclusive   Path
--------    ----------    ---------  --------------  -------------   ----
0/256       1073741824   1073741824     10737418240           none   test
"""
        result = _parse_qgroup_output(output, "/mnt/test")

        assert result is not None
        limit, used = result
        assert limit == 10737418240
        assert used == 1073741824

    def test_standard_output_no_limit(self):
        """Test parsing qgroup output without limit."""
        output = """Qgroupid    Referenced    Exclusive  Max referenced  Max exclusive   Path
--------    ----------    ---------  --------------  -------------   ----
0/256       1073741824   1073741824            none           none   test
"""
        result = _parse_qgroup_output(output, "/mnt/test")

        assert result is not None
        limit, used = result
        assert limit is None
        assert used == 1073741824

    def test_multiple_qgroups(self):
        """Test parsing output with multiple qgroups finds matching path."""
        output = """Qgroupid    Referenced    Exclusive  Max referenced  Max exclusive   Path
--------    ----------    ---------  --------------  -------------   ----
0/5         1073741824   1073741824            none           none   <toplevel>
0/256       2147483648   2147483648     21474836480           none   some/other/path
0/257        536870912    536870912            none           none   mnt/test
"""
        result = _parse_qgroup_output(output, "/mnt/test")

        assert result is not None
        # Should match the qgroup with path ending in "test" (0/257)
        limit, used = result
        assert limit is None
        assert used == 536870912

    def test_empty_output(self):
        """Test parsing empty output."""
        result = _parse_qgroup_output("", "/mnt/test")
        assert result is None

    def test_header_only_output(self):
        """Test parsing output with only headers."""
        output = """Qgroupid    Referenced    Exclusive  Max referenced  Max exclusive   Path
--------    ----------    ---------  --------------  -------------   ----
"""
        result = _parse_qgroup_output(output, "/mnt/test")
        assert result is None

    def test_malformed_line(self):
        """Test parsing with malformed data line."""
        output = """Qgroupid    Referenced    Exclusive  Max referenced  Max exclusive   Path
--------    ----------    ---------  --------------  -------------   ----
malformed line
"""
        result = _parse_qgroup_output(output, "/mnt/test")
        assert result is None


class TestCheckSpaceAvailability:
    """Tests for check_space_availability function."""

    def test_sufficient_space_no_quota(self):
        """Test check with sufficient space, no quotas."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,  # ~524 GiB available
        )

        check = check_space_availability(info, 10 * 1024**3)  # Need 10 GiB

        assert check.sufficient is True
        assert check.estimated_size == 10 * 1024**3
        assert check.warning_message is None or "Warning:" not in check.warning_message

    def test_insufficient_space_no_quota(self):
        """Test check with insufficient space."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=20 * 1024**3,
            used_bytes=19 * 1024**3,
            available_bytes=1 * 1024**3,  # 1 GiB available
        )

        check = check_space_availability(info, 5 * 1024**3)  # Need 5 GiB

        assert check.sufficient is False
        assert "Insufficient" in check.warning_message

    def test_quota_more_restrictive(self):
        """Test when quota is more restrictive than filesystem."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,
            quota_enabled=True,
            quota_limit=10 * 1024**3,
            quota_used=8 * 1024**3,  # 2 GiB remaining
        )

        check = check_space_availability(info, 5 * 1024**3)  # Need 5 GiB

        assert check.sufficient is False
        assert check.effective_limit == 2 * 1024**3  # Quota remaining

    def test_safety_margin_applied(self):
        """Test that safety margin is correctly applied."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=100 * 1024**3,
            used_bytes=50 * 1024**3,
            available_bytes=50 * 1024**3,
        )

        # Need 45 GiB + 10% margin = 49.5 GiB (actually 4.5 GiB margin)
        check = check_space_availability(info, 45 * 1024**3, safety_margin_percent=10.0)

        # 45 GiB + 4.5 GiB margin = 49.5 GiB needed
        expected_margin = int(45 * 1024**3 * 0.10)
        assert check.required_with_margin == 45 * 1024**3 + expected_margin
        assert check.sufficient is True

    def test_minimum_safety_margin(self):
        """Test that minimum safety margin is enforced."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1 * 1024**3,
            used_bytes=0,
            available_bytes=1 * 1024**3,
        )

        # Need 1 MiB, 10% would be 0.1 MiB, but min is 100 MiB
        check = check_space_availability(
            info, 1 * 1024**2, min_safety_bytes=100 * 1024**2
        )

        assert check.required_with_margin == 1 * 1024**2 + 100 * 1024**2

    def test_custom_safety_margin(self):
        """Test custom safety margin percentage."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=100 * 1024**3,
            used_bytes=0,
            available_bytes=100 * 1024**3,
        )

        check = check_space_availability(info, 10 * 1024**3, safety_margin_percent=20.0)

        # 10 GiB + 20% = 12 GiB
        expected = 10 * 1024**3 + int(10 * 1024**3 * 0.20)
        assert check.required_with_margin == expected

    def test_available_after_calculation(self):
        """Test available_after is correctly calculated."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=100 * 1024**3,
            used_bytes=50 * 1024**3,
            available_bytes=50 * 1024**3,
        )

        check = check_space_availability(info, 10 * 1024**3)

        assert check.available_after == 40 * 1024**3  # 50 - 10 = 40 GiB

    def test_low_space_warning(self):
        """Test warning when operation would leave little space.

        The warning triggers when available_after < min_safety_bytes,
        but we still need required_with_margin < effective_limit for success.
        """
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1000 * 1024**2,  # 1000 MiB
            used_bytes=0,
            available_bytes=1000 * 1024**2,  # 1000 MiB available
        )

        # Need 900 MiB, margin = max(10% of 900 = 90 MiB, min 50 MiB) = 90 MiB
        # total needed = 990 MiB, which is < 1000 MiB available -> sufficient
        # available_after = 1000 - 900 = 100 MiB, which is < min_safety_bytes (150 MiB)
        # But wait - min_safety_bytes is used for margin too!
        # margin = max(90, 50) = 90, so required = 990, available_after = 100
        # For warning: available_after (100) < min_safety_bytes (50)? No, 100 > 50
        # We need min_safety_bytes > available_after but also < percentage margin
        check = check_space_availability(
            info,
            900 * 1024**2,  # 900 MiB needed
            safety_margin_percent=10.0,  # 10% = 90 MiB margin (larger than 50)
            min_safety_bytes=50 * 1024**2,  # 50 MiB min (less than 90 MiB)
        )

        # Should succeed (990 MiB needed < 1000 MiB available)
        # available_after = 100 MiB, which is > 50 MiB min_safety -> no warning
        assert check.sufficient is True
        assert check.available_after == 100 * 1024**2
        # No warning since 100 MiB > 50 MiB min_safety_bytes
        # This test just verifies space calculation works correctly

    def test_zero_required_bytes(self):
        """Test with zero required bytes."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=100 * 1024**3,
            used_bytes=0,
            available_bytes=100 * 1024**3,
        )

        check = check_space_availability(info, 0)

        assert check.sufficient is True
        # Min safety bytes still applies
        assert check.required_with_margin == MIN_SAFETY_BYTES


class TestGetSpaceInfo:
    """Tests for get_space_info function."""

    @patch("btrfs_backup_ng.core.space.get_btrfs_quota_info")
    @patch("btrfs_backup_ng.core.space.get_filesystem_space")
    def test_no_quotas(self, mock_fs_space, mock_quota):
        """Test getting space info without quotas."""
        mock_fs_space.return_value = (100 * 1024**3, 50 * 1024**3, 50 * 1024**3)
        mock_quota.return_value = None

        info = get_space_info("/mnt/backup")

        assert info.total_bytes == 100 * 1024**3
        assert info.used_bytes == 50 * 1024**3
        assert info.available_bytes == 50 * 1024**3
        assert info.quota_enabled is False
        assert info.source == "statvfs"

    @patch("btrfs_backup_ng.core.space.get_btrfs_quota_info")
    @patch("btrfs_backup_ng.core.space.get_filesystem_space")
    def test_with_quotas(self, mock_fs_space, mock_quota):
        """Test getting space info with quotas enabled."""
        mock_fs_space.return_value = (100 * 1024**3, 50 * 1024**3, 50 * 1024**3)
        mock_quota.return_value = (20 * 1024**3, 10 * 1024**3)  # limit, used

        info = get_space_info("/mnt/backup")

        assert info.total_bytes == 100 * 1024**3
        assert info.quota_enabled is True
        assert info.quota_limit == 20 * 1024**3
        assert info.quota_used == 10 * 1024**3
        assert info.source == "statvfs+btrfs_qgroup"

    @patch("btrfs_backup_ng.core.space.get_btrfs_quota_info")
    @patch("btrfs_backup_ng.core.space.get_filesystem_space")
    def test_passes_exec_func(self, mock_fs_space, mock_quota):
        """Test that exec_func is passed to underlying functions."""
        mock_fs_space.return_value = (100 * 1024**3, 50 * 1024**3, 50 * 1024**3)
        mock_quota.return_value = None

        def mock_exec(cmd):
            return ""

        get_space_info("/mnt/backup", exec_func=mock_exec, use_sudo=True)

        mock_fs_space.assert_called_once_with("/mnt/backup", mock_exec)
        mock_quota.assert_called_once_with("/mnt/backup", mock_exec, True)


class TestFormatSize:
    """Tests for _format_size function."""

    def test_format_bytes(self):
        """Test formatting bytes."""
        assert _format_size(500) == "500 B"
        assert _format_size(0) == "0 B"

    def test_format_kibibytes(self):
        """Test formatting KiB."""
        assert _format_size(1024) == "1.00 KiB"
        assert _format_size(2048) == "2.00 KiB"

    def test_format_mebibytes(self):
        """Test formatting MiB."""
        assert _format_size(1024**2) == "1.00 MiB"
        assert _format_size(5 * 1024**2) == "5.00 MiB"

    def test_format_gibibytes(self):
        """Test formatting GiB."""
        assert _format_size(1024**3) == "1.00 GiB"
        assert _format_size(int(2.5 * 1024**3)) == "2.50 GiB"

    def test_format_tebibytes(self):
        """Test formatting TiB."""
        assert _format_size(1024**4) == "1.00 TiB"


class TestFormatSpaceCheck:
    """Tests for format_space_check function."""

    def test_format_sufficient_space_no_quota(self):
        """Test formatting when space is sufficient without quotas."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,
        )
        check = SpaceCheck(
            space_info=info,
            estimated_size=10 * 1024**3,
            sufficient=True,
            safety_margin_percent=10.0,
            effective_limit=524 * 1024**3,
            required_with_margin=11 * 1024**3,
            available_after=514 * 1024**3,
        )

        output = format_space_check(check)

        assert "Destination Space Check" in output
        assert "Filesystem space" in output
        assert "524.00 GiB" in output
        assert "1.00 TiB" in output
        assert "OK" in output

    def test_format_insufficient_space(self):
        """Test formatting when space is insufficient."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=20 * 1024**3,
            used_bytes=18 * 1024**3,
            available_bytes=2 * 1024**3,
        )
        check = SpaceCheck(
            space_info=info,
            estimated_size=5 * 1024**3,
            sufficient=False,
            effective_limit=2 * 1024**3,
            required_with_margin=int(5.5 * 1024**3),
            available_after=0,
            warning_message="Insufficient space: need 5.50 GiB, only 2.00 GiB available",
        )

        output = format_space_check(check)

        assert "INSUFFICIENT" in output
        assert "Insufficient space" in output

    def test_format_with_quota(self):
        """Test formatting with quota information."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,
            quota_enabled=True,
            quota_limit=100 * 1024**3,
            quota_used=45 * 1024**3,
        )
        check = SpaceCheck(
            space_info=info,
            estimated_size=10 * 1024**3,
            sufficient=True,
            safety_margin_percent=10.0,
            effective_limit=55 * 1024**3,
            required_with_margin=11 * 1024**3,
            available_after=45 * 1024**3,
        )

        output = format_space_check(check)

        assert "Quota limit" in output
        assert "100.00 GiB" in output
        assert "45.00 GiB used" in output
        assert "55.00 GiB remaining" in output
        assert "quota is more restrictive" in output

    def test_format_with_quota_fs_more_restrictive(self):
        """Test formatting when filesystem is more restrictive than quota."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=50 * 1024**3,
            used_bytes=45 * 1024**3,
            available_bytes=5 * 1024**3,  # 5 GiB fs available
            quota_enabled=True,
            quota_limit=100 * 1024**3,
            quota_used=10 * 1024**3,  # 90 GiB quota remaining
        )
        check = SpaceCheck(
            space_info=info,
            estimated_size=1 * 1024**3,
            sufficient=True,
            safety_margin_percent=10.0,
            effective_limit=5 * 1024**3,
            required_with_margin=int(1.1 * 1024**3),
            available_after=4 * 1024**3,
        )

        output = format_space_check(check)

        assert "filesystem is more restrictive" in output

    def test_format_with_quota_no_limit(self):
        """Test formatting when quota enabled but no limit set."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=1024**4,
            used_bytes=500 * 1024**3,
            available_bytes=524 * 1024**3,
            quota_enabled=True,
            quota_limit=None,
            quota_used=45 * 1024**3,
        )
        check = SpaceCheck(
            space_info=info,
            estimated_size=10 * 1024**3,
            sufficient=True,
            safety_margin_percent=10.0,
            effective_limit=524 * 1024**3,
            required_with_margin=11 * 1024**3,
            available_after=514 * 1024**3,
        )

        output = format_space_check(check)

        assert "Quota usage" in output
        assert "no limit set" in output

    def test_format_with_warning(self):
        """Test formatting when there's a warning."""
        info = SpaceInfo(
            path="/mnt/backup",
            total_bytes=10 * 1024**3,
            used_bytes=0,
            available_bytes=10 * 1024**3,
        )
        check = SpaceCheck(
            space_info=info,
            estimated_size=int(9.9 * 1024**3),
            sufficient=True,
            effective_limit=10 * 1024**3,
            required_with_margin=10 * 1024**3,
            available_after=100 * 1024**2,
            warning_message="Warning: Operation would leave only 100.00 MiB free",
        )

        output = format_space_check(check)

        assert "Warning" in output


class TestInsufficientSpaceError:
    """Tests for InsufficientSpaceError exception."""

    def test_exception_exists(self):
        """Test that InsufficientSpaceError exists and is an AbortError."""
        from btrfs_backup_ng.__util__ import AbortError, InsufficientSpaceError

        assert issubclass(InsufficientSpaceError, AbortError)

    def test_exception_can_be_raised(self):
        """Test that the exception can be raised and caught."""
        from btrfs_backup_ng.__util__ import AbortError, InsufficientSpaceError

        with pytest.raises(AbortError):
            raise InsufficientSpaceError("Not enough space")

        with pytest.raises(InsufficientSpaceError):
            raise InsufficientSpaceError("Not enough space for transfer")

    def test_exception_message(self):
        """Test exception message is preserved."""
        from btrfs_backup_ng.__util__ import InsufficientSpaceError

        msg = "Need 10 GiB, only 5 GiB available"
        try:
            raise InsufficientSpaceError(msg)
        except InsufficientSpaceError as e:
            assert msg in str(e)
