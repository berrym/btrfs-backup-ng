"""Tests for utility module."""

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng import encode_path_for_dir
from btrfs_backup_ng.__util__ import (
    DATE_FORMAT,
    AbortError,
    Snapshot,
    SnapshotTransferError,
    date_to_str,
    log_heading,
    read_locks,
    str_to_date,
    write_locks,
)


class TestEncodePathForDir:
    """Tests for encode_path_for_dir function."""

    def test_root_path(self):
        """Test encoding root path."""
        result = encode_path_for_dir(Path("/"))
        assert result == ""

    def test_simple_path(self):
        """Test encoding a simple path."""
        result = encode_path_for_dir(Path("/home"))
        assert result == "home"

    def test_nested_path(self):
        """Test encoding a nested path."""
        result = encode_path_for_dir(Path("/home/user/documents"))
        assert result == "home_user_documents"

    def test_var_log_path(self):
        """Test encoding /var/log path."""
        result = encode_path_for_dir(Path("/var/log"))
        assert result == "var_log"


class TestDateToStr:
    """Tests for date_to_str function."""

    def test_with_timestamp(self):
        """Test converting a specific timestamp."""
        ts = time.strptime("20240115-143022", DATE_FORMAT)
        result = date_to_str(ts)
        assert result == "20240115-143022"

    def test_with_custom_format(self):
        """Test with custom format string."""
        ts = time.strptime("20240115-143022", DATE_FORMAT)
        result = date_to_str(ts, fmt="%Y-%m-%d")
        assert result == "2024-01-15"

    def test_without_timestamp(self):
        """Test that it returns current time when no timestamp given."""
        result = date_to_str()
        # Should be a valid date string matching the format
        parsed = time.strptime(result, DATE_FORMAT)
        assert parsed is not None

    def test_returns_string(self):
        """Test that result is a string."""
        result = date_to_str()
        assert isinstance(result, str)


class TestStrToDate:
    """Tests for str_to_date function."""

    def test_parse_valid_string(self):
        """Test parsing a valid date string."""
        result = str_to_date("20240115-143022")
        assert result.tm_year == 2024
        assert result.tm_mon == 1
        assert result.tm_mday == 15
        assert result.tm_hour == 14
        assert result.tm_min == 30
        assert result.tm_sec == 22

    def test_with_custom_format(self):
        """Test parsing with custom format."""
        result = str_to_date("2024-01-15", fmt="%Y-%m-%d")
        assert result.tm_year == 2024
        assert result.tm_mon == 1
        assert result.tm_mday == 15

    def test_without_time_string(self):
        """Test that it returns current time when no string given."""
        result = str_to_date()
        # Should be close to current time
        now = time.localtime()
        assert result.tm_year == now.tm_year
        assert result.tm_mon == now.tm_mon
        assert result.tm_mday == now.tm_mday

    def test_invalid_format_raises(self):
        """Test that invalid format raises ValueError."""
        with pytest.raises(ValueError):
            str_to_date("not-a-date")


class TestLogHeading:
    """Tests for log_heading function."""

    def test_basic_heading(self):
        """Test creating a basic heading."""
        result = log_heading("Test")
        assert "Test" in result
        assert result.startswith("--[ Test ]")
        assert len(result) == 50

    def test_long_caption(self):
        """Test with a long caption."""
        result = log_heading("This is a very long caption for testing")
        assert "This is a very long caption" in result

    def test_empty_caption(self):
        """Test with empty caption."""
        result = log_heading("")
        assert "--[  ]" in result


class TestReadLocks:
    """Tests for read_locks function."""

    def test_read_empty_string(self):
        """Test reading empty string."""
        result = read_locks("")
        assert result == {}

    def test_read_whitespace_only(self):
        """Test reading whitespace-only string."""
        result = read_locks("   \n  \t  ")
        assert result == {}

    def test_read_valid_locks(self):
        """Test reading valid lock file content."""
        content = json.dumps(
            {
                "home-20240115-143022": {
                    "locks": ["backup-job-1"],
                    "parent_locks": ["child-snap"],
                }
            }
        )
        result = read_locks(content)
        assert "home-20240115-143022" in result
        assert result["home-20240115-143022"]["locks"] == ["backup-job-1"]
        assert result["home-20240115-143022"]["parent_locks"] == ["child-snap"]

    def test_read_multiple_snapshots(self):
        """Test reading multiple snapshot locks."""
        content = json.dumps(
            {
                "snap1": {"locks": ["lock1"], "parent_locks": []},
                "snap2": {"locks": ["lock2", "lock3"], "parent_locks": ["parent1"]},
            }
        )
        result = read_locks(content)
        assert len(result) == 2
        assert "snap1" in result
        assert "snap2" in result

    def test_deduplicates_locks(self):
        """Test that duplicate locks are removed."""
        content = json.dumps(
            {"snap1": {"locks": ["lock1", "lock1", "lock1"], "parent_locks": []}}
        )
        result = read_locks(content)
        assert len(result["snap1"]["locks"]) == 1

    def test_invalid_json_raises(self):
        """Test that invalid JSON raises ValueError."""
        with pytest.raises(ValueError):
            read_locks("not valid json {")

    def test_invalid_structure_raises(self):
        """Test that invalid structure raises ValueError."""
        # Not a dict at top level
        with pytest.raises(ValueError):
            read_locks('["list", "not", "dict"]')

    def test_invalid_lock_type_raises(self):
        """Test that invalid lock type raises ValueError."""
        content = json.dumps({"snap1": {"invalid_type": ["lock1"]}})
        with pytest.raises(ValueError):
            read_locks(content)

    def test_non_string_lock_raises(self):
        """Test that non-string lock raises ValueError."""
        content = json.dumps({"snap1": {"locks": [123], "parent_locks": []}})
        with pytest.raises(ValueError):
            read_locks(content)


class TestWriteLocks:
    """Tests for write_locks function."""

    def test_write_empty_dict(self):
        """Test writing empty dict."""
        result = write_locks({})
        assert json.loads(result) == {}

    def test_write_valid_locks(self):
        """Test writing valid lock dict."""
        locks = {"snap1": {"locks": ["lock1"], "parent_locks": []}}
        result = write_locks(locks)
        parsed = json.loads(result)
        assert parsed == locks

    def test_roundtrip(self):
        """Test that write and read are inverse operations."""
        original = {
            "snap1": {"locks": ["lock1", "lock2"], "parent_locks": ["parent1"]},
            "snap2": {"locks": [], "parent_locks": ["parent2"]},
        }
        written = write_locks(original)
        read_back = read_locks(written)
        # Note: order may differ due to set conversion in read_locks
        assert set(read_back.keys()) == set(original.keys())


class TestSnapshot:
    """Tests for Snapshot class."""

    def test_create_snapshot(self):
        """Test creating a snapshot."""
        endpoint = MagicMock()
        snap = Snapshot("/snapshots", "home-", endpoint)
        assert snap.location.as_posix() == "/snapshots"
        assert snap.prefix == "home-"
        assert snap.endpoint == endpoint
        assert snap.time_obj is not None

    def test_create_with_time(self):
        """Test creating snapshot with specific time."""
        endpoint = MagicMock()
        time_obj = str_to_date("20240115-143022")
        snap = Snapshot("/snapshots", "home-", endpoint, time_obj)
        assert snap.time_obj == time_obj

    def test_get_name(self):
        """Test getting snapshot name."""
        endpoint = MagicMock()
        time_obj = str_to_date("20240115-143022")
        snap = Snapshot("/snapshots", "home-", endpoint, time_obj)
        assert snap.get_name() == "home-20240115-143022"

    def test_get_path(self):
        """Test getting snapshot path."""
        endpoint = MagicMock()
        time_obj = str_to_date("20240115-143022")
        snap = Snapshot("/snapshots", "home-", endpoint, time_obj)
        assert snap.get_path().as_posix() == "/snapshots/home-20240115-143022"

    def test_repr(self):
        """Test string representation."""
        endpoint = MagicMock()
        time_obj = str_to_date("20240115-143022")
        snap = Snapshot("/snapshots", "home-", endpoint, time_obj)
        assert repr(snap) == "home-20240115-143022"

    def test_equality(self):
        """Test snapshot equality."""
        endpoint = MagicMock()
        time_obj = str_to_date("20240115-143022")
        snap1 = Snapshot("/snapshots", "home-", endpoint, time_obj)
        snap2 = Snapshot("/other", "home-", endpoint, time_obj)
        assert snap1 == snap2  # Same prefix and time

    def test_inequality(self):
        """Test snapshot inequality."""
        endpoint = MagicMock()
        time1 = str_to_date("20240115-143022")
        time2 = str_to_date("20240115-153022")
        snap1 = Snapshot("/snapshots", "home-", endpoint, time1)
        snap2 = Snapshot("/snapshots", "home-", endpoint, time2)
        assert snap1 != snap2

    def test_less_than(self):
        """Test snapshot ordering."""
        endpoint = MagicMock()
        time1 = str_to_date("20240115-143022")
        time2 = str_to_date("20240115-153022")
        snap1 = Snapshot("/snapshots", "home-", endpoint, time1)
        snap2 = Snapshot("/snapshots", "home-", endpoint, time2)
        assert snap1 < snap2

    def test_comparison_different_prefix_raises(self):
        """Test that comparing different prefixes raises."""
        endpoint = MagicMock()
        time_obj = str_to_date("20240115-143022")
        snap1 = Snapshot("/snapshots", "home-", endpoint, time_obj)
        snap2 = Snapshot("/snapshots", "var-", endpoint, time_obj)
        with pytest.raises(NotImplementedError):
            _ = snap1 < snap2

    def test_locks_initialized_empty(self):
        """Test that locks are initialized as empty sets."""
        endpoint = MagicMock()
        snap = Snapshot("/snapshots", "home-", endpoint)
        assert snap.locks == set()
        assert snap.parent_locks == set()

    def test_find_parent_already_present(self):
        """Test find_parent when snapshot already present."""
        endpoint = MagicMock()
        time_obj = str_to_date("20240115-143022")
        snap = Snapshot("/snapshots", "home-", endpoint, time_obj)
        present = [snap]
        assert snap.find_parent(present) is None

    def test_find_parent_older_exists(self):
        """Test find_parent when older snapshot exists."""
        endpoint = MagicMock()
        time1 = str_to_date("20240114-143022")
        time2 = str_to_date("20240115-143022")
        snap1 = Snapshot("/snapshots", "home-", endpoint, time1)
        snap2 = Snapshot("/snapshots", "home-", endpoint, time2)
        present = [snap1]
        assert snap2.find_parent(present) == snap1

    def test_find_parent_no_older(self):
        """Test find_parent when no older snapshot exists."""
        endpoint = MagicMock()
        time1 = str_to_date("20240116-143022")
        time2 = str_to_date("20240115-143022")
        snap1 = Snapshot("/snapshots", "home-", endpoint, time1)
        snap2 = Snapshot("/snapshots", "home-", endpoint, time2)
        present = [snap1]
        # Should return oldest present
        assert snap2.find_parent(present) == snap1

    def test_find_parent_empty_list(self):
        """Test find_parent with empty present list."""
        endpoint = MagicMock()
        snap = Snapshot("/snapshots", "home-", endpoint)
        assert snap.find_parent([]) is None


class TestExceptions:
    """Tests for custom exceptions."""

    def test_abort_error(self):
        """Test AbortError exception."""
        with pytest.raises(AbortError):
            raise AbortError("test error")

    def test_snapshot_transfer_error(self):
        """Test SnapshotTransferError inherits from AbortError."""
        with pytest.raises(AbortError):
            raise SnapshotTransferError("transfer failed")

    def test_snapshot_transfer_error_message(self):
        """Test SnapshotTransferError message."""
        try:
            raise SnapshotTransferError("transfer failed")
        except SnapshotTransferError as e:
            assert "transfer failed" in str(e)


class TestExecSubprocess:
    """Tests for exec_subprocess function."""

    def test_successful_command(self):
        """Test running a successful command."""
        from btrfs_backup_ng.__util__ import exec_subprocess

        result = exec_subprocess(["echo", "hello"])
        assert b"hello" in result

    def test_command_with_check_call(self):
        """Test running command with check_call method."""
        from btrfs_backup_ng.__util__ import exec_subprocess

        result = exec_subprocess(["true"], method="check_call")
        assert result == 0

    def test_command_not_found(self):
        """Test handling of command not found."""
        from btrfs_backup_ng.__util__ import exec_subprocess

        with pytest.raises(AbortError):
            exec_subprocess(["nonexistent_command_12345"])

    def test_command_failure(self):
        """Test handling of command failure."""
        from btrfs_backup_ng.__util__ import exec_subprocess

        with pytest.raises(AbortError):
            exec_subprocess(["false"])

    def test_command_with_env(self):
        """Test passing environment variables."""
        from btrfs_backup_ng.__util__ import exec_subprocess

        env = {"MY_VAR": "test_value", "PATH": "/usr/bin:/bin"}
        result = exec_subprocess(["printenv", "MY_VAR"], env=env)
        assert b"test_value" in result

    def test_converts_args_to_strings(self):
        """Test that arguments are converted to strings."""
        from btrfs_backup_ng.__util__ import exec_subprocess

        # Pass an integer argument
        result = exec_subprocess(["echo", 123])
        assert b"123" in result

    def test_command_with_absolute_path_not_found(self):
        """Test that absolute path command not found raises AbortError."""
        from btrfs_backup_ng.__util__ import exec_subprocess

        # Command with absolute path that doesn't exist
        with pytest.raises(AbortError):
            exec_subprocess(["/nonexistent/path/to/command"])

    @patch("subprocess.run")
    @patch("subprocess.check_output")
    def test_which_fallback_on_not_found(self, mock_check_output, mock_run):
        """Test that 'which' is tried when command not found."""
        from btrfs_backup_ng.__util__ import exec_subprocess

        # First call fails with FileNotFoundError
        mock_check_output.side_effect = [
            FileNotFoundError("command not found"),
            b"success",  # Retry succeeds
        ]
        # 'which' finds the command
        mock_run.return_value = MagicMock(returncode=0, stdout="/usr/bin/somecommand\n")

        result = exec_subprocess(["somecommand", "arg"])
        assert result == b"success"

    @patch("subprocess.run")
    @patch("subprocess.check_output")
    def test_which_fallback_not_found_in_path(self, mock_check_output, mock_run):
        """Test behavior when 'which' doesn't find the command."""
        from btrfs_backup_ng.__util__ import exec_subprocess

        mock_check_output.side_effect = FileNotFoundError("command not found")
        # 'which' doesn't find the command
        mock_run.return_value = MagicMock(returncode=1, stdout="")

        with pytest.raises(AbortError):
            exec_subprocess(["somecommand"])

    @patch("subprocess.run")
    @patch("subprocess.check_output")
    def test_which_fallback_exception(self, mock_check_output, mock_run):
        """Test behavior when 'which' itself fails."""
        from btrfs_backup_ng.__util__ import exec_subprocess

        mock_check_output.side_effect = FileNotFoundError("command not found")
        # 'which' raises an exception
        mock_run.side_effect = OSError("which failed")

        with pytest.raises(AbortError):
            exec_subprocess(["somecommand"])

    @patch("subprocess.check_output")
    def test_unexpected_exception(self, mock_check_output):
        """Test handling of unexpected exceptions."""
        from btrfs_backup_ng.__util__ import exec_subprocess

        mock_check_output.side_effect = RuntimeError("unexpected error")

        with pytest.raises(AbortError, match="Error executing"):
            exec_subprocess(["somecommand"])


class TestIsBtrfs:
    """Tests for is_btrfs function."""

    def test_with_existing_path(self, tmp_path):
        """Test checking an existing path."""
        from btrfs_backup_ng.__util__ import is_btrfs

        # tmp_path is likely not btrfs
        result = is_btrfs(tmp_path)
        # Result depends on actual filesystem, just verify it returns bool
        assert isinstance(result, bool)

    def test_with_root(self):
        """Test checking root path."""
        from btrfs_backup_ng.__util__ import is_btrfs

        result = is_btrfs("/")
        assert isinstance(result, bool)

    def test_parses_mounts_file(self, tmp_path):
        """Test parsing of /proc/mounts format."""
        from btrfs_backup_ng.__util__ import is_btrfs

        # Create a mock mounts file
        mounts_content = """/dev/sda1 / ext4 rw,relatime 0 0
/dev/sdb1 /home btrfs rw,relatime,ssd 0 0
tmpfs /tmp tmpfs rw,nosuid,nodev 0 0
"""
        mock_mounts = tmp_path / "mounts"
        mock_mounts.write_text(mounts_content)

        with patch("btrfs_backup_ng.__util__.MOUNTS_FILE", str(mock_mounts)):
            # /home should be detected as btrfs
            assert is_btrfs("/home") is True
            assert is_btrfs("/home/user") is True
            # / should be ext4, not btrfs
            assert is_btrfs("/") is False
            # /tmp should be tmpfs, not btrfs
            assert is_btrfs("/tmp") is False

    def test_handles_malformed_mounts_line(self, tmp_path):
        """Test handling of malformed lines in mounts file."""
        from btrfs_backup_ng.__util__ import is_btrfs

        # Create mounts file with malformed lines
        mounts_content = """malformed line without enough fields
/dev/sda1 / btrfs rw,relatime 0 0
another bad line
"""
        mock_mounts = tmp_path / "mounts"
        mock_mounts.write_text(mounts_content)

        with patch("btrfs_backup_ng.__util__.MOUNTS_FILE", str(mock_mounts)):
            # Should still work despite malformed lines
            result = is_btrfs("/")
            assert result is True

    def test_best_match_selection(self, tmp_path):
        """Test that best (longest) mount point is selected."""
        from btrfs_backup_ng.__util__ import is_btrfs

        # Create mounts file with nested mount points
        mounts_content = """/dev/sda1 / ext4 rw,relatime 0 0
/dev/sdb1 /home btrfs rw,relatime 0 0
/dev/sdc1 /home/user ext4 rw,relatime 0 0
"""
        mock_mounts = tmp_path / "mounts"
        mock_mounts.write_text(mounts_content)

        with patch("btrfs_backup_ng.__util__.MOUNTS_FILE", str(mock_mounts)):
            # /home/user/docs should match /home/user (ext4), not /home (btrfs)
            assert is_btrfs("/home/user/docs") is False
            # /home/other should match /home (btrfs)
            assert is_btrfs("/home/other") is True


class TestIsSubvolume:
    """Tests for is_subvolume function."""

    def test_with_regular_dir(self, tmp_path):
        """Test checking a regular directory."""
        from btrfs_backup_ng.__util__ import is_subvolume

        # A regular directory should not be a subvolume
        result = is_subvolume(tmp_path)
        assert result is False

    def test_with_nonexistent_path(self, tmp_path):
        """Test checking nonexistent path."""
        from btrfs_backup_ng.__util__ import is_subvolume

        # Should return False for non-btrfs or handle error gracefully
        result = is_subvolume(tmp_path / "nonexistent")
        # May raise or return False depending on implementation
        assert isinstance(result, bool) or result is False

    @patch("btrfs_backup_ng.__util__.is_btrfs")
    def test_returns_false_if_not_btrfs(self, mock_is_btrfs, tmp_path):
        """Test that is_subvolume returns False if not on btrfs."""
        from btrfs_backup_ng.__util__ import is_subvolume

        mock_is_btrfs.return_value = False
        result = is_subvolume(tmp_path)
        assert result is False

    @patch("btrfs_backup_ng.__util__.is_btrfs")
    def test_checks_inode_on_btrfs(self, mock_is_btrfs, tmp_path):
        """Test that is_subvolume checks inode when on btrfs."""
        from btrfs_backup_ng.__util__ import is_subvolume

        mock_is_btrfs.return_value = True
        # Regular dir won't have inode 256
        result = is_subvolume(tmp_path)
        assert result is False  # tmp_path is not inode 256


class TestDeleteSubvolume:
    """Tests for delete_subvolume function."""

    @patch("btrfs_backup_ng.__util__.is_subvolume")
    def test_raises_if_not_subvolume(self, mock_is_subvolume, tmp_path):
        """Test that delete_subvolume raises if path is not a subvolume."""
        from btrfs_backup_ng.__util__ import delete_subvolume

        mock_is_subvolume.return_value = False
        with pytest.raises(AbortError, match="not a subvolume"):
            delete_subvolume(tmp_path)

    @patch("btrfs_backup_ng.__util__.exec_subprocess")
    @patch("btrfs_backup_ng.__util__.is_subvolume")
    def test_calls_btrfs_delete(self, mock_is_subvolume, mock_exec, tmp_path):
        """Test that delete_subvolume calls btrfs subvolume delete."""
        from btrfs_backup_ng.__util__ import delete_subvolume

        mock_is_subvolume.return_value = True
        delete_subvolume(tmp_path)

        mock_exec.assert_called_once()
        call_args = mock_exec.call_args[0][0]
        assert call_args[0] == "btrfs"
        assert call_args[1] == "subvolume"
        assert call_args[2] == "delete"
        assert str(tmp_path.resolve()) in call_args[3]


class TestIsMounted:
    """Tests for is_mounted function."""

    def test_root_is_mounted(self, tmp_path):
        """Test that root (/) is detected as mounted."""
        from btrfs_backup_ng.__util__ import is_mounted

        # Create a mock mounts file
        mounts_content = """/dev/sda1 / btrfs rw,relatime 0 0
/dev/sdb1 /home btrfs rw,relatime 0 0
"""
        mock_mounts = tmp_path / "mounts"
        mock_mounts.write_text(mounts_content)

        with patch("btrfs_backup_ng.__util__.MOUNTS_FILE", str(mock_mounts)):
            assert is_mounted("/") is True
            assert is_mounted("/home") is True

    def test_non_mount_point(self, tmp_path):
        """Test that non-mount points return False."""
        from btrfs_backup_ng.__util__ import is_mounted

        # Create a mock mounts file
        mounts_content = """/dev/sda1 / btrfs rw,relatime 0 0
"""
        mock_mounts = tmp_path / "mounts"
        mock_mounts.write_text(mounts_content)

        with patch("btrfs_backup_ng.__util__.MOUNTS_FILE", str(mock_mounts)):
            # /home is not a mount point in this case
            assert is_mounted("/home") is False
            # Subdirectories are not mount points
            assert is_mounted("/var/log") is False

    def test_handles_malformed_lines(self, tmp_path):
        """Test that malformed lines are skipped."""
        from btrfs_backup_ng.__util__ import is_mounted

        mounts_content = """malformed
/dev/sda1 / btrfs rw 0 0

"""
        mock_mounts = tmp_path / "mounts"
        mock_mounts.write_text(mounts_content)

        with patch("btrfs_backup_ng.__util__.MOUNTS_FILE", str(mock_mounts)):
            assert is_mounted("/") is True


class TestGetMountInfo:
    """Tests for get_mount_info function."""

    def test_get_mount_info_for_root(self, tmp_path):
        """Test getting mount info for root filesystem."""
        from btrfs_backup_ng.__util__ import get_mount_info

        mounts_content = """/dev/sda1 / btrfs rw,relatime 0 0
/dev/sdb1 /home ext4 rw,relatime 0 0
"""
        mock_mounts = tmp_path / "mounts"
        mock_mounts.write_text(mounts_content)

        with patch("btrfs_backup_ng.__util__.MOUNTS_FILE", str(mock_mounts)):
            info = get_mount_info("/")
            assert info is not None
            assert info["mount_point"] == "/"
            assert info["fs_type"] == "btrfs"
            assert info["device"] == "/dev/sda1"

    def test_get_mount_info_for_subpath(self, tmp_path):
        """Test getting mount info for path under mount point."""
        from btrfs_backup_ng.__util__ import get_mount_info

        mounts_content = """/dev/sda1 / btrfs rw,relatime 0 0
/dev/sdb1 /home ext4 rw,relatime 0 0
"""
        mock_mounts = tmp_path / "mounts"
        mock_mounts.write_text(mounts_content)

        with patch("btrfs_backup_ng.__util__.MOUNTS_FILE", str(mock_mounts)):
            info = get_mount_info("/home/user/documents")
            assert info is not None
            assert info["mount_point"] == "/home"
            assert info["fs_type"] == "ext4"

    def test_get_mount_info_best_match(self, tmp_path):
        """Test that longest matching mount point is selected."""
        from btrfs_backup_ng.__util__ import get_mount_info

        mounts_content = """/dev/sda1 / btrfs rw,relatime 0 0
/dev/sdb1 /home ext4 rw,relatime 0 0
/dev/sdc1 /home/user xfs rw,relatime 0 0
"""
        mock_mounts = tmp_path / "mounts"
        mock_mounts.write_text(mounts_content)

        with patch("btrfs_backup_ng.__util__.MOUNTS_FILE", str(mock_mounts)):
            info = get_mount_info("/home/user/documents")
            assert info is not None
            assert info["mount_point"] == "/home/user"
            assert info["fs_type"] == "xfs"

    def test_get_mount_info_handles_malformed(self, tmp_path):
        """Test that malformed lines are skipped."""
        from btrfs_backup_ng.__util__ import get_mount_info

        mounts_content = """malformed line
/dev/sda1 / btrfs rw 0 0
another bad
"""
        mock_mounts = tmp_path / "mounts"
        mock_mounts.write_text(mounts_content)

        with patch("btrfs_backup_ng.__util__.MOUNTS_FILE", str(mock_mounts)):
            info = get_mount_info("/")
            assert info is not None
            assert info["mount_point"] == "/"

    def test_get_mount_info_not_found(self, tmp_path):
        """Test returning None when no match found."""
        from btrfs_backup_ng.__util__ import get_mount_info

        mounts_content = """/dev/sda1 /mnt/disk btrfs rw 0 0
"""
        mock_mounts = tmp_path / "mounts"
        mock_mounts.write_text(mounts_content)

        with patch("btrfs_backup_ng.__util__.MOUNTS_FILE", str(mock_mounts)):
            # /other is not under any mount point in the file
            info = get_mount_info("/other/path")
            assert info is None


class TestInsufficientSpaceError:
    """Tests for InsufficientSpaceError exception."""

    def test_inherits_from_abort_error(self):
        """Test that InsufficientSpaceError inherits from AbortError."""
        from btrfs_backup_ng.__util__ import InsufficientSpaceError

        with pytest.raises(AbortError):
            raise InsufficientSpaceError("Not enough space")

    def test_error_message(self):
        """Test error message is preserved."""
        from btrfs_backup_ng.__util__ import InsufficientSpaceError

        try:
            raise InsufficientSpaceError("Need 10GB, only 5GB available")
        except InsufficientSpaceError as e:
            assert "10GB" in str(e)
            assert "5GB" in str(e)
