"""Tests for SSH endpoint utilities."""

import pytest

from btrfs_backup_ng.endpoint.ssh import RECEIVE_IDLE_TIMEOUT, _build_receive_command


class TestBuildReceiveCommand:
    """Tests for _build_receive_command function."""

    def test_basic_receive_no_sudo(self):
        """Test basic receive command without sudo."""
        cmd = _build_receive_command("/mnt/backup")
        assert "btrfs receive /mnt/backup" in cmd
        assert "sudo" not in cmd
        assert "sh -c" in cmd

    def test_receive_with_passwordless_sudo(self):
        """Test receive command with passwordless sudo."""
        cmd = _build_receive_command("/mnt/backup", use_sudo=True)
        assert "sudo -n btrfs receive /mnt/backup" in cmd
        assert "sh -c" in cmd

    def test_receive_with_password_sudo(self):
        """Test receive command with password-based sudo."""
        cmd = _build_receive_command(
            "/mnt/backup", use_sudo=True, password_on_stdin=True
        )
        assert "sudo -S btrfs receive /mnt/backup" in cmd
        assert "sh -c" in cmd

    def test_uses_exec_for_direct_signal_handling(self):
        """Test that exec is used to replace shell with btrfs receive."""
        cmd = _build_receive_command("/mnt/backup", use_sudo=True)
        # Using exec ensures signals go directly to btrfs receive
        assert "exec" in cmd

    def test_traps_pipe_signal(self):
        """Test that SIGPIPE is handled."""
        cmd = _build_receive_command("/mnt/backup", use_sudo=True)
        # Should trap PIPE signal
        assert "trap" in cmd
        assert "PIPE" in cmd

    def test_escaped_path_preserved(self):
        """Test that already-escaped paths are preserved."""
        # Path with spaces (pre-escaped)
        cmd = _build_receive_command("'/mnt/my backup'")
        assert "'/mnt/my backup'" in cmd

    def test_default_idle_timeout_constant(self):
        """Test that default idle timeout is defined."""
        assert RECEIVE_IDLE_TIMEOUT == 300  # 5 minutes

    def test_no_password_uses_sudo_n(self):
        """Test that passwordless mode uses sudo -n flag."""
        cmd = _build_receive_command(
            "/mnt/backup", use_sudo=True, password_on_stdin=False
        )
        assert "sudo -n" in cmd
        assert "sudo -S" not in cmd

    def test_password_mode_uses_sudo_s(self):
        """Test that password mode uses sudo -S flag."""
        cmd = _build_receive_command(
            "/mnt/backup", use_sudo=True, password_on_stdin=True
        )
        assert "sudo -S" in cmd
        assert "sudo -n" not in cmd
