"""Tests for SSH endpoint utilities."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.endpoint.ssh import (
    RECEIVE_IDLE_TIMEOUT,
    SSHEndpoint,
    _build_receive_command,
)


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


class TestSSHEndpointConfigPreservation:
    """SSH-specific config keys survive the base Endpoint.__init__ whitelist.

    Regression: the base rebuilds config from a fixed key set, dropping SSH keys,
    so ssh://user@host and --ssh-sudo / --ssh-key were silently lost (the username
    fell back to SUDO_USER/current user).
    """

    @pytest.fixture(autouse=True)
    def _isolated_home(self, tmp_path, monkeypatch):
        # Constructing a real SSHEndpoint sets up an SSH ControlMaster dir under
        # ~/.ssh; point HOME at a temp dir so tests never touch the real one (and
        # so they run on a fresh account/CI runner with no ~/.ssh).
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("SUDO_USER", raising=False)

    def test_username_from_url_is_preserved(self):
        from btrfs_backup_ng.endpoint import choose_endpoint

        ep = choose_endpoint(
            "ssh://root@host/path",
            {"path": "ssh://root@host/path", "snap_prefix": ""},
        )
        assert ep.config.get("username") == "root"

    def test_ssh_sudo_and_identity_are_preserved(self):
        from btrfs_backup_ng.endpoint import choose_endpoint

        ep = choose_endpoint(
            "ssh://u@host/p",
            {
                "path": "ssh://u@host/p",
                "snap_prefix": "",
                "ssh_sudo": True,
                "ssh_identity_file": "/key",
            },
        )
        assert ep.config.get("username") == "u"
        assert ep.config.get("ssh_sudo") is True
        assert ep.config.get("ssh_identity_file") == "/key"


class TestVerifySnapshotUsesExactPath:
    """`_verify_snapshot_exists` checks the exact received path, not a bare name.

    `btrfs receive` names the received subvolume after the source subvol's
    basename. For snapper that is always "snapshot" (source is
    <config>/.snapshots/<N>/snapshot), so verification must be scoped to exactly
    {dest_path}/snapshot. A filesystem-wide name search would match a same-named
    subvolume elsewhere on the destination (e.g. a sibling .snapshots/<other>/
    snapshot) and report a FAILED transfer as succeeded -- a silent phantom
    backup.
    """

    @staticmethod
    def _endpoint(existing_paths: set[str]) -> SSHEndpoint:
        # Build a bare endpoint without opening any SSH connection. The fake exec
        # reports success only when a command targets a path that "exists" -- the
        # target path is always the last argument (`subvolume show <p>` / `test -d <p>`).
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"ssh_sudo": False}

        def fake_exec(cmd, **kwargs):
            target = cmd[-1]
            rc = 0 if target in existing_paths else 1
            return SimpleNamespace(returncode=rc, stdout=b"", stderr=b"")

        ep._exec_remote_command = MagicMock(side_effect=fake_exec)  # type: ignore[method-assign]
        ep._exec_remote_command_with_retry = MagicMock(side_effect=fake_exec)  # type: ignore[method-assign]
        return ep

    def test_exact_received_path_is_verified(self):
        ep = self._endpoint({"/root/dest/.snapshots/5/snapshot"})
        assert ep._verify_snapshot_exists("/root/dest/.snapshots/5", "snapshot") is True

    def test_sibling_snapshot_is_not_a_false_positive(self):
        # The transfer into .snapshots/5 failed (no .snapshots/5/snapshot), but a
        # prior .snapshots/4/snapshot exists on the destination. Verification must
        # NOT report success off the sibling.
        ep = self._endpoint({"/root/dest/.snapshots/4/snapshot"})
        assert (
            ep._verify_snapshot_exists("/root/dest/.snapshots/5", "snapshot") is False
        )

    def test_missing_path_is_false(self):
        ep = self._endpoint(set())
        assert (
            ep._verify_snapshot_exists("/root/dest/.snapshots/5", "snapshot") is False
        )

    def test_native_unique_name_verified(self):
        # Native backups pass a globally unique name; exact-path check still works.
        ep = self._endpoint({"/mnt/backup/host-20260718-1200"})
        assert ep._verify_snapshot_exists("/mnt/backup", "host-20260718-1200") is True
