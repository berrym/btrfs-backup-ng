"""Tests for systemd install unit generation (CLI-polish batch)."""

import argparse
from unittest.mock import patch

from btrfs_backup_ng.cli.install import (
    SERVICE_TEMPLATE,
    _resolve_exec_start,
    execute_install,
)


class TestResolveExecStart:
    """The service ExecStart must be an ABSOLUTE path to the real binary, not a
    hardcoded /usr/bin (which breaks --user / pipx / uv / venv installs)."""

    def test_resolves_via_path(self):
        with patch(
            "btrfs_backup_ng.cli.install.shutil.which",
            return_value="/home/u/.local/bin/btrfs-backup-ng",
        ):
            assert _resolve_exec_start() == "/home/u/.local/bin/btrfs-backup-ng"

    def test_falls_back_when_not_on_path(self):
        with patch("btrfs_backup_ng.cli.install.shutil.which", return_value=None):
            assert _resolve_exec_start() == "/usr/bin/btrfs-backup-ng"

    def test_template_uses_resolved_path(self):
        content = SERVICE_TEMPLATE.format(exec_start="/opt/venv/bin/btrfs-backup-ng")
        assert "ExecStart=/opt/venv/bin/btrfs-backup-ng run" in content
        assert "/usr/bin/btrfs-backup-ng run" not in content


class TestExecuteInstallWritesResolvedExecStart:
    """End-to-end: a --user install writes a service file whose ExecStart is the
    resolved binary path."""

    def test_user_install_service_has_resolved_execstart(self, tmp_path, capsys):
        args = argparse.Namespace(
            timer="daily", oncalendar=None, user=True, verbose=False, quiet=False
        )
        with (
            patch("btrfs_backup_ng.cli.install.Path.home", return_value=tmp_path),
            patch(
                "btrfs_backup_ng.cli.install.shutil.which",
                return_value="/home/u/.local/bin/btrfs-backup-ng",
            ),
        ):
            rc = execute_install(args)
        assert rc == 0
        service = tmp_path / ".config" / "systemd" / "user" / "btrfs-backup-ng.service"
        assert service.exists()
        assert "ExecStart=/home/u/.local/bin/btrfs-backup-ng run" in service.read_text()
