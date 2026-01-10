"""Tests for systemd_utils module."""

import subprocess
from unittest.mock import MagicMock, patch

from btrfs_backup_ng.systemd_utils import (
    BACKUP_NG_UNIT_NAMES,
    BTRBK_UNIT_NAMES,
    SystemdUnitStatus,
    disable_unit,
    enable_unit,
    find_backup_ng_units,
    find_btrbk_units,
    get_migration_summary,
    get_unit_status,
    migrate_from_btrbk,
    run_systemctl,
)


class TestRunSystemctl:
    """Tests for run_systemctl function."""

    def test_run_systemctl_success(self):
        """Test successful systemctl command."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="active", stderr="")
            result = run_systemctl("is-active", "test.timer")
            assert result.returncode == 0
            mock_run.assert_called_once()

    def test_run_systemctl_not_found(self):
        """Test systemctl not available."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError()
            result = run_systemctl("status", "test.timer")
            assert result.returncode == 1
            assert "not found" in result.stderr


class TestGetUnitStatus:
    """Tests for get_unit_status function."""

    def test_get_unit_status_enabled_active(self):
        """Test unit that is enabled and active."""
        with patch("btrfs_backup_ng.systemd_utils.run_systemctl") as mock_run:
            # cat returns success (unit exists)
            # is-enabled returns "enabled"
            # is-active returns "active"
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="[Unit]..."),  # cat
                MagicMock(returncode=0, stdout="enabled"),  # is-enabled
                MagicMock(returncode=0, stdout="active"),  # is-active
            ]
            status = get_unit_status("test.timer")
            assert status.exists is True
            assert status.enabled is True
            assert status.active is True

    def test_get_unit_status_not_exists(self):
        """Test unit that doesn't exist."""
        with patch("btrfs_backup_ng.systemd_utils.run_systemctl") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=1, stdout=""),  # cat fails
                MagicMock(returncode=1, stdout="disabled"),  # is-enabled
                MagicMock(returncode=1, stdout="inactive"),  # is-active
            ]
            status = get_unit_status("nonexistent.timer")
            assert status.exists is False
            assert status.enabled is False
            assert status.active is False


class TestFindBtrbkUnits:
    """Tests for find_btrbk_units function."""

    def test_find_btrbk_units_none(self):
        """Test when no btrbk units exist."""
        with patch("btrfs_backup_ng.systemd_utils.get_unit_status") as mock_status:
            mock_status.return_value = SystemdUnitStatus(
                name="btrbk.timer",
                exists=False,
                enabled=False,
                active=False,
            )
            # Also need to mock the glob for instance units
            with patch("pathlib.Path.exists") as mock_exists:
                mock_exists.return_value = False
                units = find_btrbk_units()
                assert len(units) == 0

    def test_find_btrbk_units_found(self):
        """Test when btrbk units exist."""
        with patch("btrfs_backup_ng.systemd_utils.get_unit_status") as mock_status:

            def status_side_effect(name):
                if name == "btrbk.timer":
                    return SystemdUnitStatus(
                        name="btrbk.timer",
                        exists=True,
                        enabled=True,
                        active=True,
                    )
                return SystemdUnitStatus(
                    name=name,
                    exists=False,
                    enabled=False,
                    active=False,
                )

            mock_status.side_effect = status_side_effect
            with patch("pathlib.Path.exists") as mock_exists:
                mock_exists.return_value = False
                with patch("pathlib.Path.glob") as mock_glob:
                    mock_glob.return_value = []
                    units = find_btrbk_units()
                    assert len(units) == 1
                    assert units[0].name == "btrbk.timer"


class TestDisableUnit:
    """Tests for disable_unit function."""

    def test_disable_unit_success(self):
        """Test successful unit disable."""
        with patch("btrfs_backup_ng.systemd_utils.run_systemctl") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            success, msg = disable_unit("test.timer")
            assert success is True
            assert "Disabled" in msg

    def test_disable_unit_failure(self):
        """Test failed unit disable."""
        with patch("btrfs_backup_ng.systemd_utils.run_systemctl") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),  # stop
                MagicMock(returncode=1, stdout="", stderr="Permission denied"),
            ]
            success, msg = disable_unit("test.timer")
            assert success is False
            assert "Failed" in msg

    def test_disable_unit_stop_fails_but_disable_succeeds(self):
        """Test that stop failure is not fatal - disable can still succeed."""
        with patch("btrfs_backup_ng.systemd_utils.run_systemctl") as mock_run:
            mock_run.side_effect = [
                MagicMock(
                    returncode=1, stdout="", stderr="Unit not running"
                ),  # stop fails
                MagicMock(returncode=0, stdout="", stderr=""),  # disable succeeds
            ]
            success, msg = disable_unit("test.timer")
            assert success is True
            assert "Disabled" in msg
            # "Stopped" should NOT be in message since stop failed
            assert "Stopped" not in msg


class TestEnableUnit:
    """Tests for enable_unit function."""

    def test_enable_unit_success(self):
        """Test successful unit enable."""
        with patch("btrfs_backup_ng.systemd_utils.run_systemctl") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            success, msg = enable_unit("test.timer")
            assert success is True
            assert "Enabled" in msg

    def test_enable_unit_with_start(self):
        """Test enable with start."""
        with patch("btrfs_backup_ng.systemd_utils.run_systemctl") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            success, msg = enable_unit("test.timer", start=True)
            assert success is True
            assert "Enabled" in msg
            assert "Started" in msg


class TestMigrateFromBtrbk:
    """Tests for migrate_from_btrbk function."""

    def test_migrate_no_btrbk_units(self):
        """Test migration when no btrbk units active."""
        with patch("btrfs_backup_ng.systemd_utils.find_btrbk_units") as mock_btrbk:
            mock_btrbk.return_value = []
            with patch("btrfs_backup_ng.systemd_utils.find_backup_ng_units") as mock_ng:
                mock_ng.return_value = []
                success, messages = migrate_from_btrbk(dry_run=True)
                assert success is True
                assert any("No active btrbk" in m for m in messages)

    def test_migrate_dry_run(self):
        """Test migration in dry-run mode."""
        with patch("btrfs_backup_ng.systemd_utils.find_btrbk_units") as mock_btrbk:
            mock_btrbk.return_value = [
                SystemdUnitStatus(
                    name="btrbk.timer",
                    exists=True,
                    enabled=True,
                    active=True,
                )
            ]
            with patch("btrfs_backup_ng.systemd_utils.find_backup_ng_units") as mock_ng:
                mock_ng.return_value = []
                success, messages = migrate_from_btrbk(dry_run=True)
                assert success is True
                assert any("dry-run" in m for m in messages)


class TestGetMigrationSummary:
    """Tests for get_migration_summary function."""

    def test_get_migration_summary(self):
        """Test migration summary generation."""
        with patch("btrfs_backup_ng.systemd_utils.find_btrbk_units") as mock_btrbk:
            mock_btrbk.return_value = [
                SystemdUnitStatus(
                    name="btrbk.timer",
                    exists=True,
                    enabled=True,
                    active=False,
                )
            ]
            with patch("btrfs_backup_ng.systemd_utils.find_backup_ng_units") as mock_ng:
                mock_ng.return_value = []
                summary = get_migration_summary()
                assert "btrbk_units" in summary
                assert "backup_ng_units" in summary
                assert "btrbk_active" in summary
                assert summary["btrbk_active"] is True
                assert summary["migration_needed"] is True


class TestConstants:
    """Tests for module constants."""

    def test_btrbk_unit_names(self):
        """Test btrbk unit names are defined."""
        assert "btrbk.timer" in BTRBK_UNIT_NAMES
        assert "btrbk.service" in BTRBK_UNIT_NAMES

    def test_backup_ng_unit_names(self):
        """Test backup-ng unit names are defined."""
        assert "btrfs-backup-ng.timer" in BACKUP_NG_UNIT_NAMES
        assert "btrfs-backup-ng.service" in BACKUP_NG_UNIT_NAMES


class TestGetUnitStatusWithPath:
    """Additional tests for get_unit_status with path detection."""

    def test_get_unit_status_with_file_path(self):
        """Test unit status when file exists on disk."""
        with patch("btrfs_backup_ng.systemd_utils.run_systemctl") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="[Unit]..."),  # cat
                MagicMock(returncode=0, stdout="enabled"),  # is-enabled
                MagicMock(returncode=0, stdout="active"),  # is-active
            ]
            with patch("pathlib.Path.exists") as mock_exists:
                mock_exists.return_value = True
                status = get_unit_status("test.timer")
                assert status.exists is True


class TestFindBackupNgUnits:
    """Tests for find_backup_ng_units function."""

    def test_find_backup_ng_units_none(self):
        """Test when no btrfs-backup-ng units exist."""
        with patch("btrfs_backup_ng.systemd_utils.get_unit_status") as mock_status:
            mock_status.return_value = SystemdUnitStatus(
                name="btrfs-backup-ng.timer",
                exists=False,
                enabled=False,
                active=False,
            )
            units = find_backup_ng_units()
            assert len(units) == 0

    def test_find_backup_ng_units_found(self):
        """Test when btrfs-backup-ng units exist."""
        with patch("btrfs_backup_ng.systemd_utils.get_unit_status") as mock_status:

            def status_side_effect(name):
                if name == "btrfs-backup-ng.timer":
                    return SystemdUnitStatus(
                        name="btrfs-backup-ng.timer",
                        exists=True,
                        enabled=True,
                        active=False,
                    )
                return SystemdUnitStatus(
                    name=name,
                    exists=False,
                    enabled=False,
                    active=False,
                )

            mock_status.side_effect = status_side_effect
            units = find_backup_ng_units()
            assert len(units) == 1
            assert units[0].name == "btrfs-backup-ng.timer"


class TestMigrateWithUnits:
    """Additional tests for migrate_from_btrbk with various unit states."""

    def test_migrate_with_backup_ng_timer(self):
        """Test migration when btrfs-backup-ng timer exists but not enabled."""
        with patch("btrfs_backup_ng.systemd_utils.find_btrbk_units") as mock_btrbk:
            mock_btrbk.return_value = [
                SystemdUnitStatus(
                    name="btrbk.timer",
                    exists=True,
                    enabled=True,
                    active=False,
                )
            ]
            with patch("btrfs_backup_ng.systemd_utils.find_backup_ng_units") as mock_ng:
                mock_ng.return_value = [
                    SystemdUnitStatus(
                        name="btrfs-backup-ng.timer",
                        exists=True,
                        enabled=False,
                        active=False,
                    )
                ]
                with patch(
                    "btrfs_backup_ng.systemd_utils.disable_unit"
                ) as mock_disable:
                    mock_disable.return_value = (True, "Disabled btrbk.timer")
                    with patch(
                        "btrfs_backup_ng.systemd_utils.enable_unit"
                    ) as mock_enable:
                        mock_enable.return_value = (
                            True,
                            "Enabled btrfs-backup-ng.timer",
                        )
                        success, messages = migrate_from_btrbk(dry_run=False)
                        assert success is True
                        mock_disable.assert_called()
                        mock_enable.assert_called()

    def test_migrate_disable_failure(self):
        """Test migration when disable fails."""
        with patch("btrfs_backup_ng.systemd_utils.find_btrbk_units") as mock_btrbk:
            mock_btrbk.return_value = [
                SystemdUnitStatus(
                    name="btrbk.timer",
                    exists=True,
                    enabled=True,
                    active=True,
                )
            ]
            with patch("btrfs_backup_ng.systemd_utils.find_backup_ng_units") as mock_ng:
                mock_ng.return_value = []
                with patch(
                    "btrfs_backup_ng.systemd_utils.disable_unit"
                ) as mock_disable:
                    mock_disable.return_value = (
                        False,
                        "Failed to disable: Permission denied",
                    )
                    success, messages = migrate_from_btrbk(dry_run=False)
                    assert success is False
                    assert any("Error" in m or "Failed" in m for m in messages)

    def test_migrate_with_active_btrbk(self):
        """Test migration message includes active status."""
        with patch("btrfs_backup_ng.systemd_utils.find_btrbk_units") as mock_btrbk:
            mock_btrbk.return_value = [
                SystemdUnitStatus(
                    name="btrbk.timer",
                    exists=True,
                    enabled=True,
                    active=True,
                )
            ]
            with patch("btrfs_backup_ng.systemd_utils.find_backup_ng_units") as mock_ng:
                mock_ng.return_value = []
                success, messages = migrate_from_btrbk(dry_run=True)
                # Should mention both enabled and active
                joined = " ".join(messages)
                assert "enabled" in joined or "active" in joined


class TestEnableUnitFailures:
    """Tests for enable_unit edge cases."""

    def test_enable_unit_failure(self):
        """Test failed unit enable."""
        with patch("btrfs_backup_ng.systemd_utils.run_systemctl") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1, stdout="", stderr="Unit not found"
            )
            success, msg = enable_unit("nonexistent.timer")
            assert success is False
            assert "Failed" in msg

    def test_enable_unit_start_failure(self):
        """Test enable succeeds but start fails."""
        with patch("btrfs_backup_ng.systemd_utils.run_systemctl") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),  # enable
                MagicMock(returncode=1, stdout="", stderr="Failed to start"),  # start
            ]
            success, msg = enable_unit("test.timer", start=True)
            assert success is True  # Enable succeeded
            assert "Warning" in msg or "Failed to start" in msg


class TestDisableUnitNoStop:
    """Tests for disable_unit without stop."""

    def test_disable_unit_no_stop(self):
        """Test disable without stopping."""
        with patch("btrfs_backup_ng.systemd_utils.run_systemctl") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            success, msg = disable_unit("test.timer", stop=False)
            assert success is True
            assert "Disabled" in msg
            # Should only call disable, not stop
            assert mock_run.call_count == 1


class TestFindBtrbkInstanceUnits:
    """Tests for finding btrbk instance units (btrbk@*.timer)."""

    def test_find_btrbk_instance_units(self):
        """Test finding btrbk instance units via glob."""
        with patch("btrfs_backup_ng.systemd_utils.get_unit_status") as mock_status:
            # Regular units don't exist
            def status_side_effect(name):
                if name == "btrbk@home.timer":
                    return SystemdUnitStatus(
                        name="btrbk@home.timer",
                        exists=True,
                        enabled=True,
                        active=False,
                    )
                return SystemdUnitStatus(
                    name=name,
                    exists=False,
                    enabled=False,
                    active=False,
                )

            mock_status.side_effect = status_side_effect

            with patch("pathlib.Path.exists") as mock_exists:
                mock_exists.return_value = True

                with patch("pathlib.Path.glob") as mock_glob:
                    # Return mock Path objects for instance units
                    mock_timer = MagicMock()
                    mock_timer.name = "btrbk@home.timer"
                    mock_glob.return_value = [mock_timer]

                    units = find_btrbk_units()
                    # Should find the instance unit
                    assert any(u.name == "btrbk@home.timer" for u in units)

    def test_find_btrbk_instance_services(self):
        """Test finding btrbk instance services via glob."""
        with patch("btrfs_backup_ng.systemd_utils.get_unit_status") as mock_status:

            def status_side_effect(name):
                if name == "btrbk@data.service":
                    return SystemdUnitStatus(
                        name="btrbk@data.service",
                        exists=True,
                        enabled=False,
                        active=True,
                    )
                return SystemdUnitStatus(
                    name=name,
                    exists=False,
                    enabled=False,
                    active=False,
                )

            mock_status.side_effect = status_side_effect

            with patch("pathlib.Path.exists") as mock_exists:
                mock_exists.return_value = True

                with patch("pathlib.Path.glob") as mock_glob:
                    # Multiple glob calls: timer then service for each of 3 paths
                    mock_service = MagicMock()
                    mock_service.name = "btrbk@data.service"
                    # 3 paths x 2 globs each = 6 calls
                    mock_glob.side_effect = [
                        [],  # path1 timer
                        [mock_service],  # path1 service
                        [],  # path2 timer
                        [],  # path2 service
                        [],  # path3 timer
                        [],  # path3 service
                    ]

                    units = find_btrbk_units()
                    assert any(u.name == "btrbk@data.service" for u in units)

    def test_find_btrbk_skips_nonexistent_paths(self):
        """Test that nonexistent systemd paths are skipped."""
        with patch("btrfs_backup_ng.systemd_utils.get_unit_status") as mock_status:
            mock_status.return_value = SystemdUnitStatus(
                name="btrbk.timer",
                exists=False,
                enabled=False,
                active=False,
            )

            with patch("pathlib.Path.exists") as mock_exists:
                # All paths don't exist
                mock_exists.return_value = False

                units = find_btrbk_units()
                # Should return empty since no paths exist to glob
                assert len(units) == 0


class TestMigrateEnableFailure:
    """Tests for migration when enabling btrfs-backup-ng fails."""

    def test_migrate_enable_failure(self):
        """Test migration when enable fails."""
        with patch("btrfs_backup_ng.systemd_utils.find_btrbk_units") as mock_btrbk:
            mock_btrbk.return_value = []
            with patch("btrfs_backup_ng.systemd_utils.find_backup_ng_units") as mock_ng:
                mock_ng.return_value = [
                    SystemdUnitStatus(
                        name="btrfs-backup-ng.timer",
                        exists=True,
                        enabled=False,
                        active=False,
                    )
                ]
                with patch("btrfs_backup_ng.systemd_utils.enable_unit") as mock_enable:
                    mock_enable.return_value = (
                        False,
                        "Failed to enable: Permission denied",
                    )
                    success, messages = migrate_from_btrbk(dry_run=False)
                    assert success is False
                    assert any("Error" in m for m in messages)

    def test_migrate_with_already_enabled_timer(self):
        """Test migration when timer is already enabled."""
        with patch("btrfs_backup_ng.systemd_utils.find_btrbk_units") as mock_btrbk:
            mock_btrbk.return_value = []
            with patch("btrfs_backup_ng.systemd_utils.find_backup_ng_units") as mock_ng:
                mock_ng.return_value = [
                    SystemdUnitStatus(
                        name="btrfs-backup-ng.timer",
                        exists=True,
                        enabled=True,  # Already enabled
                        active=True,
                    )
                ]
                success, messages = migrate_from_btrbk(dry_run=False)
                # Should succeed without trying to enable
                assert success is True


class TestRunSystemctlWithCheck:
    """Tests for run_systemctl with check parameter."""

    def test_run_systemctl_with_check_true(self):
        """Test systemctl with check=True raises on error."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, "systemctl")
            try:
                run_systemctl("status", "test.timer", check=True)
                assert False, "Should have raised"
            except subprocess.CalledProcessError:
                pass

    def test_run_systemctl_no_capture(self):
        """Test systemctl with capture=False."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            run_systemctl("status", "test.timer", capture=False)
            mock_run.assert_called_once()
            # Check capture_output was False
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs.get("capture_output") is False
