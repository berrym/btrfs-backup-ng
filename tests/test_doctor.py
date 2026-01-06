"""Tests for the doctor diagnostic module."""

import argparse
import time
from unittest.mock import MagicMock, patch

from btrfs_backup_ng.cli.doctor import (
    _get_severity_prefix,
    _print_fix_results,
    _print_json,
    _print_report,
    execute_doctor,
)
from btrfs_backup_ng.core.doctor import (
    DiagnosticCategory,
    DiagnosticFinding,
    DiagnosticReport,
    DiagnosticSeverity,
    Doctor,
    FixResult,
)


class TestDiagnosticSeverity:
    """Tests for DiagnosticSeverity enum."""

    def test_severity_values(self):
        """Test severity enum values."""
        assert DiagnosticSeverity.OK.value == "ok"
        assert DiagnosticSeverity.INFO.value == "info"
        assert DiagnosticSeverity.WARN.value == "warn"
        assert DiagnosticSeverity.ERROR.value == "error"
        assert DiagnosticSeverity.CRITICAL.value == "critical"

    def test_all_severities_exist(self):
        """Test all expected severity levels exist."""
        severities = list(DiagnosticSeverity)
        assert len(severities) == 5


class TestDiagnosticCategory:
    """Tests for DiagnosticCategory enum."""

    def test_category_values(self):
        """Test category enum values."""
        assert DiagnosticCategory.CONFIG.value == "config"
        assert DiagnosticCategory.SNAPSHOTS.value == "snapshots"
        assert DiagnosticCategory.TRANSFERS.value == "transfers"
        assert DiagnosticCategory.SYSTEM.value == "system"

    def test_all_categories_exist(self):
        """Test all expected categories exist."""
        categories = list(DiagnosticCategory)
        assert len(categories) == 4


class TestDiagnosticFinding:
    """Tests for DiagnosticFinding dataclass."""

    def test_basic_finding(self):
        """Test creating a basic finding."""
        finding = DiagnosticFinding(
            category=DiagnosticCategory.CONFIG,
            severity=DiagnosticSeverity.OK,
            check_name="test_check",
            message="Test passed",
        )
        assert finding.category == DiagnosticCategory.CONFIG
        assert finding.severity == DiagnosticSeverity.OK
        assert finding.check_name == "test_check"
        assert finding.message == "Test passed"
        assert finding.details == {}
        assert finding.fixable is False
        assert finding.fix_description is None
        assert finding.fix_action is None

    def test_finding_with_details(self):
        """Test finding with details."""
        finding = DiagnosticFinding(
            category=DiagnosticCategory.SYSTEM,
            severity=DiagnosticSeverity.WARN,
            check_name="space_check",
            message="Low disk space",
            details={"available": "10GB", "path": "/backup"},
        )
        assert finding.details["available"] == "10GB"
        assert finding.details["path"] == "/backup"

    def test_fixable_finding(self):
        """Test fixable finding."""
        finding = DiagnosticFinding(
            category=DiagnosticCategory.TRANSFERS,
            severity=DiagnosticSeverity.WARN,
            check_name="stale_locks",
            message="Stale lock found",
            fixable=True,
            fix_description="Remove stale lock",
        )
        assert finding.fixable is True
        assert finding.fix_description == "Remove stale lock"

    def test_fix_action_property(self):
        """Test fix_action property getter/setter."""
        finding = DiagnosticFinding(
            category=DiagnosticCategory.TRANSFERS,
            severity=DiagnosticSeverity.WARN,
            check_name="test",
            message="test",
            fixable=True,
        )

        def fix_func() -> bool:
            return True

        finding.fix_action = fix_func
        assert finding.fix_action is fix_func
        assert finding.fix_action() is True

    def test_to_dict(self):
        """Test JSON serialization."""
        finding = DiagnosticFinding(
            category=DiagnosticCategory.CONFIG,
            severity=DiagnosticSeverity.ERROR,
            check_name="config_check",
            message="Config invalid",
            details={"error": "syntax error"},
            fixable=False,
        )
        result = finding.to_dict()
        assert result["category"] == "config"
        assert result["severity"] == "error"
        assert result["check"] == "config_check"
        assert result["message"] == "Config invalid"
        assert result["details"]["error"] == "syntax error"
        assert result["fixable"] is False

    def test_to_dict_with_fix_description(self):
        """Test to_dict includes fix_description when set."""
        finding = DiagnosticFinding(
            category=DiagnosticCategory.TRANSFERS,
            severity=DiagnosticSeverity.WARN,
            check_name="test",
            message="test",
            fixable=True,
            fix_description="Run cleanup",
        )
        result = finding.to_dict()
        assert result["fix_description"] == "Run cleanup"


class TestDiagnosticReport:
    """Tests for DiagnosticReport dataclass."""

    def test_empty_report(self):
        """Test empty report properties."""
        report = DiagnosticReport()
        assert report.ok_count == 0
        assert report.info_count == 0
        assert report.warn_count == 0
        assert report.error_count == 0
        assert report.fixable_count == 0
        assert report.has_critical is False
        assert report.exit_code == 0

    def test_report_counts(self):
        """Test report counts with various findings."""
        report = DiagnosticReport()
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.OK,
                "check1",
                "ok",
            )
        )
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.OK,
                "check2",
                "ok",
            )
        )
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.WARN,
                "check3",
                "warn",
            )
        )
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.SYSTEM,
                DiagnosticSeverity.INFO,
                "check4",
                "info",
            )
        )

        assert report.ok_count == 2
        assert report.info_count == 1
        assert report.warn_count == 1
        assert report.error_count == 0

    def test_exit_code_healthy(self):
        """Test exit code 0 for healthy system."""
        report = DiagnosticReport()
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.OK,
                "check",
                "ok",
            )
        )
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.INFO,
                "check",
                "info",
            )
        )
        assert report.exit_code == 0

    def test_exit_code_warnings(self):
        """Test exit code 1 for warnings."""
        report = DiagnosticReport()
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.OK,
                "check",
                "ok",
            )
        )
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.WARN,
                "check",
                "warn",
            )
        )
        assert report.exit_code == 1

    def test_exit_code_errors(self):
        """Test exit code 2 for errors."""
        report = DiagnosticReport()
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.ERROR,
                "check",
                "error",
            )
        )
        assert report.exit_code == 2

    def test_exit_code_critical(self):
        """Test exit code 2 for critical."""
        report = DiagnosticReport()
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.CRITICAL,
                "check",
                "critical",
            )
        )
        assert report.exit_code == 2
        assert report.has_critical is True

    def test_fixable_count(self):
        """Test fixable findings count."""
        report = DiagnosticReport()
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.TRANSFERS,
                DiagnosticSeverity.WARN,
                "check1",
                "fixable",
                fixable=True,
            )
        )
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.TRANSFERS,
                DiagnosticSeverity.WARN,
                "check2",
                "not fixable",
                fixable=False,
            )
        )
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.TRANSFERS,
                DiagnosticSeverity.WARN,
                "check3",
                "fixable",
                fixable=True,
            )
        )
        assert report.fixable_count == 2

    def test_duration(self):
        """Test duration calculation."""
        report = DiagnosticReport()
        report.started_at = time.time() - 5.0
        report.completed_at = time.time()
        assert 4.9 <= report.duration <= 5.1

    def test_to_dict(self):
        """Test JSON serialization of report."""
        report = DiagnosticReport()
        report.config_path = "/etc/config.toml"
        report.categories_checked = {DiagnosticCategory.CONFIG}
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.OK,
                "check",
                "ok",
            )
        )
        report.completed_at = report.started_at + 1.0

        result = report.to_dict()
        assert "timestamp" in result
        assert result["config_path"] == "/etc/config.toml"
        assert "config" in result["categories_checked"]
        assert result["summary"]["ok"] == 1
        assert len(result["findings"]) == 1


class TestFixResult:
    """Tests for FixResult dataclass."""

    def test_successful_fix(self):
        """Test successful fix result."""
        finding = DiagnosticFinding(
            DiagnosticCategory.TRANSFERS,
            DiagnosticSeverity.WARN,
            "stale_lock",
            "Stale lock",
            fixable=True,
        )
        result = FixResult(
            finding=finding,
            success=True,
            message="Lock removed",
        )
        assert result.success is True
        assert result.message == "Lock removed"

    def test_failed_fix(self):
        """Test failed fix result."""
        finding = DiagnosticFinding(
            DiagnosticCategory.TRANSFERS,
            DiagnosticSeverity.WARN,
            "stale_lock",
            "Stale lock",
            fixable=True,
        )
        result = FixResult(
            finding=finding,
            success=False,
            message="Permission denied",
            details={"exception": "PermissionError"},
        )
        assert result.success is False
        assert result.details["exception"] == "PermissionError"


class TestDoctor:
    """Tests for Doctor diagnostic engine."""

    def test_init_without_config(self):
        """Test initializing doctor without config."""
        doctor = Doctor()
        assert doctor.config is None
        assert doctor.config_path is None

    def test_init_with_config_path(self, tmp_path):
        """Test initializing with config path."""
        config_path = tmp_path / "config.toml"
        doctor = Doctor(config_path=config_path)
        assert doctor.config_path == config_path

    def test_checks_registered(self):
        """Test that checks are registered on init."""
        doctor = Doctor()
        assert len(doctor._checks) > 0
        # Check some expected checks exist
        check_names = [c.name for c in doctor._checks]
        assert "config_exists" in check_names
        assert "config_valid" in check_names
        assert "stale_locks" in check_names
        assert "destination_space" in check_names

    @patch("btrfs_backup_ng.config.find_config_file")
    def test_check_config_exists_no_config(self, mock_find):
        """Test config_exists check when no config found."""
        mock_find.return_value = None
        doctor = Doctor()
        findings = doctor._check_config_exists()

        assert len(findings) == 1
        assert findings[0].severity == DiagnosticSeverity.ERROR
        assert "No configuration file found" in findings[0].message

    def test_check_config_exists_file_exists(self, tmp_path):
        """Test config_exists check when config exists."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("[global]\n")

        doctor = Doctor(config_path=config_file)
        findings = doctor._check_config_exists()

        assert len(findings) == 1
        assert findings[0].severity == DiagnosticSeverity.OK

    def test_check_config_exists_file_missing(self, tmp_path):
        """Test config_exists check when specified config missing."""
        config_file = tmp_path / "nonexistent.toml"

        doctor = Doctor(config_path=config_file)
        findings = doctor._check_config_exists()

        assert len(findings) == 1
        assert findings[0].severity == DiagnosticSeverity.ERROR

    @patch("btrfs_backup_ng.config.load_config")
    def test_check_config_valid_success(self, mock_load, tmp_path):
        """Test config_valid check success."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("[global]\n")

        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = []
        mock_load.return_value = (mock_config, [])

        doctor = Doctor(config_path=config_file)
        findings = doctor._check_config_valid()

        assert any(f.severity == DiagnosticSeverity.OK for f in findings)

    @patch("btrfs_backup_ng.config.load_config")
    def test_check_config_valid_with_warnings(self, mock_load, tmp_path):
        """Test config_valid check with warnings."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("[global]\n")

        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = []
        mock_load.return_value = (mock_config, ["Warning 1", "Warning 2"])

        doctor = Doctor(config_path=config_file)
        findings = doctor._check_config_valid()

        warnings = [f for f in findings if f.severity == DiagnosticSeverity.WARN]
        assert len(warnings) == 2

    def test_check_compression_no_config(self):
        """Test compression check without config."""
        doctor = Doctor()
        findings = doctor._check_compression_programs()
        assert findings == []

    @patch("shutil.which")
    def test_check_compression_available(self, mock_which):
        """Test compression check when program available."""
        mock_which.return_value = "/usr/bin/zstd"

        mock_config = MagicMock()
        mock_config.global_config.compress = "zstd"
        mock_config.get_enabled_volumes.return_value = []

        doctor = Doctor(config=mock_config)
        findings = doctor._check_compression_programs()

        assert any(
            f.severity == DiagnosticSeverity.OK and "zstd" in f.message
            for f in findings
        )

    @patch("shutil.which")
    def test_check_compression_missing(self, mock_which):
        """Test compression check when program missing."""
        mock_which.return_value = None

        mock_config = MagicMock()
        mock_config.global_config.compress = "zstd"
        mock_config.get_enabled_volumes.return_value = []

        doctor = Doctor(config=mock_config)
        findings = doctor._check_compression_programs()

        assert any(
            f.severity == DiagnosticSeverity.WARN and "zstd" in f.message
            for f in findings
        )

    def test_is_lock_stale_not_pid(self):
        """Test _is_lock_stale with non-PID lock ID."""
        doctor = Doctor()
        # Session ID format - can't determine staleness
        assert doctor._is_lock_stale("restore:abc123") is False

    @patch("os.kill")
    def test_is_lock_stale_process_running(self, mock_kill):
        """Test _is_lock_stale when process is running."""
        mock_kill.return_value = None  # Process exists

        doctor = Doctor()
        assert doctor._is_lock_stale("transfer:12345") is False

    @patch("os.kill")
    def test_is_lock_stale_process_not_running(self, mock_kill):
        """Test _is_lock_stale when process is not running."""
        mock_kill.side_effect = OSError("No such process")

        doctor = Doctor()
        assert doctor._is_lock_stale("transfer:12345") is True

    def test_run_diagnostics_filters_categories(self):
        """Test run_diagnostics filters by category."""
        doctor = Doctor()

        # Run only config checks
        report = doctor.run_diagnostics(categories={DiagnosticCategory.CONFIG})

        assert DiagnosticCategory.CONFIG in report.categories_checked
        assert DiagnosticCategory.SYSTEM not in report.categories_checked

    def test_run_diagnostics_all_categories(self):
        """Test run_diagnostics runs all categories when None."""
        doctor = Doctor()
        report = doctor.run_diagnostics(categories=None)

        # Should include all categories
        assert len(report.categories_checked) == 4

    def test_run_diagnostics_progress_callback(self):
        """Test progress callback is called."""
        doctor = Doctor()
        progress_calls = []

        def on_progress(name: str, current: int, total: int):
            progress_calls.append((name, current, total))

        doctor.run_diagnostics(
            categories={DiagnosticCategory.CONFIG},
            on_progress=on_progress,
        )

        assert len(progress_calls) > 0
        # Check callback format
        assert all(len(call) == 3 for call in progress_calls)

    def test_apply_fixes_no_fixable(self):
        """Test apply_fixes with no fixable findings."""
        doctor = Doctor()
        report = DiagnosticReport()
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.ERROR,
                "check",
                "error",
                fixable=False,
            )
        )

        results = doctor.apply_fixes(report)
        assert results == []

    def test_apply_fixes_calls_fix_action(self):
        """Test apply_fixes calls fix_action."""
        doctor = Doctor()
        report = DiagnosticReport()

        fix_called = []

        def mock_fix() -> bool:
            fix_called.append(True)
            return True

        finding = DiagnosticFinding(
            DiagnosticCategory.TRANSFERS,
            DiagnosticSeverity.WARN,
            "test",
            "test",
            fixable=True,
        )
        finding.fix_action = mock_fix
        report.add_finding(finding)

        results = doctor.apply_fixes(report)

        assert len(fix_called) == 1
        assert len(results) == 1
        assert results[0].success is True

    def test_apply_fixes_handles_exception(self):
        """Test apply_fixes handles exceptions in fix_action."""
        doctor = Doctor()
        report = DiagnosticReport()

        def failing_fix() -> bool:
            raise RuntimeError("Fix failed")

        finding = DiagnosticFinding(
            DiagnosticCategory.TRANSFERS,
            DiagnosticSeverity.WARN,
            "test",
            "test",
            fixable=True,
        )
        finding.fix_action = failing_fix
        report.add_finding(finding)

        results = doctor.apply_fixes(report)

        assert len(results) == 1
        assert results[0].success is False
        assert "Fix failed" in results[0].message


class TestDoctorSystemChecks:
    """Tests for Doctor system state checks."""

    @patch("subprocess.run")
    def test_check_systemd_timer_active(self, mock_run):
        """Test systemd timer check when active."""
        # Mock is-active returning success
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="active\n"),
            MagicMock(
                returncode=0, stdout="NextElapseUSecRealtime=Mon 2026-01-06 12:00:00\n"
            ),
        ]

        doctor = Doctor()
        findings = doctor._check_systemd_timer()

        assert any(
            f.severity == DiagnosticSeverity.OK and "active" in f.message
            for f in findings
        )

    @patch("subprocess.run")
    def test_check_systemd_timer_not_installed(self, mock_run):
        """Test systemd timer check when not installed."""
        mock_run.return_value = MagicMock(returncode=4, stdout="")

        doctor = Doctor()
        findings = doctor._check_systemd_timer()

        assert any(
            f.severity == DiagnosticSeverity.INFO and "No systemd timer" in f.message
            for f in findings
        )

    def test_check_destination_space_no_config(self):
        """Test destination space check without config."""
        doctor = Doctor()
        findings = doctor._check_destination_space()
        assert len(findings) == 0

    def test_check_last_backup_age_no_config(self):
        """Test last backup age check without config."""
        doctor = Doctor()
        findings = doctor._check_last_backup_age()
        assert len(findings) == 0

    @patch("btrfs_backup_ng.core.space.get_space_info")
    @patch("btrfs_backup_ng.core.doctor.Path.exists")
    def test_check_destination_space_with_config(self, mock_exists, mock_get_space):
        """Test destination space check with config."""
        mock_exists.return_value = True
        mock_space = MagicMock()
        mock_space.available_bytes = 50_000_000_000
        mock_space.total_bytes = 100_000_000_000
        mock_get_space.return_value = mock_space

        mock_target = MagicMock()
        mock_target.path = "/mnt/backup"
        mock_target.parsed_url = None

        mock_volume = MagicMock()
        mock_volume.targets = [mock_target]

        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = [mock_volume]

        doctor = Doctor(config=mock_config)
        findings = doctor._check_destination_space()

        assert len(findings) > 0
        # Should have OK finding for space (50% free)
        assert any(f.severity == DiagnosticSeverity.OK for f in findings)

    @patch("btrfs_backup_ng.core.space.get_space_info")
    @patch("btrfs_backup_ng.core.doctor.Path.exists")
    def test_check_destination_space_low(self, mock_exists, mock_get_space):
        """Test destination space check with low space."""
        mock_exists.return_value = True
        # 15% free (below 20% warning threshold)
        mock_space = MagicMock()
        mock_space.available_bytes = 15_000_000_000
        mock_space.total_bytes = 100_000_000_000
        mock_get_space.return_value = mock_space

        mock_target = MagicMock()
        mock_target.path = "/mnt/backup"
        mock_target.parsed_url = None

        mock_volume = MagicMock()
        mock_volume.targets = [mock_target]

        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = [mock_volume]

        doctor = Doctor(config=mock_config)
        findings = doctor._check_destination_space()

        assert any(f.severity == DiagnosticSeverity.WARN for f in findings)

    @patch("btrfs_backup_ng.core.space.get_space_info")
    @patch("btrfs_backup_ng.core.doctor.Path.exists")
    def test_check_destination_space_critical(self, mock_exists, mock_get_space):
        """Test destination space check with critical space."""
        mock_exists.return_value = True
        # 3% free (below 5% critical threshold)
        mock_space = MagicMock()
        mock_space.available_bytes = 3_000_000_000
        mock_space.total_bytes = 100_000_000_000
        mock_get_space.return_value = mock_space

        mock_target = MagicMock()
        mock_target.path = "/mnt/backup"
        mock_target.parsed_url = None

        mock_volume = MagicMock()
        mock_volume.targets = [mock_target]

        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = [mock_volume]

        doctor = Doctor(config=mock_config)
        findings = doctor._check_destination_space()

        assert any(f.severity == DiagnosticSeverity.ERROR for f in findings)


class TestDoctorVolumePaths:
    """Tests for Doctor volume path checks."""

    def test_check_volume_paths_no_config(self):
        """Test volume paths check without config."""
        doctor = Doctor()
        findings = doctor._check_volume_paths()
        assert len(findings) == 0

    @patch("btrfs_backup_ng.core.doctor.Path.exists")
    @patch("btrfs_backup_ng.__util__.is_btrfs")
    def test_check_volume_paths_valid(self, mock_is_btrfs, mock_exists):
        """Test volume paths check with valid paths."""
        mock_exists.return_value = True
        mock_is_btrfs.return_value = True

        mock_volume = MagicMock()
        mock_volume.path = "/home"

        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = [mock_volume]

        doctor = Doctor(config=mock_config)
        findings = doctor._check_volume_paths()

        assert any(f.severity == DiagnosticSeverity.OK for f in findings)

    def test_check_volume_paths_missing(self):
        """Test volume paths check with missing path."""
        mock_volume = MagicMock()
        mock_volume.path = "/nonexistent/path"

        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = [mock_volume]

        doctor = Doctor(config=mock_config)
        findings = doctor._check_volume_paths()

        assert any(f.severity == DiagnosticSeverity.ERROR for f in findings)

    @patch("btrfs_backup_ng.core.doctor.Path.exists")
    @patch("btrfs_backup_ng.__util__.is_btrfs")
    def test_check_volume_paths_not_btrfs(self, mock_is_btrfs, mock_exists):
        """Test volume paths check when path is not btrfs."""
        mock_exists.return_value = True
        mock_is_btrfs.return_value = False

        mock_volume = MagicMock()
        mock_volume.path = "/home"

        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = [mock_volume]

        doctor = Doctor(config=mock_config)
        findings = doctor._check_volume_paths()

        assert any(
            f.severity == DiagnosticSeverity.ERROR and "not on btrfs" in f.message
            for f in findings
        )


class TestDoctorTargetReachability:
    """Tests for Doctor target reachability checks."""

    def test_check_target_reachability_no_config(self):
        """Test target reachability check without config."""
        doctor = Doctor()
        findings = doctor._check_target_reachability()
        assert len(findings) == 0

    def test_check_target_reachability_local_exists(self):
        """Test target reachability with existing local path."""
        mock_target = MagicMock()
        mock_target.path = "/tmp"
        mock_target.parsed_url = None

        mock_volume = MagicMock()
        mock_volume.targets = [mock_target]

        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = [mock_volume]

        doctor = Doctor(config=mock_config)
        findings = doctor._check_target_reachability()

        assert any(f.severity == DiagnosticSeverity.OK for f in findings)

    def test_check_target_reachability_local_missing(self):
        """Test target reachability with missing local path."""
        mock_target = MagicMock()
        mock_target.path = "/nonexistent/target/path"
        mock_target.parsed_url = None

        mock_volume = MagicMock()
        mock_volume.targets = [mock_target]

        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = [mock_volume]

        doctor = Doctor(config=mock_config)
        findings = doctor._check_target_reachability()

        assert any(f.severity == DiagnosticSeverity.ERROR for f in findings)


class TestCliDoctor:
    """Tests for CLI doctor module."""

    def test_get_severity_prefix(self):
        """Test severity prefix formatting."""
        assert _get_severity_prefix(DiagnosticSeverity.OK) == "[OK]"
        assert _get_severity_prefix(DiagnosticSeverity.INFO) == "[INFO]"
        assert _get_severity_prefix(DiagnosticSeverity.WARN) == "[WARN]"
        assert _get_severity_prefix(DiagnosticSeverity.ERROR) == "[ERROR]"
        assert _get_severity_prefix(DiagnosticSeverity.CRITICAL) == "[CRIT]"

    def test_print_report_basic(self, capsys):
        """Test basic report printing."""
        report = DiagnosticReport()
        report.config_path = "/etc/config.toml"
        report.categories_checked = {DiagnosticCategory.CONFIG}
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.OK,
                "test",
                "Test passed",
            )
        )
        report.completed_at = report.started_at + 1.0

        _print_report(report)
        captured = capsys.readouterr()

        assert "btrfs-backup-ng Doctor" in captured.out
        assert "/etc/config.toml" in captured.out
        assert "Configuration" in captured.out
        assert "[OK]" in captured.out
        assert "Test passed" in captured.out
        assert "1 passed" in captured.out

    def test_print_report_quiet_mode(self, capsys):
        """Test report printing in quiet mode."""
        report = DiagnosticReport()
        report.categories_checked = {DiagnosticCategory.CONFIG}
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.OK,
                "ok_test",
                "OK message",
            )
        )
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.WARN,
                "warn_test",
                "Warning message",
            )
        )
        report.completed_at = report.started_at + 1.0

        _print_report(report, quiet=True)
        captured = capsys.readouterr()

        # OK message should be hidden in quiet mode
        assert "OK message" not in captured.out
        # Warning should still show
        assert "Warning message" in captured.out

    def test_print_report_with_fixable(self, capsys):
        """Test report printing with fixable finding."""
        report = DiagnosticReport()
        report.categories_checked = {DiagnosticCategory.TRANSFERS}
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.TRANSFERS,
                DiagnosticSeverity.WARN,
                "stale_lock",
                "Stale lock found",
                fixable=True,
            )
        )
        report.completed_at = report.started_at + 1.0

        _print_report(report)
        captured = capsys.readouterr()

        assert "[FIXABLE]" in captured.out
        assert "run with --fix" in captured.out

    def test_print_report_with_hint(self, capsys):
        """Test report printing with hint in details."""
        report = DiagnosticReport()
        report.categories_checked = {DiagnosticCategory.SYSTEM}
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.SYSTEM,
                DiagnosticSeverity.INFO,
                "timer_check",
                "No timer installed",
                details={"hint": "Install with: btrfs-backup-ng install"},
            )
        )
        report.completed_at = report.started_at + 1.0

        _print_report(report)
        captured = capsys.readouterr()

        assert "Hint:" in captured.out
        assert "Install with:" in captured.out

    def test_print_json(self, capsys):
        """Test JSON output printing."""
        report = DiagnosticReport()
        report.config_path = "/etc/config.toml"
        report.categories_checked = {DiagnosticCategory.CONFIG}
        report.add_finding(
            DiagnosticFinding(
                DiagnosticCategory.CONFIG,
                DiagnosticSeverity.OK,
                "test",
                "Test passed",
            )
        )
        report.completed_at = report.started_at + 1.0

        _print_json(report, [])
        captured = capsys.readouterr()

        import json

        data = json.loads(captured.out)
        assert data["config_path"] == "/etc/config.toml"
        assert data["summary"]["ok"] == 1
        assert len(data["findings"]) == 1

    def test_print_json_with_fixes(self, capsys):
        """Test JSON output with fix results."""
        report = DiagnosticReport()
        report.categories_checked = {DiagnosticCategory.TRANSFERS}

        finding = DiagnosticFinding(
            DiagnosticCategory.TRANSFERS,
            DiagnosticSeverity.WARN,
            "stale_lock",
            "Stale lock found",
            fixable=True,
        )
        report.add_finding(finding)
        report.completed_at = report.started_at + 1.0

        fix_result = FixResult(
            finding=finding,
            success=True,
            message="Lock removed",
        )

        _print_json(report, [fix_result])
        captured = capsys.readouterr()

        import json

        data = json.loads(captured.out)
        assert "fixes" in data
        assert len(data["fixes"]) == 1
        assert data["fixes"][0]["success"] is True

    def test_print_fix_results(self, capsys):
        """Test fix results printing."""
        finding = DiagnosticFinding(
            DiagnosticCategory.TRANSFERS,
            DiagnosticSeverity.WARN,
            "stale_lock",
            "Stale lock found",
            fixable=True,
        )

        results = [
            FixResult(finding=finding, success=True, message="Fixed"),
            FixResult(finding=finding, success=False, message="Permission denied"),
        ]

        _print_fix_results(results)
        captured = capsys.readouterr()

        assert "Fix Results" in captured.out
        assert "[OK]" in captured.out
        assert "[FAILED]" in captured.out
        assert "Permission denied" in captured.out

    @patch("btrfs_backup_ng.cli.doctor.Doctor")
    @patch("btrfs_backup_ng.cli.doctor.find_config_file")
    def test_execute_doctor_basic(self, mock_find_config, mock_doctor_class):
        """Test basic execute_doctor."""
        mock_find_config.return_value = None

        mock_report = DiagnosticReport()
        mock_report.categories_checked = {DiagnosticCategory.CONFIG}
        mock_report.completed_at = mock_report.started_at + 1.0

        mock_doctor = MagicMock()
        mock_doctor.run_diagnostics.return_value = mock_report
        mock_doctor_class.return_value = mock_doctor

        args = argparse.Namespace(
            config=None,
            check=None,
            volume=None,
            quiet=False,
            json=False,
            fix=False,
            interactive=False,
            verbose=0,
            debug=False,
        )

        exit_code = execute_doctor(args)
        assert exit_code == 0
        mock_doctor.run_diagnostics.assert_called_once()

    @patch("btrfs_backup_ng.cli.doctor.Doctor")
    @patch("btrfs_backup_ng.cli.doctor.find_config_file")
    def test_execute_doctor_json_output(
        self, mock_find_config, mock_doctor_class, capsys
    ):
        """Test execute_doctor with JSON output."""
        mock_find_config.return_value = None

        mock_report = DiagnosticReport()
        mock_report.categories_checked = {DiagnosticCategory.CONFIG}
        mock_report.completed_at = mock_report.started_at + 1.0

        mock_doctor = MagicMock()
        mock_doctor.run_diagnostics.return_value = mock_report
        mock_doctor_class.return_value = mock_doctor

        args = argparse.Namespace(
            config=None,
            check=None,
            volume=None,
            quiet=False,
            json=True,
            fix=False,
            interactive=False,
            verbose=0,
            debug=False,
        )

        exit_code = execute_doctor(args)
        captured = capsys.readouterr()

        assert exit_code == 0
        import json

        data = json.loads(captured.out)
        assert "summary" in data

    @patch("btrfs_backup_ng.cli.doctor.Doctor")
    @patch("btrfs_backup_ng.cli.doctor.find_config_file")
    def test_execute_doctor_with_category_filter(
        self, mock_find_config, mock_doctor_class
    ):
        """Test execute_doctor with category filter."""
        mock_find_config.return_value = None

        mock_report = DiagnosticReport()
        mock_report.categories_checked = {DiagnosticCategory.CONFIG}
        mock_report.completed_at = mock_report.started_at + 1.0

        mock_doctor = MagicMock()
        mock_doctor.run_diagnostics.return_value = mock_report
        mock_doctor_class.return_value = mock_doctor

        args = argparse.Namespace(
            config=None,
            check=["config"],
            volume=None,
            quiet=False,
            json=False,
            fix=False,
            interactive=False,
            verbose=0,
            debug=False,
        )

        execute_doctor(args)

        # Verify categories were passed
        call_kwargs = mock_doctor.run_diagnostics.call_args[1]
        assert DiagnosticCategory.CONFIG in call_kwargs["categories"]

    @patch("btrfs_backup_ng.cli.doctor.Doctor")
    @patch("btrfs_backup_ng.cli.doctor.find_config_file")
    def test_execute_doctor_with_fix(self, mock_find_config, mock_doctor_class):
        """Test execute_doctor with fix flag."""
        mock_find_config.return_value = None

        mock_report = DiagnosticReport()
        mock_report.categories_checked = {DiagnosticCategory.CONFIG}
        mock_report.completed_at = mock_report.started_at + 1.0

        mock_doctor = MagicMock()
        mock_doctor.run_diagnostics.return_value = mock_report
        mock_doctor.apply_fixes.return_value = []
        mock_doctor_class.return_value = mock_doctor

        args = argparse.Namespace(
            config=None,
            check=None,
            volume=None,
            quiet=False,
            json=False,
            fix=True,
            interactive=False,
            verbose=0,
            debug=False,
        )

        execute_doctor(args)

        mock_doctor.apply_fixes.assert_called_once()

    @patch("btrfs_backup_ng.cli.doctor.Doctor")
    @patch("btrfs_backup_ng.cli.doctor.find_config_file")
    def test_execute_doctor_with_volume_filter(
        self, mock_find_config, mock_doctor_class
    ):
        """Test execute_doctor with volume filter."""
        mock_find_config.return_value = None

        mock_report = DiagnosticReport()
        mock_report.completed_at = mock_report.started_at + 1.0

        mock_doctor = MagicMock()
        mock_doctor.run_diagnostics.return_value = mock_report
        mock_doctor_class.return_value = mock_doctor

        args = argparse.Namespace(
            config=None,
            check=None,
            volume=["/home"],
            quiet=False,
            json=False,
            fix=False,
            interactive=False,
            verbose=0,
            debug=False,
        )

        execute_doctor(args)

        call_kwargs = mock_doctor.run_diagnostics.call_args[1]
        assert call_kwargs["volume_filter"] == "/home"


class TestDoctorSSHTarget:
    """Tests for SSH target reachability checks."""

    @patch("btrfs_backup_ng.sshutil.diagnose.test_ssh_connection")
    def test_check_ssh_target_success(self, mock_test_ssh):
        """Test SSH target check when connection succeeds."""
        mock_test_ssh.return_value = {"success": True}

        mock_target = MagicMock()
        mock_target.path = "ssh://user@host:/backup"
        mock_target.parsed_url = MagicMock()
        mock_target.parsed_url.scheme = "ssh"

        doctor = Doctor()
        findings = doctor._check_ssh_target(mock_target)

        assert any(f.severity == DiagnosticSeverity.OK for f in findings)
        assert any("reachable" in f.message for f in findings)

    @patch("btrfs_backup_ng.sshutil.diagnose.test_ssh_connection")
    def test_check_ssh_target_failure(self, mock_test_ssh):
        """Test SSH target check when connection fails."""
        mock_test_ssh.return_value = {"success": False, "error": "Connection refused"}

        mock_target = MagicMock()
        mock_target.path = "ssh://user@host:/backup"

        doctor = Doctor()
        findings = doctor._check_ssh_target(mock_target)

        assert any(f.severity == DiagnosticSeverity.ERROR for f in findings)

    @patch("btrfs_backup_ng.sshutil.diagnose.test_ssh_connection")
    def test_check_ssh_target_exception(self, mock_test_ssh):
        """Test SSH target check when exception occurs."""
        mock_test_ssh.side_effect = Exception("Network error")

        mock_target = MagicMock()
        mock_target.path = "ssh://user@host:/backup"

        doctor = Doctor()
        findings = doctor._check_ssh_target(mock_target)

        assert any(f.severity == DiagnosticSeverity.ERROR for f in findings)
        assert any("failed" in f.message.lower() for f in findings)


class TestDoctorStaleLocks:
    """Tests for stale lock detection and fixing."""

    def test_fix_stale_lock_success(self, tmp_path):
        """Test successfully fixing a stale lock."""
        # Create a mock lock file
        lock_file = tmp_path / ".locks"
        lock_content = '{"snapshot-1": {"locks": ["transfer:99999"]}}'
        lock_file.write_text(lock_content)

        doctor = Doctor()
        result = doctor._fix_stale_lock(lock_file, "snapshot-1", "transfer:99999")

        assert result is True
        # Lock should be removed
        new_content = lock_file.read_text()
        assert "transfer:99999" not in new_content

    def test_fix_stale_lock_file_not_found(self, tmp_path):
        """Test fixing lock when file doesn't exist."""
        lock_file = tmp_path / "nonexistent_locks"

        doctor = Doctor()
        result = doctor._fix_stale_lock(lock_file, "snapshot-1", "transfer:99999")

        assert result is False

    def test_fix_stale_lock_snapshot_not_in_locks(self, tmp_path):
        """Test fixing lock when snapshot not in lock file."""
        lock_file = tmp_path / ".locks"
        lock_content = '{"other-snapshot": {"locks": ["transfer:12345"]}}'
        lock_file.write_text(lock_content)

        doctor = Doctor()
        result = doctor._fix_stale_lock(lock_file, "snapshot-1", "transfer:99999")

        assert result is False

    def test_make_lock_fix_action(self, tmp_path):
        """Test creating a lock fix action."""
        lock_file = tmp_path / ".locks"
        lock_content = '{"snapshot-1": {"locks": ["transfer:99999"]}}'
        lock_file.write_text(lock_content)

        doctor = Doctor()
        fix_action = doctor._make_lock_fix_action(
            lock_file, "snapshot-1", "transfer:99999"
        )

        # Should be callable and return True on success
        assert callable(fix_action)
        result = fix_action()
        assert result is True


class TestDoctorRecentFailures:
    """Tests for recent failures check."""

    def test_check_recent_failures_no_config(self):
        """Test recent failures check without config."""
        doctor = Doctor()
        findings = doctor._check_recent_failures()
        assert len(findings) == 0

    def test_check_recent_failures_no_log_configured(self):
        """Test recent failures when transaction log not configured."""
        mock_config = MagicMock()
        mock_config.global_config.transaction_log = None

        doctor = Doctor(config=mock_config)
        findings = doctor._check_recent_failures()

        assert any(f.severity == DiagnosticSeverity.INFO for f in findings)
        assert any("not enabled" in f.message for f in findings)

    def test_check_recent_failures_log_not_exists(self, tmp_path):
        """Test recent failures when log file doesn't exist yet."""
        mock_config = MagicMock()
        mock_config.global_config.transaction_log = str(tmp_path / "nonexistent.jsonl")

        doctor = Doctor(config=mock_config)
        findings = doctor._check_recent_failures()

        assert any(f.severity == DiagnosticSeverity.OK for f in findings)
        assert any("No transaction history" in f.message for f in findings)


class TestDoctorInteractiveFix:
    """Tests for interactive fix mode."""

    def test_apply_fixes_interactive_skips(self):
        """Test that interactive mode skips fixes (not implemented)."""
        doctor = Doctor()
        report = DiagnosticReport()

        finding = DiagnosticFinding(
            DiagnosticCategory.TRANSFERS,
            DiagnosticSeverity.WARN,
            "test",
            "test finding",
            fixable=True,
        )
        finding.fix_action = lambda: True
        report.add_finding(finding)

        # Interactive mode should skip all fixes
        results = doctor.apply_fixes(report, interactive=True)
        assert len(results) == 0

    def test_apply_fixes_fix_returns_false(self):
        """Test apply_fixes when fix action returns False."""
        doctor = Doctor()
        report = DiagnosticReport()

        finding = DiagnosticFinding(
            DiagnosticCategory.TRANSFERS,
            DiagnosticSeverity.WARN,
            "test",
            "test finding",
            fixable=True,
        )
        finding.fix_action = lambda: False
        report.add_finding(finding)

        results = doctor.apply_fixes(report, interactive=False)

        assert len(results) == 1
        assert results[0].success is False


class TestDoctorCheckFailure:
    """Tests for check failure handling."""

    def test_run_diagnostics_check_exception(self):
        """Test that exceptions in checks are handled gracefully."""
        doctor = Doctor()

        # Replace a check with one that throws
        def failing_check():
            raise RuntimeError("Check exploded")

        # Find and replace a check
        for check in doctor._checks:
            if check.name == "config_exists":
                check.check_func = failing_check
                break

        report = doctor.run_diagnostics(categories={DiagnosticCategory.CONFIG})

        # Should have an error finding for the failed check
        assert any(
            f.severity == DiagnosticSeverity.ERROR and "failed" in f.message.lower()
            for f in report.findings
        )


class TestDoctorLastBackupAge:
    """Tests for last backup age check."""

    def test_check_last_backup_age_no_transaction_log(self):
        """Test last backup age when no transaction log configured."""
        mock_config = MagicMock()
        mock_config.global_config.transaction_log = None

        doctor = Doctor(config=mock_config)
        findings = doctor._check_last_backup_age()

        assert any(f.severity == DiagnosticSeverity.INFO for f in findings)


class TestDoctorSnapshotChecks:
    """Tests for snapshot health checks."""

    def test_check_parent_chains_no_config(self):
        """Test parent chain check without config returns empty."""
        doctor = Doctor()
        findings = doctor._check_parent_chains()

        # Returns empty when no config
        assert len(findings) == 0

    def test_check_parent_chains_with_config(self):
        """Test parent chain check with config."""
        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = []

        doctor = Doctor(config=mock_config)
        findings = doctor._check_parent_chains()

        # Should return stub/placeholder finding
        assert len(findings) > 0

    def test_check_snapshot_dates_with_config(self):
        """Test snapshot dates check with config but no volumes."""
        mock_config = MagicMock()
        mock_config.get_enabled_volumes.return_value = []

        doctor = Doctor(config=mock_config)
        findings = doctor._check_snapshot_dates()

        # Should return stub finding since no endpoint access
        assert len(findings) > 0
