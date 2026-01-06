"""Tests for the doctor diagnostic module."""

import time
from unittest.mock import MagicMock, patch

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
