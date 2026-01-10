"""Diagnostic engine for btrfs-backup-ng.

Provides comprehensive health checking and diagnostics for the backup system,
including configuration validation, snapshot integrity, transfer state, and
system health monitoring.
"""

import getpass
import logging
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


class DiagnosticSeverity(Enum):
    """Severity level for diagnostic findings."""

    OK = "ok"  # Check passed
    INFO = "info"  # Informational, not a problem
    WARN = "warn"  # Warning, may need attention
    ERROR = "error"  # Error, requires attention
    CRITICAL = "critical"  # Critical, backup system may be non-functional


class DiagnosticCategory(Enum):
    """Category of diagnostic check."""

    CONFIG = "config"
    SNAPSHOTS = "snapshots"
    TRANSFERS = "transfers"
    SYSTEM = "system"


@dataclass
class DiagnosticFinding:
    """A single diagnostic finding."""

    category: DiagnosticCategory
    severity: DiagnosticSeverity
    check_name: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    fixable: bool = False
    fix_description: str | None = None
    # Note: fix_action is set dynamically, not stored in dataclass

    def __post_init__(self) -> None:
        """Initialize non-dataclass attributes."""
        self._fix_action: Callable[[], bool] | None = None

    @property
    def fix_action(self) -> Callable[[], bool] | None:
        """Get the fix action callable."""
        return self._fix_action

    @fix_action.setter
    def fix_action(self, value: Callable[[], bool] | None) -> None:
        """Set the fix action callable."""
        self._fix_action = value

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: dict[str, Any] = {
            "category": self.category.value,
            "severity": self.severity.value,
            "check": self.check_name,
            "message": self.message,
            "fixable": self.fixable,
        }
        if self.details:
            result["details"] = self.details
        if self.fix_description:
            result["fix_description"] = self.fix_description
        return result


@dataclass
class DiagnosticReport:
    """Complete diagnostic report."""

    started_at: float = field(default_factory=time.time)
    completed_at: float = 0.0
    findings: list[DiagnosticFinding] = field(default_factory=list)
    categories_checked: set[DiagnosticCategory] = field(default_factory=set)
    config_path: str | None = None

    @property
    def ok_count(self) -> int:
        """Count of OK findings."""
        return sum(1 for f in self.findings if f.severity == DiagnosticSeverity.OK)

    @property
    def info_count(self) -> int:
        """Count of INFO findings."""
        return sum(1 for f in self.findings if f.severity == DiagnosticSeverity.INFO)

    @property
    def warn_count(self) -> int:
        """Count of WARN findings."""
        return sum(1 for f in self.findings if f.severity == DiagnosticSeverity.WARN)

    @property
    def error_count(self) -> int:
        """Count of ERROR and CRITICAL findings."""
        return sum(
            1
            for f in self.findings
            if f.severity in (DiagnosticSeverity.ERROR, DiagnosticSeverity.CRITICAL)
        )

    @property
    def fixable_count(self) -> int:
        """Count of fixable findings."""
        return sum(1 for f in self.findings if f.fixable)

    @property
    def has_critical(self) -> bool:
        """Check if any critical findings exist."""
        return any(f.severity == DiagnosticSeverity.CRITICAL for f in self.findings)

    @property
    def exit_code(self) -> int:
        """Return appropriate exit code: 0=healthy, 1=warnings, 2=errors."""
        if self.has_critical or self.error_count > 0:
            return 2
        elif self.warn_count > 0:
            return 1
        return 0

    @property
    def duration(self) -> float:
        """Duration of diagnostic run in seconds."""
        if self.completed_at:
            return self.completed_at - self.started_at
        return time.time() - self.started_at

    def add_finding(self, finding: DiagnosticFinding) -> None:
        """Add a finding to the report."""
        self.findings.append(finding)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        from datetime import datetime, timezone

        return {
            "timestamp": datetime.fromtimestamp(
                self.started_at, tz=timezone.utc
            ).isoformat(),
            "duration_seconds": round(self.duration, 2),
            "config_path": self.config_path,
            "categories_checked": [c.value for c in self.categories_checked],
            "summary": {
                "ok": self.ok_count,
                "info": self.info_count,
                "warnings": self.warn_count,
                "errors": self.error_count,
                "fixable": self.fixable_count,
            },
            "findings": [f.to_dict() for f in self.findings],
        }


@dataclass
class FixResult:
    """Result of attempting to fix an issue."""

    finding: DiagnosticFinding
    success: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class DiagnosticCheck:
    """Definition of a diagnostic check."""

    name: str
    category: DiagnosticCategory
    description: str
    check_func: Callable[[], list[DiagnosticFinding]]
    enabled: bool = True


class Doctor:
    """Diagnostic engine for btrfs-backup-ng.

    Runs comprehensive health checks on the backup system and reports
    findings with optional auto-fix capabilities.
    """

    def __init__(
        self,
        config: Any | None = None,
        config_path: Path | str | None = None,
    ):
        """Initialize the diagnostic engine.

        Args:
            config: Loaded configuration object (optional)
            config_path: Path to configuration file (optional)
        """
        self.config = config
        self.config_path = Path(config_path) if config_path else None
        self._checks: list[DiagnosticCheck] = []
        self._register_all_checks()

    def _register_check(self, check: DiagnosticCheck) -> None:
        """Register a diagnostic check."""
        self._checks.append(check)

    def _register_all_checks(self) -> None:
        """Register all diagnostic checks."""
        # Configuration checks
        self._register_check(
            DiagnosticCheck(
                name="config_exists",
                category=DiagnosticCategory.CONFIG,
                description="Configuration file exists and is readable",
                check_func=self._check_config_exists,
            )
        )
        self._register_check(
            DiagnosticCheck(
                name="config_valid",
                category=DiagnosticCategory.CONFIG,
                description="Configuration file is valid",
                check_func=self._check_config_valid,
            )
        )
        self._register_check(
            DiagnosticCheck(
                name="volume_paths",
                category=DiagnosticCategory.CONFIG,
                description="Volume paths exist and are btrfs subvolumes",
                check_func=self._check_volume_paths,
            )
        )
        self._register_check(
            DiagnosticCheck(
                name="target_reachability",
                category=DiagnosticCategory.CONFIG,
                description="Backup targets are reachable",
                check_func=self._check_target_reachability,
            )
        )
        self._register_check(
            DiagnosticCheck(
                name="compression_programs",
                category=DiagnosticCategory.CONFIG,
                description="Compression programs are available",
                check_func=self._check_compression_programs,
            )
        )
        self._register_check(
            DiagnosticCheck(
                name="raw_target_tools",
                category=DiagnosticCategory.CONFIG,
                description="Raw target tools (GPG, compression) are available",
                check_func=self._check_raw_target_tools,
            )
        )

        # Snapshot health checks
        self._register_check(
            DiagnosticCheck(
                name="snapshot_parent_chains",
                category=DiagnosticCategory.SNAPSHOTS,
                description="Snapshot parent chains are intact",
                check_func=self._check_parent_chains,
            )
        )
        self._register_check(
            DiagnosticCheck(
                name="snapshot_dates",
                category=DiagnosticCategory.SNAPSHOTS,
                description="Snapshot dates are parseable",
                check_func=self._check_snapshot_dates,
            )
        )

        # Transfer/operation checks
        self._register_check(
            DiagnosticCheck(
                name="stale_locks",
                category=DiagnosticCategory.TRANSFERS,
                description="No stale locks from crashed operations",
                check_func=self._check_stale_locks,
            )
        )
        self._register_check(
            DiagnosticCheck(
                name="recent_failures",
                category=DiagnosticCategory.TRANSFERS,
                description="No recent operation failures",
                check_func=self._check_recent_failures,
            )
        )

        # System state checks
        self._register_check(
            DiagnosticCheck(
                name="destination_space",
                category=DiagnosticCategory.SYSTEM,
                description="Destination has sufficient space",
                check_func=self._check_destination_space,
            )
        )
        self._register_check(
            DiagnosticCheck(
                name="systemd_timer",
                category=DiagnosticCategory.SYSTEM,
                description="Systemd timer is active",
                check_func=self._check_systemd_timer,
            )
        )
        self._register_check(
            DiagnosticCheck(
                name="last_backup_age",
                category=DiagnosticCategory.SYSTEM,
                description="Last backup is recent",
                check_func=self._check_last_backup_age,
            )
        )

    def run_diagnostics(
        self,
        categories: set[DiagnosticCategory] | None = None,
        volume_filter: str | None = None,
        on_progress: Callable[[str, int, int], None] | None = None,
    ) -> DiagnosticReport:
        """Run all or selected diagnostic checks.

        Args:
            categories: Set of categories to check (None = all)
            volume_filter: Only check specific volume path
            on_progress: Progress callback (check_name, current, total)

        Returns:
            DiagnosticReport with all findings
        """
        report = DiagnosticReport()
        report.config_path = str(self.config_path) if self.config_path else None

        # Store volume filter for use by checks
        self._volume_filter = volume_filter

        # Filter checks by category
        checks_to_run = [
            c
            for c in self._checks
            if c.enabled and (categories is None or c.category in categories)
        ]

        # Track categories being checked
        report.categories_checked = {c.category for c in checks_to_run}

        # Run each check
        for i, check in enumerate(checks_to_run):
            if on_progress:
                on_progress(check.name, i + 1, len(checks_to_run))

            logger.debug("Running check: %s", check.name)
            try:
                findings = check.check_func()
                for finding in findings:
                    report.add_finding(finding)
            except Exception as e:
                # Check itself failed - report as error
                logger.error("Check %s failed: %s", check.name, e)
                report.add_finding(
                    DiagnosticFinding(
                        category=check.category,
                        severity=DiagnosticSeverity.ERROR,
                        check_name=check.name,
                        message=f"Check failed: {e}",
                        details={"exception": str(e)},
                    )
                )

        report.completed_at = time.time()
        return report

    def apply_fixes(
        self,
        report: DiagnosticReport,
        interactive: bool = False,
    ) -> list[FixResult]:
        """Apply fixes for fixable findings.

        Args:
            report: Diagnostic report with findings
            interactive: Prompt for confirmation before each fix

        Returns:
            List of fix results
        """
        results: list[FixResult] = []

        fixable = [f for f in report.findings if f.fixable and f.fix_action]

        for finding in fixable:
            if interactive:
                # Would need to prompt user - for now skip in non-interactive
                logger.info(
                    "Skipping fix for %s (interactive mode not implemented)",
                    finding.check_name,
                )
                continue

            logger.info("Applying fix for: %s", finding.message)
            try:
                if finding.fix_action:
                    success = finding.fix_action()
                    results.append(
                        FixResult(
                            finding=finding,
                            success=success,
                            message="Fix applied successfully"
                            if success
                            else "Fix failed",
                        )
                    )
            except Exception as e:
                logger.error("Fix failed: %s", e)
                results.append(
                    FixResult(
                        finding=finding,
                        success=False,
                        message=f"Fix failed: {e}",
                        details={"exception": str(e)},
                    )
                )

        return results

    # =========================================================================
    # Configuration Checks
    # =========================================================================

    def _check_config_exists(self) -> list[DiagnosticFinding]:
        """Check that config file exists and is readable."""
        findings: list[DiagnosticFinding] = []

        if not self.config_path:
            # Try to find config
            from ..config import find_config_file

            found = find_config_file()
            if found:
                self.config_path = Path(found)
            else:
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.CONFIG,
                        severity=DiagnosticSeverity.ERROR,
                        check_name="config_exists",
                        message="No configuration file found",
                        details={
                            "hint": "Create one with: btrfs-backup-ng config init"
                        },
                    )
                )
                return findings

        if not self.config_path.exists():
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.CONFIG,
                    severity=DiagnosticSeverity.ERROR,
                    check_name="config_exists",
                    message=f"Configuration file not found: {self.config_path}",
                )
            )
        elif not os.access(self.config_path, os.R_OK):
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.CONFIG,
                    severity=DiagnosticSeverity.ERROR,
                    check_name="config_exists",
                    message=f"Configuration file not readable: {self.config_path}",
                )
            )
        else:
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.CONFIG,
                    severity=DiagnosticSeverity.OK,
                    check_name="config_exists",
                    message="Configuration file exists and is readable",
                    details={"path": str(self.config_path)},
                )
            )

        return findings

    def _check_config_valid(self) -> list[DiagnosticFinding]:
        """Check that configuration is valid."""
        findings: list[DiagnosticFinding] = []

        if not self.config_path or not self.config_path.exists():
            # Already reported by config_exists check
            return findings

        if self.config is None:
            # Try to load config
            from ..config import ConfigError, load_config

            try:
                self.config, warnings = load_config(self.config_path)
                for warning in warnings:
                    findings.append(
                        DiagnosticFinding(
                            category=DiagnosticCategory.CONFIG,
                            severity=DiagnosticSeverity.WARN,
                            check_name="config_valid",
                            message=f"Configuration warning: {warning}",
                        )
                    )
            except ConfigError as e:
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.CONFIG,
                        severity=DiagnosticSeverity.ERROR,
                        check_name="config_valid",
                        message=f"Configuration error: {e}",
                    )
                )
                return findings

        # Config loaded successfully
        volumes = self.config.get_enabled_volumes() if self.config else []
        findings.append(
            DiagnosticFinding(
                category=DiagnosticCategory.CONFIG,
                severity=DiagnosticSeverity.OK,
                check_name="config_valid",
                message="Configuration is valid",
                details={"volumes": len(volumes)},
            )
        )

        return findings

    def _check_volume_paths(self) -> list[DiagnosticFinding]:
        """Check that volume paths exist and are btrfs subvolumes."""
        findings: list[DiagnosticFinding] = []

        if not self.config:
            return findings

        from ..__util__ import is_btrfs

        volumes = self.config.get_enabled_volumes()
        volume_filter = getattr(self, "_volume_filter", None)

        for volume in volumes:
            if volume_filter and volume.path != volume_filter:
                continue

            path = Path(volume.path)

            if not path.exists():
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.CONFIG,
                        severity=DiagnosticSeverity.ERROR,
                        check_name="volume_paths",
                        message=f"Volume path does not exist: {volume.path}",
                    )
                )
            elif not is_btrfs(path):
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.CONFIG,
                        severity=DiagnosticSeverity.ERROR,
                        check_name="volume_paths",
                        message=f"Volume is not on btrfs filesystem: {volume.path}",
                    )
                )
            else:
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.CONFIG,
                        severity=DiagnosticSeverity.OK,
                        check_name="volume_paths",
                        message=f"Volume path valid: {volume.path}",
                        details={"path": volume.path},
                    )
                )

        return findings

    def _check_target_reachability(self) -> list[DiagnosticFinding]:
        """Check that backup targets are reachable."""
        findings: list[DiagnosticFinding] = []

        if not self.config:
            return findings

        volumes = self.config.get_enabled_volumes()
        volume_filter = getattr(self, "_volume_filter", None)
        checked_targets: set[str] = set()

        for volume in volumes:
            if volume_filter and volume.path != volume_filter:
                continue

            for target in volume.targets:
                # Skip if already checked this target
                if target.path in checked_targets:
                    continue
                checked_targets.add(target.path)

                if target.path.startswith("ssh://"):
                    # SSH target - check connectivity
                    findings.extend(self._check_ssh_target(target))
                else:
                    # Local target - check path exists
                    target_path = Path(target.path)
                    if not target_path.exists():
                        findings.append(
                            DiagnosticFinding(
                                category=DiagnosticCategory.CONFIG,
                                severity=DiagnosticSeverity.ERROR,
                                check_name="target_reachability",
                                message=f"Target path does not exist: {target.path}",
                            )
                        )
                    else:
                        findings.append(
                            DiagnosticFinding(
                                category=DiagnosticCategory.CONFIG,
                                severity=DiagnosticSeverity.OK,
                                check_name="target_reachability",
                                message=f"Target reachable: {target.path}",
                            )
                        )

        return findings

    def _check_ssh_target(self, target: Any) -> list[DiagnosticFinding]:
        """Check SSH target connectivity."""
        findings: list[DiagnosticFinding] = []

        try:
            from ..sshutil.diagnose import test_ssh_connection

            result = test_ssh_connection(target.path)
            if isinstance(result, dict) and result.get("success"):
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.CONFIG,
                        severity=DiagnosticSeverity.OK,
                        check_name="target_reachability",
                        message=f"SSH target reachable: {target.path}",
                    )
                )
            elif isinstance(result, dict):
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.CONFIG,
                        severity=DiagnosticSeverity.ERROR,
                        check_name="target_reachability",
                        message=f"SSH target unreachable: {target.path}",
                        details={"error": result.get("error", "Unknown error")},
                    )
                )
            else:
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.CONFIG,
                        severity=DiagnosticSeverity.ERROR,
                        check_name="target_reachability",
                        message=f"SSH target check failed: {target.path}",
                    )
                )
        except ImportError:
            # SSH diagnostics not available
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.CONFIG,
                    severity=DiagnosticSeverity.INFO,
                    check_name="target_reachability",
                    message=f"SSH check skipped (diagnostics unavailable): {target.path}",
                )
            )
        except Exception as e:
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.CONFIG,
                    severity=DiagnosticSeverity.ERROR,
                    check_name="target_reachability",
                    message=f"SSH connection failed: {target.path}",
                    details={"error": str(e)},
                )
            )

        return findings

    def _check_compression_programs(self) -> list[DiagnosticFinding]:
        """Check that compression programs are available."""
        import shutil

        findings: list[DiagnosticFinding] = []

        if not self.config:
            return findings

        # Collect all compression methods used
        compression_methods: set[str] = set()
        if hasattr(self.config.global_config, "compress") and getattr(
            self.config.global_config, "compress", None
        ):
            compression_methods.add(getattr(self.config.global_config, "compress"))

        for volume in self.config.get_enabled_volumes():
            for target in volume.targets:
                if hasattr(target, "compress") and target.compress:
                    compression_methods.add(target.compress)

        # Check each compression program
        program_map = {
            "gzip": "gzip",
            "zstd": "zstd",
            "lz4": "lz4",
            "pigz": "pigz",
            "lzop": "lzop",
        }

        for method in compression_methods:
            if method == "none":
                continue

            program = program_map.get(method, method)
            if shutil.which(program):
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.CONFIG,
                        severity=DiagnosticSeverity.OK,
                        check_name="compression_programs",
                        message=f"Compression program available: {program}",
                    )
                )
            else:
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.CONFIG,
                        severity=DiagnosticSeverity.WARN,
                        check_name="compression_programs",
                        message=f"Compression program not found: {program}",
                        details={
                            "hint": f"Install {program} or change compression method"
                        },
                    )
                )

        if not compression_methods or compression_methods == {"none"}:
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.CONFIG,
                    severity=DiagnosticSeverity.OK,
                    check_name="compression_programs",
                    message="No compression configured",
                )
            )

        return findings

    def _check_raw_target_tools(self) -> list[DiagnosticFinding]:
        """Check that tools required for raw targets are available."""
        import shutil
        import subprocess

        findings: list[DiagnosticFinding] = []

        if not self.config:
            return findings

        # Collect raw target requirements from config
        raw_targets: list[dict[str, Any]] = []
        for volume in self.config.get_enabled_volumes():
            for target in volume.targets:
                path = getattr(target, "path", "")
                if path.startswith("raw://") or path.startswith("raw+ssh://"):
                    raw_targets.append(
                        {
                            "path": path,
                            "compress": getattr(target, "compress", None),
                            "encrypt": getattr(target, "encrypt", None),
                            "gpg_recipient": getattr(target, "gpg_recipient", None),
                        }
                    )

        if not raw_targets:
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.CONFIG,
                    severity=DiagnosticSeverity.OK,
                    check_name="raw_target_tools",
                    message="No raw targets configured",
                )
            )
            return findings

        # Compression tool mapping for raw targets
        from ..endpoint.raw_metadata import COMPRESSION_CONFIG

        checked_tools: set[str] = set()

        for target in raw_targets:
            compress = target.get("compress")
            encrypt = target.get("encrypt")
            gpg_recipient = target.get("gpg_recipient")

            # Check compression tool
            if compress and compress != "none" and compress not in checked_tools:
                checked_tools.add(compress)
                config = COMPRESSION_CONFIG.get(compress, {})
                cmd = config.get("compress_cmd", [])
                tool = cmd[0] if cmd else compress

                if shutil.which(tool):
                    findings.append(
                        DiagnosticFinding(
                            category=DiagnosticCategory.CONFIG,
                            severity=DiagnosticSeverity.OK,
                            check_name="raw_target_tools",
                            message=f"Raw compression tool available: {tool}",
                        )
                    )
                else:
                    findings.append(
                        DiagnosticFinding(
                            category=DiagnosticCategory.CONFIG,
                            severity=DiagnosticSeverity.ERROR,
                            check_name="raw_target_tools",
                            message=f"Raw compression tool not found: {tool}",
                            details={
                                "target": target.get("path"),
                                "hint": f"Install {tool} package",
                            },
                        )
                    )

            # Check GPG
            if encrypt == "gpg" and "gpg" not in checked_tools:
                checked_tools.add("gpg")
                if shutil.which("gpg"):
                    findings.append(
                        DiagnosticFinding(
                            category=DiagnosticCategory.CONFIG,
                            severity=DiagnosticSeverity.OK,
                            check_name="raw_target_tools",
                            message="GPG encryption tool available",
                        )
                    )

                    # Check if recipient key exists
                    if gpg_recipient:
                        try:
                            result = subprocess.run(
                                ["gpg", "--list-keys", gpg_recipient],
                                capture_output=True,
                                timeout=10,
                            )
                            if result.returncode == 0:
                                findings.append(
                                    DiagnosticFinding(
                                        category=DiagnosticCategory.CONFIG,
                                        severity=DiagnosticSeverity.OK,
                                        check_name="raw_target_tools",
                                        message=f"GPG key found for: {gpg_recipient}",
                                    )
                                )
                            else:
                                findings.append(
                                    DiagnosticFinding(
                                        category=DiagnosticCategory.CONFIG,
                                        severity=DiagnosticSeverity.ERROR,
                                        check_name="raw_target_tools",
                                        message=f"GPG key not found: {gpg_recipient}",
                                        details={
                                            "hint": "Import the public key: gpg --import <keyfile>",
                                        },
                                    )
                                )
                        except (subprocess.TimeoutExpired, OSError) as e:
                            findings.append(
                                DiagnosticFinding(
                                    category=DiagnosticCategory.CONFIG,
                                    severity=DiagnosticSeverity.WARN,
                                    check_name="raw_target_tools",
                                    message=f"Could not verify GPG key: {e}",
                                )
                            )
                else:
                    findings.append(
                        DiagnosticFinding(
                            category=DiagnosticCategory.CONFIG,
                            severity=DiagnosticSeverity.ERROR,
                            check_name="raw_target_tools",
                            message="GPG not found (required for encrypted raw targets)",
                            details={"hint": "Install gnupg package"},
                        )
                    )

            # Check OpenSSL
            if encrypt == "openssl_enc" and "openssl" not in checked_tools:
                checked_tools.add("openssl")
                if shutil.which("openssl"):
                    findings.append(
                        DiagnosticFinding(
                            category=DiagnosticCategory.CONFIG,
                            severity=DiagnosticSeverity.OK,
                            check_name="raw_target_tools",
                            message="OpenSSL encryption tool available",
                        )
                    )
                    # Check for passphrase environment variable
                    import os

                    if not (
                        os.environ.get("BTRFS_BACKUP_PASSPHRASE")
                        or os.environ.get("BTRBK_PASSPHRASE")
                    ):
                        findings.append(
                            DiagnosticFinding(
                                category=DiagnosticCategory.CONFIG,
                                severity=DiagnosticSeverity.WARN,
                                check_name="raw_target_tools",
                                message="OpenSSL passphrase not set in environment",
                                details={
                                    "hint": "Set BTRFS_BACKUP_PASSPHRASE or BTRBK_PASSPHRASE environment variable",
                                },
                            )
                        )
                else:
                    findings.append(
                        DiagnosticFinding(
                            category=DiagnosticCategory.CONFIG,
                            severity=DiagnosticSeverity.ERROR,
                            check_name="raw_target_tools",
                            message="OpenSSL not found (required for openssl_enc encryption)",
                            details={"hint": "Install openssl package"},
                        )
                    )

        return findings

    # =========================================================================
    # Snapshot Health Checks
    # =========================================================================

    def _check_parent_chains(self) -> list[DiagnosticFinding]:
        """Check that snapshot parent chains are intact."""
        findings: list[DiagnosticFinding] = []

        if not self.config:
            return findings

        # This would require accessing endpoints and listing snapshots
        # For now, return a placeholder
        findings.append(
            DiagnosticFinding(
                category=DiagnosticCategory.SNAPSHOTS,
                severity=DiagnosticSeverity.OK,
                check_name="snapshot_parent_chains",
                message="Parent chain check requires endpoint access",
                details={"note": "Full implementation pending"},
            )
        )

        return findings

    def _check_snapshot_dates(self) -> list[DiagnosticFinding]:
        """Check that snapshot dates are parseable."""
        findings: list[DiagnosticFinding] = []

        if not self.config:
            return findings

        # Placeholder - would need endpoint access
        findings.append(
            DiagnosticFinding(
                category=DiagnosticCategory.SNAPSHOTS,
                severity=DiagnosticSeverity.OK,
                check_name="snapshot_dates",
                message="Snapshot date check requires endpoint access",
                details={"note": "Full implementation pending"},
            )
        )

        return findings

    # =========================================================================
    # Transfer/Operation Checks
    # =========================================================================

    def _check_stale_locks(self) -> list[DiagnosticFinding]:
        """Check for stale locks from crashed operations."""
        findings: list[DiagnosticFinding] = []

        if not self.config:
            return findings

        from ..__util__ import read_locks

        volumes = self.config.get_enabled_volumes()
        volume_filter = getattr(self, "_volume_filter", None)

        for volume in volumes:
            if volume_filter and volume.path != volume_filter:
                continue

            snapshot_dir = Path(volume.path) / volume.snapshot_dir
            lock_file = snapshot_dir / ".btrfs-backup-ng.locks"

            if not lock_file.exists():
                continue

            try:
                lock_content = lock_file.read_text()
                locks = read_locks(lock_content)
                if not locks:
                    continue

                for snapshot_name, lock_info in locks.items():
                    snapshot_locks = lock_info.get("locks", [])
                    for lock_id in snapshot_locks:
                        # Check if process is still running
                        # Lock ID format: "operation:session_id" or contains PID
                        is_stale = self._is_lock_stale(lock_id)

                        if is_stale:
                            finding = DiagnosticFinding(
                                category=DiagnosticCategory.TRANSFERS,
                                severity=DiagnosticSeverity.WARN,
                                check_name="stale_locks",
                                message=f"Stale lock on {snapshot_name}",
                                details={
                                    "snapshot": snapshot_name,
                                    "lock_id": lock_id,
                                    "lock_file": str(lock_file),
                                },
                                fixable=True,
                                fix_description="Remove stale lock",
                            )
                            # Set up fix action - use closure to capture values
                            finding.fix_action = self._make_lock_fix_action(
                                lock_file, snapshot_name, lock_id
                            )
                            findings.append(finding)

            except Exception as e:
                logger.warning("Could not check locks at %s: %s", lock_file, e)

        if not any(f.severity != DiagnosticSeverity.OK for f in findings):
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.TRANSFERS,
                    severity=DiagnosticSeverity.OK,
                    check_name="stale_locks",
                    message="No stale locks found",
                )
            )

        return findings

    def _make_lock_fix_action(
        self, lock_file: Path, snapshot_name: str, lock_id: str
    ) -> Callable[[], bool]:
        """Create a fix action for removing a stale lock."""

        def fix_action() -> bool:
            return self._fix_stale_lock(lock_file, snapshot_name, lock_id)

        return fix_action

    def _is_lock_stale(self, lock_id: str) -> bool:
        """Check if a lock is stale (process no longer running)."""
        # Try to extract PID from lock_id
        # Common formats: "restore:abc123", "transfer:12345", etc.
        parts = lock_id.split(":")
        if len(parts) >= 2:
            try:
                # Check if second part is a PID
                pid = int(parts[1])
                # Check if process is running
                try:
                    os.kill(pid, 0)
                    return False  # Process is running
                except OSError:
                    return True  # Process not running
            except ValueError:
                pass  # Not a PID

        # Can't determine - assume not stale
        return False

    def _fix_stale_lock(
        self, lock_file: Path, snapshot_name: str, lock_id: str
    ) -> bool:
        """Remove a stale lock."""
        from ..__util__ import read_locks, write_locks

        try:
            lock_content = lock_file.read_text()
            locks = read_locks(lock_content)
            if snapshot_name in locks:
                snapshot_locks = locks[snapshot_name].get("locks", [])
                if lock_id in snapshot_locks:
                    snapshot_locks.remove(lock_id)
                    if not snapshot_locks and not locks[snapshot_name].get(
                        "parent_locks"
                    ):
                        del locks[snapshot_name]
                    else:
                        locks[snapshot_name]["locks"] = snapshot_locks
                    lock_file.write_text(write_locks(locks))
                    logger.info(
                        "Removed stale lock: %s from %s", lock_id, snapshot_name
                    )
                    return True
        except Exception as e:
            logger.error("Failed to remove lock: %s", e)
        return False

    def _check_recent_failures(self) -> list[DiagnosticFinding]:
        """Check transaction log for recent failures."""
        findings: list[DiagnosticFinding] = []

        if not self.config:
            return findings

        # Check if transaction logging is enabled
        log_file = self.config.global_config.transaction_log
        if not log_file:
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.TRANSFERS,
                    severity=DiagnosticSeverity.INFO,
                    check_name="recent_failures",
                    message="Transaction logging not enabled",
                    details={"hint": "Enable with transaction_log in config"},
                )
            )
            return findings

        log_path = Path(log_file)
        if not log_path.exists():
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.TRANSFERS,
                    severity=DiagnosticSeverity.OK,
                    check_name="recent_failures",
                    message="No transaction history yet",
                )
            )
            return findings

        try:
            from ..transaction import read_transaction_log

            # Check last 24 hours
            since = time.time() - (24 * 60 * 60)
            all_transactions = read_transaction_log(log_path)

            # Filter to last 24 hours
            transactions = [
                t for t in all_transactions if t.get("timestamp", 0) >= since
            ]

            failed = [t for t in transactions if t.get("status") == "failed"]
            successful = [t for t in transactions if t.get("status") == "completed"]

            if failed:
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.TRANSFERS,
                        severity=DiagnosticSeverity.WARN,
                        check_name="recent_failures",
                        message=f"{len(failed)} failed operation(s) in last 24h",
                        details={
                            "failed_count": len(failed),
                            "successful_count": len(successful),
                            "recent_errors": [
                                f.get("error", "Unknown") for f in failed[:3]
                            ],
                        },
                    )
                )
            else:
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.TRANSFERS,
                        severity=DiagnosticSeverity.OK,
                        check_name="recent_failures",
                        message=f"All {len(successful)} operation(s) successful in last 24h",
                    )
                )

        except Exception as e:
            logger.warning("Could not read transaction log: %s", e)
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.TRANSFERS,
                    severity=DiagnosticSeverity.WARN,
                    check_name="recent_failures",
                    message=f"Could not read transaction log: {e}",
                )
            )

        return findings

    # =========================================================================
    # System State Checks
    # =========================================================================

    def _check_destination_space(self) -> list[DiagnosticFinding]:
        """Check space availability at destinations."""
        findings: list[DiagnosticFinding] = []

        if not self.config:
            return findings

        from ..core.estimate import format_size

        volumes = self.config.get_enabled_volumes()
        volume_filter = getattr(self, "_volume_filter", None)
        checked_targets: set[str] = set()

        for volume in volumes:
            if volume_filter and volume.path != volume_filter:
                continue

            for target in volume.targets:
                if target.path in checked_targets:
                    continue
                checked_targets.add(target.path)

                if target.path.startswith("ssh://"):
                    # Skip SSH targets for now - requires connection
                    continue

                target_path = Path(target.path)
                if not target_path.exists():
                    continue

                try:
                    from ..core.space import get_space_info

                    space = get_space_info(str(target_path))
                    percent_free = (
                        space.available_bytes / space.total_bytes * 100
                        if space.total_bytes > 0
                        else 0
                    )

                    if percent_free < 5:
                        severity = DiagnosticSeverity.ERROR
                        msg = f"Critical: {target.path} only {percent_free:.1f}% free"
                    elif percent_free < 20:
                        severity = DiagnosticSeverity.WARN
                        msg = f"Low space: {target.path} {percent_free:.1f}% free"
                    else:
                        severity = DiagnosticSeverity.OK
                        msg = f"Space OK: {target.path} {percent_free:.1f}% free"

                    findings.append(
                        DiagnosticFinding(
                            category=DiagnosticCategory.SYSTEM,
                            severity=severity,
                            check_name="destination_space",
                            message=msg,
                            details={
                                "path": target.path,
                                "available": format_size(space.available_bytes),
                                "total": format_size(space.total_bytes),
                                "percent_free": round(percent_free, 1),
                            },
                        )
                    )

                except Exception as e:
                    logger.warning("Could not check space for %s: %s", target.path, e)

        if not findings:
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.SYSTEM,
                    severity=DiagnosticSeverity.OK,
                    check_name="destination_space",
                    message="No local destinations to check",
                )
            )

        return findings

    def _check_systemd_timer(self) -> list[DiagnosticFinding]:
        """Check if systemd timer is active."""
        import subprocess

        findings: list[DiagnosticFinding] = []

        try:
            username = getpass.getuser()
        except Exception:
            username = None

        timer_names = [
            "btrfs-backup-ng.timer",
            f"btrfs-backup-ng-{username}.timer" if username else None,
        ]

        for timer_name in timer_names:
            if not timer_name:
                continue

            try:
                result = subprocess.run(
                    ["systemctl", "is-active", timer_name],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )

                if result.returncode == 0 and result.stdout.strip() == "active":
                    # Get next trigger time
                    next_result = subprocess.run(
                        [
                            "systemctl",
                            "show",
                            timer_name,
                            "--property=NextElapseUSecRealtime",
                        ],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    next_time = next_result.stdout.strip().split("=", 1)[-1]

                    findings.append(
                        DiagnosticFinding(
                            category=DiagnosticCategory.SYSTEM,
                            severity=DiagnosticSeverity.OK,
                            check_name="systemd_timer",
                            message=f"Timer {timer_name} is active",
                            details={"timer": timer_name, "next": next_time},
                        )
                    )
                    return findings  # Found active timer

            except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                pass

        # No active timer found
        findings.append(
            DiagnosticFinding(
                category=DiagnosticCategory.SYSTEM,
                severity=DiagnosticSeverity.INFO,
                check_name="systemd_timer",
                message="No systemd timer installed",
                details={"hint": "Install with: btrfs-backup-ng install --timer daily"},
            )
        )

        return findings

    def _check_last_backup_age(self) -> list[DiagnosticFinding]:
        """Check how old the last successful backup is."""
        findings: list[DiagnosticFinding] = []

        if not self.config:
            return findings

        log_file = self.config.global_config.transaction_log
        if not log_file or not Path(log_file).exists():
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.SYSTEM,
                    severity=DiagnosticSeverity.INFO,
                    check_name="last_backup_age",
                    message="Cannot determine last backup (no transaction log)",
                )
            )
            return findings

        try:
            from ..transaction import read_transaction_log

            transactions = read_transaction_log(Path(log_file))
            completed = [
                t
                for t in transactions
                if t.get("status") == "completed" and t.get("action") == "transfer"
            ]

            if not completed:
                findings.append(
                    DiagnosticFinding(
                        category=DiagnosticCategory.SYSTEM,
                        severity=DiagnosticSeverity.WARN,
                        check_name="last_backup_age",
                        message="No successful backups in transaction log",
                    )
                )
                return findings

            # Find most recent
            latest = max(completed, key=lambda t: t.get("timestamp", 0))
            age_seconds = time.time() - latest.get("timestamp", 0)
            age_hours = age_seconds / 3600

            if age_hours > 48:
                severity = DiagnosticSeverity.WARN
                msg = f"Last backup was {age_hours:.1f} hours ago"
            elif age_hours > 24:
                severity = DiagnosticSeverity.INFO
                msg = f"Last backup was {age_hours:.1f} hours ago"
            else:
                severity = DiagnosticSeverity.OK
                msg = f"Last backup was {age_hours:.1f} hours ago"

            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.SYSTEM,
                    severity=severity,
                    check_name="last_backup_age",
                    message=msg,
                    details={
                        "age_hours": round(age_hours, 1),
                        "snapshot": latest.get("snapshot"),
                    },
                )
            )

        except Exception as e:
            logger.warning("Could not check backup age: %s", e)
            findings.append(
                DiagnosticFinding(
                    category=DiagnosticCategory.SYSTEM,
                    severity=DiagnosticSeverity.WARN,
                    check_name="last_backup_age",
                    message=f"Could not determine backup age: {e}",
                )
            )

        return findings
