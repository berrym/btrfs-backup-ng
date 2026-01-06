"""Doctor command: Diagnose backup system health.

Provides comprehensive diagnostics for the backup system including
configuration validation, snapshot integrity, transfer state, and
system health monitoring.
"""

import argparse
import json
import logging

from ..__logger__ import create_logger
from ..config import ConfigError, find_config_file, load_config
from ..core.doctor import (
    DiagnosticCategory,
    DiagnosticReport,
    DiagnosticSeverity,
    Doctor,
)
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_doctor(args: argparse.Namespace) -> int:
    """Execute the doctor command.

    Runs diagnostic checks on the backup system and reports findings.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0=healthy, 1=warnings, 2=errors)
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    # Determine config path
    config_path = getattr(args, "config", None)
    if not config_path:
        config_path = find_config_file()

    # Try to load config (doctor will work even without valid config)
    config = None
    if config_path:
        try:
            config, _warnings = load_config(config_path)
        except ConfigError:
            pass  # Doctor will report this

    # Parse categories to check
    categories = None
    check_args = getattr(args, "check", None)
    if check_args:
        category_map = {
            "config": DiagnosticCategory.CONFIG,
            "snapshots": DiagnosticCategory.SNAPSHOTS,
            "transfers": DiagnosticCategory.TRANSFERS,
            "system": DiagnosticCategory.SYSTEM,
        }
        categories = {category_map[c] for c in check_args if c in category_map}

    # Get volume filter
    volume_filter_arg = getattr(args, "volume", None)
    volume_filter: str | None = None
    if isinstance(volume_filter_arg, list) and volume_filter_arg:
        volume_filter = str(
            volume_filter_arg[0]
        )  # Use first volume if multiple specified
    elif isinstance(volume_filter_arg, str):
        volume_filter = volume_filter_arg

    # Create doctor and run diagnostics
    doctor = Doctor(config=config, config_path=config_path)

    quiet = getattr(args, "quiet", False)
    json_output = getattr(args, "json", False)

    # Progress callback (skip in quiet or JSON mode)
    def on_progress(check_name: str, current: int, total: int) -> None:
        if not quiet and not json_output:
            print(f"\rRunning checks... [{current}/{total}]", end="", flush=True)

    report = doctor.run_diagnostics(
        categories=categories,
        volume_filter=volume_filter,
        on_progress=on_progress if not quiet and not json_output else None,
    )

    # Clear progress line
    if not quiet and not json_output:
        print("\r" + " " * 40 + "\r", end="")

    # Apply fixes if requested
    fix_results = []
    if getattr(args, "fix", False):
        interactive = getattr(args, "interactive", False)
        fix_results = doctor.apply_fixes(report, interactive=interactive)

    # Output results
    if json_output:
        _print_json(report, fix_results)
    else:
        _print_report(report, quiet=quiet)
        if fix_results:
            _print_fix_results(fix_results)

    return report.exit_code


def _print_report(report: DiagnosticReport, quiet: bool = False) -> None:
    """Print diagnostic report in human-readable format.

    Args:
        report: The diagnostic report
        quiet: If True, only show warnings and errors
    """
    print("btrfs-backup-ng Doctor")
    print("=" * 60)

    if report.config_path:
        print(f"Config: {report.config_path}")
    print()

    # Group findings by category
    by_category: dict[DiagnosticCategory, list] = {}
    for finding in report.findings:
        if finding.category not in by_category:
            by_category[finding.category] = []
        by_category[finding.category].append(finding)

    # Print each category
    category_names = {
        DiagnosticCategory.CONFIG: "Configuration",
        DiagnosticCategory.SNAPSHOTS: "Snapshots",
        DiagnosticCategory.TRANSFERS: "Transfers",
        DiagnosticCategory.SYSTEM: "System",
    }

    for category in DiagnosticCategory:
        if category not in by_category:
            continue

        findings = by_category[category]

        # In quiet mode, skip categories with only OK/INFO findings
        if quiet:
            has_problems = any(
                f.severity
                in (
                    DiagnosticSeverity.WARN,
                    DiagnosticSeverity.ERROR,
                    DiagnosticSeverity.CRITICAL,
                )
                for f in findings
            )
            if not has_problems:
                continue

        print(category_names.get(category, category.value))
        print("-" * 40)

        for finding in findings:
            # In quiet mode, skip OK/INFO findings
            if quiet and finding.severity in (
                DiagnosticSeverity.OK,
                DiagnosticSeverity.INFO,
            ):
                continue

            prefix = _get_severity_prefix(finding.severity)
            print(f"  {prefix}  {finding.message}")

            # Show fix hint for fixable issues
            if finding.fixable:
                print("          [FIXABLE] Run with --fix to repair")

            # Show details hint if present
            if finding.details.get("hint"):
                print(f"          Hint: {finding.details['hint']}")

        print()

    # Summary
    print("=" * 60)
    print(
        f"Summary: {report.ok_count} passed, "
        f"{report.warn_count} warnings, "
        f"{report.error_count} errors"
    )
    if report.fixable_count > 0:
        print(f"Fixable: {report.fixable_count} issue(s) (run with --fix)")
    print(f"Duration: {report.duration:.2f}s")


def _print_json(report: DiagnosticReport, fix_results: list) -> None:
    """Print report as JSON.

    Args:
        report: The diagnostic report
        fix_results: List of fix results (if any)
    """
    data = report.to_dict()

    if fix_results:
        data["fixes"] = [
            {
                "check": r.finding.check_name,
                "message": r.finding.message,
                "success": r.success,
                "result_message": r.message,
            }
            for r in fix_results
        ]

    print(json.dumps(data, indent=2))


def _print_fix_results(results: list) -> None:
    """Print fix results.

    Args:
        results: List of FixResult objects
    """
    print()
    print("Fix Results")
    print("-" * 40)

    for result in results:
        status = "[OK]" if result.success else "[FAILED]"
        print(f"  {status}  {result.finding.message}")
        if not result.success:
            print(f"          Error: {result.message}")


def _get_severity_prefix(severity: DiagnosticSeverity) -> str:
    """Get display prefix for severity level.

    Args:
        severity: The severity level

    Returns:
        Formatted prefix string
    """
    prefixes = {
        DiagnosticSeverity.OK: "[OK]",
        DiagnosticSeverity.INFO: "[INFO]",
        DiagnosticSeverity.WARN: "[WARN]",
        DiagnosticSeverity.ERROR: "[ERROR]",
        DiagnosticSeverity.CRITICAL: "[CRIT]",
    }
    return prefixes.get(severity, "[????]")
