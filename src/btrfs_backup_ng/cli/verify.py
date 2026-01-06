"""CLI handler for verify command."""

import argparse
import logging

from rich.console import Console
from rich.table import Table

from .. import endpoint
from ..core.verify import (
    VerifyLevel,
    VerifyReport,
    verify_full,
    verify_metadata,
    verify_stream,
)
from .common import get_fs_checks_mode

logger = logging.getLogger(__name__)
console = Console()


def execute(args: argparse.Namespace) -> int:
    """Execute verify command.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 = success, 1 = failures found, 2 = error)
    """
    # Determine verification level
    level = VerifyLevel(args.level)

    # Build endpoint kwargs
    endpoint_kwargs = {
        "snap_prefix": args.prefix or "",
        "convert_rw": False,
        "subvolume_sync": False,
        "btrfs_debug": False,
        "fs_checks": get_fs_checks_mode(args),
    }

    # SSH options
    if args.location.startswith("ssh://"):
        if args.ssh_sudo:
            endpoint_kwargs["ssh_sudo"] = True
        if args.ssh_key:
            endpoint_kwargs["ssh_identity_file"] = args.ssh_key
    else:
        # For local paths, set 'path' for LocalEndpoint
        from pathlib import Path

        endpoint_kwargs["path"] = Path(args.location).resolve()

    # Create endpoint for backup location
    try:
        backup_ep = endpoint.choose_endpoint(
            args.location,
            endpoint_kwargs,
            source=False,  # Path goes to config["path"]
        )
        # Prepare endpoint (runs diagnostics for SSH, detects passwordless sudo, etc.)
        backup_ep.prepare()
    except Exception as e:
        console.print(f"[red]Error:[/red] Cannot access backup location: {e}")
        return 2

    # Progress callback
    def on_progress(current: int, total: int, name: str):
        if not args.quiet:
            console.print(f"  [{current}/{total}] Verifying {name}...")

    # Run verification based on level
    try:
        if not args.quiet:
            console.print(f"\n[bold]Verifying backups at:[/bold] {args.location}")
            console.print(f"[bold]Level:[/bold] {level.value}\n")

        if level == VerifyLevel.METADATA:
            report = verify_metadata(
                backup_ep,
                snapshot_name=args.snapshot,
                on_progress=on_progress if not args.quiet else None,
            )

        elif level == VerifyLevel.STREAM:
            report = verify_stream(
                backup_ep,
                snapshot_name=args.snapshot,
                on_progress=on_progress if not args.quiet else None,
            )

        elif level == VerifyLevel.FULL:
            if not args.temp_dir:
                # For remote backups, temp-dir is required
                if "://" in args.location or args.location.startswith("ssh:"):
                    console.print(
                        "[red]Error:[/red] --temp-dir is required for remote backup "
                        "verification (must be on local btrfs filesystem)"
                    )
                    return 2

            report = verify_full(
                backup_ep,
                snapshot_name=args.snapshot,
                temp_dir=Path(args.temp_dir) if args.temp_dir else None,
                cleanup=not args.no_cleanup,
                on_progress=on_progress if not args.quiet else None,
            )

    except KeyboardInterrupt:
        console.print("\n[yellow]Verification interrupted[/yellow]")
        return 2
    except Exception as e:
        console.print(f"[red]Verification error:[/red] {e}")
        logger.exception("Verification failed")
        return 2

    # Display results
    _display_report(report, args)

    # Return appropriate exit code
    if report.errors:
        return 2
    elif report.failed > 0:
        return 1
    else:
        return 0


def _display_report(report: VerifyReport, args: argparse.Namespace):
    """Display verification report."""
    if args.json:
        _display_json(report)
        return

    console.print()

    # Results table
    if report.results:
        table = Table(title="Verification Results")
        table.add_column("Snapshot", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Details")

        for result in report.results:
            if result.passed:
                status = "[green]PASS[/green]"
            else:
                status = "[red]FAIL[/red]"

            details = result.message
            if not details and result.details:
                if result.details.get("is_base"):
                    details = "Base snapshot (no parent)"
                elif result.details.get("parent"):
                    details = f"Parent: {result.details['parent']}"

            table.add_row(result.snapshot_name, status, details)

        console.print(table)

    # Errors
    if report.errors:
        console.print("\n[red]Errors:[/red]")
        for err in report.errors:
            console.print(f"  - {err}")

    # Summary
    console.print()
    console.print("[bold]Summary:[/bold]")
    console.print(f"  Location: {report.location}")
    console.print(f"  Level: {report.level.value}")
    console.print(f"  Duration: {report.duration:.1f}s")
    console.print(
        f"  Results: [green]{report.passed} passed[/green], "
        f"[red]{report.failed} failed[/red] "
        f"(of {report.total} total)"
    )

    if report.failed == 0 and not report.errors:
        console.print("\n[green bold]✓ All verifications passed[/green bold]")
    else:
        console.print("\n[red bold]✗ Verification found issues[/red bold]")


def _display_json(report: VerifyReport):
    """Display report as JSON."""
    import json

    data = {
        "level": report.level.value,
        "location": report.location,
        "duration_seconds": report.duration,
        "summary": {
            "passed": report.passed,
            "failed": report.failed,
            "total": report.total,
        },
        "results": [
            {
                "snapshot": r.snapshot_name,
                "passed": r.passed,
                "message": r.message,
                "duration_seconds": r.duration_seconds,
                "details": r.details,
            }
            for r in report.results
        ],
        "errors": report.errors,
    }

    print(json.dumps(data, indent=2))
