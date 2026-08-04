"""CLI handler for verify command."""

import argparse
import logging
from pathlib import Path

from rich.console import Console
from rich.table import Table

from .. import endpoint
from ..core.verify import (
    VerifyLevel,
    VerifyReport,
    verify_full,
    verify_metadata,
    verify_raw_checksums,
    verify_stream,
)
from .common import get_fs_checks_mode, resolve_timestamp_format

logger = logging.getLogger(__name__)
console = Console()


def execute(args: argparse.Namespace) -> int:
    """Execute verify command.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 = success, 1 = failures found, 2 = error)
    """
    # Build endpoint kwargs. Thread timestamp_format so custom-named snapshots
    # are parsed (otherwise verify silently skips them and can report success).
    endpoint_kwargs = {
        "snap_prefix": args.prefix or "",
        "convert_rw": False,
        "subvolume_sync": False,
        "btrfs_debug": False,
        "fs_checks": get_fs_checks_mode(args),
        "timestamp_format": resolve_timestamp_format(
            getattr(args, "timestamp_format", None)
        ),
    }

    # Host-key policy override (R12b): threaded for any remote; harmless (ignored) for local.
    if getattr(args, "ssh_host_key_policy", None):
        endpoint_kwargs["ssh_host_key_policy"] = args.ssh_host_key_policy

    # SSH / raw options
    if args.location.startswith("ssh://"):
        if args.ssh_sudo:
            endpoint_kwargs["ssh_sudo"] = True
        if args.ssh_key:
            endpoint_kwargs["ssh_identity_file"] = args.ssh_key
        if getattr(args, "ssh_auth_sock", None):
            endpoint_kwargs["ssh_auth_sock"] = args.ssh_auth_sock
    elif args.location.startswith(("raw://", "raw+ssh://")):
        # Raw target: choose_endpoint parses the path (and host, for raw+ssh) from the
        # spec itself, so do NOT set 'path' -- Path().resolve() on a raw:// URL would
        # mangle it. Thread ssh creds for raw+ssh (SSHRawEndpoint reads
        # ssh_sudo/ssh_key/ssh_auth_sock from config); the base RawEndpoint ignores them.
        if args.location.startswith("raw+ssh://"):
            if args.ssh_sudo:
                endpoint_kwargs["ssh_sudo"] = True
            if args.ssh_key:
                endpoint_kwargs["ssh_key"] = args.ssh_key
            if getattr(args, "ssh_auth_sock", None):
                endpoint_kwargs["ssh_auth_sock"] = args.ssh_auth_sock
    else:
        # For local paths, set 'path' for LocalEndpoint
        endpoint_kwargs["path"] = Path(args.location).resolve()

    # NOTE: verify does NOT thread a decryption keyring/cipher. Raw verification
    # recomputes the sha256 of the stored (still-encrypted) stream file and
    # compares it to the checksum sealed in the .meta sidecar -- it never decodes
    # the stream -- so a keyring is not needed to verify an encrypted raw backup.
    # (restore, which does decode, threads them; see cli/restore.py.)

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

    # A raw:// or raw+ssh:// target stores send STREAMS, not subvolumes, so its
    # verification is the sealed-checksum check (not btrfs send/restore).
    from ..endpoint.raw import (
        RawEndpoint,
    )  # local import: avoid an endpoint import cycle

    is_raw = isinstance(backup_ep, RawEndpoint)

    # Resolve level: an explicit --level wins; otherwise default to metadata for a btrfs
    # target, but stream (the sealed-checksum check) for a raw target -- a bare
    # `verify raw://X` must read the stored bytes, not report a hollow pass from a listing.
    if args.level:
        level = VerifyLevel(args.level)
    else:
        level = VerifyLevel.STREAM if is_raw else VerifyLevel.METADATA

    # In --json mode stdout must be machine-readable, so the human header/progress
    # lines (which go to stdout) are suppressed just like under --quiet -- otherwise a
    # `verify ... --json` consumer gets unparseable output unless they also pass --quiet.
    human = not args.quiet and not args.json

    # Progress callback
    def on_progress(current: int, total: int, name: str):
        if human:
            console.print(f"  [{current}/{total}] Verifying {name}...")

    # Run verification based on level
    report: VerifyReport
    try:
        if human:
            console.print(f"\n[bold]Verifying backups at:[/bold] {args.location}")
            console.print(f"[bold]Level:[/bold] {level.value}\n")

        if is_raw and level in (VerifyLevel.STREAM, VerifyLevel.FULL):
            # For a raw target, stream/full verification means recompute each stream's
            # sha256 and compare it to the sealed sidecar checksum (a raw backup is a
            # stored stream, not a subvolume -- btrfs send/restore cannot operate on it).
            report = verify_raw_checksums(
                backup_ep,
                level,
                snapshot_name=args.snapshot,
                on_progress=on_progress if human else None,
            )

        elif level == VerifyLevel.METADATA:
            report = verify_metadata(
                backup_ep,
                snapshot_name=args.snapshot,
                on_progress=on_progress if human else None,
            )

        elif level == VerifyLevel.STREAM:
            report = verify_stream(
                backup_ep,
                snapshot_name=args.snapshot,
                on_progress=on_progress if human else None,
                all_snapshots=getattr(args, "all", False),
            )

        else:  # level == VerifyLevel.FULL
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
                on_progress=on_progress if human else None,
                all_snapshots=getattr(args, "all", False),
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
            # An unverifiable result is NOT a pass -- give it a distinct (yellow) status so
            # it is never read as a clean green PASS.
            status_key = result.details.get("status")
            if not result.passed:
                status = "[red]FAIL[/red]"
            elif status_key == "unverifiable":
                status = "[yellow]UNVERIFIABLE[/yellow]"
            else:
                status = "[green]PASS[/green]"

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
    # Honest counts: 'verified' is positively-confirmed (status ok), separate from
    # 'unverifiable' (not a failure, but not confirmed) -- never lumped into a green pass.
    # Skipped for a run that verified nothing (empty/errored) -- the errors above say why.
    if report.results:
        console.print(
            f"  Results: [green]{report.verified_ok} verified[/green], "
            f"[red]{report.failed} failed[/red], "
            f"[yellow]{report.unverifiable} unverifiable[/yellow] "
            f"(checked {report.total} of {report.available} snapshots)"
        )
    # If only a subset was checked (stream/full default to the latest), make the sampling
    # visible and actionable -- otherwise "verified" reads as if the whole history passed.
    # Suppressed when the user explicitly named one snapshot (--all is mutually exclusive
    # with --snapshot, so the hint would be unfollowable) or when nothing was checked.
    if (
        report.results
        and report.available > report.total
        and not report.errors
        and not getattr(args, "snapshot", None)
    ):
        console.print(
            f"  [dim]Only the latest {report.total} of {report.available} checked -- "
            f"pass --all to verify every snapshot.[/dim]"
        )

    verdict = report.verdict
    if verdict == "pass":
        console.print(
            f"\n[green bold]✓ All {report.total} checked snapshot(s) verified[/green bold]"
        )
    elif verdict == "unverifiable":
        console.print(
            f"\n[yellow bold]⚠ {report.verified_ok} verified, {report.unverifiable} "
            f"could not be verified (no failures)[/yellow bold]"
        )
    else:  # fail
        console.print("\n[red bold]✗ Verification found issues[/red bold]")


def _display_json(report: VerifyReport):
    """Display report as JSON."""
    import json

    data = {
        "level": report.level.value,
        "location": report.location,
        # Authoritative top-level tri-state for monitoring: gate on this instead of
        # re-deriving from counts (a naive summary.failed==0 treats an empty/errored run as
        # success). 'fail' = a real failure or run error; 'unverifiable' = no failures but
        # something could not be confirmed; 'pass' = every checked snapshot verified.
        "verdict": report.verdict,
        "duration_seconds": report.duration,
        "summary": {
            "verified": report.verified_ok,
            "failed": report.failed,
            "unverifiable": report.unverifiable,
            "passed": report.passed,  # back-compat: not-failed (verified + unverifiable)
            "checked": report.total,
            "available": report.available,
            "total": report.total,  # back-compat alias for checked
        },
        "results": [
            {
                "snapshot": r.snapshot_name,
                # Every verify path sets details["status"]; the fallback NEVER guesses "ok"
                # for a status-less passed result (that would falsely claim a snapshot was
                # verified) -- an unset status degrades to the honest "unverifiable".
                "status": r.details.get(
                    "status", "failed" if not r.passed else "unverifiable"
                ),
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
