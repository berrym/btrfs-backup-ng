"""CLI dispatcher with legacy mode detection.

This module handles routing between the new subcommand-based CLI
and legacy positional argument mode for backwards compatibility.
"""

import argparse
import sys
from typing import Callable

from .common import add_fs_checks_args, add_progress_args, add_verbosity_args

# Known subcommands for the new CLI
SUBCOMMANDS = frozenset(
    {
        "run",
        "snapshot",
        "transfer",
        "prune",
        "list",
        "status",
        "config",
        "install",
        "uninstall",
        "restore",
        "verify",
        "estimate",
        "completions",
        "manpages",
        "doctor",
    }
)


def is_legacy_mode(argv: list[str]) -> bool:
    """Detect if arguments indicate legacy CLI mode.

    Legacy mode is when the first argument looks like a path rather
    than a subcommand. This allows backwards compatibility with:
        btrfs-backup-ng /source /dest

    Args:
        argv: Command line arguments (without program name)

    Returns:
        True if legacy mode should be used
    """
    if not argv:
        return False

    first = argv[0]

    # Explicit subcommand - not legacy
    if first in SUBCOMMANDS:
        return False

    # Help/version flags - not legacy
    if first in {"-h", "--help", "-V", "--version"}:
        return False

    # Absolute or relative path - legacy mode
    if first.startswith("/") or first.startswith("./") or first.startswith("../"):
        return True

    # Contains path separator but not URL scheme - legacy mode
    if "/" in first and "://" not in first:
        return True

    # Starts with common option flags - not legacy (let parser handle)
    if first.startswith("-"):
        return False

    # Default: assume new mode (will error if invalid subcommand)
    return False


def create_subcommand_parser() -> argparse.ArgumentParser:
    """Create the main argument parser with subcommands."""
    parser = argparse.ArgumentParser(
        prog="btrfs-backup-ng",
        description="Automated btrfs backup management with incremental transfers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    add_verbosity_args(parser)

    parser.add_argument(
        "-V",
        "--version",
        action="store_true",
        help="Show version and exit",
    )

    parser.add_argument(
        "-c",
        "--config",
        metavar="FILE",
        help="Path to configuration file",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        title="commands",
        description="Available commands (use 'command --help' for details)",
    )

    # run command
    run_parser = subparsers.add_parser(
        "run",
        help="Execute all configured backup jobs",
        description="Snapshot, transfer, and prune according to configuration",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    run_parser.add_argument(
        "--parallel-volumes",
        type=int,
        metavar="N",
        help="Max concurrent volume backups (overrides config)",
    )
    run_parser.add_argument(
        "--parallel-targets",
        type=int,
        metavar="N",
        help="Max concurrent target transfers per volume (overrides config)",
    )
    run_parser.add_argument(
        "--compress",
        metavar="METHOD",
        choices=["none", "gzip", "zstd", "lz4", "pigz", "lzop"],
        help="Compression method for transfers (overrides config)",
    )
    run_parser.add_argument(
        "--rate-limit",
        metavar="RATE",
        help="Bandwidth limit (e.g., '10M', '1G') (overrides config)",
    )
    run_parser.add_argument(
        "--no-check-space",
        action="store_true",
        help="Disable pre-flight space availability check",
    )
    run_parser.add_argument(
        "--force",
        action="store_true",
        help="Proceed with transfers even if space check fails",
    )
    run_parser.add_argument(
        "--safety-margin",
        metavar="PERCENT",
        type=float,
        default=10.0,
        help="Safety margin percentage for space check (default: 10%%)",
    )
    add_progress_args(run_parser)

    # snapshot command
    snapshot_parser = subparsers.add_parser(
        "snapshot",
        help="Create snapshots only",
        description="Take snapshots without transferring to targets",
    )
    snapshot_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    snapshot_parser.add_argument(
        "--volume",
        metavar="PATH",
        action="append",
        help="Only snapshot specific volume(s)",
    )

    # transfer command
    transfer_parser = subparsers.add_parser(
        "transfer",
        help="Transfer existing snapshots to targets",
        description="Transfer snapshots without creating new ones",
    )
    transfer_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    transfer_parser.add_argument(
        "--volume",
        metavar="PATH",
        action="append",
        help="Only transfer specific volume(s)",
    )
    transfer_parser.add_argument(
        "--compress",
        metavar="METHOD",
        choices=["none", "gzip", "zstd", "lz4", "pigz", "lzop"],
        help="Compression method for transfers (overrides config)",
    )
    transfer_parser.add_argument(
        "--rate-limit",
        metavar="RATE",
        help="Bandwidth limit (e.g., '10M', '1G') (overrides config)",
    )
    transfer_parser.add_argument(
        "--no-check-space",
        action="store_true",
        help="Disable pre-flight space availability check",
    )
    transfer_parser.add_argument(
        "--force",
        action="store_true",
        help="Proceed with transfers even if space check fails",
    )
    transfer_parser.add_argument(
        "--safety-margin",
        metavar="PERCENT",
        type=float,
        default=10.0,
        help="Safety margin percentage for space check (default: 10%%)",
    )
    add_progress_args(transfer_parser)

    # prune command
    prune_parser = subparsers.add_parser(
        "prune",
        help="Apply retention policies",
        description="Clean up old snapshots and backups according to retention settings",
    )
    prune_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without making changes",
    )

    # list command
    list_parser = subparsers.add_parser(
        "list",
        help="Show snapshots and backups",
        description="List all snapshots across configured volumes and targets",
    )
    list_parser.add_argument(
        "--volume",
        metavar="PATH",
        action="append",
        help="Only list specific volume(s)",
    )
    list_parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format",
    )

    # status command
    status_parser = subparsers.add_parser(
        "status",
        help="Show job status and statistics",
        description="Display last run times, snapshot counts, and health status",
    )
    status_parser.add_argument(
        "-t",
        "--transactions",
        action="store_true",
        help="Show recent transaction history",
    )
    status_parser.add_argument(
        "-n",
        "--limit",
        type=int,
        default=10,
        metavar="N",
        help="Number of transactions to show (default: 10)",
    )

    # config command with subcommands
    config_parser = subparsers.add_parser(
        "config",
        help="Configuration management",
        description="Validate, initialize, or import configuration",
    )
    config_subs = config_parser.add_subparsers(dest="config_action")

    config_subs.add_parser(
        "validate",
        help="Validate configuration file",
    )

    init_parser = config_subs.add_parser(
        "init",
        help="Generate example configuration",
    )
    init_parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Run interactive configuration wizard",
    )
    init_parser.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        help="Output file (default: stdout)",
    )

    import_parser = config_subs.add_parser(
        "import",
        help="Import btrbk configuration",
    )
    import_parser.add_argument(
        "btrbk_config",
        metavar="FILE",
        help="Path to btrbk.conf file",
    )
    import_parser.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        help="Output file (default: stdout)",
    )

    detect_parser = config_subs.add_parser(
        "detect",
        help="Detect btrfs subvolumes on the system",
        description="Scan for btrfs subvolumes and suggest backup configuration",
    )
    detect_parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format for scripting",
    )
    detect_parser.add_argument(
        "-w",
        "--wizard",
        action="store_true",
        help="Launch interactive wizard with detected volumes",
    )

    # install command
    install_parser = subparsers.add_parser(
        "install",
        help="Install systemd timer/service",
        description="Generate and install systemd units for automated backups",
    )
    install_parser.add_argument(
        "--timer",
        choices=["hourly", "daily", "weekly"],
        help="Use preset timer interval",
    )
    install_parser.add_argument(
        "--oncalendar",
        metavar="SPEC",
        help="Custom OnCalendar specification (e.g., '*:0/15' for every 15 minutes)",
    )
    install_parser.add_argument(
        "--user",
        action="store_true",
        help="Install as user service instead of system service",
    )

    # uninstall command
    subparsers.add_parser(
        "uninstall",
        help="Remove systemd timer/service",
        description="Remove installed systemd units",
    )

    # restore command
    restore_parser = subparsers.add_parser(
        "restore",
        help="Restore snapshots from backup location",
        description="Pull snapshots from backup storage back to local system",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available snapshots at backup location
  btrfs-backup-ng restore --list ssh://backup@server:/backups/home

  # Restore latest snapshot
  btrfs-backup-ng restore ssh://backup@server:/backups/home /mnt/restore

  # Restore specific snapshot
  btrfs-backup-ng restore ssh://...:/backups/home /mnt/restore --snapshot home-20260104

  # Restore snapshot before a specific date
  btrfs-backup-ng restore ssh://...:/backups/home /mnt/restore --before "2026-01-01"

  # Interactive selection
  btrfs-backup-ng restore ssh://...:/backups/home /mnt/restore --interactive

Config-driven restore:
  # List volumes and backup targets from config
  btrfs-backup-ng restore --list-volumes

  # List snapshots available for a configured volume
  btrfs-backup-ng restore --volume /home --list

  # Restore /home from its configured backup target
  btrfs-backup-ng restore --volume /home --to /mnt/restore

  # Restore from second target (index 1)
  btrfs-backup-ng restore --volume /home --target 1 --to /mnt/restore
""",
    )
    restore_parser.add_argument(
        "source",
        nargs="?",
        metavar="SOURCE",
        help="Backup location (local path or ssh://user@host:/path)",
    )
    restore_parser.add_argument(
        "destination",
        nargs="?",
        metavar="DESTINATION",
        help="Local path to restore to",
    )
    restore_parser.add_argument(
        "-l",
        "--list",
        action="store_true",
        help="List available snapshots at backup location",
    )
    restore_parser.add_argument(
        "-s",
        "--snapshot",
        metavar="NAME",
        help="Restore specific snapshot by name",
    )
    restore_parser.add_argument(
        "--before",
        metavar="DATETIME",
        help="Restore snapshot closest to this time (YYYY-MM-DD [HH:MM:SS])",
    )
    restore_parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Restore all snapshots (full mirror)",
    )
    restore_parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Interactively select snapshot to restore",
    )
    restore_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be restored without making changes",
    )
    restore_parser.add_argument(
        "--no-incremental",
        action="store_true",
        help="Force full transfers (don't use incremental)",
    )
    restore_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing snapshots instead of skipping",
    )
    restore_parser.add_argument(
        "--in-place",
        action="store_true",
        help="Restore to original location (DANGEROUS)",
    )
    restore_parser.add_argument(
        "--yes-i-know-what-i-am-doing",
        action="store_true",
        help="Confirm dangerous operations like in-place restore",
    )
    restore_parser.add_argument(
        "--prefix",
        metavar="PREFIX",
        help="Snapshot prefix filter",
    )
    restore_parser.add_argument(
        "--ssh-sudo",
        action="store_true",
        help="Use sudo for btrfs commands on remote host",
    )
    restore_parser.add_argument(
        "--ssh-key",
        metavar="FILE",
        help="SSH private key file",
    )
    restore_parser.add_argument(
        "--compress",
        metavar="METHOD",
        choices=["none", "gzip", "zstd", "lz4", "pigz", "lzop"],
        help="Compression method for transfers",
    )
    restore_parser.add_argument(
        "--rate-limit",
        metavar="RATE",
        help="Bandwidth limit (e.g., '10M', '1G')",
    )
    add_fs_checks_args(restore_parser)

    # Config-driven restore options
    config_group = restore_parser.add_argument_group(
        "Config-driven restore",
        "Use configuration file to determine backup sources",
    )
    config_group.add_argument(
        "-c",
        "--config",
        metavar="FILE",
        help="Path to configuration file",
    )
    config_group.add_argument(
        "--volume",
        metavar="PATH",
        help="Restore backups for volume defined in config (e.g., /home)",
    )
    config_group.add_argument(
        "--target",
        metavar="INDEX",
        type=int,
        help="Target index to restore from (0-based, default: first target)",
    )
    config_group.add_argument(
        "--list-volumes",
        action="store_true",
        help="List volumes and their backup targets from config",
    )
    config_group.add_argument(
        "--to",
        metavar="PATH",
        help="Destination path for config-driven restore (used with --volume)",
    )

    # Recovery commands group
    recovery_group = restore_parser.add_argument_group(
        "Recovery options",
        "Commands for managing failed or incomplete restores",
    )
    recovery_group.add_argument(
        "--status",
        action="store_true",
        help="Show status of locks and incomplete restores at backup location",
    )
    recovery_group.add_argument(
        "--unlock",
        metavar="LOCK_ID",
        nargs="?",
        const="all",
        help="Unlock stuck restore session(s). Use 'all' or specify a lock ID",
    )
    recovery_group.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up partial/incomplete snapshot restores at destination",
    )

    add_progress_args(restore_parser)

    # verify command
    verify_parser = subparsers.add_parser(
        "verify",
        help="Verify backup integrity",
        description="Check that backups are valid and restorable",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Verification levels:
  metadata  Quick check of snapshot existence and parent chain integrity
  stream    Verify btrfs send stream can be generated (no data transfer)
  full      Complete restore test to temporary location (most thorough)

Examples:
  # Quick metadata check
  btrfs-backup-ng verify /mnt/backup/home

  # Verify remote backup over SSH
  btrfs-backup-ng verify ssh://backup@server:/backups/home --ssh-sudo

  # Stream integrity check
  btrfs-backup-ng verify /mnt/backup/home --level stream

  # Full restore test (requires temp dir on btrfs)
  btrfs-backup-ng verify ssh://...:/backups/home --level full --temp-dir /mnt/test

  # Verify specific snapshot
  btrfs-backup-ng verify /mnt/backup/home --snapshot home-20260104-120000
""",
    )
    verify_parser.add_argument(
        "location",
        metavar="LOCATION",
        help="Backup location to verify (local path or ssh://user@host:/path)",
    )
    verify_parser.add_argument(
        "--level",
        choices=["metadata", "stream", "full"],
        default="metadata",
        help="Verification level (default: metadata)",
    )
    verify_parser.add_argument(
        "--snapshot",
        metavar="NAME",
        help="Verify specific snapshot only",
    )
    verify_parser.add_argument(
        "--temp-dir",
        metavar="PATH",
        help="Temporary directory for full verification (must be on btrfs)",
    )
    verify_parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Don't delete restored snapshots after full verification",
    )
    verify_parser.add_argument(
        "--prefix",
        metavar="PREFIX",
        help="Snapshot prefix filter",
    )
    verify_parser.add_argument(
        "--ssh-sudo",
        action="store_true",
        help="Use sudo for btrfs commands on remote host",
    )
    verify_parser.add_argument(
        "--ssh-key",
        metavar="FILE",
        help="SSH private key file",
    )
    add_fs_checks_args(verify_parser)
    verify_parser.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format",
    )
    verify_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress output",
    )

    # completions command
    completions_parser = subparsers.add_parser(
        "completions",
        help="Install shell completion scripts",
        description="Install or locate shell completion scripts for bash, zsh, or fish",
    )
    completions_subs = completions_parser.add_subparsers(dest="completions_action")

    completions_install = completions_subs.add_parser(
        "install",
        help="Install completions for your shell",
    )
    completions_install.add_argument(
        "--shell",
        choices=["bash", "zsh", "fish"],
        required=True,
        help="Shell to install completions for",
    )
    completions_install.add_argument(
        "--system",
        action="store_true",
        help="Install system-wide (requires root)",
    )

    completions_subs.add_parser(
        "path",
        help="Show path to completion scripts",
    )

    # manpages command
    manpages_parser = subparsers.add_parser(
        "manpages",
        help="Install man pages",
        description="Install or locate man pages for btrfs-backup-ng commands",
    )
    manpages_subs = manpages_parser.add_subparsers(dest="manpages_action")

    manpages_install = manpages_subs.add_parser(
        "install",
        help="Install man pages",
    )
    manpages_install.add_argument(
        "--system",
        action="store_true",
        help="Install system-wide to /usr/local/share/man (requires root)",
    )
    manpages_install.add_argument(
        "--prefix",
        metavar="PATH",
        help="Install to PREFIX/share/man/man1",
    )

    manpages_subs.add_parser(
        "path",
        help="Show path to man page files",
    )

    # estimate command
    estimate_parser = subparsers.add_parser(
        "estimate",
        help="Estimate backup transfer sizes",
        description="Calculate data sizes before transferring backups",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Estimate transfer size for direct paths
  btrfs-backup-ng estimate /mnt/snapshots ssh://backup@server:/backups

  # Estimate using configuration file
  btrfs-backup-ng estimate --volume /home

  # Estimate with JSON output
  btrfs-backup-ng estimate --volume /home --json

  # Estimate for specific target
  btrfs-backup-ng estimate --volume /home --target 1

  # Check if destination has sufficient space
  btrfs-backup-ng estimate --volume /home --check-space

  # Check space with custom safety margin (20%)
  btrfs-backup-ng estimate --volume /home --check-space --safety-margin 20
""",
    )
    estimate_parser.add_argument(
        "source",
        nargs="?",
        metavar="SOURCE",
        help="Source snapshot location",
    )
    estimate_parser.add_argument(
        "destination",
        nargs="?",
        metavar="DESTINATION",
        help="Backup destination (local path or ssh://user@host:/path)",
    )
    estimate_parser.add_argument(
        "-c",
        "--config",
        metavar="FILE",
        help="Path to configuration file",
    )
    estimate_parser.add_argument(
        "--volume",
        metavar="PATH",
        help="Estimate for volume defined in config (e.g., /home)",
    )
    estimate_parser.add_argument(
        "--target",
        metavar="INDEX",
        type=int,
        help="Target index to estimate for (0-based, default: first target)",
    )
    estimate_parser.add_argument(
        "--prefix",
        metavar="PREFIX",
        help="Snapshot prefix filter",
    )
    estimate_parser.add_argument(
        "--ssh-sudo",
        action="store_true",
        help="Use sudo for btrfs commands on remote host",
    )
    estimate_parser.add_argument(
        "--ssh-key",
        metavar="FILE",
        help="SSH private key file",
    )
    add_fs_checks_args(estimate_parser)
    estimate_parser.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format",
    )
    estimate_parser.add_argument(
        "--check-space",
        action="store_true",
        help="Check if destination has sufficient space for the transfer",
    )
    estimate_parser.add_argument(
        "--safety-margin",
        metavar="PERCENT",
        type=float,
        default=10.0,
        help="Safety margin percentage for space check (default: 10%%)",
    )

    # doctor command
    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Diagnose backup system health",
        description="Analyze and diagnose problems with the backup system",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full system diagnosis
  btrfs-backup-ng doctor

  # JSON output for scripting
  btrfs-backup-ng doctor --json

  # Check specific category only
  btrfs-backup-ng doctor --check config
  btrfs-backup-ng doctor --check snapshots
  btrfs-backup-ng doctor --check transfers
  btrfs-backup-ng doctor --check system

  # Auto-fix safe issues
  btrfs-backup-ng doctor --fix

  # Fix with confirmation prompts
  btrfs-backup-ng doctor --fix --interactive
""",
    )
    doctor_parser.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format",
    )
    doctor_parser.add_argument(
        "--check",
        choices=["config", "snapshots", "transfers", "system"],
        action="append",
        metavar="CATEGORY",
        help="Check specific category only (can be repeated)",
    )
    doctor_parser.add_argument(
        "--fix",
        action="store_true",
        help="Attempt to automatically fix safe issues",
    )
    doctor_parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Prompt for confirmation before each fix",
    )
    doctor_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Only show warnings and errors",
    )
    doctor_parser.add_argument(
        "--volume",
        metavar="PATH",
        action="append",
        help="Only check specific volume(s)",
    )

    return parser


def show_migration_notice() -> None:
    """Show one-time notice about config file migration."""
    from pathlib import Path

    notice_file = (
        Path.home() / ".config" / "btrfs-backup-ng" / ".migration-notice-shown"
    )

    if notice_file.exists():
        return

    print("=" * 70)
    print("TIP: btrfs-backup-ng now supports TOML configuration files!")
    print("")
    print("Instead of command-line arguments, you can define your backup")
    print("configuration in a config file for easier management.")
    print("")
    print("Generate an example config:")
    print("  btrfs-backup-ng config init > ~/.config/btrfs-backup-ng/config.toml")
    print("")
    print("Then run backups with:")
    print("  btrfs-backup-ng run")
    print("")
    print("This notice will only be shown once.")
    print("=" * 70)
    print("")

    # Create notice file to prevent showing again
    try:
        notice_file.parent.mkdir(parents=True, exist_ok=True)
        notice_file.touch()
    except OSError:
        pass  # Ignore if we can't write the notice file


def run_legacy_mode(argv: list[str]) -> int:
    """Run in legacy mode using the original CLI interface.

    Args:
        argv: Command line arguments

    Returns:
        Exit code
    """
    # Show migration notice (one time only)
    show_migration_notice()

    # Import and run the original main function
    from .._legacy_main import legacy_main

    return legacy_main(argv)


def run_subcommand(args: argparse.Namespace) -> int:
    """Run the specified subcommand.

    Args:
        args: Parsed arguments

    Returns:
        Exit code
    """
    from .. import __version__

    if args.version:
        print(f"btrfs-backup-ng {__version__}")
        return 0

    if not args.command:
        print("No command specified. Use --help for usage information.")
        return 1

    # Route to appropriate command handler
    handlers: dict[str, Callable] = {
        "run": cmd_run,
        "snapshot": cmd_snapshot,
        "transfer": cmd_transfer,
        "prune": cmd_prune,
        "list": cmd_list,
        "status": cmd_status,
        "config": cmd_config,
        "install": cmd_install,
        "uninstall": cmd_uninstall,
        "restore": cmd_restore,
        "verify": cmd_verify,
        "estimate": cmd_estimate,
        "completions": cmd_completions,
        "manpages": cmd_manpages,
        "doctor": cmd_doctor,
    }

    handler = handlers.get(args.command)
    if handler:
        return handler(args)
    else:
        print(f"Unknown command: {args.command}")
        return 1


# Command handlers - these will be implemented in separate modules
# For now, they're stubs that print what would happen


def cmd_run(args: argparse.Namespace) -> int:
    """Execute run command."""
    from .run import execute_run

    return execute_run(args)


def cmd_snapshot(args: argparse.Namespace) -> int:
    """Execute snapshot command."""
    from .snapshot import execute_snapshot

    return execute_snapshot(args)


def cmd_transfer(args: argparse.Namespace) -> int:
    """Execute transfer command."""
    from .transfer import execute_transfer

    return execute_transfer(args)


def cmd_prune(args: argparse.Namespace) -> int:
    """Execute prune command."""
    from .prune import execute_prune

    return execute_prune(args)


def cmd_list(args: argparse.Namespace) -> int:
    """Execute list command."""
    from .list_cmd import execute_list

    return execute_list(args)


def cmd_status(args: argparse.Namespace) -> int:
    """Execute status command."""
    from .status import execute_status

    return execute_status(args)


def cmd_config(args: argparse.Namespace) -> int:
    """Execute config command."""
    from .config_cmd import execute_config

    return execute_config(args)


def cmd_install(args: argparse.Namespace) -> int:
    """Execute install command."""
    from .install import execute_install

    return execute_install(args)


def cmd_uninstall(args: argparse.Namespace) -> int:
    """Execute uninstall command."""
    from .install import execute_uninstall

    return execute_uninstall(args)


def cmd_restore(args: argparse.Namespace) -> int:
    """Execute restore command."""
    from .restore import execute_restore

    return execute_restore(args)


def cmd_verify(args: argparse.Namespace) -> int:
    """Execute verify command."""
    from .verify import execute

    return execute(args)


def cmd_estimate(args: argparse.Namespace) -> int:
    """Execute estimate command."""
    from .estimate import execute_estimate

    return execute_estimate(args)


def cmd_completions(args: argparse.Namespace) -> int:
    """Execute completions command."""
    from .completions import execute_completions

    return execute_completions(args)


def cmd_manpages(args: argparse.Namespace) -> int:
    """Execute manpages command."""
    from .manpages import execute_manpages

    return execute_manpages(args)


def cmd_doctor(args: argparse.Namespace) -> int:
    """Execute doctor command."""
    from .doctor import execute_doctor

    return execute_doctor(args)


def main(argv: list[str] | None = None) -> int:
    """Main entry point for btrfs-backup-ng CLI.

    Args:
        argv: Command line arguments (defaults to sys.argv[1:])

    Returns:
        Exit code
    """
    if argv is None:
        argv = sys.argv[1:]

    # Check for legacy mode
    if is_legacy_mode(argv):
        return run_legacy_mode(argv)

    # Parse with new subcommand interface
    parser = create_subcommand_parser()
    args = parser.parse_args(argv)

    return run_subcommand(args)
