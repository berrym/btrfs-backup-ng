"""CLI dispatcher with legacy mode detection.

This module handles routing between the new subcommand-based CLI
and legacy positional argument mode for backwards compatibility.
"""

import argparse
import sys
from typing import Callable

from .common import add_progress_args, add_verbosity_args, create_global_parser

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

    return parser


def show_migration_notice() -> None:
    """Show one-time notice about config file migration."""
    import os
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
