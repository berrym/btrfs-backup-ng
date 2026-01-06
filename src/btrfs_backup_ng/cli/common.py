"""Shared CLI utilities and argument parsers."""

import argparse
import sys


def is_interactive() -> bool:
    """Check if we're running in an interactive terminal.

    Returns True if stdout is a TTY, which typically means
    a human is watching and progress bars are appropriate.

    Returns:
        True if running interactively
    """
    return sys.stdout.isatty()


def should_show_progress(args: argparse.Namespace) -> bool:
    """Determine if progress bars should be shown.

    Logic:
    - If --progress is set, always show
    - If --no-progress is set, never show
    - Otherwise, auto-detect based on TTY

    Args:
        args: Parsed command line arguments

    Returns:
        True if progress should be shown
    """
    # Explicit flags take precedence
    if getattr(args, "progress", False):
        return True
    if getattr(args, "no_progress", False):
        return False

    # Quiet mode implies no progress
    if getattr(args, "quiet", False):
        return False

    # Auto-detect based on TTY
    return is_interactive()


def add_progress_args(parser: argparse.ArgumentParser) -> None:
    """Add progress-related arguments to a parser."""
    group = parser.add_argument_group("Progress options")
    mutex = group.add_mutually_exclusive_group()
    mutex.add_argument(
        "--progress",
        action="store_true",
        help="Show progress bars (default when running in terminal)",
    )
    mutex.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars (default when not in terminal)",
    )


def create_global_parser() -> argparse.ArgumentParser:
    """Create a parser with global options that can be used as a parent."""
    parser = argparse.ArgumentParser(add_help=False)
    add_verbosity_args(parser)
    return parser


def add_verbosity_args(parser: argparse.ArgumentParser) -> None:
    """Add verbosity-related arguments to a parser."""
    group = parser.add_argument_group("Output options")
    group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    group.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress non-essential output",
    )
    group.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )


def get_log_level(args: argparse.Namespace) -> str:
    """Determine log level from parsed arguments.

    Args:
        args: Parsed command line arguments

    Returns:
        Log level string (DEBUG, INFO, WARNING, ERROR)
    """
    if getattr(args, "debug", False):
        return "DEBUG"
    elif getattr(args, "quiet", False):
        return "WARNING"
    elif getattr(args, "verbose", False):
        return "DEBUG"
    else:
        return "INFO"


def add_fs_checks_args(parser: argparse.ArgumentParser) -> None:
    """Add filesystem check arguments to a parser.

    Adds --fs-checks with choices (auto, strict, skip) and --no-fs-checks
    as a convenience alias for --fs-checks=skip.
    """
    group = parser.add_argument_group("Filesystem check options")
    group.add_argument(
        "--fs-checks",
        choices=["auto", "strict", "skip"],
        default="auto",
        help="Filesystem verification mode: 'auto' (warn and continue), "
        "'strict' (error on failure), 'skip' (no checks). Default: auto",
    )
    group.add_argument(
        "--no-fs-checks",
        action="store_const",
        const="skip",
        dest="fs_checks",
        help="Skip btrfs subvolume verification (alias for --fs-checks=skip)",
    )


def get_fs_checks_mode(args: argparse.Namespace) -> str:
    """Get the filesystem checks mode from parsed arguments.

    Args:
        args: Parsed command line arguments

    Returns:
        One of "auto", "strict", or "skip"
    """
    return getattr(args, "fs_checks", "auto") or "auto"
