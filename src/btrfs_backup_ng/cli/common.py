"""Shared CLI utilities and argument parsers."""

import argparse


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
