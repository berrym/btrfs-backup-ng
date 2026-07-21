"""Shared CLI utilities and argument parsers."""

import argparse
import sys

from .. import __util__


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


def get_timestamp_format(config=None) -> str:
    """Return the configured snapshot ``timestamp_format`` or the built-in default.

    Endpoints consume this through their config dict so snapshot naming and
    parsing honor the user's ``[global] timestamp_format``. When no config (or
    no global section) is available, the built-in ``DATE_FORMAT`` is used.

    Args:
        config: A loaded ``Config`` object, or ``None``.

    Returns:
        A strftime format string.
    """
    global_config = getattr(config, "global_config", None)
    fmt = getattr(global_config, "timestamp_format", None)
    return fmt or __util__.DATE_FORMAT


def resolve_timestamp_format(explicit: str | None = None) -> str:
    """Resolve the snapshot timestamp_format for direct-mode commands.

    ``verify`` and ``restore`` operate on a location argument and may have no
    config object, so without this they parse snapshot names with only the
    default format and silently skip custom-named snapshots. An explicit
    ``--timestamp-format`` wins; otherwise honor ``[global] timestamp_format``
    from a discoverable config; else the built-in default.
    """
    if explicit:
        return explicit
    try:
        from ..config import find_config_file, load_config

        path = find_config_file(None)
        if path is not None:
            config, _ = load_config(path)
            return get_timestamp_format(config)
    except Exception:
        pass
    return __util__.DATE_FORMAT


def thread_raw_encryption(kwargs: dict, target) -> None:
    """Copy a target's raw-encryption settings into an endpoint config dict.

    Ensures ``choose_endpoint`` can pass encrypt/gpg_recipient/gpg_keyring/
    openssl_cipher to a raw endpoint. Without this the fields are dropped and a
    raw target configured for encryption writes plaintext. The values are passed
    to ``choose_endpoint`` for every target but only applied when building a raw
    endpoint (non-raw endpoints drop them via the base config whitelist), so this
    is harmless for ssh/local btrfs targets. Pair with
    ``endpoint.assert_encryption_applied`` after building the endpoint for a
    fail-closed guarantee.
    """
    kwargs["encrypt"] = getattr(target, "encrypt", "none")
    kwargs["gpg_recipient"] = getattr(target, "gpg_recipient", None)
    kwargs["gpg_keyring"] = getattr(target, "gpg_keyring", None)
    kwargs["openssl_cipher"] = getattr(target, "openssl_cipher", None)


def thread_raw_compression(kwargs: dict, target, override: str | None = None) -> None:
    """Thread the EFFECTIVE compression into an endpoint config dict.

    The mirror of ``thread_raw_encryption`` for compression. Without this, a raw
    target's ``compress`` is dropped from the endpoint config and instead applied
    by the generic *transfer layer* -- which is invisible to the raw ``.meta``
    sidecar, so the sidecar records ``compress: null`` while the stream is actually
    compressed. Restore then does not decompress and the backup is UNRESTORABLE.
    Threading it here makes the raw endpoint own compression (in its own
    ``send|compress|encrypt>file`` pipeline) and RECORD it in the sidecar, so
    restore can reverse it. ``send_snapshot`` separately suppresses the
    transfer-layer stage for raw destinations so the stream is never
    double-compressed.

    The effective value is the CLI ``--compress`` ``override`` if given, else the
    target's configured ``compress`` -- the SAME expression the transfer options
    use -- so a ``--compress`` override actually compresses a raw target (not just
    non-raw ones) instead of being silently dropped. Harmless for non-raw targets
    (dropped by the base config whitelist). After threading, feed the resulting
    ``kwargs["compress"]`` to ``endpoint.assert_compression_applied`` so the guard
    checks the effective value that was requested, not a possibly-different config
    value (otherwise the guard is tautological and cannot catch a dropped override).
    """
    kwargs["compress"] = override or getattr(target, "compress", None)
