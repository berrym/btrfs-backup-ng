"""Shared CLI utilities and argument parsers."""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

from .. import __util__
from ..core.target import parse_target

logger = logging.getLogger(__name__)


# Filesystems that live in RAM. Naming one in require_mount is always a mistake:
# they are mounted whether or not any drive is attached, so the check can never
# fail, and a backup written to one disappears at reboot after consuming memory.
_MEMORY_BACKED_FILESYSTEMS = frozenset(
    {"tmpfs", "ramfs", "devtmpfs", "proc", "sysfs", "devpts", "cgroup", "cgroup2"}
)


def assert_target_mounted(target_path: str, require_mount: bool | str = True) -> None:
    """Enforce ``require_mount`` for a target. Raises AbortError if unsatisfied.

    THE single mount-check path. It lived in ``cli/transfer.py`` while ``cli/run.py``
    carried two inline copies of its own, and the copies disagreed with it: they
    tested the scheme with ``path.startswith(("ssh://", "raw://", "raw+ssh://"))``,
    which put ``raw://`` on the exempt list. So ``run`` -- the primary command --
    silently skipped the check for a raw target, and a user who set
    ``require_mount = true`` on an unmounted USB raw target got no protection at
    all while being given no indication of it.

    Separate from any command so it can be exercised directly: the inline version
    sat behind a real btrfs source with real snapshots, so nothing tested it and it
    was wrong in both directions.

    Remote targets are exempt -- the README scopes require_mount to local ones, and
    a local mount table says nothing about a remote filesystem.

    ``require_mount`` may be a bool or a mount point:

    ``True``
        the target itself must be a mount point. Unchanged, and unusable when
        several machines back up into subdirectories of one drive -- with the
        drive at /mnt/backup, a target of /mnt/backup/box1 can never satisfy it,
        because is_mounted compares for equality.

    a path
        THAT path must be a mount point, and the target must live at or under it.

    Both halves of the string form are required. Asserting only that
    /mnt/backup is mounted, while writing to /somewhere/else, would pass a check
    that protects nothing -- the point is to prevent a write landing on the root
    filesystem when a specific drive is absent, so the drive being checked has to
    be the drive being written to.
    """
    if not require_mount:
        return

    scheme = parse_target(target_path)
    if scheme.is_remote:
        return

    # Fail CLOSED. Gating on supports_mount_check alone let an unclassifiable
    # target skip the guard entirely -- `raw://mnt/usb/backups` is UNSUPPORTED,
    # yet choose_endpoint happily builds an endpoint writing to /usb/backups. The
    # old code refused it (for the wrong reason); skipping the check would write
    # the backup to the root filesystem and then prune the source as though it
    # had succeeded, which is the exact accident require_mount exists to prevent.
    # supports_mount_check is true only for LOCAL and RAW, and neither can carry
    # an empty path, so this covers the Path("") -> Path(".") hazard as well.
    if not scheme.supports_mount_check:
        raise __util__.AbortError(
            f"Target {target_path} requires a mount check but its path cannot "
            f"be determined" + (f": {scheme.reason}" if scheme.reason else ".")
        )

    # The filesystem path, not the URI. This branch also covers raw:// now: an
    # unmounted USB raw target is exactly the case require_mount exists for, and
    # Path("raw:///mnt/usb") could never be a mount point, so the check aborted
    # every raw transfer instead of guarding it.
    resolved = Path(scheme.path).resolve()

    if isinstance(require_mount, str):
        expected = Path(require_mount).resolve()

        # Vacuity is decided HERE, after resolve(), because that is where the
        # value acquires its meaning. The loader also refuses this, but only the
        # gate sees the resolved path: "/.", "/..", "/mnt/.." and any number of
        # other spellings all resolve to "/", and a lexical check on the raw
        # string lets every one of them through. Root is always mounted and every
        # path is under it, so such a value passes unconditionally -- a check that
        # reports success while protecting nothing, which is the same failure the
        # containment test below refuses from the other direction.
        #
        # Checking here also covers a TargetConfig built in code, which never
        # passes through the loader at all.
        if expected == Path("/"):
            raise __util__.AbortError(
                f"require_mount = {require_mount!r} resolves to / , which would "
                f"always pass: the root filesystem is always mounted and every "
                f"target is under it, so the check would protect nothing. Name "
                f"the mount point the target actually lives under, or use true to "
                f"require the target itself to be a mount point."
            )

        if not resolved.is_relative_to(expected):
            raise __util__.AbortError(
                f"Target {target_path} is not inside {require_mount}, which "
                f"require_mount says must be mounted. As written the check would "
                f"confirm a drive that this target is not written to, so it would "
                f"report success while protecting nothing. Point require_mount at "
                f"the mount the target lives under, or set it to true to require "
                f"the target itself to be a mount point."
            )
        if not __util__.is_mounted(expected):
            raise __util__.AbortError(
                f"{require_mount} is not mounted, so {target_path} cannot be "
                f"written to the drive it names. Ensure the drive is connected "
                f"and mounted, or set require_mount = false."
            )

        # A memory-backed filesystem is never the drive an operator means, and
        # naming one produces a check that always passes. This is not academic:
        # /run is tmpfs and always mounted, and udisks2 mounts removable drives
        # under /run/media/<user>/<label> -- so require_mount = "/run" confirms a
        # filesystem that is present precisely when the drive is ABSENT, and the
        # backup is written into RAM. The containment test cannot catch it,
        # because the target genuinely is under /run.
        info = __util__.get_mount_info(expected)
        fs_type = (info or {}).get("fs_type", "")
        if fs_type in _MEMORY_BACKED_FILESYSTEMS:
            raise __util__.AbortError(
                f"require_mount names {require_mount}, which is a {fs_type} "
                f"filesystem held in memory, not the drive you mean. It is always "
                f"mounted, so the check would always pass -- and with the drive "
                f"absent the backup would be written into RAM. Name the mount "
                f"point of the drive itself, such as "
                f"/run/media/<user>/<volume-label>."
            )

        logger.debug("Mount check passed for %s (under %s)", target_path, expected)
        return

    if not __util__.is_mounted(resolved):
        raise __util__.AbortError(
            f"Target {target_path} is not mounted. "
            f"Ensure the drive is connected and mounted, or set require_mount = false."
        )
    logger.debug("Mount check passed for %s", target_path)


def space_options_from_args(args: argparse.Namespace) -> dict[str, Any]:
    """Space-check transfer options derived from the parsed CLI flags.

    Without threading these, ``--no-check-space``/``--force``/``--safety-margin`` are
    dead flags: ``send_snapshot`` defaults ``check_space=True`` and ``force=False``, so
    the destination space preflight always runs and can never be bypassed -- which bites
    hardest on raw targets, whose size estimate is conservative and can refuse a
    transfer that would actually fit. Merge the result into the transfer ``options``
    dict so the flags take effect (the default -- no flags -- reproduces today's
    always-check behavior)."""
    opts: dict[str, Any] = {
        "check_space": not getattr(args, "no_check_space", False),
        "force": getattr(args, "force", False),
    }
    margin = getattr(args, "safety_margin", None)
    if margin is not None:
        opts["safety_margin"] = margin
    return opts


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


def add_ssh_hostkey_arg(parser: argparse.ArgumentParser) -> None:
    """Add the ``--ssh-host-key-policy`` argument to an ssh-capable parser (R12b).

    ``default=None`` so that an unset flag never overrides a config-file setting -- the
    handler only threads it into endpoint kwargs when the operator passed it explicitly.
    """
    parser.add_argument(
        "--ssh-host-key-policy",
        choices=["accept-new", "strict"],
        default=None,
        help="SSH host-key verification for this run: 'accept-new' (trust first contact, "
        "reject a changed key) or 'strict' (known_hosts-only, reject an unknown host). "
        "Overrides the target config.",
    )


def add_remote_lock_arg(parser: argparse.ArgumentParser) -> None:
    """Add ``--skip-remote-lock`` to a parser that touches a remote target.

    Locking is the default and a target that cannot record a lock stops the
    operation, because continuing would run it unprotected while a prune
    elsewhere is free to delete what is being read.

    That default is wrong for one real setup: a destination the operator can
    read but not write, where no lock can be recorded and none is needed
    because nothing here will be deleting either. This flag is how that
    operator says so explicitly. It is deliberately not a config-file default
    for a whole site -- the risk is per-run and should be visible in the
    command that takes it.

    ``default=None`` so an unset flag never overrides a target's own setting.
    """
    parser.add_argument(
        "--skip-remote-lock",
        action="store_true",
        default=None,
        help="Proceed even if a lock cannot be recorded on the remote target. "
        "Only safe when nothing else can prune this target during the run.",
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


def thread_ssh_target_config(kwargs: dict, target) -> None:
    """Copy a target's SSH connection settings into an endpoint config dict.

    The single source of truth for turning a ``TargetConfig``'s ``ssh_*`` fields
    into the keys ``choose_endpoint`` / the endpoints consume. Previously each CLI
    command (run, transfer, prune, status, list, estimate) threaded these inline
    and INCONSISTENTLY:

    * ``ssh_port`` was dropped everywhere -- a non-default ``ssh_port`` in the
      config never reached ``ssh -p`` (the endpoint fell back to 22), so a target
      on a non-standard port silently failed to connect.
    * ``prune``/``status``/``list`` set only sudo/host-key-policy/password-fallback
      and dropped ``ssh_key``/``ssh_auth_sock`` entirely, so those read-only remote
      operations could not authenticate with a configured key file.
    * The identity file must be threaded under BOTH names: the btrfs ``SSHEndpoint``
      reads ``ssh_identity_file`` while the ``SSHRawEndpoint`` (raw+ssh) reads
      ``ssh_key``. Some sites set only one, so a raw+ssh target with a configured
      key silently used the default identity.

    Threading the full set here, once, fixes all three classes at every call site.
    ``port`` is always set; ``choose_endpoint`` gives a URL-embedded port
    (``ssh://host:2222/``) precedence over it, so this is the target-level default,
    not an override. Harmless for local targets (dropped by the base config
    whitelist).
    """
    kwargs["port"] = getattr(target, "ssh_port", 22)
    kwargs["ssh_sudo"] = getattr(target, "ssh_sudo", False)
    kwargs["skip_remote_lock"] = getattr(target, "skip_remote_lock", False)
    kwargs["ssh_host_key_policy"] = getattr(target, "ssh_host_key_policy", "accept-new")
    kwargs["ssh_password_fallback"] = getattr(target, "ssh_password_auth", True)
    ssh_key = getattr(target, "ssh_key", None)
    if ssh_key:
        # SSHEndpoint reads ssh_identity_file; SSHRawEndpoint reads ssh_key.
        kwargs["ssh_identity_file"] = ssh_key
        kwargs["ssh_key"] = ssh_key
    ssh_auth_sock = getattr(target, "ssh_auth_sock", None)
    if ssh_auth_sock:
        kwargs["ssh_auth_sock"] = ssh_auth_sock


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
