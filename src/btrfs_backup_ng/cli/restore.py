"""Restore command: Restore snapshots from backup locations.

Enables pulling snapshots from backup storage (SSH or local) back to local systems
for disaster recovery, migration, or backup verification.
"""

import argparse
import dataclasses
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .. import __util__, endpoint
from ..__logger__ import create_logger
from ..config import ConfigError, find_config_file, load_config
from ..core.target import parse_target
from ..core.restore import (
    _retry_with_inferred_prefix,
    RestoreError,
    list_remote_snapshots,
    restore_snapshots,
    validate_restore_destination,
)
from .common import (
    get_fs_checks_mode,
    get_log_level,
    resolve_timestamp_format,
    should_show_progress,
)

logger = logging.getLogger(__name__)


def execute_restore(args: argparse.Namespace) -> int:
    """Execute the restore command.

    Restores snapshots from a backup location to a local destination.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    # Handle --list-volumes mode (list configured volumes)
    if getattr(args, "list_volumes", False):
        return _execute_list_volumes(args)

    # Handle config-driven restore with --volume flag
    volume_path = getattr(args, "volume", None)
    if volume_path:
        return _execute_config_restore(args, volume_path)

    # Handle --list mode (just list available snapshots)
    if getattr(args, "list", False):
        return _execute_list(args)

    # Handle --status mode (show locks and incomplete restores)
    if getattr(args, "status", False):
        return _execute_status(args)

    # Handle --unlock mode (unlock stuck sessions)
    unlock_arg = getattr(args, "unlock", None)
    if unlock_arg is not None:
        return _execute_unlock(args, unlock_arg)

    # Handle --cleanup mode (clean up partial restores)
    if getattr(args, "cleanup", False):
        return _execute_cleanup(args)

    # Get source and destination
    source = getattr(args, "source", None)
    destination = getattr(args, "destination", None)

    if not source:
        print("Error: Source backup location required")
        print("Usage: btrfs-backup-ng restore <source> <destination>")
        print("       btrfs-backup-ng restore --list <source>")
        print("       btrfs-backup-ng restore --volume <path> --to <destination>")
        return 1

    if not destination:
        print("Error: Destination path required")
        print("Usage: btrfs-backup-ng restore <source> <destination>")
        return 1

    # Delegate to main restore logic
    args.source = source
    args.destination = destination
    return _execute_main_restore(args)


def _execute_list_volumes(args: argparse.Namespace) -> int:
    """List volumes and their backup targets from config.

    Args:
        args: Command arguments

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Load config
    config_file = getattr(args, "config", None)
    try:
        if config_file:
            config, _warnings = load_config(config_file)
            config_path = config_file
        else:
            found_path = find_config_file()
            if not found_path:
                print("Error: No configuration file found")
                print("Use --config to specify a config file, or create one at:")
                print("  ~/.config/btrfs-backup-ng/config.toml")
                print("  /etc/btrfs-backup-ng/config.toml")
                return 1
            config, _warnings = load_config(found_path)
            config_path = found_path
    except ConfigError as e:
        logger.error("Failed to load config: %s", e)
        return 1

    print(f"Configuration: {config_path}")
    print("=" * 60)
    print()

    volumes = config.get_enabled_volumes()
    if not volumes:
        print("No volumes configured")
        return 0

    for i, vol in enumerate(volumes):
        print(f"Volume {i + 1}: {vol.path}")
        print(f"  Snapshot prefix: {vol.snapshot_prefix}")
        print(f"  Snapshot dir: {vol.snapshot_dir}")

        if vol.targets:
            print("  Backup targets:")
            for j, target in enumerate(vol.targets):
                ssh_info = " (ssh_sudo)" if target.ssh_sudo else ""
                mount_info = " (require_mount)" if target.require_mount else ""
                print(f"    [{j}] {target.path}{ssh_info}{mount_info}")
        else:
            print("  No backup targets configured")
        print()

    print(f"Total: {len(volumes)} volume(s)")
    return 0


def _execute_config_restore(args: argparse.Namespace, volume_path: str) -> int:
    """Execute config-driven restore for a specific volume.

    Uses the configuration file to determine where backups are stored,
    then restores from the configured target.

    Args:
        args: Command arguments
        volume_path: Path of the volume to restore (e.g., /home)

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Load config
    config_file = getattr(args, "config", None)
    try:
        if config_file:
            config, _warnings = load_config(config_file)
        else:
            found_path = find_config_file()
            if not found_path:
                print("Error: No configuration file found")
                print("Use --config to specify a config file")
                return 1
            config, _warnings = load_config(found_path)
    except ConfigError as e:
        logger.error("Failed to load config: %s", e)
        return 1

    # Find the requested volume
    volume = None
    for vol in config.get_enabled_volumes():
        if vol.path == volume_path:
            volume = vol
            break

    if not volume:
        print(f"Error: Volume '{volume_path}' not found in configuration")
        print("Available volumes:")
        for vol in config.get_enabled_volumes():
            print(f"  {vol.path}")
        return 1

    # Check volume has targets
    if not volume.targets:
        print(f"Error: Volume '{volume_path}' has no backup targets configured")
        return 1

    # Get target index
    target_idx = getattr(args, "target", None) or 0
    if target_idx < 0 or target_idx >= len(volume.targets):
        print(f"Error: Invalid target index {target_idx}")
        print(f"Volume '{volume_path}' has {len(volume.targets)} target(s):")
        for j, t in enumerate(volume.targets):
            print(f"  [{j}] {t.path}")
        return 1

    target = volume.targets[target_idx]
    source = target.path

    # Get destination
    destination = getattr(args, "to", None) or getattr(args, "destination", None)

    # Handle --list mode for config-driven restore
    if getattr(args, "list", False):
        # Set source and call list
        args.source = source
        # Apply target's SSH settings
        if target.ssh_sudo:
            args.ssh_sudo = True
        if target.ssh_key:
            args.ssh_key = target.ssh_key
        if not getattr(args, "ssh_host_key_policy", None):
            args.ssh_host_key_policy = target.ssh_host_key_policy
        # Use volume's snapshot prefix
        if volume.snapshot_prefix and not getattr(args, "prefix", None):
            args.prefix = volume.snapshot_prefix
        return _execute_list(args)

    # For actual restore, need destination
    if not destination:
        print("Error: Destination required for restore")
        print(
            f"Usage: btrfs-backup-ng restore --volume {volume_path} --to <destination>"
        )
        return 1

    # Build effective arguments
    print("Config-driven restore:")
    print(f"  Volume: {volume_path}")
    print(f"  Source: {source}")
    print(f"  Destination: {destination}")
    print()

    # Update args with config values
    args.source = source
    args.destination = destination

    # Apply target's settings if not overridden on CLI
    if target.ssh_sudo and not getattr(args, "ssh_sudo", False):
        args.ssh_sudo = True
    if target.ssh_key and not getattr(args, "ssh_key", None):
        args.ssh_key = target.ssh_key
    if not getattr(args, "ssh_host_key_policy", None):
        args.ssh_host_key_policy = target.ssh_host_key_policy
    if target.compress != "none" and not getattr(args, "compress", None):
        args.compress = target.compress
    if target.rate_limit and not getattr(args, "rate_limit", None):
        args.rate_limit = target.rate_limit

    # Use volume's snapshot prefix if not overridden
    if volume.snapshot_prefix and not getattr(args, "prefix", None):
        args.prefix = volume.snapshot_prefix

    # Now call the main restore logic (fall through to execute_restore's main path)
    # We need to reprocess since we've set up the args
    return _execute_main_restore(args)


def _execute_main_restore(args: argparse.Namespace) -> int:
    """Execute the main restore logic after arguments are resolved.

    This is called both from direct CLI usage and config-driven restore.

    Args:
        args: Parsed arguments with source and destination set

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    source = args.source
    destination = args.destination

    # Validate destination
    dest_path = Path(destination).resolve()
    in_place = getattr(args, "in_place", False)
    force = getattr(args, "yes_i_know_what_i_am_doing", False)

    try:
        validate_restore_destination(dest_path, in_place=in_place, force=force)
    except RestoreError as e:
        logger.error("Destination validation failed: %s", e)
        return 1

    # Prepare backup endpoint (source)
    try:
        backup_endpoint = _prepare_backup_endpoint(args, source)
    except Exception as e:
        logger.error("Failed to prepare backup endpoint: %s", e)
        return 1

    # Prepare local endpoint (destination). Resolve the SAME timestamp_format AND the
    # SAME snapshot prefix the backup (source) endpoint uses, so already-restored
    # snapshots are recognized by name (skip-existing + incremental-base detection). An
    # empty prefix here made list_snapshots fail to parse prefixed names like
    # "home-20240101-..." -> the chain never saw the local parent and re-restored it.
    try:
        local_endpoint = _prepare_local_endpoint(
            dest_path,
            resolve_timestamp_format(getattr(args, "timestamp_format", None)),
            snap_prefix=getattr(args, "prefix", "") or "",
        )
    except Exception as e:
        logger.error("Failed to prepare local endpoint: %s", e)
        return 1

    # Parse time if --before specified
    before_time = None
    before_str = getattr(args, "before", None)
    if before_str:
        try:
            before_time = _parse_datetime(before_str)
            logger.info(
                "Restoring snapshot before: %s",
                time.strftime("%Y-%m-%d %H:%M:%S", before_time),
            )
        except ValueError as e:
            logger.error("Invalid date format: %s", e)
            return 1

    # Get options
    dry_run = getattr(args, "dry_run", False)
    snapshot_name = getattr(args, "snapshot", None)
    restore_all = getattr(args, "all", False)
    skip_existing = not getattr(args, "overwrite", False)
    no_incremental = getattr(args, "no_incremental", False)
    interactive = getattr(args, "interactive", False)

    # Interactive mode
    if interactive:
        snapshot_name = _interactive_select(backup_endpoint)
        if snapshot_name is None:
            logger.info("No snapshot selected, aborting")
            return 0

    # Build transfer options
    show_progress = should_show_progress(args)
    options = {
        "compress": getattr(args, "compress", None) or "none",
        "rate_limit": getattr(args, "rate_limit", None),
        "show_progress": show_progress,
    }

    # Execute restore
    logger.info(__util__.log_heading(f"Restore started at {time.ctime()}"))
    logger.info("Source: %s", source)
    logger.info("Destination: %s", dest_path)

    try:
        stats = restore_snapshots(
            backup_endpoint=backup_endpoint,
            local_endpoint=local_endpoint,
            snapshot_name=snapshot_name,
            before_time=before_time,
            restore_all=restore_all,
            skip_existing=skip_existing,
            no_incremental=no_incremental,
            options=options,
            dry_run=dry_run,
        )
    except RestoreError as e:
        logger.error("Restore failed: %s", e)
        return 1
    except Exception as e:
        logger.error("Unexpected error during restore: %s", e)
        logger.debug("Exception details:", exc_info=True)
        return 1

    logger.info(__util__.log_heading(f"Restore finished at {time.ctime()}"))

    # Return appropriate exit code
    if stats["failed"] > 0:
        return 1
    return 0


def _execute_list(args: argparse.Namespace) -> int:
    """List available snapshots at backup location."""
    source = getattr(args, "source", None)
    if not source:
        print("Error: Source backup location required")
        print("Usage: btrfs-backup-ng restore --list <source>")
        return 1

    try:
        backup_endpoint = _prepare_backup_endpoint(args, source)
    except Exception as e:
        logger.error("Failed to prepare backup endpoint: %s", e)
        return 1

    try:
        snapshots = list_remote_snapshots(backup_endpoint)
    except Exception as e:
        logger.error("Failed to list snapshots: %s", e)
        return 1

    if not snapshots:
        # Do for `--list` what `restore` already does: work out the prefix this
        # location actually uses and show those snapshots, rather than telling
        # the operator to re-run with an argument we just derived ourselves.
        #
        # The most ordinary command there is -- `restore --list <dest>` with no
        # --prefix -- otherwise answered "No snapshots matched" for a location
        # holding a perfectly good backup, which reads as data loss.
        #
        # Same primitive as the restore path, so the rules cannot diverge: it
        # infers only when NO prefix was asked for, and refuses to choose when
        # the location holds more than one (two prefixes usually means two
        # volumes side by side, and listing the wrong one is worse than asking).
        inferred = _retry_with_inferred_prefix(backup_endpoint)
        if inferred:
            used = backup_endpoint.config.get("snap_prefix", "") or ""
            print(
                f"No snapshots use the default prefix here; listing {used!r} "
                f"instead, which is what this location uses."
            )
            print("")
            snapshots = inferred

    if not snapshots:
        # "No snapshots found" is only honest when the location really is empty.
        # A prefix that matched nothing produces the identical message, and an
        # operator reading it during a restore concludes their backups are gone.
        explanation = None
        try:
            explanation = backup_endpoint.describe_empty_listing()
        except Exception as e:  # noqa: BLE001 - never fail because a hint failed
            logger.debug("Could not build an empty-listing explanation: %s", e)
        if explanation:
            print(f"No snapshots matched at {source}.")
            print("")
            print(explanation)
            return 1
        print("No snapshots found at backup location")
        return 0

    print(f"Available snapshots at {source}:")
    print("")

    # Format nicely
    for i, snap in enumerate(snapshots, 1):
        name = snap.get_name()
        # Try to format the timestamp nicely
        if hasattr(snap, "time_obj") and snap.time_obj:
            time_str = time.strftime("%Y-%m-%d %H:%M:%S", snap.time_obj)
        else:
            time_str = "unknown"
        print(f"  {i:3}. {name:<40} ({time_str})")

    print("")
    print(f"Total: {len(snapshots)} snapshot(s)")

    return 0


def _configured_target_options(args: argparse.Namespace, source: str) -> dict:
    """SSH/decryption options configured for ``source`` in the user's config.

    A restore names its source as a URI on the command line, so nothing tied it
    back to the target entry that produced those backups -- the options lived in
    the config file and only ``--ssh-*`` flags were consulted. The result was a
    backup that wrote successfully for months and then could not be READ:

        $ btrfs-backup-ng -c config.toml restore --list raw+ssh://host:/backups
        ERROR  Failed to create remote directory: ...

    while the identical target backed up fine, because `run` reads the config and
    `restore` did not. Passing --ssh-key by hand made it work, which is the whole
    diagnosis. Measured on a real host.

    Matching is on the target path as written, after stripping a trailing slash.
    Returns {} when there is no config, no match, or anything goes wrong -- a
    convenience lookup must never be the reason a restore cannot start.
    """
    try:
        config_file = find_config_file(getattr(args, "config", None))
        if config_file is None:
            return {}
        config, _warnings = load_config(config_file)
    except Exception as e:  # noqa: BLE001 - a lookup failure must not block a restore
        logger.debug("Could not read config for target options: %s", e)
        return {}

    wanted = source.rstrip("/")
    for volume in getattr(config, "volumes", []) or []:
        for target in getattr(volume, "targets", []) or []:
            if str(getattr(target, "path", "")).rstrip("/") != wanted:
                continue
            defaults = {
                field.name: field.default
                for field in dataclasses.fields(target)
                if field.default is not dataclasses.MISSING
            }
            found = {}
            for key in (
                "ssh_key",
                "ssh_sudo",
                "ssh_port",
                "ssh_auth_sock",
                "ssh_host_key_policy",
                "ssh_password_auth",
                "gpg_keyring",
                "openssl_cipher",
            ):
                value = getattr(target, key, None)
                # Only unset values are skipped here. False is a SETTING, not an
                # absence: ssh_password_auth defaults to True, so `false` -- the
                # only useful thing to write -- was dropped as "unset" on the way
                # in and dropped again below as "same as default" if it were
                # true. The option could not be returned under any value it
                # could hold. Booleans that merely restate the schema default are
                # still skipped by the check that follows.
                if value is None or value == "":
                    continue
                # Only what this target actually says. Schema defaults are not
                # choices the user made, and reporting them as "configured" turns
                # a local restore into a claim about SSH settings it never had.
                if key in defaults and value == defaults[key]:
                    continue
                found[key] = value
            if found:
                logger.info(
                    "Using the options configured for this target in %s: %s",
                    config_file,
                    ", ".join(sorted(found)),
                )
            return found
    return {}


def _prepare_backup_endpoint(args: argparse.Namespace, source: str):
    """Prepare the backup endpoint for restore.

    Args:
        args: Command arguments
        source: Source path (local or ssh://)

    Returns:
        Configured endpoint
    """
    # Build endpoint kwargs. Thread timestamp_format so custom-named snapshots
    # are parsed rather than silently skipped when listing/matching backups.
    endpoint_kwargs = {
        "snap_prefix": getattr(args, "prefix", "") or "",
        "convert_rw": False,
        "subvolume_sync": False,
        "btrfs_debug": False,
        "fs_checks": get_fs_checks_mode(args),
        "timestamp_format": resolve_timestamp_format(
            getattr(args, "timestamp_format", None)
        ),
    }

    # SSH options. Dispatch on the scheme, not on a "ssh://" prefix: raw+ssh://
    # does not start with it, so a remote raw source used to be given none of the
    # --ssh-* options and its remote listing ran as whoever ssh logged in as.
    scheme = parse_target(source)
    # What the config says about THIS target. Command-line flags still win; this
    # only fills what was not given, so a restore works with the same settings
    # that produced the backup instead of demanding they be retyped.
    configured = _configured_target_options(args, source)

    if scheme.needs_ssh_options:
        endpoint_kwargs["ssh_sudo"] = getattr(args, "ssh_sudo", False) or bool(
            configured.get("ssh_sudo", False)
        )
        # `port`, not `ssh_port`: that is the key choose_endpoint and the
        # endpoints read. Threaded under the config's own spelling it was
        # dropped by the endpoint's key whitelist, so a restore against a
        # non-standard port quietly used 22 -- while the log said the target's
        # ssh_port had been applied. cli/common.py already maps it this way for
        # the backup path, and records the same bug being fixed there before.
        _port = configured.get("ssh_port")
        if _port:
            endpoint_kwargs["port"] = _port
        _hkp = getattr(args, "ssh_host_key_policy", None) or configured.get(
            "ssh_host_key_policy"
        )
        if _hkp:
            endpoint_kwargs["ssh_host_key_policy"] = _hkp
        # There is no --ssh-password-auth flag, so the args lookup is a constant
        # True unless a caller sets the attribute directly; reading only it meant
        # a configured `ssh_password_auth = false` had no effect on a restore at
        # all, while the same setting was honoured for backups. Either side may
        # disable the fallback and neither can re-enable what the other refused.
        endpoint_kwargs["ssh_password_fallback"] = bool(
            getattr(args, "ssh_password_auth", True)
        ) and bool(configured.get("ssh_password_auth", True))
        ssh_key = getattr(args, "ssh_key", None) or configured.get("ssh_key")
        if ssh_key:
            # The two remote endpoints read the identity file under DIFFERENT
            # config keys: SSHEndpoint accepts either name, SSHRawEndpoint reads
            # only "ssh_key". Threading one name for both silently drops
            # --ssh-key for raw+ssh://, producing an ssh invocation with no -i
            # flag and no error.
            endpoint_kwargs[scheme.ssh_identity_config_key or "ssh_identity_file"] = (
                ssh_key
            )
        ssh_auth_sock = getattr(args, "ssh_auth_sock", None) or configured.get(
            "ssh_auth_sock"
        )
        if ssh_auth_sock:
            endpoint_kwargs["ssh_auth_sock"] = ssh_auth_sock
    elif not scheme.is_raw:
        # For local paths, we need to set 'path' as well since LocalEndpoint
        # always resolves config["path"] during initialization. A raw:// source
        # is excluded: choose_endpoint parses the path out of the URI itself, and
        # Path("raw:///x").resolve() would resolve against the working directory.
        endpoint_kwargs["path"] = Path(source).resolve()

    # Decryption settings for an encrypted raw backup. Needed for both raw://
    # (local) and raw+ssh:// sources -- neither starts with "ssh://", so thread
    # them here (outside the ssh branch) and only when the operator supplied one
    # so the sidecar-recorded cipher stays authoritative; the endpoint value is
    # the fallback for a legacy sidecar. Harmless for non-raw endpoints (dropped
    # by the base config whitelist).
    gpg_keyring = getattr(args, "gpg_keyring", None) or configured.get("gpg_keyring")
    if gpg_keyring:
        endpoint_kwargs["gpg_keyring"] = gpg_keyring
    openssl_cipher = getattr(args, "openssl_cipher", None) or configured.get(
        "openssl_cipher"
    )
    if openssl_cipher:
        endpoint_kwargs["openssl_cipher"] = openssl_cipher

    # Create endpoint - for restore, backup location needs to be set as "path"
    # (not "source") because list_snapshots() uses config["path"]
    # The source=False means the path will be stored in config["path"]
    backup_ep = endpoint.choose_endpoint(
        source,
        endpoint_kwargs,
        source=False,
    )
    # Restore-time integrity check for raw targets: default ON, disabled by
    # --skip-verify (for last-copy recovery of a partially-corrupt backup).
    backup_ep.config["verify_before_restore"] = not getattr(args, "skip_verify", False)
    backup_ep.prepare()

    return backup_ep


def _prepare_local_endpoint(
    dest_path: Path,
    timestamp_format: str | None = None,
    snap_prefix: str = "",
):
    """Prepare the local endpoint for receiving restored snapshots.

    Args:
        dest_path: Local destination path
        timestamp_format: Format for parsing names of snapshots already at the
            destination, so skip-existing and incremental-base detection see
            custom-named snapshots instead of silently dropping them. Must match
            the format the backup (source) endpoint uses.
        snap_prefix: Snapshot name prefix the backup uses (e.g. ``home-``). The local
            endpoint must parse already-restored subvolumes under the SAME prefix, or it
            fails to recognize them and the restore chain re-restores an existing parent.

    Returns:
        Configured local endpoint
    """
    from ..endpoint.local import LocalEndpoint

    # Ensure directory exists
    dest_path.mkdir(parents=True, exist_ok=True)

    endpoint_kwargs = {
        "path": dest_path,
        "source": None,  # This is the destination for receive
        "snap_prefix": snap_prefix,
        "convert_rw": False,
        "subvolume_sync": False,
        "btrfs_debug": False,
        "fs_checks": "auto",
        "timestamp_format": timestamp_format,
    }

    local_ep = LocalEndpoint(config=endpoint_kwargs)
    local_ep.prepare()

    return local_ep


def _parse_datetime(dt_str: str) -> time.struct_time:
    """Parse a datetime string to struct_time.

    Supports formats:
        - 2026-01-04
        - 2026-01-04 12:00
        - 2026-01-04 12:00:00
        - 2026-01-04T12:00:00

    Args:
        dt_str: Datetime string

    Returns:
        time.struct_time

    Raises:
        ValueError: If format is not recognized
    """
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(dt_str, fmt)
            return dt.timetuple()
        except ValueError:
            continue

    raise ValueError(
        f"Could not parse date '{dt_str}'. "
        f"Use format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"
    )


def _interactive_select(backup_endpoint) -> str | None:
    """Interactively select a snapshot to restore.

    Args:
        backup_endpoint: Endpoint to list snapshots from

    Returns:
        Selected snapshot name, or None if cancelled
    """
    try:
        snapshots = list_remote_snapshots(backup_endpoint)
    except Exception as e:
        logger.error("Failed to list snapshots: %s", e)
        return None

    if not snapshots:
        print("No snapshots available")
        return None

    print("")
    print("Available snapshots:")
    print("")

    for i, snap in enumerate(snapshots, 1):
        name = snap.get_name()
        if hasattr(snap, "time_obj") and snap.time_obj:
            time_str = time.strftime("%Y-%m-%d %H:%M:%S", snap.time_obj)
        else:
            time_str = "unknown"
        print(f"  {i:3}. {name:<40} ({time_str})")

    print("")
    print("  0. Cancel")
    print("")

    while True:
        try:
            choice = input("Select snapshot to restore [0]: ").strip()
            if not choice or choice == "0":
                return None

            idx = int(choice) - 1
            if 0 <= idx < len(snapshots):
                selected = snapshots[idx]
                print(f"\nSelected: {selected.get_name()}")
                confirm = input("Proceed with restore? [y/N]: ").strip().lower()
                if confirm in ("y", "yes"):
                    return selected.get_name()
                else:
                    print("Cancelled")
                    return None
            else:
                print(f"Invalid selection. Enter 1-{len(snapshots)} or 0 to cancel.")

        except ValueError:
            print("Invalid input. Enter a number.")
        except (EOFError, KeyboardInterrupt):
            print("\nCancelled")
            return None


def _execute_status(args: argparse.Namespace) -> int:
    """Show status of locks and incomplete restores at backup location.

    Displays:
    - Active locks on snapshots (from restore or backup operations)
    - Any partial/incomplete snapshots detected

    Args:
        args: Command arguments

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    source = getattr(args, "source", None)
    if not source:
        print("Error: Source backup location required")
        print("Usage: btrfs-backup-ng restore --status <source>")
        return 1

    try:
        backup_endpoint = _prepare_backup_endpoint(args, source)
    except Exception as e:
        logger.error("Failed to prepare backup endpoint: %s", e)
        return 1

    print(f"Restore status for {source}")
    print("=" * 60)
    print()

    # Endpoints whose set_lock is in-memory only never write a lock file, so
    # there is nothing here to inspect. Reporting "no active locks" would be
    # technically true and actively misleading: it reads as "we looked, and the
    # target is clean" when nothing was ever recorded to look at.
    if not backup_endpoint.persists_locks:
        print("This target does not persist locks.")
        print()
        print("Locks on ssh://, raw:// and raw+ssh:// targets are held in memory for")
        print("the duration of a single run, so no lock file is written. An")
        print("interrupted run leaves nothing behind here to inspect or unlock.")
        return 0

    # Read through the endpoint rather than rebuilding the path here. The
    # endpoint already coerces config['path'] to a Path -- SSH endpoints keep it
    # a str -- and this duplicate build did not, so it raised
    # "unsupported operand type(s) for /: 'str' and 'str'", swallowed it, and
    # printed "No active locks found." for a target it had never opened.
    try:
        locks = backup_endpoint._read_locks()
    except Exception as e:
        # AbortError carries no message of its own -- the specific cause was
        # already logged by _read_locks -- so name the type rather than printing
        # a sentence that trails off after a colon.
        detail = str(e) or f"{type(e).__name__}; see the error logged above"
        print(f"Error: the lock state at {source} could not be read ({detail}).")
        print()
        print("This is NOT a report of zero locks. Nothing was read, so nothing is")
        print("known about what is locked; resolve the error above before treating")
        print("this target as unlocked.")
        return 1

    # Display locks
    if locks:
        print("Active Locks:")
        print("-" * 40)
        restore_locks: dict[str, Any] = {}
        other_locks: dict[str, Any] = {}

        for snap_name, lock_info in locks.items():
            snap_locks = lock_info.get("locks", [])
            parent_locks = lock_info.get("parent_locks", [])
            all_locks = snap_locks + parent_locks

            for lock_id in all_locks:
                if lock_id.startswith("restore:"):
                    if snap_name not in restore_locks:
                        restore_locks[snap_name] = []
                    restore_locks[snap_name].append(lock_id)
                else:
                    if snap_name not in other_locks:
                        other_locks[snap_name] = []
                    other_locks[snap_name].append(lock_id)

        if restore_locks:
            print("\n  Restore locks (from restore operations):")
            for snap_name, lock_ids in restore_locks.items():
                for lock_id in lock_ids:
                    session_id = lock_id.replace("restore:", "")
                    print(f"    {snap_name}: session {session_id}")

        if other_locks:
            print("\n  Other locks (from backup/transfer operations):")
            for snap_name, lock_ids in other_locks.items():
                for lock_id in lock_ids:
                    print(f"    {snap_name}: {lock_id}")

        print()
        print(f"Total: {len(locks)} snapshot(s) with locks")
        print()
        print("To unlock restore sessions:")
        print("  btrfs-backup-ng restore --unlock all <source>")
        print("  btrfs-backup-ng restore --unlock <session-id> <source>")
    else:
        print("No active locks found.")
        print()

    # List snapshots for reference
    try:
        snapshots = list_remote_snapshots(backup_endpoint)
        print(f"\nAvailable snapshots: {len(snapshots)}")
    except Exception as e:
        logger.warning("Could not list snapshots: %s", e)

    return 0


def _execute_unlock(args: argparse.Namespace, lock_id: str) -> int:
    """Unlock stuck restore session(s).

    Args:
        args: Command arguments
        lock_id: Lock ID to unlock, or "all" to unlock all restore locks

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    source = getattr(args, "source", None)
    if not source:
        print("Error: Source backup location required")
        print("Usage: btrfs-backup-ng restore --unlock [LOCK_ID] <source>")
        return 1

    try:
        backup_endpoint = _prepare_backup_endpoint(args, source)
    except Exception as e:
        logger.error("Failed to prepare backup endpoint: %s", e)
        return 1

    # Endpoints whose set_lock is in-memory only never write a lock file, so
    # there is nothing here to inspect. Reporting "no active locks" would be
    # technically true and actively misleading: it reads as "we looked, and the
    # target is clean" when nothing was ever recorded to look at.
    if not backup_endpoint.persists_locks:
        print("This target does not persist locks, so there is nothing to unlock.")
        print()
        print("Locks on ssh://, raw:// and raw+ssh:// targets are held in memory for")
        print("the duration of a single run, so no lock file is written. An")
        print("interrupted run leaves nothing behind here to inspect or unlock.")
        return 0

    # Same endpoint API as --status, for the same reason.
    try:
        locks = backup_endpoint._read_locks()
    except Exception as e:
        logger.error("The lock state could not be read, so nothing was unlocked: %s", e)
        return 1

    if not locks:
        print("No locks found - nothing to unlock.")
        return 0

    # Find and remove matching locks
    unlocked_count = 0
    new_locks: dict[str, Any] = {}

    for snap_name, lock_info in locks.items():
        snap_locks = set(lock_info.get("locks", []))
        parent_locks = set(lock_info.get("parent_locks", []))

        if lock_id == "all":
            # Remove all restore locks
            restore_snap_locks = {lk for lk in snap_locks if lk.startswith("restore:")}
            restore_parent_locks = {
                lk for lk in parent_locks if lk.startswith("restore:")
            }
            unlocked_count += len(restore_snap_locks) + len(restore_parent_locks)
            snap_locks -= restore_snap_locks
            parent_locks -= restore_parent_locks
        else:
            # Remove specific lock
            full_lock_id = (
                f"restore:{lock_id}" if not lock_id.startswith("restore:") else lock_id
            )
            if full_lock_id in snap_locks:
                snap_locks.discard(full_lock_id)
                unlocked_count += 1
            if full_lock_id in parent_locks:
                parent_locks.discard(full_lock_id)
                unlocked_count += 1

        # Keep entry if it still has locks
        if snap_locks or parent_locks:
            new_entry = {}
            if snap_locks:
                new_entry["locks"] = list(snap_locks)
            if parent_locks:
                new_entry["parent_locks"] = list(parent_locks)
            new_locks[snap_name] = new_entry

    # Write through the endpoint's own writer, which does the atomic replace
    # (R7: temp -> fsync -> os.replace -> parent fsync, 0600). A torn lock file
    # is misread as "no locks" and lets retention prune a snapshot that is still
    # locked, so this must not be a plain open("w") -- and must not be a second
    # copy of the careful version either.
    try:
        backup_endpoint._write_locks(new_locks)
    except Exception as e:
        logger.error("Could not write the lock file: %s", e)
        return 1

    if unlocked_count > 0:
        print(f"Unlocked {unlocked_count} lock(s).")
        if new_locks:
            print(f"Remaining locks: {len(new_locks)} snapshot(s)")
        else:
            print("All locks cleared.")
    else:
        if lock_id == "all":
            print("No restore locks found to unlock.")
        else:
            print(f"Lock '{lock_id}' not found.")
        return 1

    return 0


def _execute_cleanup(args: argparse.Namespace) -> int:
    """Clean up partial/incomplete snapshot restores at destination.

    Scans the destination for partial subvolumes (from interrupted restores)
    and offers to delete them.

    Args:
        args: Command arguments

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # For cleanup, accept either destination or source as the path
    # (user might use either positional argument)
    destination = getattr(args, "destination", None) or getattr(args, "source", None)
    if not destination:
        print("Error: Destination path required")
        print("Usage: btrfs-backup-ng restore --cleanup <destination>")
        return 1

    dest_path = Path(destination).resolve()

    if not dest_path.exists():
        print(f"Error: Destination path does not exist: {dest_path}")
        return 1

    print(f"Scanning for partial restores in {dest_path}")
    print("=" * 60)
    print()

    # Look for partial subvolumes
    # Partial restores typically:
    # 1. Have incomplete btrfs receive (no received_uuid)
    # 2. Are empty or nearly empty
    # 3. May have .partial suffix or similar markers

    partial_subvolumes = []

    try:
        for item in dest_path.iterdir():
            if not item.is_dir():
                continue

            # Check if it's a subvolume
            if not __util__.is_subvolume(item):
                continue

            # Check for signs of incomplete restore
            is_partial = False
            reason = ""

            # Check if subvolume is empty or very small
            try:
                contents = list(item.iterdir())
                if len(contents) == 0:
                    is_partial = True
                    reason = "empty subvolume"
                elif len(contents) == 1 and contents[0].name == ".btrfs-backup-ng":
                    is_partial = True
                    reason = "only contains metadata directory"
            except PermissionError:
                pass

            # Check for .partial marker in name
            if item.name.endswith(".partial"):
                is_partial = True
                reason = "has .partial suffix"

            if is_partial:
                partial_subvolumes.append((item, reason))

    except Exception as e:
        logger.error("Error scanning destination: %s", e)
        return 1

    if not partial_subvolumes:
        print("No partial restores found.")
        return 0

    print(f"Found {len(partial_subvolumes)} potentially incomplete restore(s):\n")
    for i, (subvol, reason) in enumerate(partial_subvolumes, 1):
        print(f"  {i}. {subvol.name}")
        print(f"      Reason: {reason}")
        print()

    # Ask for confirmation
    dry_run = getattr(args, "dry_run", False)
    if dry_run:
        print("Dry run - no changes made.")
        return 0

    print("These subvolumes appear to be from incomplete restores.")
    confirm = input("Delete all partial subvolumes? [y/N]: ").strip().lower()

    if confirm not in ("y", "yes"):
        print("Cancelled.")
        return 0

    # Delete partial subvolumes
    deleted = 0
    failed = 0

    for subvol, reason in partial_subvolumes:
        try:
            logger.info("Deleting partial subvolume: %s", subvol)
            # Use btrfs subvolume delete
            import subprocess

            result = subprocess.run(
                ["btrfs", "subvolume", "delete", str(subvol)],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                print(f"  Deleted: {subvol.name}")
                deleted += 1
            else:
                logger.error("Failed to delete %s: %s", subvol, result.stderr)
                failed += 1
        except Exception as e:
            logger.error("Failed to delete %s: %s", subvol, e)
            failed += 1

    print()
    print(f"Cleanup complete: {deleted} deleted, {failed} failed")

    return 0 if failed == 0 else 1
