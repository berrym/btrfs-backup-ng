# pyright: standard

"""Legacy CLI implementation for btrfs-backup-ng.

This module contains the original command-line interface that uses
positional arguments (source, destinations) rather than subcommands.
It is preserved for backwards compatibility.

Copyright (c) 2024 Michael Berry <trismegustis@gmail.com>
Copyright (c) 2017 Robert Schindler <r.schindler@efficiosoft.com>
Copyright (c) 2014 Chris Lawrence <lawrencc@debian.org>

MIT License - See LICENSE file for details.
"""

import argparse
import logging
import os
import pwd
import sys
import time
import traceback
from pathlib import Path

from . import __util__, __version__, endpoint
from .__logger__ import create_logger, logger
from .core.operations import send_snapshot, sync_snapshots
from .core.planning import clear_locks, delete_corrupt_snapshots, plan_transfers


def parse_options(global_parser, argv):
    """Parse legacy command line arguments."""
    description = """\
This provides incremental backups for btrfs filesystems. It can be
used for taking regular backups of any btrfs subvolume and syncing them
with local and/or remote locations. Multiple targets are supported as
well as retention settings for both source snapshots and backups. If
a snapshot transfer fails for any reason (e.g. due to network outage),
btrfs-backup-ng will notice it and prevent the snapshot from being deleted
until it finally makes it over to its destination."""

    epilog = """\
You may also pass one or more file names prefixed with '@' at the
command line. Arguments are then read from these files, treating each
line as a flag or '--arg value'-style pair you would normally
pass directly. Note that you must not escape whitespaces (or anything
else) within argument values. Lines starting with '#' are treated
as comments and silently ignored. Blank lines and indentation are allowed
and have no effect. Argument files can be nested, meaning you may include
a file from another one. When doing so, make sure to not create infinite
loops by including files mutually. Mixing of direct arguments and argument
files is allowed as well."""

    parser = argparse.ArgumentParser(
        prog="btrfs-backup-ng",
        description=description,
        epilog=epilog,
        add_help=False,
        fromfile_prefix_chars="@",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=global_parser,
    )
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        help="Show this help message and exit.",
    )
    parser.add_argument("-V", "--version", action="version", version=f"{__version__}")

    group = parser.add_argument_group(
        "Retention settings",
        description="By default, snapshots are kept forever at both source "
        "and destination. With these settings you may specify an "
        "alternate retention policy.",
    )
    group.add_argument(
        "-N",
        "--num-snapshots",
        type=int,
        default=20,
        help="Only keep latest n snapshots on source filesystem. "
        "Default is 20 to ensure incremental transfers have parent snapshots available. "
        "Set to 0 to keep all snapshots.",
    )
    group.add_argument(
        "-n",
        "--num-backups",
        type=int,
        default=0,
        help="Only keep latest n backups at destination. "
        "Default is 0 (keep all backups). "
        "Not supported for 'shell://' storage.",
    )

    group = parser.add_argument_group("Snapshot creation settings")
    group.add_argument(
        "-S",
        "--no-snapshot",
        action="store_true",
        help="Don't take a new snapshot, just transfer existing ones.",
    )
    group.add_argument(
        "-f",
        "--snapshot-folder",
        help="Snapshot folder in source filesystem; either relative to source or absolute. "
        "Default is '.btrfs-backup-ng/snapshots'.",
    )
    group.add_argument(
        "-p",
        "--snapshot-prefix",
        help="Prefix for snapshot names. Default is system hostname.",
    )

    group = parser.add_argument_group("Transfer related options")
    group.add_argument(
        "-T",
        "--no-transfer",
        action="store_true",
        help="Don't transfer any snapshot.",
    )
    group.add_argument(
        "-I",
        "--no-incremental",
        action="store_true",
        help="Don't ever try to send snapshots incrementally. "
        "This might be useful when piping to a file for storage.",
    )

    group = parser.add_argument_group("SSH related options")
    group.add_argument(
        "--ssh-opt",
        action="append",
        default=[],
        help="Pass extra ssh_config options to ssh(1). "
        "Example: '--ssh-opt Cipher=aes256-ctr --ssh-opt IdentityFile=/root/id_rsa'",
    )
    group.add_argument(
        "--ssh-sudo",
        action="store_true",
        default=False,
        help="Execute commands with sudo on the remote host. REQUIRED for btrfs operations "
        "if the remote user doesn't have direct permissions.",
    )
    group.add_argument(
        "--use-mbuffer",
        action="store_true",
        default=True,
        help="Use mbuffer if available for more reliable SSH transfers.",
    )
    group.add_argument(
        "--verify-transfer",
        action="store_true",
        default=True,
        help="Verify that snapshots were successfully created after transfer.",
    )
    group.add_argument(
        "--verify-ssh-transfer",
        action="store_true",
        default=True,
        help="Verify SSH transfers with comprehensive checks.",
    )
    group.add_argument(
        "--ssh-identity-file",
        help="Explicitly specify the SSH identity (private key) file to use.",
    )
    group.add_argument(
        "--ssh-username",
        help="Explicitly specify the SSH username to use when connecting to remote hosts.",
    )
    group.add_argument(
        "--no-password-auth",
        action="store_true",
        default=False,
        help="Disable SSH password authentication prompts.",
    )
    group.add_argument(
        "--simple-progress",
        action="store_true",
        default=True,
        help="Use simplified progress monitoring for SSH transfers (default).",
    )
    group.add_argument(
        "--advanced-progress",
        action="store_true",
        default=False,
        help="Use advanced real-time monitoring system for SSH transfers.",
    )

    group = parser.add_argument_group("Miscellaneous options")
    group.add_argument(
        "-s",
        "--sync",
        action="store_true",
        help="Run 'btrfs subvolume sync' after deleting subvolumes.",
    )
    group.add_argument(
        "-w",
        "--convert-rw",
        action="store_true",
        help="Convert read-only snapshots to read-write before deleting them.",
    )
    group.add_argument(
        "--remove-locks",
        action="store_true",
        help="Remove locks for all given destinations from all snapshots.",
    )
    group.add_argument(
        "--skip-fs-checks",
        action="store_true",
        help="Don't check whether source / destination is a btrfs subvolume / filesystem.",
    )

    group = parser.add_argument_group("Source and destination")
    group.add_argument(
        "--locked-destinations",
        action="store_true",
        help="Automatically add all destinations for which locks exist at any source snapshot.",
    )
    group.add_argument(
        "source",
        help="Subvolume to backup. Formats: /path/to/subvolume or ssh://[user@]host[:port]/path",
    )
    group.add_argument(
        "destinations",
        nargs="*",
        help="Destination(s) to send backups to. Formats: /path/to/backups, "
        "ssh://[user@]host[:port]/path, or 'shell://cat > some-file'",
    )

    options = {}
    try:
        args = parser.parse_args(argv)
        for k, v in vars(args).items():
            if v is not None:
                options[k] = v
    except RecursionError as e:
        raise __util__.AbortError from e

    # Ensure retention options are integers
    options["num_snapshots"] = int(options.get("num_snapshots", 20))
    options["num_backups"] = int(options.get("num_backups", 0))

    # Handle progress monitoring mode
    if options.get("advanced_progress", False):
        options["simple_progress"] = False
    else:
        options["simple_progress"] = True

    # Process SSH identity file
    if "ssh_identity_file" in options and options["ssh_identity_file"]:
        identity_file = options["ssh_identity_file"]
        running_as_sudo = os.geteuid() == 0 and os.environ.get("SUDO_USER")

        if running_as_sudo:
            sudo_user = os.environ.get("SUDO_USER")
            sudo_user_home = pwd.getpwnam(sudo_user).pw_dir

            if identity_file.startswith("~"):
                identity_file = identity_file.replace("~", sudo_user_home, 1)
            elif not os.path.isabs(identity_file):
                identity_file = os.path.join(sudo_user_home, identity_file)
        else:
            identity_file = os.path.expanduser(identity_file)

        identity_path = Path(identity_file).absolute()
        options["ssh_identity_file"] = str(identity_path)

        if not identity_path.exists():
            logger.warning("SSH identity file does not exist: %s", identity_path)
        elif not os.access(identity_path, os.R_OK):
            logger.warning("SSH identity file is not readable: %s", identity_path)

    return options


def run_task(options):
    """Run a backup task with the given options."""
    try:
        if "source" not in options or not options["source"]:
            raise __util__.AbortError("No source specified")
        options.setdefault("destinations", [])
    except Exception as e:
        print(f"Error initializing task options: {e}")
        traceback.print_exc()
        raise __util__.AbortError(f"Failed to initialize task: {e}")

    if "quiet" in options:
        options["verbosity"] = "warning"

    log_initial_settings(options)

    source_endpoint = prepare_source_endpoint(options)
    destination_endpoints = prepare_destination_endpoints(options, source_endpoint)

    if not options["no_snapshot"]:
        snapshot = take_snapshot(source_endpoint, options)
    else:
        snapshot = None

    for destination_endpoint in destination_endpoints:
        try:
            sync_snapshots(
                source_endpoint,
                destination_endpoint,
                keep_num_backups=options["num_backups"],
                no_incremental=options["no_incremental"],
                snapshot=snapshot,
                options=options,
            )
        except __util__.AbortError as e:
            logger.error("Aborting snapshot transfer to %s", destination_endpoint)
            logger.debug("Exception was: %s", e)

    time.sleep(1)
    cleanup_snapshots(source_endpoint, destination_endpoints, options)


def log_initial_settings(options):
    """Log the initial settings for the task."""
    logger.info(__util__.log_heading(f"Started at {time.ctime()}"))
    logger.debug(__util__.log_heading("Settings"))
    logger.debug("Enable btrfs debugging: %r", options["btrfs_debug"])
    logger.debug("Don't take a new snapshot: %r", options["no_snapshot"])
    logger.debug("Number of snapshots to keep: %d", options["num_snapshots"])
    logger.debug("Number of backups to keep: %s", options["num_backups"])
    logger.debug(
        "Snapshot folder: %s",
        options.get("snapshot_folder", ".btrfs-backup-ng/snapshots"),
    )
    logger.debug(
        "Snapshot prefix: %s", options.get("snapshot_prefix", f"{os.uname()[1]}-")
    )
    logger.debug("Don't transfer snapshots: %r", options["no_transfer"])
    logger.debug("Don't send incrementally: %r", options["no_incremental"])
    logger.debug("Extra SSH config options: %s", options["ssh_opt"])
    logger.debug("Use sudo at SSH remote host: %r", options["ssh_sudo"])


def cleanup_snapshots(source_endpoint, destination_endpoints, options):
    """Clean up old snapshots."""
    logger.info(__util__.log_heading("Cleaning up..."))
    if options.get("num_snapshots", 0) > 0:
        try:
            source_endpoint.delete_old_snapshots(options["num_snapshots"])
        except __util__.AbortError as e:
            logger.debug("Error while deleting source snapshots: %s", e)

    if options.get("num_backups", 0) > 0:
        for destination_endpoint in destination_endpoints:
            try:
                destination_endpoint.delete_old_snapshots(options["num_backups"])
            except __util__.AbortError as e:
                logger.debug("Error while deleting backups: %s", e)
    logger.info(__util__.log_heading(f"Finished at {time.ctime()}"))


def prepare_source_endpoint(options):
    """Prepare the source endpoint."""
    logger.debug("Source: %s", options["source"])
    endpoint_kwargs = build_endpoint_kwargs(options)
    source_endpoint_kwargs = dict(endpoint_kwargs)

    source_abs = Path(options["source"]).expanduser().resolve(strict=False)
    snapshot_folder = options.get("snapshot_folder", ".btrfs-backup-ng/snapshots")
    snapshot_root = Path(snapshot_folder).expanduser()
    if not snapshot_root.is_absolute():
        snapshot_root = source_abs.parent / snapshot_root
    snapshot_root = snapshot_root.resolve(strict=False)

    relative_source = str(source_abs).lstrip(os.sep)
    snapshot_dir = snapshot_root.joinpath(*relative_source.split(os.sep))
    snapshot_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    source_endpoint_kwargs["path"] = snapshot_dir

    try:
        source_endpoint = endpoint.choose_endpoint(
            str(source_abs),
            source_endpoint_kwargs,
            source=True,
        )
    except ValueError as e:
        logger.error("Couldn't parse source specification: %s", e)
        raise __util__.AbortError

    logger.debug("Source endpoint: %s", source_endpoint)
    source_endpoint.prepare()
    return source_endpoint


def prepare_destination_endpoints(options, source_endpoint):
    """Prepare the destination endpoints."""
    if options.get("locked_destinations"):
        for snap in source_endpoint.list_snapshots():
            for lock in snap.locks:
                if lock not in options["destinations"]:
                    options["destinations"].append(lock)

    if options["no_transfer"] and options["num_backups"] <= 0:
        logger.debug("Skipping destination endpoint creation.")
        return []

    destination_endpoints = []
    endpoint_kwargs = build_endpoint_kwargs(options)
    endpoint_kwargs.pop("path", None)

    ssh_sudo = options.get("ssh_sudo", False)
    if ssh_sudo:
        endpoint_kwargs["ssh_sudo"] = True
        options["ssh_sudo"] = True

    endpoint_kwargs["direct_ssh_pipe"] = True
    endpoint_kwargs["verify_transfer"] = True

    for destination in options["destinations"]:
        logger.debug("Setting up destination: %s", destination)
        try:
            destination_endpoint = endpoint.choose_endpoint(
                destination,
                endpoint_kwargs,
                source=False,
            )

            if (
                hasattr(destination_endpoint, "_is_remote")
                and destination_endpoint._is_remote
                and options.get("ssh_sudo", False)
            ):
                destination_endpoint.config["ssh_sudo"] = True

            destination_endpoint.prepare()
            destination_endpoints.append(destination_endpoint)

        except ValueError as e:
            logger.error("Couldn't parse destination specification: %s", e)
            raise __util__.AbortError
        except Exception as e:
            logger.error("Error setting up destination %s: %s", destination, e)
            raise __util__.AbortError(
                f"Failed to set up destination {destination}: {e}"
            )

    return destination_endpoints


def build_endpoint_kwargs(options):
    """Build common kwargs for endpoints."""
    no_password_auth = options.get("no_password_auth", False) or os.environ.get(
        "BTRFS_BACKUP_NO_PASSWORD_AUTH", ""
    ).lower() in ("1", "true", "yes")

    kwargs = {
        "snap_prefix": options.get("snapshot_prefix", f"{os.uname()[1]}-"),
        "convert_rw": options["convert_rw"],
        "subvolume_sync": options["sync"],
        "btrfs_debug": options["btrfs_debug"],
        "fs_checks": not options["skip_fs_checks"],
        "ssh_opts": options["ssh_opt"],
        "ssh_sudo": options["ssh_sudo"],
        "simple_progress": options.get("simple_progress", True),
        "ssh_password_fallback": not no_password_auth,
    }

    if "ssh_username" in options and options["ssh_username"]:
        kwargs["username"] = options["ssh_username"]

    if "ssh_identity_file" in options and options["ssh_identity_file"]:
        kwargs["ssh_identity_file"] = options["ssh_identity_file"]

    return kwargs


def take_snapshot(source_endpoint, options):
    """Take a snapshot on the source endpoint."""
    logger.info(__util__.log_heading("Transferring ..."))
    snapshot = source_endpoint.snapshot()
    if options.get("num_snapshots", 0) > 0:
        try:
            source_endpoint.delete_old_snapshots(options["num_snapshots"])
        except Exception as e:
            logger.debug("Error while deleting source snapshots: %s", e)
    return snapshot


def legacy_main(argv: list[str] | None = None) -> int:
    """Legacy main function entry point.

    Args:
        argv: Command line arguments (without program name)

    Returns:
        Exit code
    """
    if argv is None:
        argv = sys.argv[1:]

    # Check if running with sudo
    if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
        sudo_user = os.environ.get("SUDO_USER")
        print(f"Running as root (via sudo from user {sudo_user}).")
        print(
            "NOTE: When running with sudo, your regular SSH keys may not be accessible."
        )
        print("If connecting to SSH destinations fails, try:")
        print("  1. Use --ssh-identity-file to specify your SSH private key explicitly")
        print("  2. Run without sudo and configure appropriate permissions")
        print("")

    if any("ssh://" in arg for arg in argv):
        print("NOTE: SSH destination detected. For btrfs operations on remote systems:")
        print(
            "  - Use --ssh-sudo if the remote user doesn't have direct btrfs permissions"
        )
        print(
            "  - Ensure the remote user has passwordless sudo rights for btrfs commands"
        )
        print("")

    global_parser = [argparse.ArgumentParser(add_help=False)]
    group = global_parser[0].add_argument_group("Global Display settings")
    group.add_argument(
        "-v",
        "--verbosity",
        default="info",
        choices=["debug", "info", "warning", "error"],
        help="Set verbosity level.",
    )
    group.add_argument(
        "-q",
        "--quiet",
        default=False,
        action="store_true",
        help="Shortcut for --verbosity 'warning'.",
    )
    group.add_argument(
        "-d",
        "--btrfs-debug",
        default=False,
        action="store_true",
        help="Enable debugging on btrfs send / receive.",
    )

    command_line = " ".join(argv)
    tasks = [task.split() for task in command_line.split("::")]
    task_options = [parse_options(global_parser, task) for task in tasks]

    level = task_options[0].get("verbosity", "INFO").upper()
    create_logger(False, level=level)
    logger.debug("Logger initialized")

    try:
        for n, options in enumerate(task_options):
            logger.debug(f"Starting task {n + 1}/{len(task_options)}")
            run_task(options)
            logger.debug(f"Completed task {n + 1}/{len(task_options)}")
        logger.info("All tasks completed successfully")
        return 0
    except (__util__.AbortError, KeyboardInterrupt):
        logger.error("Process aborted by user or error")
        return 1
