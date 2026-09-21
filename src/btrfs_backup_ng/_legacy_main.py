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
import os
import pwd
import sys
import time
import traceback
from pathlib import Path

from . import __util__, __version__, endpoint
from .__logger__ import create_logger, logger
from .core import progress as progress_utils
from .core.operations import sync_snapshots


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
        help="Folder the snapshots are taken into; relative to the source, or "
        "absolute. It must exist when absolute. Default is '.snapshots' inside "
        "the source.",
    )
    group.add_argument(
        "--accept-full-send",
        action="store_true",
        help="Start a new snapshot chain in --snapshot-folder although the "
        "source's .snapshots already holds one, accepting that the next "
        "transfer to every destination is a full send. Needed once at most.",
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

    group = parser.add_argument_group("Space checking options")
    group.add_argument(
        "--no-check-space",
        action="store_true",
        help="Disable pre-flight space availability check before transfers.",
    )
    group.add_argument(
        "--force",
        action="store_true",
        help="Proceed with transfers even if space check fails.",
    )
    group.add_argument(
        "--safety-margin",
        type=float,
        default=10.0,
        metavar="PERCENT",
        help="Safety margin percentage for space check (default: 10%%).",
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
        "--fs-checks",
        choices=["auto", "strict", "skip"],
        default="auto",
        help="Filesystem verification mode: 'auto' (warn and continue), "
        "'strict' (error on failure), 'skip' (no checks). Default: auto",
    )
    group.add_argument(
        "--skip-fs-checks",
        action="store_const",
        const="skip",
        dest="fs_checks",
        help="Alias for --fs-checks=skip (deprecated, use --fs-checks instead).",
    )

    group = parser.add_argument_group("Source and destination")
    group.add_argument(
        "--locked-destinations",
        action="store_true",
        help="Automatically add all destinations for which locks exist at any source snapshot.",
    )
    group.add_argument(
        "source",
        help="Subvolume to back up: a local path. A remote (ssh://) source is not "
        "supported; ssh:// is accepted for destinations.",
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

    # Handle space checking options
    # Convert --no-check-space to check_space (inverted)
    options["check_space"] = not options.get("no_check_space", False)
    # force and safety_margin are passed through as-is

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
            assert sudo_user is not None  # guaranteed by running_as_sudo check
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

    # Enable Rich progress bars for interactive terminals
    if "show_progress" not in options:
        options["show_progress"] = progress_utils.is_interactive()

    log_initial_settings(options)

    source_endpoint = prepare_source_endpoint(options)
    destination_endpoints = prepare_destination_endpoints(options, source_endpoint)

    if options.get("remove_locks"):
        return remove_locks(source_endpoint, destination_endpoints)

    if not options["no_snapshot"]:
        snapshot = take_snapshot(source_endpoint, options)
    else:
        snapshot = None

    any_failed = False
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
            # A failed transfer (SnapshotTransferError subclasses AbortError) must
            # not be reported as overall success. Record it, continue to the other
            # destinations, then signal failure so the exit code is non-zero.
            logger.error("Aborting snapshot transfer to %s", destination_endpoint)
            logger.debug("Exception was: %s", e)
            any_failed = True

    time.sleep(1)
    # A retention failure is a failure of the run: the backup may be safe but
    # the target is not being kept within its policy, and reporting success
    # hides that until the disk fills.
    if not cleanup_snapshots(source_endpoint, destination_endpoints, options):
        any_failed = True
    return not any_failed


def remove_locks(source_endpoint, destination_endpoints):
    """Drop every lock the given destinations hold on the source's snapshots.

    This is what ``--remove-locks`` promises. The flag had been accepted and
    parsed since the option was introduced but was never read by anything, so
    the command exited 0 having done nothing at all.

    Locks are keyed by destination id (``set_lock`` in ``core.operations``), so
    only the destinations named on this command line are cleared -- a lock held
    for some other destination is left alone rather than being swept up.
    """
    destination_ids = [endpoint.get_id() for endpoint in destination_endpoints]
    removed = 0
    for snapshot in source_endpoint.list_snapshots():
        for destination_id in destination_ids:
            if destination_id in snapshot.locks:
                source_endpoint.set_lock(snapshot, destination_id, False)
                removed += 1
            if destination_id in snapshot.parent_locks:
                source_endpoint.set_lock(snapshot, destination_id, False, parent=True)
                removed += 1
    logger.info(
        "Removed %d lock(s) for %d destination(s) from %s",
        removed,
        len(destination_ids),
        source_endpoint,
    )
    return True


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
        options.get("snapshot_folder", LEGACY_SNAPSHOT_FOLDER),
    )
    logger.debug(
        "Snapshot prefix: %s", options.get("snapshot_prefix", f"{os.uname()[1]}-")
    )
    logger.debug("Don't transfer snapshots: %r", options["no_transfer"])
    logger.debug("Don't send incrementally: %r", options["no_incremental"])
    logger.debug("Extra SSH config options: %s", options["ssh_opt"])
    logger.debug("Use sudo at SSH remote host: %r", options["ssh_sudo"])


def _report_deletion(result, what) -> bool:
    """Report one deletion batch and say whether it succeeded.

    ``delete_old_snapshots`` returns a DeletionResult saying what it actually
    did. Both call sites here discarded it, so a batch that deleted nothing --
    because a lock file was unreadable, a target was busy, or every btrfs delete
    failed -- was indistinguishable from a clean prune. A skip is not a failure:
    refusing to delete a locked snapshot is the guard working.
    """
    if result is None:  # pragma: no cover - endpoints all return a result now
        return True
    if result.deleted_count:
        logger.info("Deleted %d old %s", result.deleted_count, what)
    for snapshot, reason in result.skipped:
        logger.info("Kept %s: %s", snapshot, reason)
    for snapshot, error in result.failed:
        logger.error("FAILED to delete %s: %s", snapshot, error)
    return result.ok


def cleanup_snapshots(source_endpoint, destination_endpoints, options):
    """Clean up old snapshots.

    Returns True when every deletion the retention policy asked for either
    happened or was deliberately skipped. A failure used to be logged at DEBUG
    and dropped, so a run whose retention failed outright still reported
    success -- while the target filled up. The modern `run` path already exits
    non-zero rather than silently skipping retention; this matches it.
    """
    logger.info(__util__.log_heading("Cleaning up..."))
    clean = True
    if options.get("num_snapshots", 0) > 0:
        try:
            clean &= _report_deletion(
                source_endpoint.delete_old_snapshots(options["num_snapshots"]),
                "source snapshot(s)",
            )
        except __util__.AbortError as e:
            logger.error("Error while deleting source snapshots: %s", e)
            clean = False

    if options.get("num_backups", 0) > 0:
        for destination_endpoint in destination_endpoints:
            try:
                clean &= _report_deletion(
                    destination_endpoint.delete_old_snapshots(options["num_backups"]),
                    f"backup(s) on {destination_endpoint}",
                )
            except __util__.AbortError as e:
                logger.error("Error while deleting backups: %s", e)
                clean = False
    logger.info(__util__.log_heading(f"Finished at {time.ctime()}"))
    return clean


#: Where legacy mode takes its snapshots: inside the source, like the
#: config-driven commands. This is also where every legacy run has in fact put
#: them: --snapshot-folder computed a directory and set the endpoint's path,
#: but never passed snapshot_folder, so Endpoint.snapshot() used its own
#: default and the computed directory was created and then ignored.
LEGACY_SNAPSHOT_FOLDER = ".snapshots"


def _snapshots_for_prefix(folder: Path, prefix: str) -> list[str]:
    """Names in ``folder`` that look like this run's snapshots; [] if absent."""
    if not folder.is_dir():
        return []
    return sorted(
        p.name for p in folder.iterdir() if p.is_dir() and p.name.startswith(prefix)
    )


def resolve_snapshot_folder(options, source_abs: Path) -> Path:
    """The directory legacy mode will snapshot into. Creates nothing.

    Relative to the source, or absolute; ``.snapshots`` inside the source by
    default.
    """
    folder = Path(options.get("snapshot_folder", LEGACY_SNAPSHOT_FOLDER)).expanduser()
    if folder.is_absolute():
        return folder.resolve(strict=False)
    return (source_abs / folder).resolve(strict=False)


def refuse_silent_new_chain(options, source_abs: Path, snapshot_dir: Path) -> None:
    """Refuse to start a second chain when the first one is where it always was.

    Until this release ``--snapshot-folder`` had no effect on placement, so a
    user who passed it has their chain in ``<source>/.snapshots``. Honouring
    the option now would begin a NEW chain in the folder they named, and the
    next transfer to every destination would be a full send -- silently, from
    a cron job, over whatever link the destination is behind. Refuse, and name
    the three ways out. ``--accept-full-send`` is the third; it is needed at
    most once, because the folder then holds snapshots.
    """
    default_chain = (source_abs / LEGACY_SNAPSHOT_FOLDER).resolve(strict=False)
    if snapshot_dir == default_chain:
        return
    prefix = options.get("snapshot_prefix", f"{os.uname()[1]}-")
    existing = _snapshots_for_prefix(default_chain, prefix)
    if not existing or _snapshots_for_prefix(snapshot_dir, prefix):
        return
    if options.get("accept_full_send"):
        logger.warning(
            "--accept-full-send: starting a new snapshot chain in %s; %s holds "
            "%d snapshot(s) for prefix %r that will not be used as parents.",
            snapshot_dir,
            default_chain,
            len(existing),
            prefix,
        )
        return
    raise __util__.AbortError(
        f"Snapshot folder {snapshot_dir} holds no snapshots for prefix "
        f"{prefix!r}, but {default_chain} holds {len(existing)}. Earlier "
        "releases ignored --snapshot-folder and took every legacy-mode "
        "snapshot there, so continuing would start a NEW chain in the folder "
        "you named, and the next transfer to every destination would be a FULL "
        "send. Nothing was created. Choose one: (1) if both are on the same "
        f"filesystem, move the chain intact: mkdir -p {snapshot_dir} && "
        f"mv {default_chain}/{prefix}* {snapshot_dir}/ ; (2) keep the chain "
        "where it is: pass "
        f"--snapshot-folder {default_chain} ; (3) accept one full send: add "
        "--accept-full-send, which is needed only once because the folder then "
        "holds snapshots."
    )


def prepare_source_endpoint(options):
    """Prepare the source endpoint."""
    logger.debug("Source: %s", options["source"])
    endpoint_kwargs = build_endpoint_kwargs(options)
    source_endpoint_kwargs = dict(endpoint_kwargs)

    source_abs = Path(options["source"]).expanduser().resolve(strict=False)
    # The source is looked at BEFORE anything derived from it is created.
    # The snapshot tree used to be built first, with parents=True, so a
    # source that was not there (its disk unmounted, or a typo) got a tree
    # of directories built beside it before the run failed for the missing
    # source -- and an absolute --snapshot-folder was built wherever it
    # pointed. A backup location is a statement that something is there;
    # only what lies BELOW an existing base is created.
    if not source_abs.is_dir():
        raise __util__.AbortError(
            __util__.missing_backup_location_message("Source", source_abs)
        )
    snapshot_dir = resolve_snapshot_folder(options, source_abs)
    refuse_silent_new_chain(options, source_abs, snapshot_dir)

    if snapshot_dir == source_abs or snapshot_dir.is_relative_to(source_abs):
        # Under the source, which exists: created one component at a time.
        __util__.create_below(
            source_abs,
            str(snapshot_dir.relative_to(source_abs)),
            mode=0o700,
            what="Source",
        )
    else:
        # Absolute, or a relative folder that climbs out of the source: a
        # place the operator says exists. Nothing is created.
        __util__.create_below(snapshot_dir, what="Snapshot folder")

    source_endpoint_kwargs["path"] = snapshot_dir
    # Passed through, so the snapshot is TAKEN where the option says. Without
    # this the endpoint fell back to its own default and the option was a
    # no-op for placement.
    source_endpoint_kwargs["snapshot_folder"] = str(snapshot_dir)

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
        "fs_checks": options.get("fs_checks", "auto"),
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
        all_ok = True
        for n, options in enumerate(task_options):
            logger.debug(f"Starting task {n + 1}/{len(task_options)}")
            if not run_task(options):
                all_ok = False
            logger.debug(f"Completed task {n + 1}/{len(task_options)}")
        if all_ok:
            logger.info("All tasks completed successfully")
            return 0
        logger.error("One or more transfers failed")
        return 1
    except __util__.AbortError as e:
        # The diagnosis travels in the exception; a refusal that does not say
        # why (a source or snapshot folder that is not there, and what to do
        # about it) only relocates the confusion.
        if str(e):
            logger.error("%s", e)
        logger.error("Process aborted by user or error")
        return 1
    except KeyboardInterrupt:
        logger.error("Process aborted by user or error")
        return 1
