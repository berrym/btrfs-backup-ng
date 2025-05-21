# pyright: standard

"""btrfs-backup-ng: btrfs-backup/__main__.py.

Backup a btrfs volume to another, incrementally
Requires Python >= 3.9, btrfs-progs >= 3.12 most likely.

Copyright (c) 2024 Michael Berry <trismegustis@gmail.com>
Copyright (c) 2017 Robert Schindler <r.schindler@efficiosoft.com>
Copyright (c) 2014 Chris Lawrence <lawrencc@debian.org>

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation files
(the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge,
publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import argparse
import concurrent.futures
import logging
import logging.handlers
import multiprocessing
import os
import pwd
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path  # Required for path handling

from rich.align import Align
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TimeElapsedColumn
from rich.text import Text

from . import __util__, __version__, endpoint
from .__logger__ import RichLogger, cons, create_logger, logger


def send_snapshot(snapshot, destination_endpoint, parent=None, clones=None) -> None:
    """Sends snapshot to destination endpoint, using given parent and clones."""
    logger.info("Sending %s ...", snapshot)
    logger.debug("Source endpoint type: %s", type(snapshot.endpoint).__name__)
    logger.debug("Destination endpoint type: %s", type(destination_endpoint).__name__)
    logger.debug("Source snapshot path: %s", snapshot.get_path())
    logger.debug("Destination path: %s", destination_endpoint.config["path"])

    # Verify destination path is accessible
    logger.debug("Verifying destination path is accessible...")
    try:
        # For SSH endpoints, ensure directory exists before transfer
        if (
            hasattr(destination_endpoint, "_is_remote")
            and destination_endpoint._is_remote
        ):
            if hasattr(destination_endpoint, "_exec_remote_command"):
                path = destination_endpoint._normalize_path(
                    destination_endpoint.config["path"]
                )
                logger.debug("Ensuring remote destination path exists: %s", path)
                try:
                    cmd = ["test", "-d", path]
                    result = destination_endpoint._exec_remote_command(cmd, check=False)
                    if result.returncode != 0:
                        # Path doesn't exist, create it
                        logger.warning(
                            "Destination path doesn't exist, creating it: %s", path
                        )
                        mkdir_cmd = ["mkdir", "-p", path]
                        mkdir_result = destination_endpoint._exec_remote_command(
                            mkdir_cmd, check=False
                        )
                        if mkdir_result.returncode != 0:
                            stderr = mkdir_result.stderr.decode(
                                "utf-8", errors="replace"
                            )
                            logger.error(
                                "Failed to create destination directory: %s", stderr
                            )
                            raise __util__.SnapshotTransferError(
                                f"Cannot create destination directory: {stderr}"
                            )
                except Exception as e:
                    logger.error("Error verifying/creating destination: %s", e)
        # For local endpoints, ensure directory exists
        elif hasattr(destination_endpoint, "config") and "path" in destination_endpoint.config:
            path = destination_endpoint.config["path"]
            logger.debug("Ensuring local destination path exists: %s", path)
            try:
                path_obj = Path(path)
                if not path_obj.exists():
                    logger.warning("Local destination path doesn't exist, creating it: %s", path)
                    path_obj.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.error("Error creating local destination directory: %s", e)
                raise __util__.SnapshotTransferError(f"Cannot create local destination directory: {e}")
    except Exception as e:
        logger.warning(
            "Error during destination verification (will try transfer anyway): %s", e
        )

    log_msg = (
        f"  Using parent: {parent}"
        if parent
        else "  No parent snapshot available, sending in full mode."
    )
    logger.info(log_msg)
    logger.info(f"  Using clones: {clones!r}" if clones else "")

    send_process = None
    receive_process = None
    try:
        logger.debug("Starting send process from %s", snapshot.endpoint)
        send_process = snapshot.endpoint.send(snapshot, parent=parent, clones=clones)
        if send_process is None:
            logger.error("Failed to start send process - send_process is None")
            raise __util__.SnapshotTransferError("Send process failed to start")

        logger.debug("Starting receive process on %s", destination_endpoint)
        # Check if using SSH destination without sudo
        is_ssh_endpoint = (
            hasattr(destination_endpoint, "_is_remote")
            and destination_endpoint._is_remote
        )
        is_using_sudo = is_ssh_endpoint and destination_endpoint.config.get(
            "ssh_sudo", False
        )

        if is_ssh_endpoint and not is_using_sudo:
            logger.warning(
                "Using SSH destination without --ssh-sudo. This may fail if the remote user doesn't have permissions for btrfs commands."
            )

        try:
            receive_process = destination_endpoint.receive(send_process.stdout)
            if receive_process is None:
                logger.error("Failed to start receive process - receive_process is None")
                if is_ssh_endpoint and not is_using_sudo:
                    logger.error(
                        "For SSH destinations, try using --ssh-sudo if the remote user needs elevated permissions"
                    )
                raise __util__.SnapshotTransferError("Receive process failed to start")
        except Exception as e:
            logger.error("Failed to start receive process: %s", e)
            raise __util__.SnapshotTransferError(f"Receive process failed to start: {e}")

        logger.debug("Waiting for processes to complete...")
        return_codes = [p.wait() for p in [send_process, receive_process]]
        logger.debug(
            "Process return codes: send=%d, receive=%d",
            return_codes[0],
            return_codes[1],
        )

        if any(rc != 0 for rc in return_codes):
            error_message = (
                f"btrfs send/receive failed with return codes: {return_codes}"
            )
            logger.error(error_message)
            # Try to get stderr from processes
            if hasattr(send_process, "stderr") and send_process.stderr:
                send_err = send_process.stderr.read().decode("utf-8", errors="replace")
                logger.error("Send process stderr: %s", send_err)
            if hasattr(receive_process, "stderr") and receive_process.stderr:
                recv_err = receive_process.stderr.read().decode(
                    "utf-8", errors="replace"
                )
                logger.error("Receive process stderr: %s", recv_err)
            raise __util__.SnapshotTransferError(error_message)

        logger.info("Transfer completed successfully")

    except (OSError, subprocess.CalledProcessError) as e:
        logger.error("Error during snapshot transfer: %r", e)
        if hasattr(e, "stderr") and e.stderr:
            stderr = e.stderr.decode("utf-8", errors="replace")
            logger.error("Process stderr: %s", stderr)
            # Check for common SSH/sudo permission issues
            if (
                "permission denied" in stderr.lower() or "sudo" in stderr.lower()
            ) and hasattr(destination_endpoint, "_is_remote"):
                logger.error(
                    "This appears to be a permission issue with the remote btrfs command"
                )
                logger.error(
                    "For SSH destinations, use --ssh-sudo and ensure the remote user has passwordless sudo rights"
                )
                logger.error(
                    "Add this to remote /etc/sudoers: username ALL=(ALL) NOPASSWD: /usr/bin/btrfs"
                )
        if hasattr(e, "stdout") and e.stdout:
            logger.error(
                "Process stdout: %s", e.stdout.decode("utf-8", errors="replace")
            )
        raise __util__.SnapshotTransferError(f"Exception during transfer: {e}") from e
    finally:
        for pipe in [send_process, receive_process]:
            if pipe:
                try:
                    if hasattr(pipe, "stdout") and pipe.stdout:
                        pipe.stdout.close()
                    if hasattr(pipe, "stdin") and pipe.stdin:
                        pipe.stdin.close()
                    if hasattr(pipe, "stderr") and pipe.stderr:
                        pipe.stderr.close()
                except (AttributeError, IOError) as e:
                    logger.warning("Error closing pipe: %s", e)


def delete_corrupt_snapshots(
    destination_endpoint, source_snapshots, destination_snapshots
):
    """Deletes corrupt snapshots from the destination."""
    to_remove = []
    destination_id = destination_endpoint.get_id()
    for snapshot in source_snapshots:
        if snapshot in destination_snapshots and destination_id in snapshot.locks:
            destination_snapshot = destination_snapshots[
                destination_snapshots.index(snapshot)
            ]
            logger.info(
                "Potentially corrupt snapshot %s found at %s",
                destination_snapshot,
                destination_endpoint,
            )
            to_remove.append(destination_snapshot)
    if to_remove:
        destination_endpoint.delete_snapshots(to_remove)
        destination_snapshots = (
            destination_endpoint.list_snapshots()
        )  # Refresh after deletion
    return destination_snapshots


def clear_locks(source_endpoint, source_snapshots, destination_id):
    """Clears locks for the given destination from source snapshots."""
    for snapshot in source_snapshots:
        if destination_id in snapshot.locks:
            source_endpoint.set_lock(snapshot, destination_id, False)
        if destination_id in snapshot.parent_locks:
            source_endpoint.set_lock(snapshot, destination_id, False, parent=True)


def plan_transfers(source_snapshots, destination_snapshots, keep_num_backups):
    """Plans which snapshots need to be transferred."""
    to_consider = (
        source_snapshots[-keep_num_backups:]
        if keep_num_backups > 0
        else source_snapshots
    )
    to_transfer = [
        snapshot for snapshot in to_consider if snapshot not in destination_snapshots
    ]
    return to_transfer


def sync_snapshots(
    source_endpoint,
    destination_endpoint,
    keep_num_backups=0,
    no_incremental=False,
    snapshot=None,
    **kwargs,
) -> None:
    """Synchronizes snapshots from source to destination."""
    logger.info(__util__.log_heading(f"  To {destination_endpoint} ..."))

    if snapshot is None:
        source_snapshots = source_endpoint.list_snapshots()
    else:
        source_snapshots = [snapshot]

    destination_snapshots = destination_endpoint.list_snapshots()
    # destination_snapshots = delete_corrupt_snapshots(  # REMOVE THIS LINE
    #     destination_endpoint, source_snapshots, destination_snapshots
    # )
    clear_locks(source_endpoint, source_snapshots, destination_endpoint.get_id())

    to_transfer = plan_transfers(
        source_snapshots, destination_snapshots, keep_num_backups
    )

    if not to_transfer:
        logger.info("No snapshots need to be transferred.")
        return

    logger.info("Going to transfer %d snapshot(s):", len(to_transfer))
    transfer_objs = {
        "source_endpoint": source_endpoint,
        "destination_endpoint": destination_endpoint,
        "source_snapshots": source_snapshots,
        "destination_snapshots": destination_snapshots,
        "to_transfer": to_transfer,
        "no_incremental": no_incremental,
    }
    for snap in to_transfer:
        logger.info("  %s", snap)
        do_sync_transfer(transfer_objs, **kwargs)


def do_sync_transfer(transfer_objs, **kwargs):
    """Handle the data transfer part of snapshot syncing."""

    source_endpoint = transfer_objs["source_endpoint"]
    destination_endpoint = transfer_objs["destination_endpoint"]
    source_snapshots = transfer_objs["source_snapshots"]
    destination_snapshots = transfer_objs["destination_snapshots"]
    destination_id = destination_endpoint.get_id()
    to_transfer = transfer_objs["to_transfer"]
    no_incremental = transfer_objs["no_incremental"]

    logger.debug("to_transfer: %r", to_transfer)
    logger.debug(
        "Destination endpoint: %s (type: %s)",
        destination_endpoint,
        type(destination_endpoint).__name__,
    )
    logger.debug(
        "Source endpoint: %s (type: %s)",
        source_endpoint,
        type(source_endpoint).__name__,
    )

    # Before starting transfers, verify the destination path again
    try:
        destination_path = destination_endpoint.config.get("path")
        logger.debug("Verifying destination path before transfer: %s", destination_path)

        # For remote endpoints, ensure path is accessible
        if (
            hasattr(destination_endpoint, "_is_remote")
            and destination_endpoint._is_remote
        ):
            logger.debug("Destination is remote, ensuring directory exists")
            # This will be handled in each send_snapshot call
        # For local endpoints, ensure path exists and is writable
        else:
            logger.debug("Destination is local, ensuring directory exists")
            if destination_path:
                path_obj = Path(destination_path)
                if not path_obj.exists():
                    logger.warning("Creating local destination directory: %s", destination_path)
                    path_obj.mkdir(parents=True, exist_ok=True)
                elif not path_obj.is_dir():
                    logger.error("Destination exists but is not a directory: %s", destination_path)
                    raise __util__.AbortError("Destination is not a directory")
    except Exception as e:
        logger.warning(
            "Pre-transfer destination check failed (continuing anyway): %s", e
        )

    while to_transfer:
        if no_incremental:
            best_snapshot = to_transfer[-1]
            parent = None
            logger.debug(
                "Using non-incremental transfer for snapshot: %s", best_snapshot
            )
        else:
            present_snapshots = [
                snapshot
                for snapshot in source_snapshots
                if snapshot in destination_snapshots
                and snapshot.get_id() not in snapshot.locks
            ]
            logger.debug(
                "Found %d present snapshots for incremental transfer",
                len(present_snapshots),
            )

            def key(s):
                p = s.find_parent(present_snapshots)
                if p is None:
                    return float("inf")  # Use float('inf') for infinity
                d = source_snapshots.index(s) - source_snapshots.index(p)
                return -d if d < 0 else d

            best_snapshot = min(to_transfer, key=key)
            parent = best_snapshot.find_parent(present_snapshots)
            logger.debug("Selected snapshot %s with parent %s", best_snapshot, parent)

        # Set locks to prevent snapshots from being deleted during transfer
        logger.debug(
            "Setting lock on %s for destination %s", best_snapshot, destination_id
        )
        source_endpoint.set_lock(best_snapshot, destination_id, True)
        if parent:
            logger.debug(
                "Setting parent lock on %s for destination %s", parent, destination_id
            )
            source_endpoint.set_lock(parent, destination_id, True, parent=True)

        # Attempt the transfer
        try:
            logger.info(
                "Starting transfer of snapshot %s to %s",
                best_snapshot,
                destination_endpoint,
            )
            # Ensure destination_endpoint is properly initialized
            if transfer_objs["destination_endpoint"] is None:
                logger.error("Destination endpoint is None, cannot proceed with transfer")
                raise __util__.AbortError("Invalid destination endpoint for transfer")
                
            send_snapshot(
                best_snapshot,
                transfer_objs["destination_endpoint"],
                parent=parent,
                **kwargs,
            )
            logger.info("Transfer of %s completed successfully", best_snapshot)

            # Release locks and update destination
            source_endpoint.set_lock(best_snapshot, destination_id, False)
            if parent:
                source_endpoint.set_lock(parent, destination_id, False, parent=True)

            # Update destination snapshot list
            logger.debug("Adding snapshot %s to destination", best_snapshot)
            destination_endpoint.add_snapshot(best_snapshot)
            # Refresh snapshot list
            destination_endpoint.list_snapshots()

        except __util__.SnapshotTransferError as e:
            logger.error("Snapshot transfer failed for %s: %s", best_snapshot, e)
            logger.info(
                "Keeping %s locked to prevent it from getting removed.",
                best_snapshot,
            )
            # Try to get more diagnostic info about the destination
            try:
                if hasattr(destination_endpoint, "_exec_remote_command"):
                    path = destination_endpoint.config.get("path")
                    logger.debug(
                        "Checking destination path after failed transfer: %s", path
                    )
                    ls_cmd = ["ls", "-la", path]
                    result = destination_endpoint._exec_remote_command(
                        ls_cmd, check=False
                    )
                    if result.returncode == 0 and hasattr(result, "stdout"):
                        logger.debug(
                            "Destination directory contents: %s",
                            result.stdout.decode("utf-8", errors="replace"),
                        )
            except Exception as e_diagnostic:
                logger.debug("Diagnostic check failed: %s", e_diagnostic)

        # Move to next snapshot
        to_transfer.remove(best_snapshot)
        logger.debug("%d snapshots left to transfer", len(to_transfer))

    logger.info(__util__.log_heading(f"Transfers to {destination_endpoint} complete!"))


def parse_options(global_parser, argv):
    """Run the program. Items in ``argv`` are treated as command line
    arguments.
    """
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

    # Parse command line arguments
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
        description="By default, snapshots are "
        "kept forever at both source "
        "and destination. With these "
        "settings you may specify an "
        "alternate retention policy.",
    )
    group.add_argument(
        "-N",
        "--num-snapshots",
        type=int,
        default=0,
        help="Only keep latest n snapshots on source filesystem.",
    )
    group.add_argument(
        "-n",
        "--num-backups",
        type=int,
        default=0,
        help="Only keep latest n backups at destination."
        " This option is not supported for 'shell://' storage.",
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
        help="Snapshot folder in source filesystem; either relative to source or absolute."
        " Default is '.btrfs-backup-ng/snapshots'.",
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
        help="Don't ever try to send snapshots incrementally."
        " This might be useful when piping to a file for storage.",
    )

    group = parser.add_argument_group("SSH related options")
    group.add_argument(
        "--ssh-opt",
        action="append",
        default=[],
        help="Pass extra ssh_config options to ssh(1). "
        "Example: '--ssh-opt Cipher=aes256-ctr --ssh-opt IdentityFile=/root/id_rsa' "
        "would result in 'ssh -o Cipher=aes256-ctr -o IdentityFile=/root/id_rsa'.",
    )
    group.add_argument(
        "--ssh-sudo",
        action="store_true",
        help="Execute commands with sudo on the remote host. REQUIRED for btrfs operations if the remote user doesn't have direct permissions to run btrfs commands. Remote user must have passwordless sudo access configured for btrfs commands.",
    )
    group.add_argument(
        "--ssh-identity-file",
        help="Explicitly specify the SSH identity (private key) file to use. "
        "Useful when running btrfs-backup-ng with sudo where your regular user's SSH keys aren't accessible.",
    )
    group.add_argument(
        "--ssh-username",
        help="Explicitly specify the SSH username to use when connecting to remote hosts. "
        "Overrides usernames specified in SSH URLs.",
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
        help="Convert read-only snapshots to read-write before deleting them."
        " This allows regular users to delete "
        "subvolumes when mount option user_subvol_rm_allowed is enabled.",
    )
    group.add_argument(
        "--remove-locks",
        action="store_true",
        help="Remove locks for all given destinations from all snapshots present at source."
        " You should only use this flag if you can assure that no partially"
        " transferred snapshot is left at any given destination. It "
        "might be useful together with '--no-snapshot --no-transfer --locked-destinations'"
        " in order to clean up any existing lock without doing anything else.",
    )
    group.add_argument(
        "--skip-fs-checks",
        action="store_true",
        help="Don't check whether source / destination is a btrfs subvolume / filesystem."
        " Normally, you shouldn't need to use this flag."
        " If it is necessary in a working setup, please consider filing a bug.",
    )

    # for backwards compatibility only
    group = parser.add_argument_group(
        "Deprecated options",
        description="These options are available for backwards compatibility only"
        " and might be removed in future releases. Please stop using them.",
    )
    group.add_argument(
        "--latest-only",
        action="store_true",
        help="Shortcut for '--num-snapshots 1'.",
    )

    group = parser.add_argument_group("Source and destination")
    group.add_argument(
        "--locked-destinations",
        action="store_true",
        help="Automatically add all destinations for which locks exist at any source snapshot.",
    )
    group.add_argument(
        "source",
        help="Subvolume to backup. "
        "The following schemes are possible:"
        " - /path/to/subvolume "
        " - ssh://[user@]host[:port]/path/to/subvolume "
        "Specifying a source is mandatory.",
    )
    group.add_argument(
        "destinations",
        nargs="*",
        help="Destination to send backups to. "
        "The following schemes are possible:"
        " - /path/to/backups"
        " - ssh://[user@]host[:port]/path/to/backups"
        " - 'shell://cat > some-file' "
        "You may use this argument multiple times to transfer backups to multiple locations. "
        "You may even omit it "
        "completely in what case no snapshot is transferred at all. That allows, for instance, "
        "for well-organized local snapshotting without backing up.",
    )

    # parse args then convert to dict format
    options = {}
    try:
        args = parser.parse_args(argv)
        for k, v in vars(args).items():
            if v is not None:
                options[k] = v
    except RecursionError as e:
        raise __util__.AbortError from e

    # Ensure retention options are always integers
    try:
        options["num_snapshots"] = int(options.get("num_snapshots", 0))
    except Exception:
        options["num_snapshots"] = 0
    try:
        options["num_backups"] = int(options.get("num_backups", 0))
    except Exception:
        options["num_backups"] = 0

    # Process SSH identity file if provided
    if "ssh_identity_file" in options and options["ssh_identity_file"]:
        identity_file = options["ssh_identity_file"]
        
        # Special handling when running as root via sudo
        running_as_sudo = os.geteuid() == 0 and os.environ.get("SUDO_USER")
        if running_as_sudo:
            sudo_user = os.environ.get("SUDO_USER")
            sudo_user_home = pwd.getpwnam(sudo_user).pw_dir
            logger.debug("Running as sudo from user: %s (home: %s)", sudo_user, sudo_user_home)
            
            # Handle ~ expansion for sudo
            if identity_file.startswith("~"):
                # Replace ~ with the sudo user's home
                identity_file = identity_file.replace("~", sudo_user_home, 1)
                logger.debug("Expanded identity file path for sudo user: %s", identity_file)
            
            # For relative paths that don't start with ~, still ensure they're under the user's home
            elif not os.path.isabs(identity_file):
                identity_file = os.path.join(sudo_user_home, identity_file)
                logger.debug("Converted relative path to absolute: %s", identity_file)
        else:
            # Normal expansion for regular user
            identity_file = os.path.expanduser(identity_file)
            
        # Convert to absolute path and store
        identity_path = Path(identity_file).absolute()
        options["ssh_identity_file"] = str(identity_path)
        
        # Validate the identity file
        if not identity_path.exists():
            logger.warning("SSH identity file does not exist: %s", identity_path)
            if running_as_sudo:
                logger.warning("When running with sudo, ensure the path is correct and accessible to root")
                logger.warning("Try using absolute path: /home/%s/.ssh/your_key", sudo_user)
        elif not os.access(identity_path, os.R_OK):
            logger.warning("SSH identity file is not readable: %s", identity_path)
            if running_as_sudo:
                logger.warning("Check file permissions: chmod 644 %s", identity_path)
        else:
            logger.info("Using SSH identity file: %s", identity_path)

    return options


def run_task(options, queue=None):
    """Create a list of tasks to run."""
    # Set up process-specific logger if queue is provided
    if queue is not None:
        try:
            verbosity = options.get("verbosity", "INFO").upper()
            log = setup_logger(queue, verbosity)
            log.debug(f"Process {os.getpid()} logger initialized with queue")
        except Exception as e:
            print(f"Error setting up logger in process {os.getpid()}: {e}")
            traceback.print_exc()

    try:
        # Ensure options dict has all required keys with defaults
        if "source" not in options or not options["source"]:
            raise __util__.AbortError("No source specified")
        
        # Ensure destination path exists
        options.setdefault("destinations", [])
        
    except Exception as e:
        print(f"Error initializing task options: {e}")
        traceback.print_exc()
        raise __util__.AbortError(f"Failed to initialize task: {e}")

    apply_shortcuts(options)
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
            )
        except __util__.AbortError as e:
            logger.error(
                "Aborting snapshot transfer to %s due to exception.",
                destination_endpoint,
            )
            logger.debug("Exception was: %s", e)
            if (
                hasattr(destination_endpoint, "_is_remote")
                and destination_endpoint._is_remote
            ):
                if not options.get("ssh_sudo", False):
                    logger.error(
                        "For SSH destinations, try using --ssh-sudo if you're encountering permission issues"
                    )
                    logger.error(
                        "Remote user needs passwordless sudo rights for btrfs commands"
                    )
                    logger.error(
                        "Add this to remote /etc/sudoers: username ALL=(ALL) NOPASSWD: /usr/bin/btrfs"
                    )

    # Enforce retention for source and destination endpoints only once, at the end
    cleanup_snapshots(source_endpoint, destination_endpoints, options)


def setup_logger(queue, level):
    """Set up the logger with the appropriate verbosity."""
    # Simple QueueHandler setup - works reliably with multiprocessing
    qh = logging.handlers.QueueHandler(queue)
    # Get the logger instance
    log = logging.getLogger("btrfs-backup-ng")
    # Reset handlers
    log.handlers.clear()
    log.propagate = False
    # Add queue handler and set level
    log.addHandler(qh)
    try:
        log.setLevel(level)
    except (ValueError, TypeError):
        # Fallback to INFO if level is invalid
        print(f"Invalid log level: {level}, using INFO")
        log.setLevel(logging.INFO)
    # Return for confirmation
    return log


def apply_shortcuts(options):
    """Apply shortcuts for verbosity and snapshot settings."""
    if "quiet" in options:
        options["verbosity"] = "warning"
    # Only override if user did NOT supply --num-snapshots
    if "latest_only" in options and (
        "num_snapshots" not in options or options["num_snapshots"] == 0
    ):
        options["num_snapshots"] = 1


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
    if options["ssh_sudo"]:
        logger.info(
            "Using sudo for btrfs commands on remote hosts - ensure passwordless sudo is configured"
        )
    elif any(dest.startswith("ssh://") for dest in options.get("destinations", [])):
        logger.warning(
            "Using SSH destination without --ssh-sudo. This may fail if the remote user doesn't have permissions."
        )
        logger.warning("Consider adding --ssh-sudo if you encounter permission errors")
    logger.debug("Run 'btrfs subvolume sync' afterwards: %r", options["sync"])
    logger.debug(
        "Convert subvolumes to read-write before deletion: %r", options["convert_rw"]
    )
    logger.debug("Remove locks for given destinations: %r", options["remove_locks"])
    logger.debug("Skip filesystem checks: %r", options["skip_fs_checks"])
    logger.debug("Auto add locked destinations: %r", options["locked_destinations"])


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

    # Always resolve to absolute, normalized path
    source_abs = Path(options["source"]).expanduser().resolve(strict=False)
    snapshot_folder = options.get("snapshot_folder", ".btrfs-backup-ng/snapshots")
    snapshot_root = Path(snapshot_folder).expanduser()
    if not snapshot_root.is_absolute():
        # Make relative to the source subvolume
        snapshot_root = source_abs.parent / snapshot_root
    snapshot_root = snapshot_root.resolve(strict=False)

    # Recreate the full directory structure of the source under snapshot_root
    relative_source = str(source_abs).lstrip(os.sep)
    snapshot_dir = snapshot_root.joinpath(*relative_source.split(os.sep))

    # Ensure the snapshot directory exists with correct permissions (e.g., 0o700)
    snapshot_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    source_endpoint_kwargs["path"] = snapshot_dir

    try:
        source_endpoint = endpoint.choose_endpoint(
            str(source_abs),  # Always pass the normalized absolute path
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
    # Only add locked destinations if the option is set
    if options.get("locked_destinations"):
        add_locked_destinations(source_endpoint, options)

    if options["no_transfer"] and options["num_backups"] <= 0:
        logger.debug("Skipping destination endpoint creation.")
        return []

    destination_endpoints = []
    endpoint_kwargs = build_endpoint_kwargs(options)
    # Ensure 'path' is NOT in endpoint_kwargs
    endpoint_kwargs.pop("path", None)

    logger.info("Preparing destination endpoints...")
    for destination in options["destinations"]:
        logger.info("Setting up destination: %s", destination)
        try:
            # Create the destination endpoint
            logger.debug("Creating destination endpoint object for: %s", destination)
            try:
                destination_endpoint = endpoint.choose_endpoint(
                    destination,
                    endpoint_kwargs,
                    source=False,
                )

                # Log endpoint details for debugging
                logger.debug("Created destination endpoint: %s", destination_endpoint)
                logger.debug("Endpoint type: %s", type(destination_endpoint).__name__)
                logger.debug(
                    "Endpoint path: %s", destination_endpoint.config.get("path")
                )
            except NameError as e:
                logger.error("NameError in choose_endpoint: %s", e)
                logger.error("Missing import? %s", str(e))
                raise __util__.AbortError(f"NameError in endpoint setup: {str(e)}")

            # Prepare the endpoint (creates directories, etc.)
            logger.debug("Preparing endpoint...")
            try:
                destination_endpoint.prepare()
                logger.debug("Endpoint preparation successful")
            except Exception as e:
                logger.error("Failed to prepare destination endpoint: %s", e)
                logger.error(
                    "Destination endpoint preparation error details: %s",
                    traceback.format_exc(),
                )
                raise __util__.AbortError(f"Failed to prepare destination: {str(e)}")

            destination_endpoints.append(destination_endpoint)
            logger.info("Destination endpoint ready: %s", destination_endpoint)

        except ValueError as e:
            logger.error("Couldn't parse destination specification: %s", e)
            raise __util__.AbortError
        except NameError as e:
            logger.error("Import error setting up destination %s: %s", destination, e)
            logger.error("This is likely a missing import in one of the modules")
            logger.error("Error details: %s", traceback.format_exc())
            raise __util__.AbortError(
                f"Import error for destination {destination}: {str(e)}"
            )
        except Exception as e:
            logger.error(
                "Unexpected error setting up destination %s: %s", destination, e
            )
            logger.error("Error details: %s", traceback.format_exc())
            if "permission denied" in str(e).lower() and "ssh://" in destination:
                logger.error(
                    "For SSH destinations with permission issues, use --ssh-sudo to run commands as root on the remote host"
                )
                logger.error(
                    "Ensure the remote user has passwordless sudo rights for btrfs commands"
                )
                if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
                    logger.error(
                        "You are running with sudo, which may be causing SSH key access issues."
                    )
                    logger.error(
                        "Try using --ssh-identity-file to specify your SSH key explicitly"
                    )
            raise __util__.AbortError(
                f"Failed to set up destination {destination}: {str(e)}"
            )

    return destination_endpoints


def build_endpoint_kwargs(options):
    """Build common kwargs for endpoints."""
    kwargs = {
        "snap_prefix": options.get("snapshot_prefix", f"{os.uname()[1]}-"),
        "convert_rw": options["convert_rw"],
        "subvolume_sync": options["sync"],
        "btrfs_debug": options["btrfs_debug"],
        "fs_checks": not options["skip_fs_checks"],
        "ssh_opts": options["ssh_opt"],
        "ssh_sudo": options["ssh_sudo"],
        # DO NOT include 'path' here!
    }
    
    # Include SSH username if specified
    if "ssh_username" in options and options["ssh_username"]:
        kwargs["username"] = options["ssh_username"]
        logger.info("Using explicitly configured SSH username: %s", options["ssh_username"])

    # Include SSH identity file if specified
    if "ssh_identity_file" in options and options["ssh_identity_file"]:
        identity_file = options["ssh_identity_file"]
        kwargs["ssh_identity_file"] = identity_file
        logger.info("SSH identity file configured: %s", identity_file)
        
        # When running as sudo, add special options to help with auth
        if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
            # Preserve SSH agent forwarding if available
            ssh_auth_sock = os.environ.get("SSH_AUTH_SOCK")
            if ssh_auth_sock:
                logger.debug("SSH agent socket found: %s", ssh_auth_sock)
                kwargs.setdefault("ssh_opts", []).append(f"IdentityAgent={ssh_auth_sock}")
            
            # Add debug flag for more verbose SSH output
            if options.get("debug", False):
                kwargs.setdefault("ssh_opts", []).append("LogLevel=DEBUG")

    logger.debug("Built endpoint kwargs: %s", kwargs)
    # Log SSH-specific options for better debugging
    if options["ssh_opt"]:
        logger.debug("SSH options: %s", options["ssh_opt"])
    if options["ssh_sudo"]:
        logger.debug("SSH sudo enabled: %s", options["ssh_sudo"])
        logger.info(
            "Using sudo for remote commands - ensure remote user has passwordless sudo rights for btrfs commands"
        )

    return kwargs


def add_locked_destinations(source_endpoint, options):
    """Add locked destinations to the options if not already present."""
    for snap in source_endpoint.list_snapshots():
        for lock in snap.locks:
            if lock not in options["destinations"]:
                options["destinations"].append(lock)


def remove_locks(source_endpoint, options):
    """Remove locks from the source endpoint."""
    logger.info("Removing locks (--remove-locks) ...")
    for snap in source_endpoint.list_snapshots():
        for destination in options["destinations"]:
            if destination in snap.locks:
                logger.info("  %s (%s)", snap, destination)
                source_endpoint.set_lock(snap, destination, False)
            if destination in snap.parent_locks:
                logger.info("  %s (%s) [parent]", snap, destination)
                source_endpoint.set_lock(snap, destination, False, parent=True)


def take_snapshot(source_endpoint, options):
    """Take a snapshot on the source endpoint and enforce retention."""
    logger.info(__util__.log_heading("Transferring ..."))
    snapshot = source_endpoint.snapshot()
    # Enforce retention immediately after snapshot creation
    if options.get("num_snapshots", 0) > 0:
        try:
            source_endpoint.delete_old_snapshots(options["num_snapshots"])
        except Exception as e:
            logger.debug("Error while deleting source snapshots: %s", e)
    return snapshot


def transfer_snapshots(source_endpoint, destination_endpoints, options):
    """Transfer snapshots to the destination endpoints."""
    logger.info(__util__.log_heading("Transferring ..."))

    if not options["no_snapshot"]:
        snapshot = take_snapshot(source_endpoint, options=options)
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
            )
        except __util__.AbortError as e:
            logger.error(
                "Aborting snapshot transfer to %s due to exception.",
                destination_endpoint,
            )
            logger.debug("Exception was: %s", e)


def serve_logger_thread(queue) -> None:
    """Run the logger from a thread in main to talk to all children."""
    print("Logger thread started")
    while True:
        try:
            record = queue.get()
            if record is None:
                print("Logger thread received shutdown signal")
                break
            logger.handle(record)
        except Exception as e:
            print(f"Error in logger thread: {e}")
            traceback.print_exc()
            break
    print("Logger thread shutting down")


def main() -> None:
    """Main function."""
    # Check if we're running with sudo and provide guidance
    if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
        sudo_user = os.environ.get("SUDO_USER")
        print(f"Running as root (via sudo from user {sudo_user}).")
        print(
            "NOTE: When running with sudo, your regular SSH keys may not be accessible."
        )
        print("If connecting to SSH destinations fails, try one of these approaches:")
        print("  1. Use --ssh-identity-file to specify your SSH private key explicitly")
        print("  2. Run without sudo and configure appropriate permissions")
        print("  3. Use ssh-agent forwarding with sudo")
        print("")

    # Check for SSH destinations in argv and add a recommendation
    if any("ssh://" in arg for arg in sys.argv):
        print("NOTE: SSH destination detected. For btrfs operations on remote systems:")
        print(
            "  - Use --ssh-sudo if the remote user doesn't have direct btrfs permissions"
        )
        print(
            "  - Ensure the remote user has passwordless sudo rights for btrfs commands"
        )
        print("  - Example sudoers entry: username ALL=(ALL) NOPASSWD: /usr/bin/btrfs")
        print("")

    global_parser = argparse.ArgumentParser(add_help=False)
    group = global_parser.add_argument_group("Global Display settings")
    group.add_argument(
        "-l",
        "--live-layout",
        default=False,
        action="store_true",
        help="EXPERIMENTAL - Display a Live layout interface.",
    )
    group.add_argument(
        "-v",
        "--verbosity",
        default="info",
        choices=["debug", "info", "warning", "error"],
        help="Set verbosity level. Default is 'info'.",
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

    # pylint: disable=consider-using-join
    command_line = ""
    for arg in sys.argv[1:]:
        command_line += f"{arg} "  # Assume no space => no quotes

    tasks = [task.split() for task in command_line.split("::")]

    task_options = [parse_options([global_parser], task) for task in tasks]

    # Determine if we're using a live layout
    live_layout = False
    for options in task_options:
        if options.get("live_layout"):
            live_layout = True
            break

    # Create a shared logger for all child processes
    create_logger(live_layout)
    # Setup queue for cross-process logging
    queue = multiprocessing.Manager().Queue(-1)
    # Create and start logger thread
    logger_thread = threading.Thread(target=serve_logger_thread, args=(queue,))
    logger_thread.daemon = True  # Make thread daemon so it doesn't block program exit
    logger_thread.start()
    # Configure main process logger
    level = task_options[0].get("verbosity", "INFO").upper()
    setup_logger(queue, level)
    logger.debug("Main process logger initialized")

    try:
        if live_layout:
            do_live_layout(tasks, task_options, queue)
        else:
            do_logging(tasks, task_options, queue)
    finally:
        # Ensure logger thread gets shutdown signal
        logger.info("All tasks completed, shutting down logger")
        queue.put_nowait(None)
        # Wait for logger thread to finish processing all messages
        logger_thread.join(timeout=5.0)
        print("Logger thread joined")


def do_logging(tasks, task_options, queue) -> None:
    """Execute tasks output only logging."""
    futures = []  # keep track of the concurrent futures
    try:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=min(8, len(tasks))
        ) as executor:
            # Submit all tasks, passing queue to each
            for n in range(len(tasks)):
                logger.debug(f"Submitting task {n+1}/{len(tasks)}")
                futures.append(executor.submit(run_task, task_options[n], queue))

            # Wait for all futures to complete
            logger.info(f"Waiting for {len(futures)} tasks to complete")
            concurrent.futures.wait(futures)
            logger.info("All tasks completed")
    except (__util__.AbortError, KeyboardInterrupt):
        logger.error("Process aborted by user or error")
        sys.exit(1)


def do_live_layout(tasks, task_options, queue) -> None:
    """Execute tasks using rich live layout."""
    logger.debug("Starting live layout with queue")
    layout = Layout(name="root")

    layout.split(
        Layout(name="header", size=5),
        Layout(name="main"),
        Layout(name="footer", size=3),
    )

    layout["main"].split_row(
        Layout(name="tasks"),
        Layout(name="logs", ratio=2, minimum_size=80),
    )

    layout["header"].update(
        Panel(
            Align.center(
                Text(
                    """btrfs-backup-ng\n\nIncremental backups for the btrfs filesystem.""",
                    justify="center",
                ),
                vertical="middle",
            ),
        ),
    )

    overall_progress = Progress(
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        SpinnerColumn(),
        TimeElapsedColumn(),
    )

    tasks_progress = Progress(
        "{task.description}",
        BarColumn(),
        SpinnerColumn(),
        TimeElapsedColumn(),
    )

    overall_task_id = overall_progress.add_task(
        "[green]All jobs progress:",
        total=len(tasks),
    )

    log = RichLogger()

    layout["tasks"].update(Panel(tasks_progress))
    layout["logs"].update(Panel(Text("\n".join(log.messages))))
    layout["footer"].update(Panel(overall_progress))

    futures = []  # keep track of the concurrent futures
    futures_id_map = {}  # associate a task_id with futures

    try:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=min(8, len(tasks))
        ) as executor:
            for n in range(len(tasks)):
                logger.debug(f"Submitting live layout task {n+1}/{len(tasks)}")
                futures.append(executor.submit(run_task, task_options[n], queue))
                task_id = tasks_progress.add_task(
                    f"[red]task: [cyan]{task_options[n]['source']}",
                    total=None,
                )
                futures_id_map[futures[n]] = task_id

            with Live(layout, console=cons):
                while not overall_progress.finished:
                    layout["logs"].update(Panel(Text("\n".join(log.messages))))
                    done, _ = concurrent.futures.wait(futures, timeout=1)
                    for future in done:
                        layout["logs"].update(Panel(Text("\n".join(log.messages))))
                        task_id = futures_id_map[future]
                        tasks_progress.update(
                            task_id,
                            total=1,
                            completed=1,
                        )
                        overall_progress.update(
                            overall_task_id,
                            advance=1,
                        )
                        futures.remove(future)
                        time.sleep(1)
                overall_progress.update(
                    overall_task_id,
                    completed=len(tasks),
                )
                time.sleep(1)
                layout["logs"].update(Panel(Text("\n".join(log.messages))))
                logger.debug("Live layout completed all tasks")
        # Signal is sent in main() after this function returns
    except (__util__.AbortError, KeyboardInterrupt):
        logger.error("Live layout aborted by user or error")
        sys.exit(1)
