"""btrfs-backup-ng: btrfs-backup/__main__.py

Backup a btrfs volume to another, incrementally
Requires Python >= 3.3, btrfs-progs >= 3.12 most likely.

Copyright (c) 2023 Michael Berry <trismegustis@gmail.com>
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

import logging
import os
import subprocess
import sys
import time
from math import inf

from . import endpoint
from . import util


SNAPSHOT = None


def send_snapshot(
    snap, destination_endpoint, parent=None, clones=None, no_progress=False
):
    """
    Sends snapshot to destination endpoint, using given parent and clones.
    It connects the pipes of source and destination together and shows
    progress data using the pv command.
    """

    # Now we need to send the snapshot (incrementally, if possible)
    logging.info("Sending %s ...", snap)
    if parent:
        logging.info("  Using parent: %s", parent)
    else:
        logging.info("  No parent snapshot available, sending in full mode.")
    if clones:
        logging.info("  Using clones: %s", clones)

    pv = False
    if not no_progress:
        # check whether pv is available
        logging.debug("Checking for pv ...")
        cmd = ["pv", "--help"]
        logging.debug("Executing: %s", cmd)
        try:
            subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except FileNotFoundError as e:
            logging.debug("  -> got exception: %s", e)
            logging.debug("  -> pv is not available")
        else:
            logging.debug("  -> pv is available")
            pv = True

    pipes = [snap.endpoint.send(snap, parent=parent, clones=clones)]

    if pv:
        cmd = ["pv"]
        logging.debug("Executing: %s", cmd)
        pipes.append(
            subprocess.Popen(cmd, stdin=pipes[-1].stdout, stdout=subprocess.PIPE)
        )

    pipes.append(destination_endpoint.receive(pipes[-1].stdout))

    pids = [pipe.pid for pipe in pipes]
    while pids:
        pid, return_code = os.wait()
        if pid in pids:
            logging.debug("  -> PID %d exited with return code %d", pid, return_code)
            pids.remove(pid)
        if return_code != 0:
            logging.error("Error during btrfs send / receive")
            raise util.SnapshotTransferError()


def sync_snapshots(
    source_endpoint,
    destination_endpoint,
    keep_num_backups=0,
    no_incremental=False,
    **kwargs,
):
    """
    Synchronizes snapshots from source to destination. Takes care
    about locking and deletion of corrupt snapshots from failed transfers.
    It never transfers snapshots that would anyway be deleted afterward
    due to retention policy.
    """

    global SNAPSHOT
    logging.info(util.log_heading(f"  To {destination_endpoint} ..."))

    source_snapshots = source_endpoint.list_snapshots()
    destination_snapshots = destination_endpoint.list_snapshots()
    destination_id = destination_endpoint.get_id()

    # delete corrupt snapshots from destination
    to_remove = []
    for SNAPSHOT in source_snapshots:
        if SNAPSHOT in destination_snapshots and destination_id in SNAPSHOT.locks:
            # seems to have failed previously and is present at
            # destination; delete corrupt snapshot there
            destination_snapshot = destination_snapshots[
                destination_snapshots.index(SNAPSHOT)
            ]
            logging.info(
                "Potentially corrupt snapshot %s found at %s",
                destination_snapshot,
                destination_endpoint,
            )
            to_remove.append(destination_snapshot)
    if to_remove:
        destination_endpoint.delete_snapshots(to_remove)
        # refresh list of snapshots at destination to have deleted ones
        # disappear
        destination_snapshots = destination_endpoint.list_snapshots()
    # now that deletion worked, remove all locks for this destination
    for SNAPSHOT in source_snapshots:
        if destination_id in SNAPSHOT.locks:
            source_endpoint.set_lock(SNAPSHOT, destination_id, False)
        if destination_id in SNAPSHOT.parent_locks:
            source_endpoint.set_lock(SNAPSHOT, destination_id, False, parent=True)

    logging.debug("Planning transmissions ...")
    to_consider = source_snapshots
    if keep_num_backups > 0:
        # it wouldn't make sense to transfer snapshots that would be deleted
        # afterward anyway
        to_consider = to_consider[-keep_num_backups:]
    to_transfer = [
        SNAPSHOT for SNAPSHOT in to_consider if SNAPSHOT not in destination_snapshots
    ]

    if not to_transfer:
        logging.info("No snapshots need to be transferred.")
        return

    logging.info("Going to transfer %d snapshot(s):", len(to_transfer))
    for _ in to_transfer:
        logging.info("  %s", SNAPSHOT)

    while to_transfer:
        if no_incremental:
            # simply choose the last one
            best_snapshot = to_transfer[-1]
            parent = None
            clones = []
        else:
            # pick the snapshots common among source and destination,
            # exclude those that had a failed transfer before
            present_snapshots = [
                SNAPSHOT
                for SNAPSHOT in source_snapshots
                if SNAPSHOT in destination_snapshots
                and destination_id not in SNAPSHOT.locks
            ]

            # choose snapshot with the smallest distance to its parent
            def key(s):
                p = s.find_parent(present_snapshots)
                if p is None:
                    return inf
                d = source_snapshots.index(s) - source_snapshots.index(p)
                return -d if d < 0 else d

            best_snapshot = min(to_transfer, key=key)
            parent = best_snapshot.find_parent(present_snapshots)
            # we don't use clones at the moment, because they don't seem
            # to speed things up
            # clones = present_snapshots
            clones = []
        source_endpoint.set_lock(best_snapshot, destination_id, True)
        if parent:
            source_endpoint.set_lock(parent, destination_id, True, parent=True)
        try:
            send_snapshot(
                best_snapshot,
                destination_endpoint,
                parent=parent,
                clones=clones,
                **kwargs,
            )
        except util.SnapshotTransferError:
            logging.info(
                "Keeping %s locked to prevent it from getting removed.", best_snapshot
            )
        else:
            source_endpoint.set_lock(best_snapshot, destination_id, False)
            if parent:
                source_endpoint.set_lock(parent, destination_id, False, parent=True)
            destination_endpoint.add_snapshot(best_snapshot)
            destination_snapshots = destination_endpoint.list_snapshots()
        to_transfer.remove(best_snapshot)

    logging.info(util.log_heading(f"Transfers to {destination_endpoint} complete!"))


def run(argv):
    """Run the program. Items in ``argv`` are treated as command line
    arguments."""

    description = """\
This provides incremental backups for btrfs filesystems. It can be
used for taking regular backups of any btrfs subvolume and syncing them
with local and/or remote locations. Multiple targets are supported as
well as retention settings for both source snapshots and backups. If
a snapshot transfer fails for any reason (e.g. due to network outage),
btrfs-backup will notice it and prevent the snapshot from being deleted
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
    parser = util.MyArgumentParser(
        description=description,
        epilog=epilog,
        add_help=False,
        fromfile_prefix_chars="@",
        formatter_class=util.MyHelpFormatter,
    )

    group = parser.add_argument_group("Display settings")
    group.add_argument(
        "-h", "--help", action="help", help="Show this help message and exit."
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
        action="store_true",
        help="Shortcut for '--no-progress --verbosity " "warning'.",
    )
    group.add_argument(
        "-d",
        "--btrfs-debug",
        action="store_true",
        help="Enable debugging on btrfs send / receive.",
    )
    group.add_argument(
        "-P",
        "--no-progress",
        action="store_true",
        help="Don't display progress and stats during backup.",
    )

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
        " Default is '.snapshots'.",
    )
    group.add_argument(
        "-p",
        "--snapshot-prefix",
        help="Prefix for snapshot names. Default is ''.",
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
        help="N|Pass extra ssh_config options to ssh(1).\n"
        "Example: '--ssh-opt Cipher=aes256-ctr --ssh-opt IdentityFile=/root/id_rsa'\n"
        "would result in 'ssh -o Cipher=aes256-ctr -o IdentityFile=/root/id_rsa'.",
    )
    group.add_argument(
        "--ssh-sudo",
        action="store_true",
        help="Execute commands with sudo on the remote host.",
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
        help="N|Subvolume to backup.\n"
        "The following schemes are possible:\n"
        " - /path/to/subvolume\n"
        " - ssh://[user@]host[:port]/path/to/subvolume\n"
        "Specifying a source is mandatory.",
    )
    group.add_argument(
        "destination",
        nargs="*",
        default=[],
        help="N|Destination to send backups to.\n"
        "The following schemes are possible:\n"
        " - /path/to/backups\n"
        " - ssh://[user@]host[:port]/path/to/backups\n"
        " - 'shell://cat > some-file'\n"
        "You may use this argument multiple times to transfer backups to multiple locations."
        " You may even omit it "
        "completely in what case no snapshot is transferred at all. That allows, for instance, "
        "for well-organized local snapshotting without backing up.",
    )

    try:
        args = parser.parse_args(argv)
    except RecursionError as e:
        print(
            "Recursion error while parsing arguments.\n"
            "Maybe you produced a loop in argument files? "
            f"Caught: ({e})",
            file=sys.stderr,
        )
        raise util.AbortError()

    # applying shortcuts
    if args.quiet:
        args.no_progress = True
        args.verbosity = "warning"
    if args.latest_only:
        args.num_snapshots = 1

    logging.basicConfig(
        format="%(asctime)s  [%(levelname)-5s]  %(message)s",
        datefmt="%H:%M:%S",
        level=getattr(logging, args.verbosity.upper()),
    )

    logging.info(util.log_heading(f"Started at {time.ctime()}"))

    logging.debug(util.log_heading("Settings"))
    if args.snapshot_folder:
        snap_dir = args.snapshot_folder
    else:
        snap_dir = ".snapshots"

    if args.snapshot_prefix:
        snap_prefix = args.snapshot_prefix
    else:
        snap_prefix = str("")

    logging.debug("Enable btrfs debugging: %r", args.btrfs_debug)
    logging.debug("Don't display progress: %r", args.no_progress)
    logging.debug("Don't take a new snapshot: %r", args.no_snapshot)
    logging.debug("Number of snapshots to keep: %d", {args.num_snapshots})
    logging.debug(
        "Number of backups to keep: %s",
        (args.num_backups if args.num_backups > 0 else "Any"),
    )
    logging.debug("Snapshot folder: %s", snap_dir)
    logging.debug("Snapshot prefix: %s", snap_prefix if snap_prefix else None)
    logging.debug("Don't transfer snapshots: %r", args.no_transfer)
    logging.debug("Don't send incrementally: %r", args.no_incremental)
    logging.debug("Extra SSH config options: %s", args.ssh_opt)
    logging.debug("Use sudo at SSH remote host: %r", args.ssh_sudo)
    logging.debug("Run 'btrfs subvolume sync' afterwards: %r", args.sync)
    logging.debug(
        "Convert subvolumes to read-write before deletion: %r", args.convert_rw
    )
    logging.debug("Remove locks for given destinations: %r", args.remove_locks)
    logging.debug("Skip filesystem checks: %r", args.skip_fs_checks)
    logging.debug("Auto add locked destinations: %r", args.locked_destinations)

    # kwargs that are common between all endpoints
    endpoint_kwargs = {
        "snap_prefix": snap_prefix,
        "convert_rw": args.convert_rw,
        "subvolume_sync": args.sync,
        "btrfs_debug": args.btrfs_debug,
        "fs_checks": not args.skip_fs_checks,
        "ssh_opts": args.ssh_opt,
        "ssh_sudo": args.ssh_sudo,
    }

    logging.debug("Source: %s", args.source)
    source_endpoint_kwargs = dict(endpoint_kwargs)
    source_endpoint_kwargs["path"] = snap_dir
    try:
        source_endpoint = endpoint.choose_endpoint(
            args.source, source_endpoint_kwargs, source=True
        )
    except ValueError as e:
        logging.error("Couldn't parse source specification: %s", e)
        raise util.AbortError()
    logging.debug("Source endpoint: %s", source_endpoint)
    source_endpoint.prepare()

    # add endpoint creation strings for locked destinations, if desired
    if args.locked_destinations:
        for snap in source_endpoint.list_snapshots():
            for lock in snap.locks:
                if lock not in args.destination:
                    args.destination.append(lock)

    if args.remove_locks:
        logging.info("Removing locks (--remove-locks) ...")
        for snap in source_endpoint.list_snapshots():
            for destination in args.destination:
                if destination in snap.locks:
                    logging.info("  %s (%s)", snap, destination)
                    source_endpoint.set_lock(snap, destination, False)
                if destination in snap.parent_locks:
                    logging.info("  %s (%s) [parent]", snap, destination)
                    source_endpoint.set_lock(snap, destination, False, parent=True)

    destination_endpoints = []
    # only create destination endpoints if they are needed
    if args.no_transfer and args.num_backups <= 0:
        logging.debug(
            "Don't create destination endpoints because they won't be needed "
            "(--no-transfer and no --num-backups)."
        )
    else:
        for destination in args.destination:
            logging.debug("Destination: %s", destination)
            try:
                destination_endpoint = endpoint.choose_endpoint(
                    destination, endpoint_kwargs, source=False
                )
            except ValueError as e:
                logging.error("Couldn't parse destination specification: %s", e)
                raise util.AbortError()
            destination_endpoints.append(destination_endpoint)
            logging.debug("Destination endpoint: %s", destination_endpoint)
            destination_endpoint.prepare()

    if args.no_snapshot:
        logging.info("Taking no snapshot (--no-snapshot).")
    else:
        # First we need to create a new snapshot on the source disk
        logging.info(util.log_heading("Snapshotting ..."))
        source_endpoint.snapshot()

    if args.no_transfer:
        logging.info(util.log_heading("Not transferring (--no-transfer)."))
    else:
        logging.info(util.log_heading("Transferring ..."))
        for destination_endpoint in destination_endpoints:
            try:
                sync_snapshots(
                    source_endpoint,
                    destination_endpoint,
                    keep_num_backups=args.num_backups,
                    no_incremental=args.no_incremental,
                    no_progress=args.no_progress,
                )
            except util.AbortError as e:
                logging.error(
                    "Aborting snapshot transfer to %s due to exception.",
                    destination_endpoint,
                )
                logging.debug("Exception was: %s", e)
        if not destination_endpoints:
            logging.info("No destination configured, don't sending anything.")

    logging.info(util.log_heading("Cleaning up..."))
    # cleanup snapshots > num_snapshots in snap_dir
    if args.num_snapshots > 0:
        try:
            source_endpoint.delete_old_snapshots(args.num_snapshots)
        except util.AbortError as e:
            logging.debug(
                "Got AbortError while deleting source snapshots at %s", source_endpoint
            )
    # cleanup backups > num_backups in backup target
    if args.num_backups > 0:
        for destination_endpoint in destination_endpoints:
            try:
                destination_endpoint.delete_old_snapshots(args.num_backups)
            except util.AbortError as e:
                logging.debug(
                    "Got AbortError while deleting backups at %s", destination_endpoint
                )

    logging.info(util.log_heading(f"Finished at {time.ctime()}"))


def main():
    """Main function."""
    try:
        run(sys.argv[1:])
    except (util.AbortError, KeyboardInterrupt):
        sys.exit(1)


# Program entry point
if __name__ == "__main__":
    main()
