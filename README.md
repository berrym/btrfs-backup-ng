# btrfs-backup-ng

This project supports incremental backups for *btrfs* using *snapshots*
and *send/receive* between filesystems. Think of it as a basic version
of Time Machine.

Backups can be stored locally and/or remotely (e.g. via SSH).
Multi-target setups are supported as well as dealing with transmission
failures (e.g. due to network outage).

Its main goals are to be **reliable** and **functional** while
maintaining **user-friendliness**. It should be easy to get started in
just a few minutes without detailed knowledge on how btrfs send/receive
works. However, you should have a basic understanding of snapshots and
subvolumes.

btrfs-backup-ng has almost no dependencies and hence is well suited for
many kinds of setups with only minimal maintenance effort.

This project is a fork of btrfs-backup written by Chris Lawrence,
and since then maintained by Robert Schindler, this codebase is
written and maintained by Michael Berry.

Originally, it started as a fork of a project with the same name,
written by Chris Lawrence. Since then, most of the code has been
refactored and many new features were added before this repository has
been transferred to Robert Schindler. Many thanks to Chris for his work. The old code
base has been tagged with `legacy`. If, for any reason, you want to
continue using it and miss the new features, you can check that out.

Latest release  
v0.5.9

Downloads  
<http://pypi.python.org/pypi/btrfs_backup_ng>

Source  
<https://github.com/berrym/btrfs-backup-ng>

Platforms  
Linux >= 3.12, Python >= 3.6

Keywords  
backup, btrfs, snapshot, send, receive, ssh

## Features

-   Initial creation of full backups
-   Incremental backups on subsequent runs
-   Different backup storage engines:
    -   Local storage
    -   Remote storage via SSH
    -   Custom storage: Alternatively, the output of `btrfs send` may be
        piped to a custom shell command.
-   Multi-target support with tracking of which snapshots are missing at
    each location.
-   Retransmission on errors (e.g. due to network outage).
-   Simple and configurable retention policy for local and remote
    snapshots
-   Optionally, create snapshots without transferring them anywhere and
    vice versa.
-   Creation of backups without root privileges, if some special
    conditions are met
-   Detailed logging output with configurable log level
-   Concurrent process execution of tasks

## Installation

### Requirements

-   Python 3.8 or later
-   Appropriate btrfs-progs; typically you'll want **at least** 3.12
    with Linux 3.12/3.13
-   (optional) OpenSSH's `ssh` command - needed for remote backup
    pulling and pushing via SSH
-   (optional) `sshfs` - only needed for pulling via SSH

### Install via PIP

The easiest way to get up and running with the latest stable version is
via PIP. If `pip3` is missing on your system, and you run a Debian-based
distribution, simply install it via:

    $ sudo apt-get install python3-pip python3-wheel

Then, you can fetch the latest version of btrfs-backup-ng:

    $ sudo pip3 install btrfs-backup-ng

### Pre-built packages

There are currently pre-built packages available for Fedora and OpenSUSE Tumbleweed:

Fedora 38, Fedora 39, Fedora Rawhide

    $ dnf copr enable mberry/btrfs-backup-ng
    $ dnf install btrfs-backup-ng --refresh

OpenSUSE Tumbleweed

    $ sudo zypper addrepo https://download.opensuse.org/repositories/home:berrym/openSUSE_Tumbleweed/home:berrym.repo
    $ sudo zypper refresh
    $ zypper install btrfs-backup-ng

### Manual installation

Note: This package now uses a pyproject.toml based build as outlined in
PEP 517 and PEP 621.

Clone this git repository

    $ git clone https://github.com/berrym/btrfs-backup-ng.git
    $ cd btrfs-backup-ng
    $ git checkout tags/v0.5.9  # optionally checkout a specific version
    $ python3 -m venv /path/to/btrfs-backup-ng/venv # optionally use venv
    $ sh /path/to/btrfs-backup-ng/venv/bin/activate # using venv
    $ python3 -m build
    # using venv
    $ python3 -m pip install .
    # or 
    $ sudo python3 -m pip install .

## Sample usage

Not every feature of btrfs-backup-ng is explained in this README, since
there is a detailed and descriptive help included with the command.

However, there are some sections about the general concepts and
different sample usages to get started as quick as possible.

For reference, a copy of the output of `btrfs-backup-ng --help` is attached
below.

As root (if not root btrfs-backup-ng will try and re-run itself with sudo):

    $ btrfs-backup-ng /home /backup

This will create a read-only snapshot of `/home` in
`/home/.snapshots/YYMMDD-HHMMSS`, and then send it to
`/backup/YYMMDD-HHMMSS`. On future runs, it will take a new read-only
snapshot and send the difference between the previous snapshot and the
new one.

**Note: Both source and destination need to be on btrfs filesystems.
Additionally, the source has to be either the root or any other
subvolume, but not just an ordinary directory because snapshots can only
be created of subvolumes.**

For the backup to be sensible, source and destination shouldn't be the
same filesystem. Otherwise, you could just snapshot and save the hassle.

You can back up multiple subvolumes to multiple sub-folders or subvolumes
at the destination. For example, you might want to back up both `/` and
`/home`. The main caveat is you'll want to put the backups in separate
folders on the destination drive to avoid confusion.

    $ btrfs-backup-ng / /backup/root
    $ btrfs-backup-ng /home /backup/home

If you really want to store backups of different subvolumes at the same
location, you have to specify a prefix using the `-p/--snapshot-prefix`
option. Without that, btrfs-backup-ng can't distinguish between your
different backup chains and will mix them up. Using the example from
above, it could look like the following:

    $ btrfs-backup-ng --snapshot-prefix root / /backup
    $ btrfs-backup-ng --snapshot-prefix home /home /backup

You can specify `-N/--num-snapshots <num>` to only keep the latest
`<num>` number of snapshots on the source filesystem.
`-n/--num-backups <num>` does the same thing for the backup location.

### Remote backups

Backing up to a remote server via SSH is as easy as:

    $ btrfs-backup-ng /home ssh://server/mnt/backups

btrfs-backup-ng doesn't need to be installed on the remote side for this to
work. It is recommended to set up public key authentication to eliminate
the need for entering passwords. A full description of how to customize
the `ssh` call can be found in the help text.

Pulling backups from a remote side is now supported as well! Simply use
the `ssh://` scheme as source.

You could even do something like:

    $ btrfs-backup-ng ssh://source_server/home ssh://destination_server/mnt/backups

to pull backups from `source_server` and store them at `destinstation_server`.
This might be used if you can't install btrfs-backup-ng on either remote
host for any reason. But keep in mind that this procedure will generate
double traffic (from `source_server` to you and from you to
`destination_server`).

Okay, just one last example, because I really like that one:

    $ btrfs-backup-ng ssh://source_server/home \
                   /mnt/backups \
                   ssh://dest_server/mnt/backups

Can you guess what it does? Well, it does the same as the command
before + an extra sending to your local `/mnt/backups` folder. Please
note that btrfs-backup-ng is not smart enough to prevent the same data from
being pulled from `source_server` twice. But that wouldn't be easy to
implement with the current design.


### Concurrent tasks

Multiple btrfs-backup-ng tasks can be run in concurrent processes by
separating the desired tasks with a ':'

    $ btrfs-backup-ng /home /mnt/backups/home:/opt /mnt/backups/opt

or a similarly chained command. Each task will have its own set of options.


## Help text

This is the output of `btrfs-backup-ng --help`. Taking a look at it, you
should get a good insight in what it can and can't do (yet).

    Coming at the release.

## Configuration files

By default, btrfs-backup-ng doesn't read any configuration file. However,
you can create one or more and specify them at the command line:

    $ btrfs-backup-ng @path/to/backup_home.conf

Any argument prefixed by a `@` is treated as file name of a
configuration file.

The format of these files is simple. On every line, there may be one
flag, option or argument you would normally specify at the command line.
Valid configuration files might look like the following.

`backup_home.conf`:

    # This is a comment and thus ignored, as well as blank lines.

    # Include another configuration file here.
    @global.conf

            # Indentation has no effect.
            -p home

    # This is the source.
    /home

    # Back up to both local and remote storage.
    /mnt/backups/home
    ssh://server/mnt/btrfs_storage/backups/home

`global.conf`:

    # This file gets included by the other one.
    --quiet

    --num-snapshots 1
    --num-backups 3

A more detailed explanation about the format can be found in the help
text.

## What are locks?

btrfs-backup-ng uses so-called "locks" to keep track of failed snapshot
transfers. There is a file called `.outstanding_transfers` created in
the snapshot folder. This file is in JSON format and thus
human-readable, if necessary.

Locking works as follows:

1.  When a snapshot transfer is started, an entry is created in that
    file, telling that a snapshot transfer of a specific snapshot to a
    specific destination has begun. We call this entry a lock.
2.  If the snapshot transfer used another snapshot as parent, that one
    gets an entry as well, but no lock, just the note that it's a parent
    for something that failed to transfer.
3.  When the transfer
    1.  finishes without errors, the locks for the snapshot (and its
        parent) are removed.
    2.  aborts (e.g. due to network outage or a full disk), the locks
        are kept.

Now, there are multiple options for dealing with those failed transfers.

When you run btrfs-backup-ng the next time, it finds the corrupt snapshot
at the destination and deletes it, together with the corresponding lock
and parent notes. Afterward, the way is free for a new transfer. You
may also use `--no-snapshot` to only do the transfers without creating
new snapshots.

There is a special flag called `--locked-destinations` available. If supplied,
it automatically adds all destinations which locks exist for as if they
were specified at the command line. You might do something like:

    $ btrfs-backup-ng --no-snapshot --locked-destinations /home

to retry all failed backup transfers of snapshots of `/home`. This could
be executed periodically because it just does nothing if there are no
locks.

Snapshots for which locks or parent notes exist are excluded from the
retention policy and won't be purged until the locks are removed either
automatically (because the partially transferred snapshots could be
deleted from the destination) or manually (see below).

As a last resort for removing locks for transfers you don't want to
retry anymore, there is a flag called `--remove-locks`. Use it with
caution and only if you can assure that there are no corrupt snapshots
at the destinations you apply the flag on.

    $ btrfs-backup-ng --no-snapshot --no-transfer --remove-locks /home ssh://nas/backups

will remove all locks for the destination `ssh://nas/backups` from
`/home/.snapshots/.outstanding_transfers`. Of course, using
`--locked-destinations` instead of specifying the destination explicitly is
possible as well.

## Backing up regularly

Note that there is no locking included with btrfs-backup. If you back up
too often (i.e. more quickly than it takes the first call to finish,
which can take several minutes, hours or even days on a filesystem with
lots of files), you might end up with a new backup starting while an old
one is still in progress.

You can work around the lack of locking using the `flock(1)` command, as
suggested at <https://github.com/efficiosoft/btrfs-backup/issues/4>.

With anacron on Debian, you could simply add a file
`/etc/cron.daily/local-backup`:

``` sh
#!/bin/sh
flock -n /tmp/btrfs-backup-home.lock \
    ionice -c 3 btrfs-backup-ng --quiet --num-snapshots 1 --num-backups 3 \
                /home /backup/home
```

You may omit the `-n` flag if you want to wait rather than fail in case
a backup is already running.

More or less frequent backups could be made using other `cron.*`
scripts.

## Restoring a snapshot

If necessary, you can restore a whole snapshot by using e.g.

    $ mkdir /home/.snapshots
    $ btrfs send /backup/YYMMDD-HHMMSS | btrfs receive /home/.snapshots

Then you need to take the read-only snapshot and turn it back into a
root filesystem:

    $ cp -aR --reflink /home/.snapshots/YYMMDD-HHMMSS /home

You might instead have some luck taking the restored snapshot and
turning it into a read-write snapshot, and then re-pivoting your mounted
subvolume to the read-write snapshot.

## Alternative workflow

An alternative structure is to keep all subvolumes in the root directory

    /
    /active
    /active/root
    /active/home
    /inactive
    /.snapshots/root/YYMMDD-HHMMSS
    /.snapshots/home/YYMMDD-HHMMSS

and have corresponding entries in `/etc/fstab` to mount the subvolumes
from `/active/*`. One benefit of this approach is that restoring a
snapshot can be done entirely with btrfs tools:

    $ btrfs send /backup/root/YYMMDD-HHMMSS | btrfs receive /.snapshots/root
    $ btrfs send /backup/home/YYMMDD-HHMMSS | btrfs receive /.snapshots/home
    $ mv /active/root /inactive
    $ mv /active/home /inactive
    $ btrfs subvolume snapshot /.snapshots/root/YYMMDD-HHMMSS /active/root
    $ btrfs subvolume snapshot /.snapshots/home/YYMMDD-HHMMSS /active/home

The snapshots from btrfs-backup-ng may be placed in `/.snapshots` by using
the `--snapshot-folder` option.

## Issues and Contribution

As in every piece of software, there likely are bugs. When you find one,
please open an issue on GitHub. If you do so, please include the output
with debug log level (`-v debug`) and provide steps to reproduce the
problem. Thank you!

## Copyright

Copyright © 2023 Michael Berry <trismegustis@gmail.com>\
Copyright © 2017 Robert Schindler <r.schindler@efficiosoft.com>\
Copyright © 2014 Chris Lawrence <lawrencc@debian.org>

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![build result](https://build.opensuse.org/projects/home:berrym/packages/btrfs-backup-ng/badge.svg?type=default)](https://build.opensuse.org/package/show/home:berrym/btrfs-backup-ng)
[![Copr build status](https://copr.fedorainfracloud.org/coprs/mberry/btrfs-backup-ng/package/btrfs-backup-ng/status_image/last_build.png)](https://copr.fedorainfracloud.org/coprs/mberry/btrfs-backup-ng/package/btrfs-backup-ng/)
