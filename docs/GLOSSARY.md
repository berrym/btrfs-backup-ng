# Glossary

The words this project uses, each with its plain meaning, its technical
meaning, and where it lives in the code. Terms are grouped so that each group
explains itself; the index below points into the groups.

A term earns an entry when it appears in a message the tool prints, an option,
the changelog, or a docstring a maintainer has to read. An entry carries a code
excerpt only where the definition alone would leave a reasonable reader asking
"but how". Anything beyond that lives in the module docstrings, which the
entries point to rather than repeat. Every code reference in this document is
checked by `tests/test_glossary.py`: a reference must import, an excerpt must
still be in the file it names, and an on-disk format must be what the writer
produces today. The entries use three layers:

- **Plain**: one or two sentences anyone can follow.
- **Technical**: what the project means by the word, and what it does not mean.
- **Code**: the module and symbol, as `package.module:Symbol`, and where it
  helps, a short excerpt copied from the source.

## Index

[beat](#heartbeat) · [catch-up](#catch-up) · [correspondence](#correspondence)
· [dead](#stale-and-dead) · [destination](#source-and-destination) ·
[destination id](#lock-id-and-destination-id) · [directory store](#directory-store)
· [elevation](#elevation) · [evidence](#holder-record) · [guard](#guard) ·
[heartbeat](#heartbeat) · [holder](#holder) · [holder record](#holder-record) ·
[holders directory](#holder-record) · [incremental](#incremental-and-parent) ·
[ledger](#lock-file) · [lifecycle](#lifecycle-and-the-exit-drain) · [live](#stale-and-dead)
· [lock](#lock) · [lock file](#lock-file) · [lock id](#lock-id-and-destination-id)
· [marker](#run-marker) · [own pin](#own-pin-parent-pin-restore-pin) ·
[parent](#incremental-and-parent) · [parent pin](#own-pin-parent-pin-restore-pin)
· [pin](#pin) · [plan](#plan) · [policy](#retention-policy) · [prune](#prune) ·
[reconcile](#reconcile) · [restore pin](#own-pin-parent-pin-restore-pin) ·
[retention](#retention-policy) · [run lock](#run-lock) · [session](#restore-session)
· [sidecar](#sidecar) · [snapshot](#snapshot) · [source](#source-and-destination)
· [stale](#stale-and-dead) · [store](#directory-store) · [sweep](#sweep) ·
[target](#source-and-destination) · [target lock](#lock) ·
[unrecorded pin](#unrecorded-pin)

## Snapshots and transfers

A backup run lists the snapshots of a source, works out which of them a
destination is missing, and sends each missing one, as an incremental against a
parent the destination already holds where it can. The words below are the
ones that story uses.

### Snapshot

**Plain.** A frozen copy of a subvolume at one moment, named by its prefix and
timestamp, such as `home-20260101-120000`.

**Technical.** A read-only btrfs subvolume under a volume's `snapshot_dir`, or
on a raw target a stream file holding `btrfs send` output. A snapshot object
carries its name, its uuids where the endpoint can read them, and its two pin
sets, `locks` and `parent_locks`.

**Code.** `btrfs_backup_ng.__util__:Snapshot`.

### Source and destination

**Plain.** The source is where snapshots are taken; a destination is a place
they are sent to. A configured destination is called a target.

**Technical.** Both are endpoints. The source endpoint's `path` is the
snapshot directory; a destination endpoint's `path` is the backup location,
local (`/mnt/backup`), over ssh (`ssh://user@host:/backups`), raw
(`raw:///mnt/usb`) or raw over ssh. The destination's id, which is its path or
URI, is the name under which it pins snapshots on the source.

**Code.** `btrfs_backup_ng.endpoint.common:Endpoint`,
`btrfs_backup_ng.endpoint:choose_endpoint`,
`btrfs_backup_ng.core.target:parse_target`.

### Incremental and parent

**Plain.** An incremental sends only what changed since an earlier snapshot the
destination already has; that earlier snapshot is the parent.

**Technical.** `btrfs send -p PARENT SNAPSHOT`. The parent is chosen by the
planner and must be held by the destination by correspondence, never by name
alone. While the send runs the parent is pinned on the source under the
destination's id, as a parent pin.

**Code.** `btrfs_backup_ng.core.planning:plan_transfer_sequence`.

### Correspondence

**Plain.** The rule that says whether a snapshot on the destination is the
same snapshot as one on the source, rather than merely one with the same name.

**Technical.** For btrfs destinations, the destination copy's `received_uuid`
must equal the source snapshot's stream uuid; for raw destinations the stream
is found by its name and must have been sent from the same source, as far as
its sidecar and the source snapshot record it (a stream whose record names no
source is matched by name alone). Presence on a destination is decided by this
rule and nothing else, so a re-created snapshot with an old name is correctly
absent, and so is another volume's stream under a shared prefix. The planner
and the reconcile share one presence authority so they cannot disagree.

**Code.** `btrfs_backup_ng.endpoint.common:Endpoint.correspondent_of`,
`btrfs_backup_ng.core.planning:snapshots_present_on`.

From `src/btrfs_backup_ng/core/planning.py`:

```python
    Presence is decided purely by correspondence -- ``received_uuid == stream_uuid`` for
    btrfs, the name and the source the stream was sent from for raw -- via the polymorphic
    ``correspondent_of`` (which never raises; a listing failure yields None -> absent). This
    is the shared presence authority used by both the
```

### Plan

**Plain.** The ordered list of what a sync will send, and against which parent.

**Technical.** A list of `(snapshot, parent)` pairs, `parent` being `None` for
a full send, produced by one planner that decides order and parents; the
executor only carries it out.

**Code.** `btrfs_backup_ng.core.planning:plan_transfer_sequence`,
`btrfs_backup_ng.core.operations:sync_snapshots`.

### Reconcile

**Plain.** At the start of every sync, the source lets go of the pins this
destination no longer needs.

**Technical.** For each source snapshot: this destination's parent pin is
released whether or not the destination holds the snapshot, since a snapshot
the destination does not hold cannot be the base of an incremental to it; the
snapshot's own pin is released only when the destination holds the snapshot by
correspondence, and kept otherwise, because that transfer has not finished.

**Code.** `btrfs_backup_ng.core.operations:sync_snapshots`.

From `src/btrfs_backup_ng/core/operations.py`:

```python
    for snap in source_snapshots:
        if destination_id in snap.parent_locks:
            source_endpoint.set_lock(snap, destination_id, False, parent=True)
        if snap.get_name() not in present_names:
            continue  # not yet on the destination -> keep its own lock
        if destination_id in snap.locks:
            source_endpoint.set_lock(snap, destination_id, False)
```

### Catch-up

**Plain.** When a target has been away and is missing many snapshots, `run`
sends only the ones the target's own retention would keep.

**Technical.** `run` prunes each target right after transferring to it, so the
same retention decision is asked first, of the target's snapshots plus the
missing ones; only the missing ones it keeps are selected, each sent against
the newest earlier snapshot the target holds. A snapshot this destination
holds an own pin on is always selected, since that transfer did not finish;
a parent pin does not count, since the reconcile releases it.

**Code.** `btrfs_backup_ng.cli.run:_catch_up_selector`.

## Locks and pins

Two different things keep operations from treading on each other, and the
words for them are kept apart on purpose. A **lock** gives one holder the
right to do something, such as receive into a target, and excludes everyone
else. A **pin** keeps one snapshot from being deleted while someone still
needs it, and any number of holders may pin the same snapshot. The story: a
run to two targets lists the source, pins each snapshot it sends under the
target's id and its parent under the same id, sends, and releases both pins
when the transfer completes. If the run is interrupted the pins stay, so the
source cannot prune a snapshot a later run still has to send; the next run's
reconcile releases what the destination turns out to hold.

### Lock

**Plain.** An exclusive right. Whoever holds the target lock may receive into
that target; nobody else may until it is released.

**Technical.** An exclusive lock on a location, taken by `mkdir` of a lock
directory, held by an `owner.<token>` file whose mtime is the holder's
heartbeat, and released by removing only that file. The target lock
(`target.lock`) covers a prune of the whole location; a receiving lock
(`receiving-<dest>.lock`) covers the creation of one subvolume. A stale lock
may be broken by the next contender, and a holder that was stalled past the
threshold finds its record gone and leaves the successor's lock alone. A lock
is not a pin, and the word is not used for pins outside the on-disk key names
`locks` and `parent_locks`, which predate the distinction.

**Code.** `btrfs_backup_ng.sshutil.lock:RemoteLockManager.acquire_once`,
`btrfs_backup_ng.sshutil.lock:RemoteLockManager.release`,
`btrfs_backup_ng.sshutil.lock:RemoteLockBusy`.

### Pin

**Plain.** A note on a snapshot saying "do not delete this yet; something
still needs it."

**Technical.** An id under a snapshot's name in a location's lock state, keyed
by who needs the snapshot: a destination id for a transfer, `restore:<session>`
for a restore. Retention skips a pinned snapshot, and a deletion reads the
lock state again at delete time before it decides. Pins are shared: several
holders may pin one snapshot, and a release drops only the releasing holder's
own pin. A pin in a lock file survives the process that took it, on purpose.

**Code.** `btrfs_backup_ng.endpoint.common:Endpoint.set_lock`,
`btrfs_backup_ng.endpoint.common:Endpoint.delete_snapshots`.

From `src/btrfs_backup_ng/endpoint/common.py`, the deletion's check:

```python
            if snapshot.locks or snapshot.parent_locks:
                logger.info("Skipping locked snapshot: %s", snapshot)
                result.skip(snapshot, "held by a retention lock")
                continue
```

### Own pin, parent pin, restore pin

**Plain.** Three reasons a snapshot is pinned: it is being sent, it is the
base another snapshot is being sent against, or a restore is reading it.

**Technical.** An **own pin** is a destination id under `locks`: this
destination is transferring this snapshot, and an interrupted transfer keeps
it until a later run completes the transfer. A **parent pin** is a destination
id under `parent_locks`: this snapshot is the parent of an incremental to that
destination, released at the end of the transfer and by every reconcile. A
**restore pin** is `restore:<session>` under `locks` on the backup location a
restore reads from; it has no life after the restore's process, which is why
doctor may remove one whose holder is gone and why `restore --unlock` exists.

**Code.** `btrfs_backup_ng.core.operations:_execute_transfers`,
`btrfs_backup_ng.cli.restore:_execute_unlock`.

### Lock id and destination id

**Plain.** The name a pin is filed under: who needs the snapshot.

**Technical.** A lock id is a string. For a transfer it is the destination id,
which is the destination endpoint's path or URI (`/mnt/backup`,
`ssh://user@host:/backups/home`, `raw:///mnt/usb`); for a restore it is
`restore:` followed by the session. No lock id carries a process id or a host;
who holds a pin is recorded beside it, in a holder record.

**Code.** `btrfs_backup_ng.endpoint.common:Endpoint.get_id`.

### Unrecorded pin

**Plain.** A pin this process holds in memory only, because the lock file
could not be written.

**Technical.** `set_lock` refuses to continue unprotected unless the location
is mounted read-only or `--skip-remote-lock` was given; in those two cases the
pin is kept in memory and added back after the delete-time re-read, so this
process cannot delete what it pinned, while another process cannot see it.

**Code.** `btrfs_backup_ng.endpoint.common:Endpoint._unrecorded_pins`.

## Where pins and locks live

A local btrfs location keeps a **lock file**, a JSON ledger of pins, with the
**holder records** beside it that say who took each pin. A location reached
over ssh, a raw location, and a local mirror of an ssh location keep a
**directory store** instead, one directory per lock or pin with one record per
holder; there the record is the pin. Both are written only through the
location's own writers, and both are read again at delete time.

### Lock file

**Plain.** The file at a local location that lists which snapshots are pinned
and by whom. It is the ledger of pins.

**Technical.** `.btrfs-backup-ng.locks` in the snapshot directory of a source,
or at a local backup location: a JSON object mapping each snapshot name to its
`locks` and `parent_locks` lists. It is written only through the guarded
by-change writer, which applies one change to the entry as the file holds it
at that moment and writes every other entry back as read, through an atomic
replace. A corrupt or unreadable file refuses every deletion rather than
reading as "no pins". The format has not changed and is read by every
version. The name of the file is configurable as `lock_file_name`. In prose
the file may be called the ledger, which names its role; the file itself is
the lock file.

**Code.** `btrfs_backup_ng.endpoint.common:Endpoint._update_lock_entry`,
`btrfs_backup_ng.__util__:read_locks`, `btrfs_backup_ng.__util__:write_locks`.

On disk (`.btrfs-backup-ng.locks`), as `write_locks` produces it for one
snapshot pinned by a local target, a restore, and as the parent of an
incremental to an ssh target:

```json
{
    "home-20260101-120000": {
        "locks": [
            "/mnt/backup",
            "restore:3f8a1c2e"
        ],
        "parent_locks": [
            "ssh://user@host:/backups/home"
        ]
    }
}
```

In Python the same state is `{"home-20260101-120000": {"locks": [...],
"parent_locks": [...]}}`, and a snapshot object carries its own two lists as
the sets `snapshot.locks` and `snapshot.parent_locks`.

From `src/btrfs_backup_ng/endpoint/common.py`, the one change `set_lock`
applies to an entry:

```python
        def change(entry: Dict[str, List[Any]]) -> Dict[str, List[Any]]:
            held = entry.get(key, [])
            if lock_state:
                if lock_id not in held:
                    held.append(lock_id)
            else:
                held = [other for other in held if other != lock_id]
            entry[key] = held
            return entry
```

### Guard

**Plain.** The file next to the lock file that makes writers take turns.

**Technical.** `<lock file>.guard`, a `FileLock` taken around every read,
change and write of the lock file, so threads and processes writing the same
file cannot lose each other's changes. The guard is a mutex over the writers;
it is not a lock in the sense above and holds no state of its own.

**Code.** `btrfs_backup_ng.endpoint.common:Endpoint._update_lock_entry`.

From `src/btrfs_backup_ng/endpoint/common.py`:

```python
        guard = str(self._get_lock_file_path()) + ".guard"
        with FileLock(guard):
            lock_dict = self._read_locks()
```

### Holder

**Plain.** Whoever took a pin or a lock: one process, on one host.

**Technical.** A process identified by the hostname and pid in its record,
holding a pin or a lock for as long as it refreshes the record's heartbeat.
In code a `Holder` is one live record as the listing found it: the directory
and file it lives in and its parsed payload, so that a record whose payload
cannot be read can still be addressed and removed.

**Code.** `btrfs_backup_ng.sshutil.lock:Holder`.

### Holder record

**Plain.** A small file that says who holds a pin and proves they are still
there by being touched regularly.

**Technical.** One file per holder per pin, named by an encoding of the lock
id, whose content is the holder's payload and whose mtime is the holder's
heartbeat. In a directory store the record is the pin. Beside a lock file the
records live in `<lock file>.holders`, written by the same machinery when a pin
is taken and removed when it is released; there the record is evidence of who
took the pin and whether they are still at it, and the lock file remains the
protection. A record beside a lock file outlives its process as the pin does,
and its beat stops with the process, so doctor can tell a pin at work from one
whose holder is gone. A record that cannot be written is a warning, never a
failed pin.

**Code.** `btrfs_backup_ng.sshutil.lock:RemoteLockManager.acquire_shared`,
`btrfs_backup_ng.sshutil.lock:holders_beside`,
`btrfs_backup_ng.sshutil.lock:RemoteLockManager.keep_record_past_exit`,
`btrfs_backup_ng.endpoint.common:Endpoint._record_holder`.

From `src/btrfs_backup_ng/sshutil/lock.py`, how a record beside a lock file
is kept past the exit cleanups:

```python
        _unregister_for_cleanup(self, self._held_key(name, lock_id))
```

On disk, the holders directory beside a lock file, and one record's payload:

```text
.btrfs-backup-ng.locks                   the lock file
.btrfs-backup-ng.locks.guard             the guard
.btrfs-backup-ng.locks.holders/          the holder records
    snap-home-20260101-120000-<hash>.lock/
        holders/
            restore_3f8a1c2e-<hash>      one record per holder; its mtime is the beat
```

```json
{"name": "snap-home-20260101-120000", "lock_id": "restore:3f8a1c2e", "operation": "restore:3f8a1c2e", "hostname": "fedora", "pid": 4211, "shared": true}
```

From `src/btrfs_backup_ng/sshutil/lock.py`, the payload as written:

```python
                "name": name,
                "lock_id": str(lock_id),
                "operation": operation or str(lock_id),
                "hostname": self._hostname or "unknown",
                "pid": os.getpid(),
                "shared": True,
```

The lifecycle of a pin in a lock file and of its record:

```text
set_lock(True)
   |
   v
pin in the lock file  +  record beside it, beat every 30 s
   |                            |
   | set_lock(False)            | process ends (completed: pin released first;
   |                            |   interrupted or killed: pin and record stay)
   v                            v
pin and record removed     beat stops
                                |  180 s
                                v
                           holder gone (record stale)
                                |
                   +------------+------------------+
                   |            |                  |
             restore pin    parent pin        own pin
             doctor --fix   doctor --fix,     kept for the next run;
             removes it     or the next       released by the reconcile
                            run's reconcile   once the destination holds
                                              the snapshot
```

### Directory store

**Plain.** The way a location reached over ssh, a raw location, or a local
mirror of one keeps its locks and pins: a directory per lock, a file per
holder, no file rewritten by several writers.

**Technical.** `.btrfs-backup-ng.locks/` under the location. An exclusive lock
is a directory whose `owner.<token>` file names its holder; a pin is a
directory `snap-<name>-<hash>.lock/holders/` with one record per holder.
Everything is read and written by short POSIX shell scripts that run the same
way under bash, dash and busybox, locally through `sh` or on the remote over
ssh, and the age of every record is judged by the target's own clock. A local
endpoint that finds the directory where its lock file would be uses the store
instead, so an ssh mirror and a local prune of it see the same pins.

**Code.** `btrfs_backup_ng.sshutil.lock:RemoteLockManager`,
`btrfs_backup_ng.sshutil.lock:local_manager`,
`btrfs_backup_ng.endpoint.common:Endpoint._lock_store_manager`.

On disk:

```text
<location>/.btrfs-backup-ng.locks/
    target-<hash>.lock/                 exclusive: the whole-target lock a prune holds
        owner.<token>                   the holder; its mtime is the heartbeat
        heartbeat                       refreshed beside it, for older versions
    receiving-<dest>-<hash>.lock/       exclusive: the right to create one subvolume
        owner.<token>
    snap-<name>-<hash>.lock/            shared: a pin on one snapshot
        holders/
            restore_abc-<hash>          one record per holder, payload as above
            _mnt_backup-<hash>
```

### Heartbeat

**Plain.** The regular touch that proves a holder is still alive. One touch is
a beat.

**Technical.** A thread per held lock or pin that, every
`DEFAULT_HEARTBEAT_INTERVAL` seconds, refreshes the holder's record: for a pin
it runs the script that wrote the record again, whole, so a record removed from
under a live holder returns with its identity; for an exclusive lock it touches
the owner file and the `heartbeat` beside it with `touch -c`, so a holder whose
lock was broken does not write itself into the successor's lock. A beat
elevates when the unprivileged attempt could not write, as the acquire did.

**Code.** `btrfs_backup_ng.sshutil.lock:RemoteLockManager._start_heartbeat`.

From `src/btrfs_backup_ng/sshutil/lock.py`:

```python
DEFAULT_HEARTBEAT_INTERVAL = 30
```

### Stale and dead

**Plain.** A record nobody has touched for three minutes is stale: its holder
is taken to be gone. One untouched for six minutes is dead and may be removed.

**Technical.** A record is **live** while its beat is younger than
`DEFAULT_STALE_AFTER` seconds by the target's clock; a live record blocks a
deletion and an exclusive lock with a live record is busy. A **stale** record
no longer blocks or excludes: a stale exclusive lock is broken by the next
contender, and doctor reports a stale pin's holder as gone. A record older
than `DEAD_HOLDER_MULTIPLE` times the threshold is **dead** and is swept. A
record whose age cannot be read, because the target's clock or the file's
mtime could not be read, is treated as live: "cannot tell" never rounds down to
"nobody holds this".

**Code.** `btrfs_backup_ng.sshutil.lock:RemoteLockManager.live_locks`,
`btrfs_backup_ng.sshutil.lock:RemoteLockManager.shared_records`,
`btrfs_backup_ng.core.doctor:Doctor._lock_file_findings`.

From `src/btrfs_backup_ng/core/doctor.py`, the one test that separates a
holder at work from one that is gone:

```python
                if age is None or age <= DEFAULT_STALE_AFTER:
```

From `src/btrfs_backup_ng/sshutil/lock.py`:

```python
DEFAULT_STALE_AFTER = 180
```

From `src/btrfs_backup_ng/sshutil/lock.py`:

```python
DEAD_HOLDER_MULTIPLE = 2
```

The lifecycle of a record in a directory store, where the record is the pin:

```text
acquire_shared
   |
   v
live: beat every 30 s, blocks deletion
   |                        |
   | release_shared         | beat stops
   v                        v  180 s
removed                  stale: no longer blocks; doctor reports the holder gone
                            |  360 s
                            v
                         dead: swept by the next release, by --unlock, or by doctor --fix
```

### Sweep

**Plain.** Removing dead records.

**Technical.** Removal of every holder record older than the dead threshold,
by the record it was found in rather than by a name, so a record whose payload
cannot be parsed is removed too. The sweep runs after `restore --unlock` and
after a whole-state reconcile of a store; `doctor --fix` removes dead records
one at a time by the same means. A removal elevates when the unprivileged
attempt could not write. A live holder refreshes its record six times within
one threshold, so a sweep cannot remove a pin that is held.

**Code.** `btrfs_backup_ng.sshutil.lock:RemoteLockManager.sweep_dead_holders`,
`btrfs_backup_ng.sshutil.lock:RemoteLockManager.release_holder`.

### Elevation

**Plain.** Using `sudo` for the one command that needs it, and only then.

**Technical.** The lock scripts run unprivileged first and are run again
through `sudo -n sh -c` only when the verdict says a directory or record could
not be created or written, which is a root-owned destination such as a mirror
received under `ssh_sudo`. The acquire, every heartbeat, every release and the
sweep elevate on that one rule, since a record the acquire wrote elevated is
root-owned for the whole of its life. Transfers elevate `btrfs` alone; a
sudoers policy that permits `btrfs` and nothing else refuses `sh`, and the
docs describe both.

**Code.** `btrfs_backup_ng.sshutil.lock:local_manager`,
`btrfs_backup_ng.endpoint.ssh:SSHEndpoint._build_lock_manager`.

## Retention

Retention decides which snapshots to keep and which to delete at a location,
by age buckets; a prune applies it. A pinned snapshot is skipped whatever the
policy says.

### Retention policy

**Plain.** How many snapshots to keep per hour, day, week, month and year,
and the minimum age before any may go.

**Technical.** A `RetentionConfig` with `min`, `hourly`, `daily`, `weekly`,
`monthly`, `yearly` and `keep`; the planner sorts snapshots into time buckets,
keeps the representative of each bucket the policy asks for, and returns the
kept and the deletable lists. A snapshot whose time cannot be read is
quarantined and kept. A policy that would delete nothing is degenerate; the
catch-up then leaves nothing out and sends everything the target is missing.

**Code.** `btrfs_backup_ng.config.schema:RetentionConfig`,
`btrfs_backup_ng.retention:apply_retention`,
`btrfs_backup_ng.cli.prune:plan_retention_of`,
`btrfs_backup_ng.cli.prune:is_degenerate_policy`.

### Prune

**Plain.** Deleting the snapshots retention says may go.

**Technical.** `prune`, and the prune `run` performs at the source after every
target succeeded and at each target after transferring to it. Deletion goes
through the endpoint's own `delete_snapshots`, which reads the lock state
again, skips what is pinned, asks a directory store at delete time, and
reports what it deleted, skipped and failed rather than inferring success from
the absence of an exception.

**Code.** `btrfs_backup_ng.cli.prune:execute_prune`,
`btrfs_backup_ng.endpoint.common:DeletionResult`.

## Restore

A restore reads snapshots from a backup location and receives them into a
destination, pinning what it reads on the backup location under its session
and marking what it writes at the destination.

### Restore session

**Plain.** The short id a restore runs under, such as `3f8a1c2e`.

**Technical.** Eight characters of a uuid, generated per restore. The restore
pins each snapshot it reads under `restore:<session>` on the backup location,
and `restore --unlock <session>` or `--unlock all` releases what an
interrupted restore left.

**Code.** `btrfs_backup_ng.core.restore:restore_snapshots`,
`btrfs_backup_ng.cli.restore:_cleared_by_unlock`.

From `src/btrfs_backup_ng/cli/restore.py`, what `--unlock` clears:

```python
    held = str(held)
    if lock_id == "all":
        return held == "unknown" or held.startswith("restore:")
    wanted = lock_id if lock_id.startswith("restore:") else f"restore:{lock_id}"
    return held == wanted
```

### Run marker

**Plain.** A note a restore leaves at the destination while it is writing a
subvolume, removed once the subvolume is verified.

**Technical.** `DEST/.btrfs-backup-ng/restore/<token>.json`, written before a
receive and removed after its verdict. A marker that outlives its process is
what `restore --cleanup` reads: it may remove the subvolume a marker names when
that subvolume has no `received_uuid`, and nothing else, so a cleanup can never
delete something this tool did not leave half-made.

**Code.** `btrfs_backup_ng.core.layout:PlainLayout`,
`btrfs_backup_ng.core.layout:read_markers`,
`btrfs_backup_ng.cli.restore:_execute_cleanup`.

### Sidecar

**Plain.** The small metadata file that sits beside every raw backup stream.

**Technical.** `<stream>.meta`, written with the stream on a raw target and
read back as the authoritative description of the stream: its snapshot name,
parent, compression and encryption, and the sealed checksum. The word belongs
to raw targets only; a holder record beside a lock file is not called a
sidecar.

**Code.** `btrfs_backup_ng.endpoint.raw_metadata:RawSnapshot`.

## The process

### Run lock

**Plain.** The lock that stops two runs of the same configuration from
overlapping.

**Technical.** A file lock in a runtime directory, keyed by a digest of the
resolved configuration path, taken for the whole of `run`. A second run is
refused while one holds it. If the runtime directory cannot be created the run
warns and proceeds without it, since a missing guard must not become an
outage.

**Code.** `btrfs_backup_ng.cli.run:_run_lock`, `btrfs_backup_ng.cli.run:_run_lock_path`.

From `src/btrfs_backup_ng/cli/run.py`:

```python
    Keyed by a digest of the resolved config path so two different
    configurations never share a lock, while the same configuration always
```

### Lifecycle and the exit drain

**Plain.** What happens when the program is told to stop: children are stopped
first, then every lock and pin that should be let go is let go.

**Technical.** A registry of exit cleanups by stage, drained at exit, on
`SIGTERM`, `SIGHUP` and `Ctrl-C`: child processes are stopped before any lock
is released, and a cleanup that did not complete is retried. Holders register
their releases when they take a lock or a pin in a directory store; a pin in a
lock file keeps its record past the drain on purpose, so the record can say
afterwards whose pin it was.

**Code.** `btrfs_backup_ng.lifecycle:register`, `btrfs_backup_ng.lifecycle:drain`.

## Terms we do not use

One word per concept. These are the words the code, the messages and the
docs avoid, and what to say instead. `tests/test_glossary.py` keeps the
retired phrases out of the source and the documentation.

- **sidecar** for a holder record. A sidecar is the `.meta` file beside a raw
  stream; a holder record is a holder record, and the directory holding them is
  the holders directory.
- **stale lock** as doctor's verdict. A lock id carries no process id, so no
  lock can be judged stale from the lock file alone; doctor reports a pin's
  holder as gone when the holder record's beat has stopped, and the check keeps
  its historical name `stale_locks` for `--check` and JSON consumers only.
- **ledger** in a message or a path. In prose it names the lock file's role;
  the file is the lock file, and the option is `lock_file_name`.
- **lock** for a pin, outside the on-disk keys `locks` and `parent_locks`.
  A pin keeps a snapshot; a lock excludes other holders.
- **remote lock** as a kind. The directory store is one thing wherever the
  location is, local, raw or over ssh. (`btrbk`'s own `lockfile` option keeps
  its name where the importer names it.)
