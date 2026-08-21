# Remote Locks

Locks that outlive the process that took them, so a restore reading a snapshot
on a remote target cannot have that snapshot deleted out from under it by a
prune running somewhere else.

## The problem

A restore pins the snapshot it is reading. A prune consults those pins and skips
what is pinned. Both halves worked. They were looking at different things.

The pin lived in the restoring process's memory. A prune is usually a different
process — a cron job, a second terminal, a scheduler on another machine — and it
lists the target fresh. Every snapshot it sees therefore has an empty lock set,
including the one being read at that moment. The guard could not see the other
process, so it let the delete through, and `btrfs receive` failed partway with a
stream whose parent had vanished.

`restore --status` reported this state as:

```
This target does not persist locks.
```

Accurate, and no use to anyone. A backup tool that reports it cannot protect a
restore is not protecting the restore.

## Two kinds of lock

The distinction matters more than any other detail here.

**A snapshot pin is shared.** It says "someone is reading this; do not delete
it". Any number of restores and transfers may pin the same snapshot at once,
because two reads do not conflict, and it stays pinned until the last of them
lets go. This mirrors the in-memory contract exactly: `snapshot.locks` is a
*set* of lock ids, and the local lock file stores it as a list.

**A whole-target lock is exclusive.** It says "this target is being modified;
wait". A prune takes it. Only one holder at a time.

An early version of this made snapshot pins exclusive too. It looked correct in
isolation and was wrong: a second restore of the same snapshot failed against
the first, a concurrency regression the local path never had. If the two kinds
are ever collapsed again, that is the failure to expect.

## The approach

The lock is recorded on the target, beside the data it protects, so any process
that can reach the target can see it. There is no persistent connection to hold
an `flock` on, so the mechanism is built from POSIX operations that are already
atomic on the remote filesystem:

* **`mkdir` is atomic.** Exactly one of any number of racing creators wins; the
  rest get `EEXIST`. This is the whole mutual-exclusion primitive.
* **`mv` is atomic within a filesystem.** This is what makes breaking a dead
  lock safe: a contender that judges a lock stale renames it and proceeds only
  if the rename succeeded, so two contenders that both see the same dead lock
  cannot both go on to acquire it.

Both were verified against a real remote with 20 concurrent contenders: one
winner in each case.

### Layout

```
<target>/.btrfs-backup-ng.locks/
    target.lock/                  EXCLUSIVE: a whole-target lock
        info.json                 holder: operation, hostname, pid, token
        heartbeat                 mtime refreshed while the holder lives
    snap-<name>.lock/             SHARED: a pin on one snapshot
        holders/
            restore_abc           one file per holder; its mtime is that
            transfer_9            holder's own heartbeat
```

The `snap-` prefix is what separates a pin on a snapshot from a lock on the
whole target. One constant drives it, so the writer, the delete guards and
`restore --status` cannot drift apart.

### Names on disk are encoded, identity is not

A lock's directory is named `<sanitised>-<digest>`, where the digest is of the
exact original name. The sanitised part keeps `ls` readable; the digest makes
the mapping injective.

That second half is not cosmetic. An earlier version replaced every unsafe
character with `_` and used the result as the identity, which broke twice:

* `restore:x/y` and `restore:x:y` became the same file, so one holder releasing
  removed the other's pin and the snapshot it protected became deletable while
  still in use.
* The writer stored a rewritten snapshot name while the guard looked its
  snapshots up by their real ones, so a snapshot whose name needed rewriting was
  pinned under a name nothing would ever ask for. The pin existed and was
  invisible.

The lock's real name therefore travels inside its payload, and the directory
name is treated as nothing more than a filesystem-safe address. Reads key off
the payload; the guard additionally matches the exact directory name, so a lock
whose payload cannot be parsed — the one case with no name to offer — still
blocks.

For the same reason the remote emits only names, never paths. It sent full paths
at first, split off a space-delimited line, so a target directory containing a
space produced a truncated path that the sweeper handed to `rm -f`: it deleted
something unrelated, reported success, and left the real holder in place. Paths
are rebuilt on this side from the root the manager already knows.

A holder writes and removes only its own file. That is what makes the pin
reference-counted without any counter: the snapshot is locked while any holder
file is fresh, one holder releasing cannot drop another's pin, and the empty
directories are tidied with `rmdir`, which fails harmlessly while anyone else is
still there. The last one out cleans up.

### Exclusive acquisition is one round trip

Try `mkdir`; if it fails, judge staleness; if stale, break it with `mv` and try
again. This is deliberately a single script rather than a sequence of calls:
split across round trips, another contender can slip between the staleness check
and the break, which is exactly the race the atomic rename exists to close. The
evaluation cannot be moved to the client for the same reason.

Shared pins need none of this. There is nothing to win, so acquiring one is a
single write with no contention to resolve.

### One window that had to be closed

Between the `mkdir` that wins an exclusive lock and the `touch` that writes its
first heartbeat, the heartbeat does not exist. Ageing a missing file from epoch
zero makes that brand-new lock look infinitely old, so a contender arriving
inside the window judged it dead, broke it, and took a lock somebody already
held -- two winners. It surfaced as an intermittent failure of the one-winner
test and was then reproduced exactly by creating the directory without its
heartbeat.

The age now falls back to the lock directory's own mtime, which `mkdir` sets
atomically, so a lock taken microseconds ago cannot read as abandoned. The
inverse still holds: a genuinely old lock is still broken.

### Staleness is judged from the target's clock

The holder refreshes its file every 30 seconds. Age is `remote_now -
remote_mtime`, both read **from the target** — its own `date`, its own `stat`.
No client clock is ever compared to a remote one.

Listing emits those raw facts and does the arithmetic in Python. Conditional
logic in a script that must behave identically under bash, dash and busybox ash
is where this project has shipped bugs before, and listing — unlike acquisition
— has no race to lose, so there is nothing to gain by deciding on the far side.
A test runs the whole protocol under every POSIX shell present and requires the
results to match.

This is not hypothetical tidiness. The two machines used to develop this feature
disagreed by four seconds. With client-side arithmetic, two hosts would reach
different verdicts about the same lock, and the one with the fast clock would
break locks that were alive.

A lock whose heartbeat is older than 180 seconds is treated as a leftover from a
process that died: an exclusive lock is broken by the next contender, and a
stale holder file stops counting as a pin. A crashed restore therefore costs one
staleness window, not a permanent outage requiring manual cleanup.

Abandoned holder files are deleted only once they are past *twice* the
threshold. A live holder refreshes six times inside one threshold, so nothing
that far behind can still be alive — and deleting somebody's live pin is far
worse than leaving a small file lying around.

An interrupted run does not wait for any of that. Pins are released on normal
exit and on SIGINT/SIGTERM, so Ctrl-C on a restore frees the snapshot at once.
The signal handlers chain to whatever was installed before them rather than
replacing it. The stale window remains the backstop for what no handler can
catch: SIGKILL, a power cut, a severed network.

## What each side does

### Taking a lock

`Endpoint.set_lock` records the pin in memory *and* on the target. The in-memory
set is still maintained because the transfer and prune logic within a single run
reads it directly, and a remote round trip per query would turn a prune into a
conversation.

If the lock **cannot** be recorded, the operation stops by default. It does not
continue with a warning. Continuing would leave the restore running with no
protection at all while a prune elsewhere sees nothing holding the snapshot —
the exact failure this exists to prevent, with a log line where the protection
should be. The error names what to grant:

```
Could not lock root.20240115T120000 on this destination: ...
Refusing to continue unprotected: another process pruning this target would not
see the snapshot as in use and could delete it while it is being read. Make the
destination writable by the account running this, allow that account to elevate
for it, or pass --skip-remote-lock to proceed unprotected on purpose.
```

### `--skip-remote-lock`

One real setup is not served by that default: a destination the operator can
read but not write. No lock can be recorded there, and none is needed, because
nothing running as that account will be deleting from it either.

`--skip-remote-lock` is how the operator says so. It relaxes exactly one thing:
the abort above becomes a warning that states plainly that the run is
unprotected. It does **not** stop anything from *reading* locks. Every guard
still consults the target, so nothing begins reporting a target as unlocked
without having looked — the false all-clear this whole mechanism exists to
remove.

A target may also carry `skip_remote_lock` in its config, for a destination
that is permanently read-only. The flag wins when both are present.

Failing to *clear* a lock is not the same risk and is only reported: the
heartbeat stops, the lock goes stale, and the next contender breaks it.

### Deleting

Every remote delete path asks the target which snapshots are locked before
deleting anything, and skips those. The answer covers locks taken by any
process, which is the point.

Three outcomes, kept distinct because they demand different responses:

| Outcome | Meaning | Response |
|---|---|---|
| a set of locked names | the target answered | skip those, delete the rest |
| `RemoteLockBusy` | another process holds it | wait or come back later |
| `RemoteLockUnavailable` | the question could not be asked | delete nothing, say so |

The third is the one worth dwelling on. Neither available answer would be
honest. "Nothing is locked" prunes on an unanswered question and can delete the
snapshot someone is restoring. "Everything is locked" silently skips every
deletion, which is how retention stops running while the operator is told the
prune succeeded. So it raises, and each caller turns it into the report it
already has for a prune that could not run.

### Reporting

`restore --status` and `--unlock` go through `Endpoint._read_locks` and
`_write_locks`. Remote endpoints implement both against the target, so those
commands describe a remote target as accurately as a local one:

```
Active Locks:
----------------------------------------
  Restore locks (from restore operations):
    root.20240115T120000: session abc123
```

A lock whose `info.json` cannot be parsed is still reported, with the holder
shown as `unknown`. "Something holds this and we cannot say what" must never be
rounded down to "nothing holds this".

`--unlock` clears leftover snapshot pins. It addresses holders by the record
they were found in rather than by lock id, so it can clear the one holder it
cannot name — the one whose payload is unreadable. Addressed by id, that holder
survived `--unlock all` forever and the only remedy was deleting files on the
target by hand.

It deliberately leaves whole-target locks alone: those belong to an operation
running right now, and the command exists to clear leftovers, not to interrupt
work in progress.

## Which targets persist locks

| Target | Persists | Where |
|---|---|---|
| local | yes | lock file beside the backups |
| `ssh://` | yes | lock directory on the remote target |
| `raw+ssh://` | yes | lock directory on the remote target |
| local `raw://` | no | in memory, for the run only |

`persists_locks` states this per endpoint, and a test asserts the flag cannot
drift from what `set_lock` actually does.

## Permissions

Locks live beside the data they protect, so the lock directory inherits that
directory's permissions. Backup destinations are commonly root-owned. The
unprivileged attempt is made first; if the lock directory cannot be created, the
same script is retried elevated.

A directory that cannot be created is reported as such and never as contention.
A bare `mkdir` failure looks exactly like losing a race, and reporting it that
way sends an operator hunting for a competing process that does not exist.

If neither attempt works, the operation stops with the message shown above
rather than proceeding unprotected.

## Tuning

| Setting | Default | Meaning |
|---|---|---|
| heartbeat interval | 30s | how often a holder refreshes its lock |
| stale threshold | 180s | age at which a lock is treated as abandoned |
| sweep threshold | 360s | age at which an abandoned holder file is deleted |

The threshold is six heartbeats. A holder has to miss six consecutive refreshes
before another process will break its lock, which tolerates transient network
trouble without leaving a dead lock in place for long.

## Verification

The protocol is exercised against a real filesystem rather than a mock of its
replies — a mock would pass whatever the protocol did. Unit tests run the real
scripts against a local sandbox; the properties below were also confirmed
between two physical machines, with the restore on one host and the prune on
another:

* two restores pin the same snapshot at once, both recorded, both reported
* a prune in a separate process cannot delete a snapshot a restore holds
* the same prune *does* delete an unlocked snapshot, so the refusal is specific
  rather than a prune that was broken
* the prune says which snapshots it skipped and why
* the pin survives one holder leaving and lifts only when the last one goes
* protection lifts when the restore releases
* a killed restore leaves a lock that the next contender breaks after the
  staleness window, rather than locking the target out permanently
* a third process reads the lock state correctly via `restore --status`
* SIGINT frees a pin immediately rather than after the staleness window
* an abandoned pin stops blocking after that window and is then swept
