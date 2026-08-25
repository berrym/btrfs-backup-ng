# Replacing a snapshot at a restore destination

`--overwrite` does not replace anything. It reports that existing snapshots were
left alone and the restore continues, bringing back whatever is missing.

This document records why, because the reason is a constraint on any future
attempt rather than a decision that can simply be reversed.

## What replacing would require

`btrfs receive` names the subvolume it creates after the source. Receiving onto
a name that already exists fails:

```
ERROR: creating subvolume <name> failed: File exists
```

— and it fails at the *end*, after the whole snapshot has crossed the wire. So
replacing means removing the existing copy first.

There is no way to stage the replacement and swap it in. Both halves were tested
against real btrfs:

* A received subvolume **cannot be moved**, even to a different parent under the
  same name: `mv` returns `Read-only file system`.
* It cannot be made writable to allow that: `btrfs property set ro false` refuses
  with *"cannot flip ro-&gt;rw with received_uuid set"*, and forcing it discards the
  `received_uuid` that incremental send depends on.

The snapper path publishes through a rename because its layout gives every
snapshot its own directory, and it renames the *directory* around the subvolume.
A restore destination holds the subvolume directly, so there is nothing to rename
around it.

The destination therefore holds **neither copy** for the duration of the
transfer, and that window cannot be closed without changing where restored
snapshots live.

## Why the window was not acceptable

The argument for accepting it was that a restore only ever reads the backup, so
an interruption costs a retry rather than data. Four adversarial passes each
found a way for it to cost data instead:

| Found | What it meant |
|---|---|
| The delete ran before the lock was taken | A network failure that never moved a byte destroyed the copy |
| The space check ran after the delete | The copy was destroyed for a shortage the tool could already detect |
| Replacing one snapshot deleted its whole ancestor chain | Subvolumes the operator never named, against consent that spoke of one |
| A silently declined delete reported success | The snapshot streamed in full and failed at the end with `File exists` |
| **Every check of whether the backup could be delivered ran after the delete** | A corrupt backup cost the last good copy, and the advice was to re-run — which failed identically every time |

The first four were fixed. The last is the one that settled it: the failure is
permanent loss of the copy the operator asked to refresh, and it was reproduced
on real btrfs through the real CLI.

A safety property that needs four adversarial passes and is still producing
deletion-before-verification is not a safety property.

## What was kept

The last finding produced something worth keeping regardless. `preflight_send`
exposes the read-side checks `send` already makes — a corrupt stream, a missing
decompressor, an unsupported cipher — so **every** restore now discovers an
undeliverable backup before streaming rather than during it. That is a smaller
benefit than preventing data loss, but it is real and it costs nothing.

## The constraint for a future design

Any future `--overwrite` must satisfy one rule:

> The deletion must not precede the proof that the replacement can be delivered.

Two shapes could satisfy it. Prove deliverability completely up front — for a
raw source that is achievable, since the stream and its sealed checksum are both
on disk; for a btrfs source it means proving the send will run, which is harder.
Or change the destination layout so a snapshot lives inside its own directory,
making the snapper-style rename available and removing the window entirely — a
larger change that affects every existing destination.

Until one of those exists, removing the snapshot by hand and restoring again is
the supported way to replace one, and it keeps the decision with the operator.
