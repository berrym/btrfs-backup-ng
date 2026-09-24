# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`min = "all"` in a retention block** keeps every snapshot in that scope
  for ever: retention deletes nothing there, whatever the bucket counts or
  `keep` say. It is btrbk's `snapshot_preserve_min all`, and btrbk's
  default. Accepted at every scope (`[global.retention]`,
  `[volumes.retention]`, `[volumes.source_retention]`,
  `[volumes.targets.retention]`), inherited like any other key, and not
  a degenerate policy.

### Fixed

- **`config import` kept less than btrbk did.** Three faults in one command,
  each silent, each first noticed by a prune that deleted history btrbk was
  keeping. The lexer discarded any character it had no class for, so
  `snapshot_preserve 14d 8w *m` was read as `14d 8w m` and the `*m` (keep
  every monthly snapshot) became no monthly snapshots; a leading `~` in a
  path vanished the same way. A missing `snapshot_preserve_min` was written
  as `min = "1d"` and a missing `snapshot_preserve` as this tool's default
  buckets, while btrbk's defaults are no schedule and a minimum of `all`: a
  btrbk configuration that says nothing about retention keeps everything.
  And `target_preserve` / `target_preserve_min` were dropped with a warning
  that this tool "uses one retention per volume", which has not been true
  since retention became per target. Now a directive's value is the rest of
  its line, verbatim, as btrbk reads it; an absent or `all` minimum is
  `min = "all"`; `latest` and `no` are `0s`; a minimum the importer does not
  understand is `all` with a warning; each target's `target_preserve*`,
  resolved at the narrowest scope that sets it, becomes that target's own
  `[volumes.targets.retention]`, and a target that sets neither keeps every
  backup, as btrbk does. The one shape this tool cannot prune under -- no
  schedule and a minimum of a day or less, which in btrbk keeps only the
  newest snapshot -- gets the default schedule with a warning that it keeps
  more. An unterminated quote no longer swallows the rest of the file.
  btrbk's numbers are inclusive -- `snapshot_preserve 14d` keeps the first
  snapshot of each of days 0..14, fifteen days, and `snapshot_preserve_min
  2d` keeps a snapshot for the whole of the second calendar day back -- so
  every count and every `N<unit>` minimum is written one higher (`daily =
  15`, `min = "3d"`), which keeps at least what btrbk keeps; `0` still
  disables a period and `00`, true to btrbk, keeps the current one. Each
  generated retention block names the btrbk lines it came from and the
  one-higher rule, and the import says the rule once. Measured against
  btrbk 0.32.7 live, at every hour of the day and day of the week in four
  zones: the minimum and the hourly and daily counts never delete a
  snapshot btrbk keeps. The migration guide describes the mapping and the
  one remaining difference: btrbk starts weeks on `preserve_day_of_week`
  and months and years on the first such weekday, this tool on ISO Monday
  and the first of the month, so the first snapshot of a week can differ.
- **A `raw+ssh://` receive could stall on its own stderr.** The pipeline
  that writes the stream over ssh was started with a stderr pipe nobody
  read, so an ssh that said more than the 64 KiB pipe holds blocked, and
  the stall detector reported a transfer that stopped moving. The chunked
  `ssh://` receive read its stderr only after the process had exited, the
  same fault one step later. Both are now drained as they are written, like
  every other process an endpoint starts; a structural test over the
  endpoint modules refuses any new `Popen(stderr=PIPE)` without a drain,
  and a flood test per endpoint class proves the drain.
- **`restore --status` reported "Available snapshots: 0" for a location
  holding snapper backups.** Snapper backups are numbered slots, not
  prefix-named snapshots, so the prefix listing was empty however many
  backups the location held. The status now lists the snapper backups the
  way `snapper restore --list` does, with the pins a snapper restore holds
  on each, and says when the layout is there but could not be enumerated
  rather than printing zero.
- **`prune` did not prune snapper destinations.** A snapper destination
  holds numbered slots, not prefix-named snapshots, so `prune` listed it
  through the native endpoint, found nothing, reported "Keeping 0, deleting
  0" and deleted nothing -- while `run` pruned the same destination after
  transferring. `prune` now makes the same decision `run` makes
  (`plan_snapper_retention`, each target under its own policy, the
  destination opened with the same connection, encryption and compression
  options) and carries out the same deletion (`delete_snapper_backups`),
  local, `ssh://`, `raw://` and `raw+ssh://` alike. `--dry-run` lists the
  slots it would delete, with their dates, and deletes nothing; the
  confirmation prompt shows them the same way. The source is snapper's own
  timeline and is not pruned, as before. This is a behaviour change for a
  `prune` run on a snapper volume: it now deletes what `run` would.
- **The configuration wizards replaced options they had not asked about.**
  The carry-over that saves a wizard's answers over an existing file worked
  from one fixed list of "asked" keys per wizard, while the wizards ask
  conditionally. Declining "Configure global settings?" in `config detect
  --wizard` replaced `snapshot_dir`, `timestamp_format`, `incremental`, the
  parallel counts, the retention policy, `log_file` and the notifications
  with defaults; a `raw+ssh://` target lost its `ssh_sudo` (the sudo
  question is asked only for `ssh://`) and a target outside `/mnt` its
  `require_mount`; a snapper volume re-entered through `config init -i`
  became native and lost `[volumes.snapper]`; webhook headers and timeouts
  were dropped; two volumes sharing a path collapsed into one; `--force`
  skipped the carry-over along with the question; a changed answer was not
  reported; and the diff summary compared the wizard's raw answers instead
  of what would be written. Each wizard now records, as it prompts, which
  keys it asked -- per volume and per target, with the snapper question
  covering only the config name -- and everything else in the existing file
  is kept, over any default the wizard wrote for it. Volumes and targets
  are matched by path in order of occurrence; a changed answer is reported
  with both values; `--force` skips only the question; the diff summary
  reads the configuration that would be saved.
- **Snapper backup and restore: seven edges.** After one snapshot's receive
  failed and a later one's publish failed, the engine's cleanup of the
  partial derived a path from the endpoint's current path and the copy's
  name -- by then the config's subvolume, so `<subvolume>/snapshot-<n>`,
  outside `.snapshots` -- and would have deleted a directory of the
  operator's under that name; the snapper layout's receiver now removes
  exactly the slot it opened and nothing else. A pin that could not be
  written to a location's lock FILE aborted the restore, so a backup medium
  mounted read-only could not be a snapper restore source and
  `--skip-remote-lock` did not cover that store: the flag covers it, and a
  read-only location proceeds without the pin and says so. That is one rule
  for every pin writer -- the lock file of a local btrfs location and the
  lock directory of `raw://`, `ssh://` and `raw+ssh://` alike: a location
  whose filesystem is mounted read-only cannot have anything deleted from
  it, so the pin protects against nothing and is not taken. Read-only is
  decided from the kernel's mount table together with a failed write
  attempt, by exit status, never from a tool's message; a location that
  merely refuses the write keeps the refusal and its opt-out. A restore's
  source got this tool's bookkeeping tree
  (`.btrfs-backup-ng/`) created under it -- on a dry run too -- and it no
  longer does. Two `snapper backup` runs into one local target both opened
  slot n and the second removed the first's in-flight `.incoming`; the
  backup direction now holds the same writer lock a restore holds
  (`.snapshots/.btrfs-backup-ng.restore.lock`), for the whole sync, and a
  second writer is refused with the reason. A regular file named like a
  slot in `.snapshots` made the restore's publish ask for the same number a
  thousand times; the number is now occupied by any entry named like a
  slot, and a number that stands still is refused with the entry named.
  `--dry-run` took the config's writer lock and created its file; it now
  creates nothing. A pin stayed on the backup when the send died of
  anything but the transfer error the executor expected (Ctrl-C included);
  it is released for every kind of failure.
- **Pins now protect on every location type.** A pin on a `raw://` stream
  lived only in the process that took it, so a prune in another process
  could delete the stream a restore was reading; it is now recorded in the
  location's lock store, the same directory store `ssh://` and `raw+ssh://`
  use, `--status` and `--unlock` read it, and the raw deletion asks for it
  at delete time. The deletion of a snapper slot (`run`'s prune and
  `prune`, local and `ssh://`) never consulted the lock store at all, so a
  slot pinned by a restore could be deleted from under it; it now skips a
  pinned slot and deletes nothing when the store cannot be read. The
  documentation's "a prune cannot delete what is being read" is now true
  wherever a backup lives.
- **`BTRFS_BACKUP_LOG_LEVEL` did nothing.** Documented for years, read once
  at import and overwritten by every command's logger setup. It now sets
  the console level where neither a command-line flag nor the
  configuration's `quiet`/`verbose`/`btrfs_debug` says anything, and loses
  to both; a value that is not `DEBUG`, `INFO`, `WARNING` or `ERROR` is
  ignored with a warning.
- **Retention and `list` disagreed about a snapshot's date.** Retention kept
  a parser of its own -- a list of guessed formats and an unanchored search
  for digits -- so under `timestamp_format = "%Y%m%d"` the snapshot
  `20260921_1000000` was the 21st to the listing and 10:00:00 to retention,
  and a name the listing showed as "unknown" could be bucketed and deleted
  by a date only retention believed in. Retention now reads every name
  with the listing's one rule, and a name the listing leaves undated stays
  undated -- kept, never deleted -- as the README has always said. Names
  that parse under neither the configured nor the default format, which
  only the old guessed formats dated, are now kept rather than pruned.
- **A snapper `run` sent a target that was behind everything it was
  missing** and then pruned most of it. The native pipeline already sends
  only what the target's prune keeps; the snapper pipeline now asks the
  same decision its prune makes, over the target's backups plus the
  snapshots it is missing, and sends only those the prune would keep. The
  target ends up holding exactly what it would have held had everything
  been sent and pruned. `snapper backup`, which does not prune, still sends
  everything.
- **`[global] quiet` could silence the endpoints for the rest of the
  process.** The shared logger the endpoints write through was built
  outside the logging manager, whose job it is to clear every logger's
  level cache when a level changes; after `quiet` raised it to WARNING and
  one INFO line was refused, lowering it again (a log file at DEBUG added
  afterwards, `verbose`) changed the level and nothing else. The logger is
  now registered with the manager.
- **Snapper dates were read as local time; snapper writes them in UTC.**
  Every `info.xml` `<date>` is UTC (a snapshot `snapper list` shows at
  20:00 EDT is dated 00:00 the next day in its file), and it was parsed as
  local time. West of UTC a fresh snapper backup was "dated in the future"
  and kept out of retention for hours; east of UTC it looked older than it
  was and left its minimum window early; and a destination's backups (dated
  from `info.xml`) never agreed with the source's snapshots (dated by
  `snapper list`) about when the same snapshot was taken. The date is now
  converted at the file boundary in both directions; a raw sidecar's
  stored `info.xml` is the authority for its date, so sidecars written
  before this read correctly too.
- **A collision counter was read off any name ending in digits.** The `_N`
  that orders two snapshots sharing a timestamp was taken from the end of
  the whole name, so under a `timestamp_format` ending in `_%H` or
  `_%H%M%S`, or a prefix ending in `_`, the bare name carried a huge
  "counter" and sorted newest on a tie: retention kept the first snapshot
  of the hour as "latest" and deleted the ones created after it. The
  counter now comes only from a name that parsed with a trailing `_N`
  removed, in retention and in the listing's own order.
- **`run`'s catch-up could strand a lock and send an undated subvolume.** A
  missing snapshot this destination still held a transfer lock on (a send
  that did not finish) was left out of the catch-up, so nothing ever
  released the lock and the source kept the snapshot for ever; it is now
  always sent. A subvolume in the snapshot directory whose name yields no
  timestamp was kept by retention as unparseable, so the selection carried
  it, and a selection is an explicit request to the planner: it was sent
  as a full send, first, to every target, which the plan without a
  selection never does. It is left out of the selection.
- **The log file's completeness depended on the console level.** The
  file handler is meant to record at DEBUG whatever the screen shows, but
  the shared endpoint logger kept the console's level, so under `-q` every
  endpoint INFO line was missing from `log_file` and under the default
  level every endpoint DEBUG line. Adding the file handler now opens that
  logger to the file's level, and removing it puts the level back.
- **Commands that read the configuration for a `timestamp_format` or a
  target's options ignored its `quiet` / `verbose`:** `restore` on a
  location (`--list`, `--status`, a plain restore), `verify`, `estimate` on
  paths, `snapper list`, `snapper backup` and `snapper status`. They apply
  it now; `verify` also sets up the console logger every other command has,
  so `-v verify` shows debug output and its warnings are formatted. `run`
  printed one INFO line after reading a `quiet` configuration (the one
  announcing the log file); it applies the setting before that line.
- **A snapper slot could be published without its `info.xml`.** The write
  of the slot's metadata was soft-fail, so a publish went ahead without it
  and a restore reported success for a slot snapper does not list. In both
  directions a slot whose `info.xml` cannot be written is now abandoned and
  the transfer fails, as the documentation has said all along.
- **Two configuration keys the loader read were reported as unknown.**
  `skip_remote_lock` on a target and `timeout` under
  `[global.notifications.email]` produced "Unknown config key ... (ignored)"
  although the first was honoured and the second was in the schema; the
  warning was untrue for one and the value was never read for the other.
  Both are known and read.
- **The chunked `ssh://` receive kept a stdout pipe it never read** and
  left its stderr pipe open until garbage collection; the pipe nobody reads
  is gone and the stderr is drained like every other receive's.
- **Two snapper snapshots taken within one second could have the newer one
  deleted.** snapper dates have one-second resolution and no collision
  counter, so snapshots taken in quick succession share a timestamp, and
  retention broke the tie by input order read newest-first: the lowest
  number of a tie became "latest" and the highest its bucket's oldest member
  or nothing. Measured on real btrfs with four snapshots taken in two
  seconds under `daily = 1`: the plan kept 2 and 3 and deleted 1 and 4, the
  newest snapshot among them -- from the prune and from `run`'s catch-up
  alike. A snapper number is its creation order and now breaks the tie in
  every snapper retention path.
- Slot numbers under `.snapshots` are recognised as decimal digits only; a
  name `str.isdigit` accepted but `int` refused (a superscript digit) made
  the enumeration and the stale-temp sweep raise instead of skipping it.

## [0.9.10] - 2026-09-23

This release fixes behaviour that did not match what the tool documents.
`snapper restore` now does what the README's disaster-recovery walkthrough
says: one snapshot restored onto empty media lands, and an `ssh://` source
with `--ssh-sudo` runs unattended where the remote grants passwordless
`btrfs`. `[global] quiet`, `verbose` and `btrfs_debug` now take effect, and
the configuration wizard no longer drops options it does not ask about when
it saves over a configuration.

Three changes an upgrade can notice:

- **Retention now prunes snapshots named with a collision counter**
  (`home-20260923_1`), which earlier releases kept forever. The first `run`
  after upgrading applies each policy to them, on the source and on every
  target, and `run` prunes without asking; run `prune --dry-run` first to see
  what it will remove.
- **`run` sends a target that is behind only what its prune will keep**, so a
  catch-up transfers fewer snapshots and ends with the same ones.
- **`snapper restore` runs one at a time per config**, and a second one is
  refused with the reason. It leaves a lock file,
  `.snapshots/.btrfs-backup-ng.restore.lock`, in the config.

### Fixed

- **`snapper restore --snapshot N` onto media without the parent failed in
  the receive.** The incremental parent was chosen from the BACKUP side -- the
  highest-numbered backup below N -- and only the backup was probed for it, so
  on recovery media the send went out as an increment the receive had nothing
  to apply to. That is exactly the situation the README's walkthrough
  describes. The parent is now chosen from what the LOCAL config already
  holds, by identity (a slot's received_uuid against the identity the backup's
  stream carries), never by snapper number, which snapper reuses: a backup
  whose parent is restored is an increment from that copy, one whose parent is
  not is a full send. `--snapshot N` onto empty media is one full send into
  slot 1; `--all` rebuilds the chain with each increment received against the
  slot before it. The selection is never expanded.
- **`snapper restore ssh://... --ssh-sudo` demanded a sudo password on a
  remote that grants NOPASSWD `btrfs`.** The remote endpoint was built without
  being prepared, so the passwordless probe the backup direction and the
  native restore run never recorded that `sudo -n` would do, and the remote
  `btrfs send` was issued as `sudo -S`. The source is prepared like every
  other endpoint, and the walkthrough's command runs unattended.
- **After a snapper restore, the tool told you to run `snapper -c <config>
  list` so snapper's daemon would see the restored snapshots. That does not
  work:** snapperd keeps its cached list across repeated `snapper list` calls,
  and `snapper diff`/`undochange`/rollback go on reporting the slot as not
  found. Restarting the daemon does (`systemctl restart snapperd`), as does a
  reboot. The restore's reminder, the README, the snapper guide, the CLI
  reference and the man page now say so.
- **`[global] quiet`, `verbose` and `btrfs_debug` reach the console.** The
  console is set up from the command line before the configuration is read,
  so `quiet = true` printed exactly what a config without it did,
  `verbose = true` showed no debug output, and `btrfs_debug = true` turned
  on btrfs's `-vv` output but left the console at the level that drops it:
  without a `log_file` the setting showed nothing. Every command that reads a
  configuration now applies them as soon as it has loaded it: `btrfs_debug`
  and `verbose` show debug output, `quiet` shows warnings and errors only. A
  `-q`, `-v`, `--debug` or `--btrfs-debug` on the command line still wins. Only
  the console changes; a configured `log_file` keeps its own level. The line
  naming the configuration file is printed before the file is read, so it
  follows the command line alone.
- **The configuration wizard, saved over an existing configuration, kept
  only what it asks about.** It builds its configuration from its prompts
  and writes a target's path, `ssh_sudo`, `require_mount` and (for raw
  targets) encryption -- so saving over a file dropped every other option.
  A target's `ssh_host_key_policy = "strict"` became trust on first use,
  `ssh_key` and `ssh_port` vanished and the next run could not authenticate,
  an `optional` drive became a required one, and a target's own `retention`
  and a volume's `source_retention` were lost, behind an "Overwrite?" prompt
  whose default summary showed only per-volume counts. Volumes and targets are
  now matched by path and every option the wizard does not ask about is
  carried over; its answers win for what it does ask, including an option
  answered by leaving it out. Before the prompt it lists what it kept and
  what it will remove -- a volume or target the new configuration no longer
  has, with its options. This applies to every path that saves a wizard
  configuration over a file, and the "view changes" comparison shows what
  will actually be written.
- **Retention now manages snapshots named with a collision counter, instead
  of keeping them forever.** A second snapshot in one period under a coarse
  `timestamp_format` such as `"%Y%m%d"` -- or a scheduler that fires twice, or
  a pool migrated from btrbk -- is named `home-20260923_1`, `_2`, and so on.
  Listings, transfers and restores dated those by the timestamp before the
  counter, and the listing said retention now managed them; but retention
  parsed names on its own, found no timestamp, and kept every one on the
  source and on every target, so under a daily format each extra run of the
  day accumulated without limit. Retention now reads the counter the way the
  listings do, and only after the name as written fails, so a timestamp that
  ends in `_<digits>` keeps its meaning. Snapshots that share a timestamp
  order by the counter, numerically: the day's first is its bucket's
  representative, and the last one created is the latest, which is always
  kept. Measured on real btrfs: three runs in a day under `"%Y%m%d"` and
  `daily = 7` now keep `home-<day>` and `home-<day>_2` and prune `_1` on the
  source and the target.

### Changed

- **`run` no longer sends a target what the target's own prune deletes
  straight after** ([#104](https://github.com/berrym/btrfs-backup-ng/issues/104)).
  `run` sends everything a target is missing and then prunes it with the
  target's policy, so a target that had been away was sent its whole backlog --
  including snapshots that are neither the newest nor the oldest of their time
  bucket -- only for the prune to delete them. `run` now asks the prune's own
  retention decision first, over the target's snapshots plus the ones it is
  missing, and sends only what it keeps, each against the newest earlier
  snapshot the target holds. Measured on real btrfs with a 30-snapshot backlog
  and `daily = 3` on the target: 0.9.9 sent 31 and pruned 27; now 4 are sent,
  and the target holds the same four either way. On a raw target the result can
  hold fewer streams than before, since a snapshot that was never sent needs no
  protection as a stored increment's parent. Nothing is left out when the
  target's policy is one the prune refuses to apply, when the target cannot be
  listed, or when a name collides; `transfer` (which does not prune) and
  `run --newest-only` are unchanged.
- **A snapper restore is a transfer through the engine, into the snapper
  layout.** `restore_snapper_snapshot` had its own `btrfs send`/`btrfs
  receive` pipes, its own progress handling, three separate ways of writing
  `info.xml` (one per source kind), no pin on the backup, no check of what
  arrived, and a cleanup that deleted by existence. It is replaced by the
  planner and executor every backup uses, with the roles swapped, and by a
  snapper layout that the backup direction now runs as well: `snapper backup`
  to a btrfs target and `snapper restore` into a local config open, fill,
  publish and abandon a numbered slot through the same code. What that gives
  a restore: the backup is pinned on its location for the duration under
  `restore:<session>` and released if a transfer fails (a location you can
  read but not write takes `--skip-remote-lock`); the copy is checked in its
  slot -- a subvolume whose received_uuid is the backup's identity -- before
  the slot is published, and a copy that is not the backup's is not
  published; what the send and the receive print reaches the report through
  the drain every transfer uses, and `--btrfs-debug` puts `-vv` on both; and
  `--dry-run` prints the plan the run executes. Every selected backup lands
  in a NEW slot whether or not a copy is already there, as before.
- **A slot appears complete or not at all, in both directions.** The
  `info.xml` is written into `.snapshots/<n>.incoming/` before the directory
  is renamed to `.snapshots/<n>/`; the backup direction used to write it after
  the rename, leaving a window in which a published slot had no metadata. A
  restore that fails mid-receive leaves no numbered slot and no `.incoming`.
- **A snapshot snapper creates during a restore is never touched, and one
  restore runs into a config at a time.** The config is live while a restore
  runs and its temp is invisible to snapper, so snapper's timeline or a manual
  `snapper create` can take the number the restore was heading for. The copy
  is now published under the number that is free at that moment, with its
  `info.xml` renumbered to match, through a rename that cannot replace an
  existing entry (`renameat2` with `RENAME_NOREPLACE`; an empty directory
  snapper has just made is refused too, where `rename(2)` would take it
  over); the report names the number the copy landed under. A second
  `snapper restore` into the same config while one runs is refused with the
  reason and restores nothing (an exclusive lock on
  `.snapshots/.btrfs-backup-ng.restore.lock` in the config, which the kernel
  releases if the restore is killed); the temp a killed restore leaves is
  removed by the next restore into that config, and only then.
- **One `info.xml` for every source.** The restored slot's `info.xml` is the
  backup's own with only `<num>` changed, from local, `ssh://` and raw
  sources alike, so `<uid>`, every userdata block and any element snapper
  wrote survive verbatim. The local-source branch used to parse and
  regenerate it, which dropped what this tool does not model.
- **A `raw://` increment whose parent is neither in the config nor selected
  is refused before a byte moves.** A stored increment applies only onto its
  parent, and there is no full stream to send instead. It used to stream the
  whole increment and fail in the receive.

## [0.9.9] - 2026-09-23

Restore now runs through the same planner and executor as a backup. Three
things a restore script can notice: a same-name entry at the destination that
is not this backup's copy now refuses the run (it used to be skipped and
reported as restored); `restore --cleanup` deletes only what this tool's run
marker names; and `--compress` on an `ssh://` restore now compresses the
transfer instead of being ignored. Chain restore stays the default.

### Changed

- **A native restore is a transfer through the engine.** `restore` had its
  own planner (a chain built by walking timestamps, presence and collision
  decided by NAME), its own parent chooser, its own verifier (an existence
  check) and its own lock lifecycle around the shared transfer. It now
  selects the snapshot and hands the rest to the planner and executor every
  backup uses, with the roles swapped: presence is correspondence -- the
  copy's received_uuid against the identity the backup's stream carries, two
  hops from the original -- so a partial receive, a foreign subvolume and a
  re-created snapshot are all "absent"; the incremental parent is chosen
  from what the destination holds; the pins are taken under
  `restore:<session>` and released when a transfer fails, so a failed restore
  never leaves a pin on a remote target; every copy gets the artifact
  verdict; a partial the run made is cleaned under the authorship rule; and
  `--dry-run` prints the very plan the run executes, line for line. The
  chain a snapshot depends on comes along by default, as it always has, but
  from one rule: the source is asked what each snapshot requires
  (`required_parent_of` -- the time-ordered predecessor for a btrfs
  location, the sidecar's parent for a raw store), and the selection grows
  to that chain until it reaches what the destination already holds.
  `--no-incremental` still brings the chain, as full sends.
- **A same-name entry at the destination that is not this backup's copy
  refuses the run.** An interrupted restore leaves a subvolume under the
  right name with no received_uuid; it listed as "already restored", was
  skipped, and the run exited 0 having restored nothing -- the signature
  defect, live in every release with a `restore` command. Anything under a
  name the restore would receive is examined before a byte moves and, when
  it is not the copy (no received_uuid; a copy of a different snapshot; an
  identity that cannot be read; a plain directory), the run refuses, names
  the path and what it is, and transfers nothing. It is never deleted.
- **`restore --cleanup` deletes what a run marker names, and only that.**
  A restore now records each receive in flight in
  `DESTINATION/.btrfs-backup-ng/restore/<token>.json`, written atomically
  and removed once the copy is verified. A marker that outlived its process
  is the authorship record of a restore killed mid-receive, and `--cleanup`
  removes the subvolume it names when that subvolume has no received_uuid.
  Nothing else is deleted: a marker whose restore is still running, a marker
  over a complete received copy (the marker is stale, the copy is not), a
  marker naming a path outside the destination (including `..`) or a
  symlink, and every subvolume under no marker -- however empty -- are
  reported and left; each deletion re-checks its entry first. The
  `.partial`-suffix and metadata-only heuristics are gone; nothing ever wrote
  the first, and the second was an operator's own subvolume as often as
  anything.
- **A stored raw increment whose parent is not at the store is refused
  before streaming.** A raw backup is the stream as written, and an
  incremental stream applies only onto its parent. Restoring such an
  increment used to stream all of it and fail in the receive; the planner
  now asks the store, brings the parent first when it is there, and refuses
  the plan when it is not, leaving nothing at the destination.
- **A transfer onward from a mirror is incremental.** `btrfs send` of a
  received subvolume carries the original's uuid rather than its own, so a copy
  of a copy records the original as its received_uuid. Presence was compared
  against the middle copy's own uuid, which matched only the first hop: a
  transfer onward from an `ssh://` mirror re-sent every snapshot in full and
  then failed on the names already at the destination. Presence is now
  compared against the identity a stream actually carries, so the second hop
  is an ordinary incremental and a rerun reports the target up to date. Raw
  backups record that identity in a new sidecar field, `source_uuid`; a
  sidecar written before this release lacks it and is matched by name, as
  before.
- **`restore --in-place` refuses instead of pretending.** The flag was
  accepted and ignored: the ordinary restore ran, landed the snapshot as a
  nested subvolume at `DESTINATION/<name>`, replaced nothing, and exited 0 --
  while the README documented `--in-place` as a disaster-recovery strategy
  with copy-paste commands, and the config-driven `--volume` path never read
  the flag at all. The command now refuses ahead of every mode, before any
  endpoint is prepared (exit 2), and names the procedure that works today:
  restore into a staging directory, verify, swap the subvolumes by hand
  (README Strategy 2, whose commands now include making the received
  snapshot writable). In-place restore that verifies the staged copy against
  the backup's identity before swapping is scheduled; the running root will
  only ever be replaceable from a rescue system, because btrfs-backup-ng does
  not touch the bootloader.

### Added

- **`--compress` on an `ssh://` restore source compresses the wire.** The
  README recommends it for a slow-link restore, and the option was accepted
  and did nothing: the transfer layer dropped it for a local destination and
  the ssh endpoint's restore-direction send had no compressor. The remote now
  runs `btrfs send | <compressor>` and this host decompresses before `btrfs
  receive`, both from the one configured method, mirroring the backup
  direction. The remote pipeline exits with the SEND's status, not the
  compressor's, through a POSIX-only construction run under bash, dash and
  busybox ash; the local ssh and decompressor run under the one `pipefail`
  runner the raw pipelines use, so a failure at either end is the failure the
  executor sees. A missing local decompressor refuses before connecting.
- **`--btrfs-debug` for every command, and `[global] btrfs_debug`.** Legacy
  mode has had `--btrfs-debug` since the original tool; it put `-vv` on
  `btrfs send` and `btrfs receive`, whose output then went to DEVNULL, so the
  option did nothing anyone could see. The config-driven commands did not have
  it at all: nine of them hard-coded it off. The stderr drain now logs each
  line as it arrives, prefixed with the process that printed it (`btrfs
  receive: At subvol ...`), the option implies `--debug` because that is the
  level the lines are logged at, one helper answers every command, and a
  non-boolean value in the configuration is refused at load. The `ssh://`
  direct path, which builds its own local send and remote receive, carries
  `-vv` on both and drains the remote receive's lines as they come back over
  ssh; it had ignored the option entirely. Lines are logged on their own
  thread so a slow console cannot slow the transfer, and every transfer waits
  for its queued lines before it returns, so none are lost at exit.
- **Every transfer records a verdict on what the receive left.** After
  `btrfs receive` exits 0 the engine had nothing more to say: exit 0 was the
  whole verdict, and a subvolume under the right name that was not the
  received copy counted as a backup. The executor now records one of three
  verdicts, in the shape `verify` already uses: `ok`, when the copy's
  received_uuid is the identity the stream carried (so a copy of a copy is
  judged against the original, not the middle hop); `invalid`, when the
  artifact provably is not that copy -- not a subvolume, no received_uuid, or
  a different one -- in which case the transfer fails and the artifact is
  removed under the same authorship rule as a partial, on local and on remote
  btrfs destinations; and `unverifiable`, when the identity could not be read,
  in which case the data is kept, the transfer counts, and the log says what
  was not confirmed. A verdict that cannot be computed at all is unverifiable
  too: the data has landed, and a post-check must never be able to turn that
  into a failure. Raw destinations are judged by the endpoint's own structural
  check on the committed stream, which carries the sealed sha256; nothing is
  re-hashed. The identity is read through one probe that the listing uses as
  well, so the verdict and the planner cannot disagree about what a subvolume
  is; that probe consults the subvolume's inode and not the mount-table walk,
  because a check that can be wrong about the environment must not be able to
  condemn a copy.
- Every source endpoint answers `required_parent_of`, and the planner's
  `only=` accepts a selection and a `source_endpoint` to expand it through.
  The backup run's contract is unchanged: without a source endpoint the
  selection is planned as given.

### Fixed

- **A failed send or receive now reports what btrfs said, not only how it
  exited.** The local `btrfs send` and `btrfs receive` -- and by inheritance
  the `ssh://` receive, and the remote send -- sent stderr to DEVNULL, so a
  restore from a corrupt stream reported "btrfs send/receive failed with
  return codes: [-13, 1]" and nothing else, while "ERROR: crc32 mismatch in
  command" had been printed and discarded. Every send and receive an endpoint
  starts now has its stderr drained on a thread as it is written, with the
  last 64 KiB kept for the report and the pipe closed at EOF. The drain is
  what makes a pipe safe: `--btrfs-debug` puts `-vv` on both commands, one
  line per file operation, and a pipe read only after exit would fill, stop
  the child, and read as a stall (verified: 30,000 files under `-vv`, local
  and over ssh, complete without one). The transfer engine also hands the
  receive it starts back to the failure report, which had been given None.
  Raw transfers no longer leave a stderr pipe open until garbage collection.
- **An endpoint that refuses to send fails that transfer, not the run.** A raw
  store whose stream fails its sealed sha256, a decompressor that is not
  installed, a remote sudo with no password to give: each raised out of the
  executor, past the per-snapshot handling where pins are released and
  partials cleaned, and aborted everything after it with the pin still held.
  It is now one failed entry.
- The post-receive verdict on a raw SOURCE probed `DESTINATION/<name>.btrfs.zst`
  -- the stream file's name -- found nothing, and would have called a correct
  restore invalid and deleted it. A raw snapshot now says what its stream is
  received as.
- **A refused lock never reported an empty reason again.** Four sites logged
  why and then raised an exception carrying nothing, so a summary that quotes
  the exception -- "Transfer to X failed: " -- ended at the colon. Every
  `AbortError` now carries its reason, and a scan test refuses a bare raise.
  The case that exposed it: a transfer onward from an `ssh://` mirror. An
  `ssh://` target keeps its persistent locks in a directory of the same name a
  local endpoint uses for its lock file, so a local endpoint over that mirror
  found a directory and refused with nothing said. It no longer refuses: see
  "One lock store per location" below.
- **One lock store per location.** An `ssh://` target keeps its persistent
  locks -- the pins a restore holds, the locks a receive holds -- in a
  directory named `.btrfs-backup-ng.locks` under the target; a local endpoint
  keeps a JSON file of the same name. A local endpoint over a location that
  carries the directory (a transfer onward from an `ssh://` mirror, a prune of
  it on the host itself, `restore --status` against it) now uses that store:
  the same scripts the `ssh://` endpoint runs on the remote, run locally. A
  pin a restore takes over `ssh://` is honoured by a local prune; a pin a
  local run takes at such a location is visible to an `ssh://` prune; the
  store is consulted again at delete time; an unanswerable store deletes
  nothing. A pin in the directory store lives as long as the process that
  took it, as an `ssh://` endpoint's pins always have (a pin in the lock file
  survives across runs). A location without the directory keeps its lock
  file exactly as before; nothing is renamed or migrated, and the store is
  recognised only under the default lock file name. Renaming either store
  was rejected because a local prune with an empty lock set would have
  deleted what a remote restore holds. This covers btrfs endpoints; a local
  `raw://` endpoint still keeps its locks in memory only.
- **The transfer size estimate is asked of the endpoint that holds the
  snapshot.** It ran the local measurement whatever the source, so a transfer
  from an `ssh://` source measured nothing and warned "Could not estimate
  transfer size for space check" on every run. The `ssh://` endpoint now
  measures on the remote host with `btrfs filesystem du -s --raw`, whose
  Total is what a full stream carries; the local endpoint keeps the same
  measurement it always made. An incremental send is deliberately not sized
  -- the delta is not knowable in advance -- and says so instead of being
  reported as a failed estimate. The size parser matched the unit `B` before
  `KiB`/`MiB`/`GiB` and so returned nothing for any binary unit; it parses
  them now.
- **The `restore` man page said `--overwrite` overwrites.** The option was
  withdrawn in 0.9.6 and the CLI help has said so since; the man page kept the
  original sentence. It now matches the CLI, and a test pins the help text,
  the man page and the README's options table to each other for every option
  whose truth is "does not do what its name says".
- The README gave `-vv` as the debug level. It is the same as `-v`; the debug
  level is `--debug`. The restore documentation listed six compression methods
  where `--compress` accepts nine.

## [0.9.8] - 2026-09-21

### Fixed

- **The remaining ways a backup location could be created are closed, on
  every layer and every entry point**
  ([#102](https://github.com/berrym/btrfs-backup-ng/pull/102)). 0.9.7 stopped
  the local endpoints from creating a target, a source or an absolute
  `snapshot_dir`, and said the class was closed. It was not: ten more sites
  were live in every other layer, each reproduced on real btrfs -- one of them
  only by a real cross-machine transfer -- before it was fixed:

  - The listing created the path it was asked to enumerate. A drive that
    unmounted mid-run had its mount point rebuilt on the root filesystem by
    the next read, the pool was reported empty, and the planner scheduled full
    re-sends into the directory the read had just invented. A backup
    destination that cannot be enumerated is now refused as exactly that,
    never presented as empty.
  - `ssh://` ran `mkdir -p` on the remote at the moment of first transfer
    ("Destination path doesn't exist, creating it"), and `raw+ssh://`
    created its target outright -- its writability probe itself began with
    `mkdir -p`, so even asking whether the target was writable invented it.
  - The transfer engine's `_ensure_destination_exists` rebuilt whatever the
    endpoint's `prepare()` had just refused, one layer above every endpoint
    audit, behind a name that reads as a check. A green unit suite, exact
    mutation kills and a green loopback suite all agreed the class was
    closed; a real transfer to a real host created the directory, transferred
    into it, and exited 0.
  - The remote lock scripts ran `mkdir -p` on the full lock-tree path, so
    acquiring a lock against an unmounted destination rebuilt the mount
    point. The lock tree is now created only below an existing target.
  - `run` and `snapshot` created `<volume path>/.snapshots` *before* looking at
    the volume, so a volume whose path was not there (an unmounted data disk, a
    typo in `path`) had its snapshot tree built on the root filesystem, and from
    then on the source existed as a plain directory for every later check.
  - `Endpoint.snapshot()` created an absolute snapshot folder wherever it
    pointed, on any API call and from legacy mode.
  - The `.btrfs-backup-ng` tree under a target was created with `parents=True`,
    which would rebuild a target that vanished between the check and the mkdir.
  - Legacy mode (`btrfs-backup-ng SOURCE DEST`) built its snapshot tree before
    looking at the source, and built an absolute `-f/--snapshot-folder`
    wherever it pointed, exit 0.
  - Legacy mode reported every refusal as "Process aborted by user or error"
    and dropped the message that said why.

  The rule is now stated by what a path is: a backup location (a source, a
  target, an absolute snapshot base, however it was given) must exist and is
  never created; an output location (the restore destination,
  `verify --temp-dir`, a written config, completion or man-page file) is
  created; the program's own state is created; and anything below a location
  that exists is created one component at a time through one primitive that
  has no `parents` mode at all, so a base that is missing or vanishes mid-run
  is refused rather than rebuilt.

- **A snapper backup to a btrfs target failed in 0.9.7** with
  "Destination `<target>/.snapshots/<n>.incoming` does not exist". The receive
  slot had only ever existed as a side effect of the creation sites 0.9.7
  removed. The snapper flow now creates its own slot, below a target that must
  exist, and the target is checked before the slot lock is taken.

- **Legacy mode's `-f/--snapshot-folder` had no effect on placement.** It
  computed a directory and created it, but never told the endpoint, so every
  legacy-mode snapshot went to `<source>/.snapshots` whatever the option said.
  The option is now honoured.

- **A retention bucket count of `true` kept one snapshot, not seven.** The
  five time buckets (`hourly` … `yearly`) were read with no check while `min`
  and `keep` were validated; `daily = true` loaded clean, counted as 1, and a
  policy meant to keep seven kept one -- measured over eleven snapshots, six
  the policy said to keep were deleted. A string crashed mid-prune; a negative
  silently disabled the tier. Each bucket now takes the rule `keep` already
  had: a non-negative integer, with a boolean rejected by name, refused when
  the config loads.

- **`estimate` predicted the wrong directory under an absolute
  `snapshot_dir`.** It joined `source / snapshot_dir` by hand, which resolves
  to the absolute right operand, so it enumerated the base while the transfer
  reads `<base>/<source name>`: `list` showed two snapshots, `estimate`
  reported zero. All seven commands that resolve a snapshot directory now go
  through the one helper, and the four that carried inline copies gain its
  diagnosis when the base is missing.

- **`prune` and `snapshot` dropped configuration warnings from the log file.**
  Both logged them before installing the log file named by that same config,
  so under cron or a timer -- where the console goes nowhere -- a collected
  warning vanished. `run` and `transfer` had been fixed; these two were missed
  because the test that guarded the ordering compared character offsets in
  source text and was parametrised over exactly the two modules already fixed.
  It is replaced by a test that drives each command as a subprocess and reads
  the warning back from the file.

- **A configuration file that is not UTF-8 produced a traceback** instead of
  a configuration error; it is now a `ConfigError` naming the file. A
  non-string `gpg_recipient` died mid-backup while building the gpg command;
  it is refused at load like every other encryption key. `doctor` blessed a
  valid configuration with every volume disabled ("3 passed, 0 warnings") --
  a machine that performs no backups while doctor says all clear; it now
  warns and names the remedy. Configuration writers state `encoding="utf-8"`
  at every site, so a file written on one host reads on another whatever the
  locale.

### Changed

- **Legacy mode no longer describes an `ssh://` source it cannot use.** The
  help text dated from the original btrfs-backup, whose remote sources were
  built on sshfs; nothing of that exists here, and the form was never
  dispatched -- it fell through to the subcommand parser and was rejected as
  "invalid choice" with every subcommand listed. The source help now says a
  source is a local path, and an `ssh://` first argument is refused with the
  reason and where `ssh://` is accepted. The feature is tracked in
  [#108](https://github.com/berrym/btrfs-backup-ng/issues/108).
- **Legacy mode's default snapshot folder is `.snapshots` inside the source**
  -- what has in fact happened on every run, and what the config-driven
  commands do -- so a run that never passed `-f` sees no change. A run that
  did pass `-f`, and whose chain is therefore in `<source>/.snapshots` while
  the named folder is empty, is **refused** rather than started as a new
  chain, because the next transfer to every destination would be a full send.
  The refusal lists the three remedies: move the chain into the folder (same
  filesystem), pass `-f <source>/.snapshots` to keep it where it is, or add
  the new `--accept-full-send` flag, which is needed at most once.
- **Legacy mode's absolute `-f/--snapshot-folder` must exist**, like an
  absolute `snapshot_dir` and like the legacy destination already did in 0.9.7.
  A relative folder is created under the source.
- **`Endpoint.snapshot()` no longer creates an absolute `snapshot_folder`**;
  a relative one is created under the source, which must exist.

### Added

- **A snapshot's name is a remembered fact, and a collision is absorbed with
  the `_N` counter instead of refused.** A snapshot regenerated its name on
  every call from its parsed timestamp, so a listing could name an entry that
  is not on disk (strptime accepts `2026-9-8`, strftime writes `2026-09-08`),
  and prune, lock and verify by that path; two snapshots sharing a timestamp
  compared equal whatever their names. The name is now set once, from what is
  on disk or at creation, and identity is the name. A second snapshot whose
  name collides -- the second of the day under a coarse `timestamp_format`,
  the repeated hour of a daylight-saving fall-back, a scheduler double-fire --
  was refused with advice to wait a second, which under a coarse format could
  never work. It now takes the lowest free `_N`, btrbk's own collision
  counter, allocated inside the creation lock; the diagnosis that remains is
  judged by what the configured format actually renders at one second, one
  minute, one hour and one day apart, not by scanning the format string.

- **Foreign snapshot names are listed instead of hidden.** Anything whose
  name did not parse was skipped silently, so a btrbk pool full of `_N`
  names, or any subvolume named by another tool, listed as empty while
  holding restorable data. A prefix-matching subvolume whose name yields no
  timestamp is now listed, restorable and reported; a plain file or directory
  is still skipped. A trailing `_N` is stripped when deriving a timestamp, so
  migrated btrbk pools enter retention buckets. A snapshot with no derivable
  timestamp takes part in nothing that needs an age -- never a parent, never
  counted, never deleted by count -- and each exclusion is logged.

- **The acceptance matrix's restore proof can fail.** Seven hardware cells
  byte-verified restored data and could not fail on the defects that matter:
  the restore's return code was discarded, the incremental run's return code
  was recorded and asserted nowhere, and every cell compared a file identical
  in every snapshot of the chain. A restore that delivered the parent and
  dropped the increment passed the whole matrix. Every cell now asserts every
  return code and that the increment's own bytes were restored, and the two
  empty-prefix cells own their destinations. With that, incremental restore
  is proven on local btrfs, `ssh://`, `raw://`, `raw+ssh://` and a macOS raw
  target.

- **The rule is structural, not a list of known sites.** A test walks the
  package's syntax tree for every way a directory can be created -- `mkdir`,
  `makedirs`, `mkdtemp`, the privileged helpers, and any string containing
  `mkdir` (error text included, registered as such) -- and requires each site
  to be registered with a category its layer allows. The layers that only see
  backup locations may create only below a verified base; only the command
  line and the restore and verify engines may create an output location. An
  unregistered site, a stale entry, a `parents=True` in a below-the-base site,
  or an unguarded remote `mkdir -p` fails the suite.
- **The rule is documented** in the README, `docs/CLI-REFERENCE.md`, and the
  `btrfs-backup-ng(1)` `PATHS` section with pointers from `run`, `snapshot`,
  `transfer`, `restore`, `verify` and `snapper`.
- **A snapper acceptance cell in tier3**, local btrfs and `ssh://`, proving the
  slot is published with a Received UUID, the increment's bytes land, and a
  missing target is refused without being created.

## [0.9.7] - 2026-09-19

### Added

- **A target can be declared `optional`** — a drive connected once a month, or a
  host that is not always up, is expected to be absent. Without a way to say so
  the only choices were a run that reports failure most of the time, or no mount
  check at all.

  ```toml
  [[volumes.targets]]
  path = "/mnt/monthly-archive"
  require_mount = "/mnt/monthly-archive"
  optional = true
  ```

  A target that cannot be prepared is reported and skipped rather than failing
  the run, in `run`, `transfer` and `prune` alike. It also releases source
  retention: the source stops pruning while a *required* target is missing,
  because that target is still owed those snapshots, and an optional target is by
  definition not one the run waits for. This applies to preparing a target, not
  to a transfer that started and then failed. Targets are required unless they
  say otherwise.

- **Retention can differ between the source and each target, and can be a count**
  ([#103](https://github.com/berrym/btrfs-backup-ng/issues/103)) — there was one
  policy per volume, applied identically to the source and every target, so
  "keep many recent snapshots on the machine but the long tail on the backup
  drive" could not be expressed, and two targets could not differ from each
  other.

  ```toml
  [volumes.source_retention]     # this volume's source snapshots only
  min = "6h"
  hourly = 48

  [[volumes.targets]]
  path = "/mnt/archive"

  [volumes.targets.retention]    # this one target only
  keep = 30                      # at least the 30 most recent
  ```

  A target uses its own policy, else the volume's, else the global one; the
  source uses `source_retention`, else the volume's, else global. A config that
  sets none of the new keys resolves to the same policy everywhere, exactly as
  before.

  `keep = N` is count-based retention, and it replaces the time buckets
  (`hourly` through `yearly`) for that scope rather than combining with them — a
  count is neither obviously a floor nor a ceiling, and guessing wrong either
  wastes space or deletes history. Setting both is reported as a warning naming
  which keys are ignored. `min` is not a bucket and still applies: it is a floor,
  floors compose, and ignoring it would let a documented safety setting sit in
  the config doing nothing. So `keep = N` means "at least N"; use `min = "0s"`
  for the count alone. This is
  not a new idea: the original btrfs-backup had `--num-snapshots` and
  `--num-backups`, separately for source and destination, and the subcommand CLI
  dropped them without replacement. Those flags still work in legacy mode.

  Snapshots whose timestamp cannot be read are kept regardless of policy, as
  before, and locked snapshots are never deleted — so a `keep = 30` scope can
  legitimately hold more than 30 while transfers are pending.

- **`require_mount` accepts the mount point the target lives under** — reported
  in [#100](https://github.com/berrym/btrfs-backup-ng/issues/100). Backing up
  several machines or volumes into one drive means targets like
  `/mnt/backup/box1` while the drive is mounted at `/mnt/backup`. The check
  compared the target against each mount point for equality, so only the mount
  point itself ever passed and a subdirectory never could — leaving no way to
  protect exactly the setup that needs it most.

  ```toml
  [[volumes.targets]]
  path = "/mnt/backup/box1"
  require_mount = "/mnt/backup"
  ```

  `require_mount = true` is unchanged and still requires the target itself to be
  a mount point. The named path must be mounted **and** the target must live
  under it: naming a drive the target is not written to would confirm a mounted
  filesystem while the backup went elsewhere, so that combination is refused
  rather than passed. Invalid values (empty string, relative path, wrong type)
  fail when the config is loaded rather than part-way through a backup.

  The `config init` template, both shipped example configs, and the interactive
  wizard all produced `require_mount = true` for subdirectory targets — configs
  that abort even with the drive correctly connected. The wizard now names the
  mount point instead, and the examples and template were corrected.

### Changed

- **A configured path is no longer created for you.** A target on an external
  disk may or may not be present depending on whether that disk is mounted, and
  the path was created unconditionally — so with the disk absent the mount-point
  tree was built on the ROOT filesystem and the backup written there, invisible
  once the real disk came back and charged against the wrong filesystem's free
  space. `require_mount` does not cover this: it only works where the configured
  path IS the mount point, because that one always exists, and it is off by
  default. Local and `raw://` targets, and a source, must now exist; the failure
  names the likely cause. Directories BELOW an existing configured path are
  still created, so nothing changes for a configuration whose paths are there
  ([#102](https://github.com/berrym/btrfs-backup-ng/pull/102)).

- **An absolute `snapshot_dir` must exist before snapshots are written into it.**
  The absolute form is how snapshots are moved off the root filesystem onto a
  bigger disk. It was created with its parents, so an unmounted disk put the
  directory on root — and the snapshots then SUCCEEDED there, because a btrfs
  snapshot only needs to share a filesystem with its source, which on a btrfs
  root it does. The snapshots an operator moved away from root were silently
  put back, with every command reporting success. The per-source directory below
  the configured base is still created, and a relative `snapshot_dir` is
  unaffected.

- **`raw list`, `raw verify` and `raw backfill-metadata` now fail on a target
  they cannot read.** A missing target produced a warning on stderr and then
  "0 snapshots" with exit 0. A warning beside a zero exit is still a clean empty
  report to a timer unit or a script reading the status.

- **`config import -o` and `config init -o` refuse to replace an existing file.**
  `config import` had no existence check at all, and `config init` had one only
  when running interactively, so any non-interactive invocation silently
  replaced the file. What is replaced is the statement of which subvolumes
  matter and how long their history is kept. Pass `--force` to overwrite; the
  interactive prompt is unchanged.

- **A retention `min` that cannot be used is rejected when the config loads.**
  Values like `3000y`, `999999999d` and `100000000w` were accepted by the loader
  and then failed inside the prune path, reporting an internal-sounding
  "year -974 is out of range". The boundary check tested a different function
  from the one the engine runs; it now validates through the engine's own, and
  names the value.

- **`doctor` exits 1 when the configuration it loaded produced warnings.** The
  warnings were collected, logged, and then dropped, so an empty configuration
  reported "4 passed, 0 warnings" and exit 0 — for a file the loader had
  described as having no volumes at all.

- **`run` now transfers what a destination is MISSING, not only the snapshot it
  just created.** A target that missed a run — a drive that was unplugged, a
  host that was down, a transfer that failed — stayed behind for ever, because
  every later run offered it only the newest snapshot. Nothing went back for the
  gap and nothing said so. `transfer` has always caught up, and so have snapper
  sources, so `run` disagreed with the rest of the tool and with itself.

  **Before upgrading, know this:** a first run against a *new* target now sends
  the source's whole history rather than one snapshot. Measured with six
  snapshots of history against an empty destination, 7 transferred where 0.9.6
  sent 1. That is correct — the destination holds none of them — but on an
  established source it is a long first run.

  `--newest-only` restores the previous behaviour.

  ```sh
  btrfs-backup-ng run --newest-only
  ```

- **`ssh_sudo` on a `raw://`/`raw+ssh://` target now means "elevate only where
  elevation is needed".** A raw target stores plain files and runs no btrfs
  command, so setting `ssh_sudo` used to make a valid configuration fail against
  the very sudoers policy this project's README documents (`NOPASSWD:
  /usr/bin/btrfs`), which refuses `mkdir`, `find`, `cat`, `stat`, `mv` and `rm`.
  The destination is now probed once as the login user, and when it is usable no
  file operation elevates. A destination the user genuinely cannot write — a
  root-owned directory — still elevates exactly as before, so nothing an
  operator could previously do has been taken away.

- **The compressed `ssh://` receive no longer needs permission to run a shell as
  root.** It ran `sudo -S sh -c '<decompress> | btrfs receive'`, which asks
  sudoers for `sh`; a host configured with the documented btrfs-only policy
  refused the backup outright. `sudo` is now scoped to the `btrfs` binary on
  every path. Verified against six sudoers policies on hosts whose `/bin/sh` is
  bash, dash and busybox ash.

- **A transfer that stops making progress is now given up on.** Once the local
  `btrfs send` finishes, the remote is still applying the stream and no bytes
  move on this side, so the stall check is deliberately disarmed. That tail was
  documented as covered by the wall clock, but `transfer_timeout` defaults to
  `0` — meaning *no* wall clock — so a remote wedged mid-apply hung the run for
  ever. The tail now falls back to a generous 24-hour ceiling, logged when it
  takes effect, and the failure names the limit as this tool's own rather than
  leaving an operator hunting an ssh timeout. Set `transfer_timeout` for a
  tighter deadline. The `ssh_sudo` pipeline, which had no bound of any kind, is
  bounded too.

- **The legacy CLI exits non-zero when its retention fails.** It discarded the
  deletion result and logged the failure at debug, so a prune that removed
  nothing — an unreadable lock file, a busy target, every delete failing — was
  indistinguishable from a clean one and the run still exited 0 while the target
  filled up. This matches the `run` command, which already refused to report
  success for skipped retention.

- **Retention scopes inherit key by key instead of replacing the whole policy** —
  a narrower scope now overrides only the keys it actually writes and inherits
  the rest, resolving global to volume to source or target.

  ```toml
  [global.retention]
  min = "2d"
  daily = 14
  monthly = 6

  [volumes.retention]
  daily = 7        # min = "2d" and monthly = 6 still apply
  ```

  Previously a scope replaced its parent outright, and any key it did not name
  fell back to the built-in defaults rather than to the parent's value, so the
  block above silently became `min = "1d"` with `monthly = 12`. A
  `RetentionConfig` built in code still replaces wholesale: it states a complete
  policy deliberately, and only the loader knows which keys a file named. A
  config that sets no scoped keys resolves exactly as before.

- **A degenerate retention policy now fails the endpoint it applies to, not the
  whole volume** — a consequence of resolving retention per scope. A config whose
  volume policy is degenerate but whose `source_retention` and per-target
  policies are healthy used to fail wholesale and now prunes normally; a config
  whose volume policy is healthy but which sets a degenerate policy on one target
  used to exit 0 and now exits 1 for that target. Both verdicts are more
  accurate, but a scheduled `prune` that was green can start reporting a failure.

- **`require_mount` values that are not quite right now warn instead of stopping
  the whole config from loading** — a quoted `"true"`, a number, a relative path,
  or a mount point the target does not live under all load, with a warning naming
  the value and what it was read as. The mount check itself still refuses an
  unusable value, per target, when a backup runs; a configuration error would
  have stopped `list`, `status` and `doctor` as well as `run`, for values that
  worked on 0.9.6 and were never unsafe.

  Two values still fail loudly, because coercing them would be a guess with a
  dangerous wrong answer: an empty string, which reads as false and so turns the
  check off without saying so — usually a variable that expanded to nothing — and
  a type with no interpretation, such as a list.

  A configuration that can never run is now named when the config is read rather
  than only at backup time: a target that is not inside the mount point it names,
  or a mount point that resolves to `/`, is reported by `config validate`.

### Fixed

- **The shell completions and man pages had fallen behind the CLI** — 24 flags
  had no entry in their man page and 89 flag/shell combinations were offered by
  no completion at all, `--newest-only` among them: the flag restoring the
  previous `run` behaviour was the hardest one to discover. The man pages ship
  inside the wheel, so this reached users as authoritative documentation of a
  tool that behaved differently.

  The completions are now generated from the argument parser and a test
  regenerates and compares them, so they cannot drift again. Value suggestions
  come from the project's own tables — compression methods from the compression
  table rather than a copied list that had already fallen out of step. Man pages
  stay hand-written, because their prose says things an argparse help string
  cannot, but a test now requires every flag to appear in its page.


- **A `timestamp_format` containing `%z` broke retention and snapshot naming.**
  `strptime` returns an aware datetime for `%z` while every comparison in
  retention is against a naive `datetime.now()`, so pruning died with an
  uncaught `TypeError` — in a destructive path, on a documented option that
  `config import` emits for btrbk's `long-iso`. Separately, a snapshot's name
  was rendered through a round trip that dropped the UTC offset, so the tool
  could not parse the names it had just written.

- **An ssh host reached a shell and ssh's own option parser unchecked.** A
  hostname is now validated where each endpoint stores it, and quoted at the
  one site that builds a shell string. A host beginning with `-` is read by ssh
  as an option, `-oProxyCommand=` among them, no matter how it is quoted.

- **`restore` ignored `-c`.** It resolved `timestamp_format` by searching the
  default locations rather than the configuration it was given, so a pool under
  a custom format was reported as empty with advice naming the wrong prefix.

- **`config import` wrote configurations it could not read back.** Values were
  interpolated straight into TOML, so a path containing a quote produced an
  unparseable file, and — worse — a path containing a backslash produced a
  valid one that loaded as a DIFFERENT directory: `/mnt/a\backup` became
  `/mnt/a\x08ackup`, because TOML reads `\b` as a backspace. Both write paths
  now verify the result loads before saving, and print the conversion instead
  of reporting a successful write of a file that does not work.

- **A btrbk path containing a space was truncated at the space.** btrbk reads a
  directive's value as the rest of the line, verbatim; this importer took only
  the first token, so `volume /mnt/sp ace` converted to `/mnt/sp` — a
  configuration naming a directory the operator never wrote. Internal spacing,
  tabs, surrounding quotes and mid-line comments now match btrbk 0.32.7 exactly.
- `raw+ssh://` asked the **local** filesystem about a **remote** target:
  `preflight_send` checked for the stream locally, so a restore from a raw+ssh
  backup failed every time — or, where both hosts use the same path, passed
  against the wrong file — and the pre-transfer space check measured the machine
  being backed up rather than the one receiving.
- A `raw+ssh` publish could be interrupted or collide: the rename-and-sidecar
  window took no lock, and the `.part` name was built from the local pid, so two
  hosts backing up to one target could overwrite each other.
- `prune` reported success for deletions that never happened. Every delete path
  returned `None` and none of them raised, so an unreadable lock file, a locked
  snapshot, a busy raw target, a refused remote sudo and a plain `btrfs
  subvolume delete` failure all counted as pruned: a pass that removed nothing
  reported "Deleted N snapshot(s)" and exited 0. Deletion now returns what it
  actually did, and `prune` reports and exits from that.
- A retention lock on a raw target pinned nothing — a prune deleted the stream a
  restore was reading — and on the count-based path a locked stream was paid for
  out of the number of backups asked for, leaving one usable where two were
  requested.
- `--convert-rw` and `--sync` were accepted and ignored.
- Cleanup deleted things it had not created. A failed transfer removed
  `{dest}/{name}` on the sole evidence that the path existed, so a pre-existing
  subvolume of the same name was destroyed; `restore --cleanup` treated any
  empty subvolume as debris, which is what an operator's own `btrfs subvolume
  create` looks like. Both now require positive evidence of authorship.
- `transfer_timeout = 0` means "no limit", but it was passed to a poll that read
  it as "zero seconds", so every transfer could fail instantly.
- Notification email could hang indefinitely: the SMTP connection had no timeout.
- A raw snapshot's timestamp lost its timezone, so its fields described the
  wrong moment.
- A run reported how many snapshots it *planned* to move rather than how many it
  delivered, and "3 of 5 delivered, 2 failed" logged the same as "nothing moved".
- SSH readiness was cached for five minutes, so "connectivity and filesystem
  readiness" passed for a host that had gone down.
- `doctor` reported OK for checks that did not run, and said a configuration was
  valid when it contained no usable volumes.
- `restore` overrode an explicitly empty `snapshot_prefix`, which is a supported
  choice, by inferring one from the names it found.
- A transfer could deadlock: the parent kept a pipe open after handing it to a
  child, so the pipeline never saw EOF. All pipeline construction now goes
  through one builder.
- A raw sidecar whose stream had been deleted was listed as a restorable backup.
- `list`, `status`, `config validate`, `raw` and `install` exited 0 for the very
  state they exist to detect — an unreadable source, an empty target, a config
  with no volumes, a skipped install step.
- A newly created snapshot was registered once per command rather than once.
- `skip_remote_lock` was documented and honoured by the endpoint but silently
  dropped by the config parser, so setting it did nothing — and it is the option
  a read-only destination needs.
- `--remove-locks` was accepted and did nothing at all.
- `estimate --check-space` skipped the check while announcing "No data to
  transfer" when it simply could not measure the snapshots.
- `prune` sent a "success" notification for a prune whose own exit code reported
  failure.

- **`prune` ignored the per-scope retention keys and deleted what `run` keeps** —
  the resolution added for `source_retention` and per-target `retention` landed
  on the run pipeline only. The standalone `prune` command, the snapper prune
  phase and `run --dry-run` each still resolved one policy per volume and applied
  it to the source and to every target. On a config with
  `[volumes.source_retention] keep = 8` and a target `keep = 2`, `run` kept 8 and
  2 while `prune` fell back to the global policy and deleted from both, silently,
  exiting 0 and logging a policy line that never mentioned the count.

- **`prune` performed no `require_mount` check** — the only command without one,
  so it would operate inside the empty mount-point directory of a drive that is
  not connected: reporting the real backups as gone and, under a count-based
  policy, deleting whatever an earlier unguarded run had written to the root
  filesystem there.

- **A missing snapshot directory made `prune` skip that volume's targets** — the
  source was skipped with a `continue` that left the loop, so a volume whose
  snapshot directory had been removed never had its backups pruned again, while
  `prune` reported success.

- **The source was pruned even when a target never received its snapshots** —
  `run` prunes the source after transferring, on the grounds that lock reconcile
  holds any snapshot a failed transfer still needs. A target refused before
  transfer, by `require_mount` or any other failure to prepare, never reaches the
  code that takes those locks, so the snapshots it was owed were held by nothing
  and were deleted to satisfy retention. If *every* target was refused the run
  returned before pruning, so a partial failure pruned where a total failure did
  not. The source is now pruned only when every target succeeded.

- **The completion notification miscounted what happened** — `snapshots_created`
  was the number of fully-successful volumes, so a volume that created a snapshot
  and delivered it to one target while another failed reported zero snapshots
  created. And a single-volume run reported `failure` rather than `partial`
  whenever anything failed, even with a backup successfully delivered, which
  reads identically to a run that achieved nothing.

- **`raw list` reported backups that do not exist** — any filename containing
  `.btrfs` was inferred as a stream, so a foreign sidecar such as btrbk's
  `<stream>.info` was listed as a second snapshot, offered for restore and
  counted by retention. A file is now recognised only when everything after
  `.btrfs` is a suffix this tool writes, and that rule is applied at all four
  sites that enumerate streams. The sidecar backfill scan mattered most: it would
  have written a `.meta` for the foreign file, making the phantom authoritative.

- **`--skip-remote-lock` did nothing on a `raw+ssh://` target** —
  `RawEndpoint.set_lock` reads the value, but only `SSHEndpoint` ever stored it,
  so the abort whose own message recommends the flag could not be relaxed on that
  transport.

- **`RawEndpoint.correspondents_of` could raise despite documenting that it never
  does** — a listing entry without a usable name reached an unguarded
  `get_name()`, in a method three call sites rely on.

- **`log_file` recorded only a fraction of the run while looking complete** —
  the shared logger is a standalone `logging.Logger("btrfs-backup-ng")` named
  with hyphens, while 36 modules across `cli/` and `core/` log through
  `logging.getLogger(__name__)` under `btrfs_backup_ng` with underscores. Those
  are unrelated logger trees, so a file handler attached only to the former never
  saw a single line from `run`, `transfer`, `restore`, `operations` or any other
  module that logs that way. An operator reading the log after a failure was
  missing most of what happened. Config warnings were missing for a second
  reason: they were emitted before the handler from that same config was
  installed. Both are fixed, and enabling file logging no longer changes console
  verbosity for the rest of the process.


- **Planning a transfer re-listed the destination once per snapshot**
  ([#106](https://github.com/berrym/btrfs-backup-ng/issues/106)) — deciding which
  snapshots were already at the destination asked the endpoint about each one
  individually, and each question triggered a fresh listing. On an `ssh://`
  destination that is a remote `btrfs subvolume list` per source snapshot, so a
  44-snapshot source spent over two minutes deciding what to send before sending
  anything, logging "Found 44 remote snapshots" once per snapshot. The listing is
  now taken once. Local and `raw://` destinations were unaffected in practice
  because their listings are cached; `SSHEndpoint` is the only endpoint that does
  not cache.


- **A target that failed to prepare was not counted, so the run could report
  success** — `_backup_volume` prepared each destination in a loop, and a failure
  there was logged and recorded in the error list, but the success verdict was
  computed from a variable declared below that loop and never saw it, and the
  failure counter was untouched. A volume with more than one target, where one
  failed and another transferred, therefore exited 0 and sent a "success"
  notification, with the failure visible only as a log line. Single-target
  volumes were unaffected, which is why this went unnoticed.

  This covers every reason a destination can fail to prepare, including
  `require_mount`: the mount check could correctly detect that an external drive
  was absent, refuse the target, and the run would still report that the backup
  had worked. The snapper path already accounted for this correctly; only the
  native path did not.

- **Mount points containing spaces were invisible, breaking the common desktop
  layout** — the kernel escapes space, tab, newline and backslash in every path
  field of `/proc/mounts` (`\040`, `\011`, `\012`, `\134`), and four separate
  parsers compared the raw field against a real path. udisks2 mounts removable
  drives at `/run/media/<user>/<volume label>`, and labels routinely contain
  spaces, so on a systemd desktop the single most common external-drive layout
  could not be matched at all: `require_mount` could never be satisfied, and
  `is_btrfs` reported a btrfs drive as not-btrfs purely because of its label.
  One decoder now serves every reader of the mount table.

- **`require_mount` pointed at a memory-backed filesystem is refused** — `/run`
  is `tmpfs` and always mounted, and udisks2 mounts drives beneath it, so
  `require_mount = "/run"` confirmed a filesystem that is present exactly when
  the drive is absent, and the backup was written into RAM. The containment
  check cannot catch this, because the target genuinely is under `/run`.

- **`require_mount` was silently ignored for `raw://` targets during `run`** —
  the check that exists to stop a backup landing on the root filesystem when an
  external drive is not mounted. `run` carried two inline copies of the mount
  check that decided whether a target was local by testing the path with
  `startswith(("ssh://", "raw://", "raw+ssh://"))`, which put `raw://` on the
  exempt list. So `require_mount = true` on an unmounted `raw:///mnt/usb/...`
  target was skipped entirely, the backup was written to the root filesystem,
  and the run reported success — while `transfer`, which used a different
  implementation, refused the same target correctly.

  All three call sites now share one scheme-aware check. Behaviour for local
  and remote targets is unchanged; `raw://` targets are now checked, which is
  what `require_mount` was always documented to do.

## [0.9.6] - 2026-08-25

### Fixed

- **Re-running an interrupted restore no longer fails on the snapshot it should
  have skipped** — the known issue shipped in 0.9.5. Restore works out the prefix
  a location uses when you did not pass `--prefix`, but only the source learned
  it; the destination was still read under the empty prefix it was built with, so
  a snapshot sitting right there was invisible and got re-sent onto its own name
  (`creating subvolume ... failed: File exists`). Both sides are now read under
  the same prefix.

- **Incremental restore from a `raw://` or `raw+ssh://` backup silently became a
  full transfer** — raw snapshots do not record a prefix, so a comparison that
  demanded an exact match discarded every one of them and no incremental parent
  was ever found. Restores still succeeded, just by sending everything. Where
  neither side declares a prefix, the names are asked instead, using the same
  split every listing already uses.

- **`restore --interactive` and `restore --status` disagreed with `restore
  --list` about the same location** — `--interactive` answered "No snapshots
  available" and the restore then exited 0 having restored nothing, for a
  location the same command without `-i` restores fine; `--status` reported
  "Available snapshots: 0" for a location `--list` shows as full. All three now
  go through one lister and each says which prefix it used.

- **Asking for a snapshot that is already at the destination said nothing
  useful** — it reported "No snapshots need to be restored", the same sentence
  used when nothing matched and when the location was empty. It now names what it
  found and counts it as skipped. The exit status stays 0: a request that is
  already satisfied is not a failed restore, and a restore script that re-runs
  after success must keep working.

- **A collision check that could not run reported "no collision"** — so a caller
  acting on it would receive onto a name that may already exist, which is what
  the check exists to prevent. It now raises, naming the snapshot and the cause.

- **A restore could abort entirely on a destination holding an unrelated
  snapshot** — ordering two snapshots with different prefixes raises, and the
  parent search compared as it walked. An unorderable pair now falls back to a
  full send, which always works.

- **A restore that failed to lock its parent left the snapshot locked forever**
  — the two locks were taken one after the other, but only the block that
  releases them covered the second, so a failure in between left the first in
  place. Since 0.9.5 a lock on a remote target persists rather than dying with
  the process, so the leaked one blocked every later prune of that snapshot
  until it aged out as stale. Both are now released, and only the ones actually
  taken.

- **`--dry-run` could describe a restore different from the one performed** — it
  ignored `--no-incremental` and previewed incrementals that the run would send
  in full, offered snapshots as parents that would not be at the destination
  yet, and named parents that are not on the backup side (where `btrfs send -p`
  computes the delta). The preview and the run now make the same choice from the
  same inputs, which is what sizing a restore over a slow link depends on.

- **An undeliverable backup is found before the transfer starts** — a corrupt
  stream, a missing decompressor or an unsupported cipher were all detected
  inside the send, so the failure arrived mid-transfer. They are checked first.

### Changed

- **`--overwrite` does not replace snapshots and says so** — it reports that
  existing snapshots were left in place and the restore continues, bringing back
  whatever is missing. Nothing at the destination is deleted.

  Replacement was implemented, put through four adversarial reviews, and
  withdrawn. `btrfs receive` names the subvolume after its source, so replacing
  means deleting the existing copy first, and there is no way to stage the
  replacement instead: a received subvolume cannot be renamed or moved, and btrfs
  refuses to clear its read-only flag while the `received_uuid` incremental send
  depends on is set. The destination therefore holds neither copy while the
  transfer runs, and each review found another way for that window to end in
  permanent loss — most conclusively a corrupt backup, where every check of
  whether the replacement could be delivered ran after the deletion, so the last
  good copy was destroyed to make room for something that could not arrive.

  To replace a snapshot, remove it and restore again. See
  [docs/RESTORE-OVERWRITE.md](docs/RESTORE-OVERWRITE.md) for the full reasoning
  and the constraint a future design has to satisfy.

## [0.9.5] - 2026-08-23

The "a lock only one process can see is not a lock" release.

Two processes could not see each other's work on a remote target. A restore
pinned the snapshot it was reading and a prune skipped what was pinned, but the
pin lived in the restoring process's memory, so a prune running anywhere else --
a cron job, a second terminal, another machine -- was free to delete the
snapshot mid-restore. Locks are now recorded on the target itself, where anything
that can reach it can see them.

This release also drops the paramiko dependency, removes the wall-clock limit on
transfers in favour of detecting an actually-stuck one, and fixes a set of
commands that described the same backup location three different ways.

### Known issue

Re-running an interrupted restore **without** `--prefix`, against a location
whose prefix has to be inferred, fails on the first snapshot it should have
skipped ("creating subvolume ... failed: File exists") instead of skipping it.
The restore reports the failure rather than claiming success. Passing an
explicit `--prefix` avoids it entirely. This predates the release; the fix is
tracked for 0.9.6.


### Added

- **A prune in one process can no longer delete the snapshot a restore is
  reading in another** — a restore pins the snapshot it reads and a prune skips
  what is pinned; both halves worked, on different data. The pin lived in the
  restoring process's memory, and a prune is normally a different process: a
  cron job, a second terminal, a scheduler on another machine. It lists the
  destination fresh, so every snapshot it sees carries an empty lock set —
  including the one being read at that moment. The delete went through and the
  restore failed partway with a stream whose parent had gone. Locks are now
  recorded on the destination itself, beside the data they protect, so any
  process that can reach it can see them. `ssh://` and `raw+ssh://` targets
  persist locks; a local `raw://` target still keeps them in memory for the run
  and still says so.

  A snapshot pin is shared: any number of restores and transfers may hold the
  same snapshot at once, and it stays pinned until the last of them lets go —
  which is what the in-memory contract already meant. Whole-target locks, held
  by mutating operations, remain exclusive. `raw+ssh://` gains the per-target
  mutual exclusion local `raw://` always had, and across machines, which the
  local flock never provided.

  Mutual exclusion is built from operations that are already atomic on the
  remote filesystem — `mkdir`, which exactly one of any number of racing
  creators wins, and `mv`, which is what makes breaking an abandoned lock safe.
  Staleness is computed entirely from the destination's own clock, never by
  comparing a local one: the two machines this was developed against disagree by
  four seconds, and client-side arithmetic would let the fast one break locks
  that are alive. A lock whose heartbeat stops is broken by the next contender,
  so a crashed restore costs one staleness window rather than needing manual
  cleanup, and an interrupted run releases immediately on Ctrl-C rather than
  waiting that window out.

- **`restore --status` and `--unlock` tell the truth about a remote target** —
  they used to answer "This target does not persist locks", which was accurate
  and useless: a backup tool that reports it cannot protect a restore is not
  protecting the restore. They now report what actually holds a remote
  destination, every holder of a snapshot rather than just the first, and a
  holder whose record cannot be read as `unknown` rather than omitting it —
  because "something holds this and we cannot say what" must never be rounded
  down to "nothing holds this". `--unlock` clears leftover pins without
  disturbing a lock a running operation is holding.

- **`--skip-remote-lock`, and a matching `skip_remote_lock` target option** — a
  destination that cannot record a lock now stops the operation rather than
  continuing unprotected, because continuing leaves the restore exposed while a
  prune elsewhere sees nothing holding the snapshot. That default is wrong for
  one real setup: a destination you can read but not write, where no lock can be
  taken and none is needed. This is how you say so. It relaxes only that
  failure — locks are still read, so nothing begins reporting a target as
  unlocked without having looked.

- **Two transfers can no longer create the same subvolume on one destination** —
  nothing serialised two machines writing to one `ssh://` target. What btrfs
  actually does was measured before this was designed, because the answer
  changes the fix: two `btrfs receive` runs into one directory under different
  names both succeed, while two under the same name leave one failing with
  "creating subvolume ... failed: File exists" — after it has transferred the
  entire snapshot. So the lock is scoped to the destination subvolume rather
  than the whole target, and backups of `/`, `/home` and `/var` to one
  destination still run in parallel. What it adds is timing and attribution: the
  clash is refused before the stream starts, by a message naming the host and
  process holding the path, across machines. For snapper it spans both halves of
  the transaction — the receive into `.snapshots/<n>.incoming` and the rename
  that publishes it — so a second writer cannot publish between them.

- **`restore --list` finds the prefix a location uses, like `restore` does** — the
  most ordinary command there is, `restore --list <destination>` with no
  `--prefix`, answered "No snapshots matched" for a location holding a perfectly
  good backup, then told the operator to re-run with a prefix it had just worked
  out for itself. It now lists them, and says which prefix it used. An explicit
  `--prefix` is still never second-guessed, and a location holding two prefixes is
  still reported rather than guessed between.

- **Transfers are no longer limited by a wall clock at all, by default** — a
  transfer that is moving data is succeeding, and ending it because a timer expired
  confused operator policy with fault detection. Any fixed value is a guess about
  link speed times dataset size; the old fixed one hour, in nine places, ended first
  syncs that were working perfectly (36 GB over 100 Mbit is about 50 minutes at line
  rate before overhead). `transfer_timeout` in `[global]` remains for operators who
  need a real deadline, and defaults to unlimited. Reported by Michael J Gruber
  (@mjg) (#93).
- **Stall detection** — a transfer that stops moving data is now given up on within
  minutes instead of waiting out the wall clock, and a transfer that is merely slow
  is never stopped by it. Bytes are counted out-of-band from `/proc/<pid>/io`, so
  nothing is routed through Python and the direct pipe stays direct. The local
  `btrfs send` runs under sudo and its counters are unreadable to us; the ssh
  process is ours, and bytes leaving on the socket is the same evidence. Where
  nothing can be read the check disables itself rather than reporting a healthy
  transfer as stuck. It applies only while the send is running, so the tail of a
  transfer -- where the remote is still applying what it already received -- is
  never mistaken for a stall. Tunable as `transfer_stall_timeout` in `[global]`;
  0 disables it.

### Fixed

- **`raw backfill-metadata` warned that a `raw+ssh://` target was not
  lock-protected, three lines before locking it** — the warning was true when
  `SSHRawEndpoint.target_lock` was a no-op. It is not true now, so the command
  was telling operators to work around a hazard that no longer exists. Removed.

- **`restore --interactive` and `restore --status` disagreed with `restore
  --list` about the same location** — `--list` learned to work out the prefix a
  location actually uses; its siblings did not. `--interactive` answered "No
  snapshots available" and the restore then exited 0, having restored nothing,
  for a location the same command without `-i` restores fine; `--status`
  reported "Available snapshots: 0" for a location `--list` shows as full. All
  three now go through one lister, so a fourth view cannot drift from them, and
  each says which prefix it used rather than substituting one silently.

### Changed

- **`paramiko` is no longer a dependency** — it was a second implementation of one
  case, a transfer to a remote whose `sudo` requires a password, and every install
  carried `cryptography`, `pynacl`, `bcrypt` and `cffi` for it: 31 MB and 13
  packages against 9.2 MB and 6 without. Nothing is lost. The OpenSSH pipeline
  performs the same transfer, and was measured against a real remote with
  password-required sudo doing it byte-identically, compressed and uncompressed,
  in both the backup and restore directions, with a wrong password failing cleanly
  and writing nothing. Two implementations of one path had already cost a release:
  the pipeline compressed and paramiko did not, so password-sudo users silently got
  uncompressed backups. It also opened its own connection, bypassing the OpenSSH
  ControlMaster the rest of the tool sets up. SSH host-key verification is
  unchanged.

## [0.9.4] - 2026-08-20

The "a check that did not run is not a check that passed" release.

Twenty-one fixes, nearly all of them the same defect wearing different clothes: a
value read from your config and then quietly dropped, a check that could not run
reported as though it had passed, a setting recognised and discarded without a
word. Individually small; together they meant the program could tell you it had
done something it had not. Several were found by running the real CLI between two
real machines rather than by reading code, and two of those could restore the
wrong data while reporting success.

Nothing here changes a config format. Two behaviours change, both listed below.

### Security

- **A root systemd unit could run a binary an ordinary user can replace** — `install`
  checked the executable's own owner and mode, but replacing a file is `unlink` plus
  `create`, which is a write to the *directory*. A root-owned binary in a
  user-writable directory passed the check. Every parent directory is now checked
  for a non-root owner and for group/other write, with sticky directories exempted.
  A binary that cannot be examined at all now says the check did **not** run,
  instead of producing the most reassuring output of any case.
- **Symlink race when writing as root** — the guard was `islink()` followed by the
  operation, so a link swapped in between the two won and the write or `chmod`
  followed it. Both now use `O_NOFOLLOW`, moving the check inside the kernel call.

### Fixed

- **`restore` could restore a different volume than you asked for, and report
  success** — on `raw+ssh://`, `--prefix` was ignored for every stream that has a
  `.meta` sidecar, which is all of them. Two volumes sharing one destination
  returned both sets; `restore --prefix X` restored the other volume too, said
  "Restored: 2, Failed: 0", exited 0, and used one volume's snapshot as the
  incremental parent of the other's stream. Found on real hardware between two
  machines.
- **An explicit `--prefix` was silently replaced** — prefix inference ran even when
  you had named the prefix yourself, so a restore asked for one volume listed and
  restored another, and exited 0. Inference now applies only when no prefix was
  given; a prefix that matches nothing is a mismatch to report, not a guess to make.
- **Compression to an ssh:// target delivered zero bytes on Debian, Ubuntu, Alpine
  and most NAS boxes** — POSIX gives an asynchronous list `/dev/null` for stdin when
  job control is off; bash exempts a backgrounded pipeline, dash and busybox ash do
  not, so the remote decompressor lost its input. Measured with `btrfs receive`
  replaced by a byte counter.
- **A signal left `btrfs receive` running on Debian-family remotes** — the remote
  command is a script wrapped in `sh -c`, with the cleanup trap in the inner
  shell. bash exec-replaces a sole final command, so the signal reached the trap;
  dash and busybox ash fork and wait, so the signal killed the outer shell and
  orphaned the decompressor, the receive and the subshell. The wrapper is now
  `exec`-ed, which behaves identically on both. Reproduced in a dash container --
  hardware testing runs against a bash host, so it could not have surfaced there.
- **A `raw://` location holding backups reported as empty** — a prefix mismatch
  produced "no snapshots found" and exit 0, because the prefix diagnostics could not
  parse raw filenames (`name.btrfs`, plus compression and encryption suffixes). They
  now report what prefixes are actually present, on local and remote raw alike.
- **`ssh_port` was ignored on restore** — a non-standard port connected to 22 while
  logging that the target's `ssh_port` had been applied. The value was read, then
  dropped by the endpoint's key whitelist because it was threaded under the wrong
  name.
- **`ssh_password_auth = false` did nothing on restore** — the option could not be
  returned under any value it could hold, and the endpoint read the setting from a
  command-line flag that does not exist. It was honoured for backups and ignored for
  restores, so a target configured to refuse password authentication refused it when
  writing and offered it when reading back.
- **`doctor` reported two things it had not established** — it probed for a command
  named after the compression *method* rather than its binary, so `lzo` (whose binary
  is `lzop`) was reported missing though it works; and a remote decompressor check
  passed on the strength of the local machine. Both now report only what they
  actually checked.
- **`estimate` presented a floor as a measurement** — incremental sizes come from
  `btrfs send --no-data`, a metadata-only stream whose real transfer runs roughly
  10-100x larger, and it was printed as "Total data to transfer" with no caveat and
  emitted as a bare number in `--json`. Underestimated totals now print as "AT
  LEAST" with the reason, affected rows are marked, and `--json` carries
  `total_transfer_is_lower_bound`. Measured full transfers stay unqualified.
- **A transfer killed by our own timeout looked like an ssh problem** — both monitors
  logged "Transfer timed out" and recorded no reason anywhere the run summary or
  transaction log would find it, so the natural suspect was an ssh idle timeout,
  which also commonly defaults to an hour. The limit now names itself and its value,
  and says it is not an ssh idle or keepalive timeout.
- **Overlapping runs** — a timer firing while the previous transfer was still running
  started a second run over the same volumes and targets. systemd declines to start
  a second copy of one unit, so the packaged timer was covered by systemd rather than
  by us, and a manual run racing a timer run was not covered at all. A run lock keyed
  on the config file now covers every target type.
- **The source tarball was missing `examples/`, so building from source failed its
  own tests** — the manifest named `config.example.toml`, a file that stopped
  existing when the examples moved into `examples/`, so it shipped nothing. Four
  tests read those files at collection time and failed from the sdist. This is the
  same failure that lost `docs/` in 0.9.3, so the check is now on the invariant:
  any directory the test suite reads must be declared in the manifest.
- **btrbk migration produced configs that did not match the source** — `no` (btrbk's
  "off") was carried across as a literal value, producing `ssh://no@host/...` and
  `ssh_key = "no"`; a `target` declared at global scope was discarded, migrating to
  volumes with no destination at all; an explicitly declared target type was overruled
  by an unrelated option, silently changing the backup format; `backend` was ignored,
  so a config that chose the non-sudo backend migrated to one that elevates; ssh
  options were emitted on purely local targets; and six recognised options were
  stored and never read. Each is now carried, or reported as not carried, with the
  reason.

### Changed

- **A run that cannot start because another is in progress now exits non-zero.** The
  cause is benign, but no backup was made, and a run that did not happen must not
  report success. A timer reporting this repeatedly means the schedule fires faster
  than a run takes.
- **`compress` on a local btrfs target now warns at config load.** It was accepted and
  then dropped — correctly, since compressing only to decompress on the same machine
  buys nothing — but nothing said so, and the config read as though backups were
  compressed. The backup still runs, uncompressed.

### Documentation

- **`core/progress.py` now states where its bars actually appear** — every `ssh://`
  btrfs backup takes the direct-pipe path and never reaches that module, and even on
  the traditional path it needs an interactive terminal, a known size, no compression
  and no rate limit. The direct pipe cannot use it because those helpers count bytes
  by pulling them through Python, which is exactly what that path exists to avoid.

Thanks to Michael J Gruber (@mjg) for the transfer-timeout report and the questions
that led to the timeout, overlapping-run and `estimate` fixes (#93).

## [0.9.3] - 2026-08-16

### Fixed

- **Docs are now included in the source tarball** — `docs/` was omitted from the sdist, so building
  from the source distribution and running the test suite failed at collection when
  `test_docs_commands.py` read `docs/SNAPPER-INTEGRATION.md`. The docs now ship, fixing distribution
  builds. Thanks to Michael J Gruber (@mjg) for the detailed report (#91).

## [0.9.2] - 2026-08-05

The config, restoration, and SSH-reliability polish release.

### Added

- **Restore snapper snapshots from `raw://` / `raw+ssh://` backups** — snapper backups on non-btrfs or
  remote raw destinations are now fully restorable, with snapper metadata reconstructed so the restored
  snapshot is operationally complete (`snapper diff`/`undochange`/`cleanup` all work).
- **Pick an exact snapper backup on restore** — `snapper restore` gains `--backup-name` and `--date` for
  when a snapper number was reused after a prune.
- **Encryption in the setup wizard** — `config init --interactive` now prompts for encryption on raw
  targets (none/gpg/openssl_enc), requiring a GPG recipient when you choose gpg.
- **Config typos are reported** — loading a config warns about any key it doesn't recognize (e.g.
  `retenion`), so a misplaced setting can't silently do nothing.
- **Decrypt options for restore** — `restore` / `snapper restore` accept `--gpg-keyring` /
  `--openssl-cipher` for encrypted raw backups with a non-default keyring/cipher.

### Fixed

- **`ssh_port` is now honored** — a non-default port in the config was silently ignored (connections
  always used 22).
- **SSH could fail with a misleading "authentication failed"** — the internal ControlMaster socket path
  could exceed the OS Unix-socket length limit and abort the connection; the path is now kept short.
  Affected *every* remote operation on some hosts.
- **btrbk migration preserves retention faithfully** — no longer drops yearly retention, mis-reads `3m`
  (months) as minutes, emits an unloadable minimum, or ignores per-subvolume retention; warns clearly
  about btrbk rules with no equivalent.
- **Remote failures show the real reason** — a failed remote `btrfs receive` reports the actual cause
  (e.g. "No space left on device", "cannot find parent subvolume") instead of a generic message.
- **`doctor` backup-age & failure checks work again** — they mis-read transaction-log timestamps and
  silently failed with a confusing warning.
- **`install` service works outside `/usr/bin`** — the generated systemd service points at the real
  binary (via PATH), fixing `--user` / pipx / uv / venv installs.
- **Wizard configs handle special characters** — paths/passwords with backslashes or quotes now
  serialize to valid config instead of a broken or silently-altered one.
- **`snapper restore --list` no longer requires a target config.**

### Security

- **Shell-quoting sweep** — paths and snapshot names are consistently shell-escaped everywhere they reach
  a remote shell, closing whitespace/metacharacter fragility and injection vectors (two ran under remote
  sudo).

### Documentation

- Documented `--ssh-auth-sock` in completions and manpages for every command that accepts it.
- Audited every README example against the real CLI; documented `ssh_host_key_policy`.

## [0.9.1] - 2026-07-27

The verification and SSH-security hardening release.

### Security

- **SSH host-key verification (fixes a man-in-the-middle exposure).** The password-based-sudo
  transfer path accepted any server host key — including a *changed* one — without verification,
  so a network attacker able to impersonate the backup destination could capture the SSH and sudo
  passwords and the backup stream. Key-based SSH auth was unaffected. Host keys are now verified
  on every connection (a changed key is refused loudly), and under `sudo` they are verified and
  pinned against the invoking operator's `~/.ssh/known_hosts` rather than root's. A security
  advisory (GHSA) accompanies this release.
- **Configurable host-key policy.** New `ssh_host_key_policy` (config) / `--ssh-host-key-policy`
  (CLI): `accept-new` (default — trust first contact, reject a changed key) or `strict`
  (known_hosts-only, refuse an unknown host). An unrecognized value fails closed.
- **Predictable-path hardening.** SSH ControlMaster sockets now live in an unpredictable, private
  0700 directory (preferring `$XDG_RUNTIME_DIR`) and are cleaned up on close — closing a local
  socket-hijack vector. The internal command lock and the raw+ssh remote metadata write no longer
  use predictable, symlink-plantable paths.
- Operator `ssh_opts` (e.g. an explicit `StrictHostKeyChecking`) are now honored on the primary
  SSH transport (they were previously silently dropped).

### Changed

- **raw+ssh host-key policy is now explicit.** raw+ssh targets previously set no
  `StrictHostKeyChecking` and inherited the ambient default (which, under `BatchMode`,
  accidentally refused unknown hosts). They now default to `accept-new`, matching the btrfs
  transport. If you relied on the accidental refuse-unknown behavior, set
  `ssh_host_key_policy = strict`.

### Fixed

- **Verification now actually verifies.** The `verify` command previously could report success on
  a non-subvolume, skipped data on stream checks, ran a false-negative full-restore, and never
  consulted a raw backup's sealed checksum. Verification now validates real structure, recomputes
  and compares sealed checksums, and does a standalone full-restore; output is honest ("checked N
  of M", a distinct *unverifiable* state, and a top-level JSON `verdict`), with `--all` to verify
  every snapshot.
- **Crash-atomic persistence.** Operation state, transfer manifests, and lock files are now written
  atomically (temp + fsync + rename), so a crash mid-write can no longer corrupt resume state or a
  lock file.

### Removed

- Deleted the unused, deprecated `ssh_transfer` module and a stale `master.py.new` draft.

## [0.9.0] - 2026-07-24

A large reliability release. The incremental-backup engine has been re-architected around a
single, UUID-based notion of snapshot identity, and the retention/prune system has been
hardened against several ways it could delete the wrong backups. Enhanced backup
**verification** is the headline focus of the next release (0.9.1).

### Changed

#### Incremental backups now identify snapshots by their btrfs UUID, not their name

Deciding which existing backup a new incremental backup should build on ("the parent") used to
be done inconsistently — some code paths matched snapshots by name/timestamp, others by btrfs
UUID — which could pick a parent the destination does not actually have. btrfs then either
refused the transfer ("cannot find parent subvolume") or the tool silently fell back to
re-sending the whole subvolume as a full backup. Snapshot identity is now unified across the
whole tool onto one rule: a backup on the destination corresponds to a source snapshot when it
is the received copy of it (matched by btrfs `received_uuid` for btrfs targets, and by name for
raw stream targets). Concretely:

- A **re-created snapshot** (same name, new content/UUID) is no longer mistaken for the old one
  — it is correctly seen as new and backed up, and never used as an incorrect parent.
- **Restore** picks the incremental parent by the same correspondence, and **raw** backups now
  get proper incremental chains (with the parent recorded in the backup's metadata).
- **Snapper** backups use the same logic, so a snapper number reused after a prune no longer
  causes the wrong snapshot to be skipped or re-sent.
- Within a single run, a fresh batch of snapshots forms a tight incremental chain instead of
  every snapshot being sent in full.

### Fixed

#### Remote backups over SSH now work with a passphrase-protected key under sudo

Backups run as root (btrfs send/receive need it), and `sudo` clears `SSH_AUTH_SOCK`. If your
SSH key is passphrase-protected, the usable (decrypted) key lives only in your ssh-agent — so
without the agent, the remote server accepts your public key but the client can't sign, and
the backup fails with a confusing "Permission denied". btrfs-backup-ng now **auto-discovers
your ssh-agent socket** across common locations (`~/.ssh/agent/`, `/tmp/ssh-*`,
`/run/user/<uid>/…`, gpg/gcr, 1Password, Bitwarden), validating each is a socket you own, so a
plain `sudo btrfs-backup-ng run` just works in most setups. For unusual setups you can pin the
socket explicitly with a new `ssh_auth_sock` target option (or `BTRFS_BACKUP_SSH_AUTH_SOCK`
env var, or `--ssh-auth-sock` on restore/verify/estimate/snapper), and a preserved
`SSH_AUTH_SOCK` (via `sudo -E`) is honored. When authentication does fail, the error now spells
out exactly how to fix it for your situation. **SSH password authentication continues to work
unchanged** — the agent is only tried first, then it falls through cleanly to password (and a
dead/stale agent socket is never selected, so it can't get in the way).

#### Snapshots you still need are no longer deleted after an interrupted backup

When a backup transfer fails or is interrupted partway, btrfs-backup-ng marks the source
snapshot (and the parent it builds on) as "still needed" so cleanup won't remove it before
the backup can be retried. That mark was being written to disk but **never read back on the
next run**, so the next cleanup saw the snapshot as unneeded and could delete it — breaking
the incremental chain the unfinished backup depended on. The mark is now read back and
honored across runs, so a snapshot a pending or failed transfer needs survives cleanup
until the transfer actually completes.

Related hardening to the same lock file:

- **Cleanup now refuses to delete anything if the lock file is unreadable or corrupt**,
  with a clear message, rather than guessing everything is unneeded and pruning a snapshot
  it can no longer tell is protected.
- The lock file is now written **atomically and crash-safely** (temporary file, flush to
  disk, atomic rename, directory flush), so an interrupted write can't leave a half-written
  file that later reads as "nothing is protected."
- Updates to the lock file are **serialized**, so backing up to several targets at once (or
  two runs overlapping) can no longer clobber each other's marks.

#### Retention can no longer delete the wrong backups

Several ways the retention/prune system could delete backups you meant to keep have been fixed:

- A backup whose name can't be parsed as a timestamp, or that is dated in the future (from clock
  skew or a different timezone), can no longer take over the "always keep the newest backup" slot
  and cause your real newest backup to be deleted. Such backups are now kept and set aside, with a
  warning, instead of distorting the retention math.
- An invalid minimum-retention value (`min`) now fails loudly and deletes nothing, instead of
  silently falling back to "1 day" and pruning far more than intended. Invalid values are also
  rejected up front when the configuration is loaded.
- For **raw** (stream-file) backups, retention will never delete a parent stream that a kept
  incremental backup still depends on — deleting it would make the newer backup impossible to
  restore.
- **Weekly** retention now uses ISO week numbering, so a week that straddles a year boundary is
  counted as one week instead of being split into two (which kept one extra).
- `min = "1M"` / `"1y"` now mean one **calendar** month/year (not a flat 30/365 days), so the
  minimum-retention window lines up with the monthly/yearly buckets.

#### `prune` now confirms before deleting, and refuses a "keep only the latest" policy

Running `prune` interactively now shows what it will delete and asks for confirmation first (skip
it with `--yes`). Automated (non-interactive) runs of a normal policy still proceed without
prompting so scheduled jobs don't hang. A **degenerate policy that would keep only the latest
backup** (all time buckets set to 0 with a near-zero `min`) is now refused unless you pass
`--force`, even when non-interactive — so a mistyped or empty retention configuration can't
quietly wipe out your backup history.

#### Snapshots created in the same second now chain incrementally

Two snapshots taken within the same second (for example a fast pre/post pair) share a one-second
timestamp, and the transfer planner previously could not order them — so each was sent as a full
backup. A stable secondary ordering now lets the later one build incrementally on the earlier,
producing smaller, faster transfers.

## [0.8.5] - 2026-07-22

This release makes **raw backups first-class** — a raw backup now carries everything
needed to list, check, and restore it, and there are commands to manage raw backups
directly — and hardens reliability across the board, including a fix for **standard
btrfs restores**, which were broken.

### Security

#### An openssl cipher of "none" (or an AEAD mode) could write a plaintext raw backup labelled as encrypted

Continuing the plaintext-exposure class fixed in 0.8.4 (GHSA-vr25-6vrh-869j, CWE-311/312):
a raw target configured with `openssl_cipher = "none"` — or with an AEAD mode such as
`*-gcm` that `openssl enc` cannot actually use — would previously pass a syntactic check
and could write a stream that was **not encrypted** while the backup was recorded as
encrypted. The cipher is now validated by *meaning*, not just shape: `none`, AEAD modes,
and ciphers the local `openssl` does not support are rejected up front with a clear error,
at backup time and again at restore time. If you use raw-target encryption, verify your
existing backups are genuine ciphertext (see `raw verify` and, for remediation, `raw
encrypt`).

### Added

- **Raw backups are now self-describing and self-checking.** Every raw backup writes an
  authoritative sidecar (`.meta`) recording its compression, encryption, cipher, size,
  and a checksum of the exact bytes written — so a backup can be listed, integrity-checked,
  and restored without guessing from the filename. New backups need no manual backfill.
- **New `raw` command family** for managing raw backups directly:
  - `raw list` — list raw backups at a `raw://` or `raw+ssh://` target.
  - `raw verify` — recompute each backup's checksum and report ok / corrupt / error.
  - `raw backfill-metadata` — write authoritative sidecars for older sidecar-less streams.
  - `raw encrypt` — encrypt existing plaintext raw backups in place (remediation for the
    0.8.4 issue), with a live decrypt-to-identical proof before anything is removed, and
    honest documentation that a plain delete does not physically erase data on
    copy-on-write filesystems or SSDs.
- **Restore from a `raw+ssh://` backup** (streamed back over ssh; decrypt/decompress happen
  locally so secrets never leave the host), plus a preflight that checks the needed tools
  are installed before a transfer starts.
- `--no-check-space`, `--force`, and `--safety-margin` now actually take effect for `run`
  and `transfer` (previously parsed but ignored), so a conservative space estimate on a
  raw target can be overridden.

### Fixed

- **btrfs restore now works — local AND remote.** Restoring a native btrfs backup was
  broken (it failed immediately with an internal "source hasn't been set" error). Local
  btrfs restores — full and incremental — now work and are verified byte-identical, and
  restore from a *remote* `ssh://` btrfs source works too: the stream is read back over
  ssh, and full, `--all`, and incremental top-up restores were all verified byte-identical
  against a real remote btrfs host.
- **Transfers no longer hang on a failed or interrupted stream.** The send/receive
  supervisor could block for up to an hour when the receiving side exited early (e.g. the
  subvolume already exists, or the disk is full) and the sending side did not notice; it
  now terminates cleanly and reports the failure. Fixed for local btrfs, ssh, and raw.
- **Compressed raw backups are restorable.** Compression is recorded in the sidecar, so a
  compressed raw backup can be decompressed on restore instead of failing.
- A failed transfer can no longer report success, and a partial/incomplete backup is no
  longer published as complete or left behind to be mistaken for a good backup.
- A raw backup is verified against its recorded checksum before it is restored, so silent
  corruption is caught rather than written back.
- A raw backup that used an unknown compression or encryption method, or needs a tool that
  is not installed, now fails with a clear message instead of silently producing a corrupt
  restore or a raw traceback.
- A damaged or unreadable raw sidecar warns and falls back to the filename instead of being
  silently dropped, and one bad sidecar no longer hides the healthy backups beside it.
- A `raw+ssh://` target that cannot be reached is reported as an error, not as "no backups".
- **Every failure is delivered as a clear, plain-language message** with a suggested next
  step, and the tool no longer prints a raw Python traceback: unexpected errors are shown as
  one line (with `--debug` for the full trace), a same-second snapshot name collision is
  explained instead of surfacing btrfs's misleading "Read-only file system", and command
  failures carry the real reason.
- A per-target lock serializes concurrent raw operations (backup / prune / backfill /
  encrypt) on a local raw target so they cannot corrupt each other.

## [0.8.4] - 2026-07-19

### Security

#### CRITICAL: raw-target encryption was silently ignored — backups written in plaintext

A raw target (`raw://` or `raw+ssh://`) configured with `encrypt = "gpg"` or
`encrypt = "openssl_enc"` silently wrote **unencrypted** backups. The config
loader dropped the `encrypt` / `gpg_recipient` / `gpg_keyring` / `openssl_cipher`
settings, so the raw endpoint received no encryption method and produced plaintext
stream files — with no error and no warning. This affects all prior releases that
advertised raw-target encryption.

- **Impact:** anyone who configured GPG or OpenSSL encryption for a raw target has
  backups stored in cleartext, potentially on offsite or untrusted destinations.
- **Fix:** the loader now carries the encryption settings and threads them to the
  endpoint, and the entire path **fails closed** — if encryption is requested but
  cannot be applied, the backup aborts with an error instead of writing plaintext.
  Encryption is validated at config load (`encrypt = "gpg"` requires a
  `gpg_recipient`; encryption is rejected on non-raw targets). Verified end to end
  against real gpg and openssl: the output is genuine, decryptable ciphertext that
  contains no plaintext.
- **Action required — the fix protects future backups only.** It cannot
  retroactively encrypt, nor un-expose, backups already written in cleartext. If
  you used raw-target encryption:
  - Treat existing raw "encrypted" backups as **cleartext that may already have
    been exposed** — they may have been replicated, synced to cloud storage,
    snapshotted by the destination filesystem, or written to media that cannot be
    reliably wiped. At-rest re-encryption reduces future exposure but cannot undo
    prior exposure.
  - Where practical, **recreate the affected backups from source** with this
    version.
  - A utility to encrypt existing raw backups in place (and securely remove the
    plaintext) is planned for the next release, for cases where recreating from
    source is impractical — with the same caveat that prior exposure cannot be
    undone.

### Fixed

#### Failed transfers can no longer be reported as successful backups
- Transfer success is now determined by a verified result — every process must
  exit 0 and a post-completion check must confirm the received subvolume/stream —
  instead of by subvolume existence or a warn-only exit code. A failed or partial
  `btrfs send`/`receive` (SSH, raw, and chunked paths) is no longer reported as
  success with a zero exit code; the orchestration layer raises on any failure so
  `run`/`transfer`/`snapper backup`/the legacy path exit non-zero and notifications
  reflect the real outcome. Partial-subvolume cleanup on failure is gated so a good
  backup is never deleted on an inconclusive verification.

#### Failed transfers no longer poison future runs
- A killed or failed transfer left a partial subvolume (local/SSH/chunked) or raw
  stream file at the destination that the next run's skip-detection mistook for a
  completed backup, silently skipping the real transfer. Partials are now removed
  by their exact path, on the failure path only. The standard receive timeout was
  raised from 300s to match the 3600s send timeout so a legitimately slow receive
  is not killed into a partial.

#### timestamp_format honored across all commands
- The configured `timestamp_format` is now applied consistently everywhere a snapshot name is generated or parsed, completing the work started in 0.8.3:
  - **snapper backup** names (raw stream filenames and metadata sidecars) use the configured format on both entry paths (config-driven `run` and standalone `snapper backup`).
  - **verify** and **restore** direct mode parse custom-named snapshots instead of silently skipping them (`verify` could otherwise report "all verified" while skipping); restore threads the same resolved format into both the source and destination endpoints, so skip-existing and incremental-base detection work on re-restore.
  - **retention/prune** parse custom-format snapshot times, so custom-named snapshots are pruned instead of kept forever.
  - **estimate** direct mode, **snapper status** (backed-up/pending counts), and **snapper list** (previewed name) honor the format.
- New `--timestamp-format` flag on `snapper backup`/`list`/`status`, `verify`, `restore`, and `estimate`; otherwise the `[global] timestamp_format` is used.

### Added
- Mutation-verified enforcement tests that assert every command threads the configured `timestamp_format`, so a regression fails CI.

## [0.8.3] - 2026-07-18

### Added

- **Explicit empty `snapshot_prefix`** is now honored — set `snapshot_prefix = ""` for bare-timestamp snapshot names. An omitted/unset prefix still auto-derives from the volume path (`/home` → `home-`) as before. Pair an empty prefix with a strict `timestamp_format` and a dedicated `snapshot_dir` so unrelated subvolumes are not mistaken for snapshots.
  - *Migration note:* an existing config that sets `snapshot_prefix = ""` (rather than omitting the key) now yields bare-timestamp names instead of the previously auto-derived default. Omit the key to keep the derived prefix.

### Fixed

#### Snapper Backup to Remote and Raw Targets
- **`snapper backup` now honors `ssh://`, `raw://`, and `raw+ssh://` destinations** instead of always writing locally; snapper backups are routed through the endpoint layer like regular backups
- **Native snapper layout on remote btrfs targets** — each snapshot is received into `.snapshots/{num}/snapshot` alongside its `info.xml`; raw targets get a numbered stream plus a metadata sidecar

#### SSH Transfers
- **SSH config keys are preserved through endpoint construction** — a `ssh://user@host` username, `--ssh-sudo`, and `--ssh-key` are no longer dropped (the username previously fell back to `$SUDO_USER`)
- **Transfer verification checks the exact received subvolume path** (`btrfs subvolume show`) instead of a filesystem-wide name search, which previously reported good snapper backups as failed and deleted them, and could otherwise match a sibling snapshot
- **Endpoint construction no longer fails when `~/.ssh` does not exist** (fresh accounts, containers, CI) — the ControlMaster directory is created with its parents
- **`timestamp_format` is now honored** for backup naming (was silently ignored)

#### Other
- Raw send streams are written to the target file rather than the current directory
- Remote `raw+ssh` metadata sidecars are written correctly; snapper cleanup uses `btrfs subvolume delete` for read-only received subvolumes
- Removed stray terminal output (info.xml/metadata `tee` echo) and a dead receive-log diagnostic that logged a spurious warning after every successful transfer

## [0.8.2] - 2026-01-10

### Added

#### Raw Target Support
- **Raw targets** for writing btrfs send streams to files instead of `btrfs receive`
- Enables backups to non-btrfs filesystems (NFS, SMB, cloud storage)
- New URL schemes: `raw:///path` (local) and `raw+ssh://user@host/path` (remote via SSH)
- **Compression support**: gzip, pigz, zstd, lz4, xz, lzo, bzip2, pbzip2
- **Encryption options**:
  - GPG (public-key): `encrypt = "gpg"` with `gpg_recipient`
  - OpenSSL (symmetric): `encrypt = "openssl_enc"` with passphrase via `BTRFS_BACKUP_PASSPHRASE` or `BTRBK_PASSPHRASE` environment variable
- **Metadata sidecar files** (`.meta`) for tracking incremental chains and restore information
- **Restore from raw backups** back to btrfs filesystems
- **btrbk migration support**: `config import` now converts `raw_target_compress` and `raw_target_encrypt` settings
- **Doctor command integration**: checks for raw target tool availability (compression, GPG, OpenSSL)
- New `RawEndpoint` and `SSHRawEndpoint` classes in endpoint module
- New `RawTargetConfig` schema for TOML configuration

#### Snapper Integration
- **Full Snapper integration** for backing up and restoring Snapper-managed snapshots
- New `snapper` subcommand with dedicated operations:
  - `snapper detect` - Discover Snapper configurations on the system
  - `snapper list` - List snapshots for one or all Snapper configs
  - `snapper backup` - Back up snapshots to local or remote targets
  - `snapper restore` - Restore snapshots from backup locations
  - `snapper status` - Show backup status for Snapper configurations
  - `snapper generate-config` - Generate TOML configuration for Snapper volumes
- **Native Snapper directory layout** - Backups use `.snapshots/{num}/snapshot` + `info.xml` structure
- **Metadata preservation** - Snapper's `info.xml` is preserved in backups for proper restoration
- **Incremental transfers** - Both backup and restore operations use `btrfs send -p` for efficient delta transfers
- **Snapshot type filtering** - Back up specific types: `single` (timeline), `pre`, `post`
- **Minimum age filtering** - Skip snapshots younger than a specified age with `--min-age`
- **Rich progress bars** - Visual transfer progress for Snapper operations matching standard commands
- **Configuration file integration** - Snapper volumes can be defined in `config.toml` with `source = "snapper"`
- **Auto-detection in config wizard** - Interactive wizard now detects and offers Snapper configurations
- New `SnapperSourceConfig` schema for TOML configuration:
  - `config_name` - Snapper config name or "auto" to detect
  - `include_types` - Snapshot types to include
  - `exclude_cleanup` - Cleanup algorithms to skip
  - `min_age` - Minimum snapshot age before backup
- **Sudo-aware config paths** - Helper functions `get_user_home()`, `get_user_config_dir()`, and `get_default_config_path()` for correct XDG directory handling when running under sudo

#### Documentation
- New `examples/snapper.toml` example configuration
- Comprehensive Snapper integration section in README.md
- New man page `btrfs-backup-ng-snapper.1`

### Changed
- `btrfs-backup-ng run` now handles Snapper volumes when configured with `source = "snapper"`
- Config wizard shows Snapper volumes with `[snapper:name]` markers for easy identification
- `get_next_snapshot_number()` in scanner now scans filesystem directly for accuracy after restores
- **Default `min_age` changed from `"0"` to `"1h"`** for snapper sources to avoid backing up incomplete pre/post pairs
- Shell completions updated with all raw target compression methods (xz, bzip2, pbzip2, lzo)

### Fixed
- **Config wizard saves to sudo user's home** - When running under sudo, config files are now saved to the original user's XDG config directory instead of `/root`
- **Snapper min_age default** - Changed from `"0"` to `"1h"` to prevent backing up snapshots during active package operations

## [0.8.1] - 2026-01-06

### Added

#### System Diagnostics (Doctor Command)
- **`doctor` command** for comprehensive backup system health analysis
- Checks configuration validity, volume paths, target reachability, compression availability
- Detects snapshot health issues: orphaned snapshots, missing snapshots, broken parent chains
- Identifies stale locks from crashed processes with auto-fix capability
- Monitors system state: destination space, quota limits, systemd timer status, backup age
- **Auto-fix mode** (`--fix`) to resolve safe issues like stale locks and temp files
- **Interactive fix mode** (`--fix --interactive`) for confirmation before each fix
- JSON output (`--json`) for scripting and monitoring integration
- Category filtering (`--check config|snapshots|transfers|system`)
- Volume-specific checks (`--volume /path`)
- Exit codes: 0 (healthy), 1 (warnings), 2 (errors/critical)

#### Space-Aware Operations
- **Destination space checking** before backup transfers with `--check-space` flag on estimate command
- **btrfs quota (qgroup) awareness** - detects when quota limits are more restrictive than filesystem space
- **Safety margin** calculation (default 10%, minimum 100 MiB) to prevent transfers that would fill destinations
- **JSON output** includes complete space check details including quota information
- Pre-flight space verification in operations with clear insufficient space warnings

#### Subvolume Detection
- **`config detect`** command to scan for btrfs subvolumes system-wide
- Automatic categorization of subvolumes (recommended for backup, optional, excluded)
- Suggested snapshot prefixes based on mount paths
- JSON output mode for scripting (`--json`)
- Integration with interactive wizard (`--wizard`)

#### User-Friendly Filesystem Checks
- **Three-mode `--fs-checks` system**: `auto` (default), `strict`, `skip`
  - `auto`: Warns about issues but continues operation (user-friendly default)
  - `strict`: Errors out on filesystem check failures (original behavior)
  - `skip`: Bypasses all filesystem verification checks
- Backwards-compatible aliases: `--no-fs-checks` and `--skip-fs-checks` map to `skip` mode
- Applied consistently across all commands: estimate, verify, restore, run, transfer, legacy mode

#### Legacy Mode Enhancements
- Added `--no-check-space`, `--force`, `--safety-margin` options for space-aware operations
- Added `--fs-checks` option with auto/strict/skip modes
- Full parity with subcommand mode for new features

### Changed

- **Default `--fs-checks` mode changed from `strict` to `auto`** - operations now warn and continue instead of erroring on non-critical filesystem issues
- Reduced output noise: "Could not parse date from snapshot" messages moved from WARNING to DEBUG level
- Improved quota parsing using `btrfs qgroup show --raw` for accurate byte values

### Fixed

- Quota detection now correctly matches qgroups by path basename
- Fixed MagicMock issues in tests when fs_checks attribute wasn't explicitly set
- Improved path matching in qgroup output parsing for nested subvolumes

## [0.8.0] - 2026-01-04

### Added

#### Configuration System
- TOML configuration file support (`~/.config/btrfs-backup-ng/config.toml` or `/etc/btrfs-backup-ng/config.toml`)
- Interactive configuration wizard (`btrfs-backup-ng config init`)
- Configuration validation (`btrfs-backup-ng config validate`)
- Example config generation (`btrfs-backup-ng config generate`)
- btrbk configuration importer (`btrfs-backup-ng config import`)

#### Subcommand CLI
- Modern subcommand architecture replacing positional arguments
- `run` - Execute full backup workflow (snapshot + transfer + prune)
- `snapshot` - Create snapshots only
- `transfer` - Transfer existing snapshots to targets
- `prune` - Apply retention policies
- `list` - Show snapshots and backups across volumes
- `status` - Show job status and transaction history
- `restore` - Restore backups to local system (disaster recovery)
- `verify` - Multi-level backup integrity verification
- `estimate` - Estimate backup sizes before transfer
- `install` / `uninstall` - Systemd timer/service management
- Legacy CLI mode preserved for backward compatibility

#### Backup & Recovery
- Restore command with incremental chain resolution
- Interactive snapshot selection for restore
- Point-in-time restore (`--before` flag)
- Collision detection and handling for existing snapshots
- Restore lock management (`--status`, `--unlock`, `--cleanup`)
- Backup verification at multiple levels (metadata, stream, full restore test)
- Backup size estimation before transfers

#### Retention Policies
- Time-based retention (hourly, daily, weekly, monthly, yearly)
- Minimum retention period (`min` setting)
- Per-volume retention overrides
- Automatic preservation of snapshots needed for incremental chains

#### Transfer Features
- Stream compression (zstd, gzip, lz4, pigz, lzop)
- Bandwidth throttling (`--rate-limit`)
- Rich progress bars with speed, ETA, percentage
- Parallel volume and target execution

#### Automation
- Systemd timer/service generation
- Flexible scheduling (hourly, daily, or custom OnCalendar)
- Transaction logging (structured JSON)
- File logging support
- Email notifications on backup success/failure
- Webhook notifications

#### SSH Improvements
- Password authentication fallback with Paramiko
- Improved passwordless sudo detection
- Better diagnostics for SSH connection issues

#### Documentation & Quality
- Comprehensive man pages for all commands
- Shell completion scripts (bash, zsh, fish)
- CI/CD with GitHub Actions (test, lint, build)
- Automated PyPI publishing with trusted publisher
- Tier 2 integration tests for real btrfs operations

### Changed
- Minimum Python version is now 3.11
- Replaced embedded bash scripts with pure Python implementations
- Improved snapshot retention defaults for reliable incremental transfers

### Fixed
- Write permissions diagnostics false negatives
- Endpoint snapshot_folder default alignment with config schema
- Snapshot directory path handling and remount logic
- SSH URL format in btrbk import path conversion

## [0.6.8] - 2024-xx-xx

Previous release. See git history for details.

[0.8.2]: https://github.com/berrym/btrfs-backup-ng/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/berrym/btrfs-backup-ng/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/berrym/btrfs-backup-ng/compare/v0.6.8...v0.8.0
[0.6.8]: https://github.com/berrym/btrfs-backup-ng/releases/tag/v0.6.8
