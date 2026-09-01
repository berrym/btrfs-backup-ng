# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

- **`require_mount` values that are neither a boolean nor an absolute path are
  now rejected when the config loads.** Previously `require_mount` was passed
  through unvalidated, so `1`, `0`, `"true"` and `""` all loaded — `1` and
  `"true"` enabling the check, `0` and `""` silently disabling it. They now
  raise a configuration error, which stops the whole file loading rather than
  applying a setting nobody can predict. If you wrote the value quoted or as a
  number, change it to a bare `true`/`false` or to the mount point path.

### Fixed

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
