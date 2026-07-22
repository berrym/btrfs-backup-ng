# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
