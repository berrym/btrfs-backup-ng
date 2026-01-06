# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.8.1]: https://github.com/berrym/btrfs-backup-ng/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/berrym/btrfs-backup-ng/compare/v0.6.8...v0.8.0
[0.6.8]: https://github.com/berrym/btrfs-backup-ng/releases/tag/v0.6.8
