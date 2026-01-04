# Changelog

All notable changes to btrfs-backup-ng will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2025-01-04

### Added

#### TOML Configuration System
- New TOML-based configuration file support (`~/.config/btrfs-backup-ng/config.toml`)
- Configuration validation with helpful error messages
- Global settings with volume-level overrides
- Example configurations in `examples/` directory

#### Subcommand CLI Architecture
- `btrfs-backup-ng run` - Execute full backup (snapshot + transfer + prune)
- `btrfs-backup-ng snapshot` - Create snapshots only
- `btrfs-backup-ng transfer` - Transfer existing snapshots to targets
- `btrfs-backup-ng prune` - Apply retention policies
- `btrfs-backup-ng list` - Show snapshots and backups (with `--json` option)
- `btrfs-backup-ng status` - Show job status and statistics
- `btrfs-backup-ng config validate` - Validate configuration file
- `btrfs-backup-ng config init` - Generate example configuration
- `btrfs-backup-ng config import` - Import btrbk configuration
- `btrfs-backup-ng install` - Install systemd timer/service
- `btrfs-backup-ng uninstall` - Remove systemd timer/service

#### Time-Based Retention Policies
- Intuitive bucket-based retention (hourly, daily, weekly, monthly, yearly)
- Minimum retention period (`min`) before any pruning
- Per-volume retention overrides
- Automatic preservation of latest snapshot

#### Parallel Execution
- Concurrent volume backups (`--parallel-volumes`)
- Concurrent target transfers per volume (`--parallel-targets`)
- Thread-safe endpoint handling

#### Stream Compression
- Support for zstd, gzip, lz4, pigz, lzop
- Per-target compression configuration
- CLI override with `--compress`

#### Bandwidth Throttling
- Rate limiting for transfers (`--rate-limit`)
- Supports K/M/G suffixes (e.g., `10M`, `1G`)
- Per-target rate limit configuration

#### Systemd Integration
- Built-in timer/service installation
- Preset intervals (hourly, daily, weekly)
- Custom OnCalendar specifications
- User-level and system-wide installation

#### btrbk Migration
- Import btrbk.conf and convert to TOML
- Warnings about common btrbk pitfalls
- Retention policy translation

#### Documentation
- Comprehensive README with quick start guide
- Full CLI reference (`docs/CLI-REFERENCE.md`)
- btrbk migration guide (`docs/MIGRATING-FROM-BTRBK.md`)
- Example configurations for various use cases

#### Testing
- 459 automated tests (unit, Tier 1 mocked, Tier 2 real btrfs)
- 95%+ code coverage on testable modules
- Loopback btrfs filesystem tests for real operations

### Changed
- Minimum Python version is now 3.11+ (for `tomllib` and better error messages)
- Legacy CLI mode auto-detected and preserved for backwards compatibility
- Improved logging with configurable verbosity levels

### Fixed
- SSH password authentication fallback reliability
- Snapshot retention defaults for reliable incremental transfers
- Write permissions diagnostics to prevent false negatives

## [0.6.0] - 2025-05-25

### Added
- Robust SSH authentication with SUDO_ASKPASS and sudo -S fallback
- SSH password authentication fallback for remote backups
- Simple progress monitoring as default
- Enhanced SSH security features

### Changed
- Replaced embedded bash scripts with pure Python implementations
- Improved error handling and recovery

## [0.5.0] - 2024-12-15

### Added
- Initial Paramiko SSH integration
- Rich CLI output with progress bars
- JSON-based lock system for state tracking
- Multiple destination support

### Changed
- Modernized codebase for Python 3.11+
- Improved incremental transfer optimization

## [0.4.0] - 2024-01-01

### Added
- Project forked and continued as btrfs-backup-ng
- New maintainer: Michael Berry

### Changed
- Updated dependencies and compatibility

---

## Historical Releases (btrfs-backup)

The original btrfs-backup project was created by Chris Lawrence in 2014
and later maintained by Robert Schindler. btrfs-backup-ng continues
this lineage with modern features and active development.
