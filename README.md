# btrfs-backup-ng

A modern, feature-rich tool for automated BTRFS snapshot backup management with TOML configuration, time-based retention policies, and robust SSH transfer support.

## Heritage

This project is a continuation of the **btrfs-backup** lineage:

- **2014**: Originally created by [Chris Lawrence](mailto:lawrencc@debian.org)
- **2017**: Maintained and enhanced by [Robert Schindler](mailto:r.schindler@efficiosoft.com)
- **2024-present**: Continued development by [Michael Berry](mailto:trismegustis@gmail.com) as **btrfs-backup-ng**

See the [LICENSE](LICENSE) file for full copyright attribution.

## Features

- **TOML Configuration**: Clean, validated configuration files (no custom syntax)
- **Subcommand CLI**: Modern interface with `run`, `snapshot`, `transfer`, `prune`, `list`, `status`
- **Time-based Retention**: Intuitive policies (hourly, daily, weekly, monthly, yearly)
- **Parallel Execution**: Concurrent volume and target transfers
- **Stream Compression**: zstd, gzip, lz4, pigz, lzop support
- **Bandwidth Throttling**: Rate limiting for remote transfers
- **Systemd Integration**: Built-in timer/service installation
- **btrbk Migration**: Import existing btrbk configurations
- **Robust SSH**: Password fallback, sudo support, Paramiko integration
- **Legacy Compatibility**: Original CLI still works

## Installation

### From Source

```bash
git clone https://github.com/berrym/btrfs-backup-ng.git
cd btrfs-backup-ng
pip install -e .
```

### Requirements

- Python 3.11+
- BTRFS utilities (`btrfs-progs`)
- SSH client (for remote backups)

### Optional Dependencies

```bash
# Compression support
sudo dnf install zstd lz4 pigz    # Fedora/RHEL
sudo apt install zstd lz4 pigz    # Debian/Ubuntu

# Bandwidth throttling
sudo dnf install pv               # Fedora/RHEL
sudo apt install pv               # Debian/Ubuntu
```

## Quick Start

### 1. Create a Configuration File

```bash
btrfs-backup-ng config init > ~/.config/btrfs-backup-ng/config.toml
```

### 2. Edit the Configuration

```toml
[global]
snapshot_dir = ".snapshots"
incremental = true

[global.retention]
min = "1d"
hourly = 24
daily = 7
weekly = 4
monthly = 12

[[volumes]]
path = "/home"
snapshot_prefix = "home-"

[[volumes.targets]]
path = "/mnt/backup/home"

[[volumes.targets]]
path = "ssh://backup@server:/backups/home"
ssh_sudo = true
compress = "zstd"
rate_limit = "50M"
```

### 3. Run Backups

```bash
# Full backup (snapshot + transfer + prune)
btrfs-backup-ng run

# Or individual operations
btrfs-backup-ng snapshot
btrfs-backup-ng transfer
btrfs-backup-ng prune
```

### 4. Set Up Automated Backups

```bash
# Install systemd timer (hourly backups)
sudo btrfs-backup-ng install --timer=hourly

# Or custom schedule (every 15 minutes)
sudo btrfs-backup-ng install --oncalendar='*:0/15'

# User-level timer (no sudo needed)
btrfs-backup-ng install --user --timer=daily
```

## Commands

| Command | Description |
|---------|-------------|
| `run` | Execute full backup (snapshot + transfer + prune) |
| `snapshot` | Create snapshots only |
| `transfer` | Transfer existing snapshots to targets |
| `prune` | Apply retention policies |
| `list` | Show snapshots and backups |
| `status` | Show job status and statistics |
| `config validate` | Validate configuration file |
| `config init` | Generate example configuration |
| `config import` | Import btrbk configuration |
| `install` | Install systemd timer/service |
| `uninstall` | Remove systemd timer/service |

### Common Options

```bash
# Dry run (show what would be done)
btrfs-backup-ng run --dry-run
btrfs-backup-ng prune --dry-run

# Override compression and rate limit
btrfs-backup-ng transfer --compress=zstd --rate-limit=10M

# Parallel execution
btrfs-backup-ng run --parallel-volumes=2 --parallel-targets=3

# Specify config file
btrfs-backup-ng -c /path/to/config.toml run

# Verbose output
btrfs-backup-ng -v run
```

## Configuration Reference

### Global Settings

```toml
[global]
snapshot_dir = ".snapshots"           # Relative to volume or absolute
timestamp_format = "%Y%m%d-%H%M%S"    # Snapshot timestamp format
incremental = true                     # Use incremental transfers
log_file = "/var/log/btrfs-backup-ng.log"  # Optional log file
parallel_volumes = 2                   # Concurrent volume backups
parallel_targets = 3                   # Concurrent target transfers
quiet = false                          # Suppress non-essential output
verbose = false                        # Enable verbose output
```

### Retention Policy

```toml
[global.retention]
min = "1d"       # Keep all snapshots for at least 1 day
hourly = 24      # Keep 24 hourly snapshots
daily = 7        # Keep 7 daily snapshots
weekly = 4       # Keep 4 weekly snapshots
monthly = 12     # Keep 12 monthly snapshots
yearly = 0       # Don't keep yearly (0 = disabled)
```

Duration format: `30m` (minutes), `2h` (hours), `1d` (days), `1w` (weeks)

### Volume Configuration

```toml
[[volumes]]
path = "/home"                    # Path to BTRFS subvolume
snapshot_prefix = "home-"         # Prefix for snapshot names
snapshot_dir = ".snapshots"       # Override global snapshot_dir
enabled = true                    # Enable/disable this volume

# Volume-specific retention (overrides global)
[volumes.retention]
daily = 14
weekly = 8
```

### Target Configuration

```toml
[[volumes.targets]]
path = "/mnt/backup/home"              # Local path

[[volumes.targets]]
path = "ssh://user@host:/backups"      # SSH remote path
ssh_sudo = true                        # Use sudo on remote
ssh_port = 22                          # SSH port
ssh_key = "~/.ssh/backup_key"          # SSH private key
ssh_password_auth = true               # Allow password fallback
compress = "zstd"                      # Compression (none|gzip|zstd|lz4|pigz|lzop)
rate_limit = "10M"                     # Bandwidth limit (K|M|G suffix)
```

## Migrating from btrbk

Import your existing btrbk configuration:

```bash
btrfs-backup-ng config import /etc/btrbk/btrbk.conf -o config.toml
```

The importer will:
- Convert btrbk's custom syntax to TOML
- Translate retention policies
- Warn about common btrbk pitfalls
- Suggest improvements

### Key Differences from btrbk

| Feature | btrbk | btrfs-backup-ng |
|---------|-------|-----------------|
| Config format | Custom syntax | TOML (standard) |
| Config validation | Runtime errors | Pre-flight validation |
| Indentation | Ignored (confusing) | TOML is explicit |
| Language | Perl | Python |
| CLI output | Plain text | Rich formatting |
| SSH handling | External ssh | Native Paramiko option |

## SSH Configuration

### Passwordless Sudo (Recommended)

On remote hosts, add to `/etc/sudoers` via `visudo`:

```sudoers
# Full access to btrfs commands
backup_user ALL=(ALL) NOPASSWD: /usr/bin/btrfs

# Or restricted access
backup_user ALL=(ALL) NOPASSWD: /usr/bin/btrfs subvolume *, /usr/bin/btrfs send *, /usr/bin/btrfs receive *
```

### Password Authentication

If passwordless sudo isn't available:

```bash
# Environment variable
export BTRFS_BACKUP_SUDO_PASSWORD="password"

# Or enable password prompts
export BTRFS_BACKUP_SSH_PASSWORD=1
```

### SSH Key Setup

```bash
# Generate dedicated backup key
ssh-keygen -t ed25519 -f ~/.ssh/backup_key -C "btrfs-backup"

# Copy to remote host
ssh-copy-id -i ~/.ssh/backup_key backup_user@remote

# Use in config
[[volumes.targets]]
path = "ssh://backup_user@remote:/backups"
ssh_key = "~/.ssh/backup_key"
```

## Legacy CLI Mode

The original command-line interface still works:

```bash
# Basic backup
btrfs-backup-ng /source/subvolume /destination/path

# Remote backup
btrfs-backup-ng /home ssh://backup@server:/backups/home

# With options
btrfs-backup-ng --ssh-sudo --num-snapshots 10 /source ssh://user@host:/dest
```

Legacy mode is auto-detected when the first argument is a path.

## Systemd Integration

### Install Timer

```bash
# System-wide (requires root)
sudo btrfs-backup-ng install --timer=hourly
sudo btrfs-backup-ng install --timer=daily
sudo btrfs-backup-ng install --timer=weekly

# Custom schedule
sudo btrfs-backup-ng install --oncalendar='*:0/15'     # Every 15 minutes
sudo btrfs-backup-ng install --oncalendar='*:0/5'      # Every 5 minutes
sudo btrfs-backup-ng install --oncalendar='02:00'      # Daily at 2am

# User-level (no root needed)
btrfs-backup-ng install --user --timer=hourly
```

### Enable Timer

```bash
# System-wide
sudo systemctl daemon-reload
sudo systemctl enable --now btrfs-backup-ng.timer

# User-level
systemctl --user daemon-reload
systemctl --user enable --now btrfs-backup-ng.timer
```

### Check Status

```bash
systemctl status btrfs-backup-ng.timer
systemctl list-timers btrfs-backup-ng.timer
journalctl -u btrfs-backup-ng.service
```

### Uninstall

```bash
sudo btrfs-backup-ng uninstall
# or
btrfs-backup-ng uninstall  # for user-level
```

## Troubleshooting

### Debug Logging

```bash
btrfs-backup-ng -v run           # Verbose
btrfs-backup-ng -vv run          # Debug level
```

### Common Issues

**Permission denied on remote:**
```bash
# Check sudo configuration
ssh user@remote 'sudo -n btrfs --version'

# Enable ssh_sudo in config
[[volumes.targets]]
path = "ssh://user@remote:/backup"
ssh_sudo = true
```

**Snapshot directory doesn't exist:**
```bash
# btrfs-backup-ng creates it automatically, but ensure parent exists
mkdir -p /path/to/.snapshots
```

**Transfer fails with compression:**
```bash
# Verify compression tool is installed on both ends
which zstd
ssh user@remote 'which zstd'
```

**Rate limiting not working:**
```bash
# Install pv (pipe viewer)
sudo dnf install pv
```

### Validate Configuration

```bash
btrfs-backup-ng config validate
```

## Development

### Running from Source

```bash
git clone https://github.com/berrym/btrfs-backup-ng.git
cd btrfs-backup-ng
pip install -e .
python -m btrfs_backup_ng --help
```

### Project Structure

```
src/btrfs_backup_ng/
    __main__.py          # Entry point
    config/              # TOML configuration system
        schema.py        # Config dataclasses
        loader.py        # TOML loading and validation
    cli/                 # Subcommand modules
        dispatcher.py    # Command routing
        run.py, snapshot.py, transfer.py, prune.py, ...
    core/                # Core operations
        operations.py    # send_snapshot, sync_snapshots
        transfer.py      # Compression, throttling
        planning.py      # Transfer planning
    endpoint/            # Endpoint implementations
        local.py         # Local filesystem
        ssh.py           # SSH remote
        shell.py         # Shell commands
    retention.py         # Time-based retention logic
    btrbk_import.py      # btrbk config importer
```

## Documentation

- [CLI Reference](docs/CLI-REFERENCE.md) - Complete command and option reference
- [Migrating from btrbk](docs/MIGRATING-FROM-BTRBK.md) - Guide for btrbk users
- [Changelog](CHANGELOG.md) - Version history and release notes

### Example Configurations

Ready-to-use configuration examples in `examples/`:

| File | Description |
|------|-------------|
| [`minimal.toml`](examples/minimal.toml) | Simple local backup |
| [`remote-backup.toml`](examples/remote-backup.toml) | Multi-volume SSH backup |
| [`server.toml`](examples/server.toml) | Server with frequent snapshots |
| [`config.toml`](examples/config.toml) | Full reference with all options |

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please:
- Follow PEP 8 style guidelines
- Add tests for new features
- Update documentation as needed
- Maintain backward compatibility

## See Also

- [btrbk](https://github.com/digint/btrbk) - The Perl-based tool that inspired many features
- [btrfs-progs](https://github.com/kdave/btrfs-progs) - BTRFS utilities
- [Snapper](https://github.com/openSUSE/snapper) - Snapshot management tool
