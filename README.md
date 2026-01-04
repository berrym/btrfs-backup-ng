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
- **Rich Progress Bars**: Real-time transfer progress with speed, ETA, and percentage
- **Parallel Execution**: Concurrent volume and target transfers
- **Stream Compression**: zstd, gzip, lz4, pigz, lzop support
- **Bandwidth Throttling**: Rate limiting for remote transfers
- **Transaction Logging**: Structured JSON logs for auditing and automation
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

# Progress bar control (auto-detected by default)
btrfs-backup-ng run --progress      # Force progress bars
btrfs-backup-ng run --no-progress   # Disable progress bars

# Parallel execution
btrfs-backup-ng run --parallel-volumes=2 --parallel-targets=3

# Specify config file
btrfs-backup-ng -c /path/to/config.toml run

# Verbose output
btrfs-backup-ng -v run

# View transaction history
btrfs-backup-ng status --transactions
```

## Configuration Reference

### Global Settings

```toml
[global]
snapshot_dir = ".snapshots"           # Relative to volume or absolute
timestamp_format = "%Y%m%d-%H%M%S"    # Snapshot timestamp format
incremental = true                     # Use incremental transfers
log_file = "/var/log/btrfs-backup-ng.log"           # Optional rotating log file
transaction_log = "/var/log/btrfs-backup-ng.jsonl"  # Optional JSON transaction log
parallel_volumes = 2                   # Concurrent volume backups
parallel_targets = 3                   # Concurrent target transfers
quiet = false                          # Suppress non-essential output
verbose = false                        # Enable verbose output
```

### Logging

Two logging systems are available:

**File Logging** (`log_file`): Human-readable rotating log file (10MB max, 5 backups)
```
2026-01-04 13:10:51 [INFO] btrfs-backup-ng: Creating snapshot...
2026-01-04 13:10:57 [INFO] btrfs-backup-ng: Transfer completed successfully
```

**Transaction Logging** (`transaction_log`): Structured JSONL for auditing and automation
```json
{"timestamp": "2026-01-04T18:10:57+00:00", "action": "transfer", "status": "completed", "snapshot": "home-20260104", "duration_seconds": 5.78, "size_bytes": 2175549440}
```

View transaction history via CLI:
```bash
btrfs-backup-ng status --transactions       # Show recent transactions
btrfs-backup-ng status -t -n 20             # Show last 20 transactions
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

## Configuration File Locations

btrfs-backup-ng searches for configuration files in the following locations, in priority order:

| Priority | Location | Description |
|----------|----------|-------------|
| 1 (highest) | `-c /path/to/config.toml` | Explicit path via command line |
| 2 | `~/.config/btrfs-backup-ng/config.toml` | User-specific configuration |
| 3 | `/etc/btrfs-backup-ng/config.toml` | System-wide configuration |

The first configuration file found is used. If no configuration file exists in any location and none is specified with `-c`, commands that require configuration will display an error with instructions.

### User Configuration

For personal workstations or when running backups as a regular user:

```bash
# Create user config directory
mkdir -p ~/.config/btrfs-backup-ng

# Generate example configuration
btrfs-backup-ng config init > ~/.config/btrfs-backup-ng/config.toml

# Edit to match your setup
$EDITOR ~/.config/btrfs-backup-ng/config.toml

# Run backups (config is found automatically)
sudo btrfs-backup-ng run
```

### System-Wide Configuration

For servers, shared systems, or when using systemd timers:

```bash
# Create system config directory
sudo mkdir -p /etc/btrfs-backup-ng

# Generate example configuration
btrfs-backup-ng config init | sudo tee /etc/btrfs-backup-ng/config.toml

# Edit to match your setup
sudo $EDITOR /etc/btrfs-backup-ng/config.toml

# Set appropriate permissions (readable by root only for security)
sudo chmod 600 /etc/btrfs-backup-ng/config.toml

# Run backups (config is found automatically)
sudo btrfs-backup-ng run
```

### Configuration Precedence

When both user and system configurations exist:
- **User config takes precedence** over system config
- Use `-c` to explicitly select a different configuration
- The `config validate` command shows which file is being used

```bash
# See which config file is active
btrfs-backup-ng config validate
# Output: Validating: /home/user/.config/btrfs-backup-ng/config.toml

# Force use of system config
sudo btrfs-backup-ng -c /etc/btrfs-backup-ng/config.toml run
```

### Recommended Setup for Automated Backups

For production servers with systemd timers:

```bash
# 1. Create system-wide configuration
sudo mkdir -p /etc/btrfs-backup-ng
btrfs-backup-ng config init | sudo tee /etc/btrfs-backup-ng/config.toml
sudo chmod 600 /etc/btrfs-backup-ng/config.toml

# 2. Edit configuration
sudo $EDITOR /etc/btrfs-backup-ng/config.toml

# 3. Validate configuration
sudo btrfs-backup-ng config validate

# 4. Test manually first
sudo btrfs-backup-ng run --dry-run
sudo btrfs-backup-ng run

# 5. Install and enable systemd timer
sudo btrfs-backup-ng install --timer=hourly
sudo systemctl daemon-reload
sudo systemctl enable --now btrfs-backup-ng.timer

# 6. Verify timer is active
systemctl list-timers btrfs-backup-ng.timer
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

btrfs-backup-ng can install systemd timer and service units for automated backups. There are two installation modes:

| Mode | Install Location | Runs As | Use Case |
|------|------------------|---------|----------|
| System-wide | `/etc/systemd/system/` | root | Servers, production systems |
| User-level | `~/.config/systemd/user/` | current user | Personal workstations |

### System-Wide Installation (Recommended for Servers)

System-wide installation runs backups as root, which is required for backing up system volumes.

**Prerequisites:**
1. Configuration file at `/etc/btrfs-backup-ng/config.toml` (or user config)
2. Root privileges (sudo)

**Installation:**

```bash
# Install with a preset timer
sudo btrfs-backup-ng install --timer=hourly   # Every hour at :00
sudo btrfs-backup-ng install --timer=daily    # Daily at 02:00
sudo btrfs-backup-ng install --timer=weekly   # Weekly on Sunday at 02:00

# Or use a custom OnCalendar specification
sudo btrfs-backup-ng install --oncalendar='*:0/15'           # Every 15 minutes
sudo btrfs-backup-ng install --oncalendar='*:0/5'            # Every 5 minutes
sudo btrfs-backup-ng install --oncalendar='*-*-* 02:00:00'   # Daily at 2am
sudo btrfs-backup-ng install --oncalendar='Mon *-*-* 03:00'  # Monday at 3am
```

**Enable and start the timer:**

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now btrfs-backup-ng.timer
```

**Generated files:**
- `/etc/systemd/system/btrfs-backup-ng.service` - The backup service unit
- `/etc/systemd/system/btrfs-backup-ng.timer` - The timer unit

### User-Level Installation

User-level installation runs backups as the current user. Useful for personal workstations where sudo is available but system-wide installation isn't desired.

**Note:** User-level services require the user to be logged in (or lingering enabled).

```bash
# Install user timer
btrfs-backup-ng install --user --timer=hourly

# Enable and start
systemctl --user daemon-reload
systemctl --user enable --now btrfs-backup-ng.timer

# Optional: Enable lingering so timers run even when logged out
loginctl enable-linger $USER
```

**Generated files:**
- `~/.config/systemd/user/btrfs-backup-ng.service`
- `~/.config/systemd/user/btrfs-backup-ng.timer`

### Timer Presets

| Preset | OnCalendar Value | Description |
|--------|------------------|-------------|
| `hourly` | `*:00` | Every hour at minute 00 |
| `daily` | `*-*-* 02:00:00` | Every day at 2:00 AM |
| `weekly` | `Sun *-*-* 02:00:00` | Every Sunday at 2:00 AM |

### Custom OnCalendar Examples

The `--oncalendar` option accepts any valid systemd calendar specification:

| Specification | Description |
|---------------|-------------|
| `*:0/5` | Every 5 minutes |
| `*:0/15` | Every 15 minutes |
| `*:0/30` | Every 30 minutes |
| `hourly` | Every hour |
| `*-*-* 02:00:00` | Daily at 2:00 AM |
| `*-*-* 00/6:00:00` | Every 6 hours |
| `Mon,Thu *-*-* 03:00:00` | Monday and Thursday at 3:00 AM |
| `*-*-01 04:00:00` | First day of each month at 4:00 AM |

Test your specification with: `systemd-analyze calendar '*:0/15'`

### Monitoring and Logs

**Check timer status:**

```bash
# System-wide
systemctl status btrfs-backup-ng.timer
systemctl list-timers btrfs-backup-ng.timer

# User-level
systemctl --user status btrfs-backup-ng.timer
systemctl --user list-timers btrfs-backup-ng.timer
```

**View logs:**

```bash
# System-wide - recent logs
journalctl -u btrfs-backup-ng.service -n 50

# System-wide - follow logs in real-time
journalctl -u btrfs-backup-ng.service -f

# System-wide - logs since last boot
journalctl -u btrfs-backup-ng.service -b

# User-level
journalctl --user -u btrfs-backup-ng.service -n 50
```

**Manually trigger a backup:**

```bash
# System-wide
sudo systemctl start btrfs-backup-ng.service

# User-level
systemctl --user start btrfs-backup-ng.service
```

### Uninstall

The uninstall command removes the timer and service files:

```bash
# System-wide
sudo btrfs-backup-ng uninstall

# User-level
btrfs-backup-ng uninstall
```

**Note:** Uninstall checks both system and user locations and removes files from wherever they're found.

### Complete System-Wide Setup Example

```bash
# 1. Create and configure
sudo mkdir -p /etc/btrfs-backup-ng
btrfs-backup-ng config init | sudo tee /etc/btrfs-backup-ng/config.toml
sudo chmod 600 /etc/btrfs-backup-ng/config.toml
sudo $EDITOR /etc/btrfs-backup-ng/config.toml

# 2. Validate and test
sudo btrfs-backup-ng config validate
sudo btrfs-backup-ng run --dry-run
sudo btrfs-backup-ng run

# 3. Install timer (hourly backups)
sudo btrfs-backup-ng install --timer=hourly

# 4. Enable timer
sudo systemctl daemon-reload
sudo systemctl enable --now btrfs-backup-ng.timer

# 5. Verify
systemctl list-timers btrfs-backup-ng.timer
sudo btrfs-backup-ng status
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
