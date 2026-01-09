# btrfs-backup-ng CLI Reference

Complete reference for all btrfs-backup-ng commands and options.

## Global Options

These options can be used with any command:

```
-h, --help          Show help message and exit
-V, --version       Show version and exit
-c, --config FILE   Path to configuration file
-v, --verbose       Increase output verbosity (can be repeated: -vv)
-q, --quiet         Suppress non-essential output
```

## Commands

### run

Execute all configured backup jobs (snapshot + transfer + prune).

```bash
btrfs-backup-ng run [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--dry-run` | Show what would be done without making changes |
| `--parallel-volumes N` | Max concurrent volume backups (overrides config) |
| `--parallel-targets N` | Max concurrent target transfers per volume |
| `--compress METHOD` | Compression method: none, gzip, zstd, lz4, pigz, lzop |
| `--rate-limit RATE` | Bandwidth limit (e.g., '10M', '1G', '500K') |
| `--progress` | Force progress bar display |
| `--no-progress` | Disable progress bar display |

**Examples:**
```bash
# Full backup run
btrfs-backup-ng run

# Dry run to see what would happen
btrfs-backup-ng run --dry-run

# With compression and rate limiting
btrfs-backup-ng run --compress=zstd --rate-limit=50M

# Parallel execution
btrfs-backup-ng run --parallel-volumes=3 --parallel-targets=2
```

---

### snapshot

Create snapshots only, without transferring to targets.

```bash
btrfs-backup-ng snapshot [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--dry-run` | Show what would be done without making changes |
| `--volume PATH` | Only snapshot specific volume(s), can be repeated |

**Examples:**
```bash
# Snapshot all configured volumes
btrfs-backup-ng snapshot

# Snapshot specific volume only
btrfs-backup-ng snapshot --volume=/home

# Dry run
btrfs-backup-ng snapshot --dry-run
```

---

### transfer

Transfer existing snapshots to targets without creating new snapshots.

```bash
btrfs-backup-ng transfer [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--dry-run` | Show what would be done without making changes |
| `--volume PATH` | Only transfer specific volume(s) |
| `--compress METHOD` | Compression method (overrides config) |
| `--rate-limit RATE` | Bandwidth limit (overrides config) |

**Examples:**
```bash
# Transfer all pending snapshots
btrfs-backup-ng transfer

# Transfer with compression
btrfs-backup-ng transfer --compress=zstd

# Transfer specific volume with rate limiting
btrfs-backup-ng transfer --volume=/home --rate-limit=10M
```

---

### prune

Apply retention policies to clean up old snapshots and backups.

```bash
btrfs-backup-ng prune [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--dry-run` | Show what would be deleted without making changes |

**Examples:**
```bash
# Apply retention policies
btrfs-backup-ng prune

# See what would be deleted
btrfs-backup-ng prune --dry-run
```

---

### list

Show snapshots and backups across all configured volumes and targets.

```bash
btrfs-backup-ng list [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--volume PATH` | Only list specific volume(s) |
| `--json` | Output in JSON format |

**Examples:**
```bash
# List all snapshots
btrfs-backup-ng list

# List specific volume
btrfs-backup-ng list --volume=/home

# JSON output for scripting
btrfs-backup-ng list --json
```

---

### status

Show job status, last run times, statistics, and transaction history.

```bash
btrfs-backup-ng status [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `-t, --transactions` | Show recent transaction history |
| `-n, --limit N` | Number of transactions to show (default: 10) |

**Examples:**
```bash
# Basic status
btrfs-backup-ng status

# Status with transaction history
btrfs-backup-ng status --transactions

# Show last 20 transactions
btrfs-backup-ng status -t -n 20
```

**Output includes:**
- Volume and target health status
- Snapshot counts
- Transaction statistics (if transaction_log configured)
- Recent transaction history (with --transactions flag)

---

### config

Configuration management subcommands.

#### config validate

Validate the configuration file.

```bash
btrfs-backup-ng config validate
```

**Examples:**
```bash
# Validate default config location
btrfs-backup-ng config validate

# Validate specific config file
btrfs-backup-ng -c /path/to/config.toml config validate
```

#### config init

Generate an example configuration file.

```bash
btrfs-backup-ng config init [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `-o, --output FILE` | Output file (default: stdout) |

**Examples:**
```bash
# Print example config to stdout
btrfs-backup-ng config init

# Save to file
btrfs-backup-ng config init -o ~/.config/btrfs-backup-ng/config.toml
```

#### config import

Import a btrbk configuration file and convert to TOML.

```bash
btrfs-backup-ng config import FILE [OPTIONS]
```

**Arguments:**
| Argument | Description |
|----------|-------------|
| `FILE` | Path to btrbk.conf file |

**Options:**
| Option | Description |
|--------|-------------|
| `-o, --output FILE` | Output file (default: stdout) |

**Examples:**
```bash
# Convert and print to stdout
btrfs-backup-ng config import /etc/btrbk/btrbk.conf

# Convert and save to file
btrfs-backup-ng config import /etc/btrbk/btrbk.conf -o config.toml
```

#### config detect

Scan the system for btrfs subvolumes and suggest backup configurations.

```bash
btrfs-backup-ng config detect [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--json` | Output in JSON format for scripting |
| `-w, --wizard` | Launch interactive wizard with detected volumes |

**Examples:**
```bash
# Scan for subvolumes (requires root for full access)
sudo btrfs-backup-ng config detect

# Output in JSON format
sudo btrfs-backup-ng config detect --json

# Launch wizard with detected volumes
sudo btrfs-backup-ng config detect --wizard
```

The detect command categorizes subvolumes as:
- **Recommended**: User data like `/home` that should be backed up
- **Optional**: System data (`/opt`, `/var/log`) that may or may not need backup
- **Excluded**: Existing snapshots and system-internal subvolumes

---

### install

Install systemd timer and service for automated backups.

```bash
btrfs-backup-ng install [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--timer PRESET` | Use preset interval: hourly, daily, weekly |
| `--oncalendar SPEC` | Custom OnCalendar specification |
| `--user` | Install as user service (no root required) |

**Timer Presets:**
| Preset | OnCalendar Value | Description |
|--------|------------------|-------------|
| `hourly` | `*:00` | Every hour at minute 00 |
| `daily` | `*-*-* 02:00:00` | Every day at 2:00 AM |
| `weekly` | `Sun *-*-* 02:00:00` | Every Sunday at 2:00 AM |

**Installation Modes:**
| Mode | Location | Requires |
|------|----------|----------|
| System-wide (default) | `/etc/systemd/system/` | root (sudo) |
| User-level (`--user`) | `~/.config/systemd/user/` | None |

**Examples:**
```bash
# System-wide installation (requires root)
sudo btrfs-backup-ng install --timer=hourly
sudo btrfs-backup-ng install --timer=daily
sudo btrfs-backup-ng install --timer=weekly

# Custom schedules
sudo btrfs-backup-ng install --oncalendar='*:0/15'           # Every 15 minutes
sudo btrfs-backup-ng install --oncalendar='*:0/5'            # Every 5 minutes
sudo btrfs-backup-ng install --oncalendar='*-*-* 02:00:00'   # Daily at 2am
sudo btrfs-backup-ng install --oncalendar='Mon *-*-* 03:00'  # Monday at 3am

# User-level installation (no root required)
btrfs-backup-ng install --user --timer=hourly
```

**After installation, enable the timer:**
```bash
# System-wide
sudo systemctl daemon-reload
sudo systemctl enable --now btrfs-backup-ng.timer

# User-level
systemctl --user daemon-reload
systemctl --user enable --now btrfs-backup-ng.timer
```

**Verify timer is active:**
```bash
systemctl list-timers btrfs-backup-ng.timer          # System-wide
systemctl --user list-timers btrfs-backup-ng.timer   # User-level
```

---

### uninstall

Remove installed systemd timer and service files.

```bash
btrfs-backup-ng uninstall
```

**Examples:**
```bash
# Remove system-wide installation
sudo btrfs-backup-ng uninstall

# Remove user-level installation
btrfs-backup-ng uninstall
```

---

### estimate

Estimate backup transfer sizes and optionally check destination space.

```bash
btrfs-backup-ng estimate [OPTIONS] SOURCE DESTINATION
btrfs-backup-ng estimate --volume PATH [--target INDEX] [--check-space]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--volume PATH` | Estimate for volume defined in config |
| `--target INDEX` | Target index to estimate for (0-based) |
| `--prefix PREFIX` | Snapshot prefix filter |
| `--check-space` | Check if destination has sufficient space |
| `--safety-margin PERCENT` | Safety margin percentage (default: 10%) |
| `--ssh-sudo` | Use sudo on remote host |
| `--ssh-key FILE` | SSH private key file |
| `--fs-checks MODE` | Filesystem check mode: auto, strict, skip |
| `--no-fs-checks` | Skip filesystem checks (alias for --fs-checks=skip) |
| `--json` | Output in JSON format |

**Examples:**
```bash
# Basic estimate
btrfs-backup-ng estimate /mnt/snapshots /mnt/backup

# Estimate for configured volume
btrfs-backup-ng estimate --volume /home

# Estimate with destination space check
btrfs-backup-ng estimate --volume /home --check-space

# Space check with custom safety margin
btrfs-backup-ng estimate /mnt/snapshots /mnt/backup --check-space --safety-margin 20

# JSON output for scripting
btrfs-backup-ng estimate --volume /home --check-space --json
```

**Space Check Output:**

When `--check-space` is enabled, the output includes destination space information:
- Filesystem free space
- Quota limits (if btrfs quotas are enabled)
- Effective available space (the more restrictive of fs or quota)
- Required space with safety margin
- Status (OK or INSUFFICIENT)

---

### doctor

Diagnose backup system health and optionally fix common issues.

```bash
btrfs-backup-ng doctor [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--json` | Output in JSON format for scripting |
| `--check CATEGORY` | Check specific category: config, snapshots, transfers, system |
| `--fix` | Auto-fix safe issues (stale locks, temp files) |
| `--interactive` | Confirm each fix before applying (requires --fix) |
| `-q, --quiet` | Only show problems (suppress OK messages) |
| `--volume PATH` | Check specific volume only |

**Categories:**
| Category | What It Checks |
|----------|----------------|
| `config` | Config file exists and valid, volume paths exist, targets reachable, compression programs available |
| `snapshots` | Orphaned snapshots, missing snapshots, broken parent chains |
| `transfers` | Stale locks (dead processes), incomplete transfers, recent failures in transaction log |
| `system` | Destination space, quota limits, systemd timer status, backup age |

**Exit Codes:**
| Code | Meaning |
|------|---------|
| 0 | Healthy (all OK or INFO findings) |
| 1 | Warnings present |
| 2 | Errors or critical issues |

**Examples:**
```bash
# Full diagnostics
btrfs-backup-ng doctor

# JSON output for monitoring integration
btrfs-backup-ng doctor --json

# Check only configuration
btrfs-backup-ng doctor --check config

# Check only snapshot health
btrfs-backup-ng doctor --check snapshots

# Auto-fix safe issues
btrfs-backup-ng doctor --fix

# Interactive fix (confirm each)
btrfs-backup-ng doctor --fix --interactive

# Only show problems
btrfs-backup-ng doctor --quiet

# Check specific volume
btrfs-backup-ng doctor --volume /home
```

**Example Output:**
```
btrfs-backup-ng Doctor
=======================================================

Configuration
  [OK]    Config file valid
  [OK]    Volume /home exists and is btrfs subvolume
  [WARN]  Compression program 'pigz' not found

Snapshots
  [OK]    Volume /home: 24 snapshots, chain intact

Transfers
  [WARN]  Stale lock: home-20260110 (process 12345 not running)
          [FIXABLE] Run with --fix to remove

System
  [OK]    Destination: 524 GiB available (52%)
  [OK]    Systemd timer active, next: 2h 15m

=======================================================
Summary: 7 passed, 2 warnings, 0 errors
Fixable: 1 issue (run with --fix)
```

**JSON Output:**
```json
{
  "timestamp": "2026-01-06T10:30:00Z",
  "duration_seconds": 2.5,
  "summary": {"ok": 7, "warnings": 2, "errors": 0, "fixable": 1},
  "findings": [
    {
      "category": "transfers",
      "severity": "warn",
      "check": "stale_locks",
      "message": "Stale lock detected",
      "details": {"snapshot": "home-20260110", "pid": 12345},
      "fixable": true,
      "fix_description": "Remove stale lock file"
    }
  ]
}
```

---

### transfers (Experimental)

> **Note:** The chunked transfer feature is experimental. It provides resumable transfers for large snapshots but has not been extensively tested in production environments.

Manage chunked and resumable transfers. This command allows you to list, inspect, resume, pause, and clean up incomplete transfers.

```bash
btrfs-backup-ng transfers [SUBCOMMAND] [OPTIONS]
```

#### transfers list

List all incomplete transfer sessions.

```bash
btrfs-backup-ng transfers list [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--json` | Output in JSON format |
| `--all` | Include completed and failed transfers |

**Example Output:**
```
Incomplete Transfers
======================================================================
ID         Status       Snapshot                  Progress     Age
----------------------------------------------------------------------
abc123     paused       home-20260108             5/10 (50%)   2h ago
def456     failed       root-20260109             3/8 (37%)    1d ago
----------------------------------------------------------------------
Total: 2 incomplete transfer(s)
```

#### transfers show

Show detailed information about a specific transfer.

```bash
btrfs-backup-ng transfers show TRANSFER_ID [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--json` | Output in JSON format |

**Example:**
```bash
btrfs-backup-ng transfers show abc123
```

#### transfers resume

Resume a paused or failed transfer.

```bash
btrfs-backup-ng transfers resume TRANSFER_ID [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--dry-run` | Show what would be done without resuming |
| `--rate-limit RATE` | Bandwidth limit (e.g., '10M', '1G') |
| `--progress` | Show progress bars |
| `--no-progress` | Disable progress bars |

**Example:**
```bash
btrfs-backup-ng transfers resume abc123
```

#### transfers pause

Pause an active transfer.

```bash
btrfs-backup-ng transfers pause TRANSFER_ID
```

#### transfers cleanup

Clean up stale, completed, or failed transfers.

```bash
btrfs-backup-ng transfers cleanup [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--dry-run` | Show what would be cleaned without making changes |
| `--force` | Remove even active transfers |
| `--older-than DURATION` | Only cleanup transfers older than duration (e.g., '7d', '24h') |
| `TRANSFER_ID` | Clean up a specific transfer |

**Examples:**
```bash
# See what would be cleaned
btrfs-backup-ng transfers cleanup --dry-run

# Clean up stale transfers
btrfs-backup-ng transfers cleanup

# Clean up a specific transfer
btrfs-backup-ng transfers cleanup abc123

# Force cleanup of all transfers older than 7 days
btrfs-backup-ng transfers cleanup --older-than 7d --force
```

#### transfers operations

List backup operations and their status.

```bash
btrfs-backup-ng transfers operations [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--json` | Output in JSON format |
| `--all` | Include archived operations |

---

### snapper

Manage snapper-managed snapshots. This command provides integration with snapper for discovering, backing up, and restoring snapper snapshots.

```bash
btrfs-backup-ng snapper SUBCOMMAND [OPTIONS]
```

#### snapper detect

Detect snapper configurations on the system.

```bash
btrfs-backup-ng snapper detect [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--json` | Output in JSON format |

**Example Output:**
```
Found 2 snapper configuration(s):

  root:
    Subvolume:     /
    Snapshots dir: /.snapshots
    Status:        OK

  home:
    Subvolume:     /home
    Snapshots dir: /home/.snapshots
    Status:        OK
```

#### snapper list

List snapshots for snapper configurations.

```bash
btrfs-backup-ng snapper list [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--config NAME` | List snapshots for specific config only |
| `--type TYPE` | Filter by snapshot type (single, pre, post) |
| `--json` | Output in JSON format |

**Examples:**
```bash
# List all snapshots
btrfs-backup-ng snapper list

# List only 'root' config snapshots
btrfs-backup-ng snapper list --config root

# List only 'single' type snapshots
btrfs-backup-ng snapper list --type single
```

#### snapper backup

Backup snapper snapshots to a destination.

```bash
btrfs-backup-ng snapper backup CONFIG TARGET [OPTIONS]
```

**Arguments:**
| Argument | Description |
|----------|-------------|
| `CONFIG` | Snapper configuration name (e.g., 'root', 'home') |
| `TARGET` | Destination path (local or ssh://user@host:/path) |

**Options:**
| Option | Description |
|--------|-------------|
| `--dry-run` | Show what would be done without making changes |
| `--snapshot NUM` | Backup specific snapshot by number |
| `--type TYPE` | Filter by snapshot type (can be repeated) |
| `--min-age DURATION` | Only backup snapshots older than duration (e.g., '1h') |
| `--compress METHOD` | Compression method (none, zstd, gzip, lz4) |
| `--rate-limit RATE` | Bandwidth limit (e.g., '10M', '1G') |
| `--ssh-sudo` | Use sudo on remote host |
| `--ssh-key FILE` | SSH private key file |
| `--progress` | Show progress bars |
| `--no-progress` | Disable progress bars |

**Examples:**
```bash
# Backup all snapshots for 'root' config to local path
btrfs-backup-ng snapper backup root /mnt/backup/root

# Backup to remote server
btrfs-backup-ng snapper backup root ssh://backup@server:/backups/root

# Backup only single-type snapshots with compression
btrfs-backup-ng snapper backup root /mnt/backup --type single --compress zstd

# Backup snapshots older than 1 hour
btrfs-backup-ng snapper backup root /mnt/backup --min-age 1h

# Dry run to see what would be backed up
btrfs-backup-ng snapper backup root /mnt/backup --dry-run
```

**Backup Directory Layout:**

Backups use snapper's native directory layout:
```
/mnt/backup/root/.snapshots/
├── 559/
│   ├── info.xml
│   └── snapshot/     # btrfs subvolume
├── 560/
│   ├── info.xml
│   └── snapshot/
└── 561/
    ├── info.xml
    └── snapshot/
```

#### snapper status

Show backup sync status between source and destination.

```bash
btrfs-backup-ng snapper status [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--config NAME` | Check specific config only |
| `--target PATH` | Check backup status at target |
| `--json` | Output in JSON format |

**Examples:**
```bash
# Show local snapshot counts
btrfs-backup-ng snapper status

# Show backup status for specific target
btrfs-backup-ng snapper status --target /mnt/backup/root
```

#### snapper restore

Restore snapper snapshots from backup.

```bash
btrfs-backup-ng snapper restore SOURCE --config NAME [OPTIONS]
```

**Arguments:**
| Argument | Description |
|----------|-------------|
| `SOURCE` | Backup source path (local or ssh://user@host:/path) |

**Options:**
| Option | Description |
|--------|-------------|
| `--config NAME` | Local snapper config to restore into (required) |
| `--list` | List available backups instead of restoring |
| `--snapshot NUM` | Restore specific snapshot by number (can be repeated) |
| `--all` | Restore all snapshots |
| `--dry-run` | Show what would be done without making changes |
| `--compress METHOD` | Compression method |
| `--rate-limit RATE` | Bandwidth limit |
| `--ssh-sudo` | Use sudo on remote host |
| `--ssh-key FILE` | SSH private key file |
| `--progress` | Show progress bars |
| `--json` | Output in JSON format (for --list) |

**Examples:**
```bash
# List available backups
btrfs-backup-ng snapper restore /mnt/backup/root --config root --list

# Restore specific snapshot
btrfs-backup-ng snapper restore /mnt/backup/root --config root --snapshot 559

# Restore multiple snapshots
btrfs-backup-ng snapper restore /mnt/backup/root --config root --snapshot 559 --snapshot 560

# Restore all snapshots
btrfs-backup-ng snapper restore /mnt/backup/root --config root --all

# Dry run
btrfs-backup-ng snapper restore /mnt/backup/root --config root --all --dry-run
```

#### snapper generate-config

Generate TOML configuration for snapper-managed volumes.

```bash
btrfs-backup-ng snapper generate-config [OPTIONS]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--config NAME` | Generate for specific config(s) (can be repeated) |
| `--target PATH` | Include target in generated config |
| `--type TYPE` | Snapshot types to include (default: single) |
| `--min-age DURATION` | Minimum snapshot age (default: 1h) |
| `--ssh-sudo` | Enable ssh_sudo in target config |
| `-o, --output FILE` | Write to file instead of stdout |
| `--append FILE` | Append to existing config file |
| `--json` | Output in JSON format |

**Examples:**
```bash
# Generate config for all snapper volumes
btrfs-backup-ng snapper generate-config

# Generate for specific config with target
btrfs-backup-ng snapper generate-config --config root --target ssh://backup@server:/backups

# Write to file
btrfs-backup-ng snapper generate-config -o ~/.config/btrfs-backup-ng/snapper.toml

# Append to existing config
btrfs-backup-ng snapper generate-config --append ~/.config/btrfs-backup-ng/config.toml
```

**Example Output:**
```toml
# Snapper volume configuration
# Generated by: btrfs-backup-ng snapper generate-config

[[volumes]]
path = "/"
source = "snapper"

[volumes.snapper]
config_name = "root"
include_types = ["single"]
min_age = "1h"

[[volumes.targets]]
path = "ssh://backup@server:/backups/root"
ssh_sudo = true
```

---

## Filesystem Checks

The `--fs-checks` option controls how btrfs-backup-ng validates source and destination paths:

| Mode | Behavior |
|------|----------|
| `auto` (default) | Warn about issues but continue operation |
| `strict` | Error out on any check failure |
| `skip` | Bypass all filesystem checks |

The `--no-fs-checks` flag is an alias for `--fs-checks=skip`.

**Examples:**
```bash
# Default auto mode - warns but continues
btrfs-backup-ng run

# Strict mode for production
btrfs-backup-ng run --fs-checks=strict

# Skip checks for backup directories
btrfs-backup-ng restore --list /mnt/backup --no-fs-checks
```

---

## Legacy Mode

When the first argument is a path (not a subcommand), btrfs-backup-ng runs in legacy mode for backwards compatibility.

```bash
btrfs-backup-ng [OPTIONS] SOURCE DESTINATION
```

**Legacy Options:**
| Option | Description |
|--------|-------------|
| `-n, --num-snapshots N` | Number of snapshots to keep |
| `--no-incremental` | Disable incremental transfers |
| `--ssh-sudo` | Use sudo on SSH remote |
| `--ssh-username USER` | SSH username |
| `--ssh-identity-file FILE` | SSH private key |
| `--convert-rw` | Convert to read-write before deletion |
| `--fs-checks MODE` | Filesystem check mode: auto, strict, skip |
| `--no-fs-checks` | Skip filesystem checks (alias for --fs-checks=skip) |
| `--no-check-space` | Disable pre-flight space checking |
| `--force` | Proceed despite insufficient space warnings |
| `--safety-margin PERCENT` | Safety margin for space checks (default: 10%) |

**Examples:**
```bash
# Basic local backup
btrfs-backup-ng /home /mnt/backup/home

# Remote backup with SSH
btrfs-backup-ng /home ssh://backup@server:/backups/home

# With options
btrfs-backup-ng --ssh-sudo --num-snapshots 10 /home ssh://user@host:/backup
```

---

## SSH Remote Backups

When backing up to a remote host via SSH, there are several authentication scenarios.

### SSH Authentication

**Key-based authentication (recommended):**

For key-based SSH authentication, ensure your SSH key is loaded in ssh-agent. When running with sudo, you must preserve the `SSH_AUTH_SOCK` environment variable:

```bash
# Run with sudo -E to preserve SSH agent socket
sudo -E btrfs-backup-ng run
```

The tool will automatically detect SSH keys in `~/.ssh/` (id_ed25519, id_rsa, id_ecdsa) for the original user when running via sudo.

**Password authentication:**

Set `ssh_password_auth = true` in your target configuration to enable interactive SSH password prompts:

```toml
[[volumes.targets]]
path = "ssh://user@host:/backup"
ssh_password_auth = true
```

### Sudo on Remote Host

btrfs commands require root privileges. For remote backups, configure sudo access on the remote host.

**Passwordless sudo (recommended for automated backups):**

Add this to `/etc/sudoers.d/btrfs-backup` on the remote host:

```
# Allow btrfs commands without password for backup user
username ALL=(ALL) NOPASSWD: /usr/bin/btrfs
```

Then enable sudo in your target configuration:

```toml
[[volumes.targets]]
path = "ssh://user@host:/backup"
ssh_sudo = true
```

**Password-based sudo:**

If passwordless sudo is not configured, the tool will prompt for the sudo password interactively, or you can set it via environment variable:

```bash
export BTRFS_BACKUP_SUDO_PASSWORD="your-password"
btrfs-backup-ng run
```

### Complete Example for Unattended Backups

For fully automated backups without any password prompts:

1. **Local machine:** Run with `sudo -E` to preserve SSH agent
2. **Remote machine:** Configure passwordless sudo for btrfs

```bash
# On remote host, create /etc/sudoers.d/btrfs-backup:
# myuser ALL=(ALL) NOPASSWD: /usr/bin/btrfs

# On local machine, run backup:
sudo -E btrfs-backup-ng run
```

Configuration:
```toml
[[volumes.targets]]
path = "ssh://myuser@backupserver:/mnt/backups/myvolume"
ssh_sudo = true
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `BTRFS_BACKUP_SUDO_PASSWORD` | Sudo password for remote hosts (avoids interactive prompt) |
| `BTRFS_BACKUP_SSH_PASSWORD` | Enable SSH password authentication |
| `BTRFS_BACKUP_PASSWORDLESS_ONLY` | Only use passwordless sudo (fail if password required) |
| `BTRFS_BACKUP_LOG_LEVEL` | Override log level (DEBUG, INFO, WARNING, ERROR) |
| `SSH_AUTH_SOCK` | SSH agent socket (preserve with `sudo -E`) |

---

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | Error (configuration, runtime, or transfer failure) |

---

## Configuration File Locations

btrfs-backup-ng searches for configuration files in the following locations, in priority order:

| Priority | Location | Description |
|----------|----------|-------------|
| 1 (highest) | `-c /path/to/config.toml` | Explicit path via command line |
| 2 | `~/.config/btrfs-backup-ng/config.toml` | User-specific configuration |
| 3 | `/etc/btrfs-backup-ng/config.toml` | System-wide configuration |

The first configuration file found is used. If no configuration file exists and none is specified, commands that require configuration will display an error.

### Setup Examples

**User configuration:**
```bash
mkdir -p ~/.config/btrfs-backup-ng
btrfs-backup-ng config init > ~/.config/btrfs-backup-ng/config.toml
```

**System-wide configuration:**
```bash
sudo mkdir -p /etc/btrfs-backup-ng
btrfs-backup-ng config init | sudo tee /etc/btrfs-backup-ng/config.toml
sudo chmod 600 /etc/btrfs-backup-ng/config.toml
```

**Check which config is active:**
```bash
btrfs-backup-ng config validate
# Shows: Validating: /path/to/active/config.toml
```

---

## Compression Methods

| Method | Description | Speed | Ratio |
|--------|-------------|-------|-------|
| `none` | No compression | Fastest | 1:1 |
| `lz4` | Very fast compression | Very Fast | Low |
| `lzop` | Fast compression | Fast | Low |
| `pigz` | Parallel gzip | Fast | Medium |
| `gzip` | Standard compression | Medium | Medium |
| `zstd` | Modern compression (recommended) | Medium | High |

---

## Rate Limit Format

Bandwidth limits use a number with optional suffix:

| Suffix | Multiplier |
|--------|------------|
| (none) | Bytes per second |
| `K` | Kilobytes per second (1024) |
| `M` | Megabytes per second (1024^2) |
| `G` | Gigabytes per second (1024^3) |

Examples: `500K`, `10M`, `1G`, `52428800` (50MB in bytes)

---

## Duration Format

Retention durations use a number with suffix:

| Suffix | Duration |
|--------|----------|
| `m` | Minutes |
| `h` | Hours |
| `d` | Days |
| `w` | Weeks |

Examples: `30m`, `6h`, `1d`, `2w`
