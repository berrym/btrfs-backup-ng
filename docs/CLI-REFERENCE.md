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

Show job status, last run times, and statistics.

```bash
btrfs-backup-ng status
```

**Examples:**
```bash
btrfs-backup-ng status
```

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

**Examples:**
```bash
# Install hourly system timer (requires root)
sudo btrfs-backup-ng install --timer=hourly

# Install daily timer
sudo btrfs-backup-ng install --timer=daily

# Custom schedule: every 15 minutes
sudo btrfs-backup-ng install --oncalendar='*:0/15'

# Custom schedule: daily at 2am
sudo btrfs-backup-ng install --oncalendar='02:00'

# User-level timer (no root)
btrfs-backup-ng install --user --timer=hourly
```

After installation, enable the timer:
```bash
# System-wide
sudo systemctl daemon-reload
sudo systemctl enable --now btrfs-backup-ng.timer

# User-level
systemctl --user daemon-reload
systemctl --user enable --now btrfs-backup-ng.timer
```

---

### uninstall

Remove installed systemd timer and service.

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
| `--no-fs-checks` | Skip filesystem checks |

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

btrfs-backup-ng searches for configuration in this order:

1. Path specified with `-c/--config`
2. `~/.config/btrfs-backup-ng/config.toml`
3. `/etc/btrfs-backup-ng/config.toml`

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
