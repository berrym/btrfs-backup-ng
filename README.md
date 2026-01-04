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
- **Subcommand CLI**: Modern interface with `run`, `snapshot`, `transfer`, `prune`, `restore`, `list`, `status`
- **Disaster Recovery**: Built-in restore command to pull backups back to local systems
- **Time-based Retention**: Intuitive policies (hourly, daily, weekly, monthly, yearly)
- **Rich Progress Bars**: Real-time transfer progress with speed, ETA, and percentage
- **Parallel Execution**: Concurrent volume and target transfers
- **Stream Compression**: zstd, gzip, lz4, pigz, lzop support
- **Bandwidth Throttling**: Rate limiting for remote transfers
- **Transaction Logging**: Structured JSON logs for auditing and automation
- **Email & Webhook Notifications**: Alerts on backup success/failure
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
| `restore` | Restore snapshots from backup location |
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

### Notifications

btrfs-backup-ng can send notifications on backup completion or failure via email (SMTP) or webhooks. This is particularly useful for automated/scheduled backups.

#### Email Notifications

```toml
[global.notifications.email]
enabled = true
smtp_host = "smtp.example.com"
smtp_port = 587
smtp_tls = "starttls"          # "ssl" (port 465), "starttls" (port 587), or "none"
smtp_user = "alerts@example.com"
smtp_password = "your-password"
from_addr = "btrfs-backup-ng@example.com"
to_addrs = ["admin@example.com", "ops@example.com"]
on_success = false             # Don't notify on success (default)
on_failure = true              # Notify on failure (default)
```

**SMTP Port Reference:**

| Port | TLS Mode | Description |
|------|----------|-------------|
| 465 | `ssl` | Implicit TLS (SMTPS) |
| 587 | `starttls` | Explicit TLS (submission) |
| 25 | `none` | Plain text (local mail only) |

**Example: Gmail SMTP**
```toml
[global.notifications.email]
enabled = true
smtp_host = "smtp.gmail.com"
smtp_port = 587
smtp_tls = "starttls"
smtp_user = "your-email@gmail.com"
smtp_password = "your-app-password"    # Use App Password, not account password
from_addr = "your-email@gmail.com"
to_addrs = ["your-email@gmail.com"]
on_failure = true
```

**Example: Local Postfix/Sendmail**
```toml
[global.notifications.email]
enabled = true
smtp_host = "localhost"
smtp_port = 25
smtp_tls = "none"
from_addr = "btrfs-backup-ng@myserver.local"
to_addrs = ["root@myserver.local"]
on_failure = true
```

#### Webhook Notifications

Send JSON payloads to any HTTP endpoint (Slack, Discord, PagerDuty, custom services, etc.).

```toml
[global.notifications.webhook]
enabled = true
url = "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXX"
method = "POST"                # POST or PUT
timeout = 30                   # Request timeout in seconds
on_success = false
on_failure = true

# Optional custom headers
[global.notifications.webhook.headers]
Authorization = "Bearer your-token"
X-Custom-Header = "value"
```

**Webhook Payload Format:**

The webhook receives a JSON payload with full backup details:

```json
{
  "event_type": "backup_complete",
  "status": "failure",
  "timestamp": "2026-01-04T18:30:00+00:00",
  "hostname": "myserver",
  "summary": "Backup failed: 1 volumes failed",
  "volumes_processed": 2,
  "volumes_failed": 1,
  "snapshots_created": 1,
  "transfers_completed": 1,
  "transfers_failed": 1,
  "duration_seconds": 45.2,
  "errors": ["Transfer to ssh://backup@remote:/backups: Connection refused"]
}
```

**Example: Slack Incoming Webhook**

Slack accepts the payload directly when using their Incoming Webhooks feature.

```toml
[global.notifications.webhook]
enabled = true
url = "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXX"
on_failure = true
```

**Example: Discord Webhook**

Discord webhooks expect a specific format. Use a proxy or serverless function to transform the payload.

**Example: ntfy.sh (Simple Push Notifications)**

```toml
[global.notifications.webhook]
enabled = true
url = "https://ntfy.sh/your-topic"
method = "POST"
on_failure = true

[global.notifications.webhook.headers]
Title = "btrfs-backup-ng Alert"
Priority = "high"
Tags = "warning,backup"
```

#### Notification Events

Notifications are sent for these events:

| Event | Commands | Status Values |
|-------|----------|---------------|
| `backup_complete` | `run`, `transfer` | `success`, `partial`, `failure` |
| `prune_complete` | `prune` | `success`, `partial`, `failure` |

**Status meanings:**
- `success`: All operations completed without errors
- `partial`: Some operations succeeded, some failed
- `failure`: All operations failed

#### Controlling When Notifications Are Sent

By default, notifications are only sent on failure. Configure per notification method:

```toml
# Email: notify on failure only (default)
[global.notifications.email]
enabled = true
on_success = false
on_failure = true

# Webhook: notify on both success and failure
[global.notifications.webhook]
enabled = true
on_success = true
on_failure = true
```

#### Testing Notifications

To test your notification configuration, run a backup with intentionally bad settings:

```bash
# Create a test config with a bad target
cat > /tmp/test-notify.toml << 'EOF'
[global.notifications.email]
enabled = true
smtp_host = "localhost"
smtp_port = 25
from_addr = "test@localhost"
to_addrs = ["root@localhost"]
on_failure = true

[[volumes]]
path = "/nonexistent"

[[volumes.targets]]
path = "/also/nonexistent"
EOF

# Run with the test config (will fail and trigger notification)
sudo btrfs-backup-ng -c /tmp/test-notify.toml run
```

#### Security Considerations

- **SMTP passwords in config files**: Use file permissions (`chmod 600`) to protect your configuration file
- **Webhook URLs**: Treat webhook URLs as secrets; they often provide unauthenticated access
- **Environment variables**: Future versions may support reading secrets from environment variables

```bash
# Protect config file with sensitive credentials
sudo chmod 600 /etc/btrfs-backup-ng/config.toml
sudo chown root:root /etc/btrfs-backup-ng/config.toml
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
require_mount = false                  # Require path to be a mount point (safety check)
```

### Mount Verification (External Drive Safety)

When backing up to external drives or removable media, there's a common pitfall: if the drive isn't mounted, backups will silently write to the mount point directory on your root filesystem, consuming disk space and not actually backing up your data.

The `require_mount` option prevents this by verifying that the target path is an active mount point before starting any transfers.

```toml
# External USB drive backup with mount verification
[[volumes.targets]]
path = "/mnt/usb-backup"
require_mount = true    # Fail if /mnt/usb-backup is not a mount point
```

**When to use `require_mount = true`:**
- External USB drives
- Removable media (SD cards, etc.)
- Network mounts (NFS, SMB) that may be disconnected
- Any target that might not always be available

**Example error when drive is not mounted:**
```
ERROR: Target /mnt/usb-backup is not mounted. 
Ensure the drive is connected and mounted, or set require_mount = false.
```

**Complete external drive configuration example:**
```toml
[[volumes]]
path = "/home"
snapshot_prefix = "home"

[[volumes.targets]]
path = "/mnt/external-backup/home"
require_mount = true    # Safety check - fail if drive not mounted

# Also backup to remote server (no mount check needed for SSH)
[[volumes.targets]]
path = "ssh://backup@server:/backups/home"
ssh_sudo = true
```

**Note:** `require_mount` only applies to local targets. It has no effect on SSH targets.

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

## SSH Remote Backup Setup

This section provides a complete guide for setting up passwordless SSH backups to a remote server. For fully automated backups, you need:

1. SSH key authentication (no password prompts)
2. Passwordless sudo on the remote for btrfs commands
3. A btrfs filesystem on the remote to receive backups

### Step 1: Create a Dedicated Backup User (Remote Server)

On the **remote backup server**, create a dedicated user for backups:

```bash
# Create backup user with no password (key-only auth)
sudo useradd -m -s /bin/bash backup

# Create the backup directory on a btrfs filesystem
sudo mkdir -p /mnt/backups
sudo chown backup:backup /mnt/backups

# Verify the backup location is on a btrfs filesystem
df -T /mnt/backups | grep btrfs || echo "WARNING: Not a btrfs filesystem!"
```

### Step 2: Configure Passwordless Sudo (Remote Server)

btrfs send/receive requires root privileges. On the **remote server**, configure sudo:

Create a dedicated sudoers file in `/etc/sudoers.d/` (preferred over editing `/etc/sudoers` directly):

```bash
# Create sudoers drop-in file for backup user
sudo visudo -f /etc/sudoers.d/btrfs-backup
```

Add one of these configurations:

```sudoers
# Option 1: Full btrfs access (simplest, recommended)
backup ALL=(ALL) NOPASSWD: /usr/bin/btrfs

# Option 2: Restricted to specific btrfs subcommands (more secure)
backup ALL=(ALL) NOPASSWD: /usr/bin/btrfs receive *
backup ALL=(ALL) NOPASSWD: /usr/bin/btrfs subvolume *
backup ALL=(ALL) NOPASSWD: /usr/bin/btrfs send *
backup ALL=(ALL) NOPASSWD: /usr/bin/btrfs filesystem *
```

**Important:** Set correct permissions on the sudoers file (required by most distros):

```bash
sudo chmod 440 /etc/sudoers.d/btrfs-backup
```

Verify sudo works without password:

```bash
# Test on remote server (as backup user)
sudo -n btrfs --version
# Should print version without prompting for password
```

### Step 3: Set Up SSH Key Authentication (Local Machine)

On your **local machine**, generate a dedicated SSH key for backups:

```bash
# Generate Ed25519 key (recommended)
ssh-keygen -t ed25519 -f ~/.ssh/btrfs-backup-key -C "btrfs-backup-ng"

# Or RSA if Ed25519 isn't supported
ssh-keygen -t rsa -b 4096 -f ~/.ssh/btrfs-backup-key -C "btrfs-backup-ng"
```

Copy the public key to the remote server:

```bash
# Copy key to remote backup user
ssh-copy-id -i ~/.ssh/btrfs-backup-key backup@remote-server

# Test the connection
ssh -i ~/.ssh/btrfs-backup-key backup@remote-server 'echo "SSH works!"'
```

### Step 4: Test Remote btrfs Access

Verify everything works end-to-end:

```bash
# Test SSH + sudo + btrfs (from local machine)
ssh -i ~/.ssh/btrfs-backup-key backup@remote-server 'sudo btrfs filesystem show /mnt/backups'
```

If this command succeeds without prompting for any passwords, you're ready.

### Step 5: Configure btrfs-backup-ng

Create your configuration file:

```toml
[global]
snapshot_dir = ".snapshots"
log_file = "/var/log/btrfs-backup-ng.log"
transaction_log = "/var/log/btrfs-backup-ng.jsonl"

[[volumes]]
path = "/home"
snapshot_prefix = "home"

[[volumes.targets]]
path = "ssh://backup@remote-server:/mnt/backups/home"
ssh_sudo = true                           # Required for btrfs receive
ssh_key = "~/.ssh/btrfs-backup-key"       # Path to private key
```

### Step 6: Running Backups with sudo Locally

btrfs operations on the local machine also require root. When running with `sudo`, you need to preserve your SSH agent:

```bash
# Method 1: Use sudo -E to preserve environment (including SSH_AUTH_SOCK)
sudo -E btrfs-backup-ng run

# Method 2: Explicitly specify the SSH key in config (works without agent)
# Just ensure ssh_key is set in your target configuration
sudo btrfs-backup-ng run
```

**Important:** When sudo changes to root, it normally loses access to your user's SSH agent. Options:

| Method | How | Best For |
|--------|-----|----------|
| `sudo -E` | Preserves SSH_AUTH_SOCK | Interactive use |
| `ssh_key` in config | Uses key file directly | Automated/systemd |
| Root's SSH key | Generate key for root user | Dedicated backup systems |

### Complete Working Example

**Local machine setup:**
```bash
# 1. Generate SSH key
ssh-keygen -t ed25519 -f ~/.ssh/btrfs-backup-key -N "" -C "btrfs-backup-ng"

# 2. Copy to remote
ssh-copy-id -i ~/.ssh/btrfs-backup-key backup@backupserver

# 3. Create config
sudo mkdir -p /etc/btrfs-backup-ng
sudo tee /etc/btrfs-backup-ng/config.toml << 'EOF'
[global]
snapshot_dir = ".snapshots"

[[volumes]]
path = "/home"

[[volumes.targets]]
path = "ssh://backup@backupserver:/mnt/backups/home"
ssh_sudo = true
ssh_key = "/home/myuser/.ssh/btrfs-backup-key"
EOF

# 4. Test
sudo btrfs-backup-ng run --dry-run
sudo btrfs-backup-ng run
```

**Remote server setup:**
```bash
# 1. Create backup user
sudo useradd -m backup

# 2. Configure passwordless sudo
echo 'backup ALL=(ALL) NOPASSWD: /usr/bin/btrfs' | sudo tee /etc/sudoers.d/btrfs-backup
sudo chmod 440 /etc/sudoers.d/btrfs-backup

# 3. Create backup directory (must be on btrfs)
sudo mkdir -p /mnt/backups/home
sudo chown backup:backup /mnt/backups/home
```

### SSH Troubleshooting

**"Permission denied" on SSH connect:**
```bash
# Check key permissions (must be 600)
chmod 600 ~/.ssh/btrfs-backup-key

# Test SSH manually with verbose output
ssh -vvv -i ~/.ssh/btrfs-backup-key backup@remote-server
```

**"sudo: a password is required":**
```bash
# On remote, check sudoers syntax
sudo visudo -c -f /etc/sudoers.d/btrfs-backup

# Test sudo as backup user
sudo -u backup sudo -n btrfs --version
```

**"ERROR: not a btrfs filesystem":**
```bash
# On remote, verify backup location is btrfs
df -T /mnt/backups
# Must show "btrfs" as filesystem type
```

**SSH works manually but fails in btrfs-backup-ng:**
```bash
# Check if running with sudo drops SSH agent
sudo env | grep SSH_AUTH_SOCK   # Should show your socket

# If empty, either use sudo -E or specify ssh_key in config
sudo -E btrfs-backup-ng run
```

### Password-Based Authentication (Not Recommended)

If you cannot set up passwordless authentication, btrfs-backup-ng supports password prompts:

```toml
[[volumes.targets]]
path = "ssh://backup@remote-server:/mnt/backups"
ssh_sudo = true
ssh_password_auth = true    # Enable SSH password prompts
```

For sudo passwords on remote:
```bash
# Set via environment variable (insecure - visible in process list)
export BTRFS_BACKUP_SUDO_PASSWORD="password"
sudo -E btrfs-backup-ng run
```

**Warning:** Password-based authentication is not suitable for automated/unattended backups.

## Restoring from Backups

The `restore` command enables pulling snapshots from backup storage back to local systems. This is essential for disaster recovery, migration, and backup verification.

### Basic Restore Operations

```bash
# List available snapshots at backup location
btrfs-backup-ng restore --list ssh://backup@server:/backups/home
btrfs-backup-ng restore --list /mnt/external-backup/home

# Restore latest snapshot
btrfs-backup-ng restore ssh://backup@server:/backups/home /mnt/restore

# Restore specific snapshot by name
btrfs-backup-ng restore ssh://backup@server:/backups/home /mnt/restore \
    --snapshot home-20260104-120000

# Restore snapshot before a specific date
btrfs-backup-ng restore ssh://backup@server:/backups/home /mnt/restore \
    --before "2026-01-01 12:00"

# Interactive selection (shows list, lets you pick)
btrfs-backup-ng restore ssh://backup@server:/backups/home /mnt/restore --interactive

# Restore ALL snapshots (full mirror)
btrfs-backup-ng restore ssh://backup@server:/backups/home /mnt/restore --all

# Dry run (show what would be restored)
btrfs-backup-ng restore --dry-run ssh://backup@server:/backups/home /mnt/restore
```

### How Restore Works

btrfs-backup-ng automatically handles incremental restore chains:

```
Backup has: [snap-1, snap-2, snap-3, snap-4]
                ↓       ↓       ↓       ↓
            (full)  (incr)  (incr)  (incr)

You request: snap-4

Restore chain: snap-1 → snap-2 → snap-3 → snap-4
               (2.1 GB)  (156 MB) (89 MB)  (234 MB)
```

The restore command:
1. Analyzes the parent chain required for the target snapshot
2. Checks which parents already exist locally (can skip those)
3. Restores snapshots in order (oldest first) to satisfy dependencies
4. Uses incremental transfers when possible (much faster)

### Restore Options

| Option | Description |
|--------|-------------|
| `-l, --list` | List available snapshots at backup location |
| `-s, --snapshot NAME` | Restore specific snapshot by name |
| `--before DATETIME` | Restore snapshot closest to this time (YYYY-MM-DD [HH:MM:SS]) |
| `-a, --all` | Restore all snapshots (full mirror) |
| `-i, --interactive` | Interactive snapshot selection |
| `--dry-run` | Show what would be restored without making changes |
| `--no-incremental` | Force full transfers (skip incremental) |
| `--overwrite` | Overwrite existing snapshots instead of skipping |
| `--in-place` | Restore to original location (DANGEROUS, requires confirmation) |
| `--yes-i-know-what-i-am-doing` | Confirm dangerous operations like in-place restore |
| `--prefix PREFIX` | Snapshot prefix filter (include trailing hyphen, e.g., `home-`) |
| `--ssh-sudo` | Use sudo on remote for btrfs commands |
| `--ssh-key FILE` | SSH private key file for authentication |
| `--compress METHOD` | Compression for transfer (none, zstd, gzip, lz4, pigz, lzop) |
| `--rate-limit RATE` | Bandwidth limit (e.g., '10M', '1G') |
| `--no-fs-checks` | Skip btrfs subvolume verification (needed for backup directories) |
| `--progress` | Show progress bars (default in terminal) |
| `--no-progress` | Disable progress bars |

### Restore from Local Backup

```bash
# External drive backup
btrfs-backup-ng restore /mnt/external-backup/home /mnt/restore

# With specific snapshot
btrfs-backup-ng restore /mnt/external-backup/home /mnt/restore \
    --snapshot home-20260104-120000
```

### Restore from Remote Backup

```bash
# SSH backup server
btrfs-backup-ng restore ssh://backup@server:/backups/home /mnt/restore \
    --ssh-sudo \
    --ssh-key ~/.ssh/backup_key

# With compression for slow links
btrfs-backup-ng restore ssh://backup@server:/backups/home /mnt/restore \
    --compress=zstd --rate-limit=10M
```

### Disaster Recovery Walkthrough

Complete example of recovering a system from backup:

```bash
# 1. Boot from live USB/recovery environment

# 2. Mount your btrfs filesystem
mount /dev/sda2 /mnt/newroot

# 3. Create restore target directory
mkdir -p /mnt/newroot/restored-home

# 4. List available backups
btrfs-backup-ng restore --list ssh://backup@server:/backups/home

# 5. Restore the latest snapshot
btrfs-backup-ng restore ssh://backup@server:/backups/home /mnt/newroot/restored-home \
    --ssh-sudo

# 6. (Optional) Restore to specific point in time
btrfs-backup-ng restore ssh://backup@server:/backups/home /mnt/newroot/restored-home \
    --before "2026-01-01" --ssh-sudo

# 7. Verify the restore
ls -la /mnt/newroot/restored-home/

# 8. Rename/move the restored snapshot to final location
# (depends on your specific setup)
```

### Restore Strategies: Choosing the Right Approach

There are several ways to restore data from backups, each with different trade-offs. Choose the approach that best fits your situation and comfort level.

#### Strategy 1: Restore to Temporary Location (Recommended)

**Best for:** Most recovery scenarios, file recovery, verification before committing

This is the safest approach - restore to a separate location, verify the contents, then manually move or copy what you need.

```bash
# 1. Create a temporary restore directory (must be on btrfs)
sudo mkdir -p /mnt/btrfs-restore

# 2. Restore the snapshot you want
sudo btrfs-backup-ng restore /mnt/backup/home /mnt/btrfs-restore \
    --snapshot home-20260104-120000 \
    --prefix "home-" \
    --no-fs-checks

# 3. Verify the restored snapshot contains what you expect
ls -la /mnt/btrfs-restore/home-20260104-120000/
diff -r /mnt/btrfs-restore/home-20260104-120000/Documents ~/Documents

# 4a. Copy specific files you need (safest)
cp -a /mnt/btrfs-restore/home-20260104-120000/Documents/important.doc ~/Documents/

# 4b. Or replace entire directory (after backing up current)
mv ~/Documents ~/Documents.old
cp -a /mnt/btrfs-restore/home-20260104-120000/Documents ~/Documents

# 5. Clean up when done
sudo btrfs subvolume delete /mnt/btrfs-restore/home-20260104-120000
```

**Pros:**
- Completely safe - original data untouched until you're ready
- Can inspect and verify before committing
- Can selectively restore individual files or directories
- Easy to abort if something is wrong

**Cons:**
- Requires extra disk space for temporary restore
- More manual steps involved
- Slower for full system recovery

#### Strategy 2: Restore and Rename with Btrfs Snapshots

**Best for:** Full directory replacement while keeping a safety net

Use btrfs's native snapshot capability to create a backup of current state before replacing.

```bash
# 1. Create a snapshot of your CURRENT state (safety net)
sudo btrfs subvolume snapshot /home /home.before-restore

# 2. Restore the backup snapshot to a temporary location
sudo btrfs-backup-ng restore /mnt/backup/home /mnt/btrfs-restore \
    --snapshot home-20260104-120000 \
    --prefix "home-" \
    --no-fs-checks

# 3. Verify the restore looks correct
ls -la /mnt/btrfs-restore/home-20260104-120000/

# 4. Rename current to old, restored to current
sudo mv /home /home.old
sudo mv /mnt/btrfs-restore/home-20260104-120000 /home

# 5. If everything works, clean up old versions later
sudo btrfs subvolume delete /home.old
sudo btrfs subvolume delete /home.before-restore

# 5b. If something went wrong, roll back:
sudo mv /home /home.failed-restore
sudo mv /home.before-restore /home
```

**Pros:**
- Full replacement with automatic rollback option
- Uses btrfs efficiency (snapshots are instant, space-efficient)
- Clear before/after comparison possible

**Cons:**
- Requires understanding of btrfs subvolume management
- Need to handle mount points if /home is a separate subvolume
- May require reboot or remount for mount point changes

#### Strategy 3: Manual Btrfs Send/Receive

**Best for:** Advanced users, scripted recovery, maximum control

Use raw btrfs commands for complete control over the restore process.

```bash
# 1. From local backup drive
sudo btrfs send /mnt/backup/home/home-20260104-120000 | sudo btrfs receive /mnt/restore/

# 2. From remote backup via SSH
ssh backup@server "sudo btrfs send /backups/home/home-20260104-120000" | \
    sudo btrfs receive /mnt/restore/

# 3. With compression for slow networks
ssh backup@server "sudo btrfs send /backups/home/home-20260104-120000 | zstd" | \
    zstd -d | sudo btrfs receive /mnt/restore/

# 4. Incremental restore (if you have the parent locally)
ssh backup@server "sudo btrfs send -p /backups/home/home-20260103-120000 \
    /backups/home/home-20260104-120000" | sudo btrfs receive /mnt/restore/
```

**Pros:**
- Maximum flexibility and control
- Can be easily scripted
- Works in minimal recovery environments
- No dependency on btrfs-backup-ng being installed

**Cons:**
- Must manually handle incremental parent chains
- No progress display or error recovery
- Easy to make mistakes with complex commands
- Must manually manage snapshot prefixes and naming

#### Strategy 4: In-Place Restore (Use with Caution)

**Best for:** Disaster recovery when you're certain, automated recovery scripts

Direct replacement of the target location. This is the most dangerous but fastest approach.

```bash
# WARNING: This will OVERWRITE existing data at /home
# Make absolutely sure you have the right snapshot!

# 1. First, verify what you're about to restore
btrfs-backup-ng restore --list ssh://backup@server:/backups/home \
    --prefix "home-" --no-fs-checks

# 2. Do a dry-run first
btrfs-backup-ng restore ssh://backup@server:/backups/home /home \
    --in-place \
    --snapshot home-20260104-120000 \
    --prefix "home-" \
    --dry-run

# 3. If dry-run looks correct, proceed with actual restore
btrfs-backup-ng restore ssh://backup@server:/backups/home /home \
    --in-place \
    --yes-i-know-what-i-am-doing \
    --snapshot home-20260104-120000 \
    --prefix "home-" \
    --ssh-sudo
```

**Pros:**
- Fastest for full recovery
- Single command, minimal steps
- Handles incremental chains automatically

**Cons:**
- **DESTRUCTIVE** - existing data is overwritten
- No easy rollback if wrong snapshot chosen
- Requires explicit confirmation flag
- Not suitable for partial recovery

#### Strategy Comparison

| Strategy | Safety | Speed | Disk Space | Complexity | Best For |
|----------|--------|-------|------------|------------|----------|
| Temporary location | Highest | Slow | Requires extra | Low | File recovery, verification |
| Rename with snapshots | High | Medium | Minimal extra | Medium | Full replacement with rollback |
| Manual btrfs commands | Medium | Fast | Minimal | High | Advanced users, scripting |
| In-place restore | Lowest | Fastest | None | Low | Disaster recovery, automation |

#### Recommendations by Scenario

**"I accidentally deleted some files":**
→ Use Strategy 1 (temporary location), copy just the files you need

**"My system is corrupted, I need to restore /home":**
→ Use Strategy 2 (rename with snapshots) for safety, or Strategy 4 if you're confident

**"I'm setting up a new machine from backups":**
→ Use Strategy 1 or the restore command directly - there's nothing to lose

**"I'm writing an automated disaster recovery script":**
→ Use Strategy 3 (manual commands) or Strategy 4 (in-place) depending on your safety requirements

**"I'm in a minimal recovery environment without btrfs-backup-ng":**
→ Use Strategy 3 (manual btrfs send/receive)

### Collision Handling

By default, snapshots that already exist locally are skipped:

```bash
# Skip existing (default)
btrfs-backup-ng restore ssh://...:/backups/home /mnt/restore

# Overwrite existing snapshots
btrfs-backup-ng restore ssh://...:/backups/home /mnt/restore --overwrite
```

### Troubleshooting Restore

**"Source does not seem to be a btrfs subvolume":**
```bash
# Backup directories are regular directories containing snapshot subvolumes
# Use --no-fs-checks to skip subvolume verification when listing/restoring
btrfs-backup-ng restore --list /mnt/backup/home --no-fs-checks --prefix "home-"

# This is normal - the backup location is a directory, not a subvolume itself
```

**"Destination is not on a btrfs filesystem":**
```bash
# The restore target must be on btrfs
df -T /mnt/restore | grep btrfs
# If not btrfs, create or mount a btrfs filesystem first
```

**"No snapshots found at backup location":**
```bash
# List available snapshots to see what's there
btrfs-backup-ng restore --list ssh://backup@server:/backups/home --no-fs-checks

# IMPORTANT: The prefix must include the trailing hyphen!
# If snapshots are named "home-20260104-120000", use --prefix "home-"
btrfs-backup-ng restore --list /mnt/backup --no-fs-checks --prefix "home-"

# Without the correct prefix, date parsing will fail and snapshots won't be found
```

**"Could not parse date from snapshot name":**
```bash
# This usually means the prefix doesn't match or is missing the trailing hyphen
# Snapshot name format: {prefix}{YYYYMMDD-HHMMSS}
# Example: home-20260104-120000 requires --prefix "home-"

# Check your snapshot names first
ls /mnt/backup/home/
# Output: home-20260104-120000  home-20260104-140000  ...

# Then use the correct prefix (including the hyphen)
btrfs-backup-ng restore --list /mnt/backup/home --prefix "home-" --no-fs-checks
```

**"Permission denied" during restore:**
```bash
# For SSH backups, ensure ssh_sudo is enabled
btrfs-backup-ng restore ssh://...:/backups/home /mnt/restore --ssh-sudo

# Local restore requires root for btrfs receive
sudo btrfs-backup-ng restore /mnt/backup /mnt/restore --no-fs-checks
```

**"btrfs send/receive failed" when snapshot already exists:**
```bash
# By default, existing snapshots are skipped but may show errors
# Use --overwrite to replace existing snapshots
btrfs-backup-ng restore /mnt/backup /mnt/restore --overwrite --no-fs-checks

# Or restore to a clean directory
mkdir /mnt/restore-new
btrfs-backup-ng restore /mnt/backup /mnt/restore-new --no-fs-checks
```

**Transfer is very slow:**
```bash
# Use compression for remote restores
btrfs-backup-ng restore ssh://...:/backups/home /mnt/restore --compress=zstd

# Check if incremental is working (should show "incremental from X")
btrfs-backup-ng restore ssh://...:/backups/home /mnt/restore -v

# Limit bandwidth if needed
btrfs-backup-ng restore ssh://...:/backups/home /mnt/restore --rate-limit=50M
```

### Important Notes

1. **The `--no-fs-checks` flag is usually required** when listing or restoring from backup directories. Backup locations are regular directories containing snapshot subvolumes, not subvolumes themselves.

2. **The `--prefix` must include the trailing hyphen** that separates the prefix from the timestamp. If your snapshots are named `home-20260104-120000`, use `--prefix "home-"` not `--prefix "home"`.

3. **Restore destination must be on btrfs**. The `btrfs receive` command requires a btrfs filesystem to create subvolumes.

4. **Incremental restore chains are automatic**. When restoring a snapshot that depends on parent snapshots, all required parents are restored first in the correct order.

5. **Root privileges are typically required** for `btrfs receive`. Use `sudo` for local restores or `--ssh-sudo` for remote restores.

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
