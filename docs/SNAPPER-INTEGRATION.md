# Snapper Integration

btrfs-backup-ng provides seamless integration with [snapper](http://snapper.io/), the popular snapshot management tool for btrfs filesystems. This allows you to backup snapper-managed snapshots to local or remote destinations while preserving all metadata for restoration.

## Overview

### What is Snapper?

Snapper is a tool for managing btrfs snapshots. It creates automatic snapshots before and after system changes (like package installations), provides timeline-based snapshots, and handles snapshot cleanup. Many Linux distributions (openSUSE, Fedora, Arch) use snapper by default for system snapshot management.

### Why Use btrfs-backup-ng with Snapper?

While snapper manages local snapshots excellently, it doesn't handle offsite backups. btrfs-backup-ng fills this gap by:

- **Discovering** snapper configurations automatically
- **Backing up** snapper snapshots to local or remote destinations
- **Preserving** snapper metadata (info.xml) for full compatibility
- **Using incremental transfers** via btrfs send/receive for efficiency
- **Restoring** snapshots back into snapper-managed directories

### Key Design Principles

1. **Native directory layout**: Backups use snapper's `.snapshots/{num}/snapshot` structure
2. **Metadata preservation**: `info.xml` files are copied alongside snapshots
3. **Incremental efficiency**: Parent snapshots are detected for delta transfers
4. **Seamless restoration**: Restored snapshots integrate with existing snapper configs

## Quick Start

### 1. Detect Snapper Configurations

```bash
btrfs-backup-ng snapper detect
```

Output:
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

### 2. List Available Snapshots

```bash
btrfs-backup-ng snapper list --config root
```

Output:
```
Config: root (/)
------------------------------------------------------------
     NUM  TYPE    DATE                 DESCRIPTION
  ------  ------  -------------------  --------------------
     559  single  2026-01-08 14:30:00  timeline
     560  pre     2026-01-08 15:00:00  dnf install vim
     561  post    2026-01-08 15:01:00  dnf install vim
```

### 3. Backup Snapshots

```bash
# Local backup
btrfs-backup-ng snapper backup root /mnt/backup/root

# Remote backup via SSH
btrfs-backup-ng snapper backup root ssh://backup@server:/backups/root --ssh-sudo
```

### 4. Check Backup Status

```bash
btrfs-backup-ng snapper status --target /mnt/backup/root
```

### 5. Restore When Needed

```bash
# List available backups
btrfs-backup-ng snapper restore /mnt/backup/root --config root --list

# Restore specific snapshot
btrfs-backup-ng snapper restore /mnt/backup/root --config root --snapshot 559
```

## Configuration

### Using Config Files

For automated backups, create a TOML configuration:

```bash
# Generate config automatically
btrfs-backup-ng snapper generate-config --target ssh://backup@server:/backups -o config.toml
```

**Example configuration:**

```toml
[[volumes]]
path = "/"
source = "snapper"

[volumes.snapper]
config_name = "root"
include_types = ["single", "pre", "post"]
min_age = "1h"

[[volumes.targets]]
path = "ssh://backup@server:/backups/root"
ssh_sudo = true
compress = "zstd"
```

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `config_name` | Snapper configuration name | Required |
| `include_types` | Snapshot types to backup | `["single", "pre", "post"]` |
| `exclude_cleanup` | Cleanup algorithms to skip | `[]` |
| `min_age` | Minimum age before backup | `"1h"` |

### Snapshot Types

Snapper creates different types of snapshots:

| Type | Description | When Created |
|------|-------------|--------------|
| `single` | Standalone snapshot | Timeline, manual |
| `pre` | Before a change | Before package operations |
| `post` | After a change | After package operations |

By default, all types are backed up. You can filter with `include_types`:

```toml
[volumes.snapper]
config_name = "root"
include_types = ["single"]  # Only timeline/manual snapshots
```

### Minimum Age

The `min_age` option prevents backing up snapshots that are too recent. This is useful because:

- Snapper may create temporary pre/post pairs during operations
- Very recent snapshots might be in an inconsistent state
- Reduces unnecessary transfers for short-lived snapshots

```toml
[volumes.snapper]
min_age = "1h"   # Wait 1 hour before backup
min_age = "30m"  # Wait 30 minutes
min_age = "0"    # Backup immediately (not recommended)
```

## Backup Directory Layout

Backups mirror snapper's native structure:

```
/mnt/backup/root/.snapshots/
├── 559/
│   ├── info.xml          # Snapper metadata
│   └── snapshot/         # btrfs subvolume
├── 560/
│   ├── info.xml
│   └── snapshot/
└── 561/
    ├── info.xml
    └── snapshot/
```

This layout ensures:

- Direct compatibility with snapper tools
- Easy manual inspection of backups
- Proper parent chain for incremental transfers
- Full metadata preservation

### info.xml Contents

Each snapshot's `info.xml` contains:

```xml
<?xml version="1.0"?>
<snapshot>
  <type>single</type>
  <num>559</num>
  <date>2026-01-08 14:30:00</date>
  <description>timeline</description>
  <cleanup>timeline</cleanup>
  <userdata>
    <key>important</key>
    <value>yes</value>
  </userdata>
</snapshot>
```

## Incremental Transfers

btrfs-backup-ng automatically detects parent snapshots for efficient incremental transfers:

```
Source                          Destination
/.snapshots/559/snapshot  --->  /backup/.snapshots/559/snapshot  (full)
/.snapshots/560/snapshot  --->  /backup/.snapshots/560/snapshot  (incremental from 559)
/.snapshots/561/snapshot  --->  /backup/.snapshots/561/snapshot  (incremental from 560)
```

### How It Works

1. **Scan destination** for existing backups
2. **Find highest-numbered** backup that also exists locally
3. **Use as parent** for `btrfs send -p` incremental transfer
4. **Verify integrity** after transfer

### Transfer Progress

With `--progress` (default), you'll see real-time transfer status:

```
Backing up snapshot 561...
[################----] 80% | 1.2 GB/s | ETA: 0:02
```

## Restoration

### Listing Backups

Before restoring, list available backups:

```bash
btrfs-backup-ng snapper restore /mnt/backup/root --config root --list
```

Output:
```
Snapper backups at /mnt/backup/root:

     NUM  TYPE    DATE                 DESCRIPTION
  ------  ------  -------------------  ------------------------------
     559  single  2026-01-08 14:30:00  timeline
     560  pre     2026-01-08 15:00:00  dnf install vim
     561  post    2026-01-08 15:01:00  dnf install vim

Total: 3 backup(s)
```

### Restoring Snapshots

```bash
# Restore single snapshot
btrfs-backup-ng snapper restore /mnt/backup/root --config root --snapshot 559

# Restore multiple
btrfs-backup-ng snapper restore /mnt/backup/root --config root --snapshot 559 --snapshot 560

# Restore all
btrfs-backup-ng snapper restore /mnt/backup/root --config root --all
```

### How Restoration Works

1. **Read backup info.xml** to get snapshot metadata
2. **Determine next snapshot number** by scanning local `.snapshots/`
3. **Create target directory** at `/.snapshots/{new_num}/`
4. **Transfer via btrfs receive** (incremental when possible)
5. **Copy info.xml** with updated snapshot number
6. **Run snapper cleanup** if needed (optional)

### Restored Snapshot Numbers

Restored snapshots get new numbers to avoid conflicts:

```
Original (backup)         Restored (local)
559 (timeline)      -->   890 (next available)
560 (pre dnf)       -->   891
561 (post dnf)      -->   892
```

The original metadata is preserved in `info.xml`, including the original number and description.

## Remote Backups

### SSH Configuration

For remote backups, configure SSH access:

```toml
[[volumes.targets]]
path = "ssh://backup@server:/mnt/backups/root"
ssh_sudo = true
```

### Passwordless Sudo

On the remote server, configure passwordless sudo for btrfs commands:

```bash
# /etc/sudoers.d/btrfs-backup
backup ALL=(ALL) NOPASSWD: /usr/bin/btrfs
```

### SSH Agent

When running with sudo locally, preserve SSH agent:

```bash
sudo -E btrfs-backup-ng snapper backup root ssh://backup@server:/backups/root
```

## Automated Backups

### With systemd Timer

Install the systemd timer:

```bash
sudo btrfs-backup-ng install --timer=hourly
```

Your config file should include snapper volumes:

```toml
# /etc/btrfs-backup-ng/config.toml

[[volumes]]
path = "/"
source = "snapper"

[volumes.snapper]
config_name = "root"
include_types = ["single", "pre", "post"]
min_age = "1h"

[[volumes.targets]]
path = "ssh://backup@server:/backups/root"
ssh_sudo = true
```

### Verifying Timer

```bash
systemctl status btrfs-backup-ng.timer
journalctl -u btrfs-backup-ng.service -f
```

## Troubleshooting

### Snapper Not Found

```
Error: Snapper not found: snapper command not available
```

**Solution:** Install snapper:
```bash
# Fedora/RHEL
sudo dnf install snapper

# openSUSE
sudo zypper install snapper

# Arch
sudo pacman -S snapper
```

### No Configurations Found

```
No snapper configurations found.
```

**Solution:** Create a snapper configuration:
```bash
sudo snapper -c root create-config /
```

### Permission Denied

```
Error: Permission denied accessing /.snapshots
```

**Solution:** Run with sudo or configure appropriate permissions:
```bash
sudo btrfs-backup-ng snapper backup root /mnt/backup
```

### Incremental Transfer Failed

```
Error: Could not find parent subvolume
```

This occurs when the parent snapshot no longer exists locally. Solutions:

1. **Full transfer:** The tool will automatically fall back to full transfer
2. **Maintain parent chain:** Keep at least one local snapshot that matches a backup
3. **Use --no-incremental:** Force full transfers (slower but reliable)

### Remote Connection Failed

```
Error: SSH connection failed
```

Check:
1. SSH key authentication works: `ssh backup@server`
2. SSH agent is running: `ssh-add -l`
3. Preserve agent with sudo: `sudo -E btrfs-backup-ng ...`

## Best Practices

### 1. Regular Backup Schedule

Set up hourly or daily backups:
```bash
sudo btrfs-backup-ng install --timer=hourly
```

### 2. Filter Snapshot Types

Backup only what you need:
```toml
[volumes.snapper]
include_types = ["single"]  # Skip pre/post pairs
```

### 3. Use Compression for Remote

Enable compression for remote transfers:
```toml
[[volumes.targets]]
path = "ssh://backup@server:/backups/root"
compress = "zstd"
```

### 4. Monitor Backup Status

Regularly check backup health:
```bash
btrfs-backup-ng snapper status --target /mnt/backup/root
btrfs-backup-ng doctor --check transfers
```

### 5. Test Restoration

Periodically verify backups work:
```bash
# Dry run restore
btrfs-backup-ng snapper restore /mnt/backup/root --config root --snapshot 559 --dry-run
```

## See Also

- [CLI Reference](CLI-REFERENCE.md) - Complete command documentation
- [Migrating from btrbk](MIGRATING-FROM-BTRBK.md) - For btrbk users
- [Snapper Documentation](http://snapper.io/documentation.html) - Official snapper docs
