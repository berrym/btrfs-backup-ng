# Migrating from btrbk to btrfs-backup-ng

This guide helps btrbk users transition to btrfs-backup-ng while preserving their existing backup workflows.

## Automatic Configuration Import

The fastest way to migrate is using the built-in importer:

```bash
btrfs-backup-ng config import /etc/btrbk/btrbk.conf -o config.toml
```

The importer will:
- Parse your btrbk configuration
- Convert it to TOML format
- Warn about common btrbk pitfalls
- Suggest improvements

Review the output, then copy to your config location:

```bash
mkdir -p ~/.config/btrfs-backup-ng
mv config.toml ~/.config/btrfs-backup-ng/config.toml
```

## Manual Migration

If you prefer to migrate manually or want to understand the mapping, this section explains the conversion.

### Configuration Format

**btrbk** uses a custom syntax with implicit inheritance:

```
snapshot_preserve_min   2d
snapshot_preserve       14d 4w

volume /mnt/btr_pool
  subvolume home
    target /mnt/backup/home
```

**btrfs-backup-ng** uses explicit TOML:

```toml
[global.retention]
min = "2d"
daily = 14
weekly = 4

[[volumes]]
path = "/mnt/btr_pool/home"

[[volumes.targets]]
path = "/mnt/backup/home"
```

### Retention Policy Mapping

btrbk's retention syntax can be confusing. Here's how it maps:

| btrbk | btrfs-backup-ng | Notes |
|-------|-----------------|-------|
| `snapshot_preserve_min 2d` | `min = "2d"` | Minimum retention period |
| `snapshot_preserve 14d` | `daily = 14` | Daily snapshots to keep |
| `snapshot_preserve 4w` | `weekly = 4` | Weekly snapshots to keep |
| `snapshot_preserve 6m` | `monthly = 6` | Monthly snapshots to keep |
| `target_preserve_min` | `min = "..."` | Same as snapshot (applied to backups) |
| `target_preserve` | Same as above | Same retention for targets |

**Important**: btrfs-backup-ng uses a simpler mental model:
1. `min` - Keep everything for at least this duration
2. Time buckets (hourly, daily, weekly, monthly, yearly) - Keep N snapshots per bucket

### Volume and Subvolume Mapping

btrbk uses nested `volume`/`subvolume` structure:

```
volume /mnt/btr_pool
  subvolume home
  subvolume var
```

btrfs-backup-ng uses flat volume definitions with full paths:

```toml
[[volumes]]
path = "/mnt/btr_pool/home"

[[volumes]]
path = "/mnt/btr_pool/var"
```

### Target Mapping

**btrbk:**
```
volume /mnt/btr_pool
  subvolume home
    target /mnt/backup/home
    target ssh://backup@server/backups/home
```

**btrfs-backup-ng:**
```toml
[[volumes]]
path = "/mnt/btr_pool/home"

[[volumes.targets]]
path = "/mnt/backup/home"

[[volumes.targets]]
path = "ssh://backup@server:/backups/home"
```

### SSH Configuration Mapping

| btrbk | btrfs-backup-ng |
|-------|-----------------|
| `ssh_identity /path/to/key` | `ssh_key = "/path/to/key"` |
| `ssh_user backup` | Include in URL: `ssh://backup@host:/path` |
| `ssh_port 2222` | `ssh_port = 2222` |
| `backend btrfs-progs-sudo` | `ssh_sudo = true` |

**btrbk:**
```
ssh_identity /root/.ssh/backup_key
ssh_user backup

volume /mnt/pool
  subvolume data
    target ssh://server/backups
      backend btrfs-progs-sudo
```

**btrfs-backup-ng:**
```toml
[[volumes]]
path = "/mnt/pool/data"

[[volumes.targets]]
path = "ssh://backup@server:/backups"
ssh_key = "/root/.ssh/backup_key"
ssh_sudo = true
```

### Snapshot Directory Mapping

**btrbk:**
```
snapshot_dir btrbk_snapshots

volume /mnt/pool
  subvolume home
```

**btrfs-backup-ng:**
```toml
[global]
snapshot_dir = "btrbk_snapshots"  # Or rename to ".snapshots"

[[volumes]]
path = "/mnt/pool/home"
```

### Common btrbk Pitfalls (That btrfs-backup-ng Avoids)

#### 1. Indentation Confusion

**btrbk problem**: Indentation is ignored but looks meaningful:
```
# This LOOKS like snapshot_preserve only applies to home,
# but it actually applies globally!
volume /mnt/pool
  snapshot_preserve 7d
  subvolume home
```

**btrfs-backup-ng solution**: TOML is explicit:
```toml
# Global retention
[global.retention]
daily = 7

# Or volume-specific
[[volumes]]
path = "/mnt/pool/home"
[volumes.retention]
daily = 7
```

#### 2. `subvolume .` Anti-pattern

**btrbk problem**: Using `subvolume .` is confusing and error-prone:
```
volume /mnt/pool/home
  subvolume .
```

**btrfs-backup-ng solution**: Just use the full path:
```toml
[[volumes]]
path = "/mnt/pool/home"
```

#### 3. Missing Snapshot Directories

**btrbk problem**: Cryptic errors when snapshot directory doesn't exist.

**btrfs-backup-ng solution**: Automatically creates snapshot directories.

#### 4. No Config Validation

**btrbk problem**: Errors only discovered at runtime.

**btrfs-backup-ng solution**: Validate before running:
```bash
btrfs-backup-ng config validate
```

## Command Mapping

| btrbk Command | btrfs-backup-ng Command |
|---------------|-------------------------|
| `btrbk run` | `btrfs-backup-ng run` |
| `btrbk snapshot` | `btrfs-backup-ng snapshot` |
| `btrbk resume` | `btrfs-backup-ng transfer` |
| `btrbk prune` | `btrfs-backup-ng prune` |
| `btrbk list` | `btrfs-backup-ng list` |
| `btrbk origin` | (not yet implemented) |
| `btrbk diff` | (not yet implemented) |
| `btrbk -n run` | `btrfs-backup-ng run --dry-run` |
| `btrbk -v run` | `btrfs-backup-ng -v run` |

## Timer Migration

**btrbk** with systemd timer:
```bash
# /etc/systemd/system/btrbk.timer
[Timer]
OnCalendar=hourly
```

**btrfs-backup-ng** equivalent:
```bash
sudo btrfs-backup-ng install --timer=hourly
```

Or for custom schedules:
```bash
# btrbk: OnCalendar=*:0/15
sudo btrfs-backup-ng install --oncalendar='*:0/15'
```

## Coexistence During Migration

You can run both tools during migration:

1. Keep btrbk running on its schedule
2. Configure btrfs-backup-ng with different snapshot prefix
3. Test btrfs-backup-ng manually
4. Once satisfied, disable btrbk and enable btrfs-backup-ng timer

```toml
# Use different prefix during testing
[[volumes]]
path = "/home"
snapshot_prefix = "bbng-"  # Different from btrbk's prefix
```

## Example: Complete Migration

**Original btrbk.conf:**
```
snapshot_preserve_min   2d
snapshot_preserve       14d 4w 6m
target_preserve_min     2d
target_preserve         14d 4w 6m

ssh_identity /root/.ssh/backup_key

volume /mnt/btr_pool
  snapshot_dir .snapshots
  
  subvolume home
    target /mnt/backup/home
    target ssh://backup@nas/backups/home
      backend btrfs-progs-sudo
  
  subvolume var/log
    snapshot_preserve 7d 2w
    target /mnt/backup/var-log
```

**Equivalent btrfs-backup-ng config.toml:**
```toml
[global]
snapshot_dir = ".snapshots"

[global.retention]
min = "2d"
daily = 14
weekly = 4
monthly = 6

[[volumes]]
path = "/mnt/btr_pool/home"
snapshot_prefix = "home-"

[[volumes.targets]]
path = "/mnt/backup/home"

[[volumes.targets]]
path = "ssh://backup@nas:/backups/home"
ssh_key = "/root/.ssh/backup_key"
ssh_sudo = true

[[volumes]]
path = "/mnt/btr_pool/var/log"
snapshot_prefix = "var-log-"

[volumes.retention]
daily = 7
weekly = 2

[[volumes.targets]]
path = "/mnt/backup/var-log"
```

## Getting Help

If you encounter issues during migration:

1. Run with verbose output: `btrfs-backup-ng -v run`
2. Validate your config: `btrfs-backup-ng config validate`
3. Test with dry-run: `btrfs-backup-ng run --dry-run`
4. Check the [GitHub issues](https://github.com/berrym/btrfs-backup-ng/issues)
