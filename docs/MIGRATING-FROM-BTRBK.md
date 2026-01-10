# Migrating from btrbk to btrfs-backup-ng

This guide helps btrbk users transition to btrfs-backup-ng while preserving their existing backup workflows.

## Quick Migration

For a complete migration including systemd timer:

```bash
# 1. Import btrbk configuration
btrfs-backup-ng config import /etc/btrbk/btrbk.conf -o ~/.config/btrfs-backup-ng/config.toml

# 2. Review the generated configuration
cat ~/.config/btrfs-backup-ng/config.toml

# 3. Migrate systemd timers (preview first)
sudo btrfs-backup-ng config migrate-systemd --dry-run

# 4. Apply systemd migration
sudo btrfs-backup-ng config migrate-systemd

# 5. Test the new setup
sudo btrfs-backup-ng run --dry-run
sudo btrfs-backup-ng run
```

## Automatic Configuration Import

The built-in importer converts btrbk.conf to TOML:

```bash
btrfs-backup-ng config import /etc/btrbk/btrbk.conf -o config.toml
```

The importer will:
- Parse your btrbk configuration
- Convert it to TOML format
- **Map timestamp formats correctly** (see below)
- Translate retention policies
- Convert SSH targets (`backend btrfs-progs-sudo` → `ssh_sudo = true`)
- Warn about common btrbk pitfalls
- Suggest improvements

Review the output, then copy to your config location:

```bash
mkdir -p ~/.config/btrfs-backup-ng
mv config.toml ~/.config/btrfs-backup-ng/config.toml
```

## Timestamp Format Mapping

btrbk uses named timestamp formats that are correctly mapped to strftime patterns:

| btrbk format | strftime equivalent | Example output |
|--------------|---------------------|----------------|
| `short` | `%Y%m%d` | `20260109` |
| `long` (default) | `%Y%m%dT%H%M` | `20260109T1430` |
| `long-iso` | `%Y%m%dT%H%M%S%z` | `20260109T143052+0000` |

**Example btrbk.conf:**
```
timestamp_format long-iso

volume /
  subvolume home
    target /mnt/backup/home
```

**Converted TOML:**
```toml
[global]
timestamp_format = "%Y%m%dT%H%M%S%z"

[[volumes]]
path = "/home"

[[volumes.targets]]
path = "/mnt/backup/home"
```

This ensures existing btrbk snapshots are recognized by btrfs-backup-ng, enabling seamless incremental backups.

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

### Raw Target Migration

btrbk's "raw targets" write btrfs send streams to files instead of using `btrfs receive`. This enables backups to non-btrfs filesystems (NFS, SMB, cloud storage) with optional compression and encryption.

btrfs-backup-ng fully supports raw targets for seamless migration:

| btrbk | btrfs-backup-ng |
|-------|-----------------|
| `raw_target_compress zstd` | `compress = "zstd"` |
| `raw_target_compress gzip` | `compress = "gzip"` |
| `raw_target_encrypt gpg` | `encrypt = "gpg"` |
| `raw_target_encrypt openssl_enc` | `encrypt = "openssl_enc"` |
| `gpg_recipient user@example.com` | `gpg_recipient = "user@example.com"` |
| `gpg_keyring /path/to/keyring` | `gpg_keyring = "/path/to/keyring"` |

**btrbk raw target example:**
```
volume /mnt/pool
  subvolume home
    raw_target_compress zstd
    raw_target_encrypt gpg
    gpg_recipient backup@example.com
    target /mnt/nas/backups/home
    target ssh://backup@server/backups/home
```

**btrfs-backup-ng equivalent:**
```toml
[[volumes]]
path = "/mnt/pool/home"

[[volumes.targets]]
path = "raw:///mnt/nas/backups/home"
compress = "zstd"
encrypt = "gpg"
gpg_recipient = "backup@example.com"

[[volumes.targets]]
path = "raw+ssh://backup@server/backups/home"
compress = "zstd"
encrypt = "gpg"
gpg_recipient = "backup@example.com"
```

The importer automatically:
- Detects `raw_target_compress` and `raw_target_encrypt` options
- Converts local targets to `raw://` URLs
- Converts SSH targets to `raw+ssh://` URLs
- Preserves GPG recipient and keyring settings

#### Supported Compression Algorithms

All btrbk compression algorithms are supported:

| Algorithm | Extension | Notes |
|-----------|-----------|-------|
| `gzip` | `.gz` | Standard, widely available |
| `pigz` | `.gz` | Parallel gzip, faster on multi-core |
| `zstd` | `.zst` | Best balance of speed and ratio |
| `lz4` | `.lz4` | Fastest, lower compression ratio |
| `xz` | `.xz` | Best ratio, slowest |
| `lzo` | `.lzo` | Fast compression |
| `bzip2` | `.bz2` | Good ratio, slower |
| `pbzip2` | `.bz2` | Parallel bzip2 |

#### Encryption Methods

**GPG (recommended for new setups):**
```toml
[[volumes.targets]]
path = "raw:///mnt/backup"
encrypt = "gpg"
gpg_recipient = "backup@example.com"
```

**OpenSSL (for btrbk migration compatibility):**
```toml
[[volumes.targets]]
path = "raw:///mnt/backup"
encrypt = "openssl_enc"
```

For OpenSSL encryption, set the passphrase via environment variable:
```bash
# Compatible with btrbk's BTRBK_PASSPHRASE
export BTRFS_BACKUP_PASSPHRASE="your_passphrase"
# or
export BTRBK_PASSPHRASE="your_passphrase"
```

#### Raw Backup File Format

Raw backups create files with predictable naming:
```
/mnt/backup/home.20260110T120000.btrfs.zst.gpg      # Stream file
/mnt/backup/home.20260110T120000.btrfs.zst.gpg.meta # Metadata (JSON)
```

The metadata file tracks:
- Snapshot UUID for incremental chain validation
- Parent snapshot reference
- Compression and encryption settings
- Creation timestamp and file size

This format is compatible with btrbk's raw backups, allowing you to continue incremental chains from existing btrbk backups.

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

## Systemd Timer Migration

btrfs-backup-ng provides a dedicated command to migrate from btrbk's systemd timer to btrfs-backup-ng's timer.

### Check Current Status

```bash
# See what btrbk units exist and their status
btrfs-backup-ng config migrate-systemd --dry-run
```

Example output:
```
── Systemd Migration ──

btrbk systemd units:
  btrbk.timer (enabled)
  btrbk.service (inactive)

No btrfs-backup-ng systemd units found.
Install with: btrfs-backup-ng systemd install

Dry run mode - no changes will be made.

Found 1 active btrbk unit(s):
  - btrbk.timer (enabled)
Would disable btrbk units (dry-run)
```

### Perform Migration

```bash
# Stop and disable btrbk timer, enable btrfs-backup-ng timer
sudo btrfs-backup-ng config migrate-systemd
```

This will:
1. Stop `btrbk.timer` if running
2. Disable `btrbk.timer`
3. Enable `btrfs-backup-ng.timer` (if installed)

### Install btrfs-backup-ng Timer First

If btrfs-backup-ng's systemd units aren't installed yet:

```bash
# Install with preset schedule
sudo btrfs-backup-ng install --timer=hourly

# Or with custom schedule (every 15 minutes)
sudo btrfs-backup-ng install --oncalendar='*:0/15'

# Then migrate
sudo btrfs-backup-ng config migrate-systemd
```

### Manual Timer Configuration

If you prefer manual setup, here's the equivalent:

**btrbk** timer:
```ini
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

### Verify Migration

After migration, verify the timer status:

```bash
# Check btrbk is disabled
systemctl is-enabled btrbk.timer  # Should show "disabled"

# Check btrfs-backup-ng is enabled
systemctl is-enabled btrfs-backup-ng.timer  # Should show "enabled"

# Check timer schedule
systemctl list-timers btrfs-backup-ng.timer
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
