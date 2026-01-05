# Man Pages for btrfs-backup-ng

This directory contains manual pages for btrfs-backup-ng.

## Available Man Pages

| Page | Description |
|------|-------------|
| `btrfs-backup-ng(1)` | Main command overview and configuration |
| `btrfs-backup-ng-run(1)` | Execute all backup jobs |
| `btrfs-backup-ng-snapshot(1)` | Create snapshots only |
| `btrfs-backup-ng-transfer(1)` | Transfer snapshots to targets |
| `btrfs-backup-ng-prune(1)` | Apply retention policies |
| `btrfs-backup-ng-list(1)` | Show snapshots and backups |
| `btrfs-backup-ng-status(1)` | Show job status and statistics |
| `btrfs-backup-ng-config(1)` | Configuration management |
| `btrfs-backup-ng-install(1)` | Install systemd timer/service |
| `btrfs-backup-ng-restore(1)` | Restore snapshots from backup |
| `btrfs-backup-ng-verify(1)` | Verify backup integrity |

## Installation

### System-Wide

```bash
sudo mkdir -p /usr/local/share/man/man1
sudo cp man1/*.1 /usr/local/share/man/man1/
sudo mandb
```

### Per-User

```bash
mkdir -p ~/.local/share/man/man1
cp man1/*.1 ~/.local/share/man/man1/
# Add to ~/.bashrc or ~/.zshrc if not already set:
# export MANPATH="$HOME/.local/share/man:$MANPATH"
```

## Viewing Man Pages

After installation:

```bash
man btrfs-backup-ng
man btrfs-backup-ng-restore
man btrfs-backup-ng-config
```

Without installation (from this directory):

```bash
man ./man1/btrfs-backup-ng.1
man ./man1/btrfs-backup-ng-restore.1
```

## Generating HTML/PDF

Convert to HTML:
```bash
groff -mandoc -Thtml man1/btrfs-backup-ng.1 > btrfs-backup-ng.html
```

Convert to PDF:
```bash
groff -mandoc -Tpdf man1/btrfs-backup-ng.1 > btrfs-backup-ng.pdf
```

## Checking for Errors

```bash
for f in man1/*.1; do
    echo "Checking $f..."
    mandoc -Tlint "$f"
done
```
