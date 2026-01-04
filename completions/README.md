# Shell Completions for btrfs-backup-ng

This directory contains shell completion scripts for bash, zsh, and fish.

## Installation

### Bash

**System-wide:**
```bash
sudo cp btrfs-backup-ng.bash /etc/bash_completion.d/btrfs-backup-ng
```

**Per-user:**
```bash
mkdir -p ~/.local/share/bash-completion/completions
cp btrfs-backup-ng.bash ~/.local/share/bash-completion/completions/btrfs-backup-ng
```

Or source directly in `~/.bashrc`:
```bash
source /path/to/btrfs-backup-ng.bash
```

### Zsh

**System-wide:**
```bash
sudo cp btrfs-backup-ng.zsh /usr/share/zsh/site-functions/_btrfs-backup-ng
```

**Per-user (add to fpath in ~/.zshrc before compinit):**
```bash
mkdir -p ~/.zfunc
cp btrfs-backup-ng.zsh ~/.zfunc/_btrfs-backup-ng
# Add to ~/.zshrc before compinit:
# fpath=(~/.zfunc $fpath)
```

After installation, run:
```bash
autoload -Uz compinit && compinit
```

### Fish

**System-wide:**
```bash
sudo cp btrfs-backup-ng.fish /usr/share/fish/vendor_completions.d/btrfs-backup-ng.fish
```

**Per-user:**
```bash
mkdir -p ~/.config/fish/completions
cp btrfs-backup-ng.fish ~/.config/fish/completions/
```

Fish automatically loads completions from these directories.

## Features

All completion scripts support:

- All subcommands (`run`, `snapshot`, `transfer`, `prune`, `list`, `status`, `config`, `install`, `uninstall`, `restore`)
- Global options (`-v`, `-q`, `--debug`, `-c`, etc.)
- Command-specific options with descriptions
- Nested subcommands (`config validate`, `config init`, `config import`)
- Argument completion for options (compression methods, timer presets, file paths)
- Path completion for SOURCE and DESTINATION arguments

## Regenerating Completions

If the CLI changes significantly, these completion scripts may need to be updated manually. Check `btrfs-backup-ng --help` and each subcommand's help for current options.
