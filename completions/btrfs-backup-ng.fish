# fish completion for btrfs-backup-ng
# Generated from the argument parser; edit the parser, not this file.

complete -c btrfs-backup-ng -f

function __fish_btrfs_backup_ng_no_subcommand
    set -l cmd (commandline -opc)
    test (count $cmd) -eq 1
end

function __fish_btrfs_backup_ng_using_command
    set -l cmd (commandline -opc)
    test (count $cmd) -ge 2 -a "$cmd[2]" = "$argv[1]"
end

function __fish_btrfs_backup_ng_using_subcommand
    set -l cmd (commandline -opc)
    test (count $cmd) -ge 3 -a "$cmd[2]" = "$argv[1]" -a "$cmd[3]" = "$argv[2]"
end

complete -c btrfs-backup-ng -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -s v -l verbose -d 'Enable verbose output'
complete -c btrfs-backup-ng -s q -l quiet -d 'Suppress non-essential output'
complete -c btrfs-backup-ng -l debug -d 'Enable debug output'
complete -c btrfs-backup-ng -l btrfs-debug -d 'Run btrfs send and receive with -vv and log every line they print, one per file operation; implies --debug. Same as [global] btrfs_debug'
complete -c btrfs-backup-ng -s V -l version -d 'Show version and exit'
complete -c btrfs-backup-ng -s c -l config -d 'Path to configuration file' -r -F

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a completions -d 'completions'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a config -d 'config'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a doctor -d 'doctor'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a estimate -d 'estimate'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a install -d 'install'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a list -d 'list'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a manpages -d 'manpages'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a prune -d 'prune'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a raw -d 'raw'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a restore -d 'restore'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a run -d 'run'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a snapper -d 'snapper'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a snapshot -d 'snapshot'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a status -d 'status'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a transfer -d 'transfer'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a transfers -d 'transfers'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a uninstall -d 'uninstall'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' -a verify -d 'verify'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command completions' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command completions' -a install -d 'install'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand completions install' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand completions install' -l shell -d 'Shell to install completions for' -x -a 'bash zsh fish'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand completions install' -l system -d 'Install system-wide (requires root)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command completions' -a path -d 'path'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand completions path' -s h -l help -d 'show this help message and exit'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command config' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command config' -a detect -d 'detect'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config detect' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config detect' -l json -d 'Output in JSON format for scripting'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config detect' -s w -l wizard -d 'Launch interactive wizard with detected volumes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command config' -a import -d 'import'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config import' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config import' -s o -l output -d 'Output file (default: stdout)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config import' -l force -d 'Overwrite the output file if it already exists'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command config' -a init -d 'init'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config init' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config init' -s i -l interactive -d 'Run interactive configuration wizard'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config init' -s o -l output -d 'Output file (default: stdout)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config init' -l force -d 'Overwrite the output file if it already exists'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command config' -a migrate-systemd -d 'migrate-systemd'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config migrate-systemd' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config migrate-systemd' -l dry-run -d 'Show what would be done without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command config' -a validate -d 'validate'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand config validate' -s h -l help -d 'show this help message and exit'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command doctor' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command doctor' -l json -d 'Output results in JSON format'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command doctor' -l check -d 'Check specific category only (can be repeated)' -x -a 'config snapshots transfers system'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command doctor' -l fix -d 'Attempt to automatically fix safe issues'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command doctor' -s i -l interactive -d 'Prompt for confirmation before each fix'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command doctor' -s q -l quiet -d 'Only show warnings and errors'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command doctor' -l volume -d 'Only check specific volume(s)' -r -F

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -s c -l config -d 'Path to configuration file' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l volume -d 'Estimate for volume defined in config (e.g., /home)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l target -d 'Target index to estimate for (0-based, default: first target)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l prefix -d 'Snapshot prefix filter' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l ssh-sudo -d 'Use sudo for btrfs commands on remote host'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l ssh-key -d 'SSH private key file' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l ssh-auth-sock -d 'Explicit ssh-agent socket (overrides auto-discovery; useful under sudo)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l timestamp-format -d 'strftime format for parsing snapshot timestamps in direct mode (defaults to the config'\''s [global] timestamp_format, else the built-in)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l fs-checks -d 'Filesystem verification mode: '\''auto'\'' (warn and continue), '\''strict'\'' (error on failure), '\''skip'\'' (no checks). Default: auto' -x -a 'auto strict skip'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l no-fs-checks -d 'Skip btrfs subvolume verification (alias for --fs-checks=skip)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l ssh-host-key-policy -d 'SSH host-key verification for this run: '\''accept-new'\'' (trust first contact, reject a changed key) or '\''strict'\'' (known_hosts-only, reject an unknown host). Overrides the target config.' -x -a 'accept-new strict'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l skip-remote-lock -d 'Proceed even if a lock cannot be recorded on the remote target. Only safe when nothing else can prune this target during the run.'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l json -d 'Output results in JSON format'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l check-space -d 'Check if destination has sufficient space for the transfer'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command estimate' -l safety-margin -d 'Safety margin percentage for space check (default: 10%)' -x

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command install' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command install' -l timer -d 'Use preset timer interval' -x -a 'hourly daily weekly'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command install' -l oncalendar -d 'Custom OnCalendar specification (e.g., '\''*:0/15'\'' for every 15 minutes)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command install' -l user -d 'Install as user service instead of system service'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command list' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command list' -l volume -d 'Only list specific volume(s)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command list' -l json -d 'Output in JSON format'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command manpages' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command manpages' -a install -d 'install'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand manpages install' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand manpages install' -l system -d 'Install system-wide to /usr/local/share/man (requires root)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand manpages install' -l prefix -d 'Install to PREFIX/share/man/man1' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command manpages' -a path -d 'path'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand manpages path' -s h -l help -d 'show this help message and exit'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command prune' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command prune' -l dry-run -d 'Show what would be deleted without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command prune' -s y -l yes -d 'Skip the interactive confirmation prompt (for automation)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command prune' -l force -d 'Allow pruning under a degenerate policy that would keep only the latest snapshot'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command raw' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command raw' -a backfill-metadata -d 'backfill-metadata'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw backfill-metadata' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw backfill-metadata' -l dry-run -d 'Show which streams would be backfilled without writing sidecars'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw backfill-metadata' -l json -d 'Output in JSON format for scripting'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw backfill-metadata' -l ssh-sudo -d 'Use sudo for remote commands on a raw+ssh target'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command raw' -a encrypt -d 'encrypt'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw encrypt' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw encrypt' -l encrypt -d 'Encryption method to apply' -x -a 'gpg openssl_enc'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw encrypt' -l gpg-recipient -d 'GPG recipient key (required with --encrypt gpg)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw encrypt' -l gpg-keyring -d 'Use a non-default GPG keyring for encrypt and the decrypt proof' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw encrypt' -l openssl-cipher -d 'OpenSSL cipher for --encrypt openssl_enc (default aes-256-cbc)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw encrypt' -l shred -d 'Remove each plaintext file after a verified decrypt proof (plain unlink; not a secure wipe on CoW/SSD)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw encrypt' -l yes -d 'Do not prompt before removing plaintext (for scripts; with --shred)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw encrypt' -l dry-run -d 'Show which plaintext streams would be encrypted without changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw encrypt' -l json -d 'Output in JSON format for scripting'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command raw' -a list -d 'list'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw list' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw list' -l json -d 'Output in JSON format for scripting'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw list' -l ssh-sudo -d 'Use sudo for remote commands on a raw+ssh target'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command raw' -a verify -d 'verify'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw verify' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw verify' -l snapshot -d 'Verify only the named snapshot' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw verify' -l json -d 'Output in JSON format for scripting'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand raw verify' -l ssh-sudo -d 'Use sudo for remote commands on a raw+ssh target'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -s l -l list -d 'List available snapshots at backup location'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -s s -l snapshot -d 'Restore specific snapshot by name' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l before -d 'Restore snapshot closest to this time (YYYY-MM-DD [HH:MM:SS])' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -s a -l all -d 'Restore all snapshots (full mirror)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -s i -l interactive -d 'Interactively select snapshot to restore'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l dry-run -d 'Show what would be restored without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l no-incremental -d 'Force full transfers (don'\''t use incremental)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l skip-verify -d 'Skip the pre-restore integrity check for raw backups (restore even if the stored stream'\''s checksum no longer matches, and skip the extra read). Use for last-copy recovery of a partially-corrupt backup.'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l overwrite -d 'Not supported in this release: existing snapshots are left in place and a warning is printed. Remove a snapshot yourself to replace it'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l in-place -d 'Not implemented in this release: the command refuses and restores nothing. Restore to a staging directory, verify it, and swap the subvolumes yourself (README, Strategy 2)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l yes-i-know-what-i-am-doing -d 'Confirm dangerous operations like in-place restore'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l prefix -d 'Snapshot prefix filter' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l timestamp-format -d 'strftime format for parsing snapshot timestamps in direct mode (defaults to the config'\''s [global] timestamp_format, else the built-in)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l ssh-sudo -d 'Use sudo for btrfs commands on remote host'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l ssh-key -d 'SSH private key file' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l ssh-auth-sock -d 'Explicit ssh-agent socket (overrides auto-discovery; useful under sudo)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l compress -d 'Compression method for transfers' -x -a 'none bzip2 gzip lz4 lzo lzop pbzip2 pigz xz zstd'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l rate-limit -d 'Bandwidth limit (e.g., '\''10M'\'', '\''1G'\'')' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l gpg-keyring -d 'GPG keyring to decrypt an encrypted raw backup (must match the keyring the backup was encrypted for)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l openssl-cipher -d 'OpenSSL cipher fallback for a legacy raw backup whose sidecar does not record one (modern sidecars are authoritative)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l fs-checks -d 'Filesystem verification mode: '\''auto'\'' (warn and continue), '\''strict'\'' (error on failure), '\''skip'\'' (no checks). Default: auto' -x -a 'auto strict skip'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l no-fs-checks -d 'Skip btrfs subvolume verification (alias for --fs-checks=skip)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l ssh-host-key-policy -d 'SSH host-key verification for this run: '\''accept-new'\'' (trust first contact, reject a changed key) or '\''strict'\'' (known_hosts-only, reject an unknown host). Overrides the target config.' -x -a 'accept-new strict'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l skip-remote-lock -d 'Proceed even if a lock cannot be recorded on the remote target. Only safe when nothing else can prune this target during the run.'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -s c -l config -d 'Path to configuration file' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l volume -d 'Restore backups for volume defined in config (e.g., /home)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l target -d 'Target index to restore from (0-based, default: first target)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l list-volumes -d 'List volumes and their backup targets from config'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l to -d 'Destination path for config-driven restore (used with --volume)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l status -d 'Show status of locks and incomplete restores at backup location'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l unlock -d 'Unlock stuck restore session(s). Use '\''all'\'' or specify a lock ID' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l cleanup -d 'Clean up partial/incomplete snapshot restores at destination'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l progress -d 'Show progress bars (default when running in terminal)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l no-progress -d 'Disable progress bars (default when not in terminal)'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l dry-run -d 'Show what would be done without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l parallel-volumes -d 'Max concurrent volume backups (overrides config)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l parallel-targets -d 'Max concurrent target transfers per volume (overrides config)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l compress -d 'Compression method for transfers (overrides config)' -x -a 'none bzip2 gzip lz4 lzo lzop pbzip2 pigz xz zstd'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l rate-limit -d 'Bandwidth limit (e.g., '\''10M'\'', '\''1G'\'') (overrides config)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l newest-only -d 'Transfer only the snapshot just created, leaving any earlier un-transferred ones behind (the behaviour before 0.9.7)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l no-check-space -d 'Disable pre-flight space availability check'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l force -d 'Proceed with transfers even if space check fails'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l safety-margin -d 'Safety margin percentage for space check (default: 10%)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l progress -d 'Show progress bars (default when running in terminal)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l no-progress -d 'Disable progress bars (default when not in terminal)'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapper' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapper' -a backup -d 'backup'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -s s -l snapshot -d 'Backup specific snapshot number only' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -s t -l type -d 'Filter by snapshot type (can be repeated)' -x -a 'single pre post'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l min-age -d 'Minimum snapshot age before backup (e.g., 1h, 30m)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l dry-run -d 'Show what would be done without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l ssh-sudo -d 'Use sudo for btrfs commands on remote host'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l ssh-key -d 'SSH private key file' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l ssh-auth-sock -d 'Explicit ssh-agent socket (overrides auto-discovery; useful under sudo)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l ssh-host-key-policy -d 'SSH host-key verification for this run: '\''accept-new'\'' (trust first contact, reject a changed key) or '\''strict'\'' (known_hosts-only, reject an unknown host). Overrides the target config.' -x -a 'accept-new strict'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l skip-remote-lock -d 'Proceed even if a lock cannot be recorded on the remote target. Only safe when nothing else can prune this target during the run.'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l compress -d 'Compression method for transfers' -x -a 'none bzip2 gzip lz4 lzo lzop pbzip2 pigz xz zstd'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l rate-limit -d 'Bandwidth limit (e.g., '\''10M'\'', '\''1G'\'')' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l timestamp-format -d 'strftime format for the timestamp in backup names (default: %Y%m%d-%H%M%S)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l encrypt -d 'Encrypt the backup (raw:// / raw+ssh:// targets only)' -x -a 'none gpg openssl_enc'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l gpg-recipient -d 'GPG recipient key (required with --encrypt gpg)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l gpg-keyring -d 'Optional GPG keyring file' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l openssl-cipher -d 'OpenSSL cipher for --encrypt openssl_enc (default aes-256-cbc)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l progress -d 'Show progress bars (default when running in terminal)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper backup' -l no-progress -d 'Disable progress bars (default when not in terminal)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapper' -a detect -d 'detect'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper detect' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper detect' -l json -d 'Output in JSON format'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapper' -a generate-config -d 'generate-config'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper generate-config' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper generate-config' -s c -l config -d 'Snapper config name to include (can be repeated, default: all)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper generate-config' -s t -l target -d 'Default backup target path (local or ssh://user@host:/path)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper generate-config' -s o -l output -d 'Write config to file (default: stdout)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper generate-config' -s a -l append -d 'Append volume configs to existing TOML file' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper generate-config' -l type -d 'Snapshot types to include (default: single)' -x -a 'single pre post'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper generate-config' -l min-age -d 'Minimum snapshot age (default: 1h)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper generate-config' -l ssh-sudo -d 'Enable sudo for SSH targets'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper generate-config' -l json -d 'Output in JSON format instead of TOML'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapper' -a list -d 'list'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper list' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper list' -s c -l config -d 'Specific snapper config name' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper list' -s t -l type -d 'Filter by snapshot type (can be repeated)' -x -a 'single pre post'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper list' -l json -d 'Output in JSON format'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper list' -l timestamp-format -d 'strftime format for the previewed backup_name (defaults to the config'\''s [global] timestamp_format)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapper' -a restore -d 'restore'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -s s -l snapshot -d 'Restore specific snapshot number(s) (can be repeated)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -s a -l all -d 'Restore all snapper backups'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l backup-name -d 'Restore the exact raw backup by its name (from --list); can be repeated. Use this to restore an older backup when a snapper number was reused (see --list for names).' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l date -d 'Restrict the selection to backups whose date matches DATE (YYYY-MM-DD[ HH:MM:SS] prefix). Disambiguates a reused --snapshot NUM.' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l from-config -d 'Only restore from this snapper config in backup' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l dry-run -d 'Show what would be done without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l ssh-sudo -d 'Use sudo for btrfs commands on remote host'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l ssh-key -d 'SSH private key file' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l ssh-auth-sock -d 'Explicit ssh-agent socket (overrides auto-discovery; useful under sudo)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l gpg-keyring -d 'GPG keyring to decrypt an encrypted raw snapper backup (must match the keyring the backup was encrypted for)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l openssl-cipher -d 'OpenSSL cipher fallback for a legacy raw backup whose sidecar does not record one (modern sidecars are authoritative)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l ssh-host-key-policy -d 'SSH host-key verification for this run: '\''accept-new'\'' (trust first contact, reject a changed key) or '\''strict'\'' (known_hosts-only, reject an unknown host). Overrides the target config.' -x -a 'accept-new strict'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l skip-remote-lock -d 'Proceed even if a lock cannot be recorded on the remote target. Only safe when nothing else can prune this target during the run.'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -s l -l list -d 'List available snapper backups at source'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper restore' -l json -d 'Output in JSON format (for --list)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapper' -a status -d 'status'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper status' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper status' -s c -l config -d 'Specific snapper config name' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper status' -l json -d 'Output in JSON format'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand snapper status' -l timestamp-format -d 'strftime format used when the backups were written, so status counts match (defaults to the config'\''s [global] timestamp_format)' -x

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapshot' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapshot' -l dry-run -d 'Show what would be done without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapshot' -l volume -d 'Only snapshot specific volume(s)' -r -F

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command status' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command status' -s t -l transactions -d 'Show recent transaction history'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command status' -s n -l limit -d 'Number of transactions to show (default: 10)' -x

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l dry-run -d 'Show what would be done without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l volume -d 'Only transfer specific volume(s)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l compress -d 'Compression method for transfers (overrides config)' -x -a 'none bzip2 gzip lz4 lzo lzop pbzip2 pigz xz zstd'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l rate-limit -d 'Bandwidth limit (e.g., '\''10M'\'', '\''1G'\'') (overrides config)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l no-check-space -d 'Disable pre-flight space availability check'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l force -d 'Proceed with transfers even if space check fails'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l safety-margin -d 'Safety margin percentage for space check (default: 10%)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l progress -d 'Show progress bars (default when running in terminal)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l no-progress -d 'Disable progress bars (default when not in terminal)'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfers' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfers' -a cleanup -d 'cleanup'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers cleanup' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers cleanup' -l max-age -d 'Clean up transfers older than this (default: 48 hours)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers cleanup' -l force -d 'Force cleanup of active transfers'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers cleanup' -l dry-run -d 'Show what would be cleaned up without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfers' -a list -d 'list'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers list' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers list' -l json -d 'Output in JSON format'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfers' -a operations -d 'operations'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers operations' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers operations' -l all -d 'Include archived operations'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers operations' -l json -d 'Output in JSON format'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfers' -a pause -d 'pause'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers pause' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfers' -a resume -d 'resume'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers resume' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers resume' -l dry-run -d 'Show what would be done without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfers' -a show -d 'show'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers show' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_subcommand transfers show' -l json -d 'Output in JSON format'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command uninstall' -s h -l help -d 'show this help message and exit'

complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -s h -l help -d 'show this help message and exit'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l level -d 'Verification level (default: metadata for btrfs targets; stream, which checks the sealed sha256, for raw:// and raw+ssh:// targets)' -x -a 'metadata stream full'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l snapshot -d 'Verify only this specific snapshot' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l all -d 'Verify EVERY snapshot at the backup location. stream/full default to the latest only (fast); --all checks the whole chain. metadata and raw targets already check all snapshots, so --all is a no-op there.'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l temp-dir -d 'Temporary directory for full verification (must be on btrfs)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l no-cleanup -d 'Don'\''t delete restored snapshots after full verification'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l prefix -d 'Snapshot prefix filter' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l timestamp-format -d 'strftime format for parsing snapshot timestamps in direct mode (defaults to the config'\''s [global] timestamp_format, else the built-in)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l ssh-sudo -d 'Use sudo for btrfs commands on remote host'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l ssh-key -d 'SSH private key file' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l ssh-auth-sock -d 'Explicit ssh-agent socket (overrides auto-discovery; useful under sudo)' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l fs-checks -d 'Filesystem verification mode: '\''auto'\'' (warn and continue), '\''strict'\'' (error on failure), '\''skip'\'' (no checks). Default: auto' -x -a 'auto strict skip'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l no-fs-checks -d 'Skip btrfs subvolume verification (alias for --fs-checks=skip)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l ssh-host-key-policy -d 'SSH host-key verification for this run: '\''accept-new'\'' (trust first contact, reject a changed key) or '\''strict'\'' (known_hosts-only, reject an unknown host). Overrides the target config.' -x -a 'accept-new strict'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l skip-remote-lock -d 'Proceed even if a lock cannot be recorded on the remote target. Only safe when nothing else can prune this target during the run.'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -l json -d 'Output results in JSON format'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command verify' -s q -l quiet -d 'Suppress progress output'

