#compdef btrfs-backup-ng
# Generated from the argument parser; edit the parser, not this file.

_btrfs_backup_ng() {
    local curcontext="$curcontext" state line
    typeset -A opt_args

    _arguments -C \
        '-h[show this help message and exit]' \
        '--help[show this help message and exit]' \
        '-v[Enable verbose output]' \
        '--verbose[Enable verbose output]' \
        '-q[Suppress non-essential output]' \
        '--quiet[Suppress non-essential output]' \
        '--debug[Enable debug output]' \
        '-V[Show version and exit]' \
        '--version[Show version and exit]' \
        '-c[Path to configuration file]' \
        '--config[Path to configuration file]' \
        '1: :->command' \
        '*:: :->args' && return 0

    case $state in
        command)
            local -a cmds
            cmds=(
                'completions'
                'config'
                'doctor'
                'estimate'
                'install'
                'list'
                'manpages'
                'prune'
                'raw'
                'restore'
                'run'
                'snapper'
                'snapshot'
                'status'
                'transfer'
                'transfers'
                'uninstall'
                'verify'
            )
            _describe 'command' cmds
            ;;
        args)
            case $words[1] in
                completions)
                    if (( CURRENT == 2 )); then
                        local -a subs
                        subs=(
                            'install'
                            'path'
                        )
                        _describe 'subcommand' subs
                    else
                        case $words[2] in
                            install)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--shell[Shell to install completions for]:value:(bash zsh fish)' \
                                    '--system[Install system-wide (requires root)]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            path)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                        esac
                    fi
                    ;;
                config)
                    if (( CURRENT == 2 )); then
                        local -a subs
                        subs=(
                            'detect'
                            'import'
                            'init'
                            'migrate-systemd'
                            'validate'
                        )
                        _describe 'subcommand' subs
                    else
                        case $words[2] in
                            detect)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--json[Output in JSON format for scripting]' \
                                    '-w[Launch interactive wizard with detected volumes]' \
                                    '--wizard[Launch interactive wizard with detected volumes]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            import)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '-o[Output file (default: stdout)]:file:_files' \
                                    '--output[Output file (default: stdout)]:file:_files' \
                                    '--force[Overwrite the output file if it already exists]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            init)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '-i[Run interactive configuration wizard]' \
                                    '--interactive[Run interactive configuration wizard]' \
                                    '-o[Output file (default: stdout)]:file:_files' \
                                    '--output[Output file (default: stdout)]:file:_files' \
                                    '--force[Overwrite the output file if it already exists]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            migrate-systemd)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--dry-run[Show what would be done without making changes]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            validate)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                        esac
                    fi
                    ;;
                doctor)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '--json[Output results in JSON format]' \
                        '--check[Check specific category only (can be repeated)]:value:(config snapshots transfers system)' \
                        '--fix[Attempt to automatically fix safe issues]' \
                        '-i[Prompt for confirmation before each fix]' \
                        '--interactive[Prompt for confirmation before each fix]' \
                        '-q[Only show warnings and errors]' \
                        '--quiet[Only show warnings and errors]' \
                        '--volume[Only check specific volume(s)]:file:_files' \
                        '*:: :->rest'
                    ;;
                estimate)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '-c[Path to configuration file]:file:_files' \
                        '--config[Path to configuration file]:file:_files' \
                        '--volume[Estimate for volume defined in config (e.g., /home)]:file:_files' \
                        '--target[Target index to estimate for (0-based, default: first target)]:index:' \
                        '--prefix[Snapshot prefix filter]:prefix:' \
                        '--ssh-sudo[Use sudo for btrfs commands on remote host]' \
                        '--ssh-key[SSH private key file]:file:_files' \
                        '--ssh-auth-sock[Explicit ssh-agent socket (overrides auto-discovery; useful under sudo)]:file:_files' \
                        '--timestamp-format[strftime format for parsing snapshot timestamps in direct mode (defaults to the config'\''s [global] timestamp_format, else the built-in)]:fmt:' \
                        '--fs-checks[Filesystem verification mode: '\''auto'\'' (warn and continue), '\''strict'\'' (error on failure), '\''skip'\'' (no checks). Default: auto]:value:(auto strict skip)' \
                        '--no-fs-checks[Skip btrfs subvolume verification (alias for --fs-checks=skip)]:fs_checks:' \
                        '--ssh-host-key-policy[SSH host-key verification for this run: '\''accept-new'\'' (trust first contact, reject a changed key) or '\''strict'\'' (known_hosts-only, reject an unknown host). Overrides the target config.]:value:(accept-new strict)' \
                        '--skip-remote-lock[Proceed even if a lock cannot be recorded on the remote target. Only safe when nothing else can prune this target during the run.]' \
                        '--json[Output results in JSON format]' \
                        '--check-space[Check if destination has sufficient space for the transfer]' \
                        '--safety-margin[Safety margin percentage for space check (default: 10%)]:percent:' \
                        '*:: :->rest'
                    ;;
                install)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '--timer[Use preset timer interval]:value:(hourly daily weekly)' \
                        '--oncalendar[Custom OnCalendar specification (e.g., '\''*:0/15'\'' for every 15 minutes)]:spec:' \
                        '--user[Install as user service instead of system service]' \
                        '*:: :->rest'
                    ;;
                list)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '--volume[Only list specific volume(s)]:file:_files' \
                        '--json[Output in JSON format]' \
                        '*:: :->rest'
                    ;;
                manpages)
                    if (( CURRENT == 2 )); then
                        local -a subs
                        subs=(
                            'install'
                            'path'
                        )
                        _describe 'subcommand' subs
                    else
                        case $words[2] in
                            install)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--system[Install system-wide to /usr/local/share/man (requires root)]' \
                                    '--prefix[Install to PREFIX/share/man/man1]:file:_files' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            path)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                        esac
                    fi
                    ;;
                prune)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '--dry-run[Show what would be deleted without making changes]' \
                        '-y[Skip the interactive confirmation prompt (for automation)]' \
                        '--yes[Skip the interactive confirmation prompt (for automation)]' \
                        '--force[Allow pruning under a degenerate policy that would keep only the latest snapshot]' \
                        '*:: :->rest'
                    ;;
                raw)
                    if (( CURRENT == 2 )); then
                        local -a subs
                        subs=(
                            'backfill-metadata'
                            'encrypt'
                            'list'
                            'verify'
                        )
                        _describe 'subcommand' subs
                    else
                        case $words[2] in
                            backfill-metadata)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--dry-run[Show which streams would be backfilled without writing sidecars]' \
                                    '--json[Output in JSON format for scripting]' \
                                    '--ssh-sudo[Use sudo for remote commands on a raw+ssh target]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            encrypt)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--encrypt[Encryption method to apply]:value:(gpg openssl_enc)' \
                                    '--gpg-recipient[GPG recipient key (required with --encrypt gpg)]:file:_files' \
                                    '--gpg-keyring[Use a non-default GPG keyring for encrypt and the decrypt proof]:file:_files' \
                                    '--openssl-cipher[OpenSSL cipher for --encrypt openssl_enc (default aes-256-cbc)]:cipher:' \
                                    '--shred[Remove each plaintext file after a verified decrypt proof (plain unlink; not a secure wipe on CoW/SSD)]' \
                                    '--yes[Do not prompt before removing plaintext (for scripts; with --shred)]' \
                                    '--dry-run[Show which plaintext streams would be encrypted without changes]' \
                                    '--json[Output in JSON format for scripting]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            list)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--json[Output in JSON format for scripting]' \
                                    '--ssh-sudo[Use sudo for remote commands on a raw+ssh target]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            verify)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--snapshot[Verify only the named snapshot]:name:' \
                                    '--json[Output in JSON format for scripting]' \
                                    '--ssh-sudo[Use sudo for remote commands on a raw+ssh target]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                        esac
                    fi
                    ;;
                restore)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '-l[List available snapshots at backup location]' \
                        '--list[List available snapshots at backup location]' \
                        '-s[Restore specific snapshot by name]:name:' \
                        '--snapshot[Restore specific snapshot by name]:name:' \
                        '--before[Restore snapshot closest to this time (YYYY-MM-DD [HH:MM:SS])]:datetime:' \
                        '-a[Restore all snapshots (full mirror)]' \
                        '--all[Restore all snapshots (full mirror)]' \
                        '-i[Interactively select snapshot to restore]' \
                        '--interactive[Interactively select snapshot to restore]' \
                        '--dry-run[Show what would be restored without making changes]' \
                        '--no-incremental[Force full transfers (don'\''t use incremental)]' \
                        '--skip-verify[Skip the pre-restore integrity check for raw backups (restore even if the stored stream'\''s checksum no longer matches, and skip the extra read). Use for last-copy recovery of a partially-corrupt backup.]' \
                        '--overwrite[Not supported in this release: existing snapshots are left in place and a warning is printed. Remove a snapshot yourself to replace it]' \
                        '--in-place[Not implemented in this release: the command refuses and restores nothing. Restore to a staging directory, verify it, and swap the subvolumes yourself (README, Strategy 2)]' \
                        '--yes-i-know-what-i-am-doing[Confirm dangerous operations like in-place restore]' \
                        '--prefix[Snapshot prefix filter]:prefix:' \
                        '--timestamp-format[strftime format for parsing snapshot timestamps in direct mode (defaults to the config'\''s [global] timestamp_format, else the built-in)]:fmt:' \
                        '--ssh-sudo[Use sudo for btrfs commands on remote host]' \
                        '--ssh-key[SSH private key file]:file:_files' \
                        '--ssh-auth-sock[Explicit ssh-agent socket (overrides auto-discovery; useful under sudo)]:file:_files' \
                        '--compress[Compression method for transfers]:value:(none bzip2 gzip lz4 lzo lzop pbzip2 pigz xz zstd)' \
                        '--rate-limit[Bandwidth limit (e.g., '\''10M'\'', '\''1G'\'')]:rate:' \
                        '--gpg-keyring[GPG keyring to decrypt an encrypted raw backup (must match the keyring the backup was encrypted for)]:file:_files' \
                        '--openssl-cipher[OpenSSL cipher fallback for a legacy raw backup whose sidecar does not record one (modern sidecars are authoritative)]:cipher:' \
                        '--fs-checks[Filesystem verification mode: '\''auto'\'' (warn and continue), '\''strict'\'' (error on failure), '\''skip'\'' (no checks). Default: auto]:value:(auto strict skip)' \
                        '--no-fs-checks[Skip btrfs subvolume verification (alias for --fs-checks=skip)]:fs_checks:' \
                        '--ssh-host-key-policy[SSH host-key verification for this run: '\''accept-new'\'' (trust first contact, reject a changed key) or '\''strict'\'' (known_hosts-only, reject an unknown host). Overrides the target config.]:value:(accept-new strict)' \
                        '--skip-remote-lock[Proceed even if a lock cannot be recorded on the remote target. Only safe when nothing else can prune this target during the run.]' \
                        '-c[Path to configuration file]:file:_files' \
                        '--config[Path to configuration file]:file:_files' \
                        '--volume[Restore backups for volume defined in config (e.g., /home)]:file:_files' \
                        '--target[Target index to restore from (0-based, default: first target)]:index:' \
                        '--list-volumes[List volumes and their backup targets from config]' \
                        '--to[Destination path for config-driven restore (used with --volume)]:file:_files' \
                        '--status[Show status of locks and incomplete restores at backup location]' \
                        '--unlock[Unlock stuck restore session(s). Use '\''all'\'' or specify a lock ID]:lock_id:' \
                        '--cleanup[Clean up partial/incomplete snapshot restores at destination]' \
                        '--progress[Show progress bars (default when running in terminal)]' \
                        '--no-progress[Disable progress bars (default when not in terminal)]' \
                        '*:: :->rest'
                    ;;
                run)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '--dry-run[Show what would be done without making changes]' \
                        '--parallel-volumes[Max concurrent volume backups (overrides config)]:n:' \
                        '--parallel-targets[Max concurrent target transfers per volume (overrides config)]:n:' \
                        '--compress[Compression method for transfers (overrides config)]:value:(none bzip2 gzip lz4 lzo lzop pbzip2 pigz xz zstd)' \
                        '--rate-limit[Bandwidth limit (e.g., '\''10M'\'', '\''1G'\'') (overrides config)]:rate:' \
                        '--newest-only[Transfer only the snapshot just created, leaving any earlier un-transferred ones behind (the behaviour before 0.9.7)]' \
                        '--no-check-space[Disable pre-flight space availability check]' \
                        '--force[Proceed with transfers even if space check fails]' \
                        '--safety-margin[Safety margin percentage for space check (default: 10%)]:percent:' \
                        '--progress[Show progress bars (default when running in terminal)]' \
                        '--no-progress[Disable progress bars (default when not in terminal)]' \
                        '*:: :->rest'
                    ;;
                snapper)
                    if (( CURRENT == 2 )); then
                        local -a subs
                        subs=(
                            'backup'
                            'detect'
                            'generate-config'
                            'list'
                            'restore'
                            'status'
                        )
                        _describe 'subcommand' subs
                    else
                        case $words[2] in
                            backup)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '-s[Backup specific snapshot number only]:num:' \
                                    '--snapshot[Backup specific snapshot number only]:num:' \
                                    '-t[Filter by snapshot type (can be repeated)]:value:(single pre post)' \
                                    '--type[Filter by snapshot type (can be repeated)]:value:(single pre post)' \
                                    '--min-age[Minimum snapshot age before backup (e.g., 1h, 30m)]:duration:' \
                                    '--dry-run[Show what would be done without making changes]' \
                                    '--ssh-sudo[Use sudo for btrfs commands on remote host]' \
                                    '--ssh-key[SSH private key file]:file:_files' \
                                    '--ssh-auth-sock[Explicit ssh-agent socket (overrides auto-discovery; useful under sudo)]:file:_files' \
                                    '--ssh-host-key-policy[SSH host-key verification for this run: '\''accept-new'\'' (trust first contact, reject a changed key) or '\''strict'\'' (known_hosts-only, reject an unknown host). Overrides the target config.]:value:(accept-new strict)' \
                                    '--skip-remote-lock[Proceed even if a lock cannot be recorded on the remote target. Only safe when nothing else can prune this target during the run.]' \
                                    '--compress[Compression method for transfers]:value:(none bzip2 gzip lz4 lzo lzop pbzip2 pigz xz zstd)' \
                                    '--rate-limit[Bandwidth limit (e.g., '\''10M'\'', '\''1G'\'')]:rate:' \
                                    '--timestamp-format[strftime format for the timestamp in backup names (default: %Y%m%d-%H%M%S)]:fmt:' \
                                    '--encrypt[Encrypt the backup (raw:// / raw+ssh:// targets only)]:value:(none gpg openssl_enc)' \
                                    '--gpg-recipient[GPG recipient key (required with --encrypt gpg)]:file:_files' \
                                    '--gpg-keyring[Optional GPG keyring file]:file:_files' \
                                    '--openssl-cipher[OpenSSL cipher for --encrypt openssl_enc (default aes-256-cbc)]:cipher:' \
                                    '--progress[Show progress bars (default when running in terminal)]' \
                                    '--no-progress[Disable progress bars (default when not in terminal)]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            detect)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--json[Output in JSON format]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            generate-config)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '-c[Snapper config name to include (can be repeated, default: all)]:name:' \
                                    '--config[Snapper config name to include (can be repeated, default: all)]:name:' \
                                    '-t[Default backup target path (local or ssh://user@host:/path)]:file:_files' \
                                    '--target[Default backup target path (local or ssh://user@host:/path)]:file:_files' \
                                    '-o[Write config to file (default: stdout)]:file:_files' \
                                    '--output[Write config to file (default: stdout)]:file:_files' \
                                    '-a[Append volume configs to existing TOML file]:file:_files' \
                                    '--append[Append volume configs to existing TOML file]:file:_files' \
                                    '--type[Snapshot types to include (default: single)]:value:(single pre post)' \
                                    '--min-age[Minimum snapshot age (default: 1h)]:duration:' \
                                    '--ssh-sudo[Enable sudo for SSH targets]' \
                                    '--json[Output in JSON format instead of TOML]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            list)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '-c[Specific snapper config name]:name:' \
                                    '--config[Specific snapper config name]:name:' \
                                    '-t[Filter by snapshot type (can be repeated)]:value:(single pre post)' \
                                    '--type[Filter by snapshot type (can be repeated)]:value:(single pre post)' \
                                    '--json[Output in JSON format]' \
                                    '--timestamp-format[strftime format for the previewed backup_name (defaults to the config'\''s [global] timestamp_format)]:fmt:' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            restore)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '-s[Restore specific snapshot number(s) (can be repeated)]:num:' \
                                    '--snapshot[Restore specific snapshot number(s) (can be repeated)]:num:' \
                                    '-a[Restore all snapper backups]' \
                                    '--all[Restore all snapper backups]' \
                                    '--backup-name[Restore the exact raw backup by its name (from --list); can be repeated. Use this to restore an older backup when a snapper number was reused (see --list for names).]:name:' \
                                    '--date[Restrict the selection to backups whose date matches DATE (YYYY-MM-DD[ HH:MM:SS] prefix). Disambiguates a reused --snapshot NUM.]:date:' \
                                    '--from-config[Only restore from this snapper config in backup]:name:' \
                                    '--dry-run[Show what would be done without making changes]' \
                                    '--ssh-sudo[Use sudo for btrfs commands on remote host]' \
                                    '--ssh-key[SSH private key file]:file:_files' \
                                    '--ssh-auth-sock[Explicit ssh-agent socket (overrides auto-discovery; useful under sudo)]:file:_files' \
                                    '--gpg-keyring[GPG keyring to decrypt an encrypted raw snapper backup (must match the keyring the backup was encrypted for)]:file:_files' \
                                    '--openssl-cipher[OpenSSL cipher fallback for a legacy raw backup whose sidecar does not record one (modern sidecars are authoritative)]:cipher:' \
                                    '--ssh-host-key-policy[SSH host-key verification for this run: '\''accept-new'\'' (trust first contact, reject a changed key) or '\''strict'\'' (known_hosts-only, reject an unknown host). Overrides the target config.]:value:(accept-new strict)' \
                                    '--skip-remote-lock[Proceed even if a lock cannot be recorded on the remote target. Only safe when nothing else can prune this target during the run.]' \
                                    '-l[List available snapper backups at source]' \
                                    '--list[List available snapper backups at source]' \
                                    '--json[Output in JSON format (for --list)]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            status)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '-c[Specific snapper config name]:name:' \
                                    '--config[Specific snapper config name]:name:' \
                                    '--json[Output in JSON format]' \
                                    '--timestamp-format[strftime format used when the backups were written, so status counts match (defaults to the config'\''s [global] timestamp_format)]:fmt:' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                        esac
                    fi
                    ;;
                snapshot)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '--dry-run[Show what would be done without making changes]' \
                        '--volume[Only snapshot specific volume(s)]:file:_files' \
                        '*:: :->rest'
                    ;;
                status)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '-t[Show recent transaction history]' \
                        '--transactions[Show recent transaction history]' \
                        '-n[Number of transactions to show (default: 10)]:n:' \
                        '--limit[Number of transactions to show (default: 10)]:n:' \
                        '*:: :->rest'
                    ;;
                transfer)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '--dry-run[Show what would be done without making changes]' \
                        '--volume[Only transfer specific volume(s)]:file:_files' \
                        '--compress[Compression method for transfers (overrides config)]:value:(none bzip2 gzip lz4 lzo lzop pbzip2 pigz xz zstd)' \
                        '--rate-limit[Bandwidth limit (e.g., '\''10M'\'', '\''1G'\'') (overrides config)]:rate:' \
                        '--no-check-space[Disable pre-flight space availability check]' \
                        '--force[Proceed with transfers even if space check fails]' \
                        '--safety-margin[Safety margin percentage for space check (default: 10%)]:percent:' \
                        '--progress[Show progress bars (default when running in terminal)]' \
                        '--no-progress[Disable progress bars (default when not in terminal)]' \
                        '*:: :->rest'
                    ;;
                transfers)
                    if (( CURRENT == 2 )); then
                        local -a subs
                        subs=(
                            'cleanup'
                            'list'
                            'operations'
                            'pause'
                            'resume'
                            'show'
                        )
                        _describe 'subcommand' subs
                    else
                        case $words[2] in
                            cleanup)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--max-age[Clean up transfers older than this (default: 48 hours)]:hours:' \
                                    '--force[Force cleanup of active transfers]' \
                                    '--dry-run[Show what would be cleaned up without making changes]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            list)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--json[Output in JSON format]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            operations)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--all[Include archived operations]' \
                                    '--json[Output in JSON format]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            pause)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            resume)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--dry-run[Show what would be done without making changes]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                            show)
                                _arguments \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '--json[Output in JSON format]' \
                                    '-h[show this help message and exit]' \
                                    '--help[show this help message and exit]' \
                                    '*:: :->rest'
                                ;;
                        esac
                    fi
                    ;;
                uninstall)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '*:: :->rest'
                    ;;
                verify)
                    _arguments \
                        '-h[show this help message and exit]' \
                        '--help[show this help message and exit]' \
                        '--level[Verification level (default: metadata for btrfs targets; stream, which checks the sealed sha256, for raw:// and raw+ssh:// targets)]:value:(metadata stream full)' \
                        '--snapshot[Verify only this specific snapshot]:name:' \
                        '--all[Verify EVERY snapshot at the backup location. stream/full default to the latest only (fast); --all checks the whole chain. metadata and raw targets already check all snapshots, so --all is a no-op there.]' \
                        '--temp-dir[Temporary directory for full verification (must be on btrfs)]:file:_files' \
                        '--no-cleanup[Don'\''t delete restored snapshots after full verification]' \
                        '--prefix[Snapshot prefix filter]:prefix:' \
                        '--timestamp-format[strftime format for parsing snapshot timestamps in direct mode (defaults to the config'\''s [global] timestamp_format, else the built-in)]:fmt:' \
                        '--ssh-sudo[Use sudo for btrfs commands on remote host]' \
                        '--ssh-key[SSH private key file]:file:_files' \
                        '--ssh-auth-sock[Explicit ssh-agent socket (overrides auto-discovery; useful under sudo)]:file:_files' \
                        '--fs-checks[Filesystem verification mode: '\''auto'\'' (warn and continue), '\''strict'\'' (error on failure), '\''skip'\'' (no checks). Default: auto]:value:(auto strict skip)' \
                        '--no-fs-checks[Skip btrfs subvolume verification (alias for --fs-checks=skip)]:fs_checks:' \
                        '--ssh-host-key-policy[SSH host-key verification for this run: '\''accept-new'\'' (trust first contact, reject a changed key) or '\''strict'\'' (known_hosts-only, reject an unknown host). Overrides the target config.]:value:(accept-new strict)' \
                        '--skip-remote-lock[Proceed even if a lock cannot be recorded on the remote target. Only safe when nothing else can prune this target during the run.]' \
                        '--json[Output results in JSON format]' \
                        '-q[Suppress progress output]' \
                        '--quiet[Suppress progress output]' \
                        '*:: :->rest'
                    ;;
            esac
            ;;
    esac
}

_btrfs_backup_ng "$@"
