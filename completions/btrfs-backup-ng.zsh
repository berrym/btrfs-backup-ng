#compdef btrfs-backup-ng
# Zsh completion for btrfs-backup-ng
# Install: Copy to a directory in $fpath (e.g., /usr/share/zsh/site-functions/)
#          and rename to _btrfs-backup-ng

_btrfs-backup-ng() {
    local curcontext="$curcontext" state line
    typeset -A opt_args

    local -a commands
    commands=(
        'run:Execute all configured backup jobs'
        'snapshot:Create snapshots only'
        'transfer:Transfer existing snapshots to targets'
        'prune:Apply retention policies'
        'list:Show snapshots and backups'
        'status:Show job status and statistics'
        'config:Configuration management'
        'install:Install systemd timer/service'
        'uninstall:Remove systemd timer/service'
        'restore:Restore snapshots from backup location'
        'verify:Verify backup integrity'
        'estimate:Estimate backup transfer sizes'
        'doctor:Diagnose backup system health and fix issues'
        'completions:Install shell completion scripts'
        'manpages:Install man pages'
        'transfers:Manage chunked and resumable transfers (experimental)'
        'snapper:Manage snapper-managed snapshots'
    )

    local -a global_opts
    global_opts=(
        '(-h --help)'{-h,--help}'[Show help message]'
        '(-v --verbose)'{-v,--verbose}'[Enable verbose output]'
        '(-q --quiet)'{-q,--quiet}'[Suppress non-essential output]'
        '--debug[Enable debug output]'
        '(-V --version)'{-V,--version}'[Show version and exit]'
        '(-c --config)'{-c,--config}'[Path to configuration file]:config file:_files'
    )

    local -a compress_methods
    compress_methods=(none zstd gzip lz4 pigz lzop)

    local -a timer_presets
    timer_presets=(hourly daily weekly)

    local -a fs_checks_modes
    fs_checks_modes=(auto strict skip)

    _arguments -C \
        $global_opts \
        '1: :->command' \
        '*:: :->args'

    case $state in
        command)
            _describe -t commands 'btrfs-backup-ng command' commands
            ;;
        args)
            case $line[1] in
                run)
                    _arguments \
                        '--dry-run[Show what would be done without making changes]' \
                        '--parallel-volumes[Max concurrent volume backups]:count:' \
                        '--parallel-targets[Max concurrent target transfers per volume]:count:' \
                        '--compress[Compression method for transfers]:method:(${compress_methods})' \
                        '--rate-limit[Bandwidth limit]:rate:' \
                        '(--progress --no-progress)'--progress'[Show progress bars]' \
                        '(--progress --no-progress)'--no-progress'[Disable progress bars]' \
                        '--check-space[Check destination space before transfer]' \
                        '--no-check-space[Skip destination space check]' \
                        '--force[Proceed despite insufficient space]' \
                        '--fs-checks[Filesystem validation mode]:mode:(${fs_checks_modes})'
                    ;;
                snapshot)
                    _arguments \
                        '--dry-run[Show what would be done without making changes]' \
                        '*--volume[Only snapshot specific volume]:volume path:_directories'
                    ;;
                transfer)
                    _arguments \
                        '--dry-run[Show what would be done without making changes]' \
                        '*--volume[Only transfer specific volume]:volume path:_directories' \
                        '--compress[Compression method]:method:(${compress_methods})' \
                        '--rate-limit[Bandwidth limit]:rate:' \
                        '(--progress --no-progress)'--progress'[Show progress bars]' \
                        '(--progress --no-progress)'--no-progress'[Disable progress bars]'
                    ;;
                prune)
                    _arguments \
                        '--dry-run[Show what would be deleted without making changes]'
                    ;;
                list)
                    _arguments \
                        '*--volume[Only list specific volume]:volume path:_directories' \
                        '--json[Output in JSON format]'
                    ;;
                status)
                    _arguments \
                        '(-t --transactions)'{-t,--transactions}'[Show recent transaction history]' \
                        '(-n --limit)'{-n,--limit}'[Number of transactions to show]:count:'
                    ;;
                config)
                    local -a config_commands
                    config_commands=(
                        'validate:Validate configuration file'
                        'init:Generate example configuration'
                        'import:Import btrbk configuration'
                        'detect:Detect btrfs subvolumes'
                    )
                    _arguments \
                        '1: :->config_cmd' \
                        '*:: :->config_args'
                    case $state in
                        config_cmd)
                            _describe -t commands 'config subcommand' config_commands
                            ;;
                        config_args)
                            case $line[1] in
                                validate)
                                    _arguments
                                    ;;
                                init)
                                    _arguments \
                                        '(-i --interactive)'{-i,--interactive}'[Run interactive configuration wizard]' \
                                        '(-o --output)'{-o,--output}'[Output file]:file:_files'
                                    ;;
                                import)
                                    _arguments \
                                        '(-o --output)'{-o,--output}'[Output file]:file:_files' \
                                        '1:btrbk config file:_files -g "*.conf"'
                                    ;;
                                detect)
                                    _arguments \
                                        '--json[Output results in JSON format]' \
                                        '1:path to scan:_directories'
                                    ;;
                            esac
                            ;;
                    esac
                    ;;
                install)
                    _arguments \
                        '--timer[Use preset timer interval]:preset:(${timer_presets})' \
                        '--oncalendar[Custom OnCalendar specification]:spec:' \
                        '--user[Install as user service instead of system service]'
                    ;;
                uninstall)
                    _arguments
                    ;;
                restore)
                    _arguments \
                        '(-l --list)'{-l,--list}'[List available snapshots at backup location]' \
                        '(-s --snapshot)'{-s,--snapshot}'[Restore specific snapshot by name]:snapshot name:' \
                        '--before[Restore snapshot closest to this time]:datetime:' \
                        '(-a --all)'{-a,--all}'[Restore all snapshots (full mirror)]' \
                        '(-i --interactive)'{-i,--interactive}'[Interactively select snapshot to restore]' \
                        '--dry-run[Show what would be restored without making changes]' \
                        '--no-incremental[Force full transfers]' \
                        '--overwrite[Overwrite existing snapshots instead of skipping]' \
                        '--in-place[Restore to original location (DANGEROUS)]' \
                        '--yes-i-know-what-i-am-doing[Confirm dangerous operations]' \
                        '--prefix[Snapshot prefix filter]:prefix:' \
                        '--ssh-sudo[Use sudo for btrfs commands on remote host]' \
                        '--ssh-key[SSH private key file]:key file:_files' \
                        '--compress[Compression method]:method:(${compress_methods})' \
                        '--rate-limit[Bandwidth limit]:rate:' \
                        '--fs-checks[Filesystem validation mode]:mode:(${fs_checks_modes})' \
                        '--status[Show status of locks and incomplete restores]' \
                        '--unlock[Unlock stuck restore session]:lock id or all:' \
                        '--cleanup[Clean up partial/incomplete snapshot restores]' \
                        '(--progress --no-progress)'--progress'[Show progress bars]' \
                        '(--progress --no-progress)'--no-progress'[Disable progress bars]' \
                        '(-c --config)'{-c,--config}'[Path to configuration file]:config file:_files' \
                        '--volume[Restore backups for volume defined in config]:volume path:_directories' \
                        '--target[Target index to restore from (0-based)]:index:' \
                        '--list-volumes[List volumes and their backup targets from config]' \
                        '--to[Destination path for config-driven restore]:destination:_directories' \
                        '1:source (backup location):_files -/' \
                        '2:destination (local path):_directories'
                    ;;
                verify)
                    local -a verify_levels
                    verify_levels=(metadata stream full)
                    _arguments \
                        '--level[Verification level]:level:(${verify_levels})' \
                        '--snapshot[Verify specific snapshot only]:snapshot name:' \
                        '--temp-dir[Temporary directory for full verification]:directory:_directories' \
                        '--no-cleanup[Do not delete restored snapshots after full verification]' \
                        '--prefix[Snapshot prefix filter]:prefix:' \
                        '--ssh-sudo[Use sudo for btrfs commands on remote host]' \
                        '--ssh-key[SSH private key file]:key file:_files' \
                        '--fs-checks[Filesystem validation mode]:mode:(${fs_checks_modes})' \
                        '--json[Output results in JSON format]' \
                        '(-q --quiet)'{-q,--quiet}'[Suppress progress output]' \
                        '1:backup location:_files -/'
                    ;;
                estimate)
                    _arguments \
                        '(-c --config)'{-c,--config}'[Path to configuration file]:config file:_files' \
                        '--volume[Estimate for volume defined in config]:volume path:_directories' \
                        '--target[Target index to estimate for (0-based)]:index:' \
                        '--prefix[Snapshot prefix filter]:prefix:' \
                        '--ssh-sudo[Use sudo for btrfs commands on remote host]' \
                        '--ssh-key[SSH private key file]:key file:_files' \
                        '--fs-checks[Filesystem validation mode]:mode:(${fs_checks_modes})' \
                        '--check-space[Check destination space availability]' \
                        '--safety-margin[Safety margin percentage]:percent:' \
                        '--json[Output results in JSON format]' \
                        '1:source (snapshot location):_files -/' \
                        '2:destination (backup location):_files -/'
                    ;;
                doctor)
                    local -a doctor_categories
                    doctor_categories=(config snapshots transfers system)
                    _arguments \
                        '--json[Output results in JSON format]' \
                        '--check[Check specific category only]:category:(${doctor_categories})' \
                        '--fix[Auto-fix safe issues]' \
                        '--interactive[Confirm each fix before applying]' \
                        '(-q --quiet)'{-q,--quiet}'[Only show problems]' \
                        '--volume[Check specific volume only]:volume path:_directories'
                    ;;
                completions)
                    local -a completions_commands
                    completions_commands=(
                        'install:Install completions for your shell'
                        'path:Show path to completion scripts'
                    )
                    _arguments \
                        '1: :->completions_cmd' \
                        '*:: :->completions_args'

                    case $state in
                        completions_cmd)
                            _describe -t commands 'completions command' completions_commands
                            ;;
                        completions_args)
                            case $line[1] in
                                install)
                                    _arguments \
                                        '--shell[Shell to install completions for]:shell:(bash zsh fish)' \
                                        '--system[Install system-wide (requires root)]'
                                    ;;
                            esac
                            ;;
                    esac
                    ;;
                manpages)
                    local -a manpages_commands
                    manpages_commands=(
                        'install:Install man pages'
                        'path:Show path to man page files'
                    )
                    _arguments \
                        '1: :->manpages_cmd' \
                        '*:: :->manpages_args'

                    case $state in
                        manpages_cmd)
                            _describe -t commands 'manpages command' manpages_commands
                            ;;
                        manpages_args)
                            case $line[1] in
                                install)
                                    _arguments \
                                        '--system[Install system-wide to /usr/local/share/man (requires root)]' \
                                        '--prefix[Install to PREFIX/share/man/man1]:prefix:_directories'
                                    ;;
                            esac
                            ;;
                    esac
                    ;;
                transfers)
                    local -a transfers_commands
                    transfers_commands=(
                        'list:List incomplete transfers'
                        'show:Show details of a transfer'
                        'resume:Resume a failed or paused transfer'
                        'pause:Pause an active transfer'
                        'cleanup:Clean up old or completed transfers'
                        'operations:List backup operations'
                    )
                    _arguments \
                        '1: :->transfers_cmd' \
                        '*:: :->transfers_args'

                    case $state in
                        transfers_cmd)
                            _describe -t commands 'transfers command' transfers_commands
                            ;;
                        transfers_args)
                            case $line[1] in
                                list)
                                    _arguments \
                                        '--json[Output in JSON format]'
                                    ;;
                                show)
                                    _arguments \
                                        '--json[Output in JSON format]' \
                                        '1:transfer ID:'
                                    ;;
                                resume)
                                    _arguments \
                                        '--dry-run[Show what would be done]' \
                                        '1:transfer ID:'
                                    ;;
                                pause)
                                    _arguments \
                                        '1:transfer ID:'
                                    ;;
                                cleanup)
                                    _arguments \
                                        '--force[Force cleanup even for active transfers]' \
                                        '--all[Clean up all transfers]' \
                                        '--age[Clean up transfers older than hours]:hours:' \
                                        '1:transfer ID:'
                                    ;;
                                operations)
                                    _arguments
                                    ;;
                            esac
                            ;;
                    esac
                    ;;
                snapper)
                    local -a snapper_commands
                    snapper_commands=(
                        'detect:Detect snapper configurations on the system'
                        'list:List snapper configs and snapshots'
                        'backup:Backup snapper snapshots to target'
                        'status:Show backup status for snapper configs'
                        'restore:Restore snapper backups to local snapper format'
                        'generate-config:Generate TOML config for snapper volumes'
                    )
                    local -a snapper_types
                    snapper_types=(single pre post)

                    _arguments \
                        '1: :->snapper_cmd' \
                        '*:: :->snapper_args'

                    case $state in
                        snapper_cmd)
                            _describe -t commands 'snapper command' snapper_commands
                            ;;
                        snapper_args)
                            case $line[1] in
                                detect)
                                    _arguments \
                                        '--json[Output in JSON format]'
                                    ;;
                                list)
                                    _arguments \
                                        '--config[Snapper config name]:config name:' \
                                        '--type[Snapshot type filter]:type:(${snapper_types})' \
                                        '--json[Output in JSON format]'
                                    ;;
                                backup)
                                    _arguments \
                                        '--snapshot[Backup specific snapshot number]:snapshot number:' \
                                        '--dry-run[Show what would be done]' \
                                        '--ssh-sudo[Use sudo on remote host]' \
                                        '--ssh-key[SSH private key file]:key file:_files' \
                                        '--compress[Compression method]:method:(${compress_methods})' \
                                        '(--progress --no-progress)'--progress'[Show progress bars]' \
                                        '(--progress --no-progress)'--no-progress'[Disable progress bars]' \
                                        '1:snapper config name:' \
                                        '2:target path:_files -/'
                                    ;;
                                status)
                                    _arguments \
                                        '--json[Output in JSON format]'
                                    ;;
                                restore)
                                    _arguments \
                                        '--snapshot[Restore specific snapshot number]:snapshot number:' \
                                        '--dry-run[Show what would be done]' \
                                        '--ssh-sudo[Use sudo on remote host]' \
                                        '--ssh-key[SSH private key file]:key file:_files' \
                                        '1:source (backup location):_files -/' \
                                        '2:snapper config name:'
                                    ;;
                                generate-config)
                                    _arguments \
                                        '(-o --output)'{-o,--output}'[Output file]:file:_files'
                                    ;;
                            esac
                            ;;
                    esac
                    ;;
            esac
            ;;
    esac
}

_btrfs-backup-ng "$@"
