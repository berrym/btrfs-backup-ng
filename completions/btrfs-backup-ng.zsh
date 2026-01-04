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
                        '(--progress --no-progress)'--no-progress'[Disable progress bars]'
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
                                        '(-o --output)'{-o,--output}'[Output file]:file:_files'
                                    ;;
                                import)
                                    _arguments \
                                        '(-o --output)'{-o,--output}'[Output file]:file:_files' \
                                        '1:btrbk config file:_files -g "*.conf"'
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
                        '--no-fs-checks[Skip btrfs subvolume verification]' \
                        '--status[Show status of locks and incomplete restores]' \
                        '--unlock[Unlock stuck restore session]:lock id or all:' \
                        '--cleanup[Clean up partial/incomplete snapshot restores]' \
                        '(--progress --no-progress)'--progress'[Show progress bars]' \
                        '(--progress --no-progress)'--no-progress'[Disable progress bars]' \
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
                        '--no-fs-checks[Skip btrfs subvolume verification]' \
                        '--json[Output results in JSON format]' \
                        '(-q --quiet)'{-q,--quiet}'[Suppress progress output]' \
                        '1:backup location:_files -/'
                    ;;
            esac
            ;;
    esac
}

_btrfs-backup-ng "$@"
