# Bash completion for btrfs-backup-ng
# Install: Copy to /etc/bash_completion.d/ or source in ~/.bashrc

_btrfs_backup_ng() {
    local cur prev words cword split
    _init_completion -s || return

    local commands="run snapshot transfer prune list status config install uninstall restore verify estimate doctor completions manpages transfers snapper"
    local config_subcommands="validate init import detect"
    local completions_subcommands="install path"
    local manpages_subcommands="install path"
    local transfers_subcommands="list show resume pause cleanup operations"
    local snapper_subcommands="detect list backup status restore generate-config"

    # Global options
    local global_opts="-h --help -v --verbose -q --quiet --debug -V --version -c --config"

    # Command-specific options
    local run_opts="--dry-run --parallel-volumes --parallel-targets --compress --rate-limit --progress --no-progress --check-space --no-check-space --force --fs-checks"
    local snapshot_opts="--dry-run --volume"
    local transfer_opts="--dry-run --volume --compress --rate-limit --progress --no-progress"
    local prune_opts="--dry-run"
    local list_opts="--volume --json"
    local status_opts="-t --transactions -n --limit"
    local install_opts="--timer --oncalendar --user"
    local uninstall_opts=""
    local restore_opts="-l --list -s --snapshot --before -a --all -i --interactive --dry-run --no-incremental --overwrite --in-place --yes-i-know-what-i-am-doing --prefix --ssh-sudo --ssh-key --compress --rate-limit --fs-checks --status --unlock --cleanup --progress --no-progress -c --config --volume --target --list-volumes --to"
    local config_validate_opts=""
    local config_init_opts="-i --interactive -o --output"
    local config_import_opts="-o --output"
    local config_detect_opts="--json"
    local verify_opts="--level --snapshot --temp-dir --no-cleanup --prefix --ssh-sudo --ssh-key --fs-checks --json -q --quiet"
    local estimate_opts="-c --config --volume --target --prefix --ssh-sudo --ssh-key --fs-checks --check-space --safety-margin --json"
    local doctor_opts="--json --check --fix --interactive -q --quiet --volume"
    local doctor_categories="config snapshots transfers system"
    local completions_install_opts="--shell --system"
    local manpages_install_opts="--system --prefix"
    local transfers_list_opts="--json"
    local transfers_show_opts="--json"
    local transfers_resume_opts="--dry-run"
    local transfers_cleanup_opts="--force --all --age"
    local snapper_detect_opts="--json"
    local snapper_list_opts="--config --type --json"
    local snapper_backup_opts="--snapshot --dry-run --ssh-sudo --ssh-key --compress --progress --no-progress"
    local snapper_status_opts="--json"
    local snapper_restore_opts="--snapshot --dry-run --ssh-sudo --ssh-key"
    local snapper_generate_config_opts="-o --output"
    local snapper_types="single pre post"
    local verify_levels="metadata stream full"
    local shell_types="bash zsh fish"
    local fs_checks_modes="auto strict skip"

    # Compression methods
    local compress_methods="none zstd gzip lz4 pigz lzop"

    # Timer presets
    local timer_presets="hourly daily weekly"

    # Determine the command being used
    local cmd=""
    local subcmd=""
    local i
    for ((i=1; i < cword; i++)); do
        case "${words[i]}" in
            run|snapshot|transfer|prune|list|status|config|install|uninstall|restore|verify|estimate|doctor|completions|manpages|transfers|snapper)
                cmd="${words[i]}"
                ;;
            validate|init|import|detect)
                if [[ "$cmd" == "config" ]]; then
                    subcmd="${words[i]}"
                fi
                ;;
            list|show|resume|pause|cleanup|operations)
                if [[ "$cmd" == "transfers" ]]; then
                    subcmd="${words[i]}"
                fi
                ;;
            detect|backup|status|restore|generate-config)
                if [[ "$cmd" == "snapper" ]]; then
                    subcmd="${words[i]}"
                fi
                ;;
            path|install)
                if [[ "$cmd" == "completions" || "$cmd" == "manpages" ]]; then
                    subcmd="${words[i]}"
                fi
                ;;
        esac
    done

    # Handle option arguments
    case "$prev" in
        -c|--config|--ssh-key)
            _filedir
            return
            ;;
        --volume)
            _filedir -d
            return
            ;;
        -o|--output)
            _filedir
            return
            ;;
        --compress)
            COMPREPLY=($(compgen -W "$compress_methods" -- "$cur"))
            return
            ;;
        --timer)
            COMPREPLY=($(compgen -W "$timer_presets" -- "$cur"))
            return
            ;;
        --parallel-volumes|--parallel-targets|-n|--limit)
            # Numeric argument
            return
            ;;
        --rate-limit)
            # Rate limit like 10M, 1G
            return
            ;;
        --oncalendar|--before|--snapshot|--prefix|--unlock)
            # Free-form text arguments
            return
            ;;
        --level)
            COMPREPLY=($(compgen -W "$verify_levels" -- "$cur"))
            return
            ;;
        --shell)
            COMPREPLY=($(compgen -W "$shell_types" -- "$cur"))
            return
            ;;
        --temp-dir)
            _filedir -d
            return
            ;;
        --fs-checks)
            COMPREPLY=($(compgen -W "$fs_checks_modes" -- "$cur"))
            return
            ;;
        --check)
            COMPREPLY=($(compgen -W "$doctor_categories" -- "$cur"))
            return
            ;;
        --safety-margin)
            # Numeric percentage
            return
            ;;
    esac

    # Handle command completion
    if [[ -z "$cmd" ]]; then
        # No command yet, complete commands or global options
        if [[ "$cur" == -* ]]; then
            COMPREPLY=($(compgen -W "$global_opts" -- "$cur"))
        else
            COMPREPLY=($(compgen -W "$commands" -- "$cur"))
        fi
        return
    fi

    # Complete based on command
    case "$cmd" in
        run)
            COMPREPLY=($(compgen -W "$run_opts" -- "$cur"))
            ;;
        snapshot)
            COMPREPLY=($(compgen -W "$snapshot_opts" -- "$cur"))
            ;;
        transfer)
            COMPREPLY=($(compgen -W "$transfer_opts" -- "$cur"))
            ;;
        prune)
            COMPREPLY=($(compgen -W "$prune_opts" -- "$cur"))
            ;;
        list)
            COMPREPLY=($(compgen -W "$list_opts" -- "$cur"))
            ;;
        status)
            COMPREPLY=($(compgen -W "$status_opts" -- "$cur"))
            ;;
        install)
            COMPREPLY=($(compgen -W "$install_opts" -- "$cur"))
            ;;
        uninstall)
            COMPREPLY=($(compgen -W "$uninstall_opts" -- "$cur"))
            ;;
        restore)
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "$restore_opts" -- "$cur"))
            else
                # Complete paths for SOURCE and DESTINATION
                _filedir -d
            fi
            ;;
        config)
            if [[ -z "$subcmd" ]]; then
                if [[ "$cur" == -* ]]; then
                    COMPREPLY=($(compgen -W "-h --help" -- "$cur"))
                else
                    COMPREPLY=($(compgen -W "$config_subcommands" -- "$cur"))
                fi
            else
                case "$subcmd" in
                    validate)
                        COMPREPLY=($(compgen -W "$config_validate_opts" -- "$cur"))
                        ;;
                    init)
                        COMPREPLY=($(compgen -W "$config_init_opts" -- "$cur"))
                        ;;
                    import)
                        if [[ "$cur" == -* ]]; then
                            COMPREPLY=($(compgen -W "$config_import_opts" -- "$cur"))
                        else
                            _filedir conf
                        fi
                        ;;
                    detect)
                        if [[ "$cur" == -* ]]; then
                            COMPREPLY=($(compgen -W "$config_detect_opts" -- "$cur"))
                        else
                            _filedir -d
                        fi
                        ;;
                esac
            fi
            ;;
        verify)
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "$verify_opts" -- "$cur"))
            else
                # Complete paths for LOCATION
                _filedir -d
            fi
            ;;
        estimate)
            if [[ "$cur" == -* ]]; then
                COMPREPLY=($(compgen -W "$estimate_opts" -- "$cur"))
            else
                # Complete paths for SOURCE and DESTINATION
                _filedir -d
            fi
            ;;
        doctor)
            COMPREPLY=($(compgen -W "$doctor_opts" -- "$cur"))
            ;;
        completions)
            if [[ -z "$subcmd" ]]; then
                if [[ "$cur" == -* ]]; then
                    COMPREPLY=($(compgen -W "-h --help" -- "$cur"))
                else
                    COMPREPLY=($(compgen -W "$completions_subcommands" -- "$cur"))
                fi
            else
                case "$subcmd" in
                    install)
                        COMPREPLY=($(compgen -W "$completions_install_opts" -- "$cur"))
                        ;;
                    path)
                        # No additional options
                        ;;
                esac
            fi
            ;;
        manpages)
            if [[ -z "$subcmd" ]]; then
                if [[ "$cur" == -* ]]; then
                    COMPREPLY=($(compgen -W "-h --help" -- "$cur"))
                else
                    COMPREPLY=($(compgen -W "$manpages_subcommands" -- "$cur"))
                fi
            else
                case "$subcmd" in
                    install)
                        COMPREPLY=($(compgen -W "$manpages_install_opts" -- "$cur"))
                        ;;
                    path)
                        # No additional options
                        ;;
                esac
            fi
            ;;
        transfers)
            if [[ -z "$subcmd" ]]; then
                if [[ "$cur" == -* ]]; then
                    COMPREPLY=($(compgen -W "-h --help" -- "$cur"))
                else
                    COMPREPLY=($(compgen -W "$transfers_subcommands" -- "$cur"))
                fi
            else
                case "$subcmd" in
                    list)
                        COMPREPLY=($(compgen -W "$transfers_list_opts" -- "$cur"))
                        ;;
                    show)
                        COMPREPLY=($(compgen -W "$transfers_show_opts" -- "$cur"))
                        ;;
                    resume)
                        COMPREPLY=($(compgen -W "$transfers_resume_opts" -- "$cur"))
                        ;;
                    cleanup)
                        COMPREPLY=($(compgen -W "$transfers_cleanup_opts" -- "$cur"))
                        ;;
                    pause|operations)
                        # No additional options
                        ;;
                esac
            fi
            ;;
        snapper)
            if [[ -z "$subcmd" ]]; then
                if [[ "$cur" == -* ]]; then
                    COMPREPLY=($(compgen -W "-h --help" -- "$cur"))
                else
                    COMPREPLY=($(compgen -W "$snapper_subcommands" -- "$cur"))
                fi
            else
                case "$subcmd" in
                    detect)
                        COMPREPLY=($(compgen -W "$snapper_detect_opts" -- "$cur"))
                        ;;
                    list)
                        if [[ "$prev" == "--type" ]]; then
                            COMPREPLY=($(compgen -W "$snapper_types" -- "$cur"))
                        else
                            COMPREPLY=($(compgen -W "$snapper_list_opts" -- "$cur"))
                        fi
                        ;;
                    backup)
                        if [[ "$cur" == -* ]]; then
                            COMPREPLY=($(compgen -W "$snapper_backup_opts" -- "$cur"))
                        else
                            _filedir -d
                        fi
                        ;;
                    status)
                        COMPREPLY=($(compgen -W "$snapper_status_opts" -- "$cur"))
                        ;;
                    restore)
                        if [[ "$cur" == -* ]]; then
                            COMPREPLY=($(compgen -W "$snapper_restore_opts" -- "$cur"))
                        else
                            _filedir -d
                        fi
                        ;;
                    generate-config)
                        COMPREPLY=($(compgen -W "$snapper_generate_config_opts" -- "$cur"))
                        ;;
                esac
            fi
            ;;
    esac
}

complete -F _btrfs_backup_ng btrfs-backup-ng
