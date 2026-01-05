# Bash completion for btrfs-backup-ng
# Install: Copy to /etc/bash_completion.d/ or source in ~/.bashrc

_btrfs_backup_ng() {
    local cur prev words cword split
    _init_completion -s || return

    local commands="run snapshot transfer prune list status config install uninstall restore verify estimate completions manpages"
    local config_subcommands="validate init import"
    local completions_subcommands="install path"
    local manpages_subcommands="install path"

    # Global options
    local global_opts="-h --help -v --verbose -q --quiet --debug -V --version -c --config"

    # Command-specific options
    local run_opts="--dry-run --parallel-volumes --parallel-targets --compress --rate-limit --progress --no-progress"
    local snapshot_opts="--dry-run --volume"
    local transfer_opts="--dry-run --volume --compress --rate-limit --progress --no-progress"
    local prune_opts="--dry-run"
    local list_opts="--volume --json"
    local status_opts="-t --transactions -n --limit"
    local install_opts="--timer --oncalendar --user"
    local uninstall_opts=""
    local restore_opts="-l --list -s --snapshot --before -a --all -i --interactive --dry-run --no-incremental --overwrite --in-place --yes-i-know-what-i-am-doing --prefix --ssh-sudo --ssh-key --compress --rate-limit --no-fs-checks --status --unlock --cleanup --progress --no-progress -c --config --volume --target --list-volumes --to"
    local config_validate_opts=""
    local config_init_opts="-i --interactive -o --output"
    local config_import_opts="-o --output"
    local verify_opts="--level --snapshot --temp-dir --no-cleanup --prefix --ssh-sudo --ssh-key --no-fs-checks --json -q --quiet"
    local estimate_opts="-c --config --volume --target --prefix --ssh-sudo --ssh-key --no-fs-checks --json"
    local completions_install_opts="--shell --system"
    local manpages_install_opts="--system --prefix"
    local verify_levels="metadata stream full"
    local shell_types="bash zsh fish"

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
            run|snapshot|transfer|prune|list|status|config|install|uninstall|restore|verify|completions|manpages)
                cmd="${words[i]}"
                ;;
            validate|init|import)
                if [[ "$cmd" == "config" ]]; then
                    subcmd="${words[i]}"
                fi
                ;;
            path)
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
    esac
}

complete -F _btrfs_backup_ng btrfs-backup-ng
