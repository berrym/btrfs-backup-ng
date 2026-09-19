# bash completion for btrfs-backup-ng
# Generated from the argument parser; edit the parser, not this file.
# Install: source this file, or place it in /etc/bash_completion.d/

_btrfs_backup_ng() {
    local cur prev words cword
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    local commands="completions config doctor estimate install list manpages prune raw restore run snapper snapshot status transfer transfers uninstall verify"
    local global_opts="-h --help -v --verbose -q --quiet --debug -V --version -c --config"

    case "$prev" in
        --shell)
            COMPREPLY=($(compgen -W "bash zsh fish" -- "$cur")); return ;;
        -o|--output)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
        --check)
            COMPREPLY=($(compgen -W "config snapshots transfers system" -- "$cur")); return ;;
        --volume)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
        -c|--config)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
        --ssh-key)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
        --ssh-auth-sock)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
        --fs-checks)
            COMPREPLY=($(compgen -W "auto strict skip" -- "$cur")); return ;;
        --ssh-host-key-policy)
            COMPREPLY=($(compgen -W "accept-new strict" -- "$cur")); return ;;
        --timer)
            COMPREPLY=($(compgen -W "hourly daily weekly" -- "$cur")); return ;;
        --prefix)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
        --encrypt)
            COMPREPLY=($(compgen -W "gpg openssl_enc" -- "$cur")); return ;;
        --gpg-recipient)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
        --gpg-keyring)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
        --compress)
            COMPREPLY=($(compgen -W "none bzip2 gzip lz4 lzo lzop pbzip2 pigz xz zstd" -- "$cur")); return ;;
        --to)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
        -t|--type)
            COMPREPLY=($(compgen -W "single pre post" -- "$cur")); return ;;
        -t|--target)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
        -a|--append)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
        --type)
            COMPREPLY=($(compgen -W "single pre post" -- "$cur")); return ;;
        --level)
            COMPREPLY=($(compgen -W "metadata stream full" -- "$cur")); return ;;
        --temp-dir)
            COMPREPLY=($(compgen -f -- "$cur")); return ;;
    esac

    local cmd="" sub=""
    local i
    for ((i=1; i<COMP_CWORD; i++)); do
        case "${COMP_WORDS[i]}" in
            -*) ;;
            *) if [ -z "$cmd" ]; then cmd="${COMP_WORDS[i]}"; elif [ -z "$sub" ]; then sub="${COMP_WORDS[i]}"; fi ;;
        esac
    done

    if [ -z "$cmd" ]; then
        COMPREPLY=($(compgen -W "$commands $global_opts" -- "$cur"))
        return
    fi

    case "$cmd" in
        completions)
            case "$sub" in
                install)
                    COMPREPLY=($(compgen -W "-h --help --shell --system -h --help $global_opts" -- "$cur")) ;;
                path)
                    COMPREPLY=($(compgen -W "-h --help -h --help $global_opts" -- "$cur")) ;;
                *)
                    COMPREPLY=($(compgen -W "install path -h --help $global_opts" -- "$cur")) ;;
            esac ;;
        config)
            case "$sub" in
                detect)
                    COMPREPLY=($(compgen -W "-h --help --json -w --wizard -h --help $global_opts" -- "$cur")) ;;
                import)
                    COMPREPLY=($(compgen -W "-h --help -o --output --force -h --help $global_opts" -- "$cur")) ;;
                init)
                    COMPREPLY=($(compgen -W "-h --help -i --interactive -o --output --force -h --help $global_opts" -- "$cur")) ;;
                migrate-systemd)
                    COMPREPLY=($(compgen -W "-h --help --dry-run -h --help $global_opts" -- "$cur")) ;;
                validate)
                    COMPREPLY=($(compgen -W "-h --help -h --help $global_opts" -- "$cur")) ;;
                *)
                    COMPREPLY=($(compgen -W "detect import init migrate-systemd validate -h --help $global_opts" -- "$cur")) ;;
            esac ;;
        doctor)
            COMPREPLY=($(compgen -W "-h --help --json --check --fix -i --interactive -q --quiet --volume $global_opts" -- "$cur")) ;;
        estimate)
            COMPREPLY=($(compgen -W "-h --help -c --config --volume --target --prefix --ssh-sudo --ssh-key --ssh-auth-sock --timestamp-format --fs-checks --no-fs-checks --ssh-host-key-policy --skip-remote-lock --json --check-space --safety-margin $global_opts" -- "$cur")) ;;
        install)
            COMPREPLY=($(compgen -W "-h --help --timer --oncalendar --user $global_opts" -- "$cur")) ;;
        list)
            COMPREPLY=($(compgen -W "-h --help --volume --json $global_opts" -- "$cur")) ;;
        manpages)
            case "$sub" in
                install)
                    COMPREPLY=($(compgen -W "-h --help --system --prefix -h --help $global_opts" -- "$cur")) ;;
                path)
                    COMPREPLY=($(compgen -W "-h --help -h --help $global_opts" -- "$cur")) ;;
                *)
                    COMPREPLY=($(compgen -W "install path -h --help $global_opts" -- "$cur")) ;;
            esac ;;
        prune)
            COMPREPLY=($(compgen -W "-h --help --dry-run -y --yes --force $global_opts" -- "$cur")) ;;
        raw)
            case "$sub" in
                backfill-metadata)
                    COMPREPLY=($(compgen -W "-h --help --dry-run --json --ssh-sudo -h --help $global_opts" -- "$cur")) ;;
                encrypt)
                    COMPREPLY=($(compgen -W "-h --help --encrypt --gpg-recipient --gpg-keyring --openssl-cipher --shred --yes --dry-run --json -h --help $global_opts" -- "$cur")) ;;
                list)
                    COMPREPLY=($(compgen -W "-h --help --json --ssh-sudo -h --help $global_opts" -- "$cur")) ;;
                verify)
                    COMPREPLY=($(compgen -W "-h --help --snapshot --json --ssh-sudo -h --help $global_opts" -- "$cur")) ;;
                *)
                    COMPREPLY=($(compgen -W "backfill-metadata encrypt list verify -h --help $global_opts" -- "$cur")) ;;
            esac ;;
        restore)
            COMPREPLY=($(compgen -W "-h --help -l --list -s --snapshot --before -a --all -i --interactive --dry-run --no-incremental --skip-verify --overwrite --in-place --yes-i-know-what-i-am-doing --prefix --timestamp-format --ssh-sudo --ssh-key --ssh-auth-sock --compress --rate-limit --gpg-keyring --openssl-cipher --fs-checks --no-fs-checks --ssh-host-key-policy --skip-remote-lock -c --config --volume --target --list-volumes --to --status --unlock --cleanup --progress --no-progress $global_opts" -- "$cur")) ;;
        run)
            COMPREPLY=($(compgen -W "-h --help --dry-run --parallel-volumes --parallel-targets --compress --rate-limit --newest-only --no-check-space --force --safety-margin --progress --no-progress $global_opts" -- "$cur")) ;;
        snapper)
            case "$sub" in
                backup)
                    COMPREPLY=($(compgen -W "-h --help -s --snapshot -t --type --min-age --dry-run --ssh-sudo --ssh-key --ssh-auth-sock --ssh-host-key-policy --skip-remote-lock --compress --rate-limit --timestamp-format --encrypt --gpg-recipient --gpg-keyring --openssl-cipher --progress --no-progress -h --help $global_opts" -- "$cur")) ;;
                detect)
                    COMPREPLY=($(compgen -W "-h --help --json -h --help $global_opts" -- "$cur")) ;;
                generate-config)
                    COMPREPLY=($(compgen -W "-h --help -c --config -t --target -o --output -a --append --type --min-age --ssh-sudo --json -h --help $global_opts" -- "$cur")) ;;
                list)
                    COMPREPLY=($(compgen -W "-h --help -c --config -t --type --json --timestamp-format -h --help $global_opts" -- "$cur")) ;;
                restore)
                    COMPREPLY=($(compgen -W "-h --help -s --snapshot -a --all --backup-name --date --from-config --dry-run --ssh-sudo --ssh-key --ssh-auth-sock --gpg-keyring --openssl-cipher --ssh-host-key-policy --skip-remote-lock -l --list --json -h --help $global_opts" -- "$cur")) ;;
                status)
                    COMPREPLY=($(compgen -W "-h --help -c --config --json --timestamp-format -h --help $global_opts" -- "$cur")) ;;
                *)
                    COMPREPLY=($(compgen -W "backup detect generate-config list restore status -h --help $global_opts" -- "$cur")) ;;
            esac ;;
        snapshot)
            COMPREPLY=($(compgen -W "-h --help --dry-run --volume $global_opts" -- "$cur")) ;;
        status)
            COMPREPLY=($(compgen -W "-h --help -t --transactions -n --limit $global_opts" -- "$cur")) ;;
        transfer)
            COMPREPLY=($(compgen -W "-h --help --dry-run --volume --compress --rate-limit --no-check-space --force --safety-margin --progress --no-progress $global_opts" -- "$cur")) ;;
        transfers)
            case "$sub" in
                cleanup)
                    COMPREPLY=($(compgen -W "-h --help --max-age --force --dry-run -h --help $global_opts" -- "$cur")) ;;
                list)
                    COMPREPLY=($(compgen -W "-h --help --json -h --help $global_opts" -- "$cur")) ;;
                operations)
                    COMPREPLY=($(compgen -W "-h --help --all --json -h --help $global_opts" -- "$cur")) ;;
                pause)
                    COMPREPLY=($(compgen -W "-h --help -h --help $global_opts" -- "$cur")) ;;
                resume)
                    COMPREPLY=($(compgen -W "-h --help --dry-run -h --help $global_opts" -- "$cur")) ;;
                show)
                    COMPREPLY=($(compgen -W "-h --help --json -h --help $global_opts" -- "$cur")) ;;
                *)
                    COMPREPLY=($(compgen -W "cleanup list operations pause resume show -h --help $global_opts" -- "$cur")) ;;
            esac ;;
        uninstall)
            COMPREPLY=($(compgen -W "-h --help $global_opts" -- "$cur")) ;;
        verify)
            COMPREPLY=($(compgen -W "-h --help --level --snapshot --all --temp-dir --no-cleanup --prefix --timestamp-format --ssh-sudo --ssh-key --ssh-auth-sock --fs-checks --no-fs-checks --ssh-host-key-policy --skip-remote-lock --json -q --quiet $global_opts" -- "$cur")) ;;
    esac
}

complete -F _btrfs_backup_ng btrfs-backup-ng
