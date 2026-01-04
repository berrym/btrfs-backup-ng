# Fish completion for btrfs-backup-ng
# Install: Copy to ~/.config/fish/completions/ or /usr/share/fish/vendor_completions.d/

# Disable file completion by default
complete -c btrfs-backup-ng -f

# Helper functions
function __fish_btrfs_backup_ng_no_subcommand
    set -l cmd (commandline -opc)
    set -e cmd[1]
    for c in $cmd
        switch $c
            case run snapshot transfer prune list status config install uninstall restore
                return 1
        end
    end
    return 0
end

function __fish_btrfs_backup_ng_using_command
    set -l cmd (commandline -opc)
    set -e cmd[1]
    if test (count $cmd) -gt 0
        if test $argv[1] = $cmd[1]
            return 0
        end
    end
    return 1
end

function __fish_btrfs_backup_ng_config_using_subcommand
    set -l cmd (commandline -opc)
    set -e cmd[1]
    if test (count $cmd) -gt 1
        if test $cmd[1] = config
            if test $argv[1] = $cmd[2]
                return 0
            end
        end
    end
    return 1
end

# Global options
complete -c btrfs-backup-ng -s h -l help -d 'Show help message'
complete -c btrfs-backup-ng -s v -l verbose -d 'Enable verbose output'
complete -c btrfs-backup-ng -s q -l quiet -d 'Suppress non-essential output'
complete -c btrfs-backup-ng -l debug -d 'Enable debug output'
complete -c btrfs-backup-ng -s V -l version -d 'Show version and exit'
complete -c btrfs-backup-ng -s c -l config -d 'Path to configuration file' -r -F

# Subcommands
complete -c btrfs-backup-ng -n __fish_btrfs_backup_ng_no_subcommand -a run -d 'Execute all configured backup jobs'
complete -c btrfs-backup-ng -n __fish_btrfs_backup_ng_no_subcommand -a snapshot -d 'Create snapshots only'
complete -c btrfs-backup-ng -n __fish_btrfs_backup_ng_no_subcommand -a transfer -d 'Transfer existing snapshots to targets'
complete -c btrfs-backup-ng -n __fish_btrfs_backup_ng_no_subcommand -a prune -d 'Apply retention policies'
complete -c btrfs-backup-ng -n __fish_btrfs_backup_ng_no_subcommand -a list -d 'Show snapshots and backups'
complete -c btrfs-backup-ng -n __fish_btrfs_backup_ng_no_subcommand -a status -d 'Show job status and statistics'
complete -c btrfs-backup-ng -n __fish_btrfs_backup_ng_no_subcommand -a config -d 'Configuration management'
complete -c btrfs-backup-ng -n __fish_btrfs_backup_ng_no_subcommand -a install -d 'Install systemd timer/service'
complete -c btrfs-backup-ng -n __fish_btrfs_backup_ng_no_subcommand -a uninstall -d 'Remove systemd timer/service'
complete -c btrfs-backup-ng -n __fish_btrfs_backup_ng_no_subcommand -a restore -d 'Restore snapshots from backup location'

# Compression methods
set -l compress_methods none zstd gzip lz4 pigz lzop

# Timer presets
set -l timer_presets hourly daily weekly

# run command
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l dry-run -d 'Show what would be done without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l parallel-volumes -d 'Max concurrent volume backups' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l parallel-targets -d 'Max concurrent target transfers per volume' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l compress -d 'Compression method' -xa "$compress_methods"
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l rate-limit -d 'Bandwidth limit (e.g., 10M, 1G)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l progress -d 'Show progress bars'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command run' -l no-progress -d 'Disable progress bars'

# snapshot command
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapshot' -l dry-run -d 'Show what would be done without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command snapshot' -l volume -d 'Only snapshot specific volume' -xa '(__fish_complete_directories)'

# transfer command
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l dry-run -d 'Show what would be done without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l volume -d 'Only transfer specific volume' -xa '(__fish_complete_directories)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l compress -d 'Compression method' -xa "$compress_methods"
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l rate-limit -d 'Bandwidth limit (e.g., 10M, 1G)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l progress -d 'Show progress bars'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command transfer' -l no-progress -d 'Disable progress bars'

# prune command
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command prune' -l dry-run -d 'Show what would be deleted without making changes'

# list command
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command list' -l volume -d 'Only list specific volume' -xa '(__fish_complete_directories)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command list' -l json -d 'Output in JSON format'

# status command
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command status' -s t -l transactions -d 'Show recent transaction history'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command status' -s n -l limit -d 'Number of transactions to show' -x

# config command subcommands
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command config' -a validate -d 'Validate configuration file'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command config' -a init -d 'Generate example configuration'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command config' -a import -d 'Import btrbk configuration'

# config init
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_config_using_subcommand init' -s o -l output -d 'Output file' -r -F

# config import
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_config_using_subcommand import' -s o -l output -d 'Output file' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_config_using_subcommand import' -a '(__fish_complete_suffix .conf)' -d 'btrbk config file'

# install command
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command install' -l timer -d 'Use preset timer interval' -xa "$timer_presets"
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command install' -l oncalendar -d 'Custom OnCalendar specification' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command install' -l user -d 'Install as user service instead of system service'

# restore command
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -s l -l list -d 'List available snapshots at backup location'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -s s -l snapshot -d 'Restore specific snapshot by name' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l before -d 'Restore snapshot closest to this time' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -s a -l all -d 'Restore all snapshots (full mirror)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -s i -l interactive -d 'Interactively select snapshot to restore'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l dry-run -d 'Show what would be restored without making changes'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l no-incremental -d 'Force full transfers'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l overwrite -d 'Overwrite existing snapshots instead of skipping'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l in-place -d 'Restore to original location (DANGEROUS)'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l yes-i-know-what-i-am-doing -d 'Confirm dangerous operations'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l prefix -d 'Snapshot prefix filter' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l ssh-sudo -d 'Use sudo for btrfs commands on remote host'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l ssh-key -d 'SSH private key file' -r -F
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l compress -d 'Compression method' -xa "$compress_methods"
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l rate-limit -d 'Bandwidth limit (e.g., 10M, 1G)' -x
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l no-fs-checks -d 'Skip btrfs subvolume verification'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l status -d 'Show status of locks and incomplete restores'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l unlock -d 'Unlock stuck restore session' -xa 'all'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l cleanup -d 'Clean up partial/incomplete snapshot restores'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l progress -d 'Show progress bars'
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -l no-progress -d 'Disable progress bars'
# Enable path completion for restore positional arguments
complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command restore' -a '(__fish_complete_directories)'
