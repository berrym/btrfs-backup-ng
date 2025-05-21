# BTRFS Backup NG - Endpoint Component

This directory contains the endpoint implementations for btrfs-backup-ng. Endpoints are responsible for managing connections to local and remote filesystems, and executing btrfs operations.

## Available Endpoints

- `local.py`: Local filesystem endpoint for operations on the local system
- `ssh.py`: Remote SSH endpoint for operating on remote hosts with full progress display
- `shell.py`: Shell endpoint for executing commands through shell pipelines
- `common.py`: Base endpoint class and common functionality shared across implementations

## Recent Improvements

### SSH Endpoint (May 2025)

The SSH endpoint implementation has been significantly enhanced:

1. Fixed syntax error in `_btrfs_send()` method where a missing `except` clause was causing an "unexpected indent" error
2. Added proper exception handling in all try/except blocks throughout the code
3. Improved error handling and reporting in SSH operations
4. Ensured proper cleanup of file descriptors and processes
5. Added progress display with progress bar, ETA, and transfer rate
6. Integrated with standalone `btrfs-ssh-send` script for improved transfer reliability
7. Added pre-transfer verification of remote filesystem type
8. Implemented post-transfer verification to confirm snapshot creation
9. Fixed path handling when running as root with different PATH configurations
10. Added support for mbuffer/pv for improved transfer performance

If you encounter any issues with the SSH endpoint, please report them in the project's issue tracker.

## Usage

Endpoints are typically not used directly by users, but are instantiated by the main application based on the provided configuration.

### Command Line Usage for SSH

```bash
# Basic SSH backup (remote destination)
sudo btrfs-backup-ng /path/to/source ssh://user@host:/path/to/destination

# SSH backup with full options
sudo SSH_AUTH_SOCK=$SSH_AUTH_SOCK btrfs-backup-ng \
    --ssh-identity-file ~/.ssh/id_ed25519 \
    --ssh-sudo \
    --ssh-opts="-o ServerAliveInterval=10" \
    -v debug \
    /path/to/source \
    ssh://user@host:/path/to/destination

# Using standalone SSH transfer script directly
sudo SSH_AUTH_SOCK=$SSH_AUTH_SOCK ./btrfs-ssh-send \
    -i ~/.ssh/id_ed25519 \
    --sudo \
    /path/to/snapshot \
    user@host:/path/to/destination
```

### For Developers

For developers extending this codebase, here's a basic example of how endpoints are used:

```python
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

# Create a local endpoint
local = LocalEndpoint(config={"path": "/path/to/local/subvolume"})

# Create an SSH endpoint with all options
ssh = SSHEndpoint(
    hostname="remote.example.com",
    config={
        "path": "/path/to/remote/subvolume",
        "username": "user",
        "ssh_identity_file": "~/.ssh/id_ed25519",
        "ssh_sudo": True,
        "ssh_opts": ["-o", "ServerAliveInterval=10", "-o", "BatchMode=yes"],
        "passwordless": True,
        "buffer_size": "256M"
    }
)

# Create a snapshot
snapshot = local.snapshot()

# Perform send/receive operation
local.send(snapshot)
ssh.receive(local.stdout)

# Or use the integrated method
ssh.send_receive(snapshot=snapshot, destination=ssh.get_path())
```

## Implementation Notes

When extending or modifying endpoint implementations, ensure that:

1. All try/except blocks are properly formed and handle all possible exceptions
2. Resources (file handles, processes) are properly cleaned up in finally blocks
3. Error messages are clear and provide useful information for debugging
4. Command execution is secure and properly handles user input

### SSH Implementation Details

The SSH endpoint uses multiple approaches to ensure reliable transfers:

1. **Command Path Handling**: 
   - Resolves commands like `btrfs` to their full paths
   - Handles different paths between user and root environments
   - Uses direct path references instead of relying on PATH

2. **SSH Authentication**:
   - Supports identity files with proper path handling
   - Preserves SSH agent forwarding via SSH_AUTH_SOCK
   - Implements passwordless sudo for remote operations

3. **Transfer Process**:
   - Uses direct pipe between send and receive for efficiency
   - Integrates with external buffer programs (pv/mbuffer) for progress display
   - Implements fallback mechanisms if any approach fails

4. **Verification**:
   - Tests SSH connectivity before attempting transfers
   - Verifies remote filesystem is BTRFS
   - Confirms snapshot exists after transfer
   - Provides detailed error messages for troubleshooting

5. **Security Considerations**:
   - Validates all user input before passing to shell commands
   - Uses proper quoting and escaping to prevent injection attacks
   - Enforces strict permissions on SSH credential files
