# BTRFS Backup NG - Endpoint Component

This directory contains the endpoint implementations for btrfs-backup-ng. Endpoints are responsible for managing connections to local and remote filesystems, and executing btrfs operations.

## Available Endpoints

- `local.py`: Local filesystem endpoint
- `ssh.py`: Remote SSH endpoint for operating on remote hosts
- `shell.py`: Shell endpoint for executing commands
- `common.py`: Base endpoint class and common functionality

## Recent Fixes

### SSH Endpoint (2023-07-18)

The SSH endpoint implementation has been fixed to address several issues:

1. Fixed syntax error in `_btrfs_send()` method where a missing `except` clause was causing an "unexpected indent" error
2. Added proper exception handling in all try/except blocks throughout the code
3. Improved error handling and reporting in SSH operations
4. Ensured proper cleanup of file descriptors and processes

If you encounter any issues with the SSH endpoint, please report them in the project's issue tracker.

## Usage

Endpoints are typically not used directly by users, but are instantiated by the main application based on the provided configuration.

For developers extending this codebase, here's a basic example of how endpoints are used:

```python
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

# Create a local endpoint
local = LocalEndpoint(config={"path": "/path/to/local/subvolume"})

# Create an SSH endpoint
ssh = SSHEndpoint(
    hostname="remote.example.com",
    config={
        "path": "/path/to/remote/subvolume",
        "username": "user",
        "ssh_identity_file": "~/.ssh/id_ed25519",
        "ssh_sudo": True
    }
)

# Perform send/receive operation
local.send_receive(source="/path/to/source", destination=ssh.get_path())
```

## Implementation Notes

When extending or modifying endpoint implementations, ensure that:

1. All try/except blocks are properly formed and handle all possible exceptions
2. Resources (file handles, processes) are properly cleaned up in finally blocks
3. Error messages are clear and provide useful information for debugging
4. Command execution is secure and properly handles user input