# BTRFS Backup NG - SSH Utility Module

This directory contains utilities for managing SSH connections in btrfs-backup-ng.

## Overview

The SSH utility module provides robust SSH connection management capabilities, including:

- Persistent SSH connections using master/control sockets
- Automatic retry of failed connections
- Proper cleanup of resources
- Advanced error handling and reporting

## Components

### SSHMasterManager

The `SSHMasterManager` class in `master.py` handles SSH master connections, which allow multiple SSH commands to be executed over a single connection. This significantly improves performance when multiple operations need to be performed on the same remote host.

Key features:
- Connection persistence with customizable lifetime
- Connection reuse across multiple commands
- Automatic cleanup of stale sockets
- Proper resource management using context managers (`with` statements)

## Usage

This module is used internally by the SSH endpoint implementation in `btrfs_backup_ng.endpoint.ssh`. Normal users of btrfs-backup-ng don't need to use these utilities directly.

For developers extending the codebase, here's a basic example of how to use the SSH manager:

```python
from btrfs_backup_ng.sshutil.master import SSHMasterManager

# Create an SSH manager
ssh_manager = SSHMasterManager(
    hostname="remote.example.com",
    username="user",
    port=22,
    identity_file="~/.ssh/id_ed25519",
    persist="60",  # Keep connection alive for 60 seconds after last use
    debug=True     # Enable verbose debug logging
)

# Use the manager as a context manager to ensure proper cleanup
with ssh_manager:
    # Execute commands using the manager's base SSH command
    ssh_cmd = ssh_manager._ssh_base_cmd() + ["--", "echo", "Hello from remote"]
    result = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(result.stdout.decode())
```

## Implementation Notes

The SSH master connection uses the OpenSSH ControlMaster feature. When extending or modifying this module, ensure that:

1. All sockets are properly cleaned up
2. Connections are terminated when no longer needed
3. Error handling is robust and provides useful diagnostic information
4. Security best practices are followed

## Dependencies

- OpenSSH client on the local system
- Python standard library (no external packages required)