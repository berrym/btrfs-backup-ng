# BTRFS Backup NG

A robust, production-ready tool for BTRFS snapshot backup operations over SSH with comprehensive authentication support.

## Overview

BTRFS Backup NG provides reliable snapshot transfer capabilities for BTRFS filesystems, with particular focus on robust SSH authentication handling and error recovery. The tool supports both passwordless sudo and password-based authentication with fallback mechanisms to ensure maximum compatibility across different system configurations.

## Key Features

- **Robust SSH Authentication**: Supports both passwordless sudo and password-based authentication
- **Fallback Mechanisms**: Automatic fallback from SUDO_ASKPASS to sudo -S when primary authentication fails
- **Production Ready**: Comprehensive error handling and logging for production environments
- **Progress Monitoring**: Visual progress feedback during large transfers
- **Connection Optimization**: SSH master connections for improved performance
- **Verification**: Pre and post-transfer verification to ensure data integrity

## Installation

### Prerequisites

- Python 3.7 or higher
- SSH client with key-based authentication configured
- BTRFS utilities on both source and destination systems

### Optional Dependencies

For enhanced transfer experience:

```bash
# Progress display during transfers
sudo apt install pv       # Debian/Ubuntu
sudo dnf install pv       # Fedora/RHEL
brew install pv           # macOS with Homebrew

# Buffered transfers (optional)
sudo apt install mbuffer  # Debian/Ubuntu
sudo dnf install mbuffer  # Fedora/RHEL
brew install mbuffer      # macOS with Homebrew
```

### Install from Source

```bash
git clone https://github.com/your-org/btrfs-backup-ng.git
cd btrfs-backup-ng
pip install -e .
```

## Configuration

### SSH Setup

1. **Key-based Authentication**: Configure SSH key-based authentication to your target hosts
2. **Sudo Configuration**: Configure passwordless sudo for BTRFS commands (recommended) or prepare for password authentication

#### Passwordless Sudo (Recommended)

Add to `/etc/sudoers` on target hosts via `sudo visudo`:

```sudoers
# Full access to btrfs commands
username ALL=(ALL) NOPASSWD: /usr/bin/btrfs

# Or more restricted access
username ALL=(ALL) NOPASSWD: /usr/bin/btrfs subvolume*, /usr/bin/btrfs send*, /usr/bin/btrfs receive*
```

#### Password-based Authentication

If passwordless sudo is not available, the tool supports password authentication:

```bash
# Set password via environment variable
export BTRFS_BACKUP_SUDO_PASSWORD="your_password"

# Or enable fallback method
export BTRFS_BACKUP_SUDO_FALLBACK=1
```

## Usage

### Basic Transfer

```bash
# Transfer a snapshot to remote host
btrfs-ssh-send /source/snapshot user@remote:/destination/path

# With sudo on remote host
sudo SSH_AUTH_SOCK=$SSH_AUTH_SOCK btrfs-ssh-send /source/snapshot user@remote:/destination/path
```

### Python API

```python
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

# Configure SSH endpoint
config = {
    'hostname': 'remote.example.com',
    'username': 'backup_user',
    'path': '/backup/destination',
    'ssh_sudo': True,  # Enable sudo on remote host
    'ssh_sudo_fallback': True  # Enable fallback authentication
}

# Create endpoint and perform operations
endpoint = SSHEndpoint(config)
snapshots = endpoint.list_snapshots()
```

## Authentication Methods

### Primary Method: SUDO_ASKPASS

The tool uses a sophisticated SUDO_ASKPASS approach that:
- Creates temporary authentication scripts
- Avoids stdin conflicts with data streams
- Provides clean separation between authentication and data

### Fallback Method: sudo -S

When the primary method fails, automatic fallback to:
- Direct password input via named pipes
- Coordinated password and data streaming
- Comprehensive error recovery

### Configuration Options

- `ssh_sudo_fallback: True` - Enable fallback in configuration
- `BTRFS_BACKUP_SUDO_FALLBACK=1` - Enable fallback via environment
- `BTRFS_BACKUP_SUDO_PASSWORD` - Provide password via environment

## Known Working Configurations

- **Local to Remote SSH**: Tested and working with both authentication methods
- **Passwordless Sudo**: Primary tested configuration
- **Password-based Sudo**: Tested with fallback mechanisms
- **SSH Agent Forwarding**: Working when running with proper environment

## Current Status

### ✅ Completed Features

- Robust SSH authentication with SUDO_ASKPASS
- Automatic fallback to sudo -S when needed
- Comprehensive error handling and logging
- Progress monitoring and transfer verification
- SSH master connection optimization
- Support for both interactive and non-interactive environments

### ⚠️ Needs Additional Testing

- Large-scale production deployments
- Various Linux distributions and SSH configurations
- Edge cases in network failure scenarios
- Performance optimization for very large transfers

### 🔧 Future Enhancements

- **Configuration Management**: Centralized configuration file support
- **Retry Logic**: Configurable retry attempts for failed transfers
- **Parallel Transfers**: Support for concurrent snapshot transfers
- **Monitoring Integration**: Metrics and monitoring hooks for production use
- **Cross-platform Support**: Enhanced Windows and macOS compatibility

## Troubleshooting

### Common Issues

1. **Authentication Failures**
   - Verify SSH key-based authentication works: `ssh user@remote`
   - Check sudo configuration on remote host
   - Enable fallback authentication if needed

2. **Permission Denied**
   - Ensure user has sudo access to BTRFS commands
   - Verify destination directory is writable
   - Check BTRFS filesystem availability

3. **Transfer Failures**
   - Monitor logs for specific error messages
   - Verify network connectivity and SSH configuration
   - Check available disk space on destination

### Debug Logging

Enable detailed logging:

```bash
export BTRFS_BACKUP_LOG_LEVEL=DEBUG
```

## Contributing

Contributions are welcome! Please ensure:
- Code follows PEP 8 guidelines
- Comprehensive testing of new features
- Documentation updates for user-facing changes
- Backward compatibility considerations

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Security Considerations

- SSH keys should be properly secured
- Sudo passwords should be provided via secure environment variables
- Regular security updates for SSH and system components
- Network traffic is encrypted via SSH
