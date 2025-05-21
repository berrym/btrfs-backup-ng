# BTRFS Backup NG - Source Code

This directory contains the source code for the BTRFS Backup NG project.

## Project Structure

- `btrfs_backup_ng/`: Main package directory
  - `__init__.py`: Package initialization
  - `__main__.py`: Entry point for the application
  - `__logger__.py`: Logging configuration
  - `__util__.py`: Common utility functions
  - `endpoint/`: Endpoint implementations for local and remote operations
    - `local.py`: Local filesystem operations
    - `ssh.py`: Remote SSH operations
    - `common.py`: Base endpoint class and shared functionality
    - `shell.py`: Shell command execution
  - `sshutil/`: SSH utilities
    - `master.py`: SSH master connection manager

### Standalone Scripts

- `btrfs-ssh-send`: Standalone script for SSH transfers with progress display
  - Handles btrfs snapshot transfers over SSH with visual progress feedback
  - Implements transfer buffering with pv/mbuffer for improved reliability
  - Provides verification of successful transfers
  - Can be used independently of the main package

## Development

### Setting up a development environment

1. Create a virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. Install in development mode:
   ```
   pip install -e .
   ```

3. Install optional dependencies for improved SSH transfers:
   ```
   # For progress display during transfers
   sudo apt install pv       # Debian/Ubuntu
   sudo dnf install pv       # Fedora/RHEL
   brew install pv           # macOS with Homebrew
   
   # For buffered transfers (optional)
   sudo apt install mbuffer  # Debian/Ubuntu
   sudo dnf install mbuffer  # Fedora/RHEL
   brew install mbuffer      # macOS with Homebrew
   ```

4. Run the application:
   ```
   python -m btrfs_backup_ng --help
   ```

5. For SSH transfers, ensure proper setup:
   ```
   # Make the standalone transfer script executable
   chmod +x btrfs-ssh-send
   
   # Run SSH transfers with agent forwarding when using sudo
   sudo SSH_AUTH_SOCK=$SSH_AUTH_SOCK ./btrfs-ssh-send [options] source destination
   ```

### Running tests

Tests can be run using pytest:

```
pytest
```

### Code style

This project follows PEP 8 guidelines. Code should be formatted with black and linted with flake8.

## Building

To build the package, use:

```
python -m build
```

This will create both source and wheel distributions in the `dist/` directory.

## Documentation

The code is self-documented with docstrings. You can generate documentation using Sphinx.

## Technical Implementation Details

### SSH Transfer Process

The SSH transfer functionality works through multiple components:

1. **SSHEndpoint Class**: Handles SSH connections and command execution
   - Manages SSH identity files and authentication
   - Implements the BTRFS send/receive protocol over SSH

2. **Master Connection Manager**: Optimizes SSH connections using control sockets
   - Maintains persistent SSH connections for improved performance
   - Handles connection cleanup and error recovery

3. **Standalone Transfer Script**: Provides a direct interface for transfers
   - Implements progress display using pv or mbuffer
   - Performs pre-transfer verification of remote filesystem type
   - Implements post-transfer verification to ensure snapshot creation
   - Uses fallback mechanisms for maximum compatibility

### BTRFS Command Handling

The application carefully manages BTRFS commands:
- Properly handles path differences between user and root environments
- Automatically elevates privileges when needed
- Uses direct command path resolution to avoid PATH issues
- Implements comprehensive error handling for BTRFS operations

## Contributions

Contributions are welcome! Please ensure that your code passes all tests and follows the project's coding style.