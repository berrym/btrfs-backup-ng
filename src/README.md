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

3. Run the application:
   ```
   python -m btrfs_backup_ng --help
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

## Contributions

Contributions are welcome! Please ensure that your code passes all tests and follows the project's coding style.