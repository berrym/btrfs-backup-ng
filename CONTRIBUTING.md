# Contributing to btrfs-backup-ng

Thank you for your interest in contributing to btrfs-backup-ng!

## Development Setup

```bash
# Clone the repository
git clone https://github.com/berrym/btrfs-backup-ng.git
cd btrfs-backup-ng

# Install in development mode with dev dependencies
pip install -e ".[dev]"
```

## Code Quality

We use several tools to maintain code quality:

```bash
# Run linter
ruff check src/ tests/

# Run formatter
ruff format src/ tests/

# Run type checker
mypy src/btrfs_backup_ng --ignore-missing-imports
```

## Testing

### Test Tiers

Tests are organized into two tiers:

| Tier | Description | Requirements | CI |
|------|-------------|--------------|-----|
| **Tier 1** | Unit tests and mocked integration tests | Python only | Every push |
| **Tier 2** | Real btrfs integration tests | btrfs-progs, root, Linux | Weekly/manual |

### Running Tier 1 Tests (Default)

Standard tests run without special privileges:

```bash
# Run all tier 1 tests
pytest

# With coverage
pytest --cov=src/btrfs_backup_ng --cov-report=term-missing

# Specific test file
pytest tests/test_config_loader.py -v
```

### Running Tier 2 Tests (btrfs Integration)

Tier 2 tests require:
- Linux with btrfs kernel support
- `btrfs-progs` package installed
- Root privileges (for mount/loopback operations)

```bash
# Run tier 2 tests (requires root)
sudo pytest -m tier2 tests/integration/tier2/ -v

# Run ALL tests including tier 2
sudo pytest -m "" tests/ -v
```

#### Tier 2 Test Infrastructure

Tier 2 tests use loopback-mounted btrfs filesystems:

1. Creates sparse image files (256MB each)
2. Formats them as btrfs
3. Mounts via loopback devices
4. Runs tests with real btrfs operations
5. Cleans up automatically

This allows testing actual `btrfs send/receive`, snapshots, and subvolume operations without modifying your real filesystems.

#### Running Tier 2 in a Container

For safer tier 2 testing, use a privileged container:

```bash
# Using Podman (Fedora/RHEL)
podman run --rm -it --privileged \
    -v $(pwd):/workspace:Z \
    fedora:latest \
    bash -c "dnf install -y btrfs-progs python3 python3-pip && \
             cd /workspace && \
             pip install -e '.[test]' && \
             pytest -m tier2 tests/integration/tier2/ -v"

# Using Docker
docker run --rm -it --privileged \
    -v $(pwd):/workspace \
    fedora:latest \
    bash -c "dnf install -y btrfs-progs python3 python3-pip && \
             cd /workspace && \
             pip install -e '.[test]' && \
             pytest -m tier2 tests/integration/tier2/ -v"
```

#### What Tier 2 Tests Cover

- `test_btrfs_operations.py`: Core btrfs subvolume, snapshot, send/receive operations
- `test_endpoints_real.py`: LocalEndpoint class with real btrfs filesystems

### Test Coverage

Current coverage targets:
- Tier 1: ~63% of testable code
- Goal: 70%+ with additional mocking

Many modules require real btrfs operations and are excluded from tier 1 coverage metrics but covered by tier 2 tests.

## Project Structure

```
src/btrfs_backup_ng/
├── __main__.py          # Entry point
├── __util__.py          # Common utilities
├── __logger__.py        # Logging infrastructure
├── cli/                 # CLI subcommands
│   ├── dispatcher.py    # Command routing
│   ├── run.py          # Main backup command
│   ├── restore.py      # Restore command
│   └── ...
├── config/              # Configuration handling
│   ├── loader.py       # TOML loading
│   └── schema.py       # Validation
├── core/                # Core backup logic
│   ├── operations.py   # Backup operations
│   ├── restore.py      # Restore logic
│   └── ...
└── endpoint/            # Backup targets
    ├── local.py        # Local filesystem
    └── ssh.py          # SSH remote

tests/
├── test_*.py           # Tier 1 unit tests
└── integration/
    ├── test_*.py       # Tier 1 integration tests (mocked)
    └── tier2/          # Tier 2 real btrfs tests
        ├── conftest.py # Loopback btrfs fixtures
        └── test_*.py   # Real btrfs tests
```

## Pull Request Guidelines

1. **Tests**: Add tests for new functionality
2. **Type hints**: Include type annotations for new code
3. **Linting**: Ensure `ruff check` passes
4. **Formatting**: Run `ruff format` before committing
5. **Documentation**: Update README/docstrings as needed

## CI Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `ci.yml` | Every push/PR | Tier 1 tests, linting, type checking |
| `integration-tier2.yml` | Weekly, manual, tier2 file changes | Real btrfs integration tests |

## Reporting Issues

Please include:
- Python version (`python --version`)
- btrfs-progs version (`btrfs --version`)
- Linux kernel version (`uname -r`)
- Configuration file (sanitized)
- Full error output
