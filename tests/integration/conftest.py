"""Shared fixtures for integration tests."""

import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_btrfs_commands():
    """Mock all btrfs subprocess calls.

    Returns a dict tracking all commands that would have been executed.
    """
    executed_commands = []

    def mock_run(cmd, *args, **kwargs):
        executed_commands.append(
            {
                "cmd": cmd,
                "args": args,
                "kwargs": kwargs,
            }
        )

        # Return appropriate mock responses based on command
        result = MagicMock()
        result.returncode = 0
        result.stdout = b""
        result.stderr = b""

        cmd_str = " ".join(str(c) for c in cmd) if isinstance(cmd, list) else str(cmd)

        # Handle btrfs subvolume list
        if "btrfs subvolume list" in cmd_str:
            result.stdout = b""  # Empty list by default

        # Handle btrfs subvolume show
        elif "btrfs subvolume show" in cmd_str:
            result.stdout = b"Name: test-snapshot\nUUID: abc-123\n"

        return result

    def mock_check_output(cmd, *args, **kwargs):
        executed_commands.append(
            {
                "cmd": cmd,
                "args": args,
                "kwargs": kwargs,
            }
        )

        cmd_str = " ".join(str(c) for c in cmd) if isinstance(cmd, list) else str(cmd)

        # Handle btrfs subvolume list
        if "btrfs subvolume list" in cmd_str:
            return b""

        return b""

    with patch("subprocess.run", side_effect=mock_run):
        with patch("subprocess.check_output", side_effect=mock_check_output):
            with patch(
                "subprocess.check_call",
                side_effect=lambda cmd, **kw: executed_commands.append(
                    {"cmd": cmd, "kwargs": kw}
                ),
            ):
                yield executed_commands


@pytest.fixture
def sample_config_dict():
    """Sample configuration as a dictionary (before TOML parsing)."""
    return {
        "global": {
            "snapshot_dir": ".snapshots",
            "timestamp_format": "%Y%m%d-%H%M%S",
            "incremental": True,
            "retention": {
                "min": "1d",
                "hourly": 24,
                "daily": 7,
                "weekly": 4,
                "monthly": 12,
            },
        },
        "volumes": [
            {
                "path": "/home",
                "snapshot_prefix": "home-",
                "targets": [
                    {"path": "/mnt/backup/home"},
                ],
            },
        ],
    }


@pytest.fixture
def mock_snapshot_list():
    """Generate a list of mock snapshot names with timestamps."""
    now = datetime.now()
    snapshots = []

    # Create snapshots over the past 30 days
    for days_ago in range(30):
        for hour in [6, 12, 18]:
            ts = now - timedelta(days=days_ago, hours=now.hour - hour)
            if ts < now:
                name = f"home-{ts.strftime('%Y%m%d-%H%M%S')}"
                snapshots.append(name)

    return sorted(snapshots, reverse=True)


@pytest.fixture
def mock_local_endpoint():
    """Create a mocked local endpoint."""
    endpoint = MagicMock()
    endpoint.path = Path("/mnt/data")
    endpoint.snapshot_dir = Path("/mnt/data/.snapshots")

    # Mock methods
    endpoint.list_snapshots.return_value = []
    endpoint.snapshot_exists.return_value = False
    endpoint.create_snapshot.return_value = True
    endpoint.delete_snapshot.return_value = True
    endpoint.send_snapshot.return_value = MagicMock()  # Returns a Popen-like object
    endpoint.receive_snapshot.return_value = True

    return endpoint


@pytest.fixture
def mock_ssh_endpoint():
    """Create a mocked SSH endpoint."""
    endpoint = MagicMock()
    endpoint.path = Path("/backups/home")
    endpoint.host = "backup-server"
    endpoint.user = "backup"
    endpoint.port = 22

    # Mock methods
    endpoint.list_snapshots.return_value = []
    endpoint.snapshot_exists.return_value = False
    endpoint.receive_snapshot.return_value = True
    endpoint.delete_snapshot.return_value = True
    endpoint.connect.return_value = True
    endpoint.disconnect.return_value = None

    return endpoint


@pytest.fixture
def temp_config_file(tmp_path, sample_config_dict):
    """Create a temporary TOML config file."""
    import tomli_w

    config_path = tmp_path / "config.toml"

    # Convert dict to TOML
    with open(config_path, "wb") as f:
        tomli_w.dump(sample_config_dict, f)

    return config_path


@pytest.fixture
def mock_filesystem(tmp_path):
    """Create a mock filesystem structure for testing."""
    # Create source volume structure
    source = tmp_path / "source"
    source.mkdir()
    (source / ".snapshots").mkdir()

    # Create some mock snapshot directories
    for i in range(5):
        snap_dir = source / ".snapshots" / f"home-2024011{i}-120000"
        snap_dir.mkdir()

    # Create backup destination
    backup = tmp_path / "backup"
    backup.mkdir()
    (backup / "home").mkdir()

    return {
        "source": source,
        "backup": backup,
        "snapshots_dir": source / ".snapshots",
    }
