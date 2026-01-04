"""Pytest configuration and shared fixtures."""

import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_config_dir(tmp_path):
    """Create a temporary config directory."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    return config_dir


@pytest.fixture
def sample_config_toml():
    """Return a sample valid TOML configuration string."""
    return """
[global]
snapshot_dir = ".snapshots"
timestamp_format = "%Y%m%d-%H%M%S"
incremental = true
parallel_volumes = 2
parallel_targets = 3

[global.retention]
min = "1d"
hourly = 24
daily = 7
weekly = 4
monthly = 12
yearly = 0

[[volumes]]
path = "/home"
snapshot_prefix = "home-"

[[volumes.targets]]
path = "/mnt/backup/home"

[[volumes.targets]]
path = "ssh://backup@server:/backups/home"
ssh_sudo = true
compress = "zstd"
rate_limit = "10M"

[[volumes]]
path = "/var/log"
snapshot_prefix = "logs-"
enabled = true

[volumes.retention]
daily = 14
weekly = 8

[[volumes.targets]]
path = "/mnt/backup/logs"
"""


@pytest.fixture
def minimal_config_toml():
    """Return a minimal valid TOML configuration string."""
    return """
[[volumes]]
path = "/home"

[[volumes.targets]]
path = "/mnt/backup"
"""


@pytest.fixture
def sample_btrbk_config():
    """Return a sample btrbk configuration string."""
    return """
# btrbk configuration file

snapshot_preserve_min   2d
snapshot_preserve       14d 4w 6m

target_preserve_min     2d
target_preserve         14d 4w 6m

ssh_identity            /root/.ssh/backup_key

volume /mnt/btr_pool
  snapshot_dir .snapshots

  subvolume home
    target /mnt/backup/home
    target ssh://backup@nas/backups/home
      backend btrfs-progs-sudo

  subvolume var/log
    snapshot_preserve 7d 2w
    target /mnt/backup/var-log
"""


@pytest.fixture
def config_file(tmp_config_dir, sample_config_toml):
    """Create a temporary config file with sample content."""
    config_path = tmp_config_dir / "config.toml"
    config_path.write_text(sample_config_toml)
    return config_path


@pytest.fixture
def minimal_config_file(tmp_config_dir, minimal_config_toml):
    """Create a temporary config file with minimal content."""
    config_path = tmp_config_dir / "minimal.toml"
    config_path.write_text(minimal_config_toml)
    return config_path
