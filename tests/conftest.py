"""Pytest configuration and shared fixtures."""

import functools
import logging
import subprocess

import pytest


@functools.cache
def ssh_localhost_works() -> bool:
    """Whether passwordless ssh to localhost is usable, decided ONCE.

    THE shared probe. Two copies of this existed: one with a subprocess timeout
    and one without, inline in a `skipif` expression. The one without could hang
    for minutes -- `ConnectTimeout` only bounds the TCP connect, so an ssh that
    connects and then stalls is unbounded -- and because a boolean `skipif`
    condition is evaluated when the module is IMPORTED, every pytest run paid it
    whether or not those tests were selected. Measured on a machine where
    localhost ssh stalls: 230 seconds of a 400-second suite, to decide to skip.

    Cached so a run pays it at most once, and only when something actually asks.
    """
    try:
        result = subprocess.run(
            [
                "ssh",
                "-n",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=3",
                # A shared connection makes this probe measure the wrong thing: it
                # either reuses a master whose state says nothing about a fresh
                # connection, or spawns one that outlives the probe.
                "-o",
                "ControlMaster=no",
                "-o",
                "ControlPath=none",
                "localhost",
                "true",
            ],
            capture_output=True,
            # stdin is NOT covered by capture_output, so it stays connected to
            # whatever pytest was given. ssh then reads from it and never sees
            # EOF, and the probe times out on a machine where localhost ssh works
            # perfectly -- silently skipping every test that asks for it. `-n`
            # covers the same ground; both are set because either alone is a
            # single point of failure for a check whose failure mode is silence.
            stdin=subprocess.DEVNULL,
            # A full handshake with no connection sharing measured ~5s here, so 8
            # left almost no headroom.
            timeout=20,
        )
        return result.returncode == 0
    except Exception:
        # Missing ssh, a stall that hit the timeout, anything else -- none of it
        # is the test's problem, and all of it means "cannot use localhost ssh".
        return False


@pytest.fixture
def requires_ssh_localhost():
    """Skip unless passwordless ssh to localhost works.

    A fixture rather than a `skipif` marker on purpose. A boolean skipif
    condition is evaluated when the module is IMPORTED, so the probe runs on
    every pytest invocation -- including ones that deselect the test entirely --
    and an ssh that connects and then stalls made that unbounded. A fixture runs
    at test SETUP, so a run that never reaches these tests never pays for them.
    """
    if not ssh_localhost_works():
        pytest.skip("passwordless ssh to localhost not available")


@pytest.fixture(autouse=True)
def reset_logging():
    """Reset logging handlers after each test to prevent pollution.

    Some tests (especially those calling CLI entry points like execute_restore)
    set up global logging handlers that can pollute stdout for subsequent tests.
    """
    yield
    # Reset the root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Reset the btrfs_backup_ng logger specifically
    try:
        import btrfs_backup_ng.__logger__ as logger_module

        if hasattr(logger_module, "logger"):
            logger_module.logger.handlers.clear()
    except ImportError:
        pass


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
