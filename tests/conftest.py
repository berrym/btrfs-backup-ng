"""Pytest configuration and shared fixtures."""

import functools
import logging
import subprocess
from collections.abc import Iterator

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
                # The probe spent 3.1 of its ~4 seconds on a gssapi-with-mic
                # attempt that always fails here, and that attempt is the part
                # whose duration varies -- it can block far longer than the rest
                # of the handshake. Skipping it takes the probe from ~4s to ~0.2s,
                # which is what makes the timeout below a wide margin rather than
                # a coin toss under load. Nothing this probe answers depends on
                # gssapi: the tests authenticate by key.
                "-o",
                "GSSAPIAuthentication=no",
                "localhost",
                "true",
            ],
            capture_output=True,
            # stdin is NOT covered by capture_output, so it stays attached to
            # whatever pytest was given. ssh reads from it, never sees EOF, and
            # the probe times out on a host where localhost ssh works -- silently
            # skipping every test that asks for it. `-n` covers the same ground;
            # both are set because the failure mode of this check is silence.
            stdin=subprocess.DEVNULL,
            # Measured: ~0.2s warm or cold with gssapi skipped, ~4s with it. This
            # is paid at most once per run, and only if a test actually asks.
            timeout=30,
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


class SharedLogCapture(logging.Handler):
    """Every record the shared logger handled while this was attached to it."""

    def __init__(self) -> None:
        super().__init__(level=logging.NOTSET)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def messages(self, level: int = logging.NOTSET) -> list[str]:
        """The message of each record logged at ``level`` or above."""
        return [r.getMessage() for r in self.records if r.levelno >= level]


@pytest.fixture
def shared_log() -> Iterator[SharedLogCapture]:
    """Capture what the shared logger handles during the test.

    The endpoints, the dispatcher and a few other modules log through
    ``btrfs_backup_ng.__logger__.logger``, which does not propagate. Neither
    of pytest's own captures sees its records on every pytest version:

    - ``capsys`` sees them only through ``logging.lastResort``, which writes
      WARNING and above to stderr and runs only when the logger has no
      handler at all. From 9.1 on, pytest attaches its handlers to every
      registered logger that does not propagate, so lastResort never runs and
      stderr is empty.
    - ``caplog`` sees them only from pytest 9.1 on. Before that its handler
      is on the root logger alone, which these records never reach.

    A handler the test attaches itself sees them on every version. The logger
    is opened to DEBUG for the test, since an earlier test may have left it
    at any level; a test that depends on the level a message was logged at
    reads ``messages(level)``. ``create_logger`` clears the logger's
    handlers, so a test that calls it must patch it out.
    """
    from btrfs_backup_ng.__logger__ import logger as shared

    capture = SharedLogCapture()
    level = shared.level
    shared.addHandler(capture)
    shared.setLevel(logging.DEBUG)
    try:
        yield capture
    finally:
        # Only this handler is removed: from 9.1 on, pytest adds and removes
        # its own on the logger at each test phase.
        shared.removeHandler(capture)
        shared.setLevel(level)


@pytest.fixture(autouse=True)
def exit_cleanups_end_with_the_test():
    """Run, when a test ends, every exit cleanup it registered.

    A test that takes a lock or pin and does not release it leaves an exit
    cleanup registered (and a heartbeat thread running) for the rest of the
    session, where it could act on another test's state. Running the test's
    own entries at its end keeps each test's locks inside the test.
    """
    from btrfs_backup_ng import lifecycle

    with lifecycle._LOCK:
        before = set(lifecycle._CLEANUPS)
    yield
    lifecycle.run_cleanups(lambda key: key not in before)


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
