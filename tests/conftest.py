"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import functools
import logging
import os
import shutil
import subprocess
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

# tests/test_skip_probes_are_bounded.py imports this file as a top-level
# module, where a relative import has no package.
try:
    from .toolmarks import KNOWN_TOOLS
except ImportError:
    from toolmarks import KNOWN_TOOLS

# Which missing tools fail the tests that need them instead of skipping them:
# "1" for every tool, or a comma-separated list of names, as a package build
# lists what it installed. Unset, empty or "0", every missing tool skips.
REQUIRE_TOOLS_ENV = "BBNG_TEST_REQUIRE_TOOLS"

# PATH as the run started. The lookups below must not see the stand-ins a
# fixture puts first on PATH for each test, whichever hook runs first.
_PATH_AT_START = os.environ.get("PATH", os.defpath)

_REQUIRED = pytest.StashKey[tuple[bool, frozenset[str]]]()
_UNUSED = pytest.StashKey[frozenset[str]]()


def _tool_is_installed(tool: str) -> bool:
    return shutil.which(tool, path=_PATH_AT_START) is not None


def _parse_required(value: str) -> tuple[bool, frozenset[str]]:
    """``(every tool, the named tools)`` that ``BBNG_TEST_REQUIRE_TOOLS`` asks for."""
    value = value.strip()
    if value in ("", "0"):
        return False, frozenset()
    if value == "1":
        return True, frozenset()
    return False, frozenset(name.strip() for name in value.split(",") if name.strip())


def pytest_configure(config: pytest.Config) -> None:
    every, named = _parse_required(os.environ.get(REQUIRE_TOOLS_ENV, ""))
    unknown = named - KNOWN_TOOLS
    if unknown:
        raise pytest.UsageError(
            f"{REQUIRE_TOOLS_ENV} names {', '.join(sorted(unknown))}, which no test "
            f"can require; the tools tests may require are listed in "
            f"tests/toolmarks.py: {', '.join(sorted(KNOWN_TOOLS))}"
        )
    config.stash[_REQUIRED] = (every, named)
    config.stash[_UNUSED] = frozenset()


def pytest_report_header(config: pytest.Config) -> str | None:
    every, named = config.stash[_REQUIRED]
    if every:
        return f"{REQUIRE_TOOLS_ENV}: every tool a test requires must be installed"
    if named:
        return f"{REQUIRE_TOOLS_ENV}: {', '.join(sorted(named))}"
    return None


def _is_required(config: pytest.Config, tool: str) -> bool:
    every, named = config.stash[_REQUIRED]
    return every or tool in named


def _tools_of(item: pytest.Item) -> Iterator[tuple[str, str]]:
    """``(tool, reason)`` for each ``requires_tool`` mark on ``item``."""
    for marker in item.iter_markers("requires_tool"):
        (tool,) = marker.args
        yield tool, marker.kwargs.get("reason", f"{tool} is not installed")


def _verdict(item: pytest.Item, tool: str, reason: str) -> tuple[str, str] | None:
    """The one decision: None where the tool is installed, ``("fail", why)``
    where it is missing and required, ``("skip", why)`` where it is missing."""
    if _tool_is_installed(tool):
        return None
    if _is_required(item.config, tool):
        return "fail", f"{reason}, and {REQUIRE_TOOLS_ENV} requires it"
    return "skip", reason


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Skip a test whose ``@pytest.mark.requires_tool(name)`` is missing.

    A test that runs an external program for real proves nothing where the
    program is missing. Skipping through a mark reports the reason at the test
    itself in the ``-rs`` summary. A test is not skipped when any of its tools
    is missing and required: ``pytest_runtest_setup`` fails it instead, so a
    skip for one missing tool cannot hide a required one on the same test.
    """
    used: set[str] = set()
    for item in items:
        verdicts = [_verdict(item, tool, reason) for tool, reason in _tools_of(item)]
        used.update(tool for tool, _ in _tools_of(item))
        if any(v is not None and v[0] == "fail" for v in verdicts):
            continue
        for v in verdicts:
            if v is not None:
                item.add_marker(pytest.mark.skip(reason=v[1]))
                break
    unregistered = used - KNOWN_TOOLS
    if unregistered:
        raise pytest.UsageError(
            f"requires_tool names {', '.join(sorted(unregistered))}, which "
            f"tests/toolmarks.py does not list in KNOWN_TOOLS; add it there"
        )
    config.stash[_UNUSED] = config.stash[_REQUIRED][1] - used


def pytest_terminal_summary(
    terminalreporter: pytest.TerminalReporter, config: pytest.Config
) -> None:
    unused = config.stash.get(_UNUSED, frozenset())
    if unused:
        terminalreporter.write_line(
            f"note: {REQUIRE_TOOLS_ENV} names {', '.join(sorted(unused))}, which no "
            "selected test requires"
        )


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Fail a test whose tool is missing when ``BBNG_TEST_REQUIRE_TOOLS`` requires it."""
    for tool, reason in _tools_of(item):
        verdict = _verdict(item, tool, reason)
        if verdict is not None and verdict[0] == "fail":
            pytest.fail(verdict[1], pytrace=False)


@pytest.fixture
def tool_unusable(request: pytest.FixtureRequest) -> Callable[[str, str], None]:
    """Skip a test whose tool is installed but cannot be used here, or fail it
    when ``BBNG_TEST_REQUIRE_TOOLS`` requires that tool."""

    def unusable(tool: str, reason: str) -> None:
        if _is_required(request.config, tool):
            pytest.fail(
                f"{reason}, and {REQUIRE_TOOLS_ENV} requires {tool}", pytrace=False
            )
        pytest.skip(reason)

    return unusable


def _in_a_real_tier(node: pytest.Item) -> bool:
    """Whether this test runs against real btrfs and real privilege."""
    return any(node.get_closest_marker(tier) for tier in ("tier2", "tier3"))


# Takes what the product passes to a local sudo: no option, `-n`, or `--`. An
# option it does not know, or a VAR=value word, is written down and makes the
# test fail, so a new form is decided on rather than guessed at, and a refusal
# can never pass for an answer.
_SUDO_STAND_IN = """\
#!/bin/sh
# Test stand-in for sudo: runs the command as the calling user, never as root.
while [ $# -gt 0 ]; do
    case "$1" in
        -n) shift ;;
        --) shift; break ;;
        *) break ;;
    esac
done
case "$1" in
    ''|-*|*=*)
        printf '%s\\n' "sudo $*" >> "$BBNG_SUDO_STAND_IN_UNSUPPORTED"
        echo "sudo stand-in: cannot run 'sudo $*'" >&2
        exit 1
        ;;
esac
# Under the sudo_refuses fixture: what sudo -n says and returns to a user it
# has no rule for, with the command recorded so the test can see it was asked.
if [ -n "$BBNG_SUDO_STAND_IN_REFUSES" ]; then
    printf '%s\\n' "$*" >> "$BBNG_SUDO_STAND_IN_REFUSES"
    echo "sudo: a password is required" >&2
    exit 1
fi
# What sudo itself says and returns for a command it cannot find.
if ! command -v "$1" >/dev/null 2>&1; then
    echo "sudo: $1: command not found" >&2
    exit 1
fi
exec "$@"
"""

_BTRFS_PLACEHOLDER = """\
#!/bin/sh
# Test placeholder for a btrfs this machine does not have: a lookup finds it,
# and running it fails as a missing command does.
echo "btrfs: not installed (placeholder from tests/conftest.py)" >&2
exit 127
"""


@pytest.fixture(scope="session")
def stand_in_bin(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The directory unit tests put first on PATH, checked to actually run."""
    bin_dir = tmp_path_factory.mktemp("stand-in-bin")
    scripts = {"sudo": _SUDO_STAND_IN}
    if shutil.which("btrfs") is None:
        scripts["btrfs"] = _BTRFS_PLACEHOLDER
    for name, text in scripts.items():
        (bin_dir / name).write_text(text)
        (bin_dir / name).chmod(0o755)
    # A temporary directory mounted noexec would leave the stand-in unrunnable,
    # and the product's fallbacks would read that as a refused sudo in every
    # test. Find out once, here, instead.
    sudo = bin_dir / "sudo"
    probe = subprocess.run(
        [str(sudo), "-n", "id", "-u"], capture_output=True, text=True
    )
    if probe.returncode != 0 or probe.stdout.strip() != str(os.getuid()):
        pytest.fail(
            f"the sudo stand-in at {sudo} does not run: {probe.stderr.strip()}",
            pytrace=False,
        )
    return bin_dir


@pytest.fixture(autouse=True)
def unit_tests_use_the_stand_ins(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    stand_in_bin: Path,
) -> Iterator[None]:
    """Put the stand-in directory first on PATH for every unit test.

    sudo: the product retries a privileged call through ``sudo -n`` when the
    direct one fails, and in a unit test the direct one usually fails, because
    the temporary directory is not btrfs. Those retries reached the real sudo
    about 250 times a run: as root wherever sudo needs no password, as a
    refusal wherever it does, and as "command not found" in a build root
    without it. The stand-in gives every machine the same answer, and none of
    it as root.

    btrfs, only on a machine without it: ``LocalEndpoint`` refuses to start
    when ``shutil.which("btrfs")`` finds nothing, so every unit test that
    builds one failed there, although none of them runs btrfs. The placeholder
    satisfies the lookup, in the test and in any CLI it starts, and fails like
    a missing command if run. Where btrfs is installed it is used as before.
    Tests that run btrfs only to see it fail (about 80 of them, on paths that
    are not subvolumes) get the placeholder's failure instead of btrfs-progs'
    and are not affected. A test that must see btrfs-progs' own behaviour marks
    ``requires_tool("btrfs")``, which is judged against the PATH the run
    started with; a test about the missing-btrfs path patches ``shutil.which``
    itself.

    Tier 2 and tier 3 use real btrfs and real privilege on purpose and keep
    the real tools.
    """
    if _in_a_real_tier(request.node):
        yield
        return
    unsupported = stand_in_bin / "unsupported"
    unsupported.write_text("")
    monkeypatch.setenv("BBNG_SUDO_STAND_IN_UNSUPPORTED", str(unsupported))
    monkeypatch.setenv(
        "PATH", f"{stand_in_bin}{os.pathsep}{os.environ.get('PATH', '')}"
    )
    yield
    calls = unsupported.read_text()
    if calls:
        pytest.fail(
            "the sudo stand-in was asked for something it does not take; teach it "
            f"the option in tests/conftest.py or avoid it:\n{calls}",
            pytrace=False,
        )


@pytest.fixture
def sudo_refuses(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Callable[[], list[str]]:
    """Run the test as a user whose sudo refuses, as ``sudo -n`` does without a rule.

    The stand-in otherwise always agrees, so the product's refused-sudo paths
    run only where a test asks for them, here. The process is also presented as
    non-root, because root never asks sudo. Returns a function listing the
    commands the stand-in refused, so a test can show the refusal was reached.
    """
    record = tmp_path / "sudo-refused"
    record.write_text("")
    monkeypatch.setenv("BBNG_SUDO_STAND_IN_REFUSES", str(record))
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    return lambda: record.read_text().splitlines()


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
def heartbeats_end_with_the_test(monkeypatch):
    """Stop every lock heartbeat a test started when the test ends.

    A pin taken in a lock file keeps its holder record past the exit
    cleanups, and the record's heartbeat thread beats until the process
    exits; in the suite that is the rest of the session, and a test that
    forks then runs in a multi-threaded process (``os.fork`` warns, and may
    deadlock). The beats are tracked by wrapping the method that starts
    them, so a test need not know which manager a pin went through.
    """
    from btrfs_backup_ng.sshutil.lock import RemoteLockManager

    started: list = []
    real_start = RemoteLockManager._start_heartbeat

    def tracked(self, key, *args, **kwargs):
        started.append((self, key))
        return real_start(self, key, *args, **kwargs)

    monkeypatch.setattr(RemoteLockManager, "_start_heartbeat", tracked)
    yield
    for manager, key in started:
        manager._stop_heartbeat(key)


@pytest.fixture(autouse=True)
def the_umask_the_product_creates_under():
    """The lock store refuses a directory its group or everyone can write
    (``sshutil.lock.STORE_FUNCTIONS``). The product makes its own
    directories 0755 whatever the umask, but a test that stands a store up
    with a bare ``mkdir`` gets the umask's mode: 0775 under the 002 a Fedora
    user's login shell sets, which that test would then be refused for while
    passing under 022. The suite runs under the umask the product's own
    directories end up with, so its verdicts do not depend on who runs it."""
    old = os.umask(0o022)
    yield
    os.umask(old)


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


@pytest.fixture(autouse=True)
def the_stop_flag_ends_with_the_test():
    """Clear the process-wide stop flag when a test ends.

    The program never clears it: once set, nothing new starts and nothing is
    retried for the rest of the process. A test that requests the stop would
    otherwise make every later test in the session refuse its work.
    """
    from btrfs_backup_ng import lifecycle

    yield
    lifecycle._STOP.clear()


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
