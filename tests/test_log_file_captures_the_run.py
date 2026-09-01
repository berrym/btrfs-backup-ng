"""`log_file` must record what the run actually did.

The shared logger is `logging.Logger("btrfs-backup-ng")` -- a standalone instance
named with HYPHENS, constructed directly rather than via getLogger, so it is not
registered in the logging manager and nothing can be its child. Meanwhile 36
modules across cli/ and core/ use `logging.getLogger(__name__)`, which lives
under `btrfs_backup_ng` with UNDERSCORES.

Those are unrelated trees. A file handler attached only to the shared logger
therefore never saw a single line from run, transfer, restore or operations --
so `log_file` recorded a fraction of the run while looking complete, which is
the worst way for a log to be wrong. An operator reading it after a failure was
missing most of the story.

The handler is now attached to the package-root logger as well.
"""

from __future__ import annotations

import logging

import pytest

from btrfs_backup_ng.__logger__ import (
    add_file_handler,
    logger as shared_logger,
    remove_file_handler,
)


@pytest.fixture(autouse=True)
def _restore_package_logger():
    """Leave the package logger exactly as it was found.

    These tests attach handlers to `btrfs_backup_ng` and set its level. Without
    restoring both, every later test that reads INFO records through caplog
    loses them -- which is the same "file logging quietly changed the rest of
    the process" defect these tests exist to prevent, reproduced in the tests
    themselves. It cost nine unrelated failures that all passed in isolation.
    """
    package_logger = logging.getLogger("btrfs_backup_ng")
    level = package_logger.level
    handlers = list(package_logger.handlers)
    try:
        yield
    finally:
        package_logger.setLevel(level)
        package_logger.handlers[:] = handlers


@pytest.fixture
def log_file(tmp_path):
    path = tmp_path / "run.log"
    add_file_handler(str(path))
    yield path
    remove_file_handler()


class TestBothLoggerTreesReachTheFile:
    def test_the_shared_logger_is_captured(self, log_file):
        shared_logger.warning("FROM-SHARED")
        assert "FROM-SHARED" in log_file.read_text()

    @pytest.mark.parametrize(
        "name",
        [
            "btrfs_backup_ng.cli.run",
            "btrfs_backup_ng.cli.transfer",
            "btrfs_backup_ng.core.operations",
            "btrfs_backup_ng.core.restore",
            "btrfs_backup_ng.endpoint.ssh",
        ],
    )
    def test_a_package_module_logger_is_captured(self, log_file, name):
        logging.getLogger(name).warning("FROM-%s", name)
        assert f"FROM-{name}" in log_file.read_text(), (
            f"{name} logs to a tree the file handler never saw, so log_file "
            f"silently omits everything that module reports"
        )

    def test_info_and_debug_are_captured_not_just_warnings(self, log_file):
        """The file handler defaults to DEBUG. Without raising the package
        logger's level, its effective level (inherited from root, typically
        WARNING) filters these out before the handler is reached."""
        logging.getLogger("btrfs_backup_ng.cli.run").info("AN-INFO-LINE")
        logging.getLogger("btrfs_backup_ng.cli.run").debug("A-DEBUG-LINE")
        text = log_file.read_text()
        assert "AN-INFO-LINE" in text
        assert "A-DEBUG-LINE" in text


class TestTheHandlerIsDetachedCleanly:
    def test_removal_detaches_from_both_trees(self, tmp_path):
        path = tmp_path / "a.log"
        add_file_handler(str(path))
        remove_file_handler()
        logging.getLogger("btrfs_backup_ng.cli.run").warning("AFTER-REMOVAL")
        shared_logger.warning("AFTER-REMOVAL-SHARED")
        text = path.read_text() if path.exists() else ""
        assert "AFTER-REMOVAL" not in text, (
            "a removed handler still receives records through the package "
            "logger; it was closed, so this writes to a closed file"
        )
        assert "AFTER-REMOVAL-SHARED" not in text

    def test_replacing_the_handler_does_not_double_attach(self, tmp_path):
        first = tmp_path / "first.log"
        second = tmp_path / "second.log"
        add_file_handler(str(first))
        add_file_handler(str(second))
        try:
            logging.getLogger("btrfs_backup_ng.cli.run").warning("ONLY-SECOND")
            assert "ONLY-SECOND" in second.read_text()
            assert "ONLY-SECOND" not in (first.read_text() if first.exists() else "")
        finally:
            remove_file_handler()

    def test_no_duplicate_lines_from_a_single_record(self, log_file):
        """The record must not be written once per attached tree."""
        logging.getLogger("btrfs_backup_ng.cli.run").warning("EXACTLY-ONCE")
        assert log_file.read_text().count("EXACTLY-ONCE") == 1


class TestTheWarningsAreEmittedAfterTheHandlerExists:
    """Config warnings are produced during load, before the file handler is
    installed from that same config. Logged in the original order they reached
    the console only."""

    @pytest.mark.parametrize("module", ["run", "transfer"])
    def test_the_file_handler_is_installed_first(self, module):
        import inspect

        import btrfs_backup_ng.cli.run as run_mod
        import btrfs_backup_ng.cli.transfer as transfer_mod

        mod = {"run": run_mod, "transfer": transfer_mod}[module]
        source = inspect.getsource(mod)
        handler_at = source.index("add_file_handler(")
        warn_at = source.index('logger.warning("Config: %s"')
        assert handler_at < warn_at, (
            f"cli/{module}.py logs config warnings before installing the file "
            f"handler, so log_file omits them"
        )


class TestFileLoggingDoesNotLeakState:
    """Enabling a file log must not change anything else about the process.

    The package logger's level has to be raised so DEBUG and INFO records reach
    the handler, but leaving it raised makes console output more verbose for the
    rest of the run -- a side effect of file logging that nobody asked for.
    """

    def test_the_level_is_restored_on_removal(self, tmp_path):
        package_logger = logging.getLogger("btrfs_backup_ng")
        # Set a KNOWN starting level. Reading whatever the level happens to be
        # makes this pass when an earlier test in the same process already
        # leaked it -- the assertion then compares a leaked value to itself.
        package_logger.setLevel(logging.WARNING)
        before = package_logger.level

        add_file_handler(str(tmp_path / "x.log"))
        raised = package_logger.level
        remove_file_handler()

        assert raised <= logging.DEBUG, "the handler would not see DEBUG records"
        assert package_logger.level == before, (
            f"file logging left the package logger at {package_logger.level} "
            f"instead of {before}; console verbosity is changed for the rest of "
            f"the process"
        )

    def test_repeated_enable_disable_does_not_drift(self, tmp_path):
        package_logger = logging.getLogger("btrfs_backup_ng")
        package_logger.setLevel(logging.WARNING)
        before = package_logger.level
        for i in range(3):
            add_file_handler(str(tmp_path / f"r{i}.log"))
            remove_file_handler()
        assert package_logger.level == before


class TestOnlyOurOwnLoggersAreCaptured:
    def test_a_third_party_logger_does_not_leak_into_the_file(self, log_file):
        """The handler is attached to our package logger, not to root, so an
        unrelated library's output must not end up in the user's backup log."""
        logging.getLogger("paramiko.transport").warning("THIRD-PARTY")
        logging.getLogger("urllib3.connectionpool").warning("ALSO-THIRD-PARTY")
        logging.getLogger("btrfs_backup_ng.cli.run").warning("OURS")

        text = log_file.read_text()
        assert "OURS" in text
        assert "THIRD-PARTY" not in text
        assert "ALSO-THIRD-PARTY" not in text
