"""Lowering the shared logger's level after raising it takes effect.

The endpoints log through ``btrfs_backup_ng.__logger__.logger``. It was built
as a bare ``logging.Logger`` -- unknown to the logging manager, and the
manager is what clears every logger's ``isEnabledFor`` cache when a level
changes. So after ``[global] quiet`` set it to WARNING and one INFO record
cached "disabled", lowering the level again (a log file added later at DEBUG,
verbose, a test resetting it) changed ``level`` and nothing else: every INFO
line from the endpoints stayed dropped. In the suite this surfaced as a test
that failed only after ``prune --dry-run`` had run under ``quiet = true`` in
the same process.
"""

from __future__ import annotations

import logging

import pytest

from btrfs_backup_ng import __logger__


@pytest.fixture(autouse=True)
def _restore_logging():
    yield
    __logger__.remove_file_handler()
    __logger__.create_logger(False, level="INFO")


def test_the_shared_logger_is_known_to_the_manager():
    assert logging.getLogger(__logger__.logger.name) is __logger__.logger


def test_lowering_the_level_after_an_info_was_refused_enables_info_again():
    lg = __logger__.logger
    lg.setLevel(logging.WARNING)
    assert not lg.isEnabledFor(logging.INFO)  # caches the refusal
    lg.setLevel(logging.INFO)
    assert lg.isEnabledFor(logging.INFO)


def test_quiet_then_a_lower_console_level_delivers_info_records(shared_log):
    """Through the real setters: what a config's ``quiet`` does, then what a
    later ``verbose`` (or a reset) does."""
    lg = __logger__.logger
    __logger__.set_console_level(logging.WARNING)
    lg.info("dropped, as quiet asks")
    __logger__.set_console_level(logging.INFO)
    lg.info("delivered")
    assert shared_log.messages() == ["delivered"]


def test_quiet_does_not_thin_a_debug_log_file_for_the_endpoints(tmp_path):
    """``quiet`` is "less on the screen", never "less in the record". With the
    console at WARNING and a file handler at DEBUG the shared logger's level
    is the file's, and an INFO line from an endpoint must reach the file --
    including after an INFO was refused while the console alone decided."""
    lg = __logger__.logger
    __logger__.create_logger(False, level="INFO")
    __logger__.set_console_level(logging.WARNING)
    lg.info("refused while only the console decides")
    log_file = tmp_path / "run.log"
    __logger__.add_file_handler(str(log_file), level="DEBUG")
    __logger__.set_console_level(logging.WARNING)
    lg.info("an endpoint line for the record")
    for handler in list(lg.handlers):
        handler.flush()
    assert "an endpoint line for the record" in log_file.read_text()


def test_a_log_file_receives_endpoint_lines_whatever_the_console_level(tmp_path):
    """add_file_handler lowers the shared endpoint logger to the file's level
    and remove_file_handler puts it back. Without that the file's completeness
    depended on how the console had been set up: under -q every endpoint INFO
    line was missing from the record."""
    lg = __logger__.logger
    __logger__.create_logger(False, level="WARNING")  # what -q does
    assert lg.level == logging.WARNING
    log_file = tmp_path / "quiet.log"
    __logger__.add_file_handler(str(log_file), level="DEBUG")
    assert lg.level == logging.DEBUG
    lg.info("an endpoint INFO line")
    lg.debug("an endpoint DEBUG line")
    __logger__.remove_file_handler()
    assert lg.level == logging.WARNING, "the level is restored with the handler gone"
    text = log_file.read_text()
    assert "an endpoint INFO line" in text
    assert "an endpoint DEBUG line" in text
    assert __logger__.rich_handler.level == logging.WARNING, "the console is unchanged"
