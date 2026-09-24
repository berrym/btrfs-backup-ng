"""`[global] quiet`, `verbose` and `btrfs_debug` reach the console.

The logger is created from the command line before the configuration is read,
so the three settings used to be parsed and never read: a config saying
`quiet = true` printed exactly what one without it did. These tests drive the
real command path (the dispatcher, a real config file) and check what reaches
the console and what reaches the log file.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import types

import pytest

from btrfs_backup_ng import __logger__
from btrfs_backup_ng.cli.common import config_log_level


@pytest.fixture(autouse=True)
def _restore_logging():
    yield
    __logger__.remove_file_handler()
    __logger__.create_logger(False, level="INFO")


def _config(tmp_path, *global_lines: str) -> str:
    (tmp_path / "src").mkdir(exist_ok=True)
    (tmp_path / "dst").mkdir(exist_ok=True)
    path = tmp_path / "config.toml"
    path.write_text(
        "[global]\n"
        + "".join(f"{line}\n" for line in global_lines)
        + f'\n[[volumes]]\npath = "{tmp_path / "src"}"\n'
        + f'\n[[volumes.targets]]\npath = "{tmp_path / "dst"}"\n'
    )
    return str(path)


def _run(capsys, *argv: str) -> str:
    """The console output of one real command, run as the operator runs it.

    A separate process, because the setting under test is process state (the
    console's level) and a command run in-process twice shares it -- and
    shares caches that change which lines are logged at all. Whitespace is
    collapsed: the console renderer wraps long lines at the terminal width.
    """
    env = {**os.environ, "COLUMNS": "200"}
    env.pop("BTRFS_BACKUP_LOG_LEVEL", None)
    result = subprocess.run(
        [sys.executable, "-m", "btrfs_backup_ng", *argv],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )
    return " ".join((result.stdout + result.stderr).split())


# `list` logs "Preparing endpoint" at INFO after the configuration is loaded,
# and "Creating non-SSH endpoint" at DEBUG, once per process.
INFO_AFTER_LOAD = "Preparing endpoint"
DEBUG_AFTER_LOAD = "Creating non-SSH endpoint"


class TestTheConfigReachesTheConsole:
    def test_without_a_setting_info_is_shown(self, tmp_path, capsys):
        out = _run(capsys, "-c", _config(tmp_path), "list")
        assert INFO_AFTER_LOAD in out
        assert DEBUG_AFTER_LOAD not in out

    def test_quiet_hides_info(self, tmp_path, capsys):
        out = _run(capsys, "-c", _config(tmp_path, "quiet = true"), "list")
        assert INFO_AFTER_LOAD not in out

    def test_verbose_shows_debug(self, tmp_path, capsys):
        out = _run(capsys, "-c", _config(tmp_path, "verbose = true"), "list")
        assert DEBUG_AFTER_LOAD in out

    def test_btrfs_debug_shows_debug(self, tmp_path, capsys):
        # Its lines are logged at DEBUG; a console left at INFO showed nothing.
        out = _run(capsys, "-c", _config(tmp_path, "btrfs_debug = true"), "list")
        assert DEBUG_AFTER_LOAD in out


class TestTheCommandLineWins:
    def test_verbose_flag_over_quiet_config(self, tmp_path, capsys):
        out = _run(capsys, "-v", "-c", _config(tmp_path, "quiet = true"), "list")
        assert DEBUG_AFTER_LOAD in out

    def test_quiet_flag_over_verbose_config(self, tmp_path, capsys):
        out = _run(capsys, "-q", "-c", _config(tmp_path, "verbose = true"), "list")
        assert DEBUG_AFTER_LOAD not in out
        assert INFO_AFTER_LOAD not in out


class TestTheLogFileKeepsItsOwnLevel:
    def test_quiet_does_not_thin_the_log_file(self, tmp_path, capsys):
        log = tmp_path / "run.log"
        cfg = _config(tmp_path, "quiet = true", f'log_file = "{log}"')
        _run(capsys, "-c", cfg, "prune", "--dry-run")
        text = log.read_text()
        assert "[INFO]" in text, text
        # An INFO line the ENDPOINTS log after the console went quiet: the
        # shared endpoint logger is a separate tree, and a package-logger line
        # alone cannot tell whether it was thinned.
        assert INFO_AFTER_LOAD in text, text

    def test_the_quiet_flag_does_not_thin_the_log_file_either(self, tmp_path, capsys):
        """-q is "less on the screen" too. The file's completeness used to
        depend on the console level the command line set, because only a
        config setting recomputed the shared logger's floor."""
        log = tmp_path / "flag.log"
        cfg = _config(tmp_path, f'log_file = "{log}"')
        _run(capsys, "-q", "-c", cfg, "prune", "--dry-run")
        text = log.read_text()
        assert INFO_AFTER_LOAD in text, text
        assert DEBUG_AFTER_LOAD in text, text

    def test_run_prints_no_info_after_the_load_under_quiet(self, tmp_path, capsys):
        """The line announcing the log file came after the configuration was
        read and still ignored quiet."""
        log = tmp_path / "quiet-run.log"
        cfg = _config(tmp_path, "quiet = true", f'log_file = "{log}"')
        out = _run(capsys, "-c", cfg, "run", "--dry-run")
        assert "File logging enabled" not in out, out

    def test_the_file_receives_what_the_console_drops(self, tmp_path):
        log = tmp_path / "direct.log"
        __logger__.create_logger(False, level="INFO")
        __logger__.add_file_handler(log)
        __logger__.set_console_level("WARNING")
        assert __logger__.rich_handler.level == logging.WARNING
        __logger__.logger.info("kept in the file")
        logging.getLogger("btrfs_backup_ng.test").info("also kept")
        __logger__.remove_file_handler()
        text = log.read_text()
        assert "kept in the file" in text
        assert "also kept" in text


class TestPrecedence:
    @pytest.mark.parametrize(
        "flags,settings,expected",
        [
            ({}, {}, None),
            ({}, {"quiet": True}, "WARNING"),
            ({}, {"verbose": True}, "DEBUG"),
            ({}, {"btrfs_debug": True}, "DEBUG"),
            # The order the flags use: btrfs_debug, then quiet, then verbose.
            ({}, {"quiet": True, "verbose": True}, "WARNING"),
            ({}, {"quiet": True, "btrfs_debug": True}, "DEBUG"),
            # Any flag means the command line decided.
            ({"verbose": True}, {"quiet": True}, None),
            ({"quiet": True}, {"verbose": True}, None),
            ({"debug": True}, {"quiet": True}, None),
            ({"btrfs_debug": True}, {"quiet": True}, None),
        ],
    )
    def test_config_log_level(self, flags, settings, expected):
        args = types.SimpleNamespace(**flags)
        global_config = types.SimpleNamespace(
            quiet=settings.get("quiet", False),
            verbose=settings.get("verbose", False),
            btrfs_debug=settings.get("btrfs_debug", False),
        )
        config = types.SimpleNamespace(global_config=global_config)
        assert config_log_level(args, config) == expected

    def test_no_configuration_changes_nothing(self):
        assert config_log_level(types.SimpleNamespace(), None) is None


# Every command that reads a configuration applies it. What decides what the
# console shows is the console handler's level once the configuration is
# loaded, so that is asserted directly -- for each command, whatever it then
# goes on to do (several stop early in a scratch directory with no btrfs).
COMMANDS = [
    ("list",),
    ("status",),
    ("prune", "--dry-run"),
    ("doctor",),
    ("run", "--dry-run"),
    ("snapshot", "--dry-run"),
    ("transfer", "--dry-run"),
    ("estimate", "--volume", "{src}"),
    ("estimate", "{src}", "{dst}"),
    ("restore", "--list-volumes"),
    ("restore", "--volume", "{src}", "--to", "{restore_to}", "--dry-run"),
    # The location modes read the configuration for a timestamp_format and a
    # target's options; they apply its verbosity too.
    ("restore", "--list", "{dst}"),
    ("verify", "{dst}"),
]


class TestEveryCommandAppliesIt:
    @pytest.mark.parametrize(
        "setting,expected", [("quiet", logging.WARNING), ("verbose", logging.DEBUG)]
    )
    @pytest.mark.parametrize("command", COMMANDS, ids=lambda c: " ".join(c[:2]))
    def test_console_level_after_the_configuration_loads(
        self, tmp_path, capsys, command, setting, expected
    ):
        from btrfs_backup_ng.cli.dispatcher import main

        cfg = _config(tmp_path, f"{setting} = true")
        argv = [
            part.format(
                src=tmp_path / "src",
                dst=tmp_path / "dst",
                restore_to=tmp_path / "restored",
            )
            for part in command
        ]
        try:
            main(["-c", cfg, *argv])
        except SystemExit:
            pass
        capsys.readouterr()
        assert __logger__.rich_handler.level == expected, (
            f"{' '.join(argv)} with {setting} = true left the console at "
            f"{logging.getLevelName(__logger__.rich_handler.level)}"
        )
