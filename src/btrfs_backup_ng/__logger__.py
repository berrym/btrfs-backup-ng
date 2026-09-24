"""btrfs-backup-ng: btrfs-backup_ng/Logger.py
A common logger for displaying in a rich layout with optional file logging.
"""

import logging
import logging.handlers
import os
import threading
from collections import deque
from pathlib import Path

# Note: override decorator requires Python 3.12+
from rich.console import Console
from rich.logging import RichHandler

#: The levels ``BTRFS_BACKUP_LOG_LEVEL`` may name.
ENVIRONMENT_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


def environment_log_level() -> str | None:
    """The console level ``BTRFS_BACKUP_LOG_LEVEL`` asks for, or None.

    The lowest-precedence source: a command-line flag (``--debug``, ``-q``,
    ``-v``) wins over it, and a configuration's ``[global] quiet``, ``verbose``
    or ``btrfs_debug`` wins over it too, so it decides only where neither said
    anything. Documented for years and, until this, read once at import and
    then overwritten by every command's ``create_logger``. A value that is
    not one of ``ENVIRONMENT_LEVELS`` is ignored, and said so once.
    """
    raw = os.environ.get("BTRFS_BACKUP_LOG_LEVEL")
    if raw is None or raw.strip() == "":
        return None
    name = raw.strip().upper()
    if name in ENVIRONMENT_LEVELS:
        return name
    _warn_about_environment_level(raw)
    return None


_environment_level_warned: set[str] = set()


def _warn_about_environment_level(raw: str) -> None:
    if raw in _environment_level_warned:
        return
    _environment_level_warned.add(raw)
    logging.getLogger(__name__).warning(
        "BTRFS_BACKUP_LOG_LEVEL=%r is not one of %s; it is ignored",
        raw,
        ", ".join(ENVIRONMENT_LEVELS),
    )


# Get initial log level from environment or default to INFO
_initial_level_name = environment_log_level() or "INFO"
_initial_level = getattr(logging, _initial_level_name, logging.INFO)

# Initialize basic console and handler
cons = Console()
rich_handler = RichHandler(console=cons, show_path=False)
# The shared logger every endpoint module logs through. Registered with the
# logging manager (getLogger) rather than constructed bare: a Logger built
# directly is unknown to the manager, and the manager is what clears every
# logger's isEnabledFor cache when a level changes. A bare one kept the answer
# it had cached under one level after the level moved -- `quiet` in a config
# set it to WARNING, the first INFO record cached "disabled", and every INFO
# line from the endpoints stayed dropped after the level was lowered again
# (a log file added later, a test resetting the level). propagate stays off,
# as create_logger sets it, so its records reach only its own handlers.
logger = logging.getLogger("btrfs-backup-ng")
logger.setLevel(_initial_level)
logger.propagate = False

# File handler (set by add_file_handler)
_file_handler: logging.Handler | None = None

# The package-root logger, i.e. the parent of every logging.getLogger(__name__)
# inside btrfs_backup_ng.
#
# `logger` above is named with HYPHENS and does not propagate, so nothing is
# its child and nothing above it sees its records -- while 36 modules across cli/ and core/ use
# logging.getLogger(__name__), which lives under "btrfs_backup_ng" with
# UNDERSCORES. The two are unrelated trees, so a file handler attached only to
# `logger` never saw a single line from run, transfer, restore, operations or
# any other module that logs that way: log_file recorded a fraction of the run
# and looked complete.
#
# The handler is attached to BOTH. This one keeps propagate=True, so its records
# still reach the root handler that basicConfig installs and console output is
# unchanged -- the file simply stops missing them.
_PACKAGE_LOGGER_NAME = "btrfs_backup_ng"

# The package logger's level before we raised it, so removing the file handler
# restores it. Raising it is necessary -- the handler wants DEBUG and the
# inherited level would filter records before the handler saw them -- but
# leaving it raised changes console verbosity for the rest of the process,
# which is a side effect of file logging that nothing asked for.
_package_level_before: int | None = None
#: The shared endpoint logger's level before add_file_handler lowered it, for
#: the same reason and the same restoration.
_shared_level_before: int | None = None


class RichLogger:
    """A singleton pattern class to share internal state of the rich logger.

    Implements write() and flush() as required by Rich Console's file parameter.
    """

    __instance = None
    __lock = threading.Lock()

    def __init__(self) -> None:
        """Init."""
        self.messages = deque(["btrfs-backup-ng -- logger"], maxlen=20)

    def __new__(cls, *args, **kwargs):
        """Singleton."""
        if not isinstance(cls.__instance, cls):
            with cls.__lock:
                if not isinstance(cls.__instance, cls):
                    cls.__instance = super().__new__(cls, *args, **kwargs)
        return cls.__instance

    # @override
    def write(self, message) -> int:
        """Write log message."""
        self.messages.extend(message.splitlines())
        return 0

    # @override
    def flush(self) -> None:
        """Place holder."""


def set_level(level) -> None:
    """Set the global logger level.

    Args:
        level: Either a string ('DEBUG', 'INFO', 'WARNING', 'ERROR')
               or a logging level constant (logging.DEBUG, etc.)
    """
    global logger
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(level)
    # Also update any handlers
    for handler in logger.handlers:
        handler.setLevel(level)


def set_console_level(level) -> None:
    """Change what the CONSOLE shows, and nothing else.

    A setting that arrives after the logger was created -- ``[global] quiet``,
    ``verbose`` or ``btrfs_debug`` in a configuration read after the command
    line -- must reach the console handler, and must not reach the log file:
    ``quiet`` means "less on the screen", not "less in the record". ``set_level``
    changes every handler, the file included, so it is not used for this.
    The loggers themselves stay open to the most verbose level any handler
    wants, since a logger's level filters a record before any handler sees it.
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    rich_handler.setLevel(level)
    for handler in logging.getLogger().handlers:
        if handler is not _file_handler:
            handler.setLevel(level)
    floor = level
    if _file_handler is not None:
        floor = min(floor, _file_handler.level)
    logger.setLevel(floor)
    logging.getLogger().setLevel(level)


def create_logger(live_layout, level=None) -> None:
    """Helper function to setup logging depending on visual display options.

    Args:
        live_layout: Whether to use live layout display
        level: Optional log level to set (string or logging constant)
    """
    # pylint: disable=global-statement
    global cons, rich_handler, logger

    # Determine the log level
    if level is not None:
        if isinstance(level, str):
            log_level = getattr(logging, level.upper(), logging.INFO)
        else:
            log_level = level
    else:
        log_level = logger.level or logging.INFO

    # Create new handlers
    if live_layout:
        cons = Console(file=RichLogger(), width=150)  # type: ignore[arg-type]
        rich_handler = RichHandler(console=cons, show_time=False, show_path=False)
    else:
        cons = Console()
        rich_handler = RichHandler(console=cons, show_path=False)

    rich_handler.setLevel(log_level)
    # Set a simple formatter that only shows the message (no process name, filename, etc.)
    rich_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.handlers.clear()
    logger.propagate = False
    logger.addHandler(rich_handler)
    logger.setLevel(log_level)

    logging.basicConfig(
        format="%(message)s",
        datefmt="%H:%M:%S",
        level=log_level,
        handlers=[rich_handler],
        force=True,
    )


def add_file_handler(
    log_file: str | Path,
    level: str | int | None = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
) -> None:
    """Add a rotating file handler to the logger.

    Args:
        log_file: Path to the log file
        level: Log level for file output (default: DEBUG for comprehensive logging)
        max_bytes: Maximum size of each log file before rotation (default: 10 MB)
        backup_count: Number of backup files to keep (default: 5)
    """
    global _file_handler, _package_level_before, _shared_level_before

    # Remove existing file handler if present
    if _file_handler is not None:
        logger.removeHandler(_file_handler)
        # Detached from both, or a replaced/removed handler keeps receiving
        # records through the package logger after being closed.
        logging.getLogger(_PACKAGE_LOGGER_NAME).removeHandler(_file_handler)
        _file_handler.close()

    log_path = Path(log_file)

    # Create parent directories if needed
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Determine log level (default to DEBUG for file to capture everything)
    if level is None:
        file_level = logging.DEBUG
    elif isinstance(level, str):
        file_level = getattr(logging, level.upper(), logging.DEBUG)
    else:
        file_level = level

    # Create rotating file handler
    _file_handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    _file_handler.setLevel(file_level)

    # Use detailed format for file logs
    file_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _file_handler.setFormatter(file_formatter)

    logger.addHandler(_file_handler)

    # Also catch everything logged via logging.getLogger(__name__) inside the
    # package, which is most of the application (see _PACKAGE_LOGGER_NAME).
    package_logger = logging.getLogger(_PACKAGE_LOGGER_NAME)
    package_logger.addHandler(_file_handler)
    # The file handler decides its own level; without this the package logger's
    # effective level (inherited from root, typically WARNING) would filter out
    # the INFO and DEBUG records the file is meant to capture before the handler
    # ever saw them. Remembered so removal can put it back.
    if package_logger.level == logging.NOTSET or package_logger.level > file_level:
        if _package_level_before is None:
            _package_level_before = package_logger.level
        package_logger.setLevel(file_level)
    # The shared endpoint logger gets the same treatment. Left at the console's
    # level, what the FILE received from the endpoints depended on how the
    # console had been set up: under `-q` every endpoint INFO line was missing
    # from the record, and under the default level every endpoint DEBUG line,
    # while set_console_level's floor happened to open it only when a config
    # setting ran. The console handler keeps its own level, so the screen shows
    # no more than before.
    if logger.level == logging.NOTSET or logger.level > file_level:
        if _shared_level_before is None:
            _shared_level_before = logger.level
        logger.setLevel(file_level)

    logger.debug("File logging enabled: %s", log_path)


def remove_file_handler() -> None:
    """Remove the file handler if present."""
    global _file_handler, _package_level_before, _shared_level_before

    if _file_handler is not None:
        logger.removeHandler(_file_handler)
        # Detached from both, or a replaced/removed handler keeps receiving
        # records through the package logger after being closed.
        logging.getLogger(_PACKAGE_LOGGER_NAME).removeHandler(_file_handler)
        _file_handler.close()
        _file_handler = None
        # Put the package logger's level back. File logging must not silently
        # change console verbosity for the rest of the process.
        if _package_level_before is not None:
            logging.getLogger(_PACKAGE_LOGGER_NAME).setLevel(_package_level_before)
            _package_level_before = None
        if _shared_level_before is not None:
            logger.setLevel(_shared_level_before)
            _shared_level_before = None
