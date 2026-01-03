# pyright: standard

"""btrfs-backup-ng: btrfs-backup_ng/Logger.py
A common logger for displaying in a rich layout.
"""

import logging
import logging.handlers
import os
import threading
from collections import deque
from typing import IO  # , override (requires python 3.12)

from rich.console import Console
from rich.logging import RichHandler

# Get initial log level from environment or default to INFO
_initial_level_name = os.environ.get("BTRFS_BACKUP_LOG_LEVEL", "INFO").upper()
_initial_level = getattr(logging, _initial_level_name, logging.INFO)

# Initialize basic console and handler
cons = Console()
rich_handler = RichHandler(console=cons, show_path=False)
# Create a logger - level will be set by set_level() or environment variable
logger = logging.Logger("btrfs-backup-ng", _initial_level)


class RichLogger(IO[str]):
    """A singleton pattern class to share internal state of the rich logger."""

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
        cons = Console(file=RichLogger(), width=150)
        rich_handler = RichHandler(console=cons, show_time=False, show_path=False)
    else:
        cons = Console()
        rich_handler = RichHandler(console=cons, show_path=False)

    rich_handler.setLevel(log_level)
    logger.handlers.clear()
    logger.propagate = False
    logger.addHandler(rich_handler)
    logger.setLevel(log_level)

    logging.basicConfig(
        format="(%(processName)s) %(message)s",
        datefmt="%H:%M:%S",
        level=log_level,
        handlers=[rich_handler],
        force=True,
    )
