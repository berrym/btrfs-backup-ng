# pyright: standard

"""btrfs-backup-ng: btrfs-backup_ng/Logger.py
A common logger for displaying in a rich layout.
"""

import logging
import logging.handlers
import threading
from collections import deque
from typing import IO  # , override (requires python 3.12)

from rich.console import Console
from rich.logging import RichHandler

# Initialize basic console and handler
cons = Console()
rich_handler = RichHandler(console=cons, show_path=False)
# Create a logger directly
logger = logging.Logger("btrfs-backup-ng", logging.INFO)


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


def create_logger(live_layout) -> None:
    """Helper function to setup logging depending on visual display options."""
    # pylint: disable=global-statement
    global cons, rich_handler, logger

    # Create new handlers
    if live_layout:
        cons = Console(file=RichLogger(), width=150)
        rich_handler = RichHandler(console=cons, show_time=False, show_path=False)
    else:
        cons = Console()
        rich_handler = RichHandler(console=cons, show_path=False)

    logger.handlers.clear()
    logger.propagate = False
    logger.addHandler(rich_handler)

    logging.basicConfig(
        format="(%(processName)s) %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
        handlers=[rich_handler],
        force=True,
    )
