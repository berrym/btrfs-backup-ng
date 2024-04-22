"""btrfs-backup-ng: btrfs-backup_ng/Logger.py
A common logger for displaying in a rich layout.
"""

import logging

from collections import deque

from rich.console import Console
from rich.logging import RichHandler


class RichLogger:
    """A singleton class to provide a rich interface for logging."""

    _instance = None

    def __init__(self):
        """Init"""
        self.messages = deque(["btrfs-backup-ng -- logger"])
        self.size = 10

    def __new__(cls, *args, **kwargs):
        """Singleton"""
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance

    def write(self, message):
        """Write log message"""
        self.messages.extend(message.splitlines())
        while len(self.messages) > self.size:
            self.messages.popleft()

    def flush(self):
        """Place holder"""
        pass


cons = Console(file=RichLogger(), width=150)
rich_handler = RichHandler(console=cons, show_level=False, show_path=False)

logging.basicConfig(
    format="%(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
    handlers=[rich_handler],
)
logger = logging.getLogger("rich")
