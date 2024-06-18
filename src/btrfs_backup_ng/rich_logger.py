"""btrfs-backup-ng: btrfs-backup_ng/Logger.py
A common logger for displaying in a rich layout.
"""

import logging
import threading

from collections import deque

from rich.console import Console
from rich.logging import RichHandler


cons = Console()
rich_handler = RichHandler()
logger = logging.getLogger()


# class Borg:
#     """Basic Borg pattern class."""

#     __monostate = {}

#     def __init__(self):
#         """Initialize the shared internal state."""
#         self.__dict__ = self.__monostate


# class RichLogger(Borg):
#     """A Borg subclass to share internal state of the rich logger."""

#     def __init__(self):
#         """Initialize the Borg and the logger."""
#         Borg.__init__(self)
#         self.messages = deque(["btrfs-backup-ng -- logger"], maxlen=20)

#     def write(self, message):
#         """Write log message"""
#         self.messages.extend(message.splitlines())

#     def flush(self):
#         """Place holder"""
#         pass


# class RichLogger:
#     """Borg pattern class to share internal state of the rich logger."""

#     __monostate = None

#     def __init__(self):
#         """Initialize the logger state."""
#         if RichLogger.__monostate is None:
#             RichLogger.__monostate = self.__dict__
#             self.messages = deque(["btrfs-backup-ng -- logger"], maxlen=20)
#         else:
#             self.__dict__ = RichLogger.__monostate

#     def write(self, message):
#         """Add log message"""
#         self.messages.extend(message.splitlines())

#     def flush(self):
#         """Place holder."""
#         pass


class RichLogger:
    """A singleton pattern class to share internal state of the rich logger."""

    __instance = None
    __lock = threading.Lock()

    def __init__(self):
        """Init"""
        self.messages = deque(["btrfs-backup-ng -- logger"], maxlen=20)

    def __new__(cls, *args, **kwargs):
        """Singleton"""
        if not isinstance(cls.__instance, cls):
            with cls.__lock:
                if not isinstance(cls.__instance, cls):
                    cls.__instance = super().__new__(cls, *args, **kwargs)
        return cls.__instance

    def write(self, message):
        """Write log message"""
        self.messages.extend(message.splitlines())

    def flush(self):
        """Place holder"""
        pass


def create_logger(live_layout):
    """Helper function to setup logging depending on visual display options."""
    global cons
    global rich_handler
    global logger
    if live_layout:
        cons = Console(file=RichLogger(), width=150)
        rich_handler = RichHandler(console=cons, show_time=False, show_path=False)
    else:
        cons = Console()
        rich_handler = RichHandler(console=cons, show_path=False)
    logging.basicConfig(
        format="(%(processName)s) %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
        handlers=[rich_handler],
        force=True,
    )
    logger = logging.getLogger("rich")
