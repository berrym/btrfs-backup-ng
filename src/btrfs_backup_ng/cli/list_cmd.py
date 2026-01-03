"""List command: Show snapshots and backups."""

import argparse
import logging

from ..__logger__ import create_logger
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_list(args: argparse.Namespace) -> int:
    """Execute the list command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    logger.info("List command not yet fully implemented")

    return 0
