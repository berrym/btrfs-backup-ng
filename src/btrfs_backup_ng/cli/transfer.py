"""Transfer command: Transfer existing snapshots to targets."""

import argparse
import logging

from ..__logger__ import create_logger
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_transfer(args: argparse.Namespace) -> int:
    """Execute the transfer command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    logger.info("Transfer command not yet fully implemented")
    logger.info("Use legacy mode: btrfs-backup-ng --no-snapshot /source /dest")

    return 0
