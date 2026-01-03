"""Snapshot command: Create snapshots only."""

import argparse
import logging

from ..__logger__ import create_logger
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_snapshot(args: argparse.Namespace) -> int:
    """Execute the snapshot command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    logger.info("Snapshot command not yet fully implemented")
    logger.info("Use legacy mode: btrfs-backup-ng --no-transfer /source")

    return 0
