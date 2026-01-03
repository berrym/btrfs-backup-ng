"""Run command: Execute all configured backup jobs."""

import argparse
import logging

from ..__logger__ import create_logger
from ..config import ConfigError, find_config_file, load_config
from ..core.execution import execute_parallel, execute_sequential
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_run(args: argparse.Namespace) -> int:
    """Execute the run command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Initialize logger
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    # Find and load config
    try:
        config_path = find_config_file(getattr(args, "config", None))
        if config_path is None:
            print("No configuration file found.")
            print("Create one with: btrfs-backup-ng config init")
            print("")
            print("Or use legacy mode: btrfs-backup-ng /source /dest")
            return 1

        config, warnings = load_config(config_path)

        for warning in warnings:
            logger.warning("Config: %s", warning)

    except ConfigError as e:
        logger.error("Configuration error: %s", e)
        return 1

    if not config.volumes:
        logger.error("No volumes configured")
        return 1

    if getattr(args, "dry_run", False):
        print("Dry run mode - showing what would be done:")
        print("")
        for volume in config.get_enabled_volumes():
            print(f"Volume: {volume.path}")
            print(f"  Snapshot prefix: {volume.snapshot_prefix}")
            for target in volume.targets:
                print(f"  -> {target.path}")
        return 0

    # TODO: Implement actual backup execution
    # This requires creating endpoints from config and running jobs
    logger.info("Run command not yet fully implemented")
    logger.info("Use legacy mode for now: btrfs-backup-ng /source /dest")

    return 0
