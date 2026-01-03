"""Status command: Show job status and statistics."""

import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

from .. import endpoint
from ..__logger__ import create_logger
from ..config import ConfigError, find_config_file, load_config
from .common import get_log_level

logger = logging.getLogger(__name__)


def execute_status(args: argparse.Namespace) -> int:
    """Execute the status command.

    Shows backup status, last run times, and health information.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    log_level = get_log_level(args)
    create_logger(False, level=log_level)

    # Find and load config
    try:
        config_path = find_config_file(getattr(args, "config", None))
        if config_path is None:
            print("No configuration file found.")
            print("Create one with: btrfs-backup-ng config init")
            return 1

        config, _ = load_config(config_path)

    except ConfigError as e:
        logger.error("Configuration error: %s", e)
        return 1

    volumes = config.get_enabled_volumes()

    if not volumes:
        print("No volumes configured")
        return 1

    print("btrfs-backup-ng Status")
    print("=" * 60)
    print(f"Config: {config_path}")
    print(
        f"Volumes: {len(volumes)} configured, {len(config.get_enabled_volumes())} enabled"
    )
    print(
        f"Parallelism: {config.global_config.parallel_volumes} volumes, {config.global_config.parallel_targets} targets"
    )
    print("")

    all_healthy = True

    for volume in volumes:
        print(f"Volume: {volume.path}")

        # Build endpoint kwargs
        endpoint_kwargs = {
            "snap_prefix": volume.snapshot_prefix or f"{os.uname()[1]}-",
            "convert_rw": False,
            "subvolume_sync": False,
            "btrfs_debug": False,
            "fs_checks": True,
        }

        # Check source
        source_status = "unknown"
        source_count = 0
        last_snapshot = None

        try:
            source_path = Path(volume.path).resolve()

            snapshot_dir = Path(volume.snapshot_dir)
            if not snapshot_dir.is_absolute():
                snapshot_dir = source_path.parent / snapshot_dir
            snapshot_dir = snapshot_dir.resolve()

            relative_source = str(source_path).lstrip(os.sep)
            full_snapshot_dir = snapshot_dir.joinpath(*relative_source.split(os.sep))

            if full_snapshot_dir.exists():
                source_kwargs = dict(endpoint_kwargs)
                source_kwargs["path"] = full_snapshot_dir

                source_endpoint = endpoint.choose_endpoint(
                    str(source_path),
                    source_kwargs,
                    source=True,
                )
                source_endpoint.prepare()

                snapshots = source_endpoint.list_snapshots()
                source_count = len(snapshots)

                if snapshots:
                    last_snapshot = snapshots[-1]
                    source_status = "ok"
                else:
                    source_status = "no snapshots"
            else:
                source_status = "no snapshot dir"

        except Exception as e:
            source_status = f"error: {e}"
            all_healthy = False

        print(f"  Source: {source_status} ({source_count} snapshots)")
        if last_snapshot:
            print(f"  Latest: {last_snapshot.get_name()}")

        # Check targets
        for target in volume.targets:
            target_status = "unknown"
            target_count = 0

            try:
                dest_kwargs = dict(endpoint_kwargs)
                dest_kwargs["ssh_sudo"] = target.ssh_sudo
                dest_kwargs["ssh_password_fallback"] = target.ssh_password_auth

                dest_endpoint = endpoint.choose_endpoint(
                    target.path,
                    dest_kwargs,
                    source=False,
                )
                dest_endpoint.prepare()

                dest_snapshots = dest_endpoint.list_snapshots()
                target_count = len(dest_snapshots)

                if dest_snapshots:
                    target_status = "ok"
                else:
                    target_status = "no backups"

                # Check sync status
                if source_count > 0 and target_count > 0:
                    if target_count < source_count:
                        pending = source_count - target_count
                        target_status = f"ok ({pending} pending)"

            except Exception as e:
                target_status = f"error: {e}"
                all_healthy = False

            print(f"  Target: {target.path}")
            print(f"    Status: {target_status} ({target_count} backups)")

        print("")

    # Summary
    print("=" * 60)
    if all_healthy:
        print("Overall: All systems operational")
    else:
        print("Overall: Some issues detected")

    return 0 if all_healthy else 1
