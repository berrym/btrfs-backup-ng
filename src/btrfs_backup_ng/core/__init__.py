"""Core backup operations for btrfs-backup-ng.

This module contains the extracted backup logic from the original
monolithic __main__.py, organized into focused modules.
"""

from .execution import execute_parallel, run_task
from .operations import send_snapshot, sync_snapshots
from .planning import delete_corrupt_snapshots, plan_transfers

__all__ = [
    "send_snapshot",
    "sync_snapshots",
    "plan_transfers",
    "delete_corrupt_snapshots",
    "run_task",
    "execute_parallel",
]
