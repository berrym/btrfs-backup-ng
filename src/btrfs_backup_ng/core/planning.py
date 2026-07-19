"""Transfer planning and snapshot management logic.

Handles planning which snapshots need to be transferred and
managing corrupt/locked snapshots.
"""

import logging

logger = logging.getLogger(__name__)


def plan_transfers(
    source_snapshots: list,
    destination_snapshots: list,
    keep_num_backups: int = 0,
) -> list:
    """Plan which snapshots need to be transferred.

    Args:
        source_snapshots: List of snapshots at source
        destination_snapshots: List of snapshots at destination
        keep_num_backups: Number of backups to consider (0 = all)

    Returns:
        List of snapshots that need to be transferred
    """
    # Only consider the latest N snapshots if keep_num_backups > 0
    to_consider = (
        source_snapshots[-keep_num_backups:]
        if keep_num_backups > 0
        else source_snapshots
    )

    # Filter out those already at destination
    to_transfer = [
        snapshot for snapshot in to_consider if snapshot not in destination_snapshots
    ]

    logger.debug("Planned %d snapshots for transfer", len(to_transfer))
    return to_transfer


def find_best_transfer_order(
    to_transfer: list,
    source_snapshots: list,
    destination_snapshots: list,
    no_incremental: bool = False,
) -> list[tuple]:
    """Determine optimal order for transfers with parent selection.

    For incremental transfers, we want to minimize the distance
    between a snapshot and its parent to reduce transfer size.

    Args:
        to_transfer: Snapshots that need to be transferred
        source_snapshots: All source snapshots
        destination_snapshots: Snapshots already at destination
        no_incremental: If True, don't use incremental transfers

    Returns:
        List of (snapshot, parent) tuples in optimal transfer order
    """
    if no_incremental:
        # No parent needed, just return in order
        return [(snap, None) for snap in to_transfer]

    result = []
    remaining = list(to_transfer)
    present = set(destination_snapshots)

    while remaining:
        # Find present snapshots that can serve as parents
        present_snapshots = [
            snap
            for snap in source_snapshots
            if snap in present and snap.get_name() not in snap.locks
        ]

        def key(s):
            p = s.find_parent(present_snapshots)
            if p is None:
                return float("inf")
            d = source_snapshots.index(s) - source_snapshots.index(p)
            return -d if d < 0 else d

        best = min(remaining, key=key)
        parent = best.find_parent(present_snapshots)

        result.append((best, parent))
        remaining.remove(best)

        # After transfer, this snapshot will be present
        present.add(best)

    return result
