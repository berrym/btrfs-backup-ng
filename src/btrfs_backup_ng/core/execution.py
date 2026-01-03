"""Task execution with parallel processing support.

Provides the execution framework for running backup jobs with
configurable parallelism for volumes and targets.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable

from .. import __util__
from ..config import Config, TargetConfig, VolumeConfig
from .operations import sync_snapshots

logger = logging.getLogger(__name__)


@dataclass
class JobResult:
    """Result of a backup job execution."""

    volume_path: str
    target_path: str
    success: bool
    error: str | None = None
    snapshots_transferred: int = 0
    duration_seconds: float = 0.0


def run_task(
    source_endpoint,
    destination_endpoints: list,
    options: dict,
) -> list[JobResult]:
    """Run a backup task with given endpoints.

    This is the main entry point for executing a backup task.
    Handles snapshot creation, transfer to all destinations,
    and cleanup.

    Args:
        source_endpoint: Prepared source endpoint
        destination_endpoints: List of prepared destination endpoints
        options: Options dict with settings

    Returns:
        List of JobResult for each destination
    """
    results = []

    # Create snapshot if requested
    snapshot = None
    if not options.get("no_snapshot", False):
        logger.info(__util__.log_heading("Creating snapshot..."))
        try:
            snapshot = source_endpoint.snapshot()
            logger.info("Created snapshot: %s", snapshot)
        except Exception as e:
            logger.error("Failed to create snapshot: %s", e)
            raise __util__.AbortError(f"Snapshot creation failed: {e}")

    # Transfer to each destination
    for dest_endpoint in destination_endpoints:
        start_time = time.time()
        try:
            sync_snapshots(
                source_endpoint,
                dest_endpoint,
                keep_num_backups=options.get("num_backups", 0),
                no_incremental=options.get("no_incremental", False),
                snapshot=snapshot,
                options=options,
            )
            results.append(
                JobResult(
                    volume_path=source_endpoint.config.get("path", ""),
                    target_path=dest_endpoint.config.get("path", ""),
                    success=True,
                    duration_seconds=time.time() - start_time,
                )
            )
        except Exception as e:
            logger.error("Transfer to %s failed: %s", dest_endpoint, e)
            results.append(
                JobResult(
                    volume_path=source_endpoint.config.get("path", ""),
                    target_path=dest_endpoint.config.get("path", ""),
                    success=False,
                    error=str(e),
                    duration_seconds=time.time() - start_time,
                )
            )

    # Cleanup old snapshots
    _cleanup_snapshots(source_endpoint, destination_endpoints, options)

    return results


def _cleanup_snapshots(source_endpoint, destination_endpoints, options) -> None:
    """Clean up old snapshots according to retention settings."""
    logger.info(__util__.log_heading("Cleaning up..."))

    num_snapshots = options.get("num_snapshots", 0)
    num_backups = options.get("num_backups", 0)

    if num_snapshots > 0:
        try:
            source_endpoint.delete_old_snapshots(num_snapshots)
        except Exception as e:
            logger.debug("Error deleting source snapshots: %s", e)

    if num_backups > 0:
        for dest in destination_endpoints:
            try:
                dest.delete_old_snapshots(num_backups)
            except Exception as e:
                logger.debug("Error deleting backups at %s: %s", dest, e)

    logger.info(__util__.log_heading(f"Finished at {time.ctime()}"))


def execute_parallel(
    config: Config,
    job_func: Callable[[VolumeConfig, TargetConfig], JobResult],
    parallel_volumes: int | None = None,
    parallel_targets: int | None = None,
) -> list[JobResult]:
    """Execute backup jobs with parallelism.

    Runs volume backups in parallel, and within each volume,
    runs target transfers in parallel.

    Args:
        config: Configuration with volumes and targets
        job_func: Function to execute for each (volume, target) pair
        parallel_volumes: Max concurrent volumes (None = use config)
        parallel_targets: Max concurrent targets (None = use config)

    Returns:
        List of all JobResults
    """
    max_volumes = parallel_volumes or config.global_config.parallel_volumes
    max_targets = parallel_targets or config.global_config.parallel_targets

    logger.info(
        "Executing with parallelism: %d volumes, %d targets per volume",
        max_volumes,
        max_targets,
    )

    all_results = []
    enabled_volumes = config.get_enabled_volumes()

    with ThreadPoolExecutor(max_workers=max_volumes) as volume_executor:
        volume_futures = {
            volume_executor.submit(
                _execute_volume_targets,
                volume,
                job_func,
                max_targets,
            ): volume
            for volume in enabled_volumes
        }

        for future in as_completed(volume_futures):
            volume = volume_futures[future]
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as e:
                logger.error("Volume %s failed: %s", volume.path, e)
                # Add failed result for each target
                for target in volume.targets:
                    all_results.append(
                        JobResult(
                            volume_path=volume.path,
                            target_path=target.path,
                            success=False,
                            error=str(e),
                        )
                    )

    return all_results


def _execute_volume_targets(
    volume: VolumeConfig,
    job_func: Callable[[VolumeConfig, TargetConfig], JobResult],
    max_targets: int,
) -> list[JobResult]:
    """Execute all targets for a volume in parallel.

    Args:
        volume: Volume configuration
        job_func: Function to execute for each target
        max_targets: Max concurrent target transfers

    Returns:
        List of JobResults for this volume
    """
    results = []

    if not volume.targets:
        logger.warning("Volume %s has no targets configured", volume.path)
        return results

    logger.info("Processing volume: %s", volume.path)

    with ThreadPoolExecutor(max_workers=max_targets) as target_executor:
        target_futures = {
            target_executor.submit(job_func, volume, target): target
            for target in volume.targets
        }

        for future in as_completed(target_futures):
            target = target_futures[future]
            try:
                result = future.result()
                results.append(result)
                if result.success:
                    logger.info(
                        "  -> %s: success (%.1fs)",
                        target.path,
                        result.duration_seconds,
                    )
                else:
                    logger.error(
                        "  -> %s: failed - %s",
                        target.path,
                        result.error,
                    )
            except Exception as e:
                logger.error("Target %s failed: %s", target.path, e)
                results.append(
                    JobResult(
                        volume_path=volume.path,
                        target_path=target.path,
                        success=False,
                        error=str(e),
                    )
                )

    return results


def execute_sequential(
    config: Config,
    job_func: Callable[[VolumeConfig, TargetConfig], JobResult],
) -> list[JobResult]:
    """Execute backup jobs sequentially (no parallelism).

    Useful for debugging or when resources are constrained.

    Args:
        config: Configuration with volumes and targets
        job_func: Function to execute for each (volume, target) pair

    Returns:
        List of all JobResults
    """
    results = []

    for volume in config.get_enabled_volumes():
        logger.info("Processing volume: %s", volume.path)

        for target in volume.targets:
            try:
                result = job_func(volume, target)
                results.append(result)
                if result.success:
                    logger.info(
                        "  -> %s: success (%.1fs)",
                        target.path,
                        result.duration_seconds,
                    )
                else:
                    logger.error("  -> %s: failed - %s", target.path, result.error)
            except Exception as e:
                logger.error("Target %s failed: %s", target.path, e)
                results.append(
                    JobResult(
                        volume_path=volume.path,
                        target_path=target.path,
                        success=False,
                        error=str(e),
                    )
                )

    return results
