"""Backup verification and integrity checking.

Provides multiple levels of verification to ensure backups are valid
and restorable:

- metadata: Quick check of snapshot existence and parent chain integrity
- stream: Verify btrfs send stream can be generated (no data transfer)
- full: Complete restore test to temporary location
"""

import logging
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from .. import __util__

logger = logging.getLogger(__name__)


class VerifyLevel(Enum):
    """Verification level."""

    METADATA = "metadata"  # Quick: check existence and chain
    STREAM = "stream"  # Medium: verify send stream works
    FULL = "full"  # Thorough: actual restore test


@dataclass
class VerifyResult:
    """Result of a verification operation."""

    snapshot_name: str
    level: VerifyLevel
    passed: bool
    message: str = ""
    duration_seconds: float = 0.0
    details: dict = field(default_factory=dict)


@dataclass
class VerifyReport:
    """Complete verification report."""

    level: VerifyLevel
    location: str
    started_at: float = field(default_factory=time.time)
    completed_at: float = 0.0
    results: list[VerifyResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def duration(self) -> float:
        if self.completed_at:
            return self.completed_at - self.started_at
        return time.time() - self.started_at


class VerifyError(Exception):
    """Error during verification."""

    pass


def verify_metadata(
    backup_endpoint,
    source_endpoint=None,
    snapshot_name: str | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> VerifyReport:
    """Verify backup metadata integrity.

    Checks:
    - Snapshots exist at backup location
    - Parent chain is complete (no missing incremental parents)
    - Optionally compares with source to find missing backups

    Args:
        backup_endpoint: Endpoint where backups are stored
        source_endpoint: Optional source endpoint to compare against
        snapshot_name: Specific snapshot to verify (None = all)
        on_progress: Progress callback (current, total, name)

    Returns:
        VerifyReport with results
    """
    location = str(backup_endpoint.config.get("path", "unknown"))
    report = VerifyReport(level=VerifyLevel.METADATA, location=location)

    try:
        # List snapshots at backup location
        logger.info("Listing snapshots at backup location...")
        backup_snapshots = backup_endpoint.list_snapshots()

        if not backup_snapshots:
            report.errors.append("No snapshots found at backup location")
            report.completed_at = time.time()
            return report

        logger.info("Found %d snapshot(s) at backup location", len(backup_snapshots))

        # Filter to specific snapshot if requested
        if snapshot_name:
            backup_snapshots = [
                s for s in backup_snapshots if s.get_name() == snapshot_name
            ]
            if not backup_snapshots:
                report.errors.append(f"Snapshot '{snapshot_name}' not found")
                report.completed_at = time.time()
                return report

        # Build set of all snapshot names for chain checking
        all_names = {s.get_name() for s in backup_snapshots}

        # Verify each snapshot
        for i, snap in enumerate(backup_snapshots, 1):
            name = snap.get_name()

            if on_progress:
                on_progress(i, len(backup_snapshots), name)

            start = time.monotonic()
            result = VerifyResult(
                snapshot_name=name,
                level=VerifyLevel.METADATA,
                passed=True,
            )

            # Check 1: Snapshot exists (we already know it does from list)
            result.details["exists"] = True

            # Check 2: Parent chain integrity
            # Find what would be the parent for this snapshot
            parent = _find_parent_snapshot(snap, backup_snapshots)
            if parent:
                parent_name = parent.get_name()
                result.details["parent"] = parent_name
                if parent_name not in all_names:
                    result.passed = False
                    result.message = f"Missing parent: {parent_name}"
                    result.details["parent_missing"] = True
            else:
                result.details["parent"] = None
                result.details["is_base"] = True  # This is a base snapshot

            result.duration_seconds = time.monotonic() - start
            report.results.append(result)

        # Optional: Compare with source
        if source_endpoint:
            try:
                source_snapshots = source_endpoint.list_snapshots()
                source_names = {s.get_name() for s in source_snapshots}
                backup_names = {s.get_name() for s in backup_snapshots}

                missing = source_names - backup_names
                if missing:
                    report.errors.append(
                        f"Snapshots in source but not backup: {sorted(missing)}"
                    )

                extra = backup_names - source_names
                if extra:
                    # This is informational, not an error
                    logger.info(
                        "Snapshots in backup but not source (may be pruned): %s",
                        sorted(extra),
                    )
            except Exception as e:
                logger.warning("Could not compare with source: %s", e)

    except Exception as e:
        report.errors.append(f"Verification failed: {e}")
        logger.error("Metadata verification failed: %s", e)

    report.completed_at = time.time()
    return report


def verify_stream(
    backup_endpoint,
    snapshot_name: str | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> VerifyReport:
    """Verify btrfs send stream can be generated.

    Uses 'btrfs send --no-data' to verify stream integrity without
    transferring actual file data. This validates that the snapshot
    metadata is intact and a restore would be possible.

    Args:
        backup_endpoint: Endpoint where backups are stored
        snapshot_name: Specific snapshot to verify (None = latest only)
        on_progress: Progress callback

    Returns:
        VerifyReport with results
    """
    location = str(backup_endpoint.config.get("path", "unknown"))
    report = VerifyReport(level=VerifyLevel.STREAM, location=location)

    try:
        backup_snapshots = backup_endpoint.list_snapshots()

        if not backup_snapshots:
            report.errors.append("No snapshots found at backup location")
            report.completed_at = time.time()
            return report

        # Filter or select snapshots
        if snapshot_name:
            to_verify = [s for s in backup_snapshots if s.get_name() == snapshot_name]
            if not to_verify:
                report.errors.append(f"Snapshot '{snapshot_name}' not found")
                report.completed_at = time.time()
                return report
        else:
            # Default: verify latest snapshot only (stream check is slower)
            to_verify = [backup_snapshots[-1]]

        for i, snap in enumerate(to_verify, 1):
            name = snap.get_name()

            if on_progress:
                on_progress(i, len(to_verify), name)

            start = time.monotonic()
            result = VerifyResult(
                snapshot_name=name,
                level=VerifyLevel.STREAM,
                passed=True,
            )

            try:
                # Find parent for incremental stream test
                parent = _find_parent_snapshot(snap, backup_snapshots)

                # Test send stream generation
                _test_send_stream(backup_endpoint, snap, parent)
                result.message = "Stream verified successfully"
                result.details["incremental"] = parent is not None
                if parent:
                    result.details["parent"] = parent.get_name()

            except Exception as e:
                result.passed = False
                result.message = f"Stream verification failed: {e}"
                logger.error("Stream verify failed for %s: %s", name, e)

            result.duration_seconds = time.monotonic() - start
            report.results.append(result)

    except Exception as e:
        report.errors.append(f"Verification failed: {e}")
        logger.error("Stream verification failed: %s", e)

    report.completed_at = time.time()
    return report


def verify_full(
    backup_endpoint,
    snapshot_name: str | None = None,
    temp_dir: Path | None = None,
    cleanup: bool = True,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> VerifyReport:
    """Perform full restore verification.

    Actually restores snapshot(s) to a temporary location and verifies
    the restored subvolume is valid. This is the most thorough check
    but also the slowest.

    Args:
        backup_endpoint: Endpoint where backups are stored
        snapshot_name: Specific snapshot to verify (None = latest only)
        temp_dir: Directory for temporary restore (must be on btrfs)
        cleanup: Whether to delete restored snapshots after verification
        on_progress: Progress callback

    Returns:
        VerifyReport with results
    """
    location = str(backup_endpoint.config.get("path", "unknown"))
    report = VerifyReport(level=VerifyLevel.FULL, location=location)

    # Determine temp directory
    if temp_dir:
        temp_path = Path(temp_dir)
        if not temp_path.exists():
            temp_path.mkdir(parents=True)
        own_temp = False
    else:
        # Try to create temp dir on same filesystem as backup if local
        # For remote, we need user to specify
        backup_path = backup_endpoint.config.get("path")
        if backup_path and Path(backup_path).exists():
            temp_path = Path(
                tempfile.mkdtemp(
                    prefix="btrfs-verify-",
                    dir=backup_path,
                )
            )
            own_temp = True
        else:
            report.errors.append(
                "For remote backups, --temp-dir must be specified "
                "(must be on a local btrfs filesystem)"
            )
            report.completed_at = time.time()
            return report

    # Verify temp dir is on btrfs
    if not __util__.is_btrfs(temp_path):  # type: ignore[attr-defined]
        report.errors.append(f"Temp directory {temp_path} is not on btrfs filesystem")
        report.completed_at = time.time()
        return report

    logger.info("Using temp directory: %s", temp_path)

    try:
        backup_snapshots = backup_endpoint.list_snapshots()

        if not backup_snapshots:
            report.errors.append("No snapshots found at backup location")
            report.completed_at = time.time()
            return report

        # Filter or select snapshots
        if snapshot_name:
            to_verify = [s for s in backup_snapshots if s.get_name() == snapshot_name]
            if not to_verify:
                report.errors.append(f"Snapshot '{snapshot_name}' not found")
                report.completed_at = time.time()
                return report
        else:
            # Default: verify latest snapshot only
            to_verify = [backup_snapshots[-1]]

        # Create local endpoint for receiving
        from .. import endpoint

        local_endpoint = endpoint.LocalEndpoint(
            {
                "path": str(temp_path),
                "snapshot_prefix": "",
            }
        )

        restored: list[Any] = []

        for i, snap in enumerate(to_verify, 1):
            name = snap.get_name()

            if on_progress:
                on_progress(i, len(to_verify), name)

            start = time.monotonic()
            result = VerifyResult(
                snapshot_name=name,
                level=VerifyLevel.FULL,
                passed=True,
            )

            try:
                # Find parent
                parent = _find_parent_snapshot(snap, backup_snapshots + restored)

                # Restore to temp location
                logger.info("Test restoring %s...", name)
                _test_restore(backup_endpoint, local_endpoint, snap, parent)

                # Verify restored snapshot
                restored_path = temp_path / name
                if not restored_path.exists():
                    raise VerifyError(f"Restored snapshot not found at {restored_path}")

                if not __util__.is_subvolume(restored_path):  # type: ignore[attr-defined]
                    raise VerifyError(
                        f"Restored path {restored_path} is not a valid subvolume"
                    )

                result.message = "Full restore verified successfully"
                result.details["restored_path"] = str(restored_path)
                result.details["incremental"] = parent is not None

                # Track for potential use as parent
                restored.append(snap)

            except Exception as e:
                result.passed = False
                result.message = f"Full verification failed: {e}"
                logger.error("Full verify failed for %s: %s", name, e)

            result.duration_seconds = time.monotonic() - start
            report.results.append(result)

    except Exception as e:
        report.errors.append(f"Verification failed: {e}")
        logger.error("Full verification failed: %s", e)

    finally:
        # Cleanup
        if cleanup and own_temp:
            logger.info("Cleaning up temp directory...")
            try:
                # Delete any restored subvolumes first
                for snap in to_verify:
                    snap_path = temp_path / snap.get_name()
                    if snap_path.exists() and __util__.is_subvolume(snap_path):  # type: ignore[attr-defined]
                        __util__.delete_subvolume(snap_path)  # type: ignore[attr-defined]
                # Remove temp dir
                if temp_path.exists():
                    shutil.rmtree(temp_path, ignore_errors=True)
            except Exception as e:
                logger.warning("Cleanup failed: %s", e)

    report.completed_at = time.time()
    return report


def _find_parent_snapshot(snapshot, all_snapshots: list):
    """Find the parent snapshot for incremental operations.

    Returns the most recent snapshot that is older than the given snapshot.
    """
    candidates = []
    for s in all_snapshots:
        if s.get_name() != snapshot.get_name() and s < snapshot:
            candidates.append(s)

    if not candidates:
        return None

    return max(candidates, key=lambda s: s.time_obj if hasattr(s, "time_obj") else 0)


def _test_send_stream(endpoint, snapshot, parent=None):
    """Test that btrfs send stream can be generated.

    Uses --no-data flag to verify stream without transferring data.
    """
    import subprocess

    snapshot_path = Path(endpoint.config["path"]) / snapshot.get_name()

    cmd = ["btrfs", "send", "--no-data"]
    if parent:
        parent_path = Path(endpoint.config["path"]) / parent.get_name()
        cmd.extend(["-p", str(parent_path)])
    cmd.append(str(snapshot_path))

    # For SSH endpoints, wrap command
    if hasattr(endpoint, "ssh_client") and endpoint.ssh_client:
        # Execute via SSH
        cmd_str = " ".join(cmd)
        if endpoint.config.get("ssh_sudo"):
            cmd_str = f"sudo {cmd_str}"

        stdin, stdout, stderr = endpoint.ssh_client.exec_command(
            f"{cmd_str} > /dev/null"
        )
        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            err = stderr.read().decode().strip()
            raise VerifyError(f"Send stream test failed: {err}")
    else:
        # Local execution
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=300,
        )
        if result.returncode != 0:
            raise VerifyError(
                f"Send stream test failed: {result.stderr.decode().strip()}"
            )


def _test_restore(backup_endpoint, local_endpoint, snapshot, parent=None):
    """Perform actual test restore."""
    from .operations import send_snapshot

    send_snapshot(
        snapshot,
        local_endpoint,
        parent=parent,
        options={"show_progress": False},
    )
