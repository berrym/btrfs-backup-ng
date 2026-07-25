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

    Checks, per snapshot:
    - Structural validity -- that the entry is a REAL backup artifact, not just a
      name-shaped directory entry: a received read-only btrfs subvolume, or a raw stream
      with an authoritative ``.meta`` sidecar. The endpoint checks its own kind via
      ``verify_structure`` (polymorphic). A plain directory left by an interrupted receive
      FAILS instead of passing; a subvolume whose received status cannot be confirmed
      (missing privilege) is reported ``unverifiable`` rather than failed.
    - Authoritative incremental-parent continuity -- for a raw snapshot that records its
      parent's name in the sidecar, that the parent actually exists among the backups (a
      recorded-but-absent parent is an unrestorable chain break). btrfs snapshots carry no
      recorded parent name here, so their chain is validated at ``--level full``.
    - Optionally compares with source to find missing backups.

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

        # The set of names actually present at the backup -- the INDEPENDENT reference a
        # recorded parent is checked against. (The old check drew the parent FROM this same
        # list and then tested membership IN it: a tautology that could never fail.)
        present_names = {s.get_name() for s in backup_snapshots}

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

            # Check 1: structural validity -- is this a REAL backup artifact (a received
            # subvolume / a sidecar-backed raw stream), not merely a name-shaped entry?
            # The endpoint validates its own kind (polymorphic verify_structure).
            structure = backup_endpoint.verify_structure(snap)
            result.details["structure"] = structure.status
            result.message = structure.message
            if structure.is_failure:
                result.passed = False
            # 'unverifiable' keeps passed=True (never fail a good backup the environment
            # could not prove) but the message surfaces it -- not read as a clean pass.

            # Check 2: authoritative incremental-parent continuity. A raw snapshot records
            # its parent's NAME in the sidecar; a parent recorded but absent from the
            # backup is an unrestorable chain break. btrfs Snapshots carry no recorded
            # parent name here (getattr -> None), so this is a no-op for them.
            required_parent = getattr(snap, "parent_name", None)
            if required_parent:
                result.details["parent"] = required_parent
                if required_parent not in present_names:
                    result.passed = False
                    miss = f"missing incremental parent: {required_parent}"
                    result.message = (
                        f"{result.message}; {miss}" if result.message else miss
                    )
                    result.details["parent_missing"] = True

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

    # Initialize to_verify before try block to avoid "possibly unbound" in finally
    to_verify: list[Any] = []

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


def verify_raw_checksums(
    backup_endpoint,
    level: VerifyLevel,
    snapshot_name: str | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> VerifyReport:
    """Verify a raw target by recomputing each stream's sha256 and comparing it to the
    checksum sealed in the ``.meta`` sidecar.

    This is what ``stream``/``full`` verification MEANS for a raw:// or raw+ssh:// target
    (a raw backup is a stored send stream, not a subvolume, so ``btrfs send`` cannot be
    run on it). It routes through ``RawEndpoint.verify_stream_checksum`` -- the SAME
    primitive ``raw verify`` and the restore-time integrity guard use -- so the
    ok/corrupt/error/unverifiable classification cannot drift between them.

    A ``corrupt`` (bytes changed) or ``error`` (stream unreadable) snapshot FAILS; an
    ``unverifiable`` one (no recorded checksum, or a non-sha256 algorithm) is not a
    failure but is reported as such (never a silent green). All snapshots are checked,
    not just the latest -- the sealed-checksum compare is cheap, so there is no reason
    to leave a blind spot in the chain.
    """
    location = str(backup_endpoint.config.get("path", "unknown"))
    report = VerifyReport(level=level, location=location)

    try:
        backup_snapshots = backup_endpoint.list_snapshots()

        if not backup_snapshots:
            report.errors.append("No snapshots found at backup location")
            report.completed_at = time.time()
            return report

        if snapshot_name:
            backup_snapshots = [
                s for s in backup_snapshots if s.get_name() == snapshot_name
            ]
            if not backup_snapshots:
                report.errors.append(f"Snapshot '{snapshot_name}' not found")
                report.completed_at = time.time()
                return report

        for i, snap in enumerate(backup_snapshots, 1):
            name = snap.get_name()

            if on_progress:
                on_progress(i, len(backup_snapshots), name)

            start = time.monotonic()
            verdict = backup_endpoint.verify_stream_checksum(snap)

            result = VerifyResult(
                snapshot_name=name,
                level=level,
                # ok/unverifiable pass; corrupt/error fail -- the single source of
                # truth (ChecksumVerdict.is_failure) shared with `raw verify`'s exit code.
                passed=not verdict.is_failure,
            )
            result.details["checksum_status"] = verdict.status
            result.details["checksum_recorded"] = verdict.recorded
            result.details["checksum_computed"] = verdict.computed
            result.details["checksum_algorithm"] = verdict.algorithm
            if verdict.remote_untrusted:
                # A raw+ssh digest is computed by the untrusted remote: consistency, not
                # authenticity. Surface it so an operator does not read `ok` as tamper-proof.
                result.details["trust"] = "consistency-only (remote hash)"

            if verdict.status == "ok":
                result.message = "Checksum verified"
                if verdict.remote_untrusted:
                    result.message += " (consistency-only: remote hash)"
            elif verdict.status == "corrupt":
                rec = (verdict.recorded or "-")[:12]
                comp = (verdict.computed or "-")[:12]
                result.message = (
                    f"CORRUPT: stored stream no longer matches its sealed checksum "
                    f"(recorded={rec}... computed={comp}...)"
                )
            elif verdict.status == "error":
                result.message = "Stream could not be read to verify its checksum"
            else:  # unverifiable
                result.message = (
                    "Unverifiable: no sha256 checksum was recorded for this stream "
                    "(a legacy backup or one sealed before checksums existed)"
                )

            result.duration_seconds = time.monotonic() - start
            report.results.append(result)

    except Exception as e:
        report.errors.append(f"Verification failed: {e}")
        logger.error("Raw checksum verification failed: %s", e)

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


# =============================================================================
# Pre-Transfer Parent Validation
# =============================================================================


class ParentViability(Enum):
    """Result of parent viability check."""

    VIABLE = "viable"  # Parent is usable for incremental transfer
    MISSING = "missing"  # Parent doesn't exist at destination
    CORRUPTED = "corrupted"  # Parent exists but send stream fails
    LOCKED = "locked"  # Parent is locked by another operation
    MISMATCH = "mismatch"  # Parent exists but differs from source


@dataclass
class ParentViabilityResult:
    """Result of a parent viability check."""

    status: ParentViability
    parent_name: str | None
    message: str = ""
    fallback_to_full: bool = False
    details: dict = field(default_factory=dict)

    @property
    def is_viable(self) -> bool:
        """Return True if the parent is usable."""
        return self.status == ParentViability.VIABLE

    @property
    def should_use_full_send(self) -> bool:
        """Return True if we should fall back to full send."""
        return self.fallback_to_full or self.status != ParentViability.VIABLE


def check_parent_viability(
    snapshot,
    parent,
    source_endpoint,
    destination_endpoint,
    check_level: str = "quick",
) -> ParentViabilityResult:
    """Verify parent snapshot is usable for incremental transfer.

    This function checks whether a parent snapshot can be used for
    incremental btrfs send/receive. It verifies:
    1. Parent exists at destination
    2. Parent is not locked
    3. (Optional) Parent send stream is valid

    Args:
        snapshot: The snapshot to be transferred
        parent: The proposed parent snapshot for incremental transfer
        source_endpoint: Source endpoint where snapshot/parent exist
        destination_endpoint: Destination endpoint to verify parent
        check_level: Validation depth - "none", "quick", "stream", "paranoid"
            - none: Skip validation entirely
            - quick: Check existence and locks only
            - stream: Also test send stream with --no-data
            - paranoid: Full verification (not recommended for routine use)

    Returns:
        ParentViabilityResult with status and recommendations
    """
    if parent is None:
        return ParentViabilityResult(
            status=ParentViability.VIABLE,
            parent_name=None,
            message="No parent specified, will use full send",
            fallback_to_full=True,
        )

    parent_name = parent.get_name()
    logger.debug("Checking parent viability for %s", parent_name)

    if check_level == "none":
        logger.debug("Skipping parent validation (check_level=none)")
        return ParentViabilityResult(
            status=ParentViability.VIABLE,
            parent_name=parent_name,
            message="Validation skipped",
        )

    # Check 1: Parent exists at destination
    try:
        dest_snapshots = destination_endpoint.list_snapshots()
        dest_names = {s.get_name() for s in dest_snapshots}

        if parent_name not in dest_names:
            logger.warning("Parent %s not found at destination", parent_name)
            return ParentViabilityResult(
                status=ParentViability.MISSING,
                parent_name=parent_name,
                message=f"Parent snapshot '{parent_name}' not found at destination",
                fallback_to_full=True,
                details={"available_parents": sorted(dest_names)[-5:]},  # Last 5
            )
    except Exception as e:
        logger.warning("Could not list destination snapshots: %s", e)
        # Proceed cautiously - assume parent exists
        logger.debug("Assuming parent exists, will fail at transfer time if not")

    # Check 2: Parent is not locked
    try:
        if hasattr(parent, "locks") and parent.locks:
            logger.warning("Parent %s is locked: %s", parent_name, parent.locks)
            return ParentViabilityResult(
                status=ParentViability.LOCKED,
                parent_name=parent_name,
                message=f"Parent snapshot '{parent_name}' is locked",
                fallback_to_full=False,  # Don't auto-fallback, wait for lock
                details={"locks": list(parent.locks)},
            )
    except Exception as e:
        logger.debug("Could not check locks: %s", e)

    if check_level == "quick":
        logger.debug("Parent %s passed quick validation", parent_name)
        return ParentViabilityResult(
            status=ParentViability.VIABLE,
            parent_name=parent_name,
            message="Parent exists at destination",
        )

    # Check 3: Test send stream (for "stream" and "paranoid" levels)
    if check_level in ("stream", "paranoid"):
        try:
            logger.debug("Testing send stream with parent %s", parent_name)
            _test_send_stream(source_endpoint, snapshot, parent)
            logger.debug("Send stream test passed")
        except Exception as e:
            logger.warning("Send stream test failed with parent %s: %s", parent_name, e)
            return ParentViabilityResult(
                status=ParentViability.CORRUPTED,
                parent_name=parent_name,
                message=f"Send stream test failed: {e}",
                fallback_to_full=True,
                details={"error": str(e)},
            )

    return ParentViabilityResult(
        status=ParentViability.VIABLE,
        parent_name=parent_name,
        message=f"Parent '{parent_name}' is viable for incremental transfer",
    )


def find_viable_parent(
    snapshot,
    present_snapshots: list,
    source_endpoint,
    destination_endpoint,
    check_level: str = "quick",
    max_candidates: int = 3,
) -> ParentViabilityResult:
    """Find the best viable parent for incremental transfer.

    If the ideal parent isn't viable, tries older snapshots as fallback
    before giving up and recommending a full send.

    Args:
        snapshot: The snapshot to be transferred
        present_snapshots: Snapshots present at destination
        source_endpoint: Source endpoint
        destination_endpoint: Destination endpoint
        check_level: Validation depth (see check_parent_viability)
        max_candidates: Maximum number of parent candidates to try

    Returns:
        ParentViabilityResult with the best viable parent or fallback recommendation
    """
    if not present_snapshots:
        return ParentViabilityResult(
            status=ParentViability.MISSING,
            parent_name=None,
            message="No snapshots present at destination, using full send",
            fallback_to_full=True,
        )

    # Find candidate parents (older than snapshot)
    candidates = []
    for s in present_snapshots:
        try:
            if s < snapshot:
                candidates.append(s)
        except (TypeError, NotImplementedError):
            # Can't compare, skip
            continue

    if not candidates:
        return ParentViabilityResult(
            status=ParentViability.MISSING,
            parent_name=None,
            message="No older snapshots available as parent",
            fallback_to_full=True,
        )

    # Sort by time (most recent first)
    candidates.sort(reverse=True)

    # Try candidates in order
    tried = []
    for parent in candidates[:max_candidates]:
        parent_name = parent.get_name()
        tried.append(parent_name)

        result = check_parent_viability(
            snapshot,
            parent,
            source_endpoint,
            destination_endpoint,
            check_level,
        )

        if result.is_viable:
            logger.debug("Found viable parent: %s", parent_name)
            return result

        logger.debug(
            "Parent %s not viable (%s), trying next",
            parent_name,
            result.status.value,
        )

    # No viable parent found
    return ParentViabilityResult(
        status=ParentViability.MISSING,
        parent_name=None,
        message=f"No viable parent found after trying: {tried}",
        fallback_to_full=True,
        details={"tried_parents": tried},
    )


def validate_transfer_chain(
    snapshots_to_transfer: list,
    present_at_destination: list,
    source_endpoint,
    destination_endpoint,
    check_level: str = "quick",
) -> list[tuple[Any, ParentViabilityResult]]:
    """Validate parent chain for a series of transfers.

    For each snapshot to transfer, determines the best parent and
    validates it. Returns a list of (snapshot, parent_result) tuples
    that can be used to plan the transfer order.

    Args:
        snapshots_to_transfer: Snapshots to be transferred (in order)
        present_at_destination: Snapshots already at destination
        source_endpoint: Source endpoint
        destination_endpoint: Destination endpoint
        check_level: Validation depth

    Returns:
        List of (snapshot, ParentViabilityResult) tuples
    """
    results = []
    # Track what will be present after each transfer
    will_be_present = list(present_at_destination)

    for snapshot in snapshots_to_transfer:
        result = find_viable_parent(
            snapshot,
            will_be_present,
            source_endpoint,
            destination_endpoint,
            check_level,
        )
        results.append((snapshot, result))

        # After this transfer, snapshot will be present
        will_be_present.append(snapshot)

    return results
