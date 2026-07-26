"""Backup verification and integrity checking.

Provides multiple levels of verification to ensure backups are valid
and restorable:

- metadata: Quick check of snapshot existence and parent chain integrity
- stream: Verify btrfs send stream can be generated (no data transfer)
- full: Complete restore test to temporary location
"""

import logging
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from .. import __util__
from .errors import ErrorCategory, classify_error

logger = logging.getLogger(__name__)


def _can_run_btrfs_privileged() -> bool:
    """Whether this process can run ``btrfs receive`` -- i.e. is root. Full verification
    actually receives a send stream into a temp subvolume (needs CAP_SYS_ADMIN), so it
    requires the PROCESS to be root. A non-root run with passwordless sudo is deliberately
    NOT accepted here: the send/receive path escalates btrfs with a plain ``sudo`` (no
    ``-n``), which prompts for a password in the receive pipe (no TTY) and hangs/fails --
    so probing ``sudo -n`` would pass while the real receive breaks. Requiring root keeps
    verification honest and non-interactive; run the command itself via ``sudo``. This is
    a whole-run preflight: a lack of privilege is reported as an inability to verify (a
    run-level error), never as a per-snapshot backup FAILURE."""
    return os.geteuid() == 0


def _estimate_temp_shortfall(snapshot, local_endpoint, temp_path) -> str | None:
    """Best-effort local-temp space preflight for a full restore of ``snapshot``.

    Returns a human-readable message if the restore would CONFIDENTLY not fit in
    ``temp_path`` (applying the project's standard safety margin), else None. When the
    size cannot be estimated -- e.g. a remote (ssh://) source, whose subvolume a LOCAL
    ``btrfs`` command cannot measure -- returns None to PROCEED; an actual ENOSPC during
    receive is then surfaced as unverifiable. Reuses the same estimator + space check the
    transfer preflight uses (no new machinery)."""
    from .estimate import estimate_snapshot_full_size, format_size
    from .space import check_space_availability

    try:
        # verify_full is root-only, so no sudo is needed for the local estimate.
        size, _method = estimate_snapshot_full_size(snapshot.get_path(), use_sudo=False)
    except Exception:  # noqa: BLE001 - estimation is best-effort; proceed on any failure
        return None
    if size is None:
        return None  # can't estimate (e.g. remote source) -> proceed, catch ENOSPC
    try:
        space_info = local_endpoint.get_space_info(str(temp_path))
    except Exception:  # noqa: BLE001 - can't probe temp space -> proceed
        return None
    check = check_space_availability(space_info, size)
    if check.sufficient:
        return None
    return check.warning_message or (
        f"Insufficient temp space for restore test: ~{format_size(size)} needed, "
        f"{format_size(space_info.effective_available)} available"
    )


def _delete_temp_subvolume(path: Path) -> None:
    """Delete a restored subvolume from the verify temp dir. verify_full is root-only, so
    no sudo escalation is needed (the subvolume was received as root, and we run as root).
    Raises RuntimeError with the btrfs stderr on failure so the caller can log the real
    reason rather than a bare exit code."""
    result = subprocess.run(
        ["btrfs", "subvolume", "delete", str(path)],
        capture_output=True,
        timeout=600,
    )
    if result.returncode != 0:
        stderr = (result.stderr or b"").decode(errors="replace").strip()
        raise RuntimeError(
            stderr or f"btrfs subvolume delete exited {result.returncode}"
        )


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

                # Test send stream generation -- polymorphic: the endpoint runs
                # `btrfs send --no-data` where the subvolume actually lives (locally for a
                # local target, ON THE REMOTE for an ssh:// target), raising on failure.
                backup_endpoint.test_send_stream(snap, parent)
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

    Actually restores snapshot(s) to a temporary btrfs location and verifies the restored
    subvolume is valid -- the most thorough check (it reads every data block of the target,
    so btrfs's own checksums catch at-rest corruption), but the slowest.

    A target is restored with a FULL ``btrfs send`` (no ``-p``): a btrfs backup's received
    subvolumes are already fully materialized, so a full send proves the target is a
    complete, standalone, restorable subvolume -- and, unlike an incremental send into an
    empty temp, it does not require a parent to be present (the old code sent ``-p parent``
    into an empty temp where the parent was never restored, so ``btrfs receive`` failed and
    a GOOD backup was reported FAIL).

    False-negative-safe: a genuine restore failure (corrupt/rejected stream) is a FAILURE;
    an environmental inability to run the test (no root/sudo, insufficient/undeterminable
    temp space, temp not btrfs) is reported as UNVERIFIABLE or a run-level error, NEVER a
    per-snapshot backup failure.

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

    # Whole-run privilege preflight: a restore test actually runs `btrfs receive`, which
    # needs root or passwordless sudo. Without it we CANNOT verify -- report that inability
    # (exit 2), never a per-snapshot FAIL (a good backup is not corrupt just because we
    # lacked privilege to test it).
    if not _can_run_btrfs_privileged():
        report.errors.append(
            "Full verification requires root privileges (it runs 'btrfs receive'); "
            "run the command via sudo, e.g. 'sudo btrfs-backup-ng verify --level full ...'."
        )
        report.completed_at = time.time()
        return report

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
                "snap_prefix": "",
            }
        )

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

            # Per-snapshot space preflight (best-effort). A CONFIDENT shortfall is an
            # inability to run the test -> UNVERIFIABLE (passed stays True), not a FAIL.
            shortfall = _estimate_temp_shortfall(snap, local_endpoint, temp_path)
            if shortfall is not None:
                result.details["status"] = "unverifiable"
                result.message = shortfall
                result.duration_seconds = time.monotonic() - start
                report.results.append(result)
                continue

            try:
                # FULL send (no parent): materialize the target standalone in temp. Reads
                # every data block of the target -> btrfs checksums catch corruption.
                logger.info("Test restoring %s (full send)...", name)
                _test_restore(backup_endpoint, local_endpoint, snap, None)

                # Verify restored snapshot
                restored_path = temp_path / name
                if not restored_path.exists():
                    raise VerifyError(f"Restored snapshot not found at {restored_path}")

                if not __util__.is_subvolume(restored_path):  # type: ignore[attr-defined]
                    raise VerifyError(
                        f"Restored path {restored_path} is not a valid subvolume"
                    )

                result.message = "Full restore verified successfully"
                result.details["status"] = "ok"
                result.details["restored_path"] = str(restored_path)
                result.details["full_send"] = True

            except Exception as e:
                # Distinguish an ENVIRONMENTAL inability from a GENUINE restore failure.
                # Under root-only, the ONLY expected per-snapshot environmental condition is
                # running OUT OF SPACE in the temp fs -- so ONLY a space signal maps to
                # UNVERIFIABLE, and everything else (including corruption AND the anomalous
                # permission error, which a root receive should never hit) is a real
                # FAILURE. Mapping ONLY space (not permission) is deliberate: it means a
                # corruption error whose stderr happens to mention "permission" can never be
                # mis-downgraded to unverifiable and hide a bad backup.
                #
                # Two space signals: (a) the transfer's OWN preflight raises
                # __util__.InsufficientSpaceError BEFORE the receive (its message is not "no
                # space left", so match by TYPE); (b) a real ENOSPC during receive -- the
                # enriched SnapshotTransferError now carries the btrfs stderr ("No space left
                # on device"), which the structured classifier recognizes as TRANSIENT_SPACE.
                out_of_space = (
                    isinstance(e, __util__.InsufficientSpaceError)
                    or classify_error(e).category == ErrorCategory.TRANSIENT_SPACE
                )
                if out_of_space:
                    result.details["status"] = "unverifiable"
                    result.message = (
                        f"Restore test could not run (out of temp space): {e}"
                    )
                    logger.warning(
                        "Full verify unverifiable (out of space) for %s", name
                    )
                else:
                    result.passed = False
                    result.details["status"] = "failed"
                    result.message = f"Full verification failed: {e}"
                    logger.error("Full verify failed for %s: %s", name, e)

            result.duration_seconds = time.monotonic() - start
            report.results.append(result)

    except Exception as e:
        report.errors.append(f"Verification failed: {e}")
        logger.error("Full verification failed: %s", e)

    finally:
        # Cleanup. Delete EVERY restored subvolume regardless of who owns the temp dir --
        # a user-supplied --temp-dir must never leak received subvolumes (which carry a
        # received_uuid and could be mistaken for real backups). Only the temp DIRECTORY
        # itself is removed just for a temp we created (never a user-supplied one).
        if cleanup:
            for snap in to_verify:
                snap_path = temp_path / snap.get_name()
                try:
                    if snap_path.exists() and __util__.is_subvolume(snap_path):  # type: ignore[attr-defined]
                        _delete_temp_subvolume(snap_path)
                except Exception as e:  # noqa: BLE001 - cleanup is best-effort
                    logger.warning(
                        "Could not delete restored subvolume %s: %s", snap_path, e
                    )
            if own_temp and temp_path.exists():
                try:
                    shutil.rmtree(temp_path, ignore_errors=True)
                except Exception as e:  # noqa: BLE001
                    logger.warning("Could not remove temp directory: %s", e)

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


def _test_restore(backup_endpoint, local_endpoint, snapshot, parent=None):
    """Perform actual test restore."""
    from .operations import send_snapshot

    send_snapshot(
        snapshot,
        local_endpoint,
        parent=parent,
        options={"show_progress": False},
    )
