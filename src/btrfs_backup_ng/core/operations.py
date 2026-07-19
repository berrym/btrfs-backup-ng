"""Core backup operations: send_snapshot, sync_snapshots.

Extracted from __main__.py for modularity and reuse.
"""

import logging
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from .. import __util__
from ..transaction import log_transaction
from . import progress as progress_utils
from . import transfer as transfer_utils
from .chunked_transfer import (
    ChunkedTransferManager,
    TransferManifest,
)
from .space import (
    DEFAULT_SAFETY_MARGIN_PERCENT,
    check_space_availability,
    format_space_check,
)

logger = logging.getLogger(__name__)


@dataclass
class TransferResult:
    """Outcome of a multi-snapshot synchronization.

    ``transferred`` holds the snapshots that were verified onto the destination;
    ``failed`` holds ``(snapshot, error)`` pairs for those whose transfer failed.
    A sync with any failures raises ``SnapshotTransferError`` with this object
    attached as ``err.result`` so the caller can report precise counts (e.g.
    "3 of 5 transferred, 2 failed") instead of inferring success from the mere
    absence of an exception.
    """

    transferred: list = field(default_factory=list)
    failed: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed

    @property
    def transferred_count(self) -> int:
        return len(self.transferred)

    @property
    def failed_count(self) -> int:
        return len(self.failed)

    @property
    def attempted(self) -> int:
        return len(self.transferred) + len(self.failed)


def _raise_transfer_failures(result: "TransferResult", label: str) -> None:
    """Raise SnapshotTransferError (with ``result`` attached) if any transfer failed.

    Fail-loud: a caller cannot accidentally treat a partial or total failure as
    success, and the attached result carries the transferred/failed breakdown for
    accurate exit codes and notifications.
    """
    if result.ok:
        return
    names = ", ".join(str(item) for item, _ in result.failed)
    err: Any = __util__.SnapshotTransferError(
        f"{result.failed_count} of {result.attempted} {label} transfer(s) "
        f"failed: {names}"
    )
    err.result = result
    raise err


def send_snapshot(
    snapshot,
    destination_endpoint,
    parent=None,
    clones=None,
    options=None,
    chunked_manager: Optional[ChunkedTransferManager] = None,
    resume_transfer_id: Optional[str] = None,
) -> Optional[str]:
    """Send a snapshot to destination endpoint using btrfs send/receive.

    Args:
        snapshot: Source snapshot to send
        destination_endpoint: Endpoint to receive the snapshot
        parent: Optional parent snapshot for incremental transfer
        clones: Optional clone sources
        options: Additional options dict (ssh_sudo, use_chunked, etc.)
        chunked_manager: Optional ChunkedTransferManager for chunked transfers
        resume_transfer_id: Optional transfer ID to resume

    Returns:
        Transfer ID if chunked transfer was used, None otherwise
    """
    if options is None:
        options = {}

    logger.info("Sending %s ...", snapshot)
    logger.debug("Source endpoint type: %s", type(snapshot.endpoint).__name__)
    logger.debug("Destination endpoint type: %s", type(destination_endpoint).__name__)
    logger.debug("Source snapshot path: %s", snapshot.get_path())
    logger.debug("Destination path: %s", destination_endpoint.config["path"])

    # Verify destination path is accessible
    _ensure_destination_exists(destination_endpoint)

    # Pre-flight space check (enabled by default, can be disabled with options)
    check_space = options.get("check_space", True)
    force = options.get("force", False)
    if check_space and not force:
        _verify_destination_space(snapshot, destination_endpoint, parent, options)

    log_msg = (
        f"  Using parent: {parent}"
        if parent
        else "  No parent snapshot available, sending in full mode."
    )
    logger.info(log_msg)
    if clones:
        logger.info(f"  Using clones: {clones!r}")

    # Check if chunked transfer is requested
    use_chunked = options.get("use_chunked", False)
    if use_chunked and chunked_manager is None:
        # Create a default manager if not provided
        chunked_manager = ChunkedTransferManager()

    # Handle chunked transfer path
    if use_chunked:
        assert chunked_manager is not None  # guaranteed by above check
        return _do_chunked_transfer(
            snapshot=snapshot,
            destination_endpoint=destination_endpoint,
            parent=parent,
            clones=clones,
            options=options,
            chunked_manager=chunked_manager,
            resume_transfer_id=resume_transfer_id,
        )

    # Standard (non-chunked) transfer path
    send_process = None
    receive_process = None
    transfer_start = time.monotonic()
    estimated_size = None

    # Log transaction start
    source_path = str(snapshot.get_path())
    dest_path = str(destination_endpoint.config.get("path", ""))
    snapshot_name = str(snapshot)
    parent_name = str(parent) if parent else None

    log_transaction(
        action="transfer",
        status="started",
        source=source_path,
        destination=dest_path,
        snapshot=snapshot_name,
        parent=parent_name,
    )

    try:
        logger.debug("Starting send process from %s", snapshot.endpoint)
        send_process = snapshot.endpoint.send(snapshot, parent=parent, clones=clones)

        if send_process is None:
            logger.error("Failed to start send process - send_process is None")
            raise __util__.SnapshotTransferError("Send process failed to start")

        logger.debug("Send process started successfully")

        # Check if using SSH destination
        is_ssh_endpoint = (
            hasattr(destination_endpoint, "_is_remote")
            and destination_endpoint._is_remote
        )

        # Propagate ssh_sudo option
        if is_ssh_endpoint and options.get("ssh_sudo", False):
            destination_endpoint.config["ssh_sudo"] = True

        # Check for direct SSH pipe capability
        use_direct_pipe = is_ssh_endpoint and hasattr(
            destination_endpoint, "send_receive"
        )

        # Get compression, rate limit, and progress options
        compress = options.get("compress", "none")
        rate_limit = options.get("rate_limit")
        show_progress = options.get("show_progress", False)

        logger.debug(
            "Transfer options: compress=%s, rate_limit=%s, show_progress=%s",
            compress,
            rate_limit,
            show_progress,
        )
        logger.debug(
            "use_direct_pipe=%s, is_ssh_endpoint=%s", use_direct_pipe, is_ssh_endpoint
        )

        # Get snapshot info for progress display
        snapshot_name = str(snapshot)
        snapshot_path = str(snapshot.get_path())
        parent_path = str(parent.get_path()) if parent else None
        estimated_size = None
        if show_progress:
            logger.debug(
                "Getting size estimate for: %s (parent: %s)", snapshot_path, parent_path
            )
            estimated_size = progress_utils.estimate_snapshot_size(
                snapshot_path, parent_path
            )
            if estimated_size:
                logger.debug(
                    "Estimated transfer size: %d bytes (%.2f MB)",
                    estimated_size,
                    estimated_size / (1024 * 1024),
                )
            else:
                logger.debug("Could not estimate transfer size")

        if use_direct_pipe:
            return_codes = _do_direct_pipe_transfer(
                snapshot,
                destination_endpoint,
                parent,
                clones,
                send_process,
                show_progress=show_progress,
            )
        else:
            return_codes = _do_process_transfer(
                send_process,
                destination_endpoint,
                receive_process,
                is_ssh_endpoint,
                compress=compress,
                rate_limit=rate_limit,
                show_progress=show_progress,
                snapshot_name=snapshot_name,
                estimated_size=estimated_size,
            )

        if any(rc != 0 for rc in return_codes):
            error_message = (
                f"btrfs send/receive failed with return codes: {return_codes}"
            )
            logger.error(error_message)
            _log_process_errors(send_process, receive_process)
            raise __util__.SnapshotTransferError(error_message)

        logger.info("Transfer completed successfully")

        # Log successful transaction
        duration = time.monotonic() - transfer_start
        log_transaction(
            action="transfer",
            status="completed",
            source=source_path,
            destination=dest_path,
            snapshot=snapshot_name,
            parent=parent_name,
            duration_seconds=duration,
            size_bytes=estimated_size,
        )

    except __util__.SnapshotTransferError as e:
        # Log failed transaction
        duration = time.monotonic() - transfer_start
        log_transaction(
            action="transfer",
            status="failed",
            source=source_path,
            destination=dest_path,
            snapshot=snapshot_name,
            parent=parent_name,
            duration_seconds=duration,
            error=str(e),
        )
        raise

    except (OSError, subprocess.CalledProcessError) as e:
        logger.error("Error during snapshot transfer: %r", e)
        _log_subprocess_error(e, destination_endpoint)
        # Log failed transaction
        duration = time.monotonic() - transfer_start
        log_transaction(
            action="transfer",
            status="failed",
            source=source_path,
            destination=dest_path,
            snapshot=snapshot_name,
            parent=parent_name,
            duration_seconds=duration,
            error=str(e),
        )
        raise __util__.SnapshotTransferError(f"Exception during transfer: {e}") from e

    finally:
        _cleanup_processes(send_process, receive_process)

    return None  # No chunked transfer ID for standard transfers


def _do_chunked_transfer(
    snapshot,
    destination_endpoint,
    parent,
    clones,
    options: dict,
    chunked_manager: ChunkedTransferManager,
    resume_transfer_id: Optional[str] = None,
) -> str:
    """Perform a chunked transfer with resume capability.

    This function splits the btrfs send stream into checksummed chunks,
    transfers them individually with verification, and reassembles them
    at the destination for btrfs receive.

    Args:
        snapshot: Source snapshot to send
        destination_endpoint: Endpoint to receive the snapshot
        parent: Optional parent snapshot for incremental transfer
        clones: Optional clone sources
        options: Transfer options dict
        chunked_manager: The ChunkedTransferManager instance
        resume_transfer_id: Optional transfer ID to resume from

    Returns:
        The transfer ID for tracking/resume

    Raises:
        SnapshotTransferError: If transfer fails
    """
    transfer_start = time.monotonic()
    source_path = str(snapshot.get_path())
    dest_path = str(destination_endpoint.config.get("path", ""))
    snapshot_name = str(snapshot)
    parent_name = str(parent) if parent else None
    show_progress = options.get("show_progress", False)

    manifest: Optional[TransferManifest] = None

    try:
        # Check if we're resuming an existing transfer
        if resume_transfer_id:
            manifest = chunked_manager.resume_transfer(resume_transfer_id)
            if manifest is None:
                raise __util__.SnapshotTransferError(
                    f"Cannot resume transfer {resume_transfer_id}: not found or not resumable"
                )
            logger.info(
                "Resuming chunked transfer %s from chunk %d/%d",
                manifest.transfer_id,
                manifest.get_resume_point() or 0,
                manifest.chunk_count,
            )
        else:
            # Create a new chunked transfer
            manifest = chunked_manager.create_transfer(
                snapshot_path=source_path,
                snapshot_name=snapshot_name,
                destination=str(destination_endpoint),
                parent_path=str(parent.get_path()) if parent else None,
                parent_name=parent_name,
            )

            log_transaction(
                action="chunked_transfer",
                status="started",
                source=source_path,
                destination=dest_path,
                snapshot=snapshot_name,
                parent=parent_name,
            )

            # Start btrfs send and chunk the stream
            logger.info("Starting chunked transfer %s", manifest.transfer_id)
            send_process = snapshot.endpoint.send(
                snapshot, parent=parent, clones=clones
            )

            if send_process is None:
                raise __util__.SnapshotTransferError("Send process failed to start")

            # Chunk the send stream
            logger.info("Chunking btrfs send stream...")

            def on_chunk_progress(chunk_num: int, total: int, bytes_done: int) -> None:
                if show_progress:
                    mb_done = bytes_done / (1024 * 1024)
                    logger.info(
                        "Chunking progress: chunk %d, %.1f MB written",
                        chunk_num,
                        mb_done,
                    )

            try:
                manifest = chunked_manager.chunk_stream(
                    manifest,
                    send_process.stdout,
                    on_progress=on_chunk_progress,
                )
            finally:
                # Ensure send process is cleaned up
                try:
                    send_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    send_process.kill()
                    send_process.wait()

            # The send exit code is authoritative. If `btrfs send` failed (parent
            # UUID mismatch, I/O error, killed) it closes stdout early, so chunking
            # sees EOF and records a TRUNCATED manifest. Without this check the
            # truncated stream would be transferred and reported as a successful
            # backup.
            if send_process.returncode not in (0, None):
                send_stderr = ""
                if send_process.stderr is not None:
                    try:
                        send_stderr = send_process.stderr.read().decode(
                            errors="replace"
                        )
                    except Exception:
                        pass
                raise __util__.SnapshotTransferError(
                    f"btrfs send failed during chunking "
                    f"(exit {send_process.returncode}): {send_stderr}"
                )

            logger.info(
                "Chunking complete: %d chunks, %d bytes",
                manifest.chunk_count,
                manifest.total_size or 0,
            )

        # Now transfer chunks to destination
        transfer_id = manifest.transfer_id
        is_ssh_endpoint = (
            hasattr(destination_endpoint, "_is_remote")
            and destination_endpoint._is_remote
        )

        if is_ssh_endpoint:
            # Use SSH chunked transfer
            _transfer_chunks_ssh(
                manifest=manifest,
                destination_endpoint=destination_endpoint,
                chunked_manager=chunked_manager,
                options=options,
                show_progress=show_progress,
            )
        else:
            # Use local chunked transfer
            _transfer_chunks_local(
                manifest=manifest,
                destination_endpoint=destination_endpoint,
                chunked_manager=chunked_manager,
                options=options,
                show_progress=show_progress,
            )

        # Mark transfer as complete
        chunked_manager.complete_transfer(manifest)

        duration = time.monotonic() - transfer_start
        log_transaction(
            action="chunked_transfer",
            status="completed",
            source=source_path,
            destination=dest_path,
            snapshot=snapshot_name,
            parent=parent_name,
            duration_seconds=duration,
            size_bytes=manifest.total_size,
        )

        logger.info(
            "Chunked transfer %s completed successfully in %.1fs",
            transfer_id,
            duration,
        )

        return transfer_id

    except __util__.SnapshotTransferError:
        if manifest:
            chunked_manager.fail_transfer(
                manifest, str(manifest.error_message or "Transfer failed")
            )
        raise

    except Exception as e:
        logger.error("Error during chunked transfer: %s", e)
        if manifest:
            chunked_manager.fail_transfer(manifest, str(e))

        duration = time.monotonic() - transfer_start
        log_transaction(
            action="chunked_transfer",
            status="failed",
            source=source_path,
            destination=dest_path,
            snapshot=snapshot_name,
            parent=parent_name,
            duration_seconds=duration,
            error=str(e),
        )

        raise __util__.SnapshotTransferError(f"Chunked transfer failed: {e}") from e


def _transfer_chunks_local(
    manifest: TransferManifest,
    destination_endpoint,
    chunked_manager: ChunkedTransferManager,
    options: dict,
    show_progress: bool = False,
) -> None:
    """Transfer chunks to a local destination and reassemble.

    For local transfers, we reassemble the chunks directly into btrfs receive.

    Args:
        manifest: Transfer manifest with chunk information
        destination_endpoint: Local destination endpoint
        chunked_manager: The ChunkedTransferManager
        options: Transfer options
        show_progress: Whether to show progress
    """
    logger.info("Reassembling %d chunks for local btrfs receive", manifest.chunk_count)

    # Create a reader to reassemble chunks
    reader = chunked_manager.create_reassembly_reader(manifest)

    # Start btrfs receive
    receive_process = destination_endpoint.receive(
        subprocess.PIPE, manifest.snapshot_name
    )
    if receive_process is None:
        raise __util__.SnapshotTransferError("Receive process failed to start")

    try:
        # Pipe chunks to receive
        total_bytes = reader.pipe_to_process(receive_process)

        # Wait for receive to complete
        return_code = receive_process.wait(timeout=3600)

        if return_code != 0:
            stderr = ""
            if receive_process.stderr:
                stderr = receive_process.stderr.read().decode("utf-8", errors="replace")
            raise __util__.SnapshotTransferError(
                f"btrfs receive failed with code {return_code}: {stderr}"
            )

        # Mark all chunks as transferred
        for chunk in manifest.chunks:
            chunked_manager.mark_chunk_transferred(manifest, chunk.sequence)

        logger.info("Local reassembly complete: %d bytes", total_bytes)

    except subprocess.TimeoutExpired:
        receive_process.kill()
        _cleanup_partial_local_subvolume(
            destination_endpoint, Path(manifest.snapshot_path).name
        )
        raise __util__.SnapshotTransferError("Timeout waiting for btrfs receive")

    except Exception:
        try:
            receive_process.kill()
        except Exception:
            pass
        _cleanup_partial_local_subvolume(
            destination_endpoint, Path(manifest.snapshot_path).name
        )
        raise


def _transfer_chunks_ssh(
    manifest: TransferManifest,
    destination_endpoint,
    chunked_manager: ChunkedTransferManager,
    options: dict,
    show_progress: bool = False,
) -> None:
    """Transfer chunks to an SSH destination.

    For SSH transfers, we can either:
    1. Transfer each chunk individually and reassemble remotely
    2. Stream reassembled chunks through SSH pipe

    This implementation uses option 2 for simplicity - streaming through
    the existing SSH pipe mechanism but with resume capability.

    Args:
        manifest: Transfer manifest with chunk information
        destination_endpoint: SSH destination endpoint
        chunked_manager: The ChunkedTransferManager
        options: Transfer options
        show_progress: Whether to show progress
    """
    # Get pending chunks (for resume support)
    pending_chunks = manifest.pending_chunks

    if not pending_chunks:
        logger.info("All chunks already transferred")
        return

    logger.info(
        "Transferring %d chunks to SSH destination (resume point: %d/%d)",
        len(pending_chunks),
        manifest.completed_chunks,
        manifest.chunk_count,
    )

    # For SSH, we stream the reassembled chunks through the SSH pipe
    # The destination will receive them as a continuous btrfs send stream
    reader = chunked_manager.create_reassembly_reader(manifest)

    # Use the endpoint's send_receive if available for direct pipe
    if hasattr(destination_endpoint, "receive_chunked"):
        # Future: dedicated chunked receive method
        success = destination_endpoint.receive_chunked(
            reader,
            manifest,
            show_progress=show_progress,
        )
        if not success:
            raise __util__.SnapshotTransferError("SSH chunked receive failed")
    else:
        # Fall back to streaming through regular receive
        # Start btrfs receive on remote
        receive_process = destination_endpoint.receive(
            subprocess.PIPE, manifest.snapshot_name
        )
        if receive_process is None:
            raise __util__.SnapshotTransferError("SSH receive process failed to start")

        # Track chunk progress
        chunks_sent = 0
        try:
            for chunk_data in reader.read_chunks():
                if receive_process.stdin:
                    receive_process.stdin.write(chunk_data)
                chunks_sent += 1
                if show_progress:
                    logger.info(
                        "Chunk %d/%d streamed",
                        chunks_sent,
                        manifest.chunk_count,
                    )

            # Close stdin to signal end of stream
            if receive_process.stdin:
                receive_process.stdin.close()

            # Wait for receive to complete
            return_code = receive_process.wait(timeout=3600)

            if return_code != 0:
                stderr = ""
                if receive_process.stderr:
                    stderr = receive_process.stderr.read().decode(
                        "utf-8", errors="replace"
                    )
                raise __util__.SnapshotTransferError(
                    f"SSH btrfs receive failed with code {return_code}: {stderr}"
                )

            # Only NOW, after `btrfs receive` has confirmed success, mark the
            # chunks transferred. Marking them as they were written to stdin (before
            # receive completed) meant a receive that failed after ingesting bytes
            # left a manifest full of TRANSFERRED chunks -- on the next run
            # pending_chunks was empty, so nothing was re-sent and the failed
            # transfer looked resumable-complete. btrfs receive applies the whole
            # stream atomically, so all-or-nothing marking is correct.
            for chunk in manifest.chunks:
                chunked_manager.mark_chunk_transferred(manifest, chunk.sequence)

            logger.info("SSH chunked transfer complete: %d chunks", chunks_sent)

        except subprocess.TimeoutExpired:
            receive_process.kill()
            _cleanup_partial_remote_subvolume(destination_endpoint, manifest)
            raise __util__.SnapshotTransferError(
                "Timeout waiting for SSH btrfs receive"
            )

        except Exception as e:
            try:
                receive_process.kill()
            except Exception:
                pass
            _cleanup_partial_remote_subvolume(destination_endpoint, manifest)
            # Re-raise with context about which chunk failed
            if chunks_sent < len(manifest.chunks):
                chunk = manifest.chunks[chunks_sent]
                chunked_manager.mark_chunk_failed(manifest, chunk.sequence, str(e))
            raise


def _verify_destination_space(snapshot, destination_endpoint, parent, options) -> None:
    """Verify destination has sufficient space for the transfer.

    Args:
        snapshot: Source snapshot to send
        destination_endpoint: Destination endpoint
        parent: Parent snapshot for incremental (or None for full)
        options: Options dict with safety_margin, etc.

    Raises:
        InsufficientSpaceError: If space is insufficient and force is not set
    """
    safety_margin = options.get("safety_margin", DEFAULT_SAFETY_MARGIN_PERCENT)

    try:
        # Get space info from destination
        space_info = destination_endpoint.get_space_info()

        # Estimate transfer size
        snapshot_path = str(snapshot.get_path())
        parent_path = str(parent.get_path()) if parent else None
        estimated_size = progress_utils.estimate_snapshot_size(
            snapshot_path, parent_path
        )

        if estimated_size is None:
            # Can't estimate size, log warning and proceed
            logger.warning(
                "Could not estimate transfer size for space check, proceeding anyway"
            )
            return

        # Check space availability
        space_check = check_space_availability(
            space_info, estimated_size, safety_margin_percent=safety_margin
        )

        if not space_check.sufficient:
            logger.error("Destination space check failed:")
            logger.error(format_space_check(space_check))
            raise __util__.InsufficientSpaceError(
                space_check.warning_message
                or f"Insufficient space at destination: need {estimated_size} bytes"
            )
        else:
            logger.debug(
                "Space check passed: %d bytes available, %d bytes needed",
                space_check.effective_limit,
                space_check.required_with_margin,
            )
            if space_check.warning_message:
                logger.warning(space_check.warning_message)

    except __util__.InsufficientSpaceError:
        raise
    except Exception as e:
        # Log warning but don't block the transfer
        logger.warning("Could not verify destination space: %s", e)
        logger.debug("Proceeding with transfer despite space check failure")


def _ensure_destination_exists(destination_endpoint) -> None:
    """Ensure destination path exists, creating it if necessary."""
    try:
        if (
            hasattr(destination_endpoint, "_is_remote")
            and destination_endpoint._is_remote
        ):
            if hasattr(destination_endpoint, "_exec_remote_command"):
                path = destination_endpoint._normalize_path(
                    destination_endpoint.config["path"]
                )
                logger.debug("Ensuring remote destination path exists: %s", path)
                cmd = ["test", "-d", path]
                result = destination_endpoint._exec_remote_command(cmd, check=False)
                if result.returncode != 0:
                    logger.warning(
                        "Destination path doesn't exist, creating it: %s", path
                    )
                    mkdir_cmd = ["mkdir", "-p", path]
                    mkdir_result = destination_endpoint._exec_remote_command(
                        mkdir_cmd, check=False
                    )
                    if mkdir_result.returncode != 0:
                        stderr = mkdir_result.stderr.decode("utf-8", errors="replace")
                        logger.error(
                            "Failed to create destination directory: %s", stderr
                        )
                        raise __util__.SnapshotTransferError(
                            f"Cannot create destination directory: {stderr}"
                        )
        else:
            path = destination_endpoint.config.get("path")
            if path:
                path_obj = Path(path)
                if not path_obj.exists():
                    logger.warning(
                        "Local destination path doesn't exist, creating it: %s", path
                    )
                    path_obj.mkdir(parents=True, exist_ok=True)
    except __util__.SnapshotTransferError:
        raise
    except Exception as e:
        logger.warning(
            "Error during destination verification (will try transfer anyway): %s", e
        )


def _do_direct_pipe_transfer(
    snapshot,
    destination_endpoint,
    parent,
    clones,
    send_process,
    show_progress: bool = False,
) -> list[int]:
    """Perform transfer using SSH direct pipe method."""
    try:
        logger.debug("Using SSH direct pipe transfer method")

        # Close the send process since we'll use direct pipe
        if send_process:
            send_process.terminate()
            send_process.wait()

        success = destination_endpoint.send_receive(
            snapshot,
            parent=parent,
            clones=clones,
            timeout=3600,  # 1 hour timeout
            show_progress=show_progress,
        )

        if not success:
            raise __util__.SnapshotTransferError("SSH direct pipe transfer failed")

        return [0, 0]

    except Exception as e:
        logger.error("Error during SSH direct pipe transfer: %s", e)
        raise __util__.SnapshotTransferError(f"SSH direct pipe transfer failed: {e}")


def _do_process_transfer(
    send_process,
    destination_endpoint,
    receive_process,
    is_ssh_endpoint,
    compress: str = "none",
    rate_limit: str | None = None,
    show_progress: bool = False,
    snapshot_name: str = "",
    estimated_size: int | None = None,
) -> list[int]:
    """Perform transfer using traditional process piping.

    Args:
        send_process: btrfs send subprocess
        destination_endpoint: Destination endpoint
        receive_process: Placeholder for receive process
        is_ssh_endpoint: Whether destination is SSH
        compress: Compression method (none, gzip, zstd, lz4, etc.)
        rate_limit: Bandwidth limit (e.g., '10M', '1G')
        show_progress: Whether to show progress bars
        snapshot_name: Name of snapshot for progress display
        estimated_size: Estimated transfer size for progress bar

    Returns:
        List of return codes from all processes
    """
    logger.debug("Using traditional send/receive process approach")

    # Check if we can use Rich progress (no compression or rate limiting)
    # Only show Rich progress for full transfers (where we know the size)
    # For incremental transfers (estimated_size is None), skip progress display
    # since they typically complete in under a second
    use_rich_progress = (
        show_progress
        and estimated_size is not None  # Only for full transfers with known size
        and (compress == "none" or not compress)
        and not rate_limit
        and progress_utils.is_interactive()
    )

    # For incremental transfers, skip pv progress too (too fast to be useful)
    skip_progress_for_incremental = show_progress and estimated_size is None

    logger.debug(
        "Progress decision: show_progress=%s, estimated_size=%s, compress=%s, rate_limit=%s -> use_rich=%s",
        show_progress,
        estimated_size,
        compress,
        rate_limit,
        use_rich_progress,
    )

    if use_rich_progress:
        return _do_rich_progress_transfer(
            send_process,
            destination_endpoint,
            is_ssh_endpoint,
            snapshot_name,
            estimated_size,
        )

    pipeline_processes = []
    current_stdout = send_process.stdout

    # For incremental transfers, don't show pv progress (too fast to be useful)
    effective_show_progress = show_progress and not skip_progress_for_incremental

    try:
        # Build transfer pipeline with compression and throttling
        if compress != "none" or rate_limit or effective_show_progress:
            current_stdout, pipeline_processes = transfer_utils.build_transfer_pipeline(
                send_stdout=send_process.stdout,
                compress=compress,
                rate_limit=rate_limit,
                show_progress=effective_show_progress,
            )

        # Start receive process with potentially modified input stream
        receive_process = destination_endpoint.receive(current_stdout, snapshot_name)
        if receive_process is None:
            logger.error("Failed to start receive process")
            if is_ssh_endpoint and not destination_endpoint.config.get(
                "ssh_sudo", False
            ):
                logger.error("Try using --ssh-sudo for SSH destinations")
            raise __util__.SnapshotTransferError("Receive process failed to start")
    except __util__.SnapshotTransferError:
        transfer_utils.cleanup_pipeline(pipeline_processes)
        raise
    except Exception as e:
        transfer_utils.cleanup_pipeline(pipeline_processes)
        logger.error("Failed to start receive process: %s", e)
        raise __util__.SnapshotTransferError(f"Receive process failed to start: {e}")

    timeout_seconds = 3600  # 1 hour

    try:
        return_code_send = send_process.wait(timeout=timeout_seconds)
        logger.debug("Send process completed with return code: %d", return_code_send)
    except subprocess.TimeoutExpired:
        logger.error("Timeout waiting for send process")
        send_process.kill()
        transfer_utils.cleanup_pipeline(pipeline_processes)
        raise __util__.SnapshotTransferError("Timeout waiting for send process")

    # Wait for pipeline processes
    pipeline_return_codes = transfer_utils.wait_for_pipeline(
        pipeline_processes, timeout=timeout_seconds
    )

    try:
        # Match the send timeout: applying a large received stream can legitimately
        # take well over the old 300s, and killing it here manufactures exactly the
        # partial subvolume this path must avoid.
        return_code_receive = receive_process.wait(timeout=timeout_seconds)
        logger.debug(
            "Receive process completed with return code: %d", return_code_receive
        )
    except subprocess.TimeoutExpired:
        logger.error("Timeout waiting for receive process")
        receive_process.kill()
        raise __util__.SnapshotTransferError("Timeout waiting for receive process")

    return [return_code_send] + pipeline_return_codes + [return_code_receive]


def _do_rich_progress_transfer(
    send_process,
    destination_endpoint,
    is_ssh_endpoint: bool,
    snapshot_name: str,
    estimated_size: int | None,
) -> list[int]:
    """Perform transfer with Rich progress bar display.

    Args:
        send_process: btrfs send subprocess
        destination_endpoint: Destination endpoint
        is_ssh_endpoint: Whether destination is SSH
        snapshot_name: Name of snapshot for progress display
        estimated_size: Estimated transfer size for progress bar

    Returns:
        List of return codes [send_rc, receive_rc]
    """
    logger.debug("Using Rich progress bar for transfer")

    # Start receive process (stderr is suppressed at the endpoint level)
    receive_process = destination_endpoint.receive(subprocess.PIPE, snapshot_name)
    if receive_process is None:
        logger.error("Failed to start receive process")
        if is_ssh_endpoint and not destination_endpoint.config.get("ssh_sudo", False):
            logger.error("Try using --ssh-sudo for SSH destinations")
        raise __util__.SnapshotTransferError("Receive process failed to start")

    # Run transfer with Rich progress
    try:
        send_rc, receive_rc = progress_utils.run_transfer_with_progress(
            send_process=send_process,
            receive_process=receive_process,
            snapshot_name=snapshot_name or "snapshot",
            estimated_size=estimated_size,
        )
        return [send_rc, receive_rc]
    except Exception as e:
        logger.error("Error during Rich progress transfer: %s", e)
        # Try to clean up
        try:
            send_process.kill()
        except Exception:
            pass
        try:
            receive_process.kill()
        except Exception:
            pass
        raise __util__.SnapshotTransferError(f"Transfer failed: {e}")


def _log_process_errors(send_process, receive_process) -> None:
    """Log stderr from send/receive processes."""
    if hasattr(send_process, "stderr") and send_process.stderr:
        send_err = send_process.stderr.read().decode("utf-8", errors="replace")
        if send_err:
            logger.error("Send process stderr: %s", send_err)

    if (
        receive_process
        and hasattr(receive_process, "stderr")
        and receive_process.stderr
    ):
        recv_err = receive_process.stderr.read().decode("utf-8", errors="replace")
        if recv_err:
            logger.error("Receive process stderr: %s", recv_err)


def _log_subprocess_error(e, destination_endpoint) -> None:
    """Log detailed error information from subprocess failure."""
    if hasattr(e, "stderr") and e.stderr:
        stderr = e.stderr.decode("utf-8", errors="replace")
        logger.error("Process stderr: %s", stderr)
        if (
            "permission denied" in stderr.lower() or "sudo" in stderr.lower()
        ) and hasattr(destination_endpoint, "_is_remote"):
            logger.error("This appears to be a permission issue")
            logger.error("For SSH destinations, use --ssh-sudo")
            logger.error(
                "Add to remote /etc/sudoers: username ALL=(ALL) NOPASSWD: /usr/bin/btrfs"
            )

    if hasattr(e, "stdout") and e.stdout:
        logger.error("Process stdout: %s", e.stdout.decode("utf-8", errors="replace"))


def _cleanup_processes(send_process, receive_process) -> None:
    """Clean up subprocess pipes."""
    for pipe in [send_process, receive_process]:
        if pipe:
            try:
                if hasattr(pipe, "stdout") and pipe.stdout:
                    pipe.stdout.close()
                if hasattr(pipe, "stdin") and pipe.stdin:
                    pipe.stdin.close()
                if hasattr(pipe, "stderr") and pipe.stderr:
                    pipe.stderr.close()
            except (AttributeError, IOError) as e:
                logger.warning("Error closing pipe: %s", e)


def sync_snapshots(
    source_endpoint,
    destination_endpoint,
    keep_num_backups=0,
    no_incremental=False,
    snapshot=None,
    options=None,
    **kwargs,
) -> TransferResult:
    """Synchronize snapshots from source to destination.

    Args:
        source_endpoint: Source endpoint with snapshots
        destination_endpoint: Destination endpoint to receive backups
        keep_num_backups: Number of backups to retain (0 = all)
        no_incremental: If True, never use incremental transfers
        snapshot: Specific snapshot to transfer (None = use planning)
        options: Additional options dict
        **kwargs: Additional keyword arguments

    Returns:
        TransferResult with the transferred/failed breakdown.

    Raises:
        SnapshotTransferError: if any planned snapshot failed to transfer. The
            exception carries the TransferResult as ``err.result``.
    """
    from .planning import plan_transfers

    logger.info(__util__.log_heading(f"  To {destination_endpoint} ..."))

    # List all source snapshots
    all_source_snapshots = source_endpoint.list_snapshots()

    if snapshot is None:
        source_snapshots = all_source_snapshots
        snapshots_to_transfer = None
    else:
        source_snapshots = all_source_snapshots
        snapshots_to_transfer = [snapshot]

    destination_snapshots = destination_endpoint.list_snapshots()

    # Clear locks for this destination
    destination_id = destination_endpoint.get_id()
    for snap in source_snapshots:
        if destination_id in snap.locks:
            source_endpoint.set_lock(snap, destination_id, False)
        if destination_id in snap.parent_locks:
            source_endpoint.set_lock(snap, destination_id, False, parent=True)

    logger.debug("Source snapshots found: %d", len(source_snapshots))
    logger.debug("Destination snapshots found: %d", len(destination_snapshots))

    # Plan transfers
    if snapshots_to_transfer is not None:
        to_transfer = [
            snap for snap in snapshots_to_transfer if snap not in destination_snapshots
        ]
    else:
        to_transfer = plan_transfers(
            source_snapshots, destination_snapshots, keep_num_backups
        )

    if not to_transfer:
        logger.info("No snapshots need to be transferred.")
        return TransferResult()

    logger.info("Going to transfer %d snapshot(s):", len(to_transfer))
    for snap in to_transfer:
        logger.info("  %s", snap)

    # Execute transfers
    result = _execute_transfers(
        source_endpoint,
        destination_endpoint,
        source_snapshots,
        destination_snapshots,
        to_transfer,
        no_incremental,
        options,
        **kwargs,
    )

    # Fail loud: any failed transfer must surface as an exception so callers
    # cannot mistake the absence of an exception for success.
    _raise_transfer_failures(result, "snapshot")
    return result


def _cleanup_partial_local_subvolume(destination_endpoint, name: str) -> None:
    """Best-effort removal of a partial received subvolume after a failed transfer.

    A killed or failed local ``btrfs receive`` leaves an incomplete subvolume at
    ``{dest}/{name}`` (``name`` is the received subvolume's on-disk name, i.e. the
    SOURCE basename), which the next run's skip-detection would enumerate and
    mistake for a completed backup -- silently skipping the real transfer. Remove
    it so a re-run starts clean.

    Safety (the received subvolume is never a good backup here):
      * only called on the failure path, so a successful transfer never triggers it;
      * skip-detection already excluded any snapshot whose name is present at the
        destination BEFORE the transfer was attempted, so anything now at the exact
        path is this failed run's partial, not a prior good backup;
      * scoped to the EXACT single path via ``Path.exists()`` -- never a
        filesystem-wide name search -- so siblings are untouched.

    Only handles LOCAL btrfs destinations: SSH endpoints clean their own partials
    during the transfer, and raw destinations are handled by the raw cleanup path.
    """
    import os

    from ..endpoint.raw import RawEndpoint

    if getattr(destination_endpoint, "_is_remote", False):
        return
    if isinstance(destination_endpoint, RawEndpoint):
        return

    base = str(destination_endpoint.config["path"]).rstrip("/")
    expected = f"{base}/{name}"
    try:
        if not Path(expected).exists():
            return
        logger.warning("Cleaning up partial local transfer artifact at %s", expected)
        sudo = [] if os.geteuid() == 0 else ["sudo"]
        deleted = subprocess.run(
            [*sudo, "btrfs", "subvolume", "delete", expected],
            capture_output=True,
        )
        if deleted.returncode != 0:
            # A killed receive can leave a plain directory rather than a subvolume.
            subprocess.run([*sudo, "rm", "-rf", expected], capture_output=True)
    except Exception as cleanup_e:
        logger.debug(
            "Partial local-subvolume cleanup failed for %s: %s", expected, cleanup_e
        )


def _cleanup_partial_remote_subvolume(destination_endpoint, manifest) -> None:
    """Best-effort removal of a partial REMOTE subvolume after a failed chunked
    SSH transfer, using the endpoint's own exact-path cleaner when present.

    Scoped to the exact received path (``{dest}/{source_basename}``) by the
    underlying ``_cleanup_partial_subvolume``, which guards on existence and never
    searches by name -- so a good backup is never deleted. A no-op for endpoints
    that do not expose the cleaner.
    """
    cleaner = getattr(destination_endpoint, "_cleanup_partial_subvolume", None)
    if cleaner is None:
        return
    try:
        dest_path = str(destination_endpoint.config["path"])
        received_name = Path(manifest.snapshot_path).name
        cleaner(dest_path, received_name)
    except Exception as e:
        logger.debug("Partial remote-subvolume cleanup failed: %s", e)


def _cleanup_partial_raw_stream(destination_endpoint) -> None:
    """Best-effort removal of the exact raw stream file a failed raw transfer wrote.

    Raw backups use a distinct timestamped filename each run, and a generic raw
    backup carries no ``.meta`` sidecar (``finalize_receive`` is unused), so a
    failed partial is NOT overwritten by the next run and cannot be distinguished
    from a complete backup by "missing .meta". ``discover_raw_snapshots``' filename
    fallback would therefore re-list the partial as a phantom backup.

    Delete ONLY the exact ``_pending_metadata['stream_path']`` this run wrote --
    never a name pattern or a "no .meta" heuristic (which would destroy a good
    generic raw backup). Called only on the failure path where the transfer
    pipeline exited nonzero, so the stream file is genuinely incomplete.
    """
    from ..endpoint.raw import RawEndpoint, SSHRawEndpoint

    if not isinstance(destination_endpoint, RawEndpoint):
        return
    pending = getattr(destination_endpoint, "_pending_metadata", None)
    if not pending:
        return
    stream_path = pending.get("stream_path")
    if not stream_path:
        return
    try:
        if isinstance(destination_endpoint, SSHRawEndpoint):
            logger.warning(
                "Cleaning up partial remote raw stream file at %s", stream_path
            )
            destination_endpoint._exec_remote_command(
                ["rm", "-f", str(stream_path)], check=False
            )
        else:
            p = Path(stream_path)
            if p.exists():
                logger.warning("Cleaning up partial raw stream file at %s", p)
                p.unlink()
    except Exception as e:
        logger.debug("Partial raw-stream cleanup failed for %s: %s", stream_path, e)


def _execute_transfers(
    source_endpoint,
    destination_endpoint,
    source_snapshots,
    destination_snapshots,
    to_transfer,
    no_incremental,
    options,
    **kwargs,
) -> TransferResult:
    """Execute the actual snapshot transfers.

    Returns a TransferResult recording which snapshots were verified onto the
    destination and which failed. Locks are released and the snapshot registered
    at the destination only for verified successes; a failed snapshot is kept
    locked and recorded in ``result.failed`` (never silently dropped).
    """
    destination_id = destination_endpoint.get_id()
    result = TransferResult()

    while to_transfer:
        if no_incremental:
            best_snapshot = to_transfer[-1]
            parent = None
        else:
            # Find snapshots present on destination for incremental
            present_snapshots = [
                snap
                for snap in source_snapshots
                if snap in destination_snapshots and snap.get_name() not in snap.locks
            ]

            def key(s):
                p = s.find_parent(present_snapshots)
                if p is None:
                    return float("inf")
                d = source_snapshots.index(s) - source_snapshots.index(p)
                return -d if d < 0 else d

            best_snapshot = min(to_transfer, key=key)
            parent = best_snapshot.find_parent(present_snapshots)

        # Set locks
        source_endpoint.set_lock(best_snapshot, destination_id, True)
        if parent:
            source_endpoint.set_lock(parent, destination_id, True, parent=True)

        try:
            logger.info("Starting transfer of %s", best_snapshot)
            send_snapshot(
                best_snapshot,
                destination_endpoint,
                parent=parent,
                options=options or {},
            )
            logger.info("Transfer of %s completed successfully", best_snapshot)

            # Release locks
            source_endpoint.set_lock(best_snapshot, destination_id, False)
            if parent:
                source_endpoint.set_lock(parent, destination_id, False, parent=True)

            # Update destination
            destination_endpoint.add_snapshot(best_snapshot)
            try:
                destination_endpoint.list_snapshots()
            except Exception as e:
                logger.debug("Post-transfer snapshot list refresh failed: %s", e)

            result.transferred.append(best_snapshot)

        except __util__.SnapshotTransferError as e:
            logger.error("Snapshot transfer failed for %s: %s", best_snapshot, e)
            logger.info("Keeping %s locked to prevent deletion.", best_snapshot)
            result.failed.append((best_snapshot, e))
            # Remove any partial received subvolume the failed transfer left at the
            # destination so the next run's skip-detection cannot mistake it for a
            # completed backup. Local btrfs and raw (whose distinct-per-run stream
            # file would otherwise be re-listed as a phantom backup) are cleaned
            # here; SSH btrfs endpoints clean their own partials during transfer.
            _cleanup_partial_local_subvolume(
                destination_endpoint, best_snapshot.get_name()
            )
            _cleanup_partial_raw_stream(destination_endpoint)

        to_transfer.remove(best_snapshot)
        logger.debug("%d snapshots left to transfer", len(to_transfer))

    logger.info(__util__.log_heading(f"Transfers to {destination_endpoint} complete!"))
    return result


# =============================================================================
# Snapper Integration Operations
# =============================================================================


def parse_min_age(min_age: str) -> timedelta:
    """Parse a min_age string like '1h', '30m', '2d' into a timedelta.

    Args:
        min_age: Age string with unit suffix (s, m, h, d, w)

    Returns:
        timedelta representing the duration

    Raises:
        ValueError: If format is invalid
    """
    if not min_age or min_age == "0":
        return timedelta(0)

    min_age = min_age.strip().lower()

    # Map of suffixes to timedelta kwargs
    units = {
        "s": "seconds",
        "m": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }

    for suffix, kwarg in units.items():
        if min_age.endswith(suffix):
            try:
                value = int(min_age[:-1])
                return timedelta(**{kwarg: value})
            except ValueError as e:
                raise ValueError(f"Invalid min_age value: {min_age}") from e

    # Try parsing as pure number (assume seconds)
    try:
        return timedelta(seconds=int(min_age))
    except ValueError as e:
        raise ValueError(f"Invalid min_age format: {min_age}") from e


def get_snapper_snapshots_for_backup(
    scanner,
    config_name: str,
    include_types: list[str] | None = None,
    exclude_cleanup: list[str] | None = None,
    min_age: str = "0",
) -> list:
    """Get snapper snapshots that are eligible for backup.

    Args:
        scanner: SnapperScanner instance
        config_name: Snapper config name or 'auto'
        include_types: Snapshot types to include
        exclude_cleanup: Cleanup algorithms to exclude
        min_age: Minimum age before backing up

    Returns:
        List of SnapperSnapshot objects eligible for backup
    """

    if include_types is None:
        include_types = ["single", "pre", "post"]

    snapshots = scanner.get_snapshots(
        config_name,
        include_types=include_types,
        exclude_cleanup=exclude_cleanup,
    )

    # Filter by min_age
    min_age_delta = parse_min_age(min_age)
    if min_age_delta.total_seconds() > 0:
        cutoff = datetime.now() - min_age_delta
        snapshots = [s for s in snapshots if s.date <= cutoff]

    return snapshots


def _list_backed_up_snapper_numbers(destination_endpoint) -> set[int]:
    """Return snapper snapshot numbers already present at the destination.

    btrfs targets: numbered subdirs under ``{base}/.snapshots`` that contain a
    ``snapshot`` subvolume. Raw targets have no numbered layout, so this returns
    an empty set (the caller re-sends; raw skip-detection is a follow-up).
    """
    from ..endpoint.raw import RawEndpoint

    if isinstance(destination_endpoint, RawEndpoint):
        return set()

    numbers: set[int] = set()
    base = str(destination_endpoint.config["path"]).rstrip("/")
    snap_dir = f"{base}/.snapshots"
    is_remote = getattr(destination_endpoint, "_is_remote", False)

    if is_remote and hasattr(destination_endpoint, "_exec_remote_command"):
        try:
            result = destination_endpoint._exec_remote_command(
                ["ls", "-1", snap_dir], check=False
            )
            if result.returncode == 0:
                for name in result.stdout.decode().split():
                    if name.isdigit():
                        numbers.add(int(name))
        except Exception as e:
            logger.debug("Could not list remote snapper backups: %s", e)
    else:
        snap_path = Path(snap_dir)
        if snap_path.exists():
            for item in snap_path.iterdir():
                if (
                    item.is_dir()
                    and item.name.isdigit()
                    and (item / "snapshot").exists()
                ):
                    numbers.add(int(item.name))

    return numbers


def _place_info_xml(snapper_snapshot, destination_endpoint) -> None:
    """Copy the snapper info.xml into the current destination directory.

    ``destination_endpoint.config["path"]`` is the ``.snapshots/{num}`` directory
    at call time (btrfs targets only; raw folds info.xml into the metadata sidecar).
    """
    import os
    import shutil

    info_xml_src = snapper_snapshot.info_xml_path
    if not info_xml_src.exists():
        return

    dest_dir = str(destination_endpoint.config["path"])
    is_remote = getattr(destination_endpoint, "_is_remote", False)

    try:
        if is_remote and hasattr(destination_endpoint, "_exec_remote_command"):
            destination_endpoint._exec_remote_command(
                ["tee", f"{dest_dir}/info.xml"],
                input=info_xml_src.read_bytes(),
                check=True,
                stdout=subprocess.DEVNULL,
            )
        else:
            dst = Path(dest_dir) / "info.xml"
            if os.geteuid() != 0:
                subprocess.run(
                    ["sudo", "cp", str(info_xml_src), str(dst)],
                    check=True,
                    capture_output=True,
                )
            else:
                shutil.copy2(info_xml_src, dst)
        logger.debug("Placed info.xml at %s", dest_dir)
    except Exception as e:
        logger.warning("Failed to place info.xml: %s", e)


def _cleanup_snapper_backup(destination_endpoint, snapshot_num, is_raw) -> None:
    """Best-effort removal of a partial snapper backup after a failed transfer.

    A received btrfs subvolume is read-only and cannot be removed with ``rm``, so
    the ``.snapshots/{num}/snapshot`` subvolume is deleted with
    ``btrfs subvolume delete`` before the numbered directory is removed.
    """
    import os

    if is_raw:
        # Raw partial files are overwritten on the next attempt; nothing to undo.
        return

    base = str(destination_endpoint.config["path"]).rstrip("/")
    snap_dir = f"{base}/.snapshots/{snapshot_num}"
    subvol = f"{snap_dir}/snapshot"
    is_remote = getattr(destination_endpoint, "_is_remote", False)

    try:
        if is_remote and hasattr(destination_endpoint, "_exec_remote_command"):
            destination_endpoint._exec_remote_command(
                ["btrfs", "subvolume", "delete", subvol], check=False
            )
            destination_endpoint._exec_remote_command(
                ["rm", "-rf", snap_dir], check=False
            )
        else:
            sudo = [] if os.geteuid() == 0 else ["sudo"]
            if Path(subvol).exists():
                subprocess.run(
                    [*sudo, "btrfs", "subvolume", "delete", subvol],
                    capture_output=True,
                )
            if Path(snap_dir).exists():
                subprocess.run([*sudo, "rm", "-rf", snap_dir], capture_output=True)
    except Exception as cleanup_e:
        logger.warning("Cleanup failed: %s", cleanup_e)


def send_snapper_snapshot(
    snapper_snapshot,
    destination_endpoint,
    parent_snapper_snapshot=None,
    options: dict | None = None,
) -> None:
    """Send a snapper snapshot to a destination endpoint.

    btrfs targets receive into ``{base}/.snapshots/{num}`` so the sent
    ``snapshot`` subvolume lands as ``.snapshots/{num}/snapshot`` alongside an
    ``info.xml``, matching snapper's on-disk layout. Raw targets write a single
    stream file named by the backup name. Both route through the standard
    ``send_snapshot`` pipeline, so ssh:// and raw+ssh:// work transparently.

    Args:
        snapper_snapshot: SnapperSnapshot object to send
        destination_endpoint: Destination Endpoint (its config["path"] is the base)
        parent_snapper_snapshot: Optional parent for incremental transfer
        options: Transfer options (compress, show_progress, rate_limit)

    Raises:
        SnapshotTransferError: If transfer fails
    """
    from ..endpoint.raw import RawEndpoint

    if options is None:
        options = {}

    snapshot_num = snapper_snapshot.number
    is_raw = isinstance(destination_endpoint, RawEndpoint)
    base_path = str(destination_endpoint.config["path"])

    # Skip if this snapshot number is already present at the destination.
    if snapshot_num in _list_backed_up_snapper_numbers(destination_endpoint):
        logger.info("Snapshot %d already exists at destination, skipping", snapshot_num)
        return

    parent_num = parent_snapper_snapshot.number if parent_snapper_snapshot else None
    if parent_snapper_snapshot:
        logger.info(
            "Sending snapshot %d (incremental from %d) ...", snapshot_num, parent_num
        )
    else:
        logger.info("Sending snapshot %d (full) ...", snapshot_num)

    transfer_start = time.monotonic()
    log_transaction(
        action="snapper_backup",
        status="started",
        source=str(snapper_snapshot.subvolume_path),
        destination=base_path,
        snapshot=str(snapshot_num),
        parent=str(parent_num) if parent_num else None,
    )

    # Wrap the snapper snapshots so the standard send/receive pipeline can carry
    # them (the wrapper's source is a LocalEndpoint on the snapper subvolume).
    source_wrapper = _create_snapper_snapshot_wrapper(
        snapper_snapshot, destination_endpoint
    )
    parent_wrapper = (
        _create_snapper_snapshot_wrapper(parent_snapper_snapshot, destination_endpoint)
        if parent_snapper_snapshot
        else None
    )

    try:
        if is_raw:
            # Raw targets have no subvolumes: write one stream file named by the
            # backup name. Compression/encryption are intrinsic to the raw
            # endpoint, so avoid double-compressing in the pipeline.
            raw_options = dict(options)
            raw_options["compress"] = "none"
            send_snapshot(
                source_wrapper,
                destination_endpoint,
                parent=parent_wrapper,
                options=raw_options,
            )
        else:
            # btrfs targets: temporarily point the same endpoint (reusing its SSH
            # connection) at .snapshots/{num} so receive lands
            # .snapshots/{num}/snapshot, then place info.xml beside it.
            snap_path = f"{base_path.rstrip('/')}/.snapshots/{snapshot_num}"
            saved_path = destination_endpoint.config["path"]
            destination_endpoint.config["path"] = snap_path
            try:
                send_snapshot(
                    source_wrapper,
                    destination_endpoint,
                    parent=parent_wrapper,
                    options=options,
                )
                _place_info_xml(snapper_snapshot, destination_endpoint)
            finally:
                destination_endpoint.config["path"] = saved_path

        # Metadata sidecar (endpoint-aware; carries original_xml for restore).
        _write_snapper_metadata(snapper_snapshot, destination_endpoint)

        duration = time.monotonic() - transfer_start
        log_transaction(
            action="snapper_backup",
            status="completed",
            source=str(snapper_snapshot.subvolume_path),
            destination=base_path,
            snapshot=str(snapshot_num),
            parent=str(parent_num) if parent_num else None,
            duration_seconds=duration,
        )
        logger.info(
            "Snapshot %d transferred successfully (%.1fs)", snapshot_num, duration
        )

    except Exception as e:
        duration = time.monotonic() - transfer_start
        log_transaction(
            action="snapper_backup",
            status="failed",
            source=str(snapper_snapshot.subvolume_path),
            destination=base_path,
            snapshot=str(snapshot_num),
            parent=str(parent_num) if parent_num else None,
            duration_seconds=duration,
            error=str(e),
        )
        _cleanup_snapper_backup(destination_endpoint, snapshot_num, is_raw)
        logger.error("Failed to transfer snapshot %d: %s", snapshot_num, e)
        raise __util__.SnapshotTransferError(
            f"Failed to transfer snapshot {snapshot_num}: {e}"
        )


def _create_snapper_snapshot_wrapper(snapper_snapshot, destination_endpoint=None):
    """Create a Snapshot wrapper for a snapper snapshot.

    This creates a __util__.Snapshot object that points to the snapper
    snapshot's actual subvolume path, allowing it to be used with the
    standard send/receive infrastructure.

    Args:
        snapper_snapshot: SnapperSnapshot object
        destination_endpoint: Destination endpoint; its configured
            timestamp_format is applied to the backup name (default when None)

    Returns:
        __util__.Snapshot wrapper object with a local source endpoint
    """
    from ..endpoint.local import LocalEndpoint

    # The backup name follows the format: {config}-{number}-{date}, honoring the
    # destination's configured timestamp_format (default when no endpoint given).
    date_format = (
        destination_endpoint.config.get("timestamp_format")
        if destination_endpoint is not None
        else None
    )
    backup_name = snapper_snapshot.get_backup_name(date_format)

    # Parse the date from snapper snapshot
    time_obj = snapper_snapshot.date.timetuple()

    # Create a LOCAL source endpoint for the snapper snapshot
    # This is critical - the send() method requires config["source"] to be set
    source_endpoint = LocalEndpoint(
        config={
            "source": snapper_snapshot.subvolume_path,
            "path": snapper_snapshot.subvolume_path.parent,
            "snap_prefix": "",
        }
    )

    # Create wrapper - use the snapper subvolume path as the location's parent
    # and the backup name as the effective name
    wrapper = __util__.Snapshot(
        location=snapper_snapshot.subvolume_path.parent,
        prefix="",  # No prefix - we use the full backup name
        endpoint=source_endpoint,
        time_obj=time_obj,
    )

    # Override get_name and get_path to return snapper-specific values
    # Use setattr to avoid type checker complaints about dynamic attributes
    setattr(wrapper, "_snapper_name", backup_name)
    setattr(wrapper, "_snapper_path", snapper_snapshot.subvolume_path)

    # Monkey-patch methods to return correct values
    def get_name_override():
        return getattr(wrapper, "_snapper_name")

    def get_path_override():
        return getattr(wrapper, "_snapper_path")

    wrapper.get_name = get_name_override  # type: ignore[method-assign]
    wrapper.get_path = get_path_override  # type: ignore[method-assign]

    return wrapper


def _write_snapper_metadata(snapper_snapshot, destination_endpoint) -> None:
    """Write snapper metadata file to destination.

    Args:
        snapper_snapshot: SnapperSnapshot object
        destination_endpoint: Destination endpoint
    """
    from ..snapper.metadata import BackupMetadata, save_backup_metadata

    # Read original info.xml content
    try:
        original_xml = snapper_snapshot.info_xml_path.read_text()
    except Exception as e:
        logger.warning("Could not read original info.xml: %s", e)
        original_xml = ""

    # Create backup metadata
    backup_meta = BackupMetadata.from_snapper_metadata(
        config_name=snapper_snapshot.config_name,
        metadata=snapper_snapshot.metadata,
        original_xml=original_xml,
    )

    # Determine metadata file path at destination
    backup_name = snapper_snapshot.get_backup_name(
        destination_endpoint.config.get("timestamp_format")
    )
    dest_path = Path(destination_endpoint.config["path"])
    meta_file = dest_path / f"{backup_name}.snapper-meta.json"

    # Check if destination is remote (SSH)
    is_remote = (
        hasattr(destination_endpoint, "_is_remote") and destination_endpoint._is_remote
    )

    if is_remote:
        # For SSH destinations, write via remote command
        _write_remote_metadata(destination_endpoint, meta_file, backup_meta)
    else:
        # For local destinations, write directly
        save_backup_metadata(meta_file, backup_meta)
        logger.debug("Wrote snapper metadata to %s", meta_file)


def _write_remote_metadata(endpoint, meta_path: Path, metadata) -> None:
    """Write metadata file to remote SSH destination.

    Args:
        endpoint: SSH endpoint
        meta_path: Remote path for metadata file
        metadata: BackupMetadata object
    """
    import json
    from dataclasses import asdict

    # Convert metadata to JSON
    json_content = json.dumps(asdict(metadata), indent=2)

    # Write via SSH
    if hasattr(endpoint, "_exec_remote_command"):
        # Use echo with heredoc-style input
        cmd = ["tee", str(meta_path)]
        try:
            endpoint._exec_remote_command(
                cmd,
                input=json_content.encode("utf-8"),
                check=True,
                stdout=subprocess.DEVNULL,
            )
            logger.debug("Wrote remote snapper metadata to %s", meta_path)
        except Exception as e:
            logger.warning("Failed to write remote metadata: %s", e)
    else:
        logger.warning("Cannot write metadata to remote - endpoint lacks remote exec")


def sync_snapper_snapshots(
    scanner,
    config_name: str,
    destination_endpoint,
    snapper_config=None,
    options: dict | None = None,
) -> int:
    """Synchronize snapper snapshots to a destination.

    This is the main entry point for backing up snapper-managed snapshots.
    It discovers snapper snapshots, determines which need to be backed up,
    and transfers them with metadata preservation.

    Backup layout mirrors snapper:
        {destination}/.snapshots/{num}/snapshot
        {destination}/.snapshots/{num}/info.xml

    Args:
        scanner: SnapperScanner instance
        config_name: Snapper config name
        destination_endpoint: Destination Endpoint (its config["path"] is the backup base)
        snapper_config: Optional SnapperSourceConfig with filtering options
        options: Additional transfer options

    Returns:
        Number of snapshots transferred (on full success).

    Raises:
        SnapshotTransferError: if any eligible snapshot failed to transfer. The
            exception carries a TransferResult as ``err.result`` (transferred vs
            failed) so the caller reports a non-zero exit and accurate counts.
    """
    if options is None:
        options = {}

    logger.info(__util__.log_heading(f"Syncing snapper config '{config_name}'"))

    # Get filtering options from snapper_config or use defaults
    include_types = ["single", "pre", "post"]
    exclude_cleanup = []
    min_age = "1h"

    if snapper_config:
        include_types = snapper_config.include_types
        exclude_cleanup = snapper_config.exclude_cleanup
        min_age = snapper_config.min_age

    # Get eligible snapshots
    snapper_snapshots = get_snapper_snapshots_for_backup(
        scanner,
        config_name,
        include_types=include_types,
        exclude_cleanup=exclude_cleanup,
        min_age=min_age,
    )

    if not snapper_snapshots:
        logger.info("No snapper snapshots found for backup")
        return 0

    logger.info("Found %d snapper snapshot(s) to consider", len(snapper_snapshots))

    # Get existing backup snapshot numbers at destination (endpoint-aware)
    backed_up_numbers = _list_backed_up_snapper_numbers(destination_endpoint)

    logger.debug("Already backed up: %s", sorted(backed_up_numbers))

    # Filter to snapshots not yet backed up
    to_transfer = [s for s in snapper_snapshots if s.number not in backed_up_numbers]

    if not to_transfer:
        logger.info("All snapper snapshots already backed up")
        return 0

    # Sort by number to ensure proper incremental chain
    to_transfer.sort(key=lambda s: s.number)

    logger.info("Transferring %d snapshot(s):", len(to_transfer))
    for snap in to_transfer:
        logger.info("  %d", snap.number)

    # Build a map of all available snapshots (local + already backed up)
    # for finding parents
    all_snapshots_by_num = {s.number: s for s in snapper_snapshots}

    # Transfer snapshots
    result = TransferResult()
    for i, snap in enumerate(to_transfer, 1):
        # Find parent for incremental transfer
        # Look for the highest numbered snapshot that:
        # 1. Is lower than current snapshot number
        # 2. Either already backed up OR will be backed up before this one
        parent = None
        for candidate_num in sorted(all_snapshots_by_num.keys(), reverse=True):
            if candidate_num >= snap.number:
                continue
            # Check if this candidate is available as parent
            if candidate_num in backed_up_numbers or candidate_num in [
                s.number for s in to_transfer[: i - 1]
            ]:
                parent = all_snapshots_by_num.get(candidate_num)
                break

        try:
            logger.info("[%d/%d] Snapshot %d", i, len(to_transfer), snap.number)
            send_snapper_snapshot(
                snap,
                destination_endpoint,
                parent_snapper_snapshot=parent,
                options=options,
            )
            result.transferred.append(snap)
            backed_up_numbers.add(snap.number)
        except __util__.SnapshotTransferError as e:
            logger.error("Failed to transfer snapshot %d: %s", snap.number, e)
            result.failed.append((snap, e))
            # Continue attempting the remaining snapshots; the failure is recorded
            # and surfaced below so it is never silently dropped.

    logger.info("")
    logger.info(
        "Sync complete: %d/%d transferred", result.transferred_count, len(to_transfer)
    )

    # Fail loud: if any snapshot failed, raise with the breakdown attached so the
    # caller reports a non-zero exit / failure notification instead of exit 0.
    _raise_transfer_failures(result, "snapper snapshot")

    return result.transferred_count


def _list_snapper_backups_at_destination(endpoint) -> set[str]:
    """List snapper backup names at destination.

    Looks for .snapper-meta.json files to identify snapper backups.

    Args:
        endpoint: Destination endpoint

    Returns:
        Set of backup names (without .snapper-meta.json suffix)
    """
    dest_path = Path(endpoint.config["path"])
    backup_names = set()

    is_remote = hasattr(endpoint, "_is_remote") and endpoint._is_remote

    if is_remote:
        # List remote directory
        if hasattr(endpoint, "_exec_remote_command"):
            try:
                result = endpoint._exec_remote_command(
                    ["ls", "-1", str(dest_path)],
                    check=False,
                )
                if result.returncode == 0:
                    for name in result.stdout.decode().strip().split("\n"):
                        if name.endswith(".snapper-meta.json"):
                            backup_names.add(name[:-18])  # Remove suffix
            except Exception as e:
                logger.warning("Could not list remote backups: %s", e)
    else:
        # List local directory
        try:
            if dest_path.exists():
                for item in dest_path.iterdir():
                    if item.name.endswith(".snapper-meta.json"):
                        backup_names.add(item.name[:-18])
        except Exception as e:
            logger.warning("Could not list local backups: %s", e)

    return backup_names
