"""Core backup operations: send_snapshot, sync_snapshots.

Extracted from __main__.py for modularity and reuse.
"""

import logging
import subprocess
import time
from pathlib import Path
from typing import Optional

from .. import __util__
from ..transaction import log_transaction
from . import progress as progress_utils
from . import transfer as transfer_utils
from .chunked_transfer import (
    ChunkedTransferManager,
    TransferConfig,
    TransferManifest,
    TransferStatus,
)
from .space import (
    DEFAULT_SAFETY_MARGIN_PERCENT,
    check_space_availability,
    format_space_check,
)

logger = logging.getLogger(__name__)


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
            send_process = snapshot.endpoint.send(snapshot, parent=parent, clones=clones)

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
            chunked_manager.fail_transfer(manifest, str(manifest.error_message or "Transfer failed"))
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
    receive_process = destination_endpoint.receive(subprocess.PIPE)
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
        raise __util__.SnapshotTransferError("Timeout waiting for btrfs receive")

    except Exception as e:
        try:
            receive_process.kill()
        except Exception:
            pass
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
        receive_process = destination_endpoint.receive(subprocess.PIPE)
        if receive_process is None:
            raise __util__.SnapshotTransferError("SSH receive process failed to start")

        try:
            # Track chunk progress
            chunks_sent = 0
            for chunk_data in reader.read_chunks():
                if receive_process.stdin:
                    receive_process.stdin.write(chunk_data)

                # Find and mark chunk as transferred
                if chunks_sent < len(manifest.chunks):
                    chunk = manifest.chunks[chunks_sent]
                    chunked_manager.mark_chunk_transferred(manifest, chunk.sequence)
                    chunks_sent += 1

                    if show_progress:
                        logger.info(
                            "Chunk %d/%d transferred",
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
                    stderr = receive_process.stderr.read().decode("utf-8", errors="replace")
                raise __util__.SnapshotTransferError(
                    f"SSH btrfs receive failed with code {return_code}: {stderr}"
                )

            logger.info("SSH chunked transfer complete: %d chunks", chunks_sent)

        except subprocess.TimeoutExpired:
            receive_process.kill()
            raise __util__.SnapshotTransferError("Timeout waiting for SSH btrfs receive")

        except Exception as e:
            try:
                receive_process.kill()
            except Exception:
                pass
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
        if hasattr(destination_endpoint, "_last_receive_log"):
            logger.error(
                "Check remote log file: %s", destination_endpoint._last_receive_log
            )
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
        receive_process = destination_endpoint.receive(current_stdout)
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
        return_code_receive = receive_process.wait(timeout=300)
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
    receive_process = destination_endpoint.receive(subprocess.PIPE)
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
) -> None:
    """Synchronize snapshots from source to destination.

    Args:
        source_endpoint: Source endpoint with snapshots
        destination_endpoint: Destination endpoint to receive backups
        keep_num_backups: Number of backups to retain (0 = all)
        no_incremental: If True, never use incremental transfers
        snapshot: Specific snapshot to transfer (None = use planning)
        options: Additional options dict
        **kwargs: Additional keyword arguments
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
        return

    logger.info("Going to transfer %d snapshot(s):", len(to_transfer))
    for snap in to_transfer:
        logger.info("  %s", snap)

    # Execute transfers
    _execute_transfers(
        source_endpoint,
        destination_endpoint,
        source_snapshots,
        destination_snapshots,
        to_transfer,
        no_incremental,
        options,
        **kwargs,
    )


def _execute_transfers(
    source_endpoint,
    destination_endpoint,
    source_snapshots,
    destination_snapshots,
    to_transfer,
    no_incremental,
    options,
    **kwargs,
) -> None:
    """Execute the actual snapshot transfers."""
    destination_id = destination_endpoint.get_id()

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

        except __util__.SnapshotTransferError as e:
            logger.error("Snapshot transfer failed for %s: %s", best_snapshot, e)
            logger.info("Keeping %s locked to prevent deletion.", best_snapshot)

        to_transfer.remove(best_snapshot)
        logger.debug("%d snapshots left to transfer", len(to_transfer))

    logger.info(__util__.log_heading(f"Transfers to {destination_endpoint} complete!"))
