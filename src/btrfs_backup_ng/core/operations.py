"""Core backup operations: send_snapshot, sync_snapshots.

Extracted from __main__.py for modularity and reuse.
"""

import logging
import subprocess
from pathlib import Path

from .. import __util__
from . import transfer as transfer_utils

logger = logging.getLogger(__name__)


def send_snapshot(
    snapshot, destination_endpoint, parent=None, clones=None, options=None
) -> None:
    """Send a snapshot to destination endpoint using btrfs send/receive.

    Args:
        snapshot: Source snapshot to send
        destination_endpoint: Endpoint to receive the snapshot
        parent: Optional parent snapshot for incremental transfer
        clones: Optional clone sources
        options: Additional options dict (ssh_sudo, etc.)
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

    log_msg = (
        f"  Using parent: {parent}"
        if parent
        else "  No parent snapshot available, sending in full mode."
    )
    logger.info(log_msg)
    if clones:
        logger.info(f"  Using clones: {clones!r}")

    send_process = None
    receive_process = None

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

        # Get compression and rate limit options
        compress = options.get("compress", "none")
        rate_limit = options.get("rate_limit")

        if use_direct_pipe:
            return_codes = _do_direct_pipe_transfer(
                snapshot, destination_endpoint, parent, clones, send_process
            )
        else:
            return_codes = _do_process_transfer(
                send_process,
                destination_endpoint,
                receive_process,
                is_ssh_endpoint,
                compress=compress,
                rate_limit=rate_limit,
            )

        if any(rc != 0 for rc in return_codes):
            error_message = (
                f"btrfs send/receive failed with return codes: {return_codes}"
            )
            logger.error(error_message)
            _log_process_errors(send_process, receive_process)
            raise __util__.SnapshotTransferError(error_message)

        logger.info("Transfer completed successfully")

    except (OSError, subprocess.CalledProcessError) as e:
        logger.error("Error during snapshot transfer: %r", e)
        _log_subprocess_error(e, destination_endpoint)
        raise __util__.SnapshotTransferError(f"Exception during transfer: {e}") from e

    finally:
        _cleanup_processes(send_process, receive_process)


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
    snapshot, destination_endpoint, parent, clones, send_process
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
) -> list[int]:
    """Perform transfer using traditional process piping.

    Args:
        send_process: btrfs send subprocess
        destination_endpoint: Destination endpoint
        receive_process: Placeholder for receive process
        is_ssh_endpoint: Whether destination is SSH
        compress: Compression method (none, gzip, zstd, lz4, etc.)
        rate_limit: Bandwidth limit (e.g., '10M', '1G')

    Returns:
        List of return codes from all processes
    """
    logger.debug("Using traditional send/receive process approach")

    pipeline_processes = []
    current_stdout = send_process.stdout

    try:
        # Build transfer pipeline with compression and throttling
        if compress != "none" or rate_limit:
            current_stdout, pipeline_processes = transfer_utils.build_transfer_pipeline(
                send_stdout=send_process.stdout,
                compress=compress,
                rate_limit=rate_limit,
                show_progress=True,
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
