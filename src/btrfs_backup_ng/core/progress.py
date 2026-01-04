"""Rich progress bar support for transfers.

Provides pretty progress bars with percentage, speed, and ETA for interactive use.
"""

import logging
import os
import subprocess
import sys
import threading
from typing import IO, Optional

logger = logging.getLogger(__name__)


def is_interactive() -> bool:
    """Check if we're running in an interactive terminal."""
    return sys.stdout.isatty()


def estimate_snapshot_size(
    snapshot_path: str, parent_path: Optional[str] = None
) -> Optional[int]:
    """Estimate the size of a snapshot transfer for progress display.

    For full transfers: Uses btrfs subvolume show to get exclusive data size.
    For incremental transfers: Returns None (indeterminate) since estimating
    the delta accurately is expensive and the actual transfer is usually fast.

    Args:
        snapshot_path: Path to the snapshot
        parent_path: Optional parent snapshot path for incremental transfers

    Returns:
        Estimated size in bytes, or None if unable to determine or incremental
    """
    logger.debug(
        "estimate_snapshot_size called: snapshot_path=%s, parent_path=%s",
        snapshot_path,
        parent_path,
    )

    # For incremental transfers, return None (indeterminate progress)
    if parent_path and os.path.exists(str(parent_path)):
        logger.debug("Incremental transfer - using indeterminate progress bar")
        return None

    # Try btrfs subvolume show for full transfers (requires quotas enabled)
    try:
        cmd = ["btrfs", "subvolume", "show", snapshot_path]
        if os.geteuid() != 0:
            cmd = ["sudo", "-n"] + cmd

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        logger.debug("btrfs subvolume show returned: %d", result.returncode)
        if result.returncode == 0:
            # Parse "Exclusive" line for data size (only present if quotas enabled)
            for line in result.stdout.splitlines():
                if line.strip().startswith("Exclusive"):
                    parts = line.split(":")
                    if len(parts) >= 2:
                        size_str = parts[1].strip()
                        parsed = _parse_size(size_str)
                        if parsed:
                            logger.debug(
                                "Parsed exclusive size '%s' -> %s bytes",
                                size_str,
                                parsed,
                            )
                            return parsed
        else:
            logger.debug("btrfs subvolume show failed: %s", result.stderr)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        logger.debug("Failed to get snapshot size via btrfs: %s", e)

    # Fallback: use btrfs filesystem du for accurate size (handles reflinks/dedup)
    try:
        cmd = ["btrfs", "filesystem", "du", "-s", "--raw", snapshot_path]
        if os.geteuid() != 0:
            cmd = ["sudo", "-n"] + cmd

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            # Output format with --raw:
            # Total   Exclusive  Set shared  Filename
            # 12345   6789       5000        /path
            lines = result.stdout.strip().splitlines()
            if len(lines) >= 2:
                # Parse the data line (skip header)
                parts = lines[1].split()
                if len(parts) >= 3:
                    try:
                        total_size = int(parts[0])
                        logger.debug(
                            "Estimated size via btrfs filesystem du: %d bytes",
                            total_size,
                        )
                        return total_size
                    except ValueError:
                        logger.debug("Failed to parse btrfs du output: %s", lines[1])
        else:
            logger.debug("btrfs filesystem du failed: %s", result.stderr)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        logger.debug("Failed to get snapshot size via btrfs filesystem du: %s", e)

    # Final fallback: use regular du
    try:
        cmd = ["du", "-sb", snapshot_path]
        if os.geteuid() != 0:
            cmd = ["sudo", "-n"] + cmd

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            size_str = result.stdout.split()[0]
            try:
                size = int(size_str)
                logger.debug("Estimated size via du: %d bytes", size)
                return size
            except ValueError:
                logger.debug("Failed to parse du output: %s", result.stdout)
        else:
            logger.debug("du failed: %s", result.stderr)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        logger.debug("Failed to get snapshot size via du: %s", e)

    return None


def _parse_size(size_str: str) -> Optional[int]:
    """Parse a size string like '1.23GiB' or '123.45MiB' to bytes."""
    size_str = size_str.strip()

    multipliers = {
        "B": 1,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
    }

    for suffix, multiplier in multipliers.items():
        if size_str.endswith(suffix):
            try:
                value = float(size_str[: -len(suffix)].strip())
                return int(value * multiplier)
            except ValueError:
                return None

    # Try parsing as plain bytes
    try:
        return int(float(size_str))
    except ValueError:
        return None


class RichProgressPipe:
    """A pipe wrapper that updates a Rich progress bar as data flows through.

    This wraps a file-like object and updates a Rich progress task
    as data is read from it.
    """

    def __init__(
        self,
        source: IO[bytes],
        progress,
        task_id,
        chunk_size: int = 65536,
    ):
        """Initialize the progress pipe.

        Args:
            source: Source file object to read from
            progress: Rich Progress instance
            task_id: Task ID from progress.add_task()
            chunk_size: Size of chunks to read
        """
        self.source = source
        self.progress = progress
        self.task_id = task_id
        self.chunk_size = chunk_size
        self.bytes_read = 0
        self._closed = False

    def read(self, size: int = -1) -> bytes:
        """Read data and update progress."""
        if size == -1:
            size = self.chunk_size

        data = self.source.read(size)
        if data:
            self.bytes_read += len(data)
            self.progress.update(self.task_id, advance=len(data))
        return data

    def fileno(self) -> int:
        """Return the file descriptor."""
        return self.source.fileno()

    def close(self) -> None:
        """Close the source."""
        if not self._closed:
            self._closed = True
            self.source.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class ProgressReader(threading.Thread):
    """Thread that reads from source, writes to dest, and updates progress."""

    def __init__(
        self,
        source: IO[bytes],
        dest: IO[bytes],
        progress,
        task_id,
        chunk_size: int = 65536,
    ):
        super().__init__(daemon=True)
        self.source = source
        self.dest = dest
        self.progress = progress
        self.task_id = task_id
        self.chunk_size = chunk_size
        self.bytes_transferred = 0
        self.error: Optional[Exception] = None

    def run(self):
        """Read from source, write to dest, update progress."""
        try:
            while True:
                chunk = self.source.read(self.chunk_size)
                if not chunk:
                    break
                self.dest.write(chunk)
                self.bytes_transferred += len(chunk)
                self.progress.update(self.task_id, advance=len(chunk))
        except Exception as e:
            self.error = e
        finally:
            try:
                self.dest.close()
            except Exception:
                pass


def create_rich_progress():
    """Create a Rich Progress instance configured for transfers.

    Returns:
        A configured Rich Progress instance, or None if Rich is not available
    """
    try:
        from rich.console import Console
        from rich.progress import (
            BarColumn,
            DownloadColumn,
            Progress,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
            TransferSpeedColumn,
        )

        # Use a console that handles output properly
        console = Console(
            stderr=True
        )  # Output to stderr to avoid mixing with btrfs stdout

        return Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            "[progress.percentage]{task.percentage:>3.1f}%",
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            TextColumn("elapsed:"),
            TimeElapsedColumn(),
            console=console,
            transient=False,
            refresh_per_second=10,
        )
    except ImportError:
        logger.debug("Rich library not available for progress bars")
        return None


def run_transfer_with_progress(
    send_process: subprocess.Popen,
    receive_process: subprocess.Popen,
    snapshot_name: str,
    estimated_size: Optional[int] = None,
) -> tuple[int, int]:
    """Run a transfer with Rich progress bar.

    Args:
        send_process: The btrfs send subprocess
        receive_process: The btrfs receive subprocess
        snapshot_name: Name of snapshot for display
        estimated_size: Estimated size in bytes (None for indeterminate)

    Returns:
        Tuple of (send_return_code, receive_return_code)
    """
    import time

    progress = create_rich_progress()

    if progress is None:
        # Fall back to simple pipe without progress
        return _simple_transfer(send_process, receive_process)

    # Brief pause to let any btrfs "At subvol" messages flush before progress bar
    time.sleep(0.1)
    sys.stderr.flush()
    sys.stdout.flush()

    with progress:
        task = progress.add_task(
            f"Transferring {snapshot_name}",
            total=estimated_size,
        )

        # Create reader thread to pipe data with progress updates
        reader = ProgressReader(
            source=send_process.stdout,
            dest=receive_process.stdin,
            progress=progress,
            task_id=task,
        )
        reader.start()

        # Wait for transfer to complete
        reader.join()

        # Check for errors
        if reader.error:
            logger.error("Transfer error: %s", reader.error)

        # Wait for processes
        send_rc = send_process.wait()
        receive_rc = receive_process.wait()

        # Always update to 100% complete with actual bytes transferred
        # (estimated size may differ from actual due to btrfs stream overhead)
        progress.update(
            task, total=reader.bytes_transferred, completed=reader.bytes_transferred
        )

        return send_rc, receive_rc


def _simple_transfer(
    send_process: subprocess.Popen,
    receive_process: subprocess.Popen,
) -> tuple[int, int]:
    """Simple transfer without progress display."""
    import shutil

    # Pipe data through
    if send_process.stdout and receive_process.stdin:
        shutil.copyfileobj(send_process.stdout, receive_process.stdin)
        receive_process.stdin.close()

    send_rc = send_process.wait()
    receive_rc = receive_process.wait()
    return send_rc, receive_rc
