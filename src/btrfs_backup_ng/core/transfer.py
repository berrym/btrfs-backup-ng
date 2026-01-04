"""Transfer utilities: compression and bandwidth throttling.

Provides stream processing for btrfs send/receive pipelines.
"""

import logging
import shutil
import subprocess
from typing import Optional

logger = logging.getLogger(__name__)

# Available compression programs with their compress/decompress commands
COMPRESSION_PROGRAMS = {
    "gzip": {
        "compress": ["gzip", "-c"],
        "decompress": ["gzip", "-dc"],
        "check": "gzip",
    },
    "zstd": {
        "compress": ["zstd", "-c", "-T0"],  # -T0 uses all CPU cores
        "decompress": ["zstd", "-dc", "-T0"],
        "check": "zstd",
    },
    "lz4": {
        "compress": ["lz4", "-c"],
        "decompress": ["lz4", "-dc"],
        "check": "lz4",
    },
    "pigz": {
        "compress": ["pigz", "-c"],  # Parallel gzip
        "decompress": ["pigz", "-dc"],
        "check": "pigz",
    },
    "lzop": {
        "compress": ["lzop", "-c"],
        "decompress": ["lzop", "-dc"],
        "check": "lzop",
    },
}


def check_compression_available(method: str) -> bool:
    """Check if a compression method is available on the system.

    Args:
        method: Compression method name (gzip, zstd, lz4, pigz, lzop)

    Returns:
        True if the compression program is available
    """
    if method == "none" or not method:
        return True

    if method not in COMPRESSION_PROGRAMS:
        logger.warning("Unknown compression method: %s", method)
        return False

    check_cmd = COMPRESSION_PROGRAMS[method]["check"]
    return shutil.which(check_cmd) is not None


def check_pv_available() -> bool:
    """Check if pv (pipe viewer) is available for bandwidth limiting."""
    return shutil.which("pv") is not None


def parse_rate_limit(rate_str: Optional[str]) -> Optional[int]:
    """Parse a rate limit string like '10M', '1G', '500K' to bytes per second.

    Args:
        rate_str: Rate limit string with optional suffix (K, M, G)

    Returns:
        Rate in bytes per second, or None if no limit
    """
    if not rate_str:
        return None

    rate_str = rate_str.strip().upper()

    multipliers = {
        "K": 1024,
        "M": 1024 * 1024,
        "G": 1024 * 1024 * 1024,
    }

    # Check for suffix
    if rate_str[-1] in multipliers:
        try:
            value = float(rate_str[:-1])
            return int(value * multipliers[rate_str[-1]])
        except ValueError:
            logger.warning("Invalid rate limit format: %s", rate_str)
            return None
    else:
        # Assume bytes
        try:
            return int(rate_str)
        except ValueError:
            logger.warning("Invalid rate limit format: %s", rate_str)
            return None


def create_compress_process(
    method: str,
    stdin,
    stdout=subprocess.PIPE,
) -> Optional[subprocess.Popen]:
    """Create a compression subprocess.

    Args:
        method: Compression method (gzip, zstd, lz4, etc.)
        stdin: Input pipe (from btrfs send)
        stdout: Output pipe (default: PIPE)

    Returns:
        Popen object for the compression process, or None if no compression
    """
    if method == "none" or not method:
        return None

    if method not in COMPRESSION_PROGRAMS:
        logger.warning("Unknown compression method %s, skipping compression", method)
        return None

    if not check_compression_available(method):
        logger.warning(
            "Compression program %s not available, skipping compression", method
        )
        return None

    cmd = COMPRESSION_PROGRAMS[method]["compress"]
    logger.debug("Starting compression process: %s", cmd)

    return subprocess.Popen(
        cmd,
        stdin=stdin,
        stdout=stdout,
        stderr=subprocess.PIPE,
    )


def create_decompress_process(
    method: str,
    stdin,
    stdout=subprocess.PIPE,
) -> Optional[subprocess.Popen]:
    """Create a decompression subprocess.

    Args:
        method: Compression method (gzip, zstd, lz4, etc.)
        stdin: Input pipe (compressed stream)
        stdout: Output pipe (to btrfs receive)

    Returns:
        Popen object for the decompression process, or None if no compression
    """
    if method == "none" or not method:
        return None

    if method not in COMPRESSION_PROGRAMS:
        logger.warning("Unknown compression method %s, skipping decompression", method)
        return None

    if not check_compression_available(method):
        logger.warning(
            "Compression program %s not available, skipping decompression", method
        )
        return None

    cmd = COMPRESSION_PROGRAMS[method]["decompress"]
    logger.debug("Starting decompression process: %s", cmd)

    return subprocess.Popen(
        cmd,
        stdin=stdin,
        stdout=stdout,
        stderr=subprocess.PIPE,
    )


def create_progress_process(
    stdin,
    stdout=subprocess.PIPE,
) -> Optional[subprocess.Popen]:
    """Create a progress display subprocess using pv.

    Shows transfer progress (bytes, rate, time, ETA) without rate limiting.

    Args:
        stdin: Input pipe
        stdout: Output pipe

    Returns:
        Popen object for the pv process, or None if pv not available
    """
    if not check_pv_available():
        logger.debug(
            "pv (pipe viewer) not available, progress display disabled. "
            "Install with: dnf install pv"
        )
        return None

    # Build pv command with progress display options
    # -f forces output even when stderr is not a TTY
    cmd = [
        "pv",
        "-f",
        "-p",
        "-t",
        "-e",
        "-r",
        "-b",
    ]  # force, progress, time, eta, rate, bytes

    logger.debug("Starting progress process: %s", cmd)

    return subprocess.Popen(
        cmd,
        stdin=stdin,
        stdout=stdout,
        stderr=None,  # Let progress output go to stderr (terminal)
    )


def create_throttle_process(
    rate_limit: Optional[str],
    stdin,
    stdout=subprocess.PIPE,
    show_progress: bool = True,
) -> Optional[subprocess.Popen]:
    """Create a bandwidth throttling subprocess using pv.

    Args:
        rate_limit: Rate limit string (e.g., '10M', '1G')
        stdin: Input pipe
        stdout: Output pipe
        show_progress: Whether to show progress bar (default True)

    Returns:
        Popen object for the pv process, or None if no throttling
    """
    if not rate_limit:
        return None

    if not check_pv_available():
        logger.warning(
            "pv (pipe viewer) not available, bandwidth throttling disabled. "
            "Install with: dnf install pv"
        )
        return None

    rate_bytes = parse_rate_limit(rate_limit)
    if rate_bytes is None:
        return None

    # Build pv command
    cmd = ["pv"]

    # Add rate limit
    cmd.extend(["-L", str(rate_bytes)])

    # Add progress display options
    if show_progress:
        # -f forces output even when stderr is not a TTY
        cmd.extend(
            ["-f", "-p", "-t", "-e", "-r", "-b"]
        )  # force, progress, time, eta, rate, bytes
    else:
        cmd.append("-q")  # quiet mode

    logger.debug("Starting throttle process: %s (rate: %d bytes/s)", cmd, rate_bytes)

    return subprocess.Popen(
        cmd,
        stdin=stdin,
        stdout=stdout,
        stderr=subprocess.PIPE if not show_progress else None,
    )


def build_transfer_pipeline(
    send_stdout,
    compress: str = "none",
    rate_limit: Optional[str] = None,
    show_progress: bool = True,
):
    """Build a transfer pipeline with optional compression and throttling.

    Creates a chain of processes:
    btrfs send -> [compress] -> [throttle] -> output

    Args:
        send_stdout: stdout from btrfs send process
        compress: Compression method
        rate_limit: Bandwidth limit string
        show_progress: Whether to show progress

    Returns:
        Tuple of (final_stdout, process_list) where:
        - final_stdout is the pipe to feed to btrfs receive
        - process_list is list of intermediate processes to monitor/cleanup
    """
    processes = []
    current_stdout = send_stdout

    # Add compression if requested
    if compress and compress != "none":
        compress_proc = create_compress_process(compress, stdin=current_stdout)
        if compress_proc:
            processes.append(("compress", compress_proc))
            current_stdout = compress_proc.stdout
            logger.info("Transfer compression enabled: %s", compress)

    # Add throttling if requested (includes progress display)
    if rate_limit:
        throttle_proc = create_throttle_process(
            rate_limit,
            stdin=current_stdout,
            show_progress=show_progress,
        )
        if throttle_proc:
            processes.append(("throttle", throttle_proc))
            current_stdout = throttle_proc.stdout
            logger.info("Transfer rate limited to: %s", rate_limit)
    elif show_progress:
        # Add progress display without rate limiting
        progress_proc = create_progress_process(stdin=current_stdout)
        if progress_proc:
            processes.append(("progress", progress_proc))
            current_stdout = progress_proc.stdout
            logger.debug("Transfer progress display enabled")

    return current_stdout, processes


def build_receive_pipeline(
    input_stdout,
    compress: str = "none",
):
    """Build a receive-side pipeline with optional decompression.

    Creates a chain:
    input -> [decompress] -> output (to btrfs receive)

    Args:
        input_stdout: Input pipe (from network/compressed stream)
        compress: Compression method to decompress

    Returns:
        Tuple of (final_stdout, process_list)
    """
    processes = []
    current_stdout = input_stdout

    # Add decompression if compression was used
    if compress and compress != "none":
        decompress_proc = create_decompress_process(compress, stdin=current_stdout)
        if decompress_proc:
            processes.append(("decompress", decompress_proc))
            current_stdout = decompress_proc.stdout
            logger.debug("Transfer decompression enabled: %s", compress)

    return current_stdout, processes


def cleanup_pipeline(processes: list) -> None:
    """Clean up pipeline processes.

    Args:
        processes: List of (name, Popen) tuples
    """
    for name, proc in processes:
        try:
            if proc.poll() is None:  # Still running
                proc.terminate()
                proc.wait(timeout=5)
        except Exception as e:
            logger.warning("Error cleaning up %s process: %s", name, e)
            try:
                proc.kill()
            except Exception:
                pass


def wait_for_pipeline(processes: list, timeout: int = 3600) -> list[int]:
    """Wait for all pipeline processes to complete.

    Args:
        processes: List of (name, Popen) tuples
        timeout: Timeout in seconds

    Returns:
        List of return codes
    """
    return_codes = []
    for name, proc in processes:
        try:
            rc = proc.wait(timeout=timeout)
            return_codes.append(rc)
            if rc != 0:
                stderr = ""
                if proc.stderr:
                    stderr = proc.stderr.read().decode("utf-8", errors="replace")
                logger.error("%s process failed with code %d: %s", name, rc, stderr)
        except subprocess.TimeoutExpired:
            logger.error("Timeout waiting for %s process", name)
            proc.kill()
            return_codes.append(-1)

    return return_codes


def get_available_compression_methods() -> list[str]:
    """Get list of available compression methods on this system.

    Returns:
        List of available compression method names
    """
    available = ["none"]
    for method in COMPRESSION_PROGRAMS:
        if check_compression_available(method):
            available.append(method)
    return available
