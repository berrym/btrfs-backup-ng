"""Transfer utilities: compression and bandwidth throttling.

Provides stream processing for btrfs send/receive pipelines.
"""

import io
import logging
import queue
import shutil
import subprocess
import threading
import time
from typing import Any, Optional, TypedDict

from .. import __util__
from .compression import COMPRESSION_METHODS

logger = logging.getLogger(__name__)


class CompressionConfig(TypedDict):
    """Type definition for compression program configuration."""

    compress: list[str]
    decompress: list[str]
    check: str


# Available compression programs, as a view onto the one canonical table.
# `core.compression` is the definition; this name is kept because it is what the
# transfer path has always imported, and it exposes exactly the fields that path
# uses. Deriving it means the ssh:// and raw:// sides can no longer disagree
# about which methods exist -- they used to, and `compress` meant different
# things depending on the destination.
COMPRESSION_PROGRAMS: dict[str, CompressionConfig] = {
    name: {
        "compress": entry["compress"],
        "decompress": entry["decompress"],
        "check": entry["program"],
    }
    for name, entry in COMPRESSION_METHODS.items()
}


#: Wall-clock limit on a transfer. UNLIMITED by default.
#:
#: A transfer that is moving data is succeeding, slowly; ending it because a
#: clock expired confuses operator policy with fault detection. Faults are
#: detected by measuring bytes (see ``wait_with_progress``), which is the real
#: signal, so the clock is not needed to keep a broken transfer from hanging.
#:
#: Setting `transfer_timeout` in [global] is therefore an operational constraint
#: -- "this must finish before the working day" -- and not a health check. It was
#: a fixed 3600 in nine places, which ended first syncs that were working
#: perfectly: 36 GB over 100 Mbit is about 50 minutes at line rate before any
#: overhead (#93).
DEFAULT_TRANSFER_TIMEOUT = 0

#: Seconds a transfer may move NO data before it is treated as stuck. This is the
#: fault detector -- the thing the wall clock used to stand in for badly.
DEFAULT_STALL_TIMEOUT = 900

#: Applied ONLY when byte progress cannot be sampled at all -- no readable
#: /proc/<pid>/io for anything in the pipeline, or the stall check switched off.
#:
#: Guard by measurement where we can, by clock where we cannot. Without this an
#: unmeasurable transfer would have no guard whatsoever, which is worse than an
#: arbitrary one. It is deliberately generous, and its use is logged so the
#: operator knows which regime is in force.
UNMEASURABLE_FALLBACK_TIMEOUT = 86400


def check_compression_available(method: str) -> bool:
    """Check if a compression method is available on the system.

    Args:
        method: Compression method name (see COMPRESSION_PROGRAMS)

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


def popen_pipeline_pipefail(shell_cmd: str, **popen_kwargs: Any) -> subprocess.Popen:
    """Run a multi-stage shell pipeline with ``pipefail``.

    Without ``pipefail`` a shell pipeline's exit status is that of its LAST stage
    only, so a failure of an upstream stage -- ``btrfs send`` dying, or a
    compressor/``gpg`` erroring mid-stream -- is masked by the final redirect/ssh
    exiting 0, and a truncated or empty stream file is reported as a successful
    backup. ``set -o pipefail`` makes any stage's failure fail the whole pipeline
    so the returncode the caller checks is honest.

    The one pipeline runner for every LOCAL multi-stage pipeline: the raw
    endpoint's write and read-back pipelines, and the ssh endpoint's
    decompressing restore read. Uses bash (which supports ``pipefail``); falls
    back to plain ``sh`` with a warning only when bash is unavailable.
    """
    bash_path = shutil.which("bash")
    if bash_path:
        return subprocess.Popen(
            "set -o pipefail; " + shell_cmd,
            shell=True,
            executable=bash_path,
            **popen_kwargs,
        )
    logger.warning(
        "bash not found; running raw pipeline without pipefail (a mid-pipe "
        "failure may be masked and produce a truncated backup)"
    )
    return subprocess.Popen(shell_cmd, shell=True, **popen_kwargs)


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


def hand_over(pipe: Any) -> None:
    """Release this process's copy of a pipe a child has just inherited.

    After ``Popen(stdin=previous.stdout)`` the pipe has two readers: the new
    child, and this process. Only the child will ever read it, but the kernel
    keeps the pipe alive while ANY reader holds it -- so when the child dies,
    the stage writing into it blocks forever instead of taking SIGPIPE and
    ending.

    Observed on a real transfer: `btrfs-backup-ng run` asleep in a poll loop
    with one child, `pv -q -B 32M`, parked in poll_schedule_timeout on a pipe
    whose read end this process still held after the consumer had gone. The run
    never terminated, and it held its configuration's run lock the whole time,
    so the next run refused to start.

    endpoint/ssh.py does this by hand at each handoff and explains it at one of
    them. Shared here because the miss is always at the LAST handoff in a chain,
    where it is easiest to forget that the pipe was passed on rather than kept.
    """
    if pipe is None or not hasattr(pipe, "close"):
        return
    try:
        pipe.close()
    except Exception as e:  # noqa: BLE001 - a started pipeline must not fail on this
        logger.debug("Could not release a handed-over pipe: %s", e)


#: How much of a process's stderr is kept for the report: the last 64 KiB. A
#: diagnosis is at the END of the output (the error line follows whatever
#: progress preceded it), and 64 KiB is also the size of the pipe buffer, so
#: the tail is at least what read-after-exit could ever have seen.
STDERR_TAIL_BYTES = 64 * 1024


class StderrTail:
    """Drain a child's stderr as it is written, keeping the tail for the report.

    A child whose stderr is a pipe nobody reads blocks once the kernel buffer
    (64 KiB) is full. ``btrfs send``/``receive`` print one line each in normal
    use, which is why reading the pipe after the process has exited seemed to
    work -- but ``btrfs_debug`` puts ``-vv`` on both, one line per file
    operation, and a large volume under that option would fill the buffer, the
    child would stop, and the stall detector would then kill a healthy
    transfer. So the pipe is read on a thread from the moment the process
    starts, only the last ``keep`` bytes are retained, and the pipe is closed at
    EOF -- which is also what stops these pipes leaking until garbage
    collection.

    ``text()`` waits for EOF (bounded) and returns what was kept. It may be
    called any number of times.

    The tail OWNS the pipe. A pipe can have one reader, so ``tail_stderr``
    detaches it from the process (``proc.stderr`` becomes None) the moment the
    tail takes it: ``communicate()`` then returns ``(stdout, None)`` instead of
    racing the drain for bytes and reading a file the drain has closed, and
    every caller that wants the text goes through :func:`stderr_text`.
    """

    def __init__(
        self, proc: Any, keep: int = STDERR_TAIL_BYTES, log_as: str | None = None
    ) -> None:
        self._pipe = proc.stderr
        proc.stderr = None
        self._keep = keep
        self._buf = bytearray()
        self._lock = threading.Lock()
        # With ``log_as``, every complete line is also logged at DEBUG as it
        # arrives, prefixed with that name. This is what btrfs_debug means:
        # the -vv it puts on send and receive prints a line per file
        # operation, and without a reader those lines went to DEVNULL and the
        # option was a no-op. The tail for the failure report is unchanged.
        #
        # Logging happens on a SECOND thread fed by a queue. Rendering a line
        # on the console costs far more than reading it from the pipe, and a
        # drain that logged as it read fell behind on a slow terminal, the pipe
        # filled, and the child blocked on stderr -- the transfer's throughput
        # became bound by terminal rendering, the very coupling the drain
        # exists to break (measured: 3,000 files, 9 s logging inline against
        # 1 s without). The reader never waits on the logger.
        self._log_as = log_as
        self._partial = b""
        self._lines: queue.SimpleQueue[bytes | None] | None = None
        self._log_thread: threading.Thread | None = None
        if log_as is not None:
            self._lines = queue.SimpleQueue()
            self._log_thread = threading.Thread(
                target=self._log_worker,
                name=f"stderr-log-{getattr(proc, 'pid', '?')}",
                daemon=True,
            )
            self._log_thread.start()
        self._thread = threading.Thread(
            target=self._drain,
            name=f"stderr-tail-{getattr(proc, 'pid', '?')}",
            daemon=True,
        )
        self._thread.start()

    def _drain(self) -> None:
        pipe = self._pipe
        try:
            while True:
                chunk = (
                    pipe.read1(65536) if hasattr(pipe, "read1") else pipe.read(65536)
                )
                if not isinstance(chunk, bytes) or not chunk:
                    break
                with self._lock:
                    # Trim by BYTES, not by chunk: dropping whole chunks while
                    # over the limit left as little as one chunk behind, so the
                    # kept size depended on where the reads happened to split.
                    self._buf += chunk
                    excess = len(self._buf) - self._keep
                    if excess > 0:
                        del self._buf[:excess]
                if self._log_as is not None:
                    self._log_lines(chunk)
        except (OSError, ValueError):
            # The pipe was closed under us (a kill, or the process object being
            # torn down); whatever was read stands.
            pass
        finally:
            if self._lines is not None:
                if self._partial:
                    self._lines.put(self._partial)
                    self._partial = b""
                self._lines.put(None)
            try:
                pipe.close()
            except (OSError, ValueError):
                pass

    def _log_lines(self, chunk: bytes) -> None:
        assert self._lines is not None
        data = self._partial + chunk
        lines = data.split(b"\n")
        self._partial = lines.pop()
        for line in lines:
            self._lines.put(line)

    def _log_worker(self) -> None:
        assert self._lines is not None
        while True:
            line = self._lines.get()
            if line is None:
                return
            logger.debug("%s: %s", self._log_as, line.decode("utf-8", "replace"))

    def text(self, timeout: float = 5.0) -> str:
        """The retained tail, decoded, after EOF (or after ``timeout`` seconds).

        EOF follows the exit of every holder of the pipe's write end. A child
        that exited has released it; a grandchild that inherited it and lingers
        would delay EOF, so the wait is bounded and the report is whatever has
        arrived, rather than a hang on a process that already finished.
        """
        self.finish(timeout)
        with self._lock:
            data = bytes(self._buf)
        return data.decode("utf-8", errors="replace")

    def finish(self, timeout: float = 5.0) -> None:
        """Wait (bounded) for EOF, then log every line read so far.

        The engine calls this at the end of every transfer, success included.
        Without it the success path never waited on the logger thread, and a
        run under btrfs_debug exited with most of its lines still queued
        (measured: 348 of 24,000 logged). The drain's wait is bounded because
        EOF may never come -- an ssh control master that inherited the pipe
        holds it open -- but the logger is then told to stop after what is
        already queued and is waited for without a bound: that wait is bounded
        by the lines in the queue, and finishing them is the whole point.
        """
        self._thread.join(timeout)
        if self._log_thread is not None and self._lines is not None:
            self._lines.put(None)
            self._log_thread.join()


def tail_stderr(proc: Any, log_as: str | None = None) -> Any:
    """Attach a :class:`StderrTail` to ``proc`` (as ``proc.stderr_tail``) and return it.

    With ``log_as`` (a name such as "btrfs receive"), each line is also logged
    at DEBUG as it arrives; endpoints pass it when ``btrfs_debug`` is on.

    A no-op returning None when the process has no stderr pipe. Endpoints call
    this right after ``Popen(stderr=PIPE)``; the engine reads the result through
    :func:`stderr_text`, so a process started without a pipe (or by a caller
    that did not attach a tail) still reports what it can. After this call
    ``proc.stderr`` is None: the pipe belongs to the tail.
    """
    # Only a real pipe gets a drain. A test double whose ``stderr`` is a mock
    # object answers every read with another truthy mock, and a thread reading
    # it would spin for the rest of the process; the isinstance check is what
    # keeps a mocked endpoint from starting one.
    if proc is None or not isinstance(getattr(proc, "stderr", None), io.IOBase):
        return None
    tail = StderrTail(proc, log_as=log_as)
    proc.stderr_tail = tail
    return tail


def finish_stderr(*procs: Any) -> None:
    """Finish the tails of the given processes: wait for EOF (bounded) and let
    every queued debug line reach the log. Safe on None, on a process without
    a tail, and more than once."""
    for proc in procs:
        tail = getattr(proc, "stderr_tail", None)
        if isinstance(tail, StderrTail):
            tail.finish()


def stderr_text(proc: Any) -> str:
    """Everything a finished process said on stderr, from its tail if it has one.

    Falls back to a direct read for a process that has a pipe but no tail (a
    caller that built its own ``Popen``), closing the pipe afterwards so it is
    not left to garbage collection. Returns "" for a process without stderr.
    """
    if proc is None:
        return ""
    tail = getattr(proc, "stderr_tail", None)
    if isinstance(tail, StderrTail):
        return tail.text()
    pipe = getattr(proc, "stderr", None)
    if not isinstance(pipe, io.IOBase):
        return ""
    try:
        data = pipe.read()
        return data.decode("utf-8", errors="replace") if isinstance(data, bytes) else ""
    except (OSError, ValueError):
        return ""
    finally:
        try:
            pipe.close()
        except (OSError, ValueError):
            pass


def chain_stages(source_stdout: Any, stages: list) -> tuple:
    """Chain ``stages`` onto ``source_stdout``, releasing each pipe as it goes.

    ``stages`` is an ORDERED list of ``(name, factory)``, where ``factory(stdin)``
    returns a Popen or None to skip that stage. The order is the caller's, because
    the paths genuinely differ -- a local transfer compresses and then throttles,
    while an ssh transfer buffers and then compresses, and the buffer size was
    chosen by measurement. What they should NOT differ in is the construction.

    This exists because that construction was written out three times and the
    same defect appeared in all three: the pipe handed to a child was never
    released by this process, so a dying consumer left the stage upstream of it
    blocked forever rather than taking SIGPIPE. Two of the three even had the
    idiom at their earlier handoffs and missed it at the last one. One chainer
    means hand_over() happens once, where it cannot be forgotten.

    Returns ``(final_stdout, [(name, proc), ...])``. The caller still owns the
    final stdout and must hand it over to whatever consumes it.
    """
    processes: list = []
    current = source_stdout
    for name, factory in stages:
        proc = factory(current)
        if proc is None:
            continue
        processes.append((name, proc))
        hand_over(current)
        current = proc.stdout
    return current, processes


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

    def _compress(stdin):
        if not compress or compress == "none":
            return None
        proc = create_compress_process(compress, stdin=stdin)
        if proc:
            logger.info("Transfer compression enabled: %s", compress)
        return proc

    def _throttle(stdin):
        if not rate_limit:
            return None
        proc = create_throttle_process(
            rate_limit, stdin=stdin, show_progress=show_progress
        )
        if proc:
            logger.info("Transfer rate limited to: %s", rate_limit)
        return proc

    def _progress(stdin):
        # Only when NOT throttling: create_throttle_process already displays
        # progress, so both would put two meters on one stream.
        if rate_limit or not show_progress:
            return None
        proc = create_progress_process(stdin=stdin)
        if proc:
            logger.debug("Transfer progress display enabled")
        return proc

    # Compression first, then throttling: the rate limit is meant to bound what
    # goes on the wire, which is the compressed stream. The stage NAMES are the
    # ones the monitors and cleanup already use.
    return chain_stages(
        send_stdout,
        [
            ("compress", _compress),
            ("throttle", _throttle),
            ("progress", _progress),
        ],
    )


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
            hand_over(current_stdout)
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


def wait_for_pipeline(
    processes: list, timeout: int = DEFAULT_TRANSFER_TIMEOUT
) -> list[int]:
    """Wait for all pipeline processes to complete.

    Args:
        processes: List of (name, Popen) tuples
        timeout: Wall limit in seconds. ``0`` (the shipped default, see
            DEFAULT_TRANSFER_TIMEOUT) means NO limit.

    Returns:
        List of return codes
    """
    # Zero is this project's "no limit" sentinel everywhere else -- wait_with_progress
    # spells it `wall_timeout <= 0` and substitutes a fallback. Passed straight to
    # Popen.wait it is a literal zero, i.e. a non-blocking poll that raises
    # TimeoutExpired immediately (measured: 0.000s). The handler below then SIGKILLs
    # the process and records -1, and operations.py scores any non-zero return code
    # as a failed transfer. So on the DEFAULT configuration every compress/throttle/
    # progress stage not already reaped when the send finished was killed with no
    # grace and a transfer whose data had been fully received was reported failed.
    # It survived because the compressor has usually drained by then, which is
    # exactly why it would never show up in a passing test.
    wait_timeout = timeout if timeout and timeout > 0 else None
    return_codes = []
    for name, proc in processes:
        try:
            rc = proc.wait(timeout=wait_timeout)
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


def wait_with_progress(
    process: Any,
    *,
    stall_pids: list[int],
    stall_timeout: int,
    wall_timeout: int = DEFAULT_TRANSFER_TIMEOUT,
    poll_interval: float = 0.5,
    description: str = "transfer",
) -> int:
    """Wait for ``process``, giving up only when it stops making progress.

    Replaces ``process.wait(timeout=N)``, which blocks in the kernel and so
    cannot tell a working transfer from a dead one -- it can only count seconds.
    This polls, so the same loop that waits can also measure, and raises
    ``subprocess.TimeoutExpired`` exactly as ``wait`` would, leaving every
    caller's error handling unchanged.

    Three regimes, in order of preference:

    1. **Bytes are readable.** The transfer is given up on when nothing has moved
       for ``stall_timeout``. Elapsed time is not consulted; a slow transfer runs
       to completion.
    2. **A wall limit was configured.** Enforced as well, because an operator who
       asks for a deadline is expressing policy, not a health check.
    3. **Nothing is measurable** -- no readable /proc/<pid>/io and no configured
       limit. A generous fallback applies so an unmonitorable process cannot hang
       forever, and it is logged, because degrading silently to "no guard" is how
       a backup wedges a scheduler at 3am.
    """
    start = time.monotonic()
    last_bytes = __util__.any_bytes_moved(stall_pids)
    measurable = stall_timeout > 0 and last_bytes is not None
    last_progress = start

    effective_wall = wall_timeout
    if not measurable and wall_timeout <= 0:
        effective_wall = UNMEASURABLE_FALLBACK_TIMEOUT
        logger.warning(
            "Cannot measure byte progress for this %s, so it is guarded by a "
            "%ds limit instead of by whether it is actually moving. A slow "
            "transfer may be ended by it.",
            description,
            effective_wall,
        )

    while True:
        # `wait`, not `poll`, so the return value and its semantics are exactly
        # what a plain process.wait(timeout=N) would have produced -- this is a
        # drop-in for the blocking call it replaces, and callers (and their
        # test doubles) see no difference. The short timeout is what turns the
        # blocking wait into a loop that can also measure.
        try:
            return process.wait(timeout=poll_interval)
        except subprocess.TimeoutExpired:
            pass

        now = time.monotonic()

        if measurable:
            moved = __util__.any_bytes_moved(stall_pids)
            if moved is None:
                measurable = False
                if wall_timeout <= 0:
                    effective_wall = UNMEASURABLE_FALLBACK_TIMEOUT
            elif moved != last_bytes:
                last_bytes = moved
                last_progress = now
            elif now - last_progress >= stall_timeout:
                raise subprocess.TimeoutExpired(
                    cmd=description, timeout=float(stall_timeout)
                )

        if effective_wall > 0 and now - start >= effective_wall:
            raise subprocess.TimeoutExpired(
                cmd=description, timeout=float(effective_wall)
            )
