"""A child's stderr is drained as it is written, kept as a tail, and closed.

The transfer engine reads a process's stderr after the process has exited.
That works only while the child prints less than the pipe buffer (64 KiB):
``btrfs send``/``receive`` print one line each -- unless ``btrfs_debug`` puts
``-vv`` on them, one line per file operation, at which point an undrained
pipe fills, the child stops, and the stall detector kills a healthy transfer.
``core.transfer.StderrTail`` reads the pipe on a thread from the start, keeps
the last 64 KiB, and closes the pipe at EOF; ``stderr_text`` is how the
engine reads it. These tests drive real processes.
"""

from __future__ import annotations

import subprocess
import time

from btrfs_backup_ng.core import transfer as T


def _popen(script: str, **kw) -> subprocess.Popen:
    return subprocess.Popen(["sh", "-c", script], stderr=subprocess.PIPE, **kw)


class TestTheTailDrains:
    def test_a_child_writing_far_more_than_the_pipe_buffer_finishes(self):
        """1 MiB to stderr, 16x the buffer. With nobody reading, this child
        would block forever; with the tail it exits, and promptly."""
        proc = _popen("head -c 1048576 /dev/zero | tr '\\0' 'x' >&2; exit 0")
        T.tail_stderr(proc)
        t0 = time.monotonic()
        assert proc.wait(timeout=20) == 0
        assert time.monotonic() - t0 < 15
        text = T.stderr_text(proc)
        assert len(text) == T.STDERR_TAIL_BYTES
        assert set(text) == {"x"}

    def test_the_tail_is_the_end_of_the_output_not_the_start(self):
        proc = _popen(
            'i=0; while [ $i -lt 20000 ]; do echo "line $i" >&2; i=$((i+1)); done; exit 1'
        )
        T.tail_stderr(proc)
        assert proc.wait(timeout=20) == 1
        text = T.stderr_text(proc)
        assert text.endswith("line 19999\n")
        assert "line 0\n" not in text
        assert len(text) <= T.STDERR_TAIL_BYTES

    def test_the_tail_is_exactly_the_last_bytes_whatever_the_read_boundaries(self):
        """A 70000-byte burst, a pause, then a 16384-byte burst: two reads of
        very different sizes. Trimming by whole chunks kept only the second
        (16384 bytes, measured in the suite once in three runs); trimming by
        bytes keeps exactly the last 65536, ending with the second burst."""
        proc = _popen(
            "head -c 70000 /dev/zero | tr '\\0' 'a' >&2; sleep 0.3; "
            "head -c 16384 /dev/zero | tr '\\0' 'b' >&2; exit 0"
        )
        T.tail_stderr(proc)
        assert proc.wait(timeout=20) == 0
        text = T.stderr_text(proc)
        assert len(text) == T.STDERR_TAIL_BYTES
        assert text.endswith("b" * 16384)
        assert text.startswith("a")

    def test_a_short_message_is_kept_whole(self):
        proc = _popen("echo 'ERROR: unexpected header' >&2; exit 1")
        T.tail_stderr(proc)
        proc.wait(timeout=10)
        assert T.stderr_text(proc) == "ERROR: unexpected header\n"

    def test_text_may_be_read_more_than_once(self):
        proc = _popen("echo twice >&2")
        T.tail_stderr(proc)
        proc.wait(timeout=10)
        assert T.stderr_text(proc) == T.stderr_text(proc) == "twice\n"


class TestTheTailOwnsThePipe:
    def test_the_process_no_longer_exposes_the_pipe(self):
        """One pipe, one reader: communicate() must not race the drain."""
        proc = _popen("echo x >&2; cat >/dev/null", stdin=subprocess.PIPE)
        pipe = proc.stderr
        T.tail_stderr(proc)
        assert proc.stderr is None
        out, err = proc.communicate(b"")
        assert err is None
        assert T.stderr_text(proc) == "x\n"
        assert pipe.closed

    def test_the_pipe_is_closed_at_eof_without_anyone_asking(self):
        proc = _popen("echo done >&2")
        pipe = proc.stderr
        tail = T.tail_stderr(proc)
        proc.wait(timeout=10)
        tail._thread.join(5)
        assert pipe.closed, "the drain must close the pipe itself; GC is not a plan"

    def test_a_lingering_grandchild_does_not_hang_the_report(self):
        """A grandchild that inherited the pipe delays EOF; the report is
        bounded and returns what arrived, rather than waiting on a process
        that already finished."""
        proc = _popen("echo 'ERROR: parent failed' >&2; sleep 4 & exit 1")
        T.tail_stderr(proc)
        assert proc.wait(timeout=10) == 1
        t0 = time.monotonic()
        text = proc.stderr_tail.text(timeout=1.0)
        assert time.monotonic() - t0 < 3, "the report waited on the grandchild"
        assert "ERROR: parent failed" in text


class TestStderrTextWithoutATail:
    def test_reads_a_plain_pipe_and_closes_it(self):
        proc = _popen("echo plain >&2; exit 3")
        proc.wait(timeout=10)
        pipe = proc.stderr
        assert T.stderr_text(proc) == "plain\n"
        assert pipe.closed

    def test_a_process_without_stderr_reports_nothing(self):
        proc = subprocess.Popen(["true"])
        proc.wait(timeout=10)
        assert T.stderr_text(proc) == ""

    def test_none_reports_nothing(self):
        assert T.stderr_text(None) == ""

    def test_tail_stderr_is_a_no_op_without_a_pipe(self):
        proc = subprocess.Popen(["true"])
        assert T.tail_stderr(proc) is None
        proc.wait(timeout=10)


class TestOnlyARealPipeGetsADrain:
    def test_a_mock_process_never_starts_a_thread(self):
        """A test double answers every read with another truthy mock; a thread
        reading it would spin for the rest of the process."""
        import threading
        from unittest.mock import MagicMock

        before = threading.active_count()
        proc = MagicMock()
        assert T.tail_stderr(proc) is None
        assert threading.active_count() == before
        assert T.stderr_text(proc) == ""
