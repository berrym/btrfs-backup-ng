"""A pipeline stage must not block forever when its consumer dies.

After ``Popen(stdin=previous.stdout)`` the pipe has two readers: the new child,
and this process. Only the child ever reads it, but the kernel keeps the pipe
alive while ANY reader holds it -- so if the child dies, the stage writing into
the pipe blocks forever instead of taking SIGPIPE and ending.

Observed on a real transfer before the fix: ``btrfs-backup-ng run`` asleep in a
poll loop with a single child, ``pv -q -B 32M``, parked in
poll_schedule_timeout on a pipe whose read end the parent still held after the
consumer had gone. /proc showed it exactly:

    pv     fd 1 -> pipe:[95722843]
    parent fd 7 -> pipe:[95722843]

The run never terminated, and it held its configuration's run lock the whole
time, so the next run refused to start with "Another btrfs-backup-ng run is
using this configuration".

endpoint/ssh.py applied the idiom at two of its three handoffs and said why at
one of them; the miss was the LAST handoff in the chain, which is the pattern --
it is easiest to forget a pipe was passed on rather than kept. Three live sites
had it: the ssh receive, the shared pipeline builder, and the local receive.

These tests use real processes and real pipes. Mocking the pipe would test the
mock's close(), and the defect is entirely about what the kernel does with a
second reader.
"""

from __future__ import annotations

import subprocess
import time

import pytest

from btrfs_backup_ng.core.transfer import hand_over


def _blocked_writer():
    """A process that writes more than a pipe buffer, so it blocks unless read."""
    return subprocess.Popen(
        ["sh", "-c", "head -c 10000000 /dev/zero"],
        stdout=subprocess.PIPE,
    )


class TestTheHelperItself:
    def test_closing_lets_the_writer_die_when_the_consumer_goes(self):
        writer = _blocked_writer()
        assert writer.stdout is not None
        # A consumer inherits the pipe, then dies immediately.
        consumer = subprocess.Popen(["true"], stdin=writer.stdout)
        consumer.wait()

        hand_over(writer.stdout)

        # With our copy released, the writer takes SIGPIPE and ends.
        try:
            writer.wait(timeout=10)
        except subprocess.TimeoutExpired:
            writer.kill()
            writer.wait()
            pytest.fail(
                "the writer blocked forever after its consumer died, which is "
                "the deadlock this releases"
            )

    def test_without_it_the_writer_blocks(self):
        """The defect, demonstrated. This is what the three live sites did."""
        writer = _blocked_writer()
        assert writer.stdout is not None
        consumer = subprocess.Popen(["true"], stdin=writer.stdout)
        consumer.wait()

        # Deliberately NOT handing over: this process keeps a reader alive.
        try:
            writer.wait(timeout=3)
            blocked = False
        except subprocess.TimeoutExpired:
            blocked = True
        finally:
            writer.kill()
            writer.wait()
            writer.stdout.close()

        assert blocked, (
            "the writer exited even though this process still held the read "
            "end -- if this ever passes, the premise behind hand_over is gone "
            "and the call sites can be revisited"
        )

    def test_it_tolerates_none_and_an_already_closed_pipe(self):
        hand_over(None)
        writer = _blocked_writer()
        assert writer.stdout is not None
        writer.stdout.close()
        hand_over(writer.stdout)  # must not raise
        writer.kill()
        writer.wait()

    def test_it_never_raises_on_a_hostile_object(self):
        """A started transfer must not fail because a pipe could not be closed."""

        class _Hostile:
            def close(self):
                raise OSError("nope")

        hand_over(_Hostile())


class TestEveryHandoffReleasesItsPipe:
    """Checked at the source: driving three real subprocess chains per site
    would be slow and would not say WHICH handoff regressed."""

    def test_the_shared_builder_releases_before_reassigning(self):
        import ast
        import inspect

        from btrfs_backup_ng.core import transfer as t

        tree = ast.parse(inspect.getsource(t))
        for fn in ("build_transfer_pipeline", "build_receive_pipeline"):
            node = next(
                n
                for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef) and n.name == fn
            )
            reassigns = [
                n.lineno
                for n in ast.walk(node)
                if isinstance(n, ast.Assign)
                and any(
                    isinstance(t_, ast.Name) and t_.id == "current_stdout"
                    for t_ in n.targets
                )
                and isinstance(n.value, ast.Attribute)
                and n.value.attr == "stdout"
            ]
            releases = [
                n.lineno
                for n in ast.walk(node)
                if isinstance(n, ast.Call)
                and isinstance(n.func, ast.Name)
                and n.func.id == "hand_over"
            ]
            assert len(releases) >= len(reassigns), (
                f"{fn}: {len(reassigns)} handoff(s) but only {len(releases)} "
                "release(s) -- a stage will block when its consumer dies"
            )

    def test_the_ssh_receive_releases_the_pipe_it_was_given(self):
        import inspect

        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        source = inspect.getsource(SSHEndpoint._btrfs_receive)
        assert "stdin_pipe.close()" in source, (
            "the ssh receive keeps a copy of the pipe handed to it, so the last "
            "local stage cannot see SIGPIPE when the receive exits"
        )

    def test_the_local_receive_releases_the_pipe_it_was_given(self):
        import inspect

        from btrfs_backup_ng.core import operations as ops

        source = inspect.getsource(ops)
        assert "transfer_utils.hand_over(current_stdout)" in source, (
            "the local transfer keeps a copy of the pipe handed to the receive"
        )


def test_a_full_three_stage_chain_unwinds_when_the_consumer_dies():
    """End to end with real processes, in the shape the transfer builds:
    producer | middle | consumer, consumer dies first."""
    producer = _blocked_writer()
    assert producer.stdout is not None
    middle = subprocess.Popen(["cat"], stdin=producer.stdout, stdout=subprocess.PIPE)
    hand_over(producer.stdout)
    assert middle.stdout is not None
    consumer = subprocess.Popen(["true"], stdin=middle.stdout)
    hand_over(middle.stdout)
    consumer.wait()

    deadline = time.monotonic() + 15
    for proc in (middle, producer):
        remaining = max(0.1, deadline - time.monotonic())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            for p in (middle, producer):
                p.kill()
                p.wait()
            pytest.fail("a stage in the chain did not unwind after the consumer died")


class TestNoHandoffCanBeWrittenWithoutReleasingThePipe:
    """The structural guard, which is the only version of this that survives.

    "Remember to close the pipe" is an instruction; three separate places were
    told it and two of them forgot at the last handoff. This makes the suite fail
    instead.

    Every ``Popen(stdin=<something>.stdout)`` in the tree must be accompanied by
    a release of that same pipe in the same function -- either hand_over(), or
    chain_stages() doing it on the caller's behalf.
    """

    @staticmethod
    def _source_files():
        import pathlib

        root = pathlib.Path(__file__).resolve().parent.parent / "src"
        return sorted(root.rglob("*.py"))

    def test_every_popen_handoff_is_released(self):
        import ast

        offenders = []
        for path in self._source_files():
            tree = ast.parse(path.read_text())
            for fn in ast.walk(tree):
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                handoffs = [
                    node.lineno
                    for node in ast.walk(fn)
                    if isinstance(node, ast.Call)
                    and any(
                        kw.arg == "stdin"
                        and isinstance(kw.value, ast.Attribute)
                        and kw.value.attr == "stdout"
                        for kw in node.keywords
                    )
                ]
                if not handoffs:
                    continue
                releases = [
                    node
                    for node in ast.walk(fn)
                    if isinstance(node, ast.Call)
                    and (
                        (
                            isinstance(node.func, ast.Name)
                            and node.func.id in ("hand_over", "chain_stages")
                        )
                        or (
                            isinstance(node.func, ast.Attribute)
                            and node.func.attr in ("hand_over", "chain_stages", "close")
                        )
                    )
                ]
                if not releases:
                    offenders.append(f"{path.name}::{fn.name} at line(s) {handoffs}")

        assert not offenders, (
            "a pipe is handed to a child and never released, so the stage "
            "upstream of it will block forever when that child dies: "
            + "; ".join(offenders)
        )

    def test_the_chainer_releases_each_pipe_as_it_hands_it_on(self):
        """Two loops building pipelines is how the constructions drifted. There
        is one, and this is the line that makes it safe."""
        import inspect

        from btrfs_backup_ng.core import transfer as t

        source = inspect.getsource(t.chain_stages)
        assert "hand_over(current)" in source, (
            "the chainer no longer releases each pipe as it hands it on"
        )

    def test_both_pipelines_declare_their_own_stage_order(self):
        """They genuinely differ -- local compresses then throttles, ssh buffers
        then compresses, and the buffer size was chosen by measurement. Sharing
        the CONSTRUCTION must not have flattened them into one order."""
        import inspect

        from btrfs_backup_ng.core import transfer as t
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        local = inspect.getsource(t.build_transfer_pipeline)
        assert local.index('("compress"') < local.index('("throttle"')

        ssh_src = inspect.getsource(SSHEndpoint._try_direct_transfer)
        assert '("buffer", _buffer)' in ssh_src
        assert ssh_src.index('("buffer"') < ssh_src.index('("compress"')
