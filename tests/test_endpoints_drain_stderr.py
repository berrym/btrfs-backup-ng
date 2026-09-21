"""Every send and receive an endpoint starts has its stderr drained into a tail.

The engine reports what a failed process said through core.transfer's
stderr_text, which reads the tail an endpoint attached when it started the
process. An endpoint that goes back to DEVNULL -- or to a bare PIPE nobody
drains -- passes every engine test, because those use fake endpoints. These
pin the real ones: the base send and receive (local btrfs and, by
inheritance, ssh://), and the raw send and receive pipelines.
"""

from __future__ import annotations

import subprocess

from btrfs_backup_ng.core import transfer as T
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint


def _fake_command(script: str):
    """What _build_*_command returns: (arg, is_path) tuples the base
    _exec_command runs through subprocess."""
    return [("sh", False), ("-c", False), (script, False)]


class TestTheBaseEndpoint:
    def test_receive_attaches_a_tail_and_the_report_reads_it(
        self, tmp_path, monkeypatch
    ):
        ep = LocalEndpoint(config={"path": str(tmp_path), "fs_checks": "skip"})
        monkeypatch.setattr(
            ep,
            "_build_receive_command",
            lambda dest: _fake_command(
                "cat >/dev/null; echo 'ERROR: unexpected header' >&2; exit 1"
            ),
        )
        feed = subprocess.Popen(["echo", "stream"], stdout=subprocess.PIPE)
        proc = ep.receive(feed.stdout, "snap")
        feed.stdout.close()
        assert proc.stderr is None, "the pipe belongs to the tail, not the process"
        assert getattr(proc, "stderr_tail", None) is not None
        assert proc.wait(timeout=10) == 1
        feed.wait(timeout=5)
        assert "unexpected header" in T.stderr_text(proc)

    def test_send_attaches_a_tail(self, tmp_path, monkeypatch):
        ep = LocalEndpoint(config={"path": str(tmp_path), "fs_checks": "skip"})
        monkeypatch.setattr(
            ep,
            "_build_send_command",
            lambda snapshot, parent=None, clones=None: _fake_command(
                "echo 'At subvol x' >&2; echo data; exit 0"
            ),
        )
        proc = ep.send(object())
        assert proc.stderr is None
        assert proc.stdout.read() == b"data\n"
        proc.stdout.close()
        assert proc.wait(timeout=10) == 0
        assert T.stderr_text(proc) == "At subvol x\n"

    def test_a_receive_that_floods_stderr_still_finishes(self, tmp_path, monkeypatch):
        """btrfs_debug puts -vv on the receive: a line per file operation.
        Undrained, 1 MiB of it blocks the child on a 64 KiB pipe."""
        ep = LocalEndpoint(config={"path": str(tmp_path), "fs_checks": "skip"})
        monkeypatch.setattr(
            ep,
            "_build_receive_command",
            lambda dest: _fake_command(
                "cat >/dev/null; head -c 1048576 /dev/zero | tr '\\0' 'v' >&2; exit 0"
            ),
        )
        feed = subprocess.Popen(["echo", "stream"], stdout=subprocess.PIPE)
        proc = ep.receive(feed.stdout, "snap")
        feed.stdout.close()
        assert proc.wait(timeout=20) == 0
        feed.wait(timeout=5)
        assert len(T.stderr_text(proc)) == T.STDERR_TAIL_BYTES


class TestTheRawEndpoint:
    def test_receive_pipeline_attaches_a_tail(self, tmp_path):
        ep = RawEndpoint(config={"path": str(tmp_path), "compress": "gzip"})
        src = tmp_path / "in.src"
        src.write_bytes(b"payload" * 100)
        with open(src, "rb") as f:
            proc = ep.receive(f, snapshot_name="root.20240101T120000")
            assert proc.stderr is None
            assert getattr(proc, "stderr_tail", None) is not None
            proc.wait(timeout=10)
        ep.commit_receive()
        assert T.stderr_text(proc) == ""

    def test_send_pipeline_attaches_a_tail(self, tmp_path):
        ep = RawEndpoint(config={"path": str(tmp_path), "compress": "gzip"})
        src = tmp_path / "in.src"
        src.write_bytes(b"payload" * 100)
        name = "root.20240101T120000"
        with open(src, "rb") as f:
            ep.receive(f, snapshot_name=name).wait(timeout=10)
        ep.commit_receive()
        snap = next(s for s in ep.list_snapshots() if s.get_name() == name)
        proc = ep.send(snap)
        assert proc.stderr is None
        assert getattr(proc, "stderr_tail", None) is not None
        assert proc.stdout.read() == b"payload" * 100
        proc.stdout.close()
        assert proc.wait(timeout=10) == 0
