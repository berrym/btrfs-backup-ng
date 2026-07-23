"""Enforcement tests for R2 — no cleanup / poisoned re-runs.

A failed/killed transfer must not leave a partial artifact that the next run's
skip-detection mistakes for a completed backup. Cleanup is scoped to the EXACT
artifact path and runs only on the failure path, so a good backup is never
deleted (the R1 false-negative guard, extended to the local/standard path).
"""

from __future__ import annotations

import io
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.core.operations as ops
from btrfs_backup_ng import __util__


def _fake_snap(name: str) -> MagicMock:
    s = MagicMock()
    s.locks = {}
    s.parent_locks = {}
    s.get_name.return_value = name
    return s


class TestCleanupPartialLocalSubvolume:
    def _local_endpoint(self, path):
        ep = MagicMock()
        ep._is_remote = False
        ep.config = {"path": str(path)}
        return ep

    def test_deletes_exact_local_path_when_present(self, monkeypatch, tmp_path):
        (tmp_path / "snap-1").mkdir()  # the partial artifact this failed run left
        ep = self._local_endpoint(tmp_path)

        calls: list[list[str]] = []

        def fake_run(cmd, **kwargs):
            calls.append(list(cmd))
            return SimpleNamespace(returncode=0)

        monkeypatch.setattr(ops.subprocess, "run", fake_run)
        ops._cleanup_partial_local_subvolume(ep, "snap-1")

        deletes = [c for c in calls if c[-3:-1] == ["subvolume", "delete"]]
        assert deletes, "expected a btrfs subvolume delete of the partial"
        assert deletes[0][-1] == str(tmp_path / "snap-1")

    def test_noop_when_nothing_present(self, monkeypatch, tmp_path):
        ep = self._local_endpoint(tmp_path)
        called: list[int] = []
        monkeypatch.setattr(ops.subprocess, "run", lambda *a, **k: called.append(1))
        ops._cleanup_partial_local_subvolume(ep, "absent")
        assert not called

    def test_skips_remote_endpoint(self, monkeypatch, tmp_path):
        (tmp_path / "snap-1").mkdir()
        ep = MagicMock()
        ep._is_remote = True  # SSH cleans its own partials during the transfer
        ep.config = {"path": str(tmp_path)}
        called: list[int] = []
        monkeypatch.setattr(ops.subprocess, "run", lambda *a, **k: called.append(1))
        ops._cleanup_partial_local_subvolume(ep, "snap-1")
        assert not called

    def test_bad_config_does_not_escape(self):
        # Best-effort contract: a malformed endpoint config must never raise out
        # of cleanup and mask the original transfer error.
        ep = MagicMock()
        ep._is_remote = False
        ep.config = {}  # missing "path"
        ops._cleanup_partial_local_subvolume(ep, "snap-1")  # must not raise

    def test_skips_raw_endpoint(self, monkeypatch, tmp_path):
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        (tmp_path / "snap-1").mkdir()
        ep = RawEndpoint.__new__(RawEndpoint)
        ep._is_remote = False  # local raw: handled by the raw cleanup path, not here
        ep.config = {"path": str(tmp_path)}
        called: list[int] = []
        monkeypatch.setattr(ops.subprocess, "run", lambda *a, **k: called.append(1))
        ops._cleanup_partial_local_subvolume(ep, "snap-1")
        assert not called


class TestExecuteTransfersCleansPartialOnFailure:
    def test_failure_triggers_local_cleanup(self, monkeypatch):
        snap = _fake_snap("s1")
        src = MagicMock()
        dst = MagicMock()
        dst.get_id.return_value = "d"
        dst._is_remote = False

        monkeypatch.setattr(
            ops,
            "send_snapshot",
            MagicMock(side_effect=__util__.SnapshotTransferError("boom")),
        )
        spy = MagicMock()
        monkeypatch.setattr(ops, "_cleanup_partial_local_subvolume", spy)

        ops._execute_transfers(src, dst, [(snap, None)], {})

        spy.assert_called_once()
        # cleanup targets the exact received name of the snapshot that failed
        assert spy.call_args[0][1] == "s1"

    def test_success_does_not_trigger_cleanup(self, monkeypatch):
        snap = _fake_snap("s1")
        src = MagicMock()
        dst = MagicMock()
        dst.get_id.return_value = "d"
        dst._is_remote = False

        monkeypatch.setattr(ops, "send_snapshot", MagicMock(return_value=None))
        spy = MagicMock()
        monkeypatch.setattr(ops, "_cleanup_partial_local_subvolume", spy)

        ops._execute_transfers(src, dst, [(snap, None)], {})
        spy.assert_not_called()


class TestChunkedPartialCleanup:
    def test_local_chunked_failure_cleans_exact_partial(self, monkeypatch):
        manifest = SimpleNamespace(
            snapshot_path="/src/snapshot",
            snapshot_name="snap",
            parent_name=None,
            chunk_count=1,
            chunks=[SimpleNamespace(sequence=0)],
        )
        dst = MagicMock()
        dst._is_remote = False
        recv = MagicMock()
        recv.wait.return_value = 1  # btrfs receive fails
        recv.stderr = io.BytesIO(b"boom")
        dst.receive.return_value = recv

        mgr = MagicMock()
        reader = MagicMock()
        reader.pipe_to_process.return_value = 100
        mgr.create_reassembly_reader.return_value = reader

        spy = MagicMock()
        monkeypatch.setattr(ops, "_cleanup_partial_local_subvolume", spy)

        with pytest.raises(__util__.SnapshotTransferError):
            ops._transfer_chunks_local(manifest, dst, mgr, {})

        spy.assert_called_once()
        # cleanup uses the source basename, NOT manifest.snapshot_name
        assert spy.call_args[0][1] == "snapshot"

    def test_remote_cleanup_calls_endpoint_cleaner(self):
        manifest = SimpleNamespace(snapshot_path="/src/snapshot")
        ep = MagicMock()
        ep.config = {"path": "/remote/dest"}
        ops._cleanup_partial_remote_subvolume(ep, manifest)
        ep._cleanup_partial_subvolume.assert_called_once_with(
            "/remote/dest", "snapshot"
        )

    def test_remote_cleanup_noop_without_cleaner(self):
        manifest = SimpleNamespace(snapshot_path="/src/snapshot")
        ep = MagicMock(spec=["config"])  # no _cleanup_partial_subvolume
        ep.config = {"path": "/remote/dest"}
        # must not raise
        ops._cleanup_partial_remote_subvolume(ep, manifest)


class TestRawPartialCleanup:
    """A failed raw transfer leaves an uncommitted ``.part`` file; cleanup must
    remove exactly that ``part_path`` (never the final name, which could be a
    prior good backup, and never a name pattern)."""

    def test_local_raw_deletes_exact_part_file(self, tmp_path):
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        final = tmp_path / "host-20260719.btrfs"
        part = tmp_path / "host-20260719.btrfs.part"
        part.write_bytes(b"partial stream")
        ep = RawEndpoint.__new__(RawEndpoint)
        ep._pending_metadata = {"stream_path": final, "part_path": part}
        ops._cleanup_partial_raw_stream(ep)
        assert not part.exists()

    def test_local_raw_preserves_committed_final(self, tmp_path):
        """False-negative guard: cleanup must NEVER delete a committed backup
        that happens to sit at the final name."""
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        final = tmp_path / "host-20260719.btrfs"
        final.write_bytes(b"good backup")
        part = tmp_path / "host-20260719.btrfs.part"
        ep = RawEndpoint.__new__(RawEndpoint)
        ep._pending_metadata = {"stream_path": final, "part_path": part}
        ops._cleanup_partial_raw_stream(ep)
        assert final.exists()
        assert final.read_bytes() == b"good backup"

    def test_ssh_raw_deletes_via_remote_command(self):
        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

        ep = SSHRawEndpoint.__new__(SSHRawEndpoint)
        ep._pending_metadata = {
            "stream_path": "/remote/host-20260719.btrfs",
            "part_path": "/remote/host-20260719.btrfs.part",
        }
        ep._exec_remote_command = MagicMock()
        ops._cleanup_partial_raw_stream(ep)
        ep._exec_remote_command.assert_called_once_with(
            ["rm", "-f", "/remote/host-20260719.btrfs.part"], check=False
        )

    def test_non_raw_endpoint_is_noop(self):
        ep = MagicMock()  # not a RawEndpoint
        ops._cleanup_partial_raw_stream(ep)  # must not raise

    def test_no_pending_metadata_is_noop(self):
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        ep = RawEndpoint.__new__(RawEndpoint)  # no _pending_metadata set
        ops._cleanup_partial_raw_stream(ep)  # must not raise

    def test_funnel_cleans_raw_stream_on_failure(self, monkeypatch):
        snap = _fake_snap("s1")
        src = MagicMock()
        dst = MagicMock()
        dst.get_id.return_value = "d"
        monkeypatch.setattr(
            ops,
            "send_snapshot",
            MagicMock(side_effect=__util__.SnapshotTransferError("boom")),
        )
        monkeypatch.setattr(ops, "_cleanup_partial_local_subvolume", MagicMock())
        spy = MagicMock()
        monkeypatch.setattr(ops, "_cleanup_partial_raw_stream", spy)
        ops._execute_transfers(src, dst, [(snap, None)], {})
        spy.assert_called_once_with(dst)
