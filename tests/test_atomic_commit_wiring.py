"""Engine wiring for atomic raw commit (0.8.5 PR1).

These lock the contract that the transfer engine actually CALLS commit_receive()
on the raw success path and treats a commit failure as a transfer FAILURE.
Without them, the one-line engine hooks (operations.py send_snapshot and
_do_chunked_transfer) could be deleted with every endpoint-level test still
green, silently breaking all raw backups: the pipeline writes a ``.part`` file,
the engine reports success, discovery ignores ``.part``, and the backup vanishes.

Mutation-verified: deleting the ``commit_receive()`` call, or the commit
try/except, makes these fail.
"""

import io
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import operations as ops


class _SpyDest:
    """Minimal destination endpoint double with a commit_receive spy."""

    def __init__(self, commit=None):
        self.config = {"path": "/backup"}
        self._is_remote = False
        self.commit_receive = commit or MagicMock()

    def receive(self, *args, **kwargs):
        return MagicMock(returncode=0)


def _snapshot():
    snap = MagicMock()
    snap.get_path.return_value = "/src/.snapshots/snap"
    snap.get_name.return_value = "snap"
    snap.endpoint.send.return_value = MagicMock(returncode=0)
    return snap


def _patch_engine(monkeypatch, return_codes):
    monkeypatch.setattr(ops, "_ensure_destination_exists", lambda e: None)
    monkeypatch.setattr(ops, "log_transaction", lambda **k: None)
    monkeypatch.setattr(ops, "_do_process_transfer", lambda *a, **k: return_codes)


def test_send_snapshot_commits_on_success(monkeypatch):
    """A successful raw transfer must call commit_receive() exactly once -- this
    is the wiring that actually publishes the .part file."""
    _patch_engine(monkeypatch, [0, 0])
    dest = _SpyDest()
    ops.send_snapshot(_snapshot(), dest, options={"check_space": False})
    dest.commit_receive.assert_called_once()


def test_send_snapshot_does_not_commit_on_pipeline_failure(monkeypatch):
    """A failed pipeline must NOT commit -- a .part must never be published when
    the transfer did not actually succeed."""
    _patch_engine(monkeypatch, [1])  # nonzero return code -> failure
    dest = _SpyDest()
    with pytest.raises(__util__.SnapshotTransferError):
        ops.send_snapshot(_snapshot(), dest, options={"check_space": False})
    dest.commit_receive.assert_not_called()


def test_send_snapshot_commit_failure_is_a_transfer_failure(monkeypatch):
    """If commit_receive() raises, the transfer must be reported as FAILED (not a
    success with no file on disk)."""
    _patch_engine(monkeypatch, [0])
    dest = _SpyDest(commit=MagicMock(side_effect=RuntimeError("disk full")))
    with pytest.raises(
        __util__.SnapshotTransferError, match="Failed to commit received data"
    ):
        ops.send_snapshot(_snapshot(), dest, options={"check_space": False})


def test_chunked_transfer_commits_on_success(monkeypatch, tmp_path):
    """The chunked path must also publish the .part via commit_receive()."""
    from btrfs_backup_ng.core.chunked_transfer import (
        ChunkedTransferManager,
        TransferConfig,
    )

    monkeypatch.setattr(ops, "log_transaction", lambda **k: None)
    monkeypatch.setattr(ops, "_transfer_chunks_local", lambda **k: None)

    dest = _SpyDest()
    snap = _snapshot()
    snap.endpoint.send.return_value = MagicMock(
        returncode=0, stdout=io.BytesIO(b"x" * 4096)
    )
    manager = ChunkedTransferManager(
        TransferConfig(cache_directory=tmp_path, chunk_size_mb=1)
    )

    ops._do_chunked_transfer(
        snapshot=snap,
        destination_endpoint=dest,
        parent=None,
        clones=None,
        options={},
        chunked_manager=manager,
    )
    dest.commit_receive.assert_called_once()
