"""Native btrfs restore must not be blocked by mis-scoped source guards.

Restoring a snapshot swaps endpoints: it sets a lock on, and calls ``send()`` of, the
BACKUP endpoint -- which legitimately has no ``source``. Two mistakes in
``endpoint/common.py`` aborted native btrfs restore before any byte moved:

1. ``@require_source`` was applied to the lock-persistence methods and ``send()``, none
   of which read ``config["source"]`` -- they use ``config["path"]``. The backup
   endpoint has no source, so the guard raised "source hasn't been set".
2. ``_get_lock_file_path`` did ``config["path"] / name``; SSHEndpoint keeps ``path`` as
   a str (LocalEndpoint resolves it to Path), so building the lock path raised
   ``TypeError: unsupported operand type(s) for /: 'str' and 'str'``.

Raw endpoints were always immune (they override ``set_lock`` decorator-free and their
``send`` reads a stream). Removing the guards + coercing the path fully enable LOCAL
btrfs restore. Restore from a REMOTE ssh:// btrfs source is a separate matter: the base
``send``/lock run locally, so it cannot stream a snapshot back from the remote host --
the restore CLI now rejects it up front with a clear message (also pinned here) until a
remote-aware ssh send lands. These tests pin the local-restore fixes and that guard.
"""

from __future__ import annotations

import argparse

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint


def _local(tmp_path, source=None):
    return LocalEndpoint(
        config={
            "path": tmp_path,
            "source": source,
            "snapshot_folder": ".snapshots",
            "snap_prefix": "home-",
        }
    )


def _snap(ep, tmp_path):
    return __util__.Snapshot(tmp_path, "home-", ep)


def test_set_lock_without_source_does_not_raise(tmp_path):
    """The backup endpoint (no source) must be lockable during a restore. Mutation
    guard: re-adding ``@require_source`` to set_lock makes this raise 'source hasn't
    been set'."""
    ep = _local(tmp_path, source=None)
    snap = _snap(ep, tmp_path)
    ep.set_lock(snap, "restore:abc", True)  # must not raise
    assert "restore:abc" in snap.locks
    # A real lock file is persisted at the backup path (not in-memory only, unlike raw).
    assert (tmp_path / ".btrfs-backup-ng.locks").exists()
    ep.set_lock(snap, "restore:abc", False)
    assert "restore:abc" not in snap.locks


def test_send_without_source_reaches_exec(tmp_path, monkeypatch):
    """send() of a backup endpoint (no source) must proceed to run btrfs send, not be
    blocked by a source guard. Mutation guard: re-adding ``@require_source`` to send()
    makes this raise before reaching _exec_command."""
    ep = _local(tmp_path, source=None)
    snap = _snap(ep, tmp_path)
    sentinel = object()
    monkeypatch.setattr(ep, "_exec_command", lambda *a, **k: sentinel)
    assert ep.send(snap) is sentinel  # got past the (removed) guard to the real send


def test_read_write_locks_without_source_round_trip(tmp_path):
    """The lock read/write helpers work off ``path``, not ``source``. Mutation guard:
    re-adding ``@require_source`` to _read_locks/_write_locks raises here."""
    ep = _local(tmp_path, source=None)
    ep._write_locks({"home-20240101": {"locks": ["restore:1"]}})
    assert ep._read_locks() == {"home-20240101": {"locks": ["restore:1"]}}


def test_ssh_endpoint_str_path_lock_file(tmp_path):
    """A real SSHEndpoint keeps ``path`` as a str, which broke ``path / name`` with a
    TypeError. Build one (no connection happens at construction) and confirm the lock
    path resolves. Mutation guard: reverting ``Path(self.config["path"])`` to
    ``self.config["path"]`` raises TypeError ('str' / 'str') here."""
    from pathlib import Path

    ep = SSHEndpoint(hostname="backup-host", config={"path": "/remote/backup"})
    assert isinstance(ep.config["path"], str)  # SSHEndpoint does NOT resolve to Path
    result = ep._get_lock_file_path()
    assert isinstance(result, Path)
    assert result == Path("/remote/backup") / ep.config["lock_file_name"]


def test_snapshot_still_requires_source(tmp_path):
    """Guard against over-removal: creating a NEW snapshot genuinely needs a source, so
    ``snapshot()`` must still reject a source-less endpoint."""
    ep = _local(tmp_path, source=None)
    with pytest.raises(ValueError, match="source hasn't been set"):
        ep.snapshot()


def test_remote_ssh_native_restore_source_fails_fast():
    """Restoring a native btrfs backup from a remote ssh:// source is not supported yet
    (SSHEndpoint runs send/lock locally). The restore path must reject it up front with a
    clear, actionable message -- NOT let it die later with a misleading local-path error.
    Mutation guard: neutering the ``source.startswith('ssh://')`` check drops the raise."""
    from btrfs_backup_ng.cli.restore import _reject_remote_ssh_restore

    with pytest.raises(__util__.AbortError, match="not supported yet"):
        _reject_remote_ssh_restore("ssh://user@backup-host:/backups/data")


def test_reject_does_not_block_raw_ssh_or_raw_or_local():
    """The reject must be scoped to the ``ssh://`` scheme only: raw+ssh:// (which DOES
    support remote restore), raw://, and local paths must NOT be rejected."""
    from btrfs_backup_ng.cli.restore import _reject_remote_ssh_restore

    for source in (
        "raw+ssh://user@backup-host:/backups/data",
        "raw:///mnt/backup",
        "/mnt/backup/data",
    ):
        _reject_remote_ssh_restore(source)  # must not raise


def test_ssh_list_path_is_not_rejected_wiring(monkeypatch):
    """The reject fires on RESTORE but must NOT touch ``restore --list ssh://...`` (a
    read-only op that works). This pins the CALLER WIRING, not just the pure function: a
    future change routing the reject through _execute_list (or a shared helper both
    restore and list call) would silently break ssh:// listing. Mutation guard: adding a
    ``_reject_remote_ssh_restore(source)`` call into _execute_list trips the sentinel."""
    import btrfs_backup_ng.cli.restore as restore_mod

    def _must_not_call(*a, **k):
        raise AssertionError("reject must NOT be invoked on the list path")

    monkeypatch.setattr(restore_mod, "_reject_remote_ssh_restore", _must_not_call)
    monkeypatch.setattr(
        restore_mod, "_prepare_backup_endpoint", lambda *a, **k: object()
    )
    monkeypatch.setattr(restore_mod, "list_remote_snapshots", lambda *a, **k: [])

    args = argparse.Namespace(source="ssh://user@backup-host:/backups/data")
    rc = restore_mod._execute_list(args)
    assert rc == 0  # listed (empty) cleanly -- NOT rejected with exit 1
