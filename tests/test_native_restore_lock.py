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
``send`` reads a stream). Removing the guards + coercing the path enable LOCAL btrfs
restore; SSHEndpoint additionally overrides ``set_lock`` (in-memory) and ``send`` (stream
``btrfs send`` FROM the remote over ssh) so REMOTE ssh:// btrfs restore works too -- see
``tests/test_ssh_remote_restore.py`` for the ssh-specific overrides. These tests pin the
shared base fixes.
"""

from __future__ import annotations


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


def test_restore_local_endpoint_uses_backup_prefix(tmp_path):
    """The destination endpoint must parse already-restored snapshots under the SAME
    prefix the backup uses; an empty prefix made it miss prefixed names, so the restore
    chain re-restored an existing parent (and hung over ssh). Mutation guard: hardcoding
    ``snap_prefix=''`` in _prepare_local_endpoint fails this."""
    from btrfs_backup_ng.cli.restore import _prepare_local_endpoint

    ep = _prepare_local_endpoint(
        tmp_path, timestamp_format="%Y%m%d-%H%M%S", snap_prefix="home-"
    )
    assert ep.config["snap_prefix"] == "home-"


def test_incremental_parent_is_the_backup_side_snapshot(tmp_path, monkeypatch):
    """``btrfs send -p`` runs where the backup lives (the REMOTE host for ssh://), so the
    parent handed to the send must be the BACKUP snapshot -- whose path is valid there --
    not a locally-restored copy whose path is meaningless on the remote. The planner
    chooses parents from the SOURCE listing by construction; this drives the facade and
    checks the object identity of what reached the transfer."""
    import time as _time

    import btrfs_backup_ng.core.layout as layout_mod
    import btrfs_backup_ng.core.operations as ops
    import btrfs_backup_ng.core.restore as restore_mod
    from btrfs_backup_ng.endpoint.common import Endpoint
    from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict

    def _snap(name_time, endpoint, path, uuid, received_uuid=""):
        s = __util__.Snapshot(
            path, "s-", endpoint, time_obj=_time.strptime(name_time, "%Y%m%d-%H%M%S")
        )
        s.uuid, s.received_uuid = uuid, received_uuid
        return s

    class _Backup:
        config = {"path": "/remote/backup", "snap_prefix": "s-"}
        _is_remote = True

        def __init__(self, snaps):
            self.snaps = snaps

        def list_snapshots(self, flush_cache=False):
            return list(self.snaps)

        def set_lock(self, *a, **k):
            pass

        def get_id(self):
            return "backup"

        def required_parent_of(self, snapshot):
            return Endpoint.required_parent_of(self, snapshot)  # type: ignore[arg-type]

    backup_ep = _Backup([])
    b0 = _snap("20240101-000000", backup_ep, "/remote/backup", "B0", received_uuid="O0")
    b1 = _snap("20240102-000000", backup_ep, "/remote/backup", "B1", received_uuid="O1")
    backup_ep.snaps = [b0, b1]
    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    local_ep = _local(dest_dir)
    # Already restored locally: the copy of b0, carrying O0 as its received_uuid.
    l0 = _snap("20240101-000000", local_ep, str(dest_dir), "L0", received_uuid="O0")
    (dest_dir / l0.get_name()).mkdir()
    local_ep.list_snapshots = lambda flush_cache=False: [l0]  # type: ignore[method-assign]
    local_ep.receive = lambda *a, **k: None  # type: ignore[method-assign]
    monkeypatch.setattr(layout_mod, "_received_subvolume_shape", lambda p: None)
    monkeypatch.setattr(
        ops, "artifact_verdict", lambda ep, s: StructureVerdict("ok", "verified")
    )

    captured = {}

    def fake_send(snapshot, destination_endpoint, parent=None, options=None, **k):
        captured[snapshot.get_name()] = parent
        (dest_dir / snapshot.get_name()).mkdir(exist_ok=True)

    monkeypatch.setattr(ops, "send_snapshot", fake_send)

    restore_mod.restore_snapshots(backup_ep, local_ep, snapshot_name=b1.get_name())

    parent = captured[b1.get_name()]
    assert parent is b0  # the BACKUP-side object (remote path), not the local copy l0
    assert str(parent.get_path()).startswith("/remote/backup")
