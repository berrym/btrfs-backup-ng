"""R7 -- atomic state & manifest persistence.

Proves the shared ``__util__.atomic_write_bytes`` primitive is crash-atomic and that the
three operational-state writers migrated to it -- operation state
(``OperationRecord.save``), the chunked-transfer manifest (``TransferManifest.save``), and
the restore-unlock lock write (``restore._execute_unlock``) -- can never leave a torn file
on a mid-write crash.

The R3 lock suite (``test_r3_lock_persistence.py``) already mutation-guards the primitive's
stale-temp cleanup, no-residue, and O_NOFOLLOW-symlink behavior *through* the lock path
(``_write_locks`` now delegates to the primitive); this file exercises the primitive
directly plus the new callers.

Keystone mutation guard: patch ``os.replace`` to raise mid-save. A caller that still did a
plain ``open(path, "w") + json.dump`` would truncate the previous good file and leave a
torn one, so the reload would return the wrong record (or raise). The atomic writer leaves
the OLD complete file intact and, for best-effort callers, swallows the error.
"""

import os
import types

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.cli import restore as restore_cli
from btrfs_backup_ng.core.chunked_transfer import TransferManifest
from btrfs_backup_ng.core.state import (
    OperationManager,
    OperationRecord,
    OperationState,
)


def _raise_oserror(*_a, **_k):
    raise OSError("simulated crash before the rename completes")


# --------------------------------------------------------------------------- primitive


def test_atomic_write_bytes_writes_and_reads_back(tmp_path):
    p = tmp_path / "f.json"
    __util__.atomic_write_bytes(p, b'{"a": 1}')
    assert p.read_bytes() == b'{"a": 1}'


def test_atomic_write_bytes_accepts_str(tmp_path):
    p = tmp_path / "f.txt"
    __util__.atomic_write_bytes(p, "héllo")
    assert p.read_text(encoding="utf-8") == "héllo"


def test_atomic_write_bytes_mode_0600(tmp_path):
    p = tmp_path / "secret"
    __util__.atomic_write_bytes(p, b"x", mode=0o600)
    assert (p.stat().st_mode & 0o777) == 0o600


def test_atomic_write_bytes_leaves_no_temp(tmp_path):
    p = tmp_path / "f"
    __util__.atomic_write_bytes(p, b"x")
    assert not (tmp_path / "f.tmp").exists()
    assert list(tmp_path.iterdir()) == [p]


def test_atomic_write_bytes_replaces_stale_temp(tmp_path):
    """A leftover ``.tmp`` from a prior crash is reclaimed, not left to accumulate."""
    p = tmp_path / "f"
    stale = tmp_path / "f.tmp"
    stale.write_text("garbage from a prior crash")
    __util__.atomic_write_bytes(p, b"fresh")
    assert p.read_bytes() == b"fresh"
    assert not stale.exists()


def test_atomic_write_bytes_overwrites_existing(tmp_path):
    p = tmp_path / "f"
    p.write_bytes(b"old")
    __util__.atomic_write_bytes(p, b"new")
    assert p.read_bytes() == b"new"


def test_atomic_write_bytes_symlinked_temp_does_not_clobber_victim(tmp_path):
    """A symlink planted at the temp path must not redirect the write to its target.

    Mutation guard: a plain ``open(tmp, "wb")`` with no unlink / no O_NOFOLLOW would
    follow the symlink and overwrite the victim -> this fails."""
    victim = tmp_path / "victim"
    victim.write_text("precious")
    p = tmp_path / "f"
    (tmp_path / "f.tmp").symlink_to(victim)
    __util__.atomic_write_bytes(p, b"payload")
    assert victim.read_text() == "precious"  # untouched
    assert p.read_bytes() == b"payload"


def test_atomic_write_bytes_fsyncs_file_and_parent_dir(tmp_path, monkeypatch):
    """The content AND the rename must be made durable (file fsync + parent-dir fsync).

    Mutation guard: dropping either fsync reduces the count below 2."""
    synced = []
    real_fsync = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: (synced.append(fd), real_fsync(fd))[1])
    __util__.atomic_write_bytes(tmp_path / "f", b"x")
    assert len(synced) >= 2  # file content + parent directory


def test_atomic_write_bytes_no_fsync_when_disabled(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(os, "fsync", lambda fd: calls.append(fd))
    __util__.atomic_write_bytes(tmp_path / "f", b"x", fsync=False)
    assert calls == []
    assert (tmp_path / "f").read_bytes() == b"x"


def test_atomic_write_bytes_failure_preserves_old_and_cleans_temp(
    tmp_path, monkeypatch
):
    """os.replace failing mid-write leaves the OLD file intact and removes the temp."""
    p = tmp_path / "f"
    p.write_bytes(b"old")
    monkeypatch.setattr(os, "replace", _raise_oserror)
    with pytest.raises(OSError):
        __util__.atomic_write_bytes(p, b"new")
    assert p.read_bytes() == b"old"  # untouched
    assert not (tmp_path / "f.tmp").exists()  # temp cleaned up


# ------------------------------------------------------- G1: operation state (state.py)


def _make_record(op_id="op1"):
    return OperationRecord(
        operation_id=op_id,
        state=OperationState.TRANSFERRING,
        source_volume="/data",
    )


def test_operation_record_save_atomic_roundtrip(tmp_path):
    p = tmp_path / "op.json"
    _make_record().save(p)
    loaded = OperationRecord.load(p)
    assert loaded.operation_id == "op1"
    assert loaded.state == OperationState.TRANSFERRING
    assert not (tmp_path / "op.json.tmp").exists()


def test_operation_record_save_torn_write_preserves_old(tmp_path, monkeypatch):
    """KEYSTONE: a crash mid-save must never leave a torn state file.

    Mutation guard: revert ``save`` to ``open("w") + json.dump`` and the interrupted
    write truncates the previous record -> the reload returns v2 (clobbered) or raises,
    either of which fails the ``== "v1"`` assertion below."""
    p = tmp_path / "op.json"
    _make_record("v1").save(p)
    assert OperationRecord.load(p).operation_id == "v1"

    monkeypatch.setattr(os, "replace", _raise_oserror)
    _make_record("v2").save(p)  # best-effort: swallows the OSError, must NOT raise

    assert OperationRecord.load(p).operation_id == "v1"  # old intact, not torn


def test_operation_record_save_best_effort_does_not_raise(tmp_path, monkeypatch):
    """A state-save I/O failure must never propagate to fail a good operation."""
    monkeypatch.setattr(os, "replace", _raise_oserror)
    _make_record().save(tmp_path / "op.json")  # no exception == pass


# --------------------------------------------------- G2: transfer manifest (chunked_transfer.py)


def _make_manifest(tid="t1"):
    return TransferManifest(
        transfer_id=tid,
        snapshot_name="snap",
        snapshot_path="/snap",
        parent_name=None,
        parent_path=None,
        destination="ssh://h/p",
        total_size=None,
        chunk_size=1024,
        checksum_algorithm="sha256",
    )


def test_manifest_save_atomic_roundtrip(tmp_path):
    p = tmp_path / "manifest.json"
    _make_manifest().save(p)
    loaded = TransferManifest.load(p)
    assert loaded.transfer_id == "t1"
    assert not (tmp_path / "manifest.json.tmp").exists()


def test_manifest_save_torn_write_preserves_old(tmp_path, monkeypatch):
    """A crash mid-save must not leave a torn manifest that forces a full retransmit."""
    p = tmp_path / "manifest.json"
    _make_manifest("t1").save(p)

    monkeypatch.setattr(os, "replace", _raise_oserror)
    _make_manifest("t2").save(p)  # best-effort swallow

    assert TransferManifest.load(p).transfer_id == "t1"  # old intact, not torn


# ------------------------------------------------- G3: restore-unlock lock write (restore.py)


class _FakeEndpoint(Endpoint):
    """A real Endpoint, so the atomic write under test is the endpoint's own
    _write_locks -- which is what the CLI now calls."""

    def __init__(self, path):
        self.config = {"path": path, "lock_file_name": ".btrfs-backup-ng.locks"}


def _setup_unlock(tmp_path, monkeypatch, lock_content):
    lock_path = tmp_path / ".btrfs-backup-ng.locks"
    lock_path.write_text(lock_content)
    monkeypatch.setattr(
        restore_cli,
        "_prepare_backup_endpoint",
        lambda args, source: _FakeEndpoint(tmp_path),
    )
    args = types.SimpleNamespace(source=str(tmp_path))
    return lock_path, args


def test_unlock_writes_lock_file_atomically(tmp_path, monkeypatch):
    content = __util__.write_locks({"snap": {"locks": ["restore:abc"]}})
    lock_path, args = _setup_unlock(tmp_path, monkeypatch, content)

    rc = restore_cli._execute_unlock(args, "all")

    assert rc == 0
    assert __util__.read_locks(lock_path.read_text()) == {}  # the restore lock is gone
    assert not (tmp_path / ".btrfs-backup-ng.locks.tmp").exists()


def test_unlock_torn_write_preserves_lock_file(tmp_path, monkeypatch):
    """Mutation guard for G3: revert to plain ``open("w")`` and a crash before the rename
    truncates the lock file -> retention then misreads it as 'no locks' and can prune a
    still-locked snapshot. The atomic writer leaves the original lock file intact."""
    content = __util__.write_locks({"snap": {"locks": ["restore:abc"]}})
    lock_path, args = _setup_unlock(tmp_path, monkeypatch, content)

    monkeypatch.setattr(os, "replace", _raise_oserror)
    rc = restore_cli._execute_unlock(args, "all")

    assert rc == 1  # write failed -> non-zero exit
    assert __util__.read_locks(lock_path.read_text()) == {
        "snap": {"locks": ["restore:abc"]}
    }


# ---------------------------------- review fix: doctor _fix_stale_lock atomic (was a bypass)


def test_doctor_fix_stale_lock_is_atomic(tmp_path):
    """The doctor's --fix stale-lock action must use the atomic lock writer (it was a
    plain write_text bypass, the same class as the G3 restore-unlock bypass)."""
    from btrfs_backup_ng.core.doctor import Doctor

    lock_file = tmp_path / ".btrfs-backup-ng.locks"
    lock_file.write_text(__util__.write_locks({"snap": {"locks": ["stale:1"]}}))

    ok = Doctor()._fix_stale_lock(lock_file, "snap", "stale:1")

    assert ok is True
    assert __util__.read_locks(lock_file.read_text()) == {}  # stale lock removed
    assert not (tmp_path / ".btrfs-backup-ng.locks.tmp").exists()


def test_doctor_fix_stale_lock_torn_write_preserves_lock_file(tmp_path, monkeypatch):
    """Mutation guard: revert to plain ``write_text`` and a crash before the rename
    truncates the lock file -> retention misreads it as 'no locks'. The atomic writer
    leaves the original lock file intact."""
    from btrfs_backup_ng.core.doctor import Doctor

    lock_file = tmp_path / ".btrfs-backup-ng.locks"
    lock_file.write_text(__util__.write_locks({"snap": {"locks": ["stale:1"]}}))

    monkeypatch.setattr(os, "replace", _raise_oserror)
    ok = Doctor()._fix_stale_lock(lock_file, "snap", "stale:1")

    assert ok is False  # write failed -> fix reports failure
    assert __util__.read_locks(lock_file.read_text()) == {
        "snap": {"locks": ["stale:1"]}
    }


# ---------------------------------- review fix: archive_operation never drops a record


def test_archive_operation_keeps_source_when_archive_write_fails(tmp_path, monkeypatch):
    """Because save() is now best-effort (swallows OSError), archive_operation must NOT
    unlink the source record unless the archive copy is proven written and loadable --
    otherwise a failed archive write silently loses a completed operation's record.

    Mutation guard: revert to an unconditional ``src_path.unlink()`` and this fails (the
    source is gone and the archive was never written)."""
    mgr = OperationManager(state_dir=tmp_path)
    op = mgr.create_operation("/data", ["ssh://h/p"])
    op.state = OperationState.SUCCESS
    src = mgr._get_operation_path(op.operation_id)
    op.save(src)  # persist the completed record (os.replace not yet patched)
    assert src.exists()

    monkeypatch.setattr(os, "replace", _raise_oserror)
    archived = mgr.archive_operation(op.operation_id)

    assert archived is False  # archive write failed -> reported failure
    assert src.exists()  # SOURCE RECORD PRESERVED (never destroyed on a failed archive)
    assert not mgr._get_archive_path(op.operation_id).exists()


def test_archive_operation_keeps_source_when_archive_unreadable(tmp_path, monkeypatch):
    """Even if the archive copy is written but comes back UNREADABLE (torn/corrupt JSON),
    the source record is kept -- the load-verification except-branch. Mutation guard:
    drop the loadability check (existence only) and a corrupt archive would pass, unlinking
    the source."""
    mgr = OperationManager(state_dir=tmp_path)
    op = mgr.create_operation("/data", ["ssh://h/p"])
    op.state = OperationState.SUCCESS
    src = mgr._get_operation_path(op.operation_id)
    op.save(src)

    # The archive copy is written fine, but loading it back raises (simulated corruption).
    # The source-record load (operations/, not archive/) must still succeed.
    archive_path = str(mgr._get_archive_path(op.operation_id))
    real_load = OperationRecord.load

    def flaky_load(path):
        if str(path) == archive_path:
            raise ValueError("simulated corrupt archive record")
        return real_load(path)

    monkeypatch.setattr(OperationRecord, "load", staticmethod(flaky_load))

    archived = mgr.archive_operation(op.operation_id)

    assert archived is False  # unreadable archive -> reported failure
    assert src.exists()  # SOURCE RECORD PRESERVED
