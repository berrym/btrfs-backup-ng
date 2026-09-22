"""An AbortError always says why; the report that quotes it is never empty.

Four sites logged a reason and then raised a bare ``AbortError``, so the
summary that quotes the exception -- "Transfer to X failed: " -- ended at the
colon. The first real second-hop transfer from an ssh:// mirror produced
exactly that: the reason (a lock directory where a lock file was expected)
was on a log line above, and the verdict line carried nothing. Every
AbortError now carries its reason.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.local import LocalEndpoint

SRC = Path(__file__).resolve().parent.parent / "src" / "btrfs_backup_ng"


def test_no_bare_abort_error_anywhere():
    bare = re.compile(r"raise (?:__util__\.)?AbortError(?:\(\))?\s*$", re.M)
    offenders = [
        f"{p.relative_to(SRC)}:{bare.search(p.read_text(encoding='utf-8')).start()}"
        for p in sorted(SRC.rglob("*.py"))
        if bare.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == [], offenders


class TestTheLockStoreIsShared:
    """An ssh:// target keeps its locks in a DIRECTORY of the same name a local
    endpoint uses for its lock FILE. Meeting the directory locally used to be
    refused, with the store named; now the local endpoint reads and writes
    that store, so a remote restore's pin is honoured by a local prune. The
    full protocol is under tests/test_local_lock_directory_store.py; this
    pins the two facts the refusal used to stand in for."""

    def test_a_local_endpoint_honours_a_remote_restores_pin(self, tmp_path):
        import subprocess

        from btrfs_backup_ng.sshutil.lock import RemoteLockManager

        (tmp_path / "home-20260101-120000").mkdir()

        def run(script):
            proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
            return proc.returncode, proc.stdout, proc.stderr

        RemoteLockManager(run, str(tmp_path), hostname="remote").acquire_shared(
            "snap-home-20260101-120000", "restore:20260101-abc"
        )
        assert (tmp_path / ".btrfs-backup-ng.locks").is_dir()

        ep = LocalEndpoint(
            config={"path": str(tmp_path), "snap_prefix": "home-", "fs_checks": "skip"}
        )
        assert ep._read_locks() == {
            "home-20260101-120000": {"locks": ["restore:20260101-abc"]}
        }
        (snapshot,) = ep.list_snapshots()
        assert snapshot.locks == {"restore:20260101-abc"}
        result = ep.delete_snapshots([snapshot])
        assert result.deleted == [] and result.skipped_count == 1

    def test_a_store_that_cannot_be_read_says_so_in_the_exception(
        self, tmp_path, monkeypatch
    ):
        """What a caller reports with str(e) is the full reason."""
        (tmp_path / ".btrfs-backup-ng.locks").mkdir()
        ep = LocalEndpoint(config={"path": str(tmp_path), "fs_checks": "skip"})

        def cannot(manager):
            raise RuntimeError("sh is unavailable")

        monkeypatch.setattr("btrfs_backup_ng.sshutil.lock.read_persisted_locks", cannot)
        try:
            ep._read_locks()
        except __util__.AbortError as e:
            assert "Cannot read the lock store" in str(e)
            assert str(tmp_path / ".btrfs-backup-ng.locks") in str(e)
            assert "sh is unavailable" in str(e)
        else:
            pytest.fail("no refusal")

    def test_an_unreadable_lock_file_names_the_os_error(self, tmp_path):
        import os

        if os.geteuid() == 0:
            pytest.skip("root reads a mode-0 file; the refusal cannot be provoked")
        lock = tmp_path / ".btrfs-backup-ng.locks"
        lock.write_text("{}")
        lock.chmod(0)
        ep = LocalEndpoint(config={"path": str(tmp_path), "fs_checks": "skip"})
        try:
            with pytest.raises(__util__.AbortError) as e:
                ep._read_locks()
            assert "Permission denied" in str(e.value), str(e.value)
        finally:
            lock.chmod(0o600)
