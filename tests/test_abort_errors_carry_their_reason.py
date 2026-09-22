"""An AbortError always says why; the report that quotes it is never empty.

Four sites logged a reason and then raised a bare ``AbortError``, so the
summary that quotes the exception -- "Transfer to X failed: " -- ended at the
colon. The first real second-hop transfer from an ssh:// mirror produced
exactly that: the reason (a lock directory where a lock file was expected)
was on a log line above, and the verdict line carried nothing. Every
AbortError now carries its reason, and one of them names what it found.
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


class TestTheLockStoreCollision:
    """An ssh:// target keeps its locks in a DIRECTORY of the same name a local
    endpoint uses for its lock FILE. Meeting the directory locally is refused
    -- proceeding with an empty lock set would let a local prune delete what
    a remote restore holds -- and the refusal says which store it is."""

    def test_a_lock_directory_is_refused_with_its_name(self, tmp_path):
        (tmp_path / ".btrfs-backup-ng.locks").mkdir()
        ep = LocalEndpoint(config={"path": str(tmp_path), "fs_checks": "skip"})
        with pytest.raises(__util__.AbortError) as e:
            ep._read_locks()
        text = str(e.value)
        assert "ssh://" in text and "directory" in text, text
        assert str(tmp_path / ".btrfs-backup-ng.locks") in text
        assert "not supported yet" in text

    def test_the_reason_travels_in_the_exception_not_only_the_log(self, tmp_path):
        """What a caller reports with str(e) is the full reason."""
        (tmp_path / ".btrfs-backup-ng.locks").mkdir()
        ep = LocalEndpoint(config={"path": str(tmp_path), "fs_checks": "skip"})
        try:
            ep._read_locks()
        except __util__.AbortError as e:
            assert str(e).strip() != ""
            assert "Cannot read the lock file" in str(e)
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
