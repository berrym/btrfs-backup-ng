"""Per-target mutual-exclusion lock (0.8.5).

Mutating raw operations (backup commit, prune, `raw backfill-metadata`, `raw
encrypt`) hold an exclusive flock on the target dir so they cannot race. The lock
file must never be mistaken for a backup stream.
"""

import argparse
import os
import signal
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

from btrfs_backup_ng.cli import raw_cmd
from btrfs_backup_ng.endpoint import raw as raw_mod
from btrfs_backup_ng.endpoint.raw import LOCK_FILENAME, RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def _args(**kw):
    ns = argparse.Namespace()
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


def test_target_lock_is_mutually_exclusive(tmp_path):
    ep = RawEndpoint(config={"path": str(tmp_path)})
    with ep.target_lock():
        # A second acquisition (a distinct open fd, i.e. a peer operation) must not
        # get the lock -> times out -> "busy".
        with pytest.raises(RuntimeError, match="busy"):
            with ep.target_lock(timeout=0.1):
                pass


def test_target_lock_released_after_context(tmp_path):
    ep = RawEndpoint(config={"path": str(tmp_path)})
    with ep.target_lock():
        pass
    # Released on exit -> immediately re-acquirable.
    with ep.target_lock(timeout=0.1):
        pass


def test_target_lock_excludes_across_processes(tmp_path):
    """The real cross-process guarantee (a same-process two-fd test is only a proxy):
    a forked child holding the lock makes a short-deadline acquire in the parent time
    out (busy), and a generous-deadline acquire SERIALIZE -- block until the child
    releases, then succeed, NOT fail."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:  # child: take the lock, signal the parent, hold ~1s, release
        os.close(r)
        try:
            with ep.target_lock():
                os.write(w, b"held")
                time.sleep(1.0)
        except BaseException:
            pass
        finally:
            os.close(w)
            os._exit(0)
    # parent
    os.close(w)
    # b"" (EOF) would mean the child never acquired -> this assert fails cleanly.
    assert os.read(r, 4) == b"held"
    # Child still holds it: a short deadline reports busy (a bounded fail, no crash).
    with pytest.raises(RuntimeError, match="busy"):
        with ep.target_lock(timeout=0.1):
            pass
    # A generous deadline serializes: block until the child releases (~1s), then get it.
    with ep.target_lock(timeout=5.0):
        pass
    os.close(r)
    os.waitpid(pid, 0)


def _raise_hung(*_a):
    raise TimeoutError("target_lock hung")


def test_fifo_lock_file_does_not_hang(tmp_path):
    """A FIFO planted as the lock file must NOT block target_lock forever (O_NONBLOCK
    -> ENXIO -> bounded RuntimeError). Without the fix the (often root) open blocks
    indefinitely -- a silent permanent DoS on every backup/prune. A SIGALRM converts a
    regression (hang) into a test failure instead of wedging the whole suite."""
    os.mkfifo(tmp_path / LOCK_FILENAME)
    ep = RawEndpoint(config={"path": str(tmp_path)})
    old = signal.signal(signal.SIGALRM, _raise_hung)
    signal.alarm(5)
    try:
        with pytest.raises(RuntimeError, match="FIFO|not a regular file"):
            with ep.target_lock(timeout=1.0):
                pass
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


def test_lock_fstat_failure_maps_to_runtime_error(tmp_path, monkeypatch):
    """If fstat on the freshly-opened lock fd fails (exotic/hostile fs), it must
    degrade to a bounded RuntimeError -- NOT escape as an uncaught OSError (the prune
    path catches only RuntimeError, and the CLI would misreport an OSError on stdout)."""
    ep = RawEndpoint(config={"path": str(tmp_path)})

    def boom(_fd):
        raise OSError(5, "simulated fstat failure")

    monkeypatch.setattr(raw_mod.os, "fstat", boom)
    with pytest.raises(RuntimeError, match="cannot stat"):
        with ep.target_lock(timeout=1.0):
            pass


def test_hostile_lock_messages_are_plain_language(tmp_path):
    """Hostile/mis-created lock files must surface a plain-language reason (not a bare
    [Errno NN] repr), so a regular user understands what to fix."""
    for i, (setup, word) in enumerate(
        [
            (lambda p: p.mkdir(), "directory"),
            (lambda p: p.symlink_to("/etc/hostname"), "symlink"),
        ]
    ):
        d = tmp_path / f"c{i}"
        d.mkdir()
        setup(d / LOCK_FILENAME)
        ep = RawEndpoint(config={"path": str(d)})
        with pytest.raises(RuntimeError, match=word):
            with ep.target_lock(timeout=1.0):
                pass


def test_planted_lock_directory_degrades_to_runtime_error(tmp_path):
    """A hostile or mis-created lock file (here a directory) must surface as a bounded
    RuntimeError, never an uncaught OSError -- otherwise a planted lock in a shared
    target dir would permanently crash every backup and prune (a DoS), which is worse
    than having no lock at all."""
    (tmp_path / LOCK_FILENAME).mkdir()
    ep = RawEndpoint(config={"path": str(tmp_path)})
    with pytest.raises(RuntimeError):
        with ep.target_lock(timeout=0.1):
            pass


def test_planted_lock_symlink_is_refused(tmp_path):
    """A planted lock symlink cannot redirect the (often root) open: O_NOFOLLOW makes
    it fail, mapped to RuntimeError, and the link target is never opened/written."""
    victim = tmp_path / "victim"
    victim.write_text("original")
    (tmp_path / LOCK_FILENAME).symlink_to(victim)
    ep = RawEndpoint(config={"path": str(tmp_path)})
    with pytest.raises(RuntimeError):
        with ep.target_lock(timeout=0.1):
            pass
    assert victim.read_text() == "original"  # never followed


def test_lock_timeout_defaults_from_config(tmp_path):
    """target_lock reads its default timeout from the lock_timeout config key, so a
    slow-storage target can widen the serialize window without a code change."""
    ep = RawEndpoint(config={"path": str(tmp_path), "lock_timeout": 0.1})
    with ep.target_lock():
        started = time.monotonic()
        with pytest.raises(RuntimeError, match="busy"):
            with ep.target_lock():  # no explicit timeout -> config's 0.1s, quick busy
                pass
        assert time.monotonic() - started < 5.0  # used 0.1s, not the 30s hard default


def test_lock_file_is_not_a_snapshot(tmp_path):
    """The .btrfs-backup-ng.lock file contains '.btrfs' but must never be discovered
    as a backup stream (list, verify, backfill)."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    with ep.target_lock():
        pass
    assert (tmp_path / LOCK_FILENAME).exists()
    assert ep.list_snapshots(flush_cache=True) == []
    assert ep.streams_without_sidecar() == []


def test_ssh_target_lock_is_noop(tmp_path):
    """raw+ssh locking is deferred: target_lock is a no-op that never blocks."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    with ep.target_lock():
        with ep.target_lock(timeout=0.1):  # no conflict, no RuntimeError
            pass


def _busy_lock(self, **kw):
    @contextmanager
    def _cm():
        raise RuntimeError("raw target /x is busy (test)")
        yield

    return _cm()


def test_backfill_reports_busy_target(tmp_path, monkeypatch, capsys):
    # Seed a legacy (sidecar-less) stream so there is work to do.
    ep0 = RawEndpoint(config={"path": str(tmp_path)})
    src = tmp_path / "s.src"
    src.write_bytes(b"data")
    with open(src, "rb") as f:
        ep0.receive(f, snapshot_name="root.20240101T120000").communicate()
    ep0.commit_receive()
    for meta in tmp_path.glob("*.meta"):
        meta.unlink()
    src.unlink()

    monkeypatch.setattr(RawEndpoint, "target_lock", _busy_lock)
    rc = raw_cmd.execute_raw(
        _args(
            raw_action="backfill-metadata",
            target=str(tmp_path),
            dry_run=False,
            json=False,
        )
    )
    assert rc == 1
    # The lock error goes to stderr (so a --json run's stdout stays valid JSON).
    assert "busy" in capsys.readouterr().err


def test_encrypt_reports_busy_target(tmp_path, monkeypatch, capsys):
    ep0 = RawEndpoint(config={"path": str(tmp_path), "compress": "gzip"})
    src = tmp_path / "s.src"
    src.write_bytes(b"data")
    with open(src, "rb") as f:
        ep0.receive(f, snapshot_name="root.20240101T120000").communicate()
    ep0.commit_receive()
    src.unlink()

    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "x")
    monkeypatch.setattr(RawEndpoint, "target_lock", _busy_lock)
    rc = raw_cmd.execute_raw(
        _args(
            raw_action="encrypt",
            target=str(tmp_path),
            encrypt="openssl_enc",
            gpg_recipient=None,
            gpg_keyring=None,
            openssl_cipher=None,
            shred=False,
            yes=True,
            dry_run=False,
            json=False,
        )
    )
    assert rc == 1
    # The lock error goes to stderr (so a --json run's stdout stays valid JSON).
    assert "busy" in capsys.readouterr().err


def test_prune_takes_lock_once_for_whole_pass(tmp_path, monkeypatch):
    """delete_old_snapshots holds ONE lock for the entire prune (atomic as a unit --
    a concurrent commit cannot interleave between two deletions, and a busy target
    yields a single skip decision), not one acquisition per snapshot."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    for i in range(3):
        src = tmp_path / f"s{i}.src"
        src.write_bytes(b"x")
        with open(src, "rb") as f:
            ep.receive(f, snapshot_name=f"root.2024010{i + 1}T120000").communicate()
        ep.commit_receive()
        src.unlink()
    assert len(ep.list_snapshots(flush_cache=True)) == 3

    calls = {"n": 0}
    real = RawEndpoint.target_lock

    def counting(self, **kw):
        calls["n"] += 1
        return real(self, **kw)

    monkeypatch.setattr(RawEndpoint, "target_lock", counting)
    ep.delete_old_snapshots(keep=1)  # deletes the 2 oldest
    assert calls["n"] == 1  # one lock for the whole pass, not one per snapshot
    assert len(ep.list_snapshots(flush_cache=True)) == 1


def test_ssh_prune_issues_remote_rm(monkeypatch):
    """delete_old_snapshots must prune a raw+ssh target via a remote rm. The atomic-
    prune refactor routes deletion through _delete_snapshots_locked, so SSHRawEndpoint
    overrides THAT (not delete_snapshots); otherwise remote retention silently deletes
    nothing (it would fall through to the inherited LOCAL unlink of a remote path)."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    snaps = [
        RawSnapshot(
            name=f"root.2024010{i + 1}T120000",
            stream_path=Path(f"/backup/root.2024010{i + 1}T120000.btrfs"),
        )
        for i in range(3)
    ]
    monkeypatch.setattr(ep, "list_snapshots", lambda *a, **k: list(snaps))

    rm_targets: list[str] = []

    def fake_run(cmd, **kwargs):
        joined = " ".join(str(c) for c in cmd)
        if "rm -f" in joined:
            rm_targets.append(joined)

        class _R:
            returncode = 0
            stdout = b""
            stderr = b""

        return _R()

    monkeypatch.setattr(raw_mod.subprocess, "run", fake_run)
    ep.delete_old_snapshots(keep=1)  # prune the 2 oldest remotely
    assert len(rm_targets) == 2
    assert all("root.20240103" not in t for t in rm_targets)  # newest kept


def test_backup_commit_still_works_under_lock(tmp_path):
    """A normal backup commit acquires + releases the lock and publishes the
    stream + sidecar (the lock does not break the hardware-proven commit path)."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    src = tmp_path / "src"
    src.write_bytes(b"stream-bytes" * 100)
    with open(src, "rb") as f:
        ep.receive(f, snapshot_name="root.20240101T120000").communicate()
    ep.commit_receive()
    (snap,) = ep.list_snapshots(flush_cache=True)
    assert snap.name == "root.20240101T120000"
    assert snap.metadata_path.exists()
