"""Restore-side integrity (T1).

Restore must not feed bad data into ``btrfs receive``: it verifies the stored
stream against the sha256 sealed at backup time BEFORE decoding (so a corrupted
backup is refused, not decoded into a corrupt subvolume), and the multi-stage
restore pipeline runs under ``pipefail`` so a mid-pipe decrypt/decompress failure
cannot be masked by the last stage exiting 0.
"""

from __future__ import annotations

import hashlib
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.__util__ import AbortError
from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def _sealed(tmp_path, *, corrupt=False, checksum=True):
    data = b"btrfs-stream\x00" + b"payload " * 2000
    stream = tmp_path / "root.20240101T120000.btrfs"
    stream.write_bytes(data)
    kw = {"checksum_value": hashlib.sha256(data).hexdigest()} if checksum else {}
    RawSnapshot(
        name="root.20240101T120000", stream_path=stream, size=len(data), **kw
    ).save_metadata()
    if corrupt:
        b = bytearray(stream.read_bytes())
        b[5000] ^= 0xFF
        stream.write_bytes(bytes(b))
    ep = RawEndpoint(config={"path": str(tmp_path)})
    return ep, ep.list_snapshots(flush_cache=True)[0]


def test_corrupt_stored_stream_is_refused_before_restore(tmp_path):
    ep, snap = _sealed(tmp_path, corrupt=True)
    with pytest.raises(AbortError, match="CORRUPT"):
        ep.send(snap)


def test_intact_stream_passes_verification_and_restores(tmp_path):
    ep, snap = _sealed(tmp_path, corrupt=False)
    proc = ep.send(snap)  # verify passes -> restore proceeds
    proc.stdout.read()
    proc.wait()
    assert proc.returncode == 0


def test_legacy_stream_without_checksum_skips_verify(tmp_path):
    """A legacy backup with no recorded checksum has nothing to compare against, so
    verification is skipped (it must not block the restore)."""
    ep, snap = _sealed(tmp_path, checksum=False)
    assert snap.checksum_value is None
    proc = ep.send(snap)  # must not raise
    proc.stdout.read()
    proc.wait()


def test_verify_does_not_block_when_checksum_unreadable(tmp_path, monkeypatch):
    """If the stream cannot be hashed, verification must not BLOCK the restore (the
    decode step surfaces a genuine read error); it warns and proceeds."""
    ep, snap = _sealed(tmp_path, corrupt=False)
    monkeypatch.setattr(ep, "compute_stream_checksum", lambda _s: None)
    proc = ep.send(snap)  # must not raise despite unreadable checksum
    proc.stdout.read()
    proc.wait()


def test_skip_verify_allows_corrupt_restore_for_last_copy_recovery(tmp_path):
    """--skip-verify (verify_before_restore=False) must let a corrupt stream through
    (last-copy recovery), warning instead of hard-refusing."""
    ep, snap = _sealed(tmp_path, corrupt=True)
    ep.config["verify_before_restore"] = False  # as _prepare_backup_endpoint sets it
    proc = ep.send(snap)  # must NOT raise despite the corruption
    proc.stdout.read()
    proc.wait()


def test_restore_pipeline_pipefail_surfaces_midpipe_failure(tmp_path):
    """A real multi-stage restore where an EARLY stage fails but the LAST exits 0 must
    still report non-zero -- proving pipefail actually surfaces a masked mid-pipe
    failure (without it, `false | cat` returns the last stage's 0)."""
    ep, snap = _sealed(tmp_path, corrupt=False)
    proc = ep._execute_restore_pipeline([["false"], ["cat"]], snap.stream_path)
    proc.communicate()
    assert proc.returncode != 0


def test_multistage_restore_pipeline_uses_pipefail(tmp_path, monkeypatch):
    """A multi-stage restore pipeline (decrypt|decompress) must run under pipefail so
    a mid-pipe failure is not masked by the last stage's exit 0."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "x")
    stream = tmp_path / "root.20240101T120000.btrfs.gz.enc"
    stream.write_bytes(b"data")
    RawSnapshot(
        name="root.20240101T120000",
        stream_path=stream,
        compress="gzip",
        encrypt="openssl_enc",
        openssl_cipher="aes-256-cbc",
        size=4,
    ).save_metadata()
    ep = RawEndpoint(config={"path": str(tmp_path)})
    (snap,) = ep.list_snapshots(flush_cache=True)
    with patch("btrfs_backup_ng.endpoint.raw._popen_pipeline_pipefail") as pp:
        pp.return_value = MagicMock()
        ep.send(snap)
    assert pp.called  # the multi-stage restore went through the pipefail helper
