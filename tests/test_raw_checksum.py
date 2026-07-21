"""Write-time ciphertext checksum (0.8.5 PR6b).

Every raw backup records the sha256 of its committed stream in the ``.meta``
sidecar (``checksum.value``), computed by reading the file back AFTER the atomic
commit -- so it reflects the bytes that actually landed on disk (Option 1:
post-commit read-back; see the design notes). raw verify (PR6c) recomputes and
compares. The seal is best-effort: a checksum failure never fails a durable backup.
"""

import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.endpoint import raw as raw_mod
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint, _sha256_file
from btrfs_backup_ng.endpoint.raw_metadata import discover_raw_snapshots


def _commit_backup(path, name="root.20260101T120000", payload=b"stream-" * 500):
    ep = RawEndpoint(config={"path": str(path)})
    src = path / "src.bin"
    src.write_bytes(payload)
    with open(src, "rb") as stdin:
        ep.receive(stdin, snapshot_name=name).communicate()
    ep.commit_receive()
    src.unlink()
    return ep


# --------------------------------------------------------------------------- #
# _sha256_file helper
# --------------------------------------------------------------------------- #
def test_sha256_file_matches_hashlib(tmp_path):
    f = tmp_path / "blob"
    data = b"arbitrary-bytes-" * 1000
    f.write_bytes(data)
    assert _sha256_file(f) == hashlib.sha256(data).hexdigest()


def test_sha256_file_missing_returns_none(tmp_path):
    """Best-effort: a missing/unreadable file yields None, not an exception."""
    assert _sha256_file(tmp_path / "nope") is None


def test_sha256_file_without_posix_fadvise(tmp_path, monkeypatch):
    """The POSIX_FADV_DONTNEED call is getattr-guarded for non-Linux; removing
    posix_fadvise entirely must not break hashing (pins the guard)."""
    monkeypatch.delattr(os, "posix_fadvise", raising=False)
    f = tmp_path / "blob"
    data = b"no-fadvise-" * 300
    f.write_bytes(data)
    assert _sha256_file(f) == hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------- #
# local write-time seal
# --------------------------------------------------------------------------- #
def test_commit_records_checksum_of_committed_stream(tmp_path):
    """The sidecar checksum equals the sha256 of the on-disk stream file. If the
    commit stops passing the computed checksum into the sidecar, value is null and
    this fails."""
    _commit_backup(tmp_path)
    (snap,) = discover_raw_snapshots(tmp_path)
    assert snap.checksum_value is not None
    on_disk = hashlib.sha256(snap.stream_path.read_bytes()).hexdigest()
    assert snap.checksum_value == on_disk


def test_checksum_seal_is_best_effort(tmp_path, monkeypatch):
    """If the checksum cannot be computed, the backup still commits durably with a
    null checksum (never fail a durable backup on a checksum error). Reverting the
    best-effort wrapping would surface the failure instead."""
    monkeypatch.setattr(raw_mod, "_sha256_file", lambda p: None)
    _commit_backup(tmp_path)
    (snap,) = discover_raw_snapshots(tmp_path)
    # Stream is durable and discoverable; only the checksum is absent.
    assert snap.stream_path.exists()
    assert snap.checksum_value is None


# --------------------------------------------------------------------------- #
# remote (raw+ssh) seal
# --------------------------------------------------------------------------- #
def test_remote_sha256_parses_valid_digest():
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    digest = "a" * 64
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=0, stdout=(digest + "\n").encode())
    )
    assert ep._remote_sha256(Path("/backup/x.btrfs")) == digest
    # Uses a portable hash command.
    script = ep._exec_remote_command.call_args[0][0][2]
    assert "sha256sum" in script and "shasum -a 256" in script and "openssl" in script


def test_remote_sha256_rejects_non_hex_output():
    """A garbage/error line must not be recorded as a checksum."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=0, stdout=b"stat: cannot stat\n")
    )
    assert ep._remote_sha256(Path("/backup/x.btrfs")) is None


def test_remote_sha256_empty_output_returns_none():
    """rc==0 with EMPTY stdout (the exact symptom of the old broken fallback on a
    host without the first hash tool) must yield None, not an empty checksum."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=0, stdout=b"")
    )
    assert ep._remote_sha256(Path("/backup/x.btrfs")) is None


def test_remote_hash_script_falls_through_when_sha256sum_absent(tmp_path):
    """Run the ACTUAL remote-hash shell string through a real /bin/sh with a PATH
    that lacks sha256sum/shasum (simulating macOS's missing sha256sum), leaving only
    openssl+awk. The tool must be selected by existence so the fallback fires and a
    correct digest is emitted -- NOT mocked, so it catches the pipeline-exit-status
    bug the mocked tests miss. Reverting to `tool | awk || ...` makes this fail."""
    openssl = shutil.which("openssl")
    awk = shutil.which("awk")
    if not openssl or not awk:
        pytest.skip("openssl/awk not available")

    blob = tmp_path / "stream.btrfs"
    data = b"remote-fallthrough-" * 500
    blob.write_bytes(data)

    # Capture the real shell string _remote_sha256 builds (SSH itself is stubbed).
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    captured = {}

    def capture(argv, **kw):
        captured["script"] = argv[2]
        return MagicMock(returncode=0, stdout=b"")

    ep._exec_remote_command = capture
    ep._remote_sha256(blob)
    script = captured["script"]

    # A PATH with ONLY openssl + awk: sha256sum and shasum are absent (like macOS,
    # which actually ships shasum -- here we prove the deepest fallback, openssl).
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "openssl").symlink_to(openssl)
    (bindir / "awk").symlink_to(awk)
    res = subprocess.run(
        ["/bin/sh", "-c", script],
        env={"PATH": str(bindir)},
        capture_output=True,
        text=True,
    )
    digest = res.stdout.strip().lower()
    assert digest == hashlib.sha256(data).hexdigest()


def test_remote_sidecar_records_checksum():
    """_write_remote_sidecar seals the remote checksum into the sidecar it writes.
    Mocks all three remote calls (size stat, checksum, sidecar write) and inspects
    the JSON fed to the remote write."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    digest = "b" * 64
    ep._exec_remote_command = MagicMock(
        side_effect=[
            MagicMock(returncode=0, stdout=b"123", stderr=b""),  # size stat
            MagicMock(returncode=0, stdout=(digest + "\n").encode()),  # checksum
            MagicMock(returncode=0, stdout=b"", stderr=b""),  # sidecar write
        ]
    )
    ep._pending_metadata = {
        "name": "root.20260101T120000",
        "stream_path": Path("/backup/root.20260101T120000.btrfs"),
        "part_path": Path("/backup/root.20260101T120000.btrfs.part"),
        "parent_name": None,
        "compress": None,
        "encrypt": None,
        "gpg_recipient": None,
        "openssl_cipher": None,
    }
    ep._write_remote_sidecar(Path("/backup/root.20260101T120000.btrfs"))
    payload = ep._exec_remote_command.call_args_list[2].kwargs["input"]
    doc = json.loads(payload)
    assert doc["checksum"]["value"] == digest


def test_remote_checksum_exception_stays_best_effort():
    """An unexpected exception from the remote hash must NOT fail an already-durable
    backup: _write_remote_sidecar wraps the checksum + sidecar write in one
    best-effort try/except. With the remote hash outside the try this propagates."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=0, stdout=b"123", stderr=b"")
    )
    ep._remote_sha256 = MagicMock(side_effect=RuntimeError("boom"))
    ep._pending_metadata = {
        "name": "snap",
        "stream_path": Path("/backup/snap.btrfs"),
        "part_path": Path("/backup/snap.btrfs.part"),
        "parent_name": None,
        "compress": None,
        "encrypt": None,
        "gpg_recipient": None,
        "openssl_cipher": None,
    }
    # Must not raise.
    ep._write_remote_sidecar(Path("/backup/snap.btrfs"))
