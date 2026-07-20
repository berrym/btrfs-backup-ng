"""Auto-written authoritative .meta sidecar (0.8.5 PR3).

Every raw backup writes a v2 sidecar in commit_receive (after the stream is
durable). The v2 schema is additive over v1 (adds pipeline.openssl_cipher, a
reserved checksum block, provenance); from_dict reads either version. Written to
FAIL if the sidecar auto-write is removed or the schema regresses.
"""

import json
from pathlib import Path

from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot, discover_raw_snapshots


def _receive_and_commit(ep, tmp_path, name="snap1", parent_name=None):
    src = tmp_path / "src.bin"
    src.write_bytes(b"payload-bytes-1234567890")
    with open(src, "rb") as stdin:
        proc = ep.receive(stdin, snapshot_name=name, parent_name=parent_name)
        proc.communicate()
    assert proc.returncode == 0
    ep.commit_receive()


def test_commit_writes_authoritative_v2_sidecar(tmp_path):
    ep = RawEndpoint(config={"path": str(tmp_path), "compress": "gzip"})
    _receive_and_commit(ep, tmp_path, name="snap1", parent_name="snap0")

    stream = tmp_path / "snap1.btrfs.gz"
    meta = tmp_path / "snap1.btrfs.gz.meta"
    assert stream.exists()
    assert meta.exists()

    data = json.loads(meta.read_text())
    assert data["version"] == 2
    assert data["name"] == "snap1"
    assert data["parent_name"] == "snap0"
    assert data["size"] == stream.stat().st_size
    assert data["pipeline"]["compress"] == "gzip"
    assert data["pipeline"]["encrypt"] is None
    assert data["pipeline"]["openssl_cipher"] is None  # gated: only for openssl_enc
    assert data["checksum"] == {"algorithm": "sha256", "value": None}  # reserved
    assert data["provenance"]["origin"] == "native-write"
    assert data["created"].endswith("+00:00")  # ISO-8601 UTC


def test_sidecar_written_atomically_at_0600(tmp_path):
    ep = RawEndpoint(config={"path": str(tmp_path)})
    _receive_and_commit(ep, tmp_path, name="snap1")
    meta = tmp_path / "snap1.btrfs.meta"
    assert meta.exists()
    assert (meta.stat().st_mode & 0o777) == 0o600  # not world-readable
    assert not (tmp_path / "snap1.btrfs.meta.tmp").exists()  # temp cleaned


def test_committed_sidecar_is_discovered_with_authoritative_metadata(tmp_path):
    """After commit, a fresh endpoint lists the snapshot from the SIDECAR (not
    filename inference), so restore/prune get authoritative metadata."""
    ep = RawEndpoint(config={"path": str(tmp_path), "compress": "gzip"})
    _receive_and_commit(ep, tmp_path, name="snap1", parent_name="snap0")

    lister = RawEndpoint(config={"path": str(tmp_path)})
    snaps = lister.list_snapshots()
    assert len(snaps) == 1
    s = snaps[0]
    assert s.name == "snap1"
    assert s.compress == "gzip"
    assert s.parent_name == "snap0"  # only the sidecar carries this; filenames don't
    assert s.provenance_origin == "native-write"


def test_sidecar_records_openssl_cipher(tmp_path):
    """openssl_cipher must be recorded (restore reads it in a later PR instead of
    guessing aes-256-cbc)."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    ep._pending_metadata = {
        "name": "s",
        "stream_path": tmp_path / "s.btrfs.enc",
        "part_path": tmp_path / "s.btrfs.enc.part",
        "parent_name": None,
        "compress": None,
        "encrypt": "openssl_enc",
        "gpg_recipient": None,
        "openssl_cipher": "aes-128-cbc",
    }
    snap = ep._sidecar_snapshot(tmp_path / "s.btrfs.enc", 42)
    assert snap.encrypt == "openssl_enc"
    assert snap.openssl_cipher == "aes-128-cbc"
    assert snap.to_dict()["pipeline"]["openssl_cipher"] == "aes-128-cbc"


def test_receive_gates_openssl_cipher_to_openssl_only(tmp_path):
    """A plaintext (or gpg) target must not record an openssl cipher."""
    ep = RawEndpoint(config={"path": str(tmp_path)})  # encrypt=None
    src = tmp_path / "src.bin"
    src.write_bytes(b"x")
    with open(src, "rb") as stdin:
        proc = ep.receive(stdin, snapshot_name="s")
        proc.communicate()
    assert ep._pending_metadata["openssl_cipher"] is None


def test_sidecar_write_failure_does_not_fail_the_backup(tmp_path):
    """PR3 headline invariant: the stream is durable BEFORE the sidecar, so a
    sidecar-write error must NOT flip an already-successful backup into a failure
    (it degrades to filename inference). commit_receive must not raise, and the
    committed stream must exist."""
    from unittest.mock import patch

    ep = RawEndpoint(config={"path": str(tmp_path)})
    src = tmp_path / "src.bin"
    src.write_bytes(b"payload")
    with open(src, "rb") as stdin:
        proc = ep.receive(stdin, snapshot_name="snap1")
        proc.communicate()
    with patch.object(RawSnapshot, "save_metadata", side_effect=OSError("disk full")):
        ep.commit_receive()  # must NOT raise
    # The backup itself succeeded (stream published); only the sidecar is missing.
    assert (tmp_path / "snap1.btrfs").exists()
    assert not (tmp_path / "snap1.btrfs.meta").exists()


def test_snapper_raw_meta_and_snapper_meta_coexist_without_double_count(tmp_path):
    """A snapper-raw backup has BOTH a .meta (stream) and a .snapper-meta.json
    (snapper context). Discovery must list exactly one snapshot -- the .json is
    neither a .meta sidecar nor a .btrfs stream."""
    (tmp_path / "root-5-20240115.btrfs").write_bytes(b"stream")
    RawSnapshot(
        name="root-5-20240115",
        stream_path=tmp_path / "root-5-20240115.btrfs",
    ).save_metadata()
    (tmp_path / "root-5-20240115.snapper-meta.json").write_text('{"snapper": 1}')

    snaps = discover_raw_snapshots(tmp_path)
    assert [s.name for s in snaps] == ["root-5-20240115"]


def test_from_dict_tolerates_explicit_null_blocks(tmp_path):
    """A sidecar with explicit JSON null for pipeline/checksum/provenance must not
    crash from_dict (nulls are coalesced), so one odd file can't abort a listing."""
    data = {
        "version": 2,
        "name": "s",
        "created": "2024-01-15T12:00:00+00:00",
        "pipeline": None,
        "checksum": None,
        "provenance": None,
    }
    snap = RawSnapshot.from_dict(data, Path("/b/s.btrfs"))
    assert snap.name == "s"
    assert snap.compress is None
    assert snap.checksum_value is None
    assert snap.provenance_origin == "native-write"


def test_leftover_meta_tmp_is_not_discovered(tmp_path):
    """A crash mid-sidecar-write can leave <name>.meta.tmp; discovery must ignore
    it (it contains '.btrfs' and would otherwise be listed as a phantom backup)."""
    (tmp_path / "good.btrfs").write_bytes(b"stream")
    (tmp_path / "good.btrfs.meta.tmp").write_bytes(b"{partial json")
    names = [s.name for s in discover_raw_snapshots(tmp_path)]
    assert names == ["good"]


def test_v1_sidecar_loads_with_defaults(tmp_path):
    """A v1 sidecar (no openssl_cipher/checksum/provenance) loads via from_dict
    without error, defaulting the new fields."""
    v1 = {
        "version": 1,
        "name": "root.20240115T120000",
        "uuid": "u",
        "parent_uuid": None,
        "parent_name": None,
        "created": "2024-01-15T12:00:00+00:00",
        "size": 10,
        "pipeline": {"compress": "zstd", "encrypt": "gpg", "gpg_recipient": "a@b"},
        "btrfs_backup_ng_version": "0.8.3",
    }
    snap = RawSnapshot.from_dict(v1, Path("/b/root.20240115T120000.btrfs.zst.gpg"))
    assert snap.compress == "zstd"
    assert snap.encrypt == "gpg"
    assert snap.openssl_cipher is None
    assert snap.checksum_value is None
    assert snap.provenance_origin == "native-write"


def test_v2_round_trip_preserves_new_fields(tmp_path):
    snap = RawSnapshot(
        name="s",
        stream_path=Path("/b/s.btrfs.enc"),
        encrypt="openssl_enc",
        openssl_cipher="aes-256-gcm",
        checksum_value="deadbeef",
        provenance_origin="backfill",
    )
    restored = RawSnapshot.from_dict(snap.to_dict(), snap.stream_path)
    assert restored.encrypt == "openssl_enc"
    assert restored.openssl_cipher == "aes-256-gcm"
    assert restored.checksum_value == "deadbeef"
    assert restored.provenance_origin == "backfill"
