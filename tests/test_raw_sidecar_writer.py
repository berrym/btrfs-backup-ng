"""Shared sidecar writer (0.8.5 PR5).

Every ``.meta`` sidecar -- written by the transfer engine on commit, or by a raw
maintenance command (0.8.5 PR6) with any provenance -- goes through one path:
``RawSnapshot.serialize()`` for the wire bytes and ``endpoint.write_sidecar()`` for
the atomic write (local direct, raw+ssh on the remote). These tests pin that
contract so the local and remote writers cannot drift and the engine keeps routing
through the shared entry point.

Each test is written to FAIL if the shared-writer wiring is reverted.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def _snapshot(stream_path):
    return RawSnapshot(
        name="snap",
        stream_path=Path(stream_path),
        size=123,
        compress="gzip",
        encrypt="openssl_enc",
        openssl_cipher="aes-256-cbc",
        provenance_origin="native-write",
    )


def test_serialize_is_exactly_the_on_disk_bytes(tmp_path):
    """The bytes written to the local .meta are exactly ``serialize()`` -- so the
    one wire-format definition is authoritative. If save_metadata stops using
    serialize(), the two diverge and this fails."""
    snap = _snapshot(tmp_path / "snap.btrfs")
    snap.save_metadata()
    on_disk = snap.metadata_path.read_bytes()
    assert on_disk == snap.serialize()


def test_local_write_sidecar_is_atomic_and_0600(tmp_path):
    """RawEndpoint.write_sidecar publishes a 0600 .meta and leaves no .tmp."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    snap = _snapshot(tmp_path / "snap.btrfs")
    ep.write_sidecar(snap)

    meta = snap.metadata_path
    assert meta.exists()
    assert (meta.stat().st_mode & 0o777) == 0o600
    assert not meta.with_name(meta.name + ".tmp").exists()
    assert meta.read_bytes() == snap.serialize()


def test_local_and_remote_sidecar_bytes_are_identical(tmp_path):
    """The raw+ssh writer must feed the remote exactly the bytes the local writer
    puts on disk for the same snapshot -- the anti-drift invariant. If the remote
    writer builds its own payload (e.g. reverts to an inline json.dumps), the two
    can diverge and this fails."""
    # ONE snapshot (its `created` stamp is fixed) written both ways; the sidecar
    # body is path-independent, so only a format divergence could differ.
    snap = _snapshot(tmp_path / "snap.btrfs")
    snap.save_metadata()
    local_bytes = snap.metadata_path.read_bytes()

    # Remote bytes: capture what write_sidecar feeds the remote shell.
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=0, stderr=b"", stdout=b"")
    )
    ep.write_sidecar(snap)
    fed = ep._exec_remote_command.call_args.kwargs["input"]
    assert fed == snap.serialize()
    # Same document regardless of where it lands.
    assert fed == local_bytes


def test_ssh_write_sidecar_builds_atomic_script_at_meta_path():
    """The remote write is atomic (temp -> sync -> mv -> chmod 600) and targets
    exactly ``<stream>.meta``."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=0, stderr=b"", stdout=b"")
    )
    snap = _snapshot("/backup/snap.btrfs")
    ep.write_sidecar(snap)

    script = ep._exec_remote_command.call_args[0][0][2]
    assert str(snap.metadata_path) == "/backup/snap.btrfs.meta"
    assert "/backup/snap.btrfs.meta.tmp" in script
    assert "sync &&" in script
    assert "mv -f" in script
    assert "chmod 600" in script


def test_ssh_write_sidecar_raises_on_remote_failure():
    """A nonzero remote return must raise so a maintenance command can tell the
    write failed. (The engine wraps this to stay best-effort -- see the next
    test.)"""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=1, stderr=b"No space left on device")
    )
    with pytest.raises(RuntimeError, match="Failed to write remote sidecar"):
        ep.write_sidecar(_snapshot("/backup/snap.btrfs"))


def test_ssh_commit_sidecar_failure_stays_best_effort():
    """A remote sidecar write failure must NOT fail an already-durable backup:
    _write_remote_sidecar catches write_sidecar's raise and only warns. If the
    best-effort wrapper is dropped, the RuntimeError propagates and this fails."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    # size stat succeeds; the sidecar write returns nonzero (would raise unwrapped).
    ep._exec_remote_command = MagicMock(
        side_effect=[
            MagicMock(returncode=0, stdout=b"123", stderr=b""),  # size stat
            MagicMock(returncode=1, stderr=b"disk full"),  # sidecar write
        ]
    )
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


@pytest.mark.parametrize(
    "stream",
    [
        "snap.btrfs",
        "snap",
        "root.20240115T120000.btrfs.zst.gpg",
        "weird name.btrfs",
        "a.b.c.d.e",
    ],
)
def test_remote_meta_path_equals_stream_plus_meta(stream):
    """The raw+ssh writer targets ``str(snapshot.metadata_path)``; it must equal the
    historical ``f"{stream}.meta"`` for every filename shape so the remote sidecar
    filename never moves. metadata_path uses ``with_suffix(suffix + ".meta")``,
    which nets to a pure append -- this pins that."""
    snap = _snapshot(stream)
    assert str(snap.metadata_path) == f"{stream}.meta"


def test_custom_provenance_round_trips_through_write_sidecar(tmp_path):
    """The PR5 -> PR6 enabler: a maintenance command writes a sidecar with a
    non-native provenance (e.g. 'remediation') by handing write_sidecar a snapshot
    that carries it. Prove the origin survives to the on-disk bytes AND the bytes
    the ssh writer feeds the remote. If anything hardcoded 'native-write' on the
    write path, PR6 would silently break -- this fails."""
    import json

    snap = _snapshot(tmp_path / "snap.btrfs")
    snap.provenance_origin = "remediation"

    # Local write records the custom provenance.
    RawEndpoint(config={"path": str(tmp_path)}).write_sidecar(snap)
    on_disk = json.loads(snap.metadata_path.read_bytes())
    assert on_disk["provenance"]["origin"] == "remediation"

    # The ssh writer feeds the remote the same provenance.
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=0, stderr=b"", stdout=b"")
    )
    ep.write_sidecar(snap)
    fed = json.loads(ep._exec_remote_command.call_args.kwargs["input"])
    assert fed["provenance"]["origin"] == "remediation"


def test_local_commit_routes_through_write_sidecar(tmp_path):
    """The engine commit path must publish the sidecar via write_sidecar (the
    shared entry point), passing the native-write snapshot. If commit calls
    save_metadata directly again, write_sidecar is not invoked and this fails."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    src = tmp_path / "src.bin"
    src.write_bytes(b"stream-bytes")
    with open(src, "rb") as stdin:
        ep.receive(stdin, snapshot_name="snap").communicate()

    with patch.object(RawEndpoint, "write_sidecar", autospec=True) as spy:
        ep.commit_receive()
    assert spy.called
    written = spy.call_args[0][1]  # (self, snapshot)
    assert isinstance(written, RawSnapshot)
    assert written.provenance_origin == "native-write"
    assert written.name == "snap"
