"""raw backfill-metadata (0.8.5 PR6d).

Writes authoritative .meta sidecars for LEGACY streams that have none, stamped
provenance_origin=backfill and stream_completeness=unknown -- a legacy stream's
completeness cannot be proven, so a backfilled sidecar is never authoritative.
"""

import argparse
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from btrfs_backup_ng.endpoint import raw as raw_mod
from btrfs_backup_ng.cli import raw_cmd
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot, discover_raw_snapshots


def _legacy_stream(path, name, *, compress=None, encrypt=None):
    """Create a committed backup then remove its sidecar (a pre-0.8.5 stream)."""
    cfg = {"path": str(path)}
    if compress:
        cfg["compress"] = compress
    if encrypt:
        cfg["encrypt"] = encrypt
        cfg["openssl_cipher"] = "aes-256-cbc"
    ep = RawEndpoint(config=cfg)
    src = path / (name + ".src")
    src.write_bytes(b"legacy-" * 300)
    with open(src, "rb") as f:
        ep.receive(f, snapshot_name=name).communicate()
    ep.commit_receive()
    src.unlink()
    # Strip the sidecar to simulate a legacy backup written before sidecars.
    for meta in path.glob(f"{name}.btrfs*.meta"):
        meta.unlink()


def _args(**kw):
    ns = argparse.Namespace()
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


def _backfill(tmp_path, **kw):
    base = {
        "raw_action": "backfill-metadata",
        "target": str(tmp_path),
        "dry_run": False,
        "json": False,
    }
    base.update(kw)
    return raw_cmd.execute_raw(_args(**base))


# --------------------------------------------------------------------------- #
# candidate discovery
# --------------------------------------------------------------------------- #
def test_streams_without_sidecar_finds_only_sidecarless(tmp_path, monkeypatch):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "x")
    _legacy_stream(tmp_path, "legacy")  # no sidecar
    # A backup WITH a sidecar (not a candidate).
    ep = RawEndpoint(config={"path": str(tmp_path)})
    src = tmp_path / "kept.src"
    src.write_bytes(b"kept")
    with open(src, "rb") as f:
        ep.receive(f, snapshot_name="kept").communicate()
    ep.commit_receive()
    src.unlink()

    cands = ep.streams_without_sidecar()
    names = {c.name for c in cands}
    assert "legacy" in names
    assert "kept" not in names  # already has a sidecar
    # Candidates are stamped honestly.
    (leg,) = [c for c in cands if c.name == "legacy"]
    assert leg.provenance_origin == "backfill"
    assert leg.stream_completeness == "unknown"


# --------------------------------------------------------------------------- #
# honest labeling of sidecar-less streams (pre-backfill)
# --------------------------------------------------------------------------- #
def test_filename_inferred_snapshot_is_labeled_honestly(tmp_path):
    _legacy_stream(tmp_path, "leg")
    (snap,) = discover_raw_snapshots(tmp_path)
    assert snap.provenance_origin == "filename-inferred"
    assert snap.stream_completeness == "unknown"


# --------------------------------------------------------------------------- #
# backfill outcomes
# --------------------------------------------------------------------------- #
def test_backfill_writes_unknown_completeness_sidecar(tmp_path):
    _legacy_stream(tmp_path, "root.20240101T120000", compress="gzip")
    rc = _backfill(tmp_path)
    assert rc == 0
    meta = next(tmp_path.glob("*.meta"))
    doc = json.loads(meta.read_text())
    # A legacy stream must NEVER be blessed as complete/native.
    assert doc["provenance"]["origin"] == "backfill"
    assert doc["provenance"]["stream_completeness"] == "unknown"
    assert doc["pipeline"]["compress"] == "gzip"
    assert doc["checksum"]["value"] is not None  # sealed by hashing the stream


def test_backfill_then_verify_is_ok(tmp_path):
    _legacy_stream(tmp_path, "root.20240101T120000")
    assert _backfill(tmp_path) == 0
    rc = raw_cmd.execute_raw(
        _args(
            raw_action="verify",
            target=str(tmp_path),
            snapshot=None,
            json=False,
        )
    )
    assert rc == 0  # the sealed checksum matches the stream


def test_backfill_dry_run_writes_nothing(tmp_path, capsys):
    _legacy_stream(tmp_path, "root.20240101T120000")
    rc = _backfill(tmp_path, dry_run=True)
    out = capsys.readouterr().out
    assert rc == 0
    assert "WOULD-BACKFILL" in out
    assert list(tmp_path.glob("*.meta")) == []  # nothing written


def test_backfill_no_candidates(tmp_path, capsys):
    """A target with only sidecar-backed backups has nothing to backfill."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    src = tmp_path / "s.src"
    src.write_bytes(b"data")
    with open(src, "rb") as f:
        ep.receive(f, snapshot_name="s").communicate()
    ep.commit_receive()
    src.unlink()
    rc = _backfill(tmp_path)
    out = capsys.readouterr().out
    assert rc == 0
    assert "0 legacy stream" in out


def test_backfill_json_output(tmp_path, capsys):
    _legacy_stream(tmp_path, "root.20240101T120000", compress="gzip")
    rc = _backfill(tmp_path, json=True)
    data = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert data[0]["name"] == "root.20240101T120000"
    assert data[0]["action"] == "backfilled"
    assert data[0]["compress"] == "gzip"


# --------------------------------------------------------------------------- #
# remote (raw+ssh) candidate discovery
# --------------------------------------------------------------------------- #
def test_ssh_streams_without_sidecar(monkeypatch):
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    # Two streams; one already has a sidecar.
    ep._remote_find = lambda pattern: (
        ["/backup/legacy.btrfs.gz", "/backup/kept.btrfs"]
        if pattern == "*.btrfs*"
        else ["/backup/kept.btrfs.meta"]
    )
    ep._remote_stat = lambda p: (datetime(2024, 1, 1, tzinfo=timezone.utc), 123)
    cands = ep.streams_without_sidecar()
    names = {c.name for c in cands}
    assert names == {"legacy"}  # kept has a sidecar
    (leg,) = cands
    assert leg.provenance_origin == "backfill"
    assert leg.stream_completeness == "unknown"
    assert leg.compress == "gzip"


# --------------------------------------------------------------------------- #
# schema round-trip + dispatcher
# --------------------------------------------------------------------------- #
def test_stream_completeness_round_trips(tmp_path):
    snap = RawSnapshot(
        name="s",
        stream_path=tmp_path / "s.btrfs",
        provenance_origin="backfill",
        stream_completeness="unknown",
    )
    doc = snap.to_dict()
    assert doc["provenance"]["stream_completeness"] == "unknown"
    restored = RawSnapshot.from_dict(doc, tmp_path / "s.btrfs")
    assert restored.stream_completeness == "unknown"
    # Legacy sidecar without the field defaults to "complete".
    doc["provenance"].pop("stream_completeness")
    assert (
        RawSnapshot.from_dict(doc, tmp_path / "s.btrfs").stream_completeness
        == "complete"
    )


def test_dispatcher_parses_raw_backfill():
    from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

    parser = create_subcommand_parser()
    args = parser.parse_args(
        ["raw", "backfill-metadata", "/x", "--dry-run", "--json", "--ssh-sudo"]
    )
    assert args.command == "raw"
    assert args.raw_action == "backfill-metadata"
    assert args.target == "/x"
    assert args.dry_run is True
    assert args.json is True
    assert args.ssh_sudo is True


def test_native_backup_stays_complete(tmp_path):
    """A native atomic backup's sidecar must record stream_completeness=complete
    (only legacy/backfill/filename-inferred are 'unknown')."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    src = tmp_path / "s.src"
    src.write_bytes(b"data")
    with open(src, "rb") as f:
        ep.receive(f, snapshot_name="s").communicate()
    ep.commit_receive()
    (snap,) = discover_raw_snapshots(tmp_path)
    assert snap.provenance_origin == "native-write"
    assert snap.stream_completeness == "complete"


# --------------------------------------------------------------------------- #
# security: symlink handling (backfill writes while walking an untrusted dir)
# --------------------------------------------------------------------------- #
def test_backfill_symlinked_meta_tmp_cannot_truncate_outside_file(tmp_path):
    """A pre-planted <name>.meta.tmp symlink must NOT let the O_CREAT|O_TRUNC write
    redirect to an arbitrary file (O_NOFOLLOW in save_metadata). The outside file
    stays intact and no sidecar is written for the malicious candidate."""
    outside = tmp_path / "outside.secret"
    outside.write_text("DO-NOT-TRUNCATE")
    stream = tmp_path / "pwn.btrfs"
    stream.write_bytes(b"payload")
    (tmp_path / "pwn.btrfs.meta.tmp").symlink_to(outside)

    _backfill(tmp_path)  # rc may be 1 (write errored); the point is no damage
    assert outside.read_text() == "DO-NOT-TRUNCATE"  # never truncated
    assert not (tmp_path / "pwn.btrfs.meta").exists()


def test_backfill_skips_symlinked_stream(tmp_path):
    """A symlinked 'stream' is not a backfill candidate (defends the write + hash
    against a <name>.btrfs symlink pointing at an arbitrary file)."""
    real = tmp_path / "real.dat"
    real.write_bytes(b"x")
    (tmp_path / "link.btrfs").symlink_to(real)
    ep = RawEndpoint(config={"path": str(tmp_path)})
    assert ep.streams_without_sidecar() == []


# --------------------------------------------------------------------------- #
# concurrency: re-check before write, so a racing commit is not overwritten
# --------------------------------------------------------------------------- #
def test_backfill_rechecks_and_skips_if_sidecar_appears(tmp_path, monkeypatch, capsys):
    """If a sidecar appears between the scan and the write (a backup committed
    concurrently), backfill must SKIP -- never overwrite an authoritative sidecar
    with a backfill record. Dropping the re-check would clobber it."""
    _legacy_stream(tmp_path, "root.20240101T120000")
    monkeypatch.setattr(RawEndpoint, "sidecar_exists", lambda self, snap: True)
    rc = _backfill(tmp_path)
    out = capsys.readouterr().out
    assert rc == 0
    assert "SKIPPED" in out
    assert list(tmp_path.glob("*.meta")) == []  # nothing written


# --------------------------------------------------------------------------- #
# security: remote find output must stay inside the target dir
# --------------------------------------------------------------------------- #
def test_remote_find_rejects_out_of_target_paths(monkeypatch):
    """A crafted remote filename containing a newline (or any path outside the
    target dir) must not inject an out-of-target write target."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._build_ssh_command = lambda: ["ssh", "nas"]
    fake = MagicMock(returncode=0, stdout="/backup/ok.btrfs\x00/etc/passwd\x00")
    monkeypatch.setattr(raw_mod.subprocess, "run", lambda *a, **k: fake)
    assert ep._remote_find("*.btrfs*") == ["/backup/ok.btrfs"]


def test_ssh_list_snapshots_stamps_filename_inferred(monkeypatch):
    """A remote sidecar-less stream comes back from list_snapshots stamped
    filename-inferred/unknown (never native-write/complete)."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._build_ssh_command = lambda: ["ssh", "nas"]
    seq = [
        MagicMock(returncode=0, stdout=""),  # find *.meta -> none
        MagicMock(returncode=0, stdout="/backup/legacy.btrfs.gz"),  # find *.btrfs*
        MagicMock(returncode=0, stdout="1704067200 123"),  # stat mtime size
    ]
    monkeypatch.setattr(raw_mod.subprocess, "run", MagicMock(side_effect=seq))
    snaps = ep.list_snapshots(flush_cache=True)
    assert len(snaps) == 1
    assert snaps[0].provenance_origin == "filename-inferred"
    assert snaps[0].stream_completeness == "unknown"


# --------------------------------------------------------------------------- #
# empty / nonexistent target
# --------------------------------------------------------------------------- #
def test_backfill_nonexistent_target(tmp_path, capsys):
    rc = _backfill(tmp_path, target=str(tmp_path / "nope"))
    out = capsys.readouterr().out
    assert rc == 0
    assert "0 legacy stream" in out


def test_ssh_streams_without_sidecar_empty(monkeypatch):
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._remote_find = lambda pattern: []
    assert ep.streams_without_sidecar() == []
