"""raw verify (0.8.5 PR6c).

`raw verify` recomputes each stream's sha256 and compares it to the checksum
recorded in the sidecar (PR6b). Per-snapshot status: ok / corrupt / error /
unverifiable; exit 1 if any snapshot is corrupt or errored.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.cli import raw_cmd
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def _build(path, name):
    ep = RawEndpoint(config={"path": str(path)})
    src = path / (name + ".src")
    src.write_bytes(b"verify-payload-" * 300)
    with open(src, "rb") as f:
        ep.receive(f, snapshot_name=name).communicate()
    ep.commit_receive()
    src.unlink()


def _args(**kw):
    ns = argparse.Namespace()
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


def _verify(tmp_path, **kw):
    base = {
        "raw_action": "verify",
        "target": str(tmp_path),
        "snapshot": None,
        "json": False,
    }
    base.update(kw)
    return raw_cmd.execute_raw(_args(**base))


# --------------------------------------------------------------------------- #
# compute_stream_checksum
# --------------------------------------------------------------------------- #
def test_compute_stream_checksum_matches_sealed(tmp_path):
    _build(tmp_path, "s1")
    ep = RawEndpoint(config={"path": str(tmp_path)})
    (snap,) = ep.list_snapshots(flush_cache=True)
    current = ep.compute_stream_checksum(snap)
    assert current == hashlib.sha256(snap.stream_path.read_bytes()).hexdigest()
    assert current == snap.checksum_value  # a fresh backup verifies clean


def test_ssh_compute_stream_checksum_delegates_to_remote():
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._remote_sha256 = MagicMock(return_value="c" * 64)
    snap = RawSnapshot(name="s", stream_path=Path("/backup/s.btrfs"))
    assert ep.compute_stream_checksum(snap) == "c" * 64
    ep._remote_sha256.assert_called_once_with(Path("/backup/s.btrfs"))


# --------------------------------------------------------------------------- #
# raw verify outcomes
# --------------------------------------------------------------------------- #
def test_verify_all_ok(tmp_path, capsys):
    _build(tmp_path, "good1")
    _build(tmp_path, "good2")
    rc = _verify(tmp_path)
    out = capsys.readouterr().out
    assert rc == 0
    assert "2 ok, 0 corrupt" in out


def test_verify_detects_corruption(tmp_path, capsys):
    """A stream whose bytes changed after backup (its recorded checksum no longer
    matches) is reported corrupt with a nonzero exit. If verify stopped comparing
    the recomputed checksum, this fails."""
    _build(tmp_path, "good")
    _build(tmp_path, "bad")
    bad_stream = tmp_path / "bad.btrfs"
    bad_stream.write_bytes(bad_stream.read_bytes() + b"CORRUPTION")
    rc = _verify(tmp_path)
    out = capsys.readouterr().out
    assert rc == 1
    assert "CORRUPT" in out and "bad" in out
    assert "1 ok, 1 corrupt" in out
    # The corrupt row shows the mismatch (recorded vs computed) for triage.
    assert "recorded=" in out and "computed=" in out


def test_verify_unverifiable_when_no_recorded_checksum(tmp_path, capsys):
    """A backup whose sidecar recorded no checksum (legacy / best-effort null) is
    'unverifiable', not a failure (exit 0)."""
    _build(tmp_path, "legacy")
    meta = tmp_path / "legacy.btrfs.meta"
    doc = json.loads(meta.read_text())
    doc["checksum"]["value"] = None
    meta.write_text(json.dumps(doc))
    rc = _verify(tmp_path)
    out = capsys.readouterr().out
    assert rc == 0
    assert "1 unverifiable" in out


@pytest.mark.skipif(
    os.geteuid() == 0, reason="root bypasses chmod 0o000, so the stream stays readable"
)
def test_verify_error_when_stream_unreadable(tmp_path, capsys):
    """A recorded checksum but an unreadable stream is 'error' (exit 1) -- distinct
    from corrupt."""
    _build(tmp_path, "locked")
    stream = tmp_path / "locked.btrfs"
    stream.chmod(0o000)
    try:
        rc = _verify(tmp_path)
        out = capsys.readouterr().out
        assert rc == 1
        assert "1 error" in out
    finally:
        stream.chmod(0o600)


def test_verify_error_over_ssh_when_remote_hash_none(monkeypatch, tmp_path):
    """For raw+ssh, a None remote hash (no hash tool / hashing failed) maps to
    'error' with exit 1 -- not corrupt, not a crash."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    snap = RawSnapshot(
        name="s", stream_path=Path("/backup/s.btrfs"), checksum_value="a" * 64
    )
    ep.list_snapshots = lambda flush_cache=False: [snap]
    ep._remote_sha256 = lambda p: None  # remote hashing failed
    monkeypatch.setattr(raw_cmd.endpoint, "choose_endpoint", lambda *a, **k: ep)
    rc = _verify(tmp_path, target="raw+ssh://nas/backup")
    assert rc == 1


def test_verify_snapshot_filter(tmp_path, capsys):
    _build(tmp_path, "aaa")
    _build(tmp_path, "bbb")
    rc = _verify(tmp_path, snapshot="aaa")
    out = capsys.readouterr().out
    assert rc == 0
    assert "verifying 1 snapshot" in out
    assert "aaa" in out


def test_verify_snapshot_not_found(tmp_path, capsys):
    _build(tmp_path, "aaa")
    rc = _verify(tmp_path, snapshot="nope")
    assert rc == 2
    assert "No snapshot named" in capsys.readouterr().out


def test_verify_json(tmp_path, capsys):
    _build(tmp_path, "jjj")
    rc = _verify(tmp_path, json=True)
    data = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert data[0]["name"] == "jjj"
    assert data[0]["status"] == "ok"
    assert data[0]["recorded"] == data[0]["computed"]


def test_verify_rejects_non_raw_scheme(capsys):
    rc = _verify_target("ssh://host/path")
    assert rc == 2
    assert "not a raw target" in capsys.readouterr().out


def _verify_target(target):
    return raw_cmd.execute_raw(
        _args(raw_action="verify", target=target, snapshot=None, json=False)
    )


def test_verify_empty_target(tmp_path, capsys):
    rc = _verify(tmp_path)
    out = capsys.readouterr().out
    assert rc == 0
    assert "verifying 0 snapshot" in out


def test_verify_all_unverifiable_exits_zero(tmp_path, capsys):
    """A target whose backups all lack a recorded checksum must exit 0, not 1
    (unverifiable is not a failure). Folding unverifiable into the bad set fails."""
    for n in ("a", "b"):
        _build(tmp_path, n)
        meta = tmp_path / f"{n}.btrfs.meta"
        doc = json.loads(meta.read_text())
        doc["checksum"]["value"] = None
        meta.write_text(json.dumps(doc))
    rc = _verify(tmp_path)
    out = capsys.readouterr().out
    assert rc == 0
    assert "2 unverifiable" in out


def test_verify_unsupported_algorithm_is_unverifiable(tmp_path, capsys):
    """A sidecar recording a non-sha256 algorithm (with a value) must NOT be
    false-flagged corrupt -- we only compute sha256, so it is unverifiable."""
    _build(tmp_path, "other")
    meta = tmp_path / "other.btrfs.meta"
    doc = json.loads(meta.read_text())
    doc["checksum"]["algorithm"] = "blake2b"  # value stays (a real sha256 hex)
    meta.write_text(json.dumps(doc))
    rc = _verify(tmp_path)
    out = capsys.readouterr().out
    assert rc == 0
    assert "1 unverifiable" in out
    assert "corrupt" not in out.lower().replace("0 corrupt", "")


def test_open_target_construction_error_keeps_prefix(capsys):
    """A choose_endpoint construction failure (e.g. a hostname-less raw+ssh spec)
    keeps the 'Cannot open raw target:' framing; a bare coercion error does not."""
    rc = raw_cmd.execute_raw(
        _args(raw_action="list", target="raw+ssh:///backups/x", json=False)
    )
    assert rc == 2
    assert "Cannot open raw target:" in capsys.readouterr().out


def test_dispatcher_parses_raw_verify():
    """The real parser recognizes `raw verify TARGET --snapshot ... --json
    --ssh-sudo`. A missing subparser registration would fail here."""
    from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

    parser = create_subcommand_parser()
    args = parser.parse_args(
        ["raw", "verify", "/x", "--snapshot", "s1", "--json", "--ssh-sudo"]
    )
    assert args.command == "raw"
    assert args.raw_action == "verify"
    assert args.target == "/x"
    assert args.snapshot == "s1"
    assert args.json is True
    assert args.ssh_sudo is True
