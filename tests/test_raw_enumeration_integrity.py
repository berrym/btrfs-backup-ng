"""Raw enumeration integrity (T4).

One physical stream must be listed exactly once, under one name. Bugs this guards:
a sidecar whose recorded name differs from its filename double-counted the stream
(name-based dedup missed it); two sidecar-less streams sharing a base name
(``x.btrfs`` + ``x.btrfs.zst``) were both listed under the same name (violating the
name-based identity restore/prune rely on); an empty/missing name in a sidecar
produced a phantom ``name=''`` record plus a duplicate.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import btrfs_backup_ng.endpoint.raw as raw_mod
from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot, discover_raw_snapshots


# --- local discover_raw_snapshots --------------------------------------------


def test_sidecar_name_differs_from_filename_counts_once(tmp_path):
    stream = tmp_path / "actual.20240101T000000.btrfs"
    stream.write_bytes(b"d")
    RawSnapshot(name="RECORDED_NAME", stream_path=stream, size=1).save_metadata()
    snaps = discover_raw_snapshots(tmp_path, "")
    assert len(snaps) == 1  # ONE stream -> ONE entry (not one per name)
    assert snaps[0].name == "RECORDED_NAME"  # the authoritative sidecar name wins


def test_two_streams_same_base_name_count_once(tmp_path):
    (tmp_path / "dup.20240101T000000.btrfs").write_bytes(b"plain")
    (tmp_path / "dup.20240101T000000.btrfs.zst").write_bytes(b"zstd")
    snaps = discover_raw_snapshots(tmp_path, "")
    names = [s.name for s in snaps]
    assert names.count("dup.20240101T000000") == 1  # no same-name duplicate


def test_empty_name_sidecar_is_not_a_phantom(tmp_path):
    (tmp_path / "noname.20240101T000000.btrfs").write_bytes(b"d")
    (tmp_path / "noname.20240101T000000.btrfs.meta").write_text(
        json.dumps({"version": 2, "size": 1})  # no "name"
    )
    snaps = discover_raw_snapshots(tmp_path, "")
    names = {s.name for s in snaps}
    assert "" not in names  # no phantom empty-name record
    assert names == {"noname.20240101T000000"}  # listed once, from the filename


# --- raw+ssh list_snapshots --------------------------------------------------


def _ssh_run(meta_cat_json=None, streams=()):
    """Build a subprocess.run stand-in dispatching by the remote command shape."""

    def run(cmd, **kwargs):
        joined = " ".join(cmd)
        if "-name '*.meta'" in joined:
            out = "/backup/s.20240101T000000.btrfs.meta\n" if meta_cat_json else ""
            return MagicMock(returncode=0, stdout=out, stderr="")
        if joined.startswith("ssh") and " cat " in f" {joined} ":
            return MagicMock(returncode=0, stdout=meta_cat_json or "", stderr="")
        if "-name '*.btrfs*'" in joined:
            return MagicMock(returncode=0, stdout="\n".join(streams), stderr="")
        if "stat -c" in joined:
            return MagicMock(returncode=0, stdout="1700000000 100\n", stderr="")
        return MagicMock(returncode=0, stdout="", stderr="")

    return run


def test_ssh_empty_name_sidecar_is_skipped(monkeypatch):
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    monkeypatch.setattr(
        raw_mod.subprocess,
        "run",
        _ssh_run(
            meta_cat_json=json.dumps({"version": 2, "size": 1}),  # no name
            streams=["/backup/s.20240101T000000.btrfs"],
        ),
    )
    names = {s.name for s in ep.list_snapshots(flush_cache=True)}
    assert "" not in names  # no phantom
    assert names == {"s.20240101T000000"}  # filename-inferred instead


def test_ssh_double_slash_config_path_does_not_double_count(monkeypatch):
    """A config path containing '//' must still dedup a sidecar'd stream: find output
    is unnormalized ('/backup//x') but the sidecar's stored path is Path-normalized
    ('/backup/x'), so the dedup must normalize both sides (else a double-count)."""
    ep = SSHRawEndpoint(config={"path": "/backup//", "hostname": "nas"})
    meta = "/backup//s.20240101T000000.btrfs.meta"
    stream = "/backup//s.20240101T000000.btrfs"
    # The sidecar name differs from the filename so ONLY path-dedup (not name-dedup)
    # can catch the duplicate -- isolating the '//' normalization.
    good = json.dumps({"version": 2, "name": "RECORDED_DIFFERENT", "size": 4})

    def run(cmd, **kwargs):
        joined = " ".join(cmd)
        if "-name '*.meta'" in joined:
            return MagicMock(returncode=0, stdout=meta + "\n", stderr="")
        if " cat " in f" {joined} ":
            return MagicMock(returncode=0, stdout=good, stderr="")
        if "-name '*.btrfs*'" in joined:
            return MagicMock(returncode=0, stdout=stream + "\n", stderr="")
        return MagicMock(returncode=0, stdout="1700000000 4\n", stderr="")

    monkeypatch.setattr(raw_mod.subprocess, "run", run)
    snaps = ep.list_snapshots(flush_cache=True)
    assert len(snaps) == 1  # one physical stream -> one entry, despite the '//'


def test_ssh_two_streams_same_base_name_count_once(monkeypatch):
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    monkeypatch.setattr(
        raw_mod.subprocess,
        "run",
        _ssh_run(
            meta_cat_json=None,  # no sidecars
            streams=[
                "/backup/d.20240101T000000.btrfs",
                "/backup/d.20240101T000000.btrfs.zst",
            ],
        ),
    )
    names = [s.name for s in ep.list_snapshots(flush_cache=True)]
    assert names.count("d.20240101T000000") == 1
