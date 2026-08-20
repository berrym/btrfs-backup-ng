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


# --- raw+ssh prefix filtering -------------------------------------------------


class TestTheRemoteListingHonoursTheSnapshotPrefix:
    """A prefix must select the same snapshots locally and remotely.

    ``list_snapshots`` makes two passes: sidecars first, then sidecar-less
    streams by filename. Only the second filtered on ``snap_prefix``, so which
    snapshots a prefix selected depended on whether a .meta happened to sit
    beside them -- and since every backup this project writes has one, the
    prefix was ignored in practice.

    Measured .203 -> .70 with two volumes sharing one raw destination:
    ``restore --list --prefix sweep-`` returned both volumes' snapshots, and
    ``restore --prefix sweep-`` restored the other volume too, reported
    "Restored: 2, Failed: 0", and used one volume's snapshot as the incremental
    parent of the other volume's stream. The local raw:// listing of the very
    same files returned only the requested set.
    """

    def _endpoint(self, monkeypatch, prefix, sidecar_names):
        metas = [f"/backup/{name}.btrfs.meta" for name in sidecar_names]
        by_meta = {
            meta: json.dumps({"version": 2, "name": name, "size": 4})
            for meta, name in zip(metas, sidecar_names)
        }

        def run(cmd, **kwargs):
            joined = " ".join(cmd)
            if "-name '*.meta'" in joined:
                return MagicMock(
                    returncode=0, stdout="\n".join(metas) + "\n", stderr=""
                )
            if " cat " in f" {joined} ":
                for meta, payload in by_meta.items():
                    if meta in joined:
                        return MagicMock(returncode=0, stdout=payload, stderr="")
                return MagicMock(returncode=0, stdout="", stderr="")
            if "-name '*.btrfs*'" in joined:
                return MagicMock(returncode=0, stdout="", stderr="")
            return MagicMock(returncode=0, stdout="1700000000 4\n", stderr="")

        monkeypatch.setattr(raw_mod.subprocess, "run", run)
        return SSHRawEndpoint(
            config={"path": "/backup", "hostname": "nas", "snap_prefix": prefix}
        )

    def test_a_prefix_excludes_another_volumes_sidecar_backed_stream(self, monkeypatch):
        endpoint = self._endpoint(
            monkeypatch, "sweep-", ["sweep-20260820-095317", "other-20260820-095644"]
        )
        names = {s.name for s in endpoint.list_snapshots(flush_cache=True)}
        assert names == {"sweep-20260820-095317"}, (
            f"the prefix did not exclude another volume's backup: {names}"
        )

    def test_the_other_prefix_selects_the_other_volume(self, monkeypatch):
        endpoint = self._endpoint(
            monkeypatch, "other-", ["sweep-20260820-095317", "other-20260820-095644"]
        )
        names = {s.name for s in endpoint.list_snapshots(flush_cache=True)}
        assert names == {"other-20260820-095644"}, names

    def test_no_prefix_still_lists_everything(self, monkeypatch):
        """The filter must not become an over-filter: no prefix means no filtering."""
        endpoint = self._endpoint(
            monkeypatch, "", ["sweep-20260820-095317", "other-20260820-095644"]
        )
        names = {s.name for s in endpoint.list_snapshots(flush_cache=True)}
        assert names == {"sweep-20260820-095317", "other-20260820-095644"}, names

    def test_it_filters_on_the_recorded_name_not_the_filename(self, monkeypatch):
        """The sidecar's name is authoritative; the local listing filters on it,
        so a stream whose FILENAME matches but whose recorded name does not must
        be excluded, and the two transports must agree."""
        metas = ["/backup/sweep-20260820-095317.btrfs.meta"]

        def run(cmd, **kwargs):
            joined = " ".join(cmd)
            if "-name '*.meta'" in joined:
                return MagicMock(returncode=0, stdout=metas[0] + "\n", stderr="")
            if " cat " in f" {joined} ":
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps(
                        {"version": 2, "name": "other-RECORDED", "size": 4}
                    ),
                    stderr="",
                )
            if "-name '*.btrfs*'" in joined:
                return MagicMock(returncode=0, stdout="", stderr="")
            return MagicMock(returncode=0, stdout="1700000000 4\n", stderr="")

        monkeypatch.setattr(raw_mod.subprocess, "run", run)
        endpoint = SSHRawEndpoint(
            config={"path": "/backup", "hostname": "nas", "snap_prefix": "sweep-"}
        )
        assert endpoint.list_snapshots(flush_cache=True) == []

    def test_the_two_transports_agree_on_the_same_files(self, monkeypatch, tmp_path):
        """The bug was an asymmetry, so the asymmetry itself is what is pinned."""
        for name in ("sweep-20260820-095317", "other-20260820-095644"):
            stream = tmp_path / f"{name}.btrfs"
            stream.write_bytes(b"data")
            RawSnapshot(name=name, stream_path=stream, size=4).save_metadata()
        local = {s.name for s in discover_raw_snapshots(tmp_path, "sweep-")}

        endpoint = self._endpoint(
            monkeypatch, "sweep-", ["sweep-20260820-095317", "other-20260820-095644"]
        )
        remote = {s.name for s in endpoint.list_snapshots(flush_cache=True)}
        assert local == remote, f"local {local} != remote {remote}"
