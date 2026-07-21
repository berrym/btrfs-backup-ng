"""Damaged raw sidecars must degrade LOUDLY, never silently or catastrophically (T3).

A corrupt/truncated/unparseable ``.meta`` sidecar must (1) never abort discovery of
the healthy backups sharing its directory (a single bad neighbour must not blind the
tool), and (2) never be silently swallowed -- the operator must be warned that the
authoritative record was lost and the stream fell back to filename inference.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import btrfs_backup_ng.endpoint.raw as raw_mod
import btrfs_backup_ng.endpoint.raw_metadata as meta_mod
from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot, discover_raw_snapshots


def test_damaged_sidecar_does_not_abort_directory_listing(tmp_path, monkeypatch):
    """A sidecar that raises an exception OUTSIDE the old narrow catch tuple (e.g. a
    RecursionError from pathologically-nested JSON -- forced here so the guard is
    deterministic across CPython versions and json backends, some of which parse deep
    nesting iteratively) must NOT abort the listing of the healthy backups sharing its
    directory. Mutation guard: reverting the broad `except Exception` re-raises this."""
    (tmp_path / "good.20240101T000000.btrfs").write_bytes(b"data")
    RawSnapshot(
        name="good.20240101T000000",
        stream_path=tmp_path / "good.20240101T000000.btrfs",
        size=4,
    ).save_metadata()
    bad_meta = tmp_path / "bad.20240102T000000.btrfs.meta"
    bad_meta.write_text("{}")
    (tmp_path / "bad.20240102T000000.btrfs").write_bytes(b"data")

    real_load = meta_mod.RawSnapshot.load_metadata

    def load(meta_path):
        if meta_path == bad_meta:
            raise RecursionError("deeply nested sidecar")  # NOT in the old narrow tuple
        return real_load(meta_path)

    monkeypatch.setattr(meta_mod.RawSnapshot, "load_metadata", staticmethod(load))
    names = {s.name for s in discover_raw_snapshots(tmp_path, "")}
    assert "good.20240101T000000" in names  # not hidden by the bad neighbour


def test_vanishing_sidecarless_stream_does_not_abort_listing(tmp_path, monkeypatch):
    """A sidecar-less stream removed between iterdir() and stat() (a concurrent prune)
    must skip just that one, not abort the whole listing -- the same one-bad-file
    class the sidecar loop closes, one loop down."""
    good = tmp_path / "good.20240101T000000.btrfs"
    good.write_bytes(b"d")
    RawSnapshot(name="good.20240101T000000", stream_path=good, size=1).save_metadata()
    vanish = tmp_path / "vanish.20240102T000000.btrfs"
    vanish.write_bytes(b"d")

    real_stat = meta_mod.Path.stat
    seen = {"n": 0}

    def stat(self, *a, **k):
        if self == vanish:  # is_file()'s stat succeeds; the explicit .stat() fails
            seen["n"] += 1
            if seen["n"] >= 2:
                raise FileNotFoundError(str(vanish))
        return real_stat(self, *a, **k)

    monkeypatch.setattr(meta_mod.Path, "stat", stat)
    names = {s.name for s in discover_raw_snapshots(tmp_path, "")}
    assert "good.20240101T000000" in names  # not aborted by the vanishing neighbour


def test_corrupt_sidecar_warns_rather_than_silently_degrading(tmp_path, monkeypatch):
    st = tmp_path / "x.20240101T000000.btrfs.zst"
    st.write_bytes(b"data")
    RawSnapshot(
        name="x.20240101T000000", stream_path=st, compress="zstd", size=4
    ).save_metadata()
    (tmp_path / "x.20240101T000000.btrfs.zst.meta").write_text('{"version":2, TRUNC')

    calls = []
    monkeypatch.setattr(meta_mod.logger, "warning", lambda *a, **k: calls.append(a))
    snaps = discover_raw_snapshots(tmp_path, "")
    # Warned by name (not silent), and the stream is still surfaced (filename-inferred).
    assert any("unreadable/corrupt" in str(a) for a in calls)
    assert any(s.provenance_origin == "filename-inferred" for s in snaps)


def test_ssh_corrupt_remote_sidecar_warns(monkeypatch):
    """The raw+ssh listing must also warn (not silently skip) on a corrupt remote
    sidecar, and a RecursionError from one must not abort the remote listing."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})

    def fake_run(cmd, **kwargs):
        joined = " ".join(cmd)
        if "-name '*.meta'" in joined:  # first-pass find -> one .meta
            return MagicMock(
                returncode=0, stdout="/backup/s.20240101T000000.btrfs.meta\n", stderr=""
            )
        if "cat " in joined:  # reading that sidecar -> corrupt JSON
            return MagicMock(
                returncode=0, stdout="{ this is not valid json ]]]", stderr=""
            )
        return MagicMock(
            returncode=0, stdout="", stderr=""
        )  # second-pass find -> empty

    monkeypatch.setattr(raw_mod.subprocess, "run", fake_run)
    calls = []
    monkeypatch.setattr(raw_mod.logger, "warning", lambda *a, **k: calls.append(a))
    ep.list_snapshots(flush_cache=True)  # must not raise
    assert any("unreadable/corrupt" in str(a) for a in calls)
