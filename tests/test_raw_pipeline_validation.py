"""Raw pipeline tool validation (restore + backup).

A restore must FAIL LOUD when it cannot actually decode a stream -- an unknown
compression algorithm recorded in the sidecar (T5) or a missing decompress/decrypt
tool (T2) -- instead of silently piping bad data into ``btrfs receive`` or crashing
with a raw ``FileNotFoundError``. A backup must fail its preflight (not mid-transfer)
when a required compress/encrypt tool is absent.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import btrfs_backup_ng.endpoint.raw as raw_mod
from btrfs_backup_ng.__util__ import AbortError
from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


# --- T5: unknown compression algorithm must not silently pass through ---------


def test_unknown_compress_algo_fails_loud(tmp_path):
    ep = RawEndpoint(config={"path": str(tmp_path)})
    snap = RawSnapshot(
        name="s", stream_path=tmp_path / "s.btrfs", compress="frobnicate"
    )
    with pytest.raises(AbortError, match="cannot decompress"):
        ep._build_restore_pipeline(snap)


def test_unknown_encrypt_method_fails_loud(tmp_path):
    """Symmetric to the compress case: an encryption method the sidecar records but
    this version cannot reverse must NOT silently pass the encrypted bytes through."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    snap = RawSnapshot(
        name="s", stream_path=tmp_path / "s.btrfs.enc", encrypt="future_cipher_v9"
    )
    with pytest.raises(AbortError, match="cannot decrypt"):
        ep._build_restore_pipeline(snap)


# --- T2: tool preflight (restore) --------------------------------------------


def test_restore_missing_tool_preflights_with_clear_message(tmp_path, monkeypatch):
    stream = tmp_path / "s.20240101T120000.btrfs.zst"
    stream.write_bytes(b"x")
    RawSnapshot(
        name="s.20240101T120000", stream_path=stream, compress="zstd", size=1
    ).save_metadata()
    ep = RawEndpoint(config={"path": str(tmp_path)})
    (snap,) = ep.list_snapshots(flush_cache=True)
    monkeypatch.setattr(raw_mod.shutil, "which", lambda _t: None)  # nothing installed
    with pytest.raises(AbortError, match="zstd.*not installed|Install 'zstd'"):
        ep.send(snap)


def test_restore_preflight_passes_when_tools_present(tmp_path):
    ep = RawEndpoint(config={"path": str(tmp_path)})
    snap = RawSnapshot(name="s", stream_path=tmp_path / "s.btrfs.zst", compress="zstd")
    pipeline = ep._build_restore_pipeline(snap)
    ep._preflight_restore_tools(pipeline, snap)  # zstd installed -> no raise


def test_pipeline_construction_is_independent_of_tool_availability(
    tmp_path, monkeypatch
):
    """Building the pipeline must NOT require the tools to be installed -- the
    availability check is a separate preflight. Guards the construction/preflight
    split so `_build_restore_pipeline` can be inspected in any environment."""
    monkeypatch.setattr(raw_mod.shutil, "which", lambda _t: None)
    ep = RawEndpoint(config={"path": str(tmp_path)})
    snap = RawSnapshot(name="s", stream_path=tmp_path / "s.btrfs.zst", compress="zstd")
    pipeline = ep._build_restore_pipeline(snap)  # must not raise despite which=None
    assert any("zstd" in stage[0] for stage in pipeline)


# --- T2: tool preflight (backup) ---------------------------------------------


def test_backup_prepare_fails_loud_on_missing_tool(tmp_path, monkeypatch):
    ep = RawEndpoint(config={"path": str(tmp_path), "compress": "zstd"})
    monkeypatch.setattr(raw_mod.shutil, "which", lambda _t: None)
    with pytest.raises(AbortError, match="not installed"):
        ep.prepare()


def test_backup_prepare_ok_when_tools_present(tmp_path):
    ep = RawEndpoint(config={"path": str(tmp_path), "compress": "zstd"})
    ep.prepare()  # zstd installed here -> no raise
    assert Path(tmp_path).exists()
