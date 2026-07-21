"""Compression integrity for raw targets (0.8.5).

A raw target's ``compress`` must be owned by the raw endpoint -- applied in its
own pipeline AND recorded in the ``.meta`` sidecar -- so restore can reverse it.
The bug these tests guard: compression routed to the generic transfer layer is
invisible to the sidecar, so the sidecar records ``compress: null`` while the
stream is compressed, producing an UNRESTORABLE backup.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import Mock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli.common import thread_raw_compression
from btrfs_backup_ng.core import operations
from btrfs_backup_ng.endpoint import (
    assert_compression_applied,
    choose_endpoint,
)
from btrfs_backup_ng.endpoint.raw import RawEndpoint


def _target(**kw):
    ns = argparse.Namespace()
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


# --- thread_raw_compression --------------------------------------------------


def test_thread_raw_compression_copies_target_compress():
    kwargs: dict = {}
    thread_raw_compression(kwargs, _target(compress="zstd"))
    assert kwargs["compress"] == "zstd"


def test_thread_raw_compression_defaults_none_when_absent():
    kwargs: dict = {}
    thread_raw_compression(kwargs, _target())  # no compress attr
    assert kwargs["compress"] is None


def test_thread_raw_compression_override_wins():
    """A CLI --compress override must reach a raw endpoint (else it is silently
    dropped and the raw target is written uncompressed despite the flag)."""
    kwargs: dict = {}
    thread_raw_compression(kwargs, _target(compress="none"), override="zstd")
    assert kwargs["compress"] == "zstd"


def test_thread_raw_compression_no_override_uses_target():
    kwargs: dict = {}
    thread_raw_compression(kwargs, _target(compress="gzip"), override=None)
    assert kwargs["compress"] == "gzip"


# --- assert_compression_applied (fail-closed guard) --------------------------


def test_guard_raises_when_raw_endpoint_missing_compress(tmp_path):
    """A raw endpoint that did NOT receive the requested compression must abort --
    otherwise the stream is compressed by the transfer layer but the sidecar records
    null, an unrestorable backup."""
    ep = RawEndpoint(config={"path": str(tmp_path)})  # compress unset
    assert ep.compress is None
    with pytest.raises(__util__.AbortError, match="UNRESTORABLE"):
        assert_compression_applied("zstd", ep)


def test_guard_passes_when_raw_endpoint_has_compress(tmp_path):
    ep = RawEndpoint(config={"path": str(tmp_path), "compress": "zstd"})
    assert_compression_applied("zstd", ep)  # no raise


def test_guard_noop_when_no_compression_requested(tmp_path):
    ep = RawEndpoint(config={"path": str(tmp_path)})
    assert_compression_applied(None, ep)
    assert_compression_applied("none", ep)  # neither raises


def test_guard_noop_for_non_raw_endpoint():
    """Non-raw targets legitimately delegate compression to the transfer layer
    (their receive side decompresses symmetrically), so the guard must not fire."""

    class _NotRaw:
        compress = None

    assert_compression_applied("zstd", _NotRaw())  # no raise


# --- config -> endpoint threading -------------------------------------------


def test_choose_endpoint_threads_compress_into_raw_endpoint(tmp_path):
    """The threaded compress key must survive choose_endpoint's whitelist and reach
    RawEndpoint.compress (else it never gets recorded in the sidecar)."""
    ep = choose_endpoint(f"raw://{tmp_path}", {"compress": "zstd"})
    assert isinstance(ep, RawEndpoint)
    assert ep.compress == "zstd"


def test_compress_none_sentinel_normalized_not_rejected(tmp_path):
    """The config sentinel compress="none" (the default for every target) must
    normalize to no-compression, NOT be rejected as an unknown algorithm -- so
    threading it on plaintext/uncompressed targets does not abort the backup."""
    ep = choose_endpoint(f"raw://{tmp_path}", {"compress": "none"})
    assert ep.compress is None
    assert_compression_applied("none", ep)  # no false abort


# --- write-time sidecar recording -------------------------------------------


def test_commit_records_compress_in_sidecar_and_round_trips(tmp_path):
    """End-to-end at the endpoint layer (no btrfs needed): a compress-configured raw
    endpoint compresses the stream, records compress in the sidecar, and restore
    (send) decompresses back to the exact original bytes."""
    payload = b"btrfs-stream\x00" + b"the quick brown fox " * 5000
    ep = RawEndpoint(config={"path": str(tmp_path), "compress": "gzip"})
    src = tmp_path / "in.bin"
    src.write_bytes(payload)
    with open(src, "rb") as f:
        ep.receive(f, snapshot_name="root.20240101T120000").communicate()
    ep.commit_receive()

    (snap,) = ep.list_snapshots(flush_cache=True)
    assert snap.compress == "gzip"  # recorded in the sidecar
    # The committed stream is really compressed (smaller than the raw payload).
    assert snap.stream_path.stat().st_size < len(payload)
    # Restore reverses it: send() decompresses back to the original bytes.
    proc = ep.send(snap)
    out, _ = proc.communicate()
    assert out == payload


# --- no double-compression at the transfer layer for raw ---------------------


def test_send_snapshot_suppresses_transfer_compress_for_raw(tmp_path, monkeypatch):
    """send_snapshot must neutralize transfer-layer compression for a raw
    destination, so the raw endpoint is the ONLY thing that compresses (single,
    sidecar-recorded compression). Otherwise: double-compression + null sidecar."""
    captured: dict = {}

    class _Stop(Exception):
        pass

    def fake_process_transfer(send_process, dest, receive_process, is_ssh, **kw):
        captured["compress"] = kw.get("compress")
        raise _Stop  # stop before the (irrelevant) post-transfer path

    monkeypatch.setattr(operations, "_do_process_transfer", fake_process_transfer)

    raw_ep = RawEndpoint(config={"path": str(tmp_path), "compress": "zstd"})
    snap = Mock()
    snap.get_path.return_value = str(tmp_path / "snap")
    snap.__str__ = lambda self: "root.20240101T120000"
    snap.endpoint.send.return_value = Mock()  # non-None send process

    with pytest.raises(_Stop):
        operations.send_snapshot(
            snap, raw_ep, options={"compress": "zstd", "show_progress": False}
        )
    # The transfer layer was asked for NO compression despite options requesting zstd.
    assert captured["compress"] == "none"


def test_send_snapshot_preserves_transfer_compress_for_non_raw(tmp_path, monkeypatch):
    """The raw suppression must NOT strip transfer-layer compression from non-raw
    targets (local/ssh btrfs) -- they legitimately compress at the transfer layer and
    decompress symmetrically on receive. Guards against a future edit widening the
    isinstance check."""
    captured: dict = {}

    class _Stop(Exception):
        pass

    def fake_process_transfer(send_process, dest, receive_process, is_ssh, **kw):
        captured["compress"] = kw.get("compress")
        raise _Stop

    monkeypatch.setattr(operations, "_do_process_transfer", fake_process_transfer)

    # A non-raw destination (plain object, NOT a RawEndpoint instance).
    class _NotRaw:
        _is_remote = False
        config = {"path": "/dest"}

    snap = Mock()
    snap.get_path.return_value = "/x/snap"
    snap.__str__ = lambda self: "root.20240101T120000"
    snap.endpoint.send.return_value = Mock()

    with pytest.raises(_Stop):
        operations.send_snapshot(
            snap, _NotRaw(), options={"compress": "zstd", "show_progress": False}
        )
    assert captured["compress"] == "zstd"  # preserved for non-raw


def test_ssh_raw_receive_pipeline_is_compress_then_encrypt(monkeypatch):
    """raw+ssh (SSHRawEndpoint) must build the write pipeline compress-THEN-encrypt so
    restore's decrypt-then-decompress reverses it, and it inherits the compress path
    so the sidecar records it."""
    from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "x")
    ep = SSHRawEndpoint(
        config={
            "path": "/backup",
            "hostname": "nas",
            "compress": "zstd",
            "encrypt": "openssl_enc",
        }
    )
    assert ep.compress == "zstd"  # normalized/kept, will be recorded in the sidecar
    stages = ep._build_receive_pipeline(Path("/backup/x.btrfs.zst.enc"))
    # Find the compress and encrypt stages; compress must come first.
    prog_order = [s[0] for s in stages]
    assert "zstd" in prog_order and "openssl" in prog_order
    assert prog_order.index("zstd") < prog_order.index("openssl")
