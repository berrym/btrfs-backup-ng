"""A raw stream whose file is removed but whose .meta sidecar cannot be is
counted ONCE -- as deleted -- never both deleted and failed, and it leaves the
endpoint's cache without the removed stream.

Regression for the delete-accounting defect: the sidecar unlink sat in the same
``try`` as the stream unlink, so a sidecar ``OSError`` jumped to ``except`` and
called ``result.fail`` AFTER the stream unlink had already appended the snapshot
to ``result.deleted`` -- the same snapshot credited to ``deleted_count`` and
listed in ``failed`` -- and the cache drop in between was skipped.
"""

from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def _rig(tmp_path):
    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    snap = RawSnapshot(
        name="s.20240101T120000", stream_path=tmp_path / "s.20240101T120000.btrfs"
    )
    snap.stream_path.write_bytes(b"stream")
    snap.metadata_path.write_bytes(b"{}")
    endpoint._cached_snapshots = [snap]
    return endpoint, snap


def test_sidecar_unlink_failure_is_not_a_delete_failure(tmp_path, monkeypatch):
    """Stream removed, sidecar unlink raises: deleted once, not failed, cache cleared."""
    endpoint, snap = _rig(tmp_path)
    meta = snap.metadata_path
    real_unlink = type(meta).unlink

    def flaky_unlink(self, *args, **kwargs):
        if self == meta:
            raise OSError(1, "Operation not permitted")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(type(meta), "unlink", flaky_unlink)

    result = endpoint.delete_snapshots([snap])

    # The one defect being killed: never both.
    assert snap in result.deleted
    assert snap not in [s for s, _ in result.failed]
    assert result.deleted_count == 1
    assert result.failed_count == 0
    # The stream file (the backup) really is gone.
    assert not snap.stream_path.exists()
    # The cache no longer lists a stream whose file is gone.
    assert endpoint._cached_snapshots == []


def test_clean_delete_removes_both_files_and_counts_once(tmp_path):
    """The ordinary path still removes stream + sidecar and counts one deletion."""
    endpoint, snap = _rig(tmp_path)

    result = endpoint.delete_snapshots([snap])

    assert result.deleted_count == 1
    assert result.failed_count == 0
    assert not snap.stream_path.exists()
    assert not snap.metadata_path.exists()
    assert endpoint._cached_snapshots == []


def test_stream_unlink_failure_is_a_delete_failure_and_keeps_cache(
    tmp_path, monkeypatch
):
    """If the STREAM itself cannot be unlinked, that IS a failure and nothing is
    counted deleted; the cache entry stays (the stream is still there)."""
    endpoint, snap = _rig(tmp_path)
    stream = snap.stream_path
    real_unlink = type(stream).unlink

    def flaky_unlink(self, *args, **kwargs):
        if self == stream:
            raise OSError(1, "Operation not permitted")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(type(stream), "unlink", flaky_unlink)

    result = endpoint.delete_snapshots([snap])

    assert result.deleted_count == 0
    assert result.failed_count == 1
    assert endpoint._cached_snapshots == [snap]
