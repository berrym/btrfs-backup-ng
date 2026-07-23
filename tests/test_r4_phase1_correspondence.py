"""R4 Phase 1: the polymorphic ``endpoint.correspondent_of(snapshot)`` primitive.

Correspondence is the single authority for "does THIS endpoint hold the subvolume that
corresponds to that snapshot". For btrfs it is ``received_uuid == snapshot.uuid`` (what
incremental send/receive actually resolves on the destination); for raw it is name (a
self-contained stream, no received_uuid). Phase 1 is strictly NON-behavioral: the method
exists and is proven, but nothing in the planner or restore calls it yet (P2/P3 do).
"""

from __future__ import annotations

import inspect
import time
from pathlib import Path

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.local import LocalEndpoint


def _local(path):
    path.mkdir(parents=True, exist_ok=True)
    return LocalEndpoint(
        config={
            "path": path,
            "source": None,
            "snapshot_folder": ".snapshots",
            "snap_prefix": "home-",
        }
    )


def _snap(ep, path, stamp, uuid="", received_uuid=""):
    s = __util__.Snapshot(
        path,
        "home-",
        ep,
        time_obj=time.strptime(stamp, "%Y%m%d-%H%M%S"),
        time_format="%Y%m%d-%H%M%S",
    )
    s.uuid = uuid
    s.received_uuid = received_uuid
    return s


# --------------------------------------------------------------------------- #
# btrfs correspondence: by received_uuid, NOT name
# --------------------------------------------------------------------------- #
def test_correspondent_matches_by_received_uuid_not_name(tmp_path, monkeypatch):
    """A destination copy corresponds when its received_uuid == the source's uuid -- even
    if its on-disk name differs. Mutation guard: matching candidate.uuid (instead of
    received_uuid) or matching by name fails this."""
    dest = _local(tmp_path)
    d_match = _snap(
        dest, tmp_path, "20240109-000000", uuid="Ddiff", received_uuid="SRC-UUID"
    )
    d_other = _snap(dest, tmp_path, "20240102-000000", uuid="Do", received_uuid="ZZZ")
    monkeypatch.setattr(dest, "list_snapshots", lambda: [d_other, d_match])

    source = _snap(dest, tmp_path, "20240101-000000", uuid="SRC-UUID")
    assert dest.correspondent_of(source) is d_match  # by uuid, though names differ


def test_recreated_snapshot_same_name_new_uuid_is_not_present(tmp_path, monkeypatch):
    """THE R4 WIN: a re-created source snapshot reuses the name but has a NEW uuid, so it
    does NOT correspond to the destination's old copy -- returns None (correctly "not
    present"), instead of the name-collision a name match would wrongly report. Mutation
    guard: a name-based correspondent returns the stale copy and fails this."""
    dest = _local(tmp_path)
    old_copy = _snap(
        dest, tmp_path, "20240101-000000", uuid="Dx", received_uuid="OLD-UUID"
    )
    monkeypatch.setattr(dest, "list_snapshots", lambda: [old_copy])

    recreated = _snap(dest, tmp_path, "20240101-000000", uuid="NEW-UUID")
    assert dest.correspondent_of(recreated) is None


def test_correspondent_ignores_dest_with_matching_uuid_but_empty_received(
    tmp_path, monkeypatch
):
    """Correspondence is received_uuid==source.uuid, NOT uuid==source.uuid. A dest
    snapshot that merely shares the source's uuid (e.g. the source itself, or a reflink
    clone) but was never received (received_uuid empty) is NOT a corresponding copy.
    Mutation guard: matching on candidate.uuid returns it and fails this."""
    dest = _local(tmp_path)
    same_uuid_not_received = _snap(
        dest, tmp_path, "20240101-000000", uuid="SRC-UUID", received_uuid=""
    )
    monkeypatch.setattr(dest, "list_snapshots", lambda: [same_uuid_not_received])

    source = _snap(dest, tmp_path, "20240101-000000", uuid="SRC-UUID")
    assert dest.correspondent_of(source) is None


def test_correspondent_none_when_source_uuid_unknown(tmp_path, monkeypatch):
    """An un-enriched source (empty uuid) has no known identity -> None (safe: the planner
    will full-send). Mutation guard: dropping the empty-uuid guard could false-match a dest
    whose received_uuid is also empty."""
    dest = _local(tmp_path)
    d = _snap(dest, tmp_path, "20240101-000000", uuid="D", received_uuid="")
    monkeypatch.setattr(dest, "list_snapshots", lambda: [d])

    source = _snap(dest, tmp_path, "20240101-000000", uuid="")
    assert dest.correspondent_of(source) is None


def test_correspondent_none_when_no_match(tmp_path, monkeypatch):
    dest = _local(tmp_path)
    d = _snap(dest, tmp_path, "20240101-000000", uuid="D", received_uuid="OTHER")
    monkeypatch.setattr(dest, "list_snapshots", lambda: [d])
    source = _snap(dest, tmp_path, "20240101-000000", uuid="SRC")
    assert dest.correspondent_of(source) is None


# --------------------------------------------------------------------------- #
# Restore direction (backup_ep.correspondent_of(local)) == find_parent_by_uuid relation
# --------------------------------------------------------------------------- #
def test_correspondent_restore_direction_matches_find_parent_by_uuid(
    tmp_path, monkeypatch
):
    """restore's find_parent_by_uuid returns the backup whose received_uuid == a local
    snapshot's uuid. That relation is exactly ``backup_ep.correspondent_of(local)``.
    Mutation guard: any direction/field change breaks this equivalence."""
    backup_ep = _local(tmp_path)
    b_match = _snap(
        backup_ep, tmp_path, "20240101-000000", uuid="B1", received_uuid="LOCAL-UUID"
    )
    b_other = _snap(
        backup_ep, tmp_path, "20240102-000000", uuid="B2", received_uuid="OTHER"
    )
    monkeypatch.setattr(backup_ep, "list_snapshots", lambda: [b_match, b_other])

    local = _snap(backup_ep, tmp_path, "20240101-000000", uuid="LOCAL-UUID")
    assert backup_ep.correspondent_of(local) is b_match


# --------------------------------------------------------------------------- #
# raw correspondence: by name, uuid ignored
# --------------------------------------------------------------------------- #
def test_raw_correspondent_matches_by_name_ignoring_uuid(tmp_path):
    from btrfs_backup_ng.endpoint.raw import RawEndpoint
    from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

    raw = RawEndpoint(config={"path": str(tmp_path)})
    backup = RawSnapshot(
        name="home-20240101-000000",
        stream_path=Path(tmp_path) / "home-20240101-000000.btrfs",
        uuid="RAW-UUID",
    )
    raw.list_snapshots = lambda flush_cache=False: [backup]  # type: ignore[method-assign]

    # A source with a totally different uuid still corresponds by NAME on a raw target.
    source = _snap(raw, tmp_path, "20240101-000000", uuid="DIFFERENT-UUID")
    assert raw.correspondent_of(source) is backup

    # A different name -> no correspondence.
    other = _snap(raw, tmp_path, "20240202-000000", uuid="DIFFERENT-UUID")
    assert raw.correspondent_of(other) is None


# --------------------------------------------------------------------------- #
# Contract: correspondent_of NEVER raises -- None is always a safe answer
# --------------------------------------------------------------------------- #
def test_correspondent_returns_none_when_list_snapshots_raises(tmp_path, monkeypatch):
    """A failing list_snapshots (permission error, missing path, transient ssh error)
    must yield None, not crash -- the documented "None is always safe" contract. Mutation
    guard: removing the try/except lets the exception propagate."""
    dest = _local(tmp_path)

    def boom(*a, **k):
        raise PermissionError("cannot access destination")

    monkeypatch.setattr(dest, "list_snapshots", boom)
    source = _snap(dest, tmp_path, "20240101-000000", uuid="SRC")
    assert dest.correspondent_of(source) is None


def test_raw_correspondent_returns_none_when_list_snapshots_raises(
    tmp_path, monkeypatch
):
    from btrfs_backup_ng.endpoint.raw import RawEndpoint

    raw = RawEndpoint(config={"path": str(tmp_path)})

    def boom(*a, **k):
        raise OSError("io error")

    monkeypatch.setattr(raw, "list_snapshots", boom)
    source = _snap(raw, tmp_path, "20240101-000000", uuid="X")
    assert raw.correspondent_of(source) is None


def test_raw_correspondent_returns_none_when_get_name_raises(tmp_path):
    """The never-raises contract also covers a callable ``get_name`` that itself raises
    (malformed time tuple, etc.) -- it is inside the try, not before it. Mutation guard:
    moving ``get_name()`` back outside the try lets the exception escape and fails this."""
    from btrfs_backup_ng.endpoint.raw import RawEndpoint

    raw = RawEndpoint(config={"path": str(tmp_path)})
    raw.list_snapshots = lambda flush_cache=False: []  # type: ignore[method-assign]

    class _BadName:
        def get_name(self):
            raise ValueError("malformed time tuple")

    assert raw.correspondent_of(_BadName()) is None


def test_correspondent_none_for_arg_without_identity(tmp_path, monkeypatch):
    """An arg with no uuid attr (btrfs) or no get_name (raw) yields None, not AttributeError.
    Mutation guard: dropping the getattr guards raises."""
    dest = _local(tmp_path)
    monkeypatch.setattr(dest, "list_snapshots", lambda: [])
    assert dest.correspondent_of(object()) is None  # no .uuid

    from btrfs_backup_ng.endpoint.raw import RawEndpoint

    raw = RawEndpoint(config={"path": str(tmp_path)})
    raw.list_snapshots = lambda flush_cache=False: []  # type: ignore[method-assign]
    assert raw.correspondent_of(object()) is None  # no .get_name


# --------------------------------------------------------------------------- #
# Polymorphism + non-behavioral guard
# --------------------------------------------------------------------------- #
def test_polymorphism_btrfs_uses_base_raw_overrides():
    from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
    from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

    # btrfs endpoints inherit the base (received_uuid) semantics.
    assert SSHEndpoint.correspondent_of is Endpoint.correspondent_of
    assert LocalEndpoint.correspondent_of is Endpoint.correspondent_of
    # raw endpoints override with name semantics; ssh-raw inherits the raw override.
    assert RawEndpoint.correspondent_of is not Endpoint.correspondent_of
    assert SSHRawEndpoint.correspondent_of is RawEndpoint.correspondent_of


def test_phase_boundary_planner_wired_restore_not_yet():
    """Phase 2 wires the backup planner onto correspondent_of; restore is converged in
    Phase 3, not before. This guards the phase boundary -- a still-name-based restore is
    expected until P3."""
    import btrfs_backup_ng.core.planning as planning
    import btrfs_backup_ng.core.restore as restore

    assert "correspondent_of" in inspect.getsource(planning), (
        "P2 should wire the planner onto correspondent_of"
    )
    assert "correspondent_of" not in inspect.getsource(restore), (
        "restore converges onto correspondent_of in Phase 3, not now"
    )
