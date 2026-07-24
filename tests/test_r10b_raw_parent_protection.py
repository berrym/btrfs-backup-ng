"""R10b: raw incremental-parent protection.

A raw backup is a ``btrfs send`` stream file; an incremental child stream cannot be applied
without its parent stream. Time-based retention must therefore never delete a stream that a KEPT
stream still needs as a parent (that would silently make the child unrestorable). btrfs targets
are unaffected -- a pruned parent there only forces the next backup to be a full send.
"""

from __future__ import annotations

from pathlib import Path

from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def _snap(name: str, parent: str | None = None, tmp: Path = Path("/b")) -> RawSnapshot:
    return RawSnapshot(name=name, stream_path=tmp / f"{name}.btrfs", parent_name=parent)


def _raw(tmp_path: Path) -> RawEndpoint:
    return RawEndpoint(config={"path": str(tmp_path)})


# --------------------------------------------------------------------------- #
# Part 1: prune-level protect_incremental_parents (chain walk)
# --------------------------------------------------------------------------- #
def test_protect_rescues_kept_childs_parent_chain(tmp_path):
    """A kept incremental child rescues its ENTIRE parent chain from to_delete (transitive), so a
    raw child stream can never be orphaned. Mutation guard: the btrfs no-op (return unchanged)
    leaves the parents in to_delete."""
    ep = _raw(tmp_path)
    s1 = _snap("cfg-1-x")  # root (full)
    s2 = _snap("cfg-2-x", parent="cfg-1-x")
    s3 = _snap("cfg-3-x", parent="cfg-2-x")
    keep, delete = ep.protect_incremental_parents([s3], [s1, s2])
    assert {s.get_name() for s in keep} == {"cfg-1-x", "cfg-2-x", "cfg-3-x"}
    assert delete == []


def test_protect_allows_whole_chain_delete(tmp_path):
    """When NOTHING in a chain is kept, the whole chain is deletable -- no false protection.
    Mutation guard: protecting parents regardless of the kept set leaks the streams forever."""
    ep = _raw(tmp_path)
    s1, s2, s3 = (
        _snap("cfg-1-x"),
        _snap("cfg-2-x", parent="cfg-1-x"),
        _snap("cfg-3-x", parent="cfg-2-x"),
    )
    keep, delete = ep.protect_incremental_parents([], [s1, s2, s3])
    assert keep == []
    assert {s.get_name() for s in delete} == {"cfg-1-x", "cfg-2-x", "cfg-3-x"}


def test_protect_kept_child_missing_parent_warns_no_crash(tmp_path):
    """A kept stream whose parent is not present (chain already broken upstream) does not crash
    and fabricates nothing -- it is left exactly as the time-based decision placed it."""
    ep = _raw(tmp_path)
    orphan = _snap("cfg-9-x", parent="cfg-8-gone")
    keep, delete = ep.protect_incremental_parents([orphan], [])
    assert [s.get_name() for s in keep] == ["cfg-9-x"]
    assert delete == []


def test_protect_legacy_null_parent_left_as_is(tmp_path):
    """Legacy streams with no recorded parent_name can't be chain-resolved; the time-based
    decision stands (a null-parent stream in to_delete is still deleted)."""
    ep = _raw(tmp_path)
    keep, delete = ep.protect_incremental_parents(
        [_snap("cfg-3-x", parent=None)], [_snap("cfg-1-x", parent=None)]
    )
    assert [s.get_name() for s in delete] == ["cfg-1-x"]


def test_protect_cycle_does_not_hang(tmp_path):
    """A pathological parent_name cycle is bounded by the visited-set guard (never infinite
    loops)."""
    ep = _raw(tmp_path)
    a = _snap("a", parent="b")
    b = _snap("b", parent="a")
    keep, delete = ep.protect_incremental_parents([a], [b])
    # b is a's parent -> rescued; the walk terminates on the cycle.
    assert {s.get_name() for s in keep} == {"a", "b"}


def test_btrfs_base_protect_is_noop():
    """The base Endpoint (btrfs) never adjusts the retention decision -- a pruned parent there is
    just a full re-send, not data loss. Mutation guard: any base-level protection changes it."""
    ep = object.__new__(Endpoint)  # no __init__ needed; method is stateless
    keep, delete = ["k"], ["d"]
    k, d = ep.protect_incremental_parents(keep, delete)
    assert k is keep and d is delete


# --------------------------------------------------------------------------- #
# Part 2: delete-primitive chain guard (defense-in-depth for any caller)
# --------------------------------------------------------------------------- #
def test_chain_guard_protects_survivor_referenced_parent(tmp_path):
    """The delete primitive refuses to delete a stream referenced as parent by a SURVIVING stream
    (not in the delete session) -- so any caller, not just prune, cannot orphan a child. Mutation
    guard: a session-blind or absent guard returns an empty protected set here."""
    ep = _raw(tmp_path)
    parent = _snap("cfg-1-x", tmp=tmp_path)
    child = _snap("cfg-2-x", parent="cfg-1-x", tmp=tmp_path)
    ep.list_snapshots = lambda flush_cache=False: [parent, child]  # type: ignore[method-assign]
    protected = ep._chain_referenced_parents([parent], delete_session={"cfg-1-x"})
    assert protected == {"cfg-1-x"}


def test_chain_guard_allows_whole_chain_via_session(tmp_path):
    """When the whole chain is in the delete session, no member is a surviving reference, so all
    are deletable. Mutation guard: ignoring delete_session over-protects and leaks the chain."""
    ep = _raw(tmp_path)
    parent = _snap("cfg-1-x", tmp=tmp_path)
    child = _snap("cfg-2-x", parent="cfg-1-x", tmp=tmp_path)
    ep.list_snapshots = lambda flush_cache=False: [parent, child]  # type: ignore[method-assign]
    protected = ep._chain_referenced_parents(
        [parent], delete_session={"cfg-1-x", "cfg-2-x"}
    )
    assert protected == set()


def test_delete_locked_skips_protected_parent_on_disk(tmp_path):
    """End-to-end on real files: _delete_snapshots_locked leaves a survivor-referenced parent's
    stream + sidecar on disk (skipped), and only removes an unreferenced target. Mutation guard:
    dropping the guard unlinks the parent and orphans the child."""
    ep = _raw(tmp_path)
    for n in ("cfg-1-x", "cfg-2-x", "cfg-9-x"):
        (tmp_path / f"{n}.btrfs").write_text("stream")
        (tmp_path / f"{n}.btrfs.meta").write_text("{}")
    parent = _snap("cfg-1-x", tmp=tmp_path)
    child = _snap("cfg-2-x", parent="cfg-1-x", tmp=tmp_path)
    unref = _snap("cfg-9-x", tmp=tmp_path)  # nobody's parent
    ep.list_snapshots = lambda flush_cache=False: [parent, child, unref]  # type: ignore[method-assign]

    # Try to delete the parent (needed by survivor child) and the unreferenced stream.
    ep._delete_snapshots_locked([parent, unref], delete_session={"cfg-1-x", "cfg-9-x"})

    assert (tmp_path / "cfg-1-x.btrfs").exists()  # protected -> kept
    assert not (tmp_path / "cfg-9-x.btrfs").exists()  # unreferenced -> deleted
