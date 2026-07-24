"""R4 Phase 3b-2: snapper backup identity converged onto correspondence.

Snapper no longer decides skip/parent by the snapper NUMBER (which snapper recycles after a
prune). Instead each snapper snapshot is wrapped as a uuid-enriched Snapshot and routed
through the shared planner, whose destination view resolves correspondence: received_uuid for
btrfs (via the ``.snapshots/{num}/snapshot`` numbered-layout enumeration), name for raw. A
recycled number gets a NEW uuid, so it is correctly "absent" and re-sent -- the R4 win.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.core.operations as ops
from btrfs_backup_ng import __util__


def _wrapper(name, uuid):
    """A source-snapshot stand-in as the planner/correspondent_of see it."""
    s = MagicMock()
    s.get_name.return_value = name
    s.uuid = uuid
    s.received_uuid = ""
    return s


# --------------------------------------------------------------------------- #
# btrfs snapper dest view: correspondence by received_uuid (number-agnostic)
# --------------------------------------------------------------------------- #
def test_btrfs_dest_view_corresponds_by_received_uuid(monkeypatch):
    """A source snapshot corresponds to a numbered dest backup whose received_uuid == its
    uuid -- regardless of the snapper number. Mutation guard: matching by number or name
    fails this."""
    monkeypatch.setattr(
        ops,
        "_enumerate_snapper_btrfs_backups",
        lambda ep: [ops._SnapperBtrfsBackup(7, "U-SEVEN")],
    )
    view = ops._SnapperBtrfsDestView(MagicMock())

    present = _wrapper("root-7-20240101-000000", "U-SEVEN")
    assert view.correspondent_of(present) is not None
    absent = _wrapper("root-9-20240102-000000", "U-NINE")
    assert view.correspondent_of(absent) is None


def test_btrfs_dest_view_recycled_number_new_uuid_is_absent(monkeypatch):
    """THE R4 WIN for snapper: snapper reused number 7 for a NEW snapshot (new uuid); the
    destination still holds the OLD number-7 backup (its old received_uuid). Correspondence
    correctly reports the new snapshot ABSENT -> it is re-sent, instead of being skipped by a
    number coincidence. Mutation guard: number-based identity skips it."""
    monkeypatch.setattr(
        ops,
        "_enumerate_snapper_btrfs_backups",
        lambda ep: [ops._SnapperBtrfsBackup(7, "U-OLD-SEVEN")],
    )
    view = ops._SnapperBtrfsDestView(MagicMock())

    recreated = _wrapper(
        "root-7-20240105-000000", "U-NEW-SEVEN"
    )  # same number, new uuid
    assert view.correspondent_of(recreated) is None


def test_btrfs_dest_view_empty_uuid_source_is_absent(monkeypatch):
    """An un-enriched wrapper (empty uuid) cannot be verified present -> None (re-send)."""
    monkeypatch.setattr(
        ops,
        "_enumerate_snapper_btrfs_backups",
        lambda ep: [ops._SnapperBtrfsBackup(1, "U1")],
    )
    view = ops._SnapperBtrfsDestView(MagicMock())
    assert view.correspondent_of(_wrapper("root-1-x", "")) is None


# --------------------------------------------------------------------------- #
# dest-view factory: raw uses the real endpoint (name), btrfs uses the view
# --------------------------------------------------------------------------- #
def test_snapper_dest_view_raw_uses_endpoint_directly(tmp_path):
    """A raw target already enumerates its snapper backups by sidecar name, so the real
    endpoint is the view (name correspondence works directly)."""
    from btrfs_backup_ng.endpoint.raw import RawEndpoint

    raw = RawEndpoint(config={"path": str(tmp_path)})
    assert ops._snapper_dest_view(raw) is raw


def test_snapper_dest_view_btrfs_builds_numbered_view(monkeypatch):
    monkeypatch.setattr(ops, "_enumerate_snapper_btrfs_backups", lambda ep: [])
    view = ops._snapper_dest_view(MagicMock())
    assert isinstance(view, ops._SnapperBtrfsDestView)


def test_raw_dest_view_name_correspondence(tmp_path):
    """Raw correspondence is by NAME (the backup name {config}-{num}-{date}), uuid ignored."""
    from btrfs_backup_ng.endpoint.raw import RawEndpoint
    from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

    raw = RawEndpoint(config={"path": str(tmp_path)})
    backup = RawSnapshot(
        name="root-7-20240101-000000",
        stream_path=tmp_path / "root-7-20240101-000000.btrfs",
        uuid="RAW-UUID",
    )
    raw.list_snapshots = lambda flush_cache=False: [backup]  # type: ignore[method-assign]

    # Same NAME, totally different uuid -> corresponds on raw (name semantics).
    assert (
        raw.correspondent_of(_wrapper("root-7-20240101-000000", "DIFFERENT")) is backup
    )
    assert raw.correspondent_of(_wrapper("root-9-20240102-000000", "X")) is None


# --------------------------------------------------------------------------- #
# Enumeration: single privileged shell pass parsing (findings: stdout/sudo/O(N))
# --------------------------------------------------------------------------- #
def test_enumerate_parses_privileged_shell_output(monkeypatch):
    """_enumerate_snapper_btrfs_backups parses '<num> <received_uuid>' lines; blank/malformed/
    non-numeric lines are skipped. Mutation guard: dropping the isdigit/len check mis-parses."""
    monkeypatch.setattr(
        ops,
        "_snapper_run_shell",
        lambda ep, script: (0, "1 U-ONE\n2 U-TWO\n\nbad\n3\n"),
    )
    backups = ops._enumerate_snapper_btrfs_backups(MagicMock(config={"path": "/b"}))
    assert [(b.number, b.received_uuid) for b in backups] == [
        (1, "U-ONE"),
        (2, "U-TWO"),
    ]


def test_enumerate_degrades_to_empty_on_shell_failure(monkeypatch):
    """A non-zero shell return (ssh blip / permission / timeout) degrades to [] -- never raises;
    the sources then re-send (safe). Mutation guard: returning partial/raising fails this."""
    monkeypatch.setattr(ops, "_snapper_run_shell", lambda ep, script: (1, "1 U1\n"))
    assert ops._enumerate_snapper_btrfs_backups(MagicMock(config={"path": "/b"})) == []


def test_run_shell_remote_passes_script_as_single_quoted_arg():
    """Over ssh the privileged script MUST reach the remote ``sh -c`` as ONE ``shlex.quote``-d
    argument. ssh joins the remote argv with spaces and the remote shell re-splits it, so an
    UNQUOTED multi-word script is torn apart -- ``sudo`` then binds only its first token and the
    real work (enumeration / publish / stale cleanup) runs UNPRIVILEGED. This was a real bug the
    mocked tests missed and only ssh hardware caught. Mutation guard: passing the raw script
    (drop shlex.quote) fails this."""
    import shlex

    ep = MagicMock()
    ep._is_remote = True
    captured = {}

    def fake_exec(cmd, **kwargs):
        if "sh" in cmd and "-c" in cmd:  # the real script call, not the sudo probe
            captured["cmd"] = list(cmd)
        return MagicMock(returncode=0, stdout=b"")

    ep._exec_remote_command = fake_exec
    # A realistic multi-word script (spaces, quotes, ';', '$n', globs) -- the exact shape that
    # gets shredded without quoting.
    script = (
        'set -e; for n in */; do btrfs subvolume delete "$n/snapshot" || true; done'
    )
    ops._snapper_run_shell(ep, script)

    assert captured["cmd"][:4] == ["sudo", "-n", "sh", "-c"]
    assert captured["cmd"][4] == shlex.quote(
        script
    )  # single quoted token, not torn apart
    assert captured["cmd"][4] != script  # proves quoting actually happened


# --------------------------------------------------------------------------- #
# Publish: transactional slot swap is data-loss-safe (never rm the published slot)
# --------------------------------------------------------------------------- #
def test_publish_moves_stale_aside_never_rm_final(monkeypatch):
    """Publishing into an OCCUPIED slot (recycled number) moves the stale backup ASIDE before
    deleting and moves the new subvol in -- the published .snapshots/{num}/snapshot is never
    rm -rf'd, so there is no data-loss window. Mutation guard: an rm of the final slot, or
    deleting the stale before the new is in place, fails this."""
    captured = {}

    def fake_shell(ep, script):
        captured["s"] = script
        return (0, "")

    monkeypatch.setattr(ops, "_snapper_run_shell", fake_shell)
    ops._snapper_publish_slot(MagicMock(config={"path": "/b"}), 7)
    s = captured["s"]
    # Swap by renaming the containing DIRECTORY (a read-only received subvol can't be mv'd,
    # and dir-rename preserves received_uuid). Occupied slot moved aside, new dir moved in.
    assert "mv /b/.snapshots/7 /b/.snapshots/7.stale" in s  # occupied slot aside first
    assert "mv /b/.snapshots/7.incoming /b/.snapshots/7" in s  # new backup published
    # The published subvol is only ever deleted via the moved-aside .stale -- never in place.
    assert "btrfs subvolume delete /b/.snapshots/7/snapshot" not in s
    assert "btrfs subvolume delete /b/.snapshots/7.stale/snapshot" in s
    # Ordering: the stale is moved aside BEFORE the new one is moved in.
    assert s.index("mv /b/.snapshots/7 /b/.snapshots/7.stale") < s.index(
        "mv /b/.snapshots/7.incoming /b/.snapshots/7"
    )


def test_publish_raises_on_failure(monkeypatch):
    monkeypatch.setattr(ops, "_snapper_run_shell", lambda ep, script: (1, ""))
    with pytest.raises(__util__.SnapshotTransferError):
        ops._snapper_publish_slot(MagicMock(config={"path": "/b"}), 7)


def _run_real_shell(ep, script):
    """Execute the publish script for REAL against plain temp dirs (no sudo/btrfs needed:
    ``btrfs subvolume delete ... || true`` is a harmless no-op on plain dirs, and mv/rm do the
    real work). Lets the behavioral tests below observe on-disk outcomes, not string patterns."""
    import subprocess

    r = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return r.returncode, r.stdout


def test_publish_missing_incoming_never_disturbs_occupied_slot(tmp_path, monkeypatch):
    """THE data-loss guard: if the receive produced no ``.incoming/snapshot`` (a receive that
    reported success but wrote nothing), publish must abort WITHOUT touching the occupied slot --
    the pre-existing good backup stays exactly where it is. Mutation guard: the old "rm -rf
    .stale then move the slot aside" ordering moves the good backup out and, when the incoming
    move fails, leaves the slot EMPTY -- this test then finds no marker and fails."""
    snaps = tmp_path / ".snapshots"
    (snaps / "7" / "snapshot").mkdir(parents=True)
    (snaps / "7" / "snapshot" / "marker").write_text("OLD")
    # No .incoming at all -- the receive produced nothing.
    monkeypatch.setattr(ops, "_snapper_run_shell", _run_real_shell)

    with pytest.raises(__util__.SnapshotTransferError):
        ops._snapper_publish_slot(MagicMock(config={"path": str(tmp_path)}), 7)

    # The pre-existing good backup is UNTOUCHED (guard aborted before the aside).
    assert (snaps / "7" / "snapshot" / "marker").read_text() == "OLD"
    assert not (snaps / "7.stale").exists()  # slot never moved aside


def test_publish_recovers_stranded_stale_into_empty_slot(tmp_path, monkeypatch):
    """RECOVERY path: a prior publish crashed after moving the old backup aside -> the slot is
    EMPTY and ``.stale`` holds the only copy. The next publish (with a fresh incoming) must
    restore the stranded backup rather than blindly deleting it, then publish the new one --
    ending with the NEW backup in place and no orphaned temp dirs, exit 0. Mutation guard:
    breaking the recovery ``else mv .stale ->`` branch aborts under ``set -e`` (rc!=0 -> raises),
    or leaves an orphaned .stale -- either fails this."""
    snaps = tmp_path / ".snapshots"
    # Simulated post-crash state: slot 7 empty, old backup stranded in .stale.
    (snaps / "7.stale" / "snapshot").mkdir(parents=True)
    (snaps / "7.stale" / "snapshot" / "marker").write_text("OLD")
    # A fresh receive landed the new backup in .incoming.
    (snaps / "7.incoming" / "snapshot").mkdir(parents=True)
    (snaps / "7.incoming" / "snapshot" / "marker").write_text("NEW")
    monkeypatch.setattr(ops, "_snapper_run_shell", _run_real_shell)

    ops._snapper_publish_slot(MagicMock(config={"path": str(tmp_path)}), 7)

    # New backup published into the slot; recovery consumed the stranded copy cleanly.
    assert (snaps / "7" / "snapshot" / "marker").read_text() == "NEW"
    assert not (snaps / "7.stale").exists()
    assert not (snaps / "7.incoming").exists()


def test_publish_replaces_occupied_slot_end_to_end(tmp_path, monkeypatch):
    """Happy replacement of a recycled number end-to-end on a real fs: slot 7 holds OLD, a new
    receive is in .incoming -> after publish the slot holds NEW and no temp remains. Mutation
    guard: an rm of the final slot before the new one is in place loses the backup entirely."""
    snaps = tmp_path / ".snapshots"
    (snaps / "7" / "snapshot").mkdir(parents=True)
    (snaps / "7" / "snapshot" / "marker").write_text("OLD")
    (snaps / "7.incoming" / "snapshot").mkdir(parents=True)
    (snaps / "7.incoming" / "snapshot" / "marker").write_text("NEW")
    monkeypatch.setattr(ops, "_snapper_run_shell", _run_real_shell)

    ops._snapper_publish_slot(MagicMock(config={"path": str(tmp_path)}), 7)

    assert (snaps / "7" / "snapshot" / "marker").read_text() == "NEW"
    assert not (snaps / "7.stale").exists()
    assert not (snaps / "7.incoming").exists()


# --------------------------------------------------------------------------- #
# sync_snapper_snapshots: within-run failed-parent short-circuit (R1 safety)
# --------------------------------------------------------------------------- #
def test_sync_snapper_failed_in_run_parent_short_circuits_dependents(monkeypatch):
    """A snapper child whose in-run parent's transfer FAILED is short-circuited (recorded
    failed, never sent) -- so a snapper->raw target cannot commit a false-success stream.
    Mutation guard: removing the short-circuit sends the dependents against a missing parent."""
    snaps = []
    for n in (1, 2, 3):
        s = MagicMock()
        s.number = n
        snaps.append(s)
    monkeypatch.setattr(
        ops, "get_snapper_snapshots_for_backup", lambda *a, **k: list(snaps)
    )

    def _wrap(s, dest=None):
        w = MagicMock()
        w.get_name.return_value = f"b{s.number}"
        return w

    monkeypatch.setattr(ops, "_create_snapper_snapshot_wrapper", _wrap)
    monkeypatch.setattr(ops, "_snapper_dest_view", lambda dest: MagicMock())

    def fake_plan(wrappers, view, **k):
        byn = {w.get_name(): w for w in wrappers}
        return [(byn["b1"], None), (byn["b2"], byn["b1"]), (byn["b3"], byn["b2"])]

    monkeypatch.setattr(
        "btrfs_backup_ng.core.planning.plan_transfer_sequence", fake_plan
    )

    attempted = []

    def fake_send(snap, dest, parent_snapper_snapshot=None, options=None):
        attempted.append(snap.number)
        if snap.number == 1:
            raise __util__.SnapshotTransferError("boom")

    monkeypatch.setattr(ops, "send_snapper_snapshot", fake_send)

    with pytest.raises(__util__.SnapshotTransferError) as ei:
        ops.sync_snapper_snapshots(MagicMock(), "root", MagicMock())

    assert attempted == [1]  # s2 and s3 never attempted (parent chain failed)
    assert ei.value.result.failed_count == 3
    assert ei.value.result.transferred_count == 0


# --------------------------------------------------------------------------- #
# sync_snapper_snapshots: end-to-end with the REAL planner (correspondence skip)
# --------------------------------------------------------------------------- #
def test_sync_snapper_skips_present_via_real_planner(monkeypatch):
    """Drive sync_snapper_snapshots through the REAL plan_transfer_sequence + a real-semantics
    dest view: a snapper snapshot already present (its uuid corresponds to a dest backup's
    received_uuid) is SKIPPED (not sent); an absent one IS sent. Mutation guard: a number-based
    skip would send/skip the wrong one."""
    present = MagicMock()
    present.number = 1
    absent = MagicMock()
    absent.number = 2
    monkeypatch.setattr(
        ops, "get_snapper_snapshots_for_backup", lambda *a, **k: [present, absent]
    )

    # Wrappers carry a real uuid + name + comparable time_obj (planner sorts by it).
    def _wrap(s, dest=None):
        w = MagicMock()
        w.get_name.return_value = f"cfg-{s.number}-x"
        w.uuid = f"U{s.number}"
        w.received_uuid = ""
        w.time_obj = (2024, 1, s.number, 0, 0, 0, 0, 0, 0)
        return w

    monkeypatch.setattr(ops, "_create_snapper_snapshot_wrapper", _wrap)
    # Dest already holds a backup corresponding to snapshot 1 (received_uuid == U1).
    monkeypatch.setattr(
        ops,
        "_enumerate_snapper_btrfs_backups",
        lambda ep: [ops._SnapperBtrfsBackup(1, "U1")],
    )
    monkeypatch.setattr(
        ops, "_snapper_dest_view", lambda dest: ops._SnapperBtrfsDestView(MagicMock())
    )

    sent = []
    monkeypatch.setattr(
        ops,
        "send_snapper_snapshot",
        lambda snap, dest, parent_snapper_snapshot=None, options=None: sent.append(
            snap.number
        ),
    )

    count = ops.sync_snapper_snapshots(MagicMock(), "cfg", MagicMock())
    assert sent == [2]  # only the absent snapshot is sent; #1 skipped by correspondence
    assert count == 1
