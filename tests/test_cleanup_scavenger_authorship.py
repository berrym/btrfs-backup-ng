"""`restore --cleanup` deletes what its own run marker names, and only that.

The command runs long after the restore that left the debris. Its authorship
record is the marker the plain layout writes under
``DEST/.btrfs-backup-ng/restore/`` for each receive in flight: a marker that
outlived its process, naming a subvolume that has no received_uuid, is an
interrupted restore this tool made. Nothing else is deleted. An empty
subvolume under no marker is what an operator's own ``btrfs subvolume
create`` looks like, and it used to be deleted on emptiness alone; a marker
over a complete received copy is a stale marker, not debris; a marker whose
restore is still running is left alone.
"""

from __future__ import annotations

import json
import os
import subprocess
import types

import pytest

from btrfs_backup_ng.core.layout import marker_dir

from .interrupted_restore import REAL_RUN, build_rig, dead_pid, mark, run_cleanup


@pytest.fixture
def rig(tmp_path, monkeypatch):
    """A destination whose directories all read as subvolumes, an identity
    table the test fills, deletions recorded and never performed."""
    return build_rig(tmp_path, monkeypatch)


def test_an_unmarked_empty_subvolume_is_reported_not_deleted(rig, capsys):
    """The operator's own empty subvolume survives a cleanup sweep."""
    (rig.dest / "operators-own-work").mkdir()

    rc = run_cleanup(rig.dest)

    assert rc == 0
    assert rig.deleted == [], (
        f"cleanup deleted an unidentifiable subvolume: {rig.deleted}"
    )
    out = capsys.readouterr().out
    assert "operators-own-work" in out, "it was not even reported to the operator"
    assert "not identifiable as ours" in out
    assert "No interrupted restores found." in out


def test_a_metadata_only_body_is_no_longer_evidence(rig, capsys):
    """A subvolume holding only a .btrfs-backup-ng directory used to be
    deleted as unambiguously ours. It is a restore destination somebody
    made a subvolume, as likely as anything else; without a marker it is
    reported and left."""
    victim = rig.dest / "home.20260102"
    (victim / ".btrfs-backup-ng").mkdir(parents=True)

    run_cleanup(rig.dest)

    assert rig.deleted == []
    assert "home.20260102" in capsys.readouterr().out


def test_a_stale_marker_over_a_bare_subvolume_is_deleted_and_the_marker_removed(
    rig, capsys
):
    """THE case this command exists for: a receive killed mid-stream leaves a
    subvolume with no received_uuid under the name, and the marker names it."""
    partial = rig.dest / "home.20260101"
    partial.mkdir()
    (partial / "payload").write_bytes(b"x")  # a truncated receive holds real files
    marker = mark(rig.dest, "home.20260101", dead_pid())
    rig.identities[str(partial)] = {"uuid": "x", "received_uuid": ""}

    rc = run_cleanup(rig.dest)

    assert rc == 0
    assert rig.deleted == [str(partial)]
    assert not marker.exists(), "the marker outlived the deletion it authorised"
    out = capsys.readouterr().out
    assert "1 deleted, 0 failed" in out


def test_a_complete_copy_under_a_stale_marker_is_left_and_the_marker_dropped(
    rig, capsys
):
    """A restore killed AFTER its receive completed but before the marker came
    off: the copy carries a received_uuid, so it is a copy, not debris."""
    copy = rig.dest / "home.20260101"
    copy.mkdir()
    marker = mark(rig.dest, "home.20260101", dead_pid())
    rig.identities[str(copy)] = {"uuid": "x", "received_uuid": "R"}

    run_cleanup(rig.dest)

    assert rig.deleted == []
    assert not marker.exists(), "a stale marker over a complete copy was kept"
    assert "complete received copy" in capsys.readouterr().out


def test_a_marker_whose_restore_is_still_running_is_left_alone(rig, capsys):
    partial = rig.dest / "home.20260101"
    partial.mkdir()
    marker = mark(rig.dest, "home.20260101", os.getpid())
    rig.identities[str(partial)] = {"uuid": "x", "received_uuid": ""}

    run_cleanup(rig.dest)

    assert rig.deleted == []
    assert marker.exists()
    assert "still running" in capsys.readouterr().out


def test_an_identity_that_cannot_be_read_never_authorises_a_deletion(rig, capsys):
    partial = rig.dest / "home.20260101"
    partial.mkdir()
    marker = mark(rig.dest, "home.20260101", dead_pid())
    # identities has no entry: the probe answers None.

    run_cleanup(rig.dest)

    assert rig.deleted == []
    assert marker.exists()
    assert "could not be read" in capsys.readouterr().out


def test_a_marker_naming_a_path_outside_the_destination_is_refused(rig, capsys):
    elsewhere = rig.dest.parent / "elsewhere"
    elsewhere.mkdir()
    directory = marker_dir(rig.dest)
    directory.mkdir(parents=True)
    (directory / "s-001.json").write_text(
        json.dumps(
            {
                "format": 1,
                "pid": dead_pid(),
                "snapshot": "elsewhere",
                "path": str(elsewhere),
            }
        )
    )
    rig.identities[str(elsewhere)] = {"uuid": "x", "received_uuid": ""}

    run_cleanup(rig.dest)

    assert rig.deleted == []
    assert "not directly under this destination" in capsys.readouterr().out


def test_a_marker_with_nothing_at_its_path_is_simply_removed(rig, capsys):
    marker = mark(rig.dest, "home.20260101", dead_pid())

    rc = run_cleanup(rig.dest)

    assert rc == 0 and rig.deleted == []
    assert not marker.exists()
    assert "nothing is at the path" in capsys.readouterr().out


def test_an_unreadable_unmarked_subvolume_is_never_deleted(rig, capsys, monkeypatch):
    (rig.dest / "sealed").mkdir()
    real_iterdir = type(rig.dest).iterdir

    def denied(self):
        if self.name == "sealed":
            raise PermissionError(13, "Permission denied")
        return real_iterdir(self)

    monkeypatch.setattr(type(rig.dest), "iterdir", denied)

    run_cleanup(rig.dest)

    assert rig.deleted == []
    assert "cannot be read" in capsys.readouterr().out


def test_a_dry_run_names_the_abandoned_restore_and_deletes_nothing(rig, capsys):
    partial = rig.dest / "home.20260101"
    partial.mkdir()
    marker = mark(rig.dest, "home.20260101", dead_pid())
    rig.identities[str(partial)] = {"uuid": "x", "received_uuid": ""}

    rc = run_cleanup(rig.dest, dry_run=True)

    assert rc == 0 and rig.deleted == [] and marker.exists()
    out = capsys.readouterr().out
    assert "home.20260101" in out and "Dry run" in out


def test_a_refused_confirmation_deletes_nothing(rig, capsys, monkeypatch):
    partial = rig.dest / "home.20260101"
    partial.mkdir()
    mark(rig.dest, "home.20260101", dead_pid())
    rig.identities[str(partial)] = {"uuid": "x", "received_uuid": ""}
    monkeypatch.setattr("builtins.input", lambda *_: "n")

    rc = run_cleanup(rig.dest)

    assert rc == 0 and rig.deleted == []
    assert "Cancelled" in capsys.readouterr().out


def test_a_deletion_that_fails_is_counted_and_keeps_the_marker(
    rig, capsys, monkeypatch
):
    partial = rig.dest / "home.20260101"
    partial.mkdir()
    marker = mark(rig.dest, "home.20260101", dead_pid())
    rig.identities[str(partial)] = {"uuid": "x", "received_uuid": ""}
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda cmd, *a, **kw: types.SimpleNamespace(
            returncode=1, stdout="", stderr="Operation not permitted"
        ),
    )

    rc = run_cleanup(rig.dest)

    assert rc == 1
    assert marker.exists()
    assert "0 deleted, 1 failed" in capsys.readouterr().out


def test_a_refused_sudo_deletes_nothing_and_keeps_the_marker(
    rig, capsys, monkeypatch, sudo_refuses
):
    """A non-root cleanup whose sudo refuses: the delete is counted as failed,
    the marker stays for the next attempt, and the exit status says so."""
    partial = rig.dest / "home.20260101"
    partial.mkdir()
    marker = mark(rig.dest, "home.20260101", dead_pid())
    rig.identities[str(partial)] = {"uuid": "x", "received_uuid": ""}
    monkeypatch.setattr(subprocess, "run", REAL_RUN)

    rc = run_cleanup(rig.dest)

    assert rc == 1
    assert marker.exists() and partial.is_dir()
    assert "0 deleted, 1 failed" in capsys.readouterr().out
    assert sudo_refuses() == [f"btrfs subvolume delete {partial}"]


def test_the_delete_is_btrfs_subvolume_delete_of_the_marked_path(rig, monkeypatch):
    partial = rig.dest / "home.20260101"
    partial.mkdir()
    mark(rig.dest, "home.20260101", dead_pid())
    rig.identities[str(partial)] = {"uuid": "x", "received_uuid": ""}
    argv: list = []

    def record(cmd, *a, **kw):
        argv.append(list(cmd))
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", record)
    run_cleanup(rig.dest)
    (cmd,) = argv
    assert cmd[-4:] == ["btrfs", "subvolume", "delete", str(partial)]


def test_a_marker_naming_the_parent_directory_is_refused(rig, capsys):
    # pathlib does not collapse "..": Path("DEST/..").parent == DEST and its
    # name is "..", so a marker saying snapshot=".." passed the "directly
    # under this destination" test and aimed a root deletion at DEST's parent.
    mark(rig.dest, "..", dead_pid())
    rig.identities[str(rig.dest / "..")] = {"received_uuid": ""}

    assert run_cleanup(rig.dest) == 0
    assert rig.deleted == []
    assert "not directly under this destination" in capsys.readouterr().out


def test_a_marker_naming_a_symlink_is_refused(rig, capsys, tmp_path_factory):
    # A symlink under the destination pointing at an ordinary subvolume
    # elsewhere: stat follows it, so without the check it read as an
    # abandoned subvolume and `btrfs subvolume delete` resolved it to the
    # target.
    elsewhere = tmp_path_factory.mktemp("elsewhere")
    (rig.dest / "p-1").symlink_to(elsewhere, target_is_directory=True)
    mark(rig.dest, "p-1", dead_pid())
    rig.identities[str(rig.dest / "p-1")] = {"received_uuid": ""}

    assert run_cleanup(rig.dest) == 0
    assert rig.deleted == []
    assert "names a symlink" in capsys.readouterr().out


def test_an_entry_that_changes_after_the_prompt_is_not_deleted(
    rig, caplog, monkeypatch, tmp_path_factory
):
    # Classified abandoned, then swapped for a symlink while the operator
    # reads the prompt: the deletion re-checks and refuses.
    (rig.dest / "p-1").mkdir()
    mark(rig.dest, "p-1", dead_pid())
    rig.identities[str(rig.dest / "p-1")] = {"received_uuid": ""}
    elsewhere = tmp_path_factory.mktemp("elsewhere")

    def swap_then_confirm(*_):
        (rig.dest / "p-1").rmdir()
        (rig.dest / "p-1").symlink_to(elsewhere, target_is_directory=True)
        return "y"

    monkeypatch.setattr("builtins.input", swap_then_confirm)

    with caplog.at_level("ERROR"):
        assert run_cleanup(rig.dest) == 1
    assert rig.deleted == []
    assert "changed since it was examined" in caplog.text
