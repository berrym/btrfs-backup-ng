"""`restore --cleanup` must not delete a subvolume it cannot identify as ours.

The command runs long after the restore that left the debris, so it has no run
record to consult. It used to treat "this subvolume is empty" as grounds for
deletion, which is exactly what an operator's own freshly-created subvolume
looks like at a destination path.
"""

import subprocess
import types

import pytest

from btrfs_backup_ng.cli import restore as restore_cli


@pytest.fixture
def rig(tmp_path, monkeypatch):
    """A destination whose children all report as subvolumes, deleting nothing."""
    deleted: list[str] = []

    monkeypatch.setattr(
        restore_cli.__util__, "is_subvolume", lambda p: p.is_dir(), raising=True
    )
    monkeypatch.setattr("builtins.input", lambda *_: "y")

    def fake_run(cmd, *a, **kw):
        deleted.append(cmd[-1])
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    return tmp_path, deleted


def _cleanup(dest):
    args = types.SimpleNamespace(destination=str(dest), dry_run=False)
    return restore_cli._execute_cleanup(args)


def test_an_empty_subvolume_is_reported_not_deleted(rig, capsys):
    """The operator's own empty subvolume survives a cleanup sweep."""
    dest, deleted = rig
    (dest / "operators-own-work").mkdir()

    _cleanup(dest)

    assert deleted == [], f"cleanup deleted an unidentifiable subvolume: {deleted}"
    out = capsys.readouterr().out
    assert "operators-own-work" in out, "it was not even reported to the operator"
    assert "not identifiable as ours" in out


def test_a_partial_suffix_is_deleted(rig):
    """A name this tool writes itself is unambiguous and is still cleaned up."""
    dest, deleted = rig
    (dest / "home.20260101.partial").mkdir()
    (dest / "home.20260101.partial" / "payload").write_bytes(b"x")

    _cleanup(dest)

    assert deleted == [str(dest / "home.20260101.partial")]


def test_a_metadata_only_body_is_deleted(rig):
    """Nothing but our own metadata directory is equally unambiguous."""
    dest, deleted = rig
    victim = dest / "home.20260102"
    (victim / ".btrfs-backup-ng").mkdir(parents=True)

    _cleanup(dest)

    assert deleted == [str(victim)]


def test_an_unreadable_subvolume_is_never_deleted(rig, capsys, monkeypatch):
    """A subvolume we cannot inspect is not evidence of anything."""
    dest, deleted = rig
    (dest / "sealed").mkdir()

    real_iterdir = type(dest).iterdir

    def denied(self):
        if self.name == "sealed":
            raise PermissionError(13, "Permission denied")
        return real_iterdir(self)

    monkeypatch.setattr(type(dest), "iterdir", denied)

    _cleanup(dest)

    assert deleted == []
    assert "cannot be read" in capsys.readouterr().out
