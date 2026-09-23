"""A snapper backup to a btrfs target makes its receive slot below the target.

Reproduced on real btrfs after 0.9.7: `btrfs-backup-ng snapper backup <cfg>
<existing dir>` failed with "Destination <dir>/.snapshots/1.incoming does not
exist ... Nothing was created", exit 1. The slot had only ever existed
because the transfer engine and every endpoint ran `mkdir -p` on whatever
path the receive was pointed at, and the snapper flow points it at the slot;
removing those creation sites (#102) removed the slot with them.

The slot is the snapper flow's own transactional temp, so the flow creates
it: explicitly, one component at a time, below a target that must already
exist. No `-p` anywhere, so a missing target is refused, never rebuilt.
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import operations as ops


def _real_shell(ep, script):
    r = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return r.returncode, r.stdout


@pytest.fixture
def real_shell(monkeypatch):
    monkeypatch.setattr(ops, "_snapper_run_shell", _real_shell)


class TestTheSlotIsCreatedBelowAnExistingTarget:
    def test_the_slot_appears_under_the_target(self, tmp_path, real_shell):
        ops._snapper_prepare_slot(MagicMock(config={"path": str(tmp_path)}), 7)
        assert (tmp_path / ".snapshots" / "7.incoming").is_dir()

    def test_it_is_idempotent(self, tmp_path, real_shell):
        (tmp_path / ".snapshots" / "7.incoming").mkdir(parents=True)
        ops._snapper_prepare_slot(MagicMock(config={"path": str(tmp_path)}), 7)
        assert (tmp_path / ".snapshots" / "7.incoming").is_dir()

    def test_a_missing_target_is_refused_and_nothing_is_created(
        self, tmp_path, real_shell
    ):
        """Mutation guard: `mkdir -p` on the slot rebuilds the target here."""
        target = tmp_path / "unmounted" / "backups"
        with pytest.raises(__util__.SnapshotTransferError) as e:
            ops._snapper_prepare_slot(MagicMock(config={"path": str(target)}), 7)
        assert "Nothing was created" in str(e.value)
        assert not (tmp_path / "unmounted").exists()

    def test_the_script_never_says_mkdir_dash_p(self, monkeypatch):
        captured = {}

        def capture(ep, script):
            captured["s"] = script
            return 0, ""

        monkeypatch.setattr(ops, "_snapper_run_shell", capture)
        ops._snapper_prepare_slot(MagicMock(config={"path": "/b"}), 7)
        assert "mkdir -p" not in captured["s"]
        assert "[ -d /b ]" in captured["s"]

    def test_a_target_that_cannot_be_written_is_its_own_failure(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(ops, "_snapper_run_shell", lambda ep, s: (1, ""))
        with pytest.raises(__util__.SnapshotTransferError, match="receive slot"):
            ops._snapper_prepare_slot(MagicMock(config={"path": str(tmp_path)}), 7)


class TestTheSendUsesTheSlot:
    def test_send_snapper_snapshot_makes_the_slot_before_sending(
        self, tmp_path, monkeypatch, real_shell
    ):
        """The wiring: by the time send_snapshot runs, the endpoint is pointed
        at a slot that exists. Before the fix nothing created it and the
        engine's own check refused the missing directory."""
        from btrfs_backup_ng.endpoint.local import LocalEndpoint

        seen = {}

        def fake_send(snapshot, destination_endpoint, parent=None, options=None):
            path = destination_endpoint.config["path"]
            seen["path"] = str(path)
            seen["existed"] = __import__("pathlib").Path(path).is_dir()

        monkeypatch.setattr(ops, "send_snapshot", fake_send)
        monkeypatch.setattr(ops, "_snapper_publish_slot", lambda ep, n: None)
        monkeypatch.setattr(ops, "_write_info_xml", lambda ep, d, c: None)
        monkeypatch.setattr(ops, "_write_snapper_metadata", lambda *a, **k: None)
        monkeypatch.setattr(ops, "log_transaction", lambda **k: None)
        monkeypatch.setattr(
            ops, "_create_snapper_snapshot_wrapper", lambda snap, ep=None: MagicMock()
        )

        snap = MagicMock()
        snap.number = 3
        snap.subvolume_path = tmp_path / "src" / ".snapshots" / "3" / "snapshot"
        dest = LocalEndpoint(config={"path": str(tmp_path / "dst"), "snap_prefix": ""})
        (tmp_path / "dst").mkdir()

        ops.send_snapper_snapshot(snap, dest)

        assert seen["path"] == str(tmp_path / "dst" / ".snapshots" / "3.incoming")
        assert seen["existed"], "send_snapshot ran against a slot that did not exist"
        assert str(dest.config["path"]) == str(tmp_path / "dst"), "path not restored"

    def test_a_missing_target_is_refused_before_any_lock_is_taken(
        self, tmp_path, monkeypatch, real_shell
    ):
        """The slot lock lives under the target. Taken first, a missing
        target reported as a lock directory that "could not be created";
        the engine's own existence check now runs before it, with the
        missing-path diagnosis. Mutation guard: drop that call and the lock
        is reached."""
        from btrfs_backup_ng.endpoint.local import LocalEndpoint

        monkeypatch.setattr(
            ops,
            "_receiving_lock",
            lambda *a, **k: pytest.fail("a lock was taken under a missing target"),
        )
        monkeypatch.setattr(ops, "log_transaction", lambda **k: None)
        monkeypatch.setattr(
            ops, "_create_snapper_snapshot_wrapper", lambda snap, ep=None: MagicMock()
        )
        snap = MagicMock()
        snap.number = 3
        snap.subvolume_path = tmp_path / "src" / ".snapshots" / "3" / "snapshot"
        dest = LocalEndpoint(
            config={"path": str(tmp_path / "unmounted" / "dst"), "snap_prefix": ""}
        )
        with pytest.raises(__util__.SnapshotTransferError, match="Nothing was created"):
            ops.send_snapper_snapshot(snap, dest)
        assert not (tmp_path / "unmounted").exists()
