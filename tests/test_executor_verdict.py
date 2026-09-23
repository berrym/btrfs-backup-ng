"""The executor's verdict on what a receive left, and what it takes from restore.

After ``btrfs receive`` exits 0 the engine used to have nothing more to say:
exit 0 was the whole verdict, and a subvolume under the right name that was
not the received copy -- an interrupted receive, something else entirely --
counted as a backup. ``artifact_verdict`` now records a tri-state answer in
the shape ``verify`` uses: ``ok`` when the copy's received_uuid is the
identity the stream carried, ``invalid`` when it provably is not (the
transfer fails and the artifact is cleaned under the authorship rule),
``unverifiable`` when the identity could not be READ (the data is kept and
the transfer counts; nothing is ever deleted on a check that could not run).

The executor also learns two things a restore through it will need: the id
its pins are taken under, and whether a failed transfer keeps its pin.

Where a process is involved it is a real one: a stub ``btrfs`` on PATH answers
``subvolume show`` from a file the test writes, and a stub ``sudo -n`` runs
what it is given, so the identity read is the real subprocess path, not a
mock of it.
"""

from __future__ import annotations

import logging
import os
import stat
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.core.operations as ops
from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import progress as progress_utils
from btrfs_backup_ng.core.space import SpaceCheck, SpaceInfo
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

PREFIX = "p-"
NAME = "p-20260101-000000"
SOURCE_UUID = "11111111-2222-3333-4444-555555555555"
OTHER_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _install_stub_btrfs(tmp_path: Path, monkeypatch) -> Path:
    """A ``btrfs`` on PATH whose ``subvolume show`` prints the file next to it.

    The reply file is written per test; an absent reply file makes the stub
    exit 1, which is what an unprivileged or unreachable probe looks like.
    A ``sudo`` stub drops ``-n`` and runs the rest, so the non-root escalation
    the endpoint applies is exercised for real rather than skipped.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    reply = tmp_path / "show.reply"
    btrfs = bin_dir / "btrfs"
    btrfs.write_text(
        "#!/bin/sh\n"
        f'if [ "$1" = subvolume ] && [ "$2" = show ]; then\n'
        f"  [ -f {reply} ] || exit 1\n"
        f"  cat {reply}; exit 0\n"
        "fi\n"
        'echo unexpected: "$@" >&2; exit 2\n'
    )
    btrfs.chmod(btrfs.stat().st_mode | stat.S_IXUSR)
    sudo = bin_dir / "sudo"
    sudo.write_text('#!/bin/sh\n[ "$1" = -n ] && shift\nexec "$@"\n')
    sudo.chmod(sudo.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ.get('PATH', '')}")
    return reply


def _show_output(received: str, own: str = OTHER_UUID) -> str:
    received = received or "-"
    return (
        f"dest/{NAME}\n"
        f"\tName: \t\t\t{NAME}\n"
        f"\tUUID: \t\t\t{own}\n"
        f"\tParent UUID: \t\t-\n"
        f"\tReceived UUID: \t\t{received}\n"
        "\tFlags: \t\t\treadonly\n"
    )


@pytest.fixture
def rig(tmp_path, monkeypatch):
    """A local destination, a source snapshot with a known identity, the stubs."""
    reply = _install_stub_btrfs(tmp_path, monkeypatch)
    dest_dir = tmp_path / "dest"
    dest_dir.mkdir()
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / NAME).mkdir()
    dest = LocalEndpoint(
        config={"path": str(dest_dir), "snap_prefix": PREFIX, "fs_checks": "skip"}
    )
    source = MagicMock()
    source.get_id.return_value = "src"
    snapshot = __util__.Snapshot(src_dir, PREFIX, source, name=NAME)
    snapshot.uuid = SOURCE_UUID
    # The received copy exists at the destination path; whether it is a
    # subvolume is an inode fact tmp_path cannot supply, so the shape check
    # is answered by the test while the identity read stays real.
    (dest_dir / NAME).mkdir()
    monkeypatch.setattr(ops, "_received_subvolume_shape", lambda p: None)
    return {
        "reply": reply,
        "dest": dest,
        "dest_dir": dest_dir,
        "source": source,
        "snapshot": snapshot,
    }


class TestTheVerdictMatrix:
    def test_a_received_copy_carrying_the_stream_identity_is_ok(self, rig):
        rig["reply"].write_text(_show_output(SOURCE_UUID))
        verdict = ops.artifact_verdict(rig["dest"], rig["snapshot"])
        assert verdict.status == "ok", verdict.message
        assert SOURCE_UUID in verdict.message

    def test_a_subvolume_with_no_received_uuid_is_invalid(self, rig):
        """The interrupted-receive shape: a subvolume, the right name, no
        received_uuid. Provably not the copy."""
        rig["reply"].write_text(_show_output(""))
        verdict = ops.artifact_verdict(rig["dest"], rig["snapshot"])
        assert verdict.status == "invalid", verdict.message
        assert verdict.is_failure
        assert "no received_uuid" in verdict.message

    def test_a_copy_of_something_else_is_invalid(self, rig):
        rig["reply"].write_text(_show_output(OTHER_UUID))
        verdict = ops.artifact_verdict(rig["dest"], rig["snapshot"])
        assert verdict.status == "invalid", verdict.message
        assert OTHER_UUID in verdict.message and SOURCE_UUID in verdict.message

    def test_not_a_subvolume_is_invalid_without_asking_for_privilege(
        self, rig, monkeypatch
    ):
        """A plain directory (inode != 256) after exit 0 -- the real directory
        under tmp_path, whose inode is not 256. Decided by the privilege-free
        check, before any ``subvolume show`` runs."""
        monkeypatch.undo()  # the rig answered the shape check; here it is real
        assert os.stat(rig["dest_dir"] / NAME).st_ino != 256
        rig["reply"].write_text(_show_output(SOURCE_UUID))  # would say ok
        verdict = ops.artifact_verdict(rig["dest"], rig["snapshot"])
        assert verdict.status == "invalid", verdict.message
        assert "not a btrfs subvolume" in verdict.message
        assert f"inode {os.stat(rig['dest_dir'] / NAME).st_ino}" in verdict.message

    def test_nothing_at_the_path_is_invalid(self, rig, monkeypatch):
        monkeypatch.undo()
        (rig["dest_dir"] / NAME).rmdir()
        verdict = ops.artifact_verdict(rig["dest"], rig["snapshot"])
        assert verdict.status == "invalid", verdict.message
        assert "nothing is at" in verdict.message

    def test_the_shape_check_never_consults_the_mount_table(self, rig, monkeypatch):
        """``is_subvolume`` walks the mount table through ``is_btrfs``, which
        _prepare treats as advisory (``fs_checks = "auto"`` continues past
        it). A verdict that trusted it would delete every copy received onto a
        destination that walk misjudges. Only the inode decides here."""

        def never(path):
            raise AssertionError("is_btrfs consulted by the verdict")

        monkeypatch.setattr(__util__, "is_btrfs", never)
        monkeypatch.setattr(__util__, "is_subvolume", never)
        rig["reply"].write_text(_show_output(SOURCE_UUID))
        assert ops.artifact_verdict(rig["dest"], rig["snapshot"]).status == "ok"

    def test_an_identity_that_cannot_be_read_is_unverifiable(self, rig):
        """No reply file: the stub exits 1, as ``sudo -n`` without a grant or
        an older btrfs-progs would. Never a failure."""
        assert not rig["reply"].exists()
        verdict = ops.artifact_verdict(rig["dest"], rig["snapshot"])
        assert verdict.status == "unverifiable", verdict.message
        assert not verdict.is_failure
        assert "could not be read" in verdict.message

    def test_an_unknown_source_identity_is_unverifiable(self, rig):
        """The copy has a received_uuid but the source's own uuid was never
        enriched: there is nothing to compare against, so nothing is
        confirmed and nothing is condemned."""
        rig["snapshot"].uuid = ""
        rig["reply"].write_text(_show_output(SOURCE_UUID))
        verdict = ops.artifact_verdict(rig["dest"], rig["snapshot"])
        assert verdict.status == "unverifiable", verdict.message
        assert "source snapshot is unknown" in verdict.message

    def test_a_stat_failure_on_the_path_is_unverifiable(self, rig, monkeypatch):
        def refuse(path):
            raise PermissionError(13, "Permission denied", str(path))

        monkeypatch.setattr(ops, "_received_subvolume_shape", refuse)
        verdict = ops.artifact_verdict(rig["dest"], rig["snapshot"])
        assert verdict.status == "unverifiable", verdict.message

    def test_the_stream_identity_is_the_received_uuid_when_the_source_is_a_copy(
        self, rig
    ):
        """Second hop: the source is itself a received copy of O, so the stream
        carries O's uuid and the verdict compares against that."""
        rig["snapshot"].received_uuid = "oooooooo-0000-0000-0000-000000000000"
        rig["reply"].write_text(_show_output("oooooooo-0000-0000-0000-000000000000"))
        assert ops.artifact_verdict(rig["dest"], rig["snapshot"]).status == "ok"

    def test_the_verdict_has_the_shape_verify_uses(self, rig):
        from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict

        rig["reply"].write_text(_show_output(SOURCE_UUID))
        assert isinstance(
            ops.artifact_verdict(rig["dest"], rig["snapshot"]), StructureVerdict
        )


class TestARawDestinationKeepsItsOwnVerdict:
    """The sealed sha256 is the raw verdict; it is reported, not recomputed."""

    def _raw(self, tmp_path):
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        ep = RawEndpoint.__new__(RawEndpoint)
        ep._is_remote = False
        ep.config = {"path": str(tmp_path)}
        return ep

    def test_the_committed_stream_is_judged_by_its_sidecar(self, tmp_path):
        from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict

        ep = self._raw(tmp_path)
        stored = MagicMock()
        stored.get_name.return_value = NAME
        ep.list_snapshots = lambda flush_cache=False: [stored]
        ep.verify_structure = lambda s: StructureVerdict("ok", "authoritative sidecar")
        snapshot = MagicMock()
        snapshot.get_name.return_value = NAME
        verdict = ops.artifact_verdict(ep, snapshot)
        assert verdict.status == "ok" and "sidecar" in verdict.message

    def test_a_stream_missing_from_the_listing_is_unverifiable_not_invalid(
        self, tmp_path
    ):
        """A listing miss is not proof the commit lied; nothing is failed
        or deleted on it."""
        ep = self._raw(tmp_path)
        ep.list_snapshots = lambda flush_cache=False: []
        snapshot = MagicMock()
        snapshot.get_name.return_value = NAME
        verdict = ops.artifact_verdict(ep, snapshot)
        assert verdict.status == "unverifiable"
        assert not verdict.is_failure


class TestTheExecutorActsOnTheVerdict:
    def _run(self, rig, monkeypatch, *, preexisting=None):
        monkeypatch.setattr(ops, "send_snapshot", MagicMock(return_value=None))
        cleaned: list[dict] = []
        monkeypatch.setattr(
            ops,
            "_cleanup_partial_local_subvolume",
            lambda ep, name, *, created_by_this_run: cleaned.append(
                {"name": name, "created_by_this_run": created_by_this_run}
            ),
        )
        if preexisting is not None:
            monkeypatch.setattr(
                ops, "destination_artifact_exists", lambda ep, name: preexisting
            )
        result = ops._execute_transfers(
            rig["source"], rig["dest"], [(rig["snapshot"], None)], {}
        )
        return result, cleaned

    def test_ok_is_recorded_and_the_snapshot_counts_as_transferred(
        self, rig, monkeypatch
    ):
        rig["reply"].write_text(_show_output(SOURCE_UUID))
        result, cleaned = self._run(rig, monkeypatch)
        assert result.transferred_count == 1 and result.failed_count == 0
        assert result.verdicts[NAME].status == "ok"
        assert cleaned == []

    def test_invalid_fails_the_transfer_and_cleans_under_authorship(
        self, rig, monkeypatch
    ):
        """The receive exited 0; the copy is not the copy. The transfer is a
        failure, and the artifact this run created is removed."""
        rig["reply"].write_text(_show_output(""))
        result, cleaned = self._run(rig, monkeypatch, preexisting=False)
        assert result.failed_count == 1 and result.transferred_count == 0
        (snap, err) = result.failed[0]
        assert "not valid" in str(err) and "no received_uuid" in str(err)
        assert result.verdicts[NAME].status == "invalid"
        assert cleaned == [{"name": NAME, "created_by_this_run": True}]

    def test_invalid_does_not_clean_what_predates_the_run(self, rig, monkeypatch):
        rig["reply"].write_text(_show_output(""))
        result, cleaned = self._run(rig, monkeypatch, preexisting=True)
        assert result.failed_count == 1
        assert cleaned == [{"name": NAME, "created_by_this_run": False}]

    def test_invalid_is_cleaned_on_a_remote_btrfs_destination_too(
        self, monkeypatch, tmp_path
    ):
        """The ssh endpoint cleans its own partials only when ITS transfer
        fails. An invalid artifact after a transfer that succeeded reaches its
        exact-path cleaner from here, under the same authorship rule."""
        dest = MagicMock()
        dest.get_id.return_value = "ssh://h/dest"
        dest._is_remote = True
        dest.config = {"path": "/dest"}
        dest._receive_destination = lambda p: f"/dest/{Path(p).name}"
        dest.subvolume_identity.return_value = {"uuid": "x", "received_uuid": ""}
        snapshot = MagicMock()
        snapshot.get_name.return_value = NAME
        snapshot.get_path.return_value = f"/src/{NAME}"
        snapshot.stream_uuid = SOURCE_UUID
        monkeypatch.setattr(ops, "send_snapshot", MagicMock(return_value=None))
        monkeypatch.setattr(ops, "destination_artifact_exists", lambda ep, n: False)
        result = ops._execute_transfers(MagicMock(), dest, [(snapshot, None)], {})
        assert result.failed_count == 1
        dest._cleanup_partial_subvolume.assert_called_once_with(
            "/dest", NAME, created_by_this_run=True
        )

    def test_unverifiable_keeps_the_artifact_and_counts_as_transferred(
        self, rig, monkeypatch, caplog
    ):
        """A check that could not run is not a failure. The data stays, the
        pin is released, the transfer counts, and the report says what was
        not confirmed."""
        assert not rig["reply"].exists()
        with caplog.at_level(logging.WARNING, logger=ops.logger.name):
            result, cleaned = self._run(rig, monkeypatch, preexisting=False)
        assert result.transferred_count == 1 and result.failed_count == 0
        assert result.verdicts[NAME].status == "unverifiable"
        assert cleaned == [], "an unverifiable artifact was deleted"
        assert (rig["dest_dir"] / NAME).exists()
        rig["source"].set_lock.assert_any_call(
            rig["snapshot"], rig["dest"].get_id(), False
        )
        assert any(
            "unverifiable" in r.getMessage() and "kept" in r.getMessage()
            for r in caplog.records
        )

    def test_a_verdict_that_cannot_be_computed_is_unverifiable(self, rig, monkeypatch):
        """The data has landed; a post-check that blows up must not turn that
        into a crash or a failure: unverifiable, kept, counted."""

        def explode(ep, snapshot):
            raise RuntimeError("probe machinery broke")

        monkeypatch.setattr(ops, "artifact_verdict", explode)
        result, cleaned = self._run(rig, monkeypatch, preexisting=False)
        assert result.transferred_count == 1 and result.failed_count == 0
        assert result.verdicts[NAME].status == "unverifiable"
        assert "probe machinery broke" in result.verdicts[NAME].message
        assert cleaned == []

    def test_the_verdict_is_reported_on_the_log(self, rig, monkeypatch, caplog):
        rig["reply"].write_text(_show_output(SOURCE_UUID))
        with caplog.at_level(logging.INFO, logger=ops.logger.name):
            self._run(rig, monkeypatch)
        assert any(
            f"Received copy of {NAME} verified" in r.getMessage()
            for r in caplog.records
        )


def _doubles():
    src = MagicMock()
    dst = MagicMock()
    dst.get_id.return_value = "dest-id"
    dst.subvolume_identity.return_value = None
    snap = MagicMock()
    snap.get_name.return_value = "s1"
    parent = MagicMock()
    parent.get_name.return_value = "s0"
    return src, dst, snap, parent


class TestTheLockId:
    def test_the_default_is_the_destination_id(self, monkeypatch):
        src, dst, snap, parent = _doubles()
        monkeypatch.setattr(ops, "send_snapshot", MagicMock(return_value=None))
        ops._execute_transfers(src, dst, [(snap, parent)], {})
        src.set_lock.assert_any_call(snap, "dest-id", True)
        src.set_lock.assert_any_call(parent, "dest-id", True, parent=True)
        src.set_lock.assert_any_call(snap, "dest-id", False)
        src.set_lock.assert_any_call(parent, "dest-id", False, parent=True)

    def test_a_given_lock_id_is_used_for_every_pin(self, monkeypatch):
        """A restore pins under restore:<session>, so --status and --unlock
        keep reading its pins back unchanged."""
        src, dst, snap, parent = _doubles()
        monkeypatch.setattr(ops, "send_snapshot", MagicMock(return_value=None))
        ops._execute_transfers(
            src, dst, [(snap, parent)], {}, lock_id="restore:20260101-abc"
        )
        ids = {call.args[1] for call in src.set_lock.call_args_list}
        assert ids == {"restore:20260101-abc"}
        dst.get_id.assert_not_called()


class TestReleaseOnFailure:
    def _fail(self, monkeypatch, **kw):
        src, dst, snap, parent = _doubles()
        monkeypatch.setattr(
            ops,
            "send_snapshot",
            MagicMock(side_effect=__util__.SnapshotTransferError("boom")),
        )
        monkeypatch.setattr(
            ops, "_cleanup_partial_local_subvolume", lambda *a, **k: None
        )
        result = ops._execute_transfers(src, dst, [(snap, parent)], {}, **kw)
        assert result.failed_count == 1
        return src, snap, parent

    def test_a_failed_backup_keeps_its_pin_by_default(self, monkeypatch):
        """Retention must not prune what a future run still needs to send."""
        src, snap, parent = self._fail(monkeypatch)
        releases = [c for c in src.set_lock.call_args_list if c.args[2] is False]
        assert releases == [], releases

    def test_a_failed_restore_releases_its_pin(self, monkeypatch):
        """A persistent pin left on a remote target after a failed restore
        blocks that target's prune until someone runs --unlock."""
        src, snap, parent = self._fail(monkeypatch, release_on_failure=True)
        src.set_lock.assert_any_call(snap, "dest-id", False)
        src.set_lock.assert_any_call(parent, "dest-id", False, parent=True)


class TestTheEstimateIsAskedOfTheSource:
    def test_the_space_check_uses_the_source_endpoints_measurement(
        self, monkeypatch, caplog
    ):
        """The local measurement must not run at all: aimed at a remote path
        it measures nothing and warns on every transfer."""
        snapshot = MagicMock()
        snapshot.get_path.return_value = "/remote/p-1"
        snapshot.endpoint.estimate_transfer_size.return_value = 4096
        dest = MagicMock()
        info = SpaceInfo(
            path="/d", total_bytes=10**9, used_bytes=0, available_bytes=10**9
        )
        dest.get_space_info.return_value = info

        def forbidden(*a, **k):
            raise AssertionError("the local estimate ran")

        monkeypatch.setattr(progress_utils, "estimate_snapshot_size", forbidden)
        seen = {}

        def check(space_info, size, safety_margin_percent):
            seen["size"] = size
            return SpaceCheck(
                space_info=info,
                estimated_size=size,
                sufficient=True,
                effective_limit=10**9,
                required_with_margin=size,
            )

        monkeypatch.setattr(ops, "check_space_availability", check)
        with caplog.at_level(logging.WARNING, logger=ops.logger.name):
            ops._verify_destination_space(snapshot, dest, None, {})
        assert seen == {"size": 4096}
        snapshot.endpoint.estimate_transfer_size.assert_called_once_with(snapshot, None)
        assert not any("Could not estimate" in r.getMessage() for r in caplog.records)

    def test_an_incremental_transfer_is_not_a_failed_estimate(
        self, monkeypatch, caplog
    ):
        """The delta of an incremental send is deliberately not sized; that
        used to be reported as "Could not estimate", a warning, on every
        incremental transfer."""
        snapshot, parent = MagicMock(), MagicMock()
        snapshot.endpoint.estimate_transfer_size.return_value = None
        dest = MagicMock()
        with caplog.at_level(logging.INFO, logger=ops.logger.name):
            ops._verify_destination_space(snapshot, dest, parent, {})
        messages = [r.getMessage() for r in caplog.records]
        assert not any("Could not estimate" in m for m in messages), messages
        assert any("Incremental transfer" in m for m in messages), messages

    def test_the_parent_is_handed_to_the_source_endpoint(self):
        snapshot, parent = MagicMock(), MagicMock()
        snapshot.endpoint.estimate_transfer_size.return_value = None
        assert ops._estimate_transfer_size(snapshot, parent) is None
        snapshot.endpoint.estimate_transfer_size.assert_called_once_with(
            snapshot, parent
        )

    def test_a_snapshot_without_a_measuring_endpoint_falls_back_to_local(
        self, monkeypatch
    ):
        """A wrapper carrying only a path (a raw snapshot, a remote subvolume
        stand-in) keeps the measurement it always had."""
        snapshot = MagicMock(spec=["get_path"])
        snapshot.get_path.return_value = "/local/p-1"
        monkeypatch.setattr(
            progress_utils, "estimate_snapshot_size", lambda p, pp=None: 77
        )
        assert ops._estimate_transfer_size(snapshot, None) == 77

    def test_a_local_endpoint_measures_locally(self, monkeypatch, tmp_path):
        ep = LocalEndpoint(
            config={"path": str(tmp_path), "snap_prefix": PREFIX, "fs_checks": "skip"}
        )
        seen = {}
        monkeypatch.setattr(
            progress_utils,
            "estimate_snapshot_size",
            lambda p, pp=None: seen.update(path=p, parent=pp) or 5,
        )
        snapshot = __util__.Snapshot(tmp_path, PREFIX, ep, name=NAME)
        assert ep.estimate_transfer_size(snapshot) == 5
        assert seen == {"path": str(tmp_path / NAME), "parent": None}


class _Completed:
    def __init__(self, rc: int, out: str) -> None:
        self.returncode = rc
        self.stdout = out.encode()
        self.stderr = b""


class TestTheSshSourceMeasuresOnTheRemote:
    def _endpoint(self, calls, replies, **extra):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": "/remote/backups", **extra}

        def run(cmd, *a, **k):
            calls.append(list(cmd))
            return replies.pop(0)

        ep._exec_remote_command = run
        ep._exec_remote_command_with_retry = run
        return ep

    def _snapshot(self):
        snapshot = MagicMock()
        snapshot.get_path.return_value = f"/remote/backups/{NAME}"
        return snapshot

    def test_a_full_send_is_sized_by_a_remote_filesystem_du(self):
        """One round trip, and it is ``btrfs filesystem du``: the referenced
        Total is what a full stream carries. ``subvolume show`` is not asked;
        its exclusive figure (printed as ``Usage exclusive``, quotas only) is
        not the size of a full send."""
        calls: list = []
        ep = self._endpoint(
            calls,
            [
                _Completed(
                    0,
                    "     Total   Exclusive  Set shared  Filename\n"
                    f"   3149824     3149824           0  /remote/backups/{NAME}\n",
                )
            ],
        )
        assert ep.estimate_transfer_size(self._snapshot()) == 3149824
        assert calls == [
            ["btrfs", "filesystem", "du", "-s", "--raw", f"/remote/backups/{NAME}"]
        ]

    def test_an_incremental_send_is_not_measured(self):
        calls: list = []
        ep = self._endpoint(calls, [])
        assert ep.estimate_transfer_size(self._snapshot(), parent=MagicMock()) is None
        assert calls == []

    def test_nothing_measurable_is_none_not_an_error(self):
        calls: list = []
        ep = self._endpoint(calls, [_Completed(1, "")])
        assert ep.estimate_transfer_size(self._snapshot()) is None
        assert len(calls) == 1


class TestTheSshIdentityProbe:
    def test_reads_the_identity_on_the_remote(self):
        calls: list = []
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": "/remote/backups", "ssh_sudo": True}
        retried: list = []

        def run(cmd, *a, **k):
            calls.append(list(cmd))
            return _Completed(0, _show_output(SOURCE_UUID))

        def run_retry(cmd, *a, **k):
            retried.append(list(cmd))
            return run(cmd)

        ep._exec_remote_command = run
        ep._exec_remote_command_with_retry = run_retry
        ids = ep.subvolume_identity(f"/remote/backups/{NAME}")
        assert ids == {"uuid": OTHER_UUID, "received_uuid": SOURCE_UUID}
        assert retried == [["btrfs", "subvolume", "show", f"/remote/backups/{NAME}"]]

    def test_a_failed_probe_is_none(self):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": "/remote/backups"}
        ep._exec_remote_command = lambda *a, **k: _Completed(1, "")
        assert ep.subvolume_identity("/remote/backups/x") is None
        assert ep._subvolume_exists_at("/remote/backups/x") is False
