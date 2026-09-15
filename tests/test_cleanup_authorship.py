"""A cleanup may delete only what the run it belongs to created.

``_cleanup_partial_local_subvolume`` deleted ``{dest}/{name}`` on the sole
precondition that something was there, with an ``rm -rf`` fallback behind the
``btrfs subvolume delete``. Its docstring argued that was safe because
"skip-detection already excluded any snapshot whose name is present at the
destination BEFORE the transfer was attempted, so anything now at the exact path
is this failed run's partial".

Skip-detection is by UUID CORRESPONDENCE, not by name: ``plan_transfer_sequence``
builds its present-set from ``Endpoint.correspondents_of``, which indexes the
destination by ``received_uuid``. A destination subvolume that merely shares a
name and carries no matching ``received_uuid`` -- one another tool wrote, a
manual ``btrfs receive``, a restored copy -- is therefore PLANNED for transfer,
and was deleted when that transfer failed.

The premise was never checked at runtime. It is now recorded before the transfer
starts, which is the only moment the answer still exists: afterwards a partial
this run wrote and a backup that was already there are the same observation.
``_cleanup_partial_raw_stream`` has worked this way all along, deleting only the
``.part`` name this run generated.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.core import operations as ops


def _local_endpoint(tmp_path):
    ep = MagicMock()
    ep._is_remote = False
    ep.config = {"path": str(tmp_path)}
    return ep


class TestAPreExistingArtifactIsNeverDeleted:
    def test_a_same_named_subvolume_that_predates_the_run_survives(
        self, monkeypatch, tmp_path
    ):
        """The data-loss case, exactly: a destination subvolume with no matching
        received_uuid is planned for transfer, the transfer fails, and cleanup
        removes what it did not create."""
        victim = tmp_path / "home.20240101-120000"
        victim.mkdir()
        (victim / "irreplaceable.bin").write_bytes(b"a backup another tool wrote")

        calls: list[list[str]] = []
        monkeypatch.setattr(
            ops.subprocess,
            "run",
            lambda cmd, **k: (calls.append(list(cmd)), SimpleNamespace(returncode=0))[
                1
            ],
        )
        ops._cleanup_partial_local_subvolume(
            _local_endpoint(tmp_path),
            "home.20240101-120000",
            created_by_this_run=False,
        )

        assert calls == [], f"cleanup issued {calls} against a pre-existing artifact"
        assert (victim / "irreplaceable.bin").exists()

    def test_the_rm_rf_fallback_is_not_reached_either(self, monkeypatch, tmp_path):
        """The fallback runs when `btrfs subvolume delete` fails -- which it does
        for a plain directory, i.e. precisely the shape a foreign backup has."""
        (tmp_path / "snap-1").mkdir()
        calls: list[list[str]] = []
        monkeypatch.setattr(
            ops.subprocess,
            "run",
            lambda cmd, **k: (calls.append(list(cmd)), SimpleNamespace(returncode=1))[
                1
            ],
        )
        ops._cleanup_partial_local_subvolume(
            _local_endpoint(tmp_path), "snap-1", created_by_this_run=False
        )
        assert not any("rm" in c for c in calls)

    def test_an_artifact_this_run_created_is_still_cleaned(self, monkeypatch, tmp_path):
        """The guard must not disable the cleanup. A partial left behind is what
        the next run's skip-detection mistakes for a completed backup."""
        (tmp_path / "snap-1").mkdir()
        calls: list[list[str]] = []
        monkeypatch.setattr(
            ops.subprocess,
            "run",
            lambda cmd, **k: (calls.append(list(cmd)), SimpleNamespace(returncode=0))[
                1
            ],
        )
        ops._cleanup_partial_local_subvolume(
            _local_endpoint(tmp_path), "snap-1", created_by_this_run=True
        )
        deletes = [c for c in calls if c[-3:-1] == ["subvolume", "delete"]]
        assert deletes and deletes[0][-1] == str(tmp_path / "snap-1")


class TestAuthorshipIsRecordedBeforeTheTransfer:
    def test_an_existing_local_path_reads_as_pre_existing(self, tmp_path):
        (tmp_path / "snap-1").mkdir()
        assert ops.destination_artifact_exists(_local_endpoint(tmp_path), "snap-1")

    def test_an_absent_local_path_reads_as_ours_to_create(self, tmp_path):
        assert not ops.destination_artifact_exists(_local_endpoint(tmp_path), "snap-1")

    def test_a_remote_path_is_asked_over_the_remote_transport(self):
        """Never stat'd locally: the destination is on another machine, and a
        same-named local path would answer for the wrong host."""
        ep = MagicMock()
        ep._is_remote = True
        ep.config = {"path": "/remote/dest"}
        ep._exec_remote_command.return_value = SimpleNamespace(returncode=0)

        assert ops.destination_artifact_exists(ep, "snap-1")
        argv = ep._exec_remote_command.call_args[0][0]
        assert argv == ["test", "-e", "/remote/dest/snap-1"]

    def test_an_unanswerable_question_assumes_pre_existing(self):
        """The failure being guarded against is deleting a backup that was not
        ours, so uncertainty must resolve to leaving it alone."""
        ep = MagicMock()
        ep._is_remote = True
        ep.config = {"path": "/remote/dest"}
        ep._exec_remote_command.side_effect = OSError("connection closed")

        assert ops.destination_artifact_exists(ep, "snap-1") is True

    def test_a_remote_endpoint_with_no_transport_assumes_pre_existing(self):
        ep = MagicMock(spec=["config", "_is_remote"])
        ep._is_remote = True
        ep.config = {"path": "/remote/dest"}
        assert ops.destination_artifact_exists(ep, "snap-1") is True


class TestTheSSHCleanerHonoursItToo:
    def test_a_pre_existing_remote_subvolume_is_not_removed(self):
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"ssh_sudo": False}
        ep.hostname = "host"
        ep._exec_remote_command = MagicMock()

        ep._cleanup_partial_subvolume(
            "/remote/dest", "snap-1", created_by_this_run=False
        )

        assert ep._exec_remote_command.call_args_list == [], (
            "a remote command was issued against a subvolume that predates the run"
        )


class TestTheAuthorshipFlagIsNotOptional:
    """A cleanup that can be called without deciding authorship will be."""

    def test_the_local_cleanup_requires_it(self, tmp_path):
        with pytest.raises(TypeError):
            ops._cleanup_partial_local_subvolume(_local_endpoint(tmp_path), "snap-1")

    @pytest.mark.parametrize(
        "name",
        ["_cleanup_partial_local_subvolume", "_cleanup_partial_remote_subvolume"],
    )
    def test_no_call_site_hardcodes_it(self, name):
        """Passing a literal True satisfies the signature and defeats the guard.

        The keyword being present proves only that someone typed it; the value
        has to come from a recorded observation.
        """
        import ast
        import inspect

        tree = ast.parse(inspect.getsource(ops))
        bad = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not (isinstance(node.func, ast.Name) and node.func.id == name):
                continue
            kw = next(
                (k for k in node.keywords if k.arg == "created_by_this_run"), None
            )
            if kw is None:
                bad.append((node.lineno, "absent"))
            elif isinstance(kw.value, ast.Constant):
                bad.append((node.lineno, f"hardcoded {kw.value.value!r}"))
        assert not bad, f"{name}: {bad}"


class TestTheTransferLoopActuallyRecordsIt:
    """End to end, because a call site can satisfy the signature and still be
    wrong. This is the defect itself: a same-named destination subvolume with no
    matching received_uuid is planned for transfer, the transfer fails, and the
    cleanup removes what it did not create."""

    @staticmethod
    def _snapshot(name):
        snap = MagicMock()
        snap.get_name.return_value = name
        return snap

    def _drive(self, monkeypatch, tmp_path, *, artifact_present):
        from btrfs_backup_ng import __util__

        if artifact_present:
            (tmp_path / "s1").mkdir()

        src, dst = MagicMock(), MagicMock()
        dst.get_id.return_value = "d"
        dst._is_remote = False
        dst.config = {"path": str(tmp_path)}

        monkeypatch.setattr(
            ops,
            "send_snapshot",
            MagicMock(side_effect=__util__.SnapshotTransferError("boom")),
        )
        seen = {}
        monkeypatch.setattr(
            ops,
            "_cleanup_partial_local_subvolume",
            lambda ep, name, *, created_by_this_run: seen.update(
                created_by_this_run=created_by_this_run
            ),
        )
        ops._execute_transfers(src, dst, [(self._snapshot("s1"), None)], {})
        return seen

    def test_a_pre_existing_artifact_is_reported_as_not_ours(
        self, monkeypatch, tmp_path
    ):
        seen = self._drive(monkeypatch, tmp_path, artifact_present=True)
        assert seen == {"created_by_this_run": False}, (
            "the transfer loop told cleanup it owned a subvolume that was already "
            "at the destination before the transfer began"
        )

    def test_an_absent_artifact_is_reported_as_ours(self, monkeypatch, tmp_path):
        seen = self._drive(monkeypatch, tmp_path, artifact_present=False)
        assert seen == {"created_by_this_run": True}


def test_the_raw_cleanup_remains_the_reference_pattern():
    """It records authorship in the NAME -- the .part file carries this run's pid,
    monotonic stamp and random token -- and deletes only that, never the published
    stream. Nothing here should have changed it."""
    import inspect

    source = inspect.getsource(ops._cleanup_partial_raw_stream)
    assert "part_path" in source
    assert 'pending["stream_path"]' not in source, (
        "the raw cleanup now deletes the published stream, not just its .part"
    )


class TestVerifyDeletesOnlyWhatItRestored:
    """``verify --level full`` test-restores each backup into a temp directory and
    removes them afterwards. The removal loop iterated ``to_verify`` and deleted
    ``temp_path / snap.get_name()`` for every entry, on existence alone.

    Two ways that reaches beyond the run: ``--temp-dir`` may be a directory the
    operator already uses, where a subvolume sharing a backup's name is theirs;
    and a snapshot whose restore never ran -- a space shortfall skips it before
    the restore -- still had its path deleted.
    """

    def test_a_subvolume_already_in_a_user_temp_dir_is_not_deleted(
        self, monkeypatch, tmp_path
    ):
        """Drives the real verify_full: only _test_restore and the privilege
        preflight are stubbed, so the cleanup loop under test is the shipped one."""
        from btrfs_backup_ng.core import verify as vf

        theirs = tmp_path / "home.20240101-120000"
        theirs.mkdir()
        (theirs / "their-data.bin").write_bytes(b"not ours to delete")

        snap = MagicMock()
        snap.get_name.return_value = "home.20240101-120000"
        backup_endpoint = MagicMock()
        backup_endpoint.config = {"path": "/backups"}
        backup_endpoint.list_snapshots.return_value = [snap]

        deleted: list = []
        monkeypatch.setattr(vf, "_can_run_btrfs_privileged", lambda: True)
        monkeypatch.setattr(vf.__util__, "is_btrfs", lambda p: True)
        monkeypatch.setattr(vf, "_delete_temp_subvolume", deleted.append)
        monkeypatch.setattr(vf.__util__, "is_subvolume", lambda p: True)
        monkeypatch.setattr(vf, "_estimate_temp_shortfall", lambda *a, **k: None)
        # The restore is a no-op: the subvolume at that path is the operator's,
        # and the point is that the cleanup must not touch it regardless.
        monkeypatch.setattr(vf, "_test_restore", lambda *a, **k: None)

        report = vf.verify_full(backup_endpoint, temp_dir=tmp_path, cleanup=True)

        assert report.results, f"verify never reached a snapshot: {report.errors}"
        assert deleted == [], (
            f"verify deleted {deleted} from a temp dir it did not restore into"
        )
        assert (theirs / "their-data.bin").read_bytes() == b"not ours to delete"

    def test_a_subvolume_this_run_restored_is_still_deleted(
        self, monkeypatch, tmp_path
    ):
        """The guard must not turn the cleanup off: a received subvolume left in a
        user-supplied temp dir carries a received_uuid and could later be mistaken
        for a real backup."""
        from btrfs_backup_ng.core import verify as vf

        snap = MagicMock()
        snap.get_name.return_value = "home.20240102-120000"
        backup_endpoint = MagicMock()
        backup_endpoint.config = {"path": "/backups"}
        backup_endpoint.list_snapshots.return_value = [snap]

        target = tmp_path / "home.20240102-120000"
        deleted: list = []
        monkeypatch.setattr(vf, "_can_run_btrfs_privileged", lambda: True)
        monkeypatch.setattr(vf.__util__, "is_btrfs", lambda p: True)
        monkeypatch.setattr(vf, "_delete_temp_subvolume", deleted.append)
        monkeypatch.setattr(vf.__util__, "is_subvolume", lambda p: True)
        monkeypatch.setattr(vf, "_estimate_temp_shortfall", lambda *a, **k: None)
        monkeypatch.setattr(vf, "_test_restore", lambda *a, **k: target.mkdir())

        report = vf.verify_full(backup_endpoint, temp_dir=tmp_path, cleanup=True)

        assert report.results, f"verify never reached a snapshot: {report.errors}"
        assert deleted == [target], f"the restored subvolume was left behind: {deleted}"

    def test_the_cleanup_loop_iterates_what_was_restored_not_what_was_planned(self):
        """Pins the fix at the source, since the loop is inside a long function
        that a unit test cannot easily drive end to end."""
        import inspect

        source = inspect.getsource(
            __import__(
                "btrfs_backup_ng.core.verify", fromlist=["verify_full"]
            ).verify_full
        )
        cleanup = source.split("finally:")[-1]
        assert "for snap_path in restored_here:" in cleanup, (
            "the verify cleanup no longer iterates the paths it restored"
        )
        assert "for snap in to_verify:" not in cleanup, (
            "the verify cleanup deletes by plan again, not by what it restored"
        )


class TestSnapperRestoreDeletesOnlyWhatItCreated:
    """``restore_snapper_snapshot`` removes the destination subvolume and the whole
    numbered slot directory on ANY exception, on existence alone. The slot comes
    from ``get_next_snapshot_number`` and is supposed to be free -- but a stale
    scan, a concurrent snapper, or a slot made by hand is enough to make it not,
    and a failure before anything was written then deletes a snapshot that was
    already there.
    """

    @staticmethod
    def _source():
        import inspect

        from btrfs_backup_ng.core import restore as rs

        return inspect.getsource(rs.restore_snapper_snapshot)

    def test_existence_is_recorded_before_the_slot_is_created(self):
        source = self._source()
        record = source.index("slot_preexisted = dest_snapshot_dir.exists()")
        mkdir = source.index("privileged_mkdir(dest_snapshot_dir")
        assert record < mkdir, (
            "the slot's prior existence is recorded after it is created, which "
            "always answers 'it was already there'"
        )

    def test_the_subvolume_deletion_is_gated_on_the_record(self):
        assert (
            "if dest_snapshot_path.exists() and not snapshot_preexisted:"
            in self._source()
        )

    def test_the_slot_removal_is_gated_on_the_record(self):
        assert (
            "if dest_snapshot_dir.exists() and not slot_preexisted:" in self._source()
        )

    def test_the_two_are_judged_separately(self):
        """A restore can fail after creating the directory but before receiving
        into it, and can also fail into a slot that already held a snapshot. One
        flag for both would either leak the directory or delete the snapshot."""
        source = self._source()
        # Each must be derived from its OWN path. Counting the assignments is not
        # enough: `snapshot_preexisted = slot_preexisted` assigns exactly once and
        # collapses the two, so a failure after the directory was created but
        # before the receive would refuse to remove the directory it made.
        assert "slot_preexisted = dest_snapshot_dir.exists()" in source
        assert "snapshot_preexisted = dest_snapshot_path.exists()" in source


class TestNoCleanupCanBeReachedWithoutDecidingAuthorship:
    """The lesson from the first attempt at this fix.

    created_by_this_run was given a default of True "to preserve the behaviour of
    any caller that has not been taught to record it". There were six such
    callers, all inside endpoint/ssh.py, on every ssh:// transfer route -- so the
    guard read as applied while every remote path went straight past it. A
    parameter that can be omitted on the path it protects is not a guard.
    """

    CLEANUPS = [
        ("btrfs_backup_ng.core.operations", "_cleanup_partial_local_subvolume"),
        ("btrfs_backup_ng.core.operations", "_cleanup_partial_remote_subvolume"),
        ("btrfs_backup_ng.endpoint.ssh", "SSHEndpoint._cleanup_partial_subvolume"),
    ]

    @pytest.mark.parametrize(
        ("module", "qualname"), CLEANUPS, ids=[q for _m, q in CLEANUPS]
    )
    def test_the_parameter_has_no_default(self, module, qualname):
        import importlib
        import inspect

        obj = importlib.import_module(module)
        for part in qualname.split("."):
            obj = getattr(obj, part)
        param = inspect.signature(obj).parameters["created_by_this_run"]
        assert param.default is inspect.Parameter.empty, (
            f"{qualname} defaults created_by_this_run to {param.default!r}; a "
            "caller can then reach the delete without deciding"
        )

    def test_every_ssh_call_site_passes_it(self):
        """All six live inside the module that defines the method, which is how
        they were missed: threading the flag from core/operations.py looked like
        the whole job."""
        import ast
        import inspect

        from btrfs_backup_ng.endpoint import ssh as ssh_mod

        tree = ast.parse(inspect.getsource(ssh_mod))
        sites, bad = 0, []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (
                isinstance(func, ast.Attribute)
                and func.attr == "_cleanup_partial_subvolume"
            ):
                continue
            sites += 1
            kw = next(
                (k for k in node.keywords if k.arg == "created_by_this_run"), None
            )
            if kw is None:
                bad.append((node.lineno, "absent"))
            elif isinstance(kw.value, ast.Constant):
                bad.append((node.lineno, f"hardcoded {kw.value.value!r}"))
        assert sites >= 6, f"expected at least 6 call sites, found {sites}"
        assert not bad, f"call sites not deciding authorship: {bad}"

    def test_the_remote_probe_answers_the_remote(self):
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"ssh_sudo": False}
        ep.hostname = "host"
        ep._exec_remote_command = MagicMock(return_value=SimpleNamespace(returncode=0))

        assert ep.artifact_exists("/dest", "snap-1") is True
        assert ep._exec_remote_command.call_args[0][0] == [
            "test",
            "-e",
            "/dest/snap-1",
        ]

    def test_an_unreachable_probe_assumes_pre_existing(self):
        """Uncertainty must resolve to leaving the artifact alone."""
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"ssh_sudo": False}
        ep.hostname = "host"
        ep._exec_remote_command = MagicMock(side_effect=OSError("connection closed"))

        assert ep.artifact_exists("/dest", "snap-1") is True
