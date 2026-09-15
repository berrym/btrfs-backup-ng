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
