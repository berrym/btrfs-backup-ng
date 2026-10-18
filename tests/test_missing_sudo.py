"""Where sudo is not installed at all, every local escalation says so.

btrfs needs root. Not running as root, the product escalates through sudo, and
every local site used to build its own `sudo` prefix; where there was no sudo,
as in a container or a minimal install, the result was "[Errno 2] No such file
or directory: 'sudo'", the shell's "sudo: not found", or nothing at all (#116).
`__util__.sudo_argv()` is now the one place that decides, and raises the one
message; `local_elevation_problem()` answers the same question for doctor and
for the start of a run.

Each site is driven as an ordinary user, where it must say that sudo is not
installed and what to do, and as root, where it must not ask for sudo at all
and runs the real command, which on this PATH is not there either -- so the
root case fails on that command, not on sudo. PATH holds a shell and rm and
nothing else; what has no local equivalent (the mount table, the remote side
of ssh) is stood in for.
"""

from __future__ import annotations

import contextlib
import logging
import os
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli import prune
from btrfs_backup_ng.core import operations
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint
from btrfs_backup_ng.retention import RetentionError

from .interrupted_restore import REAL_RUN, build_rig, dead_pid, mark, run_cleanup

MESSAGE = "sudo is not installed"


def _take_sudo_away(tmp_path, monkeypatch):
    """A PATH holding a shell and rm, nothing else: no sudo, no btrfs, no mount."""
    bin_dir = tmp_path / "only-sh"
    bin_dir.mkdir(exist_ok=True)
    for tool in ("sh", "rm"):
        if not (bin_dir / tool).exists():
            (bin_dir / tool).symlink_to(shutil.which(tool))
    monkeypatch.setenv("PATH", str(bin_dir))


@pytest.fixture
def no_sudo(tmp_path, monkeypatch):
    _take_sudo_away(tmp_path, monkeypatch)


@pytest.fixture(params=["not-root", "root"])
def uid(request, monkeypatch):
    """The process as an ordinary user, and as root, which never asks sudo."""
    monkeypatch.setattr(os, "geteuid", lambda: 0 if request.param == "root" else 1000)
    return request.param


@pytest.fixture
def not_root(monkeypatch):
    monkeypatch.setattr(os, "geteuid", lambda: 1000)


@pytest.fixture
def rig(tmp_path, monkeypatch):
    return build_rig(tmp_path, monkeypatch)


def _errors(caplog, level=logging.WARNING):
    return [r.getMessage() for r in caplog.records if r.levelno >= level]


class TestTheSeam:
    def test_root_needs_no_prefix_and_no_sudo(self, no_sudo, monkeypatch):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        assert __util__.sudo_argv() == []
        assert __util__.local_elevation_problem() is None

    def test_a_user_with_sudo_gets_the_prefix(self, not_root):
        assert __util__.sudo_argv() == ["sudo", "-n"]  # the unit tests' stand-in
        assert __util__.sudo_argv(allow_prompt=True) == ["sudo"]
        assert __util__.local_elevation_problem() is None

    def test_a_user_without_sudo_is_refused_with_the_one_message(
        self, no_sudo, not_root
    ):
        with pytest.raises(__util__.AbortError, match=MESSAGE) as excinfo:
            __util__.sudo_argv()
        assert str(excinfo.value) == __util__.local_elevation_problem()
        assert "as root" in str(excinfo.value)


class TestThePrivilegedWrites:
    def test_a_denied_write_says_sudo_is_not_installed(self, tmp_path, no_sudo, uid):
        """The direct write is refused (EACCES, injected so this holds as a real
        root too); root is told that alone, a user is told about sudo."""
        target = tmp_path / "f"

        def denied():
            raise PermissionError(13, "Permission denied")

        with pytest.raises(PermissionError) as excinfo:
            __util__._privileged_fs(
                denied, ["tee", str(target)], action="write", path=target
            )
        msg = str(excinfo.value)
        assert "Permission denied" in msg
        if uid == "root":
            assert MESSAGE not in msg
        else:
            assert MESSAGE in msg and str(target) in msg


class TestTheLocalBtrfsCommand:
    """_exec_command is the escalation every local btrfs command goes through:
    snapshot, sync, send, receive, delete."""

    def test_a_btrfs_command_says_sudo_is_not_installed(self, tmp_path, no_sudo, uid):
        endpoint = LocalEndpoint(config={"path": str(tmp_path)})
        command = [
            ("btrfs", False),
            ("subvolume", False),
            ("list", False),
            (str(tmp_path), True),
        ]
        with patch(
            "btrfs_backup_ng.endpoint.common._command_lock_path",
            return_value=tmp_path / "cmd.lock",
        ):
            with pytest.raises(__util__.AbortError) as excinfo:
                endpoint._exec_command({"command": command, "no_password_sudo": True})
        msg = str(excinfo.value)
        if uid == "root":
            # No sudo asked for: the real btrfs is run, and is not on this PATH.
            assert "btrfs" in msg and MESSAGE not in msg
        else:
            assert MESSAGE in msg


class TestTheRemount:
    def test_a_remount_says_sudo_is_not_installed(self, tmp_path, no_sudo, uid):
        endpoint = LocalEndpoint(config={"path": str(tmp_path)})
        listing = f"/dev/sdz1 on {tmp_path} type btrfs (ro,relatime)\n"
        with patch(
            "btrfs_backup_ng.endpoint.common.subprocess.check_output",
            return_value=listing,
        ):
            if uid == "root":
                with pytest.raises(FileNotFoundError) as excinfo:
                    endpoint._remount(tmp_path, read_write=True)
                assert excinfo.value.filename == "mount"
            else:
                with pytest.raises(__util__.AbortError, match=MESSAGE):
                    endpoint._remount(tmp_path, read_write=True)


class TestTheLockManagers:
    """A lock directory the user cannot write is escalated through `sudo -n sh -c`;
    the manager reports stderr, so the reason reaches the operator."""

    def test_the_local_store_reports_the_reason(self, tmp_path, no_sudo, not_root):
        endpoint = LocalEndpoint(config={"path": str(tmp_path)})
        manager = endpoint._build_local_lock_manager()
        rc, out, err = manager._run_elevated("echo should-not-run")
        assert (rc, out) == (127, "") and MESSAGE in err

    def test_the_raw_store_reports_the_reason(self, tmp_path, no_sudo, not_root):
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        endpoint = RawEndpoint(config={"path": str(tmp_path)})
        manager = endpoint._build_lock_manager()
        rc, out, err = manager._run_elevated("echo should-not-run")
        assert (rc, out) == (127, "") and MESSAGE in err

    def test_root_runs_the_script_itself(self, tmp_path, no_sudo, monkeypatch):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        endpoint = LocalEndpoint(config={"path": str(tmp_path)})
        manager = endpoint._build_local_lock_manager()
        rc, out, _err = manager._run_elevated("echo ran-as-root")
        assert (rc, out.strip()) == (0, "ran-as-root")


class TestTheSnapperPrune:
    def test_a_slot_delete_says_sudo_is_not_installed(self, tmp_path, no_sudo, uid):
        slot = tmp_path / ".snapshots" / "9"
        (slot / "snapshot").mkdir(parents=True)
        (slot / "info.xml").write_text("<snapshot/>")

        if uid == "root":
            with pytest.raises(FileNotFoundError) as excinfo:
                prune._delete_snapper_slot_btrfs(None, str(slot), False)
            assert excinfo.value.filename == "btrfs"
        else:
            with pytest.raises(RetentionError, match=MESSAGE):
                prune._delete_snapper_slot_btrfs(None, str(slot), False)
        assert (slot / "info.xml").exists() and (slot / "snapshot").is_dir()


class TestTheRestoreCleanup:
    def test_it_refuses_once_before_asking_the_operator(
        self, rig, tmp_path, monkeypatch, not_root, capsys
    ):
        """One message, before the confirmation prompt, not one per subvolume
        after it."""
        for name in ("home.20260101", "home.20260102"):
            (rig.dest / name).mkdir()
            mark(rig.dest, name, dead_pid(), token=name)
            rig.identities[str(rig.dest / name)] = {"uuid": name, "received_uuid": ""}
        asked: list = []
        monkeypatch.setattr("builtins.input", lambda *_: asked.append(1) or "y")
        monkeypatch.setattr(subprocess, "run", REAL_RUN)
        _take_sudo_away(tmp_path, monkeypatch)

        assert run_cleanup(rig.dest) == 1

        out = capsys.readouterr().out
        assert out.count(MESSAGE) == 1, out
        assert asked == [], (
            "the operator was asked to confirm a deletion that cannot run"
        )
        assert all((rig.dest / n).is_dir() for n in ("home.20260101", "home.20260102"))

    def test_a_dry_run_needs_no_sudo(
        self, rig, tmp_path, monkeypatch, not_root, capsys
    ):
        (rig.dest / "home.20260101").mkdir()
        mark(rig.dest, "home.20260101", dead_pid())
        rig.identities[str(rig.dest / "home.20260101")] = {
            "uuid": "x",
            "received_uuid": "",
        }
        _take_sudo_away(tmp_path, monkeypatch)

        assert run_cleanup(rig.dest, dry_run=True) == 0
        assert MESSAGE not in capsys.readouterr().out

    def test_root_deletes_without_sudo(self, rig, tmp_path, monkeypatch, caplog):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        (rig.dest / "home.20260101").mkdir()
        mark(rig.dest, "home.20260101", dead_pid())
        rig.identities[str(rig.dest / "home.20260101")] = {
            "uuid": "x",
            "received_uuid": "",
        }
        monkeypatch.setattr(subprocess, "run", REAL_RUN)
        _take_sudo_away(tmp_path, monkeypatch)

        with caplog.at_level(logging.ERROR):
            assert run_cleanup(rig.dest) == 1  # the real btrfs is not on this PATH
        errors = _errors(caplog, logging.ERROR)
        assert any("'btrfs'" in m for m in errors)
        assert not any(MESSAGE in m for m in errors)


class TestTheSnapperSlotScripts:
    """The slot scripts carry their btrfs verbs themselves, under sudo when not
    root, and some tolerate the verb failing (`|| rm -rf`). Without sudo the
    verb must fail and say why while the rest of the script still runs."""

    def test_the_fallback_still_runs_when_the_verb_fails(
        self, tmp_path, no_sudo, not_root
    ):
        endpoint = SimpleNamespace(config={"path": str(tmp_path)})
        leftover = tmp_path / "5.incoming"
        leftover.mkdir()
        script = (
            f"{operations._snapper_btrfs(endpoint)} subvolume delete {leftover}/snapshot "
            f"|| rm -rf {leftover}"
        )

        rc, _out = operations._snapper_exec_shell(endpoint, script, privileged=False)

        assert rc == 0, "the script's own fallback did not run"
        assert not leftover.exists(), "the leftover was not removed"

    def test_a_failed_verb_names_the_reason_in_the_log(
        self, tmp_path, no_sudo, not_root, caplog
    ):
        endpoint = SimpleNamespace(config={"path": str(tmp_path)})
        script = f"{operations._snapper_btrfs(endpoint)} subvolume show {tmp_path}"
        with caplog.at_level(logging.ERROR):
            rc, out = operations._snapper_exec_shell(endpoint, script, privileged=False)
        assert (rc, out) == (127, "")
        assert any(MESSAGE in m for m in _errors(caplog, logging.ERROR))

    def test_an_elevated_shell_is_refused_with_the_reason(
        self, tmp_path, no_sudo, not_root, caplog
    ):
        endpoint = SimpleNamespace(config={"path": str(tmp_path)})
        with caplog.at_level(logging.ERROR):
            rc, out = operations._snapper_exec_shell(
                endpoint, "exit 0", privileged=True
            )
        assert (rc, out) == (127, "")
        assert any(MESSAGE in m for m in _errors(caplog, logging.ERROR))

    def test_root_runs_the_verb_itself(self, tmp_path, no_sudo, monkeypatch):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        endpoint = SimpleNamespace(config={"path": str(tmp_path)})
        assert operations._snapper_btrfs(endpoint) == "btrfs"
        rc, _ = operations._snapper_exec_shell(endpoint, "exit 0", privileged=True)
        assert rc == 0

    def test_the_writability_probe_still_runs(self, tmp_path, no_sudo, not_root):
        endpoint = SimpleNamespace(config={"path": str(tmp_path)})
        assert operations._snapper_exec_shell(endpoint, "exit 0", privileged=False) == (
            0,
            "",
        )


class TestThePartialCleanup:
    def test_a_partial_says_it_could_not_be_removed(
        self, tmp_path, no_sudo, uid, caplog
    ):
        """It used to fail silently, at debug level, and leave the partial."""
        (tmp_path / "home.1").mkdir()
        endpoint = LocalEndpoint(config={"path": str(tmp_path)})

        with caplog.at_level(logging.DEBUG):
            operations._cleanup_partial_local_subvolume(
                endpoint, "home.1", created_by_this_run=True
            )

        warnings = _errors(caplog)
        if uid == "root":
            assert not any(MESSAGE in m for m in warnings), warnings
        else:
            assert any(MESSAGE in m and "home.1" in m for m in warnings), warnings
        assert (tmp_path / "home.1").is_dir()


class TestTheSshEndpointsLocalSide:
    @pytest.fixture
    def endpoint(self, monkeypatch):
        monkeypatch.setattr(SSHEndpoint, "_sudo_password_cache", {})
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": "/remote/dest", "username": "u", "ssh_sudo": False}
        ep.hostname = "nas"
        ep.ssh_manager = SimpleNamespace(
            _cached_ssh_password=None, control_path="/tmp/cp"
        )
        ep._cached_sudo_password = None
        return ep

    def _ready_to_send(self, endpoint, monkeypatch):
        endpoint.artifact_exists = MagicMock(return_value=False)
        endpoint._run_diagnostics = MagicMock(
            return_value={
                "ssh_connection": True,
                "btrfs_command": True,
                "write_permissions": True,
                "btrfs_filesystem": True,
                "passwordless_sudo": True,
            }
        )
        endpoint._find_buffer_program = MagicMock(return_value=(None, None))
        started: list = []
        monkeypatch.setattr(
            "btrfs_backup_ng.endpoint.ssh.subprocess.Popen",
            lambda *a, **k: started.append(a),
        )
        return started

    def test_the_direct_send_stops_with_the_reason(
        self, endpoint, tmp_path, monkeypatch, not_root
    ):
        started = self._ready_to_send(endpoint, monkeypatch)
        _take_sudo_away(tmp_path, monkeypatch)
        with pytest.raises(__util__.AbortError, match=MESSAGE):
            endpoint._try_direct_transfer(
                source_path=str(tmp_path), dest_path="/remote/dest", snapshot_name="s"
            )
        assert started == []

    def test_the_whole_send_is_not_retried(
        self, endpoint, tmp_path, monkeypatch, not_root
    ):
        """send_receive retries a False as a transient network fault, three
        attempts and seconds of waiting; a missing sudo is not transient."""
        attempts: list = []

        def refuse(**_k):
            attempts.append(1)
            raise __util__.AbortError(__util__.local_elevation_problem() or MESSAGE)

        endpoint._try_direct_transfer = refuse
        endpoint._require_remote_destination = MagicMock(return_value=True)
        endpoint._run_diagnostics = MagicMock(
            return_value={
                "ssh_connection": True,
                "btrfs_command": True,
                "write_permissions": True,
                "btrfs_filesystem": True,
                "passwordless_sudo": True,
            }
        )
        snapshot = SimpleNamespace(get_path=lambda: tmp_path, get_name=lambda: "s")
        _take_sudo_away(tmp_path, monkeypatch)
        with pytest.raises(__util__.AbortError, match=MESSAGE):
            endpoint.send_receive(snapshot)
        assert len(attempts) == 1

    def test_priming_as_root_asks_no_sudo(self, endpoint, no_sudo, monkeypatch):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        asked: list = []
        endpoint._get_sudo_password = lambda **k: asked.append(1) or "pw"
        assert endpoint._prime_local_sudo() is True
        assert asked == []

    def test_priming_without_sudo_stops_with_the_reason(
        self, endpoint, no_sudo, not_root
    ):
        asked: list = []
        endpoint._get_sudo_password = lambda **k: asked.append(1) or "pw"
        with pytest.raises(__util__.AbortError, match=MESSAGE):
            endpoint._prime_local_sudo()
        assert asked == []

    def test_the_shell_pipeline_builds_no_sudo_for_root(
        self, endpoint, no_sudo, monkeypatch
    ):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        launched: list = []

        def fake_popen(cmd, **kwargs):
            launched.append(cmd)
            proc = MagicMock()
            proc.stderr = SimpleNamespace(read=lambda _n=-1: b"")
            proc.wait.return_value = 1
            proc.returncode = 1
            return proc

        endpoint._estimate_snapshot_size = MagicMock(return_value=None)
        endpoint._check_command_exists = MagicMock(return_value=False)
        endpoint._verify_snapshot_exists = MagicMock(return_value=False)
        endpoint._cleanup_partial_subvolume = MagicMock()
        monkeypatch.setattr(
            "btrfs_backup_ng.endpoint.ssh.shutil.which", lambda n: f"/usr/bin/{n}"
        )
        monkeypatch.setattr("btrfs_backup_ng.endpoint.ssh.subprocess.Popen", fake_popen)
        endpoint._do_shell_pipeline_transfer(
            source_path="/src/snapshot", dest_path="/d/5", snapshot_name="snapshot"
        )
        (cmd,) = launched
        assert "sudo" not in cmd and "btrfs send" in cmd


class TestTheRunStartsWithTheAnswer:
    # cli/run.py logs through a module logger that propagates to the root, so
    # caplog sees its records on every pytest version once create_logger,
    # which replaces the root's handlers, is patched out as the prune tests do.
    def test_run_says_so_before_touching_anything(
        self, minimal_config_file, tmp_path, monkeypatch, not_root, caplog
    ):
        from btrfs_backup_ng.cli import run as run_cli

        monkeypatch.setattr(run_cli, "create_logger", lambda *_a, **_k: None)
        monkeypatch.setattr(
            run_cli, "find_config_file", lambda *_a, **_k: minimal_config_file
        )
        locked: list = []
        monkeypatch.setattr(
            run_cli,
            "_run_lock",
            lambda *_a: locked.append(1) or contextlib.nullcontext(),
        )
        _take_sudo_away(tmp_path, monkeypatch)
        args = SimpleNamespace(
            config=None,
            dry_run=False,
            verbose=False,
            quiet=False,
            debug=False,
            log_level=None,
        )

        with caplog.at_level(logging.ERROR, logger="btrfs_backup_ng.cli.run"):
            rc = run_cli.execute_run(args)

        assert rc == 1
        errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert any(MESSAGE in m for m in errors), errors
        assert locked == [], (
            "the run lock was taken before the machine was found unable"
        )


class TestDoctor:
    @staticmethod
    def _finding():
        from btrfs_backup_ng.core.doctor import Doctor

        # The check reads no state of the Doctor, so no configuration is needed.
        (finding,) = Doctor._check_local_privilege(Doctor.__new__(Doctor))
        return finding

    def test_root_is_fine(self, no_sudo, monkeypatch):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        finding = self._finding()
        assert finding.severity.value == "ok" and "root" in finding.message

    def test_no_sudo_is_an_error_with_the_remedy(self, no_sudo, not_root):
        finding = self._finding()
        assert finding.severity.value == "error"
        assert (
            MESSAGE in finding.details["hint"] and "as root" in finding.details["hint"]
        )

    def test_a_refusing_sudo_is_noted_not_an_error(self, not_root, sudo_refuses):
        finding = self._finding()
        assert finding.severity.value == "info" and "password" in finding.message
