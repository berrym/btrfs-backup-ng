"""R12c -- control-socket dir + command-lock path hardening.

P2: the ControlMaster socket dir is now an UNPREDICTABLE, 0700, euid-owned mkdtemp dir
(prefer $XDG_RUNTIME_DIR base), removed on cleanup -- the old predictable
/tmp/ssh-controlmasters-<user> + mkdir(exist_ok=True) was a socket-hijack vector.

P5: the per-user btrfs-command lock lives in a euid-owned dir when possible, and FAILS
CLOSED (never follows) a symlink planted at the /tmp fallback path.
"""

import os
import stat

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint import common as common_mod
from btrfs_backup_ng.endpoint.common import _command_lock_path
from btrfs_backup_ng.sshutil import master as master_mod
from btrfs_backup_ng.sshutil.master import SSHMasterManager, _control_dir_base


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("SUDO_USER", raising=False)


# ------------------------------------ P2: control-socket dir


def _mgr():
    return SSHMasterManager(hostname="h", username="u")


def test_control_dir_is_unpredictable_and_0700():
    """mkdtemp -> random name + 0700 + euid-owned. Mutation guard: revert to the predictable
    /tmp/ssh-controlmasters-<user> and the random-suffix assertion fails."""
    m = _mgr()
    try:
        assert m._own_control_dir is True
        assert "btrfs-backup-ng-cm-" in m.control_dir.name
        st = m.control_dir.stat()
        assert stat.S_IMODE(st.st_mode) == 0o700
        assert st.st_uid == os.geteuid()
    finally:
        m.cleanup_socket()


def test_two_managers_get_distinct_control_dirs():
    a, b = _mgr(), _mgr()
    try:
        assert a.control_dir != b.control_dir  # unpredictable -> no collision/hijack
    finally:
        a.cleanup_socket()
        b.cleanup_socket()


def test_cleanup_removes_owned_control_dir():
    m = _mgr()
    d = m.control_dir
    assert d.exists()
    m.cleanup_socket()
    assert not d.exists()  # our private dir is torn down


def test_explicit_control_dir_is_respected_and_not_removed(tmp_path):
    """An explicit control_dir override is used as-is and NEVER removed by cleanup."""
    d = tmp_path / "explicit"
    m = SSHMasterManager(hostname="h", username="u", control_dir=str(d))
    assert m._own_control_dir is False
    assert m.control_dir == d
    m.cleanup_socket()
    assert d.exists()  # not ours to remove


def test_control_dir_base_requires_euid_ownership(tmp_path, monkeypatch):
    """$XDG_RUNTIME_DIR is used as the mkdtemp base only when euid-owns it -- so under sudo
    we never place root's socket inside a non-root user's runtime dir."""
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    assert _control_dir_base() == str(tmp_path)  # owned by us -> used
    monkeypatch.setattr(master_mod.os, "geteuid", lambda: 999999)
    assert _control_dir_base() is None  # not euid-owned -> refused


# ------------------------------------ P5: command-lock path


def test_command_lock_path_prefers_owned_xdg(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    p = _command_lock_path()
    assert p == tmp_path / f"btrfs-backup-ng-{os.geteuid()}" / "command.lock"
    assert p.parent.exists() and stat.S_IMODE(p.parent.stat().st_mode) == 0o700


def test_command_lock_path_falls_back_to_secure_tmp_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    monkeypatch.setattr(common_mod.tempfile, "gettempdir", lambda: str(tmp_path))
    p = _command_lock_path()
    # lock lives INSIDE a euid-owned 0700 dir (not bare in /tmp) -> no plantable symlink path
    assert p == tmp_path / f"btrfs-backup-ng-{os.geteuid()}" / "command.lock"
    assert stat.S_IMODE(p.parent.stat().st_mode) == 0o700


def test_command_lock_path_refuses_symlinked_dir_fail_closed(tmp_path, monkeypatch):
    """THE P5 guard: if the per-euid lock DIR is a symlink (or foreign-owned), refuse --
    never place the lock through an attacker path. Mutation guard: drop the lstat S_ISDIR/
    ownership check and this raises nothing."""
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    monkeypatch.setattr(common_mod.tempfile, "gettempdir", lambda: str(tmp_path))
    victim = tmp_path / "victim_dir"
    victim.mkdir()
    (tmp_path / f"btrfs-backup-ng-{os.geteuid()}").symlink_to(victim)
    with pytest.raises(__util__.AbortError):
        _command_lock_path()


# ------------------------------------ P2: deterministic cleanup on stop_master


def test_stop_master_removes_owned_control_dir(monkeypatch):
    """stop_master() tears the mkdtemp control dir down deterministically (not only via the
    atexit backstop). Mutation guard: drop the _cleanup_control_dir() call in stop_master."""
    import subprocess as sp

    m = _mgr()
    d = m.control_dir
    m._master_started = True  # pretend a master is running
    monkeypatch.setattr(
        master_mod.subprocess, "run", lambda *a, **k: sp.CompletedProcess(a, 0)
    )
    assert m.stop_master() is True
    assert not d.exists()
