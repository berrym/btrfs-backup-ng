"""R12a -- SSH host-key hardening.

- P1: the Paramiko password-sudo transport now loads the operator's known_hosts BEFORE
  connecting, so a changed key is refused (BadHostKeyException) and a genuinely new host is
  trust-on-first-use pinned -- accept-new parity. The old code loaded nothing, so
  AutoAddPolicy blindly accepted ANY key (including a changed one) -- a fail-open MITM on the
  path that carries the SSH + sudo passwords.
- P3: operator-supplied ssh_opts are applied on the primary subprocess transport, and FIRST
  (ssh uses the first value -> operator hardening wins). They were silently dropped.
- P4: UserKnownHostsFile points at the OPERATOR's known_hosts (even under sudo), so accept-new
  verifies/pins against the operator's curated trust, not root's empty store.

The changed-key -> refused behaviour itself is OpenSSH's guarantee once the store is loaded,
and is validated end-to-end on real hardware; here the enforcement guard is that the store IS
loaded (revert the load -> fail-open returns -> the wiring test fails).
"""

import os
import pwd
import stat
from pathlib import Path

import pytest

from btrfs_backup_ng.sshutil import master as master_mod
from btrfs_backup_ng.sshutil.master import (
    SSHMasterManager,
    ensure_operator_known_hosts,
    operator_ssh_dir,
)


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path, monkeypatch):
    # Constructing the manager touches ~/.ssh; point HOME at a temp dir and clear SUDO_USER
    # so tests never touch the real home and run on a fresh account/CI runner.
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("SUDO_USER", raising=False)


# ------------------------------------ operator known_hosts resolution (P4 foundation)


def test_operator_ssh_dir_non_sudo_uses_home(tmp_path):
    assert operator_ssh_dir() == tmp_path / ".ssh"


def test_operator_ssh_dir_under_sudo_uses_sudo_user(monkeypatch):
    """Under sudo the operator's (SUDO_USER's) ~/.ssh is used, NOT root's -- so host-key
    trust matches what the operator curated. Mutation guard: ignore SUDO_USER -> returns
    Path.home()/.ssh (the temp HOME) and this fails."""
    user = pwd.getpwuid(os.getuid()).pw_name
    monkeypatch.setenv("SUDO_USER", user)
    monkeypatch.setattr(master_mod.os, "geteuid", lambda: 0)
    assert operator_ssh_dir() == Path(pwd.getpwnam(user).pw_dir) / ".ssh"


def test_ensure_operator_known_hosts_creates_0600_file(tmp_path):
    kh = ensure_operator_known_hosts()
    assert kh == tmp_path / ".ssh" / "known_hosts"
    assert kh.exists()
    assert stat.S_IMODE(kh.stat().st_mode) == 0o600


def test_ensure_known_hosts_refuses_to_chown_through_symlink_under_sudo(
    tmp_path, monkeypatch
):
    """CRITICAL guard: under sudo (as root), a symlink planted at the operator's known_hosts
    must NOT be followed/chowned -- else a sudo user escalates by symlinking it to a
    root-owned file. Mutation guard: revert to stat()+os.chown() (follow symlinks) -> with
    the target faked root-owned, os.chown fires -> this fails."""
    user = pwd.getpwuid(os.getuid()).pw_name
    monkeypatch.setenv("SUDO_USER", user)
    monkeypatch.setattr(master_mod.os, "geteuid", lambda: 0)
    monkeypatch.setattr(master_mod, "operator_ssh_dir", lambda: tmp_path / ".ssh")
    (tmp_path / ".ssh").mkdir()
    victim = tmp_path / "victim"
    victim.write_text("root-owned-secret")
    kh = tmp_path / ".ssh" / "known_hosts"
    kh.symlink_to(victim)

    # Simulate the symlink TARGET being root-owned (the escalation precondition) so a naive
    # stat()+chown() would fire. Only the follow-symlinks stat() (what the old buggy code
    # used) is faked; follow_symlinks=False (pathlib lstat / islink) delegates to the real
    # stat so os.path.islink still works.
    real_stat = os.stat

    def fake_stat(p, *a, follow_symlinks=True, **k):
        if str(p) == str(kh) and follow_symlinks:
            return type("S", (), {"st_uid": 0})()
        return real_stat(p, *a, follow_symlinks=follow_symlinks, **k)

    monkeypatch.setattr(master_mod.os, "stat", fake_stat)
    chowns = []
    monkeypatch.setattr(master_mod.os, "chown", lambda *a, **k: chowns.append(a))
    monkeypatch.setattr(master_mod.os, "fchown", lambda *a, **k: chowns.append(a))

    ensure_operator_known_hosts()

    assert chowns == []  # never chowned through the symlink
    assert os.path.islink(kh)  # the planted symlink was not replaced
    assert victim.read_text() == "root-owned-secret"  # victim untouched


def test_ensure_known_hosts_fchowns_only_the_fresh_file_under_sudo(
    tmp_path, monkeypatch
):
    """The legit path: under sudo a freshly-created known_hosts is chowned to the OPERATOR
    via its fd (symlink-safe). Mutation guard: drop the fchown -> not recorded."""
    user = pwd.getpwuid(os.getuid()).pw_name
    pw = pwd.getpwnam(user)
    monkeypatch.setenv("SUDO_USER", user)
    monkeypatch.setattr(master_mod.os, "geteuid", lambda: 0)
    monkeypatch.setattr(master_mod, "operator_ssh_dir", lambda: tmp_path / ".ssh")
    fchowns = []
    monkeypatch.setattr(
        master_mod.os, "fchown", lambda fd, uid, gid: fchowns.append((uid, gid))
    )

    kh = ensure_operator_known_hosts()

    assert kh.exists() and not kh.is_symlink()
    assert fchowns == [(pw.pw_uid, pw.pw_gid)]


# ------------------------------------ P4: UserKnownHostsFile on the primary transport


def _mgr(**kw):
    return SSHMasterManager(hostname="h", username="u", **kw)


def test_ssh_base_cmd_sets_user_known_hosts_file_under_sudo(monkeypatch):
    """Under sudo the subprocess transport pins to the OPERATOR's known_hosts (was unset ->
    ssh, as root, used root's store). Mutation guard: drop the sudo-branch append -> absent."""
    user = pwd.getpwuid(os.getuid()).pw_name
    monkeypatch.setenv("SUDO_USER", user)
    monkeypatch.setattr(master_mod.os, "geteuid", lambda: 0)
    operator_kh = Path(pwd.getpwnam(user).pw_dir) / ".ssh" / "known_hosts"
    cmd = _mgr()._ssh_base_cmd()
    assert f"UserKnownHostsFile={operator_kh}" in cmd


def test_ssh_base_cmd_leaves_known_hosts_alone_non_sudo(tmp_path):
    """Non-sudo, ssh already uses the operator's ~/.ssh/known_hosts and respects ssh_config,
    so we do NOT force UserKnownHostsFile (avoids overriding a power-user's ssh_config)."""
    cmd = _mgr()._ssh_base_cmd()
    assert not any(c.startswith("UserKnownHostsFile=") for c in cmd)


# ------------------------------------ P3: operator ssh_opts applied, and FIRST (they win)


def test_ssh_base_cmd_applies_operator_ssh_opts_first():
    """Operator ssh_opts are honored on the primary transport (were silently dropped) and
    precede the tool default so ssh's first-value-wins gives the operator override effect.
    Mutation guard: drop `list(self.ssh_opts)` -> the operator opt is absent."""
    cmd = _mgr(ssh_opts=["StrictHostKeyChecking=yes"])._ssh_base_cmd()
    assert "StrictHostKeyChecking=yes" in cmd
    assert cmd.index("StrictHostKeyChecking=yes") < cmd.index(
        "StrictHostKeyChecking=accept-new"
    )


# ------------------------------------ P1: Paramiko loads the operator known_hosts (no fail-open)


# ------------------------------------ P1: the BadHostKeyException handler refuses loudly
