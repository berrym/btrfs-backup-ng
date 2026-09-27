"""SSH-agent discovery under sudo (SSHMasterManager).

Backups run under sudo (btrfs send/receive need root), and sudo strips SSH_AUTH_SOCK.
A passphrase-protected key can only sign via its ssh-agent, so if the agent socket can't
be located the server accepts the offered public key but the client cannot sign it ->
"Permission denied". These tests pin the discovery + override + error behavior.
"""

from __future__ import annotations

import os
import shutil
import socket
import tempfile
from pathlib import Path

import pytest

from btrfs_backup_ng.sshutil.master import SSHMasterManager


@pytest.fixture
def short_tmp():
    """A temporary directory with a SHORT path, for the real sockets bound here.

    A Unix socket path is limited to 108 bytes. pytest's tmp_path grows with the
    base temp directory and the test's name, and under a long base (`--basetemp`
    somewhere deep, or a TMPDIR like one) `bind` fails with "AF_UNIX path too
    long" -- a failure of the test's setup, not of the discovery under test.
    """
    base = "/tmp" if os.access("/tmp", os.W_OK) else tempfile.gettempdir()
    path = Path(tempfile.mkdtemp(prefix="bbng-", dir=base))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _mgr(short_tmp, **kw):
    # control_dir kept in tmp so tests never touch the real ~/.ssh.
    return SSHMasterManager(
        hostname="testhost", control_dir=str(short_tmp / "cm"), **kw
    )


def _real_socket(path):
    """Bind a real AF_UNIX socket at path and return it (keep a ref so it stays open)."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.bind(str(path))
    return s


# --------------------------------------------------------------------------- #
# _owned_socket (type + ownership + symlink handling)
# --------------------------------------------------------------------------- #
def test_owned_socket_true_for_owned_socket(short_tmp):
    sock = _real_socket(short_tmp / "s.sock")
    try:
        assert (
            SSHMasterManager._owned_socket(
                str(short_tmp / "s.sock"), os.getuid(), follow=False
            )
            is True
        )
    finally:
        sock.close()


def test_owned_socket_false_for_regular_file_missing_and_none(short_tmp):
    f = short_tmp / "regular"
    f.write_text("x")
    uid = os.getuid()
    assert SSHMasterManager._owned_socket(str(f), uid, follow=False) is False
    assert (
        SSHMasterManager._owned_socket(str(short_tmp / "no"), uid, follow=False)
        is False
    )
    assert SSHMasterManager._owned_socket(None, uid, follow=False) is False


def test_owned_socket_rejects_wrong_owner(short_tmp):
    """A socket owned by neither the target uid nor root is rejected (security gate)."""
    sock = _real_socket(short_tmp / "s.sock")
    try:
        # uid nobody-ish: use a uid that is neither ours nor 0.
        other = os.getuid() + 12345
        assert (
            SSHMasterManager._owned_socket(
                str(short_tmp / "s.sock"), other, follow=False
            )
            is False
        )
    finally:
        sock.close()


def test_owned_socket_no_follow_rejects_symlink(short_tmp):
    """A symlink (even to a real owned socket) is rejected in no-follow mode, closing the
    TOCTOU/redirection class in auto-discovery. Mutation guard: switching discovery to
    os.stat (follow) would accept the symlink and fail this."""
    real = _real_socket(short_tmp / "real.sock")
    link = short_tmp / "link.sock"
    link.symlink_to(short_tmp / "real.sock")
    try:
        uid = os.getuid()
        assert SSHMasterManager._owned_socket(str(link), uid, follow=False) is False
        # ...but follow=True (a user-pinned override) accepts it via the real target.
        assert SSHMasterManager._owned_socket(str(link), uid, follow=True) is True
    finally:
        real.close()


# --------------------------------------------------------------------------- #
# _resolve_agent_socket precedence
# --------------------------------------------------------------------------- #
def test_explicit_override_wins_over_env(short_tmp):
    override = _real_socket(short_tmp / "override.sock")
    envsock = _real_socket(short_tmp / "env.sock")
    try:
        mgr = _mgr(short_tmp, ssh_auth_sock=str(short_tmp / "override.sock"))
        got = mgr._resolve_agent_socket(
            os.getuid(), {"SSH_AUTH_SOCK": str(short_tmp / "env.sock")}
        )
        assert got == str(short_tmp / "override.sock")
    finally:
        override.close()
        envsock.close()


def test_invalid_override_falls_back_to_env(short_tmp):
    envsock = _real_socket(short_tmp / "env.sock")
    try:
        mgr = _mgr(short_tmp, ssh_auth_sock=str(short_tmp / "does-not-exist.sock"))
        got = mgr._resolve_agent_socket(
            os.getuid(), {"SSH_AUTH_SOCK": str(short_tmp / "env.sock")}
        )
        assert got == str(short_tmp / "env.sock")
    finally:
        envsock.close()


def test_preserved_env_socket_used_when_no_override(short_tmp):
    envsock = _real_socket(short_tmp / "env.sock")
    try:
        mgr = _mgr(short_tmp)
        got = mgr._resolve_agent_socket(
            os.getuid(), {"SSH_AUTH_SOCK": str(short_tmp / "env.sock")}
        )
        assert got == str(short_tmp / "env.sock")
    finally:
        envsock.close()


def test_falls_through_to_discovery_when_nothing_explicit(short_tmp, monkeypatch):
    mgr = _mgr(short_tmp)
    monkeypatch.setattr(mgr, "_find_ssh_agent_socket", lambda uid, env: "DISCOVERED")
    got = mgr._resolve_agent_socket(os.getuid(), {})  # no override, no env
    assert got == "DISCOVERED"


# --------------------------------------------------------------------------- #
# _find_ssh_agent_socket searches ~/.ssh/agent/ (the real-world gap)
# --------------------------------------------------------------------------- #
def _reachable_only_under(mgr, monkeypatch, base):
    """Make _agent_status treat only sockets under `base` as a reachable-but-empty agent
    (status 1) and everything else (real system sockets on the box) as dead (status 2), so
    discovery tests are isolated from the tester's real agents."""
    monkeypatch.setattr(mgr, "_agent_status", lambda s: 1 if str(base) in str(s) else 2)


def test_discovery_finds_socket_in_dot_ssh_agent(short_tmp, monkeypatch):
    """A socket under <home>/.ssh/agent/ must be discovered. Mutation guard: removing the
    ~/.ssh/agent/* entry from search_paths makes this return None."""
    home = short_tmp / "home"
    agent_dir = home / ".ssh" / "agent"
    agent_dir.mkdir(parents=True)
    sock = _real_socket(agent_dir / "s.abc.agent.def")
    try:
        uid = os.getuid()

        class _PW:
            pw_dir = str(home)

        monkeypatch.setattr(
            "btrfs_backup_ng.sshutil.master.pwd.getpwuid", lambda u: _PW()
        )
        mgr = _mgr(short_tmp)
        _reachable_only_under(mgr, monkeypatch, home)
        got = mgr._find_ssh_agent_socket(uid, {})
        # The agent is reachable but has no keys, so it comes from pass 2.
        assert got == str(agent_dir / "s.abc.agent.def")
        assert mgr._agent_socket_had_keys is False  # empty-agent path recorded
    finally:
        sock.close()


def test_discovery_skips_non_socket_files(short_tmp, monkeypatch):
    """A regular file at a search path (e.g. ~/.ssh/*.sock) must NOT be returned as an
    agent socket. The regular file sorts BEFORE the real socket, so only the socket-type
    check can make discovery skip it and return the real socket. Mutation guard: dropping
    the _owned_socket type check in pass 2 returns the (earlier-sorting) regular file."""
    home = short_tmp / "home"
    ssh_dir = home / ".ssh"
    ssh_dir.mkdir(parents=True)
    (ssh_dir / "a-notasocket.sock").write_text("regular file")  # sorts first
    real = _real_socket(ssh_dir / "z-real.sock")  # sorts after
    try:

        class _PW:
            pw_dir = str(home)

        monkeypatch.setattr(
            "btrfs_backup_ng.sshutil.master.pwd.getpwuid", lambda u: _PW()
        )
        mgr = _mgr(short_tmp)
        _reachable_only_under(mgr, monkeypatch, home)
        got = mgr._find_ssh_agent_socket(os.getuid(), {})
        assert got == str(ssh_dir / "z-real.sock")
    finally:
        real.close()


def test_discovery_skips_dead_socket(short_tmp, monkeypatch):
    """A socket whose agent is dead/unreachable (ssh-add rc 2) must NOT be chosen -- setting
    SSH_AUTH_SOCK to a dead socket would only slow down the fall-through to password auth.
    Mutation guard: accepting any reachability status in pass 2 returns the dead socket."""
    home = short_tmp / "home"
    agent_dir = home / ".ssh" / "agent"
    agent_dir.mkdir(parents=True)
    sock = _real_socket(agent_dir / "dead.sock")
    try:

        class _PW:
            pw_dir = str(home)

        monkeypatch.setattr(
            "btrfs_backup_ng.sshutil.master.pwd.getpwuid", lambda u: _PW()
        )
        mgr = _mgr(short_tmp)
        monkeypatch.setattr(mgr, "_agent_status", lambda s: 2)  # everything dead
        assert mgr._find_ssh_agent_socket(os.getuid(), {}) is None
    finally:
        sock.close()


def test_discovery_home_fallback_uses_env_not_root(short_tmp, monkeypatch):
    """When pwd.getpwuid fails (containers/NSS), the home fallback must come from env HOME,
    not os.path.expanduser('~') (which under sudo is root's home). Mutation guard: reverting
    the fallback to os.path.expanduser makes this miss the socket."""
    home = short_tmp / "userhome"
    agent_dir = home / ".ssh" / "agent"
    agent_dir.mkdir(parents=True)
    sock = _real_socket(agent_dir / "a.sock")
    try:

        def _boom(u):
            raise KeyError("uid not in passwd db")

        monkeypatch.setattr("btrfs_backup_ng.sshutil.master.pwd.getpwuid", _boom)
        mgr = _mgr(short_tmp)
        _reachable_only_under(mgr, monkeypatch, home)
        got = mgr._find_ssh_agent_socket(os.getuid(), {"HOME": str(home)})
        assert got == str(agent_dir / "a.sock")
    finally:
        sock.close()


# --------------------------------------------------------------------------- #
# BTRFS_BACKUP_SSH_AUTH_SOCK env override picked up at construction
# --------------------------------------------------------------------------- #
def test_env_var_sets_explicit_override(short_tmp, monkeypatch):
    monkeypatch.setenv("BTRFS_BACKUP_SSH_AUTH_SOCK", "/tmp/some/agent.sock")
    mgr = _mgr(short_tmp)  # no ssh_auth_sock kwarg
    assert mgr.ssh_auth_sock == "/tmp/some/agent.sock"


def test_explicit_kwarg_beats_env_var(short_tmp, monkeypatch):
    monkeypatch.setenv("BTRFS_BACKUP_SSH_AUTH_SOCK", "/tmp/from/env.sock")
    mgr = _mgr(short_tmp, ssh_auth_sock="/tmp/from/kwarg.sock")
    assert mgr.ssh_auth_sock == "/tmp/from/kwarg.sock"


# --------------------------------------------------------------------------- #
# Actionable auth-failure guidance
# --------------------------------------------------------------------------- #
def test_auth_failure_help_under_sudo_no_agent(short_tmp, monkeypatch):
    """When no agent was found under sudo, the guidance must name the sudo/env fix.
    Mutation guard: dropping the running_as_sudo branch removes the preserve-env hint."""
    mgr = _mgr(short_tmp)
    mgr.running_as_sudo = True
    mgr._resolved_agent_sock = None
    msgs = []
    monkeypatch.setattr(
        "btrfs_backup_ng.sshutil.master.logger.info",
        lambda msg, *a: msgs.append(msg % a if a else msg),
    )
    mgr._log_auth_failure_help()
    joined = "\n".join(msgs)
    # The preserve-env hint appears ONLY in the running_as_sudo branch.
    assert "preserve-env" in joined
    assert "ssh_auth_sock" in joined


def test_auth_failure_help_when_agent_was_used(short_tmp, monkeypatch):
    """If an agent WAS used (with keys) but rejected, the guidance says so (points at
    ssh-add -l), not the sudo hint. Mutation guard: ignoring _resolved_agent_sock prints
    the wrong branch."""
    mgr = _mgr(short_tmp)
    mgr._resolved_agent_sock = "/run/agent.sock"
    mgr._agent_socket_had_keys = True
    msgs = []
    monkeypatch.setattr(
        "btrfs_backup_ng.sshutil.master.logger.info",
        lambda msg, *a: msgs.append(msg % a if a else msg),
    )
    mgr._log_auth_failure_help()
    joined = "\n".join(msgs)
    assert "/run/agent.sock" in joined
    assert "ssh-add -l" in joined


def test_auth_failure_help_when_agent_had_no_keys(short_tmp, monkeypatch):
    """A found-but-empty agent must produce a distinct 'no keys loaded, run ssh-add'
    message, not the 'server rejected the key' one. Mutation guard: collapsing the
    had_keys branch prints the wrong guidance."""
    mgr = _mgr(short_tmp)
    mgr._resolved_agent_sock = "/run/empty.sock"
    mgr._agent_socket_had_keys = False
    msgs = []
    monkeypatch.setattr(
        "btrfs_backup_ng.sshutil.master.logger.info",
        lambda msg, *a: msgs.append(msg % a if a else msg),
    )
    mgr._log_auth_failure_help()
    joined = "\n".join(msgs)
    assert "NO keys loaded" in joined
    assert "ssh-add" in joined
