"""Snapper slot operations ask for the least privilege the destination allows.

The sudoers policy this project documents grants ``NOPASSWD: /usr/bin/btrfs`` and nothing else.
Under it ``sudo sh`` is refused outright -- verified on a real host::

    $ sudo -n sh -c 'echo hi'
    sudo: a password is required                       # rc=1

So wrapping the whole snapper script in sudo cannot work there. Only the btrfs verbs genuinely
need privilege; the slot layout is manipulated with directory renames and removals, which need
no privilege at all when the connecting user can write the ``.snapshots`` tree.

Validated on real hardware: with the tree reachable by the connecting user, ``mv`` (publish),
``mv`` (move-aside), ``cd`` + glob and ``rm -rf`` all return 0 unprivileged, while
``btrfs subvolume show/delete/receive`` succeed via ``sudo -n``. Renaming the CONTAINING
directory relocates a root-owned read-only subvolume without touching it, which is why the
publish sequence works without privilege.

Access can come from ownership or from an ACL. ``setfacl -m u:<user>:rwx`` on a root-owned
0700 ``.snapshots`` turns every one of those operations from failure into success, which is
also how snapper itself grants users access via ALLOW_USERS -- so existing root-owned
destinations need one ACL rather than a chown.
"""

from __future__ import annotations

import shlex
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.core.operations as ops


def _remote_endpoint(writable: bool, ssh_sudo: bool = True):
    """A remote endpoint whose destination is or is not user-writable."""
    calls: list[list[str]] = []

    def fake_exec(command, **kwargs):
        cmd = [str(c) for c in command]
        calls.append(cmd)
        script = cmd[-1]
        if "[ -w " in script:  # the writability probe
            return SimpleNamespace(
                returncode=0 if writable else 1, stdout=b"", stderr=b""
            )
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    ep = SimpleNamespace(
        _is_remote=True,
        config={"path": "/mnt/backup", "ssh_sudo": ssh_sudo},
        _exec_remote_command=MagicMock(side_effect=fake_exec),
        _prime_remote_sudo=MagicMock(),
    )
    return ep, calls


def _script_calls(calls):
    """Dispatches of the script under test, excluding the probe and the sudo capability check."""
    return [c for c in calls if "sh" in c and "-c" in c and "[ -w " not in c[-1]]


class TestShellPrivilegeFollowsDestinationAccess:
    def test_writable_destination_runs_the_shell_unprivileged(self):
        ep, calls = _remote_endpoint(writable=True)
        ops._snapper_run_shell(ep, "echo work")

        sent = _script_calls(calls)
        assert sent, calls
        assert sent[0][0] == "sh", (
            f"the shell was elevated despite a writable destination: {sent[0]!r}. "
            "A btrfs-only sudoers policy refuses `sudo sh`."
        )
        assert "sudo" not in sent[0]

    def test_unwritable_destination_falls_back_to_a_privileged_shell(self):
        """Existing root-owned destinations must keep working, not break on upgrade."""
        ep, calls = _remote_endpoint(writable=False)
        ops._snapper_run_shell(ep, "echo work")

        sent = _script_calls(calls)
        assert sent, calls
        assert sent[0][:4] == ["sudo", "-n", "sh", "-c"], sent[0]

    def test_script_is_raw_in_both_modes(self):
        script = 'set -e; echo "a b"; echo c'
        for writable in (True, False):
            ep, calls = _remote_endpoint(writable=writable)
            ops._snapper_run_shell(ep, script)
            sent = _script_calls(calls)
            assert sent[0][-1] == script, (writable, sent[0])
            assert sent[0][-1] != shlex.quote(script), writable

    def test_writability_is_probed_once_per_endpoint(self):
        ep, calls = _remote_endpoint(writable=True)
        for _ in range(3):
            ops._snapper_run_shell(ep, "echo work")
        probes = [c for c in calls if "[ -w " in c[-1]]
        assert len(probes) == 1, f"probed {len(probes)} times: {probes}"

    def test_unwritable_destination_advises_the_acl_grant(self, caplog):
        ep, _calls = _remote_endpoint(writable=False)
        with caplog.at_level("INFO"):
            ops._snapper_run_shell(ep, "echo work")
        joined = " ".join(r.getMessage() for r in caplog.records)
        assert "setfacl" in joined, joined
        assert "/mnt/backup/.snapshots" in joined, joined


class TestBtrfsVerbsCarryTheirOwnSudo:
    def test_remote_with_ssh_sudo(self):
        ep, _ = _remote_endpoint(writable=True, ssh_sudo=True)
        assert ops._snapper_btrfs(ep) == "sudo -n btrfs"

    def test_remote_without_ssh_sudo(self):
        ep, _ = _remote_endpoint(writable=True, ssh_sudo=False)
        assert ops._snapper_btrfs(ep) == "btrfs"

    def test_local_as_root_needs_no_sudo(self, monkeypatch):
        monkeypatch.setattr(ops.os, "geteuid", lambda: 0)
        assert (
            ops._snapper_btrfs(SimpleNamespace(_is_remote=False, config={})) == "btrfs"
        )

    def test_local_as_user_elevates_btrfs_only(self, monkeypatch):
        monkeypatch.setattr(ops.os, "geteuid", lambda: 1000)
        assert (
            ops._snapper_btrfs(SimpleNamespace(_is_remote=False, config={}))
            == "sudo -n btrfs"
        )


class TestGeneratedScriptsElevateOnlyBtrfs:
    """The scripts themselves must carry sudo on btrfs and nothing else."""

    @staticmethod
    def _capture_script(fn, *args):
        captured = {}

        def fake_run(endpoint, script):
            captured["script"] = script
            return 0, ""

        original = ops._snapper_run_shell
        ops._snapper_run_shell = fake_run
        try:
            fn(*args)
        finally:
            ops._snapper_run_shell = original
        return captured["script"]

    def _endpoint(self):
        ep, _ = _remote_endpoint(writable=True, ssh_sudo=True)
        return ep

    def test_enumerate_script(self):
        script = self._capture_script(
            ops._enumerate_snapper_btrfs_backups, self._endpoint()
        )
        assert "sudo -n btrfs subvolume show" in script, script
        self._assert_only_btrfs_is_elevated(script)

    def test_cleanup_script(self):
        ep = self._endpoint()
        script = self._capture_script(ops._cleanup_snapper_backup, ep, 7, False)
        assert "sudo -n btrfs subvolume delete" in script, script
        self._assert_only_btrfs_is_elevated(script)

    @staticmethod
    def _assert_only_btrfs_is_elevated(script: str):
        """Every `sudo` in the script must be immediately followed by btrfs."""
        tokens = script.split()
        for i, token in enumerate(tokens):
            if token == "sudo":
                rest = [t for t in tokens[i + 1 :] if not t.startswith("-")]
                assert rest and rest[0] == "btrfs", (
                    f"sudo elevates a non-btrfs binary ({rest[:1]}), which a "
                    f"/usr/bin/btrfs-only policy refuses: {script}"
                )


def test_capability_probe_uses_btrfs_not_true():
    """`sudo -n true` fails under a btrfs-only policy and would trigger pointless priming."""
    ep, calls = _remote_endpoint(writable=False)
    ops._snapper_run_shell(ep, "echo work")

    probes = [c for c in calls if "sudo" in c and "sh" not in c]
    assert probes, f"no capability probe was issued: {calls}"
    assert probes[0] == ["sudo", "-n", "btrfs", "--version"], probes[0]
    assert "true" not in probes[0]


@pytest.mark.parametrize("writable", [True, False])
def test_never_raises_on_transport_failure(writable):
    """Best-effort contract: a broken transport degrades, it does not crash the caller."""
    ep, _ = _remote_endpoint(writable=writable)
    ep._exec_remote_command = MagicMock(side_effect=OSError("connection reset"))
    rc, out = ops._snapper_run_shell(ep, "echo work")
    assert rc == 1 and out == ""
