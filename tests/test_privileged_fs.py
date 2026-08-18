"""Privileged filesystem operations: elevate only when the direct route fails.

Five call sites across the backup and restore paths assumed "not running as
root, therefore shell out to sudo". That is wrong twice over. It elevates for a
path the running user already owns, and -- under the sudoers policy this project
documents, NOPASSWD limited to /usr/bin/btrfs -- the shell-out is refused
outright, because mkdir, tee and chmod are not btrfs.

Measured on a real host with exactly that policy:

    sudo -n btrfs --version   -> ALLOWED
    sudo -n mkdir /tmp/probe  -> sudo: a password is required

and a non-root `snapper restore` died with

    Failed to restore snapshot 2: Command '['sudo', 'mkdir', '-p',
    '/mnt/bbng-snapper-test/.snapshots/6']' returned non-zero exit status 1

naming an argv the operator never typed and suggesting no remedy.
"""

from __future__ import annotations

import os
import subprocess
from unittest.mock import patch

import pytest

from btrfs_backup_ng import __util__

pytestmark = pytest.mark.skipif(
    os.geteuid() == 0,
    reason="these pin NON-root behaviour; root can write regardless of mode bits",
)


def _sealed(path):
    """A real directory the current user genuinely cannot write into."""
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o500)
    return path


class TestTheDirectRouteIsTriedFirst:
    def test_a_writable_destination_never_shells_out(self, tmp_path):
        with patch.object(__util__.subprocess, "run") as run:
            __util__.privileged_write_bytes(tmp_path / "f", b"hello")
        assert (tmp_path / "f").read_bytes() == b"hello"
        assert not run.called, "sudo was invoked for a path we can already write"

    def test_mkdir_in_a_writable_parent_never_shells_out(self, tmp_path):
        with patch.object(__util__.subprocess, "run") as run:
            __util__.privileged_mkdir(tmp_path / "a" / "b")
        assert (tmp_path / "a" / "b").is_dir()
        assert not run.called

    def test_chmod_of_an_owned_path_never_shells_out(self, tmp_path):
        target = tmp_path / "d"
        target.mkdir()
        with patch.object(__util__.subprocess, "run") as run:
            __util__.privileged_chmod(target, 0o755)
        assert oct(target.stat().st_mode & 0o777) == oct(0o755)
        assert not run.called

    def test_rmtree_of_an_owned_tree_never_shells_out(self, tmp_path):
        target = tmp_path / "d"
        (target / "sub").mkdir(parents=True)
        with patch.object(__util__.subprocess, "run") as run:
            __util__.privileged_rmtree(target)
        assert not target.exists()
        assert not run.called

    def test_the_uid_does_not_decide_it_ownership_does(self, tmp_path):
        """The old code branched on geteuid() alone. What matters is whether the
        write actually works."""
        with patch("os.geteuid", return_value=1000):
            with patch.object(__util__.subprocess, "run") as run:
                __util__.privileged_write_bytes(tmp_path / "f", b"x")
        assert not run.called


class TestElevationIsTheFallback:
    def _fake_sudo(self, returncode=0, stderr=b""):
        return lambda argv, **kw: subprocess.CompletedProcess(
            argv, returncode, b"", stderr
        )

    def test_an_unwritable_destination_falls_back_to_sudo(self, tmp_path):
        sealed = _sealed(tmp_path / "sealed")
        with patch.object(
            __util__.subprocess, "run", side_effect=self._fake_sudo()
        ) as run:
            __util__.privileged_write_bytes(sealed / "f", b"x")
        assert run.called, "no fallback was attempted for an unwritable destination"
        assert run.call_args[0][0][:1] == ["sudo"], run.call_args[0][0]

    def test_the_fallback_is_non_interactive_by_default(self, tmp_path):
        """Most callers run headless, where an interactive sudo cannot ask
        anybody anything -- it just hangs."""
        sealed = _sealed(tmp_path / "sealed")
        with patch.object(
            __util__.subprocess, "run", side_effect=self._fake_sudo()
        ) as run:
            __util__.privileged_mkdir(sealed / "d")
        assert run.call_args[0][0][:2] == ["sudo", "-n"], run.call_args[0][0]

    def test_a_foreground_caller_may_allow_the_prompt(self, tmp_path):
        """A restore has a person waiting on it, so a user with full sudo is
        prompted exactly as they were before rather than told to start over."""
        sealed = _sealed(tmp_path / "sealed")
        with patch.object(
            __util__.subprocess, "run", side_effect=self._fake_sudo()
        ) as run:
            __util__.privileged_mkdir(sealed / "d", allow_prompt=True)
        argv = run.call_args[0][0]
        assert argv[0] == "sudo" and "-n" not in argv, argv

    def test_the_written_bytes_reach_sudo_on_stdin(self, tmp_path):
        sealed = _sealed(tmp_path / "sealed")
        seen = {}

        def record(argv, **kw):
            seen["argv"] = argv
            seen["input"] = kw.get("input")
            return subprocess.CompletedProcess(argv, 0, b"", b"")

        with patch.object(__util__.subprocess, "run", side_effect=record):
            __util__.privileged_write_bytes(sealed / "f", "some xml")
        assert seen["argv"][-2:] == ["tee", str(sealed / "f")], seen["argv"]
        assert seen["input"] == b"some xml"

    def test_root_does_not_shell_out_at_all(self, tmp_path):
        """Root failing to write is a real filesystem error; sudo-ing to root
        cannot fix it and would only hide what went wrong."""
        sealed = _sealed(tmp_path / "sealed")
        with patch("os.geteuid", return_value=0):
            with patch.object(__util__.subprocess, "run") as run:
                with pytest.raises(OSError):
                    __util__.privileged_write_bytes(sealed / "f", b"x")
        assert not run.called


class TestWhenBothRoutesFail:
    def _refused(self, argv, **kw):
        return subprocess.CompletedProcess(
            argv, 1, b"", b"sudo: a password is required\n"
        )

    def _fail(self, tmp_path, op="write"):
        sealed = _sealed(tmp_path / "sealed")
        with patch.object(__util__.subprocess, "run", side_effect=self._refused):
            with pytest.raises(PermissionError) as excinfo:
                if op == "write":
                    __util__.privileged_write_bytes(sealed / "f", b"x")
                elif op == "mkdir":
                    __util__.privileged_mkdir(sealed / "d")
                elif op == "chmod":
                    __util__.privileged_chmod(sealed / "f", 0o755)
        return str(excinfo.value), sealed

    def test_it_names_the_path_that_could_not_be_written(self, tmp_path):
        msg, sealed = self._fail(tmp_path)
        assert str(sealed / "f") in msg

    def test_it_repeats_what_sudo_actually_said(self, tmp_path):
        """Swallowing sudo's own diagnostic is what made the original failure
        unreadable."""
        msg, _ = self._fail(tmp_path)
        assert "a password is required" in msg

    def test_it_offers_a_remedy(self, tmp_path):
        msg, _ = self._fail(tmp_path)
        assert "as root" in msg
        assert "write access" in msg

    def test_it_explains_why_the_documented_sudoers_did_not_cover_this(self, tmp_path):
        """The operator followed the README, granted NOPASSWD for btrfs, and is
        now being refused. Saying which binary was refused closes that loop."""
        msg, _ = self._fail(tmp_path, op="mkdir")
        assert "/usr/bin/btrfs" in msg
        assert "mkdir" in msg

    def test_the_binary_named_is_the_one_that_was_refused(self, tmp_path):
        msg, _ = self._fail(tmp_path, op="chmod")
        assert "does not cover chmod" in msg

    def test_it_is_not_a_calledprocesserror_repr(self, tmp_path):
        """The old failure surfaced an argv the operator never typed."""
        msg, _ = self._fail(tmp_path)
        assert "returned non-zero exit status" not in msg
        assert "Command '[" not in msg
