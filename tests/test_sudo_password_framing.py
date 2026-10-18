"""The sudo password reaches the remote in the form its receive command reads.

The compressed receive takes the password with `dd`, by byte count, because a
shell's `read` may read past the line: dash 0.5.13.0-0.5.13.4 lose one byte per
NUL behind it, and what follows it is the compressed stream (#115). The local
side must send exactly that frame, and a password `sudo -S` can never accept
must be refused before anything is sent.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.endpoint.ssh as ssh_mod
from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint, _frame_sudo_password


@pytest.fixture
def endpoint(monkeypatch):
    """An ssh endpoint with no connection whose sudo wants a password."""
    monkeypatch.setattr(SSHEndpoint, "_sudo_password_cache", {})
    monkeypatch.delenv("BTRFS_BACKUP_SUDO_PASSWORD", raising=False)
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"username": "u", "port": None, "ssh_sudo": True}
    ep.hostname = "host"
    ep.ssh_manager = SimpleNamespace(control_path="/tmp/cp", _cached_ssh_password=None)
    ep._cached_sudo_password = None
    return ep


def _fake_launches(ep, monkeypatch):
    """Stub out everything but the transfer itself; return what Popen gets."""
    ep._estimate_snapshot_size = MagicMock(return_value=None)  # type: ignore[method-assign]
    ep._check_command_exists = MagicMock(return_value=False)  # type: ignore[method-assign]
    ep._verify_snapshot_exists = MagicMock(return_value=True)  # type: ignore[method-assign]
    ep._cleanup_partial_subvolume = MagicMock()  # type: ignore[method-assign]
    launched: list[dict] = []

    def fake_popen(cmd, **kwargs):
        launched.append({"cmd": cmd, "env": kwargs.get("env") or {}})
        proc = MagicMock()
        proc.stderr = SimpleNamespace(read=lambda _n=-1: b"")
        proc.wait.return_value = 0
        proc.returncode = 0
        return proc

    monkeypatch.setattr(ssh_mod.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(ssh_mod.subprocess, "Popen", fake_popen)
    return launched


def _transfer(ep):
    return ep._do_shell_pipeline_transfer(
        source_path="/src/snapshot",
        dest_path="/d/5",
        snapshot_name="snapshot",
        password_sudo=True,
    )


class TestTheFrame:
    def test_the_count_is_of_bytes_not_characters(self):
        """`dd` on the remote counts bytes. A count of characters would leave
        the tail of a multibyte password in front of the stream."""
        frame = _frame_sudo_password("pässwörd")
        assert frame == "0011pässwörd\n"

    def test_the_line_comes_once_with_its_newline(self):
        """sudo -S reads up to the newline, so the newline is part of the line."""
        assert _frame_sudo_password("hunter2") == "0008hunter2\n"

    def test_the_largest_password_that_fits(self):
        assert _frame_sudo_password("x" * 9998).startswith("9999x")

    def test_a_longer_password_is_refused_rather_than_misframed(self):
        with pytest.raises(__util__.AbortError, match="9999 bytes long"):
            _frame_sudo_password("x" * 9999)


class TestTheLocalSideSendsTheFrame:
    def test_a_compressed_receive_gets_the_frame(self, endpoint, monkeypatch):
        endpoint.config["compress"] = "gzip"
        monkeypatch.setenv("BTRFS_BACKUP_SUDO_PASSWORD", "hunter2")

        launched = _fake_launches(endpoint, monkeypatch)
        _transfer(endpoint)
        (launched,) = launched

        assert launched["env"]["BTRFS_BACKUP_SUDO_PW"] == "0008hunter2\n"
        assert 'printf "%s" "$BTRFS_BACKUP_SUDO_PW"' in launched["cmd"]

    def test_an_uncompressed_receive_gets_one_line(self, endpoint, monkeypatch):
        """There `sudo -S` takes the line itself, as it always has."""
        monkeypatch.setenv("BTRFS_BACKUP_SUDO_PASSWORD", "hunter2")

        launched = _fake_launches(endpoint, monkeypatch)
        _transfer(endpoint)
        (launched,) = launched

        assert launched["env"]["BTRFS_BACKUP_SUDO_PW"] == "hunter2\n"

    def test_the_password_is_never_in_the_command(self, endpoint, monkeypatch):
        endpoint.config["compress"] = "gzip"
        monkeypatch.setenv("BTRFS_BACKUP_SUDO_PASSWORD", "hunter2")

        launched = _fake_launches(endpoint, monkeypatch)
        _transfer(endpoint)
        (launched,) = launched

        assert "hunter2" not in launched["cmd"]


class TestALineBreakIsRefused:
    """sudo stops reading at a newline or a carriage return (measured with
    1.9.16p2 and 1.9.17p2), so such a password can never authenticate."""

    @pytest.mark.parametrize("password", ["ab\ncd", "ab\rcd", "abcd\n"])
    def test_from_the_environment(self, endpoint, monkeypatch, password):
        monkeypatch.setenv("BTRFS_BACKUP_SUDO_PASSWORD", password)

        with pytest.raises(__util__.AbortError, match="contains a line break") as ei:
            endpoint._get_sudo_password()
        assert password.strip() not in str(ei.value)

    def test_from_the_reused_ssh_password(self, endpoint):
        endpoint.ssh_manager._cached_ssh_password = "ssh\npass"

        with pytest.raises(__util__.AbortError, match="contains a line break"):
            endpoint._get_sudo_password()

    def test_an_ordinary_password_is_returned_unchanged(self, endpoint, monkeypatch):
        monkeypatch.setenv("BTRFS_BACKUP_SUDO_PASSWORD", "p@ss w\\rd")

        assert endpoint._get_sudo_password() == "p@ss w\\rd"

    def test_nothing_is_started_with_such_a_password(self, endpoint, monkeypatch):
        endpoint.config["compress"] = "gzip"
        monkeypatch.setenv("BTRFS_BACKUP_SUDO_PASSWORD", "ab\ncd")
        launched = _fake_launches(endpoint, monkeypatch)

        with pytest.raises(__util__.AbortError, match="contains a line break"):
            _transfer(endpoint)
        assert launched == []

    def test_the_chunked_receive_starts_nothing_with_such_a_password(
        self, endpoint, monkeypatch
    ):
        """The chunked path asked for the password after starting the remote
        receive, which a refusal would have left running with nobody to stop it
        and outside the lock that should hold it."""
        endpoint.config["path"] = "/d/5"
        endpoint._normalize_path = lambda p: p  # type: ignore[method-assign]
        endpoint.artifact_exists = MagicMock(return_value=False)  # type: ignore[method-assign]
        endpoint.ssh_manager.get_ssh_base_cmd = lambda force_tty=False: ["ssh", "h"]
        monkeypatch.setenv("BTRFS_BACKUP_SUDO_PASSWORD", "pw\r")
        launched: list = []
        monkeypatch.setattr(
            ssh_mod.subprocess, "Popen", lambda *a, **k: launched.append(a)
        )
        manifest = SimpleNamespace(
            snapshot_name="snap", snapshot_path="/src/snapshot", chunk_count=1
        )

        with pytest.raises(__util__.AbortError, match="contains a line break"):
            endpoint.receive_chunked(MagicMock(), manifest)
        assert launched == []
