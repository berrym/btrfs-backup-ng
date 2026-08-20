"""The paramiko strategy must compress what its remote command decompresses.

This strategy built a remote command containing `zstd -dc` and then streamed
`btrfs send` output straight into the channel, uncompressed. The remote side
fails with "unsupported format" and delivers nothing to `btrfs receive`, so
every compressed backup on this path was broken.

It is the path taken when `ssh_sudo` is on and the remote's sudo needs a
password -- the configuration the shipped examples use -- and precisely the one
a test host with passwordless sudo never exercises, which is why the hardware
matrix passed while this was broken.

The test drives the real method; only paramiko, `btrfs send` and the network
are replaced.
"""

from __future__ import annotations

import io
import os
import subprocess

import pytest

from btrfs_backup_ng.core.transfer import COMPRESSION_PROGRAMS
from btrfs_backup_ng.endpoint import ssh as ssh_module

BTRFS_STREAM = b"btrfs-stream\x00" + bytes(range(256)) * 32


class FakeChannel:
    def __init__(self):
        self.sent = bytearray()
        self.command = None

    def exec_command(self, command):
        self.command = command

    def sendall(self, data):
        self.sent.extend(data)

    def shutdown_write(self):
        pass

    def recv_exit_status(self):
        return 0

    def recv_ready(self):
        return False

    def recv_stderr_ready(self):
        return False

    def close(self):
        pass


class FakeTransport:
    def __init__(self, channel):
        self._channel = channel

    def open_session(self):
        return self._channel


class FakeClient:
    def __init__(self, channel):
        self._transport = FakeTransport(channel)

    def connect(self, *args, **kwargs):
        pass

    def get_transport(self):
        return self._transport

    def close(self):
        pass


class _FakeParamiko:
    """Just the attributes the strategy's error handling reaches for."""

    class BadHostKeyException(Exception):
        pass

    class AuthenticationException(Exception):
        pass

    class SSHException(Exception):
        pass


class FakeSend:
    """Stands in for `btrfs send`, emitting a recognisable plain stream."""

    def __init__(self):
        # A real pipe, not BytesIO: the compressor is a real subprocess and
        # needs a file descriptor to read from.
        read_fd, write_fd = os.pipe()
        os.write(write_fd, BTRFS_STREAM)
        os.close(write_fd)
        self.stdout = os.fdopen(read_fd, "rb")
        self.stderr = io.BytesIO(b"")
        self.returncode = 0

    def wait(self, timeout=None):
        return 0

    def poll(self):
        return 0

    def kill(self):
        pass

    def terminate(self):
        pass


@pytest.fixture
def endpoint(monkeypatch):
    ep = ssh_module.SSHEndpoint.__new__(ssh_module.SSHEndpoint)
    ep.config = {
        "username": "u",
        "port": 22,
        "ssh_sudo": True,
        "ssh_password_fallback": False,
    }
    ep.hostname = "host"
    channel = FakeChannel()
    monkeypatch.setattr(
        ssh_module.SSHEndpoint,
        "_new_verified_paramiko_client",
        lambda self: FakeClient(channel),
    )
    monkeypatch.setattr(ssh_module, "paramiko", _FakeParamiko())
    monkeypatch.setattr(
        ssh_module.SSHEndpoint, "_estimate_snapshot_size", lambda *a, **k: None
    )
    # The receive side is out of scope here: this test is about what leaves the
    # machine. Verification talks to a remote that does not exist.
    monkeypatch.setattr(
        ssh_module.SSHEndpoint, "_verify_snapshot_exists", lambda *a, **k: True
    )
    ep._cached_sudo_password = "pw"

    real_popen = subprocess.Popen

    def fake_popen(cmd, *args, **kwargs):
        # `btrfs send` is faked; a real compressor still runs, so what reaches
        # the channel is genuinely compressed data and not a stub's say-so.
        if list(cmd[:3]) == ["sudo", "btrfs", "send"]:
            return FakeSend()
        return real_popen(cmd, *args, **kwargs)

    monkeypatch.setattr(ssh_module.subprocess, "Popen", fake_popen)
    ep._test_channel = channel
    return ep


def _run(endpoint):
    return endpoint._do_paramiko_transfer(
        source_path="/snaps/snap",
        dest_path="/backups",
        snapshot_name="snap",
        parent_path=None,
        sudo_password="pw",
    )


def _strip_password(sent: bytes) -> bytes:
    """The strategy writes the sudo password line before the stream."""
    return sent.split(b"\n", 1)[1] if sent.startswith(b"pw\n") else sent


@pytest.mark.parametrize("method", ["zstd", "gzip"])
def test_what_reaches_the_wire_is_what_the_remote_will_decompress(endpoint, method):
    endpoint.config["compress"] = method
    assert _run(endpoint) is True

    channel = endpoint._test_channel
    payload = _strip_password(bytes(channel.sent))

    assert COMPRESSION_PROGRAMS[method]["decompress"][0] in channel.command, (
        "the remote command does not decompress, so this test proves nothing"
    )
    assert not payload.startswith(b"btrfs-stream"), (
        "a raw btrfs stream is on the wire while the remote runs a decompressor"
    )
    restored = subprocess.run(
        COMPRESSION_PROGRAMS[method]["decompress"],
        input=payload,
        capture_output=True,
        check=True,
    ).stdout
    assert restored == BTRFS_STREAM, "the remote would not recover the original stream"


def test_without_compression_the_plain_stream_is_sent(endpoint):
    """The uncompressed path must not regress."""
    assert _run(endpoint) is True
    payload = _strip_password(bytes(endpoint._test_channel.sent))
    assert payload == BTRFS_STREAM
    for program in ("zstd", "gzip", "lz4", "pigz", "lzop"):
        assert program not in endpoint._test_channel.command
