"""Remote ssh:// btrfs restore: SSHEndpoint must send FROM the remote and lock in memory.

A restore reads FROM the backup endpoint. When that endpoint is a native btrfs
``SSHEndpoint``, the snapshot lives on the REMOTE host, so:

* ``send()`` must run ``btrfs send`` ON THE REMOTE and stream its stdout back over ssh
  (the base implementation runs it locally, which fails -- the subvolume is not local);
* ``set_lock()`` must be in-memory only (the base writes a lock file at ``config['path']``,
  which for ssh is a remote path opened as a local file -> aborts the restore).

Both are proven end-to-end byte-identical against a real remote btrfs host; these unit
tests pin the command construction and the in-memory locking.
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import btrfs_backup_ng.endpoint.ssh as ssh_mod
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint


def _ep(ssh_sudo=True):
    ep = SSHEndpoint(
        hostname="backup-host",
        config={"path": "/backup", "ssh_sudo": ssh_sudo, "passwordless": True},
    )
    ep.ssh_manager = MagicMock()
    ep.ssh_manager.get_ssh_base_cmd.return_value = [
        "ssh",
        "-o",
        "ControlPath=/tmp/cm",
        "user@backup-host",
    ]
    return ep


def _snap(path):
    s = MagicMock()
    s.get_path.return_value = path
    return s


def _capture_send(monkeypatch, ep, snapshot, **kw):
    captured = {}

    def fake_popen(cmd, **popen_kw):
        captured["cmd"] = cmd
        captured["popen_kw"] = popen_kw
        return MagicMock(spec=subprocess.Popen)

    monkeypatch.setattr(ssh_mod.subprocess, "Popen", fake_popen)
    ep.send(snapshot, **kw)
    return captured


def test_send_runs_btrfs_send_on_the_remote_and_streams(monkeypatch):
    """send() must invoke ``btrfs send <remote_path>`` over the ssh base command and
    stream stdout (PIPE) for the local receive. Mutation guard: the base (local) send
    would run ``['btrfs','send',...]`` directly with no ssh prefix -- this asserts the
    ssh base is present and the remote path is sent."""
    ep = _ep()
    captured = _capture_send(monkeypatch, ep, _snap("/backup/snap-1"))
    cmd = captured["cmd"]
    # ssh base command comes first...
    assert cmd[:4] == ["ssh", "-o", "ControlPath=/tmp/cm", "user@backup-host"]
    # ...then a single remote command string running btrfs send of the REMOTE path.
    remote = cmd[-1]
    assert "btrfs send" in remote
    assert "/backup/snap-1" in remote
    assert "sudo" in remote  # ssh_sudo -> remote sudo
    # stdout must be a pipe so btrfs receive can consume the stream.
    assert captured["popen_kw"]["stdout"] is subprocess.PIPE


def test_send_incremental_threads_parent(monkeypatch):
    """Incremental restore must send with ``-p <parent_remote_path>``."""
    ep = _ep()
    captured = _capture_send(
        monkeypatch, ep, _snap("/backup/snap-1"), parent=_snap("/backup/snap-0")
    )
    remote = captured["cmd"][-1]
    assert "-p" in remote
    assert "/backup/snap-0" in remote  # the parent, on the remote


def test_send_quotes_remote_path(monkeypatch):
    """A path with a space/metacharacter must be quoted so the remote shell cannot split
    or inject it."""
    ep = _ep()
    captured = _capture_send(monkeypatch, ep, _snap("/backup/weird name;rm -rf"))
    remote = captured["cmd"][-1]
    # The dangerous path is present but quoted (not a bare, splittable token).
    assert "'/backup/weird name;rm -rf'" in remote


def test_set_lock_is_in_memory_only_and_never_writes(tmp_path):
    """SSHEndpoint.set_lock must only mutate the in-memory lock set -- no local lock file
    (its path is on the remote). Mutation guard: falling back to the base set_lock would
    try to write a lock file and (for a remote path) raise."""
    ep = SSHEndpoint(hostname="backup-host", config={"path": str(tmp_path)})
    snap = MagicMock()
    snap.locks = set()
    snap.parent_locks = set()
    ep.set_lock(snap, "restore:abc", True)
    assert snap.locks == {"restore:abc"}
    ep.set_lock(snap, "xfer:1", True, parent=True)
    assert snap.parent_locks == {"xfer:1"}
    ep.set_lock(snap, "restore:abc", False)
    assert snap.locks == set()
    # No lock file was written next to the (here local-stand-in) path.
    assert not (tmp_path / ".btrfs-backup-ng.locks").exists()
