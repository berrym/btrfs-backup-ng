"""Atomic raw stream write (0.8.5 PR1).

A raw receive writes to a ``.part`` file and is published to its final name only
by ``commit_receive()``, which the transfer engine calls after confirming the
pipeline succeeded. A crash therefore leaves at most a ``.part`` file, which
discovery must ignore -- so a partial transfer can never be listed as a complete
backup (the raw phantom-backup bug).

These tests are written to FAIL if the atomic-write behavior is reverted:
  * remove the ``.part`` exclusion in discover_raw_snapshots -> the phantom
    tests fail (a partial is listed as a backup);
  * make receive() write straight to the final name -> the "final absent before
    commit" assertions fail.
"""

import contextlib
import shlex
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import discover_raw_snapshots


@contextlib.contextmanager
def _null_lock(self, **kwargs):
    """Stand in for the remote lock where the test is about what runs INSIDE it.

    test_ssh_commit_holds_the_target_lock_across_rename_and_sidecar covers the
    lock itself, and test_ssh_commit_wraps_in_sudo_when_configured runs the real
    protocol against a sandbox."""
    yield


def test_part_file_is_never_discovered(tmp_path):
    """A leftover ``.part`` file (a crashed/uncommitted transfer) is not a backup."""
    (tmp_path / "good-20260101.btrfs").write_bytes(b"complete")
    (tmp_path / "crashed-20260102.btrfs.part").write_bytes(b"partial")

    found = discover_raw_snapshots(tmp_path)
    names = {s.name for s in found}
    assert "good-20260101" in names
    # The partial must not appear under any name...
    assert not any("crashed" in n for n in names)
    # ...and no discovered stream is ever a .part file.
    assert all(not s.stream_path.name.endswith(".part") for s in found)


def test_receive_then_commit_publishes_atomically(tmp_path):
    """receive() writes to .part; the final name appears only after commit."""
    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    src = tmp_path / "src.bin"
    src.write_bytes(b"payload-bytes")

    with open(src, "rb") as stdin:
        proc = endpoint.receive(stdin, snapshot_name="snap")
        proc.communicate()
    assert proc.returncode == 0

    final = tmp_path / "snap.btrfs"
    # The .part name carries this transfer's pid and a monotonic stamp so two
    # concurrent runs cannot share one temp file, so it is matched by glob.
    parts = list(tmp_path.glob("snap.btrfs.*.part"))
    # Uncommitted: only the .part exists, and discovery ignores it -- so a crash
    # here leaves nothing that looks like a complete backup.
    assert len(parts) == 1, parts
    assert not final.exists()
    assert discover_raw_snapshots(tmp_path) == []

    endpoint.commit_receive()
    # Committed: the final name exists, the .part is gone, and it is now the only
    # discoverable snapshot.
    assert final.exists()
    assert not list(tmp_path.glob("snap.btrfs.*.part"))
    assert final.read_bytes() == b"payload-bytes"
    assert [s.name for s in discover_raw_snapshots(tmp_path)] == ["snap"]


def test_commit_missing_part_fails_loud(tmp_path):
    """If a receive ran but its .part is gone at commit time, fail loud rather
    than report a success with no file on disk (never fabricate a final file)."""
    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    endpoint._pending_metadata = {
        "name": "x",
        "stream_path": tmp_path / "x.btrfs",
        "part_path": tmp_path / "x.btrfs.part",  # does not exist
        "parent_name": None,
        "compress": None,
        "encrypt": None,
        "gpg_recipient": None,
    }
    with pytest.raises(RuntimeError, match="is missing"):
        endpoint.commit_receive()
    assert not (tmp_path / "x.btrfs").exists()


def test_commit_without_receive_is_noop(tmp_path):
    """A fresh endpoint that never received anything must commit as a safe no-op
    (its dummy metadata has no name), not touch the filesystem, and not raise."""
    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    # Do NOT call receive(); _pending_metadata is the dummy init.
    endpoint.commit_receive()
    assert list(tmp_path.iterdir()) == []


def _ssh_commit_endpoint(**config):
    """A raw+ssh endpoint staged as if a transfer had just finished."""
    base = {"path": "/backup", "hostname": "nas"}
    base.update(config)
    ep = SSHRawEndpoint(config=base)
    ep._pending_metadata = {
        "name": "snap",
        "stream_path": "/backup/snap.btrfs",
        "part_path": "/backup/snap.btrfs.part",
    }
    return ep


def test_ssh_commit_publishes_with_sync_mv_sync_then_writes_the_sidecar():
    """The publishing call is exactly 'sync && mv -f <part> <final> && sync' --
    the leading sync flushes the bytes before the rename, the trailing one makes
    the rename durable -- and the sidecar is written remotely and atomically
    afterwards. Mocked; runs without any real SSH."""
    ep = _ssh_commit_endpoint()
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=0, stderr=b"", stdout=b"1")
    )
    with patch.object(SSHRawEndpoint, "target_lock", _null_lock):
        ep.commit_receive()

    scripts = [
        c[0][0][2] for c in ep._exec_remote_command.call_args_list if c[0][0][0] == "sh"
    ]
    assert "sync && mv -f /backup/snap.btrfs.part /backup/snap.btrfs && sync" in scripts
    # The sidecar is published remotely and atomically (temp -> mv -> chmod 600).
    joined = " ".join(scripts)
    assert "/backup/snap.btrfs.meta" in joined
    assert "chmod 600" in joined


def test_ssh_commit_measures_the_part_file_before_the_rename():
    """Size and digest are read from the ``.part`` name, not the final one.

    ``mv`` is a pure rename so both describe the same bytes, but only the
    ``.part`` can be measured before the target lock is taken -- and a remote
    sha256 of a multi-GB stream held under that lock would make a legitimately
    concurrent commit time out instead of serialize.
    """
    ep = _ssh_commit_endpoint()
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=0, stderr=b"", stdout=b"1")
    )
    with patch.object(SSHRawEndpoint, "target_lock", _null_lock):
        ep.commit_receive()

    scripts = [
        c[0][0][2] for c in ep._exec_remote_command.call_args_list if c[0][0][0] == "sh"
    ]
    publish = next(i for i, s in enumerate(scripts) if s.startswith("sync && mv"))
    measured = [s for s in scripts[:publish] if "stat -c" in s or "sha256sum" in s]
    assert len(measured) == 2, f"stream not measured before the rename: {scripts}"
    assert all("snap.btrfs.part" in s for s in measured), (
        "the stream was measured under its final name, which does not exist yet"
    )


def test_ssh_commit_hashes_the_stream_once_even_when_the_digest_fails():
    """A remote with no sha256 tool must not be asked to hash the stream twice.

    The measurement is handed to the sidecar writer as one (size, checksum) pair
    precisely because a failed hash legitimately yields None: keyed on the
    checksum alone, the streams whose digest could not be taken -- the ones
    already costing a full remote read for nothing -- would pay for it twice.
    """
    ep = _ssh_commit_endpoint()
    ep._exec_remote_command = MagicMock(
        # stdout is not a 64-hex digest, so _remote_sha256 returns None.
        return_value=MagicMock(returncode=0, stderr=b"", stdout=b"1")
    )
    with patch.object(SSHRawEndpoint, "target_lock", _null_lock):
        ep.commit_receive()

    scripts = [
        c[0][0][2] for c in ep._exec_remote_command.call_args_list if c[0][0][0] == "sh"
    ]
    assert sum("sha256sum" in s for s in scripts) == 1, (
        f"the stream was hashed {sum('sha256sum' in s for s in scripts)} times"
    )


def test_ssh_commit_holds_the_target_lock_across_rename_and_sidecar():
    """A concurrent prune or backfill must not observe the published stream in the
    window before its authoritative sidecar exists -- it would stamp the backup
    `unknown`/inferred over the record this commit is about to write. The local
    commit path has held the lock over exactly this window since R7."""
    ep = _ssh_commit_endpoint()
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=0, stderr=b"", stdout=b"1")
    )
    held: list[str] = []

    @contextlib.contextmanager
    def recording(self, **kwargs):
        held.append("acquired")
        try:
            yield
        finally:
            held.append("released")

    def record_call(cmd, *a, **kw):
        held.append(cmd[2] if cmd[0] == "sh" else " ".join(cmd))
        return MagicMock(returncode=0, stderr=b"", stdout=b"1")

    ep._exec_remote_command = MagicMock(side_effect=record_call)
    with patch.object(SSHRawEndpoint, "target_lock", recording):
        ep.commit_receive()

    inside = held[held.index("acquired") + 1 : held.index("released")]
    assert any(s.startswith("sync && mv") for s in inside), "rename ran unlocked"
    assert any("snap.btrfs.meta" in s for s in inside), "sidecar written unlocked"


def test_ssh_commit_raises_on_remote_failure():
    """A nonzero remote return must raise, so the engine treats an unpublished
    remote stream as a failed transfer."""
    ep = _ssh_commit_endpoint()
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=1, stderr=b"mv: cannot stat")
    )
    with patch.object(SSHRawEndpoint, "target_lock", _null_lock):
        with pytest.raises(RuntimeError, match="Failed to publish remote raw stream"):
            ep.commit_receive()


def test_ssh_commit_wraps_in_sudo_when_configured(tmp_path):
    """With ssh_sudo, the remote commit is wrapped in sudo. Exercises the real
    command construction (only subprocess.run is mocked) -- no live SSH -- so the
    sudo path is proven without setting up passwordless root access.

    The lock protocol runs for real against a sandbox directory (see
    tests/lockshell.py): answering it with a canned object would have the manager
    correctly refuse a lock it could not confirm, and the commit would never
    reach the mv this test is about.
    """
    from .lockshell import lock_aware

    ep = _ssh_commit_endpoint(ssh_sudo=True)
    with patch("btrfs_backup_ng.endpoint.raw.subprocess.run") as mrun:
        mrun.side_effect = lock_aware(
            lambda cmd, **kw: MagicMock(returncode=0, stderr=b"", stdout=b"1"), tmp_path
        )
        ep.commit_receive()

    remote_cmds = [c[0][0][-1] for c in mrun.call_args_list]
    publish = next(c for c in remote_cmds if "sync && mv -f" in c)
    # LC_ALL=C pins sudo's (localised) diagnostic; -n fails fast rather than
    # prompting on a connection that has no tty.
    assert publish.startswith("LC_ALL=C sudo -n ")
    assert publish.endswith("&& sync'")


def test_commit_never_overwrites_a_committed_final(tmp_path):
    """commit_receive publishes the current .part; it must not touch an unrelated
    already-committed backup that shares the directory."""
    endpoint = RawEndpoint(config={"path": str(tmp_path)})
    prior = tmp_path / "prior.btrfs"
    prior.write_bytes(b"prior-good-backup")

    src = tmp_path / "src.bin"
    src.write_bytes(b"new-stream")
    with open(src, "rb") as stdin:
        proc = endpoint.receive(stdin, snapshot_name="new")
        proc.communicate()
    endpoint.commit_receive()

    # The prior backup is untouched; the new one is published alongside it.
    assert prior.read_bytes() == b"prior-good-backup"
    assert (tmp_path / "new.btrfs").read_bytes() == b"new-stream"


def test_part_name_distinguishes_machines():
    """pid and monotonic_ns do not distinguish HOSTS, and a raw+ssh target is
    routinely shared by several: pids collide across machines by nature, and
    monotonic_ns is uptime on Linux, so two boxes booting together on the same
    timer is not an exotic case. Colliding there put two streams in one file."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    names = set()
    for _ in range(50):
        with (
            patch("btrfs_backup_ng.endpoint.raw.os.getpid", return_value=4321),
            patch(
                "btrfs_backup_ng.endpoint.raw.time.monotonic_ns", return_value=0xABCDEF
            ),
        ):
            ep._execute_pipeline = MagicMock(return_value=MagicMock())
            ep.receive(None, snapshot_name="snap")
            names.add(str(ep._pending_metadata["part_path"]))
    assert len(names) == 50, (
        "two transfers with the same pid and clock reading -- i.e. two machines -- "
        f"produced {50 - len(names) + 1} identical .part names"
    )


def test_remote_part_write_refuses_to_clobber_or_follow_a_symlink():
    """The remote counterpart of the local O_EXCL|O_NOFOLLOW open.

    A plain `cat >` truncates whatever is at the path and writes THROUGH a
    symlink, so a colliding .part interleaved two streams into one file, and a
    symlink planted in a world-writable target redirected a root-run backup onto
    whatever it pointed at. Measured on dash, bash and POSIX sh: `set -C` refuses
    an existing file, a symlink and a dangling symlink, and exits non-zero.
    """
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._pending_metadata = {"part_path": "/backup/snap.btrfs.1.2.3.part"}
    with patch("btrfs_backup_ng.endpoint.raw.subprocess.Popen") as popen:
        ep._execute_pipeline([["cat"]], None)
    remote = popen.call_args[0][0][-1]
    assert "set -C" in remote, f"remote write can still clobber: {remote}"
    assert "umask 077" in remote, f"remote stream left world-readable: {remote}"


@pytest.mark.parametrize("shell", ["sh", "dash", "bash"])
def test_the_refusal_works_in_the_shells_a_remote_may_run(tmp_path, shell):
    """The remote is not this machine. Two shipped bugs came from assuming bash;
    the script has to behave in whatever /bin/sh the target actually has."""
    import shutil as _shutil
    import subprocess as _sp

    if not _shutil.which(shell):
        pytest.skip(f"{shell} not installed")

    ep = SSHRawEndpoint(config={"path": str(tmp_path), "hostname": "nas"})
    victim = tmp_path / "secret"
    victim.write_bytes(b"do not touch")
    for target, label in (
        (tmp_path / "existing", "an existing file"),
        (tmp_path / "link", "a symlink"),
    ):
        if label == "a symlink":
            target.symlink_to(victim)
        else:
            target.write_bytes(b"prior stream")
        ep._pending_metadata = {"part_path": str(target)}
        with patch("btrfs_backup_ng.endpoint.raw.subprocess.Popen") as popen:
            ep._execute_pipeline([["cat"]], None)
        # The remote command is the last argv element, of the form
        # `sh -c '<script>'`. Split it as a shell would to recover the script --
        # slicing the prefix off leaves the surrounding quotes, and the shell then
        # reports "command not found", a non-zero status that looks exactly like
        # the refusal being tested for.
        script = shlex.split(popen.call_args[0][0][-1])[-1]
        proc = _sp.run(
            [shell, "-c", f"echo overwritten | {script}"], capture_output=True
        )
        assert proc.returncode != 0, f"{shell} overwrote {label}"
        assert b"not found" not in proc.stderr, (
            f"the script did not run at all: {proc.stderr!r}"
        )

    assert victim.read_bytes() == b"do not touch"
