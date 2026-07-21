"""raw+ssh restore + preflight (0.8.5 Stage 2).

SSHRawEndpoint.send() reads the stream back from the REMOTE host over ssh and
decrypts/decompresses it LOCALLY. prepare() preflights the remote for the POSIX
tools raw+ssh needs. Written to FAIL if the remote-read or preflight regress.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.__util__ import AbortError
from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def _ep():
    return SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})


def test_send_reads_remote_stream_and_decompresses_locally():
    """send() must stream `ssh host 'cat <remote>'` and pipe it through LOCAL
    decompression -- not open a local file (the base RawEndpoint behavior)."""
    ep = _ep()
    ep._exec_remote_command = MagicMock(return_value=MagicMock(returncode=0))  # test -f
    snap = RawSnapshot(
        name="s", stream_path=Path("/backup/s.btrfs.gz"), compress="gzip"
    )
    with patch("btrfs_backup_ng.endpoint.raw._popen_pipeline_pipefail") as pp:
        pp.return_value = MagicMock()
        ep.send(snap)
    shell_cmd = pp.call_args[0][0]
    assert "ssh" in shell_cmd
    assert "/backup/s.btrfs.gz" in shell_cmd  # reads the REMOTE path
    assert "cat" in shell_cmd
    assert "gzip -d" in shell_cmd  # decompression runs locally, after the ssh cat


def test_send_plaintext_is_just_remote_cat():
    ep = _ep()
    ep._exec_remote_command = MagicMock(return_value=MagicMock(returncode=0))
    snap = RawSnapshot(name="s", stream_path=Path("/backup/s.btrfs"))  # no comp/enc
    with patch("btrfs_backup_ng.endpoint.raw._popen_pipeline_pipefail") as pp:
        pp.return_value = MagicMock()
        ep.send(snap)
    shell_cmd = pp.call_args[0][0]
    assert "ssh" in shell_cmd and "/backup/s.btrfs" in shell_cmd
    # Structural invariant: plaintext is a single remote cat with NO local stage
    # (catches any unexpected local staging, not just gpg/gzip).
    assert "|" not in shell_cmd


def test_send_missing_remote_stream_raises_clearly():
    ep = _ep()
    ep._exec_remote_command = MagicMock(
        return_value=MagicMock(returncode=1)
    )  # test -f fails
    snap = RawSnapshot(name="nope", stream_path=Path("/backup/nope.btrfs"))
    with pytest.raises(FileNotFoundError, match="Remote stream not found"):
        ep.send(snap)


def test_prepare_preflight_fails_loud_when_remote_lacks_posix_tools():
    """A remote missing the POSIX tools (no RAWSSHOK) must fail with actionable
    guidance, not a cryptic mid-transfer error."""
    ep = _ep()
    with patch("subprocess.run", return_value=MagicMock(returncode=0, stdout=b"")):
        with patch.object(ep, "_check_tools", return_value=[]):
            with pytest.raises(RuntimeError, match="does not provide the POSIX tools"):
                ep._prepare()


def test_prepare_preflight_passes_when_tools_present():
    ep = _ep()
    with patch(
        "subprocess.run", return_value=MagicMock(returncode=0, stdout=b"RAWSSHOK\n")
    ):
        with patch.object(ep, "_check_tools", return_value=[]):
            ep._prepare()  # must not raise


def test_prepare_fails_loud_when_local_compress_tool_missing():
    """A raw+ssh backup whose LOCAL compress/encrypt tool is missing (compression
    runs locally before the ssh pipe) must fail its preflight with an actionable
    message, not a raw errno part-way through the transfer."""
    ep = SSHRawEndpoint(
        config={"path": "/backup", "hostname": "nas", "compress": "zstd"}
    )
    with patch(
        "subprocess.run", return_value=MagicMock(returncode=0, stdout=b"RAWSSHOK\n")
    ):
        with patch("btrfs_backup_ng.endpoint.raw.shutil.which", return_value=None):
            with pytest.raises(AbortError, match="not installed"):
                ep._prepare()


def test_send_encrypted_decrypts_locally_after_ssh_cat():
    """Security invariant: gpg decrypt runs LOCALLY, downstream of the ssh cat --
    the key/passphrase are never shipped to the (untrusted) remote."""
    ep = _ep()
    ep._exec_remote_command = MagicMock(return_value=MagicMock(returncode=0))
    snap = RawSnapshot(
        name="s",
        stream_path=Path("/backup/s.btrfs.gz.gpg"),
        encrypt="gpg",
        compress="gzip",
    )
    with patch("btrfs_backup_ng.endpoint.raw._popen_pipeline_pipefail") as pp:
        pp.return_value = MagicMock()
        ep.send(snap)
    shell_cmd = pp.call_args[0][0]
    # Order proves decrypt is a LOCAL downstream stage: ssh cat | gpg | gzip -d.
    assert shell_cmd.index("ssh") < shell_cmd.index("gpg") < shell_cmd.index("gzip")
    assert "--decrypt" in shell_cmd


def test_send_ssh_sudo_wraps_remote_cat_in_sudo_sh_c():
    """With ssh_sudo the WHOLE remote cat is wrapped in `sudo sh -c`, not a bare
    `sudo cat` (which would mis-scope under a shell)."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas", "ssh_sudo": True})
    ep._exec_remote_command = MagicMock(return_value=MagicMock(returncode=0))
    snap = RawSnapshot(name="s", stream_path=Path("/backup/s.btrfs"))
    with patch("btrfs_backup_ng.endpoint.raw._popen_pipeline_pipefail") as pp:
        pp.return_value = MagicMock()
        ep.send(snap)
    shell_cmd = pp.call_args[0][0]
    assert "sudo sh -c" in shell_cmd


def test_list_second_pass_stat_is_portable_and_sudo_scoped():
    """The sidecar-less list stat tries GNU then BSD, and under ssh_sudo the WHOLE
    fallback runs inside one `sudo sh -c` (not just the first stat)."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas", "ssh_sudo": True})
    with patch("subprocess.run") as mrun:
        mrun.side_effect = [
            MagicMock(returncode=0, stdout=""),  # find *.meta -> none
            MagicMock(returncode=0, stdout="/backup/x.btrfs\n"),  # find stream
            MagicMock(returncode=0, stdout="1700000000 4096\n"),  # stat
        ]
        ep.list_snapshots()
    stat_str = " ".join(mrun.call_args_list[2][0][0])  # the stat call's argv
    # Quote-insensitive (the sudo wrapping shlex-escapes the inner quotes):
    assert "stat -c" in stat_str and "%Y %s" in stat_str  # GNU branch
    assert "stat -f" in stat_str and "%m %z" in stat_str  # BSD/macOS fallback
    assert "sudo sh -c" in stat_str  # whole fallback under one sudo


def test_remote_sidecar_size_cmd_is_portable():
    """The remote sidecar size uses GNU -> BSD -> wc, so a macOS/NAS target records
    a real size (GNU `stat -c` is unsupported on BSD)."""
    ep = SSHRawEndpoint.__new__(SSHRawEndpoint)
    ep._pending_metadata = {
        "name": "s",
        "stream_path": "/backup/s.btrfs",
        "part_path": "/backup/s.btrfs.part",
        "parent_name": None,
        "compress": None,
        "encrypt": None,
        "gpg_recipient": None,
        "openssl_cipher": None,
    }
    seen = []
    ep._exec_remote_command = MagicMock(
        side_effect=lambda argv, **kw: (
            seen.append(argv) or MagicMock(returncode=0, stdout=b"10")
        )
    )
    ep._write_remote_sidecar(Path("/backup/s.btrfs"))
    size_script = " ".join(seen[0])  # first remote call is the size command
    assert "stat -c %s" in size_script
    assert "stat -f %z" in size_script
    assert "wc -c" in size_script
