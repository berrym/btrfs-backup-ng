"""``--compress`` on an ``ssh://`` restore SOURCE compresses the wire.

The README recommends ``--compress zstd`` for a slow-link restore, and the
option was accepted and did nothing: the transfer layer dropped it for a local
destination, and the ssh endpoint's restore-direction ``send`` had no
compressor. The backup direction had both halves; this is their mirror. The
remote compresses ``btrfs send`` before the wire and this host decompresses
before ``btrfs receive``, both from the one configured method.

Where a shell is involved a real one runs it: the remote script's exit-status
contract is checked under the ``sh`` on PATH and under dash when present, with
the send replaced by a command that fails after writing, which is exactly the
shape a failing ``btrfs send`` has.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.core.operations as ops
import btrfs_backup_ng.endpoint.ssh as ssh_mod
from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint, _compressed_send_script


def _ep(compress=None, **config):
    base = {"path": "/backup", "ssh_sudo": True, "passwordless": True}
    base.update(config)
    ep = SSHEndpoint(hostname="backup-host", config=base)
    if compress:
        # Set the way the transfer layer sets it -- on the built endpoint's
        # config -- not through the constructor, whose key whitelist is the
        # endpoint's own business.
        ep.config["compress"] = compress
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


@pytest.fixture
def pipeline(monkeypatch):
    """Capture what the ssh endpoint hands the pipefail runner, and what it
    hands a plain Popen, so the two paths can be told apart."""
    captured: dict = {}

    def fake_pipefail(shell_cmd, **kw):
        captured["pipeline"] = shell_cmd
        captured["popen_kw"] = kw
        proc = MagicMock(spec=subprocess.Popen)
        proc.stdin = MagicMock()
        captured["proc"] = proc
        return proc

    def fake_popen(cmd, **kw):
        captured["argv"] = cmd
        return MagicMock(spec=subprocess.Popen)

    monkeypatch.setattr(
        "btrfs_backup_ng.core.transfer.popen_pipeline_pipefail", fake_pipefail
    )
    monkeypatch.setattr(ssh_mod.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(ssh_mod, "tail_stderr", lambda *a, **k: None)
    return captured


class TestTheRemoteCompressesAndThisHostDecompresses:
    def test_the_send_runs_through_a_compressor_and_a_local_decompressor(
        self, pipeline
    ):
        ep = _ep(compress="zstd")
        ep.send(_snap("/backup/snap-1"))
        assert "argv" not in pipeline, "the uncompressed Popen path ran"
        line = pipeline["pipeline"]
        remote, local = line.rsplit(" | ", 1)
        assert local == "zstd -dc -T0", local
        assert remote.startswith("ssh -o ControlPath=/tmp/cm user@backup-host ")
        assert "btrfs send" in remote and "/backup/snap-1" in remote
        assert "zstd -c -T0" in remote, "the remote does not compress"
        assert "sudo" in remote, "ssh_sudo was dropped from the remote send"
        assert pipeline["popen_kw"]["stdout"] is subprocess.PIPE
        assert pipeline["popen_kw"]["stdin"] is subprocess.DEVNULL

    def test_the_method_is_the_one_configured(self, pipeline):
        ep = _ep(compress="gzip")
        ep.send(_snap("/backup/snap-1"))
        assert "gzip -c" in pipeline["pipeline"]
        assert pipeline["pipeline"].endswith("| gzip -dc")

    def test_an_incremental_send_keeps_its_parent(self, pipeline):
        ep = _ep(compress="zstd")
        ep.send(_snap("/backup/snap-1"), parent=_snap("/backup/snap-0"))
        assert "-p /backup/snap-0" in pipeline["pipeline"]

    def test_without_compress_the_plain_path_is_untouched(self, pipeline):
        ep = _ep()
        ep.send(_snap("/backup/snap-1"))
        assert "pipeline" not in pipeline
        assert pipeline["argv"][:4] == [
            "ssh",
            "-o",
            "ControlPath=/tmp/cm",
            "user@backup-host",
        ]

    def test_a_password_remote_still_gets_its_password_first(self, pipeline):
        """``sudo -S btrfs send`` is the first remote stage and inherits ssh's
        stdin, so the password line is written there and nowhere else."""
        ep = _ep(compress="zstd", passwordless=False)
        ep._get_sudo_password = lambda *a, **k: "hunter2"  # type: ignore[method-assign]
        ep.send(_snap("/backup/snap-1"))
        assert "sudo -S btrfs send" in pipeline["pipeline"]
        assert pipeline["popen_kw"]["stdin"] is subprocess.PIPE
        pipeline["proc"].stdin.write.assert_called_once_with(b"hunter2\n")
        pipeline["proc"].stdin.close.assert_called_once()

    def test_a_missing_local_decompressor_refuses_before_connecting(
        self, pipeline, monkeypatch
    ):
        monkeypatch.setattr(
            "btrfs_backup_ng.core.transfer.check_compression_available",
            lambda m: False,
        )
        ep = _ep(compress="zstd")
        with pytest.raises(__util__.AbortError, match="'zstd'"):
            ep.send(_snap("/backup/snap-1"))
        assert "pipeline" not in pipeline and "argv" not in pipeline

    def test_it_says_so_on_the_log(self, pipeline, shared_log):
        """The tier3 zstd-over-ssh cell requires this line in the restore's
        output; it is what tells a compressed restore leg from a plain one."""
        ep = _ep(compress="zstd")
        ep.send(_snap("/backup/snap-1"))
        assert any(
            "Decompressing the restore stream with zstd" in m
            for m in shared_log.messages(logging.INFO)
        )


def _shells():
    found = ["sh"]
    if shutil.which("dash"):
        found.append("dash")
    return found


class TestTheRemoteScriptExitsWithTheSendsStatus:
    """A real shell runs the script the remote will run, with ``cat`` standing
    in for the compressor so the bytes can be read back."""

    def _run(self, shell, remote_send, stdin=b""):
        script = _compressed_send_script(remote_send, "cat")
        # The builder returns `sh -c '<script>'` for the remote login shell;
        # here the chosen shell runs the inner script directly.
        inner = script[len("sh -c ") :]
        return subprocess.run(
            [shell, "-c", "eval " + inner],
            input=stdin,
            capture_output=True,
            timeout=30,
        )

    @pytest.mark.parametrize("shell", _shells())
    def test_a_failing_send_fails_the_script_even_through_the_compressor(self, shell):
        """This is the case a plain pipeline gets wrong: ``cat`` exits 0 after
        the send failed, and the pipeline's status would be 0."""
        r = self._run(shell, "printf abc; false")
        assert r.stdout == b"abc"
        assert r.returncode == 1, r.stderr

    @pytest.mark.parametrize("shell", _shells())
    def test_a_clean_send_exits_zero_with_its_bytes_on_stdout(self, shell):
        r = self._run(shell, "printf abc")
        assert r.stdout == b"abc" and r.returncode == 0

    @pytest.mark.parametrize("shell", _shells())
    def test_the_send_stage_reads_the_sessions_stdin(self, shell):
        """Where ``sudo -S`` sits, the password must arrive: the first stage
        inherits the script's stdin, the compressor reads only the pipe."""
        r = self._run(shell, 'IFS= read -r pw; printf "%s-data" "$pw"', b"hunter2\n")
        assert r.stdout == b"hunter2-data" and r.returncode == 0

    def test_the_status_carried_is_the_sends_not_a_constant(self):
        # A process exiting 7, as `btrfs send` would; a shell `exit` here
        # would leave the enclosing group before the status is recorded.
        r = self._run("sh", "printf abc; (exit 7)")
        assert r.returncode == 7


class TestTheTransferLayerHandsCompressToARemoteSource:
    def _drive(self, tmp_path, monkeypatch, source_endpoint, compress="zstd"):
        snapshot = MagicMock()
        snapshot.get_name.return_value = "p-1"
        snapshot.get_path.return_value = "/backup/p-1"
        snapshot.__str__ = lambda self: "p-1"  # type: ignore[assignment]
        snapshot.stream_uuid = ""
        snapshot.endpoint = source_endpoint
        dest = MagicMock()
        dest.config = {"path": str(tmp_path)}
        dest._is_remote = False
        seen: dict = {}

        def fake_transfer(
            send_process, destination_endpoint, receive_process, is_ssh, **kw
        ):
            seen["compress"] = kw.get("compress")
            return [0, 0]

        monkeypatch.setattr(ops, "_do_process_transfer", fake_transfer)
        monkeypatch.setattr(ops.transfer_utils, "finish_stderr", lambda *a: None)
        ops.send_snapshot(
            snapshot, dest, options={"compress": compress, "check_space": False}
        )
        return seen

    def test_a_remote_btrfs_source_is_handed_the_method(self, tmp_path, monkeypatch):
        source = _ep()
        source.send = MagicMock(return_value=MagicMock(spec=subprocess.Popen))  # type: ignore[method-assign]
        seen = self._drive(tmp_path, monkeypatch, source)
        assert source.config["compress"] == "zstd", (
            "the ssh source never learned the method, so --compress is inert"
        )
        assert seen["compress"] == "none", (
            "the transfer layer would compress a second time, with no decompressor"
        )

    def test_a_local_source_to_a_local_destination_drops_it_and_says_so(
        self, tmp_path, monkeypatch, caplog
    ):
        source = MagicMock()
        source._is_remote = False
        source.config = {}
        source.send.return_value = MagicMock(spec=subprocess.Popen)
        with caplog.at_level(logging.INFO, logger=ops.logger.name):
            seen = self._drive(tmp_path, monkeypatch, source)
        assert "compress" not in source.config
        assert seen["compress"] == "none"
        assert any("Not compressing" in r.getMessage() for r in caplog.records)

    def test_a_remote_raw_source_is_not_handed_a_wire_method(
        self, tmp_path, monkeypatch
    ):
        """A raw+ssh store's compression is at rest and recorded in the
        sidecar; the wire read-back is the raw endpoint's own pipeline."""
        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

        source = SSHRawEndpoint(config={"path": "/raw", "hostname": "nas"})
        source.send = MagicMock(return_value=MagicMock(spec=subprocess.Popen))  # type: ignore[method-assign]
        before = dict(source.config)
        self._drive(tmp_path, monkeypatch, source)
        assert source.config.get("compress") == before.get("compress")


def test_the_pipefail_runner_has_one_home(tmp_path):
    """raw.py used to own it; the ssh endpoint needs the same runner, and two
    copies would drift. The raw module keeps its old name as an alias so its
    tests' patches keep landing."""
    import btrfs_backup_ng.core.transfer as transfer_mod
    import btrfs_backup_ng.endpoint.raw as raw_mod

    assert raw_mod._popen_pipeline_pipefail is transfer_mod.popen_pipeline_pipefail
    src = Path(raw_mod.__file__).read_text()
    assert "def _popen_pipeline_pipefail" not in src
