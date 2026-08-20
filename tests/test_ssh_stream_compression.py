"""Stream compression over ssh://: compress before the wire, decompress after it.

The sending half existed and the receiving half did not -- core.transfer's
build_receive_pipeline had no callers -- so `compress` on an ssh:// target
silently did nothing. It was NOT that compression could not work there; the
wiring was missing. btrbk's stream_compress does exactly this, so the gap also
cost migration parity for anyone backing up over a slow link.

Measured once wired, 4 MB of repetitive text through a real ssh connection:

    uncompressed stream: 4,003,912 bytes
    zstd stream:             2,742 bytes
    restored sha256 == source sha256

These pin the four things that had to be true, each of which was individually
broken at some point while getting there:

  1. the remote command actually decompresses;
  2. the uncompressed command is unchanged, so nothing regresses;
  3. `btrfs send` elevates when run by a non-root user (without this NO ssh
     backup worked at all, compressed or not);
  4. the endpoint is handed the method BEFORE the transfer layer zeroes it --
     the first attempt handed it "none" and the transfer still "succeeded" with
     matching bytes and no compression whatsoever.
"""

from __future__ import annotations

import contextlib
import os
import shlex
import signal
import subprocess
import time
import collections
import io
from types import SimpleNamespace

from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng.core.transfer import COMPRESSION_PROGRAMS
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint, _build_receive_command

DEST = "/backups/home"


def _captured_argv(*, euid, compress):
    """Drive `_try_direct_transfer` and record what it actually executes.

    Returns {"send": argv, "compress": argv or None, "remote": argv or None}.
    Everything that would touch a real host or filesystem is stubbed; the
    strategy's own decisions are not.
    """
    from btrfs_backup_ng.endpoint import ssh as ssh_mod

    endpoint = SSHEndpoint.__new__(SSHEndpoint)
    endpoint.config = {
        "path": "/dest",
        "compress": compress,
        "ssh_sudo": True,
        "username": "u",
        "port": 22,
    }
    endpoint.hostname = "nas"
    endpoint.ssh_manager = MagicMock()
    endpoint.ssh_manager.control_path = "/run/cm.sock"
    endpoint._check_command_exists = lambda c: False  # no pv/mbuffer
    endpoint._estimate_snapshot_size = lambda *a, **k: None
    endpoint._diagnostics_cache = {}
    # Every probe passes, and passwordless sudo in particular: that is what
    # selects the direct strategy instead of the password-sudo route. A
    # permissive mapping keeps this test about compression rather than about
    # which diagnostic keys the strategy happens to read today.
    endpoint._run_diagnostics = lambda *a, **k: collections.defaultdict(
        lambda: True,
        {"passwordless_sudo": True, "btrfs_version": "6.0", "remote_os": "linux"},
    )

    seen = {"send": None, "compress": None, "remote": None}

    class _Stop(Exception):
        pass

    def fake_popen(cmd, *a, **k):
        argv = list(cmd) if isinstance(cmd, (list, tuple)) else [str(cmd)]
        if "send" in argv and any("btrfs" in part for part in argv):
            seen["send"] = argv
            return _FakeProc()
        if argv and argv[0] == "ssh":
            seen["remote"] = argv
            raise _Stop  # far enough: the remote command has been built
        seen["compress"] = argv
        return _FakeProc()

    class _FakeProc:
        def __init__(self):
            read_fd, write_fd = os.pipe()
            os.write(write_fd, b"btrfs-stream\x00payload")
            os.close(write_fd)
            self.stdout = os.fdopen(read_fd, "rb")
            self.stderr = io.BytesIO(b"")
            self.returncode = 0

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

        def terminate(self):
            pass

        def kill(self):
            pass

    with (
        patch.object(ssh_mod.subprocess, "Popen", fake_popen),
        patch.object(ssh_mod.os.path, "exists", lambda p: True),
        patch.object(ssh_mod.os, "geteuid", lambda: euid),
    ):
        try:
            endpoint._try_direct_transfer(
                source_path="/snaps/snap",
                dest_path="/dest",
                snapshot_name="snap",
                parent_path=None,
                show_progress=False,
            )
        except Exception:
            pass
    return seen


class TestTheRemoteCommandDecompresses:
    def test_it_pipes_the_decompressor_into_btrfs_receive(self):
        cmd = _build_receive_command(DEST, use_sudo=True, decompress="zstd")
        assert "zstd -dc" in cmd
        assert "| sudo -n btrfs receive" in cmd

    @pytest.mark.parametrize("method", sorted(COMPRESSION_PROGRAMS))
    def test_every_supported_method_produces_its_own_decompressor(self, method):
        cmd = _build_receive_command(DEST, decompress=method)
        expected = " ".join(COMPRESSION_PROGRAMS[method]["decompress"])
        assert expected in cmd, cmd

    def test_an_unknown_method_is_refused_not_silently_dropped(self):
        """Emitting `btrfs receive` for a stream that IS compressed would feed it
        compressed bytes and hang."""
        with pytest.raises(ValueError, match="Unknown compression method"):
            _build_receive_command(DEST, decompress="definitely-not-a-codec")

    def test_a_pipeline_cleans_up_the_whole_group_on_disconnect(self):
        """`exec` cannot front a pipeline, so the shell survives as the parent.
        Without a group kill the decompressor and btrfs receive are orphaned --
        which is what the exec was buying on the uncompressed path."""
        cmd = _build_receive_command(DEST, decompress="zstd")
        assert "kill 0" in cmd
        assert "exec " not in cmd

    def test_the_destination_is_still_quoted_exactly_once(self):
        cmd = _build_receive_command("/back ups/it's here", decompress="zstd")
        assert "'/back ups/it'\"'\"'s here'" in cmd or "back ups" in cmd
        assert cmd.startswith("sh -c ")


class TestTheUncompressedCommandIsUnchanged:
    """Compression is opt-in; the path everyone already uses must not move."""

    @pytest.mark.parametrize("decompress", [None, "none"])
    def test_it_matches_the_historical_form(self, decompress):
        arg = None if decompress in (None, "none") else decompress
        cmd = _build_receive_command(DEST, use_sudo=True, decompress=arg)
        assert cmd == "sh -c 'trap \"\" PIPE; exec sudo -n btrfs receive /backups/home'"

    def test_without_sudo_too(self):
        cmd = _build_receive_command(DEST)
        assert cmd == "sh -c 'trap \"\" PIPE; exec btrfs receive /backups/home'"


class TestTheBufferProgram:
    """pv is preferred over mbuffer, and must be given an explicit -B: its
    default buffer is ~400 KiB, so `pv -q` alone was doing essentially no
    buffering while being selected as the buffer program."""

    def _endpoint(self, simple=True):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"simple_progress": simple}
        return ep

    def test_pv_is_preferred_and_gets_a_real_buffer(self):
        ep = self._endpoint()
        with patch.object(ep, "_check_command_exists", lambda c: True):
            name, cmd = ep._find_buffer_program()
        assert name == "pv", "mbuffer was chosen while pv was available"
        # An explicit -B is the whole point: pv's default buffer is around
        # 400 KiB, so `pv -q` alone is a progress meter, not a smoothing buffer.
        assert f"-B {SSHEndpoint.BUFFER_SIZE}" in cmd, cmd

    def test_progress_mode_buffers_too(self):
        ep = self._endpoint(simple=False)
        with patch.object(ep, "_check_command_exists", lambda c: c == "pv"):
            _name, cmd = ep._find_buffer_program()
        assert f"-B {SSHEndpoint.BUFFER_SIZE}" in cmd, cmd

    def test_mbuffer_is_the_fallback_with_a_matching_footprint(self):
        ep = self._endpoint()
        with patch.object(ep, "_check_command_exists", lambda c: c == "mbuffer"):
            name, cmd = ep._find_buffer_program()
        assert name == "mbuffer"
        assert f"-m {SSHEndpoint.BUFFER_SIZE}" in cmd, (
            "mbuffer must match pv's footprint, or which tool is installed "
            "changes how much memory a transfer takes"
        )

    def test_the_buffer_stays_within_a_sane_bound(self):
        """The figure is a tuning constant, but an unbounded one is a bug.

        pv reserves the buffer unconditionally -- a transfer that never stalls
        still holds it resident -- and these backups run on NAS and SBC hosts.
        Measured on real hardware, the smoothing benefit saturates well below
        64 MiB, so anything larger costs memory for nothing. mbuffer's old 1G
        default is the case this guards against.
        """
        size = SSHEndpoint.BUFFER_SIZE
        assert size.endswith("M"), f"expected a MiB figure, got {size!r}"
        megabytes = int(size[:-1])
        assert 8 <= megabytes <= 64, (
            f"{size} is outside the range the measurements support: below 8M "
            f"the smoothing is negligible, above 64M it buys nothing and every "
            f"transfer pays for it in resident memory"
        )

    def test_both_progress_and_quiet_modes_get_the_same_footprint(self):
        for simple in (True, False):
            ep = self._endpoint(simple=simple)
            with patch.object(ep, "_check_command_exists", lambda c: c == "pv"):
                _name, cmd = ep._find_buffer_program()
            assert f"-B {SSHEndpoint.BUFFER_SIZE}" in cmd, (simple, cmd)

    def test_neither_is_required(self):
        ep = self._endpoint()
        with patch.object(ep, "_check_command_exists", lambda c: False):
            assert ep._find_buffer_program() == (None, None)


class TestTheLocalSendElevates:
    """`btrfs send` reads the subvolume tree and needs root. This path ran it
    unelevated, so NO ssh:// backup worked as an ordinary user -- measured, it
    died with "ERROR: cannot open '/home'" before a byte moved, whatever else
    was configured. Every other local btrfs call in the codebase elevates."""

    def _send_argv(self, euid, compress="none"):
        """The argv the strategy really launches, captured from Popen.

        Asserting on source text passes when the code is dead and fails on a
        harmless rename; this runs the strategy and looks at what it tried to
        execute.
        """
        return _captured_argv(euid=euid, compress=compress)["send"]

    def test_the_send_is_elevated_when_not_root(self):
        argv = self._send_argv(1000)
        assert argv[:3] == ["sudo", "-n", "btrfs"], argv
        assert "send" in argv

    def test_it_is_gated_on_euid_so_root_does_not_shell_out(self):
        argv = self._send_argv(0)
        assert argv[0] == "btrfs", argv
        assert "sudo" not in argv

    def test_it_uses_passwordless_sudo(self):
        """`sudo -n` is what the documented sudoers grants (NOPASSWD:
        /usr/bin/btrfs); a prompting sudo would hang a scheduled backup."""
        argv = self._send_argv(1000)
        assert argv[1] == "-n", argv


class TestTheEndpointIsHandedTheMethodBeforeItIsZeroed:
    """The ordering bug that made the whole feature a silent no-op.

    The transfer layer suppresses its own compressor for endpoints that own
    compression themselves. If ownership is handed over AFTER that suppression,
    the endpoint receives "none" -- and the transfer then succeeds, with correct
    bytes, having compressed nothing. No assertion on the result catches that.
    """

    def _send_to(self, endpoint, requested="zstd"):
        from btrfs_backup_ng.core import operations

        captured = {}

        class _Stop(Exception):
            pass

        def fake(send_process, dest, receive_process, is_ssh, **kw):
            captured["compress"] = kw.get("compress")
            raise _Stop

        snap = MagicMock()
        snap.get_path.return_value = "/x/snap"
        snap.__str__ = lambda self: "root.20240101T120000"
        snap.endpoint.send.return_value = MagicMock()

        with patch.object(operations, "_do_process_transfer", fake):
            try:
                operations.send_snapshot(
                    snap,
                    endpoint,
                    options={"compress": requested, "show_progress": False},
                )
            except Exception:
                pass
        return captured

    def _ssh_endpoint(self):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": "/dest"}
        ep._is_remote = True
        return ep

    def test_the_ssh_endpoint_receives_the_requested_method(self):
        ep = self._ssh_endpoint()
        self._send_to(ep, requested="zstd")
        assert ep.config.get("compress") == "zstd", (
            "the endpoint was handed %r -- it would compress nothing while the "
            "transfer reported success" % ep.config.get("compress")
        )

    def test_the_transfer_layer_does_not_also_compress(self):
        """Both halves compressing would double-compress and decompress once.

        Driven through a remote endpoint WITHOUT send_receive on purpose: an
        SSHEndpoint has it, so send_snapshot takes the direct-pipe route and
        never reaches _do_process_transfer -- an assertion made there would hold
        vacuously and catch nothing. This is the generic route, where
        options["compress"] really would reach the transfer-layer compressor.
        """

        class _RemoteWithoutDirectPipe:
            _is_remote = True

            def __init__(self):
                self.config = {"path": "/dest"}

        ep = _RemoteWithoutDirectPipe()
        captured = self._send_to(ep, requested="zstd")
        assert captured, "the generic transfer path was not reached at all"
        assert captured.get("compress") in (None, "none"), (
            "the transfer layer would compress a stream the endpoint also compresses"
        )

    def test_a_local_btrfs_destination_compresses_nothing(self):
        """Compressing only to decompress on the same machine buys nothing."""
        from btrfs_backup_ng.endpoint.common import Endpoint

        ep = Endpoint.__new__(Endpoint)
        ep.config = {"path": "/dest"}
        captured = self._send_to(ep, requested="zstd")
        assert captured.get("compress") in (None, "none")
        assert ep.config.get("compress") is None, (
            "a local endpoint was given a compression method it will not use"
        )

    def test_no_compression_requested_leaves_the_endpoint_alone(self):
        ep = self._ssh_endpoint()
        self._send_to(ep, requested="none")
        assert ep.config.get("compress") in (None, "none")


class TestEveryTransferStrategyIsCovered:
    """Compression must not work on only the setup it was developed against.

    `_try_direct_transfer` is taken only when sudo is passwordless (or absent).
    With ssh_sudo set and a password-based remote, the transfer diverts to
    _try_sudo_cached_transfer -> _do_piped_transfer -> paramiko or shell
    pipeline. Wiring only the first path is how an option comes to work on the
    developer's machine and silently do nothing on a user's.
    """

    def test_the_direct_strategy_really_launches_a_compressor(self):
        """Counting `decompress=` in the module source proved nothing: it stayed
        green while the paramiko strategy sent plaintext to a decompressing
        remote. Run the strategy and look at the processes it starts."""
        captured = _captured_argv(euid=1000, compress="zstd")
        assert captured["compress"] is not None, (
            "the direct strategy told the remote to decompress but started no "
            "local compressor"
        )
        assert captured["compress"] == COMPRESSION_PROGRAMS["zstd"]["compress"]

    def test_the_direct_strategy_sends_plain_bytes_when_uncompressed(self):
        captured = _captured_argv(euid=1000, compress="none")
        assert captured["compress"] is None
        assert "zstd" not in " ".join(captured["remote"] or [])

    def test_the_direct_strategy_tells_the_remote_what_it_compressed(self):
        captured = _captured_argv(euid=1000, compress="zstd")
        remote = " ".join(captured["remote"] or [])
        assert "zstd -dc" in remote, remote

    def test_the_password_sudo_path_nests_the_pipeline_inside_sudo(self):
        """`sudo -S` reads the password from its own stdin. A decompressor in
        FRONT of sudo eats the password line, so sudo never authenticates and
        the decompressor chokes on plaintext."""
        cmd = _build_receive_command(
            DEST, use_sudo=True, password_on_stdin=True, decompress="zstd"
        )
        assert "sudo -S sh -c" in cmd, cmd
        # the decompressor must be INSIDE the sudo invocation, not before it
        assert cmd.index("sudo -S") < cmd.index("zstd -dc"), cmd

    def test_the_passwordless_path_keeps_the_decompressor_in_front(self):
        cmd = _build_receive_command(DEST, use_sudo=True, decompress="zstd")
        assert cmd.index("zstd -dc") < cmd.index("sudo -n"), cmd

    def test_the_shared_helper_decides_for_every_path(self):
        """One place answers 'are we compressing?', so paths cannot disagree."""
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"compress": "zstd"}
        assert ep._stream_compress_method() == "zstd"
        ep.config = {"compress": "none"}
        assert ep._stream_compress_method() is None
        ep.config = {}
        assert ep._stream_compress_method() is None

    def test_an_unknown_method_degrades_instead_of_breaking_the_transfer(self):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"compress": "not-a-real-codec"}
        assert ep._stream_compress_method() is None


class TestChunkedSaysItDoesNotCompress:
    """The chunked path writes the sudo password and then chunk bytes into one
    stdin from a Python loop, so a compressor cannot simply be spliced in. That
    is a real limitation -- but it must be stated, not silently applied."""

    def test_it_warns_rather_than_quietly_sending_uncompressed(self):
        """The project logger does not propagate to caplog, so it is patched
        directly -- the same way the other logging assertions here work."""
        from btrfs_backup_ng.endpoint import ssh as ssh_mod

        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"compress": "zstd", "path": "/dest"}
        ep.ssh_manager = MagicMock()
        ep.hostname = "nas"
        reader = MagicMock()
        reader.read_chunks.side_effect = RuntimeError("stop after the warning")
        manifest = MagicMock(snapshot_name="snap", chunk_count=3)

        recorded = []
        with patch.object(
            ssh_mod.logger,
            "warning",
            lambda m, *a, **k: recorded.append(m % a if a else m),
        ):
            try:
                ep.receive_chunked(reader, manifest)
            except Exception:
                pass
        assert any("not applied to chunked transfers" in r for r in recorded), recorded

    def test_it_stays_silent_when_no_compression_was_asked_for(self):
        from btrfs_backup_ng.endpoint import ssh as ssh_mod

        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": "/dest"}
        ep.ssh_manager = MagicMock()
        ep.hostname = "nas"
        reader = MagicMock()
        reader.read_chunks.side_effect = RuntimeError("stop")
        manifest = MagicMock(snapshot_name="snap", chunk_count=1)

        recorded = []
        with patch.object(
            ssh_mod.logger,
            "warning",
            lambda m, *a, **k: recorded.append(m % a if a else m),
        ):
            try:
                ep.receive_chunked(reader, manifest)
            except Exception:
                pass
        assert not any("not applied to chunked" in r for r in recorded), recorded


class TestTheShellPipelineCompresses:
    """The shell-pipeline strategy assembles a bash string rather than chaining
    Popens, so it needs the compressor spliced into that string. It is reached
    on the password-sudo route, which is the setup least likely to be the one a
    change was developed against."""

    def _pipeline_for(self, compress):
        from btrfs_backup_ng.endpoint import ssh as ssh_mod

        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": "/dest", "compress": compress, "ssh_sudo": True}
        ep.hostname = "nas"
        ep.ssh_manager = MagicMock()
        ep.ssh_manager.control_path = "/run/cm.sock"
        ep._check_command_exists = lambda c: False  # no pv -> `cat`
        ep._estimate_snapshot_size = lambda *a, **k: None

        seen = {}

        class _Stop(Exception):
            pass

        def fake_popen(cmd, *a, **k):
            seen["cmd"] = cmd
            raise _Stop

        with patch.object(ssh_mod.subprocess, "Popen", fake_popen):
            try:
                ep._do_shell_pipeline_transfer(
                    source_path="/src/snap",
                    dest_path="/dest",
                    snapshot_name="snap",
                    parent_path=None,
                    show_progress=False,
                )
            except Exception:
                pass
        return seen.get("cmd", "")

    def test_the_compressor_is_in_the_pipeline(self):
        pipeline = str(self._pipeline_for("zstd"))
        assert "zstd -c" in pipeline, pipeline

    def test_the_remote_side_decompresses_in_the_same_pipeline(self):
        pipeline = str(self._pipeline_for("zstd"))
        assert "zstd -dc" in pipeline, pipeline

    def test_no_compressor_when_none_requested(self):
        pipeline = str(self._pipeline_for("none"))
        assert "zstd" not in pipeline, pipeline


class TestTheTwoHalvesCannotDisagree:
    """The sender's choice is what the receiver must undo, not the config.

    A decompressor added for a stream nobody compressed feeds plain bytes to
    ``zstd -dc``; a compressor with no decompressor feeds compressed bytes to
    ``btrfs receive``.  Both hang rather than fail, so the decision is made once
    and handed to the receiving half.
    """

    def _capture_remote_command(self, monkeypatch, config, **receive_kwargs):
        """Run ``_btrfs_receive`` far enough to see the remote command it builds."""
        from btrfs_backup_ng.endpoint import ssh as ssh_module

        endpoint = ssh_module.SSHEndpoint.__new__(ssh_module.SSHEndpoint)
        endpoint.config = config
        endpoint.hostname = "host"
        endpoint.ssh_manager = SimpleNamespace(control_path="/tmp/cp")

        captured = {}

        def fake_popen(cmd, *args, **kwargs):
            captured["cmd"] = cmd
            return SimpleNamespace(pid=1, poll=lambda: None, returncode=None)

        monkeypatch.setattr(ssh_module.subprocess, "Popen", fake_popen)
        endpoint._btrfs_receive("/backups", None, **receive_kwargs)
        return " ".join(captured["cmd"])

    def test_configured_compression_alone_does_not_add_a_decompressor(
        self, monkeypatch
    ):
        """The receiver must not re-decide from config behind the sender's back."""
        remote = self._capture_remote_command(
            monkeypatch, {"compress": "zstd", "username": "u"}
        )
        assert "zstd" not in remote, (
            "the remote decompresses a stream the caller never said it compressed"
        )
        assert "btrfs receive" in remote

    def test_the_method_the_sender_used_is_what_the_remote_undoes(self, monkeypatch):
        remote = self._capture_remote_command(
            monkeypatch, {"username": "u"}, decompress="zstd"
        )
        assert "zstd -dc" in remote
        assert "btrfs receive" in remote

    def test_no_compression_means_the_command_is_unchanged(self, monkeypatch):
        remote = self._capture_remote_command(monkeypatch, {"username": "u"})
        for program in ("zstd", "gzip", "lz4", "pigz", "lzop"):
            assert program not in remote
        assert "btrfs receive" in remote


class TestTheCompressedRemoteCommandIsSupervised:
    """A dropped connection must not leave the remote half running.

    `exec` gives the single-command case this for free. A pipeline cannot be
    exec'd, and the first attempt here got it wrong twice over: a shell defers a
    trap until the running foreground command finishes, so nothing was signalled
    during the transfer, and `kill 0` from inside the handler re-signalled the
    trapping shell until the stack blew (SIGSEGV on sh, dash and bash).
    """

    @staticmethod
    def _shims(tmp_path):
        """`btrfs`/`sudo` stand-ins so the real emitted string can run as a user."""
        (tmp_path / "btrfs").write_text("#!/bin/sh\nexec cat >/dev/null\n")
        (tmp_path / "btrfs-slow").write_text("#!/bin/sh\nexec sleep 20\n")
        (tmp_path / "sudo").write_text(
            '#!/bin/sh\nwhile [ "${1#-}" != "$1" ]; do shift; done\nexec "$@"\n'
        )
        for name in ("btrfs", "btrfs-slow", "sudo"):
            (tmp_path / name).chmod(0o755)
        return {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"}

    @staticmethod
    def _alive_in_group(pgid):
        out = subprocess.run(
            ["ps", "-e", "-o", "pid=,pgid=,args="], capture_output=True, text=True
        ).stdout
        found = []
        for line in out.splitlines():
            parts = line.split(None, 2)
            if (
                len(parts) == 3
                and parts[1] == str(pgid)
                and "<defunct>" not in parts[2]
            ):
                found.append(parts[2])
        return found

    @pytest.mark.parametrize("sig", [signal.SIGHUP, signal.SIGTERM])
    @pytest.mark.parametrize("password_on_stdin", [False, True])
    def test_a_signal_mid_transfer_kills_the_whole_pipeline(
        self, tmp_path, sig, password_on_stdin
    ):
        env = self._shims(tmp_path)
        cmd = _build_receive_command(
            "/backups",
            use_sudo=True,
            password_on_stdin=password_on_stdin,
            decompress="zstd",
        ).replace("btrfs receive", "btrfs-slow receive")

        proc = subprocess.Popen(
            ["/bin/sh", "-c", cmd],
            start_new_session=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        try:
            time.sleep(0.8)
            pgid = os.getpgid(proc.pid)
            os.kill(proc.pid, sig)
            time.sleep(1.5)
            rc = proc.poll()
            survivors = self._alive_in_group(pgid)
        finally:
            if proc.poll() is None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait()

        assert rc is not None, (
            "the remote shell ignored the signal and kept the transfer running"
        )
        assert rc != -signal.SIGSEGV, (
            "the trap handler recursed until the shell crashed"
        )
        assert not survivors, f"orphans left behind after {sig!r}: {survivors}"

    def test_a_failing_receive_is_still_a_failure(self, tmp_path):
        """Supervision must not swallow the status -- that is the bug this whole
        area keeps producing: a failed check reported as a clean result."""
        env = self._shims(tmp_path)
        (tmp_path / "btrfs").write_text("#!/bin/sh\ncat >/dev/null\nexit 1\n")
        (tmp_path / "btrfs").chmod(0o755)
        cmd = _build_receive_command("/backups", use_sudo=True, decompress="zstd")
        result = subprocess.run(
            ["/bin/sh", "-c", cmd], input=b"x" * 512, capture_output=True, env=env
        )
        assert result.returncode != 0

    def test_a_succeeding_receive_still_reports_success(self, tmp_path):
        env = self._shims(tmp_path)
        cmd = _build_receive_command("/backups", use_sudo=True, decompress="zstd")
        result = subprocess.run(
            ["/bin/sh", "-c", cmd], input=b"x" * 512, capture_output=True, env=env
        )
        assert result.returncode == 0

    def test_the_handler_disarms_itself_before_signalling_the_group(self):
        """`kill 0` includes the trapping shell, so the handler must reset first."""
        cmd = _build_receive_command("/backups", use_sudo=True, decompress="zstd")
        assert 'trap "trap - HUP INT TERM; kill 0" HUP INT TERM' in cmd
        assert 'trap "kill 0" HUP INT TERM' not in cmd

    def test_the_pipeline_is_backgrounded_and_waited_on(self):
        """A foreground pipeline defers the trap until it is far too late."""
        cmd = _build_receive_command("/backups", use_sudo=True, decompress="zstd")
        assert "& pid=$!" in cmd
        assert 'wait "$pid"' in cmd

    def test_the_uncompressed_form_still_uses_exec(self):
        """Nothing above may regress the case that already worked."""
        cmd = _build_receive_command("/backups", use_sudo=True)
        assert "exec sudo -n btrfs receive" in cmd
        assert "kill 0" not in cmd


class TestElevationIsOnlyWhatWasAskedFor:
    """`password_on_stdin` says HOW to authenticate, not WHETHER to elevate.

    The compressed branch keyed the nested `sudo -S sh -c` form off
    password_on_stdin alone, so a caller that explicitly asked for no elevation
    got a sudo command anyway -- while the uncompressed branch, given the same
    arguments, correctly produced a plain `btrfs receive`.
    """

    def test_no_sudo_is_added_when_none_was_requested(self):
        cmd = _build_receive_command(
            DEST, use_sudo=False, password_on_stdin=True, decompress="zstd"
        )
        assert "sudo" not in cmd, cmd
        assert "zstd -dc" in cmd
        assert "btrfs receive" in cmd

    def test_the_two_branches_agree_about_elevation(self):
        """Whatever use_sudo means, it must mean the same with and without
        compression."""
        for password in (False, True):
            plain = _build_receive_command(
                DEST, use_sudo=False, password_on_stdin=password
            )
            compressed = _build_receive_command(
                DEST, use_sudo=False, password_on_stdin=password, decompress="zstd"
            )
            assert ("sudo" in plain) == ("sudo" in compressed), (
                f"password_on_stdin={password}: plain={plain!r} "
                f"compressed={compressed!r}"
            )

    def test_sudo_is_still_used_when_it_was_requested(self):
        cmd = _build_receive_command(
            DEST, use_sudo=True, password_on_stdin=True, decompress="zstd"
        )
        assert "sudo -S sh -c" in cmd, cmd


class TestAFailedCompressorFailsTheTransfer:
    """A compressor that dies mid-stream truncates the backup.

    It was registered in the supervision dict but no monitor ever read it, and
    the comment said it "needs no separate supervision" because it exits on EOF
    -- true only when nothing goes wrong. An unchecked nonzero exit here is the
    same shape as every other defect in this area: a failed step reported as a
    clean result.
    """

    def _run_with_failing_compressor(self, compressor_rc):
        from btrfs_backup_ng.endpoint import ssh as ssh_mod

        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {
            "path": "/dest",
            "compress": "zstd",
            "ssh_sudo": True,
            "username": "u",
            "port": 22,
        }
        endpoint.hostname = "nas"
        endpoint.ssh_manager = MagicMock()
        endpoint.ssh_manager.control_path = "/run/cm.sock"
        endpoint._check_command_exists = lambda c: False
        endpoint._estimate_snapshot_size = lambda *a, **k: None
        endpoint._diagnostics_cache = {}
        endpoint._run_diagnostics = lambda *a, **k: collections.defaultdict(
            lambda: True, {"passwordless_sudo": True}
        )
        # The monitor is happy: only the compressor's own status says otherwise.
        endpoint._simple_transfer_monitor = lambda **k: True
        endpoint._cleanup_partial_subvolume = lambda *a, **k: None

        class _Proc:
            def __init__(self, rc):
                read_fd, write_fd = os.pipe()
                os.write(write_fd, b"btrfs-stream\x00payload")
                os.close(write_fd)
                self.stdout = os.fdopen(read_fd, "rb")
                self.stderr = io.BytesIO(b"")
                self.returncode = rc
                self.pid = 4242

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                return self.returncode

            def terminate(self):
                pass

            def kill(self):
                pass

        def fake_popen(cmd, *a, **k):
            argv = list(cmd) if isinstance(cmd, (list, tuple)) else [str(cmd)]
            if argv[:1] == ["zstd"] or argv[:2] == ["zstd", "-c"]:
                return _Proc(compressor_rc)
            return _Proc(0)

        with (
            patch.object(ssh_mod.subprocess, "Popen", fake_popen),
            patch.object(ssh_mod.os.path, "exists", lambda p: True),
            patch.object(ssh_mod.os, "geteuid", lambda: 1000),
        ):
            return endpoint._try_direct_transfer(
                source_path="/snaps/snap",
                dest_path="/dest",
                snapshot_name="snap",
                parent_path=None,
                show_progress=False,
            )

    def test_a_compressor_that_fails_is_not_a_successful_backup(self):
        assert self._run_with_failing_compressor(1) is False

    def test_a_compressor_that_succeeds_leaves_the_result_alone(self):
        assert self._run_with_failing_compressor(0) is True


class TestTheDestinationSurvivesBothShells:
    """The destination crosses two shells: the local one that runs `ssh`, and
    the remote `sh -c`. The compressed forms add a THIRD level for the password
    route (`sudo -S sh -c '...'`), and a path with a space, an apostrophe or a
    semicolon has to arrive at `btrfs receive` as exactly one argument through
    all of them. Asserting the string merely "contains" the path cannot tell a
    correctly quoted command from an injection.
    """

    NASTY = "/back ups/it's here; touch {marker}"

    def _run_and_capture(self, tmp_path, **kwargs):
        marker = tmp_path / "INJECTED"
        dest = self.NASTY.format(marker=marker)
        record = tmp_path / "argv"
        (tmp_path / "btrfs").write_text(
            "#!/bin/sh\n"
            f'printf "%s\\n" "$@" > {shlex.quote(str(record))}\n'
            "cat >/dev/null\n"
        )
        (tmp_path / "sudo").write_text(
            '#!/bin/sh\nwhile [ "${1#-}" != "$1" ]; do shift; done\nexec "$@"\n'
        )
        for name in ("btrfs", "sudo"):
            (tmp_path / name).chmod(0o755)
        env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"}

        cmd = _build_receive_command(dest, **kwargs)
        subprocess.run(
            ["/bin/sh", "-c", cmd], input=b"x" * 256, capture_output=True, env=env
        )
        argv = record.read_text().splitlines() if record.exists() else []
        return dest, argv, marker.exists()

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"use_sudo": True},
            {"use_sudo": True, "decompress": "zstd"},
            {"use_sudo": True, "password_on_stdin": True, "decompress": "zstd"},
        ],
        ids=["plain", "compressed", "compressed-password-sudo"],
    )
    def test_the_path_arrives_intact_and_nothing_is_injected(self, tmp_path, kwargs):
        dest, argv, injected = self._run_and_capture(tmp_path, **kwargs)
        assert not injected, "the embedded command ran: the destination was not quoted"
        assert argv == ["receive", dest], argv


class TestOneMethodSetForEveryDestination:
    """`compress` must mean the same thing whatever the destination.

    There were two tables -- one for streams over ssh://, one for raw files at
    rest -- and they disagreed. A config could compress with xz at rest but not
    over the wire, `--compress lzop` was accepted for one destination and
    refused by the other, and a btrbk `stream_compress xz` had nowhere to go on
    import. Nothing technical required the split; both sides run `<prog> -c` and
    `<prog> -dc`.
    """

    def test_the_two_transports_know_the_same_methods(self):
        from btrfs_backup_ng.endpoint.raw_metadata import COMPRESSION_CONFIG

        assert set(COMPRESSION_PROGRAMS) == set(COMPRESSION_CONFIG)

    def test_the_command_line_offers_exactly_those(self):
        from btrfs_backup_ng.cli.dispatcher import _compression_choices

        assert set(_compression_choices()) == set(COMPRESSION_PROGRAMS) | {"none"}

    @pytest.mark.parametrize("method", ["xz", "lzo", "bzip2", "pbzip2"])
    def test_a_method_that_used_to_be_raw_only_now_works_over_ssh(self, method):
        """These four existed only for raw targets and are the reason the
        asymmetry was visible to users."""
        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"compress": method}
        assert endpoint._stream_compress_method() == method

        cmd = _build_receive_command(DEST, use_sudo=True, decompress=method)
        program = COMPRESSION_PROGRAMS[method]["decompress"][0]
        assert program in cmd, cmd
        assert "btrfs receive" in cmd

    def test_an_unknown_method_still_degrades_with_a_warning(self):
        from btrfs_backup_ng.endpoint import ssh as ssh_mod

        endpoint = SSHEndpoint.__new__(SSHEndpoint)
        endpoint.config = {"compress": "not-a-real-codec"}
        said = []
        with patch.object(
            ssh_mod.logger, "warning", lambda msg, *a: said.append(msg % a)
        ):
            assert endpoint._stream_compress_method() is None
        assert "UNCOMPRESSED" in " ".join(said)

    def test_the_config_loader_accepts_them_for_either_destination(self, tmp_path):
        from btrfs_backup_ng.config import load_config

        for index, method in enumerate(["xz", "lzo", "bzip2", "pbzip2", "lzop"]):
            for target in ("ssh://u@h:/b", "raw:///backups"):
                path = tmp_path / f"c{index}-{target.count('/')}.toml"
                path.write_text(
                    f'[[volumes]]\npath = "/data"\n'
                    f'snapshot_dir = "/data/.snapshots"\n\n'
                    f'[[volumes.targets]]\npath = "{target}"\n'
                    f'compress = "{method}"\n',
                    encoding="utf-8",
                )
                load_config(path)  # must not raise
