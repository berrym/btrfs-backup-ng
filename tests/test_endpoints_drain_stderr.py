"""Every process an endpoint starts with a stderr pipe has that pipe drained.

A child whose stderr is a pipe nobody reads blocks once the kernel buffer
(64 KiB) is full, and the stall detector then kills a healthy transfer. The
engine reports what a failed process said through core.transfer's
stderr_text, which reads the tail an endpoint attached when it started the
process (``tail_stderr``). An endpoint that goes back to DEVNULL -- or to a
bare PIPE nobody drains -- passes every engine test, because those use fake
endpoints, and passed the example-based tests here too: raw+ssh:// receive
started its pipeline with ``stderr=PIPE`` and nothing read it, and so did the
chunked ssh:// receive, while the examples covered only the local btrfs and
local raw endpoints.

So this file has two halves. A STRUCTURAL check walks every endpoint module's
source for a ``Popen(stderr=PIPE)`` and requires the function that starts it
to attach a tail, or to be one of two named sites that drain another way; a
new site in any function fails it. And a BEHAVIOURAL flood per endpoint class
and path proves the drain: a child writes 1 MiB to stderr and the receive
still finishes.
"""

from __future__ import annotations

import ast
import pathlib
import subprocess
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng.core import transfer as T
from btrfs_backup_ng.endpoint import raw as raw_mod
from btrfs_backup_ng.endpoint import ssh as ssh_mod
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

ENDPOINT_DIR = pathlib.Path(raw_mod.__file__).parent

#: A megabyte of stderr, then the stream is consumed. Sixteen times the pipe.
FLOOD = "head -c 1048576 /dev/zero | tr '\\0' 'v' >&2"


# --------------------------------------------------------------------------- #
# Structural: no Popen(stderr=PIPE) without a drain
# --------------------------------------------------------------------------- #
#: Sites that drain their stderr pipe without a tail, each with how. A site
#: named here must still exist and still drain that way; anything not named
#: must attach a tail in the function that starts the process.
DRAINS_OTHERWISE = {
    ("raw.py", "remediate_plaintext"): "communicate",
    ("ssh.py", "_do_shell_pipeline_transfer"): "Thread",
}


def _call_name(func: ast.AST) -> str:
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return ""


def _is_pipe(value: ast.AST) -> bool:
    return (isinstance(value, ast.Attribute) and value.attr == "PIPE") or (
        isinstance(value, ast.Name) and value.id == "PIPE"
    )


def _stderr_pipe_sites() -> list[tuple[str, int, ast.FunctionDef | None, ast.Module]]:
    sites = []
    for path in sorted(ENDPOINT_DIR.glob("*.py")):
        tree = ast.parse(path.read_text())
        parents: dict[ast.AST, ast.AST] = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parents[child] = node
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if _call_name(node.func) not in ("Popen", "_popen_pipeline_pipefail"):
                continue
            if not any(k.arg == "stderr" and _is_pipe(k.value) for k in node.keywords):
                continue
            holder: ast.AST | None = node
            while holder in parents:
                holder = parents[holder]
                if isinstance(holder, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    break
            else:
                holder = None
            sites.append((path.name, node.lineno, holder, tree))
    return sites


def _calls(function: ast.AST, name: str) -> bool:
    return any(
        isinstance(n, ast.Call) and _call_name(n.func) == name
        for n in ast.walk(function)
    )


class TestEveryStderrPipeIsDrained:
    def test_the_scan_sees_the_known_sites(self):
        """A scan that finds nothing proves nothing."""
        names = {(f, fn.name) for f, _, fn, _ in _stderr_pipe_sites() if fn}
        assert ("raw.py", "_execute_pipeline") in names
        assert ("ssh.py", "_btrfs_receive") in names
        assert ("ssh.py", "receive_chunked") in names

    def test_each_site_attaches_a_tail_or_is_named_here(self):
        undrained = []
        for filename, lineno, function, _ in _stderr_pipe_sites():
            if function is None:
                undrained.append(f"{filename}:{lineno} at module level")
                continue
            if _calls(function, "tail_stderr"):
                continue
            how = DRAINS_OTHERWISE.get((filename, function.name))
            if how and _calls(function, how):
                continue
            undrained.append(f"{filename}:{lineno} in {function.name}")
        assert not undrained, (
            "a process is started with a stderr pipe nobody drains: "
            + ", ".join(undrained)
        )

    def test_the_named_exceptions_still_exist(self):
        """An allowlist entry for a site that is gone hides the next one."""
        present = {(f, fn.name) for f, _, fn, _ in _stderr_pipe_sites() if fn}
        for site in DRAINS_OTHERWISE:
            assert site in present, f"{site} no longer starts a stderr pipe"


# --------------------------------------------------------------------------- #
# Behavioural: a flood on stderr does not stall the receive
# --------------------------------------------------------------------------- #
def _fake_command(script: str):
    """What _build_*_command returns: (arg, is_path) tuples the base
    _exec_command runs through subprocess."""
    return [("sh", False), ("-c", False), (script, False)]


def _feed(payload: bytes = b"stream") -> subprocess.Popen:
    return subprocess.Popen(["printf", "%s", payload.decode()], stdout=subprocess.PIPE)


class TestTheBaseEndpoint:
    def test_receive_attaches_a_tail_and_the_report_reads_it(
        self, tmp_path, monkeypatch
    ):
        ep = LocalEndpoint(config={"path": str(tmp_path), "fs_checks": "skip"})
        monkeypatch.setattr(
            ep,
            "_build_receive_command",
            lambda dest: _fake_command(
                "cat >/dev/null; echo 'ERROR: unexpected header' >&2; exit 1"
            ),
        )
        feed = _feed()
        proc = ep.receive(feed.stdout, "snap")
        feed.stdout.close()
        assert proc.stderr is None, "the pipe belongs to the tail, not the process"
        assert getattr(proc, "stderr_tail", None) is not None
        assert proc.wait(timeout=10) == 1
        feed.wait(timeout=5)
        assert "unexpected header" in T.stderr_text(proc)

    def test_send_attaches_a_tail(self, tmp_path, monkeypatch):
        ep = LocalEndpoint(config={"path": str(tmp_path), "fs_checks": "skip"})
        monkeypatch.setattr(
            ep,
            "_build_send_command",
            lambda snapshot, parent=None, clones=None: _fake_command(
                "echo 'At subvol x' >&2; echo data; exit 0"
            ),
        )
        proc = ep.send(object())
        assert proc.stderr is None
        assert proc.stdout.read() == b"data\n"
        proc.stdout.close()
        assert proc.wait(timeout=10) == 0
        assert T.stderr_text(proc) == "At subvol x\n"

    def test_a_receive_that_floods_stderr_still_finishes(self, tmp_path, monkeypatch):
        """btrfs_debug puts -vv on the receive: a line per file operation."""
        ep = LocalEndpoint(config={"path": str(tmp_path), "fs_checks": "skip"})
        monkeypatch.setattr(
            ep,
            "_build_receive_command",
            lambda dest: _fake_command(f"cat >/dev/null; {FLOOD}; exit 0"),
        )
        feed = _feed()
        proc = ep.receive(feed.stdout, "snap")
        feed.stdout.close()
        assert proc.wait(timeout=20) == 0
        feed.wait(timeout=5)
        assert len(T.stderr_text(proc)) == T.STDERR_TAIL_BYTES


class TestTheRawEndpoint:
    def test_receive_pipeline_attaches_a_tail(self, tmp_path):
        ep = RawEndpoint(config={"path": str(tmp_path), "compress": "gzip"})
        src = tmp_path / "in.src"
        src.write_bytes(b"payload" * 100)
        with open(src, "rb") as f:
            proc = ep.receive(f, snapshot_name="root.20240101T120000")
            assert proc.stderr is None
            assert getattr(proc, "stderr_tail", None) is not None
            proc.wait(timeout=10)
        ep.commit_receive()
        assert T.stderr_text(proc) == ""

    def test_send_pipeline_attaches_a_tail(self, tmp_path):
        ep = RawEndpoint(config={"path": str(tmp_path), "compress": "gzip"})
        src = tmp_path / "in.src"
        src.write_bytes(b"payload" * 100)
        name = "root.20240101T120000"
        with open(src, "rb") as f:
            ep.receive(f, snapshot_name=name).wait(timeout=10)
        ep.commit_receive()
        snap = next(s for s in ep.list_snapshots() if s.get_name() == name)
        proc = ep.send(snap)
        assert proc.stderr is None
        assert getattr(proc, "stderr_tail", None) is not None
        assert proc.stdout.read() == b"payload" * 100
        proc.stdout.close()
        assert proc.wait(timeout=10) == 0

    @pytest.mark.parametrize("stages", [1, 2], ids=["single stage", "pipefail"])
    def test_a_receive_pipeline_that_floods_stderr_still_finishes(
        self, tmp_path, monkeypatch, stages
    ):
        """Both shapes of the local raw pipeline: one Popen for a single stage,
        the pipefail shell for several."""
        ep = RawEndpoint(config={"path": str(tmp_path), "compress": "gzip"})
        noisy = ["sh", "-c", f"{FLOOD}; cat"]
        pipeline = [noisy] if stages == 1 else [noisy, ["cat"]]
        monkeypatch.setattr(ep, "_build_receive_pipeline", lambda out: pipeline)
        feed = _feed(b"payload")
        proc = ep.receive(feed.stdout, snapshot_name="root.20240101T120000")
        feed.stdout.close()
        assert proc.wait(timeout=30) == 0
        feed.wait(timeout=5)
        ep.commit_receive()
        assert len(T.stderr_text(proc)) == T.STDERR_TAIL_BYTES
        stored = next(
            p
            for p in tmp_path.iterdir()
            if p.name.startswith("root.") and not p.name.endswith(".meta")
        )
        assert stored.read_bytes() == b"payload"


class TestTheRawSshEndpoint:
    """raw+ssh:// receive: the pipeline's last stage is ssh, and ssh's own
    output -- its diagnostics, anything the remote shell prints -- comes back
    on the pipe this endpoint started with nobody reading it."""

    def _endpoint(self, tmp_path, compress: str | None):
        config = {"path": str(tmp_path), "hostname": "nas"}
        if compress:
            config["compress"] = compress
        ep = SSHRawEndpoint(config=config)
        ep._pending_metadata = {
            "name": "root.20240101T120000",
            "part_path": tmp_path / "root.20240101T120000.btrfs.part",
            "stream_path": tmp_path / "root.20240101T120000.btrfs",
        }
        # The "ssh" is a local shell that floods stderr, then swallows the
        # remote command it is handed as $1 along with the stream.
        ep._build_ssh_command = lambda: ["sh", "-c", f"{FLOOD}; cat >/dev/null", "sh"]
        return ep

    @pytest.mark.parametrize(
        "compress", [None, "gzip"], ids=["direct to ssh", "local stage then ssh"]
    )
    def test_a_receive_whose_ssh_floods_stderr_still_finishes(self, tmp_path, compress):
        ep = self._endpoint(tmp_path, compress)
        pipeline = ep._build_receive_pipeline(ep._pending_metadata["part_path"])
        feed = _feed(b"payload")
        proc = ep._execute_pipeline(pipeline, feed.stdout)
        feed.stdout.close()
        assert proc.stderr is None, "the pipe belongs to the tail, not the process"
        assert getattr(proc, "stderr_tail", None) is not None
        assert proc.wait(timeout=30) == 0
        feed.wait(timeout=5)
        assert len(T.stderr_text(proc)) == T.STDERR_TAIL_BYTES


def _ssh_endpoint() -> SSHEndpoint:
    """An SSHEndpoint without a connection: the paths under test build their
    ssh command line themselves and hand it to Popen."""
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"path": "/dest", "username": "u", "port": 22, "compress": "none"}
    ep.hostname = "nas"
    ep.ssh_manager = MagicMock()
    ep.ssh_manager.control_path = "/run/cm.sock"
    return ep


class TestTheSshEndpoint:
    def test_a_remote_receive_that_floods_stderr_still_finishes(self, monkeypatch):
        """``_btrfs_receive`` is the receive the direct path uses (with and
        without sudo: the remote command differs, the process handling does
        not). The "ssh" it starts is replaced with a local shell that floods."""
        ep = _ssh_endpoint()
        real_popen = subprocess.Popen

        def popen(argv, **kwargs):
            if argv and argv[0] == "ssh":
                argv = ["sh", "-c", f"{FLOOD}; cat >/dev/null; exit 0"]
            return real_popen(argv, **kwargs)

        monkeypatch.setattr(ssh_mod.subprocess, "Popen", popen)
        feed = _feed(b"payload")
        proc = ep._btrfs_receive("/dest", feed.stdout)
        assert proc.stderr is None
        assert getattr(proc, "stderr_tail", None) is not None
        assert proc.wait(timeout=30) == 0
        proc.stdout.close()  # the engine's job in production
        feed.wait(timeout=5)
        assert len(T.stderr_text(proc)) == T.STDERR_TAIL_BYTES

    def test_a_chunked_receive_that_floods_stderr_still_finishes(self, monkeypatch):
        """The chunked path writes the stream into the receive's stdin from a
        Python loop and read stderr only after the exit, so a remote that
        said more than a pipe holds stopped taking chunks while this side
        waited to write the next one. Bounded: a stall here is a failure,
        not a hang."""
        ep = _ssh_endpoint()
        ep.ssh_manager.get_ssh_base_cmd = lambda force_tty=False: [
            "sh",
            "-c",
            f"{FLOOD}; cat >/dev/null; exit 0",
            "sh",
        ]
        ep._normalize_path = lambda p: p
        ep.artifact_exists = lambda *a, **k: False
        ep._verify_snapshot_exists = lambda *a, **k: True
        ep._cleanup_partial_subvolume = lambda *a, **k: None
        chunks = [b"x" * 65536] * 8  # 512 KiB, eight times the pipe
        reader = SimpleNamespace(read_chunks=lambda: iter(chunks))
        manifest = SimpleNamespace(
            snapshot_name="snap", chunk_count=len(chunks), snapshot_path="/src/snap"
        )
        result: dict = {}

        def run():
            result["ok"] = ep.receive_chunked(reader, manifest, timeout=60)

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        worker.join(60)
        assert not worker.is_alive(), "the chunked receive stalled on a flooded stderr"
        assert result["ok"] is True
