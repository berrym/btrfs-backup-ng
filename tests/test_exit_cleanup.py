"""What a run owns is let go however it ends, in the order that keeps it safe.

A run owns child processes (the send, the ssh carrying the stream), locks and
pins on its targets, and ssh control connections with a private directory
each. They must be let go when the run ends -- by returning, by Ctrl-C, by
SIGTERM from systemd, by SIGHUP from a closed terminal -- and in this order:
the writers stop, then the locks go, then the connections. A lock released
while its writer is still writing protects nothing.

The signal cases run in a real subprocess that raises the signal at itself.
Its signal dispositions are reset to the defaults at exec, as sshd and init
do, so the tests do not depend on how pytest itself was started: under
``nohup`` or as a background job pytest inherits SIGHUP or SIGINT ignored,
and a test that relied on the default would test the wrong thing.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

from btrfs_backup_ng import lifecycle

_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


def _defaults() -> None:
    for signum in _SIGNALS:
        signal.signal(signum, signal.SIG_DFL)


def _ignore(*names: str):
    def preexec() -> None:
        _defaults()
        for name in names:
            signal.signal(getattr(signal, name), signal.SIG_IGN)

    return preexec


def _run(script: str, tmp_path: Path, *, preexec=_defaults, timeout: float = 60):
    runtime = tmp_path / "runtime"
    runtime.mkdir(exist_ok=True)
    env = {**os.environ, "XDG_RUNTIME_DIR": str(runtime)}
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
        preexec_fn=preexec,
        cwd=tmp_path,
    )


def _leftover_dirs(tmp_path: Path) -> list[str]:
    runtime = tmp_path / "runtime"
    return sorted(p.name for p in runtime.iterdir() if p.name.startswith("bbng-cm-"))


# A pin taken with the real lock protocol against a directory standing in for
# the target, and an ssh manager that owns a control directory. The child
# prints READY once both exist.
_OWN_THINGS = """
import os, signal, subprocess, sys, threading, time
from btrfs_backup_ng import lifecycle
from btrfs_backup_ng.sshutil.lock import RemoteLockManager
from btrfs_backup_ng.sshutil.master import SSHMasterManager

def run(script):
    p = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr

target = os.path.abspath("target")
os.makedirs(target, exist_ok=True)
manager = RemoteLockManager(run, target, hostname="h")
manager.acquire_shared_persistent("snap-a", "restore:1")
ssh = SSHMasterManager("nowhere.invalid", username="nobody")
"""


def _pins(tmp_path: Path) -> list[str]:
    holders = list((tmp_path / "target").rglob("holders/*"))
    return sorted(p.name for p in holders)


class TestAFatalSignalEndsTheRunCleanly:
    @pytest.mark.parametrize("sig", ["SIGTERM", "SIGHUP"])
    def test_pins_and_control_dirs_are_let_go_and_the_process_dies_of_it(
        self, tmp_path, sig
    ):
        proc = _run(
            _OWN_THINGS
            + f"""
lifecycle.install_signal_handlers()
assert os.listdir(os.path.join(target, ".btrfs-backup-ng.locks"))
signal.raise_signal(signal.{sig})
os._exit(99)  # a fatal signal that did not end the process is a failure
""",
            tmp_path,
        )
        assert proc.returncode == -getattr(signal, sig), (proc.returncode, proc.stderr)
        assert _pins(tmp_path) == [], proc.stderr
        assert _leftover_dirs(tmp_path) == [], proc.stderr

    def test_the_entry_point_is_what_installs_the_handlers(self, tmp_path):
        """Library code registers; only the program installs. The command-line
        entry point does it before dispatching anything."""
        proc = _run(
            """
import signal
import btrfs_backup_ng.__main__ as entry
seen = {}
def fake_cli():
    seen.update({s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)})
    return 0
entry.cli_main = fake_cli
try:
    entry.main()
except SystemExit:
    pass
from btrfs_backup_ng import lifecycle
print(seen[signal.SIGTERM] is lifecycle._on_fatal_signal,
      seen[signal.SIGHUP] is lifecycle._on_fatal_signal,
      seen[signal.SIGINT] is signal.default_int_handler)
""",
            tmp_path,
        )
        assert proc.stdout.split() == ["True", "True", "True"], proc.stderr

    def test_a_connection_made_on_a_worker_thread_is_covered(self, tmp_path):
        """A multi-volume run makes its connections on worker threads. The
        handlers are installed once at the entry point, so where a connection
        is made cannot decide whether it is cleaned up."""
        proc = _run(
            """
import signal, threading
from btrfs_backup_ng import lifecycle
from btrfs_backup_ng.sshutil.master import SSHMasterManager
lifecycle.install_signal_handlers()
made = []
threads = [threading.Thread(target=lambda: made.append(SSHMasterManager("h.invalid", username="u"))) for _ in range(2)]
for t in threads: t.start()
for t in threads: t.join()
assert len(made) == 2 and all(m.control_dir.is_dir() for m in made)
signal.raise_signal(signal.SIGTERM)
""",
            tmp_path,
        )
        assert proc.returncode == -signal.SIGTERM, proc.stderr
        assert _leftover_dirs(tmp_path) == []

    def test_a_second_signal_does_not_abandon_the_cleanup(self, tmp_path):
        """A closed terminal sends SIGHUP more than once. The second must not
        cut the first one's cleanup short: every entry still runs, and the
        process still dies of the first signal."""
        proc = _run(
            """
import os, signal, time
from btrfs_backup_ng import lifecycle
lifecycle.install_signal_handlers()
done = []
def slow():
    os.kill(os.getpid(), signal.SIGHUP)   # arrives during the drain
    time.sleep(0.2)
    done.append("slow")
def later():
    done.append("later")
    open("ran-later", "w").write(",".join(done))
lifecycle.register("slow", slow, lifecycle.STAGE_LOCKS)
lifecycle.register("later", later, lifecycle.STAGE_CONNECTIONS)
signal.raise_signal(signal.SIGTERM)
""",
            tmp_path,
        )
        assert proc.returncode == -signal.SIGTERM, proc.stderr
        assert (tmp_path / "ran-later").read_text() == "slow,later"

    def test_the_handler_can_run_while_the_registry_is_being_changed(self, tmp_path):
        """A signal landing while the main thread holds the registry lock must
        not deadlock the handler that needs it."""
        proc = _run(
            """
import os, signal
from btrfs_backup_ng import lifecycle
lifecycle.install_signal_handlers()
lifecycle.register("a", lambda: open("ran", "w").write("yes"), lifecycle.STAGE_LOCKS)
with lifecycle._LOCK:
    signal.raise_signal(signal.SIGTERM)
""",
            tmp_path,
            timeout=20,
        )
        assert proc.returncode == -signal.SIGTERM, proc.stderr
        assert (tmp_path / "ran").read_text() == "yes"


class TestAnIgnoredSignalStaysIgnored:
    def test_under_nohup_a_hangup_releases_nothing(self, tmp_path):
        """``nohup`` means: keep running when the terminal goes. A handler
        installed over that ignore released every pin and removed the control
        directory while the run carried on without them."""
        proc = _run(
            _OWN_THINGS
            + """
lifecycle.install_signal_handlers()
assert signal.getsignal(signal.SIGHUP) is signal.SIG_IGN
signal.raise_signal(signal.SIGHUP)
time.sleep(0.1)
held = os.listdir(os.path.join(target, ".btrfs-backup-ng.locks"))
print("ALIVE", bool(held), ssh.control_dir.is_dir())
sys.stdout.flush()
""",
            tmp_path,
            preexec=_ignore("SIGHUP"),
        )
        assert proc.returncode == 0, proc.stderr
        assert "ALIVE True True" in proc.stdout
        # ...and the normal exit that follows still lets everything go.
        assert _pins(tmp_path) == []
        assert _leftover_dirs(tmp_path) == []


class TestCtrlC:
    def test_there_is_no_sigint_handler_and_the_exit_sweeps(self, tmp_path):
        """Ctrl-C is Python's KeyboardInterrupt, unwinding as usual; whatever
        the unwind did not release, the exit drain does."""
        proc = _run(
            _OWN_THINGS
            + """
lifecycle.install_signal_handlers()
assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
signal.raise_signal(signal.SIGINT)
""",
            tmp_path,
        )
        assert proc.returncode in (-signal.SIGINT, 1), (proc.returncode, proc.stderr)
        assert "KeyboardInterrupt" in proc.stderr
        assert _pins(tmp_path) == []
        assert _leftover_dirs(tmp_path) == []

    def test_the_connection_is_still_there_while_the_interrupt_unwinds(self, tmp_path):
        """The unwind releases locks over the connection, so nothing may take
        the connection away before the unwind has run."""
        proc = _run(
            _OWN_THINGS
            + """
lifecycle.install_signal_handlers()
try:
    signal.raise_signal(signal.SIGINT)
except KeyboardInterrupt:
    print("UNWINDING", ssh.control_dir.is_dir(), bool(os.listdir(target)))
""",
            tmp_path,
        )
        assert "UNWINDING True True" in proc.stdout, proc.stderr

    def test_a_worker_transfer_keeps_its_lock_until_it_finishes(self, tmp_path):
        """``kill -INT`` interrupts the main thread only. A worker thread's
        transfer goes on, so its pin must stay held until that worker lets it
        go -- never swept from under it by the interrupted main thread."""
        proc = _run(
            _OWN_THINGS
            + """
lifecycle.install_signal_handlers()
seen = []
def worker():
    w = RemoteLockManager(run, target, hostname="w")
    w.acquire_shared_persistent("snap-w", "transfer:1")
    time.sleep(0.5)
    seen.append(sorted(os.listdir(os.path.join(target, ".btrfs-backup-ng.locks"))))
    w.release_shared("snap-w", "transfer:1")
t = threading.Thread(target=worker)
t.start()
time.sleep(0.1)
try:
    signal.raise_signal(signal.SIGINT)
except KeyboardInterrupt:
    pass
t.join()
print("WORKER-SAW", any("snap-w" in n for n in seen[0]))
""",
            tmp_path,
        )
        assert "WORKER-SAW True" in proc.stdout, proc.stderr


class TestWritersStopBeforeLocksGo:
    def test_an_interrupt_stops_the_scope_s_children_before_the_outer_release(
        self, tmp_path
    ):
        """The scope is inside the lock; unwinding runs inner cleanup first.
        So by the time the lock's release runs, its writer is dead."""
        seen: dict[str, object] = {}
        child: list[subprocess.Popen] = []

        class Lock:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                seen["writer_alive_at_release"] = child[0].poll() is None
                return False

        with pytest.raises(KeyboardInterrupt):
            with Lock():
                with lifecycle.process_scope():
                    child.append(lifecycle.track(subprocess.Popen(["sleep", "30"])))
                    raise KeyboardInterrupt
        assert seen["writer_alive_at_release"] is False

    def test_a_shell_pipeline_is_stopped_with_its_own_children(self, tmp_path):
        """``sh -c 'a | b'`` does its work in grandchildren; terminating the
        shell alone would leave them writing."""
        marker = tmp_path / "grandchild.pid"
        with pytest.raises(RuntimeError):
            with lifecycle.process_scope():
                lifecycle.track(
                    subprocess.Popen(
                        ["sh", "-c", f"sleep 30 & echo $! > {marker}; wait"]
                    )
                )
                deadline = time.monotonic() + 10
                while not (marker.exists() and marker.read_text().strip()):
                    assert time.monotonic() < deadline
                    time.sleep(0.02)
                raise RuntimeError("the transfer failed")
        grandchild = int(marker.read_text())
        time.sleep(0.1)
        with pytest.raises(ProcessLookupError):
            os.kill(grandchild, 0)

    def test_a_normal_return_hands_live_children_to_the_enclosing_scope(self):
        """A function that starts a process and returns it (an endpoint's
        send) hands it to its caller; the caller's scope then owns it."""
        with pytest.raises(RuntimeError):
            with lifecycle.process_scope() as outer:
                with lifecycle.process_scope():
                    proc = lifecycle.track(subprocess.Popen(["sleep", "30"]))
                assert proc in outer.children
                raise RuntimeError
        assert proc.poll() is not None

    def test_a_scope_that_succeeds_leaves_its_children_alone(self):
        with lifecycle.process_scope():
            proc = lifecycle.track(subprocess.Popen(["sleep", "0.2"]))
        assert proc.poll() is None
        assert proc.wait(timeout=10) == 0

    def test_the_transfer_engine_runs_in_a_scope(self, monkeypatch):
        """Every transfer, restore and verify goes through send_snapshot; the
        scope there is what sits inside the caller's locks."""
        from btrfs_backup_ng.core import operations

        started: list[subprocess.Popen] = []

        def body(*_a, **_k):
            started.append(lifecycle.track(subprocess.Popen(["sleep", "30"])))
            raise KeyboardInterrupt

        monkeypatch.setattr(operations, "_send_snapshot", body)
        with pytest.raises(KeyboardInterrupt):
            operations.send_snapshot(object(), object())
        assert started[0].poll() is not None

    def test_a_fatal_signal_stops_children_before_it_releases(self, tmp_path):
        proc = _run(
            """
import os, signal, subprocess
from btrfs_backup_ng import lifecycle
lifecycle.install_signal_handlers()
child = lifecycle.track(subprocess.Popen(["sleep", "30"]))
def release():
    open("writer-at-release", "w").write(str(child.poll() is None))
lifecycle.register("lock", release, lifecycle.STAGE_LOCKS)
signal.raise_signal(signal.SIGTERM)
""",
            tmp_path,
        )
        assert proc.returncode == -signal.SIGTERM, proc.stderr
        assert (tmp_path / "writer-at-release").read_text() == "False"


class TestTheControlDirectory:
    def test_its_removal_is_registered_before_it_exists(self, tmp_path, monkeypatch):
        """No moment exists at which the directory is there and nothing would
        remove it."""
        from btrfs_backup_ng.sshutil import master

        monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
        seen = []
        real_mkdir = os.mkdir

        def mkdir(path, mode=0o777):
            if "bbng-cm-" in str(path):
                seen.append(any(k[0] == "ssh-control" for k in lifecycle._CLEANUPS))
            return real_mkdir(path, mode)

        monkeypatch.setattr(master.os, "mkdir", mkdir)
        manager = master.SSHMasterManager("h.invalid", username="u")
        assert seen == [True]
        assert lifecycle.registered(manager._exit_key)
        manager.cleanup_socket()
        assert not manager.control_dir.exists()
        assert not lifecycle.registered(manager._exit_key)

    def test_a_stopped_master_can_be_started_again(self, tmp_path, monkeypatch):
        """stop_master removes the directory; a later start_master used to
        run ssh against a socket path in a directory that no longer existed,
        which ssh reports as a failed login."""
        from btrfs_backup_ng.sshutil import master

        monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
        manager = master.SSHMasterManager("h.invalid", username="u")
        dirs_at_login = []

        def key_auth(_env):
            dirs_at_login.append(manager.control_path.parent.is_dir())
            manager._master_started = True
            return True

        monkeypatch.setattr(manager, "_try_key_auth", key_auth)
        monkeypatch.setattr(manager, "is_master_alive", lambda: False)
        monkeypatch.setattr(
            master.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0)
        )
        assert manager.start_master()
        assert manager.stop_master()
        assert not manager.control_dir.exists()
        assert manager.start_master()
        assert dirs_at_login == [True, True]
        manager.cleanup_socket()

    def test_the_exit_cleanup_stops_a_running_master_first(self, tmp_path, monkeypatch):
        """A master left running outlives the process (ControlPersist) and
        keeps the session to the target open; the exit cleanup asks it to
        exit, then removes the directory -- without taking the manager's lock,
        which the interrupted code may be holding."""
        from btrfs_backup_ng.sshutil import master

        monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
        manager = master.SSHMasterManager("h.invalid", username="u")
        manager._master_started = True
        calls = []
        monkeypatch.setattr(
            master.subprocess,
            "run",
            lambda cmd, **k: calls.append(cmd) or subprocess.CompletedProcess(cmd, 0),
        )
        assert manager._lock.acquire(blocking=False)
        try:
            manager._exit_cleanup()
        finally:
            manager._lock.release()
        assert calls and calls[0][:3] == ["ssh", "-O", "exit"]
        assert not manager.control_dir.exists()

    def test_the_removal_never_touches_a_directory_it_does_not_own(self, tmp_path):
        from btrfs_backup_ng.sshutil import master

        manager = master.SSHMasterManager("h.invalid", username="u")
        manager.cleanup_socket()
        victim = tmp_path / "victim"
        victim.mkdir()
        (victim / "keep").write_text("x")
        manager.control_dir = tmp_path / "link"
        manager.control_dir.symlink_to(victim)
        manager._remove_own_control_dir()
        assert (victim / "keep").exists()


class TestTheDrainIsResumable:
    def test_an_interrupted_cleanup_does_not_stop_the_others(self):
        order = []

        def interrupted():
            order.append("a")
            raise KeyboardInterrupt

        lifecycle.register(("t", "a"), interrupted, lifecycle.STAGE_LOCKS)
        lifecycle.register(("t", "b"), lambda: order.append("b"), lifecycle.STAGE_LOCKS)
        lifecycle.run_cleanups(lambda k: k[:1] == ("t",))
        assert order == ["a", "b"]
        # The interrupted one is still owed; the finished one is not.
        assert lifecycle.registered(("t", "a"))
        assert not lifecycle.registered(("t", "b"))
        lifecycle.unregister(("t", "a"))

    def test_connections_go_after_locks(self):
        order = []
        lifecycle.register(
            ("t", "conn"), lambda: order.append("conn"), lifecycle.STAGE_CONNECTIONS
        )
        lifecycle.register(
            ("t", "lock"), lambda: order.append("lock"), lifecycle.STAGE_LOCKS
        )
        lifecycle.run_cleanups(lambda k: k[:1] == ("t",))
        assert order == ["lock", "conn"]

    def test_installing_off_the_main_thread_is_refused(self):
        errors = []

        def attempt():
            try:
                lifecycle.install_signal_handlers()
            except RuntimeError as exc:
                errors.append(exc)

        t = threading.Thread(target=attempt)
        t.start()
        t.join()
        assert errors
