"""After Ctrl-C a run begins nothing new and retries nothing.

SIGINT reaches only the main thread. With parallel targets or volumes the
transfers run on worker threads, which never see it: measured on real hosts, a
worker finished its in-flight snapshot after ``kill -INT`` and then STARTED the
next one in its plan, and after a terminal's Ctrl-C (the whole process group,
so the children die) the ``ssh://`` worker counted its killed transfer as a
transient failure and sent the whole snapshot again. A volume worker went on
to prune, and queued pool jobs still ran.

Now the main thread requests a stop the moment the interrupt reaches it while
it waits on its workers, and cancels what no worker has begun; every point
where a worker would begin something new asks first, and the retry framework
refuses a further attempt. What is already running is not cancelled.

The end-to-end cases run the real ``run`` command in a subprocess whose signal
dispositions are reset to the defaults at exec, as in test_exit_cleanup.py, so
they do not depend on how pytest itself was started.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng import __util__, lifecycle
from btrfs_backup_ng.cli import prune as prune_cli
from btrfs_backup_ng.cli import run as run_cli
from btrfs_backup_ng.config.schema import RetentionConfig
from btrfs_backup_ng.core import execution, operations, planning
from btrfs_backup_ng.core.errors import TransientNetworkError
from btrfs_backup_ng.core.operations import TransferResult
from btrfs_backup_ng.core.retry import (
    RetryAttempt,
    RetryContext,
    RetryPolicy,
    retry_call,
    with_retry,
)
from btrfs_backup_ng.endpoint.common import DeletionResult
from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict
from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

INTERRUPTED = "the run was interrupted"


class _Snap:
    def __init__(self, name: str) -> None:
        self.name = name
        self.locks: set = set()
        self.parent_locks: set = set()

    def get_name(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.name


def _warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


@pytest.fixture
def warnings_seen(caplog):
    """What the package's module loggers warned about during the test. The
    package logger is opened for the test: an earlier test may have left it
    at any level."""
    caplog.set_level(logging.DEBUG, logger="btrfs_backup_ng")
    return lambda: _warnings(caplog)


def _retention():
    return RetentionConfig(
        min="0s", hourly=0, daily=1, weekly=0, monthly=0, yearly=0, keep=0
    )


# ------------------------------------------------------------ the primitives


class TestTheStopFlag:
    def test_nothing_is_refused_until_a_stop_is_requested(self, warnings_seen):
        assert not lifecycle.stop_requested()
        assert lifecycle.stopped_before("the transfer of a") is False
        assert warnings_seen() == []

    def test_after_a_stop_the_refusal_is_said_once_in_plain_language(
        self, warnings_seen
    ):
        lifecycle.request_stop()
        assert lifecycle.stopped_before("the transfer of a", later=2) is True
        assert warnings_seen() == [
            "Not starting the transfer of a or the 2 after it: the run was interrupted."
        ]

    def test_a_backoff_wait_returns_at_once_when_the_stop_comes(self):
        threading.Timer(0.2, lifecycle.request_stop).start()
        started = time.monotonic()
        assert lifecycle.sleep_unless_stopped(30) is True
        assert time.monotonic() - started < 5

    def test_a_backoff_wait_after_the_stop_does_not_wait_at_all(self):
        lifecycle.request_stop()
        started = time.monotonic()
        assert lifecycle.sleep_unless_stopped(30) is True
        assert time.monotonic() - started < 1


class TestThePoolHelper:
    def _pool(self, raise_what):
        started = threading.Event()
        release = threading.Event()
        ran: list[str] = []

        def job(name):
            ran.append(name)
            started.set()
            release.wait(10)
            return name

        futures = []
        with pytest.raises(raise_what):
            with (
                ThreadPoolExecutor(max_workers=1) as pool,
                lifecycle.stop_on_interrupt(pool),
            ):
                futures.append(pool.submit(job, "running"))
                futures.append(pool.submit(job, "queued"))
                assert started.wait(10)
                threading.Timer(0.2, release.set).start()
                raise raise_what
        return ran, futures

    def test_an_interrupt_stops_new_work_cancels_the_queue_and_is_re_raised(self):
        ran, (running, queued) = self._pool(KeyboardInterrupt)
        assert lifecycle.stop_requested()
        assert queued.cancelled()
        assert ran == ["running"]

    def test_what_is_already_running_is_not_cancelled(self):
        ran, (running, _queued) = self._pool(KeyboardInterrupt)
        assert not running.cancelled()
        assert running.result(timeout=10) == "running"

    def test_an_ordinary_failure_is_not_an_interrupt(self):
        """Only Ctrl-C stops the run; a failure in the waiting thread leaves
        the queued work alone, as before."""
        ran, (_running, queued) = self._pool(RuntimeError)
        assert not lifecycle.stop_requested()
        assert not queued.cancelled()
        assert ran == ["running", "queued"]


class TestTheDrainRequestsTheStop:
    def test_a_fatal_signal_stops_new_work_before_the_locks_go(self, tmp_path):
        runtime = tmp_path / "runtime"
        runtime.mkdir()
        script = """
            import signal
            from btrfs_backup_ng import lifecycle
            lifecycle.install_signal_handlers()
            def probe():
                open("stop-seen", "w").write(str(lifecycle.stop_requested()))
            lifecycle.register("probe", probe, lifecycle.STAGE_LOCKS)
            signal.raise_signal(signal.SIGTERM)
        """
        proc = subprocess.run(
            [sys.executable, "-c", textwrap.dedent(script)],
            capture_output=True,
            text=True,
            env={**os.environ, "XDG_RUNTIME_DIR": str(runtime)},
            timeout=60,
            preexec_fn=_defaults,
            cwd=tmp_path,
        )
        assert proc.returncode == -signal.SIGTERM, proc.stderr
        assert (tmp_path / "stop-seen").read_text() == "True"


# ------------------------------------------------ where a worker begins work


def _executor_rig(monkeypatch, on_send):
    """A three-snapshot plan through the real executor. ``on_send`` runs as
    each transfer does."""
    sent: list[str] = []

    def send(snapshot, destination, parent=None, options=None):
        sent.append(snapshot.get_name())
        on_send(snapshot)

    monkeypatch.setattr(operations, "send_snapshot", send)
    monkeypatch.setattr(operations, "destination_artifact_exists", lambda *a: False)
    monkeypatch.setattr(
        operations, "artifact_verdict", lambda *a: StructureVerdict("ok", "stand-in")
    )
    source = MagicMock()
    destination = MagicMock()
    destination.get_id.return_value = "dest"
    snaps = [_Snap("s1"), _Snap("s2"), _Snap("s3")]
    plan = [(snaps[0], None), (snaps[1], snaps[0]), (snaps[2], snaps[1])]
    return source, destination, snaps, plan, sent


class TestTheExecutorBeginsNoFurtherTransfer:
    def test_the_transfer_in_flight_finishes_and_the_rest_are_not_started(
        self, monkeypatch, warnings_seen
    ):
        """The interrupt arrives while s1 is being sent: s1 completes as a
        delivery, s2 and s3 are never begun and are reported as not
        transferred."""

        def interrupt_during(snapshot):
            if snapshot.get_name() == "s1":
                lifecycle.request_stop()

        source, dest, snaps, plan, sent = _executor_rig(monkeypatch, interrupt_during)
        result = operations._execute_transfers(source, dest, plan, {})
        assert sent == ["s1"]
        assert result.transferred == [snaps[0]]
        assert [s for s, _ in result.failed] == snaps[1:]
        assert all(lifecycle.NOT_STARTED in str(e) for _, e in result.failed)
        assert any(
            "Not starting the transfer of s2 or the 1 after it" in m
            for m in warnings_seen()
        )

    def test_a_snapshot_not_started_is_never_reported_as_delivered(self, monkeypatch):
        """Through sync_snapshots: the run is told it failed, with the counts."""
        source, dest, snaps, plan, sent = _executor_rig(
            monkeypatch, lambda s: lifecycle.request_stop()
        )
        source.list_snapshots.return_value = snaps
        monkeypatch.setattr(planning, "snapshots_present_on", lambda *a: set())
        monkeypatch.setattr(planning, "plan_transfer_sequence", lambda *a, **k: plan)
        with pytest.raises(__util__.SnapshotTransferError) as caught:
            operations.sync_snapshots(source, dest)
        partial = caught.value.result  # type: ignore[attr-defined]
        assert partial.transferred_count == 1
        assert partial.failed_count == 2


class TestTheSnapperSyncBeginsNoFurtherSnapshot:
    def test_the_rest_of_the_plan_is_not_started(self, monkeypatch, warnings_seen):
        snapper = [SimpleNamespace(number=n) for n in (1, 2, 3)]
        sent: list[int] = []

        def send(snap, destination, **kwargs):
            sent.append(snap.number)
            lifecycle.request_stop()

        monkeypatch.setattr(
            operations,
            "_create_snapper_snapshot_wrapper",
            lambda s, d: _Snap(f"snapshot-{s.number}"),
        )
        monkeypatch.setattr(operations, "_snapper_dest_view", lambda d: object())
        monkeypatch.setattr(
            planning,
            "plan_transfer_sequence",
            lambda wrappers, view, **k: [
                (wrappers[0], None),
                (wrappers[1], wrappers[0]),
                (wrappers[2], wrappers[1]),
            ],
        )
        monkeypatch.setattr(operations, "send_snapper_snapshot", send)
        with pytest.raises(__util__.SnapshotTransferError) as caught:
            operations._sync_snapper_snapshots_locked(object(), snapper, {}, None)
        partial = caught.value.result  # type: ignore[attr-defined]
        assert sent == [1]
        assert partial.transferred == [snapper[0]]
        assert [s for s, _ in partial.failed] == snapper[1:]
        assert all(lifecycle.NOT_STARTED in str(e) for _, e in partial.failed)
        assert any(
            "Not starting the transfer of snapper snapshot 2 or the 1 after it" in m
            for m in warnings_seen()
        )


def _load(tmp_path, targets: int, parallel_targets: int = 1):
    from btrfs_backup_ng.config.loader import load_config

    src = tmp_path / "src"
    (src / ".snapshots").mkdir(parents=True)
    text = (
        f"[global]\nparallel_targets = {parallel_targets}\n\n"
        f'[[volumes]]\npath = "{src}"\nsnapshot_prefix = "home-"\n\n'
    )
    for n in range(1, targets + 1):
        dest = tmp_path / f"t{n}"
        dest.mkdir()
        text += f'[[volumes.targets]]\npath = "{dest}"\n\n'
    cfg = tmp_path / "config.toml"
    cfg.write_text(text)
    res = load_config(str(cfg))
    return res[0] if isinstance(res, tuple) else res


class _Endpoint:
    """Stands in for every endpoint `run` opens; records what it is asked."""

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def snapshot(self, **_kw):
        self.calls.append("snapshot")
        return _Snap("home-20260101-000000")

    def prepare(self):
        return None

    def __getattr__(self, _name):
        return lambda *a, **k: None


class TestTheRunPipelineBeginsNothingNew:
    def test_no_snapshot_is_taken(self, tmp_path, monkeypatch, warnings_seen):
        config = _load(tmp_path, targets=1)
        calls: list[str] = []
        monkeypatch.setattr(
            run_cli.endpoint, "choose_endpoint", lambda *a, **k: _Endpoint(calls)
        )
        lifecycle.request_stop()
        ok, _stats, errors = run_cli._backup_volume(config.volumes[0], config, 1)
        assert calls == []
        assert ok is False
        assert any(lifecycle.NOT_STARTED in e for e in errors)
        assert any("Not starting the backup of" in m for m in warnings_seen())

    def test_a_target_a_worker_picks_up_is_not_transferred(self, monkeypatch):
        """A queued target in a pool the interrupt never reached (a volume
        worker's) is refused by the worker that picks it up, and counted as
        not done."""
        synced: list[object] = []
        monkeypatch.setattr(
            run_cli,
            "sync_snapshots",
            lambda *a, **k: synced.append(a) or TransferResult(),
        )
        lifecycle.request_stop()
        outcome = run_cli._transfer_to_target(
            MagicMock(),
            MagicMock(),
            SimpleNamespace(path="/t1", compress=None, rate_limit=None, ssh_sudo=False),
            _Snap("home-20260101-000000"),
            True,
        )
        assert outcome is None
        assert synced == []

    def test_the_prune_after_the_transfers_is_not_started(self, monkeypatch):
        planned: list[object] = []
        monkeypatch.setattr(
            run_cli,
            "plan_endpoint_retention",
            lambda *a, **k: planned.append(a) or ([], []),
        )
        monkeypatch.setattr(
            run_cli, "execute_retention_deletes", lambda *a, **k: (0, [])
        )
        volume = SimpleNamespace(path="/home", snapshot_prefix="home-")
        config = SimpleNamespace(
            get_source_retention=lambda v: _retention(),
            get_target_retention=lambda v, t: _retention(),
            global_config=SimpleNamespace(timestamp_format=None),
        )
        errors: list[str] = []
        lifecycle.request_stop()
        ok = run_cli._prune_after_transfer(
            volume,
            config,
            MagicMock(),
            [(MagicMock(), SimpleNamespace(path="/t1"))],
            errors,
        )
        assert ok is False
        assert planned == []
        assert any(lifecycle.NOT_STARTED in e for e in errors)

    def test_a_snapper_volume_begins_no_further_target(self, monkeypatch):
        synced: list[object] = []
        monkeypatch.setattr(
            operations, "sync_snapper_snapshots", lambda *a, **k: synced.append(a)
        )
        monkeypatch.setattr(
            "btrfs_backup_ng.snapper.SnapperScanner", lambda *a, **k: object()
        )
        monkeypatch.setattr(
            run_cli, "snapper_destination_options", lambda *a: {"compress": "none"}
        )
        monkeypatch.setattr(run_cli, "_snapper_catch_up_selector", lambda *a: None)
        monkeypatch.setattr(run_cli.endpoint, "choose_endpoint", lambda *a: object())
        for check in ("assert_encryption_applied", "assert_compression_applied"):
            monkeypatch.setattr(run_cli.endpoint, check, lambda *a: None)
        targets = [
            SimpleNamespace(
                path=path,
                require_mount=False,
                encrypt=None,
                compress=None,
                rate_limit=None,
            )
            for path in ("/t1", "/t2")
        ]
        volume = SimpleNamespace(
            path="/", snapper=SimpleNamespace(config_name="root"), targets=targets
        )
        lifecycle.request_stop()
        ok, stats, errors = run_cli._backup_snapper_volume(volume, MagicMock())
        assert synced == []
        assert ok is False
        assert stats["failed"] == 2
        assert sum(lifecycle.NOT_STARTED in e for e in errors) == 2

    def test_the_snapper_prune_is_not_started(self, monkeypatch):
        planned: list[object] = []
        monkeypatch.setattr(
            prune_cli,
            "plan_snapper_retention",
            lambda *a, **k: planned.append(a) or ([], []),
        )
        errors: list[str] = []
        lifecycle.request_stop()
        ok = run_cli._prune_snapper_after_transfer(
            SimpleNamespace(path="/"),
            SimpleNamespace(get_target_retention=lambda v, t: _retention()),
            [(SimpleNamespace(path="/t1"), {})],
            errors,
        )
        assert ok is False
        assert planned == []
        assert any(lifecycle.NOT_STARTED in e for e in errors)


class TestDeletionsStopBetweenSnapshots:
    def test_a_prune_pass_begins_no_further_deletion(self, warnings_seen):
        snaps = [_Snap("a"), _Snap("b"), _Snap("c")]
        asked: list[str] = []

        class Endpoint:
            def delete_snapshots(self, batch, delete_session=None):
                asked.extend(s.get_name() for s in batch)
                lifecycle.request_stop()
                return DeletionResult(deleted=list(batch))

        deleted, errors = prune_cli.execute_retention_deletes(Endpoint(), snaps)
        assert asked == ["a"]
        assert deleted == 1
        assert errors == [f"Delete b, c: {lifecycle.NOT_STARTED}"]
        assert any(
            "Not starting the deletion of b or the 1 after it" in m
            for m in warnings_seen()
        )

    def test_a_snapper_slot_pass_begins_no_further_deletion(self, monkeypatch):
        removed: list[str] = []

        def remove(endpoint_obj, slot_dir, remote):
            removed.append(slot_dir)
            lifecycle.request_stop()

        endpoint_obj = MagicMock()
        endpoint_obj.config = {"path": "/dest"}
        endpoint_obj._read_locks.return_value = {}
        monkeypatch.setattr(
            "btrfs_backup_ng.endpoint.choose_endpoint", lambda *a, **k: endpoint_obj
        )
        monkeypatch.setattr(prune_cli, "_delete_snapper_slot_btrfs", remove)
        deleted, errors = prune_cli.delete_snapper_backups(
            "/dest", [{"number": 1}, {"number": 2}, {"number": 3}]
        )
        assert removed == ["/dest/.snapshots/1"]
        assert deleted == 1
        assert errors == [f"Delete slot(s) 2, 3: {lifecycle.NOT_STARTED}"]

    def test_a_raw_snapper_batch_is_not_started(self, monkeypatch):
        endpoint_obj = MagicMock()
        endpoint_obj.list_snapshots.return_value = [_Snap("s-1"), _Snap("s-2")]
        monkeypatch.setattr(
            "btrfs_backup_ng.endpoint.choose_endpoint", lambda *a, **k: endpoint_obj
        )
        lifecycle.request_stop()
        deleted, errors = prune_cli.delete_snapper_backups(
            "raw:///dest",
            [{"number": 1, "backup_name": "s-1"}, {"number": 2, "backup_name": "s-2"}],
        )
        endpoint_obj.delete_snapshots.assert_not_called()
        assert deleted == 0
        assert errors == [f"Delete s-1, s-2: {lifecycle.NOT_STARTED}"]


# ---------------------------------------------------- the pools, wired


def _interrupt_on_main_thread(monkeypatch, module, started, count, release):
    """Replace ``module.as_completed`` so the MAIN thread, once ``count`` jobs
    have started, is interrupted exactly as Ctrl-C interrupts it. Other
    threads wait as usual."""
    real = module.as_completed

    def as_completed(futures):
        if threading.current_thread() is not threading.main_thread():
            return real(futures)
        deadline = time.monotonic() + 10
        while len(started) < count:
            assert time.monotonic() < deadline, started
            time.sleep(0.01)
        threading.Timer(0.2, release.set).start()
        raise KeyboardInterrupt

    monkeypatch.setattr(module, "as_completed", as_completed)


class TestThePoolsInRun:
    def test_the_volume_pool_starts_no_queued_volume(self, monkeypatch):
        started: list[str] = []
        release = threading.Event()

        def backup_volume(volume, *a, **k):
            started.append(volume.path)
            release.wait(10)
            return True, {}, []

        monkeypatch.setattr(run_cli, "_backup_volume", backup_volume)
        _interrupt_on_main_thread(monkeypatch, run_cli, started, 2, release)
        volumes = [SimpleNamespace(path=f"/v{n}") for n in (1, 2, 3)]
        config = SimpleNamespace(
            global_config=SimpleNamespace(
                parallel_volumes=2, parallel_targets=1, btrfs_debug=False
            ),
            get_enabled_volumes=lambda: volumes,
        )
        with pytest.raises(KeyboardInterrupt):
            run_cli._run_configured_backups(
                argparse.Namespace(no_progress=True), config
            )
        assert lifecycle.stop_requested()
        assert sorted(started) == ["/v1", "/v2"]

    def test_the_target_pool_starts_no_queued_target_and_does_not_prune(
        self, tmp_path, monkeypatch
    ):
        config = _load(tmp_path, targets=3, parallel_targets=2)
        started: list[str] = []
        release = threading.Event()
        pruned: list[object] = []

        def transfer(source, dest, target_config, *a, **k):
            started.append(target_config.path)
            release.wait(10)
            return TransferResult()

        monkeypatch.setattr(
            run_cli.endpoint, "choose_endpoint", lambda *a, **k: _Endpoint([])
        )
        monkeypatch.setattr(run_cli, "_transfer_to_target", transfer)
        monkeypatch.setattr(
            run_cli, "_prune_after_transfer", lambda *a, **k: pruned.append(a)
        )
        _interrupt_on_main_thread(monkeypatch, run_cli, started, 2, release)
        with pytest.raises(KeyboardInterrupt):
            run_cli._backup_volume(config.volumes[0], config, 2)
        assert lifecycle.stop_requested()
        assert sorted(Path(p).name for p in started) == ["t1", "t2"]
        assert pruned == []


class TestThePoolsInExecution:
    """``execute_parallel`` dispatches to worker threads too."""

    def test_the_volume_pool_starts_no_queued_volume(self, monkeypatch):
        started: list[str] = []
        release = threading.Event()

        def job(volume, target):
            started.append(volume.path)
            release.wait(10)
            return execution.JobResult(volume.path, target.path, True)

        _interrupt_on_main_thread(monkeypatch, execution, started, 1, release)
        volumes = [
            SimpleNamespace(path=f"/v{n}", targets=[SimpleNamespace(path="/t")])
            for n in (1, 2)
        ]
        config = SimpleNamespace(
            global_config=SimpleNamespace(parallel_volumes=1, parallel_targets=1),
            get_enabled_volumes=lambda: volumes,
        )
        with pytest.raises(KeyboardInterrupt):
            execution.execute_parallel(config, job)
        assert lifecycle.stop_requested()
        assert started == ["/v1"]

    def test_the_target_pool_starts_no_queued_target(self, monkeypatch):
        started: list[str] = []
        release = threading.Event()

        def job(volume, target):
            started.append(target.path)
            release.wait(10)
            return execution.JobResult(volume.path, target.path, True)

        _interrupt_on_main_thread(monkeypatch, execution, started, 1, release)
        volume = SimpleNamespace(
            path="/v",
            targets=[SimpleNamespace(path="/t1"), SimpleNamespace(path="/t2")],
        )
        with pytest.raises(KeyboardInterrupt):
            execution._execute_volume_targets(volume, job, 1)
        assert lifecycle.stop_requested()
        assert started == ["/t1"]

    def test_a_queued_target_in_a_volume_worker_s_pool_is_not_run(self):
        """The interrupt never reaches a volume worker, so its target pool is
        not cancelled: each queued job asks for itself."""
        started: list[str] = []

        def job(volume, target):
            started.append(target.path)
            lifecycle.request_stop()
            return execution.JobResult(volume.path, target.path, True)

        volume = SimpleNamespace(
            path="/v",
            targets=[SimpleNamespace(path="/t1"), SimpleNamespace(path="/t2")],
        )
        config = SimpleNamespace(
            global_config=SimpleNamespace(parallel_volumes=1, parallel_targets=1),
            get_enabled_volumes=lambda: [volume],
        )
        results = execution.execute_parallel(config, job)
        assert started == ["/t1"]
        assert sorted((r.target_path, r.success) for r in results) == [
            ("/t1", True),
            ("/t2", False),
        ]

    def test_a_job_a_worker_picks_up_after_the_stop_is_not_run(self):
        ran: list[object] = []
        lifecycle.request_stop()
        result = execution._start_job(
            lambda v, t: ran.append(t),
            SimpleNamespace(path="/v"),
            SimpleNamespace(path="/t"),
        )
        assert ran == []
        assert result.success is False
        assert result.error == lifecycle.NOT_STARTED


class TestTheEntryPointsRequestTheStop:
    """Belt and braces: however the interrupt reaches the top, a worker thread
    still finishing hears of it."""

    def test_the_dispatcher(self, monkeypatch, capsys):
        from btrfs_backup_ng.cli import dispatcher

        def interrupted(args):
            raise KeyboardInterrupt

        monkeypatch.setattr(dispatcher, "cmd_run", interrupted)
        args = argparse.Namespace(version=False, command="run")
        assert dispatcher.run_subcommand(args) == 130
        assert lifecycle.stop_requested()

    def test_the_program_entry_point(self, monkeypatch, capsys):
        import btrfs_backup_ng.__main__ as entry

        def interrupted():
            raise KeyboardInterrupt

        monkeypatch.setattr(entry, "cli_main", interrupted)
        monkeypatch.setattr(lifecycle, "install_signal_handlers", lambda: [])
        with pytest.raises(SystemExit) as caught:
            entry.main()
        assert caught.value.code == 130
        assert lifecycle.stop_requested()


# --------------------------------------------------------- nothing retried


class TestTheRetryFrameworkRetriesNothing:
    def test_the_context_allows_the_first_attempt_and_no_other(self):
        lifecycle.request_stop()
        ctx = RetryContext(RetryPolicy(max_attempts=3, initial_delay=30))
        assert not ctx.exhausted
        assert ctx.record_failure(TransientNetworkError("reset")) is False
        assert ctx.exhausted

    def test_the_context_does_not_sleep_out_its_backoff(self):
        ctx = RetryContext(RetryPolicy(max_attempts=3, initial_delay=30, jitter=0))
        assert ctx.record_failure(TransientNetworkError("reset")) is True
        threading.Timer(0.2, lifecycle.request_stop).start()
        started = time.monotonic()
        ctx.wait()
        assert time.monotonic() - started < 5
        assert ctx.exhausted

    def test_the_context_waits_nothing_once_stopped(self):
        ctx = RetryContext(RetryPolicy(max_attempts=3, initial_delay=30, jitter=0))
        ctx.record_failure(TransientNetworkError("reset"))
        lifecycle.request_stop()
        with patch("btrfs_backup_ng.lifecycle.sleep_unless_stopped") as sleeper:
            assert ctx.wait() == 0
        sleeper.assert_not_called()

    def test_an_attempt_is_not_retried(self):
        lifecycle.request_stop()
        attempt = RetryAttempt(0, 3, RetryPolicy(initial_delay=30))
        assert attempt.should_retry(TransientNetworkError("reset")) is False

    def test_an_attempt_does_not_sleep_out_its_backoff(self):
        attempt = RetryAttempt(0, 3, RetryPolicy(initial_delay=30, jitter=0))
        threading.Timer(0.2, lifecycle.request_stop).start()
        started = time.monotonic()
        attempt.wait()
        assert time.monotonic() - started < 5

    def test_an_attempt_waits_nothing_once_stopped(self):
        attempt = RetryAttempt(0, 3, RetryPolicy(initial_delay=30, jitter=0))
        lifecycle.request_stop()
        with patch("btrfs_backup_ng.lifecycle.sleep_unless_stopped") as sleeper:
            assert attempt.wait() == 0
        sleeper.assert_not_called()

    async def test_an_async_attempt_waits_nothing_once_stopped(self):
        attempt = RetryAttempt(0, 3, RetryPolicy(initial_delay=30, jitter=0))
        lifecycle.request_stop()
        with patch("asyncio.sleep") as sleeper:
            assert await attempt.wait_async() == 0
        sleeper.assert_not_called()

    def test_the_attempts_stop_after_the_first(self):
        lifecycle.request_stop()
        assert len(list(RetryPolicy(max_attempts=3).attempts())) == 1

    def test_a_decorated_call_is_made_once(self, warnings_seen):
        calls: list[int] = []

        @with_retry(max_attempts=3, initial_delay=30)
        def flaky():
            calls.append(1)
            lifecycle.request_stop()
            raise TransientNetworkError("reset")

        with pytest.raises(TransientNetworkError):
            flaky()
        assert calls == [1]
        assert any(f"not retrying: {INTERRUPTED}" in m for m in warnings_seen())
        assert not any("Non-retryable" in m for m in warnings_seen())

    def test_a_decorated_call_interrupted_in_its_backoff_is_not_repeated(
        self, warnings_seen
    ):
        calls: list[int] = []

        @with_retry(max_attempts=3, initial_delay=30, jitter=0)
        def flaky():
            calls.append(1)
            threading.Timer(0.2, lifecycle.request_stop).start()
            raise TransientNetworkError("reset")

        started = time.monotonic()
        with pytest.raises(TransientNetworkError):
            flaky()
        assert calls == [1]
        assert time.monotonic() - started < 5
        assert any(f"Not retrying: {INTERRUPTED}" in m for m in warnings_seen())
        assert not any("attempts failed" in m for m in warnings_seen())

    def test_retry_call_is_made_once(self):
        calls: list[int] = []

        def flaky():
            calls.append(1)
            lifecycle.request_stop()
            raise TransientNetworkError("reset")

        result = retry_call(flaky, max_attempts=3, initial_delay=30)
        assert result.success is False
        assert calls == [1]


def _ssh_endpoint(attempt):
    ep = SSHEndpoint.__new__(SSHEndpoint)
    ep.config = {"path": "/remote/dest", "username": "u"}
    ep.hostname = "nas"
    ep._last_transfer_error = None
    ep._require_remote_destination = lambda path: True
    ep._run_diagnostics = lambda *a, **k: {
        "ssh_connection": True,
        "btrfs_command": True,
        "write_permissions": True,
        "btrfs_filesystem": True,
    }
    calls: list[int] = []

    def try_direct_transfer(**kwargs):
        calls.append(1)
        return attempt(len(calls))

    ep._try_direct_transfer = try_direct_transfer
    return ep, calls


def _snapshot():
    snap = MagicMock()
    snap.get_path.return_value = "/src/home-1"
    snap.get_name.return_value = "home-1"
    return snap


_FAST = RetryPolicy(max_attempts=3, initial_delay=0, jitter=0)


class TestTheSshTransferIsNotRetried:
    def test_without_an_interrupt_a_failed_attempt_is_retried(self, shared_log):
        """Control: the harness can see a retry when there should be one."""
        ep, calls = _ssh_endpoint(lambda n: n == 3)
        assert ep.send_receive(_snapshot(), retry_policy=_FAST) is True
        assert len(calls) == 3

    def test_a_transfer_killed_by_the_interrupt_is_not_sent_again(self, shared_log):
        """A terminal's Ctrl-C kills the transfer's children: the attempt
        fails, and it must not be repeated."""

        def killed(n):
            lifecycle.request_stop()
            return False

        ep, calls = _ssh_endpoint(killed)
        assert ep.send_receive(_snapshot(), retry_policy=_FAST) is False
        assert len(calls) == 1
        messages = shared_log.messages()
        assert any(f"not retrying: {INTERRUPTED}" in m for m in messages)
        assert not any("retrying in" in m for m in messages)

    def test_a_retryable_error_after_the_interrupt_is_not_retried(self, shared_log):
        def killed(n):
            lifecycle.request_stop()
            raise TransientNetworkError("connection reset")

        ep, calls = _ssh_endpoint(killed)
        assert ep.send_receive(_snapshot(), retry_policy=_FAST) is False
        assert len(calls) == 1
        assert any(f"not retrying: {INTERRUPTED}" in m for m in shared_log.messages())

    def test_an_interrupt_during_the_backoff_ends_it(self, shared_log, monkeypatch):
        def interrupted_wait(seconds):
            lifecycle.request_stop()
            return True

        monkeypatch.setattr(lifecycle, "sleep_unless_stopped", interrupted_wait)
        ep, calls = _ssh_endpoint(lambda n: False)
        assert ep.send_receive(_snapshot(), retry_policy=_FAST) is False
        assert len(calls) == 1
        messages = shared_log.messages()
        assert any(f"not retrying: {INTERRUPTED}" in m for m in messages)
        assert not any("exhausting all" in m for m in messages)


class TestRemoteCommandsAreNotRetried:
    def _endpoint(self, reply):
        ep = SSHEndpoint.__new__(SSHEndpoint)
        ep.config = {"path": "/remote/dest", "username": "u"}
        ep.hostname = "nas"
        ep._cached_sudo_password = "old"
        calls: list[int] = []

        def run(command, **kwargs):
            calls.append(1)
            return reply(len(calls))

        ep._exec_remote_command = run
        ep._build_remote_command = lambda command: ["sudo", "-S", *command]
        return ep, calls

    @staticmethod
    def _refused(n):
        return SimpleNamespace(returncode=1, stderr=b"Sorry, try again.", stdout=b"")

    def test_without_an_interrupt_a_failed_command_is_retried(self):
        """Control for the two cases below."""

        def boom(n):
            raise RuntimeError("connection dropped")

        ep, calls = self._endpoint(boom)
        with pytest.raises(RuntimeError):
            ep._exec_remote_command_with_retry(["btrfs", "x"], max_retries=2)
        assert len(calls) == 3

    def test_a_failed_command_is_not_retried_after_the_interrupt(self):
        def boom(n):
            lifecycle.request_stop()
            raise RuntimeError("connection dropped")

        ep, calls = self._endpoint(boom)
        with pytest.raises(RuntimeError):
            ep._exec_remote_command_with_retry(["btrfs", "x"], max_retries=2)
        assert len(calls) == 1

    def test_a_refused_password_is_not_asked_again_after_the_interrupt(self):
        ep, calls = self._endpoint(self._refused)
        asked: list[int] = []
        ep._get_sudo_password = lambda **k: asked.append(1) or "new"
        lifecycle.request_stop()
        result = ep._exec_remote_command_with_retry(["btrfs", "x"], max_retries=2)
        assert result.returncode == 1
        assert asked == []
        assert len(calls) == 1

    def test_without_an_interrupt_a_refused_password_is_asked_again(self):
        ep, calls = self._endpoint(self._refused)
        asked: list[int] = []
        ep._get_sudo_password = lambda **k: asked.append(1) or "new"
        ep._exec_remote_command_with_retry(["btrfs", "x"], max_retries=1)
        assert asked == [1]
        assert len(calls) == 2


# ------------------------------------------------- the real run, end to end


def _defaults() -> None:
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, signal.SIG_DFL)


# The real `run` command over three targets with parallel_targets = 2, so two
# workers are transferring while the third target waits in the pool. Each
# target's plan is two snapshots; a "transfer" is a tracked `sleep` child,
# watched the way the ssh endpoint watches its pipeline (a poll every 0.2s).
# Only the endpoints, the planner and the verdict are stand-ins.
_RUN = """
import os, subprocess, sys, time
from btrfs_backup_ng import __util__, endpoint, lifecycle
from btrfs_backup_ng.cli import run as run_cli
from btrfs_backup_ng.core import operations, planning
from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict

SLEEP = sys.argv[1]
events = open("events", "a", buffering=1)

class Snap:
    def __init__(self, name):
        self.name = name
        self.locks = set()
        self.parent_locks = set()
    def get_name(self):
        return self.name
    def __str__(self):
        return self.name

SNAPS = [Snap("s1"), Snap("s2")]

class Source:
    def prepare(self):
        pass
    def snapshot(self):
        return SNAPS[-1]
    def list_snapshots(self, flush_cache=False):
        return list(SNAPS)
    def set_lock(self, *a, **k):
        pass
    def get_id(self):
        return "src"

class Destination:
    def __init__(self, label):
        self.label = label
        self.config = {"path": label}
    def prepare(self):
        pass
    def get_id(self):
        return self.label
    def add_snapshot(self, snap):
        pass
    def list_snapshots(self, flush_cache=False):
        return []
    def __repr__(self):
        return self.label

def choose_endpoint(path, kwargs, source=False):
    return Source() if source else Destination(os.path.basename(str(path)))

def send(snapshot, destination, parent=None, options=None):
    events.write(f"start {destination.label} {snapshot.name}\\n")
    with lifecycle.process_scope():
        child = lifecycle.track(subprocess.Popen(["sleep", SLEEP]))
        while child.poll() is None:
            time.sleep(0.2)
    events.write(f"end {destination.label} {snapshot.name} {child.returncode}\\n")
    if child.returncode != 0:
        raise __util__.SnapshotTransferError(f"sleep exited {child.returncode}")

endpoint.choose_endpoint = choose_endpoint
endpoint.assert_encryption_applied = lambda *a, **k: None
endpoint.assert_compression_applied = lambda *a, **k: None
run_cli._catch_up_selector = lambda *a, **k: None
planning.snapshots_present_on = lambda *a: set()
planning.plan_transfer_sequence = lambda *a, **k: [(SNAPS[0], None), (SNAPS[1], SNAPS[0])]
operations.send_snapshot = send
operations.destination_artifact_exists = lambda *a: False
operations._cleanup_this_runs_partial = lambda *a, **k: None
operations.artifact_verdict = lambda *a: StructureVerdict("ok", "stand-in")

import btrfs_backup_ng.__main__ as entry
sys.argv = ["btrfs-backup-ng", "-c", "config.toml", "run", "--no-progress"]
entry.main()
"""


class TestTheRealRunAfterCtrlC:
    @pytest.mark.parametrize("reach", ["process", "group"])
    def test_in_flight_work_ends_and_nothing_new_starts(self, tmp_path, reach):
        """``process`` is ``kill -INT <pid>``: the transfers in flight finish.
        ``group`` is a terminal's Ctrl-C: their children die with the rest of
        the group. Either way no queued target starts, no worker begins its
        next snapshot, and the run exits through KeyboardInterrupt."""
        src = tmp_path / "src"
        src.mkdir()
        text = (
            "[global]\nparallel_targets = 2\n\n"
            f'[[volumes]]\npath = "{src}"\nsnapshot_prefix = "s"\n\n'
        )
        for label in ("t1", "t2", "t3"):
            (tmp_path / label).mkdir()
            text += f'[[volumes.targets]]\npath = "{tmp_path / label}"\n\n'
        (tmp_path / "config.toml").write_text(text)
        runtime = tmp_path / "runtime"
        runtime.mkdir()
        events = tmp_path / "events"
        proc = subprocess.Popen(
            [sys.executable, "-c", _RUN, "3"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            # Wide enough that the console does not wrap the message checked.
            env={**os.environ, "XDG_RUNTIME_DIR": str(runtime), "COLUMNS": "200"},
            preexec_fn=_defaults,
            start_new_session=True,
            cwd=tmp_path,
        )
        try:
            deadline = time.monotonic() + 30
            while True:
                seen = events.read_text().split("\n") if events.exists() else []
                if sum(line.startswith("start ") for line in seen) >= 2:
                    break
                assert proc.poll() is None, proc.communicate()
                assert time.monotonic() < deadline, seen
                time.sleep(0.05)
            if reach == "process":
                os.kill(proc.pid, signal.SIGINT)
            else:
                os.killpg(proc.pid, signal.SIGINT)
            out, err = proc.communicate(timeout=60)
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
        lines = [line for line in events.read_text().split("\n") if line]
        starts = sorted(line for line in lines if line.startswith("start "))
        ends = sorted(line.rsplit(" ", 1) for line in lines if line.startswith("end "))
        assert proc.returncode == 130, err
        assert "Interrupted." in out + err
        # Only the two transfers that were running; never t3, never s2.
        assert starts == ["start t1 s1", "start t2 s1"], lines
        assert [who for who, _rc in ends] == ["end t1 s1", "end t2 s1"], lines
        if reach == "process":
            assert [rc for _who, rc in ends] == ["0", "0"], lines
        else:
            assert [rc for _who, rc in ends] == ["-2", "-2"], lines
        assert "Not starting the transfer of s2" in out + err


class TestTheLastMomentsBeforeWorkBegins:
    """Stops that arrive after a worker has passed one check but before the
    work itself begins: preparing the targets, pinning and checking a
    destination, or an ssh transfer's remote diagnostics."""

    def test_no_target_is_prepared_after_a_stop_during_the_snapshot(
        self, tmp_path, monkeypatch, warnings_seen
    ):
        """Preparing a target opens an ssh master, runs remote diagnostics
        with a write test and may prompt for a password; none of it begins."""
        config = _load(tmp_path, targets=3)
        calls: list[str] = []

        class StopsDuringTheSnapshot(_Endpoint):
            def snapshot(self, **kw):
                taken = super().snapshot(**kw)
                lifecycle.request_stop()
                return taken

            def prepare(self):
                self.calls.append("prepare")

        monkeypatch.setattr(
            run_cli.endpoint,
            "choose_endpoint",
            lambda *a, **k: StopsDuringTheSnapshot(calls),
        )
        ok, stats, errors = run_cli._backup_volume(config.volumes[0], config, 1)
        assert "snapshot" in calls
        assert calls[calls.index("snapshot") + 1 :] == []
        assert ok is False
        assert stats["failed"] == 3
        assert sum(lifecycle.NOT_STARTED in e for e in errors) == 3
        assert any(
            "Not starting the transfer to" in m and "or the 2 after it" in m
            for m in warnings_seen()
        )

    def test_a_transfer_begins_nothing_after_a_stop_during_its_setup(self, monkeypatch):
        """The executor's check came before the source was pinned and the
        destination checked; the transfer itself checks again."""
        started: list[int] = []
        monkeypatch.setattr(
            operations, "_send_snapshot", lambda *a, **k: started.append(1)
        )
        lifecycle.request_stop()
        with pytest.raises(__util__.SnapshotTransferError, match="interrupted"):
            operations.send_snapshot(_Snap("s1"), MagicMock())
        assert started == []

    def test_an_ssh_transfer_starts_no_stream_after_a_stop_during_its_checks(
        self, shared_log
    ):
        ep, calls = _ssh_endpoint(lambda n: True)
        checks = ep._run_diagnostics

        def checks_then_stop(*a, **k):
            lifecycle.request_stop()
            return checks(*a, **k)

        ep._run_diagnostics = checks_then_stop
        assert ep.send_receive(_snapshot(), retry_policy=_FAST) is False
        assert calls == []


class TestTheRetryHelpersWithoutProductionCallers:
    def test_an_async_backoff_ends_when_the_stop_comes(self):
        import asyncio

        attempt = RetryAttempt(
            attempt_number=0,
            max_attempts=3,
            policy=RetryPolicy(max_attempts=3, initial_delay=5, jitter=0),
        )

        async def backoff() -> float:
            asyncio.get_running_loop().call_later(0.2, lifecycle.request_stop)
            began = time.monotonic()
            await attempt.wait_async()
            return time.monotonic() - began

        assert asyncio.run(backoff()) < 2

    def test_retry_call_reports_the_calls_it_made_when_a_stop_ends_its_backoff(
        self, monkeypatch
    ):
        calls: list[int] = []

        def failing():
            calls.append(1)
            raise TransientNetworkError("reset")

        def stopped_during_the_wait(seconds):
            lifecycle.request_stop()
            return True

        monkeypatch.setattr(lifecycle, "sleep_unless_stopped", stopped_during_the_wait)
        result = retry_call(failing, max_attempts=3, initial_delay=30)
        assert result.success is False
        assert calls == [1]
        assert result.attempts == 1
