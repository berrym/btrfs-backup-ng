"""Only one run at a time, for one configuration, on one machine.

Raised by mjg in #93: a timer firing while the previous transfer is still running
started a second run over the same volumes and targets. systemd declines to start
a second copy of one unit, so the packaged timer was covered by systemd rather
than by us, and a manual run racing a timer run was not covered at all. Local
raw:// targets take a per-target flock; ssh:// has none, and raw+ssh:// cannot
hold one -- its target_lock is a documented no-op.
"""

from __future__ import annotations

import argparse
import multiprocessing
import os
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli.run import _run_lock_path, execute_run


def _hold_lock(lock_file, ready, release):
    """Hold the run lock in a separate PROCESS -- flock is per-process, so a
    thread in this interpreter would not contend the way a second run does."""
    with __util__.exclusive_lock(Path(lock_file), timeout=5, subject="holder"):
        ready.set()
        release.wait(timeout=30)


class TestTheRunLockPath:
    def test_two_configs_do_not_share_a_lock(self, tmp_path):
        a = _run_lock_path(tmp_path / "a.toml")
        b = _run_lock_path(tmp_path / "b.toml")
        assert a != b

    def test_same_named_configs_in_different_directories_do_not_collide(self, tmp_path):
        """The realistic collision, and the reason the name carries a digest.

        Practically every installation calls its file config.toml, so a system
        config and a user config differ only by directory. Keying on the name
        alone would give them one lock, and an unrelated user backup would
        block the system one -- or worse, be blocked by it and skip silently.
        """
        (tmp_path / "etc").mkdir()
        (tmp_path / "home").mkdir()
        system = _run_lock_path(tmp_path / "etc" / "config.toml")
        user = _run_lock_path(tmp_path / "home" / "config.toml")
        assert system != user, (
            "two different configurations share one run lock, so one would "
            "silently block the other"
        )

    def test_the_same_config_always_resolves_to_one_lock(self, tmp_path):
        """However it was spelled: a relative path and a padded one are the
        same configuration, and must not each get their own lock."""
        config = tmp_path / "c.toml"
        config.write_text("")
        assert _run_lock_path(config) == _run_lock_path(Path(str(config)))
        assert _run_lock_path(config) == _run_lock_path(tmp_path / "." / "c.toml")

    def test_a_string_path_is_accepted(self, tmp_path):
        """Config discovery hands back a str on some paths; a lock that raised
        AttributeError here would take down every run."""
        assert _run_lock_path(str(tmp_path / "c.toml")).name.endswith(".lock")

    def test_it_lives_in_a_runtime_dir_not_the_config_dir(self, tmp_path, monkeypatch):
        """The config dir may be read-only and the target may be remote."""
        monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "rt"))
        path = _run_lock_path(tmp_path / "c.toml")
        assert str(tmp_path / "rt") in str(path)
        assert path.parent.exists()

    @pytest.mark.skipif(os.geteuid() == 0, reason="checks the non-root branch")
    def test_the_lock_directory_is_not_world_writable(self, tmp_path, monkeypatch):
        """It coordinates work that often runs as root."""
        monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "rt"))
        path = _run_lock_path(tmp_path / "c.toml")
        assert not (path.parent.stat().st_mode & 0o022), oct(path.parent.stat().st_mode)


class TestOnlyOneRunAtATime:
    def _config(self, tmp_path):
        config = tmp_path / "config.toml"
        config.write_text(
            "[[volumes]]\n"
            f'path = "{tmp_path / "src"}"\n'
            "\n"
            "[[volumes.targets]]\n"
            f'path = "{tmp_path / "dest"}"\n'
        )
        return config

    def test_a_second_run_is_refused_while_one_is_in_progress(self, tmp_path):
        config = self._config(tmp_path)
        lock_file = _run_lock_path(config)

        ctx = multiprocessing.get_context("fork")
        ready, release = ctx.Event(), ctx.Event()
        holder = ctx.Process(target=_hold_lock, args=(str(lock_file), ready, release))
        holder.start()
        try:
            assert ready.wait(timeout=15), "the holder never acquired the lock"
            args = argparse.Namespace(
                config=str(config), verbose=0, quiet=False, dry_run=False
            )
            started = time.monotonic()
            rc = execute_run(args)
            waited = time.monotonic() - started
        finally:
            release.set()
            holder.join(timeout=15)

        assert rc == 1, "a run that did not happen must not report success"
        # Bounded: it must give up, not queue behind a transfer that could run
        # for hours.
        assert waited < 60, f"waited {waited:.1f}s -- the wait is not bounded"

    def test_the_lock_is_released_when_the_run_finishes(self, tmp_path):
        """Otherwise the first run poisons every later one on this machine."""
        config = self._config(tmp_path)
        execute_run(
            argparse.Namespace(
                config=str(config), verbose=0, quiet=False, dry_run=False
            )
        )
        # Acquiring it now must succeed immediately.
        with __util__.exclusive_lock(
            _run_lock_path(config), timeout=1, subject="after"
        ):
            pass

    def test_a_dry_run_takes_no_lock(self, tmp_path):
        """It changes nothing, so it must not block a real run."""
        config = self._config(tmp_path)
        lock_file = _run_lock_path(config)
        ctx = multiprocessing.get_context("fork")
        ready, release = ctx.Event(), ctx.Event()
        holder = ctx.Process(target=_hold_lock, args=(str(lock_file), ready, release))
        holder.start()
        try:
            assert ready.wait(timeout=15)
            rc = execute_run(
                argparse.Namespace(
                    config=str(config), verbose=0, quiet=False, dry_run=True
                )
            )
        finally:
            release.set()
            holder.join(timeout=15)
        assert rc == 0, "a dry run was blocked by a running backup"


class TestAnUnavailableLockIsNotAnOutage:
    """A guard that cannot be set up must not become the failure it prevents.

    _run_lock_path creates a runtime directory, and mkdir raises OSError, not
    RuntimeError -- so an unwritable runtime directory (a read-only home, a
    stripped-down container) escaped the handler and took down every run. The
    lock existed to stop two runs colliding; it must not stop the one run that
    was going to work.

    Contention is different and still refuses: that is a real second run.
    """

    def _config(self, tmp_path):
        config = tmp_path / "config.toml"
        config.write_text(
            "[[volumes]]\n"
            f'path = "{tmp_path / "src"}"\n'
            "\n[[volumes.targets]]\n"
            f'path = "{tmp_path / "dest"}"\n'
        )
        return config

    @pytest.mark.skipif(os.geteuid() == 0, reason="root can write anywhere")
    def test_a_run_still_happens_when_the_lock_dir_cannot_be_made(
        self, tmp_path, monkeypatch
    ):
        blocked = tmp_path / "ro"
        blocked.mkdir()
        blocked.chmod(0o500)
        monkeypatch.setenv("XDG_RUNTIME_DIR", str(blocked / "nope"))
        config = self._config(tmp_path)
        try:
            # The regression is that this RAISED PermissionError out of the lock
            # setup. Whether the backup then succeeds is beside the point here
            # (tmp_path is not btrfs); what matters is that the run got past the
            # lock and returned an exit code like any other run.
            rc = execute_run(
                argparse.Namespace(
                    config=str(config), verbose=0, quiet=False, dry_run=False
                )
            )
        except OSError as e:  # pragma: no cover - the bug being pinned
            pytest.fail(f"an unwritable runtime dir took the whole run down: {e!r}")
        finally:
            blocked.chmod(0o700)
        assert isinstance(rc, int)

    @pytest.mark.skipif(os.geteuid() == 0, reason="root can write anywhere")
    def test_the_degradation_is_announced_not_silent(self, tmp_path, monkeypatch):
        """Running without the guard is acceptable; doing it quietly is not."""
        blocked = tmp_path / "ro2"
        blocked.mkdir()
        blocked.chmod(0o500)
        monkeypatch.setenv("XDG_RUNTIME_DIR", str(blocked / "nope"))
        from btrfs_backup_ng.cli.run import _run_lock

        messages = []
        with patch(
            "btrfs_backup_ng.cli.run.logger.warning", lambda *a, **k: messages.append(a)
        ):
            try:
                with _run_lock(tmp_path / "config.toml"):
                    pass
            finally:
                blocked.chmod(0o700)
        assert messages, "degraded silently"
        assert "NOT" in str(messages[0])
