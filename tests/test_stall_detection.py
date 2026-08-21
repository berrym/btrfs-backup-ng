"""A slow transfer and a stuck one are not the same thing.

Reported as #93 by mjg: an initial 36 GB sync over 100 Mbit hit a one-hour
wall-clock cap. That cap cannot distinguish the two cases -- it kills a healthy
transfer that simply has a lot to move, and waits the full hour on a pipe that
died in the first minute. Neither behaviour is the one anyone wants.

Bytes moved is the signal that separates them, read out-of-band from
/proc/<pid>/io so nothing is routed through Python and the direct pipe stays a
direct pipe.

The local `btrfs send` runs under sudo, so ITS io file is root-owned and
unreadable to us. That is not a failure: the ssh process is ours, and bytes
leaving on the socket is the same evidence. What must never happen is treating
"cannot read" as "no bytes moved", which would report a healthy transfer as
stuck.
"""

from __future__ import annotations

import os
import subprocess
import time

from btrfs_backup_ng import __util__


class TestTheByteSignal:
    def test_a_moving_process_reports_increasing_bytes(self):
        proc = subprocess.Popen(
            ["dd", "if=/dev/zero", "of=/dev/null", "bs=64k"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            time.sleep(0.3)
            first = __util__.process_io_bytes(proc.pid)
            time.sleep(0.5)
            second = __util__.process_io_bytes(proc.pid)
        finally:
            proc.terminate()
            proc.wait()
        assert first is not None and second is not None
        assert second > first, "a process moving data reported no movement"

    def test_an_idle_process_reports_no_movement(self):
        """The other half: if idling looked like movement, a wedged transfer
        would never be caught."""
        proc = subprocess.Popen(["sleep", "5"])
        try:
            time.sleep(0.3)
            first = __util__.process_io_bytes(proc.pid)
            time.sleep(0.6)
            second = __util__.process_io_bytes(proc.pid)
        finally:
            proc.terminate()
            proc.wait()
        assert first == second, "an idle process appeared to move data"

    def test_an_unreadable_process_is_None_not_zero(self):
        """Zero would mean 'no bytes moved' and kill a healthy transfer."""
        assert __util__.process_io_bytes(999_999) is None

    def test_a_process_owned_by_someone_else_is_None(self):
        """The sudo case, stated directly: PID 1 is root's."""
        if os.geteuid() == 0:
            return  # as root everything is readable; nothing to prove here
        assert __util__.process_io_bytes(1) is None


class TestTheAggregate:
    def test_unreadable_pids_are_ignored_when_others_can_be_read(self):
        """The real shape: `sudo btrfs send` unreadable, ssh readable."""
        total = __util__.any_bytes_moved([os.getpid(), 999_999])
        assert total is not None and total > 0

    def test_nothing_readable_yields_None(self):
        """Which the caller must treat as 'disable the check', never as a stall."""
        assert __util__.any_bytes_moved([999_999, 999_998]) is None

    def test_an_empty_set_yields_None(self):
        assert __util__.any_bytes_moved([]) is None

    def test_the_total_moves_when_a_member_moves(self):
        proc = subprocess.Popen(
            ["dd", "if=/dev/zero", "of=/dev/null", "bs=64k"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            time.sleep(0.3)
            first = __util__.any_bytes_moved([proc.pid, 999_999])
            time.sleep(0.5)
            second = __util__.any_bytes_moved([proc.pid, 999_999])
        finally:
            proc.terminate()
            proc.wait()
        assert second > first


class TestTheMonitorActsOnIt:
    """The signal is only worth having if the monitor stops a stuck transfer."""

    def _endpoint(self):
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        endpoint = SSHEndpoint(hostname="nas", path="/backup")
        endpoint._last_transfer_error = None
        return endpoint

    def test_a_stalled_transfer_is_terminated_and_explained(self, monkeypatch):
        """Two processes that are alive but moving nothing: the exact shape of a
        connection that died without closing."""
        import btrfs_backup_ng.endpoint.ssh as ssh_mod

        monkeypatch.setattr(ssh_mod, "STALL_TIMEOUT_SECONDS", 1)
        endpoint = self._endpoint()
        idle = [subprocess.Popen(["sleep", "30"]) for _ in range(2)]
        try:
            result = endpoint._monitor_transfer_progress(
                {"send": idle[0], "receive": idle[1]},
                start_time=time.time(),
                dest_path="/backup",
                snapshot_name="snap",
                max_wait_time=600,  # long: the STALL check must be what fires
            )
        finally:
            for proc in idle:
                if proc.poll() is None:
                    proc.kill()
                proc.wait()

        assert result is False, "a stalled transfer was not stopped"
        assert endpoint._last_transfer_error, "it stopped without recording why"
        assert "moved no data" in endpoint._last_transfer_error
        assert "NOT an ssh timeout" in endpoint._last_transfer_error

    def test_a_finished_send_is_not_a_stall(self, monkeypatch):
        """The tail of every transfer, and the worst false positive available.

        Once the local send has finished, the remote is still applying what it
        already received and no bytes move on this side. That is completion, not
        a stall. Killing it there would destroy a transfer that was about to
        succeed -- so the check only judges while the send is alive.

        A remote wedged MID-receive still blocks the send, so that case is
        unaffected; only the tail is exempt, and the wall clock covers it.
        """
        import btrfs_backup_ng.endpoint.ssh as ssh_mod

        monkeypatch.setattr(ssh_mod, "STALL_TIMEOUT_SECONDS", 1)
        endpoint = self._endpoint()

        finished_send = subprocess.Popen(["true"])
        finished_send.wait()  # send is DONE
        idle_receive = subprocess.Popen(["sleep", "4"])  # remote still applying
        try:
            endpoint._monitor_transfer_progress(
                {"send": finished_send, "receive": idle_receive},
                start_time=time.time(),
                dest_path="/backup",
                snapshot_name="snap",
                max_wait_time=3,
            )
        finally:
            if idle_receive.poll() is None:
                idle_receive.kill()
            idle_receive.wait()

        reason = endpoint._last_transfer_error or ""
        assert "moved no data" not in reason, (
            f"a transfer whose send had finished was called stalled: {reason}"
        )

    def test_a_moving_transfer_is_not_killed(self, monkeypatch):
        """The failure that matters most: never stop a healthy slow transfer.

        A process that keeps moving bytes must survive a stall window far
        shorter than the time it runs for.
        """
        import btrfs_backup_ng.endpoint.ssh as ssh_mod

        monkeypatch.setattr(ssh_mod, "STALL_TIMEOUT_SECONDS", 1)
        endpoint = self._endpoint()
        mover = subprocess.Popen(
            ["dd", "if=/dev/zero", "of=/dev/null", "bs=64k"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            result = endpoint._monitor_transfer_progress(
                {"send": mover, "receive": mover},
                start_time=time.time(),
                dest_path="/backup",
                snapshot_name="snap",
                max_wait_time=3,  # the WALL CLOCK ends it, not the stall check
            )
        finally:
            if mover.poll() is None:
                mover.kill()
            mover.wait()

        reason = endpoint._last_transfer_error or ""
        assert "moved no data" not in reason, (
            f"a transfer that was actively moving data was called stalled: {reason}"
        )
        assert result is False  # it hit the wall clock, which is a different thing


class TestTheWallClockIsConfigurableAndGenerous:
    """The other half of #93: the limit itself.

    A fixed 3600 lived in eight places. 36 GB over 100 Mbit is about 50 minutes
    at line rate before any overhead, so the cap fired on transfers that were
    working perfectly. Raising it is only safe because a transfer that stops
    moving is now caught in minutes by the stall check -- otherwise a generous
    wall clock would just mean waiting a day on a dead pipe.
    """

    def _config(self, tmp_path, body):
        from btrfs_backup_ng.config.loader import load_config

        path = tmp_path / "config.toml"
        path.write_text(body)
        return load_config(path)

    def test_the_default_is_generous_enough_for_a_large_first_sync(self):
        from btrfs_backup_ng.core.transfer import DEFAULT_TRANSFER_TIMEOUT

        # 36 GB over 100 Mbit ~ 50 min at line rate; a 500 GB first sync ~ 11 h.
        assert DEFAULT_TRANSFER_TIMEOUT >= 12 * 3600, (
            "the default cannot accommodate a large first sync over a slow link, "
            "which is the failure reported in #93"
        )

    def test_it_can_be_set_in_the_config(self, tmp_path):
        config, warnings = self._config(
            tmp_path,
            "[global]\ntransfer_timeout = 43200\n\n"
            '[[volumes]]\npath = "/home"\n\n'
            '[[volumes.targets]]\npath = "ssh://nas:/backup"\n',
        )
        assert not [w for w in warnings if "Unknown config key" in w]
        assert config.global_config.transfer_timeout == 43200

    def test_it_defaults_when_absent(self, tmp_path):
        from btrfs_backup_ng.core.transfer import DEFAULT_TRANSFER_TIMEOUT

        config, _ = self._config(
            tmp_path,
            '[[volumes]]\npath = "/home"\n\n'
            '[[volumes.targets]]\npath = "ssh://nas:/backup"\n',
        )
        assert config.global_config.transfer_timeout == DEFAULT_TRANSFER_TIMEOUT

    def test_no_transfer_path_still_hardcodes_an_hour(self):
        """It was eight places, and mjg's patch could only reach one of them.

        Anything that waits on a transfer must take the configured value, so
        raising it cannot miss a site.
        """
        import inspect

        from btrfs_backup_ng.core import operations, transfer
        from btrfs_backup_ng.endpoint import ssh

        offenders = []
        for module in (operations, transfer, ssh):
            for line_no, line in enumerate(inspect.getsource(module).splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                if "3600" in stripped and (
                    "timeout" in stripped.lower() or "max_wait" in stripped.lower()
                ):
                    offenders.append(f"{module.__name__}:{line_no}: {stripped}")
        assert not offenders, (
            "a transfer timeout is still hardcoded:\n  " + "\n  ".join(offenders)
        )


class TestBothMonitorsAreCovered:
    """There are two monitors. A guard in one is a guard for one setup.

    `_do_piped_transfer` reaches `_simple_transfer_monitor`, which had no stall
    check when the other one gained it. This file's sibling already has a class
    named TestEveryTransferStrategyIsCovered for exactly this failure.
    """

    def _endpoint(self):
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        endpoint = SSHEndpoint(hostname="nas", path="/backup")
        endpoint._last_transfer_error = None
        return endpoint

    def test_the_simple_monitor_catches_a_stall(self, monkeypatch):
        import btrfs_backup_ng.endpoint.ssh as ssh_mod

        monkeypatch.setattr(ssh_mod, "STALL_TIMEOUT_SECONDS", 1)
        endpoint = self._endpoint()
        idle = [subprocess.Popen(["sleep", "30"]) for _ in range(2)]
        try:
            result = endpoint._simple_transfer_monitor(
                {"send": idle[0], "receive": idle[1]},
                start_time=time.time(),
                dest_path="/backup",
                snapshot_name="snap",
                max_wait_time=600,
            )
        finally:
            for proc in idle:
                if proc.poll() is None:
                    proc.kill()
                proc.wait()
        assert result is False, "the simple monitor let a stalled transfer run"
        assert "moved no data" in (endpoint._last_transfer_error or "")

    def test_the_simple_monitor_does_not_kill_a_moving_transfer(self, monkeypatch):
        import btrfs_backup_ng.endpoint.ssh as ssh_mod

        monkeypatch.setattr(ssh_mod, "STALL_TIMEOUT_SECONDS", 1)
        endpoint = self._endpoint()
        mover = subprocess.Popen(
            ["dd", "if=/dev/zero", "of=/dev/null", "bs=64k"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            endpoint._simple_transfer_monitor(
                {"send": mover, "receive": mover},
                start_time=time.time(),
                dest_path="/backup",
                snapshot_name="snap",
                max_wait_time=3,
            )
        finally:
            if mover.poll() is None:
                mover.kill()
            mover.wait()
        assert "moved no data" not in (endpoint._last_transfer_error or "")

    def test_the_simple_monitor_exempts_a_finished_send(self, monkeypatch):
        import btrfs_backup_ng.endpoint.ssh as ssh_mod

        monkeypatch.setattr(ssh_mod, "STALL_TIMEOUT_SECONDS", 1)
        endpoint = self._endpoint()
        done = subprocess.Popen(["true"])
        done.wait()
        idle = subprocess.Popen(["sleep", "4"])
        try:
            endpoint._simple_transfer_monitor(
                {"send": done, "receive": idle},
                start_time=time.time(),
                dest_path="/backup",
                snapshot_name="snap",
                max_wait_time=3,
            )
        finally:
            if idle.poll() is None:
                idle.kill()
            idle.wait()
        assert "moved no data" not in (endpoint._last_transfer_error or "")


class TestTheStallWindowIsReachable:
    """A config key that loads and then does nothing is this project's signature
    defect. The stall window nearly shipped as one: it was added to the schema
    and the loader, and dropped by the endpoint's config whitelist.
    """

    def test_the_loader_accepts_it(self, tmp_path):
        from btrfs_backup_ng.config.loader import load_config

        path = tmp_path / "config.toml"
        path.write_text(
            "[global]\ntransfer_stall_timeout = 120\n\n"
            '[[volumes]]\npath = "/home"\n\n'
            '[[volumes.targets]]\npath = "ssh://nas:/backup"\n'
        )
        config, warnings = load_config(path)
        assert not [w for w in warnings if "Unknown config key" in w]
        assert config.global_config.transfer_stall_timeout == 120

    def test_it_survives_the_endpoint_config_whitelist(self):
        """The step that was missing: accepted by the loader, dropped here."""
        from btrfs_backup_ng.endpoint import choose_endpoint

        endpoint = choose_endpoint(
            "ssh://u@h/p",
            {"path": "ssh://u@h/p", "snap_prefix": "", "transfer_stall_timeout": 60},
        )
        assert endpoint.config.get("transfer_stall_timeout") == 60, (
            "the endpoint whitelist dropped the stall window, so configuring it "
            "would silently do nothing"
        )

    def test_zero_disables_the_check(self, monkeypatch):
        """The escape hatch. Someone hitting a false positive must not have to
        edit the source to get their backup through."""
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        endpoint = SSHEndpoint(hostname="nas", path="/backup", transfer_stall_timeout=0)
        endpoint._last_transfer_error = None
        idle = [subprocess.Popen(["sleep", "3"]) for _ in range(2)]
        try:
            endpoint._monitor_transfer_progress(
                {"send": idle[0], "receive": idle[1]},
                start_time=time.time(),
                dest_path="/backup",
                snapshot_name="snap",
                max_wait_time=2,
            )
        finally:
            for proc in idle:
                if proc.poll() is None:
                    proc.kill()
                proc.wait()
        assert "moved no data" not in (endpoint._last_transfer_error or ""), (
            "the check fired despite being disabled"
        )

    def test_a_configured_window_is_actually_used(self):
        """Not just stored: the monitor must read it rather than the default."""
        from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

        endpoint = SSHEndpoint(hostname="nas", path="/backup", transfer_stall_timeout=1)
        endpoint._last_transfer_error = None
        idle = [subprocess.Popen(["sleep", "30"]) for _ in range(2)]
        try:
            result = endpoint._monitor_transfer_progress(
                {"send": idle[0], "receive": idle[1]},
                start_time=time.time(),
                dest_path="/backup",
                snapshot_name="snap",
                max_wait_time=600,
            )
        finally:
            for proc in idle:
                if proc.poll() is None:
                    proc.kill()
                proc.wait()
        assert result is False
        assert "moved no data" in (endpoint._last_transfer_error or ""), (
            "a 1-second window was configured but the default was used instead"
        )
