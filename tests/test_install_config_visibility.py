"""`install` must say when the timer it just wrote cannot find your config.

Config discovery searches, in order: the SUDO_USER's home (only when running as
root via sudo), then the current user's home, then /etc. A SYSTEM systemd unit
runs as root with no SUDO_USER, so it searches /root/.config and /etc -- never
the home of whoever ran `install`.

The trap is that `sudo btrfs-backup-ng install` DOES see the invoking user's
config, because discovery honours SUDO_USER. So the install succeeds, looks
fine, and the timer later fails with "No configuration file found" while the
operator is looking straight at their config file.

The unit deliberately does not get `--config /home/<user>/...` written into it.
A root service reading its configuration from a user-writable path hands whoever
can write that file control over what root backs up and where it sends it. The
fix is to say so at install time and offer the two safe options.
"""

from __future__ import annotations

import argparse
import os
from unittest.mock import patch

from pathlib import Path

import pytest

from btrfs_backup_ng.cli.install import SERVICE_TEMPLATE, _config_visibility_warning


@pytest.fixture
def home_with_config(tmp_path, monkeypatch):
    cfg = tmp_path / ".config" / "btrfs-backup-ng" / "config.toml"
    cfg.parent.mkdir(parents=True)
    cfg.write_text('[[volumes]]\npath = "/home"\n')
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("SUDO_USER", raising=False)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    return cfg


class TestASystemUnitThatCannotSeeTheConfig:
    def test_it_warns(self, home_with_config):
        warning = _config_visibility_warning(user_mode=False)
        assert warning is not None

    def test_it_names_the_config_that_will_be_missed(self, home_with_config):
        warning = _config_visibility_warning(user_mode=False)
        assert str(home_with_config) in warning

    def test_it_predicts_the_exact_failure(self, home_with_config):
        """So the operator recognises it when the timer fails."""
        warning = _config_visibility_warning(user_mode=False)
        assert "No configuration file found" in warning

    def test_it_offers_both_safe_routes(self, home_with_config):
        warning = _config_visibility_warning(user_mode=False)
        assert "/etc/btrfs-backup-ng/config.toml" in warning
        assert "--user" in warning

    def test_it_explains_why_the_path_is_not_simply_baked_in(self, home_with_config):
        """Otherwise the obvious 'fix' -- point the unit at the home config --
        looks like an oversight rather than a refusal."""
        warning = _config_visibility_warning(user_mode=False)
        assert "user-writable" in warning or "other users" in warning


class TestWhenThereIsNothingToWarnAbout:
    def test_a_user_unit_is_silent(self, home_with_config):
        """A --user timer runs as the user, so it finds ~/.config fine."""
        assert _config_visibility_warning(user_mode=True) is None

    def test_a_system_config_is_silent(self, tmp_path, monkeypatch):
        """Config already at /etc: the system unit will read it."""
        from btrfs_backup_ng.cli import install as install_mod

        monkeypatch.setattr(
            install_mod,
            "find_config_file",
            lambda _=None: Path("/etc/btrfs-backup-ng/config.toml"),
            raising=False,
        )
        import btrfs_backup_ng.config as config_mod

        monkeypatch.setattr(
            config_mod,
            "find_config_file",
            lambda _=None: Path("/etc/btrfs-backup-ng/config.toml"),
        )
        assert _config_visibility_warning(user_mode=False) is None

    def test_no_config_at_all_still_says_something_useful(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("SUDO_USER", raising=False)
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        import btrfs_backup_ng.config as config_mod

        monkeypatch.setattr(config_mod, "find_config_file", lambda _=None: None)
        warning = _config_visibility_warning(user_mode=False)
        assert warning and "No configuration file was found" in warning

    def test_a_discovery_failure_never_breaks_the_install(self, monkeypatch):
        import btrfs_backup_ng.config as config_mod

        def boom(_=None):
            raise RuntimeError("nope")

        monkeypatch.setattr(config_mod, "find_config_file", boom)
        assert _config_visibility_warning(user_mode=False) is None


class TestTheUnitDoesNotPointAtAHomeDirectory:
    def test_the_service_template_has_no_config_flag(self):
        """Pinning the security decision: if someone later adds
        `--config {config_path}` here, this fails and they have to read why."""
        assert "--config" not in SERVICE_TEMPLATE
        assert "{exec_start} run" in SERVICE_TEMPLATE


class TestARootTimerDoesNotRunAUserWritableBinary:
    """The unit's executable is the stronger version of the config warning.

    This module already refuses to point a root service at a config in a home
    directory, because a root service must not take its input from a path other
    users may write. The ExecStart path was resolved through PATH, so a
    `--user`/pipx/uv install put `~/.local/bin/btrfs-backup-ng` into a SYSTEM
    unit: whoever can write that file decides what root runs, every night.
    """

    def _warning(self, path, user_mode=False):
        from btrfs_backup_ng.cli.install import _exec_start_trust_warning

        return _exec_start_trust_warning(str(path), user_mode)

    def test_a_home_directory_binary_is_flagged_for_a_system_unit(self, tmp_path):
        binary = tmp_path / "btrfs-backup-ng"
        binary.write_text("#!/bin/sh\n")
        binary.chmod(0o755)
        with patch("btrfs_backup_ng.cli.install.Path.home", return_value=tmp_path):
            warning = self._warning(binary)
        assert warning is not None
        assert str(binary) in warning
        assert "--user" in warning

    def test_a_world_writable_binary_is_flagged(self, tmp_path):
        binary = tmp_path / "btrfs-backup-ng"
        binary.write_text("#!/bin/sh\n")
        binary.chmod(0o777)
        assert self._warning(binary) is not None

    def test_a_user_unit_is_not_flagged(self, tmp_path):
        """A user timer runs as that user; there is no trust boundary to cross."""
        binary = tmp_path / "btrfs-backup-ng"
        binary.write_text("#!/bin/sh\n")
        binary.chmod(0o755)
        assert self._warning(binary, user_mode=True) is None

    def test_a_root_owned_system_path_is_not_flagged(self):
        assert self._warning("/usr/bin/env") is None

    def test_a_missing_binary_does_not_break_the_install(self, tmp_path):
        assert self._warning(tmp_path / "does-not-exist") is None


class TestTheDirectoryDecidesWhoCanReplaceTheBinary:
    """Checking the file's own mode answers a question nobody asked.

    Replacing an executable is unlink plus create -- a write to the DIRECTORY,
    not to the file. A root-owned 0755 binary in a directory an ordinary user
    can write is still that user's to choose, and the check passed it.
    """

    def _ancestor(self, path):
        from btrfs_backup_ng.cli.install import _untrusted_ancestor

        return _untrusted_ancestor(Path(path))

    def test_a_root_owned_system_chain_is_clean(self):
        assert self._ancestor("/usr/bin/env") is None

    def test_a_user_owned_directory_is_named(self, tmp_path):
        binary = tmp_path / "btrfs-backup-ng"
        binary.write_text("#!/bin/sh\n")
        reason = self._ancestor(binary)
        assert reason is not None
        assert str(tmp_path) in reason

    def test_a_sticky_directory_is_not_itself_a_way_in(self):
        """/tmp is 1777; sticky is precisely what stops one user removing
        another's files, so it must not be reported as replaceable."""
        assert self._ancestor("/tmp/btrfs-backup-ng") is None

    @pytest.mark.skipif(os.geteuid() == 0, reason="running as root sees no EACCES")
    def test_an_unreadable_directory_is_reported_not_skipped(self, tmp_path):
        # The blocked directory itself still stats fine; it is TRAVERSAL that
        # is denied, so the unexaminable path has to be one level further in.
        blocked = tmp_path / "blocked"
        inner = blocked / "inner"
        inner.mkdir(parents=True)
        binary = inner / "btrfs-backup-ng"
        binary.write_text("#!/bin/sh\n")
        blocked.chmod(0o000)
        try:
            reason = self._ancestor(binary)
        finally:
            blocked.chmod(0o755)
        assert reason is not None
        assert "could not be examined" in reason, reason

    def test_a_world_writable_root_directory_is_flagged(self, tmp_path, monkeypatch):
        """The case that cannot be built without root: a root-owned binary in a
        root-owned but world-writable, non-sticky directory. Only the filesystem
        metadata is substituted; the decision under test is the real one."""
        import os as os_mod

        from btrfs_backup_ng.cli import install as install_mod

        binary = tmp_path / "bin" / "btrfs-backup-ng"
        binary.parent.mkdir()
        binary.write_text("#!/bin/sh\n")
        real_stat = os_mod.stat

        class FakeStat:
            def __init__(self, uid, mode):
                self.st_uid = uid
                self.st_mode = mode

        def fake_stat(target, *a, **kw):
            # Every ancestor looks root-owned; the immediate directory is 0777
            # with no sticky bit.
            if str(target) == str(binary.parent):
                return FakeStat(0, 0o777)
            if str(target) in {str(p) for p in binary.parents}:
                return FakeStat(0, 0o755)
            return real_stat(target, *a, **kw)

        monkeypatch.setattr(install_mod.os, "stat", fake_stat)
        reason = install_mod._untrusted_ancestor(binary)
        assert reason is not None, "a world-writable directory was passed as trusted"
        assert str(binary.parent) in reason
        assert "replaced" in reason

    def test_the_warning_names_the_directory(self, tmp_path):
        """End to end: the reason reaches the operator, not just the helper."""
        from btrfs_backup_ng.cli.install import _exec_start_trust_warning

        binary = tmp_path / "btrfs-backup-ng"
        binary.write_text("#!/bin/sh\n")
        binary.chmod(0o755)
        warning = _exec_start_trust_warning(str(binary), user_mode=False)
        assert warning is not None
        # Assert on the REASON clause, not merely on the directory name: the
        # message echoes the full binary path, which contains the directory, so
        # a substring test for the directory alone passes even when the
        # directory was never examined.
        assert f"its directory {tmp_path} is owned by uid" in warning, warning


class TestAFailedTrustCheckIsNotAPass:
    """The recurring defect: a check that could not run, reported as clean."""

    @pytest.mark.skipif(os.geteuid() == 0, reason="running as root sees no EACCES")
    def test_an_unreadable_binary_says_it_was_not_checked(self, tmp_path):
        from btrfs_backup_ng.cli.install import _exec_start_trust_warning

        blocked = tmp_path / "blocked"
        blocked.mkdir()
        binary = blocked / "btrfs-backup-ng"
        binary.write_text("#!/bin/sh\n")
        blocked.chmod(0o000)
        try:
            warning = _exec_start_trust_warning(str(binary), user_mode=False)
        finally:
            blocked.chmod(0o755)
        assert warning is not None, "an unexaminable binary was reported as trusted"
        assert "NOT checked" in warning


class TestAMissingExecutableIsReportedAtInstallTime:
    """_resolve_exec_start falls back to /usr/bin/btrfs-backup-ng when the binary
    is not on PATH, so a valid-looking unit can be written aimed at nothing. The
    failure otherwise surfaces at the first timer run, in a status nobody reads.
    """

    def _warning(self, path, user_mode=False):
        from btrfs_backup_ng.cli.install import _exec_start_missing_warning

        return _exec_start_missing_warning(str(path), user_mode)

    def test_a_missing_binary_is_reported(self, tmp_path):
        warning = self._warning(tmp_path / "does-not-exist")
        assert warning is not None
        assert "does not exist" in warning

    def test_a_user_unit_is_reported_too(self, tmp_path):
        """A user timer pointed at a missing binary fails just as completely."""
        assert self._warning(tmp_path / "does-not-exist", user_mode=True) is not None

    def test_an_existing_binary_is_not_reported(self):
        assert self._warning("/usr/bin/env") is None

    def test_the_install_still_succeeds_and_says_so(self, tmp_path, capsys):
        """Warning, not failing: the unit is still written."""
        import btrfs_backup_ng.cli.install as install_mod

        args = argparse.Namespace(timer="daily", user=True, verbose=0, quiet=False)
        with (
            patch.object(install_mod.Path, "home", return_value=tmp_path),
            patch.object(
                install_mod, "_resolve_exec_start", return_value="/nonexistent/bbng"
            ),
        ):
            rc = install_mod.execute_install(args)
        out = capsys.readouterr().out
        assert rc == 0
        assert "does not exist" in out
        assert "/nonexistent/bbng" in out
