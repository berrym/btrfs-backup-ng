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
