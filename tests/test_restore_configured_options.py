"""A configured option must reach the restore that needs it.

`restore` names its source as a URI, so nothing tied it back to the target
entry that produced those backups; the options lived in the config file and
only --ssh-* flags were consulted. _configured_target_options closed that gap.
This covers what it must and must not carry: a setting the operator actually
wrote, never a schema default restated as though it were a decision.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch


class TestAConfiguredPasswordAuthChoiceReachesTheRestore:
    """`ssh_password_auth = false` was inert for restore, and only for restore.

    The option could not be returned under any value it could hold: False was
    dropped by a falsy guard as though it were unset, and True was dropped as
    "same as the schema default". Even had it survived, the endpoint kwarg was
    read from an argparse attribute that no flag defines -- a constant True.
    The same setting was honoured for backups, so a target configured to refuse
    password authentication refused it when writing the backup and offered it
    when reading the backup back.
    """

    def _options(self, tmp_path, setting):
        """A real config file through the real loader -- no stand-in for the
        thing under test, since the defect lived in exactly that path."""
        import argparse

        from btrfs_backup_ng.cli.restore import _configured_target_options

        config_file = tmp_path / "config.toml"
        config_file.write_text(
            "[[volumes]]\n"
            'path = "/home"\n'
            "\n"
            "[[volumes.targets]]\n"
            'path = "ssh://nas/backup"\n'
            f"{setting}\n"
        )
        args = argparse.Namespace(config=str(config_file))
        return _configured_target_options(args, "ssh://nas/backup")

    def test_an_explicit_false_is_carried(self, tmp_path):
        options = self._options(tmp_path, "ssh_password_auth = false")
        assert options.get("ssh_password_auth") is False, options

    def test_the_default_true_is_not_reported_as_a_choice(self, tmp_path):
        """Schema defaults are not decisions the operator made."""
        options = self._options(tmp_path, "ssh_password_auth = true")
        assert "ssh_password_auth" not in options, options

    def test_an_explicit_false_sudo_still_matches_the_default(self, tmp_path):
        """Guard the blast radius: ssh_sudo defaults to False, so an explicit
        false is still not a choice worth reporting, and this change must not
        start reporting one."""
        options = self._options(tmp_path, "ssh_sudo = false")
        assert "ssh_sudo" not in options, options

    def test_the_endpoint_is_told_not_to_fall_back(self, tmp_path):
        """End to end: the setting has to reach the endpoint kwargs, which is
        where it was read from an argparse attribute no flag ever defines."""
        import argparse

        from btrfs_backup_ng.cli.restore import _prepare_backup_endpoint

        config_file = tmp_path / "config.toml"
        config_file.write_text(
            "[[volumes]]\n"
            'path = "/home"\n'
            "\n"
            "[[volumes.targets]]\n"
            'path = "ssh://nas/backup"\n'
            "ssh_password_auth = false\n"
        )
        args = argparse.Namespace(config=str(config_file), prefix="")
        captured = {}

        def fake_choose(_source, kwargs, *_a, **_kw):
            # choose_endpoint takes the kwargs dict POSITIONALLY; capturing
            # **kwargs here would have recorded an empty result and passed.
            captured.update(kwargs)
            return SimpleNamespace(config={}, prepare=lambda: None)

        with patch("btrfs_backup_ng.cli.restore.endpoint.choose_endpoint", fake_choose):
            _prepare_backup_endpoint(args, "ssh://nas/backup")
        assert captured.get("ssh_password_fallback") is False, captured

    def test_the_fallback_stays_on_when_nothing_disables_it(self, tmp_path):
        import argparse

        from btrfs_backup_ng.cli.restore import _prepare_backup_endpoint

        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[[volumes]]\npath = "/home"\n\n'
            '[[volumes.targets]]\npath = "ssh://nas/backup"\n'
        )
        args = argparse.Namespace(config=str(config_file), prefix="")
        captured = {}

        def fake_choose(_source, kwargs, *_a, **_kw):
            # choose_endpoint takes the kwargs dict POSITIONALLY; capturing
            # **kwargs here would have recorded an empty result and passed.
            captured.update(kwargs)
            return SimpleNamespace(config={}, prepare=lambda: None)

        with patch("btrfs_backup_ng.cli.restore.endpoint.choose_endpoint", fake_choose):
            _prepare_backup_endpoint(args, "ssh://nas/backup")
        assert captured.get("ssh_password_fallback") is True, captured
