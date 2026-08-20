"""A restore must be able to read what the matching backup wrote.

A restore names its source as a URI, so nothing tied it back to the target entry
that produced those backups: the SSH key lived in the config file and only
``--ssh-*`` flags were consulted.  Backups written happily for months could not
be listed, and passing ``--ssh-key`` by hand was the only way through.
"""

from __future__ import annotations

import argparse
from unittest.mock import patch
import contextlib

import pytest

from btrfs_backup_ng.cli.restore import _configured_target_options

CONFIG = """
[[volumes]]
path = "/data"
snapshot_dir = "/data/.snapshots"

[[volumes.targets]]
path = "raw+ssh://user@host:/backups"
ssh_key = "/keys/id_ed25519"
ssh_port = 2222
compress = "zstd"

[[volumes.targets]]
path = "ssh://other@elsewhere:/vault"
ssh_key = "/keys/other"
"""


@pytest.fixture
def config_file(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(CONFIG, encoding="utf-8")
    return path


def _args(config_file, **extra):
    return argparse.Namespace(config=str(config_file), **extra)


def test_the_matching_target_supplies_its_ssh_key(config_file) -> None:
    found = _configured_target_options(
        _args(config_file), "raw+ssh://user@host:/backups"
    )
    assert found["ssh_key"] == "/keys/id_ed25519"
    assert found["ssh_port"] == 2222


def test_the_right_target_is_chosen_when_several_are_configured(config_file) -> None:
    """Picking the wrong entry would hand over a key that cannot open the source."""
    found = _configured_target_options(
        _args(config_file), "ssh://other@elsewhere:/vault"
    )
    assert found["ssh_key"] == "/keys/other"


def test_a_trailing_slash_still_matches(config_file) -> None:
    found = _configured_target_options(
        _args(config_file), "raw+ssh://user@host:/backups/"
    )
    assert found["ssh_key"] == "/keys/id_ed25519"


def test_an_unrelated_source_contributes_nothing(config_file) -> None:
    """No match must not mean 'borrow whatever key was lying around'."""
    assert _configured_target_options(_args(config_file), "raw://somewhere/else") == {}


def test_only_transport_options_are_returned(config_file) -> None:
    """``compress`` describes how to write, and must not be mistaken for an option
    the restore path should act on."""
    found = _configured_target_options(
        _args(config_file), "raw+ssh://user@host:/backups"
    )
    assert "compress" not in found


def test_no_config_file_is_not_an_error() -> None:
    """A convenience lookup must never be the reason a restore cannot start."""
    args = argparse.Namespace(config="/nonexistent/nowhere.toml")
    assert _configured_target_options(args, "raw+ssh://user@host:/backups") == {}


def test_an_unreadable_config_is_not_an_error(tmp_path) -> None:
    broken = tmp_path / "broken.toml"
    broken.write_text("this is not = valid toml [[[", encoding="utf-8")
    args = argparse.Namespace(config=str(broken))
    assert _configured_target_options(args, "raw+ssh://user@host:/backups") == {}


def test_missing_config_attribute_is_not_an_error() -> None:
    assert _configured_target_options(argparse.Namespace(), "raw+ssh://h:/b") == {}


def test_schema_defaults_are_not_reported_as_configured(tmp_path) -> None:
    """A local restore must not announce SSH settings the user never wrote.

    Every target carries schema defaults (``ssh_port = 22`` and friends).  Taking
    those for configuration made a purely local restore log that it was "using
    the SSH options configured for this target".
    """
    config = tmp_path / "config.toml"
    config.write_text(
        """
[[volumes]]
path = "/data"
snapshot_dir = "/data/.snapshots"

[[volumes.targets]]
path = "/backups"
""",
        encoding="utf-8",
    )
    assert _configured_target_options(_args(config), "/backups") == {}


def test_an_explicitly_set_value_is_kept_even_if_it_equals_nothing_special(
    tmp_path,
) -> None:
    """Filtering defaults must not swallow a real setting."""
    config = tmp_path / "config.toml"
    config.write_text(
        """
[[volumes]]
path = "/data"
snapshot_dir = "/data/.snapshots"

[[volumes.targets]]
path = "ssh://user@host:/backups"
ssh_key = "/keys/id_ed25519"
ssh_sudo = true
ssh_port = 2222
""",
        encoding="utf-8",
    )
    found = _configured_target_options(_args(config), "ssh://user@host:/backups")
    assert found == {
        "ssh_key": "/keys/id_ed25519",
        "ssh_sudo": True,
        "ssh_port": 2222,
    }


class TestTheOptionsReachTheEndpointNotJustTheDict:
    """Collected is not the same as applied.

    The configured ssh_port was gathered, logged to the operator as an option
    being used, and threaded under the config's own spelling -- which the
    endpoint's key whitelist drops. A restore against a remote on a
    non-standard port therefore connected to 22 while reporting that the
    target's port had been applied. cli/common.py maps it to `port` for the
    backup path and carries a comment about this exact bug being fixed there;
    the restore path reintroduced it.
    """

    def _endpoint_kwargs(self, tmp_path, port):
        """Capture what _prepare_backup_endpoint hands to choose_endpoint."""
        from btrfs_backup_ng.cli import restore as restore_cli

        config = tmp_path / "config.toml"
        config.write_text(
            f'[[volumes]]\npath = "/data"\nsnapshot_dir = "/data/.snapshots"\n\n'
            f'[[volumes.targets]]\npath = "ssh://user@host:/backups"\n'
            f"ssh_port = {port}\n",
            encoding="utf-8",
        )
        captured = {}

        def fake_choose(uri, cfg, *a, **k):
            captured.update(cfg)
            raise RuntimeError("stop here")

        args = argparse.Namespace(config=str(config))
        # restore.py calls it as `endpoint.choose_endpoint`, so patch it there.
        with patch.object(restore_cli.endpoint, "choose_endpoint", fake_choose):
            with contextlib.suppress(Exception):
                restore_cli._prepare_backup_endpoint(args, "ssh://user@host:/backups")
        return captured

    def test_the_configured_port_is_threaded_under_the_key_endpoints_read(
        self, tmp_path
    ):
        kwargs = self._endpoint_kwargs(tmp_path, 2222)
        assert kwargs.get("port") == 2222, (
            f"port not threaded; got keys {sorted(kwargs)}"
        )

    def test_it_actually_reaches_a_real_endpoint(self, tmp_path):
        """End to end through choose_endpoint, not just the kwargs dict."""
        from btrfs_backup_ng.endpoint import choose_endpoint

        kwargs = self._endpoint_kwargs(tmp_path, 2222)
        uri = "ssh://user@host:/backups"
        endpoint = choose_endpoint(uri, {**kwargs, "path": uri, "snap_prefix": ""})
        assert endpoint.config.get("port") == 2222, (
            "the endpoint fell back to the default port"
        )

    def test_a_port_in_the_url_still_wins(self):
        from btrfs_backup_ng.endpoint import choose_endpoint

        uri = "ssh://user@host:2200/backups"
        endpoint = choose_endpoint(uri, {"path": uri, "snap_prefix": "", "port": 2222})
        assert endpoint.config.get("port") == 2200
