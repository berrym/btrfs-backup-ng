"""A restore must be able to read what the matching backup wrote.

A restore names its source as a URI, so nothing tied it back to the target entry
that produced those backups: the SSH key lived in the config file and only
``--ssh-*`` flags were consulted.  Backups written happily for months could not
be listed, and passing ``--ssh-key`` by hand was the only way through.
"""

from __future__ import annotations

import argparse

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
