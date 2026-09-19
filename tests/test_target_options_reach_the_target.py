"""A documented per-target option must survive the loader.

`skip_remote_lock` was declared on TargetConfig and read by the endpoint, but
`_parse_target` built the dataclass field by field and never read it from the
TOML, so a user who set it got the default and no warning: the option existed
everywhere except in the one place that had to carry it.
"""

import dataclasses
import inspect
import re

import pytest

from btrfs_backup_ng.config import loader, schema
from btrfs_backup_ng.config.loader import ConfigError, load_config

BODY = """
[[volumes]]
path = "/srv/data"
[[volumes.targets]]
path = "raw:///mnt/ro-archive"
"""


def _target(tmp_path, extra=""):
    path = tmp_path / "config.toml"
    path.write_text(BODY + extra)
    return load_config(path)[0].volumes[0].targets[0]


def test_the_option_the_user_wrote_is_the_option_the_target_gets(tmp_path):
    assert _target(tmp_path, "skip_remote_lock = true\n").skip_remote_lock is True


def test_it_defaults_off_when_absent(tmp_path):
    assert _target(tmp_path).skip_remote_lock is False


def test_a_quoted_false_does_not_read_as_true(tmp_path):
    """Plain truthiness makes "false" a True string -- the require_mount trap."""
    assert _target(tmp_path, 'skip_remote_lock = "false"\n').skip_remote_lock is False


def test_an_unreadable_value_is_refused_not_guessed(tmp_path):
    with pytest.raises(ConfigError, match="skip_remote_lock"):
        _target(tmp_path, 'skip_remote_lock = "maybe"\n')


def test_every_declared_target_field_is_parsed():
    """Structural guard: adding a field to TargetConfig without teaching
    _parse_target to read it silently discards whatever the user wrote."""
    source = inspect.getsource(loader._parse_target)
    dropped = [
        f.name
        for f in dataclasses.fields(schema.TargetConfig)
        if not re.search(rf"^\s*{f.name}\s*=", source, re.M)
    ]
    assert not dropped, (
        f"TargetConfig field(s) never assigned in _parse_target: {dropped}. "
        "A user setting one of these gets the default and no warning."
    )
