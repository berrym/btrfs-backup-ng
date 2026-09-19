"""`-c <config>` must reach the timestamp-format lookup.

`resolve_timestamp_format` passed None to `find_config_file`, which then
DISCOVERED a config from the default locations instead of honouring the file
named on the command line. A `timestamp_format` set in that file was silently
replaced by the default, a pool named under it parsed as nothing, and `restore`
reported the location as holding no snapshots -- while advising a prefix that
did not exist.

`restore` is the command people run when something has already gone wrong, so a
confidently wrong "there is nothing here" is the worst available answer.
"""

import ast
import pathlib

import pytest

from btrfs_backup_ng.cli.common import resolve_timestamp_format

CUSTOM = "%Y%m%dT%H%M%S%z"


@pytest.fixture
def config_file(tmp_path):
    p = tmp_path / "custom.toml"
    p.write_text(
        f'[global]\ntimestamp_format = "{CUSTOM}"\n\n'
        '[[volumes]]\npath = "/srv/data"\n\n'
        '[[volumes.targets]]\npath = "/backup"\n'
    )
    return p


def test_the_named_config_is_the_one_read(config_file):
    assert resolve_timestamp_format(None, str(config_file)) == CUSTOM


def test_an_explicit_flag_still_wins(config_file):
    assert resolve_timestamp_format("%Y%m%d", str(config_file)) == "%Y%m%d"


def test_without_a_config_path_it_falls_back(tmp_path, monkeypatch):
    """No -c and nothing discoverable: the built-in default, not a crash."""
    import btrfs_backup_ng.config as cfg

    # Imported inside the function, so it must be patched at its source.
    monkeypatch.setattr(cfg, "find_config_file", lambda p: None)
    from btrfs_backup_ng.__util__ import DATE_FORMAT

    assert resolve_timestamp_format(None, None) == DATE_FORMAT


def test_an_unreadable_config_does_not_crash_the_command(tmp_path):
    bad = tmp_path / "broken.toml"
    bad.write_text("this is not = valid toml [[[")
    from btrfs_backup_ng.__util__ import DATE_FORMAT

    assert resolve_timestamp_format(None, str(bad)) == DATE_FORMAT


def test_every_call_site_passes_the_config_path():
    """Structural guard: the defect was one layer dropping the value, and a new
    call site added later would reintroduce it silently."""
    missing = []
    for path in sorted(pathlib.Path("src/btrfs_backup_ng/cli").glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "resolve_timestamp_format"
                and len(node.args) + len(node.keywords) < 2
            ):
                missing.append(f"{path.name}:{node.lineno}")

    assert not missing, (
        f"resolve_timestamp_format called without a config path at {missing}; "
        "that command will ignore -c and read a different config"
    )
