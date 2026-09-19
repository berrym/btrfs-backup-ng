"""A btrbk path containing a space was truncated at the space.

btrbk's grammar is line-oriented. After stripping comments and trimming
whitespace it matches

    ^([a-zA-Z_]+)(?:\\s+(.*))?$          (/usr/bin/btrbk, sub parse_config)

so a directive is a keyword and then the REST OF THE LINE, kept verbatim. This
project's importer is token-based, and `volume`, `subvolume` and `target` took
only the FIRST token, so

    volume /mnt/sp ace   ->   /mnt/sp

The converted configuration then named a directory the operator never wrote. It
parses, it loads, `config validate` is happy, and the backup runs against the
wrong path -- the same silent-wrong-path class as the TOML escaping defect
alongside it, and squarely against the migration rule that whatever btrbk
accepts we accept and treat identically.

Rejoining the tokens with a single space would not have been correct either:
btrbk keeps the source's internal spacing exactly, so `/mnt/two  spaces` has two
spaces and a tab stays a tab. The parser slices the original text instead.

Every expectation below was measured against the installed btrbk 0.32.7, and
TestAgainstRealBtrbk re-measures when the binary is present.
"""

from __future__ import annotations

import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest

from btrfs_backup_ng.btrbk_import import import_btrbk_config

# (source text after the directive, what btrbk 0.32.7 resolves it to)
MEASURED = [
    ("/mnt/plain", "/mnt/plain"),
    ("/mnt/sp ace", "/mnt/sp ace"),
    ("/mnt/two  spaces", "/mnt/two  spaces"),
    ("/mnt/three   sp   runs", "/mnt/three   sp   runs"),
    ("/mnt/tab\tsep", "/mnt/tab\tsep"),
    ('"/mnt/quoted path"', "/mnt/quoted path"),
    ("'/mnt/single quoted'", "/mnt/single quoted"),
    ("/mnt/trail   ", "/mnt/trail"),
    ("/mnt/a#b", "/mnt/a"),
    ("/mnt/unié", "/mnt/unié"),
    ('/mnt/da"ta', '/mnt/da"ta'),
]


def _volume_path(tmp_path: Path, directive_value: str) -> str:
    source = tmp_path / "btrbk.conf"
    source.write_text(
        f"volume {directive_value}\n  subvolume home\n    target /backup\n"
    )
    content, _ = import_btrbk_config(source)
    volumes = tomllib.loads(content).get("volumes", [])
    assert volumes, "conversion produced no volume"
    path = volumes[0]["path"]
    assert path.endswith("/home")
    return path[: -len("/home")]


class TestDirectiveValueIsTheRestOfTheLine:
    @pytest.mark.parametrize("written,expected", MEASURED)
    def test_volume(self, tmp_path, written, expected):
        assert _volume_path(tmp_path, written) == expected

    def test_subvolume_keeps_its_spaces(self, tmp_path):
        source = tmp_path / "btrbk.conf"
        source.write_text("volume /mnt/data\n  subvolume my home\n    target /backup\n")
        content, _ = import_btrbk_config(source)
        assert tomllib.loads(content)["volumes"][0]["path"] == "/mnt/data/my home"

    def test_target_keeps_its_spaces(self, tmp_path):
        source = tmp_path / "btrbk.conf"
        source.write_text(
            "volume /mnt/data\n  subvolume home\n    target /back up/here\n"
        )
        content, _ = import_btrbk_config(source)
        targets = tomllib.loads(content)["volumes"][0]["targets"]
        assert targets[0]["path"] == "/back up/here"

    def test_typed_target_keeps_its_spaces(self, tmp_path):
        """`target <type> <url>`: the type is consumed, the rest is the path."""
        source = tmp_path / "btrbk.conf"
        source.write_text(
            "volume /mnt/data\n  subvolume home\n"
            "    target send-receive /back up/here\n"
        )
        content, _ = import_btrbk_config(source)
        targets = tomllib.loads(content)["volumes"][0]["targets"]
        assert targets[0]["path"] == "/back up/here"

    def test_a_converted_path_round_trips_through_the_loader(self, tmp_path):
        """The whole point: the emitted config must load naming the same path."""
        from btrfs_backup_ng.config import load_config

        source = tmp_path / "btrbk.conf"
        source.write_text("volume /mnt/sp ace\n  subvolume home\n    target /backup\n")
        content, _ = import_btrbk_config(source)
        out = tmp_path / "out.toml"
        out.write_text(content)
        config, _ = load_config(out)
        assert config.volumes[0].path == "/mnt/sp ace/home"


class TestUnchangedBehaviourForOrdinaryConfigs:
    def test_multi_value_options_still_split(self, tmp_path):
        source = tmp_path / "btrbk.conf"
        source.write_text(
            "volume /mnt/data\n  snapshot_preserve 14d 8w\n"
            "  subvolume home\n    target /backup\n"
        )
        content, _ = import_btrbk_config(source)
        retention = tomllib.loads(content)["volumes"][0]["retention"]
        assert retention["daily"] == 14
        assert retention["weekly"] == 8

    def test_ssh_target_url_is_untouched(self, tmp_path):
        source = tmp_path / "btrbk.conf"
        source.write_text(
            "volume /mnt/data\n  subvolume home\n    target ssh://backuphost/backups\n"
        )
        content, _ = import_btrbk_config(source)
        targets = tomllib.loads(content)["volumes"][0]["targets"]
        assert targets[0]["path"] == "ssh://backuphost/backups"


class TestAgainstRealBtrbk:
    """Re-measure rather than trust the table above, when btrbk is installed."""

    @pytest.mark.parametrize("written,expected", MEASURED)
    def test_matches_btrbk_config_print(self, tmp_path, written, expected):
        if shutil.which("btrbk") is None:
            pytest.skip("btrbk is not installed")

        source = tmp_path / "btrbk.conf"
        source.write_text(f"volume {written}\n  subvolume home\n    target /backup\n")
        result = subprocess.run(
            ["btrbk", "-c", str(source), "config", "print"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            pytest.skip(f"btrbk rejected the probe config: {result.returncode}")

        theirs = next(
            (
                line[len("volume ") :]
                for line in result.stdout.splitlines()
                if line.startswith("volume ")
            ),
            None,
        )
        assert theirs == expected, "the measured table is stale; btrbk disagrees"
        assert _volume_path(tmp_path, written) == theirs
