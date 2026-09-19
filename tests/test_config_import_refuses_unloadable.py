"""`config import` reported success for a file its own loader rejects.

Two halves, both reproduced on the tree before this change.

The root cause was emission. The btrbk importer built TOML by interpolating
values straight into f-strings -- `lines.append(f'path = "{full_path}"')`, 14
sites -- so any value containing a quote or a backslash produced something other
than what btrbk meant:

    /mnt/da"ta   ->  path = "/mnt/da"ta"    unparseable, loud
    /mnt/a\\backup ->  path = "/mnt/a\\backup"  PARSES, loads as /mnt/a\\x08ackup

The second is the dangerous one. TOML reads \\b as a backspace, so the config
loads cleanly naming a directory the operator never wrote, and a backup tool
then snapshots and prunes that. `_toml_str` already existed in the wizard and
was correct; the importer simply did not use it. It is now one canonical
`__util__.toml_str` shared by both.

The second half was the report. `_import_config` wrote the file and printed
"Configuration written to:" without ever checking, and the wizard's parse
attempt sat inside `except Exception: pass`, gating only a summary table, so an
unparseable conversion was written over the operator's REAL configuration and
the wizard returned 0. Both paths now refuse, and refuse before writing.
"""

from __future__ import annotations

import argparse
import re
import tomllib
from pathlib import Path

import pytest

from btrfs_backup_ng.__util__ import toml_str
from btrfs_backup_ng.btrbk_import import import_btrbk_config
from btrfs_backup_ng.cli import config_cmd

AWKWARD_VALUES = [
    '/mnt/da"ta',
    "/mnt/a\\backup",
    "/mnt/a\\fine",
    "/mnt/a\\next",
    "/mnt/a\\though",
    "/mnt/plain",
    "/mnt/sp ace",
    "/mnt/\x7fdel",
]

# A path containing a space is NOT in the conversion set below. Real btrbk
# 0.32.7 accepts `volume /mnt/sp ace` and keeps the whole path (measured against
# the installed binary: `btrbk config print` dumps `/mnt/sp ace` and exits 0),
# but this project's btrbk LEXER splits the line on whitespace and keeps
# `/mnt/sp`. That is a separate, unfixed defect in the lexer rather than in
# emission, and it is the same silent-wrong-path class as the backslash bug
# above. It is tracked separately; escaping it correctly, which is what the
# round trip above proves, does not repair it.
CONVERTIBLE_VALUES = [v for v in AWKWARD_VALUES if " " not in v]


class TestTheEscaperIsLossless:
    @pytest.mark.parametrize("value", AWKWARD_VALUES)
    def test_round_trip(self, value):
        assert tomllib.loads("p = " + toml_str(value))["p"] == value

    def test_a_backslash_is_not_silently_reinterpreted(self):
        """The corruption case: \\b must survive as two characters, not a backspace."""
        assert (
            tomllib.loads("p = " + toml_str("/mnt/a\\backup"))["p"] == "/mnt/a\\backup"
        )


class TestConversionSurvivesAwkwardPaths:
    @pytest.mark.parametrize("value", CONVERTIBLE_VALUES)
    def test_converted_config_loads_back_with_the_original_path(self, tmp_path, value):
        source = tmp_path / "btrbk.conf"
        source.write_text(f"volume {value}\n  subvolume home\n    target /backup\n")
        content, _ = import_btrbk_config(source)

        parsed = tomllib.loads(content)
        assert parsed["volumes"][0]["path"] == f"{value}/home"


class TestEveryEmitterUsesTheCanonicalHelper:
    """Structural guard: a new raw emitter must not reappear.

    Fixing 14 sites is worth little if the fifteenth is written the old way.
    """

    def test_no_raw_interpolated_toml_strings_remain(self):
        source = Path(config_cmd.__file__).parent.parent / "btrbk_import.py"
        offenders = re.findall(
            r"""lines\.append\(f['"][A-Za-z_]+ = "\{.+?\}"['"]\)""",
            source.read_text(),
        )
        assert offenders == [], (
            "these emit an unescaped TOML string; route them through "
            f"__util__.toml_str: {offenders}"
        )


class TestNeitherPathWritesWhatItCannotLoad:
    BROKEN = "this is not = = valid toml [[[\n"

    def test_unloadable_reason_identifies_broken_content(self):
        assert config_cmd._unloadable_reason(self.BROKEN) is not None

    def test_unloadable_reason_passes_good_content(self):
        good = '[[volumes]]\npath = "/data"\n[[volumes.targets]]\npath = "/backup"\n'
        assert config_cmd._unloadable_reason(good) is None

    def test_unloadable_reason_leaves_no_temp_file_behind(self, tmp_path):
        before = set(Path("/tmp").glob("bbng-verify-*"))
        config_cmd._unloadable_reason(self.BROKEN)
        config_cmd._unloadable_reason("p = 1\n")
        assert set(Path("/tmp").glob("bbng-verify-*")) == before

    def test_import_refuses_and_writes_nothing(self, tmp_path, monkeypatch, capsys):
        import btrfs_backup_ng.btrbk_import as importer

        monkeypatch.setattr(
            importer, "import_btrbk_config", lambda _p: (self.BROKEN, [])
        )
        source = tmp_path / "btrbk.conf"
        source.write_text("volume /mnt/data\n  subvolume home\n    target /backup\n")
        output = tmp_path / "out.toml"

        args = argparse.Namespace(btrbk_config=str(source), output=str(output))
        result = config_cmd._import_config(args)

        assert result == 1, "reported success for a config it cannot load"
        assert not output.exists(), "wrote a file it had just refused"

    def test_import_still_writes_a_good_conversion(self, tmp_path):
        source = tmp_path / "btrbk.conf"
        source.write_text("volume /mnt/data\n  subvolume home\n    target /backup\n")
        output = tmp_path / "out.toml"

        args = argparse.Namespace(btrbk_config=str(source), output=str(output))
        assert config_cmd._import_config(args) == 0
        assert output.exists()
        assert (
            tomllib.loads(output.read_text())["volumes"][0]["path"] == "/mnt/data/home"
        )

    def test_wizard_save_refuses_and_does_not_touch_the_real_config(
        self, tmp_path, monkeypatch
    ):
        """The worst case: overwriting a working config with unparseable content."""
        real = tmp_path / "config.toml"
        real.write_text('[[volumes]]\npath = "/keepme"\n')

        monkeypatch.setattr(config_cmd, "get_default_config_path", lambda: real)
        monkeypatch.setattr(config_cmd, "prompt_choice", lambda *a, **k: "save")
        monkeypatch.setattr(config_cmd, "prompt", lambda *a, **k: str(real))
        monkeypatch.setattr(config_cmd, "prompt_bool", lambda *a, **k: True)

        result = config_cmd._save_wizard_config(self.BROKEN)

        assert result == 1
        assert real.read_text() == '[[volumes]]\npath = "/keepme"\n', (
            "the operator's working configuration was overwritten"
        )
