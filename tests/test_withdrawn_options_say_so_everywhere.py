"""A withdrawn or unimplemented option says so in every place it is described.

``--overwrite`` was withdrawn in 0.9.6 and the CLI help said so; the man page
kept saying "Overwrite existing snapshots at destination instead of skipping
them" through 0.9.8. ``--in-place`` was documented as a disaster-recovery
strategy while doing nothing. The help text, the man page and the README's
options table are three descriptions of one flag, and a reader may meet any
one of them first. This pins them to each other for the options whose truth
is "does not do what its name says".
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

ROOT = Path(__file__).resolve().parent.parent
MAN = ROOT / "man" / "man1" / "btrfs-backup-ng-restore.1"
README = ROOT / "README.md"

#: flag -> the phrase every description of it must carry
WITHDRAWN = {
    "--overwrite": "not supported",
    "--in-place": "not implemented",
}


def _restore_help(flag: str) -> str:
    parser = create_subcommand_parser()
    sub = next(
        a
        for a in parser._actions
        if isinstance(a, type(parser._subparsers._group_actions[0]))
    )
    restore = sub.choices["restore"]
    action = next(a for a in restore._actions if flag in a.option_strings)
    return action.help or ""


def _man_entry(flag: str) -> str:
    text = MAN.read_text(encoding="utf-8")
    escaped = "\\-\\-" + flag[2:]  # the roff source writes --in-place as \-\-in-place
    m = re.search(
        r"\.B " + re.escape(escaped) + r"\n(.*?)(?=\n\.TP|\n\.SS|\n\.SH)", text, re.S
    )
    assert m, f"{flag} has no entry in {MAN.name}"
    return m.group(1)


def _readme_row(flag: str) -> str:
    for line in README.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"| `{flag}`"):
            return line
    raise AssertionError(f"{flag} has no row in the README options table")


@pytest.mark.parametrize("flag,phrase", sorted(WITHDRAWN.items()))
def test_help_man_and_readme_agree(flag, phrase):
    for where, text in (
        ("argparse help", _restore_help(flag)),
        ("man page", _man_entry(flag)),
        ("README options table", _readme_row(flag)),
    ):
        assert phrase in text.lower(), (
            f"{where} for {flag} does not say '{phrase}': {text!r}"
        )
