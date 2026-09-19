"""Every flag the CLI accepts must appear in its man page.

Man pages are NOT generated. Their prose -- DESCRIPTION, EXAMPLES, INCREMENTAL
CHAINS, RECOVERY FROM FAILURES -- says things an argparse help string cannot,
and regenerating them to win consistency would throw that away. So this asserts
coverage without dictating wording: the flag must be documented, not documented
in any particular way.

It exists because they drifted. An audit before 0.9.7 found 24 flags missing
from their man pages, including `--newest-only`, the escape hatch for that
release's change to `run`'s default. The man pages ship inside the wheel, so
that reached users as authoritative documentation of a tool that behaved
differently.

Note man pages escape hyphens for roff (`\\-\\-compress`). An earlier version of
this audit searched for the literal `--compress`, matched nothing, and reported
everything as documented -- a check that passed because it could not see. The
escaping is normalised before comparing.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from btrfs_backup_ng.cli.completion_gen import build_tree
from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

MAN_DIR = Path(__file__).resolve().parent.parent / "man" / "man1"

# Commands with no man page of their own. Recorded deliberately: a command
# added without documentation should fail below rather than be waved through,
# and this list is the explicit, reviewable exception.
NO_MAN_PAGE = {"completions", "manpages", "transfers", "uninstall"}


def _unroff(text: str) -> str:
    return text.replace("\\-", "-")


def _flags_beneath(node) -> set[str]:
    out = {f for flag in node["flags"] for f in flag["long"] if f != "--help"}
    for sub in node["subcommands"].values():
        out |= _flags_beneath(sub)
    return out


def _commands():
    tree = build_tree(create_subcommand_parser())
    return sorted(tree["subcommands"].items())


@pytest.mark.parametrize(
    "command,node", _commands(), ids=lambda v: v if isinstance(v, str) else ""
)
def test_every_flag_appears_in_the_man_page(command, node):
    if command in NO_MAN_PAGE:
        pytest.skip(f"{command} has no man page by decision; see NO_MAN_PAGE")

    page = MAN_DIR / f"btrfs-backup-ng-{command}.1"
    assert page.exists(), f"{page.name} is missing"

    text = _unroff(page.read_text())
    # A word boundary, not a substring: "--newest-only" occurs inside
    # "--newest-onlyX", so a plain `in` check passes for a flag that has been
    # renamed or mangled. A mutation proved exactly that before this changed.
    missing = sorted(
        f for f in _flags_beneath(node) if not re.search(re.escape(f) + r"\b", text)
    )
    assert not missing, (
        f"btrfs-backup-ng-{command}.1 does not document: {' '.join(missing)}. "
        f"Man pages are hand-written; add an entry describing what the flag does."
    )


def test_the_no_man_page_list_names_only_real_commands():
    """A command that gains a man page must drop off the exception list."""
    known = {name for name, _ in _commands()}
    stale = sorted(NO_MAN_PAGE - known)
    assert not stale, f"NO_MAN_PAGE names commands that no longer exist: {stale}"

    have_pages = sorted(
        c for c in NO_MAN_PAGE if (MAN_DIR / f"btrfs-backup-ng-{c}.1").exists()
    )
    assert not have_pages, (
        f"these now have man pages and should be removed from NO_MAN_PAGE: {have_pages}"
    )


def test_the_escaping_normaliser_actually_works():
    """Guards the bug that made an earlier audit report a false clean result."""
    assert _unroff(r".B \-\-newest-only") == ".B --newest-only"


def test_a_mangled_flag_does_not_count_as_documented():
    r"""The check must be a word boundary, not a substring.

    A mutation renaming \-\-newest-only to \-\-newest-onlyX survived a plain
    `in` check, because the real flag is a prefix of the mangled one.
    """
    text = ".B --newest-onlyX"
    assert re.search(re.escape("--newest-only") + r"\b", text) is None
    assert re.search(re.escape("--newest-onlyX") + r"\b", text) is not None
