"""Regression guard: every example command in the user-facing docs must parse.

Extracts `btrfs-backup-ng ...` commands from fenced code blocks in README.md and
docs/SNAPPER-INTEGRATION.md and asserts each parses against the real CLI parser, so a
doc example can never drift into an unrecognized-flag / wrong-positional form that would
fail the moment a user copy-pastes it (as `snapper restore --config NAME` once did).
"""

import contextlib
import io
import re
import shlex
from pathlib import Path

import pytest

from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

_REPO = Path(__file__).resolve().parent.parent
_DOCS = [_REPO / "README.md", _REPO / "docs" / "SNAPPER-INTEGRATION.md"]

# The top-level parser rejects the bare legacy `SRC DST` form (a shim in main() handles
# it) and output banners / prose; those first tokens are neither a subcommand nor a flag,
# so the "first arg is a subcommand or a global flag" filter below skips them cleanly.
_SUBCOMMANDS = {
    "run",
    "snapshot",
    "transfer",
    "prune",
    "list",
    "status",
    "config",
    "raw",
    "install",
    "uninstall",
    "restore",
    "verify",
    "completions",
    "manpages",
    "estimate",
    "transfers",
    "doctor",
    "snapper",
}


def _iter_doc_commands(path: Path):
    """Yield (path, lineno, command) for btrfs-backup-ng commands in fenced blocks."""
    lines = path.read_text().splitlines()
    in_block = False
    i = 0
    while i < len(lines):
        ln = lines[i]
        if ln.strip().startswith("```"):
            in_block = not in_block
            i += 1
            continue
        if in_block:
            joined = ln
            while joined.rstrip().endswith("\\") and i + 1 < len(lines):
                i += 1
                joined = joined.rstrip()[:-1] + " " + lines[i]
            s = joined.strip()
            if s.startswith("$ "):
                s = s[2:].strip()
            if s.startswith("sudo "):
                s = s[5:].strip()
            s = re.sub(r"\s+#.*$", "", s)  # strip trailing inline comment
            if s.startswith("btrfs-backup-ng"):
                yield path, i + 1, s
        i += 1


def _collect():
    out = []
    for p in _DOCS:
        for path, lineno, cmd in _iter_doc_commands(p):
            # skip shell pipelines/redirects/substitutions we can't treat as one argv
            if any(t in cmd for t in ["|", "$(", "&&", ">", "<", "`"]):
                continue
            try:
                argv = shlex.split(cmd)[1:]
            except ValueError:
                continue
            if not argv:
                continue
            first = argv[0]
            # only real command lines: first token is a subcommand or a global flag
            if not (first.startswith("-") or first in _SUBCOMMANDS):
                continue
            out.append((f"{path.name}:{lineno}", cmd, argv))
    return out


_COMMANDS = _collect()


def test_docs_have_example_commands():
    # guard against the extractor silently finding nothing
    assert len(_COMMANDS) > 30


@pytest.mark.parametrize("loc,cmd,argv", _COMMANDS, ids=[c[0] for c in _COMMANDS])
def test_doc_command_parses(loc, cmd, argv):
    parser = create_subcommand_parser()
    buf = io.StringIO()
    try:
        with contextlib.redirect_stderr(buf), contextlib.redirect_stdout(buf):
            parser.parse_args(argv)
    except SystemExit as e:
        # 0/None = --help/--version (fine); 2 = a real parse error
        if e.code not in (0, None):
            pytest.fail(f"{loc}: `{cmd}` does not parse:\n{buf.getvalue().strip()}")
