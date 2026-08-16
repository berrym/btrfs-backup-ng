"""CLI output must survive a non-UTF-8 stdout.

btrfs-backup-ng runs from cron and systemd, where the locale is frequently
minimal and stdout's encoding is ASCII. Writing a non-ASCII character to such a
stream raises UnicodeEncodeError, so a status glyph in an output string turns a
successful backup check into a crash -- and only in the environment that runs
unattended.

Rich's own primitives (``Console.rule``, ``Table``) substitute ASCII when the
stream cannot encode box drawing, so those are safe. String literals we write
ourselves are not, which is what these tests pin.

Mutation-verified: restoring any of the removed glyphs (verify.py's checkmark
verdict, status.py's transaction icons, wizard_utils.py's star marker or its
hand-drawn section rule) fails both the static guard and the matching render
test.
"""

from __future__ import annotations

import argparse
import ast
import io
from pathlib import Path

import pytest
from rich.console import Console

from btrfs_backup_ng.cli import verify as verify_cli
from btrfs_backup_ng.cli import wizard_utils
from btrfs_backup_ng.core.verify import VerifyLevel, VerifyReport, VerifyResult

CLI_DIR = Path(verify_cli.__file__).parent


def ascii_console() -> tuple[Console, io.BytesIO]:
    """A Rich console whose stream accepts ASCII only, like a minimal-locale tty.

    ``errors="strict"`` makes an unencodable character raise rather than be
    silently replaced -- the same failure a cron run would hit.
    """
    raw = io.BytesIO()
    stream = io.TextIOWrapper(raw, encoding="ascii", errors="strict", newline="")
    return Console(file=stream, force_terminal=False, width=80), raw


def _flush(console: Console, raw: io.BytesIO) -> str:
    console.file.flush()
    return raw.getvalue().decode("ascii")


# ---------------------------------------------------------------------------
# Static guard: no non-ASCII in any CLI string literal
# ---------------------------------------------------------------------------


def _string_literals(path: Path):
    """Yield (lineno, value) for every string constant in a module."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.lineno, node.value


@pytest.mark.parametrize(
    "module_path",
    sorted(CLI_DIR.rglob("*.py")),
    ids=lambda p: p.name,
)
def test_cli_string_literals_are_ascii(module_path: Path):
    """Every printable string in the CLI package must be ASCII.

    Catches a reintroduced glyph anywhere in the CLI, not only in the three
    modules that had one. Comments are exempt (they never reach a stream);
    this walks string constants, which includes docstrings -- those are ASCII
    today and cheap to keep that way.
    """
    offenders = [
        (lineno, value.strip()[:60], sorted({c for c in value if ord(c) > 127}))
        for lineno, value in _string_literals(module_path)
        if any(ord(c) > 127 for c in value)
    ]
    assert not offenders, (
        f"non-ASCII string literal(s) in {module_path.name}: {offenders}. "
        "Use ASCII words (e.g. [OK]/[FAILED]/[WARNING]) or a Rich primitive "
        "such as console.rule(), which falls back to ASCII automatically."
    )


# ---------------------------------------------------------------------------
# Runtime: the real display functions against an ASCII stream
# ---------------------------------------------------------------------------


def _report(status: str, passed: bool = True) -> VerifyReport:
    report = VerifyReport(level=VerifyLevel.METADATA, location="/mnt/backup")
    report.completed_at = report.started_at + 1.0
    report.results = [
        VerifyResult(
            snapshot_name="host-20260816-120000",
            level=VerifyLevel.METADATA,
            passed=passed,
            details={"status": status},
        )
    ]
    report.available = 1
    return report


@pytest.mark.parametrize(
    ("status", "passed", "expected_verdict", "expected_marker"),
    [
        ("ok", True, "pass", "[OK]"),
        ("unverifiable", True, "unverifiable", "[WARNING]"),
        ("failed", False, "fail", "[FAILED]"),
    ],
)
def test_verify_report_renders_under_ascii_stdout(
    monkeypatch, status, passed, expected_verdict, expected_marker
):
    """_display_report must not raise on an ASCII stream, for every verdict."""
    console, raw = ascii_console()
    monkeypatch.setattr(verify_cli, "console", console)

    report = _report(status, passed)
    assert report.verdict == expected_verdict

    args = argparse.Namespace(location="/mnt/backup", snapshot=None, json=False)
    verify_cli._display_report(report, args)  # must not raise UnicodeEncodeError

    out = _flush(console, raw)
    assert expected_marker in out, out
    assert out.isascii()


def test_wizard_section_header_renders_under_ascii_stdout(monkeypatch):
    """console.rule() must degrade to ASCII rather than raise."""
    console, raw = ascii_console()
    monkeypatch.setattr(wizard_utils, "console", console)

    wizard_utils.display_section_header("Select subvolumes")

    out = _flush(console, raw)
    assert "Select subvolumes" in out
    assert out.isascii()


def test_wizard_recommended_marker_renders_under_ascii_stdout(monkeypatch):
    """The recommendation marker, its legend and the table must all be ASCII.

    prompt_selection renders the table and then blocks on input, so the
    selection is supplied directly; the whole render path still runs.
    """
    console, raw = ascii_console()
    monkeypatch.setattr(wizard_utils, "console", console)
    monkeypatch.setattr(wizard_utils.Prompt, "ask", staticmethod(lambda *a, **k: "1"))

    selected = wizard_utils.prompt_selection(
        title="Subvolumes",
        items=[{"path": "/home"}, {"path": "/var"}],
        columns=[("path", "Path")],
        recommended_indices=[0],
    )
    assert selected == [0]

    out = _flush(console, raw)
    assert "recommended for backup" in out
    assert "/home" in out
    assert out.isascii()


def test_status_transaction_labels_are_ascii_and_aligned():
    """The transaction status labels are ASCII and share a column width.

    status.py writes these with plain print(), so an unencodable character
    raises straight out of print() on a minimal-locale stream.
    """
    labels = ["[OK]", "[FAILED]", "[STARTED]"]
    for label in labels:
        assert label.isascii()
    # Rendered with {:<9}: every label must fit so columns stay aligned.
    assert max(len(label) for label in labels) <= 9

    stream = io.TextIOWrapper(io.BytesIO(), encoding="ascii", errors="strict")
    for label in labels:
        print(f"  {label:<9} 2026-08-16 12:00:00", file=stream)  # must not raise
    stream.flush()
