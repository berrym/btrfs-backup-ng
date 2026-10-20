"""The glossary cannot rot silently.

Every code reference in ``docs/GLOSSARY.md`` must import; every excerpt it
quotes must still be in the file it names; every on-disk format it shows must
be what the writer produces today; and the phrases it retires must not appear
in the source or the other documentation.
"""

from __future__ import annotations

import importlib
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
GLOSSARY = ROOT / "docs" / "GLOSSARY.md"
TEXT = GLOSSARY.read_text()

REFERENCE = re.compile(r"`(btrfs_backup_ng(?:\.\w+)*):([\w.]+)`")
EXCERPT = re.compile(r"From `([^`]+)`[^\n]*:\n\n```python\n(.*?)```", re.S)
RETIRED = re.compile(r"^- \*\*([^*]+)\*\*", re.M)


def _references() -> list[tuple[str, str]]:
    return sorted(set(REFERENCE.findall(TEXT)))


@pytest.mark.parametrize("module_name,symbol", _references())
def test_every_code_reference_resolves(module_name, symbol):
    module = importlib.import_module(module_name)
    target = module
    for part in symbol.split("."):
        assert hasattr(target, part), f"{module_name}:{symbol} does not exist"
        target = getattr(target, part)


def test_the_glossary_names_enough_references():
    assert len(_references()) >= 30


@pytest.mark.parametrize("path,excerpt", EXCERPT.findall(TEXT))
def test_every_excerpt_is_still_in_its_file(path, excerpt):
    source = (ROOT / path).read_text()
    assert excerpt in source, f"the excerpt quoted from {path} is no longer there"


def test_there_are_excerpts_to_check():
    assert 10 <= len(EXCERPT.findall(TEXT)) <= 20, "the glossary's own bound"


def test_the_lock_file_example_is_what_write_locks_produces():
    from btrfs_backup_ng import __util__

    shown = re.search(r"```json\n(\{\n    \"home-20260101-120000\".*?)```", TEXT, re.S)
    assert shown, "the lock file example is missing"
    produced = __util__.write_locks(
        {
            "home-20260101-120000": {
                "locks": ["/mnt/backup", "restore:3f8a1c2e"],
                "parent_locks": ["ssh://user@host:/backups/home"],
            }
        }
    )
    assert shown.group(1).rstrip("\n") == produced.rstrip("\n")


def test_the_holder_record_example_has_the_keys_a_record_carries(tmp_path):
    from btrfs_backup_ng.sshutil.lock import holders_beside

    shown = re.search(r"```json\n(\{\"name\".*?\})\n```", TEXT, re.S)
    assert shown, "the holder record example is missing"
    documented = set(json.loads(shown.group(1)))
    lock_file = tmp_path / ".btrfs-backup-ng.locks"
    lock_file.write_text("{}")
    holders_beside(lock_file).acquire_shared("snap-x", "restore:abc")
    (record,) = (tmp_path / ".btrfs-backup-ng.locks.holders").rglob("holders/*")
    assert set(json.loads(record.read_text())) == documented


def test_the_constants_quoted_are_the_constants_in_force():
    from btrfs_backup_ng.sshutil import lock

    for name in (
        "DEFAULT_HEARTBEAT_INTERVAL",
        "DEFAULT_STALE_AFTER",
        "DEAD_HOLDER_MULTIPLE",
    ):
        assert f"{name} = {getattr(lock, name)}\n" in TEXT, (
            f"{name} is quoted with another value"
        )


def _retired_phrases() -> list[str]:
    section = TEXT[TEXT.index("## Terms we do not use") :]
    phrases = []
    for match in RETIRED.finditer(section):
        head = match.group(1)
        phrases.extend(p.strip() for p in head.split(", and ") if p.strip())
    return phrases


def test_the_retired_phrases_are_listed():
    assert "sidecar" in _retired_phrases()


@pytest.mark.parametrize(
    "phrase,pattern",
    [
        (
            "sidecar for a holder record",
            r"holder sidecar|sidecar (beside|next to) the lock|lock[- ]file sidecar|pin sidecar",
        ),
        (
            "stale lock as a verdict",
            r"\"Stale lock|Stale lock on|stale lock(s)? (found|detected)",
        ),
        ("ledger in a message", r"logger\.\w+\([^)]*ledger|print\([^)]*ledger"),
        ("remote lock as a kind", r"remote lock store"),
    ],
)
def test_a_retired_phrase_is_not_in_the_source_or_docs(phrase, pattern):
    scanned = [p for p in (ROOT / "src").rglob("*.py")]
    scanned += [p for p in (ROOT / "docs").glob("*.md") if p.name != "GLOSSARY.md"]
    scanned += [ROOT / "README.md"]
    offenders = []
    for path in scanned:
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if re.search(pattern, line, re.I):
                offenders.append(f"{path.relative_to(ROOT)}:{number}: {line.strip()}")
    assert not offenders, f"{phrase}:\n" + "\n".join(offenders)


def test_the_glossary_is_linked_from_the_readme_and_the_lock_docs():
    assert "docs/GLOSSARY.md" in (ROOT / "README.md").read_text()
    assert "GLOSSARY.md" in (ROOT / "docs" / "REMOTE-LOCKS.md").read_text()


def test_the_glossary_ships_in_the_sdist():
    manifest = ROOT / "MANIFEST.in"
    pyproject = (ROOT / "pyproject.toml").read_text()
    assert "docs" in pyproject or (manifest.exists() and "docs" in manifest.read_text())


def test_every_index_link_points_at_a_heading():
    slugs = {
        re.sub(r"[^a-z0-9 -]", "", heading.lower()).replace(" ", "-")
        for heading in re.findall(r"^### (.+)$", TEXT, re.M)
    }
    index = TEXT[TEXT.index("## Index") : TEXT.index("## Snapshots and transfers")]
    targets = set(re.findall(r"\]\(#([a-z0-9-]+)\)", index))
    assert targets, "the index is empty"
    assert targets <= slugs, f"index links without a heading: {sorted(targets - slugs)}"
