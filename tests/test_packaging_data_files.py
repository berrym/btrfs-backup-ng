"""Man pages and completions must reach an installed environment, not just the sdist.

`manpages install` and `completions install` copy files out of the installed
tree. MANIFEST.in governs the source tarball only, so a wheel built from this
project carried neither and both commands had nothing to copy -- for every user
who installed from PyPI. This is the same class as the docs-missing-from-sdist
bug: the test suite passed against the source checkout, where the files are
present at their development paths, and said nothing about the artifact.

Verified against real wheels: built from the previous packaging, both lookups
returned None in a clean venv; with the data-files declaration they resolve to
<venv>/share/man/man1 and <venv>/share/btrfs-backup-ng/completions.

These tests guard the realistic regression -- a file added to man/man1 or
completions/ that nobody remembers to declare -- and the lookup itself. They do
not build a wheel; the declaration is checked against what is actually on disk.
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

import pytest

from btrfs_backup_ng.cli.completions import get_completions_dir
from btrfs_backup_ng.cli.manpages import get_manpages_dir

REPO_ROOT = Path(__file__).parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"


def _data_files() -> dict[str, list[str]]:
    with open(PYPROJECT, "rb") as fh:
        config = tomllib.load(fh)
    data_files = config.get("tool", {}).get("setuptools", {}).get("data-files")
    assert data_files, (
        "pyproject declares no [tool.setuptools.data-files]; a wheel built from it "
        "ships no man pages or completions, and `manpages install` / `completions "
        "install` have nothing to copy"
    )
    return data_files


def _declared_files() -> set[Path]:
    """Every file the data-files globs actually resolve to, on disk."""
    resolved: set[Path] = set()
    for patterns in _data_files().values():
        for pattern in patterns:
            resolved.update(REPO_ROOT.glob(pattern))
    return resolved


class TestEveryShippedFileIsDeclared:
    def test_all_man_pages_are_declared(self):
        on_disk = set((REPO_ROOT / "man" / "man1").glob("*.1"))
        assert on_disk, "no man pages found on disk"
        missing = on_disk - _declared_files()
        assert not missing, (
            f"man page(s) not covered by [tool.setuptools.data-files], so they will "
            f"not be in the wheel: {sorted(p.name for p in missing)}"
        )

    def test_all_completion_scripts_are_declared(self):
        on_disk = {
            p
            for p in (REPO_ROOT / "completions").iterdir()
            if p.is_file() and p.suffix != ".md"
        }
        assert on_disk, "no completion scripts found on disk"
        missing = on_disk - _declared_files()
        assert not missing, (
            f"completion script(s) not covered by [tool.setuptools.data-files], so "
            f"they will not be in the wheel: {sorted(p.name for p in missing)}"
        )

    def test_declared_patterns_all_resolve(self):
        """A glob matching nothing means a moved or renamed file shipped as absent."""
        for target, patterns in _data_files().items():
            for pattern in patterns:
                assert list(REPO_ROOT.glob(pattern)), (
                    f"data-files pattern {pattern!r} (-> {target}) matches no file; "
                    "the wheel would ship nothing for it"
                )

    def test_installed_under_fhs_share_paths(self):
        """The install targets must match where the lookup functions search."""
        targets = set(_data_files())
        assert "share/man/man1" in targets, targets
        assert "share/btrfs-backup-ng/completions" in targets, targets


class TestLookupFindsDataUnderThePrefix:
    """The lookups must resolve data installed under sys.prefix, as a wheel places it."""

    def test_manpages_found_under_prefix(self, tmp_path, monkeypatch):
        man1 = tmp_path / "share" / "man" / "man1"
        man1.mkdir(parents=True)
        (man1 / "btrfs-backup-ng.1").write_text(".TH BTRFS-BACKUP-NG 1\n")
        monkeypatch.setattr(sys, "prefix", str(tmp_path))
        monkeypatch.setattr(sys, "base_prefix", str(tmp_path))
        assert get_manpages_dir() == man1

    def test_completions_found_under_prefix(self, tmp_path, monkeypatch):
        comp = tmp_path / "share" / "btrfs-backup-ng" / "completions"
        comp.mkdir(parents=True)
        (comp / "btrfs-backup-ng.bash").write_text("# completion\n")
        monkeypatch.setattr(sys, "prefix", str(tmp_path))
        monkeypatch.setattr(sys, "base_prefix", str(tmp_path))
        assert get_completions_dir() == comp

    def test_manpages_prefix_without_the_pages_is_not_accepted(
        self, tmp_path, monkeypatch
    ):
        """An empty share/man/man1 (another package's) must not shadow the real one."""
        (tmp_path / "share" / "man" / "man1").mkdir(parents=True)
        monkeypatch.setattr(sys, "prefix", str(tmp_path))
        monkeypatch.setattr(sys, "base_prefix", str(tmp_path))
        found = get_manpages_dir()
        assert found != tmp_path / "share" / "man" / "man1", (
            "an unrelated man1 directory was accepted; the check must confirm our "
            "own page is present"
        )


@pytest.mark.parametrize(
    "lookup", [get_manpages_dir, get_completions_dir], ids=["manpages", "completions"]
)
def test_source_checkout_still_resolves(lookup):
    """Running from a checkout must keep working -- this is how developers use it."""
    found = lookup()
    assert found is not None and found.is_dir(), (
        "the lookup found nothing from a source checkout, so the command is broken "
        "for contributors as well"
    )
