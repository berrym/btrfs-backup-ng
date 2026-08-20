"""The source distribution must contain every path the test suite reads.

0.9.3 shipped an sdist without `docs/`, so building from source and running the
tests failed at collection when a test read docs/SNAPPER-INTEGRATION.md. That was
fixed by adding docs/ to MANIFEST.in -- the instance, not the class. `examples/`
was missing in exactly the same way and would have shipped in 0.9.4: the manifest
named config.example.toml, a file that stopped existing when the examples moved
into examples/, so it shipped nothing.

This checks the invariant instead of the two known cases: any repo directory the
tests reach for at module scope has to be declared in MANIFEST.in. It runs from a
git checkout, where MANIFEST.in and the tests are both present.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

MANIFEST = Path("MANIFEST.in")
TESTS = Path("tests")

#: A repo-relative path used as Path("x") or Path("x/y") in a test module.
_REPO_PATH = re.compile(r'Path\(\s*"([A-Za-z][A-Za-z0-9_.-]*)')


def _directories_the_tests_read() -> set[str]:
    """Top-level repo DIRECTORIES the suite reads by relative path.

    Filtered to names that actually exist as directories, so a string that
    merely looks like a path (a scheme, a snapshot name) is not mistaken for
    one -- the check must fail for real omissions, not for coincidences.
    """
    found: set[str] = set()
    for module in TESTS.rglob("*.py"):
        for name in _REPO_PATH.findall(module.read_text(encoding="utf-8")):
            if Path(name).is_dir():
                found.add(name)
    return found


def test_the_scan_finds_something():
    """Guard the guard: a regex that matched nothing would pass every case."""
    found = _directories_the_tests_read()
    assert "examples" in found and "docs" in found, found


def _package_roots() -> set[str]:
    """Directories the build backend ships on its own, without MANIFEST.in.

    setuptools discovers packages under ``where``, so src/ reaches the sdist
    through the package configuration rather than the manifest. Asking the
    manifest to declare it too would be a demand the build does not make.
    """
    pyproject = Path("pyproject.toml")
    if not pyproject.exists():
        return set()
    text = pyproject.read_text(encoding="utf-8")
    return set(re.findall(r'where\s*=\s*\[\s*"([^"]+)"', text))


@pytest.mark.skipif(not MANIFEST.exists(), reason="not a git checkout")
@pytest.mark.parametrize("directory", sorted(_directories_the_tests_read()))
def test_the_sdist_ships_every_directory_the_tests_read(directory):
    if directory in _package_roots():
        pytest.skip(f"{directory}/ ships via the package configuration")
    manifest = MANIFEST.read_text(encoding="utf-8")
    declared = any(
        line.split()[1] == directory or line.split()[1].startswith(f"{directory}/")
        for line in manifest.splitlines()
        if line.startswith(("recursive-include ", "graft ")) and len(line.split()) > 1
    )
    assert declared, (
        f"tests read {directory}/ but MANIFEST.in does not ship it, so the sdist "
        f"will fail its own test suite -- the way docs/ did in 0.9.3"
    )


@pytest.mark.skipif(not MANIFEST.exists(), reason="not a git checkout")
def test_the_manifest_names_no_path_that_does_not_exist():
    """A manifest line pointing at nothing ships nothing, and says otherwise.

    config.example.toml sat here long after the file moved into examples/,
    which is why the gap went unnoticed.
    """
    missing = []
    for line in MANIFEST.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0] == "include" and not Path(parts[1]).exists():
            missing.append(parts[1])
    assert not missing, f"MANIFEST.in includes paths that do not exist: {missing}"
