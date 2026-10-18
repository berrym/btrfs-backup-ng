"""The external programs a unit test may run, and the mark that names them.

``@pytest.mark.requires_tool(name)`` says a test runs ``name`` for real. Where
it is missing the test is skipped with that reason, or fails when
``BBNG_TEST_REQUIRE_TOOLS`` requires it (see tests/conftest.py).
"""

from __future__ import annotations

import pytest

#: Every program a test may name in ``requires_tool``. ``BBNG_TEST_REQUIRE_TOOLS``
#: is checked against this set rather than against the tests that happen to be
#: collected, so a packager running part of the suite is not stopped by a tool
#: that part does not use, while a misspelling still stops the run. ``btrfs`` is
#: for a test that must see btrfs-progs' own behaviour rather than the
#: placeholder tests/conftest.py puts on PATH where btrfs-progs is absent.
KNOWN_TOOLS = frozenset(
    {
        "awk",
        "bash",
        "btrbk",
        "btrfs",
        "bzip2",
        "cat",
        "csh",
        "dash",
        "fish",
        "gpg",
        "gzip",
        "lz4",
        "lzop",
        "openssl",
        "pbzip2",
        "pigz",
        "ps",
        "sh",
        "tcsh",
        "xz",
        "zsh",
        "zstd",
    }
)


def tool_params(*names: str, reason: str = "{tool} is not installed") -> list:
    """One test parameter per program, each marked ``requires_tool``.

    A program this machine lacks then shows up as a skip that says so, or as a
    failure under ``BBNG_TEST_REQUIRE_TOOLS``, rather than vanishing from the
    run while the test still passes. ``reason`` may use ``{tool}``.
    """
    return [
        pytest.param(
            name,
            marks=pytest.mark.requires_tool(name, reason=reason.format(tool=name)),
        )
        for name in names
    ]
