"""The shipped examples must not tell people to do the thing that breaks.

Every example that configured an `ssh://` target also set `compress`, which
until recently caused the transfer to HANG -- the transfer layer compressed the
send stream and nothing reversed it, so `btrfs receive` blocked forever. Ten
targets across four example files, plus README.md and SNAPPER-INTEGRATION.md,
all recommended exactly that combination.

That is the likely reason snapper-to-ssh backups were reported broken: not an
obscure opt-in, but the documented configuration.

`compress` is now ignored and warned about on btrfs targets, so following the
old examples produces a warning on every run instead of a hang. Neither is
something to ship in an example, hence this test.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

EXAMPLES = sorted(Path("examples").glob("*.toml"))
RAW_SCHEMES = ("raw://", "raw+ssh://")


def _targets():
    for path in EXAMPLES:
        data = tomllib.loads(path.read_text())
        for vi, volume in enumerate(data.get("volumes", [])):
            for ti, target in enumerate(volume.get("targets", [])):
                yield path, f"volumes[{vi}].targets[{ti}]", target


def test_there_are_examples_to_check():
    """Guard against this whole file silently passing because the glob broke."""
    assert EXAMPLES, "no example configs found"
    assert list(_targets()), "no targets found in the examples"


def test_no_example_sets_compress_on_a_non_raw_target():
    offenders = []
    for path, where, target in _targets():
        compress = target.get("compress")
        if not compress or compress == "none":
            continue
        if not str(target.get("path", "")).startswith(RAW_SCHEMES):
            offenders.append(
                f"{path}:{where} compress={compress!r} path={target.get('path')!r}"
            )
    assert not offenders, (
        "these examples recommend compression on a target that cannot "
        "decompress it -- it is ignored, and it used to hang:\n  "
        + "\n  ".join(offenders)
    )


def test_raw_targets_may_still_use_compression():
    """Guard against over-correcting: compression on raw is the case that works,
    and stripping it from the examples would hide a genuinely useful feature."""
    raw_with_compress = [
        t
        for _p, _w, t in _targets()
        if str(t.get("path", "")).startswith(RAW_SCHEMES)
        and t.get("compress") not in (None, "none")
    ]
    if not raw_with_compress:
        pytest.skip("no raw target in the examples currently sets compress")
    assert raw_with_compress


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_every_example_still_parses(path):
    tomllib.loads(path.read_text())
