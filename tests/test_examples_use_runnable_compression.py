"""Every `compress` in a shipped example must be one that path can actually run.

The examples have twice been wrong about compression in opposite directions.

First they set `compress` on ssh:// targets while the ssh endpoint had no
decompression step, so the option did nothing there. Then -- reading "no
decompressor" as "compression is impossible" rather than "the wiring is
missing" -- it was stripped out of every example, which removed a working
feature from the documentation and broke parity with btrbk's stream_compress
for anyone migrating.

The ssh endpoint now compresses before the wire and decompresses on the remote
before `btrfs receive`, so ssh:// targets legitimately use `compress` again.

What this pins is neither "always set it" nor "never set it", but that whatever
an example sets is a method the relevant code path supports:

  * a btrfs target (local or ssh://) runs the method from
    core.transfer.COMPRESSION_PROGRAMS;
  * a raw target runs the method from endpoint.raw_metadata.COMPRESSION_CONFIG,
    which is a larger set (it adds xz, lzo, bzip2, pbzip2).

Both sets come from the authoritative tables, so this cannot drift from what the
config loader accepts.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

import re

from btrfs_backup_ng.core.transfer import COMPRESSION_PROGRAMS
from btrfs_backup_ng.endpoint.raw_metadata import COMPRESSION_CONFIG

EXAMPLES = sorted(Path("examples").glob("*.toml"))
RAW_SCHEMES = ("raw://", "raw+ssh://")
TRANSFER_METHODS = frozenset(COMPRESSION_PROGRAMS)
RAW_METHODS = frozenset(COMPRESSION_CONFIG)


def _targets():
    for path in EXAMPLES:
        data = tomllib.loads(path.read_text())
        for vi, volume in enumerate(data.get("volumes", [])):
            for ti, target in enumerate(volume.get("targets", [])):
                yield path, f"volumes[{vi}].targets[{ti}]", target


def test_there_are_examples_and_targets_to_check():
    """Guard against this file passing because the glob silently found nothing."""
    assert EXAMPLES, "no example configs found"
    assert list(_targets()), "no targets found in the examples"


def test_every_compress_value_is_runnable_on_its_target():
    offenders = []
    for path, where, target in _targets():
        method = target.get("compress")
        if not method or method == "none":
            continue
        target_path = str(target.get("path", ""))
        is_raw = target_path.startswith(RAW_SCHEMES)
        allowed = RAW_METHODS if is_raw else TRANSFER_METHODS
        if method not in allowed:
            kind = "raw" if is_raw else "btrfs"
            offenders.append(
                f"{path}:{where} compress={method!r} on a {kind} target "
                f"(supported there: {', '.join(sorted(allowed))})"
            )
    assert not offenders, "\n  ".join(
        ["unrunnable compression in examples:"] + offenders
    )


def test_compression_is_demonstrated_somewhere():
    """It is a supported feature and btrbk users migrate in expecting it; an
    example set that never shows it reads as though it does not exist."""
    with_compress = [
        (p, t.get("compress"))
        for p, _w, t in _targets()
        if t.get("compress") not in (None, "none")
    ]
    assert with_compress, (
        "no example demonstrates compression at all; a user looking for how to "
        "compress a remote backup has nothing to copy"
    )


def test_an_ssh_target_demonstrates_it():
    """ssh:// is where compression earns its keep -- over the wire. This is the
    case that was removed, so it is the case worth pinning."""
    ssh_with_compress = [
        t
        for _p, _w, t in _targets()
        if str(t.get("path", "")).startswith("ssh://")
        and t.get("compress") not in (None, "none")
    ]
    assert ssh_with_compress, "no ssh:// example shows stream compression"


def test_a_raw_target_demonstrates_it_too():
    """raw compresses at rest and records the method in its sidecar -- a
    different guarantee, worth showing separately."""
    raw_with_compress = [
        t
        for _p, _w, t in _targets()
        if str(t.get("path", "")).startswith(RAW_SCHEMES)
        and t.get("compress") not in (None, "none")
    ]
    assert raw_with_compress, "no raw example shows compression at rest"


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_every_example_still_parses(path):
    tomllib.loads(path.read_text())


class TestTheProseAgreesWithTheCode:
    """The examples must not SAY something the code stopped doing.

    The checks above validate `compress` VALUES, so they stayed green while two
    files still told the reader compression does not work on a btrfs target and
    listed a method set missing four of the methods that do. Prose is what people
    act on; an example that contradicts the code is worse than no example.
    """

    DOCS = sorted(
        [
            *Path("examples").glob("*.toml"),
            *Path("docs").glob("*.md"),
            Path("README.md"),
        ]
    )

    # Claims that were true before stream compression was wired up over ssh://.
    STALE_CLAIMS = (
        "not on a btrfs target",
        "only on raw",
        "supported only on raw",
        "ignored on btrfs",
        "compression is not supported",
        "nothing decompresses",
    )

    def test_there_are_files_to_check(self):
        assert self.DOCS, "the glob found nothing; this whole class would pass blindly"

    @pytest.mark.parametrize("path", DOCS, ids=lambda p: str(p))
    def test_no_file_claims_compression_is_raw_only(self, path):
        text = path.read_text(encoding="utf-8").lower()
        for claim in self.STALE_CLAIMS:
            assert claim not in text, (
                f"{path} still says {claim!r}; compression works on ssh:// targets "
                f"(compressed before the wire, decompressed on the remote)"
            )

    @pytest.mark.parametrize("path", DOCS, ids=lambda p: str(p))
    def test_no_file_advertises_a_method_the_code_cannot_run(self, path):
        """Drift in the other direction: naming a method nothing implements."""
        runnable = set(COMPRESSION_PROGRAMS) | set(COMPRESSION_CONFIG) | {"none"}
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            # A method enumeration, e.g. "none|gzip|zstd|lz4".
            if line.count("|") < 2:
                continue
            candidates = re.findall(r"[a-z0-9]{2,8}", line.split("#")[-1])
            named = {c for c in candidates if c in runnable}
            if len(named) < 3:
                continue
            unknown = {
                c
                for c in candidates
                if c not in runnable and c in {"lzma", "bzip3", "brotli", "lz4hc"}
            }
            assert not unknown, f"{path}: {line.strip()!r} names {unknown}"
