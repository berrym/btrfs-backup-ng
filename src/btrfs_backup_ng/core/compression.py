"""The one table of compression methods this project knows.

There used to be two, in different modules and different shapes:
``core.transfer.COMPRESSION_PROGRAMS`` for streams over ssh:// and
``endpoint.raw_metadata.COMPRESSION_CONFIG`` for raw files at rest. They
disagreed about which methods existed, so ``compress`` meant different things
depending on the destination -- a config could compress with xz at rest but not
over the wire, ``--compress lzop`` was accepted by one and refused by the other,
and a btrbk ``stream_compress xz`` had nowhere to go on import. Nothing
technical required the split: both sides run ``<prog> -c`` on one end and
``<prog> -d -c`` on the other.

Keeping them merely *synchronised* would have left the same bug waiting, so
there is now one definition and two views onto it. Each consumer reads the
fields it needs:

    program      the binary, for availability checks
    compress     argv that reads stdin and writes compressed bytes to stdout
    decompress   argv that reverses it
    extension    suffix a raw stream file gets, and is recognised by on restore

``extension`` is the field that must never change casually: a raw ``.meta``
sidecar records the METHOD NAME and the stream file carries the suffix, so
altering either would strand backups already on disk. ``lzo`` and ``lzop`` are
deliberately both present, spelling the same format two ways -- btrbk and the
sidecar say "lzo", the transfer path names methods after the binary -- and they
share an extension so a file written under one spelling reads back under the
other.
"""

from __future__ import annotations

from typing import TypedDict


class CompressionMethod(TypedDict):
    """One compression method, complete for every consumer."""

    program: str
    compress: list[str]
    decompress: list[str]
    extension: str


def _method(program: str, extension: str, *extra: str) -> CompressionMethod:
    """Build an entry. ``extra`` is appended to both directions (e.g. -T0).

    ``-dc`` rather than ``-d -c``: every one of these programs accepts both, and
    this is the form the ssh:// remote command has been exercised with on real
    hardware.
    """
    return {
        "program": program,
        "compress": [program, "-c", *extra],
        "decompress": [program, "-dc", *extra],
        "extension": extension,
    }


COMPRESSION_METHODS: dict[str, CompressionMethod] = {
    "gzip": _method("gzip", ".gz"),
    # Parallel gzip, and parallel bzip2 below: different programs, same formats
    # and therefore the same file extensions.
    "pigz": _method("pigz", ".gz"),
    "zstd": _method("zstd", ".zst", "-T0"),  # -T0 uses all cores
    "lz4": _method("lz4", ".lz4"),
    "xz": _method("xz", ".xz"),
    "bzip2": _method("bzip2", ".bz2"),
    "pbzip2": _method("pbzip2", ".bz2"),
    # Two spellings of one format; see the module docstring.
    "lzo": _method("lzop", ".lzo"),
    "lzop": _method("lzop", ".lzo"),
}

#: Method names accepted anywhere ``compress`` is configured, plus "none".
COMPRESSION_CHOICES: list[str] = ["none", *sorted(COMPRESSION_METHODS)]


def extension_for(method: str) -> str:
    """The suffix a stream compressed with ``method`` carries."""
    entry = COMPRESSION_METHODS.get(method)
    return entry["extension"] if entry else ""
