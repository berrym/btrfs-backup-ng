"""The ``raw`` command family: inspect and maintain raw-target backups.

Raw targets (``raw://`` and ``raw+ssh://``) hold btrfs send streams as files, each
with an authoritative ``.meta`` sidecar. These subcommands operate directly on such
a target. The family starts with ``raw list``, which enumerates a target's backups
via their sidecars (falling back to filename inference for legacy streams); later
0.8.5 additions (verify, backfill-metadata, encrypt) build on the same endpoint.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from btrfs_backup_ng import endpoint
from btrfs_backup_ng.__logger__ import logger


def execute_raw(args: argparse.Namespace) -> int:
    """Dispatch a ``raw <action>`` subcommand."""
    action = getattr(args, "raw_action", None)
    if action == "list":
        return _raw_list(args)
    # No/unknown action: argparse allows a bare `raw`, so guide the user.
    print("No raw action specified. Try: btrfs-backup-ng raw list <target>")
    return 1


def _coerce_raw_spec(target: str) -> str:
    """Return a ``choose_endpoint`` spec for a raw target.

    A bare path is treated as a local ``raw://`` target; ``raw://`` and
    ``raw+ssh://`` pass through unchanged; any other scheme is an error (the raw
    commands operate only on raw targets)."""
    if target.startswith(("raw://", "raw+ssh://")):
        return target
    if "://" in target:
        raise ValueError(
            f"{target!r} is not a raw target. Use a raw:// or raw+ssh:// URL "
            "(a plain path is treated as raw://)."
        )
    return "raw://" + target


def _human_size(n: int) -> str:
    """Format a byte count with a binary unit (B, KiB, MiB, ...)."""
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if size < 1024:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} EiB"


def _raw_list(args: argparse.Namespace) -> int:
    """List the backups a raw target holds (via their ``.meta`` sidecars)."""
    try:
        spec = _coerce_raw_spec(args.target)
    except ValueError as e:
        print(str(e))
        return 2

    common: dict = {}
    if getattr(args, "ssh_sudo", False):
        common["ssh_sudo"] = True

    try:
        ep = endpoint.choose_endpoint(spec, common_config=common)
    except ValueError as e:
        print(f"Cannot open raw target: {e}")
        return 2

    # A local target that does not exist is almost always a typo or an unmounted
    # path; warn (on stderr, so --json stays clean) rather than let the resulting
    # empty list look like "this target holds no backups".
    if spec.startswith("raw://"):
        local_path = Path(spec[len("raw://") :])
        if not local_path.exists():
            print(
                f"warning: {local_path} does not exist or is not mounted",
                file=sys.stderr,
            )

    try:
        snapshots = ep.list_snapshots(flush_cache=True)
    except Exception as e:  # pragma: no cover - defensive; endpoint errors vary
        logger.debug("raw list failed", exc_info=True)
        print(f"Failed to list {spec}: {e}")
        return 1

    if getattr(args, "json", False):
        print(json.dumps([s.to_dict() for s in snapshots], indent=2))
        return 0

    plural = "" if len(snapshots) == 1 else "s"
    print(f"Raw target: {spec}  ({len(snapshots)} snapshot{plural})")
    if not snapshots:
        return 0

    print(
        f"  {'NAME':<32} {'CREATED':<20} {'SIZE':>10}  "
        f"{'ENC':<11} {'COMPRESS':<8} {'CIPHER':<12} ORIGIN"
    )
    for s in snapshots:
        created = s.created.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(
            f"  {s.name:<32} {created:<20} {_human_size(s.size):>10}  "
            f"{(s.encrypt or '-'):<11} {(s.compress or '-'):<8} "
            f"{(s.openssl_cipher or '-'):<12} {s.provenance_origin or '-'}"
        )
    return 0
