"""The ``raw`` command family: inspect and maintain raw-target backups.

Raw targets (``raw://`` and ``raw+ssh://``) hold btrfs send streams as files, each
with an authoritative ``.meta`` sidecar. These subcommands operate directly on such
a target. The family starts with ``raw list``, which enumerates a target's backups
via their sidecars (falling back to filename inference for legacy streams); later
0.8.5 additions (verify, backfill-metadata, encrypt) build on the same endpoint.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from pathlib import Path

from btrfs_backup_ng import endpoint
from btrfs_backup_ng.__logger__ import logger
from btrfs_backup_ng.endpoint import raw as raw_mod
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot


def execute_raw(args: argparse.Namespace) -> int:
    """Dispatch a ``raw <action>`` subcommand."""
    action = getattr(args, "raw_action", None)
    if action == "list":
        return _raw_list(args)
    if action == "verify":
        return _raw_verify(args)
    if action == "backfill-metadata":
        return _raw_backfill(args)
    if action == "encrypt":
        return _raw_encrypt(args)
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


def _open_target(args: argparse.Namespace):
    """Coerce ``args.target`` to a raw spec and open the endpoint.

    Returns ``(endpoint, spec)``. Raises ValueError on a non-raw scheme or an
    endpoint that cannot be built. Warns (on stderr, so ``--json`` stays clean)
    when a local target does not exist -- a typo/unmounted path should not read as
    an empty target."""
    spec = _coerce_raw_spec(args.target)
    common: dict = {}
    if getattr(args, "ssh_sudo", False):
        common["ssh_sudo"] = True
    try:
        ep = endpoint.choose_endpoint(spec, common_config=common)
    except ValueError as e:
        # Preserve the historical framing so a construction failure (e.g. a
        # hostname-less raw+ssh spec) reads differently from a coercion error.
        raise ValueError(f"Cannot open raw target: {e}") from e
    if spec.startswith("raw://"):
        local_path = Path(spec[len("raw://") :])
        if not local_path.exists():
            print(
                f"warning: {local_path} does not exist or is not mounted",
                file=sys.stderr,
            )
    return ep, spec


def _warn_remote_no_lock(ep) -> None:
    """Warn (on stderr, so ``--json`` stays clean) that a raw+ssh target is not
    lock-protected. The per-target mutual-exclusion lock is local-only (a remote
    lock needs a persistent connection to hold an flock, which is deferred), so a
    remote maintenance write can still race a concurrent backup/prune -- run it
    while the target is idle."""
    if getattr(ep, "_is_remote", False):
        print(
            "warning: raw+ssh targets are not lock-protected; run maintenance while "
            "the target is idle (no concurrent backup or prune to this target).",
            file=sys.stderr,
        )


def _raw_list(args: argparse.Namespace) -> int:
    """List the backups a raw target holds (via their ``.meta`` sidecars)."""
    try:
        ep, spec = _open_target(args)
    except ValueError as e:
        print(str(e))
        return 2

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


def _raw_verify(args: argparse.Namespace) -> int:
    """Verify raw backups by recomputing each stream's sha256 and comparing it to
    the checksum recorded in its ``.meta`` sidecar.

    Per-snapshot status: ``ok`` (matches), ``corrupt`` (mismatch -> the stored
    stream differs from what was backed up), ``error`` (the stream could not be
    read/hashed), or ``unverifiable`` (no checksum was recorded -- a legacy backup
    or a best-effort write-time seal that failed). Exit 1 if any snapshot is corrupt
    or errored, else 0."""
    try:
        ep, spec = _open_target(args)
    except ValueError as e:
        print(str(e))
        return 2

    try:
        snapshots = ep.list_snapshots(flush_cache=True)
    except Exception as e:  # pragma: no cover - defensive; endpoint errors vary
        logger.debug("raw verify failed", exc_info=True)
        print(f"Failed to list {spec}: {e}")
        return 1

    want = getattr(args, "snapshot", None)
    if want:
        snapshots = [s for s in snapshots if s.name == want]
        if not snapshots:
            print(f"No snapshot named {want!r} at {spec}")
            return 2

    results = []
    for s in snapshots:
        recorded = s.checksum_value
        algorithm = getattr(s, "checksum_algorithm", "sha256")
        if not recorded:
            status, computed = "unverifiable", None
        elif algorithm != "sha256":
            # We only compute sha256; comparing it to a digest of another algorithm
            # would false-flag an intact stream as corrupt. Cannot check -> unverifiable.
            status, computed = "unverifiable", None
        else:
            computed = ep.compute_stream_checksum(s)
            if computed is None:
                status = "error"
            elif computed == recorded:
                status = "ok"
            else:
                status = "corrupt"
        results.append(
            {
                "name": s.name,
                "status": status,
                "recorded": recorded,
                "computed": computed,
            }
        )

    if getattr(args, "json", False):
        print(json.dumps(results, indent=2))
    else:
        plural = "" if len(results) == 1 else "s"
        print(f"Raw target: {spec}  (verifying {len(results)} snapshot{plural})")
        for r in results:
            line = f"  {r['status'].upper():<13} {r['name']}"
            if r["status"] == "corrupt":
                # Show the mismatch on the one status an operator must investigate.
                rec = (r["recorded"] or "-")[:12]
                comp = (r["computed"] or "-")[:12]
                line += f"  recorded={rec}... computed={comp}..."
            print(line)
        counts = {k: 0 for k in ("ok", "corrupt", "error", "unverifiable")}
        for r in results:
            counts[r["status"]] += 1
        print(
            f"  {counts['ok']} ok, {counts['corrupt']} corrupt, "
            f"{counts['error']} error, {counts['unverifiable']} unverifiable"
        )

    # Fail if any backup is corrupt or its stream could not be read.
    bad = any(r["status"] in ("corrupt", "error") for r in results)
    return 1 if bad else 0


def _raw_backfill(args: argparse.Namespace) -> int:
    """Write authoritative ``.meta`` sidecars for LEGACY streams that have none
    (backups written before sidecars existed).

    Each backfilled sidecar records the pipeline inferred from the filename and a
    sha256 of the stream as it exists now, but is stamped
    ``provenance_origin=backfill`` and ``stream_completeness=unknown``: a legacy
    stream's completeness cannot be verified, so a backfilled sidecar is marked as a
    reconstructed, non-authoritative record (callers should not assume it is a
    verified complete backup). ``--dry-run`` reports the candidates without writing.
    Exit 1 if any sidecar write failed."""
    try:
        ep, spec = _open_target(args)
    except ValueError as e:
        print(str(e))
        return 2

    dry = getattr(args, "dry_run", False)
    if not dry:
        _warn_remote_no_lock(ep)
    # Hold the per-target lock across scan + write so a concurrent backup/prune
    # cannot interleave (a dry-run is read-only, so it needs no lock).
    lock_ctx = contextlib.nullcontext() if dry else ep.target_lock()
    results = []
    try:
        with lock_ctx:
            candidates = ep.streams_without_sidecar()
            for snap in candidates:
                entry = {
                    "name": snap.name,
                    "stream": str(snap.stream_path),
                    "compress": snap.compress,
                    "encrypt": snap.encrypt,
                }
                if dry:
                    entry["action"] = "would-backfill"
                else:
                    # Seal a checksum of the stream as it is now (detects later
                    # corruption); completeness stays "unknown" -- this does not
                    # prove the legacy stream is a complete btrfs send.
                    snap.checksum_value = ep.compute_stream_checksum(snap)
                    # Belt-and-suspenders under the lock: a sidecar should not appear
                    # mid-operation now, but re-check and never overwrite one.
                    if ep.sidecar_exists(snap):
                        entry["action"] = "skipped"
                        results.append(entry)
                        continue
                    try:
                        ep.write_sidecar(snap)
                        entry["action"] = "backfilled"
                        entry["checksum"] = snap.checksum_value
                    except Exception as e:
                        logger.debug("backfill sidecar write failed", exc_info=True)
                        entry["action"] = "error"
                        entry["error"] = str(e)
                results.append(entry)
    except RuntimeError as e:  # target busy (lock timeout)
        print(f"Cannot backfill {spec}: {e}")
        return 1
    except Exception as e:  # pragma: no cover - defensive; endpoint errors vary
        logger.debug("raw backfill-metadata failed", exc_info=True)
        print(f"Failed to scan {spec}: {e}")
        return 1

    if getattr(args, "json", False):
        print(json.dumps(results, indent=2))
    else:
        plural = "" if len(results) == 1 else "s"
        print(
            f"Raw target: {spec}  "
            f"({len(results)} legacy stream{plural} without a sidecar)"
        )
        for r in results:
            print(f"  {r['action'].upper():<15} {r['name']}")
        if not dry and results:
            n_ok = sum(1 for r in results if r["action"] == "backfilled")
            n_err = sum(1 for r in results if r["action"] == "error")
            print(f"  {n_ok} backfilled, {n_err} error")
            print(
                "  Note: backfilled sidecars are marked stream_completeness=unknown "
                "-- a legacy stream cannot be proven complete."
            )

    bad = any(r["action"] == "error" for r in results)
    return 1 if bad else 0


def _raw_encrypt(args: argparse.Namespace) -> int:
    """Encrypt plaintext raw streams in place (remediation for backups written as
    plaintext despite an encrypt config -- GHSA-vr25-6vrh-869j).

    For each plaintext stream: write an encrypted copy (new sidecar,
    provenance_origin=remediation), then run a LIVE decrypt-to-identical proof
    (decrypt the new stream and confirm it is byte-identical to the plaintext). The
    plaintext is removed ONLY when that proof passes AND ``--shred`` was given -- and
    even then by a plain unlink: on copy-on-write filesystems (btrfs) and SSDs an
    overwrite does not erase the underlying blocks, so true physical erasure relies
    on device-level trim/discard or full-disk encryption, which this tool cannot
    provide. Prior plaintext exposure (old media, indexes) cannot be undone."""
    spec = _coerce_raw_spec(args.target)
    if spec.startswith("raw+ssh://"):
        print(
            "raw encrypt runs locally so the passphrase/keys never leave this host. "
            "For a remote target, mount it locally and use a raw:// path."
        )
        return 2

    try:
        ep, spec = _open_target(args)
    except ValueError as e:
        print(str(e))
        return 2

    encrypt = args.encrypt
    if encrypt == "gpg" and not getattr(args, "gpg_recipient", None):
        print("--gpg-recipient is required with --encrypt gpg")
        return 2
    if encrypt == "openssl_enc" and raw_mod._selected_passphrase_env() is None:
        print(
            "openssl_enc requires a passphrase in BTRFS_BACKUP_PASSPHRASE or "
            "BTRBK_PASSPHRASE"
        )
        return 2

    try:
        snapshots = ep.list_snapshots(flush_cache=True)
    except Exception as e:  # pragma: no cover - defensive; endpoint errors vary
        logger.debug("raw encrypt failed", exc_info=True)
        print(f"Failed to list {spec}: {e}")
        return 1
    plaintext = [s for s in snapshots if not s.encrypt]
    if not plaintext:
        print(f"Raw target: {spec}  (no plaintext streams to encrypt)")
        return 0

    dry = getattr(args, "dry_run", False)
    shred = getattr(args, "shred", False)
    cipher = getattr(args, "openssl_cipher", None) or "aes-256-cbc"
    gpg_recipient = getattr(args, "gpg_recipient", None)
    gpg_keyring = getattr(args, "gpg_keyring", None)
    ext = ".gpg" if encrypt == "gpg" else ".enc"
    # The decrypt proof (and any pre-existing-twin check) uses this endpoint's
    # send(); make it use the requested gpg keyring so encrypt and verify agree.
    if gpg_keyring:
        ep.gpg_keyring = gpg_keyring

    # Destructive-op confirmation: --shred is the opt-in; an interactive TTY still
    # confirms unless --yes was passed (mirrors restore's dangerous-op pattern). The
    # prompt goes to stderr so --json output on stdout stays clean.
    if shred and not dry and not getattr(args, "yes", False) and sys.stdin.isatty():
        print(
            f"About to encrypt {len(plaintext)} plaintext stream(s) and, after a "
            "verified decrypt proof, DELETE each plaintext file (plain unlink; not a "
            "secure wipe -- see --help). Continue? [y/N] ",
            end="",
            file=sys.stderr,
        )
        if input().strip().lower() not in ("y", "yes"):
            print("Aborted.", file=sys.stderr)
            return 1

    def _process() -> list:
        out: list = []
        for s in plaintext:
            entry: dict = {"name": s.name, "stream": str(s.stream_path)}
            if dry:
                entry["action"] = (
                    "would-encrypt-and-shred" if shred else "would-encrypt"
                )
                out.append(entry)
                continue
            enc_path = Path(str(s.stream_path) + ext)
            pre_existing = enc_path.exists()
            if pre_existing:
                # Never clobber an existing encrypted stream. Verify it decrypts to
                # THIS plaintext; if so it is a valid prior remediation (idempotent).
                twin: RawSnapshot = RawSnapshot(
                    name=s.name,
                    stream_path=enc_path,
                    encrypt=encrypt,
                    compress=None,
                    openssl_cipher=cipher if encrypt == "openssl_enc" else None,
                )
            else:
                try:
                    twin = ep.remediate_plaintext(
                        s,
                        encrypt=encrypt,
                        gpg_recipient=gpg_recipient,
                        gpg_keyring=gpg_keyring,
                        openssl_cipher=cipher,
                    )
                except Exception as e:
                    logger.debug("remediate failed for %s", s.name, exc_info=True)
                    entry["action"] = "error"
                    entry["error"] = str(e)
                    out.append(entry)
                    continue
            entry["encrypted"] = str(enc_path)
            verified = ep.decrypt_matches_plaintext(twin, s.stream_path)
            entry["verified"] = verified
            if not verified:
                # Fresh: encrypted but unprovable (e.g. gpg without the secret key)
                # -> keep the plaintext. Pre-existing: it does not decrypt to this
                # plaintext -> never touch it or the plaintext.
                entry["action"] = (
                    "existing-encrypted-differs"
                    if pre_existing
                    else "encrypted-unverified"
                )
                out.append(entry)
                continue
            if shred:
                try:
                    os.unlink(s.stream_path)
                    if s.metadata_path.exists():
                        os.unlink(s.metadata_path)
                    entry["action"] = "removed-plaintext"
                except OSError as e:
                    entry["action"] = "remove-failed"
                    entry["error"] = str(e)
            else:
                entry["action"] = "already-encrypted" if pre_existing else "encrypted"
            out.append(entry)
        return out

    # Hold the per-target lock across the whole remediation (a dry-run is read-only).
    lock_ctx = contextlib.nullcontext() if dry else ep.target_lock()
    try:
        with lock_ctx:
            results = _process()
    except RuntimeError as e:  # target busy (lock timeout)
        print(f"Cannot encrypt {spec}: {e}")
        return 1

    if getattr(args, "json", False):
        print(json.dumps(results, indent=2))
    else:
        plural = "" if len(results) == 1 else "s"
        print(f"Raw target: {spec}  ({len(results)} plaintext stream{plural})")
        for r in results:
            print(f"  {r['action'].upper():<28} {r['name']}")
        if not dry:
            unver = sum(1 for r in results if r["action"] == "encrypted-unverified")
            if unver:
                print(
                    f"  {unver} encrypted but NOT verified (plaintext kept); check the "
                    "passphrase, or that a trusted gpg key with its secret is on this "
                    "host."
                )
            differs = sum(
                1 for r in results if r["action"] == "existing-encrypted-differs"
            )
            if differs:
                print(
                    f"  {differs} skipped: an encrypted stream already exists that does "
                    "NOT match the plaintext; resolve it manually (not overwritten)."
                )
            if not shred and any(
                r["action"] in ("encrypted", "already-encrypted") for r in results
            ):
                print(
                    "  Plaintext kept alongside the encrypted copy (no --shred). Once "
                    "you have confirmed the encrypted backups, re-run with --shred to "
                    "remove the plaintext."
                )
            if any(r["action"] == "removed-plaintext" for r in results):
                print(
                    "  Note: plaintext was removed by a plain unlink. On btrfs (CoW) "
                    "or SSDs this does NOT securely erase the blocks -- rely on device "
                    "trim/discard or full-disk encryption. Prior exposure cannot be "
                    "undone."
                )

    bad = any(r["action"] in ("error", "remove-failed") for r in results) or (
        shred
        and any(
            r["action"] in ("encrypted-unverified", "existing-encrypted-differs")
            for r in results
        )
    )
    return 1 if bad else 0
