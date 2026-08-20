"""Raw target endpoint for writing btrfs send streams to files.

This endpoint writes btrfs send streams directly to files instead of using
'btrfs receive'. This enables backups to non-btrfs filesystems (NFS, SMB,
cloud storage) with optional compression and encryption.

Compatible with btrbk's "raw target" feature for seamless migration.

Encryption methods:
- gpg: GPG public-key encryption (recommended for new setups)
- openssl_enc: OpenSSL symmetric encryption (for btrbk migration compatibility)
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import hashlib
import json
import os
import re
import shlex
import shutil
import stat
import subprocess
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, TypedDict

from btrfs_backup_ng import __util__
from btrfs_backup_ng.__logger__ import logger
from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.raw_metadata import (
    COMPRESSION_CONFIG,
    ChecksumVerdict,
    RawSnapshot,
    StructureVerdict,
    _fsync_directory,
    discover_raw_snapshots,
    get_file_extension,
    parse_stream_filename,
)


class PendingMetadata(TypedDict):
    """Type definition for pending metadata during receive."""

    name: str
    stream_path: Path
    part_path: Path
    parent_name: str | None
    compress: str | None
    encrypt: str | None
    gpg_recipient: str | None
    openssl_cipher: str | None


# Environment variable for OpenSSL passphrase (compatible with btrbk)
OPENSSL_PASSPHRASE_ENV = "BTRFS_BACKUP_PASSPHRASE"
BTRBK_PASSPHRASE_ENV = "BTRBK_PASSPHRASE"

# OpenSSL cipher names are alphanumerics and hyphens (aes-256-cbc, chacha20,
# aes-128-ctr, ...). Restrict to that grammar so a cipher value -- which may come
# from an on-disk .meta sidecar (semi-trusted) or from operator config -- can
# never carry a shell metacharacter, space, or quote into a pipeline. Anchored
# with \A ... \Z (not ^...$, which would match around a trailing newline) so a
# newline cannot slip through the structural guard regardless of downstream
# quoting.
_CIPHER_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9-]*\Z")

# Substrings marking an AEAD mode. `openssl enc` cannot use AEAD ciphers (it errors
# "AEAD ciphers not supported"), so accepting one only defers a cryptic failure to
# mid-transfer. No non-AEAD cipher name contains these tokens.
_AEAD_MARKERS = ("gcm", "ccm", "ocb", "poly1305")


def _validate_cipher(cipher: str) -> str:
    """Return ``cipher`` if it is a usable ``openssl enc`` cipher name, else raise
    ValueError.

    Structural check first (see ``_CIPHER_RE``: leading alphanumeric, then
    ``[A-Za-z0-9-]``, no metacharacters/whitespace/newline). Then two SEMANTIC
    rejections of values that are syntactically valid but unsafe or unusable:

      * ``none`` -- openssl's NULL cipher performs NO encryption, so accepting it
        would silently write a PLAINTEXT backup labelled as encrypted (the
        CWE-311/312 class fixed in 0.8.4 / GHSA-vr25-6vrh-869j). Refused.
      * AEAD modes (``*-gcm``/``*-ccm``/``*-ocb``/``*poly1305``) -- ``openssl enc``
        cannot use them; refuse up front with a clear message instead of a cryptic
        mid-transfer error.
    """
    if not isinstance(cipher, str) or not _CIPHER_RE.match(cipher):
        raise ValueError(
            f"Invalid openssl cipher name: {cipher!r}. Expected a name like "
            "'aes-256-cbc' (letters, digits, hyphens only)."
        )
    lowered = cipher.lower()
    if lowered == "none":
        raise ValueError(
            "Refusing openssl cipher 'none': it performs NO encryption and would "
            "write a plaintext backup labelled as encrypted. Use a real cipher "
            "such as aes-256-cbc, or set encrypt=none for an explicit plaintext "
            "target."
        )
    if any(marker in lowered for marker in _AEAD_MARKERS):
        raise ValueError(
            f"openssl cipher {cipher!r} is an AEAD mode that 'openssl enc' cannot "
            "use. Choose a non-AEAD cipher such as aes-256-cbc, aes-256-ctr, or "
            "chacha20."
        )
    return cipher


def _openssl_supports_cipher(cipher: str) -> bool:
    """Whether THIS host's openssl can actually use ``cipher`` with ``openssl enc``.

    ``_validate_cipher`` checks a cipher name's shape and rejects the unsafe/unusable
    ones, but a name it accepts can still be absent from this particular openssl build
    (a backup made on a host with a different openssl, or a hand-edited sidecar). Left
    to the decrypt pipeline, that surfaces as a cryptic multi-line OpenSSL EVP error
    dump. Probe the exact local binary (raw+ssh decrypts locally, so this is the binary
    that runs) by encrypting empty input: exit 0 means the cipher is known, non-zero
    means openssl rejects it -- the real cipher against the real binary, no fragile
    output parsing, portable across OpenSSL/LibreSSL. Returns True when the probe cannot
    run (openssl missing, an OS error, or a timeout) so this never blocks a restore it
    cannot actually adjudicate -- the decrypt pipeline still fails safe if the cipher is
    genuinely unusable."""
    openssl = shutil.which("openssl")
    if not openssl:
        return True
    try:
        proc = subprocess.run(
            [
                openssl,
                "enc",
                f"-{cipher}",
                "-in",
                os.devnull,
                "-out",
                os.devnull,
                "-pass",
                "pass:x",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired):
        return True
    return proc.returncode == 0


def _selected_passphrase_env() -> str | None:
    """Return the NAME of the passphrase environment variable that is set (primary
    ``BTRFS_BACKUP_PASSPHRASE`` preferred, then ``BTRBK_PASSPHRASE`` for btrbk
    compatibility), or None if neither is set.

    Single source of truth so the construction-time warning
    (``_get_openssl_passphrase``) and the pipeline ``-pass`` argument
    (``_openssl_pass_arg``) can never disagree about which variable is used."""
    if os.environ.get(OPENSSL_PASSPHRASE_ENV):
        return OPENSSL_PASSPHRASE_ENV
    if os.environ.get(BTRBK_PASSPHRASE_ENV):
        return BTRBK_PASSPHRASE_ENV
    return None


def _openssl_pass_arg() -> str:
    """Return the openssl ``-pass`` argument (``env:<NAME>``) for whichever
    passphrase env var is set.

    openssl reads the passphrase from the named environment variable itself, so
    the secret never appears on the command line. Raises ValueError if neither
    variable is set -- the caller must not run openssl with an empty passphrase,
    which silently produces an unreadable stream on encrypt and garbage on
    decrypt."""
    name = _selected_passphrase_env()
    if name is None:
        raise ValueError(
            f"openssl_enc requires a passphrase in {OPENSSL_PASSPHRASE_ENV} or "
            f"{BTRBK_PASSPHRASE_ENV}, but neither is set."
        )
    return f"env:{name}"


# Suffix for the in-progress stream file. A raw receive writes here and the
# transfer engine renames it to the final name only after the pipeline is
# confirmed successful (see RawEndpoint.commit_receive). A crash therefore
# leaves at most a ``.part`` file, which discovery ignores -- so a partial
# transfer can never be listed as a complete backup.
PARTIAL_SUFFIX = ".part"

# Per-target advisory lock file. Mutating operations (backup commit, prune, backfill,
# encrypt) hold an exclusive flock on it so they are mutually exclusive on one target.
LOCK_FILENAME = ".btrfs-backup-ng.lock"


def _open_failure_reason(e: OSError) -> str:
    """A plain-language reason opening a path failed, for a user-facing message.

    Translates the errno so a regular user sees why the file could not be opened and
    what to check, instead of a bare ``[Errno NN]`` repr (whose default text is
    sometimes misleading -- e.g. ELOOP prints 'Too many levels of symbolic links'
    for a single planted symlink). Shared by the lock open and the checksum open,
    both of which use O_NOFOLLOW and so surface ELOOP for a planted symlink."""
    reasons = {
        errno.ELOOP: "it is a symlink (refused for safety)",
        errno.EISDIR: "it is a directory, not a file",
        errno.ENXIO: "it is a FIFO/special file with no reader (refused)",
        errno.EACCES: (
            "permission denied -- check the target directory's ownership and "
            "permissions"
        ),
        errno.EPERM: (
            "operation not permitted -- check the target directory's ownership and "
            "permissions"
        ),
        errno.EROFS: "the filesystem is read-only",
        errno.ENOTDIR: "a parent path component is not a directory",
    }
    if e.errno is None:
        return str(e)
    return reasons.get(e.errno, str(e))


_ELEVATION_SENTINEL = "__BBNG_ELEVATED__"
"""Printed to stderr by an elevated shell AFTER the wrapped command has run.

Positive evidence beats enumeration. sudo's ways of refusing are open-ended --
localised, version-dependent, distro-patched, and it may not be installed at all
-- so a guard keyed on recognising failure will always have a gap, and each gap
is a target reported as empty. A guard keyed on recognising SUCCESS has none: if
the sentinel is absent, the shell did not run, full stop.
"""


def _elevation_proven(stderr: str) -> bool:
    """Whether an elevated shell demonstrably ran, i.e. printed the sentinel ALONE.

    Whole-line equality, never a substring. sudo's authorization refusal quotes
    the entire refused command back, so the marker appears in stderr even though
    nothing ran -- measured verbatim against real sudo 1.9.13p3, 1.9.15p5 and
    1.9.16p2:

        Sorry, user bbng is not allowed to execute '/usr/bin/sh -c find ... ;
        echo __BBNG_ELEVATED__ >&2; exit $__bbng_rc' as root on <host>.

    A substring test reads that as proof of success and reports a populated
    target as empty -- the sentinel defeating itself. It is reachable under an
    ordinary hardening policy, because this code elevates via `sudo -n sh -c`
    and a service account is commonly denied shells:

        backup ALL=(ALL) NOPASSWD: ALL, !/usr/bin/sh, !/bin/sh

    Only a line that is exactly the sentinel can have come from the echo this
    module appends, so that is what is tested.
    """
    return any(
        line.strip() == _ELEVATION_SENTINEL for line in (stderr or "").splitlines()
    )


def _strip_sentinel(stderr: str) -> str:
    """Remove the sentinel so it never reaches a user-facing message or log."""
    return "\n".join(
        line
        for line in (stderr or "").splitlines()
        if line.strip() != _ELEVATION_SENTINEL
    ).strip()


def _is_sudo_denial(stderr: str) -> bool:
    """Whether stderr shows sudo refusing to elevate, rather than the command failing.

    Matched on sudo's own diagnostics because the exit status cannot distinguish
    them: sudo exits 1 on an authentication refusal, and ``find`` also exits 1
    for perfectly ordinary reasons. Only the text separates "we were not allowed
    to look" from "we looked and there was nothing".

    ``sudo -n`` makes this reliable -- without it, sudo tries to prompt on a
    connection that has no tty and reports "a terminal is required" instead, or
    hangs.

    Matching English wording is safe here ONLY because :meth:`_elevate` pins
    ``LC_ALL=C``: sudo localises these messages, and the same refusal otherwise
    reads "Ein Passwort ist notwendig", "il est nécessaire de saisir un mot de
    passe" or "パスワードが必要です" (all measured on a real host). Without that
    pin, an English-only match would restore the false all-clear for every
    server not configured in English. The two must stay together.

    Matching the ``sudo:`` prefix instead was tried and is wrong in BOTH
    directions:

    * sudo prints diagnostics about itself and then runs the command anyway --
      "unable to load /usr/lib64/libsss_sudo.so" and "unable to initialize SSS
      source" on any RHEL/Fedora host whose nsswitch.conf still lists ``sss``
      after sssd was removed, and "setrlimit(RLIMIT_CORE)" inside containers.
      Reading those as refusals turns a working elevation into a hard abort.
    * the authorization denials do not all carry the prefix: sudo's own catalog
      has "Sorry, user %s is not allowed to execute ..." and "%s is not in the
      sudoers file." So a genuinely denied user would slip through, which is the
      very false all-clear this exists to prevent.

    Callers must also require a NON-ZERO exit before consulting this. A command
    that succeeded was obviously permitted, whatever sudo muttered on the way.

    SCOPE. The listing path no longer depends on this at all -- it uses the
    sentinel, which cannot be defeated by an unenumerated wording. This remains
    the classifier for the three sites that run a single binary through
    :meth:`_elevate` rather than a wrapped shell, and so have no sentinel:
    ``sidecar_exists``, the per-stream stat, and the delete loop. There the two
    ways to be wrong are NOT symmetric, which decides the ambiguous cases below:

    * calling a real denial benign is silent and destructive -- ``sidecar_exists``
      would report "no sidecar" and let backfill OVERWRITE a real record;
    * calling a benign warning a denial aborts loudly, visibly, and recoverably.

    That asymmetry argues for erring toward "denial" -- but only where sudo's
    behaviour is genuinely unknown, never against measurement. Two messages were
    on the deny side for exactly that reason and have been removed, because
    measurement settled them: with real sudo in stock Debian (1.9.13p3) and
    Ubuntu (1.9.15p5) containers, with no ``Defaults fqdn`` configured, a host
    whose name does not resolve emits "sudo: unable to resolve host <name>" and
    then RUNS the command; "unable to send audit message" is likewise non-fatal
    by default (``ignore_logfile_errors`` is on). Treating either as a refusal
    turned an ordinary command failure into a hard abort that blamed a sudoers
    policy which was already correct.

    So: only messages that mean sudo did NOT run the command belong here.
    """
    lowered = (stderr or "").lower()
    return any(
        marker in lowered
        for marker in (
            # Authentication refused (sudo -n, no tty, or a wrong password).
            "a password is required",
            "a terminal is required",
            "no askpass",
            "no tty present",
            "incorrect password",
            "authentication failure",
            "sorry, try again",
            # Authorization refused: this user may not run this command.
            "not allowed to execute",
            "is not in the sudoers file",
            "may not run sudo",
            "not allowed to run sudo",
            # Refused before exec, from sudo's own catalog. These were missed by
            # the first cut and each one made a populated target list as empty:
            # a requiretty policy, a syntactically broken sudoers, a root-denying
            # sudoers, and a sudo binary that cannot elevate at all.
            "must have a tty to run sudo",
            "no valid sudoers sources",
            "root is not allowed to sudo",
            "effective uid is not 0",
            # Not sudo speaking: the shell reporting sudo is absent. Elevation is
            # equally impossible, so it belongs here rather than in the routine
            # bucket -- an earlier test wrongly pinned this as an ordinary failure.
            "sudo: command not found",
        )
    )


def _check_remote_listing(
    result: subprocess.CompletedProcess,
    host: str,
    path: Any,
    *,
    elevated: bool = False,
) -> None:
    """Guard a remote enumeration so a CONNECTION failure is never reported as an
    empty target.

    A raw+ssh ``list``/``verify`` that cannot reach the host must NOT silently return
    "0 snapshots" / "all ok" -- that is a false all-clear that hides a down server (or
    makes lost backups look intentionally absent). ``ssh`` exits 255 on its own
    connection/auth/DNS failures, so that is raised as a clear error. A ``find`` on an
    empty but reachable directory exits 0 (a genuinely empty target); any other
    non-zero (e.g. a missing directory) is logged and treated as "no snapshots" rather
    than swallowed silently.

    ``elevated`` says the command was wrapped by :meth:`SSHRawEndpoint._elevate_shell`,
    which appends a sentinel that only prints if the wrapped shell actually ran. Its
    ABSENCE proves elevation failed, whatever sudo said and whatever it exited with --
    so this needs no list of sudo's failure messages, and cannot be defeated by a
    refusal wording nobody enumerated. Trying to enumerate them was the previous
    approach and it missed, among others, "you must have a tty to run sudo"
    (requiretty), "no valid sudoers sources found" (a broken sudoers), and
    "sudo: command not found" -- each of which made ``raw verify`` report
    "0 ok, 0 corrupt" and exit 0 for a target full of backups it never read.
    """
    stderr = (result.stderr or "").strip()
    if result.returncode == 255 and not _elevation_proven(result.stderr or ""):
        raise RuntimeError(
            f"Cannot reach raw+ssh target {host}: "
            f"{_strip_sentinel(stderr) or 'ssh connection failed'}. "
            "Its backups could NOT be listed -- this is NOT an empty target. Check the "
            "host is up and reachable, then retry."
        )
    # Only then elevation: a 255 above is the transport, and its sentinel is
    # legitimately absent because the remote never ran anything. Checking
    # elevation first would tell an operator whose host is simply DOWN that
    # their sudoers policy is wrong.
    if elevated and not _elevation_proven(result.stderr or ""):
        raise RuntimeError(
            f"Cannot list raw+ssh target {path} on {host}: the remote command was "
            f"never run"
            + (f" ({_strip_sentinel(stderr)})" if stderr else "")
            + ". Its backups could NOT be read -- this is NOT an empty target. "
            "ssh_sudo is enabled, and a raw+ssh target stores plain files, so the "
            "remote user needs passwordless sudo for the FILE tools (find, cat, "
            "stat, mkdir, mv, rm), not for btrfs. Either grant those in sudoers, "
            "or -- simpler and safer -- give the user ownership of the backup "
            "directory (chown/setfacl) and turn ssh_sudo off."
        )
    if result.returncode == 0:
        return
    # Invariant: 255 is attributed to an ssh transport/auth/DNS failure. This relies on
    # the remote enumeration command never itself exiting 255 -- true for the commands
    # here (find exits 0/1/2, or 127 if absent; sudo exits 1 on auth failure). Since
    # _elevate_shell re-raises the INNER status as the wrapper's own, the sentinel is
    # also required: if the remote shell demonstrably ran, a 255 came from the command,
    # not from the transport, and must not be reported as an unreachable host. ssh's
    # default BatchMode + ConnectTimeout (see _build_ssh_command) make a down or
    # black-holing host reach this path fast rather than hanging.
    # Anything still non-zero means the SEARCH ITSELF did not complete. find exits
    # 0 whenever it finished looking -- including over an empty directory -- so a
    # non-zero status is never "there are no backups", it is "we do not know what
    # is there". Reporting it as an empty target is the false all-clear this guard
    # exists to prevent, and it was reachable on both paths: unelevated, a
    # root-owned target gave rc=1 and `raw verify` printed "0 ok, 0 corrupt" and
    # exited 0; elevated, the same happened once the sentinel proved the shell ran.
    #
    # Deliberately no sudo-message classification: with the sentinel present the
    # command demonstrably ran, and re-guessing from sudo's text overruled that
    # proof. Measured with real sudo in stock Debian and Ubuntu containers (no
    # `Defaults fqdn`), a host whose name does not resolve emits "sudo: unable to
    # resolve host <name>" and runs the command anyway.
    raise RuntimeError(
        f"Cannot list raw+ssh target {path} on {host}: the listing command failed "
        f"(exit {result.returncode})"
        + (f": {_strip_sentinel(stderr)}" if _strip_sentinel(stderr) else "")
        + ". Its backups could NOT be enumerated -- this is NOT an empty target. "
        "A directory that exists and is readable returns exit 0 even when it holds "
        "nothing, so check that the path is correct, present, and readable by the "
        "user connecting (or by root, if ssh_sudo is set)."
    )


def _sha256_file(path: Path) -> str | None:
    """Return the hex sha256 of ``path``'s bytes, or None on any I/O error.

    Best-effort: a checksum failure must never fail an already-durable backup. On
    Linux, the file's page cache is dropped first (POSIX_FADV_DONTNEED) so the read
    comes from the physical medium -- verifying the bytes that actually landed on
    disk after fsync (catching write-side/media corruption a warm-cache read would
    miss) -- without evicting other data from the cache."""
    try:
        h = hashlib.sha256()
        # O_NOFOLLOW: never hash through a symlink at the final path component --
        # a backfill walking an untrusted directory must not be tricked into
        # hashing (and, with --json, disclosing the digest of) an arbitrary file
        # via a planted <name>.btrfs symlink.
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as f:
            fadvise = getattr(os, "posix_fadvise", None)
            dontneed = getattr(os, "POSIX_FADV_DONTNEED", None)
            if fadvise is not None and dontneed is not None:
                try:
                    fadvise(f.fileno(), 0, 0, dontneed)
                except OSError:
                    pass
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError as e:
        # Translate the errno to plain language: with O_NOFOLLOW a symlink stream
        # surfaces ELOOP, whose default text ("Too many levels of symbolic links")
        # wrongly implies a loop rather than "this is a symlink, refused for safety".
        logger.warning("Could not checksum %s: %s", path, _open_failure_reason(e))
        return None


def _popen_pipeline_pipefail(shell_cmd: str, **popen_kwargs: Any) -> subprocess.Popen:
    """Run a multi-stage shell pipeline with ``pipefail``.

    Without ``pipefail`` a shell pipeline's exit status is that of its LAST stage
    only, so a failure of an upstream stage -- ``btrfs send`` dying, or a
    compressor/``gpg`` erroring mid-stream -- is masked by the final redirect/ssh
    exiting 0, and a truncated or empty stream file is reported as a successful
    backup. ``set -o pipefail`` makes any stage's failure fail the whole pipeline
    so the returncode the caller checks is honest.

    Uses bash (which supports ``pipefail``); falls back to plain ``sh`` with a
    warning only when bash is unavailable.
    """
    bash_path = shutil.which("bash")
    if bash_path:
        return subprocess.Popen(
            "set -o pipefail; " + shell_cmd,
            shell=True,
            executable=bash_path,
            **popen_kwargs,
        )
    logger.warning(
        "bash not found; running raw pipeline without pipefail (a mid-pipe "
        "failure may be masked and produce a truncated backup)"
    )
    return subprocess.Popen(shell_cmd, shell=True, **popen_kwargs)


class RawEndpoint(Endpoint):
    """Endpoint that writes btrfs send streams to files.

    This endpoint writes raw btrfs send streams to files with optional
    compression and/or GPG encryption. Useful for backing up to non-btrfs
    filesystems or creating encrypted archive backups.

    Config options:
        path: Output directory for stream files
        compress: Compression algorithm (gzip, zstd, lz4, xz, lzo, pigz, pbzip2)
        encrypt: Encryption method (gpg, openssl_enc)
        gpg_recipient: GPG key recipient (required if encrypt=gpg)
        gpg_keyring: Optional path to GPG keyring
        openssl_cipher: OpenSSL cipher (default: aes-256-cbc)
        snap_prefix: Prefix for snapshot names

    Environment variables for openssl_enc:
        BTRFS_BACKUP_PASSPHRASE: Encryption passphrase
        BTRBK_PASSPHRASE: Fallback for btrbk compatibility
    """

    def __init__(self, config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        """Initialize the RawEndpoint.

        Args:
            config: Configuration dictionary
            **kwargs: Additional keyword arguments
        """
        config = config or {}
        super().__init__(config, **kwargs)

        # Raw-specific configuration
        self.compress = config.get("compress")
        # "none" (the config sentinel) and None both mean no compression; normalize
        # so a caller threading compress="none" is not treated as an unknown
        # algorithm by the validation below (mirrors the encrypt handling).
        if self.compress == "none":
            self.compress = None
        self.encrypt = config.get("encrypt")
        # "none" (the documented string) and None both mean plaintext; normalize
        # so callers threading encrypt="none" do not trip the method validation.
        if self.encrypt == "none":
            self.encrypt = None
        self.gpg_recipient = config.get("gpg_recipient")
        self.gpg_keyring = config.get("gpg_keyring")
        # Validated at construction so a bad cipher fails fast rather than
        # surfacing as a cryptic openssl error mid-transfer. An explicit None or
        # "" (the CLI threads openssl_cipher=None for gpg/plaintext targets) means
        # "unset" -> the aes-256-cbc default, exactly as an absent key would.
        self.openssl_cipher = _validate_cipher(
            config.get("openssl_cipher") or "aes-256-cbc"
        )

        # How long a mutating op waits for the per-target lock before reporting the
        # target busy. The base __init__ only keeps known keys, so register it here.
        # A generous default: the commit critical section is sub-second, but slow
        # storage (NFS/SMB) can make a legitimate peer's own commit take longer.
        try:
            self.config["lock_timeout"] = float(config.get("lock_timeout", 30.0))
        except (TypeError, ValueError):
            raise ValueError("lock_timeout must be a number of seconds") from None
        if self.config["lock_timeout"] < 0:
            raise ValueError("lock_timeout must not be negative")

        # Validate encryption config
        if self.encrypt == "gpg" and not self.gpg_recipient:
            raise ValueError("gpg_recipient is required when encrypt=gpg")

        if self.encrypt == "openssl_enc":
            # Check for passphrase in environment
            if not self._get_openssl_passphrase():
                logger.warning(
                    "openssl_enc requires passphrase in %s or %s environment variable",
                    OPENSSL_PASSPHRASE_ENV,
                    BTRBK_PASSPHRASE_ENV,
                )

        # Validate encryption method
        valid_encrypt = {None, "gpg", "openssl_enc"}
        if self.encrypt not in valid_encrypt:
            raise ValueError(
                f"Unknown encryption method: {self.encrypt}. "
                f"Valid options: gpg, openssl_enc"
            )

        # Validate compression algorithm
        if self.compress and self.compress not in COMPRESSION_CONFIG:
            valid = ", ".join(sorted(COMPRESSION_CONFIG.keys()))
            raise ValueError(
                f"Unknown compression algorithm: {self.compress}. Valid options: {valid}"
            )

        # Cache for discovered snapshots
        self._cached_snapshots: list[RawSnapshot] | None = None

        # Pending metadata during receive operation (initialized with dummy values)
        self._pending_metadata: PendingMetadata = {
            "name": "",
            "stream_path": Path(),
            "part_path": Path(),
            "parent_name": None,
            "compress": None,
            "encrypt": None,
            "gpg_recipient": None,
            "openssl_cipher": None,
        }

    def _get_openssl_passphrase(self) -> str | None:
        """Get OpenSSL passphrase from environment.

        Checks BTRFS_BACKUP_PASSPHRASE first, then BTRBK_PASSPHRASE for
        btrbk compatibility. Shares ``_selected_passphrase_env`` with the pipeline
        ``-pass`` argument so the two never disagree about which variable is used.

        Returns:
            Passphrase string or None if not set
        """
        name = _selected_passphrase_env()
        return os.environ.get(name) if name else None

    def __repr__(self) -> str:
        parts = [f"raw://{self.config['path']}"]
        if self.compress:
            parts.append(f"compress={self.compress}")
        if self.encrypt:
            parts.append(f"encrypt={self.encrypt}")
        return f"<RawEndpoint {' '.join(parts)}>"

    def get_id(self) -> str:
        """Return a unique identifier for this endpoint."""
        path = self._normalize_path(self.config["path"])
        return f"raw://{path}"

    def correspondent_of(self, snapshot: Any) -> Optional[Any]:
        """Raw override of :meth:`Endpoint.correspondent_of` -- NAME semantics.

        A raw backup is a self-contained ``btrfs send`` stream written to a file; there is
        no ``btrfs receive`` on the destination and therefore no ``received_uuid`` to match.
        Correspondence for raw targets is by snapshot name (the sidecar also records the
        parent by name), which is correct because a raw stream is replayed by name/order at
        restore time. Returns the raw backup with the same name, or ``None`` (never raises).
        """
        get_name = getattr(snapshot, "get_name", None)
        if not callable(get_name):
            return None
        try:
            name = get_name()
            candidates = self.list_snapshots()
            for candidate in candidates:
                if candidate.get_name() == name:
                    return candidate
        except Exception as e:  # noqa: BLE001 - contract: never raise; None is safe
            logger.debug("correspondent_of: could not resolve correspondent (%s)", e)
        return None

    @contextlib.contextmanager
    def target_lock(self, *, timeout: float | None = None) -> Iterator[None]:
        """Hold an exclusive lock on the target directory for a MUTATING operation.

        Backup (commit), prune, ``raw backfill-metadata`` and ``raw encrypt`` on the
        same raw target all take this lock, so they are mutually exclusive -- closing
        the transient two-files-one-name window (e.g. a backfill mislabelling a native
        backup during its non-atomic stream-then-sidecar commit, or a prune racing a
        backfill).

        A bounded-blocking exclusive ``flock``: it waits up to ``timeout`` seconds for
        a peer to finish (so legitimate parallel commits to one target SERIALIZE rather
        than fail), then raises RuntimeError if still busy. ``timeout`` defaults to the
        ``lock_timeout`` config key (30s). The lock is released when the fd is closed
        and is auto-released if the process dies, so it can never go stale.

        Failure posture: a planted lock symlink (O_NOFOLLOW -> ELOOP), a lock that is
        a directory (EISDIR), a foreign-owned/non-writable lock file (EACCES), a
        filesystem that cannot flock (ENOLCK), a planted FIFO/named-pipe (O_NONBLOCK
        -> ENXIO instead of a permanent hang), and any non-regular lock target that
        still opens (a FIFO with a reader, a device, or a socket -- refused by an
        fstat check) all raise RuntimeError rather than an uncaught OSError -- so a
        hostile or mis-created lock file degrades to the same bounded fail/skip as
        contention instead of crashing (or hanging) every backup. The lock lives
        inside the target directory, so that directory MUST NOT be writable by
        untrusted users. The raw+ssh subclass overrides this as a no-op (remote
        locking is a separate concern -- there is no persistent connection to hold an
        flock)."""
        if timeout is None:
            timeout = float(self.config.get("lock_timeout", 30.0))
        path = Path(self.config["path"])
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        lockfile = path / LOCK_FILENAME
        # Harden the lock-file open against a hostile/mis-created lock:
        #   O_NOFOLLOW  -- refuse a planted symlink (cannot redirect the often-root open)
        #   O_NONBLOCK  -- a planted FIFO/named-pipe returns ENXIO immediately instead of
        #                  blocking the open FOREVER waiting for a reader; without it a
        #                  single FIFO wedges every backup/prune/maintenance op silently
        #                  (a permanent DoS). No-op for a regular file.
        # Any open failure is mapped to a bounded RuntimeError (with a plain-language
        # reason) so it can never escape as an uncaught OSError.
        try:
            fd = os.open(
                lockfile,
                os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
                0o600,
            )
        except OSError as e:
            raise RuntimeError(
                f"raw target {path}: cannot acquire its lock file {lockfile} -- "
                f"{_open_failure_reason(e)}. The target directory must not be writable by "
                "untrusted users."
            ) from e
        # Even if the open succeeded, refuse to trust anything that is not a REGULAR
        # file: a FIFO that happened to have a reader, or a device/socket planted as
        # the lock, must not be used to coordinate (or for any I/O).
        try:
            is_regular = stat.S_ISREG(os.fstat(fd).st_mode)
        except OSError as e:
            # Map to RuntimeError like every other branch here, so a stat failure on
            # an exotic/hostile lock degrades to the bounded fail/skip posture rather
            # than escaping as an uncaught OSError (which the prune path would not
            # catch, and the CLI would misreport on stdout).
            os.close(fd)
            raise RuntimeError(
                f"raw target {path}: cannot stat its lock file {lockfile} -- "
                f"{_open_failure_reason(e)}"
            ) from e
        if not is_regular:
            os.close(fd)
            raise RuntimeError(
                f"raw target {path}: lock file {lockfile} is not a regular file (a "
                "FIFO, device, or socket may have been planted); refusing to use it"
            )
        deadline = time.monotonic() + max(0.0, timeout)
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError as e:
                    # EAGAIN/EWOULDBLOCK (BlockingIOError) is the real contention
                    # signal -- retry until the deadline. Any other errno (e.g. ENOLCK
                    # from a filesystem that cannot flock) will never clear, so fail
                    # immediately with an accurate message instead of polling for the
                    # full timeout and mislabelling it "busy".
                    if e.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                        raise RuntimeError(
                            f"raw target {path}: cannot lock {lockfile} ({e}); the "
                            "filesystem may not support flock"
                        ) from e
                    if time.monotonic() >= deadline:
                        raise RuntimeError(
                            f"raw target {path} is busy (another operation holds the "
                            "lock); retry when it finishes"
                        ) from None
                    time.sleep(0.2)
            yield
        finally:
            os.close(fd)  # releases the flock

    def _prepare(self) -> None:
        """Prepare the endpoint for use."""
        path = Path(self.config["path"])
        if not path.exists():
            logger.info("Creating raw target directory: %s", path)
            path.mkdir(parents=True, exist_ok=True, mode=0o700)

        # Fail loud (before any transfer) with an actionable message if a required
        # compression/encryption tool is missing, instead of a raw errno part-way
        # through the send pipeline.
        missing = self._check_tools()
        if missing:
            raise __util__.AbortError(
                f"Cannot back up to raw target {path}: the required tool(s) "
                f"{', '.join(missing)} are not installed. Install them (or change the "
                "compress/encrypt method) and retry."
            )

    def _check_tools(self) -> list[str]:
        """Check that required tools are available.

        Returns:
            List of missing tools (empty if all present)
        """
        missing = []

        # Check compression tool
        if self.compress:
            config = COMPRESSION_CONFIG.get(self.compress, {})
            cmd = config.get("compress_cmd", [])
            if cmd and not shutil.which(cmd[0]):
                missing.append(cmd[0])

        # Check GPG
        if self.encrypt == "gpg" and not shutil.which("gpg"):
            missing.append("gpg")

        # Check OpenSSL
        if self.encrypt == "openssl_enc" and not shutil.which("openssl"):
            missing.append("openssl")

        if missing:
            logger.warning("Missing tools for raw endpoint: %s", ", ".join(missing))

        return missing

    def receive(
        self, stdin: Any, snapshot_name: str = "", parent_name: str | None = None
    ) -> Any:
        """Write a btrfs send stream to a file.

        Unlike the standard Endpoint.receive(), this writes the stream to a file
        instead of piping to 'btrfs receive'.

        Args:
            stdin: Input stream (from btrfs send)
            snapshot_name: Name for the snapshot file
            parent_name: Parent snapshot name (for metadata)

        Returns:
            Popen object for the pipeline
        """
        if not snapshot_name:
            raise ValueError("snapshot_name is required for raw endpoint receive")

        # Build output filename
        extension = get_file_extension(self.compress, self.encrypt)
        output_path = Path(self.config["path"]) / f"{snapshot_name}{extension}"
        # Write to a temporary ".part" sibling; commit_receive() renames it to
        # output_path only after the engine confirms the pipeline succeeded.
        #
        # The name carries this transfer's pid and a monotonic stamp. Deriving it
        # from the snapshot name alone gave two concurrent runs against the same
        # target -- a cron run overlapping a manual one -- the SAME temp file, and
        # nothing serialized the write: target_lock is taken only around the rename
        # and sidecar. Their bytes interleaved into one published stream whose
        # sha256 was then sealed over the corruption, so both processes exited 0,
        # the engine's return-code gate passed, `raw verify` reported ok, and the
        # damage surfaced only at restore.
        part_path = Path(
            f"{output_path}.{os.getpid()}.{time.monotonic_ns():x}{PARTIAL_SUFFIX}"
        )

        logger.info("Writing raw stream to: %s", part_path)

        # Record metadata BEFORE executing: _execute_pipeline reads
        # _pending_metadata["part_path"] to know where to write. Setting it
        # afterwards left the default Path() ('.') in place, so the pipeline
        # tried to open the current directory as the output file.
        self._pending_metadata = {
            "name": snapshot_name,
            "stream_path": output_path,
            "part_path": part_path,
            "parent_name": parent_name,
            "compress": self.compress,
            "encrypt": self.encrypt,
            "gpg_recipient": self.gpg_recipient,
            # Only meaningful for openssl_enc; recorded so restore uses the exact
            # cipher instead of guessing aes-256-cbc.
            "openssl_cipher": (
                self.openssl_cipher if self.encrypt == "openssl_enc" else None
            ),
        }

        # Build and execute the pipeline (writes to the .part file)
        pipeline = self._build_receive_pipeline(part_path)
        proc = self._execute_pipeline(pipeline, stdin)

        return proc

    def _build_receive_pipeline(self, output_path: Path) -> list[list[str]]:
        """Build the compression/encryption pipeline for receiving.

        Args:
            output_path: Final output file path

        Returns:
            List of command lists to be piped together
        """
        pipeline: list[list[str]] = []

        # Compression stage
        if self.compress:
            config = COMPRESSION_CONFIG.get(self.compress, {})
            cmd = config.get("compress_cmd", [])
            if cmd:
                pipeline.append(list(cmd))

        # Encryption stage
        if self.encrypt == "gpg" and self.gpg_recipient:
            gpg_cmd: list[str] = ["gpg", "--encrypt", "--recipient", self.gpg_recipient]
            if self.gpg_keyring:
                gpg_cmd.extend(["--keyring", self.gpg_keyring])
            # Suppress GPG output
            gpg_cmd.extend(["--batch", "--quiet"])
            pipeline.append(gpg_cmd)
        elif self.encrypt == "openssl_enc":
            # OpenSSL symmetric encryption (btrbk compatible)
            # Uses -pbkdf2 for secure key derivation
            openssl_cmd = [
                "openssl",
                "enc",
                f"-{self.openssl_cipher}",
                "-salt",
                "-pbkdf2",
                "-pass",
                _openssl_pass_arg(),
            ]
            pipeline.append(openssl_cmd)

        # Final output stage - write to file
        # If no compression/encryption, just cat to file
        # Otherwise the last stage pipes to file via shell redirection
        if not pipeline:
            # No processing, just copy stdin to file
            pipeline.append(["cat"])

        return pipeline

    def _execute_pipeline(
        self, pipeline: list[list[str]], stdin: Any
    ) -> subprocess.Popen:
        """Execute a pipeline of commands.

        Args:
            pipeline: List of command lists
            stdin: Input stream

        Returns:
            The final Popen object in the pipeline
        """
        if not pipeline:
            raise ValueError("Empty pipeline")

        # For a single command, execute directly
        if len(pipeline) == 1:
            output_path = self._pending_metadata["part_path"]
            fd = self._open_part_file(output_path)
            try:
                proc = subprocess.Popen(
                    pipeline[0],
                    stdin=stdin,
                    stdout=fd,
                    stderr=subprocess.PIPE,
                )
            finally:
                os.close(fd)
            return proc

        # For multiple commands, chain them together
        # We use shell to handle the pipeline and file output. Quote every argv
        # element (a gpg recipient/keyring or cipher may contain spaces or shell
        # metacharacters) so nothing word-splits or injects into the shell string.
        output_path = self._pending_metadata["part_path"]
        cmd_strs = [" ".join(shlex.quote(a) for a in cmd) for cmd in pipeline]
        # No `> path` redirect: the shell would re-open the destination by name,
        # reintroducing the symlink race that _open_part_file exists to close.
        # The pipeline's last stage inherits the already-guarded descriptor.
        shell_cmd = " | ".join(cmd_strs)

        logger.debug("Executing pipeline: %s", shell_cmd)

        part_fd = self._open_part_file(output_path)
        try:
            proc = _popen_pipeline_pipefail(
                shell_cmd,
                stdin=stdin,
                stdout=part_fd,
                stderr=subprocess.PIPE,
            )
        finally:
            os.close(part_fd)
        return proc

    @staticmethod
    def _open_part_file(part_path: Path) -> int:
        """Open this transfer's ``.part`` file, refusing to follow or reuse anything.

        ``O_EXCL`` means the file must not already exist, so a concurrent transfer
        cannot be handed the same descriptor, and ``O_NOFOLLOW`` means a symlink
        planted at the path is refused rather than followed -- which, running as
        root against a target directory that untrusted users can write, would
        otherwise truncate whatever the link pointed at.

        Mode 0600 matches every other durable artifact this endpoint writes
        (``__util__.atomic_write_bytes`` and the ``.meta`` sidecars). The stream
        previously landed at the umask default, typically 0644, which left the
        most sensitive file the least protected.
        """
        return os.open(
            part_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )

    @staticmethod
    def _fsync_dir(directory: Path) -> None:
        """Best-effort fsync of a directory so a rename into it is durable.

        Delegates to the single shared implementation so the durability primitive
        has one definition."""
        _fsync_directory(directory)

    def commit_receive(self) -> None:
        """Atomically publish the received stream after a successful transfer.

        The receive pipeline writes to a ``.part`` file; only once the engine
        has confirmed the pipeline exited 0 do we fsync that file, atomically
        rename it to its final name, and fsync the directory so the rename is
        durable. A crash before this point leaves only the ``.part`` file, which
        ``discover_raw_snapshots`` ignores -- so a partial transfer can never be
        mistaken for a complete backup.

        Raises on failure so the engine treats an un-published stream as a
        failed transfer rather than reporting a success that is not on disk.
        """
        pending = getattr(self, "_pending_metadata", None)
        # No receive() has run on this endpoint (dummy init) -> nothing to publish.
        if not pending or not pending.get("name"):
            return
        part_path = Path(pending["part_path"])
        final_path = Path(pending["stream_path"])
        if not part_path.exists():
            # The stream we just received is gone; fail loud rather than report a
            # success with no file on disk -- the exact phantom-success class this
            # atomic-write scheme exists to prevent.
            raise RuntimeError(
                f"commit_receive: received stream {part_path} is missing; "
                f"cannot publish {final_path}"
            )
        # Flush the stream's bytes to disk BEFORE renaming, so the final name can
        # never refer to unflushed data. Done OUTSIDE the lock: the ``.part`` name
        # carries this transfer's pid and monotonic stamp and was created O_EXCL, so
        # no peer can touch it, and this fsync can take a long time on a multi-GB
        # stream -- holding the lock across it would make a legitimately parallel
        # commit exceed the wait and FAIL instead of serialize.
        fd = os.open(part_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        # Hash the ``.part`` now too (also outside the lock, same slow-read reason).
        # ``os.replace`` is a pure rename, so the committed stream's bytes are
        # byte-identical to the ``.part`` -- this sha256 describes the final file, and
        # ``_sha256_file`` drops the page cache (POSIX_FADV_DONTNEED) after the fsync
        # above so it reflects the bytes actually on disk. Best-effort: None on error.
        checksum = _sha256_file(part_path)
        # Only the rename (which makes the stream visible under its shared final name)
        # through the sidecar write must be mutually exclusive, so a concurrent
        # backfill/prune cannot observe the stream in the window after the rename but
        # before the sidecar is written (and mislabel it). This section is sub-second
        # (a metadata rename + a directory fsync + a small sidecar write).
        with self.target_lock():
            os.replace(part_path, final_path)
            # Persist the rename itself.
            self._fsync_dir(final_path.parent)
            # Write the authoritative sidecar now that the stream is durable at its
            # final name. Written last + atomically, so a crash yields at most a
            # stream-without-sidecar (discovery falls back to filename inference),
            # never a sidecar describing a missing/partial stream. Best-effort: the
            # backup data already succeeded, so a sidecar error must not fail it.
            try:
                size = final_path.stat().st_size
                self.write_sidecar(self._sidecar_snapshot(final_path, size, checksum))
            except Exception as e:
                # The backup data is already durable; a sidecar error must NEVER flip
                # an already-successful transfer into a reported failure (PR1
                # contract). A missing sidecar just degrades to filename inference.
                logger.warning("Failed to write sidecar for %s: %s", final_path, e)
        self._cached_snapshots = None  # re-discover to include the new sidecar
        logger.debug("Committed raw stream + sidecar: %s", final_path)

    def write_sidecar(self, snapshot: RawSnapshot) -> None:
        """Persist a snapshot's authoritative ``.meta`` sidecar.

        The single sidecar-write entry point shared by the transfer engine
        (``commit_receive``) and the raw maintenance commands, so every sidecar --
        whatever its ``provenance_origin`` (native-write, backfill, remediation) --
        is written the same atomic, 0600 way. Local endpoints write it directly
        (see ``RawSnapshot.save_metadata``); the raw+ssh subclass overrides this to
        write on the remote. Raises on failure; callers that must not fail an
        already-durable backup on a sidecar error wrap the call (as the engine
        does)."""
        snapshot.save_metadata()

    def compute_stream_checksum(self, snapshot: RawSnapshot) -> str | None:
        """Return the CURRENT sha256 of ``snapshot``'s stream file, or None if it
        cannot be read. ``raw verify`` recomputes this and compares it against the
        sidecar's recorded ``checksum_value`` to detect corruption. The raw+ssh
        subclass overrides this to hash on the remote host (no re-download)."""
        return _sha256_file(snapshot.stream_path)

    # False for a local raw:// target (we hash with this host's own kernel, so an
    # ``ok`` is tamper-evident). Overridden to True on the raw+ssh subclass, where the
    # digest is computed BY the untrusted remote (corruption-detection only).
    _checksum_is_remote: bool = False

    def verify_stream_checksum(self, snapshot: RawSnapshot) -> ChecksumVerdict:
        """Verify ``snapshot``'s stream against its sealed sha256 and classify the
        result. THE single checksum-verification path -- ``raw verify``, the
        restore-time integrity guard, and the general ``verify`` command all call this,
        so the ok/corrupt/error/unverifiable taxonomy cannot drift between them.

        Never raises (a report collects findings; it does not abort mid-scan): an
        unreadable stream is ``error``, a missing/non-sha256 checksum is
        ``unverifiable``, and the caller decides how severe each is. Skips the recompute
        entirely for the unverifiable cases -- there is nothing to compare against, so
        there is no reason to pay the read.

        Dispatch is polymorphic via ``compute_stream_checksum`` (local ``_sha256_file``
        for raw://, remote ``_remote_sha256`` for raw+ssh); ``remote_untrusted`` records
        which, so a raw+ssh verdict can be labelled consistency-only."""
        recorded = snapshot.checksum_value
        algorithm = getattr(snapshot, "checksum_algorithm", "sha256")
        remote = self._checksum_is_remote
        if not recorded:
            # Legacy backup, or a best-effort write-time seal that failed to record.
            return ChecksumVerdict("unverifiable", recorded, None, algorithm, remote)
        if algorithm != "sha256":
            # We only recompute sha256; comparing it to a digest of another algorithm
            # would false-flag an intact stream as corrupt. Cannot check.
            return ChecksumVerdict("unverifiable", recorded, None, algorithm, remote)
        computed = self.compute_stream_checksum(snapshot)
        if computed is None:
            # The recorded checksum exists and is sha256, but the current stream on
            # disk (or on the remote) could not be read/hashed.
            return ChecksumVerdict("error", recorded, None, algorithm, remote)
        if computed == recorded:
            return ChecksumVerdict("ok", recorded, computed, algorithm, remote)
        return ChecksumVerdict("corrupt", recorded, computed, algorithm, remote)

    def verify_structure(self, snapshot: RawSnapshot) -> StructureVerdict:
        """Confirm ``snapshot`` is a raw backup with AUTHORITATIVE metadata (the ``verify``
        metadata-level structural check for a raw target). The stream file itself exists by
        virtue of discovery; the structural question is whether it carries a real ``.meta``
        sidecar:

        - a native/backfilled/remediation sidecar (``provenance_origin`` is anything but
          ``filename-inferred``) -> ``ok``: authoritative metadata is present.
        - a filename-inferred stream (no ``.meta`` sidecar; metadata reconstructed from the
          filename) -> ``unverifiable``: its pipeline and parentage are guesses, not a
          failure but not a confirmed-authoritative backup either.

        Byte integrity (the sealed sha256) is the ``stream``/``full`` level's job
        (``verify_stream_checksum``); this level is the cheap structural check.
        """
        origin = getattr(snapshot, "provenance_origin", "native-write")
        if origin == "filename-inferred":
            return StructureVerdict(
                "unverifiable",
                "no .meta sidecar (metadata inferred from the filename -- run "
                "'raw backfill-metadata' to write an authoritative sidecar)",
            )
        return StructureVerdict("ok", f"authoritative sidecar (origin={origin})")

    def test_send_stream(self, snapshot: RawSnapshot, parent: Any = None) -> None:
        """Not applicable to a raw target: a raw backup is a STORED send stream (a file),
        not a subvolume, so ``btrfs send`` cannot run on it. Raw stream integrity is
        verified by its sealed sha256 (``verify_stream_checksum``), which is what the
        ``verify`` command uses for a raw target's stream/full levels. Raising keeps this
        honest if a caller ever routes a raw endpoint here (the CLI does not)."""
        from btrfs_backup_ng.core.verify import VerifyError

        raise VerifyError(
            "raw target: a stored stream is not a subvolume; its integrity is verified "
            "by checksum (raw verify / verify --level stream), not by 'btrfs send'"
        )

    def sidecar_exists(self, snapshot: RawSnapshot) -> bool:
        """Whether ``snapshot``'s ``.meta`` sidecar exists now. Used by
        ``raw backfill-metadata`` to re-check just before writing, so a sidecar that
        appeared since the scan (e.g. a backup committed concurrently) is not
        overwritten with a backfill record. The raw+ssh subclass tests the remote."""
        return snapshot.metadata_path.exists()

    def remediate_plaintext(
        self,
        snapshot: RawSnapshot,
        *,
        encrypt: str,
        gpg_recipient: str | None = None,
        gpg_keyring: str | None = None,
        openssl_cipher: str = "aes-256-cbc",
    ) -> RawSnapshot:
        """Write an ENCRYPTED copy of a plaintext ``snapshot``'s stream (the same
        bytes with an encryption layer added) atomically, plus its authoritative
        sidecar (``provenance_origin=remediation``, ``remediated_from`` audit ref).

        Does NOT touch the plaintext -- the caller removes it only after
        ``decrypt_matches_plaintext`` proves the encryption is reversible and only
        when the operator opted in. Returns the new RawSnapshot. Raises
        FileExistsError if the encrypted target already exists (never clobber a prior
        encrypted stream)."""
        if encrypt not in ("gpg", "openssl_enc"):
            # Defense in depth: a caller error must never produce a plaintext file
            # wearing an encrypted name/label (the GHSA-vr25 class this remediates).
            raise ValueError(
                f"remediate_plaintext requires a real encryption method, got {encrypt!r}"
            )
        orig = snapshot.stream_path
        ext = ".gpg" if encrypt == "gpg" else ".enc"
        enc_path = Path(str(orig) + ext)
        if enc_path.exists():
            raise FileExistsError(
                f"{enc_path} already exists; refusing to overwrite an existing "
                "encrypted stream"
            )
        part = Path(str(enc_path) + PARTIAL_SUFFIX)
        # A compress-less endpoint yields an ENCRYPT-ONLY argv (the plaintext bytes
        # are already whatever they are), reusing the PR4-hardened crypto command.
        enc_ep = RawEndpoint(
            config={
                "path": str(Path(self.config["path"])),
                "encrypt": encrypt,
                "gpg_recipient": gpg_recipient,
                "gpg_keyring": gpg_keyring,
                "openssl_cipher": openssl_cipher,
            }
        )
        pipeline = enc_ep._build_receive_pipeline(enc_path)
        if len(pipeline) != 1 or pipeline[0][:1] == ["cat"]:
            raise RuntimeError("internal: expected a single encrypt stage")
        encrypt_argv = pipeline[0]
        # Open the .part with O_NOFOLLOW|O_EXCL so a pre-planted <orig>.<ext>.part
        # symlink cannot redirect this (often root) write to an arbitrary file, and a
        # stale/hostile pre-existing .part cannot be reused -- matching the O_NOFOLLOW
        # hardening on save_metadata/_sha256_file for the untrusted-directory model.
        part_fd = os.open(
            part, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        )
        try:
            with open(orig, "rb") as stdin:
                proc = subprocess.Popen(
                    encrypt_argv, stdin=stdin, stdout=part_fd, stderr=subprocess.PIPE
                )
                _, err = proc.communicate()
        finally:
            os.close(part_fd)
        if proc.returncode != 0:
            try:
                part.unlink()
            except OSError:
                pass
            msg = err.decode(errors="replace").strip() if err else "encryption failed"
            raise RuntimeError(f"Encrypting {orig} failed: {msg}")
        # Atomic publish of the encrypted stream (fsync -> rename -> dir fsync).
        fd = os.open(part, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(part, enc_path)
        self._fsync_dir(enc_path.parent)
        new_snap = RawSnapshot(
            name=snapshot.name,
            stream_path=enc_path,
            parent_name=snapshot.parent_name,
            created=datetime.now(timezone.utc),
            size=enc_path.stat().st_size,
            compress=snapshot.compress,  # unchanged: the bytes were already compressed
            encrypt=encrypt,
            gpg_recipient=gpg_recipient,
            openssl_cipher=openssl_cipher if encrypt == "openssl_enc" else None,
            provenance_origin="remediation",
            stream_completeness=snapshot.stream_completeness,
            remediation_source=orig.name,
            checksum_value=_sha256_file(enc_path),
        )
        self.write_sidecar(new_snap)
        self._cached_snapshots = None  # a new stream now exists; re-discover on list
        return new_snap

    def decrypt_matches_plaintext(
        self, new_snapshot: RawSnapshot, plaintext_path: Path
    ) -> bool:
        """LIVE proof that ``new_snapshot``'s encrypted stream decrypts back to
        exactly ``plaintext_path``.

        Reverses ONLY the encryption (the verify snapshot has ``compress=None``), so
        the decrypt output must equal the original (possibly still-compressed)
        plaintext file byte for byte. For gpg this needs the secret key on THIS host;
        if it is absent the decrypt fails and this returns False -- so the plaintext
        is never removed on a host that cannot prove reversibility."""
        verify_snap = RawSnapshot(
            name=new_snapshot.name,
            stream_path=new_snapshot.stream_path,
            encrypt=new_snapshot.encrypt,
            compress=None,
            openssl_cipher=new_snapshot.openssl_cipher,
        )
        try:
            proc = self.send(verify_snap)
        except Exception:
            return False
        stdout = proc.stdout
        if stdout is None:
            return False
        # Any failure to PROVE reversibility -> False (so the plaintext is kept); an
        # I/O error here must never crash the batch or leave the decision ambiguous.
        try:
            h = hashlib.sha256()
            for chunk in iter(lambda: stdout.read(1024 * 1024), b""):
                h.update(chunk)
            stdout.close()
            if proc.stderr is not None:
                proc.stderr.read()  # drain (small) so wait() cannot deadlock
            if proc.wait() != 0:
                return False
            plaintext_hash = _sha256_file(plaintext_path)
            return plaintext_hash is not None and h.hexdigest() == plaintext_hash
        except OSError:
            return False

    def streams_without_sidecar(self) -> list[RawSnapshot]:
        """Return backfill candidates: RawSnapshots reconstructed from the filename
        for streams under this target that have NO ``.meta`` sidecar (legacy
        backups). Each is stamped ``provenance_origin=backfill`` and
        ``stream_completeness=unknown`` -- a legacy stream could be truncated, so a
        backfilled sidecar is never authoritative. The checksum is left None for the
        caller (``raw backfill-metadata``) to seal by hashing the stream. The raw+ssh
        subclass overrides this to scan the remote target."""
        path = Path(self.config["path"])
        if not path.exists():
            return []
        out: list[RawSnapshot] = []
        for item in sorted(path.iterdir()):
            # Skip symlinks: this scan writes a sidecar next to each candidate while
            # walking a directory of foreign/legacy content, so a symlinked "stream"
            # must not be treated as a real backup (defends the write + the hash).
            if item.is_symlink() or not item.is_file() or item.suffix == ".meta":
                continue
            if (
                item.name.endswith((".part", ".tmp", ".lock"))
                or ".btrfs" not in item.name
            ):
                continue
            if item.with_name(item.name + ".meta").exists():
                continue  # already has an authoritative sidecar
            parsed = parse_stream_filename(item.name)
            try:
                stat = item.stat()
            except OSError:
                # Raced away or unreadable: skip this one rather than abort the whole
                # backfill (mirrors the SSH path's stat-failure -> skip).
                continue
            out.append(
                RawSnapshot(
                    name=parsed["name"],
                    stream_path=item,
                    created=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                    size=stat.st_size,
                    compress=parsed["compress"],
                    encrypt=parsed["encrypt"],
                    provenance_origin="backfill",
                    stream_completeness="unknown",
                )
            )
        return out

    def _sidecar_snapshot(
        self, final_path: Path, size: int, checksum_value: str | None = None
    ) -> RawSnapshot:
        """Build the authoritative RawSnapshot to persist for a just-committed
        stream, from the pending receive metadata. openssl_cipher is recorded so
        restore uses the exact cipher; ``checksum_value`` is the sha256 of the
        committed ciphertext (None if it could not be computed -- best-effort)."""
        pending = self._pending_metadata
        return RawSnapshot(
            name=pending["name"],
            stream_path=final_path,
            parent_name=pending.get("parent_name"),
            created=datetime.now(timezone.utc),
            size=size,
            compress=pending.get("compress"),
            encrypt=pending.get("encrypt"),
            gpg_recipient=pending.get("gpg_recipient"),
            openssl_cipher=pending.get("openssl_cipher"),
            provenance_origin="native-write",
            checksum_value=checksum_value,
        )

    def send(
        self,
        snapshot: Any,
        parent: Any | None = None,
        clones: list[Any] | None = None,
    ) -> subprocess.Popen[bytes]:
        """Read and decompress/decrypt a raw stream for restore.

        Args:
            snapshot: The raw snapshot to restore (RawSnapshot)
            parent: Parent snapshot (unused, for API compatibility)
            clones: Clone snapshots (unused, for API compatibility)

        Returns:
            Popen object with stdout containing the decompressed/decrypted stream
        """
        if not isinstance(snapshot, RawSnapshot):
            raise TypeError(f"Expected RawSnapshot, got {type(snapshot)}")
        if not snapshot.stream_path.exists():
            raise FileNotFoundError(f"Stream file not found: {snapshot.stream_path}")

        pipeline = self._build_restore_pipeline(snapshot)
        self._preflight_restore_tools(pipeline, snapshot)
        self._verify_stream_integrity(snapshot)
        return self._execute_restore_pipeline(pipeline, snapshot.stream_path)

    def _verify_stream_integrity(self, snapshot: RawSnapshot) -> None:
        """Refuse to restore a stored stream that no longer matches the sha256 sealed
        when it was written -- so a corrupted/truncated backup is detected UP FRONT
        instead of being decoded into a corrupt subvolume (or, worse, partially
        applied). Reads the stream once to hash it (on the remote for raw+ssh, via
        the ``compute_stream_checksum`` override -- no re-download); the extra read is
        the price of not restoring silently-bad data.

        Skipped when the sidecar recorded no checksum (a legacy backup) or a non-sha256
        algorithm -- there is nothing to compare against, and the restore's own decode
        step still surfaces a genuinely unreadable stream. Also skipped (for last-copy
        recovery of a partially-corrupt backup, and to avoid the extra full read) when
        ``verify_before_restore`` is turned off via the restore ``--skip-verify`` flag.

        NOTE (raw+ssh trust model): for a remote target both the recomputed digest and
        the recorded checksum come from the (untrusted) remote, so this detects at-rest
        CORRUPTION, not tampering -- a compromised target could forge a match. Verify a
        ``raw://`` copy for tamper-evidence (same caveat as ``raw verify``)."""
        if not self.config.get("verify_before_restore", True):
            logger.warning(
                "Integrity check skipped for %s (--skip-verify): restoring without "
                "confirming the stored stream matches its recorded checksum.",
                snapshot.name,
            )
            return
        verdict = self.verify_stream_checksum(snapshot)
        if verdict.status == "unverifiable":
            # Nothing to compare against (legacy backup, or a non-sha256 algorithm) --
            # the restore's own decode step still surfaces a genuinely unreadable stream.
            return
        if verdict.status == "error":
            # Could not hash the stream -- do not BLOCK the restore on an inability to
            # verify (the decode step will surface a real read error); just note it.
            logger.warning(
                "Could not verify %s before restore (checksum unreadable); proceeding",
                snapshot.name,
            )
            return
        if verdict.status == "corrupt":
            raise __util__.AbortError(
                f"Refusing to restore {snapshot.name}: the stored stream is CORRUPT "
                f"(its sha256 {verdict.computed} does not match {verdict.recorded}, "
                "sealed when it was backed up). The backup on disk has changed since it "
                "was written. Restore an intact copy if you have one; if this is the "
                "only copy, 'restore --skip-verify' will attempt it anyway (the result "
                "may be incomplete). Run 'raw verify' to inspect the target."
            )

    def _build_restore_pipeline(self, snapshot: RawSnapshot) -> list[list[str]]:
        """Build the decryption/decompression pipeline for restore.

        Args:
            snapshot: The snapshot to restore

        Returns:
            List of command lists to be piped together
        """
        pipeline: list[list[str]] = []

        # Decryption stage (first, if encrypted)
        if snapshot.encrypt == "gpg":
            gpg_cmd = ["gpg", "--decrypt", "--batch", "--quiet"]
            if self.gpg_keyring:
                gpg_cmd.extend(["--keyring", self.gpg_keyring])
            pipeline.append(gpg_cmd)
        elif snapshot.encrypt == "openssl_enc":
            # Restore with the cipher RECORDED in the sidecar so a backup made
            # with a non-default cipher decrypts correctly. Fall back to this
            # endpoint's configured cipher only for legacy backups that recorded
            # none (every pre-sidecar backup used the aes-256-cbc default), and
            # log it so the assumption is never silent. Validate whichever we use:
            # the sidecar is on-disk and only semi-trusted.
            cipher = _validate_cipher(snapshot.openssl_cipher or self.openssl_cipher)
            if not snapshot.openssl_cipher:
                logger.info(
                    "No cipher recorded for %s; restoring with endpoint cipher %s",
                    snapshot.name,
                    cipher,
                )
            if not _openssl_supports_cipher(cipher):
                # Fail clearly BEFORE the pipeline dumps a cryptic OpenSSL EVP error.
                raise __util__.AbortError(
                    f"Cannot restore {snapshot.name}: the backup records openssl cipher "
                    f"{cipher!r}, which this system's openssl does not support. It was "
                    "likely created on a host with a different openssl build. Restore on "
                    "that host, install an openssl that provides the cipher, or run "
                    "'openssl enc -ciphers' to see what this host supports."
                )
            openssl_cmd = [
                "openssl",
                "enc",
                "-d",
                f"-{cipher}",
                "-pbkdf2",
                "-pass",
                _openssl_pass_arg(),
            ]
            pipeline.append(openssl_cmd)
        elif snapshot.encrypt:
            # The sidecar records an encryption method this version does not know how
            # to reverse. NEVER silently skip decryption -- that would pipe the still
            # ENCRYPTED bytes into btrfs receive and produce a corrupt restore with no
            # error (the same silent-corruption class as an unknown compression).
            raise __util__.AbortError(
                f"Cannot restore {snapshot.name}: it is encrypted with "
                f"{snapshot.encrypt!r}, which this version cannot decrypt (supported: "
                "gpg, openssl_enc). Restore with a version that supports it."
            )

        # Decompression stage
        if snapshot.compress:
            config = COMPRESSION_CONFIG.get(snapshot.compress)
            cmd = config.get("decompress_cmd", []) if config else []
            if not cmd:
                # The sidecar records a compression this version cannot reverse. NEVER
                # silently skip decompression -- that pipes the still-compressed bytes
                # into btrfs receive and produces a corrupt restore with no error.
                supported = ", ".join(sorted(COMPRESSION_CONFIG))
                raise __util__.AbortError(
                    f"Cannot restore {snapshot.name}: it is compressed with "
                    f"{snapshot.compress!r}, which this version cannot decompress "
                    f"(supported: {supported}). Restore with a version that supports "
                    "it, or decompress the stream manually."
                )
            pipeline.append(list(cmd))

        # If no processing needed, just cat
        if not pipeline:
            pipeline.append(["cat"])

        return pipeline

    @staticmethod
    def _describe_pipeline(snapshot: RawSnapshot) -> str:
        """A short human phrase describing a snapshot's pipeline, for error messages."""
        parts = []
        if snapshot.compress:
            parts.append(f"compressed with {snapshot.compress}")
        if snapshot.encrypt:
            parts.append(f"encrypted with {snapshot.encrypt}")
        return " and ".join(parts) if parts else "a plain btrfs stream"

    def _preflight_restore_tools(
        self, pipeline: list[list[str]], snapshot: RawSnapshot
    ) -> None:
        """Fail loud (before any bytes flow) if a tool the restore pipeline needs is
        not installed, so a missing decompressor/decryptor gives a clear, actionable
        message instead of a raw FileNotFoundError traceback part-way through the
        restore. Kept separate from pipeline CONSTRUCTION so the built pipeline can be
        inspected without requiring the tools to be present."""
        for stage in pipeline:
            tool = stage[0]
            # "cat" is the pipeline's no-op passthrough sentinel (always present,
            # never a decoder), so skipping it is not a generic "assume installed".
            if tool != "cat" and shutil.which(tool) is None:
                raise __util__.AbortError(
                    f"Cannot restore {snapshot.name}: the tool {tool!r} is required "
                    "to restore this backup (it is "
                    f"{self._describe_pipeline(snapshot)}) but is not installed. "
                    f"Install {tool!r} and retry."
                )

    def _execute_restore_pipeline(
        self, pipeline: list[list[str]], input_path: Path
    ) -> subprocess.Popen:
        """Execute a restore pipeline.

        Args:
            pipeline: List of command lists
            input_path: Path to the stream file

        Returns:
            Popen object with stdout containing the output
        """
        if len(pipeline) == 1:
            with open(input_path, "rb") as infile:
                proc = subprocess.Popen(
                    pipeline[0],
                    stdin=infile,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            return proc

        # Chain commands with shell; quote every argv element and the input path
        # so a gpg keyring / stream path containing spaces does not word-split.
        cmd_strs = [" ".join(shlex.quote(a) for a in cmd) for cmd in pipeline]
        shell_cmd = f"cat {shlex.quote(str(input_path))} | " + " | ".join(cmd_strs)

        logger.debug("Executing restore pipeline: %s", shell_cmd)

        # pipefail so a mid-pipe decrypt/decompress failure (e.g. a wrong passphrase,
        # a truncated stream) is NOT masked by the last stage exiting 0 -- otherwise a
        # garbage/partial stream could be fed to btrfs receive and reported as a
        # successful restore. Mirrors the write pipeline.
        return _popen_pipeline_pipefail(
            shell_cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def list_snapshots(self, flush_cache: bool = False) -> list[RawSnapshot]:
        """List all raw snapshots in the target directory.

        Args:
            flush_cache: If True, refresh the cache

        Returns:
            List of RawSnapshot objects, sorted by creation time
        """
        if self._cached_snapshots is not None and not flush_cache:
            return list(self._cached_snapshots)

        path = Path(self.config["path"])
        prefix = self.config.get("snap_prefix", "")

        snapshots = discover_raw_snapshots(path, prefix)
        # Restore/verify read the stream via this endpoint, so each snapshot must
        # know which endpoint owns it (mirrors __util__.Snapshot.endpoint).
        for snapshot in snapshots:
            snapshot.endpoint = self
        self._cached_snapshots = snapshots

        logger.debug("Found %d raw snapshots in %s", len(snapshots), path)
        return list(snapshots)

    #: Declared beside the override that makes it true, so the two cannot drift.
    #: SSHRawEndpoint inherits this, which is correct: it does not persist either.
    persists_locks: bool = False

    def set_lock(
        self,
        snapshot: Any,
        lock_id: Any,
        lock_state: bool,
        parent: bool = False,
    ) -> None:
        """Update the in-memory retention lock on a raw snapshot.

        Overrides the base Endpoint.set_lock, which requires a ``source`` and
        writes a LOCAL lock file at ``config['path']`` -- both wrong for a raw
        target (restore does not set a source, and the path is remote for
        raw+ssh, so the base write would raise and abort the restore). Raw lock
        PERSISTENCE across runs is a separate change (audit root R3); until then
        this mutates only the in-memory lock set so the restore/transfer
        lock-guard logic works without touching disk.
        """
        target = snapshot.parent_locks if parent else snapshot.locks
        if lock_state:
            target.add(lock_id)
        else:
            target.discard(lock_id)

    def protect_incremental_parents(
        self, to_keep: list, to_delete: list
    ) -> tuple[list, list]:
        """Raw override: never delete a stream a KEPT stream needs as an incremental parent.

        A raw backup is a ``btrfs send`` stream file; an incremental child cannot be applied
        without its parent stream, so deleting a parent silently makes its children unrestorable.
        Walk the ``parent_name`` chain from every kept stream back to its root and rescue any
        ancestor currently marked for deletion (transitive -- walking the full chain also covers a
        rescued parent's own parents). A kept stream whose parent is not present on this target has
        a chain already broken upstream (pre-existing, not caused by this prune): warn, never
        fabricate. Legacy streams with no recorded ``parent_name`` cannot be chain-resolved and are
        left as the time-based decision placed them.
        """
        by_name = {s.get_name(): s for s in list(to_keep) + list(to_delete)}
        delete_names = {s.get_name() for s in to_delete}
        protected: set[str] = set()
        for leaf in to_keep:
            cur = leaf
            seen: set[str] = set()
            while cur is not None:
                parent_name = getattr(cur, "parent_name", None)
                if not parent_name or parent_name in seen:
                    break
                seen.add(parent_name)
                parent = by_name.get(parent_name)
                if parent is None:
                    logger.warning(
                        "Retention: retained raw stream %r references parent %r which is not "
                        "present on this target -- its incremental chain is already broken "
                        "upstream (not caused by this prune)",
                        leaf.get_name(),
                        parent_name,
                    )
                    break
                if parent.get_name() in delete_names:
                    protected.add(parent.get_name())
                cur = parent
        if protected:
            rescued = [s for s in to_delete if s.get_name() in protected]
            to_keep = list(to_keep) + rescued
            to_delete = [s for s in to_delete if s.get_name() not in protected]
            logger.info(
                "Retention: protecting %d raw incremental parent(s) from deletion: %s",
                len(rescued),
                ", ".join(sorted(protected)),
            )
        return to_keep, to_delete

    def _chain_referenced_parents(
        self, delete_batch: list[RawSnapshot], delete_session: set[str] | None = None
    ) -> set[str]:
        """Defence-in-depth for the delete primitive: names in ``delete_batch`` that are still the
        recorded ``parent_name`` of a SURVIVING stream -- one present on the target and NOT part of
        this deletion session -- so deleting them would orphan a child. The delete primitive skips
        these for ANY caller, independent of the prune-level ``protect_incremental_parents`` pass.

        ``delete_session`` (a set of names) is the FULL set intended for deletion this pass; a
        caller that deletes a chain across multiple one-at-a-time calls (prune) passes it so a
        legitimate whole-chain delete is not mistaken for orphaning. Absent, the session is just
        this batch.
        """
        batch_names = {s.get_name() for s in delete_batch}
        session = delete_session if delete_session is not None else batch_names
        try:
            current = self.list_snapshots()
        except Exception as e:  # noqa: BLE001 - best-effort net; prune-level pass is primary
            logger.debug("chain-guard: could not list snapshots (%s)", e)
            return set()
        referenced_by_survivors = {
            s.parent_name
            for s in current
            if s.parent_name and s.get_name() not in session
        }
        return {n for n in batch_names if n in referenced_by_survivors}

    def delete_snapshots(self, snapshots: list[RawSnapshot], **kwargs: Any) -> None:
        """Delete raw snapshot files and their metadata.

        Args:
            snapshots: List of snapshots to delete
            **kwargs: ``delete_session`` (set[str]) -- the full set of names being deleted this
                pass, so the chain guard does not mistake a whole-chain delete for orphaning.
        """
        delete_session = kwargs.get("delete_session")
        # Prune under the per-target lock so it cannot race a concurrent backup
        # commit or backfill. If the target is busy, skip (safe -- do NOT delete
        # during contention); retention retries on the next run.
        try:
            with self.target_lock():
                self._delete_snapshots_locked(snapshots, delete_session)
        except RuntimeError as e:
            logger.warning("Skipping raw delete (target busy): %s", e)

    def _delete_snapshots_locked(
        self, snapshots: list[RawSnapshot], delete_session: set[str] | None = None
    ) -> None:
        protected = self._chain_referenced_parents(snapshots, delete_session)
        for snapshot in snapshots:
            if snapshot.get_name() in protected:
                logger.error(
                    "Refusing to delete raw stream %r: it is the incremental parent of a stream "
                    "that is NOT being deleted; removing it would make that child unrestorable. "
                    "Skipping.",
                    snapshot.get_name(),
                )
                continue
            try:
                # Delete stream file
                if snapshot.stream_path.exists():
                    snapshot.stream_path.unlink()
                    logger.info("Deleted stream file: %s", snapshot.stream_path)

                # Delete metadata file
                if snapshot.metadata_path.exists():
                    snapshot.metadata_path.unlink()
                    logger.debug("Deleted metadata file: %s", snapshot.metadata_path)

                # Update cache
                if self._cached_snapshots is not None:
                    self._cached_snapshots = [
                        s for s in self._cached_snapshots if s.name != snapshot.name
                    ]

            except OSError as e:
                logger.error("Failed to delete snapshot %s: %s", snapshot.name, e)

    def delete_snapshot(self, snapshot: RawSnapshot, **kwargs: Any) -> None:
        """Delete a single raw snapshot.

        Args:
            snapshot: Snapshot to delete
            **kwargs: Additional arguments
        """
        self.delete_snapshots([snapshot], **kwargs)

    def delete_old_snapshots(self, keep: int) -> None:
        """Delete old snapshots, keeping only the most recent.

        LEGACY count-based path (see ``Endpoint.delete_old_snapshots``); the modern retention
        engine is time-based ``retention.apply_retention`` via ``prune``.

        Args:
            keep: Number of snapshots to keep
        """
        if keep <= 0:
            return

        snapshots = self.list_snapshots()
        if len(snapshots) <= keep:
            return

        to_delete = snapshots[:-keep]
        for snapshot in to_delete:
            logger.info("Deleting old raw snapshot: %s", snapshot.name)
        # One lock for the whole prune pass so it is atomic as a unit (a concurrent
        # commit cannot interleave between two deletions) and a busy target yields a
        # single skip decision, not a partial prune. Call the non-locking variant --
        # delete_snapshot would re-take the lock per snapshot and self-deadlock.
        try:
            with self.target_lock():
                self._delete_snapshots_locked(to_delete)
        except RuntimeError as e:
            logger.warning("Skipping raw prune (target busy): %s", e)

    def get_space_info(self, path: str | None = None) -> Any:
        """Get space information for the raw target directory.

        Args:
            path: Optional path override

        Returns:
            SpaceInfo object
        """
        from btrfs_backup_ng.core.space import get_space_info

        if path is None:
            path = str(self.config["path"])

        use_sudo = os.geteuid() != 0
        return get_space_info(path, exec_func=None, use_sudo=use_sudo)


class SSHRawEndpoint(RawEndpoint):
    """Raw target endpoint over SSH.

    Writes raw btrfs send streams to a remote host via SSH,
    with optional local compression/encryption before transfer.
    """

    def __init__(self, config: dict[str, Any] | None = None, **kwargs: Any) -> None:
        """Initialize the SSH Raw Endpoint.

        Args:
            config: Configuration dictionary
            **kwargs: Additional keyword arguments
        """
        config = config or {}
        super().__init__(config, **kwargs)

        # SSH configuration
        self.hostname = config.get("hostname", kwargs.get("hostname", ""))
        self.username = config.get("username")
        self.port = config.get("port", 22)
        self.ssh_key = config.get("ssh_key")
        self.ssh_opts = config.get("ssh_opts", [])
        self.ssh_sudo = config.get("ssh_sudo", False)
        # Host-key policy: "accept-new" (default; unifies raw+ssh with the btrfs transport --
        # previously it set no StrictHostKeyChecking and inherited the ambient ssh default)
        # or "strict" (refuse an unknown host). R12b.
        self.ssh_host_key_policy = config.get("ssh_host_key_policy", "accept-new")
        # An explicit ssh-agent socket (e.g. `--ssh-auth-sock`), useful under sudo where
        # SSH_AUTH_SOCK is not inherited. None -> rely on ssh's own agent discovery.
        self.ssh_auth_sock = config.get("ssh_auth_sock")

        self._is_remote = True

        if not self.hostname:
            raise ValueError("hostname is required for SSHRawEndpoint")

    def __repr__(self) -> str:
        user_host = (
            f"{self.username}@{self.hostname}" if self.username else self.hostname
        )
        parts = [f"raw+ssh://{user_host}{self.config['path']}"]
        if self.compress:
            parts.append(f"compress={self.compress}")
        if self.encrypt:
            parts.append(f"encrypt={self.encrypt}")
        return f"<SSHRawEndpoint {' '.join(parts)}>"

    def get_id(self) -> str:
        """Return a unique identifier for this endpoint."""
        user_host = (
            f"{self.username}@{self.hostname}" if self.username else self.hostname
        )
        return f"raw+ssh://{user_host}{self.config['path']}"

    @contextlib.contextmanager
    def target_lock(self, *, timeout: float | None = None) -> Iterator[None]:
        """No-op for raw+ssh. A per-target lock over ssh needs a remote lockfile with
        stale-detection (there is no persistent connection to hold an flock), which is
        a separate change; concurrent-operation protection is currently local-only. Run
        maintenance commands against a raw+ssh target when it is otherwise idle. The
        raw maintenance CLI warns the operator of this at the point of use."""
        yield

    def _build_ssh_command(self) -> list[str]:
        """Build the base SSH command.

        User ``ssh_opts`` come FIRST so they take precedence (ssh uses the first value
        seen for each option), then defaults that make an unattended backup tool
        behave: ``BatchMode=yes`` (never block on a password/host-key prompt), and
        ``ConnectTimeout`` + ``ServerAlive*`` so a DOWN or packet-dropping host fails
        FAST with ssh exit 255 -- which the listing guard turns into a clear "cannot
        reach" error -- instead of hanging for the OS TCP timeout (or forever)."""
        cmd = ["ssh"]
        cmd.extend(
            self.ssh_opts
        )  # operator opts first -> they win (ssh first-value-wins)
        strict = "yes" if self.ssh_host_key_policy == "strict" else "accept-new"
        cmd.extend(
            [
                "-o",
                "BatchMode=yes",
                "-o",
                f"StrictHostKeyChecking={strict}",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "ServerAliveInterval=5",
                "-o",
                "ServerAliveCountMax=3",
            ]
        )

        if self.port and self.port != 22:
            cmd.extend(["-p", str(self.port)])

        if self.ssh_key:
            cmd.extend(["-i", self.ssh_key])

        if self.ssh_auth_sock:
            # Point ssh at the explicit agent socket (mirrors the btrfs SSHEndpoint's
            # IdentityAgent handling) so agent auth works under sudo, where the inherited
            # SSH_AUTH_SOCK is typically stripped.
            cmd.extend(["-o", f"IdentityAgent={self.ssh_auth_sock}"])

        user_host = (
            f"{self.username}@{self.hostname}" if self.username else self.hostname
        )
        cmd.append(user_host)

        return cmd

    def _elevate(self, remote_command: str) -> str:
        """Wrap a remote command in sudo when ``ssh_sudo`` is set.

        ``-n`` (non-interactive) because the ssh connection carries no tty: sudo
        would otherwise try to prompt and report "a terminal is required to read
        the password", which describes the transport rather than the problem. With
        ``-n`` it exits 1 immediately and writes "sudo: a password is required",
        which :func:`_is_sudo_denial` can tell apart from a command that ran and
        found nothing.

        Callers must not redirect this command's stderr away (``2>/dev/null``
        applied to the whole string discards sudo's diagnostic along with the
        inner command's noise); put any such redirect INSIDE, so it applies to the
        inner command only. Losing that text is what let a refused sudo be
        reported as an empty target.
        """
        # LC_ALL=C so sudo's own diagnostic is emitted in a predictable language:
        # a German remote otherwise reports "sudo: Ein Passwort ist notwendig",
        # which is correct but unhelpful in a log the operator forwards. sudo
        # reads the caller's LC_ALL for its own messages (measured), and stock
        # sudoers keeps LC_ALL in env_keep, so the inner command generally sees
        # it too -- harmless here and mildly desirable, since the numeric output
        # this code parses (stat's mtime/size) then carries no locale formatting.
        # Correctness does not rest on any of this: _is_sudo_denial keys on the
        # untranslated "sudo:" prefix, not on the wording.
        if not self.ssh_sudo:
            return remote_command
        return f"LC_ALL=C sudo -n {remote_command}"

    def _elevate_shell(self, inner: str) -> str:
        """Elevate a command that carries its own ``2>/dev/null``.

        Such a redirect, written flat, binds to the whole ``sudo find ...`` and so
        discards sudo's refusal message along with find's permission noise --
        leaving the listing guard with rc=1 and no evidence, which it then reads
        as "empty target". Running the inner command under ``sh -c`` keeps the
        redirect on the inner command only.

        When not elevating, the command is returned UNCHANGED: there is no sudo
        stderr to protect, and rewriting the wire format for no reason would be a
        gratuitous behaviour change on the path that already works.
        """
        if not self.ssh_sudo:
            return inner
        # The sentinel runs only if sudo actually handed the shell over, and is
        # emitted regardless of the inner command's exit status (find exits 1/2
        # routinely). Its absence therefore means "elevation failed", which the
        # listing guard can act on without knowing how sudo phrases refusal.
        #
        # The inner status is captured and re-raised as the shell's own, because
        # `sh -c 'cmd; echo ...'` otherwise exits with the ECHO's status -- always
        # 0. That would mask a genuine find failure (a missing directory, an
        # unreadable target) as a successful listing of an empty target, which is
        # the very failure this guard exists to prevent, reintroduced by the fix
        # for it.
        # The marker is split in the emitted text so that sudo's refusal echo --
        # which quotes the whole command back -- cannot contain the literal even
        # as a substring. The remote shell concatenates it before echoing, so a
        # command that really runs still prints the marker intact.
        head, tail = _ELEVATION_SENTINEL[:6], _ELEVATION_SENTINEL[6:]
        # printf with a LEADING newline, not echo: the marker must both end its
        # line and START one. echo only terminates, so an inner command that left
        # an unterminated write on stderr -- measured with a large listing --
        # gets the marker appended to its partial line. No line then equals the
        # sentinel, and a perfectly healthy run is reported as never elevated.
        probed = (
            f"{inner}; __bbng_rc=$?; "
            f"printf '\\n%s\\n' {head}\"{tail}\" >&2; exit $__bbng_rc"
        )
        return self._elevate(f"sh -c {shlex.quote(probed)}")

    def _exec_remote_command(
        self,
        command: list[str],
        input: bytes | None = None,
        check: bool = True,
        elevate: bool = True,
        **kwargs: Any,
    ) -> subprocess.CompletedProcess:
        """Run a command on the remote host over SSH.

        Provides the same interface the snapper helpers use on SSHEndpoint
        (metadata sidecar writes, directory listing, cleanup): accepts a command
        as a list plus optional stdin bytes, and returns the CompletedProcess.
        Output is captured by default; callers may override stdout/stderr (e.g.
        ``stdout=subprocess.DEVNULL`` to discard a ``tee`` echo).

        ``elevate=False`` runs unprivileged even when ``ssh_sudo`` is set. Use it
        for commands that do not touch backup data -- a capability probe gains
        nothing from root, and elevating it means a restrictive sudoers policy
        fails the probe before any real work, producing an error about the wrong
        thing entirely.
        """
        import shlex

        remote = " ".join(shlex.quote(str(c)) for c in command)
        if elevate:
            remote = self._elevate(remote)
        full_cmd = self._build_ssh_command() + [remote]
        if "stdout" not in kwargs and "stderr" not in kwargs:
            kwargs["capture_output"] = True
        return subprocess.run(full_cmd, input=input, check=check, **kwargs)

    def _prepare(self) -> None:
        """Prepare the endpoint by creating the remote directory."""
        path = self.config["path"]
        ssh_cmd = self._build_ssh_command()

        mkdir_cmd = self._elevate(f"mkdir -p {shlex.quote(str(path))}")

        full_cmd = ssh_cmd + [mkdir_cmd]
        logger.debug("Creating remote directory: %s", full_cmd)

        try:
            subprocess.run(full_cmd, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or b"").decode(errors="replace").strip()
            if _is_sudo_denial(stderr):
                # Say what raw+ssh actually needs. The README's sudoers recipe
                # grants NOPASSWD for /usr/bin/btrfs, which is right for ssh://
                # and useless here: a raw target stores plain files, so no btrfs
                # command ever runs on the remote.
                raise __util__.AbortError(
                    f"Cannot prepare raw+ssh target {path} on {self.hostname}: "
                    f"{stderr}. ssh_sudo is enabled, but a raw+ssh target stores "
                    "plain files -- the remote user needs passwordless sudo for "
                    "the FILE tools (mkdir, find, cat, stat, mv, rm), not for "
                    "btrfs. Either grant those in sudoers, or -- simpler and "
                    "safer -- give the user ownership of the backup directory "
                    "(chown/setfacl) and turn ssh_sudo off."
                ) from e
            logger.error("Failed to create remote directory: %s", stderr)
            raise

        # Preflight: raw+ssh runs POSIX shell commands on the remote (cat/mv/chmod
        # + a size tool). The mkdir above already proved connectivity + a POSIX-ish
        # shell, so a missing tool here means the remote can't host raw+ssh (e.g. a
        # bare Windows/cmd box) -- fail loud with actionable guidance rather than
        # failing cryptically mid-transfer.
        # `stat` (not just wc) is required: listing sidecar-less streams needs the
        # mtime only stat can give, so the preflight must promise what enumeration
        # actually depends on.
        check = (
            'for t in cat mv chmod stat mktemp dirname; do command -v "$t" '
            ">/dev/null 2>&1 || exit 1; done; echo RAWSSHOK"
        )
        # Unelevated on purpose: this only asks whether the tools EXIST, which
        # root does not change. Elevating it meant a sudoers policy that omits
        # the file utilities failed here first, and the user was told the remote
        # "does not provide the POSIX tools raw+ssh needs" -- which is false.
        res = self._exec_remote_command(["sh", "-c", check], check=False, elevate=False)
        out = res.stdout
        if isinstance(out, (bytes, bytearray)):
            out = out.decode(errors="replace")
        if "RAWSSHOK" not in (out or ""):
            raise RuntimeError(
                f"Remote host {self.hostname} does not provide the POSIX tools "
                "raw+ssh needs (sh, cat, mv, chmod, stat). For a non-POSIX or "
                "SMB/NFS/cloud target, mount it locally and use a raw:// path."
            )

        # Check local tools (compression/encryption run locally before the ssh pipe);
        # fail loud with an actionable message rather than a raw errno mid-transfer.
        missing = self._check_tools()
        if missing:
            raise __util__.AbortError(
                f"Cannot back up to raw+ssh target {self.hostname}: the required local "
                f"tool(s) {', '.join(missing)} are not installed. Install them (or "
                "change the compress/encrypt method) and retry."
            )

    def _execute_pipeline(
        self, pipeline: list[list[str]], stdin: Any
    ) -> subprocess.Popen:
        """Execute pipeline with SSH output.

        Runs compression/encryption locally, then pipes to remote via SSH.
        """
        output_path = self._pending_metadata["part_path"]
        ssh_cmd = self._build_ssh_command()

        # Build the remote write command. Quote the destination path so a path
        # with spaces/metacharacters writes to the intended file, and so the
        # receive-write and commit_receive halves quote identically (they must
        # agree on the target or a valid config could fail at commit).
        remote_cmd = f"cat > {shlex.quote(str(output_path))}"
        remote_cmd = self._elevate(f"sh -c {shlex.quote(remote_cmd)}")

        if not pipeline or pipeline == [["cat"]]:
            # No local processing, pipe directly to SSH
            full_cmd = ssh_cmd + [remote_cmd]
            proc = subprocess.Popen(
                full_cmd,
                stdin=stdin,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            return proc

        # Local processing then SSH. Quote every argv element (a gpg
        # recipient/keyring or cipher may contain spaces or shell metacharacters)
        # and ssh_cmd elements, since this is composed into a local shell string.
        cmd_strs = [" ".join(shlex.quote(a) for a in cmd) for cmd in pipeline]
        local_pipeline = " | ".join(cmd_strs)
        ssh_part = (
            " ".join(shlex.quote(c) for c in ssh_cmd) + " " + shlex.quote(remote_cmd)
        )
        shell_cmd = f"{local_pipeline} | {ssh_part}"

        logger.debug("Executing SSH pipeline: %s", shell_cmd)

        proc = _popen_pipeline_pipefail(
            shell_cmd,
            stdin=stdin,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        return proc

    def commit_receive(self) -> None:
        """Atomically publish the remote stream after a successful transfer.

        The receive pipeline writes to a remote ``.part`` file; once the engine
        confirms success we ``sync`` the remote filesystem and ``mv -f`` the
        ``.part`` file to its final name. A crash leaves only the ``.part`` file,
        which discovery ignores. Raises on failure so an un-published stream is
        treated as a failed transfer.
        """
        pending = getattr(self, "_pending_metadata", None)
        # No receive() has run on this endpoint (dummy init) -> nothing to publish.
        if not pending or not pending.get("name"):
            return
        part_path = pending["part_path"]
        final_path = pending["stream_path"]
        # The leading sync flushes the just-written bytes BEFORE the rename (so
        # the final name can never refer to unflushed data); the trailing sync
        # makes the rename itself durable, matching the local path's post-rename
        # directory fsync. _exec_remote_command quotes each argv element and adds
        # sudo itself, so the whole shell script is one quoted argument to sh -c.
        mv_script = (
            f"sync && mv -f {shlex.quote(str(part_path))} "
            f"{shlex.quote(str(final_path))} && sync"
        )
        result = self._exec_remote_command(["sh", "-c", mv_script], check=False)
        if result.returncode != 0:
            stderr = result.stderr
            if isinstance(stderr, (bytes, bytearray)):
                stderr = stderr.decode(errors="replace")
            raise RuntimeError(
                f"Failed to publish remote raw stream {final_path}: "
                f"{(stderr or '').strip()}"
            )
        # Write the authoritative sidecar remotely (best-effort: the stream is
        # already durable, so a sidecar error must not fail the backup).
        self._write_remote_sidecar(Path(str(final_path)))
        self._cached_snapshots = None
        logger.debug("Committed remote raw stream + sidecar: %s", final_path)

    def _write_remote_sidecar(self, final_path: Path) -> None:
        """Stat the committed remote stream for its size, then write its .meta
        sidecar remotely and atomically (temp -> sync -> mv -> chmod 600)."""
        size = 0
        # Portable remote size: GNU `stat -c %s`, else BSD/macOS `stat -f %z`,
        # else POSIX `wc -c`. A raw target is often a non-Linux box (NAS, macOS),
        # so GNU-only stat would record a bogus size on those.
        q = shlex.quote(str(final_path))
        size_cmd = (
            f"stat -c %s {q} 2>/dev/null || stat -f %z {q} 2>/dev/null || wc -c < {q}"
        )
        try:
            stat_res = self._exec_remote_command(["sh", "-c", size_cmd], check=False)
            out = stat_res.stdout
            if isinstance(out, (bytes, bytearray)):
                out = out.decode(errors="replace")
            if stat_res.returncode == 0:
                size = int((str(out) or "0").strip() or "0")
            else:
                # Do not silently persist a bogus authoritative size of 0 -- make
                # the failure observable (size stays 0, best-effort).
                logger.warning(
                    "Remote size of %s failed (rc=%s); recording sidecar size=0",
                    final_path,
                    stat_res.returncode,
                )
        except (ValueError, TypeError, OSError) as e:
            logger.warning(
                "Could not size remote stream %s: %s; recording sidecar size=0",
                final_path,
                e,
            )
        # Best-effort: the stream is already durable, so a checksum or sidecar error
        # must not fail the backup (mirrors the local commit path). Both the remote
        # hash and write_sidecar are inside the try so neither can flip an
        # already-successful transfer into a reported failure (the PR1/R1 contract).
        try:
            checksum = self._remote_sha256(final_path)
            self.write_sidecar(self._sidecar_snapshot(final_path, size, checksum))
        except Exception as e:
            logger.warning("Failed to write remote sidecar for %s: %s", final_path, e)

    # The digest is computed BY the untrusted remote, so a raw+ssh ``ok`` verdict is
    # corruption-detection only, not tamper-evidence (see ChecksumVerdict).
    _checksum_is_remote: bool = True

    def compute_stream_checksum(self, snapshot: RawSnapshot) -> str | None:
        """Hash ``snapshot``'s stream ON the remote host (see ``_remote_sha256``),
        so ``raw verify`` compares against the recorded checksum without
        re-downloading the stream."""
        return self._remote_sha256(snapshot.stream_path)

    def sidecar_exists(self, snapshot: RawSnapshot) -> bool:
        """Whether ``snapshot``'s ``.meta`` sidecar exists on the remote now (a
        pre-write re-check; see the base method)."""
        # Elevation belongs to _exec_remote_command alone. Wrapping the command
        # here as well produced `sudo -n sh -c 'sudo -n sh -c ...'`, so a sudoers
        # policy permitting the outer invocation still failed on the inner one.
        meta = shlex.quote(str(snapshot.metadata_path))
        res = self._exec_remote_command(["sh", "-c", f"test -f {meta}"], check=False)
        stderr = res.stderr
        if isinstance(stderr, (bytes, bytearray)):
            stderr = stderr.decode(errors="replace")
        # The exit code first: not every `sudo:` line is a refusal. sudo prints
        # benign warnings on stderr -- "sudo: unable to resolve host <name>" is
        # the common one, on any box whose hostname is missing from /etc/hosts --
        # and runs the command anyway. Consulting the text on a command that
        # SUCCEEDED would abort a working backup over a cosmetic warning.
        if res.returncode != 0 and _is_sudo_denial((stderr or "").strip()):
            # `test -f` exits 1 for a missing file, and so does a refused sudo.
            # Reading the second as the first tells backfill-metadata that no
            # sidecar exists, and it then overwrites the one that does -- the
            # exact clobber this pre-write re-check exists to prevent.
            raise RuntimeError(
                f"Cannot check for the sidecar {snapshot.metadata_path} on "
                f"{self.hostname}: {(stderr or '').strip()}. Refusing to treat a "
                "permission failure as 'no sidecar present', because that would "
                "overwrite an existing record."
            )
        return res.returncode == 0

    def _remote_find(self, pattern: str) -> list[str]:
        """Return remote file paths that are DIRECT CHILDREN of the target dir and
        match ``pattern``.

        ``-maxdepth 1`` keeps the scan flat (matching the local ``iterdir`` scan);
        ``-print0`` (NUL-separated) so a filename containing a newline cannot inject
        a second, out-of-target path into the result set; and each path is checked to
        be a direct child of the target dir as defense in depth."""
        base = str(self.config["path"]).rstrip("/") or "/"
        # find's stderr is NOT discarded. It is the only thing that explains why a
        # search failed, and a failed search must never be presented as an empty
        # target. The old `2>/dev/null` silenced "Permission denied" and "No such
        # file or directory" alike, leaving rc=1 with no evidence.
        inner = (
            f"find {shlex.quote(base)} -maxdepth 1 "
            f"-name {shlex.quote(pattern)} -type f -print0"
        )
        find_cmd = self._elevate_shell(inner)
        res = subprocess.run(
            self._build_ssh_command() + [find_cmd],
            check=False,
            capture_output=True,
            text=True,
        )
        # Never let an unreachable host look like an empty target (false all-clear).
        _check_remote_listing(res, self.hostname, base, elevated=self.ssh_sudo)
        out: list[str] = []
        for p in res.stdout.split("\x00"):
            if not p or "\n" in p:
                continue
            if os.path.dirname(p) != base:  # must stay inside the target dir
                continue
            out.append(p)
        return out

    def _remote_stat(self, remote_path: str) -> tuple[datetime | None, int]:
        """Portable remote mtime+size (GNU ``stat -c``, else BSD/macOS ``stat -f``).
        Returns ``(created_utc, size)`` or ``(None, 0)`` if it cannot be stat'd."""
        q = shlex.quote(remote_path)
        stat_cmd = f"stat -c '%Y %s' {q} 2>/dev/null || stat -f '%m %z' {q}"
        stat_cmd = self._elevate(f"sh -c {shlex.quote(stat_cmd)}")
        try:
            res = subprocess.run(
                self._build_ssh_command() + [stat_cmd],
                check=True,
                capture_output=True,
                text=True,
            )
            mtime_str, size_str = res.stdout.strip().split()
            return datetime.fromtimestamp(int(mtime_str), tz=timezone.utc), int(
                size_str
            )
        except (subprocess.CalledProcessError, ValueError):
            return None, 0

    def streams_without_sidecar(self) -> list[RawSnapshot]:
        """Remote backfill candidates: streams on the remote target with no ``.meta``
        sidecar. Two remote finds (streams, sidecars) plus a portable stat per
        candidate. Stamped ``backfill`` / ``unknown`` like the local scan; the
        checksum is left None for the caller to seal on the remote."""
        streams = self._remote_find("*.btrfs*")
        metas = set(self._remote_find("*.meta"))
        out: list[RawSnapshot] = []
        for sp in streams:
            if sp.endswith((".meta", ".part", ".tmp", ".lock")):
                continue
            if sp + ".meta" in metas:
                continue  # already has an authoritative sidecar
            created, size = self._remote_stat(sp)
            if created is None:
                continue
            parsed = parse_stream_filename(Path(sp).name)
            out.append(
                RawSnapshot(
                    name=parsed["name"],
                    stream_path=Path(sp),
                    created=created,
                    size=size,
                    compress=parsed["compress"],
                    encrypt=parsed["encrypt"],
                    provenance_origin="backfill",
                    stream_completeness="unknown",
                )
            )
        return out

    def _remote_sha256(self, final_path: Path) -> str | None:
        """Compute the sha256 of the committed remote stream ON the remote host,
        returning the 64-hex digest or None (best-effort).

        Portable across a raw target that may be Linux, macOS/BSD, or a minimal
        box: GNU ``sha256sum``, else BSD/macOS ``shasum -a 256``, else
        ``openssl dgst``. The tool is chosen by EXISTENCE (``command -v``), not by a
        pipeline's exit status -- a missing first tool must fall through to the next,
        which a ``tool | awk || ...`` chain would not do (the ``||`` keys off awk's
        exit, not the tool's). Hashing remotely keeps the bytes on the remote (no
        re-download) and offloads the work to that host's kernel.

        NOTE: because the digest is computed BY the (untrusted) target, it is only
        as trustworthy as that host -- for raw+ssh the checksum detects passive/
        accidental corruption noticed by an independent reader, but cannot catch
        corruption introduced by a compromised target (unlike the local read-back,
        which hashes honest bytes at write time). Corruption detection, not tamper
        resistance."""
        q = shlex.quote(str(final_path))
        cmd = (
            f"if command -v sha256sum >/dev/null 2>&1; then sha256sum {q} | awk '{{print $1}}'; "
            f"elif command -v shasum >/dev/null 2>&1; then shasum -a 256 {q} | awk '{{print $1}}'; "
            f"elif command -v openssl >/dev/null 2>&1; then openssl dgst -sha256 {q} | awk '{{print $NF}}'; "
            f"else exit 1; fi"
        )
        try:
            res = self._exec_remote_command(["sh", "-c", cmd], check=False)
            out = res.stdout
            if isinstance(out, (bytes, bytearray)):
                out = out.decode(errors="replace")
            digest = (str(out) or "").strip().lower()
            if (
                res.returncode == 0
                and len(digest) == 64
                and all(c in "0123456789abcdef" for c in digest)
            ):
                return digest
            logger.warning(
                "No usable sha256 tool (sha256sum/shasum/openssl) on %s or hashing "
                "failed (rc=%s); sidecar checksum=null (corruption detection "
                "disabled for this backup)",
                getattr(self, "hostname", "remote"),
                res.returncode,
            )
        except Exception as e:
            # Fully best-effort: computing a checksum must NEVER raise out of here
            # (and so can never fail a durable backup or skip the sidecar write).
            logger.warning("Could not checksum remote %s: %s", final_path, e)
        return None

    def write_sidecar(self, snapshot: RawSnapshot) -> None:
        """Write ``snapshot``'s ``.meta`` sidecar on the remote target atomically
        (temp -> sync -> mv -> chmod 600), using the same serialized bytes and
        ``.meta`` path as the local writer. Raises RuntimeError on a nonzero remote
        return so a maintenance command can tell whether the write succeeded; the
        engine's commit path wraps this to stay best-effort."""
        meta = str(snapshot.metadata_path)
        meta_q = shlex.quote(meta)
        # Write the sidecar to an UNPREDICTABLE remote temp (mktemp), then mv it atomically
        # into place. The old predictable ``<meta>.tmp`` let a local user ON THE REMOTE plant
        # a symlink at that guessable path so ``cat >`` followed it and truncated an arbitrary
        # file the remote (sudo) user can write -- an unguessable mktemp name closes that by
        # design (R12d/P6). A ``trap`` removes the temp on any failure; the name avoids
        # ``.btrfs``/``.meta`` so a leaked temp is never mis-enumerated as a stream/sidecar.
        script = (
            f"set -e; d=$(dirname -- {meta_q}); "
            'tmp=$(mktemp "$d/.sidecar-tmp.XXXXXX"); '
            "trap 'rm -f -- \"$tmp\"' EXIT; "
            f'cat > "$tmp"; chmod 600 "$tmp"; sync; mv -f -- "$tmp" {meta_q}'
        )
        result = self._exec_remote_command(
            ["sh", "-c", script], input=snapshot.serialize(), check=False
        )
        if result.returncode != 0:
            stderr = result.stderr
            if isinstance(stderr, (bytes, bytearray)):
                stderr = stderr.decode(errors="replace")
            raise RuntimeError(
                f"Failed to write remote sidecar {meta}: {(stderr or '').strip()}"
            )

    def send(
        self,
        snapshot: Any,
        parent: Any | None = None,
        clones: list[Any] | None = None,
    ) -> subprocess.Popen[bytes]:
        """Read a raw stream back from the REMOTE host for restore.

        The base RawEndpoint.send() opens a local file; for raw+ssh the stream
        lives on the remote, so we stream it down over ssh and decrypt/decompress
        it LOCALLY -- ``ssh host 'cat <remote>' | <decrypt> | <decompress>``. The
        gpg key / openssl passphrase stay on the restore host; secrets are never
        sent to the (untrusted) remote.
        """
        if not isinstance(snapshot, RawSnapshot):
            raise TypeError(f"Expected RawSnapshot, got {type(snapshot)}")
        remote = str(snapshot.stream_path)
        # Build + preflight the LOCAL decrypt/decompress pipeline FIRST, so an
        # undecodable sidecar (unknown algo/cipher) or a missing local tool fails
        # offline and instantly, before we open an ssh connection to the remote.
        pipeline = self._build_restore_pipeline(snapshot)
        self._preflight_restore_tools(pipeline, snapshot)
        # Clear error if the stream is not on the remote (vs a cryptic pipe fail).
        test = self._exec_remote_command(
            ["sh", "-c", f"test -f {shlex.quote(remote)}"], check=False
        )
        if test.returncode != 0:
            raise FileNotFoundError(
                f"Remote stream not found: {self.hostname}:{remote}"
            )
        # Verify the remote stream against its sealed sha256 (hashed ON the remote --
        # no re-download) before streaming it down, so a corrupted remote backup is
        # refused up front rather than decoded into a corrupt subvolume.
        self._verify_stream_integrity(snapshot)

        ssh_cmd = self._build_ssh_command()
        remote_cat = f"cat {shlex.quote(remote)}"
        remote_cat = self._elevate(f"sh -c {shlex.quote(remote_cat)}")
        # Quote every argv element: this string is run by a local bash. ssh_cmd
        # carries operator config (ssh_opts, key path) that may contain spaces.
        ssh_part = (
            " ".join(shlex.quote(c) for c in ssh_cmd) + " " + shlex.quote(remote_cat)
        )
        if pipeline and pipeline != [["cat"]]:
            local_stages = " | ".join(
                " ".join(shlex.quote(a) for a in cmd) for cmd in pipeline
            )
            shell_cmd = f"{ssh_part} | {local_stages}"
        else:
            shell_cmd = ssh_part
        logger.debug("Executing remote restore pipeline: %s", shell_cmd)
        return _popen_pipeline_pipefail(
            shell_cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def list_snapshots(self, flush_cache: bool = False) -> list[RawSnapshot]:
        """List raw snapshots on the remote host.

        Args:
            flush_cache: If True, refresh the cache

        Returns:
            List of RawSnapshot objects
        """
        # For now, list metadata files via SSH
        # This is a simplified implementation - a full version would
        # parse the remote metadata files
        if self._cached_snapshots is not None and not flush_cache:
            return list(self._cached_snapshots)

        path = self.config["path"]
        ssh_cmd = self._build_ssh_command()

        # List .meta files
        inner = f"find {shlex.quote(str(path))} -name '*.meta' -type f"
        find_cmd = self._elevate_shell(inner)

        full_cmd = ssh_cmd + [find_cmd]

        result = subprocess.run(full_cmd, check=False, capture_output=True, text=True)
        # Never let an unreachable host look like an empty target (false all-clear).
        _check_remote_listing(result, self.hostname, path, elevated=self.ssh_sudo)
        meta_files = result.stdout.strip().split("\n") if result.stdout.strip() else []

        # For each metadata file, fetch and parse
        snapshots: list[RawSnapshot] = []
        for meta_path in meta_files:
            if not meta_path:
                continue
            try:
                cat_cmd = f"cat {shlex.quote(meta_path)}"
                cat_cmd = self._elevate(cat_cmd)
                result = subprocess.run(
                    ssh_cmd + [cat_cmd],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                data = json.loads(result.stdout)
                # Derive stream path from meta path
                stream_path = Path(meta_path[:-5])  # Remove .meta
                snapshot = RawSnapshot.from_dict(data, stream_path)
                if not snapshot.name:
                    # An empty/missing name is a meaningless phantom; skip it so the
                    # stream is listed from its filename in the second pass instead.
                    logger.warning(
                        "Remote raw sidecar %s records no snapshot name; ignoring it "
                        "(the stream will be listed from its filename).",
                        meta_path,
                    )
                    continue
                snapshots.append(snapshot)
            except subprocess.CalledProcessError as e:
                # The `cat` failed (file vanished between find and cat, or a mid-listing
                # connection drop -- a persistent outage is already caught at the find).
                logger.debug("Could not read remote sidecar %s: %s", meta_path, e)
                continue
            except Exception as e:
                # The sidecar was readable but could not be PARSED (corrupt/truncated/
                # non-UTF8/pathologically-nested JSON). Warn instead of silently
                # degrading to filename inference (which loses the authoritative
                # compress/encrypt/cipher/checksum); the broad except also keeps a
                # RecursionError from one bad sidecar from aborting the whole listing.
                logger.warning(
                    "Remote raw sidecar %s is unreadable/corrupt and was ignored (%s); "
                    "its stream will be listed from the filename only, losing the "
                    "recorded compress/encrypt/cipher/checksum.",
                    meta_path,
                    e,
                )
                continue

        # Second pass: sidecar-less remote streams (legacy backups, direct
        # btrfs sends, lost .meta). Without this, remote backups that predate
        # .meta sidecars are invisible -- unlistable and unrestorable. Mirrors
        # discover_raw_snapshots' filename-fallback pass.
        loaded_names = {s.name for s in snapshots}
        prefix = self.config.get("snap_prefix", "")
        inner = f"find {shlex.quote(str(path))} -name '*.btrfs*' -type f"
        find_stream_cmd = self._elevate_shell(inner)
        result = subprocess.run(
            ssh_cmd + [find_stream_cmd], check=False, capture_output=True, text=True
        )
        # This is a fresh ssh call: a connection that succeeded on the first-pass find
        # but DROPS before/at this second pass (e.g. a ServerAlive keepalive timeout
        # mid-listing) must still fail loudly rather than truncate the legacy-stream
        # pass to [] and under-report the target's backups.
        _check_remote_listing(result, self.hostname, path, elevated=self.ssh_sudo)
        stream_files = (
            result.stdout.strip().split("\n") if result.stdout.strip() else []
        )

        # Dedup on the stream PATH (unambiguous) as well as the derived name, so
        # a stream that also has a .meta is never enumerated twice even if its
        # recorded name differs from the filename stem.
        loaded_paths = {str(s.stream_path) for s in snapshots}
        inferred_names: set[str] = set()
        # Sorted so a same-name collision resolves deterministically across runs.
        for stream_path_str in sorted(stream_files):
            if not stream_path_str or stream_path_str.endswith(
                (".meta", ".part", ".tmp", ".lock")
            ):
                continue
            stream_path = Path(stream_path_str)
            # Compare a NORMALIZED path: loaded_paths holds str(Path(...)) (collapsed
            # // and /./), but the raw find output is not normalized, so a config path
            # like "/backup//" would otherwise miss the dedup and double-count.
            if str(stream_path) in loaded_paths:
                continue
            parsed = parse_stream_filename(stream_path.name)
            name = parsed["name"]
            if name in loaded_names:
                continue
            if prefix and not name.startswith(prefix):
                continue
            # Two sidecar-less remote streams deriving the same name (plaintext +
            # compressed copy) violate name-based identity; keep the first, warn.
            if name in inferred_names:
                logger.warning(
                    "Two remote raw streams derive the same name %r; keeping the "
                    "first and ignoring %s.",
                    name,
                    stream_path_str,
                )
                continue
            inferred_names.add(name)
            # Portable mtime+size: GNU/busybox `stat -c`, else BSD/macOS `stat -f`.
            q = shlex.quote(stream_path_str)
            stat_cmd = f"stat -c '%Y %s' {q} 2>/dev/null || stat -f '%m %z' {q}"
            stat_cmd = self._elevate(f"sh -c {shlex.quote(stat_cmd)}")
            try:
                stat_result = subprocess.run(
                    ssh_cmd + [stat_cmd],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                mtime_str, size_str = stat_result.stdout.strip().split()
                created = datetime.fromtimestamp(int(mtime_str), tz=timezone.utc)
                size = int(size_str)
            except (subprocess.CalledProcessError, ValueError) as e:
                stderr = getattr(e, "stderr", "") or ""
                if isinstance(stderr, (bytes, bytearray)):
                    stderr = stderr.decode(errors="replace")
                if _is_sudo_denial(stderr.strip()):
                    # Not a per-file accident: sudo will refuse every stat in this
                    # loop, so the listing would silently shrink to nothing and
                    # look like a target holding no backups.
                    raise RuntimeError(
                        f"Cannot stat backups on {self.hostname}: {stderr.strip()}. "
                        "The listing is INCOMPLETE -- this is not an empty target."
                    ) from e
                # A committed stream we cannot stat (removed mid-list, permission
                # error) must NOT be surfaced with a fabricated created=now, which
                # would sort as newest and distort prune / parent selection. Skip it.
                logger.debug("Skipping un-stat-able remote stream %s", stream_path_str)
                continue
            # Reconstructed from the filename (no authoritative sidecar): mark it
            # honestly so it is never presented as a native atomic backup.
            snapshots.append(
                RawSnapshot(
                    name=name,
                    stream_path=stream_path,
                    created=created,
                    size=size,
                    compress=parsed["compress"],
                    encrypt=parsed["encrypt"],
                    provenance_origin="filename-inferred",
                    stream_completeness="unknown",
                )
            )
            loaded_names.add(name)
            loaded_paths.add(stream_path_str)

        snapshots.sort(key=lambda s: s.created)
        # Restore/verify read the stream via this endpoint (see RawEndpoint).
        for snapshot in snapshots:
            snapshot.endpoint = self
        self._cached_snapshots = snapshots
        return list(snapshots)

    def _delete_snapshots_locked(
        self, snapshots: list[RawSnapshot], delete_session: set[str] | None = None
    ) -> None:
        """Delete snapshots on the remote host (issuing a remote ``rm``).

        This overrides the LOCAL delete primitive rather than ``delete_snapshots``,
        so both entry points that wrap it in ``target_lock`` -- ``delete_snapshots``
        (per batch) and ``delete_old_snapshots`` (whole prune pass) -- dispatch to the
        remote deletion for a raw+ssh target. Inheriting the base ``delete_snapshots``
        keeps the (no-op) lock discipline uniform across local and remote. The same
        chain guard as the local primitive applies (never orphan a child stream)."""
        protected = self._chain_referenced_parents(snapshots, delete_session)
        ssh_cmd = self._build_ssh_command()

        for snapshot in snapshots:
            if snapshot.get_name() in protected:
                logger.error(
                    "Refusing to delete raw+ssh stream %r: it is the incremental parent of a "
                    "stream that is NOT being deleted; removing it would make that child "
                    "unrestorable. Skipping.",
                    snapshot.get_name(),
                )
                continue
            try:
                # Build rm command for stream and metadata
                rm_cmd = (
                    f"rm -f {shlex.quote(str(snapshot.stream_path))} "
                    f"{shlex.quote(str(snapshot.metadata_path))}"
                )
                rm_cmd = self._elevate(rm_cmd)

                full_cmd = ssh_cmd + [rm_cmd]
                subprocess.run(full_cmd, check=True, capture_output=True)
                logger.info("Deleted remote snapshot: %s", snapshot.name)

                # Update cache
                if self._cached_snapshots is not None:
                    self._cached_snapshots = [
                        s for s in self._cached_snapshots if s.name != snapshot.name
                    ]
            except subprocess.CalledProcessError as e:
                stderr = (e.stderr or b"").decode(errors="replace").strip()
                logger.error(
                    "Failed to delete remote snapshot %s: %s",
                    snapshot.name,
                    stderr or e,
                )
                if _is_sudo_denial(stderr):
                    # Every delete in this run will fail for the same reason, and
                    # prune would otherwise finish "successfully" having removed
                    # nothing -- so the retention policy silently stops applying
                    # while the operator is told it ran.
                    raise __util__.AbortError(
                        f"Cannot delete backups on {self.hostname}: {stderr}. "
                        "Retention did NOT run. A raw+ssh target stores plain "
                        "files, so with ssh_sudo the remote user needs "
                        "passwordless sudo for rm (and find/cat/stat), not for "
                        "btrfs; alternatively give the user ownership of the "
                        "backup directory and turn ssh_sudo off."
                    ) from e
