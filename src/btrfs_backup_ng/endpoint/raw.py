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
import subprocess
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict

from btrfs_backup_ng.__logger__ import logger
from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.raw_metadata import (
    COMPRESSION_CONFIG,
    RawSnapshot,
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
        logger.warning("Could not checksum %s: %s", path, e)
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
        a directory (EISDIR), a foreign-owned/non-writable lock file (EACCES), or a
        filesystem that cannot flock (ENOLCK) all raise RuntimeError rather than an
        uncaught OSError -- so a hostile or mis-owned lock file degrades to the same
        bounded fail/skip as contention instead of crashing every backup. The lock
        lives inside the target directory, so that directory MUST NOT be writable by
        untrusted users. The raw+ssh subclass overrides this as a no-op (remote
        locking is a separate concern -- there is no persistent connection to hold an
        flock)."""
        if timeout is None:
            timeout = float(self.config.get("lock_timeout", 30.0))
        path = Path(self.config["path"])
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        lockfile = path / LOCK_FILENAME
        # O_NOFOLLOW: refuse a planted <target>/.btrfs-backup-ng.lock symlink so the
        # (often root) open cannot be pointed at an arbitrary file, matching the
        # symlink hardening elsewhere in this module. Any open failure (symlink,
        # directory, foreign-owned/unwritable regular file) is mapped to RuntimeError
        # so it cannot escape as an uncaught OSError and turn a hostile/mis-owned lock
        # file into a permanent DoS on backups and prune.
        try:
            fd = os.open(lockfile, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        except OSError as e:
            logger.warning(
                "Cannot open raw target lock file %s: %s -- a planted symlink, a "
                "directory, or a foreign-owned/non-writable lock file. The target "
                "directory must not be writable by untrusted users and the lock file "
                "must be owned by the backup user.",
                lockfile,
                e,
            )
            raise RuntimeError(
                f"raw target {path} is unavailable: cannot open its lock file "
                f"{lockfile} ({e})"
            ) from e
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

        # Verify required tools are available
        self._check_tools()

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
        self, stdin_pipe: Any, snapshot_name: str = "", parent_name: str | None = None
    ) -> Any:
        """Write a btrfs send stream to a file.

        Unlike the standard Endpoint.receive(), this writes the stream to a file
        instead of piping to 'btrfs receive'.

        Args:
            stdin_pipe: Input stream (from btrfs send)
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
        part_path = Path(f"{output_path}{PARTIAL_SUFFIX}")

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
        proc = self._execute_pipeline(pipeline, stdin_pipe)

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
            with open(output_path, "wb") as outfile:
                proc = subprocess.Popen(
                    pipeline[0],
                    stdin=stdin,
                    stdout=outfile,
                    stderr=subprocess.PIPE,
                )
            return proc

        # For multiple commands, chain them together
        # We use shell to handle the pipeline and file output. Quote every argv
        # element (a gpg recipient/keyring or cipher may contain spaces or shell
        # metacharacters) so nothing word-splits or injects into the shell string.
        output_path = self._pending_metadata["part_path"]
        cmd_strs = [" ".join(shlex.quote(a) for a in cmd) for cmd in pipeline]
        shell_cmd = " | ".join(cmd_strs) + f" > {shlex.quote(str(output_path))}"

        logger.debug("Executing pipeline: %s", shell_cmd)

        proc = _popen_pipeline_pipefail(
            shell_cmd,
            stdin=stdin,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        return proc

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
        # never refer to unflushed data. Done OUTSIDE the lock: the ``.part`` name is
        # unique to this transfer so no peer touches it, and this fsync can take a
        # long time on a multi-GB stream -- holding the lock across it would make a
        # legitimately parallel commit exceed the wait and FAIL instead of serialize.
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
        return self._execute_restore_pipeline(pipeline, snapshot.stream_path)

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

        # Decompression stage
        if snapshot.compress:
            config = COMPRESSION_CONFIG.get(snapshot.compress, {})
            cmd = config.get("decompress_cmd", [])
            if cmd:
                pipeline.append(list(cmd))

        # If no processing needed, just cat
        if not pipeline:
            pipeline.append(["cat"])

        return pipeline

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

        proc = subprocess.Popen(
            shell_cmd,
            shell=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return proc

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

    def delete_snapshots(self, snapshots: list[RawSnapshot], **kwargs: Any) -> None:
        """Delete raw snapshot files and their metadata.

        Args:
            snapshots: List of snapshots to delete
            **kwargs: Additional arguments (unused)
        """
        # Prune under the per-target lock so it cannot race a concurrent backup
        # commit or backfill. If the target is busy, skip (safe -- do NOT delete
        # during contention); retention retries on the next run.
        try:
            with self.target_lock():
                self._delete_snapshots_locked(snapshots)
        except RuntimeError as e:
            logger.warning("Skipping raw delete (target busy): %s", e)

    def _delete_snapshots_locked(self, snapshots: list[RawSnapshot]) -> None:
        for snapshot in snapshots:
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
        """Build the base SSH command."""
        cmd = ["ssh"]

        if self.port and self.port != 22:
            cmd.extend(["-p", str(self.port)])

        if self.ssh_key:
            cmd.extend(["-i", self.ssh_key])

        cmd.extend(self.ssh_opts)

        user_host = (
            f"{self.username}@{self.hostname}" if self.username else self.hostname
        )
        cmd.append(user_host)

        return cmd

    def _exec_remote_command(
        self,
        command: list[str],
        input: bytes | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> subprocess.CompletedProcess:
        """Run a command on the remote host over SSH.

        Provides the same interface the snapper helpers use on SSHEndpoint
        (metadata sidecar writes, directory listing, cleanup): accepts a command
        as a list plus optional stdin bytes, and returns the CompletedProcess.
        Output is captured by default; callers may override stdout/stderr (e.g.
        ``stdout=subprocess.DEVNULL`` to discard a ``tee`` echo).
        """
        import shlex

        remote = " ".join(shlex.quote(str(c)) for c in command)
        if self.ssh_sudo:
            remote = f"sudo {remote}"
        full_cmd = self._build_ssh_command() + [remote]
        if "stdout" not in kwargs and "stderr" not in kwargs:
            kwargs["capture_output"] = True
        return subprocess.run(full_cmd, input=input, check=check, **kwargs)

    def _prepare(self) -> None:
        """Prepare the endpoint by creating the remote directory."""
        path = self.config["path"]
        ssh_cmd = self._build_ssh_command()

        mkdir_cmd = f"mkdir -p {shlex.quote(str(path))}"
        if self.ssh_sudo:
            mkdir_cmd = f"sudo {mkdir_cmd}"

        full_cmd = ssh_cmd + [mkdir_cmd]
        logger.debug("Creating remote directory: %s", full_cmd)

        try:
            subprocess.run(full_cmd, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            logger.error("Failed to create remote directory: %s", e.stderr.decode())
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
            'for t in cat mv chmod stat; do command -v "$t" >/dev/null 2>&1 '
            "|| exit 1; done; echo RAWSSHOK"
        )
        res = self._exec_remote_command(["sh", "-c", check], check=False)
        out = res.stdout
        if isinstance(out, (bytes, bytearray)):
            out = out.decode(errors="replace")
        if "RAWSSHOK" not in (out or ""):
            raise RuntimeError(
                f"Remote host {self.hostname} does not provide the POSIX tools "
                "raw+ssh needs (sh, cat, mv, chmod, stat). For a non-POSIX or "
                "SMB/NFS/cloud target, mount it locally and use a raw:// path."
            )

        # Check local tools
        self._check_tools()

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
        if self.ssh_sudo:
            remote_cmd = f"sudo sh -c {shlex.quote(remote_cmd)}"

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

    def compute_stream_checksum(self, snapshot: RawSnapshot) -> str | None:
        """Hash ``snapshot``'s stream ON the remote host (see ``_remote_sha256``),
        so ``raw verify`` compares against the recorded checksum without
        re-downloading the stream."""
        return self._remote_sha256(snapshot.stream_path)

    def sidecar_exists(self, snapshot: RawSnapshot) -> bool:
        """Whether ``snapshot``'s ``.meta`` sidecar exists on the remote now (a
        pre-write re-check; see the base method)."""
        meta = shlex.quote(str(snapshot.metadata_path))
        cmd = f"test -f {meta}"
        if self.ssh_sudo:
            cmd = f"sudo sh -c {shlex.quote(cmd)}"
        try:
            res = self._exec_remote_command(["sh", "-c", cmd], check=False)
            return res.returncode == 0
        except Exception:
            return False

    def _remote_find(self, pattern: str) -> list[str]:
        """Return remote file paths that are DIRECT CHILDREN of the target dir and
        match ``pattern``.

        ``-maxdepth 1`` keeps the scan flat (matching the local ``iterdir`` scan);
        ``-print0`` (NUL-separated) so a filename containing a newline cannot inject
        a second, out-of-target path into the result set; and each path is checked to
        be a direct child of the target dir as defense in depth."""
        base = str(self.config["path"]).rstrip("/") or "/"
        find_cmd = (
            f"find {shlex.quote(base)} -maxdepth 1 "
            f"-name {shlex.quote(pattern)} -type f -print0 2>/dev/null"
        )
        if self.ssh_sudo:
            find_cmd = f"sudo {find_cmd}"
        try:
            res = subprocess.run(
                self._build_ssh_command() + [find_cmd],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError:
            return []
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
        if self.ssh_sudo:
            stat_cmd = f"sudo sh -c {shlex.quote(stat_cmd)}"
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
        tmp = f"{meta}.tmp"
        script = (
            f"cat > {shlex.quote(tmp)} && sync && "
            f"mv -f {shlex.quote(tmp)} {shlex.quote(meta)} && "
            f"chmod 600 {shlex.quote(meta)}"
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
        # Clear error if the stream is not on the remote (vs a cryptic pipe fail).
        test = self._exec_remote_command(
            ["sh", "-c", f"test -f {shlex.quote(remote)}"], check=False
        )
        if test.returncode != 0:
            raise FileNotFoundError(
                f"Remote stream not found: {self.hostname}:{remote}"
            )

        pipeline = self._build_restore_pipeline(snapshot)  # LOCAL decrypt/decompress
        ssh_cmd = self._build_ssh_command()
        remote_cat = f"cat {shlex.quote(remote)}"
        if self.ssh_sudo:
            remote_cat = f"sudo sh -c {shlex.quote(remote_cat)}"
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
        find_cmd = f"find {shlex.quote(str(path))} -name '*.meta' -type f 2>/dev/null"
        if self.ssh_sudo:
            find_cmd = f"sudo {find_cmd}"

        full_cmd = ssh_cmd + [find_cmd]

        try:
            result = subprocess.run(
                full_cmd, check=True, capture_output=True, text=True
            )
            meta_files = (
                result.stdout.strip().split("\n") if result.stdout.strip() else []
            )
        except subprocess.CalledProcessError:
            meta_files = []

        # For each metadata file, fetch and parse
        snapshots: list[RawSnapshot] = []
        for meta_path in meta_files:
            if not meta_path:
                continue
            try:
                cat_cmd = f"cat {shlex.quote(meta_path)}"
                if self.ssh_sudo:
                    cat_cmd = f"sudo {cat_cmd}"
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
                snapshots.append(snapshot)
            except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
                logger.debug("Failed to parse remote metadata %s: %s", meta_path, e)
                continue

        # Second pass: sidecar-less remote streams (legacy backups, direct
        # btrfs sends, lost .meta). Without this, remote backups that predate
        # .meta sidecars are invisible -- unlistable and unrestorable. Mirrors
        # discover_raw_snapshots' filename-fallback pass.
        loaded_names = {s.name for s in snapshots}
        prefix = self.config.get("snap_prefix", "")
        find_stream_cmd = (
            f"find {shlex.quote(str(path))} -name '*.btrfs*' -type f 2>/dev/null"
        )
        if self.ssh_sudo:
            find_stream_cmd = f"sudo {find_stream_cmd}"
        try:
            result = subprocess.run(
                ssh_cmd + [find_stream_cmd],
                check=True,
                capture_output=True,
                text=True,
            )
            stream_files = (
                result.stdout.strip().split("\n") if result.stdout.strip() else []
            )
        except subprocess.CalledProcessError:
            stream_files = []

        # Dedup on the stream PATH (unambiguous) as well as the derived name, so
        # a stream that also has a .meta is never enumerated twice even if its
        # recorded name differs from the filename stem.
        loaded_paths = {str(s.stream_path) for s in snapshots}
        for stream_path_str in stream_files:
            if not stream_path_str or stream_path_str.endswith(
                (".meta", ".part", ".tmp", ".lock")
            ):
                continue
            if stream_path_str in loaded_paths:
                continue
            stream_path = Path(stream_path_str)
            parsed = parse_stream_filename(stream_path.name)
            name = parsed["name"]
            if name in loaded_names:
                continue
            if prefix and not name.startswith(prefix):
                continue
            # Portable mtime+size: GNU/busybox `stat -c`, else BSD/macOS `stat -f`.
            q = shlex.quote(stream_path_str)
            stat_cmd = f"stat -c '%Y %s' {q} 2>/dev/null || stat -f '%m %z' {q}"
            if self.ssh_sudo:
                stat_cmd = f"sudo sh -c {shlex.quote(stat_cmd)}"
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
            except (subprocess.CalledProcessError, ValueError):
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

    def _delete_snapshots_locked(self, snapshots: list[RawSnapshot]) -> None:
        """Delete snapshots on the remote host (issuing a remote ``rm``).

        This overrides the LOCAL delete primitive rather than ``delete_snapshots``,
        so both entry points that wrap it in ``target_lock`` -- ``delete_snapshots``
        (per batch) and ``delete_old_snapshots`` (whole prune pass) -- dispatch to the
        remote deletion for a raw+ssh target. Inheriting the base ``delete_snapshots``
        keeps the (no-op) lock discipline uniform across local and remote."""
        ssh_cmd = self._build_ssh_command()

        for snapshot in snapshots:
            try:
                # Build rm command for stream and metadata
                rm_cmd = (
                    f"rm -f {shlex.quote(str(snapshot.stream_path))} "
                    f"{shlex.quote(str(snapshot.metadata_path))}"
                )
                if self.ssh_sudo:
                    rm_cmd = f"sudo {rm_cmd}"

                full_cmd = ssh_cmd + [rm_cmd]
                subprocess.run(full_cmd, check=True, capture_output=True)
                logger.info("Deleted remote snapshot: %s", snapshot.name)

                # Update cache
                if self._cached_snapshots is not None:
                    self._cached_snapshots = [
                        s for s in self._cached_snapshots if s.name != snapshot.name
                    ]
            except subprocess.CalledProcessError as e:
                logger.error(
                    "Failed to delete remote snapshot %s: %s", snapshot.name, e
                )
