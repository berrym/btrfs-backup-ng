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

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, TypedDict

from btrfs_backup_ng.__logger__ import logger
from btrfs_backup_ng.endpoint.common import Endpoint
from btrfs_backup_ng.endpoint.raw_metadata import (
    COMPRESSION_CONFIG,
    RawSnapshot,
    discover_raw_snapshots,
    get_file_extension,
)


class PendingMetadata(TypedDict):
    """Type definition for pending metadata during receive."""

    name: str
    stream_path: Path
    parent_name: str | None
    compress: str | None
    encrypt: str | None
    gpg_recipient: str | None


# Environment variable for OpenSSL passphrase (compatible with btrbk)
OPENSSL_PASSPHRASE_ENV = "BTRFS_BACKUP_PASSPHRASE"
BTRBK_PASSPHRASE_ENV = "BTRBK_PASSPHRASE"


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
        self.gpg_recipient = config.get("gpg_recipient")
        self.gpg_keyring = config.get("gpg_keyring")
        self.openssl_cipher = config.get("openssl_cipher", "aes-256-cbc")

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
            "parent_name": None,
            "compress": None,
            "encrypt": None,
            "gpg_recipient": None,
        }

    def _get_openssl_passphrase(self) -> str | None:
        """Get OpenSSL passphrase from environment.

        Checks BTRFS_BACKUP_PASSPHRASE first, then BTRBK_PASSPHRASE for
        btrbk compatibility.

        Returns:
            Passphrase string or None if not set
        """
        return os.environ.get(OPENSSL_PASSPHRASE_ENV) or os.environ.get(
            BTRBK_PASSPHRASE_ENV
        )

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

        logger.info("Writing raw stream to: %s", output_path)

        # Build and execute the pipeline
        pipeline = self._build_receive_pipeline(output_path)
        proc = self._execute_pipeline(pipeline, stdin_pipe)

        # Store metadata for later (will be saved after transfer completes)
        self._pending_metadata = {
            "name": snapshot_name,
            "stream_path": output_path,
            "parent_name": parent_name,
            "compress": self.compress,
            "encrypt": self.encrypt,
            "gpg_recipient": self.gpg_recipient,
        }

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
                "env:BTRFS_BACKUP_PASSPHRASE",
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
            output_path = self._pending_metadata["stream_path"]
            with open(output_path, "wb") as outfile:
                proc = subprocess.Popen(
                    pipeline[0],
                    stdin=stdin,
                    stdout=outfile,
                    stderr=subprocess.PIPE,
                )
            return proc

        # For multiple commands, chain them together
        # We use shell to handle the pipeline and file output
        output_path = self._pending_metadata["stream_path"]
        cmd_strs = [" ".join(cmd) for cmd in pipeline]
        shell_cmd = " | ".join(cmd_strs) + f" > {output_path}"

        logger.debug("Executing pipeline: %s", shell_cmd)

        proc = subprocess.Popen(
            shell_cmd,
            shell=True,
            stdin=stdin,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        return proc

    def finalize_receive(
        self, proc: subprocess.Popen, uuid: str = "", parent_uuid: str | None = None
    ) -> RawSnapshot:
        """Finalize a receive operation and save metadata.

        Call this after the receive pipeline has completed.

        Args:
            proc: The Popen object from receive()
            uuid: Btrfs subvolume UUID
            parent_uuid: Parent subvolume UUID

        Returns:
            RawSnapshot with saved metadata
        """
        # Wait for pipeline to complete
        _, stderr = proc.communicate()
        if proc.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown error"
            raise RuntimeError(f"Raw receive pipeline failed: {error_msg}")

        # Get file size
        stream_path = self._pending_metadata["stream_path"]
        size = stream_path.stat().st_size

        # Create and save metadata
        snapshot = RawSnapshot(
            name=self._pending_metadata["name"],
            stream_path=stream_path,
            uuid=uuid,
            parent_uuid=parent_uuid,
            parent_name=self._pending_metadata.get("parent_name"),
            size=size,
            compress=self._pending_metadata.get("compress"),
            encrypt=self._pending_metadata.get("encrypt"),
            gpg_recipient=self._pending_metadata.get("gpg_recipient"),
        )
        snapshot.save_metadata()

        # Update cache
        if self._cached_snapshots is not None:
            self._cached_snapshots.append(snapshot)
            self._cached_snapshots.sort(key=lambda s: s.created)

        logger.info("Saved raw snapshot: %s (%d bytes)", snapshot.name, size)
        return snapshot

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
            # OpenSSL symmetric decryption
            openssl_cmd = [
                "openssl",
                "enc",
                "-d",
                f"-{self.openssl_cipher}",
                "-pbkdf2",
                "-pass",
                "env:BTRFS_BACKUP_PASSPHRASE",
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

        # Chain commands with shell
        cmd_strs = [" ".join(cmd) for cmd in pipeline]
        shell_cmd = f"cat {input_path} | " + " | ".join(cmd_strs)

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
        self._cached_snapshots = snapshots

        logger.debug("Found %d raw snapshots in %s", len(snapshots), path)
        return list(snapshots)

    def delete_snapshots(self, snapshots: list[RawSnapshot], **kwargs: Any) -> None:
        """Delete raw snapshot files and their metadata.

        Args:
            snapshots: List of snapshots to delete
            **kwargs: Additional arguments (unused)
        """
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
            self.delete_snapshot(snapshot)

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

    def _prepare(self) -> None:
        """Prepare the endpoint by creating the remote directory."""
        path = self.config["path"]
        ssh_cmd = self._build_ssh_command()

        mkdir_cmd = f"mkdir -p {path}"
        if self.ssh_sudo:
            mkdir_cmd = f"sudo {mkdir_cmd}"

        full_cmd = ssh_cmd + [mkdir_cmd]
        logger.debug("Creating remote directory: %s", full_cmd)

        try:
            subprocess.run(full_cmd, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            logger.error("Failed to create remote directory: %s", e.stderr.decode())
            raise

        # Check local tools
        self._check_tools()

    def _execute_pipeline(
        self, pipeline: list[list[str]], stdin: Any
    ) -> subprocess.Popen:
        """Execute pipeline with SSH output.

        Runs compression/encryption locally, then pipes to remote via SSH.
        """
        output_path = self._pending_metadata["stream_path"]
        ssh_cmd = self._build_ssh_command()

        # Build the remote write command
        remote_cmd = f"cat > {output_path}"
        if self.ssh_sudo:
            remote_cmd = f"sudo sh -c '{remote_cmd}'"

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

        # Local processing then SSH
        cmd_strs = [" ".join(cmd) for cmd in pipeline]
        local_pipeline = " | ".join(cmd_strs)
        ssh_part = " ".join(ssh_cmd) + f" '{remote_cmd}'"
        shell_cmd = f"{local_pipeline} | {ssh_part}"

        logger.debug("Executing SSH pipeline: %s", shell_cmd)

        proc = subprocess.Popen(
            shell_cmd,
            shell=True,
            stdin=stdin,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        return proc

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
        find_cmd = f"find {path} -name '*.meta' -type f 2>/dev/null"
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
                cat_cmd = f"cat {meta_path}"
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

        snapshots.sort(key=lambda s: s.created)
        self._cached_snapshots = snapshots
        return list(snapshots)

    def delete_snapshots(self, snapshots: list[RawSnapshot], **kwargs: Any) -> None:
        """Delete snapshots on the remote host."""
        ssh_cmd = self._build_ssh_command()

        for snapshot in snapshots:
            try:
                # Build rm command for stream and metadata
                rm_cmd = f"rm -f {snapshot.stream_path} {snapshot.metadata_path}"
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
