"""btrfs-backup-ng: btrfs_backup_ng/endpoint/local.py
Create commands with local endpoints.
"""

from pathlib import Path

from btrfs_backup_ng import __util__
from btrfs_backup_ng.__logger__ import logger

from .common import Endpoint


class LocalEndpoint(Endpoint):
    """Create a local command endpoint."""

    def __init__(self, config=None, **kwargs) -> None:
        """
        Initialize the LocalEndpoint with a configuration dictionary.

        Args:
            config (dict): Configuration dictionary containing endpoint settings.
            kwargs: Additional keyword arguments for backward compatibility.
        """
        logger.debug("Initializing LocalEndpoint with config: %s", config)
        super().__init__(config=config, **kwargs)

        # Resolve paths
        logger.debug("LocalEndpoint resolving paths")
        if self.config["source"]:
            logger.debug(
                "Original source path: %s (type: %s)",
                self.config["source"],
                type(self.config["source"]),
            )
            try:
                self.config["source"] = Path(self.config["source"]).resolve()
                logger.debug("Resolved source path: %s", self.config["source"])
            except Exception as e:
                logger.error("Error resolving source path: %s", e)
                raise ValueError(f"Invalid source path: {e}")

        logger.debug(
            "Original destination path: %s (type: %s)",
            self.config["path"],
            type(self.config["path"]),
        )
        try:
            self.config["path"] = Path(self.config["path"]).resolve()
            logger.debug("Resolved destination path: %s", self.config["path"])
        except Exception as e:
            logger.error("Error resolving destination path: %s", e)
            raise ValueError(f"Invalid destination path: {e}")

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        id_str = str(self.config["path"])
        logger.debug("LocalEndpoint ID: %s", id_str)
        return id_str

    def _prepare(self) -> None:
        """Prepare the local endpoint by creating necessary directories and validating paths."""
        # Verify that btrfs command is available
        try:
            import shutil

            btrfs_path = shutil.which("btrfs")
            if not btrfs_path:
                logger.error("btrfs command not found in PATH")
                raise __util__.AbortError(
                    "btrfs command not found in system PATH. Please ensure btrfs-progs is installed."
                )
            logger.debug("Found btrfs command at: %s", btrfs_path)
        except Exception as e:
            logger.error("Error verifying btrfs command: %s", e)
            raise __util__.AbortError(f"Failed to verify btrfs command: {e}")

        # Create directories, if needed
        dirs = []
        if self.config["source"] is not None:
            dirs.append(self.config["source"])
        dirs.append(self.config["path"])

        for d in dirs:
            if not d.is_dir():
                logger.info("Creating directory: %s", d)
                try:
                    d.mkdir(parents=True, exist_ok=True)
                except OSError as e:
                    logger.error("Error creating new location %s: %s", d, e)
                    raise __util__.AbortError(f"Failed to create directory {d}: {e}")

        # Create snapshot directory if it exists in config
        if self.config.get("snapshot_dir") and isinstance(
            self.config["snapshot_dir"], (str, Path)
        ):
            snapshot_dir = Path(self.config["snapshot_dir"])
            if not snapshot_dir.is_absolute():
                snapshot_dir = self.config["path"] / snapshot_dir

            logger.debug("Ensuring snapshot directory exists: %s", snapshot_dir)
            try:
                snapshot_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                logger.error(
                    "Error creating snapshot directory %s: %s", snapshot_dir, e
                )
                raise __util__.AbortError(
                    f"Failed to create snapshot directory {snapshot_dir}: {e}"
                )

        # Validate filesystem and subvolume checks
        if (
            self.config["source"] is not None
            and self.config["fs_checks"]
            and not __util__.is_subvolume(self.config["source"])  # type: ignore[attr-defined]
        ):
            logger.error(
                "%s does not seem to be a btrfs subvolume", self.config["source"]
            )
            raise __util__.AbortError(
                f"Source {self.config['source']} is not a btrfs subvolume. Use --no-fs-checks to override."
            )

        if self.config["fs_checks"] and not __util__.is_btrfs(self.config["path"]):  # type: ignore[attr-defined]
            logger.error(
                "%s does not seem to be on a btrfs filesystem", self.config["path"]
            )
            raise __util__.AbortError(
                f"Destination {self.config['path']} is not on a btrfs filesystem. Use --no-fs-checks to override."
            )

        logger.debug("LocalEndpoint _prepare completed successfully")

        # Create .btrfs-backup-ng directory if needed
        backup_dir = self.config["path"] / ".btrfs-backup-ng"
        try:
            backup_dir.mkdir(parents=True, exist_ok=True)
            snapshots_dir = backup_dir / "snapshots"
            snapshots_dir.mkdir(parents=True, exist_ok=True)
            logger.debug("Created backup directories: %s", backup_dir)
        except OSError as e:
            logger.error("Error creating backup infrastructure: %s", e)
            raise __util__.AbortError
