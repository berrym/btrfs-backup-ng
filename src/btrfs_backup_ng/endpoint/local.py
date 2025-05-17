# pyright: standard

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
        super().__init__(config=config, **kwargs)

        # Resolve paths
        if self.config["source"]:
            self.config["source"] = Path(self.config["source"]).resolve()
        self.config["path"] = Path(self.config["path"]).resolve()

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        return str(self.config["path"])

    def _prepare(self) -> None:
        """Prepare the local endpoint by creating necessary directories and validating paths."""
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
                    raise __util__.AbortError

        # Validate filesystem and subvolume checks
        if (
            self.config["source"] is not None
            and self.config["fs_checks"]
            and not __util__.is_subvolume(self.config["source"])
        ):
            logger.error(
                "%s does not seem to be a btrfs subvolume", self.config["source"]
            )
            raise __util__.AbortError

        if self.config["fs_checks"] and not __util__.is_btrfs(self.config["path"]):
            logger.error(
                "%s does not seem to be on a btrfs filesystem", self.config["path"]
            )
            raise __util__.AbortError
