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

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        if self.source:
            self.source = Path(self.source).resolve()
            if not self.path.is_absolute():
                self.path = self.source / self.path
        self.path = Path(self.path).resolve()

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        return str(self.path)

    def _prepare(self) -> None:
        # create directories, if needed
        dirs = []
        if self.source is not None:
            dirs.append(self.source)
        dirs.append(self.path)
        for d in dirs:
            if not d.is_dir():
                logger.info("Creating directory: %s", d)
                try:
                    d.mkdir(parents=True, exist_ok=True)
                except OSError as e:
                    logger.error("Error creating new location %s: %s", d, e)
                    raise __util__.AbortError

        if (
            self.source is not None
            and self.fs_checks
            and not __util__.is_subvolume(self.source)
        ):
            logger.error("%s does not seem to be a btrfs subvolume", self.source)
            raise __util__.AbortError
        if self.fs_checks and not __util__.is_btrfs(self.path):
            logger.error("%s does not seem to be on a btrfs filesystem", self.path)
            raise __util__.AbortError
