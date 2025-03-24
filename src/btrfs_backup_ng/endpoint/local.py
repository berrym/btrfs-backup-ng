# pyright: standard

"""btrfs-backup-ng: btrfs_backup_ng/endpoint/local.py
Create commands with local endpoints.
"""

import os

from btrfs_backup_ng import __util__
from btrfs_backup_ng.__logger__ import logger

from .common import Endpoint


class LocalEndpoint(Endpoint):
    """Create a local command endpoint."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        if self.source:
            self.source = os.path.realpath(self.source)
            if not os.path.isabs(self.path):
                self.path = os.path.join(self.source, self.path)
        self.path = os.path.realpath(self.path)

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        return self.path

    def _prepare(self) -> None:
        # create directories, if needed
        dirs = []
        if self.source is not None:
            dirs.append(self.source)
        dirs.append(self.path)
        for d in dirs:
            if not os.path.isdir(d):
                logger.info("Creating directory: %s", d)
                try:
                    os.makedirs(d)
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
