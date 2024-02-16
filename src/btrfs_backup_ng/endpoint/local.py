"""btrfs-backup-ng: btrfs-backup/local.py
Create commands with local endpoints.
"""

import logging
import os

from .common import Endpoint
from .. import util


class LocalEndpoint(Endpoint):
    """Create a local command endpoint."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.source:
            self.source = os.path.realpath(self.source)
            if not os.path.isabs(self.path):
                self.path = os.path.join(self.source, self.path)
        self.path = os.path.realpath(self.path)

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        return self.path

    def _prepare(self):
        # create directories, if needed
        dirs = []
        if self.source is not None:
            dirs.append(self.source)
        dirs.append(self.path)
        for d in dirs:
            if not os.path.isdir(d):
                logging.info("Creating directory: %s", d)
                try:
                    os.makedirs(d)
                except OSError as e:
                    logging.error("Error creating new location %s: %s", d, e)
                    raise util.AbortError()

        if (
            self.source is not None
            and self.fs_checks
            and not util.is_subvolume(self.source)
        ):
            logging.error(
                "%s does not seem to be a btrfs subvolume", self.source
            )
            raise util.AbortError()
        if self.fs_checks and not util.is_btrfs(self.path):
            logging.error(
                "%s does not seem to be on a btrfs filesystem", self.path
            )
            raise util.AbortError()
