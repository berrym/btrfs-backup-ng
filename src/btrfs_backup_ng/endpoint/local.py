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

        # A configured path is NOT created. It was created unconditionally, so a
        # target on a removable or network filesystem that happened to be
        # unmounted got its mount point built on the ROOT filesystem instead, and
        # the backup was written there -- invisible under the mount once the real
        # disk came back, and counting against the wrong filesystem's free space.
        # require_mount only covers the case where the configured path IS the
        # mount point, because that one always exists.
        #
        # Refusing also catches a typo: an explicit path is a statement that
        # something is there, not a request to make it. Directories BELOW an
        # existing configured path are still created (the .btrfs-backup-ng tree
        # at the end of this method, and the snapshot folder under the source).
        source = self.config["source"]
        if source is not None and not Path(source).is_dir():
            logger.error("Configured source does not exist: %s", source)
            raise __util__.AbortError(
                f"Source {source} does not exist. btrfs-backup-ng does not create "
                f"a configured source; check the path, or check that the "
                f"filesystem holding it is mounted."
            )

        destination = self.config["path"]
        if not Path(destination).is_dir():
            logger.error("Configured destination does not exist: %s", destination)
            raise __util__.AbortError(
                f"Destination {destination} does not exist. btrfs-backup-ng does "
                f"not create a configured destination: if it lives on a removable "
                f"or network filesystem, it is most likely not mounted. Check the "
                f"path for a typo, mount the filesystem, or create the directory "
                f"yourself to proceed."
            )

        # Validate filesystem and subvolume checks
        # fs_checks can be: "strict" (error), "auto" (warn and continue), "skip" (no check)
        fs_checks_mode = self.config["fs_checks"]

        if fs_checks_mode != "skip" and self.config["source"] is not None:
            if not __util__.is_subvolume(self.config["source"]):
                msg = f"{self.config['source']} does not seem to be a btrfs subvolume"
                if fs_checks_mode == "strict":
                    logger.error(msg)
                    raise __util__.AbortError(
                        f"Source {self.config['source']} is not a btrfs subvolume. "
                        "Use --no-fs-checks to override."
                    )
                else:  # auto mode
                    logger.warning("%s - continuing anyway (auto mode)", msg)

        if fs_checks_mode != "skip":
            if not __util__.is_btrfs(self.config["path"]):
                msg = f"{self.config['path']} does not seem to be on a btrfs filesystem"
                if fs_checks_mode == "strict":
                    logger.error(msg)
                    raise __util__.AbortError(
                        f"Destination {self.config['path']} is not on a btrfs filesystem. "
                        "Use --no-fs-checks to override."
                    )
                else:  # auto mode
                    logger.warning("%s - continuing anyway (auto mode)", msg)

        logger.debug("LocalEndpoint _prepare completed successfully")

        # Create the .btrfs-backup-ng tree BELOW the destination just verified.
        # One component at a time, never with parents: a destination that
        # vanished between the check above and here (the drive unmounted) is
        # refused rather than rebuilt on the filesystem underneath. A location
        # that is only read (a restore's source) gets no tree: it may be
        # another tool's, or a medium mounted read-only.
        backup_dir = Path(self.config["path"]) / ".btrfs-backup-ng"
        if not self.config.get("create_tree", True):
            logger.debug("Not creating %s: this endpoint only reads", backup_dir)
            return
        try:
            __util__.create_below(
                self.config["path"], ".btrfs-backup-ng", "snapshots", what="Destination"
            )
            logger.debug("Created backup directories: %s", backup_dir)
        except OSError as e:
            # create_below raises AbortError itself for a base that is not
            # there; what reaches here is the mkdir of a component being
            # refused -- a destination this user cannot write.
            logger.error("Error creating backup infrastructure: %s", e)
            raise __util__.AbortError(
                f"Cannot create {backup_dir} under the destination: {e}. The "
                "destination exists but is not writable by this user."
            ) from e
