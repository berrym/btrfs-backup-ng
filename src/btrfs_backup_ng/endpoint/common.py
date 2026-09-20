"""btrfs-backup-ng: btrfs_backup_ng/endpoint/common.py
Common functionality among modules.
"""

import contextlib
import logging
import os
import stat
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from filelock import FileLock

from btrfs_backup_ng import __util__
from btrfs_backup_ng.__logger__ import logger
from btrfs_backup_ng.core.space import SpaceInfo
from btrfs_backup_ng.core.space import get_space_info as _get_space_info
from btrfs_backup_ng.endpoint.raw_metadata import StructureVerdict

#: The highest ``_N`` collision suffix creation will allocate before refusing.
#: btrbk increments its counter unboundedly; a bound here means a pathological
#: directory (hundreds of same-name collisions) still ends in a diagnosis
#: instead of an unbounded scan. Within _TRAILING_ORDINAL_RE's 9-digit limit,
#: so every allocated name derives its timestamp on the next listing.
_MAX_COLLISION_ORDINAL = 999


def _secure_lock_dir(base: Path, euid: int) -> Optional[Path]:
    """Return a euid-owned 0700 ``btrfs-backup-ng-<euid>`` subdir of ``base`` for lock files,
    or None if it cannot be secured (a pre-planted symlink or foreign-owned dir at that
    predictable name). Placing the lock INSIDE a private 0700 dir means no untrusted symlink
    can be planted at the lock path itself -- this closes the check-then-open TOCTOU that a
    bare /tmp lock has (the filelock library opens O_TRUNC without O_NOFOLLOW). R12c/P5."""
    d = base / f"btrfs-backup-ng-{euid}"
    try:
        d.mkdir(mode=0o700, exist_ok=True)
        st = os.lstat(d)  # lstat: a symlink is seen AS a symlink, not its target
    except OSError:
        return None
    if (
        not stat.S_ISDIR(st.st_mode)
        or st.st_uid != euid
        or stat.S_IMODE(st.st_mode) != 0o700
    ):
        return None
    return d


def _command_lock_path() -> Path:
    """Stable per-euid path for the btrfs-command serialization lock, placed INSIDE a
    euid-owned 0700 directory so it is immune to a symlink race in world-writable /tmp
    (R12c/P5). Prefer ``$XDG_RUNTIME_DIR`` (euid-owned); else a verified
    ``/tmp/btrfs-backup-ng-<euid>``. Fails CLOSED if neither can be secured -- never follows
    an attacker-controlled path to truncate a victim file."""
    euid = os.geteuid()
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg and os.path.isdir(xdg) and not os.path.islink(xdg):
        try:
            if os.stat(xdg).st_uid == euid:
                d = _secure_lock_dir(Path(xdg), euid)
                if d is not None:
                    return d / "command.lock"
        except OSError:
            pass
    d = _secure_lock_dir(Path(tempfile.gettempdir()), euid)
    if d is None:
        raise __util__.AbortError(
            f"Refusing to proceed: cannot secure a lock directory "
            f"(a symlink or foreign-owned btrfs-backup-ng-{euid} is present in the temp "
            f"dir -- remove it)"
        )
    return d / "command.lock"


@dataclass
class DeletionResult:
    """What a deletion batch actually did, per snapshot.

    ``deleted`` holds the snapshots removed from the target. ``skipped`` holds
    ``(snapshot, reason)`` for those deliberately left alone -- a retention lock
    held here or by another process, a chain guard refusing to orphan a child,
    a stream that was already gone. ``failed`` holds ``(snapshot, error)`` for
    those the target was asked to remove and did not.

    Every deletion path used to return None, and none of them raise: locked
    snapshots were skipped, unreadable lock files refused the whole batch,
    btrfs failures were logged, and the caller saw the same None for all of it.
    ``prune`` therefore counted each call that did not throw, so a pass that
    removed NOTHING reported "Deleted N snapshot(s)", exited 0 and sent a
    success notification -- while the target filled up. This is the verdict that
    was missing, in the shape of ``TransferResult`` (core/operations.py), which
    exists for the same reason on the transfer side.

    A skip is not a failure: refusing to delete a locked snapshot is the guard
    working. Only ``failed`` makes ``ok`` False.
    """

    deleted: List[Any] = field(default_factory=list)
    skipped: List[Any] = field(default_factory=list)
    failed: List[Any] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed

    @property
    def deleted_count(self) -> int:
        return len(self.deleted)

    @property
    def skipped_count(self) -> int:
        return len(self.skipped)

    @property
    def failed_count(self) -> int:
        return len(self.failed)

    @property
    def attempted(self) -> int:
        return len(self.deleted) + len(self.skipped) + len(self.failed)

    def skip(self, snapshot: Any, reason: str) -> None:
        self.skipped.append((snapshot, reason))

    def fail(self, snapshot: Any, error: Any) -> None:
        self.failed.append((snapshot, error))

    def fail_all(self, snapshots: List[Any], error: Any) -> None:
        """Record a whole-batch refusal: nothing was even attempted.

        These are failures rather than skips because the operator asked for the
        deletion and it did not happen. The distinction matters at exactly one
        place -- prune's exit code -- and that is the place it was wrong.
        """
        for snapshot in snapshots:
            self.failed.append((snapshot, error))

    def extend(self, other: "DeletionResult") -> "DeletionResult":
        """Accumulate another batch's outcome into this one."""
        self.deleted.extend(other.deleted)
        self.skipped.extend(other.skipped)
        self.failed.extend(other.failed)
        return self


def require_source(method):
    """Decorator to ensure the endpoint has a source set."""

    def wrapped(self, *args, **kwargs):
        if self.config["source"] is None:
            raise ValueError("source hasn't been set")
        return method(self, *args, **kwargs)

    return wrapped


class Endpoint:
    """Generic structure of a command endpoint."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        """
        Initialize the Endpoint with a configuration dictionary.

        Args:
            config (dict): Configuration dictionary containing endpoint settings.
            kwargs: Additional keyword arguments for backward compatibility.
        """
        config = config or {}
        self.config = {}

        # Normalize source and path
        self.config["source"] = self._normalize_path(config.get("source"))
        self.config["path"] = self._normalize_path(config.get("path"))
        self.config["snap_prefix"] = config.get("snap_prefix", "")
        # Whether that prefix was CHOSEN, which the value alone cannot express:
        # "" is both "the operator asked for no prefix" (issue #6 -- bare
        # timestamp names) and "nobody said". Only the second may be overridden
        # by prefix inference, and truthiness cannot tell them apart.
        self.config["snap_prefix_explicit"] = bool(
            config.get("snap_prefix_explicit", False)
        )
        self.config["convert_rw"] = config.get("convert_rw", False)
        self.config["subvolume_sync"] = config.get("subvolume_sync", False)
        self.config["btrfs_debug"] = config.get("btrfs_debug", False)
        # fs_checks can be: "strict", "auto", "skip", True (=strict), False (=skip)
        fs_checks = config.get("fs_checks", "auto")
        if fs_checks is True:
            fs_checks = "strict"
        elif fs_checks is False:
            fs_checks = "skip"
        self.config["fs_checks"] = fs_checks
        self.config["lock_file_name"] = config.get(
            "lock_file_name", ".btrfs-backup-ng.locks"
        )
        self.config["snapshot_folder"] = config.get("snapshot_folder", ".snapshots")
        # Snapshot timestamp format (from [global] timestamp_format). None means the
        # built-in default is used when naming/parsing snapshots for this endpoint.
        self.config["timestamp_format"] = config.get("timestamp_format")

        self.btrfs_flags = ["-vv"] if self.config["btrfs_debug"] else []
        self.__cached_snapshots: List[Any] | None = None
        # Set True when the lock file could not be read (corrupt/permission); retention
        # then refuses to delete so a still-needed locked snapshot is never pruned.
        self._locks_read_failed = False

        for key, value in kwargs.items():
            self.config[key] = value

    def _normalize_path(self, val: Any) -> Any:
        if val is None:
            return None

        # Import logger here to avoid circular imports
        from btrfs_backup_ng.__logger__ import logger

        logger.debug("Normalizing path: %s (type: %s)", val, type(val).__name__)

        # Handle string paths
        if isinstance(val, str):
            # Just expanduser for remote paths to avoid resolving them locally
            if self._is_remote:
                expanded = str(Path(val).expanduser())
                logger.debug("Remote path expanded: %s -> %s", val, expanded)
                return expanded

            # For local paths, handle carefully
            try:
                path = Path(val).expanduser()
                logger.debug("Local path expanded: %s -> %s", val, path)
                # If path is absolute, no need to resolve
                if path.is_absolute():
                    logger.debug("Using absolute path as-is: %s", path)
                    return path

                # Safely resolve relative path
                try:
                    resolved = path.resolve()
                    logger.debug("Resolved relative path: %s -> %s", path, resolved)
                    return resolved
                except (FileNotFoundError, PermissionError) as e:
                    # If resolving fails, manually make it absolute with cwd
                    logger.warning(f"Path resolution failed for {path}: {e}")
                    cwd_path = Path(os.getcwd()) / path
                    logger.debug(
                        "Manually made absolute with cwd: %s -> %s", path, cwd_path
                    )
                    return cwd_path
            except Exception as e:
                logger.error(f"Path handling error: {e}")
                # Return original string if all else fails
                logger.debug("Returning original string due to error: %s", val)
                return val

        # If it's already a Path object
        if isinstance(val, Path):
            # For remote paths, convert to string to avoid resolution issues
            if self._is_remote:
                return str(val)

            # For local paths, handle safely
            if val.is_absolute():
                return val

            try:
                return val.resolve()
            except (FileNotFoundError, PermissionError) as e:
                # If resolving fails, manually make it absolute
                logger.warning(f"Path resolution failed for {val}: {e}")
                return Path(os.getcwd()) / val

        # For other types, just convert to string
        return str(val)

    def prepare(self) -> None:
        """Public access to _prepare, which is called after creating an endpoint."""
        logger.info("Preparing endpoint %r ...", self)
        return self._prepare()

    @require_source
    def snapshot(self, readonly: bool = True, sync: bool = True) -> Any:
        """Take a snapshot and return the created object."""
        base_path = Path(self.config["source"]).resolve()
        snapshot_folder = self.config["snapshot_folder"]

        # Support absolute snapshot_folder paths (external snapshot directories)
        if Path(snapshot_folder).is_absolute():
            snapshot_dir = Path(snapshot_folder).resolve()
        else:
            snapshot_dir = (base_path / snapshot_folder).resolve()

        self.config["path"] = snapshot_dir
        snap_prefix = self.config["snap_prefix"]

        snapshot_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        snapshot = __util__.Snapshot(snapshot_dir, snap_prefix, self)
        snapshot_path = snapshot.get_path()
        logger.info(
            "Creating snapshot from source: %s to destination: %s",
            self.config["source"],
            snapshot_path,
        )
        logger.debug("Snapshot directory: %s", snapshot_dir)
        logger.debug("Snapshot prefix: %s", snap_prefix)

        lock_path = snapshot_dir / ".btrfs-backup-ng.snapshot.lock"
        logger.debug("Acquiring snapshot lock: %s", lock_path)
        with FileLock(lock_path):
            logger.debug("Snapshot lock acquired: %s", lock_path)
            if Path(snapshot_path).exists():
                # A snapshot with this exact name already exists. Allocate the
                # lowest free ``_N`` -- btrbk's collision counter -- INSIDE
                # this lock, the same critical section that performs the
                # create, so two concurrent runs cannot pick the same N. This
                # is what makes a coarse timestamp_format usable (its second
                # snapshot of the period used to be refused outright) and what
                # absorbs the DST fall-back, where two instants an hour apart
                # render the same default-format name once a year. Safe only
                # because names are remembered facts and suffixed names are
                # visible on every destination type; the suffixed snapshot is
                # dated by its base timestamp on every later listing.
                base_name = snapshot.get_name()
                for ordinal in range(1, _MAX_COLLISION_ORDINAL + 1):
                    candidate = f"{base_name}_{ordinal}"
                    if not (snapshot_dir / candidate).exists():
                        snapshot = __util__.Snapshot(
                            snapshot_dir,
                            snap_prefix,
                            self,
                            time_obj=snapshot.time_obj,
                            name=candidate,
                        )
                        # Every listing re-derives this flag; set it on the
                        # object in hand too, so prune surfaces mark it in
                        # this very run.
                        snapshot.newly_visible = True
                        snapshot_path = snapshot.get_path()
                        logger.info(
                            "A snapshot named %s already exists (the "
                            "configured timestamp_format renders this moment "
                            "to the same name); creating %s instead -- _N is "
                            "the btrbk-compatible collision counter.",
                            base_name,
                            candidate,
                        )
                        break
                else:
                    # Every suffix up to the bound is taken: fall back to the
                    # diagnosis. Under a seconds-resolving format the cause is
                    # two requests in the same second (or the repeated hour of
                    # a DST fall-back), and waiting is real advice; under a
                    # coarser format the format itself cannot name another
                    # snapshot this period, and "wait a second" cannot work.
                    fmt = self.config.get("timestamp_format") or __util__.DATE_FORMAT
                    period = __util__.indistinguishable_period(fmt)
                    where = (
                        f"A snapshot named '{base_name}' already exists at "
                        f"{snapshot_path}, and every collision suffix up to "
                        f"_{_MAX_COLLISION_ORDINAL} is taken."
                    )
                    if period is None:
                        raise __util__.AbortError(
                            f"{where} Two snapshots were likely requested "
                            "within the same second (identical timestamp); "
                            "during a daylight-saving fall-back, requests an "
                            "hour apart can also collide. Wait a second and "
                            "retry; if the existing snapshot is incomplete, "
                            "remove it first."
                        )
                    if period == "more than a day":
                        raise __util__.AbortError(
                            f"{where} The configured timestamp_format {fmt!r} "
                            "renders the same name for snapshots taken even "
                            "days apart, so every new snapshot collides with "
                            "this one. Configure a timestamp_format with "
                            "finer resolution, or remove the existing "
                            "snapshot first if it is not needed."
                        )
                    raise __util__.AbortError(
                        f"{where} The configured timestamp_format {fmt!r} "
                        f"gives every snapshot taken in the same {period} "
                        "the same name, so waiting a second cannot help. "
                        f"Retry in the next {period}, configure a "
                        "timestamp_format with finer resolution, or remove "
                        "the existing snapshot first if it is not needed."
                    )
            self._remount(self.config["source"], read_write=True)
            commands = [
                self._build_snapshot_cmd(
                    self.config["source"], snapshot_path, readonly=readonly
                )
            ]
            if sync:
                commands.append(self._build_sync_command())
            for cmd in self._collapse_commands(commands):
                logger.debug("Executing snapshot command: %s", cmd)
                self._exec_command({"command": cmd})
                logger.debug("Snapshot command executed successfully: %s", cmd)
            # ONE registration for one snapshot. This sat inside the loop above,
            # and `sync` defaults to True, so there are normally two commands --
            # the create and the `btrfs subvolume sync` -- and the snapshot was
            # registered twice.
            #
            # Count-based retention then budgeted for a snapshot that does not
            # exist. Measured with the cache populated: 4 existing + 1 new under
            # `-N 2` left ONE, and `-N 1` left NONE at all -- the duplicate of the
            # newest pushes the real newest into `unlocked[:-keep]`, so the
            # snapshot just taken is destroyed along with every other one.
            #
            # Unreachable from any shipped flow today, because add_snapshot
            # returns early while the cache is None and nothing lists the source
            # before snapshotting it. That is a coincidence of call order, not a
            # property anyone maintains: one pre-flight listing added to `run`
            # for an unrelated reason turns `-N 1` into total loss.
            self.add_snapshot(snapshot)
        return snapshot

    def preflight_send(self, snapshot: Any) -> None:
        """Confirm this endpoint could actually send ``snapshot``, without sending.

        Exists so a caller that is about to DESTROY something can find out first
        whether the replacement can be delivered. `restore --overwrite` removes
        the copy at the destination before receiving, and its safety argument is
        that a restore only ever reads the backup, so an interruption costs a
        retry rather than data. That argument holds only while the backup is
        deliverable. When it is not -- a corrupted stream, a missing decompressor
        -- the copy being removed was the last good one, and every check that
        would have said so lived inside ``send``, downstream of the deletion.

        The base implementation makes no claim: an endpoint that cannot check
        cheaply must not pretend it did. Overrides should raise the same error
        ``send`` would.
        """
        return None

    def send(
        self, snapshot: Any, parent: Any = None, clones: Optional[List[Any]] = None
    ) -> Any:
        """Call 'btrfs send' for the given snapshot and return its Popen object.

        NOT ``@require_source``: this sends the GIVEN ``snapshot`` by its own path and
        never reads ``config["source"]``. The restore path deliberately swaps endpoints
        and calls send() on the BACKUP endpoint (which has no source) to stream a stored
        snapshot back -- requiring a source aborted native btrfs restore. Creating a NEW
        snapshot (``snapshot()``) still requires a source; sending an existing one does
        not. NB: the base implementation runs ``btrfs send`` LOCALLY, so it serves LOCAL
        btrfs restore; a remote ssh:// restore source is gated in the restore CLI until
        SSHEndpoint gains a remote-aware send.
        """
        cmd = self._build_send_command(snapshot, parent=parent, clones=clones)
        # Suppress stderr ("At subvol" messages) - they're just informational
        return self._exec_command(
            {"command": cmd},
            method="Popen",
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )

    def receive(
        self, stdin: Any, snapshot_name: str = "", parent_name: str | None = None
    ) -> Any:
        """Call 'btrfs receive', setting the given pipe as its stdin.

        ``snapshot_name`` and ``parent_name`` are accepted for a uniform endpoint
        interface -- RawEndpoint overrides receive() and uses them to name the
        output file and record metadata. For a real btrfs receive the subvolume
        name comes from the stream itself, so they are ignored here.
        """
        # Make sure we use the raw path without local resolution
        path = self.config["path"]
        # Ensure path is properly normalized for this endpoint type
        normalized_path = self._normalize_path(path)
        logger.debug(
            "Receive path: %s (type: %s)",
            normalized_path,
            type(normalized_path).__name__,
        )

        # Log more details for debugging
        logger.debug("Receive endpoint type: %s", type(self).__name__)
        logger.debug("Is remote endpoint: %s", getattr(self, "_is_remote", False))

        # The destination is NOT created here. This ran immediately before the
        # receive and rebuilt whatever _prepare had just refused, so declining to
        # create a configured path there would have been undone here -- and the
        # failure to create it was only warned about, leaving the receive to fail
        # afterwards with something less specific.
        #
        # A missing destination at this point means the filesystem holding it is
        # not mounted, so the stream would land on the root filesystem: hidden
        # once the real disk returns, and charged to the wrong free space.
        if isinstance(normalized_path, (str, Path)) and not getattr(
            self, "_is_remote", False
        ):
            path_obj = (
                Path(normalized_path)
                if isinstance(normalized_path, str)
                else normalized_path
            )
            if not path_obj.is_dir():
                logger.error("Destination path does not exist: %s", path_obj)
                raise __util__.AbortError(
                    f"Destination {path_obj} does not exist, so there is nowhere "
                    f"to receive into. It is most likely on a filesystem that is "
                    f"not mounted; btrfs-backup-ng does not create a configured "
                    f"destination."
                )

        cmd = self._build_receive_command(normalized_path)
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None

        logger.debug("Running receive command: %s", cmd)
        try:
            # Suppress stderr ("At subvol" messages) - they're just informational
            return self._exec_command(
                {"command": cmd},
                method="Popen",
                stdin=stdin,
                stdout=stdout,
                stderr=subprocess.DEVNULL,
            )
        except Exception as e:
            logger.error("Error executing receive command: %s", e)
            raise

    def commit_receive(self) -> None:
        """Durably commit received data on the success path.

        Called by the transfer engine only after the receive pipeline has been
        confirmed successful. The base endpoint receives into a btrfs subvolume,
        which ``btrfs receive`` already commits atomically, so this is a no-op.
        RawEndpoint overrides it to fsync a temporary stream file and atomically
        rename it to its final name.
        """
        return

    def list_snapshots(self, flush_cache: bool = False) -> List[Any]:
        """
        Return a list of all snapshots found directly in self.config['path'] using $snap_prefix.
        Populates a cache for efficient repeated access and removable snapshot checks.
        """
        snapshot_dir = Path(self.config["path"]).resolve()
        snap_prefix = self.config["snap_prefix"]
        # A listing is a READ: it must never create the path it was asked to
        # enumerate. It used to mkdir here, which quietly undid _prepare's
        # refusal to create a configured path (34904c6): every CLI command
        # calls prepare() first, but that is a per-command convention, not a
        # property of this primitive -- and the convention has a real gap.
        # Measured: with the path present, prepare() passes and a pool lists
        # its backups; the drive then unmounts mid-run, and the next listing
        # REBUILT the mount point on the root filesystem and reported the pool
        # empty -- so presence checks saw nothing and the planner scheduled
        # full re-sends into the directory the read had just invented.
        #
        # The discriminator between the two honest answers is the endpoint's
        # role, read from config["source"]: every source-side endpoint carries
        # its subvolume there, and destination endpoints do not. A missing
        # snapshot dir on the SOURCE is a legitimate baseline (a volume that
        # has never been snapshotted; snapshot() creates the directory at
        # first creation). A missing BACKUP DESTINATION is not empty, it is
        # unreadable, and saying "empty" is how an operator concludes their
        # backups are gone -- same contract as the ssh listing, which raises
        # rather than ever presenting a failed enumeration as an empty target.
        if not snapshot_dir.is_dir():
            if snapshot_dir.exists():
                # A file (or other non-directory) at the configured path is a
                # misconfiguration for either role. The old mkdir path let
                # this escape as a raw FileExistsError, which is not a
                # diagnosis.
                raise RuntimeError(
                    f"Cannot list snapshots at {snapshot_dir}: the path "
                    "exists but is not a directory."
                )
            if self.config.get("source"):
                logger.debug(
                    "Snapshot directory %s does not exist yet; a source "
                    "volume with no snapshots is a legitimate empty baseline "
                    "(nothing was created).",
                    snapshot_dir,
                )
                self.__cached_snapshots = []
                return []
            raise RuntimeError(
                f"Cannot list snapshots at {snapshot_dir}: the directory does "
                "not exist. The location could NOT be enumerated -- this is "
                "NOT an empty target. If the backups live on a removable or "
                "network filesystem it is most likely not mounted; mount it, "
                "check the path for a typo, or create the directory yourself. "
                "Nothing was created."
            )

        logger.debug("Listing snapshots in: %s", snapshot_dir)
        logger.debug("Snapshot prefix: %s", snap_prefix)

        # Use or refresh the cache
        if self.__cached_snapshots is not None and not flush_cache:
            logger.debug(
                "Returning %d cached snapshots for %r.",
                len(self.__cached_snapshots),
                self,
            )
            return list(self.__cached_snapshots)

        snapshots = []
        for item in self._listdir(snapshot_dir):
            item_path = Path(item)
            # Only consider items that are direct children of snapshot_dir and match the prefix
            if item_path.parent.resolve() == snapshot_dir and item_path.name.startswith(
                snap_prefix
            ):
                date_part = item_path.name[len(snap_prefix) :]
                logger.debug("Parsing date from: %r", date_part)
                time_obj, parsed_as_written = __util__.derive_snapshot_time(
                    date_part, self.config.get("timestamp_format")
                )
                if time_obj is None and not __util__.is_subvolume(item_path):
                    # Two different facts used to share this rejection: "not a
                    # snapshot at all" (README.md, lost+found) and "a snapshot
                    # whose name I cannot parse". What something IS answers the
                    # first: not a subvolume means not a snapshot -- debug
                    # level, normal directory clutter. A subvolume whose name
                    # yields no timestamp falls through: it IS a snapshot, with
                    # no extractable time.
                    logger.debug("Skipping non-snapshot item: %r", item_path.name)
                    continue
                snapshot = __util__.Snapshot(
                    snapshot_dir,
                    snap_prefix,
                    self,
                    time_obj=time_obj,
                    name=item_path.name,
                )
                snapshot.newly_visible = not parsed_as_written
                snapshots.append(snapshot)
        # R3: load persisted retention locks back onto the snapshots. set_lock writes them
        # to the lock file, but nothing read them back, so a snapshot kept locked after a
        # failed transfer looked unlocked on the next run and retention could prune a
        # snapshot a failed/pending transfer still needs.
        self._load_locks_into(snapshots)
        # Phase 0: best-effort btrfs identity so snapshots are self-describing (uuid +
        # received_uuid). Never fatal -- a non-root/non-btrfs enumeration keeps working
        # with empty uuids.
        self._load_subvolume_ids_into(snapshots)
        snapshots.sort()
        self._report_newly_visible(snapshots)
        self.__cached_snapshots = snapshots
        logger.debug(
            "Populated snapshot cache of %r with %d items.", self, len(snapshots)
        )
        return list(snapshots)

    def _load_subvolume_ids_into(self, snapshots: List[Any]) -> None:
        """Best-effort: populate each snapshot's btrfs ``uuid`` / ``received_uuid``.

        Runs ``btrfs subvolume show`` on each snapshot's exact path. ``show`` targets one
        path, so the identity is unambiguous even when snapshots live under a mounted
        (non-filesystem-root) subvolume -- unlike ``subvolume list``, whose id-5-relative
        paths cannot be reliably matched back to a mount-relative absolute path.

        ``subvolume show`` needs CAP_SYS_ADMIN to read the uuid/received_uuid, so it is
        sudo-escalated in the SAME context the transfer path uses (``sudo -n`` when not
        root; see the send/receive escalation and restore's parent-by-uuid lookup) --
        otherwise a non-root run WITH passwordless sudo would send/receive fine yet read
        empty uuids, silently degrading the planner to full sends and disabling the
        re-created-snapshot (received_uuid) correspondence. ``-n`` keeps enumeration
        non-interactive: with no passwordless sudo it fails fast rather than prompting.
        Purely additive and best-effort: any failure (no sudo, not btrfs, older
        btrfs-progs, command error) leaves that snapshot's uuids empty and enumeration
        unaffected. The planner consumes these identities (Phase 2 correspondence)."""
        if not snapshots:
            return
        sudo_prefix = ["sudo", "-n"] if os.geteuid() != 0 else []
        enriched = 0
        for snap in snapshots:
            try:
                result = subprocess.run(
                    [*sudo_prefix, "btrfs", "subvolume", "show", str(snap.get_path())],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
                if result.returncode != 0:
                    continue
                ids = __util__.parse_subvolume_show(result.stdout)
                snap.uuid = ids["uuid"]
                snap.received_uuid = ids["received_uuid"]
                if snap.uuid:
                    enriched += 1
            except subprocess.TimeoutExpired:
                logger.debug("subvolume show timed out for %s", snap.get_name())
            except Exception as e:  # noqa: BLE001 - best-effort, never fatal
                logger.debug(
                    "Could not read subvolume uuid for %s (ignoring): %s",
                    snap.get_name(),
                    e,
                )
        logger.debug(
            "btrfs uuid enrichment: %d/%d snapshot(s) have a uuid",
            enriched,
            len(snapshots),
        )

    def _load_locks_into(self, snapshots: List[Any]) -> None:
        """Populate each snapshot's in-memory lock sets from the persisted lock file.

        Locks are keyed by ``get_name()`` (see ``set_lock``). A damaged or unreadable
        lock file must never abort the whole listing (that would also stop new snapshots),
        so it lists as unlocked but sets ``_locks_read_failed`` -- retention then refuses
        to delete anything (fail-safe) rather than silently pruning a still-needed
        snapshot it can no longer see is locked."""
        try:
            lock_dict = self._read_locks()
            self._locks_read_failed = False
        except Exception as e:  # noqa: BLE001 - never abort a listing on a bad lock file
            # A corrupt/unreadable lock file must NOT silently become "no locks": that
            # would let retention prune a snapshot a pending transfer still needs. Keep
            # listing (so backups continue) but flag the failure so retention refuses to
            # delete anything until an operator repairs or removes the lock file.
            self._locks_read_failed = True
            logger.error(
                "Lock file %s is unreadable/corrupt (%s); snapshots will list as "
                "unlocked and retention will REFUSE to delete until it is repaired or "
                "removed.",
                self._get_lock_file_path(),
                e,
            )
            return
        for snap in snapshots:
            entry = lock_dict.get(snap.get_name())
            if not entry:
                continue
            snap.locks = set(entry.get("locks", []))
            snap.parent_locks = set(entry.get("parent_locks", []))

    def _report_newly_visible(self, snapshots: List[Any]) -> None:
        """Announce, once per listing refresh, snapshots that earlier releases
        could not list. The first run that can see such a snapshot is also the
        first that could prune it: a trailing ``_N`` (or any name retention's
        own parser can date) enters retention buckets immediately, so the
        operator hears about it BEFORE a deletion list does. INFO, not debug --
        silence here is how a foreign pool gets managed without anyone being
        told."""
        fresh = [s for s in snapshots if getattr(s, "newly_visible", False)]
        if not fresh:
            return
        shown = ", ".join(s.get_name() for s in fresh[:5])
        more = f" (and {len(fresh) - 5} more)" if len(fresh) > 5 else ""
        logger.info(
            "%d snapshot(s) at %s were not visible to earlier btrfs-backup-ng "
            "releases: %s%s. Those with a derivable timestamp are now managed "
            "by retention; those without are listed and kept but never "
            "deleted automatically.",
            len(fresh),
            self.config.get("path"),
            shown,
            more,
        )

    def correspondent_of(self, snapshot: Any) -> Optional[Any]:
        """Return THIS endpoint's snapshot that is the btrfs-receive copy of ``snapshot``.

        Two btrfs subvolumes correspond when one was produced by ``btrfs receive`` of a
        stream from the other: the received copy's ``received_uuid`` equals the source
        subvolume's ``uuid``. That is the correspondence btrfs incremental send/receive
        actually resolves on the destination -- NOT the on-disk name, which can collide
        (a re-created snapshot reuses the name but gets a new uuid). This is the single
        authority for "does this endpoint hold the correspondent of that snapshot"; it is
        used both to detect whether a source snapshot is already on a destination and to
        find a valid incremental parent.

        Returns the corresponding snapshot object, or ``None`` when identity is unknown
        (empty ``uuid`` -- a best-effort enrichment miss), no correspondent exists, or the
        listing fails. A ``None`` return is ALWAYS safe for callers: it degrades to a full
        (non-incremental) transfer, never an unapplyable ``send -p`` -- so this method
        never raises.

        This base implementation is the btrfs (uuid) semantics used by Local and SSH
        endpoints; raw endpoints override it with name semantics.
        """
        src_uuid = getattr(snapshot, "uuid", "")
        if not src_uuid:
            return None
        try:
            candidates = self.list_snapshots()
        except Exception as e:  # noqa: BLE001 - contract: never raise; None is safe
            logger.debug("correspondent_of: could not list snapshots (%s)", e)
            return None
        for candidate in candidates:
            if getattr(candidate, "received_uuid", "") == src_uuid:
                return candidate
        return None

    def correspondents_of(self, snapshots: List[Any]) -> Dict[str, Any]:
        """``{source_name: correspondent}`` for every snapshot, from ONE listing.

        The batch form of :meth:`correspondent_of`, with identical semantics --
        it is the same ``received_uuid == uuid`` rule, applied to a listing taken
        once instead of once per snapshot.

        This exists because the per-snapshot API forced the caller into O(n)
        listings. The transfer planner asks "is this already on the destination?"
        for every source snapshot, and on an ssh:// destination each of those was
        a separate remote ``btrfs subvolume list`` -- roughly three seconds each,
        so a 44-snapshot source spent over two minutes deciding what to send
        before sending anything (issue #106). The listing is the expensive part
        and it does not change while it is being consulted, so it is taken once.

        Never raises, exactly like the singular form: a listing failure yields an
        empty mapping, which reads as "nothing corresponds" and degrades to full
        transfers rather than to an unapplyable ``send -p``.
        """
        try:
            candidates = self.list_snapshots()
        except Exception as e:  # noqa: BLE001 - contract: never raise; empty is safe
            logger.debug("correspondents_of: could not list snapshots (%s)", e)
            return {}

        by_received: Dict[str, Any] = {}
        for candidate in candidates:
            received = getattr(candidate, "received_uuid", "")
            if received:
                # First wins, matching the singular form's first-match scan.
                by_received.setdefault(received, candidate)

        found: Dict[str, Any] = {}
        for snapshot in snapshots:
            src_uuid = getattr(snapshot, "uuid", "")
            if not src_uuid:
                continue
            match = by_received.get(src_uuid)
            if match is not None:
                found[snapshot.get_name()] = match
        return found

    def verify_structure(self, snapshot: Any) -> StructureVerdict:
        """Confirm ``snapshot`` is a real, valid backup ARTIFACT (not just a name-shaped
        directory entry) -- the polymorphic structural check behind the ``verify`` metadata
        level. This base implementation is the LOCAL btrfs semantics (Local endpoint);
        SSH and raw endpoints override it.

        A local backup is enumerated by ``iterdir`` (any directory entry whose NAME parses
        as a snapshot), so an interrupted ``btrfs receive`` that left a plain directory, or
        a hand-created folder, is listed like a real backup. This confirms otherwise:

        - NOT a btrfs subvolume (``is_subvolume`` is a privilege-free inode check) ->
          ``invalid``: deterministically not a backup (the interrupted-receive / plain-dir
          case). This is the core false-pass this method closes.
        - a subvolume with a non-empty ``received_uuid`` (set only by ``btrfs receive``,
          and a received subvolume is read-only by construction) -> ``ok``.
        - a subvolume whose ``received_uuid`` could not be confirmed (empty: a non-received
          subvolume, or the best-effort ``btrfs subvolume show`` enrichment could not run
          without privilege) -> ``unverifiable``: never fail a real subvolume just because
          the environment could not prove it is a received backup.
        """
        try:
            path = snapshot.get_path()
        except Exception:  # noqa: BLE001 - a snapshot with no resolvable path is not ours
            return StructureVerdict("unverifiable", "could not resolve snapshot path")
        try:
            is_subvol = __util__.is_subvolume(path)
        except OSError as e:
            # Could not stat the path (e.g. permission) -- cannot determine; do not fail.
            return StructureVerdict(
                "unverifiable", f"could not check subvolume status: {e}"
            )
        if not is_subvol:
            return StructureVerdict(
                "invalid",
                "not a btrfs subvolume (a plain directory -- e.g. an interrupted "
                "receive left a partial/empty backup)",
            )
        if getattr(snapshot, "received_uuid", ""):
            return StructureVerdict("ok", "received read-only subvolume")
        return StructureVerdict(
            "unverifiable",
            "a subvolume, but its received_uuid could not be confirmed "
            "(not a received backup, or btrfs subvolume show needs privilege)",
        )

    def test_send_stream(self, snapshot: Any, parent: Any = None) -> None:
        """Verify a ``btrfs send`` stream can be generated for ``snapshot`` -- the STREAM
        verification level for a btrfs subvolume target. Runs ``btrfs send --no-data`` (a
        metadata-only stream: it validates the subvolume and, with ``-p``, the incremental
        parent chain, without moving any file data) and raises ``VerifyError`` on failure.

        This base implementation runs the command LOCALLY (the Local endpoint). SSH
        endpoints override it to run ON THE REMOTE where the subvolume actually lives, and
        raw endpoints override it to reject (a stored stream file is not a subvolume; raw
        integrity is verified by checksum)."""
        from btrfs_backup_ng.core.verify import VerifyError

        cmd = ["btrfs", "send", "--no-data"]
        if parent is not None:
            cmd.extend(["-p", str(parent.get_path())])
        cmd.append(str(snapshot.get_path()))
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=300,
        )
        if result.returncode != 0:
            raise VerifyError(
                f"Send stream test failed: "
                f"{result.stderr.decode(errors='replace').strip()}"
            )

    #: Whether this endpoint reaches its destination over the network. Declared
    #: here so it is part of the endpoint contract rather than an attribute that
    #: some subclasses happen to set: it was referenced through
    #: ``hasattr(self, "_is_remote") and getattr(self, "_is_remote", False)`` in
    #: half a dozen places, which is what an undeclared attribute costs, and a
    #: type checker could only report it as unknown.
    _is_remote: bool = False

    #: Whether ``set_lock`` writes the lock to durable storage. True here because
    #: the base implementation persists to a lock file at ``config['path']``.
    #: Endpoints that override ``set_lock`` to mutate only the in-memory lock set
    #: set this False, so callers can tell "no locks are held" apart from "locks
    #: are never recorded here" -- reporting the second as the first is a false
    #: all-clear, and `restore --status` did exactly that.
    persists_locks: bool = True

    def set_lock(
        self, snapshot: Any, lock_id: Any, lock_state: bool, parent: bool = False
    ) -> None:
        """Add or remove the given lock from ``snapshot`` and update the lock file.

        NOT ``@require_source``: a lock lives at the endpoint's ``path`` (the backup
        location), not at a source subvolume. During a restore this is called on the
        BACKUP endpoint -- which correctly has no ``source`` -- to lock the snapshot
        being restored against deletion. Requiring a source here aborted native btrfs
        restore at the lock step before any bytes moved; raw endpoints already override
        set_lock without the guard. Removing it fully enables LOCAL btrfs restore;
        restore from a remote ssh:// btrfs source is separately gated in the restore CLI
        (base send/lock run locally, so a real remote source can't be streamed back yet).
        """
        if lock_state:
            (snapshot.parent_locks if parent else snapshot.locks).add(lock_id)
        else:
            (snapshot.parent_locks if parent else snapshot.locks).discard(lock_id)
        # Read-modify-write against the AUTHORITATIVE on-disk lock file, serialized by a
        # FileLock so concurrent parallel-target transfers (multiple threads sharing this
        # source endpoint) and concurrent processes cannot lose each other's updates. The
        # previous rebuild-from-cache dropped a lock written after this run's cache was
        # populated (last-writer-wins) and could lose the mutation entirely on a cache
        # miss. We only touch THIS snapshot's entry; every other snapshot's locks come
        # straight from disk, so a concurrent writer's locks survive.
        guard = str(self._get_lock_file_path()) + ".guard"
        with FileLock(guard):
            # _read_locks raises on a corrupt file: abort loudly rather than overwrite
            # (which would silently discard locks we could not read).
            lock_dict = self._read_locks()
            name = snapshot.get_name()
            entry: Dict[str, Any] = {}
            if snapshot.locks:
                entry["locks"] = list(snapshot.locks)
            if snapshot.parent_locks:
                entry["parent_locks"] = list(snapshot.parent_locks)
            if entry:
                lock_dict[name] = entry
            else:
                lock_dict.pop(name, None)
            self._write_locks(lock_dict)
        logger.debug(
            "Lock state for %s and lock_id %s changed to %s (parent = %s)",
            snapshot,
            lock_id,
            lock_state,
            parent,
        )

    def add_snapshot(self, snapshot: Any, rewrite: bool = True) -> None:
        """Add a snapshot to the cache, once.

        Idempotent by PATH. The caller above registers exactly one snapshot per
        create, but a cache that can hold the same snapshot twice is a retention
        budget that can be wrong by that many -- and the damage is not
        proportional: a single duplicate of the newest entry makes `-N 1` delete
        everything, including the snapshot just taken. Belt as well as braces,
        because the cost of the guard is one comparison and the cost of it being
        absent was measured at total loss.
        """
        if self.__cached_snapshots is None:
            return
        if rewrite:
            snapshot = __util__.Snapshot(
                self.config["path"],
                snapshot.prefix,
                self,
                time_obj=snapshot.time_obj,
                name=snapshot.get_name(),
            )
        path = str(snapshot.get_path())
        if any(str(s.get_path()) == path for s in self.__cached_snapshots):
            logger.debug("Snapshot already in the cache, not adding again: %s", path)
            return
        self.__cached_snapshots.append(snapshot)
        self.__cached_snapshots.sort()

    def delete_snapshots(self, snapshots: List[Any], **kwargs: Any) -> DeletionResult:
        """Delete the given snapshots (subvolumes), reporting what happened to each.

        Returns a :class:`DeletionResult` rather than None. Nothing here raises --
        a corrupt lock file refuses the batch, a locked snapshot is skipped, a
        failed ``btrfs subvolume delete`` is logged -- so a caller that inferred
        success from the absence of an exception was counting every one of those
        as a deletion.
        """
        result = DeletionResult()
        if getattr(self, "_locks_read_failed", False):
            reason = (
                "the lock file is unreadable/corrupt, so locked (still-needed) "
                f"snapshots cannot be identified; repair or remove "
                f"{self._get_lock_file_path()} and retry"
            )
            logger.error("Refusing to delete snapshots: %s.", reason)
            result.fail_all(snapshots, reason)
            return result
        for snapshot in snapshots:
            if snapshot.locks or snapshot.parent_locks:
                logger.info("Skipping locked snapshot: %s", snapshot)
                result.skip(snapshot, "held by a retention lock")
                continue
            # Built by _build_deletion_commands rather than hand-rolled here, so
            # `convert_rw` gets its `btrfs property set -ts ... ro false` ahead of
            # the delete. Both are legacy CLI flags -- `-w/--convert-rw` and
            # `-s/--sync` -- that reach this endpoint's config and were then
            # dropped, because this method assembled its own single command and
            # consulted neither. Their help text promised behaviour that never
            # happened. `subvolume_sync` is deliberately excluded per snapshot:
            # it is one trailing command for the whole batch, issued below.
            commands = self._build_deletion_commands([snapshot], subvolume_sync=False)
            logger.debug("Executing deletion command(s): %s", commands)
            for cmd in commands:
                try:
                    logger.debug("Deleting snapshot with path: %s", snapshot.get_path())
                    self._exec_command({"command": cmd})
                except Exception as e:
                    logger.error(
                        "Failed to delete snapshot %s: %s", snapshot.get_path(), e
                    )
                    logger.error("Deletion command was: %s", cmd)
                    result.fail(snapshot, e)
                    break
            else:
                logger.info("Deleted snapshot subvolume: %s", snapshot.get_path())
                result.deleted.append(snapshot)
                # Evicted only on success, inside this branch. It used to sit at
                # the loop's own indent, so a snapshot whose deletion FAILED was
                # dropped from the cache anyway and every later list_snapshots()
                # in the process reported it gone -- the listing agreeing with a
                # deletion that did not happen.
                if self.__cached_snapshots is not None:
                    with contextlib.suppress(ValueError):
                        self.__cached_snapshots.remove(snapshot)
        # `btrfs subvolume sync` waits for the deletions to finish being cleaned
        # up, so it belongs after the batch and only if the batch deleted
        # something. It is not a per-snapshot verdict: the subvolumes are already
        # gone from the tree, and failing the deletions over a failed wait would
        # report a loss that did not happen.
        if result.deleted and self.config.get("subvolume_sync", False):
            for cmd in self._build_deletion_commands(
                [], convert_rw=False, subvolume_sync=True
            ):
                try:
                    self._exec_command({"command": cmd})
                except Exception as e:
                    logger.error(
                        "Deleted %d snapshot(s), but 'btrfs subvolume sync' on %s "
                        "failed: %s. The deletions stand; the filesystem may still "
                        "be reclaiming their space.",
                        result.deleted_count,
                        self.config["path"],
                        e,
                    )
        return result

    def delete_snapshot(self, snapshot: Any, **kwargs: Any) -> DeletionResult:
        """Delete a snapshot."""
        return self.delete_snapshots([snapshot], **kwargs)

    def delete_old_snapshots(self, keep: int) -> DeletionResult:
        """Delete old snapshots, keeping only the most recent ``keep`` unlocked snapshots.

        LEGACY COUNT-based retention: keeps a fixed NUMBER of snapshots, ignoring age/time
        buckets. It is used only by the legacy CLI path (``core.execution.run_task`` ->
        ``_cleanup_snapshots``, reached via ``cli/dispatcher.is_legacy_mode``). The MODERN,
        time-based policy engine is ``retention.apply_retention`` (min + hourly/daily/weekly/
        monthly/yearly buckets), driven by the ``prune`` command -- prefer it for any new code.
        Chain/lock safety is preserved either way: this routes deletions through
        ``delete_snapshots`` (R10b incremental-parent guard) and skips R3-locked snapshots.
        """
        snapshots = self.list_snapshots()
        if getattr(self, "_locks_read_failed", False):
            logger.error(
                "Refusing to delete old snapshots: the lock file is unreadable/corrupt, "
                "so locked (still-needed) snapshots cannot be identified. Repair or "
                "remove %s and retry.",
                self._get_lock_file_path(),
            )
            result = DeletionResult()
            result.fail_all(
                snapshots,
                "the lock file is unreadable/corrupt, so locked (still-needed) "
                "snapshots cannot be identified",
            )
            return result
        unlocked = [s for s in snapshots if not s.locks and not s.parent_locks]
        # A snapshot with no derivable timestamp neither counts toward the
        # keep budget nor can be deleted by it: count-based retention keeps
        # "the newest N", and these have no place in that order. They sort
        # LAST, so leaving them in would let them occupy keep slots and push
        # real, dated snapshots into the delete slice. Reported, never silent.
        undated = [s for s in unlocked if getattr(s, "time_obj", None) is None]
        if undated:
            logger.info(
                "%d unlocked snapshot(s) have no derivable timestamp; they "
                "are kept, and occupy none of the %d keep slot(s): %s",
                len(undated),
                keep,
                ", ".join(s.get_name() for s in undated[:5])
                + (f" (and {len(undated) - 5} more)" if len(undated) > 5 else ""),
            )
            unlocked = [s for s in unlocked if getattr(s, "time_obj", None) is not None]
        if keep <= 0 or len(unlocked) <= keep:
            logger.debug(
                "No unlocked snapshots to delete (keep=%d, unlocked=%d)",
                keep,
                len(unlocked),
            )
            return DeletionResult()
        to_delete = unlocked[:-keep]
        result = DeletionResult()
        for snap in to_delete:
            logger.info("Deleting old snapshot: %s", snap)
            result.extend(self.delete_snapshots([snap]))
        return result

    def protect_incremental_parents(
        self, to_keep: List[Any], to_delete: List[Any]
    ) -> tuple[List[Any], List[Any]]:
        """Adjust a retention decision so deleting a snapshot cannot break an incremental chain
        a KEPT snapshot still depends on. Returns the (possibly adjusted) ``(to_keep, to_delete)``.

        Base (btrfs) behaviour is a NO-OP: on a btrfs destination a received subvolume is
        self-contained once committed, so pruning an incremental PARENT only forces the next
        backup to be a full send (a bandwidth/space cost) -- never data loss. Raw targets, whose
        backups are ``btrfs send`` STREAM files, override this: deleting a parent stream makes
        every dependent child stream unrestorable, so raw must protect the chain.
        """
        return to_keep, to_delete

    def get_space_info(self, path: Optional[str] = None) -> SpaceInfo:
        """Get space information for the endpoint's destination path.

        Queries filesystem space and btrfs quota information (if available)
        for the specified path or the endpoint's configured path.

        Args:
            path: Optional path to check. If None, uses self.config['path'].

        Returns:
            SpaceInfo with filesystem and quota information.

        Note:
            Subclasses may override this for remote endpoints (e.g., SSH).
        """
        if path is None:
            path = str(self.config["path"])
        else:
            path = str(path)

        use_sudo = self.config.get("ssh_sudo", False) or os.geteuid() != 0
        return _get_space_info(path, exec_func=None, use_sudo=use_sudo)

    # The following methods may be implemented by endpoints unless the
    # default behaviour is wanted.

    def __repr__(self) -> str:
        return f"{self.config['path']}"

    def get_id(self) -> str:
        """Return an id string to identify this endpoint over multiple runs."""
        # Ensure path is normalized to string for consistent IDs
        path = self._normalize_path(self.config["path"])
        return f"unknown://{path}"

    def _prepare(self) -> None:
        """Called after endpoint creation for additional checks."""
        pass

    @staticmethod
    def _build_snapshot_cmd(
        source: Any, destination: Any, readonly: bool = True
    ) -> List[Any]:
        # Use tuples to mark command arguments that shouldn't be normalized as paths
        cmd = [("btrfs", False), ("subvolume", False), ("snapshot", False)]
        if readonly:
            cmd += [("-r", False)]
        cmd += [(str(source), True), (str(destination), True)]
        logger.debug("Snapshot command: %s", [arg for arg, _ in cmd])
        return cmd

    @staticmethod
    def _build_sync_command() -> List[Any]:
        return [("sync", False)]

    def _build_send_command(
        self, snapshot: Any, parent: Any = None, clones: Optional[List[Any]] = None
    ) -> List[Any]:
        # Use tuples to mark command arguments that shouldn't be normalized as paths
        cmd = [("btrfs", False), ("send", False)]
        # Add btrfs flags
        for flag in self.btrfs_flags:
            cmd.append((flag, False))

        log_level = logging.getLogger().getEffectiveLevel()
        if log_level >= logging.WARNING:
            cmd.append(("--quiet", False))
        if parent:
            cmd.append(("-p", False))
            cmd.append((str(parent.get_path()), True))
            logger.debug("Using parent for send: %s", parent.get_path())
        if clones:
            for clone in clones:
                cmd.append((str(clone.get_path()), True))
                logger.debug("Added clone for send: %s", clone.get_path())
        cmd.append((str(snapshot.get_path()), True))
        logger.debug("Built send command: %s", [(a, p) for a, p in cmd])
        return cmd

    def _build_receive_command(self, destination: Any) -> List[Any]:
        # Ensure destination is properly formatted as a string without resolving
        # This avoids path resolution that could break remote paths
        dest_str = str(destination)
        logger.debug("Building receive command with destination: %s", dest_str)

        # Add more debug info about destination
        if isinstance(destination, Path):
            logger.debug("Destination is a Path object, converting to string")

        # Use tuples to mark command arguments that shouldn't be normalized as paths
        cmd = [("btrfs", False), ("receive", False)]
        for flag in self.btrfs_flags:
            cmd.append((flag, False))
        cmd.append((dest_str, True))
        logger.debug("Receive command: %s", [arg for arg, _ in cmd])
        return cmd

    def _build_deletion_commands(
        self,
        snapshots: List[Any],
        convert_rw: Optional[bool] = None,
        subvolume_sync: Optional[bool] = None,
    ) -> List[Any]:
        convert_rw = (
            self.config.get("convert_rw", False) if convert_rw is None else convert_rw
        )
        subvolume_sync = (
            self.config.get("subvolume_sync", False)
            if subvolume_sync is None
            else subvolume_sync
        )
        commands = []
        if convert_rw:
            for snapshot in snapshots:
                # Use tuples to mark command arguments that shouldn't be normalized as paths
                commands.append(
                    [
                        ("btrfs", False),
                        ("property", False),
                        ("set", False),
                        ("-ts", False),
                        (str(snapshot.get_path()), True),
                        ("ro", False),
                        ("false", False),
                    ]
                )
        for snapshot in snapshots:
            commands.append(
                [
                    ("btrfs", False),
                    ("subvolume", False),
                    ("delete", False),
                    (str(snapshot.get_path()), True),
                ]
            )
        if subvolume_sync:
            commands.append(
                [
                    ("btrfs", False),
                    ("subvolume", False),
                    ("sync", False),
                    (str(self.config["path"]), True),
                ]
            )
        return commands

    def _collapse_commands(
        self, commands: List[Any], abort_on_failure: bool = True
    ) -> List[Any]:
        return commands

    def _exec_command(self, options: Dict[str, Any], **kwargs: Any) -> Any:
        command = options.get("command")
        if not command:
            raise ValueError("No command specified in options for _exec_command")

        # Process command based on whether arguments are marked as paths or not
        try:
            normalized_command = []
            logger.debug("Original command to normalize: %s", command)

            # Check if command is using the tuple format (arg, is_path)
            if command and isinstance(command[0], tuple) and len(command[0]) == 2:
                # New format with (arg, is_path) tuples
                logger.debug("Using tuple format for command normalization")
                for i, (arg, is_path) in enumerate(command):
                    logger.debug(
                        "Processing arg %d: %s (is_path=%s, type=%s)",
                        i,
                        arg,
                        is_path,
                        type(arg).__name__,
                    )
                    if is_path and isinstance(arg, (str, Path)):
                        try:
                            normalized_arg = self._normalize_path(arg)
                            logger.debug(
                                "Normalized path arg %s to: %s", arg, normalized_arg
                            )
                            normalized_command.append(normalized_arg)
                        except Exception as e:
                            logger.warning(
                                "Path normalization failed for %s: %s", arg, e
                            )
                            # Use original argument if normalization fails
                            normalized_command.append(arg)
                    else:
                        # Not a path, just append as-is
                        logger.debug("Using non-path arg as-is: %s", arg)
                        normalized_command.append(arg)
                logger.debug(
                    "Processed marked command arguments: %s", normalized_command
                )
            else:
                # Legacy format - attempt to guess which args are paths
                logger.debug("Using legacy format for command normalization")
                for i, arg in enumerate(command):
                    if isinstance(arg, (str, Path)):
                        # First argument is a command - don't normalize it as a path
                        if i == 0 or (isinstance(arg, str) and arg.startswith("-")):
                            normalized_command.append(arg)
                            logger.debug("Not normalizing argument %d: %s", i, arg)
                        else:
                            try:
                                normalized_arg = self._normalize_path(arg)
                                logger.debug(
                                    "Normalized path arg %d %s to: %s",
                                    i,
                                    arg,
                                    normalized_arg,
                                )
                                normalized_command.append(normalized_arg)
                            except Exception as e:
                                logger.warning(
                                    "Path normalization failed for %s: %s", arg, e
                                )
                                # Use original argument if normalization fails
                                normalized_command.append(arg)
                    else:
                        logger.debug("Keeping non-string/path arg %d as-is: %s", i, arg)
                        normalized_command.append(arg)
                logger.debug("Processed legacy command format: %s", normalized_command)

            command = normalized_command
        except Exception as e:
            logger.error("Error normalizing command arguments: %s", e)
            # Continue with original command if normalization completely fails
            logger.warning("Using original command without normalization")

        # Convert all command arguments to strings for subprocess
        command = [str(arg) for arg in command]
        logger.debug("Executing command: %s", command)
        lock_path = _command_lock_path()

        try:
            with FileLock(lock_path):
                # Convert command to string for proper detection
                first_cmd = str(command[0]) if command else ""

                if os.geteuid() != 0 and command and first_cmd == "btrfs":
                    # Find the full path to btrfs command if needed
                    if first_cmd == "btrfs" and "/" not in first_cmd:
                        # This preserves just using "btrfs" which will use PATH
                        pass

                    if options.get("no_password_sudo"):
                        command = ["sudo", "-n"] + command
                    else:
                        command = ["sudo"] + command

                # Ensure all command arguments are strings
                command = [str(arg) for arg in command]
                logger.debug("Final command after sudo adjustment: %s", command)
                # Log the command with file paths, useful for debugging
                cwd = os.getcwd()
                logger.debug("Current working directory: %s", cwd)
                logger.debug("Executing command with absolute paths:")
                for i, arg in enumerate(command):
                    logger.debug("  Arg %d: %s", i, arg)
                # Make sure the environment includes the PATH
                env = kwargs.get("env", os.environ.copy())
                logger.debug("Environment PATH: %s", env.get("PATH", "Not set"))
                kwargs["env"] = env
                return __util__.exec_subprocess(command, **kwargs)
        except Exception as e:
            logger.error("Error in _exec_command: %s", e)
            raise

    def _listdir(self, location: Any) -> List[str]:
        # For remote endpoints, don't try to resolve the path locally
        if self._is_remote:
            # Remote endpoints should implement their own _listdir
            logger.debug(
                "Using default _listdir implementation on remote path: %s", location
            )
            location = Path(location)
        else:
            location = Path(location).resolve()

        if not location.exists():
            logger.debug("Path does not exist for _listdir: %s", location)
            return []

        logger.debug("Listing directory contents: %s", location)
        return [str(item) for item in location.iterdir()]

    def _remount(self, path: Any, read_write: bool = True) -> None:
        """Remount a filesystem as read-write or read-only.

        Note: This only works on actual mount points. Subvolumes within a btrfs
        filesystem are not mount points and cannot be remounted independently.
        If the path is not a mount point, this method logs a debug message and
        returns without error, as the parent filesystem is likely already rw.
        """
        logger.debug("Checking remount for %s as read-write: %r", path, read_write)
        mode = "rw" if read_write else "ro"
        path_str = str(path)

        # Check if this path is actually a mount point
        try:
            output = subprocess.check_output(["mount"], text=True).splitlines()
            is_mount_point = False
            already_correct_mode = False

            for line in output:
                # Parse mount output: "device on /path type fstype (options)"
                parts = line.split(" on ", 1)
                if len(parts) < 2:
                    continue
                mount_info = parts[1]
                # Extract mount point (before " type ")
                if " type " in mount_info:
                    mount_point = mount_info.split(" type ")[0].strip()
                    if mount_point == path_str:
                        is_mount_point = True
                        # Check if already in correct mode
                        if (
                            f",{mode}," in line
                            or f"({mode}," in line
                            or f",{mode})" in line
                        ):
                            already_correct_mode = True
                        break

            if not is_mount_point:
                logger.debug(
                    "%s is not a mount point (likely a btrfs subvolume), skipping remount",
                    path_str,
                )
                return

            if already_correct_mode:
                logger.debug("%s already mounted as %s", path_str, mode)
                return

        except subprocess.CalledProcessError as e:
            logger.error("Failed to check mount status %r", e)
            raise __util__.AbortError from e

        # Path is a mount point, attempt remount
        cmd = ["mount", "-o", f"remount,{mode}", path_str]
        if os.geteuid() != 0:
            cmd = ["sudo"] + cmd
        logger.debug("Executing remount command: %s", cmd)
        try:
            env = os.environ.copy()
            subprocess.check_call(cmd, env=env)
        except subprocess.CalledProcessError as e:
            logger.error(
                "Failed to remount %s as %s: %r %r %r",
                path_str,
                mode,
                e.returncode,
                e.stderr,
                e.stdout,
            )
            raise __util__.AbortError from e

    def _entry_snapshot_name(self, entry: str) -> Optional[str]:
        """The snapshot name a directory entry represents, or None if it is not one.

        Where one snapshot is one subvolume the entry IS the name, so this is
        identity. A raw destination stores FILES -- ``<name>.btrfs`` plus
        compression and encryption suffixes -- and overrides this. Without the
        seam, the prefix diagnostics tried to parse ``home-20260810-101010.btrfs``
        as ``<prefix><timestamp>``, which never parses, so a raw location full of
        backups had nothing to report and answered a prefix mismatch with silence.
        """
        return entry

    def _snapshot_names_at_location(self) -> Optional[List[str]]:
        """Entries at this location as snapshot names, or None if unreadable.

        Shared by both prefix diagnostics so they cannot disagree about what is
        there: one enumerates to decide, the other to explain, and an endpoint
        that answered one but not the other would name a prefix on screen that
        the restore could not then act on.
        """
        path = self.config.get("path")
        if not path:
            return None
        try:
            entries = [
                str(entry).rstrip("/").rsplit("/", 1)[-1]
                for entry in self._listdir(path)
            ]
        except Exception as e:  # noqa: BLE001 - a diagnostic must never itself abort
            logger.debug("Could not enumerate %s for a prefix hint: %s", path, e)
            return None
        names = [self._entry_snapshot_name(entry) for entry in entries]
        return [name for name in names if name]

    def prefixes_present(self) -> Dict[str, int]:
        """Snapshot prefixes actually at this location, and how many use each.

        ``describe_empty_listing`` already works this out in order to say "re-run
        with --prefix X", but it returns prose. A caller that wants to ACT on the
        answer -- rather than print it and give up -- needs the values, so both
        share one computation instead of the answer existing only inside a
        sentence.
        """
        names = self._snapshot_names_at_location()
        if names is None:
            return {}

        configured = self.config.get("snap_prefix", "") or ""
        fmt = self.config.get("timestamp_format")
        found: Dict[str, int] = {}
        for name in names:
            inferred = __util__.infer_snapshot_prefix(name, fmt)
            if inferred is None or inferred == configured:
                continue
            found[inferred] = found.get(inferred, 0) + 1
        return found

    def describe_empty_listing(self) -> Optional[str]:
        """Explain an empty listing, or return None if the location really is empty.

        A listing filters on ``snap_prefix`` and then requires the rest of each
        name to parse as a timestamp, so the wrong prefix -- or none -- discards
        every real snapshot and the location reports as empty. Measured: a
        destination holding a backup answered `restore --list` with "No snapshots
        found at backup location" and exit 0, and supplying the exact prefix
        listed it immediately. During disaster recovery that is the worst
        possible time to be told your backups are not there.

        This does not change what is listed. It only distinguishes "nothing is
        here" from "something is here that your prefix did not match", and names
        the prefixes that would have matched.
        """
        names = self._snapshot_names_at_location()
        if names is None:
            return None
        return self._explain_prefix_mismatch(names)

    def _explain_prefix_mismatch(
        self, names: List[str], elsewhere: Optional[List[str]] = None
    ) -> Optional[str]:
        """Wording shared by every endpoint's ``describe_empty_listing``.

        ``names`` are entries AT the location; ``elsewhere`` are snapshot-shaped
        names that exist but not at this location (an ssh:// listing sees the
        whole filesystem). Naming those separately turns "no snapshots" into
        "you are pointed at the wrong directory", which is usually the truth.
        """
        configured = self.config.get("snap_prefix", "") or ""
        fmt = self.config.get("timestamp_format")

        def prefixes(candidates: List[str]) -> Dict[str, int]:
            found: Dict[str, int] = {}
            for name in candidates or []:
                inferred = __util__.infer_snapshot_prefix(name, fmt)
                if inferred is None or inferred == configured:
                    continue
                found[inferred] = found.get(inferred, 0) + 1
            return found

        here = prefixes(names)
        there = prefixes(elsewhere or [])
        if not here and not there:
            return None

        def render(counts: Dict[str, int]) -> str:
            return ", ".join(
                f"{prefix!r} ({count} snapshot{'s' if count != 1 else ''})"
                for prefix, count in sorted(counts.items(), key=lambda kv: -kv[1])
            )

        lines = []
        if here:
            lines.append(
                f"This location is NOT empty: it holds snapshots, but none use the "
                f"prefix {configured!r} that was searched for."
            )
            lines.append(f"Prefixes actually present here: {render(here)}.")
            best = max(here.items(), key=lambda kv: kv[1])[0]
            lines.append(f"Re-run with --prefix {best!r} to list them.")
        else:
            lines.append(
                f"No snapshots are at this location under any prefix, but snapshots "
                f"DO exist elsewhere on the same filesystem: {render(there)}."
            )
            lines.append(
                "That usually means the path points somewhere other than the "
                "destination the backups were written to."
            )
        return "\n".join(lines)

    def _get_lock_file_path(self) -> Path:
        # Lock file lives at the endpoint's path (backup location), not a source.
        if self.config["path"] is None:
            raise ValueError("path hasn't been set")
        # Coerce to Path: LocalEndpoint resolves ``path`` to a Path, but SSH endpoints
        # keep it as a str, so ``path / name`` raised TypeError ('str' / 'str') on a
        # restore FROM an ssh:// btrfs backup (which sets a lock on the ssh endpoint).
        return Path(self.config["path"]) / str(self.config["lock_file_name"])

    def _read_locks(self) -> Dict[str, Any]:
        path = self._get_lock_file_path()
        try:
            # ABSENT means genuinely no locks: nothing has ever locked this
            # target. PRESENT BUT NOT A REGULAR FILE does not -- it means the
            # lock state cannot be read, and "no locks" is the one answer that
            # must never be invented here, because retention trusts it and would
            # prune a snapshot that is still locked. `is_file()` alone conflated
            # the two: a directory, a device or a dangling symlink at this path
            # all read back as zero locks.
            #
            # lstat, not stat: the writer opens with O_NOFOLLOW (via
            # atomic_write_bytes), so it never writes THROUGH a symlink. A reader
            # that followed one would return something the writer never wrote,
            # and this file can live in a backup target that untrusted users can
            # write to.
            try:
                st = os.lstat(path)
            except FileNotFoundError:
                return {}
            if not stat.S_ISREG(st.st_mode):
                raise ValueError(
                    f"lock file is not a regular file (mode {st.st_mode:o})"
                )
            with open(path, encoding="utf-8") as f:
                return __util__.read_locks(f.read())
        except (OSError, ValueError) as e:
            logger.error("Error on reading lock file %s: %s", path, e)
            raise __util__.AbortError

    def _write_locks(self, lock_dict: Dict[str, Any]) -> None:
        path = self._get_lock_file_path()
        data = __util__.write_locks(lock_dict)
        try:
            logger.debug("Writing lock file: %s", path)
            # Atomic replace via the shared primitive (R7): a crash mid-write can never
            # leave a half-written / corrupt lock file (which would then be misread as
            # "no locks" and let retention prune a locked snapshot). The primitive does
            # the temp -> fsync -> os.replace -> parent-dir fsync dance with
            # O_EXCL|O_NOFOLLOW at 0600.
            __util__.atomic_write_bytes(path, data, mode=0o600)
        except OSError as e:
            logger.error("Error on writing lock file %s: %s", path, e)
            raise __util__.AbortError
