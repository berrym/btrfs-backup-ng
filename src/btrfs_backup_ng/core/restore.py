"""Core restore operations: restore snapshots from backup locations.

A native restore is a transfer through the engine with the roles swapped: the
backup location is the SOURCE, a local btrfs filesystem is the DESTINATION,
and the planner and executor that move every backup move the restore too.
What is restore-only lives here: choosing which snapshot (by name, by time,
the latest, all of them), inferring the prefix a location uses, the stats a
run reports, and the layout the copies land in (``core.layout``).

A snapper restore is the same transfer into the snapper layout: the
backup location's slots or streams are the source, the local config's
numbered slots the destination, and what is snapper-only lives here too --
enumerating a snapper layout on a local, ssh:// or raw location, and the
renumbered ``info.xml`` a restored slot gets.
"""

import dataclasses
import json
import contextlib
import logging
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from .. import __util__
from ..transaction import log_transaction
from . import operations as _ops
from .layout import PlainLayout
from .operations import (
    _execute_transfers,
    _list_snapper_backups_at_destination,
    _SnapperBtrfsBackup,
)
from .planning import PlanningError, plan_transfer_sequence


logger = logging.getLogger(__name__)


class RestoreError(Exception):
    """Error during restore operation."""

    pass


def find_snapshot_by_name(name: str, snapshots: list):
    """Find a snapshot by name in a list of snapshots.

    Args:
        name: Snapshot name to find
        snapshots: List of Snapshot objects

    Returns:
        Snapshot object if found, None otherwise
    """
    for snap in snapshots:
        if snap.get_name() == name:
            return snap
    return None


def find_snapshot_before_time(
    target_time: time.struct_time,
    snapshots: list,
):
    """Find the most recent snapshot before a given time.

    Args:
        target_time: Time to search before
        snapshots: List of Snapshot objects (should be sorted)

    Returns:
        Most recent Snapshot before target_time, or None
    """
    candidates = []
    undated = 0
    for snap in snapshots:
        if hasattr(snap, "time_obj") and snap.time_obj is not None:
            if snap.time_obj <= target_time:
                candidates.append(snap)
        else:
            undated += 1
    if undated:
        logger.info(
            "%d snapshot(s) have no derivable timestamp and cannot be matched "
            "against a time bound; they were not considered.",
            undated,
        )

    if not candidates:
        return None

    # Return most recent (last in sorted order)
    return max(candidates, key=lambda s: s.time_obj)


def validate_restore_destination(
    path: Path,
    in_place: bool = False,
    force: bool = False,
) -> None:
    """Validate that destination is suitable for restore.

    Args:
        path: Destination path
        in_place: Whether this is an in-place restore (dangerous)
        force: Whether to bypass safety checks

    Raises:
        RestoreError: If destination is invalid or unsafe
    """
    path = Path(path).resolve()

    # Check path exists or can be created
    if not path.exists():
        try:
            path.mkdir(parents=True, exist_ok=True)
            logger.info("Created restore destination: %s", path)
        except OSError as e:
            raise RestoreError(f"Cannot create destination directory {path}: {e}")

    # Must be on btrfs filesystem
    if not __util__.is_btrfs(path):
        raise RestoreError(
            f"Destination {path} is not on a btrfs filesystem. "
            "btrfs receive requires a btrfs filesystem."
        )

    # In-place restore requires explicit confirmation
    if in_place and not force:
        raise RestoreError(
            f"In-place restore to {path} is dangerous. "
            "Use --yes-i-know-what-i-am-doing to proceed."
        )


def _retry_with_inferred_prefix(backup_endpoint: Any) -> list[Any]:
    """List again under the prefix the location actually uses, if there is one.

    Returns the snapshots found, or an empty list when nothing can be inferred
    or the choice is ambiguous. Ambiguity is deliberately NOT resolved by
    picking the most populous prefix: two prefixes at one location usually means
    two different volumes backed up side by side, and restoring the wrong one is
    worse than stopping to ask.

    Inference applies ONLY when no prefix was asked for. An operator who passed
    --prefix named the set they want, and quietly listing a different one hands
    back another volume's snapshots while reporting success -- the exact harm the
    ambiguity rule above exists to prevent, arrived at from the other direction.
    A prefix that was given and matches nothing is a mismatch to report, not a
    guess to make.
    """
    config = getattr(backup_endpoint, "config", None)
    configured = (config or {}).get("snap_prefix", "") or ""
    if configured:
        logger.debug("Not inferring a prefix: %r was asked for explicitly", configured)
        return []
    if (config or {}).get("snap_prefix_explicit"):
        # An EXPLICIT empty prefix -- `--prefix ""`, or snapshot_prefix = "" on
        # the volume -- means bare-timestamp names, and is exactly as much of a
        # choice as any other prefix. Truthiness cannot tell it from "nobody
        # said", so this ran and replaced it: measured, a location holding
        # another source's `home.` snapshots had the operator's explicit "no
        # prefix" silently rewritten to `home.` and those snapshots listed for
        # restore. That is the harm this function's own docstring describes --
        # "a prefix that was given and matches nothing is a mismatch to report,
        # not a guess to make" -- reached through the one value that is falsy.
        logger.debug("Not inferring a prefix: an empty prefix was asked for")
        return []

    discover = getattr(backup_endpoint, "prefixes_present", None)
    if not callable(discover):
        return []
    try:
        present = discover() or {}
    except Exception as e:  # noqa: BLE001 - a convenience retry must not mask the real error
        logger.debug("Could not infer a snapshot prefix: %s", e)
        return []

    # An endpoint that does not implement this (or a stand-in that returns
    # something else) must degrade to "cannot infer", not crash the restore.
    if not isinstance(present, dict) or not present:
        return []
    if len(present) > 1:
        logger.error(
            "This location holds snapshots under more than one prefix (%s), so "
            "which set to restore is ambiguous. Re-run with --prefix to choose.",
            ", ".join(f"{p!r} ({n})" for p, n in sorted(present.items())),
        )
        return []

    prefix = next(iter(present))
    logger.info(
        "No snapshots matched the configured prefix; this location uses %r, so "
        "listing again with it. Pass --prefix to select a different set.",
        prefix,
    )
    previous = backup_endpoint.config.get("snap_prefix", "")
    backup_endpoint.config["snap_prefix"] = prefix
    try:
        # flush_cache: the base endpoint memoises its listing, and the caller has
        # already listed once under the old prefix. Without this the retry gets
        # that cached (empty) result back and the whole inference is a no-op.
        found = backup_endpoint.list_snapshots(flush_cache=True)
    except TypeError:
        # An endpoint or stand-in whose list_snapshots takes no flush argument.
        found = backup_endpoint.list_snapshots()
    except Exception as e:  # noqa: BLE001 - restore the prefix, then report normally
        backup_endpoint.config["snap_prefix"] = previous
        logger.debug("Listing under inferred prefix %r failed: %s", prefix, e)
        return []
    if not found:
        backup_endpoint.config["snap_prefix"] = previous
    return found or []


def _select_targets(
    backup_snapshots: list,
    snapshot_name: str | None,
    before_time: time.struct_time | None,
    restore_all: bool,
) -> list:
    """The snapshots the operator asked for: all, one by name, one by time,
    or the latest. Restore-only by nature -- a restore names a location
    without a config, so the selection is the command line's."""
    if restore_all:
        logger.info("Restoring all %d snapshots", len(backup_snapshots))
        return list(backup_snapshots)
    if snapshot_name:
        target = find_snapshot_by_name(snapshot_name, backup_snapshots)
        if target is None:
            raise RestoreError(
                f"Snapshot '{snapshot_name}' not found at backup location. "
                f"Available: {[s.get_name() for s in backup_snapshots[:5]]}..."
            )
        logger.info("Restoring specific snapshot: %s", snapshot_name)
        return [target]
    if before_time:
        target = find_snapshot_before_time(before_time, backup_snapshots)
        if target is None:
            raise RestoreError(
                "No snapshot found before the specified time. "
                f"Oldest available: {backup_snapshots[0].get_name() if backup_snapshots else 'none'}"
            )
        logger.info("Restoring snapshot before time: %s", target.get_name())
        return [target]
    target = backup_snapshots[-1]  # Snapshots are sorted, last is newest
    logger.info("Restoring latest snapshot: %s", target.get_name())
    return [target]


def _plan_line(index: int, total: int, snapshot: Any, parent: Any) -> str:
    """One line of the plan, printed identically by the preview and the run:
    the two are the same object, so the words cannot differ either."""
    mode = f"incremental from {parent.get_name()}" if parent else "full"
    return f"  [{index}/{total}] {snapshot.get_name()} ({mode})"


def restore_snapshots(
    backup_endpoint,
    local_endpoint,
    snapshot_name: str | None = None,
    before_time: time.struct_time | None = None,
    restore_all: bool = False,
    no_incremental: bool = False,
    options: dict | None = None,
    dry_run: bool = False,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> dict:
    """Restore snapshots from a backup location: select, plan, execute.

    The main entry point for a native restore. The backup location is the
    transfer's SOURCE and the local endpoint, through the plain layout, its
    DESTINATION; ``plan_transfer_sequence`` decides what is already there (by
    correspondence, never by name), what to send and against which parent,
    and expands a selection to the chain the source says it depends on; the
    executor moves the bytes under ``restore:<session>`` pins that a failure
    releases, records the artifact verdict on every copy, and cleans a
    partial it made. The preview and the run print the one plan.

    Args:
        backup_endpoint: Endpoint where backups are stored
        local_endpoint: Local endpoint to restore to
        snapshot_name: Specific snapshot to restore (None = latest)
        before_time: Restore snapshot closest to this time
        restore_all: Restore all snapshots
        no_incremental: Force full transfers (no ``-p``); the chain a
            selection depends on is still brought
        options: Transfer options dict
        dry_run: Print the plan the run would execute, and refuse what the
            run would refuse, without transferring
        on_progress: Callback for progress updates (current, total, name)

    Returns:
        Dict with restore statistics:
        {
            'restored': int,
            'skipped': int,
            'failed': int,
            'errors': list[str],
        }

    Raises:
        RestoreError: nothing to select from, a selection that is not at the
            source, a selection whose required parent is nowhere, or a
            same-name entry at the destination that is not this backup's copy.
            Every one of these is raised before a byte moves.
    """
    if options is None:
        options = {}

    session_id = str(uuid.uuid4())[:8]
    stats: dict[str, Any] = {"restored": 0, "skipped": 0, "failed": 0, "errors": []}

    # List snapshots at backup location
    logger.info("Listing snapshots at backup location...")
    backup_snapshots = backup_endpoint.list_snapshots()

    if not backup_snapshots:
        # Before giving up: a listing filters on `snap_prefix` and then requires
        # the rest of each name to parse as a timestamp, so the wrong prefix --
        # or none, which is the default -- discards every real snapshot and the
        # location reports as empty. The endpoint can already say which prefixes
        # ARE there; it said so in a message telling the operator to re-run with
        # --prefix. Doing that for them is the whole fix, and when it is
        # ambiguous the answer is an error naming the choices, never a guess.
        backup_snapshots = _retry_with_inferred_prefix(backup_endpoint)

    if not backup_snapshots:
        # A restore that restored nothing is a FAILED restore. This returned
        # stats with failed=0, which the CLI turns into exit status 0, so
        # `btrfs-backup-ng restore ... && echo ok` printed ok having recovered
        # nothing at all -- measured against a real remote, where a prefix
        # mismatch produced exactly this. Listing legitimately finding an empty
        # location is still a failure HERE: the caller asked for data back.
        detail = ""
        describe = getattr(backup_endpoint, "describe_empty_listing", None)
        if callable(describe):
            try:
                detail = describe() or ""
            except Exception:  # noqa: BLE001 - a diagnostic must not mask the error
                detail = ""
        message = "No snapshots found at the backup location; nothing was restored"
        logger.error(message)
        if detail:
            for line in str(detail).splitlines():
                logger.error("%s", line)
        stats["failed"] = 1
        stats["errors"].append(message if not detail else f"{message}. {detail}")
        return stats

    logger.info("Found %d snapshot(s) at backup location", len(backup_snapshots))

    # Read the destination under the SAME prefix the source ended up being read
    # under. _prepare_local_endpoint's docstring already required this -- "the
    # local endpoint must parse already-restored subvolumes under the SAME
    # prefix, or it fails to recognize them" -- and it held while the prefix
    # came from --prefix. It stopped holding once the prefix could be INFERRED:
    # inference updates the source endpoint, the destination was built earlier
    # from an empty --prefix, and nothing carried the answer across. The
    # destination then listed as empty, so a copy sitting right there was
    # invisible to correspondence and got re-sent onto its own name.
    #
    # Copied UNCONDITIONALLY rather than inside the inference branch above.
    # `restore --interactive` lists (and therefore infers) before it ever calls
    # this function, so by the time we get here the source listing succeeds on
    # the first attempt and that branch never runs -- while the prefix it left
    # behind is exactly the one the destination needs.
    #
    # Only a real string is copied. Callers in tests hand in doubles whose
    # config.get returns a mock, and a mock reaching str.startswith raises
    # inside the listing rather than here, where the cause would be obvious.
    _source_prefix = backup_endpoint.config.get("snap_prefix", "")
    if isinstance(_source_prefix, str):
        logger.debug(
            "Reading the destination under the source's prefix %r", _source_prefix
        )
        local_endpoint.config["snap_prefix"] = _source_prefix

    # One fresh listing of the destination, because the prefix may have just
    # changed: a listing memoised under the old one would return the stale
    # empty set, which is the very failure being fixed and produces no error
    # of its own. The planner's correspondence reads this cache.
    local_endpoint.list_snapshots(flush_cache=True)

    targets = _select_targets(backup_snapshots, snapshot_name, before_time, restore_all)

    layout = PlainLayout(local_endpoint, session_id)

    # Plan. Presence is correspondence -- received_uuid against the identity
    # the backup's stream carries -- so a partial receive, a foreign subvolume
    # and a re-created snapshot are all "absent"; the parent is chosen from
    # what the destination holds; the selection grows to the chain the source
    # says it requires, and a requirement the source cannot meet is a refusal.
    try:
        plan = plan_transfer_sequence(
            backup_snapshots,
            layout.destination_view,
            no_incremental=no_incremental,
            only=targets,
            source_endpoint=backup_endpoint,
        )
    except PlanningError as e:
        raise RestoreError(str(e)) from e

    planned_names = {s.get_name() for s, _ in plan}
    already_present = [
        t.get_name() for t in targets if t.get_name() not in planned_names
    ]
    stats["skipped"] = len(already_present)

    if not plan:
        # Say WHY there is nothing to do. Asking for a snapshot that is
        # already at the destination is a satisfied request, and a restore
        # script that re-runs after success must keep succeeding -- but it
        # has to be said, and by name.
        logger.info(
            "Already at the destination, so nothing to do: %s",
            ", ".join(already_present),
        )
        return stats

    # Refuse, before a byte moves, anything the receive would collide with.
    # An entry under a planned name that is not this backup's copy (a partial
    # from an interrupted restore, another tool's subvolume, a copy of some
    # other snapshot) used to be skipped by name and reported as restored.
    # It is named, described, and left exactly as it is.
    collisions = layout.collisions(plan)
    if collisions:
        lines = "\n".join(f"  {c}" for c in collisions)
        raise RestoreError(
            f"Refusing to restore: the destination already holds an entry under "
            f"a name this restore would receive, and it is not this backup's "
            f"copy:\n{lines}\nNothing was transferred. Inspect or remove it, or "
            f"restore to a different path."
        )

    logger.info("")
    logger.info("Restore plan:")
    logger.info("  Target(s): %s", ", ".join(t.get_name() for t in targets))
    if already_present:
        logger.info("  Already at the destination: %s", ", ".join(already_present))
    for i, (snap, parent) in enumerate(plan, 1):
        logger.info("%s", _plan_line(i, len(plan), snap, parent))
    logger.info("  Total: %d snapshot(s) to restore", len(plan))
    logger.info("")

    if dry_run:
        logger.info("Dry run - no changes made")
        return stats

    source_path = str(backup_endpoint.config.get("path", ""))
    dest_path = str(local_endpoint.config.get("path", ""))
    run_start = time.monotonic()
    log_transaction(
        action="restore",
        status="started",
        source=source_path,
        destination=dest_path,
        snapshot=", ".join(s.get_name() for s, _ in plan),
    )

    layout.begin(plan, on_progress)
    result = _execute_transfers(
        backup_endpoint,
        layout.receive_endpoint,
        plan,
        options,
        lock_id=f"restore:{session_id}",
        release_on_failure=True,
    )
    layout.finish(result)

    duration = time.monotonic() - run_start
    parents = {s.get_name(): p for s, p in plan}
    for snap in result.transferred:
        parent = parents.get(snap.get_name())
        log_transaction(
            action="restore",
            status="completed",
            source=source_path,
            destination=dest_path,
            snapshot=snap.get_name(),
            parent=parent.get_name() if parent else None,
            duration_seconds=duration,
        )
    for snap, error in result.failed:
        parent = parents.get(snap.get_name())
        log_transaction(
            action="restore",
            status="failed",
            source=source_path,
            destination=dest_path,
            snapshot=snap.get_name(),
            parent=parent.get_name() if parent else None,
            duration_seconds=duration,
            error=str(error),
        )
        stats["errors"].append(f"{snap.get_name()}: {error}")

    stats["restored"] = result.transferred_count
    stats["failed"] = result.failed_count

    # Summary
    logger.info("")
    logger.info("Restore complete:")
    logger.info("  Restored: %d", stats["restored"])
    logger.info("  Skipped: %d", stats["skipped"])
    logger.info("  Failed: %d", stats["failed"])

    if stats["errors"]:
        logger.warning("Errors:")
        for err in stats["errors"]:
            logger.warning("  %s", err)

    return stats


def list_remote_snapshots(
    backup_endpoint,
    prefix_filter: str | None = None,
) -> list:
    """List snapshots available at a backup location.

    Args:
        backup_endpoint: Endpoint where backups are stored
        prefix_filter: Optional prefix to filter snapshots

    Returns:
        List of Snapshot objects
    """
    snapshots = backup_endpoint.list_snapshots()

    if prefix_filter:
        snapshots = [s for s in snapshots if s.get_name().startswith(prefix_filter)]

    return snapshots


# =============================================================================
# Snapper-specific Restore Operations
# =============================================================================


def list_snapper_backups(
    backup_path: str,
    endpoint_options: dict | None = None,
) -> list[dict]:
    """List snapper backups at a backup location.

    Looks for backups in the snapper directory structure:
        {backup_path}/.snapshots/{num}/snapshot
        {backup_path}/.snapshots/{num}/info.xml

    Args:
        backup_path: Path to backup location

    Returns:
        List of dicts with backup info:
        [
            {
                'number': 558,
                'snapshot_path': Path to snapshot subvolume,
                'info_xml_path': Path to info.xml,
                'metadata': SnapperMetadata or None,
            },
            ...
        ]
    """
    # raw:// and raw+ssh:// snapper backups are flat btrfs-send streams plus a
    # {name}.snapper-meta.json sidecar -- there is no .snapshots/{num}/info.xml layout
    # to scan. Dispatch those to a sidecar-based enumeration so they can be listed AND
    # restored, instead of silently returning [] as the .snapshots scan did.
    if str(backup_path).startswith(("raw://", "raw+ssh://")):
        return _list_raw_snapper_backups(backup_path, endpoint_options)

    # A btrfs target reached over ssh has the SAME .snapshots/{num} layout as a
    # local one, but Path("ssh://host:/p") is a nonexistent LOCAL path: the scan
    # below stat'd it, missed, and returned [] without ever opening a connection.
    # `snapper restore --list ssh://...` therefore reported "No snapper backups
    # found" -- exit 0 -- for a destination holding backups, which is the README's
    # flagship disaster-recovery walkthrough.
    if str(backup_path).startswith("ssh://"):
        return _list_remote_snapper_backups(backup_path, endpoint_options)

    from ..snapper.metadata import parse_info_xml

    backup_base = Path(backup_path)
    snapshots_dir = backup_base / ".snapshots"
    backups: list[dict[str, Any]] = []

    if not snapshots_dir.exists():
        # Same rule as the ssh:// and raw:// branches: a location that cannot be
        # enumerated is an error, not an empty result. Both callers are
        # restore-side, where the source is something the operator has told us
        # holds backups -- a mistyped path must not come back as "no backups".
        raise RuntimeError(
            f"Cannot list snapper backups at {backup_path}: {snapshots_dir} does "
            "not exist. The location could NOT be enumerated -- this is NOT an "
            "empty target. Check the path is correct and that it holds a snapper "
            "backup layout (.snapshots/<number>/snapshot)."
        )

    for item in snapshots_dir.iterdir():
        if item.is_dir() and item.name.isdecimal():
            snapshot_path = item / "snapshot"
            info_xml_path = item / "info.xml"

            if snapshot_path.exists():
                backup_info = {
                    "number": int(item.name),
                    "snapshot_path": snapshot_path,
                    "info_xml_path": info_xml_path if info_xml_path.exists() else None,
                    # snapper's own xml, verbatim: a restore renumbers THIS, so
                    # every element snapper wrote survives, modelled or not.
                    "info_xml": None,
                }

                # Parse info.xml if available
                if info_xml_path.exists():
                    try:
                        backup_info["info_xml"] = info_xml_path.read_text()
                    except OSError as e:
                        logger.debug("Could not read info.xml for %s: %s", item.name, e)
                    try:
                        metadata = parse_info_xml(info_xml_path)
                        backup_info["metadata"] = metadata
                    except Exception as e:
                        logger.debug(
                            "Could not parse info.xml for %s: %s", item.name, e
                        )
                        backup_info["metadata"] = None
                else:
                    backup_info["metadata"] = None

                backups.append(backup_info)

    # Sort by number
    backups.sort(key=lambda b: int(str(b["number"])))
    return backups


def _load_snapper_sidecar(endpoint: Any, name: str) -> Any:
    """Load a ``{name}.snapper-meta.json`` sidecar into a BackupMetadata.

    Local ``raw://`` reads the file directly; ``raw+ssh://`` cat's it back over
    ssh (the argv is shlex-quoted inside ``_exec_remote_command``, so the name is
    injection-safe). Both funnel through ``BackupMetadata.from_dict`` so the file
    and stream paths cannot drift.
    """
    from ..snapper.metadata import BackupMetadata, load_backup_metadata

    filename = f"{name}.snapper-meta.json"
    dest_path = Path(endpoint.config["path"])

    if getattr(endpoint, "_is_remote", False):
        remote_path = str(dest_path / filename)
        result = endpoint._exec_remote_command(["cat", remote_path], check=True)
        raw = result.stdout
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        return BackupMetadata.from_dict(json.loads(raw))

    return load_backup_metadata(dest_path / filename)


def _restore_endpoint_config(backup_path: str, endpoint_options: dict | None) -> dict:
    """Build the common_config for a restore-side endpoint, whatever its scheme.

    ``endpoint_options`` carries the CLI's ssh options (ssh_sudo / ssh_key /
    ssh_auth_sock / ssh_host_key_policy) so a target WRITTEN with --ssh-sudo
    (root-owned remote directories and streams) is READ BACK with the same
    options -- otherwise the remote commands run unprivileged and the backups
    enumerate as empty. It also carries the decryption options (gpg_keyring /
    openssl_cipher) so an encrypted raw snapper backup can be decoded on restore.

    Named for the direction rather than for raw, because ssh:// btrfs targets
    now build their endpoint through here too and a "raw" name would suggest an
    ssh:// restore goes through raw code, which it does not.
    """
    # A restore reads its source. The bookkeeping tree a destination gets
    # (``.btrfs-backup-ng/``) is not created here: a location written by
    # another tool or mounted read-only -- the disaster-recovery medium --
    # must be restorable from as it is, and a dry run must create nothing.
    config: dict[str, Any] = {
        "path": backup_path,
        "snap_prefix": "",
        "create_tree": False,
    }
    if endpoint_options:
        config.update(endpoint_options)
    return config


def _list_remote_snapper_backups(
    backup_path: str, endpoint_options: dict | None = None
) -> list[dict]:
    """Enumerate snapper backups at an ``ssh://`` btrfs target.

    Same ``.snapshots/{num}/snapshot`` + ``info.xml`` layout as a local btrfs
    target -- only the filesystem is remote, so the scan runs over the endpoint
    instead of over ``Path``. Returns the dict shape the local path returns.

    ``endpoint_options`` carries the CLI's ssh options for the same reason the raw
    sibling takes them: a destination written with ``--ssh-sudo`` is root-owned,
    and reading it back without them enumerates as empty.

    A listing that FAILS raises. "We could not look" must never be reported as
    "there is nothing there" -- during a restore that is the most dangerous
    moment to be wrong.
    """
    from ..endpoint import choose_endpoint
    from ..snapper.metadata import parse_info_xml_string

    endpoint = choose_endpoint(
        backup_path, _restore_endpoint_config(backup_path, endpoint_options)
    )
    base = f"{str(endpoint.config['path']).rstrip('/')}/.snapshots"

    # -maxdepth/-mindepth 1 -type d keeps this to the numbered slot dirs and does
    # not descend into the received subvolumes. stderr is NOT discarded: it is the
    # only explanation of a failure, and discarding it is what made the raw
    # equivalent of this bug invisible.
    # stdout/stderr must be requested explicitly: SSHEndpoint._exec_remote_command
    # passes kwargs straight to subprocess and captures nothing by default (unlike
    # SSHRawEndpoint). Without this the find output goes to the console and the
    # scan sees an empty result -- an empty listing produced by not looking.
    result = endpoint._exec_remote_command(
        ["find", base, "-mindepth", "1", "-maxdepth", "1", "-type", "d"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout = result.stdout or b""
    stderr = result.stderr or b""
    if isinstance(stdout, bytes):
        stdout = stdout.decode(errors="replace")
    if isinstance(stderr, bytes):
        stderr = stderr.decode(errors="replace")

    if result.returncode != 0:
        # find exits 0 whenever it finished looking, including over an empty or
        # absent-but-readable tree, so non-zero means the scan did not complete.
        # No special case for "No such file or directory". The raw path raises for
        # exactly this condition, and a mistyped path produces it just as readily
        # as a never-written destination -- telling someone "no backups" because
        # the location does not exist is the same lie in a different costume. The
        # two schemes must answer identically; see _check_remote_listing.
        raise RuntimeError(
            f"Cannot list snapper backups at {backup_path}: "
            f"{stderr.strip() or f'find exited {result.returncode}'}. The location "
            "could NOT be enumerated -- this is NOT an empty target. Check the path "
            "is correct and readable by the CONNECTING USER: on an ssh:// target "
            "--ssh-sudo elevates only btrfs, so it does not help here (measured: "
            "SSHEndpoint._build_remote_command passes find/test/cat through "
            "unchanged). Grant the user access instead, e.g. "
            "setfacl -m u:<user>:rx on the .snapshots directory."
        )

    backups: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        slot = line.strip()
        if not slot:
            continue
        name = slot.rsplit("/", 1)[-1]
        if not name.isdecimal():
            # .incoming / .stale are this run's transactional temps, never backups.
            continue

        snapshot_path = f"{slot}/snapshot"
        info_xml_path = f"{slot}/info.xml"

        # The slot only counts if the received subvolume is actually there; a
        # publish that never completed must not present as a restorable backup.
        # What lists here is what a restore is offered: the restore sends the
        # slots this listing names, and nothing it did not.
        if not _remote_dir_exists(endpoint, snapshot_path):
            logger.debug("Skipping snapper slot %s: no published snapshot", slot)
            continue

        metadata = None
        raw_xml = _read_remote_text(endpoint, info_xml_path)
        if raw_xml is not None:
            try:
                metadata = parse_info_xml_string(raw_xml)
            except Exception as e:
                logger.debug("Could not parse remote info.xml for %s: %s", name, e)
        else:
            # Absent info.xml is a known outcome of an older backup, not an error.
            logger.debug("No info.xml in remote snapper slot %s", slot)

        backups.append(
            {
                "number": int(name),
                "snapshot_path": snapshot_path,
                "info_xml_path": info_xml_path if raw_xml is not None else None,
                "info_xml": raw_xml,
                "metadata": metadata,
            }
        )

    backups.sort(key=lambda b: b["number"])
    return backups


def snapper_layout_present(endpoint: Any) -> bool:
    """Whether the endpoint's location is laid out as snapper backups: a
    ``.snapshots`` directory on a btrfs location (local or ssh://), or
    ``.snapper-meta.json`` sidecars on a raw one.

    Read-only. Answers the question the enumeration deliberately does not --
    ``list_snapper_backups`` raises for an absent layout, because a restore
    must never mistake a mistyped path for an empty one -- so a caller that
    needs "is there anything here at all" (the status command, the backup
    direction's first transfer to a fresh target) asks this first. A probe
    that cannot be made says no; the enumeration that follows a yes raises
    its own error when it cannot look.
    """
    from ..endpoint.raw import RawEndpoint
    from .operations import _list_snapper_backups_at_destination

    base = str(endpoint.config["path"]).rstrip("/")
    try:
        if isinstance(endpoint, RawEndpoint):
            return bool(_list_snapper_backups_at_destination(endpoint))
        if getattr(endpoint, "_is_remote", False):
            return _remote_dir_exists(endpoint, f"{base}/.snapshots")
        return Path(base, ".snapshots").is_dir()
    except Exception as e:  # noqa: BLE001 - a failed probe is "not snapper", said
        logger.debug("Could not probe %s for a snapper layout: %s", base, e)
        return False


def _remote_dir_exists(endpoint: Any, path: str) -> bool:
    """True when ``path`` is a directory on the endpoint's remote host.

    ``test -d`` is the enumeration's probe for a slot's received subvolume, so
    a slot that lists as restorable is one whose subvolume was there to be
    seen by the connecting user.

    Non-zero means "absent OR not reachable by the connecting user" -- ``test``
    cannot separate those, and callers must not phrase it as if it could.

    stdout/stderr are requested explicitly because
    ``SSHEndpoint._exec_remote_command`` captures nothing by default (unlike the
    raw endpoint), and an uncaptured probe writes to the console.
    """
    probe = endpoint._exec_remote_command(
        ["test", "-d", path],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return bool(probe.returncode == 0)


def _read_remote_text(endpoint: Any, path: str) -> str | None:
    """Return the contents of a remote file, or None when it could not be read.

    Absent and unreadable collapse to None on purpose: every caller treats a
    missing info.xml as a known outcome of an older backup rather than an error,
    and neither can be distinguished from ``cat``'s exit status alone.
    """
    result = endpoint._exec_remote_command(
        ["cat", path],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        return None
    data = result.stdout or b""
    if isinstance(data, bytes):
        return data.decode(errors="replace")
    return str(data)


def _assert_raw_location_exists(endpoint: Any, backup_path: str) -> None:
    """Raise unless the raw backup location is actually present and readable.

    Restore-side only. See the call site for why this is not folded into
    _list_snapper_backups_at_destination, which the backup side needs to tolerate.
    """
    dest_path = str(endpoint.config["path"])
    if getattr(endpoint, "_is_remote", False):
        probe = endpoint._exec_remote_command(
            ["test", "-d", dest_path],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        missing = probe.returncode != 0
    else:
        missing = not Path(dest_path).is_dir()
    if missing:
        raise RuntimeError(
            f"Cannot list snapper backups at {backup_path}: {dest_path} is not a "
            "readable directory. The location could NOT be enumerated -- this is "
            "NOT an empty target. Check the path is correct and readable by the "
            "connecting user. Unlike ssh://, a raw+ssh target written with "
            "--ssh-sudo IS read back with it -- SSHRawEndpoint elevates every "
            "remote command, not just btrfs -- so pass the same option here."
        )


def _list_raw_snapper_backups(
    backup_path: str, endpoint_options: dict | None = None
) -> list[dict]:
    """Enumerate snapper backups at a ``raw://`` / ``raw+ssh://`` target.

    Raw snapper backups have no ``.snapshots/{num}/info.xml`` layout, so the
    snapper number and metadata are recovered from each ``{name}.snapper-meta.json``
    sidecar (reusing the proven ``_list_snapper_backups_at_destination`` name scan,
    local and remote). Returns the same dict shape the btrfs path returns
    (``number``/``metadata``/``snapshot_path``/``info_xml_path``) plus ``raw`` and
    ``backup_name`` so materialization can resolve the stream by name.
    """
    from ..endpoint import choose_endpoint

    endpoint = choose_endpoint(
        backup_path, _restore_endpoint_config(backup_path, endpoint_options)
    )

    # _list_snapper_backups_at_destination is shared with the BACKUP side, where a
    # missing destination legitimately means "first run, nothing here yet" and is
    # tolerated. On the RESTORE side the same absence means the operator gave us a
    # path that does not hold backups, and answering "no backups" is the lie this
    # series exists to eliminate. The shared primitive keeps its semantics; the
    # distinction belongs here, where the caller's intent is known -- matching the
    # local and ssh:// branches, which raise for the same condition.
    _assert_raw_location_exists(endpoint, backup_path)

    backups: list[dict[str, Any]] = []
    for name in _list_snapper_backups_at_destination(endpoint):
        try:
            backup_meta = _load_snapper_sidecar(endpoint, name)
            metadata = backup_meta.to_snapper_metadata()
        except Exception as e:
            logger.warning(
                "Skipping raw snapper backup %r (unreadable sidecar): %s", name, e
            )
            continue

        backups.append(
            {
                "number": backup_meta.snapper_number,
                "snapshot_path": None,
                "info_xml_path": None,
                # The sidecar stores snapper's own xml; a restore renumbers it.
                "info_xml": backup_meta.original_info_xml or None,
                "metadata": metadata,
                "raw": True,
                "backup_name": name,
            }
        )

    # Sort by number, then by backup_name (which embeds the date) so that duplicate
    # snapper numbers -- which snapper reuses after a prune -- have a STABLE, visible
    # order rather than the arbitrary order of the underlying set.
    backups.sort(key=lambda b: (int(str(b["number"])), str(b["backup_name"])))
    return backups


def _backup_key(backup: dict) -> Any:
    """What tells two listed backups apart: the unique name a raw store gives
    each stream, or the slot number on a btrfs location (unique there)."""
    return backup.get("backup_name") or ("num", backup["number"])


def _snapper_label(backup: dict) -> str:
    """How a backup is named to the operator: its snapper number, and for a
    raw store the unique name that tells reused numbers apart."""
    label = f"snapshot {backup['number']}"
    name = backup.get("backup_name")
    return f"{label} ({name})" if name else label


def _restored_info_xml(backup: dict, new_num: int) -> bytes:
    """The ``info.xml`` a restored slot gets: snapper's own, renumbered.

    One implementation for every source. A local btrfs location has the file,
    an ssh:// location was read over the connection, a raw store keeps it in
    the sidecar; each lister puts the text under ``info_xml``. Only ``<num>``
    changes, so userdata (every block), ``<uid>`` and any element this project
    does not model survive verbatim. Regenerating from the parsed fields is
    the fallback when the text is absent or does not parse, and a slot with
    no metadata at all gets a minimal record that says where it came from.
    """
    from ..snapper.metadata import (
        SnapperMetadata,
        generate_info_xml,
        renumber_info_xml,
    )

    original = backup.get("info_xml")
    if original:
        try:
            return renumber_info_xml(original, new_num).encode("utf-8")
        except ValueError as e:
            logger.warning(
                "Could not renumber the info.xml of %s (%s); regenerating it "
                "from its parsed fields",
                _snapper_label(backup),
                e,
            )
    metadata = backup.get("metadata")
    if metadata is not None:
        renumbered = dataclasses.replace(metadata, num=new_num)
        return generate_info_xml(renumbered).encode("utf-8")
    from datetime import datetime

    minimal = SnapperMetadata(
        type="single",
        num=new_num,
        date=datetime.now(),
        description=f"Restored from backup {backup['number']}",
        cleanup="",
    )
    return generate_info_xml(minimal).encode("utf-8")


def _snapper_source(
    backup_path: str, backups: list[dict], endpoint_options: dict | None
) -> tuple[Any, list[tuple[dict, Any]]]:
    """The backup location as a transfer SOURCE: its endpoint, prepared, and
    one sendable snapshot object per listed backup.

    Prepared like the native restore's source: for an ssh:// location that
    starts the master connection and runs the diagnostics that record whether
    the remote's sudo is passwordless, which is what lets ``--ssh-sudo`` on a
    NOPASSWD-btrfs host run unattended (``sudo -n`` instead of a prompt).

    A raw store's snapshots are its own listing (``RawSnapshot``: the stored
    stream, the identity its sidecar records, the parent it applies onto),
    matched to the backups by the unique name each carries. A btrfs
    location's slots are read the way the backup direction reads a target --
    one shell pass over ``.snapshots/<n>/snapshot`` for each slot's
    received_uuid -- and each selected backup becomes a ``_SnapperBtrfsBackup``
    at its path, dated from its info.xml. A slot whose identity could not be
    read is still restorable: it plans as absent (a full send) and its copy's
    verdict is unverifiable, and that is said.
    """
    from ..endpoint import choose_endpoint
    from ..endpoint.raw import RawEndpoint

    endpoint = choose_endpoint(
        backup_path, _restore_endpoint_config(backup_path, endpoint_options)
    )
    endpoint.prepare()

    pairs: list[tuple[dict, Any]] = []
    if isinstance(endpoint, RawEndpoint):
        streams = {s.get_name(): s for s in endpoint.list_snapshots()}
        for backup in backups:
            name = backup.get("backup_name")
            stream = streams.get(str(name))
            if stream is None:
                raise RestoreError(
                    f"Raw stream for snapper backup {name!r} not found at "
                    f"{backup_path} (its .snapper-meta.json sidecar exists but "
                    f"the btrfs-send stream is missing)"
                )
            pairs.append((backup, stream))
        return endpoint, pairs

    identities = {
        slot.number: slot.received_uuid
        for slot in _ops._enumerate_snapper_btrfs_backups(endpoint)
    }
    base = str(endpoint.config["path"])
    for backup in sorted(backups, key=lambda b: int(b["number"])):
        number = int(backup["number"])
        metadata = backup.get("metadata")
        date = getattr(metadata, "date", None)
        time_obj = date.timetuple() if date is not None else None
        received_uuid = identities.get(number, "")
        if not received_uuid:
            logger.info(
                "The identity of %s at %s could not be read, so it is restored "
                "in full and its copy cannot be confirmed against the backup.",
                _snapper_label(backup),
                backup_path,
            )
        pairs.append(
            (
                backup,
                _SnapperBtrfsBackup(
                    number,
                    received_uuid,
                    base=base,
                    endpoint=endpoint,
                    time_obj=time_obj,
                ),
            )
        )
    return endpoint, pairs


def _snapper_mode(snapshot: Any, parent: Any) -> str:
    """How a planned snapshot is sent, for the plan line and the report line.

    A btrfs slot is sent in full or as an increment from the parent the
    planner chose among what the destination holds. A stored raw stream is
    replayed as it was written, so the words say what the stream IS rather
    than what a send would do.
    """
    if getattr(snapshot, "stream_path", None) is not None:
        parent_name = getattr(snapshot, "parent_name", None)
        return (
            f"stored increment of {parent_name}"
            if parent_name
            else "stored full stream"
        )
    if parent is not None:
        return f"incremental from {parent.number}"
    return "full"


def _snapper_plan_line(
    index: int, total: int, backup: dict, snapshot: Any, parent: Any
) -> str:
    """One line of the plan, printed identically by the preview and the run."""
    return f"  [{index}/{total}] {_snapper_label(backup)} ({_snapper_mode(snapshot, parent)})"


def restore_snapper_snapshots(
    backup_path: str,
    backups: list[dict],
    selected: list[dict],
    snapper_config_name: str,
    options: dict | None = None,
    dry_run: bool = False,
    endpoint_options: dict | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> dict:
    """Restore snapper backups into a local snapper config: plan, then execute.

    A snapper restore is a transfer through the engine with the roles
    swapped, into the snapper layout. The backup location is the SOURCE
    (``_snapper_source``: prepared, its slots or streams as snapshots the
    engine can send); the local config is the DESTINATION through
    ``SnapperLayout``, whose view of the config's slots is what the planner
    reads for correspondence and whose receive endpoint lands each copy in
    the next free ``.snapshots/<n>`` slot.

    What that gives, and what stays as it was:

    - Every selected backup lands in a NEW slot, as snapper keeps every
      snapshot; nothing is skipped as "already restored". The selection is
      planned exactly as given -- it is never expanded to a chain.
    - The incremental parent is chosen from what the CONFIG holds, by
      correspondence (a slot's received_uuid against the identity the
      backup's stream carries), never by snapper number: a backup whose
      parent is on the media is an increment, one whose parent is not is a
      full send. That is the disaster-recovery case: ``--snapshot N`` onto
      media holding nothing is one full send into slot 1.
    - A stored raw increment applies only onto its parent; when the parent
      is neither in the config nor selected, the plan is refused before a
      byte moves and no slot is created.
    - The backup is pinned on its location for the duration under
      ``restore:<session>``, released when a transfer fails; every copy is
      judged in its slot before the slot is published; a failed receive
      leaves no numbered slot and no ``.incoming``.
    - ``info.xml`` is snapper's own, renumbered, from one implementation.
    - ``dry_run`` prints the plan the run would execute and stops.

    ``backups`` is everything the location lists (the parent candidates);
    ``selected`` is the subset the operator asked for, already reduced by
    the command line's collision rule.

    Returns ``{"restored", "failed", "errors", "slots"}`` where ``slots`` is
    ``[(backup number, slot number)]`` for every copy published, in order.

    Raises ``RestoreError`` before anything moves: an unknown config, a raw
    stream that is gone, a plan that cannot be honoured.
    """
    from ..endpoint.local import LocalEndpoint
    from ..snapper import SnapperScanner
    from .layout import SnapperLayout

    options = dict(options or {})
    endpoint_options = dict(endpoint_options or {})
    session_id = str(uuid.uuid4())[:8]
    stats: dict[str, Any] = {"restored": 0, "failed": 0, "errors": [], "slots": []}

    scanner = SnapperScanner()
    local_config = scanner.get_config(snapper_config_name)
    if local_config is None:
        raise RestoreError(f"Local snapper config not found: {snapper_config_name}")
    subvolume = Path(local_config.subvolume)
    if not subvolume.is_dir():
        raise RestoreError(
            f"The subvolume of snapper config {snapper_config_name!r}, "
            f"{subvolume}, is not there. It is most likely on a filesystem that "
            f"is not mounted; nothing was restored."
        )
    if not Path(local_config.snapshots_dir).is_dir():
        raise RestoreError(
            f"Snapper config {snapper_config_name!r} has no {local_config.snapshots_dir}"
            f" directory; snapper itself cannot use the config without it. Nothing "
            f"was restored."
        )
    if not selected:
        raise RestoreError("Nothing was selected to restore")

    local_endpoint = LocalEndpoint(
        config={
            "path": str(subvolume),
            "snap_prefix": "",
            "fs_checks": "skip",
            "btrfs_debug": bool(endpoint_options.get("btrfs_debug", False)),
        }
    )
    layout = SnapperLayout(
        local_endpoint,
        next_number=lambda: scanner.get_next_snapshot_number(local_config),
    )

    # One writer at a time into a config, for the whole run. Two restores
    # would pick the same next free number and the second would remove the
    # first's in-flight temp as a crashed run's leftover; refused with words
    # instead. The kernel drops the lock when the holder dies. A dry run
    # takes nothing: it creates no lock file and sweeps no temp.
    held: Any = contextlib.nullcontext()
    if not dry_run:
        try:
            held = layout.writer_lock(
                f"Restoring into snapper config {snapper_config_name!r}"
            )
            held.__enter__()
        except RuntimeError as e:
            raise RestoreError(f"{e}. Nothing was restored.") from e
    try:
        return _restore_snapper_snapshots_locked(
            layout,
            scanner,
            local_config,
            backup_path,
            backups,
            selected,
            snapper_config_name,
            options,
            dry_run,
            endpoint_options,
            on_progress,
            session_id,
            stats,
        )
    finally:
        held.__exit__(None, None, None)


def _restore_snapper_snapshots_locked(
    layout: Any,
    scanner: Any,
    local_config: Any,
    backup_path: str,
    backups: list[dict],
    selected: list[dict],
    snapper_config_name: str,
    options: dict,
    dry_run: bool,
    endpoint_options: dict,
    on_progress: Callable[[int, int, str], None] | None,
    session_id: str,
    stats: dict[str, Any],
) -> dict:
    """The body of ``restore_snapper_snapshots``, run with the config's
    restore lock held: nothing else is restoring into this config, so the
    temps of restores that died can be swept, and the numbers this run reads
    are contended only by snapper itself (which ``publish_fresh`` handles)."""
    from ..endpoint.raw import RawEndpoint

    if not dry_run:
        layout.sweep_stale_temps()

    source_endpoint, pairs = _snapper_source(backup_path, backups, endpoint_options)
    backup_of = {snapshot.get_name(): backup for backup, snapshot in pairs}
    snapshot_of = {_backup_key(backup): snapshot for backup, snapshot in pairs}
    source_snapshots = [snapshot for _, snapshot in pairs]
    targets = []
    for backup in selected:
        snapshot = snapshot_of.get(_backup_key(backup))
        if snapshot is None:
            raise RestoreError(
                f"{_snapper_label(backup)} is not among the backups listed at "
                f"{backup_path}"
            )
        targets.append(snapshot)

    # A raw store's answer to "what does this need" is a FACT about the
    # stored stream and is checked before a byte moves. A btrfs location's
    # answer is the time-ordered predecessor, a policy the native restore
    # follows and a snapper restore does not: the selection is what the
    # operator named, and a full send always works.
    requirements = source_endpoint if isinstance(source_endpoint, RawEndpoint) else None
    try:
        plan = plan_transfer_sequence(
            source_snapshots,
            layout.destination_view,
            only=targets,
            source_endpoint=requirements,
            expand_selection=False,
            skip_present=False,
        )
    except PlanningError as e:
        raise RestoreError(str(e)) from e

    next_free = scanner.get_next_snapshot_number(local_config)
    logger.info("")
    logger.info("Restore plan:")
    logger.info(
        "  Into snapper config %r (%s); the next free slot is %d, and each "
        "copy lands under the number that is free when it is published",
        snapper_config_name,
        local_config.snapshots_dir,
        next_free,
    )
    for i, (snapshot, parent) in enumerate(plan, 1):
        logger.info(
            "%s",
            _snapper_plan_line(
                i, len(plan), backup_of[snapshot.get_name()], snapshot, parent
            ),
        )
    logger.info("  Total: %d snapshot(s) to restore", len(plan))
    logger.info("")

    if dry_run:
        logger.info("Dry run - no changes made")
        return stats

    dest_path = str(local_config.snapshots_dir)
    parents = {s.get_name(): p for s, p in plan}
    run_start = time.monotonic()
    log_transaction(
        action="snapper_restore",
        status="started",
        source=backup_path,
        destination=dest_path,
        snapshot=", ".join(str(backup_of[s.get_name()]["number"]) for s, _ in plan),
    )

    def _info_xml_for(snapshot: Any, number: int) -> bytes:
        return _restored_info_xml(backup_of[snapshot.get_name()], number)

    def _published(snapshot: Any, number: int) -> None:
        # The report line for each slot, as it is published: the number
        # snapper will show for it, and how it was received.
        backup = backup_of[snapshot.get_name()]
        logger.info(
            "Restored %s as local snapshot %d (%s)",
            _snapper_label(backup),
            number,
            _snapper_mode(snapshot, parents.get(snapshot.get_name())),
        )

    layout.begin(
        plan,
        info_xml_for=_info_xml_for,
        on_published=_published,
        on_progress=on_progress,
    )
    try:
        result = _execute_transfers(
            source_endpoint,
            layout.receive_endpoint,
            plan,
            options,
            lock_id=f"restore:{session_id}",
            release_on_failure=True,
        )
    finally:
        layout.finish()

    duration = time.monotonic() - run_start
    slot_of = {snapshot.get_name(): number for snapshot, number in layout.published}
    for snapshot in result.transferred:
        backup = backup_of[snapshot.get_name()]
        parent = parents.get(snapshot.get_name())
        number = slot_of.get(snapshot.get_name())
        stats["slots"].append((backup["number"], number))
        log_transaction(
            action="snapper_restore",
            status="completed",
            source=backup_path,
            destination=f"{dest_path}/{number}/snapshot",
            snapshot=str(backup["number"]),
            parent=str(backup_of[parent.get_name()]["number"]) if parent else None,
            duration_seconds=duration,
        )
    for snapshot, error in result.failed:
        backup = backup_of[snapshot.get_name()]
        parent = parents.get(snapshot.get_name())
        log_transaction(
            action="snapper_restore",
            status="failed",
            source=backup_path,
            destination=dest_path,
            snapshot=str(backup["number"]),
            parent=str(backup_of[parent.get_name()]["number"]) if parent else None,
            duration_seconds=duration,
            error=str(error),
        )
        stats["errors"].append(f"{_snapper_label(backup)}: {error}")

    stats["restored"] = result.transferred_count
    stats["failed"] = result.failed_count
    if stats["errors"]:
        logger.warning("Errors:")
        for err in stats["errors"]:
            logger.warning("  %s", err)
    return stats
