"""Core restore operations: restore snapshots from backup locations.

A native restore is a transfer through the engine with the roles swapped: the
backup location is the SOURCE, a local btrfs filesystem is the DESTINATION,
and the planner and executor that move every backup move the restore too.
What is restore-only lives here: choosing which snapshot (by name, by time,
the latest, all of them), inferring the prefix a location uses, the stats a
run reports, and the layout the copies land in (``core.layout``).

The snapper restore path further down still has its own send/receive
pipes and planning; it is the next piece to converge.
"""

import json
import logging
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from .. import __util__
from ..transaction import log_transaction
from . import progress as progress_utils
from .layout import PlainLayout
from .operations import (
    _execute_transfers,
    _list_snapper_backups_at_destination,
)
from .planning import PlanningError, plan_transfer_sequence
from .target import TargetKind, parse_target

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
        if item.is_dir() and item.name.isdigit():
            snapshot_path = item / "snapshot"
            info_xml_path = item / "info.xml"

            if snapshot_path.exists():
                backup_info = {
                    "number": int(item.name),
                    "snapshot_path": snapshot_path,
                    "info_xml_path": info_xml_path if info_xml_path.exists() else None,
                }

                # Parse info.xml if available
                if info_xml_path.exists():
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
    config: dict[str, Any] = {"path": backup_path, "snap_prefix": ""}
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
        if not name.isdigit():
            # .incoming / .stale are this run's transactional temps, never backups.
            continue

        snapshot_path = f"{slot}/snapshot"
        info_xml_path = f"{slot}/info.xml"

        # The slot only counts if the received subvolume is actually there; a
        # publish that never completed must not present as a restorable backup.
        # Same probe the restore uses (_remote_dir_exists), so what lists as
        # restorable is exactly what restore_snapper_snapshot then finds.
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
                "metadata": metadata,
            }
        )

    backups.sort(key=lambda b: b["number"])
    return backups


def _remote_dir_exists(endpoint: Any, path: str) -> bool:
    """True when ``path`` is a directory on the endpoint's remote host.

    ``test -d`` is the probe BOTH the enumeration and the restore use, so a slot
    that lists as restorable is the same thing the restore then reads; two
    different probes could disagree and would eventually be made to.

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


class _RemoteSubvolume:
    """The one thing ``SSHEndpoint.send`` needs from a snapshot: ``get_path()``.

    A restore source is a subvolume at a known remote path, not a snapshot this
    process enumerated, so there is no ``__util__.Snapshot`` to hand over:
    that class derives its path from ``prefix + timestamp``, and a snapper slot
    (``.snapshots/{num}/snapshot``) does not follow that naming at all. ``send``
    calls ``_normalize_path(snapshot.get_path())`` and touches nothing else, so
    this carries exactly that rather than pretending to be a full snapshot --
    a fake Snapshot would answer ``get_name()``/``time_obj`` with fiction.
    """

    __slots__ = ("path",)

    def __init__(self, path: str) -> None:
        self.path = path

    def get_path(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return f"_RemoteSubvolume({self.path!r})"


def _resolve_remote_snapper_backup(
    backup_path: str,
    backup_number: int,
    endpoint_options: dict | None = None,
) -> tuple[Any, str, str | None]:
    """Resolve one snapper backup at an ``ssh://`` btrfs target, for restore.

    The remote layout is identical to a local btrfs target's --
    ``{path}/.snapshots/{num}/snapshot`` beside ``info.xml`` -- so this is the
    remote twin of the local branch's existence check, run over the endpoint
    because ``Path("ssh://host:/p")`` is a nonexistent LOCAL path (it collapses
    to ``ssh:/host:/p``, which is what the failure used to name).

    Returns ``(endpoint, remote_snapshot_path, info_xml_text_or_None)``.
    """
    from ..endpoint import choose_endpoint

    endpoint = choose_endpoint(
        backup_path, _restore_endpoint_config(backup_path, endpoint_options)
    )
    slot = f"{str(endpoint.config['path']).rstrip('/')}/.snapshots/{backup_number}"
    snapshot_path = f"{slot}/snapshot"

    if not _remote_dir_exists(endpoint, snapshot_path):
        raise RestoreError(
            f"Backup snapshot not readable: {snapshot_path}. The probe failed, "
            "which means the snapshot is absent OR the connecting user cannot "
            "reach it -- those are indistinguishable from here, so check both. "
            "On an ssh:// target --ssh-sudo elevates only btrfs and so does not "
            "grant access to this path; grant the user access instead, e.g. "
            "setfacl -m u:<user>:rx on the .snapshots directory."
        )

    info_xml = _read_remote_text(endpoint, f"{slot}/info.xml")
    if info_xml is None:
        logger.debug("No readable info.xml in remote snapper slot %s", slot)
    return endpoint, snapshot_path, info_xml


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


def _resolve_raw_snapper_backup(
    backup_path: str,
    backup_number: int,
    endpoint_options: dict | None = None,
    backup_name: str | None = None,
) -> tuple[Any, Any, Any]:
    """Resolve a raw snapper backup to (endpoint, RawSnapshot, BackupMetadata).

    Two resolution modes:

    * ``backup_name`` given (EXACT) -- resolve that specific sidecar by its unique
      name. This is what the CLI threads once it has enumerated the exact backup the
      user selected (``--snapshot`` after collision-dedup, ``--backup-name``,
      ``--date``), so restore materializes precisely that backup rather than
      re-guessing by number.
    * ``backup_name`` None (FALLBACK, bare-number API callers) -- match by
      ``snapper_number``; on a collision (snapper reuses numbers after a prune, and
      raw targets accumulate streams), pick the NEWEST by snapper_date and warn.

    The matched name maps to a RawSnapshot by ``.name`` (the shared cross-type backup
    identity). One endpoint is built and reused for the lookup and the later ``send()``.

    Raises RestoreError if the requested backup has no sidecar or its stream is missing.
    """
    from ..endpoint import choose_endpoint

    endpoint = choose_endpoint(
        backup_path, _restore_endpoint_config(backup_path, endpoint_options)
    )

    if backup_name is not None:
        # EXACT: load this specific sidecar by its unique name (no number guessing).
        try:
            match_meta = _load_snapper_sidecar(endpoint, backup_name)
        except Exception as e:
            raise RestoreError(
                f"Snapper backup {backup_name!r} not found at {backup_path}: {e}"
            ) from e
        match_name = backup_name
    else:
        # FALLBACK: collect ALL sidecars matching this snapper number, then pick the
        # newest on a collision. Iterating a sorted list keeps the choice deterministic
        # (the underlying set order is salted -> would otherwise be nondeterministic).
        matches: list[tuple[str, Any]] = []
        for name in sorted(_list_snapper_backups_at_destination(endpoint)):
            try:
                meta = _load_snapper_sidecar(endpoint, name)
            except Exception as e:
                logger.warning(
                    "Skipping raw snapper backup %r (unreadable sidecar): %s", name, e
                )
                continue
            if meta.snapper_number == backup_number:
                matches.append((name, meta))

        if not matches:
            raise RestoreError(
                f"Snapper backup number {backup_number} not found at {backup_path}"
            )

        if len(matches) > 1:
            # Deterministic: newest snapper_date wins (name breaks a date tie).
            matches.sort(key=lambda nm: (nm[1].snapper_date, nm[0]), reverse=True)
            logger.warning(
                "Multiple raw snapper backups share number %d at %s: %s. Restoring the "
                "newest (%s, %s); use --backup-name or --date to restore an older copy.",
                backup_number,
                backup_path,
                ", ".join(f"{n} ({m.snapper_date})" for n, m in matches),
                matches[0][0],
                matches[0][1].snapper_date,
            )

        match_name, match_meta = matches[0]

    raw_snapshot = next(
        (s for s in endpoint.list_snapshots() if s.name == match_name), None
    )
    if raw_snapshot is None:
        raise RestoreError(
            f"Raw stream for snapper backup {match_name!r} not found at {backup_path} "
            "(its .snapper-meta.json sidecar exists but the btrfs-send stream is missing)"
        )

    return endpoint, raw_snapshot, match_meta


def restore_snapper_snapshot(
    backup_path: str,
    backup_number: int,
    snapper_config_name: str,
    parent_backup_number: int | None = None,
    options: dict | None = None,
    dry_run: bool = False,
    endpoint_options: dict | None = None,
    backup_name: str | None = None,
) -> tuple[int, Path]:
    """Restore a snapper backup to local snapper format.

    Restores from:
        {backup_path}/.snapshots/{backup_number}/snapshot
    To local snapper:
        {snapper_snapshots_dir}/{new_number}/snapshot

    Uses Rich progress bar for transfers.

    Args:
        backup_path: Base path of backup (e.g., /backup/home)
        backup_number: Snapshot number to restore from backup
        snapper_config_name: Local snapper config to restore to
        parent_backup_number: Parent snapshot number for incremental restore
        options: Transfer options
        dry_run: Show what would be done without doing it

    Returns:
        Tuple of (new snapshot number, path to restored snapshot)

    Raises:
        RestoreError: If restore fails
    """
    import os

    from ..snapper import SnapperScanner
    from ..snapper.metadata import (
        SnapperMetadata,
        generate_info_xml,
        parse_info_xml,
        parse_info_xml_string,
        renumber_info_xml,
    )

    if options is None:
        options = {}

    show_progress = options.get("show_progress", True)

    # Find the snapper config
    scanner = SnapperScanner()
    local_config = scanner.get_config(snapper_config_name)
    if local_config is None:
        raise RestoreError(f"Local snapper config not found: {snapper_config_name}")

    # Backup paths / source resolution (Seam 1). Three layouts, one per kind of
    # target:
    #   raw        -- no .snapshots/{n}/snapshot subvolume at all; the source is a
    #                 stored btrfs-send stream resolved via the sidecars.
    #   ssh://     -- the SAME layout as a local btrfs target, on the far side of a
    #                 connection, so it must be probed over the endpoint.
    #   local      -- the existing subvolume-path check, unchanged.
    #
    # The ssh:// branch is why this dispatch exists: Path("ssh://host:/p") is a
    # nonexistent LOCAL path (it collapses to "ssh:/host:/p"), so every ssh://
    # restore failed the .exists() check below and reported that mangled path as
    # a missing snapshot -- while `snapper restore --list` on the same target
    # listed the backups perfectly well.
    #
    # Classification comes from core.target, the single scheme authority, not from
    # another local startswith(): call sites each deciding "is this remote?" for
    # themselves, and disagreeing, is the family this belongs to.
    scheme = parse_target(backup_path)
    backup_base = Path(backup_path)
    is_raw = scheme.is_raw
    is_ssh = scheme.kind is TargetKind.SSH

    raw_endpoint = None
    raw_snapshot = None
    raw_backup_meta = None
    ssh_endpoint = None
    remote_snapshot_path: str | None = None
    remote_info_xml: str | None = None
    backup_snapshot_path: Path | None = None
    backup_info_xml: Path | None = None

    if is_raw:
        raw_endpoint, raw_snapshot, raw_backup_meta = _resolve_raw_snapper_backup(
            backup_path, backup_number, endpoint_options, backup_name
        )
        source_desc = raw_snapshot.name
    elif is_ssh:
        (
            ssh_endpoint,
            remote_snapshot_path,
            remote_info_xml,
        ) = _resolve_remote_snapper_backup(backup_path, backup_number, endpoint_options)
        source_desc = f"{backup_path} snapshot {backup_number}"
    else:
        backup_snapshot_dir = backup_base / ".snapshots" / str(backup_number)
        backup_snapshot_path = backup_snapshot_dir / "snapshot"
        backup_info_xml = backup_snapshot_dir / "info.xml"
        if not backup_snapshot_path.exists():
            raise RestoreError(f"Backup snapshot not found: {backup_snapshot_path}")
        source_desc = str(backup_snapshot_path)

    # Get next available snapshot number for restore
    next_num = scanner.get_next_snapshot_number(local_config)

    # Local destination paths
    dest_snapshot_dir = local_config.snapshots_dir / str(next_num)
    dest_snapshot_path = dest_snapshot_dir / "snapshot"

    # Parent path for incremental. Not applicable to raw: the stored stream already
    # encodes whatever it encodes (full, or incremental against a parent matched by
    # received_uuid, which the oldest-first restore loop lands just before this one) --
    # RawEndpoint.send replays it verbatim, there is no btrfs-send `-p` to add here.
    parent_path = None
    remote_parent_path: str | None = None
    if is_ssh and parent_backup_number:
        # The parent lives on the remote too, so it is probed there. Same
        # fall-back-to-full rule as the local branch: an absent parent must
        # degrade the restore, never fail it.
        assert ssh_endpoint is not None
        candidate = (
            f"{str(ssh_endpoint.config['path']).rstrip('/')}"
            f"/.snapshots/{parent_backup_number}/snapshot"
        )
        if _remote_dir_exists(ssh_endpoint, candidate):
            remote_parent_path = candidate
        else:
            logger.warning(
                "Parent snapshot %d not found on the remote, "
                "falling back to full restore",
                parent_backup_number,
            )
    elif not is_raw and parent_backup_number:
        parent_path = (
            backup_base / ".snapshots" / str(parent_backup_number) / "snapshot"
        )
        if not parent_path.exists():
            logger.warning(
                "Parent snapshot %d not found, falling back to full restore",
                parent_backup_number,
            )
            parent_path = None
    elif is_raw and parent_backup_number:
        logger.debug(
            "Raw source: ignoring parent %d (stream is self-contained)",
            parent_backup_number,
        )

    if parent_path or remote_parent_path:
        logger.info(
            "Restoring snapshot %d -> %d (incremental from %d) ...",
            backup_number,
            next_num,
            parent_backup_number,
        )
    else:
        logger.info("Restoring snapshot %d -> %d (full) ...", backup_number, next_num)

    if dry_run:
        logger.info("Dry run - would restore as snapshot %d", next_num)
        return next_num, Path("/dev/null")

    # Recorded BEFORE anything is created. get_next_snapshot_number is supposed to
    # hand back a free slot, but the failure path below removes the whole numbered
    # directory, and "supposed to be free" is the kind of unchecked premise that
    # turns a failed restore into the loss of a snapshot that was already there --
    # a stale scan, a concurrent snapper, or a slot made by hand is enough.
    slot_preexisted = dest_snapshot_dir.exists()
    snapshot_preexisted = dest_snapshot_path.exists()

    transfer_start = time.monotonic()

    log_transaction(
        action="snapper_restore",
        status="started",
        source=source_desc,
        destination=str(dest_snapshot_path),
        snapshot=str(backup_number),
        parent=str(parent_backup_number) if parent_backup_number else None,
    )

    # Label for send-side failures: a raw stream's decode pipeline is not `btrfs
    # send`, and an ssh:// send failed on the OTHER machine -- saying which end
    # broke is most of the diagnosis.
    if is_raw:
        send_label = "raw stream decode"
    elif is_ssh:
        send_label = "remote btrfs send"
    else:
        send_label = "btrfs send"

    try:
        # Create destination directory. Direct first, sudo only if that is
        # refused: a snapper destination under SYNC_ACL is frequently writable by
        # the backup user, and the documented sudoers (NOPASSWD: /usr/bin/btrfs)
        # does not cover mkdir at all -- so the unconditional shell-out failed a
        # restore that needed no privilege whatsoever.
        __util__.privileged_mkdir(dest_snapshot_dir, allow_prompt=True)

        # Build btrfs receive command (shared: the raw stream's embedded subvolume is
        # named "snapshot" -- the backup source was .snapshots/{n}/snapshot -- so
        # receive lands dest_snapshot_dir/snapshot exactly like the btrfs path).
        receive_cmd = ["btrfs", "receive", str(dest_snapshot_dir)]
        if os.geteuid() != 0:
            receive_cmd = ["sudo"] + receive_cmd
        logger.debug("Receive command: %s", " ".join(receive_cmd))

        # Seam 2 (send side) + Seam 3 (progress estimate).
        estimated_size = None
        if is_raw:
            # RawEndpoint.send returns a Popen whose stdout is the verified,
            # decrypted/decompressed btrfs-send stream -- a drop-in for the send
            # Popen below. Its integrity check fires INSIDE send() before any bytes
            # are received, so a corrupt stream aborts before touching the slot. No
            # size estimate: raw .size is the on-disk (compressed) size, not the
            # decoded stream size, so a progress total would mislead (spinner only).
            assert raw_endpoint is not None and raw_snapshot is not None
            send_process = raw_endpoint.send(raw_snapshot)
        elif is_ssh:
            # SSHEndpoint.send runs `btrfs send` ON THE REMOTE and returns a Popen
            # whose stdout is the stream -- the same shape RawEndpoint.send returns,
            # so the receive below is untouched by which end the bytes came from.
            #
            # No size estimate: the endpoint's _estimate_snapshot_size runs
            # `btrfs subvolume show` LOCALLY (it exists for the backup direction,
            # where the source IS local), so aiming it at a remote path measures
            # nothing and returns None after two failed subprocesses. A missing
            # total shows a spinner; a wrong total misinforms, and the raw branch
            # above already set that precedent deliberately.
            assert ssh_endpoint is not None and remote_snapshot_path is not None
            send_process = ssh_endpoint.send(
                _RemoteSubvolume(remote_snapshot_path),
                parent=(
                    _RemoteSubvolume(remote_parent_path) if remote_parent_path else None
                ),
            )
        else:
            send_cmd = ["btrfs", "send"]
            if parent_path:
                send_cmd.extend(["-p", str(parent_path)])
            send_cmd.append(str(backup_snapshot_path))
            if os.geteuid() != 0:
                send_cmd = ["sudo"] + send_cmd
            logger.debug("Send command: %s", " ".join(send_cmd))
            if show_progress and not parent_path:
                estimated_size = progress_utils.estimate_snapshot_size(
                    str(backup_snapshot_path),
                    str(parent_path) if parent_path else None,
                )
            send_process = subprocess.Popen(
                send_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

        # Use Rich progress for local transfers
        use_rich_progress = show_progress and progress_utils.is_interactive()

        if use_rich_progress:
            receive_process = subprocess.Popen(
                receive_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            send_rc, receive_rc = progress_utils.run_transfer_with_progress(
                send_process=send_process,
                receive_process=receive_process,
                snapshot_name=f"snapshot {backup_number}",
                estimated_size=estimated_size,
            )

            if send_rc != 0:
                raise RestoreError(f"{send_label} failed with code {send_rc}")
            if receive_rc != 0:
                raise RestoreError(f"btrfs receive failed with code {receive_rc}")
        else:
            # Simple pipe without progress
            receive_process = subprocess.Popen(
                receive_cmd,
                stdin=send_process.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # One idiom for releasing a handed-over pipe, everywhere. There are
            # no intermediate stages here, so there is nothing for chain_stages
            # to chain -- but the handoff itself is the same one, and the same
            # omission elsewhere left a stage blocked forever.
            from . import transfer as transfer_utils

            transfer_utils.hand_over(send_process.stdout)

            receive_stdout, receive_stderr = receive_process.communicate()
            send_process.wait()

            if send_process.returncode != 0:
                raise RestoreError(
                    f"{send_label} failed with code {send_process.returncode}"
                )
            if receive_process.returncode != 0:
                raise RestoreError(
                    f"btrfs receive failed: {receive_stderr.decode().strip()}"
                )

        # btrfs receive creates "snapshot" subvolume, which is exactly what we want
        # No rename needed since snapper expects .snapshots/{num}/snapshot

        # Copy or generate info.xml
        dest_info_xml = dest_snapshot_dir / "info.xml"
        if is_raw:
            # Seam 4: build info.xml for the fresh slot. Raw targets have no
            # .snapshots/{n}/info.xml to copy, so prefer RENUMBERING the sidecar's
            # stored original_info_xml (snapper's OWN xml) in place -- only <num>
            # changes; userdata (incl. multi-entry), <uid>, and any element we do not
            # model are preserved VERBATIM. Fall back to regenerating from the parsed
            # fields only when no original was stored or it will not parse (older
            # sidecars that also stored a mis-parsed userdata dict still round-trip
            # correctly this way, since the original xml is snapper's authoritative copy).
            assert raw_backup_meta is not None
            xml_content = None
            if raw_backup_meta.original_info_xml:
                try:
                    xml_content = renumber_info_xml(
                        raw_backup_meta.original_info_xml, next_num
                    )
                except Exception as e:
                    logger.warning(
                        "Could not renumber stored info.xml for backup %d (%s); "
                        "regenerating from sidecar fields",
                        backup_number,
                        e,
                    )
            if xml_content is None:
                metadata = raw_backup_meta.to_snapper_metadata()
                metadata.num = next_num
                xml_content = generate_info_xml(metadata)
        elif is_ssh:
            # Seam 4 over ssh. The remote info.xml was already read during
            # resolution, so no second connection is opened here. Prefer
            # RENUMBERING it: only <num> changes, and everything snapper wrote --
            # multi-entry userdata, <uid>, elements this project does not model --
            # survives verbatim. Parse-and-regenerate is the fallback because it
            # can only preserve the fields modelled here, and a synthesized
            # description is the last resort rather than the first.
            xml_content = None
            if remote_info_xml:
                try:
                    xml_content = renumber_info_xml(remote_info_xml, next_num)
                except Exception as e:
                    logger.warning(
                        "Could not renumber remote info.xml for backup %d (%s); "
                        "regenerating from its parsed fields",
                        backup_number,
                        e,
                    )
                if xml_content is None:
                    try:
                        metadata = parse_info_xml_string(remote_info_xml)
                        metadata.num = next_num
                        xml_content = generate_info_xml(metadata)
                    except Exception as e:
                        logger.warning(
                            "Could not parse remote info.xml for backup %d: %s",
                            backup_number,
                            e,
                        )
            if xml_content is None:
                from datetime import datetime

                metadata = SnapperMetadata(
                    type="single",
                    num=next_num,
                    date=datetime.now(),
                    description=f"Restored from backup {backup_number}",
                    cleanup="",
                )
                xml_content = generate_info_xml(metadata)
        elif backup_info_xml is not None and backup_info_xml.exists():
            # Copy original info.xml but update the number
            try:
                metadata = parse_info_xml(backup_info_xml)
                metadata.num = next_num
                xml_content = generate_info_xml(metadata)
            except Exception as e:
                logger.warning("Could not parse backup info.xml, generating new: %s", e)
                from datetime import datetime

                metadata = SnapperMetadata(
                    type="single",
                    num=next_num,
                    date=datetime.now(),
                    description=f"Restored from backup {backup_number}",
                    cleanup="",
                )
                xml_content = generate_info_xml(metadata)
        else:
            # Generate new info.xml
            from datetime import datetime

            metadata = SnapperMetadata(
                type="single",
                num=next_num,
                date=datetime.now(),
                description=f"Restored from backup {backup_number}",
                cleanup="",
            )
            xml_content = generate_info_xml(metadata)

        # Write info.xml
        #
        # Slot dir is 0755 to match what snapper NATIVELY creates for .snapshots/{N}
        # (verified against snapper 0.13: native slot dirs are 0755, only the parent
        # .snapshots is 0750). 0750 here would be stricter than snapper and can block
        # non-root snapper access under ALLOW_USERS/ALLOW_GROUPS/SYNC_ACL; the parent
        # .snapshots (0750, root+group) still gates who can reach the slot at all.
        __util__.privileged_write_bytes(dest_info_xml, xml_content, allow_prompt=True)
        __util__.privileged_chmod(dest_snapshot_dir, 0o755, allow_prompt=True)

        duration = time.monotonic() - transfer_start

        log_transaction(
            action="snapper_restore",
            status="completed",
            source=source_desc,
            destination=str(dest_snapshot_path),
            snapshot=str(backup_number),
            parent=str(parent_backup_number) if parent_backup_number else None,
            duration_seconds=duration,
        )

        logger.info(
            "Restored snapshot %d -> %d successfully (%.1fs)",
            backup_number,
            next_num,
            duration,
        )
        return next_num, dest_snapshot_path

    except Exception as e:
        duration = time.monotonic() - transfer_start
        log_transaction(
            action="snapper_restore",
            status="failed",
            source=source_desc,
            destination=str(dest_snapshot_path),
            snapshot=str(backup_number),
            parent=str(parent_backup_number) if parent_backup_number else None,
            duration_seconds=duration,
            error=str(e),
        )

        # Clean up partial restore -- only what this run put there. The subvolume
        # and the numbered directory are judged separately: a restore can fail
        # after creating the directory but before receiving into it, and it can
        # also fail into a slot that already held one.
        if snapshot_preexisted or slot_preexisted:
            logger.warning(
                "Not removing %s after the failed restore: it was already present "
                "before this restore started, so it is not this run's partial. "
                "Inspect it before deleting anything.",
                dest_snapshot_path if snapshot_preexisted else dest_snapshot_dir,
            )
        try:
            if dest_snapshot_path.exists() and not snapshot_preexisted:
                if os.geteuid() != 0:
                    subprocess.run(
                        [
                            "sudo",
                            "btrfs",
                            "property",
                            "set",
                            "-f",
                            str(dest_snapshot_path),
                            "ro",
                            "false",
                        ],
                        capture_output=True,
                    )
                    subprocess.run(
                        [
                            "sudo",
                            "btrfs",
                            "subvolume",
                            "delete",
                            str(dest_snapshot_path),
                        ],
                        capture_output=True,
                    )
                else:
                    subprocess.run(
                        [
                            "btrfs",
                            "property",
                            "set",
                            "-f",
                            str(dest_snapshot_path),
                            "ro",
                            "false",
                        ],
                        capture_output=True,
                    )
                    __util__.delete_subvolume(dest_snapshot_path)
            if dest_snapshot_dir.exists() and not slot_preexisted:
                __util__.privileged_rmtree(dest_snapshot_dir, allow_prompt=True)
        except Exception as cleanup_e:
            logger.warning("Cleanup failed: %s", cleanup_e)

        logger.error("Failed to restore snapshot %d: %s", backup_number, e)
        raise RestoreError(f"Failed to restore snapshot {backup_number}: {e}") from e
