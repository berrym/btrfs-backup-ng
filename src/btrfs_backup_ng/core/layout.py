"""Restore layouts: where a received snapshot lands, how the destination is read
for correspondence, how a copy is published, and how an abandoned one is found.

A restore is a transfer through the engine's planner and executor with the
roles swapped: the backup location is the SOURCE and a local btrfs filesystem
is the DESTINATION. Everything that differs between "restore into a plain
directory" and "restore into a snapper config" is a question about the
destination, and a Layout answers those questions. The engine never learns a
layout's name; it is handed ``destination_view`` to plan against and
``receive_endpoint`` to execute against, and both are ordinary endpoints.

Only the plain layout exists here. The snapper layout (one object serving the
backup and the restore direction) and the in-place layout (#109) build on the
same four answers.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from .. import __util__
from .operations import _received_subvolume_shape, received_name_of

logger = logging.getLogger(__name__)

#: Where the plain layout records an in-flight receive, below the destination:
#: ``DEST/.btrfs-backup-ng/restore/<token>.json``. The ``.btrfs-backup-ng``
#: tree is what the local endpoint's ``prepare`` already creates under a
#: destination, so nothing here creates a directory above it.
MARKER_DIR = ("restore",)
MARKER_FORMAT = 1


def marker_dir(destination: Path) -> Path:
    return Path(destination) / ".btrfs-backup-ng" / MARKER_DIR[0]


class Collision:
    """A same-name entry at the destination that is not the copy about to be
    received there, and what it is."""

    def __init__(self, name: str, path: Path, what: str) -> None:
        self.name = name
        self.path = path
        self.what = what

    def __str__(self) -> str:
        return f"{self.path} {self.what}"


def describe_entry(
    destination_endpoint: Any, path: Path, stream_uuid: str
) -> Optional[str]:
    """What sits at ``path`` when a receive of a stream carrying ``stream_uuid``
    is about to land there, or None when nothing does.

    Asked BEFORE streaming. ``btrfs receive`` names the subvolume it creates
    after the source, so anything already under that name makes the receive
    fail with "File exists" -- after transferring everything. Presence by
    correspondence has already been decided by the planner (a copy of this
    very snapshot is skipped and never reaches here), so whatever is found is
    by definition not that copy: the answer names it so the operator can
    decide, and nothing here ever deletes it.

    The one identity probe (``subvolume_identity``) is asked for the entry's
    received_uuid, so "could not read" is told apart from "has none".
    """
    if not path.exists() and not path.is_symlink():
        return None
    try:
        shape = _received_subvolume_shape(str(path))
    except OSError as e:
        return f"exists but could not be examined ({e})"
    if shape is not None:
        return f"is {shape}"
    ids = destination_endpoint.subvolume_identity(str(path))
    if ids is None:
        return (
            "is a subvolume whose identity could not be read (btrfs subvolume "
            "show needs privilege), so it cannot be confirmed as this backup's "
            "copy"
        )
    received = ids.get("received_uuid", "")
    if not received:
        return (
            "is a subvolume with no received_uuid: an interrupted restore, or "
            "something else, under this name"
        )
    if not stream_uuid:
        return (
            f"is a received subvolume (received_uuid {received}), and the "
            f"backup's own identity is unknown (a sidecar without source_uuid), "
            f"so it cannot be confirmed as this backup's copy"
        )
    return (
        f"is a received copy of a different snapshot (received_uuid {received}, "
        f"this backup's stream carries {stream_uuid})"
    )


class PlainLayout:
    """Receive into ``DEST`` as ``DEST/<name>``; the destination is the local
    endpoint's own listing; nothing to publish; a run marker per receive.

    A received subvolume cannot be moved once it exists, so the plain layout
    receives in place and gets its authorship record from the marker instead
    of from a staging slot: while a receive is in flight
    ``DEST/.btrfs-backup-ng/restore/<token>.json`` names it, and the marker is
    removed once the executor has its verdict. A marker that outlives its
    process (a SIGKILL, a power loss) is what ``restore --cleanup`` reads: it
    may remove the subvolume a stale marker names when that subvolume has no
    received_uuid, and nothing else.
    """

    def __init__(self, local_endpoint: Any, session_id: str) -> None:
        self.endpoint = local_endpoint
        self.session_id = session_id
        self.destination = Path(str(local_endpoint.config["path"]))
        self._receiver = _MarkedReceiver(self)
        self._plan: list = []
        self._landing: dict[str, str] = {}
        self._on_progress: Optional[Callable[[int, int, str], None]] = None
        self._started = 0
        self._markers: dict[str, Path] = {}

    # -- the four answers ------------------------------------------------- #

    @property
    def destination_view(self) -> Any:
        """What the planner enumerates for correspondence: the endpoint itself,
        listing ``DEST`` under the source's prefix with received_uuid enriched."""
        return self.endpoint

    @property
    def receive_endpoint(self) -> Any:
        """What the executor receives through: the endpoint, with the marker
        written as each receive starts and cleared as each verdict lands."""
        return self._receiver

    def received_path(self, snapshot: Any) -> Path:
        return self.destination / received_name_of(snapshot)

    def collisions(self, plan: list) -> list[Collision]:
        """Every planned receive whose landing name is already taken.

        Checked for the whole plan before a byte moves, so a run either
        proceeds or refuses; it never streams three snapshots and refuses the
        fourth. An entry present by correspondence never appears in the plan,
        so anything found here is not this backup's copy.
        """
        found = []
        for snapshot, _parent in plan:
            path = self.received_path(snapshot)
            what = describe_entry(
                self.endpoint, path, getattr(snapshot, "stream_uuid", "") or ""
            )
            if what is not None:
                found.append(Collision(received_name_of(snapshot), path, what))
        return found

    # -- the run ------------------------------------------------------------ #

    def begin(
        self, plan: list, on_progress: Optional[Callable[[int, int, str], None]]
    ) -> None:
        self._plan = list(plan)
        # The receive is handed the snapshot's display name (``str(snapshot)``);
        # the marker records where the copy LANDS. Mapped from the plan rather
        # than assumed equal, and looked up by name rather than by position:
        # the executor skips an entry whose in-run parent failed, so a running
        # index would drift onto the wrong snapshot.
        self._landing = {str(s): received_name_of(s) for s, _ in self._plan}
        self._on_progress = on_progress
        self._started = 0

    def _receive_started(self, snapshot_name: str) -> None:
        self._started += 1
        received_name = self._landing.get(snapshot_name, snapshot_name)
        if self._on_progress is not None:
            self._on_progress(self._started, len(self._plan), received_name)
        self._write_marker(received_name)

    def _receive_verified(self, snapshot: Any) -> None:
        self._clear_marker(received_name_of(snapshot))

    def finish(self, result: Any) -> None:
        """After the executor returns: a failed entry whose partial the executor
        removed needs no marker; one whose subvolume is still there keeps its
        marker, so ``--cleanup`` can still identify it as this run's."""
        for snapshot, _error in getattr(result, "failed", []):
            name = received_name_of(snapshot)
            if name in self._markers and not self.received_path(snapshot).exists():
                self._clear_marker(name)

    # -- markers ------------------------------------------------------------ #

    def _write_marker(self, received_name: str) -> None:
        directory = marker_dir(self.destination)
        try:
            __util__.create_below(
                self.destination, ".btrfs-backup-ng", MARKER_DIR[0], what="Destination"
            )
        except OSError as e:
            logger.warning(
                "Could not create %s, so this receive has no run marker: %s. A "
                "restore interrupted from here would have to be cleaned by hand.",
                directory,
                e,
            )
            return
        token = f"{self.session_id}-{self._started:03d}"
        path = directory / f"{token}.json"
        record = {
            "format": MARKER_FORMAT,
            "session": self.session_id,
            "pid": os.getpid(),
            "snapshot": received_name,
            "path": str(self.destination / received_name),
            "started": datetime.now(timezone.utc).isoformat(),
        }
        try:
            __util__.atomic_write_bytes(path, json.dumps(record, indent=2) + "\n")
        except OSError as e:
            logger.warning("Could not write the run marker %s: %s", path, e)
            return
        self._markers[received_name] = path
        logger.debug("Run marker written: %s -> %s", path, received_name)

    def _clear_marker(self, received_name: str) -> None:
        path = self._markers.pop(received_name, None)
        if path is None:
            return
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning(
                "Could not remove the run marker %s: %s. `restore --cleanup` "
                "will report it as stale and leave the completed copy alone.",
                path,
                e,
            )


class _MarkedReceiver:
    """The plain layout's receive endpoint: the local endpoint, with the run
    marker written as a receive starts and cleared when the executor has
    verified the copy.

    Everything the executor asks -- ``config``, ``receive``, ``commit_receive``,
    ``subvolume_identity``, ``list_snapshots``, ``add_snapshot``,
    ``get_space_info``, ``get_id`` -- is the endpoint's own; the two hooks are
    ``receive`` (the marker goes down before ``btrfs receive`` starts) and
    ``add_snapshot`` (the executor's "this copy is verified" step, so the marker
    comes off there). Not a subclass: the endpoint is built and prepared by the
    caller and may be any local btrfs endpoint, real or a test's double.
    """

    def __init__(self, layout: PlainLayout) -> None:
        self._layout = layout
        self._endpoint = layout.endpoint

    def __getattr__(self, name: str) -> Any:
        return getattr(self._endpoint, name)

    def __repr__(self) -> str:
        return repr(self._endpoint)

    def receive(
        self,
        stdin: Any,
        snapshot_name: str = "",
        parent_name: str | None = None,
        source_uuid: str = "",
    ) -> Any:
        self._layout._receive_started(snapshot_name)
        return self._endpoint.receive(
            stdin, snapshot_name, parent_name=parent_name, source_uuid=source_uuid
        )

    def add_snapshot(self, snapshot: Any, rewrite: bool = True) -> None:
        self._layout._receive_verified(snapshot)
        self._endpoint.add_snapshot(snapshot, rewrite=rewrite)


# --------------------------------------------------------------------------- #
# what --cleanup reads
# --------------------------------------------------------------------------- #


class MarkedEntry:
    """A run marker and what its subvolume turned out to be."""

    def __init__(self, marker: Path, record: dict, path: Optional[Path]) -> None:
        self.marker = marker
        self.record = record
        self.path = path
        #: One of: in-progress, missing, complete, abandoned, unreadable,
        #: not-a-subvolume, malformed, foreign.
        self.state = ""
        self.detail = ""

    @property
    def name(self) -> str:
        return str(self.record.get("snapshot", self.marker.stem))


def _process_alive(pid: Any) -> bool:
    try:
        os.kill(int(pid), 0)
    except (ProcessLookupError, ValueError, TypeError):
        return False
    except PermissionError:
        return True
    return True


def _is_plain_component(name: Any) -> bool:
    """True when ``name`` is one ordinary path component: what ``btrfs receive``
    can have created directly under a destination.

    A marker's ``snapshot`` authorises a root-run deletion, so ``..`` (which
    pathlib does not collapse: ``Path("DEST/..").parent == DEST``), ``.``, a
    separator or a NUL would aim that deletion outside the destination.
    """
    return (
        isinstance(name, str)
        and name not in ("", ".", "..")
        and "/" not in name
        and "\0" not in name
    )


def still_abandoned(entry: MarkedEntry, destination_endpoint: Any) -> bool:
    """Re-check an ``abandoned`` entry immediately before it is deleted.

    The classification and the deletion are separate steps with an operator
    prompt between them; anything under the destination may have changed
    meanwhile. The entry must still be a real directory (not a symlink) with a
    subvolume root's inode and no received_uuid, or it is left.
    """
    path = entry.path
    if path is None or path.is_symlink():
        return False
    try:
        if _received_subvolume_shape(str(path)) is not None:
            return False
    except OSError:
        return False
    ids = destination_endpoint.subvolume_identity(str(path))
    return ids is not None and not ids.get("received_uuid")


def read_markers(destination: Path, destination_endpoint: Any) -> list[MarkedEntry]:
    """Every run marker under ``destination``, classified.

    The classification is what authorises a deletion, so it is conservative
    in every direction that matters: a marker whose process is still alive is
    a restore in progress; a marker naming a path anywhere but directly under
    ``destination``, or naming a symlink, is refused as foreign; a subvolume whose identity cannot
    be read is left; a subvolume WITH a received_uuid is a complete copy whose
    marker is merely stale. Only a stale marker naming a subvolume with no
    received_uuid is ``abandoned`` -- the interrupted receive this tool left.
    """
    destination = Path(destination)
    directory = marker_dir(destination)
    if not directory.is_dir():
        return []
    entries: list[MarkedEntry] = []
    for marker in sorted(directory.glob("*.json")):
        try:
            record = json.loads(marker.read_text())
            if not isinstance(record, dict):
                raise ValueError("not an object")
        except (OSError, ValueError) as e:
            entry = MarkedEntry(marker, {}, None)
            entry.state, entry.detail = "malformed", f"cannot be read: {e}"
            entries.append(entry)
            continue
        raw_path = record.get("path")
        name = record.get("snapshot")
        path = Path(str(raw_path)) if raw_path else None
        entry = MarkedEntry(marker, record, path)
        if (
            path is None
            or not _is_plain_component(name)
            or path.parent != destination
            or path.name != name
        ):
            entry.state = "foreign"
            entry.detail = "names a path that is not directly under this destination"
        elif path.is_symlink():
            entry.state = "foreign"
            entry.detail = (
                "names a symlink; what it points at was not received here, "
                "whatever it is"
            )
        elif _process_alive(record.get("pid")):
            entry.state = "in-progress"
            entry.detail = f"its restore (pid {record.get('pid')}) is still running"
        elif not path.exists():
            entry.state = "missing"
            entry.detail = "nothing is at the path it names"
        else:
            try:
                shape = _received_subvolume_shape(str(path))
            except OSError as e:
                shape = f"unexaminable ({e})"
            if shape is not None:
                entry.state = "not-a-subvolume"
                entry.detail = f"the entry it names is {shape}"
            else:
                ids = destination_endpoint.subvolume_identity(str(path))
                if ids is None:
                    entry.state = "unreadable"
                    entry.detail = (
                        "the subvolume's identity could not be read (btrfs "
                        "subvolume show needs privilege)"
                    )
                elif ids.get("received_uuid"):
                    entry.state = "complete"
                    entry.detail = (
                        f"the subvolume is a complete received copy "
                        f"(received_uuid {ids['received_uuid']}); only the "
                        f"marker is stale"
                    )
                else:
                    entry.state = "abandoned"
                    entry.detail = (
                        "a subvolume with no received_uuid under a name this "
                        "tool's own marker recorded: an interrupted restore"
                    )
        entries.append(entry)
    return entries
