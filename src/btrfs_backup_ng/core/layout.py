"""Restore layouts: where a received snapshot lands, how the destination is read
for correspondence, how a copy is published, and how an abandoned one is found.

A restore is a transfer through the engine's planner and executor with the
roles swapped: the backup location is the SOURCE and a local btrfs filesystem
is the DESTINATION. Everything that differs between "restore into a plain
directory" and "restore into a snapper config" is a question about the
destination, and a Layout answers those questions. The engine never learns a
layout's name; it is handed ``destination_view`` to plan against and
``receive_endpoint`` to execute against, and both are ordinary endpoints.

Two layouts live here. The plain layout receives into a directory. The
snapper layout receives into a numbered slot of snapper's own on-disk form,
and it is ONE object for both directions: a snapper backup to a btrfs target
and a snapper restore into a local config open, fill, publish and abandon a
slot through the same code, so the two cannot drift. The in-place layout
(#109) builds on the same answers.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from .. import __util__
from . import operations as _ops
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
# the snapper layout: numbered slots, for both directions
# --------------------------------------------------------------------------- #


class _OpenSlot:
    """The slot a receive is landing in: its number, the endpoint path to put
    back, and the slot lock held for the receive and the publish."""

    def __init__(self, number: int, saved_path: Any) -> None:
        self.number = number
        self.saved_path = saved_path
        self.locks = contextlib.ExitStack()


class SnapperLayout:
    """Snapper's on-disk form -- ``BASE/.snapshots/<n>/snapshot`` beside
    ``info.xml`` -- as a receive destination, on a btrfs endpoint whose path
    is ``BASE``: a backup target, or a snapper config's subvolume.

    The slot lifecycle is the one thing here, and both directions run it:

    - ``open_slot(n)``: the target must exist; the slot lock the destination
      offers (ssh:// targets have one, a local one none) is taken for slot n;
      a ``.incoming`` left by a crashed run is removed; ``.snapshots/<n>.incoming``
      is created below the target; the endpoint is pointed at it, so a
      ``btrfs receive`` through the endpoint lands ``<n>.incoming/snapshot``.
    - ``publish(info_xml)``: the endpoint is pointed back at ``BASE``; the
      slot's ``info.xml`` is written INTO ``.incoming``; the directory is
      renamed to ``<n>`` (a received subvolume is read-only and cannot be
      moved, its containing directory can, and the rename keeps
      received_uuid). A slot appears complete or not at all.
    - ``abandon()``: the endpoint is pointed back; ``.incoming`` and the
      subvolume in it are removed; the published slots are never touched.

    A snapper backup (``core.operations.send_snapper_snapshot``) calls those
    three around the engine's transfer with the source's own number and
    info.xml. A snapper restore hands the executor ``receive_endpoint`` and
    ``destination_view`` instead: the receive opens the NEXT FREE slot as it
    starts, the executor's artifact verdict examines ``<n>.incoming/snapshot``
    (the endpoint still points at the slot), and ``add_snapshot`` -- the
    executor's "this copy is verified" step -- publishes it with the backup's
    info.xml renumbered, through ``publish_fresh``: under the number that is
    free at that moment and with a rename that cannot replace anything,
    because the destination is a live snapper config and snapper may have
    taken the number meanwhile. A receive that never reached that step is
    abandoned by the next receive or by ``finish()``, so a failed restore
    leaves no numbered slot and no ``.incoming``. One restore at a time into
    a config (``restore_lock``), and only under that lock are the temps of
    restores that died swept (``sweep_stale_temps``).
    """

    def __init__(
        self,
        endpoint: Any,
        *,
        next_number: Optional[Callable[[], int]] = None,
    ) -> None:
        self.endpoint = endpoint
        self.base = str(endpoint.config["path"]).rstrip("/")
        self._next_number = next_number
        self._open: Optional[_OpenSlot] = None
        self._receiver = _SlotReceiver(self)
        self._info_xml_for: Callable[[Any, int], Optional[bytes]] = lambda _s, _n: None
        self._on_published: Optional[Callable[[Any, int], None]] = None
        self._on_progress: Optional[Callable[[int, int, str], None]] = None
        self._plan_size = 0
        self._started = 0
        #: ``(snapshot, slot number)`` for every copy published in this run.
        self.published: list[tuple[Any, int]] = []

    # -- paths -------------------------------------------------------------- #

    @property
    def snapshots_dir(self) -> str:
        return f"{self.base}/.snapshots"

    def incoming_dir(self, number: int) -> str:
        return f"{self.snapshots_dir}/{number}.incoming"

    def slot_dir(self, number: int) -> str:
        return f"{self.snapshots_dir}/{number}"

    @property
    def open_number(self) -> Optional[int]:
        """The slot a receive is landing in right now, or None."""
        return self._open.number if self._open is not None else None

    # -- the four answers ------------------------------------------------- #

    @property
    def destination_view(self) -> Any:
        """What the planner enumerates for correspondence: every published
        slot's received_uuid, read in one pass (``_snapper_dest_view``)."""
        return _ops._snapper_dest_view(self.endpoint)

    @property
    def receive_endpoint(self) -> Any:
        """What the executor receives through: the endpoint, with a slot
        opened as each receive starts and published as each copy is verified."""
        return self._receiver

    # -- the slot lifecycle, shared by both directions ---------------------- #

    def open_slot(self, number: int) -> None:
        """Make ``.snapshots/<number>.incoming`` the endpoint's receive path.

        The target is checked BEFORE the slot lock is taken: the lock lives
        under the target, so against a missing target the lock acquisition
        would fail first and report a lock directory that "could not be
        created" instead of the actual condition. The check is the engine's
        own, with the same diagnosis on every transport; nothing is created
        by it. The receive and the publish are two halves of ONE transaction
        on the slot, and the lock is named for exactly what the transfer
        beneath will lock, so that call finds it already held rather than
        taking a second one.
        """
        if self._open is not None:
            raise RuntimeError(
                f"slot {self._open.number} is still open; publish or abandon it first"
            )
        incoming = self.incoming_dir(number)
        _ops._ensure_destination_exists(self.endpoint)
        slot = _OpenSlot(number, self.endpoint.config["path"])
        try:
            slot.locks.enter_context(
                _ops._receiving_lock(self.endpoint, f"{incoming}/snapshot", self.base)
            )
            # A leftover temp from a prior crashed run goes first; then the
            # slot is made, below the target and only there.
            _ops._cleanup_snapper_backup(self.endpoint, number, False)
            _ops._snapper_prepare_slot(self.endpoint, number)
        except BaseException:
            slot.locks.close()
            raise
        self.endpoint.config["path"] = incoming
        self._open = slot
        logger.debug("Receiving into snapper slot %s", incoming)

    def publish(self, info_xml: Optional[bytes] = None) -> int:
        """Publish the open slot as ``.snapshots/<n>`` and return ``n``.

        ``info_xml`` is written into the slot BEFORE the rename, so the
        published slot carries its metadata from the instant it exists. A
        publish that fails abandons the slot (the ``.incoming`` temp is
        removed, a pre-existing published backup is never touched) and
        raises the executor's transfer error.
        """
        slot = self._require_open()
        self.endpoint.config["path"] = slot.saved_path
        try:
            if info_xml is not None and not _ops._write_info_xml(
                self.endpoint, self.incoming_dir(slot.number), info_xml
            ):
                raise __util__.SnapshotTransferError(
                    f"could not write info.xml into {self.incoming_dir(slot.number)}; "
                    f"the slot was not published (a slot is complete or not at all)"
                )
            _ops._snapper_publish_slot(self.endpoint, slot.number)
        except BaseException:
            self.abandon()
            raise
        slot.locks.close()
        self._open = None
        logger.debug("Published snapper slot %s", self.slot_dir(slot.number))
        return slot.number

    def publish_fresh(self, info_xml_for: Callable[[int], Optional[bytes]]) -> int:
        """Publish the open slot under a number that is free AT THIS MOMENT,
        never replacing anything, and return the number it landed under.

        The restore direction's publish. The destination is a LIVE snapper
        config: ``<n>.incoming`` is invisible to snapper (not a number), so
        snapper's timeline, or an operator's ``snapper create``, can take
        slot n while the receive is in flight -- and the backup direction's
        publish would move that snapshot aside and delete it, which is right
        for a recycled number at a backup target and data loss here. So the
        number is asked for again now, ``info_xml_for(n)`` is written into
        the temp for THAT number, and the directory is renamed with
        ``rename_noreplace``: the kernel refuses if anything is at ``n`` --
        including an empty directory snapper made a moment ago and is about
        to fill, which ``os.rename`` would silently take over -- and the
        next free number is tried. A publish that cannot proceed abandons
        the slot and raises the executor's transfer error.
        """
        slot = self._require_open()
        self.endpoint.config["path"] = slot.saved_path
        incoming = self.incoming_dir(slot.number)
        if self._next_number is None:
            raise RuntimeError("publish_fresh needs next_number")
        try:
            for _attempt in range(1000):
                number = self._next_number()
                xml = info_xml_for(number)
                if xml is not None and not _ops._write_info_xml(
                    self.endpoint, incoming, xml
                ):
                    raise __util__.SnapshotTransferError(
                        f"could not write info.xml into {incoming}; the copy was "
                        f"not published (a slot is complete or not at all)"
                    )
                try:
                    __util__.rename_noreplace(incoming, self.slot_dir(number))
                except FileExistsError:
                    logger.debug(
                        "Slot %d appeared between choosing it and the rename; "
                        "trying the next free number",
                        number,
                    )
                    continue
                if number != slot.number:
                    logger.info(
                        "Slot %d was taken while the copy was being received "
                        "(snapper, or another writer); it is left as it is and "
                        "the copy is published as slot %d.",
                        slot.number,
                        number,
                    )
                break
            else:
                raise __util__.SnapshotTransferError(
                    f"no free slot under {self.snapshots_dir} after 1000 attempts"
                )
        except __util__.SnapshotTransferError:
            self.abandon()
            raise
        except OSError as e:
            self.abandon()
            raise __util__.SnapshotTransferError(
                f"could not publish the received copy into {self.snapshots_dir}: {e}"
            ) from e
        except BaseException:
            self.abandon()
            raise
        slot.locks.close()
        self._open = None
        logger.debug("Published snapper slot %s", self.slot_dir(number))
        return number

    def restore_lock(self, subject: str) -> Any:
        """One restore at a time into this config.

        A local target has no receive lock (``_receiving_lock`` is a no-op),
        and the next free number counts only numeric entries, so two restores
        into one config would both pick n and the second's open_slot would
        remove the first's in-flight ``<n>.incoming`` as "a temp a crashed run
        left". An exclusive flock on ``.snapshots/.btrfs-backup-ng.restore.lock``
        (``__util__.exclusive_lock``, timeout 0) is held for the whole
        restore: a second restore is refused at once with words, and the
        kernel drops the lock when the holder dies, so a SIGKILLed restore
        never leaves the config locked. Held, it is also what makes
        ``sweep_stale_temps`` safe: no live restore can own a temp here.
        """
        return __util__.exclusive_lock(
            Path(self.snapshots_dir) / ".btrfs-backup-ng.restore.lock",
            timeout=0,
            subject=subject,
        )

    def sweep_stale_temps(self) -> list[str]:
        """Remove every ``<n>.incoming`` under ``.snapshots`` and return their
        names. Only for a caller holding ``restore_lock``: with it held, every
        temp here belongs to a restore that is no longer running (SIGKILL,
        power loss), because a live one would hold the lock. A restore's temp
        is named for the number it started with, which snapper may since have
        taken, so the per-number cleanup ``open_slot`` runs would never reach
        it."""
        snapshots_dir = Path(self.snapshots_dir)
        if not snapshots_dir.is_dir():
            return []
        stale = sorted(
            p.name
            for p in snapshots_dir.iterdir()
            if p.name.endswith(".incoming") and p.name[: -len(".incoming")].isdecimal()
        )
        for name in stale:
            logger.info(
                "Removing %s/%s, left by a restore that did not finish.",
                snapshots_dir,
                name,
            )
            _ops._cleanup_snapper_backup(
                self.endpoint, int(name[: -len(".incoming")]), False
            )
        return stale

    def abandon(self) -> None:
        """Remove the open slot's ``.incoming`` -- and only that -- and point
        the endpoint back at the target. A no-op when no slot is open."""
        slot = self._open
        if slot is None:
            return
        self._open = None
        self.endpoint.config["path"] = slot.saved_path
        try:
            logger.info(
                "Abandoning snapper slot %s: the receive into it did not complete.",
                self.incoming_dir(slot.number),
            )
            _ops._cleanup_snapper_backup(self.endpoint, slot.number, False)
        finally:
            slot.locks.close()

    def _require_open(self) -> _OpenSlot:
        if self._open is None:
            raise RuntimeError("no snapper slot is open")
        return self._open

    # -- the restore direction: driven by the executor ---------------------- #

    def begin(
        self,
        plan: list,
        *,
        info_xml_for: Callable[[Any, int], Optional[bytes]],
        on_published: Optional[Callable[[Any, int], None]] = None,
        on_progress: Optional[Callable[[int, int, str], None]] = None,
    ) -> None:
        """Before the executor runs: how each planned snapshot's slot gets its
        info.xml (asked with the snapshot and the slot number it is landing
        in), and who hears about each publish."""
        if self._next_number is None:
            raise RuntimeError(
                "a restore through the snapper layout needs next_number: the "
                "receive picks the next free slot as it starts"
            )
        self._info_xml_for = info_xml_for
        self._on_published = on_published
        self._on_progress = on_progress
        self._plan_size = len(plan)
        self._started = 0
        self.published = []

    def _receive_started(self, snapshot_name: str) -> int:
        # A slot still open here belongs to a receive that never reached the
        # verified step: its transfer failed, and the executor has moved on.
        self.abandon()
        assert self._next_number is not None
        number = self._next_number()
        self.open_slot(number)
        self._started += 1
        if self._on_progress is not None:
            self._on_progress(self._started, self._plan_size, snapshot_name)
        return number

    def _receive_verified(self, snapshot: Any) -> int:
        number = self.publish_fresh(lambda n: self._info_xml_for(snapshot, n))
        self.published.append((snapshot, number))
        if self._on_published is not None:
            self._on_published(snapshot, number)
        return number

    def finish(self) -> None:
        """After the executor returns, however it returned: a slot still open
        is a receive that failed, and it is abandoned."""
        self.abandon()


class _SlotReceiver:
    """The snapper layout's receive endpoint: the btrfs endpoint over the
    target, with a slot opened as ``btrfs receive`` starts and published when
    the executor has verified the copy.

    Everything the executor asks -- ``config``, ``commit_receive``,
    ``subvolume_identity``, ``get_space_info``, ``get_id`` -- is the
    endpoint's own; ``config["path"]`` is the slot while a receive is in
    flight, which is what makes the executor's artifact verdict examine
    ``<n>.incoming/snapshot``. ``list_snapshots`` is empty: the target is a
    snapper layout, not a directory of prefix-named snapshots, and its
    contents are read through ``destination_view`` instead. Not a subclass:
    the endpoint is built by the caller and may be any local btrfs endpoint,
    real or a test's double.
    """

    def __init__(self, layout: SnapperLayout) -> None:
        self._layout = layout
        self._endpoint = layout.endpoint

    def __getattr__(self, name: str) -> Any:
        return getattr(self._endpoint, name)

    def __repr__(self) -> str:
        return f"snapper layout at {self._layout.base}"

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

    def _receive_destination(self, source_path: Any) -> str:
        """Where a receive through this endpoint lands: ``<slot>/snapshot``.

        The executor's verdict asks the destination where the copy of a
        snapshot is, and by default derives it from the source's name. Every
        snapper backup was sent from ``.snapshots/<n>/snapshot``, so the
        stream names its subvolume ``snapshot`` whatever the backup is called
        -- a raw store names its streams ``<config>-<n>-<date>``, and a
        verdict looking for that under the slot would find nothing and
        condemn a copy that is right there. The layout knows the answer for
        every source, so it gives it.
        """
        return f"{str(self._endpoint.config['path']).rstrip('/')}/snapshot"

    def list_snapshots(self, flush_cache: bool = False) -> list:
        return []


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
