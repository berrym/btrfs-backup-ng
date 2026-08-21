"""Persistent locks on a remote target, held across processes and machines.

A restore reads a snapshot on a remote target while a prune, in a different
process or on a different machine, may be deleting from that same target. Before
this, the lock guarding the snapshot under restore lived in the restoring
process's memory, so the pruning process could not see it and was free to delete
what was being read.

Saying "this target does not persist locks", as `restore --status` used to, is
honest and useless: a backup tool that reports it cannot protect a restore is not
protecting the restore.

The primitives
--------------
Everything is done with POSIX operations on the target, because there is no
persistent connection on which to hold an ``flock``:

* **``mkdir`` is atomic.** Exactly one of any number of racing creators wins;
  the rest get EEXIST. Verified against a real remote: 20 concurrent contenders,
  one winner.
* **``mv`` is atomic within a filesystem**, which is what makes breaking a stale
  lock safe. A contender that judges a lock dead renames it and proceeds only if
  the rename succeeded, so two contenders that both see a dead lock cannot both
  acquire. Also verified: 20 concurrent breakers, one winner.

Staleness is judged ON THE REMOTE
---------------------------------
The holder ``touch``es a heartbeat file; a contender computes
``remote_now - remote_mtime`` on the target itself. Client clocks are never
compared. This is not hypothetical tidiness: the two hosts used to develop this
already differ by several seconds, and two unrelated clients can differ by far
more. A client with a fast clock would break live locks; one with a slow clock
would honour dead ones forever.

Cleanup never uses ``rm -rf``
-----------------------------
These commands run under sudo on a machine we are only visiting. Files are
removed by name and the directory is then ``rmdir``ed, which fails harmlessly if
anything unexpected is inside. A mis-constructed path can therefore delete
nothing but the lock it created.
"""

from __future__ import annotations

import atexit
import hashlib
import json
import logging
import os
import signal
import shlex
import threading
import uuid
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import Any, Callable, Iterator, Optional

logger = logging.getLogger(__name__)

#: How often the holder refreshes its heartbeat, in seconds.
#:
#: Every refresh is an ssh round trip, so this is a balance rather than a
#: preference: 15s would be 240 round trips an hour per target for no extra
#: safety, since the stale threshold is what actually bounds recovery.
DEFAULT_HEARTBEAT_INTERVAL = 30

#: How long a heartbeat may go unrefreshed before the lock is considered dead.
#:
#: Six missed refreshes. Generous on purpose: a lock broken too eagerly is two
#: writers on one target, which is the exact outcome this exists to prevent,
#: while a lock broken too late only delays an operation.
DEFAULT_STALE_AFTER = 180

#: Name of the directory holding all locks for a target.
LOCK_DIR_NAME = ".btrfs-backup-ng.locks"

#: Prefix marking a lock as a pin on one snapshot, as opposed to a whole-target
#: or transfer lock. The writer, the delete guards and ``restore --status`` all
#: key off this one constant so they cannot drift apart.
SNAPSHOT_LOCK_PREFIX = "snap-"

#: Holder files for a SHARED lock live here, one per holder.
HOLDERS_DIR_NAME = "holders"

#: A holder file older than this multiple of the stale threshold is not merely
#: unrefreshed, it is definitively abandoned: a live holder refreshes six times
#: within one threshold. Only then is it safe to delete someone else's file.
DEAD_HOLDER_MULTIPLE = 2


# --------------------------------------------------------------- cleanup
#
# A lock outlives the process that took it -- that is the whole point -- so an
# interrupted run must not leave its pins sitting on the target until the stale
# window expires. Ctrl-C on a restore should free the snapshot immediately, not
# in three minutes.
#
# Registered holders are released on normal exit (atexit) and on SIGINT/SIGTERM.
# The stale window remains the backstop for what neither can catch: SIGKILL,
# a power cut, a severed network.

_CLEANUP_LOCK = threading.Lock()
_CLEANUP_REGISTRY: "list[tuple[Any, str]]" = []
_SIGNALS_INSTALLED = False
_PREVIOUS_HANDLERS: dict = {}


def _register_for_cleanup(manager: Any, key: str) -> None:
    global _SIGNALS_INSTALLED
    with _CLEANUP_LOCK:
        _CLEANUP_REGISTRY.append((manager, key))
        if not _SIGNALS_INSTALLED:
            _SIGNALS_INSTALLED = True
            atexit.register(_release_all_held)
            _install_signal_handlers()


def _unregister_for_cleanup(manager: Any, key: str) -> None:
    with _CLEANUP_LOCK:
        for i, (held_manager, held_key) in enumerate(_CLEANUP_REGISTRY):
            if held_manager is manager and held_key == key:
                del _CLEANUP_REGISTRY[i]
                return


def _release_all_held() -> None:
    """Drop every pin this process still holds. Never raises."""
    with _CLEANUP_LOCK:
        pending = list(_CLEANUP_REGISTRY)
        _CLEANUP_REGISTRY.clear()
    for manager, key in pending:
        try:
            name, _, lock_id = key.partition("\x00")
            if lock_id:
                manager.release_shared(name, lock_id)
            else:
                manager.release(name)
        except Exception as exc:  # noqa: BLE001 - cleanup must not raise on exit
            logger.debug("Could not release %r during cleanup: %s", key, exc)


def _install_signal_handlers() -> None:
    """Release on SIGINT/SIGTERM, then do what the previous handler would.

    Chained rather than replaced: this is a library, and swallowing the
    application's own handler -- or the default that turns Ctrl-C into
    KeyboardInterrupt -- would be a worse bug than the one being fixed.
    Installing is skipped off the main thread, where signal() is not allowed.
    """
    if threading.current_thread() is not threading.main_thread():
        return
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous = signal.getsignal(signum)
        except (ValueError, OSError):  # pragma: no cover - platform dependent
            continue
        _PREVIOUS_HANDLERS[signum] = previous

        def handler(sig: int, frame: Any, _previous: Any = previous) -> None:
            _release_all_held()
            if callable(_previous):
                _previous(sig, frame)
            elif _previous == signal.SIG_DFL:
                signal.signal(sig, signal.SIG_DFL)
                os.kill(os.getpid(), sig)

        try:
            signal.signal(signum, handler)
        except (ValueError, OSError):  # pragma: no cover - platform dependent
            continue


class RemoteLockBusy(RuntimeError):
    """Raised when a live lock is held by someone else.

    Carries the holder's recorded details so the caller can say WHO holds it and
    since when, rather than only that it failed.
    """

    def __init__(self, name: str, info: Optional[dict] = None):
        self.name = name
        self.info = info or {}
        holder = ""
        if self.info:
            holder = (
                f" held by {self.info.get('operation', 'an operation')} on "
                f"{self.info.get('hostname', 'another host')} "
                f"(pid {self.info.get('pid', '?')})"
            )
        super().__init__(f"remote lock {name!r} is busy{holder}")


class RemoteLockUnavailable(RuntimeError):
    """The lock could not be operated at all -- not contention, but breakage.

    Kept distinct from RemoteLockBusy because the two demand different answers:
    contention means wait or refuse, while an unusable lock directory means the
    target is not in a state anyone should be writing to.
    """


#: Characters that may appear in a lock's on-disk name unchanged.
_SAFE_NAME_CHARS = "-_.@"


def encode_name(name: str) -> str:
    """The on-disk name for a lock called ``name``.

    Filesystem-safe AND injective. The safety was there before; the injectivity
    was not, and its absence was a hole rather than an inconvenience:

    * Every unsafe character mapped to ``_``, so ``restore:x/y`` and
      ``restore:x:y`` became one file. One holder releasing removed the other's
      pin, and the snapshot it protected became deletable while still in use.
    * The guard looked its snapshots up by their REAL names while the writer had
      stored a rewritten one, so a snapshot whose name needed rewriting was
      pinned under a name nothing would ever ask for. The pin existed and was
      invisible -- a prune would delete it mid-restore.

    A digest of the exact original is appended, so two different names cannot
    share a file no matter what characters they contain. The readable part is
    kept in front because an operator reading `ls` on a lock directory should
    still recognise what is locked.
    """
    safe = "".join(c if c.isalnum() or c in _SAFE_NAME_CHARS else "_" for c in name)
    digest = hashlib.sha256(name.encode("utf-8", errors="surrogatepass")).hexdigest()
    return f"{safe}-{digest[:16]}"


def decode_name_for_display(encoded: str) -> str:
    """Best-effort readable form of an encoded lock name.

    Only used where the lock's payload could not be read, so its real name is
    genuinely unknown and the encoded directory name is the only handle. The
    digest is stripped so an operator sees something recognisable rather than a
    hex suffix. It is a DISPLAY aid: nothing matches on the result, because the
    encoding is one-way and a name that had unsafe characters cannot be
    recovered exactly.
    """
    head, sep, tail = encoded.rpartition("-")
    if sep and len(tail) == 16 and all(c in "0123456789abcdef" for c in tail):
        return head
    return encoded


class Holder:
    """One live holder of a lock, and where its record lives.

    The file name is carried rather than recomputed because ``--unlock`` has to
    be able to remove a holder whose payload cannot be parsed -- exactly the
    holder it cannot name. Reconstructing the path from a name it does not know
    is impossible; deleting the file it was found in is not.
    """

    __slots__ = ("dir_name", "file_name", "info")

    def __init__(self, dir_name: str, file_name: str, info: dict) -> None:
        self.dir_name = dir_name
        self.file_name = file_name
        self.info = info

    @property
    def lock_id(self) -> Optional[str]:
        value = self.info.get("lock_id")
        return None if value is None else str(value)

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"Holder({self.dir_name!r}, {self.file_name!r}, {self.info!r})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Holder):
            return NotImplemented
        return (self.dir_name, self.file_name, self.info) == (
            other.dir_name,
            other.file_name,
            other.info,
        )


def _remote_mtime_expr(path: str) -> str:
    """Shell yielding ``path``'s mtime, or nothing if it does not exist.

    GNU ``stat -c %Y`` with a BSD ``stat -f %m`` fallback, matching how the rest
    of the codebase probes remote files -- targets are not all Linux.
    """
    q = shlex.quote(path)
    return f"stat -c %Y {q} 2>/dev/null || stat -f %m {q} 2>/dev/null"


def _remote_age_expr(path: str, fallback: Optional[str] = None) -> str:
    """Shell computing the age in seconds of ``path``, on the remote.

    ``fallback`` is a second path to age from when the first does not exist yet.
    It closes a real race in acquisition: between the ``mkdir`` that wins a lock
    and the ``touch`` that writes its first heartbeat there is a window in which
    the heartbeat is absent. Ageing a missing file from epoch zero makes that
    brand-new lock look infinitely old, and a contender arriving inside the
    window breaks it and takes a lock somebody else already holds -- two winners.
    Observed as an intermittent failure of the one-winner test, then reproduced
    exactly by creating the directory without its heartbeat.

    The lock directory itself is the fallback: ``mkdir`` creates it atomically,
    so its mtime is a sound lower bound on the holder's age.
    """
    primary = _remote_mtime_expr(path)
    if fallback is not None:
        primary = f"{primary} || {_remote_mtime_expr(fallback)}"
    return f"$(( $(date +%s) - $({primary} || echo 0) ))"


class RemoteLockManager:
    """Acquire, hold and release locks on a remote target.

    ``run_remote`` takes a single shell command string and returns
    ``(returncode, stdout, stderr)``. It is injected rather than taking an
    endpoint because the two remote endpoints expose different call signatures,
    and because it lets the elevation decision stay where it belongs -- with the
    endpoint that knows whether this target needs sudo.
    """

    def __init__(
        self,
        run_remote: Callable[[str], tuple[int, str, str]],
        target_path: str,
        *,
        heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
        stale_after: int = DEFAULT_STALE_AFTER,
        hostname: str = "",
        run_elevated: Optional[Callable[[str], tuple[int, str, str]]] = None,
    ) -> None:
        """``run_elevated`` is used only when the unprivileged attempt cannot
        create the lock directory. Backup destinations are commonly root-owned,
        and the lock has to live beside the data it protects."""
        self._run = run_remote
        self._root = f"{str(target_path).rstrip('/')}/{LOCK_DIR_NAME}"
        self._heartbeat_interval = heartbeat_interval
        self._stale_after = stale_after
        self._hostname = hostname
        self._run_elevated = run_elevated
        self._held: dict[str, threading.Event] = {}
        self._threads: dict[str, threading.Thread] = {}

    # ---------------------------------------------------------------- helpers

    def _lock_dir(self, name: str) -> str:
        return f"{self._root}/{encode_name(name)}.lock"

    def _acquire_script(self, name: str, payload: str, token: str) -> str:
        """One round trip: try, judge staleness, break if dead, try again.

        Written as a single script deliberately. Split across calls, another
        contender can slip between the staleness check and the break, which is
        precisely the race the atomic rename exists to close.
        """
        lock = shlex.quote(self._lock_dir(name))
        root = shlex.quote(self._root)
        hb = shlex.quote(f"{self._lock_dir(name)}/heartbeat")
        info = shlex.quote(f"{self._lock_dir(name)}/info.json")
        stale_dir = shlex.quote(f"{self._lock_dir(name)}.stale.{token}")
        age = _remote_age_expr(
            f"{self._lock_dir(name)}/heartbeat", self._lock_dir(name)
        )
        return (
            # A lock directory that cannot be created is NOT contention. Reported
            # as BUSY -- which is what a bare mkdir failure looks like -- it sends
            # an operator hunting for a competing process that does not exist.
            f"mkdir -p {root} 2>/dev/null; "
            f"if [ ! -d {root} ] || [ ! -w {root} ]; then echo NOLOCKDIR; exit 0; fi; "
            f"if mkdir {lock} 2>/dev/null; then "
            f"  printf '%s' {shlex.quote(payload)} > {info}; touch {hb}; echo ACQUIRED; "
            f"else "
            f"  AGE={age}; "
            f'  if [ "$AGE" -gt {self._stale_after} ]; then '
            f"    if mv {lock} {stale_dir} 2>/dev/null; then "
            f"      rm -f {stale_dir}/info.json {stale_dir}/heartbeat 2>/dev/null; "
            f"      rmdir {stale_dir} 2>/dev/null; "
            f"      if mkdir {lock} 2>/dev/null; then "
            f"        printf '%s' {shlex.quote(payload)} > {info}; touch {hb}; "
            f"        echo ACQUIRED_STALE; "
            f"      else echo BUSY; fi; "
            f"    else echo BUSY; fi; "
            f"  else "
            # `|| true` because a lock taken microseconds ago may not have its
            # info.json yet, and cat's failure would otherwise make the script
            # exit non-zero -- read as "the lock could not be operated" when the
            # truthful answer is the BUSY it just printed.
            f"    echo BUSY; cat {info} 2>/dev/null || true; "
            f"  fi; "
            f"fi"
        )

    # ------------------------------------------------------------------- API

    # ------------------------------------------------------- shared (pins)
    #
    # A snapshot pin is SHARED, not exclusive. The in-memory contract it mirrors
    # is ``snapshot.locks``, a SET of lock ids: any number of restores and
    # transfers may pin the same snapshot at once, and it stays pinned until the
    # last of them lets go. Two reads of one snapshot do not conflict, so making
    # the remote pin exclusive would have made a second restore of the same
    # snapshot fail against the first -- a concurrency regression the local path
    # never had.
    #
    # So each holder writes its OWN file under ``<name>.lock/holders/``. No
    # exclusion is needed or wanted; the file's mtime is that holder's heartbeat,
    # and the snapshot counts as locked while ANY holder file is fresh. What the
    # pin blocks is deletion -- see blocked_by_remote_lock -- not other readers.

    def _holders_dir(self, name: str) -> str:
        return f"{self._lock_dir(name)}/{HOLDERS_DIR_NAME}"

    def _holder_file(self, name: str, lock_id: str) -> str:
        return f"{self._holders_dir(name)}/{encode_name(str(lock_id))}"

    def acquire_shared(self, name: str, lock_id: str, operation: str = "") -> str:
        """Add this holder's pin to ``name``. Never blocks on another holder."""
        import os

        payload = json.dumps(
            {
                # The lock's REAL name, because the directory holding it carries
                # an encoded one. Identity lives here; the filename is only a
                # filesystem-safe address for it.
                "name": name,
                "lock_id": str(lock_id),
                "operation": operation or str(lock_id),
                "hostname": self._hostname or "unknown",
                "pid": os.getpid(),
                "shared": True,
            }
        )
        root = shlex.quote(self._root)
        holders = shlex.quote(self._holders_dir(name))
        holder = shlex.quote(self._holder_file(name, lock_id))
        script = (
            f"mkdir -p {holders} 2>/dev/null; "
            # Reported distinctly from contention: a bare mkdir failure looks
            # exactly like losing a race, and sends an operator hunting for a
            # competing process that does not exist.
            f"if [ ! -d {root} ] || [ ! -d {holders} ] || [ ! -w {holders} ]; then "
            f"  echo NOLOCKDIR; exit 0; fi; "
            f"printf '%s' {shlex.quote(payload)} > {holder} 2>/dev/null "
            f"&& echo ACQUIRED || echo FAILED"
        )
        rc, out, err = self._run(script)
        if "NOLOCKDIR" in out and self._run_elevated is not None:
            rc, out, err = self._run_elevated(script)
        if "NOLOCKDIR" in out:
            raise RemoteLockUnavailable(
                f"the lock directory {self._root!r} on the target could not be "
                f"created or written to. Locks live beside the backups they "
                f"protect, so this path must be writable by the account running "
                f"the backup, or that account must be able to elevate for it."
            )
        if "ACQUIRED" not in out:
            raise RemoteLockUnavailable(
                f"could not record a shared lock on {name!r}: "
                f"{err.strip() or out.strip() or f'exit {rc}'}"
            )
        return "acquired"

    def release_shared(self, name: str, lock_id: str) -> None:
        """Drop THIS holder's pin. Others keep theirs.

        The empty directories are tidied with rmdir, which fails harmlessly while
        another holder's file is still there -- so the last one out cleans up and
        nobody else can remove a pin that is still held.
        """
        self._stop_heartbeat(self._held_key(name, lock_id))
        holder = shlex.quote(self._holder_file(name, lock_id))
        holders = shlex.quote(self._holders_dir(name))
        lock = shlex.quote(self._lock_dir(name))
        rc, _out, err = self._run(
            f"rm -f {holder} 2>/dev/null; "
            f"rmdir {holders} 2>/dev/null; rmdir {lock} 2>/dev/null; exit 0"
        )
        if rc != 0:
            logger.warning(
                "Could not fully release the shared lock %r on the target: %s",
                name,
                err.strip(),
            )

    def acquire_shared_persistent(
        self, name: str, lock_id: str, operation: str = ""
    ) -> str:
        """Pin ``name`` and keep the pin alive until ``release_shared``."""
        mode = self.acquire_shared(name, lock_id, operation)
        self._start_heartbeat(
            self._held_key(name, lock_id), self._holder_file(name, lock_id)
        )
        return mode

    @staticmethod
    def _held_key(name: str, lock_id: str) -> str:
        return f"{name}\x00{lock_id}"

    def holds_shared(self, name: str, lock_id: str) -> bool:
        """Whether THIS manager already holds that pin."""
        return self._held_key(name, lock_id) in self._held

    # ---------------------------------------------------- exclusive (target)

    def acquire_once(self, name: str, operation: str) -> str:
        """Take the lock, or raise. Returns the acquisition mode."""
        import os

        token = uuid.uuid4().hex[:12]
        payload = json.dumps(
            {
                "name": name,
                "lock_id": name,
                "operation": operation,
                "hostname": self._hostname or "unknown",
                "pid": os.getpid(),
                "token": token,
            }
        )
        script = self._acquire_script(name, payload, token)
        rc, out, err = self._run(script)
        if "NOLOCKDIR" in out and self._run_elevated is not None:
            # Backup destinations are usually root-owned, so the unprivileged
            # attempt failing is the ordinary case rather than an error.
            rc, out, err = self._run_elevated(script)
        if "NOLOCKDIR" in out:
            raise RemoteLockUnavailable(
                f"the lock directory {self._root!r} on the target could not be "
                f"created or written to. Locks live beside the backups they "
                f"protect, so this path must be writable by the account running "
                f"the backup, or that account must be able to elevate for it."
            )
        if rc != 0 and "ACQUIRED" not in out:
            raise RemoteLockUnavailable(
                f"could not operate the lock directory on the target: {err.strip() or rc}"
            )
        if "ACQUIRED_STALE" in out:
            logger.warning(
                "Broke a stale lock %r on the target: its heartbeat was older than "
                "%ds, so the process holding it is gone.",
                name,
                self._stale_after,
            )
            return "stale-broken"
        if "ACQUIRED" in out:
            return "acquired"

        info = None
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    info = json.loads(line)
                except ValueError:
                    info = None
        raise RemoteLockBusy(name, info)

    def release(self, name: str) -> None:
        """Remove the lock. Named files only -- never a recursive delete."""
        self._stop_heartbeat(name)
        lock = shlex.quote(self._lock_dir(name))
        info = shlex.quote(f"{self._lock_dir(name)}/info.json")
        hb = shlex.quote(f"{self._lock_dir(name)}/heartbeat")
        # rmdir, not rm -rf: if anything unexpected is inside, this fails and
        # leaves it alone rather than deleting whatever it happens to find.
        rc, _out, err = self._run(
            f"rm -f {info} {hb} 2>/dev/null; rmdir {lock} 2>/dev/null; exit 0"
        )
        if rc != 0:
            logger.warning(
                "Could not fully release remote lock %r: %s", name, err.strip()
            )

    def is_locked(self, name: str) -> Optional[dict]:
        """The holder's details if a LIVE lock exists, else None.

        A lock whose heartbeat has gone stale reports as not held: it is a
        leftover, and treating it as live would block every future operation on
        the target until someone cleaned up by hand.
        """
        info = shlex.quote(f"{self._lock_dir(name)}/info.json")
        age = _remote_age_expr(f"{self._lock_dir(name)}/heartbeat")
        script = (
            f"if [ -d {shlex.quote(self._lock_dir(name))} ]; then "
            f'  AGE={age}; if [ "$AGE" -le {self._stale_after} ]; then '
            f"    cat {info} 2>/dev/null; fi; "
            f"fi"
        )
        _rc, out, _err = self._run(script)
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    return json.loads(line)
                except ValueError:
                    return {}
        return None

    def live_locks(self) -> dict[str, list[Holder]]:
        """Every lock on this target with a live holder, mapped to its holders.

        One round trip for the whole target: a prune asks about every snapshot it
        is about to delete, and a query each would turn a prune into a
        conversation.

        The remote emits RAW FACTS -- its own clock, each holder's mtime, each
        holder's payload -- and the staleness arithmetic happens here. Two
        reasons. Conditional logic in a script that has to run identically under
        bash, dash and busybox ash is where this project has shipped bugs before;
        and unlike acquisition, listing has no race to lose, so there is nothing
        to gain by deciding on the far side.

        The clock is still entirely the target's: both `now` and each mtime come
        from it, and no client time is ever compared against them.

        A holder whose payload cannot be parsed is still reported, with an empty
        dict. "Something holds this and we cannot say what" must never be rounded
        down to "nothing holds this".
        """
        live, _dead = self._scan_holders()
        return live

    def live_lock_names(self) -> set[str]:
        """Names of locks with at least one live holder."""
        return set(self.live_locks())

    def _scan_holders(self) -> tuple[dict[str, list[Holder]], list[tuple[str, str]]]:
        """(live locks by real name, (dir, file) of abandoned holder records).

        The remote emits NAMES, never paths. Paths are rebuilt here from the root
        this manager already knows. A path was emitted at first, and split off a
        space-delimited line -- so a target directory containing a space produced
        a truncated path, which the sweeper then passed to ``rm -f``. It deleted
        something unrelated, reported success, and left the real holder in place.
        The remote knows nothing this side cannot reconstruct, so it sends the
        two encoded names, which contain no spaces by construction.
        """
        root = shlex.quote(self._root)
        holders_dir = HOLDERS_DIR_NAME
        script = (
            f'printf "NOW %s\\n" "$(date +%s)"; '
            f"for d in {root}/*.lock; do "
            f'  [ -d "$d" ] || continue; '
            f'  n=$(basename "$d" .lock); '
            f'  if [ -d "$d/{holders_dir}" ]; then '
            f'    for h in "$d"/{holders_dir}/*; do '
            f'      [ -f "$h" ] || continue; '
            f'      m=$(stat -c %Y "$h" 2>/dev/null '
            f'|| stat -f %m "$h" 2>/dev/null || echo 0); '
            f'      printf "H %s %s %s " "$n" "$m" "$(basename "$h")"; '
            f'      cat "$h" 2>/dev/null; printf "\\n"; '
            f"    done; "
            f"  else "
            f'    m=$(stat -c %Y "$d/heartbeat" 2>/dev/null '
            f'|| stat -f %m "$d/heartbeat" 2>/dev/null '
            f'|| stat -c %Y "$d" 2>/dev/null '
            f'|| stat -f %m "$d" 2>/dev/null || echo 0); '
            f'    printf "X %s %s - "  "$n" "$m"; '
            f'    cat "$d/info.json" 2>/dev/null; printf "\\n"; '
            f"  fi; "
            f"done 2>/dev/null; exit 0"
        )
        rc, out, err = self._run(script)
        if rc != 0:
            # The script ends in `exit 0`, so a non-zero status means the shell
            # never ran it -- the host is unreachable, or the lock directory is
            # not readable. Empty output then means "could not ask", and
            # returning nothing would report that as "nothing is locked": a
            # failed check read as a clean result, which is exactly what lets a
            # prune delete the snapshot a restore is reading.
            raise RemoteLockUnavailable(
                f"could not list locks on the target: {err.strip() or f'exit {rc}'}"
            )

        now: Optional[int] = None
        live: dict[str, list[Holder]] = {}
        dead: list[tuple[str, str]] = []
        for line in out.splitlines():
            if line.startswith("NOW "):
                try:
                    now = int(line[4:].strip())
                except ValueError:
                    now = None
                continue
            if not line or line[0] not in ("H", "X") or now is None:
                continue
            parts = line.split(" ", 4)
            if len(parts) < 4:
                continue
            kind, dir_name, mtime_raw, file_name = parts[:4]
            raw = parts[4] if len(parts) > 4 else ""
            try:
                age = now - int(mtime_raw)
            except ValueError:
                continue
            if age > self._stale_after:
                if kind == "H" and age > self._stale_after * DEAD_HOLDER_MULTIPLE:
                    dead.append((dir_name, file_name))
                continue
            try:
                info = json.loads(raw.strip()) if raw.strip() else {}
            except ValueError:
                info = {}
            if not isinstance(info, dict):
                info = {}
            # Keyed by the lock's REAL name, which travels in the payload. The
            # directory name is an encoding of it and cannot be decoded back, so
            # keying by that would mean the guard -- which asks using real names
            # -- never matched a snapshot whose name had to be encoded. Where the
            # payload is unreadable the encoded name is all there is; the guard
            # checks for both, so such a lock still blocks.
            key = str(info.get("name") or decode_name_for_display(dir_name))
            live.setdefault(key, []).append(Holder(dir_name, file_name, info))
        return live, dead

    def sweep_dead_holders(self) -> int:
        """Remove holder records far past the point of any doubt.

        A pin whose holder died leaves its record behind. Listing already ignores
        it, so nothing is blocked, but it would otherwise sit there forever. Only
        records older than DEAD_HOLDER_MULTIPLE times the stale threshold are
        touched: a live holder refreshes six times inside one threshold, so this
        cannot remove a pin that is still held.
        """
        _live, dead = self._scan_holders()
        if not dead:
            return 0
        paths = [
            shlex.quote(f"{self._root}/{dir_name}.lock/{HOLDERS_DIR_NAME}/{file_name}")
            for dir_name, file_name in dead
        ]
        self._run(f"rm -f {' '.join(paths)} 2>/dev/null; exit 0")
        logger.debug("Swept %d abandoned holder record(s) on the target", len(dead))
        return len(dead)

    def release_holder(self, holder: Holder) -> None:
        """Drop a holder by the record it was found in.

        ``--unlock`` must be able to clear a holder whose payload is unreadable,
        which is precisely the one it cannot name. Addressing the record instead
        of recomputing a path from a lock id makes that possible.
        """
        base = f"{self._root}/{holder.dir_name}.lock"
        record = shlex.quote(f"{base}/{HOLDERS_DIR_NAME}/{holder.file_name}")
        holders = shlex.quote(f"{base}/{HOLDERS_DIR_NAME}")
        lock = shlex.quote(base)
        if holder.lock_id is not None:
            name = str(holder.info.get("name") or "")
            if name:
                self._stop_heartbeat(self._held_key(name, holder.lock_id))
        self._run(
            f"rm -f {record} 2>/dev/null; "
            f"rmdir {holders} 2>/dev/null; rmdir {lock} 2>/dev/null; exit 0"
        )

    # ------------------------------------------------------------- heartbeat

    def _start_heartbeat(self, key: str, path: Optional[str] = None) -> None:
        """Refresh ``path``'s mtime until stopped. ``key`` identifies the holder.

        For an exclusive lock the refreshed file is the lock's ``heartbeat``; for
        a shared pin it is that holder's own file, so holders of the same
        snapshot keep their pins alive independently.
        """
        stop = threading.Event()
        target = path if path is not None else f"{self._lock_dir(key)}/heartbeat"
        hb = shlex.quote(target)

        def beat() -> None:
            while not stop.wait(self._heartbeat_interval):
                try:
                    self._run(f"touch {hb} 2>/dev/null; exit 0")
                except Exception as exc:  # noqa: BLE001 - a missed beat is not fatal
                    # One failed refresh must not end a transfer. Several in a
                    # row let the lock go stale, which is the designed outcome
                    # for a holder that can no longer reach the target.
                    logger.debug("Heartbeat for %r failed: %s", key, exc)

        thread = threading.Thread(target=beat, name="lock-hb", daemon=True)
        self._held[key] = stop
        self._threads[key] = thread
        _register_for_cleanup(self, key)
        thread.start()

    def _stop_heartbeat(self, key: str) -> None:
        stop = self._held.pop(key, None)
        thread = self._threads.pop(key, None)
        _unregister_for_cleanup(self, key)
        if stop is not None:
            stop.set()
        if thread is not None:
            thread.join(timeout=5)

    def acquire_persistent(self, name: str, operation: str) -> str:
        """Take a lock and keep it alive until ``release``, without a with-block.

        ``set_lock`` marks a snapshot pinned at the start of an operation and
        unpins it at the end, which may be hours and several call frames apart --
        so the scoped ``hold`` does not fit. The heartbeat still runs, because a
        lock that stops being refreshed is exactly how a crashed holder stops
        blocking everyone else.
        """
        mode = self.acquire_once(name, operation)
        self._start_heartbeat(name)
        return mode

    def holds(self, name: str) -> bool:
        """Whether THIS manager is the one holding ``name``."""
        return name in self._held

    @contextmanager
    def hold(self, name: str, operation: str) -> Iterator[str]:
        """Hold the lock for the duration of the block, refreshing it throughout."""
        mode = self.acquire_once(name, operation)
        self._start_heartbeat(name)
        try:
            yield mode
        finally:
            self.release(name)


def read_persisted_locks(manager: Any) -> dict[str, dict]:
    """The lock-file shape, rebuilt from the locks recorded on the target.

    ``restore --status`` and ``--unlock`` read through ``Endpoint._read_locks``.
    Giving remote endpoints a real implementation of it is what lets those
    commands report a remote target truthfully, instead of the standing
    "this target does not persist locks" they used to print -- which was honest
    and useless: a backup tool that reports it cannot protect a restore is not
    protecting the restore.

    Every holder is listed, matching the local file's list of lock ids. A holder
    whose payload could not be parsed appears as ``unknown``: "something holds
    this and we cannot say what" must never be rounded down to "nothing".
    """
    locks: dict[str, dict] = {}
    for name, holders in manager.live_locks().items():
        if not name.startswith(SNAPSHOT_LOCK_PREFIX):
            continue
        snapshot = name[len(SNAPSHOT_LOCK_PREFIX) :]
        entry = locks.setdefault(snapshot, {})
        for holder in holders:
            lock_id = holder.lock_id
            key = "parent_locks" if str(lock_id or "").startswith("p:") else "locks"
            entry.setdefault(key, []).append(
                lock_id.removeprefix("p:") if lock_id else "unknown"
            )
    return locks


def write_persisted_locks(manager: Any, lock_dict: dict[str, Any]) -> None:
    """Reconcile the target's snapshot pins to ``lock_dict``.

    The caller (``restore --unlock``) computes the state it wants and writes it
    whole, matching the local lock-file contract. Here that means dropping every
    holder the new state no longer lists -- per holder, not per snapshot, so
    clearing one session's lock leaves another session's pin on the same
    snapshot intact.

    Holders are dropped by the record they were found in, so a holder whose
    payload cannot be parsed -- reported as ``unknown``, and therefore impossible
    to name -- can still be cleared. It could not be, when the release was
    addressed by lock id: ``--unlock all`` left it in place forever and the only
    remedy was deleting files on the target by hand.

    Locks that are not snapshot pins -- the whole-target lock a prune holds, for
    instance -- are left alone: they belong to a running operation, and --unlock
    exists to clear leftovers, not to interrupt work in progress.
    """
    for name, holders in manager.live_locks().items():
        if not name.startswith(SNAPSHOT_LOCK_PREFIX):
            continue
        snapshot = name[len(SNAPSHOT_LOCK_PREFIX) :]
        entry = lock_dict.get(snapshot) or {}
        keep = {str(x) for x in entry.get("locks", [])}
        keep |= {f"p:{x}" for x in entry.get("parent_locks", [])}
        for holder in holders:
            if holder.lock_id is None or holder.lock_id not in keep:
                manager.release_holder(holder)
    manager.sweep_dead_holders()


_MANAGER_CACHE_LOCK = threading.Lock()


def cached_manager(endpoint: Any, path_key: str, build: Callable[[], Any]) -> Any:
    """The one manager an endpoint uses for a given target path.

    Shared rather than rebuilt per call for two reasons, both of which were real
    bugs when it was not:

    * ``holds()`` is per instance. With a fresh manager each time, a second lock
      on the same snapshot -- pinned directly AND as an incremental parent --
      did not recognise this process's own lock and reported the target busy
      against itself.
    * The heartbeat thread belongs to the manager that started it. A release
      through a different instance could not stop it, leaking a thread that went
      on refreshing a lock nobody held.

    Keyed by path rather than cached outright because an endpoint's config can be
    rewritten between operations, and a manager holding a stale path would lock
    somewhere other than where the work is happening. Guarded by a lock because
    transfers run threaded, and two threads racing here would produce exactly the
    duplicate managers this exists to prevent.
    """
    with _MANAGER_CACHE_LOCK:
        cache = getattr(endpoint, "_lock_manager_cache", None)
        if cache is None:
            cache = {}
            endpoint._lock_manager_cache = cache
        manager = cache.get(path_key)
        if manager is None:
            manager = build()
            cache[path_key] = manager
        return manager


def snapshot_lock_name(snapshot: Any) -> str:
    """The identity a snapshot is locked under, on the target.

    The ONE derivation, shared by the writer (each endpoint's ``set_lock``) and
    the readers (``blocked_by_remote_lock`` and the delete guards). If the two
    ever disagreed, a lock would be written under one key and looked up under
    another -- a guard that reports "not locked" for a snapshot that is, which
    is precisely the failure this module exists to remove.

    Falls back through the shapes ``delete_snapshots`` is actually called with
    (snapshot objects, path-like objects, plain strings) so that identification
    itself never fails. Failing to IDENTIFY and failing to ASK are different:
    the latter is what fails closed.
    """
    for attr in ("get_name", "get_path"):
        getter = getattr(snapshot, attr, None)
        if callable(getter):
            try:
                value = getter()
            except Exception:  # noqa: BLE001 - fall through to the next shape
                continue
            if value:
                return (
                    PurePosixPath(str(value)).name if attr == "get_path" else str(value)
                )
    name = getattr(snapshot, "name", None)
    if name:
        return str(name)
    return PurePosixPath(str(snapshot)).name


def blocked_by_remote_lock(manager: Any, snapshots: list) -> set[str]:
    """Names of ``snapshots`` a live remote lock says must not be deleted.

    The in-memory lock set cannot answer this. A prune running in another
    process lists the target fresh, so every snapshot it sees has an empty
    in-memory lock set -- including the one a restore is reading right now.
    That is the whole defect: the guard existed, it just could not see the
    other process.

    A failure to ASK raises ``RemoteLockUnavailable`` rather than answering.
    Neither available answer would be honest: "nothing is locked" prunes on an
    unanswered question and can delete the snapshot someone is restoring, and
    "everything is locked" silently skips every deletion, which is how retention
    stops running while the operator is told the prune succeeded. The caller
    decides -- a prune turns it into the abort it already reports for a refused
    delete; a caller that can proceed without pruning catches it and says so.
    """
    names = {snapshot_lock_name(s) for s in snapshots}
    if not names:
        return set()
    try:
        live = manager.live_locks()
    except Exception as exc:  # noqa: BLE001 - see docstring: the caller decides
        raise RemoteLockUnavailable(
            f"the lock state on this target could not be read ({exc}), so it is "
            f"not known whether a restore is holding any of these snapshots"
        ) from exc
    # Matched on the real name AND on the encoded directory each lock actually
    # lives in. A lock whose payload cannot be parsed has no real name to offer,
    # and its listed key is then only a best-effort readable form -- which will
    # not equal the real name if that name needed encoding. The directory name is
    # exact in every case, so it is the one that must decide. Without it, an
    # unreadable pin on an awkwardly-named snapshot was invisible, and invisible
    # means deletable while a restore is reading it.
    directories = {holder.dir_name for holders in live.values() for holder in holders}
    return {
        n
        for n in names
        if f"{SNAPSHOT_LOCK_PREFIX}{n}" in live
        or encode_name(f"{SNAPSHOT_LOCK_PREFIX}{n}") in directories
    }
