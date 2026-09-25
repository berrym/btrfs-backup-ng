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
* **``unlink`` of one name succeeds once.** Breaking a stale lock removes the
  file that identifies the dead holder -- named by that holder's own token --
  so of any number of contenders that judged the same lock dead, exactly one
  goes on to take it, and none can remove a newer holder's record (see
  ``RemoteLockManager._acquire_script``). Release removes only the holder's
  own record, so a holder whose lock was broken cannot delete its successor's.

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

import hashlib
import json
import logging
import os
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

#: Prefix marking a lock as the right to CREATE one destination subvolume.
#: Scoped per destination rather than per target: two receives into one
#: directory under different names are safe and common, and serialising them
#: would cost throughput to prevent a clash that cannot happen.
RECEIVING_LOCK_PREFIX = "receiving-"

#: Holder files for a SHARED lock live here, one per holder.
HOLDERS_DIR_NAME = "holders"

#: An exclusive lock's holder record: ``owner.<token>``, the holder's payload,
#: its mtime refreshed by the heartbeat. Named by the holder's own token so
#: that removing it can only ever remove that holder's lock.
OWNER_PREFIX = "owner."

#: Refreshed beside the owner file, for older versions that judge a lock's age
#: by this name and would otherwise take a long-held lock for a dead one.
HEARTBEAT_NAME = "heartbeat"

#: The holder record older versions wrote. Never written now; read, shown and
#: broken the same way when a lock left by an older version is met.
LEGACY_INFO_NAME = "info.json"

#: A holder file older than this multiple of the stale threshold is not merely
#: unrefreshed, it is definitively abandoned: a live holder refreshes six times
#: within one threshold. Only then is it safe to delete someone else's file.
DEAD_HOLDER_MULTIPLE = 2


# --------------------------------------------------------------- cleanup
#
# A lock outlives the process that took it -- that is the whole point -- so an
# interrupted run must not leave its pins sitting on the target until the stale
# window expires.
#
# Every held lock and pin is registered with the process's exit cleanups
# (``btrfs_backup_ng.lifecycle``): released on normal exit, after Ctrl-C has
# unwound, and on SIGTERM or SIGHUP once the command-line entry point has
# installed its handlers -- in every case only after the child processes that
# were writing under it have been stopped. An entry is dropped from the
# registry only once its release has run, so a release interrupted part-way is
# retried at exit. The stale window remains the backstop for what none of this
# can catch: SIGKILL, a power cut, a severed network.


def _cleanup_key(manager: Any, key: str) -> tuple:
    return ("remote-lock", id(manager), key)


def _register_for_cleanup(manager: Any, key: str) -> None:
    from .. import lifecycle

    def release() -> None:
        name, _, lock_id = key.partition("\x00")
        if lock_id:
            manager.release_shared(name, lock_id)
        else:
            manager.release(name)

    lifecycle.register(_cleanup_key(manager, key), release, lifecycle.STAGE_LOCKS)


def _unregister_for_cleanup(manager: Any, key: str) -> None:
    from .. import lifecycle

    lifecycle.unregister(_cleanup_key(manager, key))


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


#: Exit statuses of ``read_only_probe_script``: the location's filesystem is
#: mounted read-only / is writable / the mount table could not be read.
READ_ONLY, WRITABLE, UNKNOWN_MOUNT = 10, 11, 12


def read_only_probe_script(path: str) -> str:
    """Shell answering, by EXIT STATUS, whether ``path`` lies on a filesystem
    mounted read-only: ``READ_ONLY``, ``WRITABLE`` or ``UNKNOWN_MOUNT`` (3 when
    ``path`` is not a directory). Read-only means BOTH that a directory cannot
    be created there and that the mount table says so; either alone is not
    enough (a firmlinked path reads as its read-only parent in the table, a
    permission refusal is not a read-only mount).

    The one rule every pin writer decides read-only by, wherever the location
    is. A location mounted read-only cannot have anything deleted from it, so
    a pin there protects against nothing and is not needed; a location that
    merely refuses the write (no permission) is a different thing, and the
    pin is required. The two are told apart from the mount table -- the
    ``ro`` option token in ``/proc/self/mounts`` on Linux, the ``read-only``
    token in ``mount(8)``'s option list elsewhere -- never from a tool's
    message, which is localised. Anything the script cannot classify is
    ``UNKNOWN_MOUNT``, which the caller treats as NOT read-only, so an
    unclassifiable location still gets the refusal and its opt-out.
    ``BBNG_MOUNT_TABLE`` names another mount table file (the tests use it).
    """
    q = shlex.quote(path)
    # The matching runs in awk, present on every host this reaches (GNU,
    # BSD, busybox): one program over "MNT OPTS" lines picks the longest mount
    # point that is the real path or an ancestor of it. No shell ``case`` and
    # no unbalanced parenthesis anywhere inside a command substitution --
    # bash 3.2, macOS's /bin/sh, cannot parse those.
    pick = (
        r"""awk -v real="$real" '{ m = $1; gsub(/\\040/, " ", m); """
        r"""if (m == "/" || m == real || index(real, m "/") == 1) """
        r"""{ if (length(m) >= length(b)) { b = m; o = $2 } } } """
        r"""END { printf "%s", o }'"""
    )
    # mount(8) prints "dev on MNT (opt, opt, ...)" (macOS, the BSDs) or
    # "dev on MNT type T (opt,opt)" (Linux): reduce each line to "MNT OPTS".
    convert = (
        r"""awk '{ s = $0; i = index(s, " on "); if (i == 0) next; s = substr(s, i + 4); """
        r"""j = index(s, " ("); if (j == 0) next; o = substr(s, j + 2); s = substr(s, 1, j - 1); """
        r"""k = index(s, " type "); if (k > 0) s = substr(s, 1, k - 1); """
        r"""o = substr(o, 1, length(o) - 1); gsub(/ /, "", o); print s, o }'"""
    )
    return (
        f'p={q}; [ -d "$p" ] || exit 3; '
        'real=$(cd "$p" 2>/dev/null && pwd -P) || exit 3; '
        # A write that succeeds settles it: not read-only, whatever the table
        # says (macOS reaches a writable data volume through firmlinks from
        # its sealed, read-only root, so the table alone would call /tmp
        # read-only). Only a write that FAILS is classified by the table.
        'probe="$real/.btrfs-backup-ng.probe.$$"; '
        f'if mkdir "$probe" 2>/dev/null; then rmdir "$probe" 2>/dev/null; exit {WRITABLE}; fi; '
        "table=${BBNG_MOUNT_TABLE:-/proc/self/mounts}; "
        'if [ -r "$table" ]; then '
        # /proc/self/mounts: "dev MNT fstype OPTS freq passno"
        "bestopts=$(awk '{ print $2, $4 }' \"$table\" | " + pick + "); "
        f"else bestopts=$(mount 2>/dev/null | {convert} | {pick}) || exit {UNKNOWN_MOUNT}; fi; "
        f'[ -n "$bestopts" ] || exit {UNKNOWN_MOUNT}; '
        """if printf '%s' ",$bestopts," | grep -qF -e ',ro,' -e ',read-only,'; """
        f"then exit {READ_ONLY}; fi; exit {WRITABLE}"
    )


def local_path_is_read_only(path: str | os.PathLike) -> bool:
    """The same rule for a path on THIS machine, asked of the kernel directly:
    ``statvfs`` reports the read-only mount flag."""
    try:
        return bool(os.statvfs(path).f_flag & os.ST_RDONLY)
    except OSError:
        return False


def read_only_notice(name: str, where: str, location: str) -> None:
    """What every pin writer says when it skips a pin on a read-only location."""
    logger.info(
        "%s is mounted read-only, so %s is not pinned on this %s: nothing can "
        "delete from a read-only location while it is being read.",
        location,
        name,
        where,
    )


#: Shell defining the functions every lock script reads times with.
#:
#: ``bbng_digits VALUE`` succeeds only for a non-empty string of ASCII digits.
#: It walks the value one character at a time rather than matching a negated
#: bracket such as ``*[!0-9]*``: these scripts reach a remote target as
#: ``sh -c '<script>'`` on a command line the account's LOGIN shell parses
#: first, and csh and tcsh read a ``!`` followed by anything but a blank, ``=``
#: or ``(`` as a history reference even inside single quotes. The whole command
#: then failed with "Event not found." before ``sh`` ever ran. No remote script
#: may contain such a ``!`` (see ``csh_unsafe``).
#:
#: ``bbng_mtime FILE`` prints the file's mtime as a bare integer, or nothing.
#: Which ``stat`` this host has is decided ONCE, from a path that always
#: exists: GNU takes ``-c %Y``, BSD (macOS) ``-f %m``. The two must not be
#: chained as fallbacks per file: on GNU, ``stat -f`` means "file system
#: status", so ``stat -f %m FILE`` prints a multi-line block whenever FILE
#: exists -- and a heartbeat that appeared between the failed ``-c`` call and
#: the ``-f`` one handed that block to ``$(( ))``, which died with an
#: arithmetic syntax error under contention. Whatever ``stat`` prints is then
#: checked to be digits before it is printed at all.
#:
#: ``bbng_now`` prints the target's clock as a bare integer, or nothing.
#:
#: ``bbng_age FILE [FALLBACK]`` prints the age in seconds of FILE by the
#: target's own clock, of FALLBACK when FILE has no readable mtime, or nothing
#: when neither can be read or the clock cannot. Callers treat nothing as
#: "cannot judge", never as an age of zero or of forever.
MTIME_FUNCTIONS = (
    "if stat -c %Y / >/dev/null 2>&1; then bbng_st='-c %Y'; else bbng_st='-f %m'; fi; "
    'bbng_digits() { bbng_t=$1; [ -n "$bbng_t" ] || return 1; '
    'while [ -n "$bbng_t" ]; do case $bbng_t in [0123456789]*) bbng_t=${bbng_t#?};; '
    "*) return 1;; esac; done; return 0; }; "
    'bbng_mtime() { bbng_m=$(stat $bbng_st "$1" 2>/dev/null) || bbng_m=; '
    'bbng_digits "$bbng_m" || bbng_m=; '
    'printf "%s" "$bbng_m"; }; '
    "bbng_now() { bbng_c=$(date +%s 2>/dev/null) || bbng_c=; "
    'bbng_digits "$bbng_c" || bbng_c=; printf "%s" "$bbng_c"; }; '
    'bbng_age() { bbng_a=$(bbng_mtime "$1"); '
    'if [ -z "$bbng_a" ] && [ -n "$2" ]; then bbng_a=$(bbng_mtime "$2"); fi; '
    "bbng_n=$(bbng_now); "
    'if [ -n "$bbng_a" ] && [ -n "$bbng_n" ]; then echo $(( bbng_n - bbng_a )); fi; }; '
)


def csh_unsafe(script: str) -> list[str]:
    """The fragments of ``script`` a csh or tcsh login shell would misread.

    Every remote command is parsed by the account's login shell before ``sh``
    sees it. csh and tcsh begin a history substitution at any ``!`` that is not
    followed by a blank, ``=`` or ``(`` -- inside single quotes too -- and
    reject a quoted string that spans a line. Either fails the whole command
    before it runs. Empty means the script is safe to send.
    """
    bad = [
        script[i : i + 2]
        for i, c in enumerate(script)
        if c == "!" and script[i + 1 : i + 2] not in (" ", "\t", "=", "(")
    ]
    if "\n" in script:
        bad.append("\\n")
    return bad


def csh_safe_text(value: str) -> str:
    """``value`` (JSON) with every ``!`` written as the JSON escape ``\\u0021``,
    so a payload carrying a user-chosen name survives a csh login shell."""
    return value.replace("!", "\\u0021")


def _verdict(out: str) -> str:
    """The first line a lock script printed: its verdict. What follows it is
    detail (a holder's record), which must never be mistaken for one."""
    for line in out.splitlines():
        if line.strip():
            return line.strip()
    return ""


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
        self._target = str(target_path).rstrip("/") or "/"
        self._root = f"{self._target}/{LOCK_DIR_NAME}"
        self._heartbeat_interval = heartbeat_interval
        self._stale_after = stale_after
        self._hostname = hostname
        self._run_elevated = run_elevated
        self._held: dict[str, threading.Event] = {}
        self._threads: dict[str, threading.Thread] = {}
        # The owner token of each exclusive lock this manager holds. The lock's
        # owner file is named by it, and release removes only that file.
        self._tokens: dict[str, str] = {}
        self._state_lock = threading.RLock()

    # ---------------------------------------------------------------- helpers

    def _lock_dir(self, name: str) -> str:
        return f"{self._root}/{encode_name(name)}.lock"

    @property
    def location(self) -> str:
        return self._target

    def location_is_read_only(self) -> bool:
        """Whether this manager's location lies on a read-only filesystem, by
        ``read_only_probe_script`` run where the location is. Only the
        READ_ONLY exit status says yes; a probe that fails to run says no."""
        try:
            rc, _out, _err = self._run(read_only_probe_script(self._target))
        except Exception as exc:  # noqa: BLE001 - an unanswered probe is "not read-only"
            logger.debug("Read-only probe of %s did not run: %s", self._target, exc)
            return False
        return rc == READ_ONLY

    def _owner_file(self, name: str, token: str) -> str:
        return f"{self._lock_dir(name)}/{OWNER_PREFIX}{token}"

    def _exclusive_functions(self, name: str) -> str:
        """Shell functions judging one exclusive lock (see ``_acquire_script``).

        ``bbng_judge`` sets ``bbng_key`` to the entry that identifies THIS
        instance of the lock -- the one a breaker must remove -- and ``AGE`` to
        the lock's age, or leaves ``AGE`` empty when it cannot be read.
        ``bbng_show`` prints the holder's recorded details.
        """
        lock = shlex.quote(self._lock_dir(name))
        info = shlex.quote(f"{self._lock_dir(name)}/{LEGACY_INFO_NAME}")
        hb = shlex.quote(f"{self._lock_dir(name)}/{HEARTBEAT_NAME}")
        return (
            MTIME_FUNCTIONS + "bbng_judge() { bbng_key=; AGE=; "
            f'for bbng_f in {lock}/{OWNER_PREFIX}*; do [ -f "$bbng_f" ] && bbng_key=$bbng_f; done; '
            'if [ -n "$bbng_key" ]; then AGE=$(bbng_age "$bbng_key"); '
            f"elif [ -f {info} ]; then bbng_key={info}; AGE=$(bbng_age {hb} {lock}); "
            f"elif [ -f {hb} ]; then bbng_key={hb}; AGE=$(bbng_age {hb}); "
            f"else AGE=$(bbng_age {lock}); fi; return 0; }}; "
            f"bbng_show() {{ for bbng_f in {lock}/{OWNER_PREFIX}*; do "
            '[ -f "$bbng_f" ] && cat "$bbng_f" 2>/dev/null && echo; done; '
            f"[ -f {info} ] && cat {info} 2>/dev/null; return 0; }}; "
        )

    def _acquire_script(self, name: str, payload: str, token: str) -> str:
        """One round trip: try, judge staleness, break if dead, try again.

        Written as a single script deliberately: split across calls, another
        contender can slip between the staleness check and the break.

        The lock is a directory; ``mkdir`` decides who creates it. Inside it,
        the holder's record is a file named by the holder's own random token,
        ``owner.<token>``, whose mtime the holder's heartbeat refreshes. (A
        ``heartbeat`` file is refreshed beside it, for older versions that
        judge a lock by that name.)

        Breaking a stale lock is where two winners used to come from. The old
        break renamed the lock directory away, but the directory it renamed
        was whatever sat at that path by then: a contender that judged the
        dead lock and was descheduled before its ``mv`` could, once another
        had broken the lock and taken it afresh, rename the NEW holder's lock
        and take it too. The break is now made by removing the one entry that
        identifies the instance judged stale -- its ``owner.<token>`` file.
        That name exists only in that instance, and ``unlink`` of one name
        succeeds for exactly one caller: every other contender that judged the
        same dead lock gets "no such file" and reports BUSY, and one that
        judged an old instance can never remove a new one's record. Only the
        winner goes on to ``rmdir`` and ``mkdir``.

        Locks without an owner file are judged by what they hold: an older
        version's lock by its ``info.json`` (removed the same way; this
        version never writes one), a directory left mid-release by its
        ``heartbeat``, an empty directory by its own mtime and ``rmdir``,
        which succeeds only while it is empty.

        A new holder then checks that its owner file is the only one in the
        directory before it reports the lock as taken. The one interleaving
        the removal cannot exclude -- an empty directory, judged dead, removed
        by a slow contender just after another had created a new one and
        before it wrote its record -- ends with the second record written into
        a directory someone else created, or not written at all; either way
        the check sees it, and the later claimant withdraws. Two holders
        would each have to see only their own file, and whichever wrote
        second cannot.

        What remains is the lease assumption every such lock rests on: a
        holder that stops refreshing for longer than the stale threshold has
        lost the lock, even if it wakes later still believing it holds it.
        """
        lock = shlex.quote(self._lock_dir(name))
        root = shlex.quote(self._root)
        hb = shlex.quote(f"{self._lock_dir(name)}/{HEARTBEAT_NAME}")
        info = shlex.quote(f"{self._lock_dir(name)}/{LEGACY_INFO_NAME}")
        owner = shlex.quote(self._owner_file(name, token))
        target = shlex.quote(self._target)
        body = shlex.quote(csh_safe_text(payload))
        return (
            self._exclusive_functions(name) + "bbng_take() { "
            f"if printf '%s' {body} > {owner} 2>/dev/null; then "
            "bbng_c=0; "
            f'for bbng_f in {lock}/{OWNER_PREFIX}*; do [ -f "$bbng_f" ] && bbng_c=$((bbng_c + 1)); done; '
            f'if [ "$bbng_c" -eq 1 ]; then touch {hb} 2>/dev/null; echo "$1"; return 0; fi; '
            f"rm -f {owner} 2>/dev/null; rmdir {lock} 2>/dev/null; "
            "fi; echo BUSY; return 0; }; "
            # A lock directory that cannot be created is NOT contention. Reported
            # as BUSY -- which is what a bare mkdir failure looks like -- it sends
            # an operator hunting for a competing process that does not exist.
            # The lock tree is created only BELOW an existing target: a bare
            # `mkdir -p` on the full path would invent a missing target (an
            # unmounted destination rebuilt on the root filesystem by a lock
            # acquisition). A missing target therefore falls through to the
            # NOLOCKDIR verdict below, which callers report distinctly.
            f"if [ -d {target} ]; then mkdir -p {root} 2>/dev/null; fi; "
            f"if [ ! -d {root} ] || [ ! -w {root} ]; then echo NOLOCKDIR; exit 0; fi; "
            f"if mkdir {lock} 2>/dev/null; then bbng_take ACQUIRED; exit 0; fi; "
            "bbng_judge; "
            # No readable age: a lock that cannot be judged is never broken.
            f'if [ -z "$AGE" ]; then '
            f"  if [ -d {lock} ]; then echo BUSY; bbng_show; exit 0; fi; "
            f"  if mkdir {lock} 2>/dev/null; then bbng_take ACQUIRED; exit 0; fi; "
            "  echo BUSY; exit 0; "
            "fi; "
            f'if [ "$AGE" -le {self._stale_after} ]; then echo BUSY; bbng_show; exit 0; fi; '
            # Stale. Remove the entry identifying the instance that was judged
            # (see the docstring); only one contender can.
            'if [ -n "$bbng_key" ]; then '
            '  if rm "$bbng_key" </dev/null 2>/dev/null; then :; else echo BUSY; exit 0; fi; '
            f"  rm -f {hb} {info} 2>/dev/null; "
            "fi; "
            f"if rmdir {lock} 2>/dev/null; then :; else echo BUSY; exit 0; fi; "
            f"if mkdir {lock} 2>/dev/null; then bbng_take ACQUIRED_STALE; exit 0; fi; "
            "echo BUSY"
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
        target = shlex.quote(self._target)
        holders = shlex.quote(self._holders_dir(name))
        holder = shlex.quote(self._holder_file(name, lock_id))
        script = (
            # Same rule as acquisition: create only below an existing target,
            # never the target itself (mkdir -p would build every component).
            f"if [ -d {target} ]; then mkdir -p {holders} 2>/dev/null; fi; "
            # Reported distinctly from contention: a bare mkdir failure looks
            # exactly like losing a race, and sends an operator hunting for a
            # competing process that does not exist.
            f"if [ ! -d {root} ] || [ ! -d {holders} ] || [ ! -w {holders} ]; then "
            f"  echo NOLOCKDIR; exit 0; fi; "
            f"printf '%s' {shlex.quote(csh_safe_text(payload))} > {holder} 2>/dev/null "
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
        if _verdict(out) != "ACQUIRED":
            raise RemoteLockUnavailable(
                f"could not record a shared lock on {name!r}: "
                f"{err.strip() or out.strip() or f'exit {rc}'}"
            )
        _register_for_cleanup(self, self._held_key(name, lock_id))
        return "acquired"

    def release_shared(self, name: str, lock_id: str) -> None:
        """Drop THIS holder's pin. Others keep theirs.

        The empty directories are tidied with rmdir, which fails harmlessly while
        another holder's file is still there -- so the last one out cleans up and
        nobody else can remove a pin that is still held.
        """
        key = self._held_key(name, lock_id)
        self._stop_heartbeat(key)
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
            return
        _unregister_for_cleanup(self, key)

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
        token = uuid.uuid4().hex
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
        if _verdict(out) == "NOLOCKDIR" and self._run_elevated is not None:
            # Backup destinations are usually root-owned, so the unprivileged
            # attempt failing is the ordinary case rather than an error.
            rc, out, err = self._run_elevated(script)
        verdict = _verdict(out)
        if verdict == "NOLOCKDIR":
            raise RemoteLockUnavailable(
                f"the lock directory {self._root!r} on the target could not be "
                f"created or written to. Locks live beside the backups they "
                f"protect, so this path must be writable by the account running "
                f"the backup, or that account must be able to elevate for it."
            )
        if verdict not in ("ACQUIRED", "ACQUIRED_STALE", "BUSY"):
            raise RemoteLockUnavailable(
                f"could not operate the lock directory on the target: "
                f"{err.strip() or out.strip() or f'exit {rc}'}"
            )
        if verdict.startswith("ACQUIRED"):
            with self._state_lock:
                self._tokens[name] = token
            # Registered at once, not when the heartbeat starts: a lock taken
            # and never refreshed is still released at exit.
            _register_for_cleanup(self, name)
        if verdict == "ACQUIRED_STALE":
            logger.warning(
                "Broke a stale lock %r on the target: its heartbeat was older than "
                "%ds, so the process holding it is gone.",
                name,
                self._stale_after,
            )
            return "stale-broken"
        if verdict == "ACQUIRED":
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
        """Remove the lock -- only if it is still this holder's.

        The holder removes its own ``owner.<token>`` file and nothing else
        unless that succeeded. A holder that was stalled past the stale
        threshold, whose lock was broken and taken by another process, used to
        delete the NEW holder's lock on waking; a third process could then take
        it while the second still believed it held it. Now it finds its own
        record gone, leaves the lock alone, and says so.

        Named files only, never a recursive delete; ``rmdir`` fails harmlessly
        if anything unexpected is inside.
        """
        self._stop_heartbeat(name)
        with self._state_lock:
            token = self._tokens.get(name)
            if token is None:
                # Already released (the exit drain and the operation's own
                # unwind can both get here), or never taken by this manager.
                _unregister_for_cleanup(self, name)
                return
            lock = shlex.quote(self._lock_dir(name))
            owner = shlex.quote(self._owner_file(name, token))
            hb = shlex.quote(f"{self._lock_dir(name)}/{HEARTBEAT_NAME}")
            rc, out, err = self._run(
                f"if rm {owner} </dev/null 2>/dev/null; then "
                f"rm -f {hb} 2>/dev/null; rmdir {lock} 2>/dev/null; echo RELEASED; "
                f"elif [ -e {owner} ]; then echo KEPT; else echo NOTOWNER; fi"
            )
            verdict = _verdict(out)
            if verdict in ("RELEASED", "NOTOWNER"):
                self._tokens.pop(name, None)
                _unregister_for_cleanup(self, name)
            if verdict == "NOTOWNER":
                logger.warning(
                    "The lock %r on the target was no longer held by this process "
                    "when it came to release it: its record was gone -- broken by "
                    "another process after going unrefreshed for longer than %ds, "
                    "or removed by hand. Whatever holds that lock now was left "
                    "alone.",
                    name,
                    self._stale_after,
                )
            elif verdict != "RELEASED":
                logger.warning(
                    "Could not release remote lock %r: %s",
                    name,
                    err.strip() or out.strip() or f"exit {rc}",
                )

    def is_locked(self, name: str) -> Optional[dict]:
        """The holder's details if a live lock exists, else None.

        Raises ``RemoteLockUnavailable`` when the question could not be
        answered: "not locked" is an answer, and a failed check must never
        read as one. A lock whose age cannot be read is reported as held (its
        details, or an empty dict). A lock whose heartbeat has gone stale
        reports as not held: it is a leftover, and treating it as live would
        block every future operation on the target until someone cleaned up by
        hand.
        """
        lock = shlex.quote(self._lock_dir(name))
        script = (
            self._exclusive_functions(name) + f"if [ -d {lock} ]; then "
            'bbng_judge; printf "AGE %s\\n" "${AGE:-?}"; bbng_show; '
            f"elif [ -e {lock} ]; then echo UNKNOWN; else echo ABSENT; fi"
        )
        rc, out, err = self._run(script)
        verdict = _verdict(out)
        if rc != 0 or not (verdict == "ABSENT" or verdict.startswith("AGE ")):
            raise RemoteLockUnavailable(
                f"could not read the lock {name!r} on the target: "
                f"{err.strip() or out.strip() or f'exit {rc}'}"
            )
        if verdict == "ABSENT":
            return None
        age = verdict[len("AGE ") :].strip()
        if age.isdigit() and int(age) > self._stale_after:
            return None
        if not age.isdigit():
            logger.warning(
                "The age of the lock %r on the target could not be read, so it "
                "is treated as held.",
                name,
            )
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    info = json.loads(line)
                except ValueError:
                    return {}
                return info if isinstance(info, dict) else {}
        return {}

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
            MTIME_FUNCTIONS + 'printf "NOW %s\\n" "$(bbng_now)"; '
            # A lock root that exists but cannot be listed is not an empty one:
            # the glob below would simply not expand, and "no pins" would be
            # reported for a target this account cannot see into.
            f"if [ -d {root} ] && {{ [ ! -r {root} ] || [ ! -x {root} ]; }}; then "
            "echo UNLISTABLE; exit 0; fi; "
            f"for d in {root}/*.lock; do "
            f'  [ -d "$d" ] || continue; '
            f'  n=$(basename "$d" .lock); '
            f'  if [ -d "$d/{holders_dir}" ]; then '
            f'    if [ ! -r "$d/{holders_dir}" ] || [ ! -x "$d/{holders_dir}" ]; then '
            f'      printf "U %s\\n" "$n"; continue; fi; '
            f'    for h in "$d"/{holders_dir}/*; do '
            f'      [ -f "$h" ] || continue; '
            f'      m=$(bbng_mtime "$h"); '
            f'      printf "H %s %s %s " "$n" "${{m:-?}}" "$(basename "$h")"; '
            f'      cat "$h" 2>/dev/null; printf "\\n"; '
            f"    done; "
            f"  else "
            f'    k=; for f in "$d"/{OWNER_PREFIX}*; do [ -f "$f" ] && k=$f; done; '
            f'    if [ -n "$k" ]; then m=$(bbng_mtime "$k"); '
            f'    else k="$d/{LEGACY_INFO_NAME}"; m=$(bbng_mtime "$d/{HEARTBEAT_NAME}"); '
            f'      [ -n "$m" ] || m=$(bbng_mtime "$d"); fi; '
            f'    printf "X %s %s - " "$n" "${{m:-?}}"; '
            f'    cat "$k" 2>/dev/null; printf "\\n"; '
            f"  fi; "
            f"done 2>/dev/null; exit 0"
        )
        rc, out, err = self._run(script)
        if rc == 0 and "UNLISTABLE" in out.splitlines():
            if self._run_elevated is not None:
                rc, out, err = self._run_elevated(script)
            if rc == 0 and "UNLISTABLE" in out.splitlines():
                raise RemoteLockUnavailable(
                    f"the lock directory {self._root!r} on the target exists but "
                    f"cannot be listed by the account running this, so whether "
                    f"anything is pinned there is not known"
                )
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
        clock_read = False
        live: dict[str, list[Holder]] = {}
        dead: list[tuple[str, str]] = []
        unjudged: list[str] = []
        for line in out.splitlines():
            if line.startswith("NOW"):
                clock_read = True
                value = line[3:].strip()
                now = int(value) if value.isdigit() else None
                continue
            if line.startswith("U "):
                # A holders directory that cannot be listed: something may
                # hold this lock and nothing here can say what. Held.
                dir_name = line[2:].strip()
                live.setdefault(decode_name_for_display(dir_name), []).append(
                    Holder(dir_name, "", {})
                )
                unjudged.append(decode_name_for_display(dir_name))
                continue
            if not line or line[0] not in ("H", "X"):
                continue
            parts = line.split(" ", 4)
            if len(parts) < 4:
                continue
            kind, dir_name, mtime_raw, file_name = parts[:4]
            raw = parts[4] if len(parts) > 4 else ""
            # A holder whose age cannot be read -- no readable mtime, or no
            # readable clock on the target -- is HELD. Counting it as dead
            # (which an mtime read as 0 used to do) made the guard report a
            # live pin as absent, and `restore --unlock` swept it.
            age = (
                now - int(mtime_raw)
                if now is not None and mtime_raw.isdigit()
                else None
            )
            if age is not None and age > self._stale_after:
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
            if age is None:
                unjudged.append(key)
        if not clock_read:
            # Every run of the script prints the NOW line first; output without
            # it is not a listing, and an empty result would read as "nothing
            # is locked".
            raise RemoteLockUnavailable(
                f"could not list locks on the target: unexpected output "
                f"{(out.strip() or err.strip() or 'none')[:200]!r}"
            )
        if unjudged:
            logger.warning(
                "The age of %d lock record(s) on the target could not be read%s, "
                "so they are treated as held: %s",
                len(unjudged),
                "" if now is not None else " (the target's clock could not be read)",
                ", ".join(sorted(set(unjudged))),
            )
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
        of recomputing a path from a lock id makes that possible. A holder
        found in a directory that could not be listed has no record to
        address, and is left alone.
        """
        if not holder.file_name:
            logger.warning(
                "The holders of %r on the target could not be listed, so none of "
                "them can be cleared from here.",
                decode_name_for_display(holder.dir_name),
            )
            return
        base = f"{self._root}/{holder.dir_name}.lock"
        record = shlex.quote(f"{base}/{HOLDERS_DIR_NAME}/{holder.file_name}")
        holders = shlex.quote(f"{base}/{HOLDERS_DIR_NAME}")
        lock = shlex.quote(base)
        own_key = None
        if holder.lock_id is not None:
            name = str(holder.info.get("name") or "")
            if name:
                own_key = self._held_key(name, holder.lock_id)
                self._stop_heartbeat(own_key)
        rc, _out, _err = self._run(
            f"rm -f {record} 2>/dev/null; "
            f"rmdir {holders} 2>/dev/null; rmdir {lock} 2>/dev/null; exit 0"
        )
        if rc == 0 and own_key is not None:
            _unregister_for_cleanup(self, own_key)

    # ------------------------------------------------------------- heartbeat

    def _start_heartbeat(self, key: str, path: Optional[str] = None) -> None:
        """Refresh the holder's record until stopped. ``key`` identifies it.

        For a shared pin ``path`` is that holder's own file, so holders of the
        same snapshot keep their pins alive independently. For an exclusive
        lock (no ``path``) it is the holder's ``owner.<token>`` file and the
        ``heartbeat`` beside it, refreshed with ``touch -c``: a holder whose
        lock was broken while it was stalled must not recreate its record
        inside the next holder's lock.
        """
        stop = threading.Event()
        if path is not None:
            command = f"touch {shlex.quote(path)} 2>/dev/null; exit 0"
        else:
            with self._state_lock:
                token = self._tokens.get(key, "")
            files = [f"{self._lock_dir(key)}/{HEARTBEAT_NAME}"]
            if token:
                files.insert(0, self._owner_file(key, token))
            command = f"touch -c {' '.join(shlex.quote(f) for f in files)} 2>/dev/null; exit 0"

        def beat() -> None:
            while not stop.wait(self._heartbeat_interval):
                try:
                    self._run(command)
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
        """Stop refreshing. The exit-cleanup entry stays until the release
        itself has run, so a release that does not complete is retried."""
        stop = self._held.pop(key, None)
        thread = self._threads.pop(key, None)
        if stop is not None:
            stop.set()
        if thread is not None and thread is not threading.current_thread():
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
        """Hold the lock for the duration of the block, refreshing it throughout.

        Processes the block starts run inside it: if the block fails or is
        interrupted, they are stopped and waited for before the lock is
        released (see ``btrfs_backup_ng.lifecycle``).
        """
        from .. import lifecycle

        mode = self.acquire_once(name, operation)
        self._start_heartbeat(name)
        try:
            with lifecycle.process_scope():
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


def record_pin(
    manager: Any,
    snapshot: Any,
    lock_id: Any,
    lock_state: bool,
    *,
    parent: bool = False,
    skip_remote_lock: bool = False,
    where: str = "destination",
    noun: str = "snapshot",
    opt_out: str = "pass --skip-remote-lock",
) -> None:
    """Write or drop THIS holder's pin on ``snapshot`` in the target's lock store.

    ``manager`` is a ``RemoteLockManager`` or a zero-argument callable that
    builds one.

    The one body behind every endpoint's persistent ``set_lock``: ssh://,
    raw+ssh://, and a local endpoint over a location that carries the
    directory store. Each endpoint keeps its own in-memory lock set (the
    transfer and prune logic in this run reads that directly) and then calls
    this for the durable record. Three copies of this logic had drifted only
    in their wording; the wording is now a parameter.

    SHARED, not exclusive: the in-memory contract is a SET of lock ids, so any
    number of restores and transfers may pin one snapshot at once and it stays
    pinned until the last lets go. Each holder writes and removes only its own
    file, which is why releasing here cannot drop somebody else's pin -- and
    why a parent pin is keyed apart from a direct one.

    A pin that could not be written must never read as one that was. Continuing
    with a warning would leave the operation running unprotected while a prune
    on this target sees nothing holding the snapshot and is free to delete it
    mid-read -- the exact failure the pin exists to prevent, with a log line in
    place of the protection. So it stops, and says what to grant.
    ``skip_remote_lock`` is the operator overriding that, for a target they can
    read but not write. It relaxes only the abort: the pin is still consulted
    everywhere it is read, so nothing starts reporting a target as unlocked
    without having looked. A release failing is not the same risk: the
    heartbeat stops, the pin goes stale, and it is swept.
    """
    from .. import __util__

    name = f"{SNAPSHOT_LOCK_PREFIX}{snapshot_lock_name(snapshot)}"
    holder_id = f"p:{lock_id}" if parent else str(lock_id)
    try:
        # ``manager`` may be a zero-argument factory (an endpoint's bound
        # ``_lock_manager``), resolved here so a target whose lock directory
        # cannot be set up is reported through the same refusal as a pin that
        # cannot be written, instead of escaping as a bare exception.
        if callable(manager):
            manager = manager()
        if lock_state:
            if not manager.holds_shared(name, holder_id):
                manager.acquire_shared_persistent(name, holder_id, str(lock_id))
        else:
            manager.release_shared(name, holder_id)
    except Exception as exc:  # noqa: BLE001 - reported, never silently passed
        if lock_state and _read_only(manager):
            read_only_notice(
                snapshot_lock_name(snapshot),
                where,
                getattr(manager, "location", where),
            )
            return
        if lock_state and not skip_remote_lock:
            raise __util__.AbortError(
                f"Could not lock {snapshot_lock_name(snapshot)} on this "
                f"{where}: {reason_of(exc)}. Refusing to continue unprotected: another "
                f"process pruning this {where} would not see the {noun} as in "
                f"use and could delete it while it is being read. Make the "
                f"{where} writable by the account running this, allow that "
                f"account to elevate for it, or {opt_out} to "
                f"proceed unprotected on purpose."
            ) from exc
        if lock_state:
            logger.warning(
                "Could not record the lock for %s on this %s (%s), and "
                "--skip-remote-lock was given, so this continues WITHOUT "
                "protection: a prune elsewhere will not see it as in use.",
                snapshot_lock_name(snapshot),
                where,
                exc,
            )
        else:
            logger.warning(
                "Could not clear the lock for %s on this %s (%s). It will "
                "expire on its own once its heartbeat stops.",
                snapshot_lock_name(snapshot),
                where,
                exc,
            )


def _read_only(manager: Any) -> bool:
    probe = getattr(manager, "location_is_read_only", None)
    return callable(probe) and bool(probe())


def reason_of(exc: BaseException) -> str:
    """An exception's message as a clause: without its own final period, so
    a sentence built around it does not end in two."""
    return str(exc).rstrip(". ")


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
