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

Nothing is written through a link
---------------------------------
The store sits at a predictable path beside the backups, and the account
writing it is often root. A symbolic link planted at that path, at a lock
directory under it, or at a holder's record once made ``mkdir -p`` build the
lock tree wherever the link pointed and ``>`` truncate whatever file it
pointed at. Every creating operation now refuses a link at every component
and refuses a store its group or everyone can write: from Python with
descriptors on this machine (``LocalLockManager``), where the kernel does the
refusing and no window exists, and in the shell on a remote
(``STORE_FUNCTIONS``), where the refusal is a check followed by a use and the
swap between the two is what remains.
"""

from __future__ import annotations

import errno
import hashlib
import json
import logging
import os
import random
import shlex
import stat
import threading
import time
import uuid
from contextlib import contextmanager, suppress
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

#: The directory under a LOCAL btrfs target that its receive locks live in.
#: Deliberately not ``LOCK_DIR_NAME``: a local endpoint recognises that
#: directory as the location's PIN store and, finding it, reads its pins from
#: there instead of from the lock file -- so creating it for a receive lock
#: would hide every pin an earlier run recorded in the file. The receive
#: locks get a store of their own and the pin store is left exactly as it
#: was. ssh:// and raw locations have no lock-file mode to protect, so their
#: receive locks share the one store their pins already live in.
RECEIVING_DIR_NAME = ".btrfs-backup-ng.receiving"

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

#: How many times a SHARED acquire re-runs its record script before reporting the
#: store unavailable. A concurrent ``release_shared`` can ``rmdir`` the lock and
#: holders directories between this script creating them and writing the record
#: into them, so a single NOLOCKDIR/FAILED verdict can be a lost race rather than
#: an unwritable store; the exclusive acquire already recovers from the same race
#: by re-``mkdir``-ing within its own script.
#:
#: The bound sits well above what contention needs. Measured on x86_64 over
#: 3 threads x 300 acquire/release rounds (900 acquires): idle, almost every
#: acquire wins on the first attempt and none needs more than 2; with three test
#: suites running at once, 3-8 acquires per 900 need a 4th attempt and the worst
#: observed was 4. A slow builder -- the ppc64le Koji VMs where ``btrfs-backup-ng``
#: 0.9.19 failed this with the bound at 3 -- widens the release-vs-acquire window
#: per ``sh`` invocation, so the margin has to be generous. 20 is that margin (it
#: is also the value Fedora carries as a 0.9.19 downstream patch). Each retry is
#: one cheap shell script, and ``acquire_shared`` backs off a few milliseconds
#: between attempts so starved contenders stop thrashing in lockstep; a genuinely
#: unwritable store still fails, just after this many attempts rather than 3.
SHARED_ACQUIRE_ATTEMPTS = 20


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
#: ``bbng_now DIR`` prints the clock of the filesystem holding DIR as a bare
#: integer, read from the mtime of a probe file created in DIR -- the SAME
#: clock that stamps every lock record. ``date`` is not consulted: a store on a
#: networked mount whose server clock differs from this client's ``date`` can no
#: longer make a freshly-heartbeated lock read stale. Nothing when DIR is a
#: symbolic link or not a directory (a probe is a WRITE, and the listing paths
#: reach here with no safety walk of the store, so a link planted at the store
#: is refused rather than written through), when the probe cannot be created in
#: DIR, or when its mtime is not digits.
#:
#: ``bbng_age FILE [FALLBACK]`` prints the age in seconds of FILE against
#: ``$BBNG_NOW`` (which each judge site sets once from ``bbng_now <store root>``),
#: of FALLBACK when FILE has no readable mtime, or nothing when the mtime or
#: ``$BBNG_NOW`` cannot be read. Callers treat nothing as "cannot judge", never
#: as an age of zero or of forever.
MTIME_FUNCTIONS = (
    "if stat -c %Y / >/dev/null 2>&1; then bbng_st='-c %Y'; else bbng_st='-f %m'; fi; "
    'bbng_digits() { bbng_t=$1; [ -n "$bbng_t" ] || return 1; '
    'while [ -n "$bbng_t" ]; do case $bbng_t in [0123456789]*) bbng_t=${bbng_t#?};; '
    "*) return 1;; esac; done; return 0; }; "
    'bbng_mtime() { bbng_m=$(stat $bbng_st "$1" 2>/dev/null) || bbng_m=; '
    'bbng_digits "$bbng_m" || bbng_m=; '
    'printf "%s" "$bbng_m"; }; '
    'bbng_now() { bbng_nd=$1; [ -n "$bbng_nd" ] || return 0; '
    'if [ -L "$bbng_nd" ] || [ ! -d "$bbng_nd" ]; then return 0; fi; '
    'bbng_np=$bbng_nd/.bbng-now-probe.$$; rm -f "$bbng_np" 2>/dev/null; '
    'if ( set -C; : > "$bbng_np" ) 2>/dev/null; then '
    'bbng_nm=$(bbng_mtime "$bbng_np"); rm -f "$bbng_np" 2>/dev/null; '
    'printf "%s" "$bbng_nm"; fi; }; '
    'bbng_age() { bbng_a=$(bbng_mtime "$1"); '
    'if [ -z "$bbng_a" ] && [ -n "$2" ]; then bbng_a=$(bbng_mtime "$2"); fi; '
    'if [ -n "$bbng_a" ] && [ -n "$BBNG_NOW" ]; then echo $(( BBNG_NOW - bbng_a )); fi; }; '
)

#: Shell defining the functions every lock script creates directories and
#: records with. The scripts that write run under ``set -C`` as well.
#:
#: The records live beside the backups they protect, on a target this tool
#: may be writing as root, in a directory another account may be able to
#: write. A symbolic link planted at a predictable lock path -- the store, a
#: lock directory under it, a holder's record -- turned ``mkdir -p`` into a
#: tree built wherever the link pointed and ``>`` into a truncation of
#: whatever file it pointed at. POSIX sh has no ``O_NOFOLLOW``, so the
#: scripts refuse what they can detect and let the shell refuse the rest:
#:
#: ``bbng_mkdir DIR`` creates ONE path component with a plain ``mkdir``,
#: which never creates through a link at DIR: 0 when it was made or a real
#: directory already stood there, 2 when a symlink stands there, 1 when it
#: could not be made. ``bbng_tree DIR...`` walks the components below the
#: target in order, each checked by ``bbng_safe_dir`` before the next is
#: made beneath it, and ends the script at the first symlink (``UNSAFE
#: SYMLINK``). ``bbng_safe_dir DIR`` also refuses a directory its group or
#: everyone may write (``UNSAFE WRITABLE``): any account able to write it
#: could plant a link there. The permission bits are read from ``ls -ld``,
#: whose first ten characters are the same under GNU, BSD and busybox, where
#: ``stat`` would need a flavour probe of its own. ``bbng_safe_leaf FILE``
#: refuses a record path at which a symlink (``UNSAFE SYMLINK``) or anything
#: but a regular file (``UNSAFE NOTFILE``) stands. ``set -C`` (noclobber)
#: then makes every ``>`` fail on a name that exists, a symlink included --
#: verified under bash, dash and busybox ash -- so a link planted between the
#: check and the write is refused by the shell rather than followed.
#:
#: What this cannot close: a link swapped in at a path COMPONENT after
#: ``bbng_mkdir`` accepted it and before the next command used it. The shell
#: resolves every path afresh at every command, and only ``openat`` and
#: ``mkdirat`` against a held descriptor would pin the component. The local
#: transport (``LocalLockManager``) does exactly that from Python and closes
#: the shape fully; on a remote it is narrowed to that race. A symlink to a
#: FIFO or a device, which noclobber opens, is likewise narrowed to the
#: window after ``bbng_safe_leaf``. ``UNSAFE`` is never answered by
#: elevating: a refusal that elevated would do as root the very write that
#: was refused.
STORE_FUNCTIONS = (
    'bbng_mkdir() { if mkdir -m 755 "$1" 2>/dev/null; then return 0; fi; '
    'if [ -L "$1" ]; then return 2; fi; if [ -d "$1" ]; then return 0; fi; return 1; }; '
    'bbng_unsafe() { printf "UNSAFE %s %s" "$1" "$2" | tr -d "\\n"; echo; exit 0; }; '
    'bbng_safe_dir() { if [ -L "$1" ]; then bbng_unsafe SYMLINK "$1"; fi; '
    'bbng_p=$(ls -ld "$1" 2>/dev/null) || return 0; '
    'case $bbng_p in ?????w*|????????w*) bbng_unsafe WRITABLE "$1";; esac; return 0; }; '
    'bbng_tree() { for bbng_d in "$@"; do bbng_mkdir "$bbng_d"; '
    'case $? in 0) bbng_safe_dir "$bbng_d";; 2) bbng_unsafe SYMLINK "$bbng_d";; '
    "*) return 1;; esac; done; return 0; }; "
    'bbng_safe_leaf() { if [ -L "$1" ]; then bbng_unsafe SYMLINK "$1"; fi; '
    'if [ -e "$1" ] && [ ! -f "$1" ]; then bbng_unsafe NOTFILE "$1"; fi; return 0; }; '
)


def unsafe_store_message(verdict: str) -> str:
    """What the operator is told when a lock script, or the local store,
    answered ``UNSAFE KIND PATH`` (see ``STORE_FUNCTIONS``)."""
    _, _, rest = verdict.partition(" ")
    kind, _, path = rest.partition(" ")
    path = path.strip() or "the lock store"
    if kind == "SYMLINK":
        return (
            f"the lock store will not be used: {path!r} is a symbolic link. A "
            f"link planted at a lock path sends the lock, and every file written "
            f"for it, wherever the link points. Remove the link; whoever could "
            f"place it should not be able to write there"
        )
    if kind == "WRITABLE":
        return (
            f"the lock store will not be used: {path!r} can be written by its "
            f"group or by everyone. Any account that can write a lock directory "
            f"can plant a link that redirects a lock, so the directory must be "
            f"writable by its owner alone (chmod go-w {shlex.quote(path)})"
        )
    if kind == "NOTFILE":
        return (
            f"the lock store will not be used: {path!r} is not a regular file. "
            f"A FIFO, device or socket standing where a lock record belongs is "
            f"refused rather than written to"
        )
    return f"the lock store will not be used: {verdict.strip()}"


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


def _clock_readable(out: str) -> bool:
    """Whether a listing's ``NOW`` line carries the store's clock. The clock
    is read by writing a probe into the store, so an account that may list the
    store but not write it prints ``NOW`` with nothing after it."""
    for line in out.splitlines():
        if line.startswith("NOW"):
            return line[3:].strip().isdigit()
    return False


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
        dir_name: str = LOCK_DIR_NAME,
    ) -> None:
        """``run_elevated`` is used only when the unprivileged attempt could
        not write: the acquire when the lock directory cannot be created or
        written, and then every beat and every removal whose verdict says the
        record could not be refreshed or removed, since a record the acquire
        wrote elevated is root-owned for the whole of its life. Backup
        destinations are commonly root-owned, and the lock has to live beside
        the data it protects.

        ``dir_name`` is the directory under the target the records live in:
        ``LOCK_DIR_NAME`` for a location's lock store, and the holders
        directory beside a lock file for the records that say who holds the
        pins the file lists (``holders_beside``)."""
        self._run = run_remote
        self._target = str(target_path).rstrip("/") or "/"
        self._dir_name = dir_name
        self._root = f"{self._target}/{dir_name}"
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
            '[ ! -L "$bbng_f" ] && [ -f "$bbng_f" ] && cat "$bbng_f" 2>/dev/null && echo; done; '
            f"[ ! -L {info} ] && [ -f {info} ] && cat {info} 2>/dev/null; return 0; }}; "
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

        Nothing here is created through a symbolic link (``STORE_FUNCTIONS``):
        the store is made one component at a time and refused where a link
        or a group- or world-writable directory stands, a link at the lock
        path -- which ``mkdir`` refuses to create through, so the failed
        ``mkdir`` is where it is seen -- is refused rather than judged, and
        the record is written under ``set -C``. Still one script: the
        checks are added inside it, not as round trips before it.
        """
        lock = shlex.quote(self._lock_dir(name))
        root = shlex.quote(self._root)
        hb = shlex.quote(f"{self._lock_dir(name)}/{HEARTBEAT_NAME}")
        info = shlex.quote(f"{self._lock_dir(name)}/{LEGACY_INFO_NAME}")
        owner = shlex.quote(self._owner_file(name, token))
        target = shlex.quote(self._target)
        body = shlex.quote(csh_safe_text(payload))
        return (
            "set -C; "
            + self._exclusive_functions(name)
            + STORE_FUNCTIONS
            + "bbng_take() { "
            # A link inside the directory this holder has just created can
            # only have been planted by an account able to write the store;
            # a record written through it would land wherever it points.
            f"if [ -L {owner} ] || [ -L {hb} ]; then rmdir {lock} 2>/dev/null; "
            f"bbng_unsafe SYMLINK {lock}; fi; "
            f"if printf '%s' {body} 2>/dev/null > {owner}; then "
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
            # The walk refuses the store before judging it writable: a store
            # that is unsafe must never be answered by elevating into it.
            f"if [ -d {target} ]; then bbng_tree {root}; fi; "
            f"if [ ! -d {root} ] || [ ! -w {root} ]; then echo NOLOCKDIR; exit 0; fi; "
            f"if mkdir {lock} 2>/dev/null; then bbng_take ACQUIRED; exit 0; fi; "
            # ``mkdir`` refused to create through a link at the lock path;
            # judging what it points at would read, and could break, some
            # other lock.
            f"if [ -L {lock} ]; then bbng_unsafe SYMLINK {lock}; fi; "
            # The mkdir failed with no lock there: the holder released in
            # between (take it now), or the directory cannot be made at all
            # (a full or read-only filesystem, a quota) -- which is not
            # contention and is reported with the reason.
            f"if [ ! -d {lock} ]; then "
            f"  if bbng_e=$(mkdir {lock} 2>&1); then bbng_take ACQUIRED; exit 0; fi; "
            f'  if [ ! -d {lock} ]; then printf "NOLOCK %s" "$bbng_e" | tr -d "\\n"; echo; exit 0; fi; '
            "fi; "
            f"BBNG_NOW=$(bbng_now {root}); bbng_judge; "
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

    def _shared_payload(self, name: str, lock_id: str, operation: str) -> str:
        """This holder's record of its pin on ``name``: what every writer of
        a shared record, shell or local, puts in the file."""
        return json.dumps(
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

    def _shared_record_script(self, name: str, lock_id: str, operation: str) -> str:
        """The script that writes this holder's record of its pin on ``name``.

        Run by ``acquire_shared`` to take the pin and by the pin's heartbeat
        to keep it: the beat writes the record again, whole, so a record
        removed from under a live holder (``restore --unlock`` run during a
        restore, a sweep after a long stall) returns with its identity and
        inside its directories, rather than as an empty file ``touch`` left
        where the file was, or as nothing at all once the directories had
        been tidied away with the last record.

        The directories are made one component at a time and the record is
        written under ``set -C`` (``STORE_FUNCTIONS``). The first acquire
        creates a record that does not exist; the ``>`` fails on one that
        does, a link included. Every later beat only refreshes the mtime of
        the record already there, in place with ``touch -c``, so the record
        is never removed and a listing by a prune can never fall in a window
        where a live holder has no record. A record removed from under a
        live holder (``restore --unlock`` during a restore, a sweep after a
        long stall) is written again whole on the next beat, so the pin
        comes back with its identity and inside its directories.
        ``bbng_safe_leaf`` refuses a link or a non-regular file at the name
        before either path, so neither ``touch`` nor ``>`` follows one; the
        residual the function block describes is a link swapped in between
        that check and the write that points at a FIFO or a device.
        """
        body = shlex.quote(
            csh_safe_text(self._shared_payload(name, lock_id, operation))
        )
        root = shlex.quote(self._root)
        target = shlex.quote(self._target)
        lock = shlex.quote(self._lock_dir(name))
        holders = shlex.quote(self._holders_dir(name))
        holder = shlex.quote(self._holder_file(name, lock_id))
        return (
            "set -C; "
            + STORE_FUNCTIONS
            # Same rule as acquisition: create only below an existing target,
            # never the target itself: one component at a time, none through
            # a link, and each refused -- as a link, or as a directory others
            # can write -- before the next is made under it and before any
            # NOLOCKDIR verdict could elevate into it.
            + f"if [ -d {target} ]; then bbng_tree {root} {lock} {holders}; fi; "
            # Reported distinctly from contention: a bare mkdir failure looks
            # exactly like losing a race, and sends an operator hunting for a
            # competing process that does not exist.
            f"if [ ! -d {root} ] || [ ! -d {holders} ] || [ ! -w {holders} ]; then "
            f"  echo NOLOCKDIR; exit 0; fi; "
            f"bbng_safe_leaf {holder}; "
            # stderr is silenced before ``>`` is opened: a refusal of the
            # write is a verdict, not a message for the caller's stderr.
            # The record exists on every beat after the first. Refresh its
            # mtime in place with ``touch -c`` -- never removing it -- so a
            # listing by a prune can never fall in a window where a live
            # holder has no record. ``bbng_safe_leaf`` has already refused a
            # link or a non-regular file at this name, so ``[ -f ]`` is
            # absent-or-regular; absent (the first acquire, or a record swept
            # from under a live holder) it is written whole under noclobber,
            # which is how a removed pin comes back.
            f"if [ -f {holder} ]; then touch -c {holder} 2>/dev/null "
            f"&& echo ACQUIRED || echo FAILED; "
            f"else printf '%s' {body} 2>/dev/null > {holder} "
            f"&& echo ACQUIRED || echo FAILED; fi"
        )

    # -------------------------------------------------- the creating writes
    #
    # The three operations that CREATE on the target -- the exclusive
    # acquire, the shared record (the acquire and every beat) and the
    # exclusive beat -- go through these, so a manager over a location on
    # this machine can do them with descriptors (``LocalLockManager``) while
    # everything else, and the elevated retry, runs the scripts. Each returns
    # ``(returncode, stdout, stderr)`` with the script's own verdict words.

    def _record_exclusive(
        self, name: str, payload: str, token: str
    ) -> tuple[int, str, str]:
        return self._run(self._acquire_script(name, payload, token))

    def _record_shared(
        self, name: str, lock_id: str, operation: str
    ) -> tuple[int, str, str]:
        return self._run(self._shared_record_script(name, lock_id, operation))

    def _refresh_exclusive(self, name: str) -> tuple[int, str, str]:
        return self._run(self._refresh_script(name))

    def _refresh_script(self, name: str) -> str:
        """The exclusive beat: ``touch -c`` of the holder's ``owner.<token>``
        file and the ``heartbeat`` beside it. ``touch`` refreshes whatever a
        link points at, so a link at either name is refused first."""
        with self._state_lock:
            token = self._tokens.get(name, "")
        files = [f"{self._lock_dir(name)}/{HEARTBEAT_NAME}"]
        if token:
            files.insert(0, self._owner_file(name, token))
        quoted = [shlex.quote(f) for f in files]
        return (
            STORE_FUNCTIONS
            + "".join(f"bbng_safe_leaf {q}; " for q in quoted)
            + f"touch -c {' '.join(quoted)} 2>/dev/null "
            f"&& echo REFRESHED || echo UNWRITABLE; exit 0"
        )

    def acquire_shared(self, name: str, lock_id: str, operation: str = "") -> str:
        """Add this holder's pin to ``name``. Never blocks on another holder.

        A concurrent ``release_shared`` can ``rmdir`` the lock and holders
        directories between this script creating them and writing the record
        into them, so a NOLOCKDIR or FAILED verdict can be a lost race rather
        than an unwritable store. Re-run the record script a bounded number of
        times (``SHARED_ACQUIRE_ATTEMPTS``) -- it recreates the directories each
        time -- before reporting the store unavailable, which is how the
        exclusive acquire recovers from the same race within its own script. An
        UNSAFE verdict is a safety refusal, never a race, and is raised at once.
        """
        rc, out, err = 0, "", ""
        for attempt in range(SHARED_ACQUIRE_ATTEMPTS):
            rc, out, err = self._record_shared(name, lock_id, operation)
            if _verdict(out).startswith("UNSAFE"):
                raise RemoteLockUnavailable(unsafe_store_message(_verdict(out)))
            if "NOLOCKDIR" in out and self._run_elevated is not None:
                rc, out, err = self._run_elevated(
                    self._shared_record_script(name, lock_id, operation)
                )
                if _verdict(out).startswith("UNSAFE"):
                    raise RemoteLockUnavailable(unsafe_store_message(_verdict(out)))
            if _verdict(out) == "ACQUIRED":
                _register_for_cleanup(self, self._held_key(name, lock_id))
                return "acquired"
            # NOLOCKDIR / FAILED: a concurrent release may have removed the lock
            # directories mid-write. Loop; the next run recreates and retries.
            # Back off a few milliseconds first, with jitter, so several starved
            # contenders (a slow build VM running the churn under load) stop
            # losing the race to one another in lockstep. Only between attempts:
            # the first try pays nothing, and there is no sleep after the last.
            if attempt + 1 < SHARED_ACQUIRE_ATTEMPTS:
                time.sleep(min(0.005 * (attempt + 1), 0.03) + random.uniform(0, 0.005))
        # Every attempt lost. On a writable store this is extreme contention on
        # the release-vs-acquire race, not a broken store, so the message names
        # that first and does not assert the path is unwritable -- 0.9.19 did,
        # which sent operators of a perfectly writable store hunting a phantom
        # permissions problem. Only the elevation hint stays tied to NOLOCKDIR,
        # the one verdict an unwritable store actually produces.
        detail = (
            f"the lock directory {self._root!r} could not be created or written to"
            if "NOLOCKDIR" in out
            else (err.strip() or out.strip() or f"exit {rc}")
        )
        raise RemoteLockUnavailable(
            f"could not record a shared lock on {name!r} after "
            f"{SHARED_ACQUIRE_ATTEMPTS} attempts ({detail}). On a writable store "
            f"this means extreme contention on the lock directory; otherwise the "
            f"path is not writable by the account running the backup, which must "
            f"be able to write there (or to elevate for it)."
        )

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
        script = (
            f"if rm -f {holder} 2>/dev/null; then "
            f"rmdir {holders} 2>/dev/null; rmdir {lock} 2>/dev/null; echo RELEASED; "
            f"else echo UNWRITABLE; fi; exit 0"
        )
        rc, out, err = self._run(script)
        if _verdict(out) == "UNWRITABLE" and self._run_elevated is not None:
            rc, out, err = self._run_elevated(script)
        if rc != 0 or _verdict(out) != "RELEASED":
            logger.warning(
                "Could not fully release the shared lock %r on the target: %s",
                name,
                err.strip() or out.strip(),
            )
            return
        _unregister_for_cleanup(self, key)

    def acquire_shared_persistent(
        self, name: str, lock_id: str, operation: str = ""
    ) -> str:
        """Pin ``name`` and keep the pin alive until ``release_shared``."""
        mode = self.acquire_shared(name, lock_id, operation)
        self._start_heartbeat(
            self._held_key(name, lock_id), shared=(name, lock_id, operation)
        )
        return mode

    def keep_record_past_exit(self, name: str, lock_id: str) -> None:
        """Leave this holder's record where it is when the process exits.

        A pin in a lock file survives the process that took it, on purpose:
        an interrupted backup keeps its pin so the source cannot prune what
        a later run still has to send. The record beside the file is the
        evidence of who took the pin and whether they are still at it, so it
        has to outlive the process too: released at exit, an interrupted
        restore's pin would have no record and doctor could not tell it from
        one taken by an earlier version. The beat stops with the process, so
        the record reads as a holder that is gone, which is the truth.
        """
        _unregister_for_cleanup(self, self._held_key(name, lock_id))

    @staticmethod
    def _held_key(name: str, lock_id: str) -> str:
        return f"{name}\x00{lock_id}"

    def holds_shared(self, name: str, lock_id: str) -> bool:
        """Whether THIS manager already holds that pin."""
        with self._state_lock:
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
        rc, out, err = self._record_exclusive(name, payload, token)
        if _verdict(out) == "NOLOCKDIR" and self._run_elevated is not None:
            # Backup destinations are usually root-owned, so the unprivileged
            # attempt failing is the ordinary case rather than an error.
            rc, out, err = self._run_elevated(
                self._acquire_script(name, payload, token)
            )
        verdict = _verdict(out)
        if verdict.startswith("UNSAFE"):
            # Never elevated: the refusal is about the store, not the account.
            raise RemoteLockUnavailable(unsafe_store_message(verdict))
        if verdict == "NOLOCKDIR":
            raise RemoteLockUnavailable(
                f"the lock directory {self._root!r} on the target could not be "
                f"created or written to. Locks live beside the backups they "
                f"protect, so this path must be writable by the account running "
                f"the backup, or that account must be able to elevate for it."
            )
        if verdict.startswith("NOLOCK"):
            reason = verdict[len("NOLOCK") :].strip() or "no reason given"
            raise RemoteLockUnavailable(
                f"the lock {self._lock_dir(name)!r} could not be created on the "
                f"target ({reason}). This is not another process holding it."
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
            script = (
                f"if rm {owner} </dev/null 2>/dev/null; then "
                f"rm -f {hb} 2>/dev/null; rmdir {lock} 2>/dev/null; echo RELEASED; "
                f"elif [ -e {owner} ]; then echo KEPT; else echo NOTOWNER; fi"
            )
            rc, out, err = self._run(script)
            verdict = _verdict(out)
            if verdict == "KEPT" and self._run_elevated is not None:
                # The record is there and could not be removed: a root-owned
                # store, whose lock the acquire took elevated. NOTOWNER is
                # not this case; the record is gone, and nothing elevated
                # would change that.
                rc, out, err = self._run_elevated(script)
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
        root = shlex.quote(self._root)
        script = (
            self._exclusive_functions(name) + f"if [ -d {lock} ]; then "
            f"BBNG_NOW=$(bbng_now {root}); bbng_judge; "
            'printf "AGE %s\\n" "${AGE:-?}"; bbng_show; '
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

    def shared_records(self) -> dict[str, list[tuple[Holder, Optional[int]]]]:
        """Every shared holder record on the target, live or not, with its
        age in seconds by the target's clock (None when it cannot be read),
        keyed by the lock's real name.

        ``live_locks`` answers the guard's question, "is this held", and
        leaves out a record whose beat has stopped. Doctor asks a different
        one, "who holds this and are they still there", and needs the
        records a stopped beat would hide, with the age that says so.
        """
        records: dict[str, list[tuple[Holder, Optional[int]]]] = {}
        for kind, key, holder, age in self._scan()[2]:
            if kind == "H":
                records.setdefault(key, []).append((holder, age))
        return records

    def _scan_holders(self) -> tuple[dict[str, list[Holder]], list[tuple[str, str]]]:
        live, dead, _records = self._scan()
        return live, dead

    def _scan(
        self,
    ) -> tuple[
        dict[str, list[Holder]],
        list[tuple[str, str]],
        list[tuple[str, str, Holder, Optional[int]]],
    ]:
        """(live locks by real name, (dir, file) of abandoned holder records,
        every record as (kind, real name, holder, age)).

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
            MTIME_FUNCTIONS + f'printf "NOW %s\\n" "$(bbng_now {root})"; '
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
            f'      if [ -L "$h" ] || [ ! -f "$h" ]; then continue; fi; '
            f'      m=$(bbng_mtime "$h"); '
            f'      printf "H %s %s %s " "$n" "${{m:-?}}" "$(basename "$h")"; '
            f'      cat "$h" 2>/dev/null; printf "\\n"; '
            f"    done; "
            f"  else "
            f'    k=; for f in "$d"/{OWNER_PREFIX}*; do [ -f "$f" ] && [ ! -L "$f" ] && k=$f; done; '
            f'    if [ -n "$k" ]; then m=$(bbng_mtime "$k"); '
            f'    else k="$d/{LEGACY_INFO_NAME}"; m=$(bbng_mtime "$d/{HEARTBEAT_NAME}"); '
            f'      [ -n "$m" ] || m=$(bbng_mtime "$d"); fi; '
            f'    printf "X %s %s - " "$n" "${{m:-?}}"; '
            f'    [ ! -L "$k" ] && cat "$k" 2>/dev/null; printf "\\n"; '
            f"  fi; "
            f"done 2>/dev/null; exit 0"
        )
        rc, out, err = self._run(script)
        elevated = False
        if rc == 0 and "UNLISTABLE" in out.splitlines():
            if self._run_elevated is not None:
                rc, out, err = self._run_elevated(script)
                elevated = True
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
        # The clock is read by writing a probe into the store, so an account
        # that may list the store but not write it -- the usual case for a
        # scheduled backup's root-owned store -- cannot read it, and would
        # report every record held and the sweep nothing dead. That is what
        # ``run_elevated`` exists for: ask again as root, which can probe. If
        # root cannot probe either, every record stays held, as on a store that
        # is read-only for everyone.
        if not elevated and self._run_elevated is not None and not _clock_readable(out):
            e_rc, e_out, e_err = self._run_elevated(script)
            if e_rc == 0:
                rc, out, err = e_rc, e_out, e_err

        now: Optional[int] = None
        clock_read = False
        live: dict[str, list[Holder]] = {}
        dead: list[tuple[str, str]] = []
        records: list[tuple[str, str, Holder, Optional[int]]] = []
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
            holder = Holder(dir_name, file_name, info)
            records.append((kind, key, holder, age))
            if age is not None and age > self._stale_after:
                if kind == "H" and age > self._stale_after * DEAD_HOLDER_MULTIPLE:
                    dead.append((dir_name, file_name))
                continue
            live.setdefault(key, []).append(holder)
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
        return live, dead, records

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
        threshold = self._stale_after * DEAD_HOLDER_MULTIPLE
        root = shlex.quote(self._root)
        paths = " ".join(
            shlex.quote(f"{self._root}/{dir_name}.lock/{HOLDERS_DIR_NAME}/{file_name}")
            for dir_name, file_name in dead
        )
        # Re-check each record's age in the SAME script that removes it, rather
        # than removing by path what a separate listing judged dead: a holder
        # whose heartbeat refreshed its record between _scan_holders' listing
        # and this removal has a young mtime NOW and must be kept. A blind rm
        # removed a live holder's fresh record (the list-then-remove TOCTOU).
        # ``bbng_age`` is the same measure the listing used, so the judgement is
        # consistent. Reports how many it actually removed.
        script = (
            MTIME_FUNCTIONS + f"BBNG_NOW=$(bbng_now {root}); "
            # A probe that cannot be written is the store refusing this
            # account, not a verdict on any record: report the unwritable
            # store it is, so the elevated pass -- which can probe -- runs,
            # and remove nothing meanwhile.
            'if [ -z "$BBNG_NOW" ]; then echo "UNWRITABLE 0"; exit 0; fi; '
            "bbng_swept=0; bbng_unwr=0; "
            f"for bbng_p in {paths}; do "
            'bbng_a=$(bbng_age "$bbng_p"); '
            f'if [ -n "$bbng_a" ] && [ "$bbng_a" -gt {threshold} ]; then '
            'if rm -f "$bbng_p" 2>/dev/null; then bbng_swept=$((bbng_swept + 1)); '
            'elif [ -e "$bbng_p" ]; then bbng_unwr=1; fi; '
            "fi; done; "
            'if [ "$bbng_unwr" -eq 1 ]; then echo "UNWRITABLE $bbng_swept"; '
            'else echo "SWEPT $bbng_swept"; fi; exit 0'
        )

        def _count(out: str) -> int:
            parts = _verdict(out).split()
            return int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

        _rc, out, _err = self._run(script)
        swept = _count(out)
        if _verdict(out).startswith("UNWRITABLE") and self._run_elevated is not None:
            # A root-owned record the unprivileged rm could not remove: the
            # elevated pass re-checks age too, so it will not remove a record
            # refreshed since, nor double-count one already removed (its age
            # reads empty).
            _rc, out, _err = self._run_elevated(script)
            swept += _count(out)
        logger.debug("Swept %d abandoned holder record(s) on the target", swept)
        return swept

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
        script = (
            f"if rm -f {record} 2>/dev/null; then "
            f"rmdir {holders} 2>/dev/null; rmdir {lock} 2>/dev/null; echo RELEASED; "
            f"else echo UNWRITABLE; fi; exit 0"
        )
        rc, out, _err = self._run(script)
        if _verdict(out) == "UNWRITABLE" and self._run_elevated is not None:
            rc, out, _err = self._run_elevated(script)
        if rc == 0 and own_key is not None:
            _unregister_for_cleanup(self, own_key)

    # ------------------------------------------------------------- heartbeat

    def _start_heartbeat(
        self, key: str, shared: Optional[tuple[str, str, str]] = None
    ) -> None:
        """Refresh the holder's record until stopped. ``key`` identifies it.

        For a shared pin ``shared`` is ``(name, lock id, operation)``, and
        every beat writes this holder's own record again
        (``_record_shared``), so holders of the same snapshot keep their pins
        alive independently and a record removed from under a live holder
        comes back whole. For an exclusive lock (no ``shared``) the beat is a
        ``touch -c`` of the holder's ``owner.<token>`` file and the
        ``heartbeat`` beside it (``_refresh_exclusive``): a holder whose lock
        was broken while it was stalled must not recreate its record inside
        the next holder's lock.

        A beat elevates as the acquire did. The acquire runs unprivileged
        first and elevates when the lock directory cannot be created or
        written, which is a root-owned destination; the record it then
        writes is root-owned too, and a beat run unprivileged alone could
        not refresh it, so the pin went stale after ``stale_after`` seconds
        while its holder was still at work, and an exclusive lock could be
        broken under a running operation. A beat whose verdict says it
        could not write is run again elevated, with the same runner. A beat
        the store refused as unsafe is never elevated (``STORE_FUNCTIONS``):
        it is logged, and the record goes unrefreshed, which is the truth
        about a store that cannot safely be written.
        """
        stop = threading.Event()

        def beat_once() -> None:
            if shared is not None:
                _rc, out, _err = self._record_shared(*shared)
                command = self._shared_record_script(*shared)
            else:
                _rc, out, _err = self._refresh_exclusive(key)
                command = self._refresh_script(key)
            verdict = _verdict(out)
            if verdict.startswith("UNSAFE"):
                logger.warning(
                    "The lock record for %r was not refreshed: %s",
                    key,
                    unsafe_store_message(verdict),
                )
                return
            if (
                verdict not in ("ACQUIRED", "REFRESHED")
                and self._run_elevated is not None
            ):
                self._run_elevated(command)

        def beat() -> None:
            while not stop.wait(self._heartbeat_interval):
                try:
                    beat_once()
                except Exception as exc:  # noqa: BLE001 - a missed beat is not fatal
                    # One failed refresh must not end a transfer. Several in a
                    # row let the lock go stale, which is the designed outcome
                    # for a holder that can no longer reach the target.
                    logger.debug("Heartbeat for %r failed: %s", key, exc)

        thread = threading.Thread(target=beat, name="lock-hb", daemon=True)
        # _held/_threads are shared: one source endpoint per volume is used by
        # every target thread, and record_pin's holds_shared()/this start is a
        # check-then-act. Without this guard two threads pinning the same key
        # both reached here and the second overwrote _held[key]/_threads[key],
        # orphaning the first thread -- which kept rewriting the record after
        # release, resurrecting a dropped pin. Register and start the thread only
        # if no heartbeat is already running for this key; the record the caller
        # wrote (acquire) is idempotent, so an existing beat already keeps it
        # alive and a duplicate start is dropped.
        with self._state_lock:
            if key in self._held:
                return
            self._held[key] = stop
            self._threads[key] = thread
        _register_for_cleanup(self, key)
        thread.start()

    def _stop_heartbeat(self, key: str) -> None:
        """Stop refreshing. The exit-cleanup entry stays until the release
        itself has run, so a release that does not complete is retried."""
        with self._state_lock:
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
        with self._state_lock:
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

    The caller computes the state it wants and writes it whole, matching the
    local lock-file contract. Here that means dropping every holder the new
    state no longer lists -- per holder, not per snapshot, so clearing one
    session's lock leaves another session's pin on the same snapshot intact.
    A holder taken after the caller read the store is not in the new state
    and is dropped with the rest, which is why ``restore --unlock`` releases
    through ``release_persisted_locks`` instead.

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


def release_persisted_locks(
    manager: Any, pins: dict[str, dict[str, list[Any]]]
) -> Iterator[tuple[str, dict[str, list[str]], bool]]:
    """Release the given snapshot pins, each by its own holder record.

    ``pins`` is in the lock-file shape ``read_persisted_locks`` returns: for
    each snapshot, the ids under ``locks`` and ``parent_locks`` to release.
    The holders are listed once, when this runs, and only a holder whose id
    is listed in ``pins`` is dropped, so a pin taken after the caller read
    the store (a restore that began meanwhile) is kept, and a pin released
    meanwhile is simply not found. ``unknown`` under ``locks`` names the
    holders of that snapshot whose record could not be parsed, the way
    ``read_persisted_locks`` reports them.

    Yields, per snapshot in ``pins``, its name, the ids actually released in
    the same shape, and whether the snapshot still has a holder afterwards.
    A holder found in a directory that could not be listed cannot be
    addressed and counts as still held. Locks that are not snapshot pins --
    the whole-target lock a prune holds -- are never touched. Abandoned
    holder records are swept at the end, as before.
    """
    live = manager.live_locks()
    for snapshot, wanted in pins.items():
        ids = {str(x) for x in wanted.get("locks", [])}
        ids |= {f"p:{x}" for x in wanted.get("parent_locks", [])}
        released: dict[str, list[str]] = {}
        still_held = False
        for holder in live.get(f"{SNAPSHOT_LOCK_PREFIX}{snapshot}", []):
            lock_id = holder.lock_id
            selected = "unknown" in ids if lock_id is None else lock_id in ids
            if not selected:
                still_held = True
                continue
            manager.release_holder(holder)
            if not holder.file_name:
                # ``release_holder`` said why nothing could be removed.
                still_held = True
                continue
            key = "parent_locks" if str(lock_id or "").startswith("p:") else "locks"
            released.setdefault(key, []).append(
                lock_id.removeprefix("p:") if lock_id else "unknown"
            )
        yield snapshot, released, still_held
    manager.sweep_dead_holders()


class _StoreVerdict(Exception):
    """A verdict line reached before the operation could finish: ``NOLOCKDIR``
    (with the errno that said so), or ``UNSAFE KIND PATH``."""

    def __init__(self, line: str, error: Optional[int] = None) -> None:
        super().__init__(line)
        self.line = line
        self.errno = error


def _unsafe(kind: str, path: str) -> _StoreVerdict:
    return _StoreVerdict(f"UNSAFE {kind} {path}".replace("\n", ""))


@contextmanager
def _closing(fd: int) -> Iterator[int]:
    try:
        yield fd
    finally:
        os.close(fd)


def _writable(dir_fd: int) -> bool:
    """``[ -w DIR ]`` for the directory ``dir_fd`` is open on."""
    return os.access(
        ".",
        os.W_OK,
        dir_fd=dir_fd,
        effective_ids=os.access in os.supports_effective_ids,
    )


class LocalLockManager(RemoteLockManager):
    """A lock manager over a location on THIS machine.

    The protocol is the remote one, and most of it runs here exactly as it
    runs on a remote -- through ``sh`` -- so the pin a local run takes is the
    pin an ssh:// prune reads, and the other way round (``local_manager``).
    The three operations that CREATE on the target -- the exclusive acquire,
    the shared record and the exclusive beat -- are done from Python with
    descriptors instead: ``sh`` cannot refuse to follow a symbolic link, and
    a link planted in a store this tool may be writing as root turned a lock
    into a tree built, or a file truncated, wherever the link pointed
    (``STORE_FUNCTIONS`` says what the shell can and cannot refuse). Each
    component below the target is created or opened relative to the
    descriptor of the one above with ``O_NOFOLLOW``, and each record with
    ``O_NOFOLLOW`` -- ``O_EXCL`` where the record is new -- so there is no
    window between the check and the use: what is used is the descriptor,
    and a link at any component fails the open in the kernel. A store its
    group or everyone can write is refused outright.

    The verdicts are the scripts' own words, so the callers that read them,
    and the elevated retry -- the hardened script through ``sudo -n sh -c``
    when a root-owned store refuses the unprivileged attempt -- are the same
    code for both transports. ``UNSAFE`` is never retried elevated.
    """

    # ----------------------------------------------------------- the store

    def _open_store(self, *, create: bool) -> int:
        """A descriptor on this manager's store directory, made below the
        target when ``create``, each component refused where a link stands.

        ``NOLOCKDIR`` when the target is missing or the store cannot be made
        or entered -- the errno travels, so a beat can tell a store that is
        not there from one it may not enter -- and ``UNSAFE`` when a
        component is a link or the store is writable by its group or by
        everyone. The target itself is opened as the operator named it; the
        components below it are what another account could plant.
        """
        try:
            fd = os.open(self._target, os.O_RDONLY | os.O_DIRECTORY)
        except OSError as e:
            raise _StoreVerdict("NOLOCKDIR", e.errno) from None
        path = self._target
        for component in (c for c in self._dir_name.split("/") if c):
            path = f"{path}/{component}"
            fd = self._open_below(fd, component, path, create=create)
        return fd

    @classmethod
    def _open_below(cls, parent_fd: int, name: str, path: str, *, create: bool) -> int:
        """A descriptor on ``name`` under ``parent_fd``, which is closed:
        refused where a link stands, and refused -- once opened -- where its
        group or everyone could write it, before anything is made or
        written beneath it."""
        from .. import __util__

        try:
            fd = __util__.open_directory_nofollow(name, dir_fd=parent_fd, create=create)
        except OSError as e:
            if e.errno == errno.ELOOP:
                raise _unsafe("SYMLINK", path) from None
            raise _StoreVerdict("NOLOCKDIR", e.errno) from None
        finally:
            os.close(parent_fd)
        try:
            cls._refuse_writable(fd, path)
        except BaseException:
            os.close(fd)
            raise
        return fd

    @staticmethod
    def _refuse_writable(dir_fd: int, path: str) -> None:
        if os.fstat(dir_fd).st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise _unsafe("WRITABLE", path)

    # ------------------------------------------------- the exclusive acquire

    def _record_exclusive(
        self, name: str, payload: str, token: str
    ) -> tuple[int, str, str]:
        try:
            lines = self._acquire_here(name, payload, token)
        except _StoreVerdict as v:
            lines = [v.line]
        return 0, "\n".join(lines) + "\n", ""

    def _acquire_here(self, name: str, payload: str, token: str) -> list[str]:
        """``_acquire_script``, step for step, against descriptors. The lock
        directory is still taken by ``mkdir`` -- ``mkdirat`` here -- and the
        break still removes the one entry that identifies the instance judged
        stale, so the one-winner argument in that docstring is unchanged."""
        from .. import __util__

        lock_name = f"{encode_name(name)}.lock"
        lock_path = self._lock_dir(name)
        with _closing(self._open_store(create=True)) as root_fd:
            if not _writable(root_fd):
                return ["NOLOCKDIR"]
            if self._mkdir(root_fd, lock_name, lock_path):
                return self._take(
                    root_fd, lock_name, lock_path, token, payload, "ACQUIRED"
                )
            try:
                lock_fd = __util__.open_directory_nofollow(lock_name, dir_fd=root_fd)
            except FileNotFoundError:
                # The holder released between the mkdir and this: take it.
                if self._mkdir(root_fd, lock_name, lock_path):
                    return self._take(
                        root_fd, lock_name, lock_path, token, payload, "ACQUIRED"
                    )
                try:
                    lock_fd = __util__.open_directory_nofollow(
                        lock_name, dir_fd=root_fd
                    )
                except OSError as e:
                    return [f"NOLOCK {lock_path}: {e.strerror}"]
            except OSError as e:
                if e.errno == errno.ELOOP:
                    # ``mkdir`` refused to create through a link at the lock
                    # path; judging what it points at would read, and could
                    # break, some other lock.
                    raise _unsafe("SYMLINK", lock_path) from None
                return [f"NOLOCK {lock_path}: {e.strerror}"]
            with _closing(lock_fd):
                key, age = self._judge(lock_fd, self._probe_now(root_fd))
                if age is None:
                    # No readable age: a lock that cannot be judged is never
                    # broken, unless it is gone.
                    if self._entry_exists(root_fd, lock_name):
                        return ["BUSY", *self._show(lock_fd)]
                    if self._mkdir(root_fd, lock_name, lock_path, busy_on_failure=True):
                        return self._take(
                            root_fd, lock_name, lock_path, token, payload, "ACQUIRED"
                        )
                    return ["BUSY"]
                if age <= self._stale_after:
                    return ["BUSY", *self._show(lock_fd)]
                # Stale. Remove the entry identifying the instance that was
                # judged (see ``_acquire_script``); only one contender can.
                if key is not None:
                    try:
                        os.unlink(key, dir_fd=lock_fd)
                    except OSError:
                        return ["BUSY"]
                    for extra in (HEARTBEAT_NAME, LEGACY_INFO_NAME):
                        with suppress(OSError):
                            os.unlink(extra, dir_fd=lock_fd)
            try:
                os.rmdir(lock_name, dir_fd=root_fd)
            except OSError:
                return ["BUSY"]
            if self._mkdir(root_fd, lock_name, lock_path, busy_on_failure=True):
                return self._take(
                    root_fd, lock_name, lock_path, token, payload, "ACQUIRED_STALE"
                )
            return ["BUSY"]

    @staticmethod
    def _mkdir(
        root_fd: int, lock_name: str, lock_path: str, *, busy_on_failure: bool = False
    ) -> bool:
        """``mkdir`` of the lock directory: True when this call made it,
        False when something already stands there. Any other refusal is not
        contention: ``NOLOCK`` with the reason, or -- in the break, where the
        script says BUSY -- False."""
        try:
            os.mkdir(lock_name, 0o755, dir_fd=root_fd)
        except FileExistsError:
            return False
        except OSError as e:
            if busy_on_failure:
                return False
            raise _StoreVerdict(f"NOLOCK {lock_path}: {e.strerror}") from None
        return True

    @staticmethod
    def _entry_exists(dir_fd: int, name: str) -> bool:
        try:
            os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        except OSError:
            return False
        return True

    @staticmethod
    def _is_regular(dir_fd: int, name: str) -> bool:
        try:
            st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        except OSError:
            return False
        return stat.S_ISREG(st.st_mode)

    def _probe_now(self, root_fd: int) -> Optional[int]:
        """The store filesystem's clock, read as the mtime of a probe file
        created in the store root under ``root_fd``.

        The same clock that stamps every lock record, so a store on a networked
        mount whose server stamps mtimes cannot be aged against this machine's
        ``time.time()`` -- comparing a client wall clock to a server-stamped
        mtime is exactly the skew this replaces. None when the probe cannot be
        created or its mtime read: "cannot judge", which ``_acquire_here`` treats
        as "do not break". The probe is created in the store ROOT (never inside a
        lock or holders directory, so no lock's mtime is disturbed), O_EXCL and
        O_NOFOLLOW like every other record this manager writes, and unlinked at
        once.
        """
        probe = f".bbng-now-probe.{os.getpid()}.{uuid.uuid4().hex}"
        try:
            fd = os.open(
                probe,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=root_fd,
            )
        except OSError:
            return None
        try:
            return int(os.fstat(fd).st_mtime)
        except OSError:
            return None
        finally:
            os.close(fd)
            with suppress(OSError):
                os.unlink(probe, dir_fd=root_fd)

    def _judge(
        self, lock_fd: int, now: Optional[int]
    ) -> tuple[Optional[str], Optional[int]]:
        """``bbng_judge``: the entry identifying this instance of the lock, and
        its age against ``now`` -- the store filesystem's clock read by
        ``_probe_now`` -- or None where no age can be read. ``time.time()`` is
        not used: on a networked store the records are stamped by the server, and
        aging them against this machine's wall clock is the skew the probe
        removes."""
        from .. import __util__

        def age_of(name: str) -> Optional[int]:
            if now is None:
                return None
            try:
                st = (
                    os.fstat(lock_fd)
                    if name == "."
                    else os.stat(name, dir_fd=lock_fd, follow_symlinks=False)
                )
            except OSError:
                return None
            return now - int(st.st_mtime)

        try:
            entries = sorted(__util__.listdir_current(lock_fd))
        except OSError:
            entries = []
        owners = [
            n
            for n in entries
            if n.startswith(OWNER_PREFIX) and self._is_regular(lock_fd, n)
        ]
        if owners:
            return owners[-1], age_of(owners[-1])
        if self._is_regular(lock_fd, LEGACY_INFO_NAME):
            age = age_of(HEARTBEAT_NAME)
            return LEGACY_INFO_NAME, age if age is not None else age_of(".")
        if self._is_regular(lock_fd, HEARTBEAT_NAME):
            return HEARTBEAT_NAME, age_of(HEARTBEAT_NAME)
        return None, age_of(".")

    def _show(self, lock_fd: int) -> list[str]:
        """``bbng_show``: the holder's recorded details, one record a line."""
        from .. import __util__

        lines = []
        try:
            entries = sorted(__util__.listdir_current(lock_fd))
        except OSError:
            entries = []
        names = [n for n in entries if n.startswith(OWNER_PREFIX)] + [LEGACY_INFO_NAME]
        for name in names:
            if not self._is_regular(lock_fd, name):
                continue
            try:
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=lock_fd)
            except OSError:
                continue
            with os.fdopen(fd, "rb") as f:
                lines.append(f.read().decode("utf-8", errors="replace").strip())
        return lines

    def _take(
        self,
        root_fd: int,
        lock_name: str,
        lock_path: str,
        token: str,
        payload: str,
        verdict: str,
    ) -> list[str]:
        """``bbng_take``: write this holder's record into the directory it
        has just made, and report the lock taken only if that record is the
        only one there."""
        from .. import __util__

        try:
            lock_fd = __util__.open_directory_nofollow(lock_name, dir_fd=root_fd)
        except OSError as e:
            if e.errno == errno.ELOOP:
                raise _unsafe("SYMLINK", lock_path) from None
            return ["BUSY"]
        with _closing(lock_fd):
            owner = f"{OWNER_PREFIX}{token}"
            try:
                fd = __util__.open_regular_nofollow(
                    owner, dir_fd=lock_fd, exclusive=True
                )
            except OSError:
                return ["BUSY"]
            with os.fdopen(fd, "wb") as f:
                f.write(payload.encode("utf-8"))
            try:
                entries = __util__.listdir_current(lock_fd)
            except OSError:
                entries = []
            records = [
                n
                for n in entries
                if n.startswith(OWNER_PREFIX) and self._is_regular(lock_fd, n)
            ]
            if len(records) == 1:
                with suppress(OSError):
                    hb = __util__.open_regular_nofollow(HEARTBEAT_NAME, dir_fd=lock_fd)
                    try:
                        os.utime(hb)
                    finally:
                        os.close(hb)
                return [verdict]
            with suppress(OSError):
                os.unlink(owner, dir_fd=lock_fd)
        with suppress(OSError):
            os.rmdir(lock_name, dir_fd=root_fd)
        return ["BUSY"]

    # ---------------------------------------------------- the shared record

    def _record_shared(
        self, name: str, lock_id: str, operation: str
    ) -> tuple[int, str, str]:
        try:
            line = self._record_shared_here(name, lock_id, operation)
        except _StoreVerdict as v:
            line = v.line
        return 0, line + "\n", ""

    def _record_shared_here(self, name: str, lock_id: str, operation: str) -> str:
        """``_shared_record_script`` against descriptors. The record is
        opened ``O_NOFOLLOW`` and truncated in place -- it exists already at
        every beat -- so unlike the shell's remove-and-recreate it is never
        absent, and a planted link or FIFO fails the open in the kernel."""
        from .. import __util__

        payload = self._shared_payload(name, lock_id, operation)
        holders_path = self._holders_dir(name)
        holder_path = self._holder_file(name, lock_id)
        fd = self._open_store(create=True)
        fd = self._open_below(
            fd, f"{encode_name(name)}.lock", self._lock_dir(name), create=True
        )
        fd = self._open_below(fd, HOLDERS_DIR_NAME, holders_path, create=True)
        with _closing(fd) as holders_fd:
            if not _writable(holders_fd):
                return "NOLOCKDIR"
            try:
                record = __util__.open_regular_nofollow(
                    encode_name(str(lock_id)), dir_fd=holders_fd
                )
            except OSError as e:
                if e.errno == errno.ELOOP:
                    raise _unsafe("SYMLINK", holder_path) from None
                if e.errno in (errno.EEXIST, errno.ENXIO):
                    raise _unsafe("NOTFILE", holder_path) from None
                return "FAILED"
            with os.fdopen(record, "wb") as f:
                f.truncate(0)
                f.write(payload.encode("utf-8"))
            return "ACQUIRED"

    # --------------------------------------------------- the exclusive beat

    def _refresh_exclusive(self, name: str) -> tuple[int, str, str]:
        try:
            line = self._refresh_here(name)
        except _StoreVerdict as v:
            if v.line.startswith("UNSAFE"):
                line = v.line
            elif v.errno == errno.ENOENT:
                # ``touch -c`` of a file that is not there: nothing to do.
                line = "REFRESHED"
            else:
                line = "UNWRITABLE"
        return 0, line + "\n", ""

    def _refresh_here(self, name: str) -> str:
        """``_refresh_script`` against descriptors: the mtime of this
        holder's record and of the heartbeat beside it, refreshed through a
        descriptor opened ``O_NOFOLLOW`` on each, never created."""
        with self._state_lock:
            token = self._tokens.get(name, "")
        names = [HEARTBEAT_NAME]
        if token:
            names.insert(0, f"{OWNER_PREFIX}{token}")
        fd = self._open_store(create=False)
        fd = self._open_below(
            fd, f"{encode_name(name)}.lock", self._lock_dir(name), create=False
        )
        with _closing(fd) as lock_fd:
            for entry in names:
                try:
                    rfd = os.open(
                        entry,
                        os.O_WRONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                        dir_fd=lock_fd,
                    )
                except FileNotFoundError:
                    continue
                except OSError as e:
                    if e.errno == errno.ELOOP:
                        raise _unsafe(
                            "SYMLINK", f"{self._lock_dir(name)}/{entry}"
                        ) from None
                    return "UNWRITABLE"
                with _closing(rfd):
                    if not stat.S_ISREG(os.fstat(rfd).st_mode):
                        raise _unsafe("NOTFILE", f"{self._lock_dir(name)}/{entry}")
                    os.utime(rfd)
        return "REFRESHED"


def local_manager(
    target_path: str | os.PathLike, *, dir_name: str = LOCK_DIR_NAME
) -> "RemoteLockManager":
    """A lock manager over a location on this machine, running the same
    scripts an ssh endpoint runs on its remote, through ``sh`` -- except
    the writes that create, which it does with descriptors
    (``LocalLockManager``).

    Elevation mirrors the ssh endpoint's: the unprivileged attempt first,
    ``sudo -n sh -c`` only when a directory or record could not be created
    or written -- a mirror received under ``--ssh-sudo`` is root-owned, and
    its lock directory with it. What runs elevated is the hardened script.
    """
    import shlex
    import socket as _socket
    import subprocess

    from .. import __util__

    def run(script: str) -> tuple[int, str, str]:
        proc = subprocess.run(
            ["sh", "-c", script], capture_output=True, text=True, check=False
        )
        return proc.returncode, proc.stdout, proc.stderr

    def run_elevated(script: str) -> tuple[int, str, str]:
        try:
            sudo = " ".join(__util__.sudo_argv())
        except __util__.AbortError as e:
            # The manager reports what stderr said, so the reason reaches
            # the operator instead of the shell's "sudo: not found".
            return 127, "", str(e)
        return run(f"{sudo} sh -c {shlex.quote(script)}".lstrip())

    return LocalLockManager(
        run,
        str(target_path),
        hostname=_socket.gethostname(),
        run_elevated=run_elevated,
        dir_name=dir_name,
    )


def holders_dir_name(lock_file_name: str) -> str:
    """The directory beside a lock file that holds who-holds-what records."""
    return f"{lock_file_name}.holders"


def holders_beside(lock_file: str | os.PathLike) -> "RemoteLockManager":
    """The records beside a lock FILE: who holds each pin the file lists.

    A pin in the lock file is the protection and survives the process that
    took it. The record beside it is the evidence: the same payload a
    directory-store holder writes (name, lock id, operation, hostname, pid)
    with the same heartbeat, so doctor can say whether the holder of a pin
    is still at work, is gone, or never wrote a record. The file's format is
    untouched; every reader of it, and every older version, is unaffected.
    """
    path = os.fspath(lock_file)
    return local_manager(
        os.path.dirname(path) or ".",
        dir_name=holders_dir_name(os.path.basename(path)),
    )


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
            f"not known whether a restore or a receive is holding any of these snapshots"
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


@contextmanager
def hold_receiving_lock(
    manager: Any,
    destination: str,
    *,
    skip_remote_lock: bool = False,
) -> Iterator[str]:
    """Hold the right to create -- or to remove -- ``destination`` on a target:
    the one body behind every endpoint's ``_hold_receiving_lock``.

    ``manager`` is a ``RemoteLockManager`` or a zero-argument callable that
    builds one (an endpoint's bound ``_lock_manager``), as ``record_pin``
    takes it. Each endpoint supplies only the manager -- the store its locks
    live in -- and the protocol is this one for every transport: an ssh://
    target, a raw store on this machine or a remote one, a local btrfs target.
    Three copies of it had grown apart only in which store they opened.

    Scoped to ONE destination artifact, not the whole target: two receives
    into one directory under different names run in parallel; two under the
    same name are what collide, and the loser is refused BEFORE its stream
    starts, by a message naming the holder. Re-entrant within a process: the
    snapper flow holds this across a receive and the publish that follows it,
    and the transfer beneath must not contend with its own caller.

    Non-blocking. A holder is refused at once, not waited for: a second
    receive of the same artifact is a mistake to report, not a queue to join.
    A lock that cannot be recorded stops the operation, unless the operator
    said ``skip_remote_lock`` -- then it proceeds with a warning, unprotected
    on purpose.

    A deletion takes the same lock before removing the artifact
    (``Endpoint._lock_for_deletion``), so a prune and a receive of one
    artifact exclude each other instead of racing; the deletion skips what is
    being received, and ``ReceivingLockBusy`` carries the holder's name for
    it to say so.

    Yields how the block came to run: ``acquired`` (this call took the lock
    and will release it), ``reentrant`` (held further up this call stack) or
    ``unprotected`` (the opt-out). The caller stops the processes it starts
    before this returns (``btrfs_backup_ng.lifecycle.process_scope``), so
    nothing can take the lock while a stream is still arriving; a caller that
    cleans up a failed receive does so inside the block too, and only when
    the mode is ``acquired`` -- a re-entrant holder's cleanup belongs to the
    frame that took the lock.
    """
    from .. import __util__

    name = f"{RECEIVING_LOCK_PREFIX}{destination}"
    if callable(manager):
        manager = manager()
    if manager.holds(name):
        yield "reentrant"
        return
    try:
        manager.acquire_once(name, f"receive:{destination}")
    except RemoteLockBusy as exc:
        # Built from the holder's details rather than by interpolating the
        # exception, whose text repeats the destination and the lock name.
        # An operator reading a wrapped three-line sentence to find a
        # hostname is being made to work for it.
        info = exc.info or {}
        holder = "another process"
        if info:
            holder = (
                f"{info.get('hostname', 'an unknown host')} "
                f"(pid {info.get('pid', '?')})"
            )
        raise __util__.ReceivingLockBusy(
            destination,
            holder,
            f"Already being received by {holder}: {destination}\n"
            f"Refusing to start a second transfer to the same path -- both "
            f"would create the same copy, and the loser only finds out "
            f"after transferring everything.",
        ) from exc
    except RemoteLockUnavailable as exc:
        if skip_remote_lock:
            logger.warning(
                "Could not take the receiving lock on %s (%s), and "
                "--skip-remote-lock was given, so this continues WITHOUT "
                "protection against a concurrent transfer or deletion of the "
                "same path.",
                destination,
                exc,
            )
            yield "unprotected"
            return
        raise __util__.AbortError(
            f"Could not take the receiving lock on {destination!r}: {exc}. "
            f"Refusing to continue unprotected: a transfer and a deletion of "
            f"the same path, or two transfers, would collide. Pass "
            f"--skip-remote-lock to proceed anyway."
        ) from exc
    manager._start_heartbeat(name)
    try:
        yield "acquired"
    finally:
        manager.release(name)
