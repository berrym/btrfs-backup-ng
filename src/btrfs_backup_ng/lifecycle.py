"""What this process owns that must not outlive it, and the order it is let go.

Three kinds of thing outlive a careless exit: the child processes a transfer
runs (a ``btrfs send``, a compressor, the ``ssh`` carrying a stream), the locks
and pins held on a target, and the ssh control connection with its private
directory. They are let go in that order, whatever ends the operation:

1. **The writers stop first.** A lock or pin released while a child is still
   writing under it protects nothing: another process can take the lock, or
   prune the snapshot, while the stream is still arriving.
2. **Then the locks and pins,** which need the connection to reach the target.
3. **Then the connections.**

Child processes: the process scope
----------------------------------
Every long-lived child is registered, when it is created, with the innermost
``ProcessScope`` open on the creating thread. A scope is opened by the code
that starts children and waits for them, so it is always nested INSIDE
whatever lock or pin the caller holds. When an exception unwinds through a
scope -- ``KeyboardInterrupt`` included -- the scope terminates its children
(and their descendants), waits for them, and only then lets the exception
continue outwards, where the lock and pin releases run. Unwinding runs inner
cleanup before outer cleanup, so "no release under a live writer" is a
property of the nesting, not of any signal handler.

A scope that exits normally leaves its children alone: a function that starts
a process and returns it hands it to its caller, and the scope passes the
still-running children up to the enclosing scope, which then owns them.

SIGINT has no handler. Ctrl-C raises ``KeyboardInterrupt`` in the main thread
as Python always does; the main thread's scopes stop its own children, every
operation releases its own locks as it unwinds, and ``atexit`` sweeps whatever
remains. A worker thread's transfer is not interrupted by it: its children are
its own, and its locks stay held until it finishes.

Exit cleanups and the fatal signals
-----------------------------------
Locks, pins and connections register an exit cleanup. The registry is drained
on normal exit (``atexit``) and, when the command-line entry point has
installed the handlers, on SIGTERM and SIGHUP, whose default disposition ends
the process without running ``atexit``. The drain stops every registered
child process first (on every thread), then runs the cleanups stage by stage.

* An entry is removed only once its cleanup has run, so a drain that is
  interrupted -- a second signal, a second Ctrl-C -- leaves the rest in place
  for the next drain instead of abandoning it.
* A fatal signal arriving while a drain is already running does not start a
  second one; it is recorded, the running drain finishes, and the process then
  dies of the first signal.
* After the drain, the handler restores the default disposition and re-raises
  the signal, so the process still dies OF that signal and its parent (a
  shell, systemd) sees why.
* A signal the operator set to be ignored (``nohup``) stays ignored: the
  handlers are installed only over the default disposition, never over
  ``SIG_IGN`` or a handler someone else installed.
* The handlers are installed once, on the main thread, by the command-line
  entry point. Library code only registers; it never installs.
"""

from __future__ import annotations

import atexit
import errno
import functools
import logging
import os
import signal
import subprocess
import threading
import weakref
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional

logger = logging.getLogger(__name__)

#: Exit-cleanup stages, drained in this order: what a failed operation
#: half-made (after its writers have stopped, before the lock guarding it goes),
#: then locks and pins, then connections.
STAGE_PARTIALS = 0
STAGE_LOCKS = 1
STAGE_CONNECTIONS = 2

#: How long a terminated child gets to exit before it is killed, in seconds.
TERMINATE_GRACE = 5.0

#: The signals whose default disposition ends the process without ``atexit``.
FATAL_SIGNALS = (signal.SIGTERM, signal.SIGHUP)

_LOCK = threading.RLock()
_CLEANUPS: "dict[Any, tuple[int, Callable[[], None]]]" = {}
_ATEXIT_REGISTERED = False
_HANDLERS_INSTALLED = False
_DRAINING = False
_PENDING_SIGNAL: Optional[int] = None
_SHUTTING_DOWN = False

# Every live child registered with any scope, on any thread: the drain stops
# them all before it releases a single lock. Weak, so a child that has been
# reaped and dropped does not linger here.
_ALL_CHILDREN: "weakref.WeakSet[Any]" = weakref.WeakSet()
_SCOPES = threading.local()


# ------------------------------------------------------------ child processes


def _descendants(pid: int) -> list[int]:
    """Every live descendant of ``pid``, deepest first, read from ``/proc``.

    A shell pipeline (``sh -c 'a | b'``) is one child whose real work is done by
    ITS children; terminating the shell alone leaves them running. Where there
    is no ``/proc`` this returns nothing and only the child itself is stopped.
    """
    parents: dict[int, list[int]] = {}
    try:
        entries = os.listdir("/proc")
    except OSError:
        return []
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat", "rb") as handle:
                stat = handle.read().decode("ascii", "replace")
        except OSError:
            continue
        # The command name is parenthesised and may itself contain ") ", so
        # the fields after it are found from the LAST closing parenthesis.
        fields = stat[stat.rfind(")") + 2 :].split()
        if len(fields) < 2 or not fields[1].isdigit():
            continue
        parents.setdefault(int(fields[1]), []).append(int(entry))
    found: list[int] = []
    frontier = [pid]
    while frontier:
        children = [c for p in frontier for c in parents.get(p, [])]
        found.extend(children)
        frontier = children
    return list(reversed(found))


def _signal_pid(pid: int, signum: int) -> None:
    try:
        os.kill(pid, signum)
    except OSError as exc:  # already gone, or not ours to signal
        if exc.errno not in (errno.ESRCH, errno.EPERM):
            logger.debug("Could not signal pid %d: %s", pid, exc)


def _pid_of(proc: Any) -> Optional[int]:
    pid = getattr(proc, "pid", None)
    return pid if isinstance(pid, int) and not isinstance(pid, bool) else None


def _running(proc: Any) -> bool:
    try:
        if proc.poll() is not None:
            return False
    except Exception:  # noqa: BLE001 - an unpollable child is treated as gone
        return False
    # poll() also says "running" when another thread is inside wait() for the
    # same child and holds its lock; a zombie has exited all the same.
    pid = _pid_of(proc)
    if pid is not None:
        try:
            with open(f"/proc/{pid}/stat", "rb") as handle:
                stat = handle.read().decode("ascii", "replace")
            return stat[stat.rfind(")") + 2 : stat.rfind(")") + 3] != "Z"
        except OSError:
            pass
    return True


def stop_processes(procs: "list[Any]", grace: float = TERMINATE_GRACE) -> None:
    """Terminate ``procs`` and their descendants, wait, kill what lingers.

    Never raises. A second ``KeyboardInterrupt`` while waiting does not abandon
    the stop: it escalates straight to SIGKILL, finishes, and is re-raised.
    """
    live = [p for p in procs if _running(p)]
    if not live:
        return
    interrupted: Optional[BaseException] = None
    for signum in (signal.SIGTERM, signal.SIGKILL):
        for proc in live:
            pid = _pid_of(proc)
            if pid is not None:
                for child in _descendants(pid):
                    _signal_pid(child, signum)
            try:
                proc.send_signal(signum)
            except Exception:  # noqa: BLE001 - already reaped
                pass
        wait = grace if signum == signal.SIGTERM and interrupted is None else 2.0
        for proc in live:
            try:
                proc.wait(timeout=wait)
            except subprocess.TimeoutExpired:
                pass
            except KeyboardInterrupt as exc:
                interrupted = interrupted or exc
                break
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass
        live = [p for p in live if _running(p)]
        if not live:
            break
    if live:
        logger.warning(
            "%d child process(es) did not exit when killed: %s",
            len(live),
            ", ".join(str(_pid_of(p)) for p in live),
        )
    if interrupted is not None:
        raise interrupted


class ProcessScope:
    """The children one operation started; stopped if the operation fails.

    See the module docstring. Use through ``process_scope()``.
    """

    def __init__(self) -> None:
        self.children: list[Any] = []

    def track(self, proc: Any) -> Any:
        if proc is not None and proc not in self.children:
            self.children.append(proc)
        return proc


def _stack() -> "list[ProcessScope]":
    stack = getattr(_SCOPES, "stack", None)
    if stack is None:
        stack = []
        _SCOPES.stack = stack
    return stack


def track(proc: Any) -> Any:
    """Register a child with the innermost scope on this thread; returns it.

    Outside any scope the child is still known to the exit drain. A child
    started after a fatal signal began the drain is stopped at once: nothing
    may start writing while the locks are being let go.
    """
    if proc is None:
        return proc
    stack = _stack()
    if stack:
        stack[-1].track(proc)
    try:
        _ALL_CHILDREN.add(proc)
    except TypeError:  # pragma: no cover - an object that cannot be weakly referenced
        pass
    if _SHUTTING_DOWN:
        stop_processes([proc], grace=1.0)
    return proc


@contextmanager
def process_scope() -> Iterator[ProcessScope]:
    """Open a scope; on an exception, stop its children before unwinding on."""
    scope = ProcessScope()
    stack = _stack()
    stack.append(scope)
    try:
        yield scope
    except BaseException:
        _pop(stack, scope)
        stop_processes(scope.children)
        raise
    else:
        _pop(stack, scope)
        survivors = [p for p in scope.children if _running(p)]
        if survivors and stack:
            for proc in survivors:
                stack[-1].track(proc)


def scoped(fn: "Callable[..., Any]") -> "Callable[..., Any]":
    """Run ``fn`` inside its own process scope (see ``process_scope``)."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with process_scope():
            return fn(*args, **kwargs)

    return wrapper


def _pop(stack: "list[ProcessScope]", scope: ProcessScope) -> None:
    if stack and stack[-1] is scope:
        stack.pop()
    elif scope in stack:  # pragma: no cover - scopes are strictly nested
        stack.remove(scope)


# ------------------------------------------------------------- exit cleanups


def register(key: Any, cleanup: Callable[[], None], stage: int) -> None:
    """Run ``cleanup`` when this process exits, unless ``unregister``ed first.

    ``key`` identifies the entry (re-registering replaces it). The owner calls
    ``unregister(key)`` only once the thing is actually let go, so a release
    that did not complete is retried by the drain.
    """
    global _ATEXIT_REGISTERED
    with _LOCK:
        _CLEANUPS[key] = (stage, cleanup)
        if not _ATEXIT_REGISTERED:
            _ATEXIT_REGISTERED = True
            atexit.register(_drain_at_exit)


def unregister(key: Any) -> None:
    with _LOCK:
        _CLEANUPS.pop(key, None)


def registered(key: Any) -> bool:
    with _LOCK:
        return key in _CLEANUPS


def drain() -> None:
    """Stop every child, then run every registered cleanup, stage by stage.

    Resumable: an entry is removed only after its cleanup has run, so a drain
    interrupted part-way leaves the rest for the next one. Never raises, and a
    ``KeyboardInterrupt`` inside one cleanup moves on to the next.
    """
    global _SHUTTING_DOWN
    _SHUTTING_DOWN = True
    try:
        stop_processes(list(_ALL_CHILDREN))
    except KeyboardInterrupt:
        logger.debug("Interrupted while stopping child processes; continuing")
    run_cleanups()


def run_cleanups(select: Optional[Callable[[Any], bool]] = None) -> None:
    """Run the registered cleanups (those whose key ``select`` accepts), stage
    by stage, removing each once it has run. The cleanup half of ``drain``."""
    with _LOCK:
        pending = sorted(
            (
                (stage, index, key, cleanup)
                for index, (key, (stage, cleanup)) in enumerate(_CLEANUPS.items())
                if select is None or select(key)
            ),
            key=lambda item: (item[0], item[1]),
        )
    for _stage, _index, key, cleanup in pending:
        if not registered(key):
            continue  # its owner let it go while the drain was running
        try:
            cleanup()
        except KeyboardInterrupt:
            logger.debug("Exit cleanup %r interrupted; continuing", key)
            continue
        except Exception as exc:  # noqa: BLE001 - cleanup must not raise on exit
            logger.debug("Exit cleanup %r failed: %s", key, exc)
        unregister(key)


@contextmanager
def undo_on_failure(
    undo: Callable[[], None],
    *,
    unless: "tuple[type[BaseException], ...]" = (),
) -> Iterator[None]:
    """Run ``undo`` if the block does not complete.

    For removing what a failed operation half-made -- the partial subvolume an
    interrupted receive leaves. ``undo`` runs when an exception leaves the block
    (Ctrl-C included, except the types in ``unless``, which the caller handles
    itself), and, while the block runs, it is registered with the exit drain, so
    SIGTERM or SIGHUP run it too. It is dropped once the block completes.

    Where it is opened is the safety argument: OUTSIDE the process scope of the
    work, so the writers have been stopped before ``undo`` runs, and INSIDE the
    lock that guards the thing, so nobody else can have made it anew by then.
    A ``KeyboardInterrupt`` during ``undo`` leaves it registered for the exit
    drain to finish.
    """
    key = ("undo", object())
    register(key, undo, STAGE_PARTIALS)
    try:
        yield
    except BaseException as exc:
        if not isinstance(exc, unless):
            try:
                undo()
            except KeyboardInterrupt:
                raise
            except Exception as undo_exc:  # noqa: BLE001 - the original failure wins
                logger.debug("Undo after a failure did not complete: %s", undo_exc)
        unregister(key)
        raise
    else:
        unregister(key)


def _drain_at_exit() -> None:
    global _DRAINING
    with _LOCK:
        if _DRAINING:
            return
        _DRAINING = True
    try:
        drain()
    finally:
        _die_of_pending_signal()


def _die_of_pending_signal() -> None:
    """A fatal signal that arrived during a drain ends the process now."""
    signum = _PENDING_SIGNAL
    if signum is None:
        return
    try:
        signal.signal(signum, signal.SIG_DFL)
    except (ValueError, OSError):  # pragma: no cover - not the main thread
        pass
    os.kill(os.getpid(), signum)
    os._exit(128 + signum)  # pragma: no cover - the signal ends the process first


def _on_fatal_signal(signum: int, _frame: Any) -> None:
    global _DRAINING, _PENDING_SIGNAL
    if _PENDING_SIGNAL is None:
        _PENDING_SIGNAL = signum
    if _DRAINING:
        # A drain is already running (this signal interrupted it, or it is the
        # atexit drain): let it finish; it ends the process afterwards.
        return
    _DRAINING = True
    try:
        drain()
    finally:
        _die_of_pending_signal()


def install_signal_handlers() -> list[int]:
    """Drain on SIGTERM and SIGHUP, then die of the signal. Returns the
    signals now handled.

    Called once, from the command-line entry point, on the main thread. A
    signal whose disposition is not the default -- ignored by ``nohup`` or an
    operator, or handled by someone else -- is left exactly as it is.
    """
    global _HANDLERS_INSTALLED
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("signal handlers can only be installed on the main thread")
    installed: list[int] = []
    with _LOCK:
        if _HANDLERS_INSTALLED:
            return [s for s in FATAL_SIGNALS if signal.getsignal(s) is _on_fatal_signal]
        _HANDLERS_INSTALLED = True
    for signum in FATAL_SIGNALS:
        try:
            if signal.getsignal(signum) is not signal.SIG_DFL:
                continue
            signal.signal(signum, _on_fatal_signal)
            installed.append(signum)
        except (ValueError, OSError):  # pragma: no cover - platform dependent
            continue
    return installed
