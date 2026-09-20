"""Fixtures for Tier 3: the acceptance matrix.

Tier 2 proves btrfs primitives against a loopback filesystem. Tier 3 proves
FEATURES: it drives the installed CLI exactly as a user would -- a real config
file, a real source subvolume, a real remote host -- and then checks the effect
independently of the exit code.

That independence is the entire point. A 3000-test unit suite ran green through
two releases in which snapper backups over ssh returned 127 on every call, remote
pruning deleted nothing, and restore could exit 0 having restored no data. Exit
codes and log lines have all lied at least once; only the destination is trusted
here.

Requires root (local btrfs) and a reachable remote btrfs host. Opt in with::

    BBNG_TEST_SSH_HOST=user@host \\
      sudo -E env SSH_AUTH_SOCK=$SSH_AUTH_SOCK PATH=$PATH \\
      uv run pytest -m tier3 -q

The remote host must grant NOPASSWD on /usr/bin/btrfs -- the policy the README
documents -- because whether the product works under exactly that policy is one
of the things being measured.
"""

from __future__ import annotations

import functools
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

REMOTE_SPEC = os.environ.get("BBNG_TEST_SSH_HOST", "")
#: A second remote axis for raw+ssh, which stores PLAIN FILES and so needs
#: neither btrfs nor sudo on the far end. It cannot reuse BBNG_TEST_SSH_HOST's
#: gate, which requires NOPASSWD btrfs -- and the interesting host to point it at
#: is one that could never satisfy that gate: a NAS, or the macOS/APFS box the
#: matrix below uses, where `stat -c` does not exist, /bin/sh is not GNU, and
#: every local-filesystem assumption in the raw+ssh path fails loudly.
RAW_REMOTE_SPEC = os.environ.get("BBNG_TEST_RAW_SSH_HOST", "")
RIG_ROOT = Path(os.environ.get("BBNG_TEST_RIG", "/tmp/bbng-tier3"))
PAYLOAD_BYTES = 2 * 1024 * 1024
DELTA_BYTES = 1024 * 1024


def _have_local() -> bool:
    return (
        os.geteuid() == 0
        and shutil.which("btrfs") is not None
        and shutil.which("mkfs.btrfs") is not None
    )


@functools.cache
def _have_remote() -> bool:
    if not REMOTE_SPEC:
        return False
    r = subprocess.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            REMOTE_SPEC,
            "sudo -n btrfs --version",
        ],
        capture_output=True,
        timeout=30,
    )
    return r.returncode == 0


@functools.cache
def _have_raw_remote() -> bool:
    """Reachable, and able to hold files. Deliberately NOT a btrfs or sudo probe."""
    if not RAW_REMOTE_SPEC:
        return False
    r = subprocess.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            RAW_REMOTE_SPEC,
            "mkdir -p ~/.bbng-probe && rmdir ~/.bbng-probe",
        ],
        capture_output=True,
        timeout=30,
    )
    return r.returncode == 0


class _Deferred:
    """A condition evaluated at test setup rather than at import.

    `pytest.mark.skipif(not _have_remote())` runs the call when this module is
    IMPORTED, so the ssh probe fired during collection of every full-suite run --
    including runs that deselect tier3 entirely. With BBNG_TEST_SSH_HOST set and
    the host unreachable that cost 7.4s of ConnectTimeout per run to decide
    something no selected test needed.

    A string condition would also defer, but pytest evaluates those in the TEST
    module's namespace, where these helpers are not defined -- that spelling
    turned all six cells into setup errors while the deselected case still looked
    clean. pytest calls bool() on a non-string condition at setup, so deferring
    through __bool__ keeps the helpers here and probes only when a cell runs.
    """

    def __init__(self, predicate):
        self._predicate = predicate

    def __bool__(self) -> bool:
        return self._predicate()


requires_local = pytest.mark.skipif(
    _Deferred(lambda: not _have_local()),
    reason="Tier 3 needs root and btrfs-progs",
)
requires_remote = pytest.mark.skipif(
    _Deferred(lambda: not _have_remote()),
    reason="Tier 3 remote cells need BBNG_TEST_SSH_HOST with NOPASSWD btrfs",
)
requires_raw_remote = pytest.mark.skipif(
    _Deferred(lambda: not _have_raw_remote()),
    reason="Tier 3 foreign-target cells need BBNG_TEST_RAW_SSH_HOST (no btrfs, no sudo)",
)


#: The container runtime used by the sudoers axis below. Podman is preferred
#: because it needs no daemon and runs rootless.
CONTAINER_CMD = os.environ.get("BBNG_TEST_CONTAINER") or (
    shutil.which("podman") or shutil.which("docker") or ""
)


@functools.cache
def _have_container() -> bool:
    if not CONTAINER_CMD:
        return False
    r = subprocess.run(
        [CONTAINER_CMD, "info"], capture_output=True, timeout=120, check=False
    )
    return r.returncode == 0


requires_container = pytest.mark.skipif(
    _Deferred(lambda: not _have_container()),
    reason="Tier 3 sudoers cells need podman or docker",
)


def run_in_container(image: str, script: str, mounts: dict[str, str] | None = None):
    """Run ``script`` as root in a throwaway container.

    A real remote answers "is this permitted" from its sudoers policy, and the
    hosts this suite targets cannot represent the interesting policies without
    editing their /etc/sudoers -- a persistent change to a machine someone else
    relies on, which the teardown rule exists to prevent. A container is the
    only honest way to cover them, and `--rm` means it leaves nothing behind.
    """
    argv = [CONTAINER_CMD, "run", "--rm"]
    for host_path, container_path in (mounts or {}).items():
        argv += ["-v", f"{host_path}:{container_path}:ro,Z"]
    argv += [image, "sh", "-c", script]
    return subprocess.run(argv, capture_output=True, text=True, timeout=900)


def sh(cmd, timeout=900, check=False):
    r = subprocess.run(
        cmd, shell=isinstance(cmd, str), capture_output=True, text=True, timeout=timeout
    )
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd}\nrc={r.returncode}\n{r.stderr}")
    return r


def remote_sh(script, timeout=300):
    return sh(["ssh", "-o", "BatchMode=yes", REMOTE_SPEC, script], timeout)


def raw_remote_sh(script, timeout=300):
    return sh(["ssh", "-o", "BatchMode=yes", RAW_REMOTE_SPEC, script], timeout)


@dataclass
class Rig:
    """A real btrfs source and destination, plus remote scratch."""

    root: Path
    src: Path
    dst: Path
    raw: Path
    remote_base: str
    raw_remote_base: str
    payload: bytes
    #: Bytes written by the most recent mutate_source(). payload.bin is
    #: identical in every snapshot of the chain, so it can only prove that SOME
    #: snapshot arrived; this is the file that differs between the base and the
    #: increment, and therefore the only thing that can prove the second leg.
    delta: bytes = b""

    @property
    def source_volume(self) -> Path:
        return self.src / "data"

    def cli(self, *args, config: Path | None = None, timeout=900):
        exe = os.environ.get("BBNG_TEST_CLI") or shutil.which("btrfs-backup-ng")
        assert exe, "btrfs-backup-ng is not on PATH; set BBNG_TEST_CLI"
        argv = [exe]
        if config is not None:
            argv += ["-c", str(config)]
        return sh([*argv, *args], timeout=timeout)

    def write_config(
        self,
        path: Path,
        target_line: str,
        *,
        source="native",
        prefix="t3-",
        retention="daily = 5",
        snapper_config="",
        snapshot_dir="snapshots",
    ) -> Path:
        # snapshot_dir MUST be on the same btrfs as the source: a btrfs snapshot
        # cannot cross filesystems, and putting it on tmpfs fails at creation.
        #
        # Overridable because a snapshot name is <prefix><timestamp>: cells that
        # share this rig normally differ by prefix, so their names cannot clash.
        # Two cells using an EMPTY prefix produce byte-identical names and collide
        # on the second -- which is a true property of that configuration, not a
        # defect, and the reason those cells take their own directory.
        snap_dir = self.src / snapshot_dir
        snap_dir.mkdir(parents=True, exist_ok=True)
        volume = str(self.source_volume) if source == "native" else str(self.src)
        lines = [
            "[global]",
            f'snapshot_dir = "{snap_dir}"',
            "",
            "[global.retention]",
            retention,
            "",
            "[[volumes]]",
            f'path = "{volume}"',
            f'snapshot_prefix = "{prefix}"',
            f'source = "{source}"',
        ]
        if source == "snapper":
            lines += [
                "",
                "[volumes.snapper]",
                f'config_name = "{snapper_config}"',
                'min_age = "0s"',
            ]
        lines += ["", "[[volumes.targets]]", target_line, ""]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines))
        return path

    def mutate_source(self) -> bytes:
        """Change the source so the next backup has a genuine delta to send.

        Returns the delta bytes, and remembers them as ``self.delta``, so a
        caller can prove the INCREMENTAL leg actually landed. Without that a
        cell can only compare payload.bin, which this method deliberately does
        not touch and which is therefore byte-identical in every snapshot of
        the chain -- matching it proves a snapshot arrived, never which one.
        """
        self.delta = os.urandom(DELTA_BYTES)
        (self.source_volume / "generation").write_text(str(time.time()))
        (self.source_volume / "extra.bin").write_bytes(self.delta)
        return self.delta

    # -- effect checks: never trust the exit code -------------------------- #

    def local_btrfs_subvols(self, dest: Path) -> list[str]:
        r = sh(["btrfs", "subvolume", "list", "-o", str(dest)])
        return (
            [ln.split(" path ", 1)[-1] for ln in r.stdout.splitlines()]
            if r.returncode == 0
            else []
        )

    def remote_btrfs_subvols(self, dest: str) -> list[str]:
        r = remote_sh(f"sudo -n btrfs subvolume list -o '{dest}' 2>/dev/null")
        return [ln.split(" path ", 1)[-1] for ln in r.stdout.splitlines()]

    def local_raw_streams(self, dest: Path) -> list[str]:
        if not dest.is_dir():
            return []
        return sorted(
            p.name
            for p in dest.iterdir()
            if p.name.endswith(".btrfs") or ".btrfs." in p.name
        )

    def remote_raw_streams(self, dest: str) -> list[str]:
        r = remote_sh(f"ls -1 '{dest}' 2>/dev/null")
        return sorted(x for x in r.stdout.split() if x.endswith(".btrfs"))

    def foreign_raw_streams(self, dest: str) -> list[str]:
        r = raw_remote_sh(f"ls -1 '{dest}' 2>/dev/null")
        return sorted(x for x in r.stdout.split() if x.endswith(".btrfs"))

    def foreign_mode(self, path: str) -> str:
        """Permission bits of a file on the foreign target, portably.

        GNU `stat -c %a` first, then BSD/macOS `stat -f %Lp` -- the same order the
        product uses, and for the same reason: the hosts worth pointing this axis
        at are the ones without GNU coreutils.
        """
        r = raw_remote_sh(
            f"stat -c %a '{path}' 2>/dev/null || stat -f %Lp '{path}' 2>/dev/null"
        )
        return r.stdout.strip()


@functools.cache
def _foreign_home_base() -> str:
    """An ABSOLUTE rig path on the foreign target, resolved by its own shell.

    Not "/home/<user>": this axis exists to be pointed at hosts that are not
    Linux, and on macOS home is /Users/<user>. Not "$HOME/..." either -- the path
    is interpolated into single-quoted shell words and into a raw+ssh:// URL,
    neither of which expands it, so an unexpanded $HOME would have the teardown
    remove a directory literally named $HOME in the login directory.
    """
    home = raw_remote_sh('printf %s "$HOME"').stdout.strip()
    if not home.startswith("/"):
        raise RuntimeError(
            f"could not resolve the home directory on {RAW_REMOTE_SPEC!r} "
            f"(got {home!r}); refusing to guess a path a teardown will delete"
        )
    return f"{home}/bbng-tier3-raw"


def _rig_up(remote_base: str, raw_remote_base: str) -> Rig:
    _rig_down(remote_base, raw_remote_base)
    RIG_ROOT.mkdir(parents=True, exist_ok=True)
    payload = os.urandom(PAYLOAD_BYTES)

    for name in ("src", "dst"):
        img = RIG_ROOT / f"{name}.img"
        mnt = RIG_ROOT / name
        sh(["truncate", "-s", "2G", str(img)], check=True)
        sh(["mkfs.btrfs", "-q", str(img)], check=True)
        mnt.mkdir(parents=True, exist_ok=True)
        sh(["mount", "-o", "loop", str(img), str(mnt)], check=True)

    src, dst = RIG_ROOT / "src", RIG_ROOT / "dst"
    sh(["btrfs", "subvolume", "create", str(src / "data")], check=True)
    (src / "data" / "payload.bin").write_bytes(payload)
    (src / "data" / "dir").mkdir()
    (src / "data" / "dir" / "marker.txt").write_text("tier3\n")

    raw = RIG_ROOT / "raw"
    raw.mkdir(parents=True, exist_ok=True)

    # A destination used ONLY by the empty-prefix cells. An empty prefix is not
    # a filter, so a cell configured with one restores whatever is newest at its
    # destination -- including a prefixed sibling cell's snapshots when the
    # destination is shared. That made the increment unprovable there (payload
    # .bin is identical in every snapshot, so the payload check passed on
    # another cell's data). Isolating the destination is what lets those cells
    # prove they restored their OWN increment.
    (dst / "bare").mkdir(parents=True, exist_ok=True)

    if REMOTE_SPEC:
        remote_sh(
            f"rm -rf '{remote_base}'; "
            f"mkdir -p '{remote_base}/btrfs' '{remote_base}/btrfs-bare' "
            f"'{remote_base}/raw'"
        )
    if RAW_REMOTE_SPEC:
        raw_remote_sh(f"rm -rf '{raw_remote_base}'; mkdir -p '{raw_remote_base}/raw'")

    return Rig(
        root=RIG_ROOT,
        src=src,
        dst=dst,
        raw=raw,
        remote_base=remote_base,
        raw_remote_base=raw_remote_base,
        payload=payload,
    )


def _rig_down(remote_base: str, raw_remote_base: str) -> None:
    if RAW_REMOTE_SPEC:
        # Only the path this rig recorded at creation, removed by name. No
        # enumeration anywhere near a delete: deciding what to remove by listing
        # what is there is how a teardown reaches beyond its own rig.
        raw_remote_sh(f"rm -rf '{raw_remote_base}'")
    if REMOTE_SPEC:
        remote_sh(
            f"for d in $(find '{remote_base}' -maxdepth 4 -type d 2>/dev/null | tac); do "
            f'sudo -n btrfs subvolume delete "$d" >/dev/null 2>&1 || true; done; '
            f"rm -rf '{remote_base}'"
        )
    for name in ("dst", "src"):
        mnt = RIG_ROOT / name
        if not mnt.is_dir():
            continue
        # `btrfs subvolume list -o` is FILESYSTEM-wide, not path-scoped -- the trap
        # endpoint/ssh.py documents, and the one that cost this project a user's
        # ~/.ssh, ~/.gnupg and /home/.snapshots when an agent ran it against a rig
        # path. It is safe below ONLY because each rig is its own loopback
        # filesystem, so filesystem-wide and rig-wide are the same set. Confirm
        # that before believing it: if the mount is not up (mkfs failed, an
        # earlier run left the directory behind, BBNG_TEST_RIG points somewhere
        # unexpected) then this directory belongs to the HOST filesystem and the
        # listing would enumerate the host's subvolumes instead.
        if not os.path.ismount(mnt):
            shutil.rmtree(mnt, ignore_errors=True)
            continue
        r = sh(["btrfs", "subvolume", "list", "-o", str(mnt)])
        for line in reversed(r.stdout.splitlines()):
            rel = line.split(" path ", 1)[-1]
            sh(["btrfs", "subvolume", "delete", str(mnt / Path(rel).name)])
        for d in sorted(mnt.rglob("*"), reverse=True):
            if d.is_dir():
                sh(["btrfs", "subvolume", "delete", str(d)])
        sh(["umount", str(mnt)])
    shutil.rmtree(RIG_ROOT, ignore_errors=True)


@pytest.fixture(scope="module")
def rig():
    """A fresh rig per module, torn down unconditionally."""
    base = (
        f"/home/{REMOTE_SPEC.split('@')[0]}/bbng-tier3"
        if "@" in REMOTE_SPEC
        else "/tmp/bbng-tier3-remote"
    )
    raw_base = _foreign_home_base() if RAW_REMOTE_SPEC else ""
    r = _rig_up(base, raw_base)
    try:
        yield r
    finally:
        _rig_down(base, raw_base)


def _restored_roots(dest: Path) -> list[Path]:
    """Every directory a restore may have landed a subvolume in."""
    return [dest, *(p for p in sorted(dest.iterdir()) if p.is_dir())]


def _restored_listing(dest: Path) -> list[str]:
    return sorted(
        str(f.relative_to(dest)) for r in _restored_roots(dest) for f in r.iterdir()
    )


def assert_payload_restored(dest: Path, expected: bytes) -> None:
    """A restore counts only when the source bytes are actually present.

    The .btrfs-backup-ng bookkeeping directory is created regardless, so
    "the directory is not empty" reports success for a restore that moved no
    data -- observed on master returning rc=0 with exactly that.

    EVERY restored root is checked, not just the first match. Returning on the
    first meant a restore that delivered one good subvolume and one corrupt one
    passed, and under --all that is precisely the interesting case.
    """
    assert dest.exists(), f"restore produced no destination at {dest}"
    found = 0
    for base in _restored_roots(dest):
        candidate = base / "payload.bin"
        if candidate.is_file():
            actual = candidate.read_bytes()
            assert actual == expected, (
                f"restored payload at {candidate} differs from the source: "
                f"{len(actual)} bytes vs {len(expected)} expected"
            )
            found += 1
    if not found:
        raise AssertionError(
            f"no payload.bin anywhere under {dest}; restore moved no data. "
            f"Contents: {_restored_listing(dest)}"
        )


def assert_increment_restored(
    dest: Path, expected_delta: bytes, context: str = ""
) -> None:
    """The INCREMENTAL snapshot landed, not merely the base it descends from.

    This is the assertion the suite did not have. payload.bin is written once
    at rig setup and never changed, so it is identical in the base and in every
    increment: assert_payload_restored cannot tell a restore that delivered the
    whole chain from one that delivered the parent and silently dropped the
    delta. extra.bin exists only after mutate_source, so its presence AND its
    content are what prove the second leg arrived.

    EVERY restored root is searched, and the check passes if ANY holds the
    delta. Restoring an incremental snapshot necessarily delivers its parent
    too, so the destination legitimately holds both; an earlier version stopped
    at the first extra.bin it found, which under a shared rig is the PARENT
    carrying a previous cell's delta -- a false failure that accused the
    product of dropping an increment it had in fact delivered.
    """
    assert dest.exists(), f"restore produced no destination at {dest}"
    assert expected_delta, "no delta recorded; mutate_source() was never called"
    seen = []
    for base in _restored_roots(dest):
        candidate = base / "extra.bin"
        if candidate.is_file():
            if candidate.read_bytes() == expected_delta:
                return
            seen.append(str(candidate.relative_to(dest)))
    raise AssertionError(
        f"the increment's bytes are nowhere under {dest}. The delta written "
        f"immediately before the second backup is in none of the restored "
        f"roots, so the restore delivered the parent and dropped the increment. "
        f"extra.bin present but stale at: {seen or 'nowhere'}. "
        f"Contents: {_restored_listing(dest)}" + context
    )
