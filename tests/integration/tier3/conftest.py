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
import itertools
import os
import secrets
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


def _have_snapper() -> bool:
    """snapper is on the RUNNER: the config lives on the source machine, and
    a btrfs target only needs the `btrfs receive` it already permits."""
    if shutil.which("snapper") is None:
        return False
    r = subprocess.run(
        ["snapper", "--version"], capture_output=True, timeout=30, check=False
    )
    return r.returncode == 0


requires_snapper = pytest.mark.skipif(
    _Deferred(lambda: not _have_snapper()),
    reason="Tier 3 snapper cells need snapper on the runner",
)

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


def sh(cmd, timeout=900, check=False, env=None):
    r = subprocess.run(
        cmd,
        shell=isinstance(cmd, str),
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
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
    #: A third btrfs filesystem holding neither the originals nor the backups:
    #: the recovery media of a disaster-recovery restore. `btrfs receive`
    #: resolves an incremental stream's parent by uuid across the WHOLE
    #: filesystem it lands on, so a restore into src or dst would find the
    #: original (or the backup) and silently succeed where real recovery media
    #: has nothing to offer. Only a filesystem with nothing on it can prove a
    #: chain rebuilds from the restored base.
    rec: Path
    raw: Path
    remote_base: str
    raw_remote_base: str
    payload: bytes
    #: Bytes written by the most recent mutate_source(). payload.bin is
    #: identical in every snapshot of the chain, so it can only prove that SOME
    #: snapshot arrived; this is the file that differs between the base and the
    #: increment, and therefore the only thing that can prove the second leg.
    delta: bytes = b""
    #: Unique per rig. Anything this rig registers OUTSIDE its own loopback
    #: filesystems -- a snapper config, which lives in /etc/snapper -- carries
    #: it, so two runs cannot collide and a teardown can only ever name what
    #: this rig made.
    suffix: str = ""

    @property
    def source_volume(self) -> Path:
        return self.src / "data"

    def cli(self, *args, config: Path | None = None, timeout=900, env=None):
        """Run the installed CLI. ``env`` replaces the whole environment: an
        encrypted cell hands the product a throwaway GNUPGHOME or passphrase
        this way, and the wrong-key cells hand it the wrong one."""
        exe = os.environ.get("BBNG_TEST_CLI") or shutil.which("btrfs-backup-ng")
        assert exe, "btrfs-backup-ng is not on PATH; set BBNG_TEST_CLI"
        argv = [exe]
        if config is not None:
            argv += ["-c", str(config)]
        return sh([*argv, *args], timeout=timeout, env=env)

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


#: The rig's own filesystems, in the order they are torn down (reverse of
#: creation). Anything else mounted under RIG_ROOT belongs to a cell.
_RIG_FILESYSTEMS = ("src", "dst", "rec")


def loop_fs_up(name: str) -> Path:
    """A fresh btrfs filesystem on a sparse loopback image, mounted at
    RIG_ROOT/<name>. The rig's three come from here, and so does the virgin
    filesystem a disaster-recovery cell needs for itself."""
    RIG_ROOT.mkdir(parents=True, exist_ok=True)
    img = RIG_ROOT / f"{name}.img"
    mnt = RIG_ROOT / name
    sh(["truncate", "-s", "2G", str(img)], check=True)
    sh(["mkfs.btrfs", "-q", str(img)], check=True)
    mnt.mkdir(parents=True, exist_ok=True)
    sh(["mount", "-o", "loop", str(img), str(mnt)], check=True)
    return mnt


def loop_fs_down(name: str) -> None:
    """Delete every subvolume on the filesystem, unmount it, remove the image."""
    mnt = RIG_ROOT / name
    if mnt.is_dir():
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
        else:
            r = sh(["btrfs", "subvolume", "list", "-o", str(mnt)])
            for line in reversed(r.stdout.splitlines()):
                rel = line.split(" path ", 1)[-1]
                sh(["btrfs", "subvolume", "delete", str(mnt / Path(rel).name)])
            for d in sorted(mnt.rglob("*"), reverse=True):
                if d.is_dir():
                    sh(["btrfs", "subvolume", "delete", str(d)])
            sh(["umount", str(mnt)])
            # Only once it is no longer a mount: an rmtree into a filesystem
            # that refused to unmount would be an rmtree of its contents.
            if not os.path.ismount(mnt):
                shutil.rmtree(mnt, ignore_errors=True)
    (RIG_ROOT / f"{name}.img").unlink(missing_ok=True)


def _rig_up(remote_base: str, raw_remote_base: str) -> Rig:
    _rig_down(remote_base, raw_remote_base)
    RIG_ROOT.mkdir(parents=True, exist_ok=True)
    payload = os.urandom(PAYLOAD_BYTES)

    for name in _RIG_FILESYSTEMS:
        loop_fs_up(name)

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
        rec=RIG_ROOT / "rec",
        raw=raw,
        remote_base=remote_base,
        raw_remote_base=raw_remote_base,
        payload=payload,
        suffix=secrets.token_hex(4),
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
    for name in reversed(_RIG_FILESYSTEMS):
        loop_fs_down(name)
    # A cell's own filesystem (a virgin recovery fs) is torn down by the
    # cell's finalizer; only a run killed outright can leave one mounted,
    # and the rmtree below must not be what meets it.
    if RIG_ROOT.is_dir():
        for img in RIG_ROOT.glob("*.img"):
            loop_fs_down(img.stem)
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


def subvolumes_under(dest: Path) -> list[Path]:
    """Every subvolume a restore may have landed under ``dest``, by inode.

    Path-scoped on purpose: `btrfs subvolume list -o` enumerates the containing
    subvolume, so aimed at a directory on the rig's source filesystem it
    returns every sibling cell's snapshots and can never say "nothing here".
    The root inode of a btrfs subvolume is always 256, which is a fact about
    the filesystem and not about the tool being tested.
    """
    if not dest.is_dir():
        return []
    return [p for p in _restored_roots(dest) if p.stat().st_ino == 256]


def assert_nothing_restored(dest: Path, context: str = "") -> None:
    """A refused restore must leave no data behind -- not a subvolume, not a
    payload, not a partial one. A destination that fails closed and still
    holds a received subvolume would be reported as a failure by the exit
    code and then found "restored" by whoever looks, which is worse than
    either alone."""
    if not dest.exists():
        return
    landed = subvolumes_under(dest)
    assert not landed, (
        f"the restore was refused but {[str(p) for p in landed]} exist under "
        f"{dest}: something was received anyway.{context}"
    )
    for base in _restored_roots(dest):
        assert not (base / "payload.bin").exists(), (
            f"the restore was refused but {base / 'payload.bin'} exists.{context}"
        )


def lifecycle(
    rig,
    config,
    *,
    location,
    prefix,
    extra_args=(),
    restore_args=(),
    env=None,
    snapper=False,
):
    """Backup, incremental, verify, prune, restore -- asserting on effects.

    The restore half asserts here rather than at the call site, deliberately.
    It used to return a dict and leave every check to the caller, and the
    result was that the restore's return code was discarded outright and
    ``incremental_rc`` was recorded by this function and asserted by no cell at
    all -- so a restore that failed, or that delivered the parent and dropped
    the increment, passed every cell in the matrix. A proof a caller can forget
    to demand is not a proof.

    ``extra_args`` go to verify AND restore (the ssh options both need);
    ``restore_args`` go to the restore alone, for options verify does not take
    such as the transport compression of the restore leg. ``env`` is handed to
    every CLI call, so an encrypted cell's throwaway keyring or passphrase
    reaches the product the way an operator's would.
    """
    results = {}

    r = rig.cli("run", config=config, env=env)
    results["backup_rc"] = r.returncode
    results["backup_out"] = (r.stdout + r.stderr)[-2000:]

    delta = rig.mutate_source()
    r2 = rig.cli("run", config=config, env=env)
    results["incremental_rc"] = r2.returncode
    results["incremental_out"] = (r2.stdout + r2.stderr)[-2000:]

    rv = rig.cli("verify", location, "--prefix", prefix, *extra_args, env=env)
    results["verify_rc"] = rv.returncode
    results["verify_out"] = (rv.stdout + rv.stderr)[-2000:]

    before = rig.cli("list", config=config, env=env)
    results["listed_before_prune"] = (before.stdout + before.stderr)[-1500:]
    rp = rig.cli("prune", "--yes", config=config, env=env)
    results["prune_rc"] = rp.returncode
    results["prune_out"] = (rp.stdout + rp.stderr)[-1500:]
    after = rig.cli("list", config=config, env=env)
    results["listed_after_prune"] = (after.stdout + after.stderr)[-1500:]

    # Keyed by the config stem, not the prefix: the rig is module-scoped and
    # two cells legitimately share an empty prefix, so a prefix-derived name
    # aimed both of them at the same directory and let the second pass on the
    # first one's restored artifacts.
    dest = rig.src / f"restored-{config.stem}"
    rr = rig.cli(
        "restore",
        location,
        str(dest),
        "--prefix",
        prefix,
        "--yes-i-know-what-i-am-doing",
        *extra_args,
        *restore_args,
        env=env,
    )
    results["restore_rc"] = rr.returncode
    results["restore_out"] = (rr.stdout + rr.stderr)[-2000:]
    results["restore_dest"] = dest
    results["delta"] = delta

    assert results["incremental_rc"] == 0, results["incremental_out"]
    # verify and prune join the in-lifecycle asserts for the same reason the
    # restore did: verify_rc was asserted by three cells and forgotten by four
    # -- raw+ssh, the target whose broken restore hid through 0.9.6, among
    # them -- and prune_rc by one. verify exits 2 on zero matching snapshots
    # (core/verify.py), so rc 0 proves snapshots were found AND verified.
    assert results["verify_rc"] == 0, results["verify_out"]
    assert results["prune_rc"] == 0, results["prune_out"]
    assert results["restore_rc"] == 0, results["restore_out"]
    assert_payload_restored(dest, rig.payload)
    assert_increment_restored(
        dest,
        delta,
        context=(
            f"\n\n--- listed BEFORE prune ---\n{results['listed_before_prune']}"
            f"\n--- prune rc={results['prune_rc']} ---\n{results['prune_out']}"
            f"\n--- listed AFTER prune ---\n{results['listed_after_prune']}"
        ),
    )
    return results


@dataclass
class SnapperSource:
    """A snapper config registered on the runner for a subvolume of the rig."""

    name: str
    subvol: Path

    def slots(self) -> list[int]:
        """The numbered snapshots snapper holds for this config, on disk.

        Read from the filesystem, not from `snapper list`: snapperd caches its
        view and does not see a slot a restore created out of band until it
        rescans, and the question here is what a restore LEFT, not what the
        daemon has noticed.
        """
        snaps = self.subvol / ".snapshots"
        if not snaps.is_dir():
            return []
        return sorted(int(p.name) for p in snaps.iterdir() if p.name.isdigit())

    def slot(self, number: int) -> Path:
        return self.subvol / ".snapshots" / str(number) / "snapshot"

    def take(self, description: str) -> int:
        """Take a snapper snapshot and return the number snapper assigned.

        Never assumed: the config is shared by every cell in the module, so
        the second cell's snapshots are not 1 and 2."""
        r = sh(
            ["snapper", "-c", self.name, "create", "-d", description, "--print-number"],
            check=True,
        )
        return int(r.stdout.strip())

    def mutate(self) -> bytes:
        """A genuine delta in the snapper-managed subvolume; returned so a
        cell can prove the incremental leg landed, not merely a snapshot."""
        delta = os.urandom(DELTA_BYTES)
        (self.subvol / "extra.bin").write_bytes(delta)
        return delta


def snapper_config_down(name: str, subvol: Path) -> None:
    """Remove one snapper config and the subvolume it managed, by name.

    Never enumerates configs; only the one the caller named. Order matters:
    `snapper delete-config` FIRST, while the subvolume still exists --
    deleting the subvolume first leaves the config registered in snapperd
    with a path that is gone, and `delete-config` then refuses. Then what
    snapper (or a restore) nested in the subvolume, deepest first, each by a
    path inside the subvolume the caller created.
    """
    sh(["snapper", "-c", name, "delete-config"])
    for snap in sorted((subvol / ".snapshots").glob("*/snapshot"), reverse=True):
        sh(["btrfs", "subvolume", "delete", str(snap)])
    for path in (subvol / ".snapshots", subvol):
        if path.exists():
            sh(["btrfs", "subvolume", "delete", str(path)])


def snapper_config_up(
    request, name: str, subvol: Path, payload: bytes | None
) -> SnapperSource:
    """Register a snapper config on a fresh subvolume, teardown first.

    The teardown is a finalizer registered BEFORE anything is created, so it
    runs whether the cells passed, failed mid-run, or the setup itself broke
    halfway. ``payload`` seeds the subvolume; None leaves it empty, which is
    what recovery media looks like.
    """
    request.addfinalizer(lambda: snapper_config_down(name, subvol))
    sh(["btrfs", "subvolume", "create", str(subvol)], check=True)
    if payload is not None:
        (subvol / "payload.bin").write_bytes(payload)
    sh(["snapper", "-c", name, "create-config", str(subvol)], check=True)
    return SnapperSource(name=name, subvol=subvol)


@pytest.fixture(scope="module")
def snapper_source(rig, request):
    """A snapper config on its own subvolume of the rig's source filesystem.

    The config name carries the rig's suffix and a fixed test namespace, so
    no run can collide with another and the teardown can never name a
    production config.
    """
    return snapper_config_up(
        request, f"bbngt3{rig.suffix}", rig.src / "snapdata", rig.payload
    )


_chain_counter = itertools.count(1)


@pytest.fixture
def snapper_chain(rig, request):
    """A snapper config for ONE cell, holding exactly the snapshots it takes.

    `snapper backup` syncs every snapshot of a config to the target, so a
    restore of `--all` from a target fed by the module-scoped source brings
    back every earlier cell's snapshots too and the chain under test cannot
    be told apart from the accumulation. A cell that judges a whole chain
    gets a config of its own: base, delta, nothing else.
    """
    n = next(_chain_counter)
    return snapper_config_up(
        request, f"bbngt3c{n}{rig.suffix}", rig.src / f"snapdata-{n}", rig.payload
    )


@pytest.fixture(scope="module")
def snapper_recovery(rig, request):
    """The config a snapper restore lands in: empty, on the recovery
    filesystem, sharing nothing with the source or the backups. Module-scoped
    like the source, so the cells of one module accumulate slots here and
    each judges only the slots it added."""
    return snapper_config_up(request, f"bbngt3r{rig.suffix}", rig.rec / "recover", None)


@dataclass
class GpgIdentity:
    """A throwaway keypair in its own GNUPGHOME, and a stranger's beside it.

    Never the operator's ~/.gnupg: the runner is root under sudo and a key
    generated there would outlive the run. ``stranger`` holds a different key
    and none of the secret material, so a restore pointed at it is the
    wrong-key case exactly as an operator without the key would meet it.
    """

    home: Path
    recipient: str
    stranger: Path

    def env(self, home: Path | None = None) -> dict[str, str]:
        return {**os.environ, "GNUPGHOME": str(home or self.home)}


def _gpg_home_up(home: Path, uid: str) -> None:
    home.mkdir(mode=0o700, parents=True)
    sh(
        [
            "gpg",
            "--batch",
            "--pinentry-mode",
            "loopback",
            "--passphrase",
            "",
            "--quick-generate-key",
            uid,
            "default",
            "default",
            "never",
        ],
        check=True,
        env={**os.environ, "GNUPGHOME": str(home)},
    )


def _gpg_home_down(home: Path) -> None:
    # The agent for this home outlives the run otherwise; matched by homedir,
    # so the operator's real agent is never touched.
    sh(["gpgconf", "--homedir", str(home), "--kill", "all"])
    shutil.rmtree(home, ignore_errors=True)


@pytest.fixture(scope="module")
def gpg_identity(rig, request):
    if shutil.which("gpg") is None or shutil.which("gpgconf") is None:
        pytest.skip("Tier 3 gpg cells need gpg and gpgconf on the runner")
    home = rig.root / "gnupg"
    stranger = rig.root / "gnupg-stranger"
    request.addfinalizer(lambda: _gpg_home_down(home))
    request.addfinalizer(lambda: _gpg_home_down(stranger))
    uid = f"bbng tier3 {rig.suffix} <t3-{rig.suffix}@example.invalid>"
    _gpg_home_up(home, uid)
    _gpg_home_up(stranger, f"stranger {rig.suffix} <no-{rig.suffix}@example.invalid>")
    return GpgIdentity(
        home=home, recipient=f"t3-{rig.suffix}@example.invalid", stranger=stranger
    )


def snapper_lifecycle(rig, snap, target, *, extra_args=()):
    """Base backup, a real delta, an incremental backup -- each rc asserted
    here, and the increment proven by its bytes at the destination, never
    by a snapshot merely having arrived.

    This is the cell 0.9.7 did not have. A snapper backup to a btrfs target
    receives into `.snapshots/<n>.incoming` and publishes it as
    `.snapshots/<n>`; that slot had only ever existed as a side effect of
    the `mkdir -p` the endpoints ran on any path a receive was pointed at,
    and removing those (a backup location is never created) removed the slot
    with them. The engine then refused the missing slot and every snapper
    backup to btrfs failed, while every unit test mocked the send.
    """
    results = {}
    base_num = snap.take("base")
    r1 = rig.cli("snapper", "backup", snap.name, target, *extra_args)
    results["backup_rc"] = r1.returncode
    results["backup_out"] = (r1.stdout + r1.stderr)[-2000:]
    assert results["backup_rc"] == 0, results["backup_out"]

    delta = snap.mutate()
    delta_num = snap.take("delta")
    r2 = rig.cli("snapper", "backup", snap.name, target, *extra_args)
    results["incremental_rc"] = r2.returncode
    results["incremental_out"] = (r2.stdout + r2.stderr)[-3000:]
    assert results["incremental_rc"] == 0, results["incremental_out"]
    # The console renderer wraps long lines, so the log is compared with its
    # whitespace collapsed.
    flat = " ".join(results["incremental_out"].split())
    assert f"incremental from {base_num}" in flat, (
        "the second backup was not sent as an increment of the first\n"
        + results["incremental_out"]
    )
    results["delta"] = delta
    results["numbers"] = (base_num, delta_num)
    return results
