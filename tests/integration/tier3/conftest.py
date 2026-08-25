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
RIG_ROOT = Path(os.environ.get("BBNG_TEST_RIG", "/tmp/bbng-tier3"))
PAYLOAD_BYTES = 2 * 1024 * 1024


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


def sh(cmd, timeout=900, check=False):
    r = subprocess.run(
        cmd, shell=isinstance(cmd, str), capture_output=True, text=True, timeout=timeout
    )
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd}\nrc={r.returncode}\n{r.stderr}")
    return r


def remote_sh(script, timeout=300):
    return sh(["ssh", "-o", "BatchMode=yes", REMOTE_SPEC, script], timeout)


@dataclass
class Rig:
    """A real btrfs source and destination, plus remote scratch."""

    root: Path
    src: Path
    dst: Path
    raw: Path
    remote_base: str
    payload: bytes

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
    ) -> Path:
        # snapshot_dir MUST be on the same btrfs as the source: a btrfs snapshot
        # cannot cross filesystems, and putting it on tmpfs fails at creation.
        snap_dir = self.src / "snapshots"
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

    def mutate_source(self) -> None:
        """Change the source so the next backup has a genuine delta to send."""
        (self.source_volume / "generation").write_text(str(time.time()))
        sh(
            [
                "dd",
                "if=/dev/urandom",
                f"of={self.source_volume}/extra.bin",
                "bs=1M",
                "count=1",
                "status=none",
            ]
        )

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


def _rig_up(remote_base: str) -> Rig:
    _rig_down(remote_base)
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

    if REMOTE_SPEC:
        remote_sh(
            f"rm -rf '{remote_base}'; mkdir -p '{remote_base}/btrfs' '{remote_base}/raw'"
        )

    return Rig(
        root=RIG_ROOT,
        src=src,
        dst=dst,
        raw=raw,
        remote_base=remote_base,
        payload=payload,
    )


def _rig_down(remote_base: str) -> None:
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
    r = _rig_up(base)
    try:
        yield r
    finally:
        _rig_down(base)


def assert_payload_restored(dest: Path, expected: bytes) -> None:
    """A restore counts only when the source bytes are actually present.

    The .btrfs-backup-ng bookkeeping directory is created regardless, so
    "the directory is not empty" reports success for a restore that moved no
    data -- observed on master returning rc=0 with exactly that.
    """
    assert dest.exists(), f"restore produced no destination at {dest}"
    for base in [dest, *(p for p in dest.iterdir() if p.is_dir())]:
        candidate = base / "payload.bin"
        if candidate.is_file():
            actual = candidate.read_bytes()
            assert actual == expected, (
                f"restored payload differs from the source: {len(actual)} bytes vs "
                f"{len(expected)} expected"
            )
            return
    listing = sorted(p.name for p in dest.iterdir())
    raise AssertionError(
        f"no payload.bin anywhere under {dest}; restore moved no data. Contents: {listing}"
    )
