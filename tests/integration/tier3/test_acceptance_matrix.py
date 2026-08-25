"""Does each advertised feature actually work, against each advertised target?

One test per (source, target) pair, each driving the real CLI through a full
lifecycle -- backup, incremental, verify, prune, restore -- and asserting on the
destination rather than the exit code.

Baseline measured against master @33db51f on real hardware:

    native  -> local btrfs   all operations work
    native  -> ssh://        all operations work
    native  -> raw://        all operations work
    native  -> raw+ssh://    works (no ssh_sudo; a raw target needs no privilege)
    native  -> raw+ssh://    BROKEN when ssh_sudo is set: `sudo mkdir -p` refused
    snapper -> local btrfs   backup works
    snapper -> raw://        backup works
    snapper -> ssh://        BROKEN: `sudo sh` refused

Both broken cases share one cause: shelling out to a NON-btrfs binary under
sudo. The remote sudoers policy this project documents grants /usr/bin/btrfs
and nothing else, so `sudo mkdir` and `sudo sh` are refused.

Note on ssh_sudo: it is required for ssh:// (btrfs receive needs root) and
unnecessary for raw:// and raw+ssh:// (writing a stream file does not). Setting
it on a raw target is what breaks that target -- which is a defect in how the
option is handled, not a broken transport. An earlier draft of this file marked
plain raw+ssh as broken; that was a harness error, not a product one.

They are marked xfail(strict=True) rather than left failing, so this file stays
green while remaining an accurate inventory: the moment a fix makes one pass,
XPASS fails the run and the marker must be removed. Do not relax a marker to
make a run green -- that is the failure mode this suite exists to prevent.
"""

from __future__ import annotations

import pytest

from .conftest import assert_payload_restored, requires_local, requires_remote

pytestmark = [pytest.mark.tier3, requires_local]

BROKEN_SUDO_MKDIR = (
    "raw+ssh: remote mkdir runs under sudo, which a btrfs-only sudoers policy refuses"
)
BROKEN_SUDO_SH = "snapper over ssh: the slot script runs under `sudo sh`, which a btrfs-only sudoers policy refuses"


def _lifecycle(rig, config, *, location, prefix, extra_args=(), snapper=False):
    """Backup, incremental, verify, prune, restore -- asserting on effects."""
    results = {}

    r = rig.cli("run", config=config)
    results["backup_rc"] = r.returncode
    results["backup_out"] = (r.stdout + r.stderr)[-2000:]

    rig.mutate_source()
    r2 = rig.cli("run", config=config)
    results["incremental_rc"] = r2.returncode

    rv = rig.cli("verify", location, "--prefix", prefix, *extra_args)
    results["verify_rc"] = rv.returncode
    results["verify_out"] = (rv.stdout + rv.stderr)[-2000:]

    rp = rig.cli("prune", "--yes", config=config)
    results["prune_rc"] = rp.returncode

    dest = rig.src / f"restored-{prefix.strip('-')}"
    rig.cli(
        "restore",
        location,
        str(dest),
        "--prefix",
        prefix,
        "--yes-i-know-what-i-am-doing",
        *extra_args,
    )
    results["restore_dest"] = dest
    return results


# --------------------------------------------------------------------------- #
# native source
# --------------------------------------------------------------------------- #
class TestNativeSource:
    def test_local_btrfs(self, rig):
        cfg = rig.write_config(
            rig.root / "cfg-native-local.toml", f'path = "{rig.dst}"', prefix="t3loc-"
        )
        res = _lifecycle(rig, cfg, location=str(rig.dst), prefix="t3loc-")

        assert res["backup_rc"] == 0, res["backup_out"]
        assert rig.local_btrfs_subvols(rig.dst), "backup produced no subvolume"
        assert res["verify_rc"] == 0, res["verify_out"]
        assert res["prune_rc"] == 0
        assert_payload_restored(res["restore_dest"], rig.payload)

    def test_raw_local(self, rig):
        cfg = rig.write_config(
            rig.root / "cfg-native-raw.toml",
            f'path = "raw://{rig.raw}"',
            prefix="t3raw-",
        )
        res = _lifecycle(rig, cfg, location=f"raw://{rig.raw}", prefix="t3raw-")

        assert res["backup_rc"] == 0, res["backup_out"]
        assert rig.local_raw_streams(rig.raw), "backup produced no stream file"
        assert res["verify_rc"] == 0, res["verify_out"]
        assert_payload_restored(res["restore_dest"], rig.payload)

    @requires_remote
    def test_ssh(self, rig):
        from .conftest import REMOTE_SPEC

        loc = f"ssh://{REMOTE_SPEC}:{rig.remote_base}/btrfs"
        cfg = rig.write_config(
            rig.root / "cfg-native-ssh.toml",
            f'path = "{loc}"\nssh_sudo = true',
            prefix="t3ssh-",
        )
        res = _lifecycle(
            rig, cfg, location=loc, prefix="t3ssh-", extra_args=("--ssh-sudo",)
        )

        assert res["backup_rc"] == 0, res["backup_out"]
        assert rig.remote_btrfs_subvols(f"{rig.remote_base}/btrfs"), (
            "nothing landed remotely"
        )
        assert res["verify_rc"] == 0, res["verify_out"]
        assert_payload_restored(res["restore_dest"], rig.payload)

    @requires_remote
    def test_raw_over_ssh(self, rig):
        """A raw target writes a plain file, so it needs no remote privilege.

        Deliberately no ssh_sudo: the destination is an ordinary directory owned
        by the connecting user, and nothing in the raw path requires root.
        """
        from .conftest import REMOTE_SPEC

        loc = f"raw+ssh://{REMOTE_SPEC}:{rig.remote_base}/raw"
        cfg = rig.write_config(
            rig.root / "cfg-native-rawssh.toml", f'path = "{loc}"', prefix="t3rsh-"
        )
        res = _lifecycle(rig, cfg, location=loc, prefix="t3rsh-")

        assert res["backup_rc"] == 0, res["backup_out"]
        assert rig.remote_raw_streams(f"{rig.remote_base}/raw"), (
            "nothing landed remotely"
        )
        assert_payload_restored(res["restore_dest"], rig.payload)

    @requires_remote
    @pytest.mark.xfail(reason=BROKEN_SUDO_MKDIR, strict=True)
    def test_raw_over_ssh_with_ssh_sudo(self, rig):
        """Setting ssh_sudo on a raw target must not break it.

        A raw target needs no privilege, but ssh_sudo is a documented target
        option and an operator may reasonably set it. When they do, _prepare
        runs `sudo mkdir -p` (endpoint/raw.py), which the documented btrfs-only
        sudoers policy refuses, and the backup fails with "sudo: a password is
        required" -- an error that says nothing about the actual cause. Either
        the raw path should not elevate mkdir, or the option should be rejected
        for raw targets with an explanation.
        """
        from .conftest import REMOTE_SPEC

        loc = f"raw+ssh://{REMOTE_SPEC}:{rig.remote_base}/raw"
        cfg = rig.write_config(
            rig.root / "cfg-native-rawssh-sudo.toml",
            f'path = "{loc}"\nssh_sudo = true',
            prefix="t3rss-",
        )
        r = rig.cli("run", config=cfg)

        assert r.returncode == 0, (r.stdout + r.stderr)[-800:]
        assert rig.remote_raw_streams(f"{rig.remote_base}/raw"), "nothing landed"


# --------------------------------------------------------------------------- #
# restore honesty -- independent of any one target
# --------------------------------------------------------------------------- #
class TestRestoreReportsHonestly:
    @pytest.mark.xfail(
        reason="restore exits 0 when it restored nothing; observed on master with "
        "only a .btrfs-backup-ng bookkeeping directory created",
        strict=True,
    )
    def test_restoring_from_an_empty_location_is_not_success(self, rig, tmp_path):
        """An empty backup location must not produce a successful restore.

        A DR script that trusts the exit code would record a recovery that never
        happened -- the worst possible failure for a backup tool.
        """
        empty = rig.root / "empty-location"
        empty.mkdir(parents=True, exist_ok=True)
        dest = rig.src / "restored-from-empty"

        r = rig.cli(
            "restore",
            str(empty),
            str(dest),
            "--prefix",
            "nothing-",
            "--yes-i-know-what-i-am-doing",
        )

        assert r.returncode != 0, (
            f"restore exited {r.returncode} having restored nothing from an empty "
            f"location; output: {(r.stdout + r.stderr)[-500:]}"
        )
