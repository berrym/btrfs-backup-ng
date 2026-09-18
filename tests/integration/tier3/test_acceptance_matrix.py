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
it on a raw target used to break that target; it now elevates only where
elevation is actually needed, so the option is safe to set anywhere. An earlier
draft of this file marked plain raw+ssh as broken; that was a harness error,
not a product one.

Known-broken cells are marked xfail(strict=True) rather than left failing, so
this file stays green while remaining an accurate inventory: the moment a fix
makes one pass, XPASS fails the run and the marker must be removed. Do not
relax a marker to make a run green -- that is the failure mode this suite
exists to prevent. There are currently no such cells.
"""

from __future__ import annotations

import shlex

import pytest

from .conftest import (
    requires_container,
    assert_payload_restored,
    requires_local,
    requires_raw_remote,
    requires_remote,
)

pytestmark = [pytest.mark.tier3, requires_local]

#: NOT a marker on any cell -- an untested hypothesis, recorded so it is not
#: mistaken for covered ground. The nearest real code is ssh.py's compressed
#: receive, which under ssh_sudo WITHOUT passwordless sudo elevates a shell
#: (`sudo -S sh -c "<decompressor> | btrfs receive"`) rather than the btrfs
#: binary, and a strictly btrfs-only sudoers policy would refuse that. The
#: matrix cannot reach it: the branch is taken only when `sudo -n btrfs` fails
#: on the remote, and the host this suite runs against has passwordless sudo
#: for btrfs. Proving or disproving it needs a password-sudo host.
UNTESTED_SUDO_SH = (
    "ssh:// with compression and ssh_sudo but no passwordless sudo elevates "
    "`sh`, not `btrfs`; unverified against a btrfs-only sudoers policy"
)


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
    def test_raw_over_ssh_with_ssh_sudo(self, rig):
        """Setting ssh_sudo on a raw target must not break it.

        A raw target needs no privilege, but ssh_sudo is a documented target
        option and an operator may reasonably set it. _prepare used to run every
        remote command under sudo, starting with `mkdir -p`, which the btrfs-only
        sudoers policy the README documents refuses -- so a valid config failed
        against the very policy the project tells people to install.

        ssh_sudo now means "elevate if elevation is needed": the destination is
        probed as the login user first, and when it is writable no file
        operation elevates. This ran as an xfail against a btrfs-only sudoers
        policy until that landed.
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
# an explicit empty snapshot prefix
# --------------------------------------------------------------------------- #
class TestAnExplicitEmptyPrefix:
    """snapshot_prefix = "" is a supported choice: bare-timestamp names.

    Issue #6. The config layer honours it and the reporter was told so, but
    restore's prefix inference treated "" as "nobody said" -- the only falsy
    prefix -- and silently replaced it. Pinned here on real hardware because it
    is the whole lifecycle that has to work under it, not just the parser: the
    snapshot has to be CREATED with a bare name, transferred, found again at the
    destination, and restored.
    """

    def test_the_whole_cycle_under_a_bare_timestamp_name(self, rig):
        cfg = rig.write_config(
            rig.root / "cfg-empty-prefix.toml",
            f'path = "{rig.dst}"',
            prefix="",
            snapshot_dir="snapshots-bare-local",
        )
        res = _lifecycle(rig, cfg, location=str(rig.dst), prefix="")

        assert res["backup_rc"] == 0, res["backup_out"]

        # At least one BARE-timestamp name landed. Not "every name here is
        # bare": the rig is shared, so other cells' prefixed snapshots sit at
        # this same destination, and with an empty prefix there is no prefix to
        # filter them out by -- which is the point of the configuration.
        #
        # Basenames: both subvol helpers return the `path` field of
        # `btrfs subvolume list`, which is a path relative to the filesystem
        # root ("@home/mberry/.../t3ssh-...") rather than a bare name.
        landed = [n.rsplit("/", 1)[-1] for n in rig.local_btrfs_subvols(rig.dst)]
        assert landed, "nothing landed at the destination"
        bare = [n for n in landed if n[:1].isdigit()]
        assert bare, (
            f"no bare-timestamp name at the destination, only {landed} -- an "
            "empty prefix was replaced by a derived one"
        )

        assert_payload_restored(res["restore_dest"], rig.payload)

    @requires_remote
    def test_the_same_over_ssh(self, rig):
        from .conftest import REMOTE_SPEC

        loc = f"ssh://{REMOTE_SPEC}:{rig.remote_base}/btrfs"
        cfg = rig.write_config(
            rig.root / "cfg-empty-prefix-ssh.toml",
            # ssh_sudo, as every other ssh:// cell does: btrfs on the remote
            # needs root for both `subvolume list` and `receive`, which is what
            # the README's NOPASSWD sudoers entry is for. Without it this cell
            # was exercising an unsupported configuration and failing on that
            # rather than on the prefix.
            f'path = "{loc}"\nssh_sudo = true',
            prefix="",
            snapshot_dir="snapshots-bare-ssh",
        )
        res = _lifecycle(rig, cfg, location=loc, prefix="")

        assert res["backup_rc"] == 0, res["backup_out"]
        landed = [
            n.rsplit("/", 1)[-1]
            for n in rig.remote_btrfs_subvols(f"{rig.remote_base}/btrfs")
        ]
        assert landed, "nothing landed on the remote"
        bare = [n for n in landed if n[:1].isdigit()]
        assert bare, (
            f"no bare-timestamp name on the remote, only {landed} -- an empty "
            "prefix was replaced by a derived one"
        )

        assert_payload_restored(res["restore_dest"], rig.payload)


# --------------------------------------------------------------------------- #
# a target that is not Linux and not btrfs
# --------------------------------------------------------------------------- #
class TestForeignRawTarget:
    """raw+ssh against a host that shares none of this one's assumptions.

    A raw target stores plain files, so it need not be btrfs, need not grant
    sudo, and need not be Linux. That is exactly what makes it the cell worth
    having: on a macOS/APFS box `stat -c` does not exist, /bin/sh is not GNU,
    df's columns sit in different places, and any code that asked the LOCAL
    filesystem about the target fails visibly instead of quietly answering about
    the wrong machine.

    Measured before the fix on this axis: get_space_info returned this host's
    free space rather than the target's, and restore could not find a stream
    that was plainly there.
    """

    @requires_raw_remote
    def test_backup_and_restore_against_a_foreign_host(self, rig):
        from .conftest import RAW_REMOTE_SPEC

        dest = f"{rig.raw_remote_base}/raw"
        loc = f"raw+ssh://{RAW_REMOTE_SPEC}:{dest}"
        cfg = rig.write_config(
            rig.root / "cfg-foreign-raw.toml", f'path = "{loc}"', prefix="t3frn-"
        )
        res = _lifecycle(rig, cfg, location=loc, prefix="t3frn-")

        assert res["backup_rc"] == 0, res["backup_out"]
        streams = rig.foreign_raw_streams(dest)
        assert streams, "nothing landed on the foreign target"

        # The restore is the assertion that matters: it reads the stream back
        # ACROSS the link and decodes it here. Checking the exit code would have
        # passed while the destination held only a bookkeeping directory.
        assert_payload_restored(res["restore_dest"], rig.payload)

    @requires_raw_remote
    def test_the_published_stream_is_not_world_readable(self, rig):
        """The stream is the most sensitive file this tool writes. A remote
        `cat >` left it at the target's umask, typically 0644, while the .meta
        sidecar beside it was chmod'd 600."""
        from .conftest import RAW_REMOTE_SPEC

        dest = f"{rig.raw_remote_base}/raw"
        loc = f"raw+ssh://{RAW_REMOTE_SPEC}:{dest}"
        cfg = rig.write_config(
            rig.root / "cfg-foreign-mode.toml", f'path = "{loc}"', prefix="t3mode-"
        )
        assert rig.cli("run", config=cfg).returncode == 0

        streams = rig.foreign_raw_streams(dest)
        assert streams, "nothing landed on the foreign target"
        for name in streams:
            assert rig.foreign_mode(f"{dest}/{name}") == "600", (
                f"{name} is readable by other users on the target"
            )

    @requires_raw_remote
    def test_the_space_check_measures_the_target_not_this_host(self, rig):
        """The pre-transfer space check and `estimate` both read this figure.
        Taken from os.statvfs here, it described the machine being backed UP, so
        a full target passed the check and the transfer ran until it ran out."""
        import os as _os

        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

        from .conftest import RAW_REMOTE_SPEC, raw_remote_sh

        dest = f"{rig.raw_remote_base}/raw"
        host = RAW_REMOTE_SPEC.split("@")[-1]
        user = RAW_REMOTE_SPEC.split("@")[0] if "@" in RAW_REMOTE_SPEC else None
        endpoint = SSHRawEndpoint(
            config={"path": dest, "hostname": host, "username": user}
        )
        info = endpoint.get_space_info()

        truth = raw_remote_sh(f"df -Pk '{dest}' | tail -1").stdout.split()
        # Total is stable and compared exactly. AVAILABLE is not: it is a live
        # figure on a machine doing other things, and the two df calls are
        # seconds apart -- an exact match failed on a 680 KiB drift. The point of
        # this cell is which HOST answered, so the tolerance is generous and the
        # assertion below is the one that carries the meaning.
        assert info.total_bytes // 1024 == int(truth[1])
        drift = abs(info.available_bytes // 1024 - int(truth[3]))
        assert drift < 1024 * 1024, (
            f"available space differs from the target's own df by {drift} KiB, "
            "which is more than ordinary drift"
        )

        here = _os.statvfs("/")
        assert info.total_bytes != here.f_blocks * here.f_frsize, (
            "the reported size equals this host's root filesystem"
        )


# --------------------------------------------------------------------------- #
# sudoers policy -- the axis no physical host in this suite can represent
# --------------------------------------------------------------------------- #
class TestSudoersPolicies:
    """`ssh_sudo` without passwordless sudo, against real sudoers policies.

    .70 has NOPASSWD for btrfs, so it can never reach the password-sudo branch,
    and giving it a password-only policy would mean editing /etc/sudoers on a
    machine someone else relies on. A throwaway container is the only honest way
    to cover these, and it is where the defect was found: the compressed receive
    elevated a SHELL (`sudo -S sh -c '<decompress> | btrfs receive'`), which the
    btrfs-only sudoers recipe in this project's own README refuses.

    The command under test is the one the code actually emits, not a
    reconstruction -- a hand-written approximation passed while the real string
    failed, because it lacked the backgrounding that moves sudo's credential
    ticket to a different parent pid.
    """

    #: (label, sudoers body, must the payload arrive?)
    POLICIES = [
        ("full-sudo-caching", "bbng ALL=(ALL) ALL", True),
        ("btrfs-only-caching", "bbng ALL=(ALL) /usr/bin/btrfs", True),
        (
            "full-sudo-no-caching",
            "bbng ALL=(ALL) ALL\nDefaults:bbng timestamp_timeout=0",
            True,
        ),
        # Not a gap this release can close: sudo wants the password for every
        # invocation, and stdin is carrying the stream. It fails identically
        # before this fix, so it is pinned as known-unreachable rather than
        # quietly omitted.
        (
            "btrfs-only-no-caching",
            "bbng ALL=(ALL) /usr/bin/btrfs\nDefaults:bbng timestamp_timeout=0",
            False,
        ),
    ]

    #: /bin/sh differs across these three, and this project has shipped two bugs
    #: from exactly that difference.
    IMAGES = ["fedora:latest", "debian:stable-slim", "alpine:latest"]

    @requires_container
    @pytest.mark.parametrize("image", IMAGES)
    @pytest.mark.parametrize("label,sudoers,must_arrive", POLICIES)
    def test_the_emitted_receive_command_under_a_sudoers_policy(
        self, tmp_path, image, label, sudoers, must_arrive
    ):
        from btrfs_backup_ng.endpoint.ssh import _build_receive_command

        from .conftest import run_in_container

        emitted = _build_receive_command(
            "/backup/dest", use_sudo=True, password_on_stdin=True, decompress="gzip"
        )
        command_file = tmp_path / "emitted.txt"
        command_file.write_text(emitted)

        script = r"""
set -e
if command -v dnf >/dev/null 2>&1; then
    dnf -q -y install sudo shadow-utils util-linux gzip >/dev/null 2>&1
elif command -v apk >/dev/null 2>&1; then
    apk add --quiet sudo shadow util-linux gzip >/dev/null 2>&1
else
    apt-get -qq update >/dev/null 2>&1
    apt-get -qq install -y sudo gzip >/dev/null 2>&1
fi
useradd -m bbng 2>/dev/null || adduser -D bbng 2>/dev/null
echo 'bbng:hunter2' | chpasswd
mkdir -p /backup
cat > /usr/bin/btrfs <<'STUB'
#!/bin/sh
case "${1:-}" in --version) echo "btrfs-progs v6.17"; exit 0;; esac
cat > /tmp/received.bin
STUB
chmod 755 /usr/bin/btrfs
printf 'PAYLOAD-BYTES-THAT-MUST-SURVIVE
' | gzip > /tmp/data.gz
printf '%b
' "$SUDOERS" > /etc/sudoers.d/bbng
chmod 440 /etc/sudoers.d/bbng
CMD=$(cat /emitted.txt)
{ printf 'hunter2
'; cat /tmp/data.gz; } | runuser -u bbng -- sh -c "$CMD" >/dev/null 2>&1 || true
echo "DELIVERED:$(cat /tmp/received.bin 2>/dev/null || echo NOTHING)"
"""
        result = run_in_container(
            image,
            f"SUDOERS={shlex.quote(sudoers)}; {script}",
            mounts={str(command_file): "/emitted.txt"},
        )
        arrived = "DELIVERED:PAYLOAD-BYTES-THAT-MUST-SURVIVE" in result.stdout

        if must_arrive:
            assert arrived, (
                f"{image} / {label}: the backup did not arrive.\n"
                f"stdout: {result.stdout[-1500:]}\nstderr: {result.stderr[-1500:]}"
            )
        else:
            assert not arrived, (
                f"{image} / {label} now works. That is good news, but this cell "
                "pins it as unreachable -- confirm why and update the policy table."
            )


# --------------------------------------------------------------------------- #
# restore honesty -- independent of any one target
# --------------------------------------------------------------------------- #
class TestRestoreReportsHonestly:
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
