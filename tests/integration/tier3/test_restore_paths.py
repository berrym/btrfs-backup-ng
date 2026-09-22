"""The restore paths nothing had driven, each proven by the bytes it lands.

Three groups, all restoring through the installed CLI and comparing what
arrived with what was backed up:

    snapper restore     the README's disaster-recovery walkthrough, from a
                        local btrfs target, an ssh:// target and a raw://
                        target, into a throwaway snapper config on recovery
                        media that holds nothing else -- by --all, by number,
                        and (raw) by name and date
    decrypt on restore  encrypt = "gpg" with a keyring that exists only for
                        the run; the right key restores the bytes, the wrong
                        key restores nothing
    transports          zstd over ssh:// and raw+ssh://, openssl_enc + zstd
                        to a foreign raw+ssh:// host, each read back with
                        the bytes compared and the wrong passphrase refused

Every one of these existed only as a hand-driven check or not at all. The
0.9.6 release, whose stated purpose was restore correctness, shipped with
raw+ssh restore broken because the cell that would have caught it landed one
commit after the tag; these cells are here so the paths below cannot do the
same.

The recovery media is a third loopback filesystem (Rig.rec). `btrfs receive`
resolves an incremental stream's parent by uuid across the whole filesystem
it lands on, so restoring into the source or the destination filesystem finds
the original or the backup and succeeds for a reason a real recovery would
not have. A cell that needs media with NOTHING on it -- the single-increment
disaster-recovery case -- brings up a filesystem of its own.

The snapper parent-selection cells are xfail(strict=True): `snapper restore
--snapshot N` picks the incremental parent from the backup side and probes
only the backup for it, so on recovery media without the parent the receive
fails. The README's walkthrough is exactly that situation. The cells describe
the behaviour the fix must produce; strict xfail means the fix cannot land
without them turning green, and cannot quietly regress after.
"""

from __future__ import annotations

import json
import os
import time

import pytest

from .conftest import (
    RAW_REMOTE_SPEC,
    REMOTE_SPEC,
    assert_nothing_restored,
    assert_payload_restored,
    lifecycle,
    loop_fs_down,
    loop_fs_up,
    raw_remote_sh,
    remote_sh,
    requires_local,
    requires_raw_remote,
    requires_remote,
    requires_snapper,
    sh,
    snapper_config_up,
    snapper_lifecycle,
)

pytestmark = [pytest.mark.tier3, requires_local]


# --------------------------------------------------------------------------- #
# snapper restore
# --------------------------------------------------------------------------- #


def _snapper_restore(rig, source, recovery, *args, env=None):
    """Run `snapper restore SOURCE CONFIG ...` and report what it ADDED.

    The recovery config is shared by the cells of a module, so the slots a
    call created are the difference in the on-disk numbering, never "slot 1".
    """
    before = set(recovery.slots())
    r = rig.cli("snapper", "restore", source, recovery.name, *args, env=env)
    out = (r.stdout + r.stderr)[-3000:]
    new = sorted(set(recovery.slots()) - before)
    return r.returncode, out, new


def _assert_slot(recovery, number, payload, delta=None):
    """The slot is a received subvolume holding the source bytes.

    With ``delta`` the increment's bytes must be there too. Without it there
    must be NO extra.bin: a base restored by number or by name is then
    provably the base, not a second copy of the increment resolved by a
    selector that ignored what was asked for -- the double-restore the
    name/date addressing was added to fix.
    """
    snap = recovery.slot(number)
    assert snap.is_dir(), f"slot {number} has no snapshot"
    assert snap.stat().st_ino == 256, f"slot {number}/snapshot is not a subvolume"
    assert (snap / "payload.bin").read_bytes() == payload, (
        f"slot {number}: payload.bin differs from the source"
    )
    extra = snap / "extra.bin"
    if delta is None:
        assert not extra.exists(), (
            f"slot {number} carries an increment where the base was requested"
        )
    else:
        assert extra.is_file(), (
            f"slot {number} has no extra.bin: the increment's bytes were dropped"
        )
        assert extra.read_bytes() == delta, f"slot {number}: increment bytes differ"
    info = snap.parent / "info.xml"
    assert info.is_file(), f"slot {number} has no info.xml"
    assert f"<num>{number}</num>" in info.read_text(), (
        f"slot {number}: info.xml still carries the backup's number"
    )


def _subvolume_show(path) -> dict[str, str]:
    r = sh(["btrfs", "subvolume", "show", str(path)], check=True)
    fields = {}
    for line in r.stdout.splitlines():
        if ":" in line:
            key, _, value = line.partition(":")
            fields[key.strip()] = value.strip()
    return fields


def _assert_chain_restored(rig, res, recovery, rc, out, new, *, logged=True):
    """`--all` rebuilt the chain: two new slots, the older the base and the
    newer the increment, and the increment was received AGAINST the restored
    base rather than sent in full -- which is what makes it recovery of a
    chain and not two unrelated full restores.

    The filesystem says which: a subvolume received from an incremental
    stream records the parent it was applied to as its Parent UUID, and one
    received in full records none. The log line "(incremental from N)" is
    printed from the parent the restore CHOSE, before anything is sent, so
    it is checked only where the layout logs it (``logged``) and never
    stands in for the uuid. A raw restore replays the stored stream verbatim
    and labels every replay "(full)" whatever the stream holds.
    """
    base_num, _delta_num = res["numbers"]
    assert rc == 0, out
    assert len(new) == 2, f"expected two new slots, got {new}\n{out}"
    first, second = new
    _assert_slot(recovery, first, rig.payload)
    _assert_slot(recovery, second, rig.payload, res["delta"])
    base = _subvolume_show(recovery.slot(first))
    increment = _subvolume_show(recovery.slot(second))
    assert base.get("Parent UUID", "-") == "-", (
        f"slot {first} was received against a parent; the base must be a full receive"
    )
    assert increment.get("Parent UUID") == base.get("UUID"), (
        f"slot {second} was not received against slot {first}: Parent UUID "
        f"{increment.get('Parent UUID')!r} vs base UUID {base.get('UUID')!r} -- "
        "the increment was sent in full, or against something else\n" + out
    )
    if logged:
        flat = " ".join(out.split())
        assert f"(incremental from {base_num})" in flat, (
            "the restore did not log the increment as incremental\n" + out
        )


def _virgin_recovery(rig, request, tag):
    """A snapper config on a filesystem with nothing on it at all.

    Torn down in reverse: the config and its subvolumes first (registered
    last), then the filesystem. Both finalizers are registered before the
    thing they remove exists.
    """
    fs = f"rec-{tag}"
    request.addfinalizer(lambda: loop_fs_down(fs))
    mnt = loop_fs_up(fs)
    return snapper_config_up(request, f"bbngt3{tag}{rig.suffix}", mnt / "recover", None)


_PARENT_SELECTION = (
    "snapper restore selects the incremental parent from the backup side and "
    "never checks the destination (cli/snapper_cmd.py, core/restore.py): with "
    "the parent absent from the recovery media the receive fails instead of "
    "degrading to a full restore, which is the README walkthrough's situation"
)

_SSH_SUDO_PROMPT = (
    "snapper restore over ssh:// with --ssh-sudo demands a sudo password even "
    "where the remote grants NOPASSWD btrfs: the endpoint is built without "
    "prepare(), so the passwordless probe the native restore runs never "
    "records passwordless_sudo_available and _build_remote_command emits "
    "`sudo -S btrfs send` (core/restore.py _resolve_remote_snapper_backup, "
    "endpoint/ssh.py)"
)

#: The documented override for that: sudo -n unconditionally. The ssh cells
#: that must get PAST the prompt to prove something else set it, and say so.
_PASSWORDLESS_ONLY = {**os.environ, "BTRFS_BACKUP_PASSWORDLESS_ONLY": "1"}


@requires_snapper
class TestSnapperRestore:
    """`snapper restore` into recovery media, from every target layout."""

    def test_from_local_btrfs(self, rig, snapper_chain, snapper_recovery):
        target = rig.dst / "snapper-restore"
        target.mkdir()
        res = snapper_lifecycle(rig, snapper_chain, str(target))
        base_num, delta_num = res["numbers"]

        rc, out, new = _snapper_restore(rig, str(target), snapper_recovery, "--all")
        _assert_chain_restored(rig, res, snapper_recovery, rc, out, new)

        # By number. The base alone must come back as the base; the increment
        # by number lands against the base restored above.
        rc, out, new = _snapper_restore(
            rig, str(target), snapper_recovery, "--snapshot", str(base_num)
        )
        assert rc == 0, out
        assert len(new) == 1, f"--snapshot {base_num} added {new}\n{out}"
        _assert_slot(snapper_recovery, new[0], rig.payload)

        rc, out, new = _snapper_restore(
            rig, str(target), snapper_recovery, "--snapshot", str(delta_num)
        )
        assert rc == 0, out
        assert len(new) == 1, f"--snapshot {delta_num} added {new}\n{out}"
        _assert_slot(snapper_recovery, new[0], rig.payload, res["delta"])

    @requires_remote
    @pytest.mark.xfail(strict=True, reason=_SSH_SUDO_PROMPT)
    def test_from_ssh_as_documented(self, rig, snapper_chain, snapper_recovery):
        """The walkthrough's command against the walkthrough's sudoers policy,
        with nothing added: `snapper restore ssh://... CONFIG --all --ssh-sudo`
        on a remote that grants NOPASSWD btrfs must run unattended, as the
        backup in the other direction and the native restore both do."""
        base = f"{rig.remote_base}/snapper-restore-doc"
        remote_sh(f"mkdir -p '{base}'")
        loc = f"ssh://{REMOTE_SPEC}:{base}"
        res = snapper_lifecycle(rig, snapper_chain, loc, extra_args=("--ssh-sudo",))

        rc, out, new = _snapper_restore(
            rig, loc, snapper_recovery, "--all", "--ssh-sudo"
        )
        _assert_chain_restored(rig, res, snapper_recovery, rc, out, new)

    @requires_remote
    def test_from_ssh_with_passwordless_only(
        self, rig, snapper_chain, snapper_recovery
    ):
        """The same restore with BTRFS_BACKUP_PASSWORDLESS_ONLY=1, the documented
        way to force `sudo -n`: this is the ssh chain proof that exists today,
        and the bytes it compares came back across the link."""
        base = f"{rig.remote_base}/snapper-restore"
        remote_sh(f"mkdir -p '{base}'")
        loc = f"ssh://{REMOTE_SPEC}:{base}"
        res = snapper_lifecycle(rig, snapper_chain, loc, extra_args=("--ssh-sudo",))
        base_num, delta_num = res["numbers"]

        rc, out, new = _snapper_restore(
            rig, loc, snapper_recovery, "--all", "--ssh-sudo", env=_PASSWORDLESS_ONLY
        )
        _assert_chain_restored(rig, res, snapper_recovery, rc, out, new)

        rc, out, new = _snapper_restore(
            rig,
            loc,
            snapper_recovery,
            "--snapshot",
            str(delta_num),
            "--ssh-sudo",
            env=_PASSWORDLESS_ONLY,
        )
        assert rc == 0, out
        assert len(new) == 1, f"--snapshot {delta_num} added {new}\n{out}"
        _assert_slot(snapper_recovery, new[0], rig.payload, res["delta"])

    def test_from_raw(self, rig, snapper_chain, snapper_recovery):
        """raw:// carries a NAME per backup, so this layout is addressable
        three ways: --all, --backup-name, and --snapshot narrowed by --date.

        The name and date selectors exist for a REUSED snapper number: snapper
        hands a number out again once the snapshot that held it is gone, so
        two backups at the target can share a number and differ only in name,
        date and content. With one backup per number the selectors and the
        number all resolve to the same backup and nothing distinguishes a
        selector that was honoured from one that fell back to the number --
        so the cell makes the collision, and each selector has to return the
        bytes of the copy it names.
        """
        target = rig.raw / "snapper"
        target.mkdir()
        loc = f"raw://{target}"
        res = snapper_lifecycle(rig, snapper_chain, loc)
        base_num, delta_num = res["numbers"]

        rc, out, new = _snapper_restore(rig, loc, snapper_recovery, "--all")
        _assert_chain_restored(rig, res, snapper_recovery, rc, out, new, logged=False)

        # The collision: delta_num is deleted, the subvolume changed again, and
        # the number taken a second time with the new bytes. A raw backup is
        # named <config>-<num>-<YYYYMMDD-HHMMSS> from the snapshot's own date,
        # so a number retaken in the SAME second as the copy it replaces gets
        # the same name and is not a second backup at all; the clock has to
        # move on first. Measured: the matrix ran fast enough to hit it.
        listing = rig.cli("snapper", "restore", loc, "--list", "--json")
        assert listing.returncode == 0, listing.stdout + listing.stderr
        stamped = next(
            b["backup_name"]
            for b in json.loads(listing.stdout)["backups"]
            if b["number"] == delta_num
        )
        while stamped.endswith(time.strftime("%Y%m%d-%H%M%S")):
            time.sleep(0.05)
        sh(["snapper", "-c", snapper_chain.name, "delete", str(delta_num)], check=True)
        delta_again = snapper_chain.mutate()
        reused = snapper_chain.take("delta, number reused")
        assert reused == delta_num, f"snapper gave {reused}, not {delta_num} again"
        r = rig.cli("snapper", "backup", snapper_chain.name, loc)
        assert r.returncode == 0, (r.stdout + r.stderr)[-2000:]

        listing = rig.cli("snapper", "restore", loc, "--list", "--json")
        assert listing.returncode == 0, listing.stdout + listing.stderr
        backups = json.loads(listing.stdout)["backups"]
        base_name = next(b["backup_name"] for b in backups if b["number"] == base_num)
        copies = sorted(
            (b for b in backups if b["number"] == delta_num), key=lambda b: b["date"]
        )
        assert len(copies) == 2, f"expected two backups numbered {delta_num}: {backups}"
        older, newer = copies
        assert older["backup_name"] != newer["backup_name"], copies
        assert older["date"] != newer["date"], copies

        rc, out, new = _snapper_restore(
            rig, loc, snapper_recovery, "--backup-name", base_name
        )
        assert rc == 0, out
        assert len(new) == 1, f"--backup-name {base_name} added {new}\n{out}"
        _assert_slot(snapper_recovery, new[0], rig.payload)

        # By name: the OLDER copy, whose bytes are the first delta.
        rc, out, new = _snapper_restore(
            rig, loc, snapper_recovery, "--backup-name", older["backup_name"]
        )
        assert rc == 0, out
        assert len(new) == 1, f"--backup-name {older['backup_name']} added {new}\n{out}"
        _assert_slot(snapper_recovery, new[0], rig.payload, res["delta"])

        # By number narrowed by date: the NEWER copy, the second delta.
        rc, out, new = _snapper_restore(
            rig,
            loc,
            snapper_recovery,
            "--snapshot",
            str(delta_num),
            "--date",
            str(newer["date"])[:19],
        )
        assert rc == 0, out
        assert len(new) == 1, f"--snapshot --date added {new}\n{out}"
        _assert_slot(snapper_recovery, new[0], rig.payload, delta_again)

        # By number alone: the newest copy, and a warning that says so.
        rc, out, new = _snapper_restore(
            rig, loc, snapper_recovery, "--snapshot", str(delta_num)
        )
        assert rc == 0, out
        assert len(new) == 1, f"--snapshot {delta_num} added {new}\n{out}"
        _assert_slot(snapper_recovery, new[0], rig.payload, delta_again)
        flat = " ".join(out.split())
        assert f"Multiple backups share number {delta_num}" in flat, (
            "a reused number was restored without saying which copy\n" + out
        )

    @pytest.mark.xfail(strict=True, reason=_PARENT_SELECTION)
    def test_one_increment_onto_empty_media_from_local_btrfs(
        self, rig, snapper_chain, request
    ):
        """The walkthrough: media with nothing on it, restore ONE snapshot.

        The parent exists at the backup, so the backup-side probe is
        satisfied and the send goes out incremental; the receive has nothing
        to apply it to. What is asked for here is the fall-back the local
        branch already promises for an ABSENT parent: a full restore, exit 0,
        the bytes in slot 1.
        """
        target = rig.dst / "snapper-dr"
        target.mkdir()
        res = snapper_lifecycle(rig, snapper_chain, str(target))
        _base_num, delta_num = res["numbers"]
        virgin = _virgin_recovery(rig, request, "drl")

        rc, out, new = _snapper_restore(
            rig, str(target), virgin, "--snapshot", str(delta_num)
        )
        assert rc == 0, out
        assert new == [1], f"restore added {new}\n{out}"
        _assert_slot(virgin, 1, rig.payload, res["delta"])

    @requires_remote
    @pytest.mark.xfail(strict=True, reason=_PARENT_SELECTION)
    def test_one_increment_onto_empty_media_from_ssh(self, rig, snapper_chain, request):
        """BTRFS_BACKUP_PASSWORDLESS_ONLY is set so this cell gets past the
        sudo prompt pinned above and fails on parent selection, not on that."""
        base = f"{rig.remote_base}/snapper-dr"
        remote_sh(f"mkdir -p '{base}'")
        loc = f"ssh://{REMOTE_SPEC}:{base}"
        res = snapper_lifecycle(rig, snapper_chain, loc, extra_args=("--ssh-sudo",))
        _base_num, delta_num = res["numbers"]
        virgin = _virgin_recovery(rig, request, "drs")

        rc, out, new = _snapper_restore(
            rig,
            loc,
            virgin,
            "--snapshot",
            str(delta_num),
            "--ssh-sudo",
            env=_PASSWORDLESS_ONLY,
        )
        assert rc == 0, out
        assert new == [1], f"restore added {new}\n{out}"
        _assert_slot(virgin, 1, rig.payload, res["delta"])

    def test_one_raw_increment_onto_empty_media_is_refused_and_leaves_no_slot(
        self, rig, snapper_chain, request
    ):
        """A stored incremental stream is what it is: without its parent on
        the media it cannot be received, and there is no full send to fall
        back to. The honest outcome is a failure that leaves nothing --
        not a numbered slot with no snapshot in it for snapper to trip over."""
        target = rig.raw / "snapper-dr"
        target.mkdir()
        loc = f"raw://{target}"
        res = snapper_lifecycle(rig, snapper_chain, loc)
        _base_num, delta_num = res["numbers"]
        virgin = _virgin_recovery(rig, request, "drr")

        rc, out, new = _snapper_restore(rig, loc, virgin, "--snapshot", str(delta_num))
        assert rc != 0, f"restoring an increment onto empty media exited 0\n{out}"
        assert new == [], f"the failed restore left slots {new}\n{out}"
        assert not (virgin.subvol / ".snapshots" / "1").exists(), (
            "the failed restore left an empty numbered directory"
        )


# --------------------------------------------------------------------------- #
# decrypt on restore
# --------------------------------------------------------------------------- #

BTRFS_STREAM_MAGIC = b"btrfs-stream\0"
ZSTD_MAGIC = bytes.fromhex("28b52ffd")


def _stream_names(names, prefix):
    """This cell's stream files among a listing: <prefix><timestamp>.btrfs
    plus whatever compression and encryption suffixes it carries -- never
    the sidecars, and never the target's own dotfiles (the lock file's name
    contains ".btrfs" too)."""
    return sorted(
        n
        for n in names
        if n.startswith(prefix)
        and ".btrfs" in n
        and not n.endswith((".meta", ".part", ".json"))
    )


def _local_head(path, n=64) -> bytes:
    with open(path, "rb") as fh:
        return fh.read(n)


def _foreign_head(path, n=64) -> bytes:
    r = raw_remote_sh(f"head -c {n} '{path}' | od -An -v -tx1")
    return bytes.fromhex("".join(r.stdout.split()))


def _assert_not_plain(head: bytes, name: str, *, allow_zstd: bool) -> None:
    """The bytes at rest are neither a bare btrfs stream nor, unless the cell
    is about compression alone, a merely compressed one."""
    assert not head.startswith(BTRFS_STREAM_MAGIC), f"{name} is a plaintext stream"
    if not allow_zstd:
        assert not head.startswith(ZSTD_MAGIC), (
            f"{name} is compressed but not encrypted"
        )


def _wrong_secret_restores_nothing(rig, loc, dest, prefix, env, *args):
    r = rig.cli(
        "restore",
        loc,
        str(dest),
        "--prefix",
        prefix,
        "--yes-i-know-what-i-am-doing",
        *args,
        env=env,
    )
    out = (r.stdout + r.stderr)[-2000:]
    assert r.returncode != 0, f"a restore without the key exited 0\n{out}"
    assert_nothing_restored(dest, context="\n" + out)


class TestGpgDecryptOnRestore:
    """encrypt = "gpg" restored with the key, and refused without it."""

    def test_raw_local(self, rig, gpg_identity):
        target = rig.raw / "gpg"
        target.mkdir()
        loc = f"raw://{target}"
        cfg = rig.write_config(
            rig.root / "cfg-gpg-raw.toml",
            f'path = "{loc}"\nencrypt = "gpg"\ngpg_recipient = "{gpg_identity.recipient}"',
            prefix="t3gpg-",
        )
        res = lifecycle(rig, cfg, location=loc, prefix="t3gpg-", env=gpg_identity.env())
        assert res["backup_rc"] == 0, res["backup_out"]

        streams = _stream_names((p.name for p in target.iterdir()), "t3gpg-")
        assert streams, "no stream landed"
        for name in streams:
            assert name.endswith(".gpg"), f"{name} does not carry the .gpg suffix"
            _assert_not_plain(_local_head(target / name), name, allow_zstd=False)
            assert rig.payload[:4096] not in (target / name).read_bytes(), (
                f"{name} contains the source bytes in the clear"
            )
        assert_payload_restored(res["restore_dest"], rig.payload)

        _wrong_secret_restores_nothing(
            rig,
            loc,
            rig.src / "restored-gpg-wrong-key",
            "t3gpg-",
            gpg_identity.env(gpg_identity.stranger),
        )

    @requires_raw_remote
    def test_raw_over_ssh_to_a_foreign_host_with_zstd(self, rig, gpg_identity):
        """The read-back-over-ssh path decrypts and decompresses HERE; the
        far side is a file store that never sees a key."""
        dest = f"{rig.raw_remote_base}/gpg"
        raw_remote_sh(f"mkdir -p '{dest}'")
        loc = f"raw+ssh://{RAW_REMOTE_SPEC}:{dest}"
        cfg = rig.write_config(
            rig.root / "cfg-gpg-foreign.toml",
            f'path = "{loc}"\ncompress = "zstd"\nencrypt = "gpg"\n'
            f'gpg_recipient = "{gpg_identity.recipient}"',
            prefix="t3gpgf-",
        )
        res = lifecycle(
            rig, cfg, location=loc, prefix="t3gpgf-", env=gpg_identity.env()
        )
        assert res["backup_rc"] == 0, res["backup_out"]

        streams = _stream_names(
            raw_remote_sh(f"ls -1 '{dest}'").stdout.split(), "t3gpgf-"
        )
        assert streams, "no stream landed on the foreign host"
        for name in streams:
            assert name.endswith(".gpg"), f"{name} does not carry the .gpg suffix"
            _assert_not_plain(_foreign_head(f"{dest}/{name}"), name, allow_zstd=False)
        assert_payload_restored(res["restore_dest"], rig.payload)

        _wrong_secret_restores_nothing(
            rig,
            loc,
            rig.src / "restored-gpg-foreign-wrong-key",
            "t3gpgf-",
            gpg_identity.env(gpg_identity.stranger),
        )


# --------------------------------------------------------------------------- #
# compressed and encrypted transports, read back
# --------------------------------------------------------------------------- #


class TestCompressedAndEncryptedTransports:
    """The hand-driven checks of the 0.9.8 release verification, as cells."""

    @requires_remote
    def test_zstd_over_ssh(self, rig):
        """ssh:// stores a plain subvolume; zstd here is transport compression
        on BOTH legs. The restore leg is the one nothing exercised, and for
        a long time it was not compressed at all: `--compress` was accepted
        on an `ssh://` restore source and dropped, so this cell passed on
        plain bytes while its docstring claimed otherwise. The remote `btrfs
        send` is now compressed on the far side and undone here before
        `btrfs receive`; the restore's own log line for that is required
        below, so a decompressor that does not run fails the cell rather
        than passing on an uncompressed stream."""
        base = f"{rig.remote_base}/btrfs-zstd"
        remote_sh(f"mkdir -p '{base}'")
        loc = f"ssh://{REMOTE_SPEC}:{base}"
        cfg = rig.write_config(
            rig.root / "cfg-zstd-ssh.toml",
            f'path = "{loc}"\nssh_sudo = true\ncompress = "zstd"',
            prefix="t3zssh-",
        )
        res = lifecycle(
            rig,
            cfg,
            location=loc,
            prefix="t3zssh-",
            extra_args=("--ssh-sudo",),
            restore_args=("--compress", "zstd"),
        )
        assert res["backup_rc"] == 0, res["backup_out"]
        assert rig.remote_btrfs_subvols(base), "nothing landed remotely"
        assert_payload_restored(res["restore_dest"], rig.payload)
        flat = " ".join(res["restore_out"].split())
        assert "Decompressing the restore stream with zstd" in flat, (
            "the restore leg was not compressed: no decompressor ran here\n"
            + res["restore_out"]
        )

    @requires_raw_remote
    def test_zstd_raw_over_ssh_to_a_foreign_host(self, rig):
        dest = f"{rig.raw_remote_base}/zstd"
        raw_remote_sh(f"mkdir -p '{dest}'")
        loc = f"raw+ssh://{RAW_REMOTE_SPEC}:{dest}"
        cfg = rig.write_config(
            rig.root / "cfg-zstd-foreign.toml",
            f'path = "{loc}"\ncompress = "zstd"',
            prefix="t3zfrn-",
        )
        res = lifecycle(rig, cfg, location=loc, prefix="t3zfrn-")
        assert res["backup_rc"] == 0, res["backup_out"]

        streams = _stream_names(
            raw_remote_sh(f"ls -1 '{dest}'").stdout.split(), "t3zfrn-"
        )
        assert streams, "no stream landed on the foreign host"
        for name in streams:
            assert name.endswith(".zst"), f"{name} does not carry the .zst suffix"
            assert _foreign_head(f"{dest}/{name}").startswith(ZSTD_MAGIC), (
                f"{name} is not a zstd frame"
            )
        assert_payload_restored(res["restore_dest"], rig.payload)

    @requires_raw_remote
    def test_openssl_and_zstd_raw_over_ssh_to_a_foreign_host(self, rig):
        dest = f"{rig.raw_remote_base}/openssl"
        raw_remote_sh(f"mkdir -p '{dest}'")
        loc = f"raw+ssh://{RAW_REMOTE_SPEC}:{dest}"
        cfg = rig.write_config(
            rig.root / "cfg-openssl-foreign.toml",
            f'path = "{loc}"\ncompress = "zstd"\nencrypt = "openssl_enc"',
            prefix="t3ossl-",
        )
        env = {**os.environ, "BTRFS_BACKUP_PASSPHRASE": f"tier3-{rig.suffix}"}
        res = lifecycle(rig, cfg, location=loc, prefix="t3ossl-", env=env)
        assert res["backup_rc"] == 0, res["backup_out"]

        streams = _stream_names(
            raw_remote_sh(f"ls -1 '{dest}'").stdout.split(), "t3ossl-"
        )
        assert streams, "no stream landed on the foreign host"
        for name in streams:
            assert name.endswith(".enc"), f"{name} does not carry the .enc suffix"
            head = _foreign_head(f"{dest}/{name}")
            assert head.startswith(b"Salted__"), f"{name} is not openssl ciphertext"
        assert_payload_restored(res["restore_dest"], rig.payload)

        _wrong_secret_restores_nothing(
            rig,
            loc,
            rig.src / "restored-openssl-wrong-passphrase",
            "t3ossl-",
            {**os.environ, "BTRFS_BACKUP_PASSPHRASE": "not-the-passphrase"},
        )
