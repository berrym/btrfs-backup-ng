"""Error delivery / UX (T7).

A reliable backup tool must never hand a regular user a raw Python traceback or an
empty/misleading error. Every failure here is expected to be delivered as a plain,
self-explanatory line naming what went wrong and what to do -- and the tool must exit
non-zero, not crash. These guard the break-campaign findings grouped under T7.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.cli.dispatcher as disp
import btrfs_backup_ng.core.operations as ops
import btrfs_backup_ng.endpoint.raw as raw_mod
from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.local import LocalEndpoint
from btrfs_backup_ng.endpoint.raw import RawEndpoint, RawSnapshot


# --- top-level handler: no bare traceback ever -------------------------------


def _run(command="raw"):
    return disp.run_subcommand(argparse.Namespace(version=False, command=command))


def test_uncaught_exception_renders_clean_line_not_traceback(monkeypatch, capsys):
    """A handler raising an arbitrary error (e.g. a ValueError from a bad sidecar
    cipher, which send_snapshot's except historically did not catch) must reach the
    user as one plain line + a --debug hint + exit 1, never a Python traceback.
    Mutation guard: removing the top-level ``except Exception`` re-raises to the user."""
    monkeypatch.setattr(
        disp, "cmd_raw", MagicMock(side_effect=ValueError("bad cipher in sidecar"))
    )
    rc = _run("raw")
    err = capsys.readouterr().err
    assert rc == 1
    assert "Error: bad cipher in sidecar" in err
    assert "--debug" in err
    assert "Traceback" not in err


def test_abort_error_renders_its_message_without_debug_hint(monkeypatch, capsys):
    """An AbortError is a deliberate, already-plain-language stop; show its message as
    the reason, and do NOT append the --debug traceback hint (there is nothing to
    debug -- the message is the answer)."""
    monkeypatch.setattr(
        disp,
        "cmd_raw",
        MagicMock(side_effect=__util__.AbortError("zstd is not installed; install it")),
    )
    rc = _run("raw")
    err = capsys.readouterr().err
    assert rc == 1
    assert "Error: zstd is not installed; install it" in err
    assert "--debug" not in err
    assert "Traceback" not in err


def test_keyboard_interrupt_is_clean_130(monkeypatch, capsys):
    monkeypatch.setattr(disp, "cmd_raw", MagicMock(side_effect=KeyboardInterrupt()))
    rc = _run("raw")
    err = capsys.readouterr().err
    assert rc == 130
    assert "Interrupted" in err
    assert "Traceback" not in err


# --- exec_subprocess: a command failure carries a real reason ----------------


def test_failed_command_yields_nonempty_message():
    """``raise AbortError from e`` left the AbortError empty, so callers logged garbled
    lines like 'Failed to create snapshot: ' with no reason. The message must now name
    the command and its exit status. Mutation guard: reverting to a bare
    ``raise AbortError from e`` makes str(e) empty and fails this."""
    with pytest.raises(__util__.AbortError) as ei:
        __util__.exec_subprocess(["false"], method="check_call")
    msg = str(ei.value)
    assert msg  # not empty
    assert "false" in msg and "exit status 1" in msg


def test_failed_command_includes_captured_stderr():
    """When stderr was captured, the reason must include it (the actionable detail),
    not just the exit code."""
    with pytest.raises(__util__.AbortError) as ei:
        __util__.exec_subprocess(
            ["sh", "-c", "echo boom-detail >&2; exit 3"],
            method="check_output",
            stderr=subprocess.PIPE,
        )
    msg = str(ei.value)
    assert "boom-detail" in msg
    assert "exit 3" in msg


# --- same-second snapshot collision: clear, not "Read-only file system" ------


def test_same_second_snapshot_collision_is_explained(tmp_path, monkeypatch):
    """Two snapshots requested in the same second collide on name. btrfs would report
    the misleading 'Could not create subvolume: Read-only file system'; the tool must
    detect the pre-existing name first and explain the real cause. Mutation guard:
    removing the ``snapshot_path.exists()`` pre-check drops the AbortError (the code
    would instead reach the btrfs command, which is not run here)."""
    src = tmp_path / "src"
    src.mkdir()
    snaps = tmp_path / "snaps"

    # Freeze the timestamp so both the pre-created path and the endpoint's internal
    # snapshot derive the SAME name -- a deterministic collision, no real clock race.
    fixed = __util__.str_to_date("2026-01-02 03:04:05", fmt="%Y-%m-%d %H:%M:%S")
    monkeypatch.setattr(__util__, "str_to_date", lambda *a, **k: fixed)

    ep = LocalEndpoint(
        config={
            "source": str(src),
            "path": str(snaps),
            "snapshot_folder": str(snaps),
            "snap_prefix": "t-",
        }
    )
    name = __util__.Snapshot(snaps, "t-", ep).get_name()
    snaps.mkdir(parents=True, exist_ok=True)
    (snaps / name).mkdir()  # the pre-existing colliding snapshot

    with pytest.raises(__util__.AbortError) as ei:
        ep.snapshot()
    msg = str(ei.value)
    assert "already exists" in msg
    assert "same second" in msg
    assert "Read-only file system" not in msg


# --- openssl: an unsupported cipher fails clearly, not with an EVP dump -------


def test_unsupported_openssl_cipher_aborts_with_clear_message():
    """A cipher this host's openssl does not know (a different openssl build, or a
    hand-edited sidecar) must fail BEFORE the pipeline dumps a cryptic OpenSSL EVP
    error. Mutation guard: removing the ``_openssl_supports_cipher`` branch lets the
    pipeline build silently, so no AbortError is raised."""
    ep = RawEndpoint(config={"path": "/tmp"})
    snap = RawSnapshot(
        name="x",
        stream_path=Path("/tmp/x.btrfs.enc"),
        encrypt="openssl_enc",
        openssl_cipher="aes-256-cbcx",  # structurally valid, but no such cipher
    )
    with pytest.raises(__util__.AbortError) as ei:
        ep._build_restore_pipeline(snap)
    assert "aes-256-cbcx" in str(ei.value)
    assert "openssl" in str(ei.value).lower()


def test_supported_openssl_cipher_is_not_false_rejected(monkeypatch):
    """The probe must not block a real cipher (guards over-eager rejection)."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "pw")  # so the decrypt stage builds
    ep = RawEndpoint(config={"path": "/tmp"})
    snap = RawSnapshot(
        name="x",
        stream_path=Path("/tmp/x.btrfs.enc"),
        encrypt="openssl_enc",
        openssl_cipher="aes-256-cbc",
    )
    pipeline = ep._build_restore_pipeline(snap)  # must not raise
    assert any("openssl" in stage[0] for stage in pipeline)


def test_openssl_support_probe_discriminates():
    assert raw_mod._openssl_supports_cipher("aes-256-cbc") is True
    assert raw_mod._openssl_supports_cipher("aes-256-cbcx") is False


def test_probe_fails_open_when_openssl_missing(monkeypatch):
    """The probe must fail OPEN when it cannot adjudicate: if openssl is absent from
    PATH it returns True so the restore is NOT wrongly blocked (a missing tool is
    reported by the tool preflight, not by second-guessing the cipher). Mutation guard:
    flipping the openssl-missing ``return True`` to ``return False`` fails this."""
    monkeypatch.setattr(raw_mod.shutil, "which", lambda name: None)
    assert raw_mod._openssl_supports_cipher("aes-256-cbcx") is True


def test_probe_fails_open_on_oserror(monkeypatch):
    """If the probe subprocess cannot even spawn (OSError), fail OPEN -- do not block a
    restore on an inability to run the check. Mutation guard: flipping the except
    ``return True`` to ``return False`` fails this."""

    def boom(*a, **k):
        raise OSError("cannot spawn")

    monkeypatch.setattr(raw_mod.subprocess, "run", boom)
    assert raw_mod._openssl_supports_cipher("aes-256-cbc") is True


def test_probe_fails_open_on_timeout(monkeypatch):
    """If the probe hangs and times out, fail OPEN rather than hang or block. Mutation
    guard: dropping TimeoutExpired from the except (or the ``return True``) fails this."""

    def slow(*a, **k):
        raise subprocess.TimeoutExpired(cmd="openssl", timeout=15)

    monkeypatch.setattr(raw_mod.subprocess, "run", slow)
    assert raw_mod._openssl_supports_cipher("aes-256-cbc") is True


# --- checksum on a symlink: clear reason, not a scary ELOOP ------------------


def test_checksum_symlink_reports_plain_reason(tmp_path, monkeypatch):
    """_sha256_file opens with O_NOFOLLOW; a symlink stream surfaces ELOOP, whose
    default text 'Too many levels of symbolic links' wrongly implies a loop. The
    warning must say it is a symlink, refused for safety. Mutation guard: reverting to
    logging the raw exception restores the misleading text."""
    real = tmp_path / "real.btrfs"
    real.write_bytes(b"data")
    link = tmp_path / "link.btrfs"
    link.symlink_to(real)

    warnings = []
    monkeypatch.setattr(raw_mod.logger, "warning", lambda *a, **k: warnings.append(a))
    result = raw_mod._sha256_file(link)
    assert result is None  # refused
    rendered = " ".join(str(a) for w in warnings for a in w)
    assert "symlink" in rendered
    assert "Too many levels" not in rendered


# --- honest transfer banner: no "complete!" when a transfer failed -----------


def _drive_transfers(monkeypatch, send_side_effect):
    dest = MagicMock()
    dest.get_id.return_value = "dest-id"
    dest.config = {"path": "/dest"}
    src = MagicMock()
    snap = MagicMock()
    snap.get_name.return_value = "snap-1"

    monkeypatch.setattr(ops, "send_snapshot", MagicMock(side_effect=send_side_effect))
    monkeypatch.setattr(ops, "_cleanup_partial_local_subvolume", lambda *a, **k: None)
    monkeypatch.setattr(ops, "_cleanup_partial_raw_stream", lambda *a, **k: None)

    headings = []
    monkeypatch.setattr(
        ops.logger,
        "warning",
        lambda msg, *a, **k: headings.append(("warn", msg % a if a else msg)),
    )
    monkeypatch.setattr(
        ops.logger,
        "info",
        lambda msg, *a, **k: headings.append(("info", msg % a if a else msg)),
    )
    monkeypatch.setattr(ops.logger, "error", lambda *a, **k: None)
    monkeypatch.setattr(ops.logger, "debug", lambda *a, **k: None)

    result = ops._execute_transfers(
        source_endpoint=src,
        destination_endpoint=dest,
        source_snapshots=[snap],
        destination_snapshots=[],
        to_transfer=[snap],
        no_incremental=True,
        options={},
    )
    return result, headings


def test_banner_reports_failure_not_complete(monkeypatch):
    """A failed transfer must NOT print a 'complete!' banner. Mutation guard: reverting
    to the unconditional 'complete!' info line fails the 'no complete' assertion."""
    _, headings = _drive_transfers(
        monkeypatch, __util__.SnapshotTransferError("send died")
    )
    text = " ".join(m for _, m in headings)
    assert "failure" in text
    assert "complete!" not in text


def test_banner_reports_complete_on_success(monkeypatch):
    _, headings = _drive_transfers(monkeypatch, lambda *a, **k: None)
    text = " ".join(m for _, m in headings)
    assert "complete!" in text


# --- send_snapshot wraps a ValueError into a reported transfer failure --------


def test_send_snapshot_wraps_valueerror(monkeypatch):
    """A ValueError raised by send() (e.g. an unusable recorded cipher) must be logged
    as a failed transaction and re-raised as SnapshotTransferError, so the per-target
    handler reports the reason and continues -- not escape as a bare traceback.
    Mutation guard: removing the ``except ValueError`` clause lets the ValueError
    propagate unwrapped."""
    snap = MagicMock()
    snap.get_path.return_value = "/x"
    snap.endpoint.send.side_effect = ValueError(
        "cipher 'aes-256-gcm' is AEAD; refusing"
    )
    dest = MagicMock()
    dest.config = {"path": "/dest"}

    monkeypatch.setattr(ops, "_ensure_destination_exists", lambda *a, **k: None)
    monkeypatch.setattr(ops, "_cleanup_processes", lambda *a, **k: None)

    with pytest.raises(__util__.SnapshotTransferError) as ei:
        ops.send_snapshot(snap, dest, options={"check_space": False})
    assert "AEAD" in str(ei.value)


# --- raw encrypt: plain output shows the actionable reason -------------------


def test_raw_encrypt_plain_output_shows_failure_reason(tmp_path, monkeypatch, capsys):
    """When a stream cannot be remediated, the plain (non-JSON) output must show the
    reason -- previously it printed only 'ERROR <name>', hiding the cause unless the
    user thought to add --json. Mutation guard: dropping the ``reason:`` line hides it
    again."""
    stream = tmp_path / "s.20240101T000000.btrfs"
    stream.write_bytes(b"plaintext-stream")
    RawSnapshot(name="s.20240101T000000", stream_path=stream, size=16).save_metadata()

    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "pw")
    monkeypatch.setattr(
        RawEndpoint,
        "remediate_plaintext",
        lambda self, *a, **k: (_ for _ in ()).throw(RuntimeError("gpg exploded here")),
    )

    args = argparse.Namespace(
        target=str(tmp_path),
        encrypt="openssl_enc",
        openssl_cipher="aes-256-cbc",
        gpg_recipient=None,
        gpg_keyring=None,
        dry_run=False,
        shred=False,
        yes=True,
        json=False,
        ssh_sudo=False,
    )
    from btrfs_backup_ng.cli import raw_cmd

    rc = raw_cmd._raw_encrypt(args)
    out = capsys.readouterr().out
    assert rc == 1  # a failed remediation exits non-zero
    assert "ERROR" in out
    assert "gpg exploded here" in out  # the reason is visible in plain output
