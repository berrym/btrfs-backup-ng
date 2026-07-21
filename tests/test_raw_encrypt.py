"""raw encrypt -- plaintext remediation (0.8.5 PR6e).

Encrypts raw streams written as plaintext (GHSA-vr25-6vrh-869j), then runs a LIVE
decrypt-to-identical proof and removes the plaintext ONLY when that proof passes and
``--shred`` was given. The removal is a plain unlink (documented as non-secure on
CoW/SSD). raw+ssh is refused (crypto stays local).
"""

import argparse
import hashlib
import json

import pytest

from btrfs_backup_ng.cli import raw_cmd
from btrfs_backup_ng.endpoint.raw import RawEndpoint


def _plaintext_backup(path, name="root.20240101T120000", compress="gzip"):
    ep = RawEndpoint(config={"path": str(path), "compress": compress})
    src = path / (name + ".src")
    src.write_bytes(b"plaintext-remediation-payload-" * 200)
    with open(src, "rb") as f:
        ep.receive(f, snapshot_name=name).communicate()
    ep.commit_receive()
    src.unlink()


def _args(**kw):
    ns = argparse.Namespace()
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


def _encrypt(tmp_path, **kw):
    base = {
        "raw_action": "encrypt",
        "target": str(tmp_path),
        "encrypt": "openssl_enc",
        "gpg_recipient": None,
        "openssl_cipher": None,
        "shred": False,
        "yes": True,  # non-interactive in tests
        "dry_run": False,
        "json": False,
    }
    base.update(kw)
    return raw_cmd.execute_raw(_args(**base))


# --------------------------------------------------------------------------- #
# encrypt keeps plaintext by default; produces a verifiable encrypted stream
# --------------------------------------------------------------------------- #
def test_encrypt_keeps_plaintext_and_writes_remediation_sidecar(tmp_path, monkeypatch):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    plain = tmp_path / "root.20240101T120000.btrfs.gz"
    plain_hash = hashlib.sha256(plain.read_bytes()).hexdigest()

    rc = _encrypt(tmp_path)
    assert rc == 0
    # Plaintext kept (no --shred); encrypted copy + remediation sidecar written.
    assert plain.exists()
    enc = tmp_path / "root.20240101T120000.btrfs.gz.enc"
    assert enc.exists()
    doc = json.loads((tmp_path / "root.20240101T120000.btrfs.gz.enc.meta").read_text())
    assert doc["provenance"]["origin"] == "remediation"
    assert doc["provenance"]["remediated_from"] == "root.20240101T120000.btrfs.gz"
    assert doc["pipeline"]["encrypt"] == "openssl_enc"
    assert doc["pipeline"]["compress"] == "gzip"  # compression preserved

    # The encrypted stream decrypts back to exactly the plaintext.
    import subprocess

    dec = subprocess.run(
        [
            "openssl",
            "enc",
            "-d",
            "-aes-256-cbc",
            "-pbkdf2",
            "-pass",
            "pass:rempass",
            "-in",
            str(enc),
        ],
        capture_output=True,
    ).stdout
    assert hashlib.sha256(dec).hexdigest() == plain_hash


# --------------------------------------------------------------------------- #
# THE safety guard: plaintext removed ONLY after a verified decrypt proof
# --------------------------------------------------------------------------- #
def test_shred_removes_plaintext_only_after_verified_proof(tmp_path, monkeypatch):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    plain = tmp_path / "root.20240101T120000.btrfs.gz"

    rc = _encrypt(tmp_path, shred=True)
    assert rc == 0
    assert not plain.exists()  # removed after the proof passed
    assert not (tmp_path / "root.20240101T120000.btrfs.gz.meta").exists()
    assert (tmp_path / "root.20240101T120000.btrfs.gz.enc").exists()


def test_shred_keeps_plaintext_when_proof_fails(tmp_path, monkeypatch, capsys):
    """If the decrypt proof fails (e.g. gpg without the secret key on this host),
    the plaintext MUST be kept even with --shred. Removing regardless of the proof
    would delete it here -> this fails."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    plain = tmp_path / "root.20240101T120000.btrfs.gz"
    # Simulate an unverifiable encryption (proof fails).
    monkeypatch.setattr(
        RawEndpoint, "decrypt_matches_plaintext", lambda self, new, p: False
    )
    rc = _encrypt(tmp_path, shred=True)
    out = capsys.readouterr().out
    assert plain.exists()  # NEVER removed without a passing proof
    assert "ENCRYPTED-UNVERIFIED" in out
    assert rc == 1  # --shred requested but could not complete safely


# --------------------------------------------------------------------------- #
# decrypt_matches_plaintext directly
# --------------------------------------------------------------------------- #
def test_decrypt_matches_plaintext_true_and_false(tmp_path, monkeypatch):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    ep = RawEndpoint(config={"path": str(tmp_path)})
    (plain_snap,) = ep.list_snapshots(flush_cache=True)
    new = ep.remediate_plaintext(plain_snap, encrypt="openssl_enc")
    assert ep.decrypt_matches_plaintext(new, plain_snap.stream_path) is True
    # Decrypt SUCCEEDS but the bytes differ from a different reference -> the sha256
    # comparison (not just the return code) must catch it and return False.
    other = tmp_path / "other.plain"
    other.write_bytes(b"totally-different-bytes")
    assert ep.decrypt_matches_plaintext(new, other) is False
    # Corrupt the encrypted stream -> the decrypt itself fails -> also False.
    new.stream_path.write_bytes(new.stream_path.read_bytes() + b"TAMPER")
    assert ep.decrypt_matches_plaintext(new, plain_snap.stream_path) is False


# --------------------------------------------------------------------------- #
# guards / errors
# --------------------------------------------------------------------------- #
def test_encrypt_dry_run_changes_nothing(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    before = sorted(p.name for p in tmp_path.iterdir())
    rc = _encrypt(tmp_path, shred=True, dry_run=True)
    out = capsys.readouterr().out
    assert rc == 0
    assert "WOULD-ENCRYPT-AND-SHRED" in out
    assert sorted(p.name for p in tmp_path.iterdir()) == before


def test_encrypt_refuses_raw_ssh(capsys):
    rc = raw_cmd.execute_raw(
        _args(
            raw_action="encrypt",
            target="raw+ssh://nas/backup",
            encrypt="openssl_enc",
            gpg_recipient=None,
            openssl_cipher=None,
            shred=False,
            yes=True,
            dry_run=False,
            json=False,
        )
    )
    assert rc == 2
    assert "runs locally" in capsys.readouterr().out


def test_encrypt_gpg_requires_recipient(tmp_path, capsys):
    _plaintext_backup(tmp_path)
    rc = _encrypt(tmp_path, encrypt="gpg", gpg_recipient=None)
    assert rc == 2
    assert "--gpg-recipient is required" in capsys.readouterr().out


def test_encrypt_openssl_requires_passphrase(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("BTRFS_BACKUP_PASSPHRASE", raising=False)
    monkeypatch.delenv("BTRBK_PASSPHRASE", raising=False)
    _plaintext_backup(tmp_path)
    rc = _encrypt(tmp_path, encrypt="openssl_enc")
    assert rc == 2
    assert "requires a passphrase" in capsys.readouterr().out


def test_encrypt_no_plaintext_streams(tmp_path, monkeypatch, capsys):
    """A target whose streams are already encrypted has nothing to remediate."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    assert _encrypt(tmp_path, shred=True) == 0  # encrypt the plaintext + shred it
    capsys.readouterr()
    # Now only the encrypted stream remains -> nothing to do.
    rc = _encrypt(tmp_path)
    assert rc == 0
    assert "no plaintext streams" in capsys.readouterr().out


def test_dispatcher_parses_raw_encrypt():
    from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

    parser = create_subcommand_parser()
    args = parser.parse_args(
        [
            "raw",
            "encrypt",
            "/x",
            "--encrypt",
            "gpg",
            "--gpg-recipient",
            "KEYID",
            "--shred",
            "--yes",
            "--dry-run",
            "--json",
        ]
    )
    assert args.command == "raw"
    assert args.raw_action == "encrypt"
    assert args.encrypt == "gpg"
    assert args.gpg_recipient == "KEYID"
    assert args.shred is True and args.yes is True and args.dry_run is True


@pytest.mark.parametrize("shred", [False, True])
def test_encrypted_stream_verifies_after_remediation(tmp_path, monkeypatch, shred):
    """The remediated (encrypted) stream is a valid backup: raw verify -> ok."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    assert _encrypt(tmp_path, shred=shred) == 0
    rc = raw_cmd.execute_raw(
        _args(raw_action="verify", target=str(tmp_path), snapshot=None, json=False)
    )
    assert rc == 0


# --------------------------------------------------------------------------- #
# security: symlink at the .part write target cannot be followed
# --------------------------------------------------------------------------- #
def test_encrypt_part_symlink_cannot_redirect_write(tmp_path, monkeypatch):
    """A pre-planted <orig>.enc.part symlink must NOT redirect the (often root)
    encrypt write to an arbitrary file. O_NOFOLLOW|O_EXCL refuses it; the outside
    file stays intact and remediate raises."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    outside = tmp_path / "outside.secret"
    outside.write_text("DO-NOT-TRUNCATE")
    (tmp_path / "root.20240101T120000.btrfs.gz.enc.part").symlink_to(outside)
    ep = RawEndpoint(config={"path": str(tmp_path)})
    (snap,) = [s for s in ep.list_snapshots(flush_cache=True) if not s.encrypt]
    with pytest.raises((OSError, RuntimeError)):
        ep.remediate_plaintext(snap, encrypt="openssl_enc")
    assert outside.read_text() == "DO-NOT-TRUNCATE"


def test_remediate_refuses_non_encryption_method(tmp_path):
    """Defense in depth: never produce a plaintext file with an encrypted label."""
    _plaintext_backup(tmp_path)
    ep = RawEndpoint(config={"path": str(tmp_path)})
    (snap,) = [s for s in ep.list_snapshots(flush_cache=True) if not s.encrypt]
    with pytest.raises(ValueError, match="real encryption method"):
        ep.remediate_plaintext(snap, encrypt=None)  # type: ignore[arg-type]


def test_remediate_refuses_existing_encrypted_target(tmp_path, monkeypatch):
    """remediate_plaintext must never os.replace over an existing encrypted stream."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    ep = RawEndpoint(config={"path": str(tmp_path)})
    (snap,) = [s for s in ep.list_snapshots(flush_cache=True) if not s.encrypt]
    enc = tmp_path / "root.20240101T120000.btrfs.gz.enc"
    enc.write_bytes(b"existing-do-not-clobber")
    with pytest.raises(FileExistsError):
        ep.remediate_plaintext(snap, encrypt="openssl_enc")
    assert enc.read_bytes() == b"existing-do-not-clobber"


# --------------------------------------------------------------------------- #
# no-clobber: a pre-existing, non-matching encrypted stream is never overwritten
# --------------------------------------------------------------------------- #
def test_existing_encrypted_not_clobbered(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    enc = tmp_path / "root.20240101T120000.btrfs.gz.enc"
    enc.write_bytes(b"a-pre-existing-different-encrypted-backup")
    rc = _encrypt(tmp_path, shred=True)
    out = capsys.readouterr().out
    # The pre-existing .enc is untouched and the plaintext is kept.
    assert enc.read_bytes() == b"a-pre-existing-different-encrypted-backup"
    assert (tmp_path / "root.20240101T120000.btrfs.gz").exists()
    assert "EXISTING-ENCRYPTED-DIFFERS" in out
    assert rc == 1


# --------------------------------------------------------------------------- #
# partial failure across a batch
# --------------------------------------------------------------------------- #
def test_partial_failure_shreds_verified_keeps_unverified(tmp_path, monkeypatch):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path, name="aaa")
    _plaintext_backup(tmp_path, name="bbb")
    real = RawEndpoint.decrypt_matches_plaintext

    def selective(self, new, plaintext_path):
        # Verify "aaa" for real; force "bbb" to fail the proof.
        if "bbb" in str(plaintext_path):
            return False
        return real(self, new, plaintext_path)

    monkeypatch.setattr(RawEndpoint, "decrypt_matches_plaintext", selective)
    rc = _encrypt(tmp_path, shred=True)
    assert not (tmp_path / "aaa.btrfs.gz").exists()  # verified -> removed
    assert (tmp_path / "bbb.btrfs.gz").exists()  # unverified -> kept
    assert rc == 1  # a stream could not be safely completed


# --------------------------------------------------------------------------- #
# the interactive destructive-op confirm gate
# --------------------------------------------------------------------------- #
def test_confirm_prompt_abort_keeps_plaintext(tmp_path, monkeypatch):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    plain = tmp_path / "root.20240101T120000.btrfs.gz"
    monkeypatch.setattr(raw_cmd.sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr("builtins.input", lambda: "n")
    rc = _encrypt(tmp_path, shred=True, yes=False)
    assert rc == 1
    assert plain.exists()  # aborted before any encryption or removal
    assert not (tmp_path / "root.20240101T120000.btrfs.gz.enc").exists()


def test_confirm_prompt_yes_proceeds(tmp_path, monkeypatch):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    plain = tmp_path / "root.20240101T120000.btrfs.gz"
    monkeypatch.setattr(raw_cmd.sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr("builtins.input", lambda: "y")
    rc = _encrypt(tmp_path, shred=True, yes=False)
    assert rc == 0
    assert not plain.exists()  # proceeded, removed after the proof


def test_encrypt_json_output(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "rempass")
    _plaintext_backup(tmp_path)
    rc = _encrypt(tmp_path, json=True)
    data = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert data[0]["name"] == "root.20240101T120000"
    assert data[0]["action"] == "encrypted"
    assert data[0]["verified"] is True
