"""OpenSSL cipher/passphrase restore+send contract (0.8.5 PR4).

The raw endpoint records the openssl cipher used for a backup in its ``.meta``
sidecar (PR3). These tests pin the contract that PR4 establishes around that
value:

  * restore decrypts with the cipher RECORDED in the sidecar, not this
    endpoint's configured default (else a non-default-cipher backup decrypts to
    garbage);
  * ``BTRBK_PASSPHRASE`` is actually usable end to end (it was advertised in
    __init__ but the pipelines hardcoded ``env:BTRFS_BACKUP_PASSPHRASE``, so a
    btrbk migrant silently produced an unreadable backup);
  * an unknown/tampered cipher is rejected up front rather than reaching a shell;
  * restore without any passphrase fails loud instead of running openssl empty;
  * a legacy backup that recorded no cipher still restores (endpoint default);
  * the send-side pipelines quote every argv element (no shell injection via a
    gpg recipient/keyring or cipher), matching the already-quoted restore side.

Each test is written to FAIL if its guard is reverted (mutation-verified).
"""

import shutil
import subprocess

import pytest

from btrfs_backup_ng.endpoint import raw as raw_mod
from btrfs_backup_ng.endpoint.raw import RawEndpoint, SSHRawEndpoint
from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

requires_openssl = pytest.mark.skipif(
    shutil.which("openssl") is None, reason="openssl not installed"
)


def _write_encrypted_backup(tmp_path, cipher, payload, name="snap"):
    """Encrypt ``payload`` into a committed raw backup under ``tmp_path`` using
    ``cipher``, returning nothing (the sidecar records the cipher)."""
    ep = RawEndpoint(
        config={
            "path": str(tmp_path),
            "encrypt": "openssl_enc",
            "openssl_cipher": cipher,
        }
    )
    src = tmp_path / "src.bin"
    src.write_bytes(payload)
    with open(src, "rb") as stdin:
        proc = ep.receive(stdin, snapshot_name=name)
        proc.communicate()
    assert proc.returncode == 0, "encrypt receive failed"
    ep.commit_receive()
    src.unlink()


# --------------------------------------------------------------------------- #
# (1) restore uses the RECORDED cipher, not the endpoint default
# --------------------------------------------------------------------------- #
@requires_openssl
def test_restore_uses_recorded_cipher_not_endpoint_default(tmp_path, monkeypatch):
    """A backup encrypted with a non-default cipher must decrypt correctly on an
    endpoint left at the aes-256-cbc default. Reverting restore to
    ``self.openssl_cipher`` makes the decrypt fail -> this test fails."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "correct horse battery staple")
    payload = b"REAL-SNAPSHOT-BYTES-" * 200
    _write_encrypted_backup(tmp_path, "aes-128-cbc", payload)

    # A restore endpoint configured with the DEFAULT cipher (aes-256-cbc).
    restore_ep = RawEndpoint(config={"path": str(tmp_path)})
    snaps = restore_ep.list_snapshots()
    assert len(snaps) == 1
    # The sidecar carries the real cipher, so restore has the information it needs.
    assert snaps[0].openssl_cipher == "aes-128-cbc"

    proc = restore_ep.send(snaps[0])
    out, err = proc.communicate()
    assert proc.returncode == 0, err.decode(errors="replace")
    assert out == payload


# --------------------------------------------------------------------------- #
# (2) BTRBK_PASSPHRASE is usable end to end
# --------------------------------------------------------------------------- #
@requires_openssl
def test_btrbk_passphrase_only_round_trips(tmp_path, monkeypatch):
    """With ONLY BTRBK_PASSPHRASE set, encrypt+decrypt must round-trip. Reverting
    the pipelines to hardcoded ``env:BTRFS_BACKUP_PASSPHRASE`` makes openssl read
    an unset variable -> the encrypt receive fails -> this test fails."""
    monkeypatch.delenv("BTRFS_BACKUP_PASSPHRASE", raising=False)
    monkeypatch.setenv("BTRBK_PASSPHRASE", "btrbk-migrant-secret")
    payload = b"MIGRANT-STREAM-" * 128
    _write_encrypted_backup(tmp_path, "aes-256-cbc", payload)

    restore_ep = RawEndpoint(config={"path": str(tmp_path)})
    (snap,) = restore_ep.list_snapshots()
    proc = restore_ep.send(snap)
    out, err = proc.communicate()
    assert proc.returncode == 0, err.decode(errors="replace")
    assert out == payload


# --------------------------------------------------------------------------- #
# (3a) an invalid configured cipher is rejected at construction
# --------------------------------------------------------------------------- #
def test_invalid_cipher_rejected_at_construction(tmp_path):
    """A cipher carrying shell metacharacters must be rejected when the endpoint
    is built. Removing the __init__ _validate_cipher call -> no raise -> fails."""
    with pytest.raises(ValueError, match="Invalid openssl cipher"):
        RawEndpoint(
            config={"path": str(tmp_path), "openssl_cipher": "aes-256-cbc; touch pwned"}
        )


@pytest.mark.parametrize("cipher", [None, ""])
def test_explicit_unset_cipher_defaults_and_is_valid(tmp_path, cipher):
    """An explicit ``openssl_cipher=None`` or ``""`` (the CLI threads None for
    gpg/plaintext targets) means "unset" and must default to aes-256-cbc, NOT be
    rejected by construction validation. Regression guard: validating the raw
    ``.get()`` result instead of coalescing empties rejects every gpg/plaintext
    raw target -> this test fails."""
    ep = RawEndpoint(config={"path": str(tmp_path), "openssl_cipher": cipher})
    assert ep.openssl_cipher == "aes-256-cbc"


# --------------------------------------------------------------------------- #
# (3c) the NULL cipher "none" must be refused -- it writes plaintext
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("cipher", ["none", "NONE", "None"])
def test_null_cipher_none_is_refused(tmp_path, cipher):
    """``openssl enc -none`` performs NO encryption, so accepting openssl_cipher=
    'none' would write a PLAINTEXT backup labelled as encrypted (the CWE-311/312
    class fixed in 0.8.4). Dropping the semantic 'none' rejection -> the value is
    accepted -> this test fails. Rejected both directly and at construction of an
    encrypting endpoint."""
    with pytest.raises(ValueError, match="performs NO encryption"):
        raw_mod._validate_cipher(cipher)
    with pytest.raises(ValueError, match="performs NO encryption"):
        RawEndpoint(
            config={
                "path": str(tmp_path),
                "encrypt": "openssl_enc",
                "openssl_cipher": cipher,
            }
        )


# --------------------------------------------------------------------------- #
# (3d) AEAD ciphers are refused -- `openssl enc` cannot use them
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "cipher", ["aes-256-gcm", "aes-128-ccm", "aes-256-ocb", "chacha20-poly1305"]
)
def test_aead_cipher_is_refused(cipher):
    """`openssl enc` errors 'AEAD ciphers not supported', so an AEAD cipher must be
    rejected up front rather than failing cryptically mid-transfer. Removing the
    AEAD rejection -> accepted -> this test fails."""
    with pytest.raises(ValueError, match="AEAD"):
        raw_mod._validate_cipher(cipher)


# --------------------------------------------------------------------------- #
# (3e) a trailing newline must not slip through the structural guard
# --------------------------------------------------------------------------- #
def test_trailing_newline_cipher_is_rejected():
    """Python's ``$`` matches before a trailing newline, so ``^...$`` would accept
    'aes-256-cbc\\n' -- a shell metacharacter defeating the guard if any downstream
    quote is ever dropped. The ``\\A...\\Z`` anchors reject it. Reverting to
    ``^...$`` -> accepted -> this test fails."""
    with pytest.raises(ValueError, match="Invalid openssl cipher"):
        raw_mod._validate_cipher("aes-256-cbc\n")


# --------------------------------------------------------------------------- #
# (positive) a legitimate non-AEAD stream cipher still round-trips
# --------------------------------------------------------------------------- #
@requires_openssl
def test_chacha20_round_trips(tmp_path, monkeypatch):
    """chacha20 is a legitimate non-AEAD openssl cipher; the allowlist must not be
    over-tightened into rejecting it. Encrypt with chacha20, restore, and get the
    bytes back unchanged."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "chacha-secret")
    payload = b"CHACHA-STREAM-" * 300
    _write_encrypted_backup(tmp_path, "chacha20", payload)

    restore_ep = RawEndpoint(config={"path": str(tmp_path)})
    (snap,) = restore_ep.list_snapshots()
    assert snap.openssl_cipher == "chacha20"
    proc = restore_ep.send(snap)
    out, err = proc.communicate()
    assert proc.returncode == 0, err.decode(errors="replace")
    assert out == payload


# --------------------------------------------------------------------------- #
# (3b) a tampered sidecar cipher is rejected before reaching a shell
# --------------------------------------------------------------------------- #
def test_malicious_sidecar_cipher_rejected_at_restore(tmp_path, monkeypatch):
    """A cipher read from a hostile/corrupt sidecar must be rejected when the
    restore pipeline is built. Removing the restore-side _validate_cipher ->
    no raise -> this test fails."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "x")
    restore_ep = RawEndpoint(config={"path": str(tmp_path)})
    snap = RawSnapshot(
        name="evil",
        stream_path=tmp_path / "evil.btrfs",
        encrypt="openssl_enc",
        openssl_cipher="aes-256-cbc; touch pwned",
    )
    with pytest.raises(ValueError, match="Invalid openssl cipher"):
        restore_ep._build_restore_pipeline(snap)


# --------------------------------------------------------------------------- #
# (2/5) restore without any passphrase fails loud
# --------------------------------------------------------------------------- #
def test_restore_without_passphrase_fails_loud(tmp_path, monkeypatch):
    """Building an openssl restore pipeline with no passphrase env set must
    raise, not silently run openssl with an empty password. Reverting to the
    hardcoded ``env:BTRFS_BACKUP_PASSPHRASE`` arg removes the raise -> fails."""
    monkeypatch.delenv("BTRFS_BACKUP_PASSPHRASE", raising=False)
    monkeypatch.delenv("BTRBK_PASSPHRASE", raising=False)
    restore_ep = RawEndpoint(config={"path": str(tmp_path)})
    snap = RawSnapshot(
        name="s",
        stream_path=tmp_path / "s.btrfs",
        encrypt="openssl_enc",
        openssl_cipher="aes-256-cbc",
    )
    with pytest.raises(ValueError, match="requires a passphrase"):
        restore_ep._build_restore_pipeline(snap)


# --------------------------------------------------------------------------- #
# (legacy) a backup that recorded no cipher still restores (endpoint default)
# --------------------------------------------------------------------------- #
def test_legacy_snapshot_without_recorded_cipher_uses_endpoint_default(
    tmp_path, monkeypatch
):
    """A pre-sidecar backup records no cipher; restore must fall back to the
    endpoint default (aes-256-cbc, what every such backup used) rather than
    hard-failing. If the fallback is dropped, _validate_cipher(None) raises ->
    this test fails."""
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "x")
    restore_ep = RawEndpoint(config={"path": str(tmp_path)})
    snap = RawSnapshot(
        name="legacy",
        stream_path=tmp_path / "legacy.btrfs",
        encrypt="openssl_enc",
        openssl_cipher=None,
    )
    pipeline = restore_ep._build_restore_pipeline(snap)
    openssl_cmd = next(c for c in pipeline if c and c[0] == "openssl")
    assert "-aes-256-cbc" in openssl_cmd


# --------------------------------------------------------------------------- #
# (4) send-side pipelines quote every argv element (no injection)
# --------------------------------------------------------------------------- #
def test_send_pipeline_quotes_argv_local(tmp_path, monkeypatch):
    """The local multi-stage send pipeline must quote each argv element so a gpg
    recipient with shell metacharacters is passed as ONE argument. Reverting the
    quote in _execute_pipeline lets the shell split/execute it -> fails."""
    ep = RawEndpoint(config={"path": str(tmp_path)})
    ep._pending_metadata["part_path"] = tmp_path / "out.part"
    pipeline = [["gzip"], ["gpg", "--recipient", "evil; touch pwned"]]

    captured = {}

    def fake_popen(shell_cmd, **kw):
        captured["cmd"] = shell_cmd
        from unittest.mock import MagicMock

        return MagicMock()

    monkeypatch.setattr(raw_mod, "_popen_pipeline_pipefail", fake_popen)
    ep._execute_pipeline(pipeline, subprocess.DEVNULL)
    # The metacharacter-laden recipient survives as a single token only if quoted.
    import shlex

    assert "evil; touch pwned" in shlex.split(captured["cmd"])


def test_send_pipeline_quotes_argv_ssh(monkeypatch):
    """Same guarantee for the raw+ssh send pipeline (SSHRawEndpoint override).
    Reverting its quote -> the recipient word-splits in the local shell -> fails."""
    ep = SSHRawEndpoint(config={"path": "/backup", "hostname": "nas"})
    ep._pending_metadata["part_path"] = "/backup/out.part"
    pipeline = [["gzip"], ["gpg", "--recipient", "evil; touch pwned"]]

    captured = {}

    def fake_popen(shell_cmd, **kw):
        captured["cmd"] = shell_cmd
        from unittest.mock import MagicMock

        return MagicMock()

    monkeypatch.setattr(raw_mod, "_popen_pipeline_pipefail", fake_popen)
    ep._execute_pipeline(pipeline, subprocess.DEVNULL)
    import shlex

    assert "evil; touch pwned" in shlex.split(captured["cmd"])


# --------------------------------------------------------------------------- #
# (regression) a build-time receive() error on the rich-progress path becomes a
# clean SnapshotTransferError, not a bare ValueError that skips the audit log
# --------------------------------------------------------------------------- #
def test_rich_progress_receive_build_error_becomes_transfer_error():
    """PR4 makes receive() fail loud at build time (e.g. openssl with no
    passphrase). The rich-progress transfer path must convert that to a
    SnapshotTransferError like the standard path. Dropping the try/except around
    the rich-progress receive() -> the raw ValueError propagates -> this test
    fails."""
    from unittest.mock import MagicMock

    from btrfs_backup_ng import __util__
    from btrfs_backup_ng.core import operations

    send_process = MagicMock()
    dest = MagicMock()
    dest.receive.side_effect = ValueError("openssl_enc requires a passphrase")

    with pytest.raises(__util__.SnapshotTransferError):
        operations._do_rich_progress_transfer(send_process, dest, False, "snap", None)
    # The send process is torn down so it cannot linger after the failure.
    send_process.kill.assert_called_once()
