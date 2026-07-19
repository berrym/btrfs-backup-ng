"""Enforcement tests for R6 (0.8.5) — raw-target encryption must not silently
degrade to plaintext.

A documented `encrypt = "gpg"` on a raw target was dropped by the config loader,
so backups meant to be encrypted were written in cleartext with no error. The
loader now carries the encryption fields and FAILS CLOSED: a requested
encryption that cannot be honored refuses to load rather than writing plaintext.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli.common import thread_raw_encryption
from btrfs_backup_ng.config.loader import ConfigError, _parse_target, load_config
from btrfs_backup_ng.endpoint import assert_encryption_applied, choose_endpoint


class TestParseTargetEncryption:
    def test_raw_gpg_with_recipient_is_carried(self):
        t = _parse_target(
            {"path": "raw:///mnt/backup", "encrypt": "gpg", "gpg_recipient": "KEYID"}
        )
        assert t.encrypt == "gpg"
        assert t.gpg_recipient == "KEYID"

    def test_raw_ssh_gpg_is_carried(self):
        t = _parse_target(
            {"path": "raw+ssh://h:/p", "encrypt": "gpg", "gpg_recipient": "KEYID"}
        )
        assert t.encrypt == "gpg"

    def test_raw_openssl_enc_is_allowed(self):
        # The landmine: openssl_enc is a valid, documented method and must NOT
        # be rejected (RawTargetConfig's old valid set wrongly excluded it).
        t = _parse_target({"path": "raw:///mnt/b", "encrypt": "openssl_enc"})
        assert t.encrypt == "openssl_enc"

    def test_gpg_keyring_and_openssl_cipher_are_carried(self):
        t = _parse_target(
            {
                "path": "raw:///mnt/b",
                "encrypt": "openssl_enc",
                "openssl_cipher": "aes-128-cbc",
                "gpg_keyring": "/k.gpg",
            }
        )
        assert t.openssl_cipher == "aes-128-cbc"
        assert t.gpg_keyring == "/k.gpg"

    # --- fail-closed ---

    def test_gpg_without_recipient_fails_closed(self):
        with pytest.raises(ConfigError, match="gpg_recipient is required"):
            _parse_target({"path": "raw:///mnt/backup", "encrypt": "gpg"})

    def test_encrypt_on_ssh_btrfs_target_fails_closed(self):
        with pytest.raises(ConfigError, match="only supported on raw"):
            _parse_target(
                {"path": "ssh://h:/p", "encrypt": "gpg", "gpg_recipient": "K"}
            )

    def test_encrypt_on_local_btrfs_target_fails_closed(self):
        with pytest.raises(ConfigError, match="only supported on raw"):
            _parse_target(
                {"path": "/mnt/backup", "encrypt": "gpg", "gpg_recipient": "K"}
            )

    def test_invalid_encrypt_value_fails_closed(self):
        with pytest.raises(ConfigError, match="Invalid encryption"):
            _parse_target({"path": "raw:///mnt/b", "encrypt": "rot13"})

    # --- must NOT break valid configs (false-positive guards) ---

    def test_raw_plaintext_is_allowed(self):
        t = _parse_target({"path": "raw:///mnt/b"})
        assert t.encrypt == "none"

    def test_non_raw_without_encrypt_is_unaffected(self):
        # The overwhelmingly common case: a btrfs target that never set encrypt.
        t = _parse_target({"path": "ssh://h:/p", "compress": "zstd"})
        assert t.encrypt == "none"
        assert t.compress == "zstd"

    def test_compress_is_not_rejected_on_non_raw(self):
        # compress is shared (used by btrfs/ssh transfers), not raw-exclusive.
        t = _parse_target({"path": "/mnt/backup", "compress": "zstd"})
        assert t.compress == "zstd"


class TestLoadConfigRoundTrip:
    @staticmethod
    def _write(tmp_path, target_lines):
        toml = (
            "[global]\n"
            "\n"
            "[[volumes]]\n"
            'path = "/home"\n'
            'snapshot_prefix = "home-"\n'
            "\n"
            "[[volumes.targets]]\n" + target_lines + "\n"
        )
        p = tmp_path / "config.toml"
        p.write_text(toml)
        return p

    def test_gpg_target_survives_full_load(self, tmp_path):
        p = self._write(
            tmp_path,
            'path = "raw:///mnt/backup/home"\nencrypt = "gpg"\ngpg_recipient = "KEYID"',
        )
        config, _ = load_config(str(p))
        target = config.volumes[0].targets[0]
        assert target.encrypt == "gpg"
        assert target.gpg_recipient == "KEYID"

    def test_gpg_without_recipient_fails_at_load(self, tmp_path):
        p = self._write(tmp_path, 'path = "raw:///mnt/backup/home"\nencrypt = "gpg"')
        with pytest.raises(ConfigError):
            load_config(str(p))


class TestThreadRawEncryption:
    def test_copies_all_encryption_fields(self):
        target = SimpleNamespace(
            encrypt="gpg",
            gpg_recipient="KEYID",
            gpg_keyring="/k.gpg",
            openssl_cipher="aes-128-cbc",
        )
        kw: dict = {}
        thread_raw_encryption(kw, target)
        assert kw["encrypt"] == "gpg"
        assert kw["gpg_recipient"] == "KEYID"
        assert kw["gpg_keyring"] == "/k.gpg"
        assert kw["openssl_cipher"] == "aes-128-cbc"


class TestAssertEncryptionApplied:
    def test_noop_when_not_requested(self):
        # none / None must never raise (plaintext raw + all non-raw targets).
        assert_encryption_applied("none", SimpleNamespace(encrypt=None))
        assert_encryption_applied(None, SimpleNamespace(encrypt=None))

    def test_noop_when_applied(self):
        assert_encryption_applied("gpg", SimpleNamespace(encrypt="gpg"))

    def test_raises_when_requested_but_endpoint_lacks_it(self):
        # The whole point: encryption requested but not on the endpoint -> abort.
        with pytest.raises(__util__.AbortError, match="PLAINTEXT"):
            assert_encryption_applied("gpg", SimpleNamespace(encrypt=None))

    def test_raises_when_endpoint_encrypt_is_none_string(self):
        with pytest.raises(__util__.AbortError):
            assert_encryption_applied("openssl_enc", SimpleNamespace(encrypt="none"))


class TestChooseEndpointThreadsEncryption:
    def test_raw_endpoint_receives_encryption_config(self):
        # End-to-end through choose_endpoint's raw whitelist (incl. openssl_cipher).
        ep = choose_endpoint(
            "raw:///tmp/r6-nonexistent-dest",
            {
                "path": "raw:///tmp/r6-nonexistent-dest",
                "encrypt": "gpg",
                "gpg_recipient": "KEYID",
                "gpg_keyring": "/k.gpg",
                "openssl_cipher": "aes-128-cbc",
            },
        )
        assert ep.encrypt == "gpg"
        assert ep.gpg_recipient == "KEYID"
        assert ep.openssl_cipher == "aes-128-cbc"

    def test_encrypt_none_string_is_plaintext_not_rejected(self):
        # thread_raw_encryption sets encrypt="none" for plaintext raw targets;
        # the endpoint must treat the string "none" as plaintext (== None), not
        # reject it -- otherwise every plaintext raw backup breaks.
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        ep = RawEndpoint(
            config={"path": "/tmp/r6-x", "snap_prefix": "", "encrypt": "none"}
        )
        assert ep.encrypt is None


# --- Real encryption pipeline validation (runs the actual gpg/openssl pipeline)


def _make_raw_endpoint(dest, **enc):
    from btrfs_backup_ng.endpoint.raw import RawEndpoint

    cfg = {"path": str(dest), "snap_prefix": ""}
    cfg.update(enc)
    return RawEndpoint(config=cfg)


def _feed(plaintext: bytes):
    import subprocess

    return subprocess.Popen(
        ["cat"], stdin=subprocess.PIPE, stdout=subprocess.PIPE
    ), plaintext


@pytest.mark.skipif(__import__("shutil").which("gpg") is None, reason="gpg required")
class TestRealGpgPipeline:
    """Prove the raw pipeline produces genuine, decryptable GPG ciphertext -- and
    that a plaintext control is NOT encrypted. This is the end-to-end security
    guarantee: encrypt=gpg must never yield readable cleartext."""

    PLAINTEXT = b"BTRFS-SEND-STREAM-TOP-SECRET-CONTENTS-abcdef0123456789"

    @staticmethod
    def _gpg_key(gnupghome):
        import subprocess

        subprocess.run(
            [
                "gpg",
                "--batch",
                "--pinentry-mode",
                "loopback",
                "--passphrase",
                "",
                "--quick-generate-key",
                "R6 Test <r6-test@example.invalid>",
                "default",
                "default",
                "never",
            ],
            check=True,
            capture_output=True,
            env={**__import__("os").environ, "GNUPGHOME": str(gnupghome)},
        )
        return "r6-test@example.invalid"

    def _run_receive(self, dest, plaintext, **enc):
        import subprocess

        ep = _make_raw_endpoint(dest, **enc)
        feeder = subprocess.Popen(
            ["printf", "%s", plaintext.decode()], stdout=subprocess.PIPE
        )
        proc = ep.receive(feeder.stdout, "r6snap")
        if feeder.stdout:
            feeder.stdout.close()
        proc.wait()
        feeder.wait()
        assert proc.returncode == 0, "pipeline should succeed"
        out = next(p for p in dest.iterdir() if p.name.startswith("r6snap"))
        return out

    def test_gpg_output_is_real_ciphertext_and_decrypts(self, tmp_path, monkeypatch):
        import subprocess

        gnupghome = tmp_path / "gnupg"
        gnupghome.mkdir(mode=0o700)
        monkeypatch.setenv("GNUPGHOME", str(gnupghome))
        recipient = self._gpg_key(gnupghome)

        dest = tmp_path / "dest"
        dest.mkdir()
        out = self._run_receive(
            dest, self.PLAINTEXT, encrypt="gpg", gpg_recipient=recipient
        )

        data = out.read_bytes()
        assert self.PLAINTEXT not in data, "output must NOT contain the plaintext"
        lp = subprocess.run(
            ["gpg", "--list-packets", str(out)], capture_output=True, text=True
        )
        assert (
            "encrypted data" in lp.stdout.lower() or "pubkey enc" in lp.stdout.lower()
        )
        dec = subprocess.run(
            ["gpg", "--batch", "--yes", "--decrypt", str(out)], capture_output=True
        )
        assert dec.stdout == self.PLAINTEXT, "must decrypt back to the original stream"

    def test_plaintext_control_is_not_encrypted(self, tmp_path):
        # encrypt=none writes the raw stream; confirms the test would catch a
        # silent plaintext downgrade (the whole bug).
        dest = tmp_path / "dest"
        dest.mkdir()
        out = self._run_receive(dest, self.PLAINTEXT, encrypt="none")
        assert self.PLAINTEXT in out.read_bytes()


@pytest.mark.skipif(
    __import__("shutil").which("openssl") is None, reason="openssl required"
)
class TestRealOpensslPipeline:
    PLAINTEXT = b"BTRFS-SEND-STREAM-OPENSSL-SECRET-9876543210"

    def test_openssl_output_is_encrypted_and_decrypts(self, tmp_path, monkeypatch):
        import subprocess

        monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "test-passphrase-r6")
        dest = tmp_path / "dest"
        dest.mkdir()
        ep = _make_raw_endpoint(dest, encrypt="openssl_enc")
        feeder = subprocess.Popen(
            ["printf", "%s", self.PLAINTEXT.decode()], stdout=subprocess.PIPE
        )
        proc = ep.receive(feeder.stdout, "r6snap")
        if feeder.stdout:
            feeder.stdout.close()
        proc.wait()
        feeder.wait()
        assert proc.returncode == 0
        out = next(p for p in dest.iterdir() if p.name.startswith("r6snap"))
        data = out.read_bytes()
        assert self.PLAINTEXT not in data
        assert data[:8] == b"Salted__", "OpenSSL salted magic expected"
        dec = subprocess.run(
            [
                "openssl",
                "enc",
                "-d",
                "-aes-256-cbc",
                "-salt",
                "-pbkdf2",
                "-pass",
                "env:BTRFS_BACKUP_PASSPHRASE",
                "-in",
                str(out),
            ],
            capture_output=True,
        )
        assert dec.stdout == self.PLAINTEXT


class TestSSHRawEndpointEncryption:
    """raw+ssh:// must encrypt the stream LOCALLY before shipping it to the remote.

    SSHRawEndpoint inherits _build_receive_pipeline unchanged and only overrides
    the ssh shipping, so the encryption stage runs in the shared local pipeline.
    These prove the SSHRawEndpoint path actually builds that stage -- catching the
    audit's mutation that gated encryption on `not _is_remote` (shipping plaintext
    over SSH), which the whole raw suite otherwise missed.
    """

    @staticmethod
    def _ssh_raw(**enc):
        from pathlib import Path as _P

        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint

        ep = SSHRawEndpoint.__new__(SSHRawEndpoint)
        ep.compress = enc.get("compress")
        ep.encrypt = enc.get("encrypt")
        ep.gpg_recipient = enc.get("gpg_recipient")
        ep.gpg_keyring = enc.get("gpg_keyring")
        ep.openssl_cipher = enc.get("openssl_cipher", "aes-256-cbc")
        ep._is_remote = True  # SSHRawEndpoint is remote; the mutation gated on this
        return ep._build_receive_pipeline(_P("/tmp/r6-out"))

    def test_ssh_raw_pipeline_includes_gpg_stage(self):
        pipeline = self._ssh_raw(encrypt="gpg", gpg_recipient="KEYID")
        gpg = [s for s in pipeline if s and s[0] == "gpg"]
        assert gpg, "raw+ssh gpg config must build a gpg encryption stage"
        assert "--encrypt" in gpg[0] and "KEYID" in gpg[0]

    def test_ssh_raw_pipeline_includes_openssl_stage(self):
        pipeline = self._ssh_raw(encrypt="openssl_enc")
        enc = [s for s in pipeline if s and s[0] == "openssl"]
        assert enc, "raw+ssh openssl config must build an openssl encryption stage"
        assert "enc" in enc[0]

    def test_ssh_raw_plaintext_pipeline_has_no_encryption_stage(self):
        # Control: no encryption requested -> no gpg/openssl stage (the negative).
        pipeline = self._ssh_raw(encrypt=None)
        assert not [s for s in pipeline if s and s[0] in ("gpg", "openssl")]


def _ssh_localhost_works() -> bool:
    import subprocess

    try:
        r = subprocess.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=3",
                "localhost",
                "true",
            ],
            capture_output=True,
            timeout=8,
        )
        return r.returncode == 0
    except Exception:
        return False


@pytest.mark.skipif(
    __import__("shutil").which("gpg") is None or not _ssh_localhost_works(),
    reason="gpg + working passwordless ssh to localhost required",
)
class TestSSHRawRealRoundTrip:
    """Definitive: a real raw+ssh://localhost backup with encrypt=gpg produces a
    file on the (local)host that is genuine ciphertext and decrypts back."""

    def test_raw_ssh_localhost_gpg_roundtrip(self, tmp_path, monkeypatch):
        import subprocess

        gnupghome = tmp_path / "gnupg"
        gnupghome.mkdir(mode=0o700)
        monkeypatch.setenv("GNUPGHOME", str(gnupghome))
        subprocess.run(
            [
                "gpg",
                "--batch",
                "--pinentry-mode",
                "loopback",
                "--passphrase",
                "",
                "--quick-generate-key",
                "R6 SSH <r6-ssh@example.invalid>",
                "default",
                "default",
                "never",
            ],
            check=True,
            capture_output=True,
        )
        recipient = "r6-ssh@example.invalid"

        dest = tmp_path / "dest"
        dest.mkdir()
        from btrfs_backup_ng.endpoint import choose_endpoint

        ep = choose_endpoint(
            f"raw+ssh://localhost{dest}",
            {"path": str(dest), "encrypt": "gpg", "gpg_recipient": recipient},
        )
        plaintext = b"RAW-SSH-STREAM-SECRET-0xdeadbeef"
        feeder = subprocess.Popen(
            ["printf", "%s", plaintext.decode()], stdout=subprocess.PIPE
        )
        proc = ep.receive(feeder.stdout, "r6snap")
        if feeder.stdout:
            feeder.stdout.close()
        proc.wait()
        feeder.wait()
        assert proc.returncode == 0
        out = next(p for p in dest.iterdir() if p.name.startswith("r6snap"))
        data = out.read_bytes()
        assert plaintext not in data, "raw+ssh output must NOT contain plaintext"
        dec = subprocess.run(
            ["gpg", "--batch", "--yes", "--decrypt", str(out)], capture_output=True
        )
        assert dec.stdout == plaintext
