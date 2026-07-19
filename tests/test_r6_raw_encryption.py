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
