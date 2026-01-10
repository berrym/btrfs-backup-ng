"""Tests for btrbk raw target import functionality."""

from btrfs_backup_ng.btrbk_import import convert_to_toml, parse_btrbk_config


class TestBtrbkRawTargetImport:
    """Tests for btrbk raw target conversion."""

    def test_raw_target_with_compression(self):
        """Test converting a raw target with compression."""
        config_content = """
volume /mnt/data
  subvolume home
    raw_target_compress zstd
    target /mnt/backup/home
"""
        btrbk_config = parse_btrbk_config(config_content)
        toml_content, warnings = convert_to_toml(btrbk_config)

        # Should convert to raw:// URL
        assert 'path = "raw:///mnt/backup/home"' in toml_content
        assert 'compress = "zstd"' in toml_content

    def test_raw_target_with_gpg_encryption(self):
        """Test converting a raw target with GPG encryption."""
        config_content = """
volume /mnt/data
  subvolume home
    raw_target_encrypt gpg
    gpg_recipient backup@example.com
    target /mnt/backup/home
"""
        btrbk_config = parse_btrbk_config(config_content)
        toml_content, warnings = convert_to_toml(btrbk_config)

        assert 'path = "raw:///mnt/backup/home"' in toml_content
        assert 'encrypt = "gpg"' in toml_content
        assert 'gpg_recipient = "backup@example.com"' in toml_content

    def test_raw_target_with_compression_and_encryption(self):
        """Test converting a raw target with both compression and encryption."""
        config_content = """
volume /mnt/data
  subvolume home
    raw_target_compress lz4
    raw_target_encrypt gpg
    gpg_recipient backup@example.com
    gpg_keyring /etc/backup/keyring.gpg
    target /mnt/backup/home
"""
        btrbk_config = parse_btrbk_config(config_content)
        toml_content, warnings = convert_to_toml(btrbk_config)

        assert 'path = "raw:///mnt/backup/home"' in toml_content
        assert 'compress = "lz4"' in toml_content
        assert 'encrypt = "gpg"' in toml_content
        assert 'gpg_recipient = "backup@example.com"' in toml_content
        assert 'gpg_keyring = "/etc/backup/keyring.gpg"' in toml_content

    def test_raw_target_ssh_conversion(self):
        """Test converting a remote raw target to raw+ssh://."""
        config_content = """
volume /mnt/data
  subvolume home
    raw_target_compress zstd
    target backup@nas:/backup/home
"""
        btrbk_config = parse_btrbk_config(config_content)
        toml_content, warnings = convert_to_toml(btrbk_config)

        assert 'path = "raw+ssh://backup@nas:/backup/home"' in toml_content
        assert 'compress = "zstd"' in toml_content

    def test_global_raw_target_options(self):
        """Test that global raw target options are inherited."""
        config_content = """
raw_target_compress gzip
gpg_recipient admin@company.com

volume /mnt/data
  raw_target_encrypt gpg
  subvolume home
    target /mnt/backup/home
"""
        btrbk_config = parse_btrbk_config(config_content)
        toml_content, warnings = convert_to_toml(btrbk_config)

        # Should inherit compression from global, encryption from volume
        assert 'compress = "gzip"' in toml_content
        assert 'encrypt = "gpg"' in toml_content
        assert 'gpg_recipient = "admin@company.com"' in toml_content

    def test_openssl_enc_support(self):
        """Test that openssl_enc encryption is converted correctly."""
        config_content = """
volume /mnt/data
  subvolume home
    raw_target_encrypt openssl_enc
    target /mnt/backup/home
"""
        btrbk_config = parse_btrbk_config(config_content)
        toml_content, warnings = convert_to_toml(btrbk_config)

        # Should convert to raw:// with openssl_enc
        assert 'path = "raw:///mnt/backup/home"' in toml_content
        assert 'encrypt = "openssl_enc"' in toml_content
        # Should have info about passphrase environment variable
        assert any("BTRFS_BACKUP_PASSPHRASE" in w for w in warnings)

    def test_missing_gpg_recipient_warning(self):
        """Test that missing GPG recipient generates a warning."""
        config_content = """
volume /mnt/data
  subvolume home
    raw_target_encrypt gpg
    target /mnt/backup/home
"""
        btrbk_config = parse_btrbk_config(config_content)
        toml_content, warnings = convert_to_toml(btrbk_config)

        assert any("gpg_recipient" in w for w in warnings)

    def test_mixed_raw_and_normal_targets(self):
        """Test config with both raw and normal targets."""
        config_content = """
volume /mnt/data
  subvolume home
    # Raw target with compression
    raw_target_compress zstd
    target /mnt/backup/home-raw

  subvolume var
    # Normal btrfs target (no raw options)
    target /mnt/backup/var
"""
        btrbk_config = parse_btrbk_config(config_content)
        toml_content, warnings = convert_to_toml(btrbk_config)

        # Raw target should have raw:// prefix
        assert 'path = "raw:///mnt/backup/home-raw"' in toml_content
        assert 'compress = "zstd"' in toml_content

        # Normal target should NOT have raw:// prefix
        assert 'path = "/mnt/backup/var"' in toml_content

    def test_all_compression_algorithms(self):
        """Test that all btrbk compression algorithms are mapped correctly."""
        algorithms = ["gzip", "pigz", "bzip2", "pbzip2", "xz", "lzo", "lz4", "zstd"]

        for algo in algorithms:
            config_content = f"""
volume /mnt/data
  subvolume test
    raw_target_compress {algo}
    target /mnt/backup/test
"""
            btrbk_config = parse_btrbk_config(config_content)
            toml_content, warnings = convert_to_toml(btrbk_config)

            assert f'compress = "{algo}"' in toml_content, (
                f"Algorithm {algo} not mapped"
            )

    def test_no_raw_options_means_normal_target(self):
        """Test that targets without raw options remain as normal btrfs targets."""
        config_content = """
volume /mnt/data
  subvolume home
    target /mnt/backup/home
    target backup@nas:/backup/home
"""
        btrbk_config = parse_btrbk_config(config_content)
        toml_content, warnings = convert_to_toml(btrbk_config)

        # Local target - no raw:// prefix
        assert 'path = "/mnt/backup/home"' in toml_content
        # SSH target - ssh:// not raw+ssh://
        assert 'path = "ssh://backup@nas:/backup/home"' in toml_content
        # Should have ssh_sudo comment for normal SSH target
        assert "ssh_sudo = true" in toml_content

    def test_raw_ssh_target_no_sudo_comment(self):
        """Test that raw+ssh targets don't get ssh_sudo comment."""
        config_content = """
volume /mnt/data
  subvolume home
    raw_target_compress zstd
    target backup@nas:/backup/home
"""
        btrbk_config = parse_btrbk_config(config_content)
        toml_content, warnings = convert_to_toml(btrbk_config)

        # Should use raw+ssh://
        assert 'path = "raw+ssh://backup@nas:/backup/home"' in toml_content
        # Raw targets don't need sudo for btrfs receive (they write files)
        # The ssh_sudo comment should NOT appear after raw target path
        lines = toml_content.split("\n")
        for i, line in enumerate(lines):
            if "raw+ssh://" in line:
                # Check next non-empty line isn't ssh_sudo
                for j in range(i + 1, min(i + 3, len(lines))):
                    if lines[j].strip() and not lines[j].startswith("#"):
                        assert "ssh_sudo" not in lines[j]
                        break


class TestBtrbkRawTargetParsing:
    """Tests for btrbk raw target option parsing."""

    def test_parse_raw_target_compress(self):
        """Test parsing raw_target_compress option."""
        config_content = """
raw_target_compress zstd
volume /mnt/data
  subvolume home
    target /backup
"""
        btrbk_config = parse_btrbk_config(config_content)
        assert btrbk_config.global_options.get("raw_target_compress") == "zstd"

    def test_parse_raw_target_encrypt(self):
        """Test parsing raw_target_encrypt option."""
        config_content = """
volume /mnt/data
  raw_target_encrypt gpg
  subvolume home
    target /backup
"""
        btrbk_config = parse_btrbk_config(config_content)
        assert btrbk_config.volumes[0].options.get("raw_target_encrypt") == "gpg"

    def test_parse_gpg_options(self):
        """Test parsing GPG-related options."""
        config_content = """
gpg_recipient admin@example.com
gpg_keyring /etc/backup/pubring.gpg

volume /mnt/data
  subvolume home
    target /backup
"""
        btrbk_config = parse_btrbk_config(config_content)
        assert btrbk_config.global_options.get("gpg_recipient") == "admin@example.com"
        assert (
            btrbk_config.global_options.get("gpg_keyring") == "/etc/backup/pubring.gpg"
        )

    def test_option_inheritance(self):
        """Test that raw target options inherit correctly."""
        config_content = """
# Global level
raw_target_compress gzip

volume /mnt/data
  # Volume level overrides global
  raw_target_compress zstd

  subvolume home
    # Subvolume level overrides volume
    raw_target_compress lz4
    target /backup/home

  subvolume var
    # Uses volume level (zstd)
    target /backup/var
"""
        btrbk_config = parse_btrbk_config(config_content)

        # Check parsed options at each level
        assert btrbk_config.global_options.get("raw_target_compress") == "gzip"
        assert btrbk_config.volumes[0].options.get("raw_target_compress") == "zstd"
        assert (
            btrbk_config.volumes[0].subvolumes[0].options.get("raw_target_compress")
            == "lz4"
        )
        # var subvolume inherits from volume
        assert (
            btrbk_config.volumes[0].subvolumes[1].options.get("raw_target_compress")
            is None
        )
