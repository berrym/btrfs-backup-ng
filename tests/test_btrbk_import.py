"""Tests for btrbk config importer."""

import pytest

from btrfs_backup_ng.btrbk_import import (
    BtrbkLexer,
    TokenType,
    import_btrbk_config,
    parse_btrbk_config,
    parse_btrbk_retention,
)


class TestBtrbkLexer:
    """Tests for btrbk config lexer."""

    def test_tokenize_simple_option(self):
        """Test tokenizing a simple option."""
        lexer = BtrbkLexer("snapshot_preserve 7d")
        tokens = lexer.tokenize()

        # Filter out newlines and EOF for easier testing
        meaningful = [
            t for t in tokens if t.type not in (TokenType.NEWLINE, TokenType.EOF)
        ]

        assert len(meaningful) == 2
        assert meaningful[0].type == TokenType.KEYWORD
        assert meaningful[0].value == "snapshot_preserve"
        assert meaningful[1].type == TokenType.VALUE
        assert meaningful[1].value == "7d"

    def test_tokenize_multi_value_option(self):
        """Test tokenizing option with multiple values."""
        lexer = BtrbkLexer("snapshot_preserve 14d 4w 6m")
        tokens = lexer.tokenize()

        values = [t.value for t in tokens if t.type == TokenType.VALUE]
        assert values == ["14d", "4w", "6m"]

    def test_tokenize_volume(self):
        """Test tokenizing volume directive."""
        lexer = BtrbkLexer("volume /mnt/btr_pool")
        tokens = lexer.tokenize()

        meaningful = [
            t for t in tokens if t.type not in (TokenType.NEWLINE, TokenType.EOF)
        ]

        assert len(meaningful) == 2
        assert meaningful[0].type == TokenType.KEYWORD
        assert meaningful[0].value == "volume"
        assert meaningful[1].type == TokenType.VALUE
        assert meaningful[1].value == "/mnt/btr_pool"

    def test_tokenize_subvolume(self):
        """Test tokenizing subvolume directive."""
        lexer = BtrbkLexer("subvolume home")
        tokens = lexer.tokenize()

        meaningful = [
            t for t in tokens if t.type not in (TokenType.NEWLINE, TokenType.EOF)
        ]

        assert len(meaningful) == 2
        assert meaningful[0].type == TokenType.KEYWORD
        assert meaningful[0].value == "subvolume"
        assert meaningful[1].type == TokenType.VALUE
        assert meaningful[1].value == "home"

    def test_tokenize_target(self):
        """Test tokenizing target directive."""
        lexer = BtrbkLexer("target /mnt/backup/home")
        tokens = lexer.tokenize()

        meaningful = [
            t for t in tokens if t.type not in (TokenType.NEWLINE, TokenType.EOF)
        ]

        assert len(meaningful) == 2
        assert meaningful[0].type == TokenType.KEYWORD
        assert meaningful[0].value == "target"
        assert meaningful[1].type == TokenType.VALUE
        assert meaningful[1].value == "/mnt/backup/home"

    def test_tokenize_ssh_target(self):
        """Test tokenizing SSH target."""
        lexer = BtrbkLexer("target ssh://backup@server/backups")
        tokens = lexer.tokenize()

        values = [t for t in tokens if t.type == TokenType.VALUE]
        assert len(values) == 1
        assert "ssh://" in values[0].value

    def test_tokenize_comment(self):
        """Test that comments are captured."""
        lexer = BtrbkLexer("# This is a comment\nsnapshot_preserve 7d")
        tokens = lexer.tokenize()

        keywords = [t for t in tokens if t.type == TokenType.KEYWORD]
        assert len(keywords) == 1
        assert keywords[0].value == "snapshot_preserve"

    def test_tokenize_empty_lines(self):
        """Test that empty lines produce only newlines."""
        lexer = BtrbkLexer("\n\nsnapshot_preserve 7d\n\n")
        tokens = lexer.tokenize()

        keywords = [t for t in tokens if t.type == TokenType.KEYWORD]
        assert len(keywords) == 1

    def test_tokenize_indented_content(self):
        """Test tokenizing indented content."""
        config = """
volume /mnt/pool
  subvolume home
    target /mnt/backup
"""
        lexer = BtrbkLexer(config)
        tokens = lexer.tokenize()

        keywords = [t for t in tokens if t.type == TokenType.KEYWORD]
        assert len(keywords) == 3
        assert keywords[0].value == "volume"
        assert keywords[1].value == "subvolume"
        assert keywords[2].value == "target"


class TestBtrbkParser:
    """Tests for btrbk config parser."""

    def test_parse_simple_config(self):
        """Test parsing a simple configuration."""
        config = """
snapshot_preserve 7d

volume /mnt/pool
  subvolume home
    target /mnt/backup
"""
        result = parse_btrbk_config(config)

        assert result is not None
        assert "snapshot_preserve" in result.global_options
        assert len(result.volumes) == 1

    def test_parse_volume_with_subvolumes(self):
        """Test parsing volume with multiple subvolumes."""
        config = """
volume /mnt/pool
  subvolume home
    target /mnt/backup/home
  subvolume var
    target /mnt/backup/var
"""
        result = parse_btrbk_config(config)

        assert len(result.volumes) == 1
        assert len(result.volumes[0].subvolumes) == 2

    def test_parse_options_inheritance(self):
        """Test that options are stored in correct scope."""
        config = """
snapshot_preserve 7d

volume /mnt/pool
  snapshot_preserve 14d
  subvolume home
"""
        result = parse_btrbk_config(config)

        # Global should have 7d
        assert result.global_options.get("snapshot_preserve") == "7d"

        # Volume should have 14d
        assert result.volumes[0].options.get("snapshot_preserve") == "14d"

    def test_parse_target_options(self):
        """Test parsing target-specific options."""
        config = """
volume /mnt/pool
  subvolume home
    target ssh://backup@server/backups
      backend btrfs-progs-sudo
"""
        result = parse_btrbk_config(config)

        # The target itself doesn't capture options after it in this parser
        # Options apply to current scope
        target = result.volumes[0].subvolumes[0].targets[0]
        assert target.path == "ssh://backup@server/backups"


class TestParseBtrbkRetention:
    """Tests for retention parsing."""

    def test_parse_days(self):
        """Test parsing day retention."""
        result = parse_btrbk_retention("7d")
        assert result.get("daily") == 7

    def test_parse_weeks(self):
        """Test parsing week retention."""
        result = parse_btrbk_retention("4w")
        assert result.get("weekly") == 4

    def test_parse_months(self):
        """Test parsing month retention."""
        result = parse_btrbk_retention("6m")
        assert result.get("monthly") == 6

    def test_parse_combined(self):
        """Test parsing combined retention."""
        result = parse_btrbk_retention("14d 4w 6m")
        assert result.get("daily") == 14
        assert result.get("weekly") == 4
        assert result.get("monthly") == 6

    def test_parse_hours(self):
        """Test parsing hour retention."""
        result = parse_btrbk_retention("24h")
        assert result.get("hourly") == 24

    def test_parse_years(self):
        """Test parsing year retention."""
        result = parse_btrbk_retention("5y")
        assert result.get("yearly") == 5

    def test_parse_all(self):
        """Test parsing 'all' value."""
        result = parse_btrbk_retention("all")
        assert result.get("daily") == 999
        assert result.get("hourly") == 999

    def test_parse_none(self):
        """Test parsing 'none' value."""
        result = parse_btrbk_retention("none")
        assert result.get("daily") == 0
        assert result.get("hourly") == 0

    def test_parse_wildcard(self):
        """Test parsing wildcard count."""
        result = parse_btrbk_retention("*d")
        assert result.get("daily") == 999


class TestBtrbkLexerMore:
    """Additional tests for btrbk lexer."""

    def test_tokenize_quoted_string(self):
        """Test tokenizing quoted strings."""
        lexer = BtrbkLexer('snapshot_dir "/path/with spaces"')
        tokens = lexer.tokenize()

        values = [t for t in tokens if t.type == TokenType.VALUE]
        assert len(values) == 1
        assert values[0].value == "/path/with spaces"

    def test_tokenize_single_quoted_string(self):
        """Test tokenizing single-quoted strings."""
        lexer = BtrbkLexer("snapshot_dir '/path/with spaces'")
        tokens = lexer.tokenize()

        values = [t for t in tokens if t.type == TokenType.VALUE]
        assert len(values) == 1
        assert values[0].value == "/path/with spaces"

    def test_tokenize_escaped_quote(self):
        """Test tokenizing escaped quotes in strings."""
        lexer = BtrbkLexer('path "test\\"value"')
        lexer.tokenize()
        # Should handle escaped quotes

    def test_tokenize_special_chars(self):
        """Test tokenizing special characters."""
        lexer = BtrbkLexer("target user@host:2222/path")
        tokens = lexer.tokenize()

        values = [t for t in tokens if t.type == TokenType.VALUE]
        assert len(values) == 1
        assert "user@host" in values[0].value


class TestBtrbkParserMore:
    """Additional tests for btrbk parser."""

    def test_parse_missing_path_after_volume(self):
        """Test parsing volume without path."""
        config = "volume\nsubvolume home"
        result = parse_btrbk_config(config)
        # Should generate warning
        assert len(result.warnings) > 0

    def test_parse_missing_path_after_subvolume(self):
        """Test parsing subvolume without path."""
        config = "volume /mnt/pool\nsubvolume\ntarget /backup"
        result = parse_btrbk_config(config)
        assert len(result.warnings) > 0

    def test_parse_missing_path_after_target(self):
        """Test parsing target without path."""
        config = "volume /mnt/pool\nsubvolume home\ntarget"
        result = parse_btrbk_config(config)
        assert len(result.warnings) > 0

    def test_parse_target_outside_section(self):
        """Test parsing target outside volume/subvolume."""
        config = "target /backup"
        result = parse_btrbk_config(config)
        assert len(result.warnings) > 0

    def test_parse_subvolume_outside_volume(self):
        """Test parsing subvolume outside volume."""
        config = "subvolume home"
        result = parse_btrbk_config(config)
        assert len(result.warnings) > 0


class TestImportBtrbkConfig:
    """Tests for full btrbk config import."""

    def test_import_full_config(self, sample_btrbk_config, tmp_path):
        """Test importing a full btrbk configuration."""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(sample_btrbk_config)

        toml_output, warnings = import_btrbk_config(config_path)

        assert toml_output is not None
        assert len(toml_output) > 0
        assert "[[volumes]]" in toml_output
        assert "path" in toml_output

    def test_import_warns_about_subvolume_dot(self, tmp_path):
        """Test warning about 'subvolume .' anti-pattern."""
        config = """
volume /mnt/pool/home
  subvolume .
    target /mnt/backup
"""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(config)

        toml_output, warnings = import_btrbk_config(config_path)

        assert any(
            "subvolume ." in w.lower() or "subvolume" in w.lower() for w in warnings
        )

    def test_import_warns_about_volume_root(self, tmp_path):
        """Test warning about 'volume /' pattern."""
        config = """
volume /
  subvolume home
    target /mnt/backup
"""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(config)

        toml_output, warnings = import_btrbk_config(config_path)

        # Should warn about root volume
        assert any("root" in w.lower() or "/" in w for w in warnings)

    def test_import_converts_ssh_sudo(self, tmp_path):
        """Test that ssh targets get ssh_sudo suggestion."""
        config = """
volume /mnt/pool
  subvolume home
    target ssh://backup@server/backups
"""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(config)

        toml_output, warnings = import_btrbk_config(config_path)

        assert "ssh_sudo" in toml_output

    def test_import_preserves_retention(self, tmp_path):
        """Test that retention values are converted."""
        config = """
snapshot_preserve 14d 4w 6m

volume /mnt/pool
  subvolume home
    target /mnt/backup
"""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(config)

        toml_output, warnings = import_btrbk_config(config_path)

        assert "daily = 14" in toml_output
        assert "weekly = 4" in toml_output
        assert "monthly = 6" in toml_output

    def test_import_nonexistent_file(self, tmp_path):
        """Test importing nonexistent file."""
        with pytest.raises(FileNotFoundError):
            import_btrbk_config(tmp_path / "nonexistent.conf")

    def test_import_scp_style_path(self, tmp_path):
        """Test converting SCP-style target path."""
        config = """
volume /mnt/pool
  subvolume home
    target user@host:/path/to/backup
"""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(config)

        toml_output, warnings = import_btrbk_config(config_path)

        # Should convert to ssh:// format
        assert "ssh://" in toml_output

    def test_import_local_path_unchanged(self, tmp_path):
        """Test that local paths remain unchanged."""
        config = """
volume /mnt/pool
  subvolume home
    target /mnt/backup/home
"""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(config)

        toml_output, warnings = import_btrbk_config(config_path)

        assert 'path = "/mnt/backup/home"' in toml_output
