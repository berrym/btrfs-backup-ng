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

    def test_import_timestamp_format_short(self, tmp_path):
        """Test converting btrbk 'short' timestamp format."""
        config = """
timestamp_format short
volume /mnt/pool
  subvolume home
    target /mnt/backup
"""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(config)

        toml_output, warnings = import_btrbk_config(config_path)

        # short -> %Y%m%d
        assert 'timestamp_format = "%Y%m%d"' in toml_output

    def test_import_timestamp_format_long(self, tmp_path):
        """Test converting btrbk 'long' timestamp format (default)."""
        config = """
timestamp_format long
volume /mnt/pool
  subvolume home
    target /mnt/backup
"""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(config)

        toml_output, warnings = import_btrbk_config(config_path)

        # long -> %Y%m%dT%H%M
        assert 'timestamp_format = "%Y%m%dT%H%M"' in toml_output

    def test_import_timestamp_format_long_iso(self, tmp_path):
        """Test converting btrbk 'long-iso' timestamp format."""
        config = """
timestamp_format long-iso
volume /mnt/pool
  subvolume home
    target /mnt/backup
"""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(config)

        toml_output, warnings = import_btrbk_config(config_path)

        # long-iso -> %Y%m%dT%H%M%S%z
        assert 'timestamp_format = "%Y%m%dT%H%M%S%z"' in toml_output

    def test_import_timestamp_format_default(self, tmp_path):
        """Test default timestamp format when not specified (btrbk >= 0.32 uses long)."""
        config = """
volume /mnt/pool
  subvolume home
    target /mnt/backup
"""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(config)

        toml_output, warnings = import_btrbk_config(config_path)

        # Default is 'long' -> %Y%m%dT%H%M
        assert 'timestamp_format = "%Y%m%dT%H%M"' in toml_output

    def test_import_timestamp_format_unknown(self, tmp_path):
        """Test handling of unknown timestamp format."""
        config = """
timestamp_format custom_format
volume /mnt/pool
  subvolume home
    target /mnt/backup
"""
        config_path = tmp_path / "btrbk.conf"
        config_path.write_text(config)

        toml_output, warnings = import_btrbk_config(config_path)

        # Should fall back to 'long' and generate warning
        assert 'timestamp_format = "%Y%m%dT%H%M"' in toml_output
        assert any("custom_format" in w for w in warnings)


class TestBtrbkRetentionFidelity:
    """0.9.2 Phase 2B-i: btrbk retention must translate without silent policy loss."""

    @staticmethod
    def _toml(config: str):
        from btrfs_backup_ng.btrbk_import import convert_to_toml, parse_btrbk_config

        return convert_to_toml(parse_btrbk_config(config))

    # --- _translate_preserve_min --------------------------------------------

    def test_min_months_unit_remapped(self):
        """btrbk 'm' = MONTHS must become btrfs-backup-ng 'M', not minutes ('m')."""
        from btrfs_backup_ng.btrbk_import import _translate_preserve_min

        assert _translate_preserve_min("3m") == ("3M", [])

    def test_min_hours_days_weeks_years_passthrough(self):
        from btrfs_backup_ng.btrbk_import import _translate_preserve_min

        assert _translate_preserve_min("48h")[0] == "48h"
        assert _translate_preserve_min("2d")[0] == "2d"
        assert _translate_preserve_min("4w")[0] == "4w"
        assert _translate_preserve_min("1y")[0] == "1y"

    def test_min_no_maps_to_zero_age(self):
        from btrfs_backup_ng.btrbk_import import _translate_preserve_min

        assert _translate_preserve_min("no") == ("0s", [])

    def test_min_all_and_latest_warn_and_fall_back(self):
        from btrfs_backup_ng.btrbk_import import _translate_preserve_min

        for token in ("all", "latest"):
            value, warns = _translate_preserve_min(token)
            assert value == "1d"
            assert warns, f"{token} should warn"

    def test_min_unrecognized_warns(self):
        from btrfs_backup_ng.btrbk_import import _translate_preserve_min

        value, warns = _translate_preserve_min("bogus")
        assert value == "1d"
        assert warns

    # --- yearly (BUG #3) -----------------------------------------------------

    def test_yearly_is_emitted(self):
        """A btrbk yearly count must survive to the TOML (was silently dropped)."""
        toml, _ = self._toml(
            "snapshot_preserve 5y\n\nvolume /mnt/p\n  subvolume s\n    target /mnt/b\n"
        )
        assert "yearly = 5" in toml

    def test_yearly_default_emitted_when_absent(self):
        toml, _ = self._toml("volume /mnt/p\n  subvolume s\n    target /mnt/b\n")
        assert "yearly = 0" in toml

    # --- subvolume override (BUG #2) ----------------------------------------

    def test_subvolume_override_emits_volumes_retention(self):
        toml, _ = self._toml(
            "snapshot_preserve 30d\n\nvolume /mnt/p\n"
            "  subvolume home\n    snapshot_preserve 7d 2w\n    target /mnt/b\n"
        )
        assert "[volumes.retention]" in toml
        assert "daily = 7" in toml
        assert "weekly = 2" in toml

    def test_no_override_omits_volumes_retention(self):
        toml, _ = self._toml(
            "snapshot_preserve 30d\n\nvolume /mnt/p\n  subvolume home\n    target /mnt/b\n"
        )
        assert "[volumes.retention]" not in toml

    # --- warnings (BUG #1/#4/#5/#6) -----------------------------------------

    def test_target_preserve_divergence_warns(self):
        _, warns = self._toml(
            "snapshot_preserve 7d\ntarget_preserve 30d\n\n"
            "volume /mnt/p\n  subvolume s\n    target /mnt/b\n"
        )
        assert any(
            "target_preserve" in w and "NOT applied separately" in w for w in warns
        )

    def test_day_of_week_warns(self):
        _, warns = self._toml(
            "preserve_day_of_week sunday\n\nvolume /mnt/p\n  subvolume s\n    target /mnt/b\n"
        )
        assert any("preserve_day_of_week" in w for w in warns)

    def test_unrecognized_preserve_warns_not_silent_prune(self):
        """A 'latest' snapshot_preserve maps to all-zeros; warn instead of silently
        keeping no snapshots."""
        _, warns = self._toml(
            "snapshot_preserve latest\n\nvolume /mnt/p\n  subvolume s\n    target /mnt/b\n"
        )
        assert any("not understood" in w or "NO periodic" in w for w in warns)

    def test_warnings_are_deduped(self):
        _, warns = self._toml(
            "preserve_day_of_week sunday\n\nvolume /mnt/p\n  subvolume s\n    target /mnt/b\n"
        )
        assert len(warns) == len(set(warns))

    # --- anti-theater: generated TOML must LOAD -----------------------------

    def test_generated_toml_loads_and_is_faithful(self, tmp_path):
        from btrfs_backup_ng.config.loader import load_config

        toml, _ = self._toml(
            "snapshot_preserve_min 3m\nsnapshot_preserve 24h 14d 4w 6m 2y\n\n"
            "volume /mnt/pool\n"
            "  subvolume home\n    snapshot_preserve_min 2w\n    snapshot_preserve 7d\n    target /mnt/b/home\n"
        )
        p = tmp_path / "out.toml"
        p.write_text(toml)
        cfg, load_warns = load_config(p)

        gr = cfg.global_config.retention
        assert gr.min == "3M"  # months, not minutes
        assert gr.yearly == 2  # not dropped
        assert gr.daily == 14
        v = cfg.volumes[0]
        assert v.retention is not None
        assert v.retention.min == "2w"
        assert v.retention.daily == 7
        # Phase 2A interaction: the generated config trips no unknown-key warnings.
        assert not [w for w in load_warns if "Unknown config key" in w]

    def test_special_min_token_produces_loadable_config(self, tmp_path):
        """btrbk 'snapshot_preserve_min latest' previously emitted min = "latest",
        which the loader REJECTS. It must now produce a loadable config."""
        from btrfs_backup_ng.config.loader import load_config

        toml, _ = self._toml(
            "snapshot_preserve_min latest\n\nvolume /mnt/p\n  subvolume s\n    target /mnt/b\n"
        )
        p = tmp_path / "out.toml"
        p.write_text(toml)
        cfg, _ = load_config(p)  # must not raise ConfigError
        assert cfg.global_config.retention.min == "1d"


class TestBtrbkRetentionWarningQuality:
    """0.9.2 Phase 2B-i review fixes: retention warnings must be accurate & distinct."""

    @staticmethod
    def _toml(config: str):
        from btrfs_backup_ng.btrbk_import import convert_to_toml, parse_btrbk_config

        return convert_to_toml(parse_btrbk_config(config))

    def test_per_subvolume_divergence_warnings_are_distinct(self):
        """Two subvolumes diverging with the SAME target_preserve value must each be
        reported (the dedup must not collapse them)."""
        _, warns = self._toml(
            "volume /mnt/p\n"
            "  subvolume home\n    snapshot_preserve 7d\n    target_preserve 30d\n    target /mnt/b/home\n"
            "  subvolume data\n    snapshot_preserve 7d\n    target_preserve 30d\n    target /mnt/b/data\n"
        )
        div = [w for w in warns if "target_preserve" in w]
        assert len(div) == 2
        assert any("/mnt/p/home" in w for w in div)
        assert any("/mnt/p/data" in w for w in div)

    def test_divergence_message_accurate_when_no_snapshot_preserve(self):
        """When target_preserve is set but snapshot_preserve is absent, the warning
        must say the DEFAULT was used (not 'snapshot_preserve was used')."""
        _, warns = self._toml(
            "target_preserve 30d\n\nvolume /mnt/p\n  subvolume s\n    target /mnt/b\n"
        )
        div = [w for w in warns if "target_preserve" in w]
        assert div
        assert any("default retention was used" in w for w in div)
        assert not any("snapshot_preserve was used" in w for w in div)

    def test_zero_bucket_value_not_labelled_not_understood(self):
        """'0d' parses fine (intentional zero) -- warn it keeps nothing, but do NOT
        label it 'not understood'."""
        from btrfs_backup_ng.btrbk_import import _parse_preserve_counts

        _, warns = _parse_preserve_counts("0d")
        assert warns
        assert all("not understood" not in w for w in warns)

    def test_unparseable_value_labelled_not_understood(self):
        from btrfs_backup_ng.btrbk_import import _parse_preserve_counts

        _, warns = _parse_preserve_counts("latest")
        assert any("not understood" in w for w in warns)

    def test_reordered_target_preserve_does_not_warn(self):
        """Semantically-equal target_preserve (reordered tokens) must NOT warn."""
        _, warns = self._toml(
            "snapshot_preserve 14d 4w\ntarget_preserve 4w 14d\n\n"
            "volume /mnt/p\n  subvolume s\n    target /mnt/b\n"
        )
        assert not any("target_preserve" in w for w in warns)

    def test_differing_target_preserve_still_warns(self):
        _, warns = self._toml(
            "snapshot_preserve 14d\ntarget_preserve 30d\n\n"
            "volume /mnt/p\n  subvolume s\n    target /mnt/b\n"
        )
        assert any("target_preserve" in w for w in warns)

    def test_archive_preserve_warns(self):
        _, warns = self._toml(
            "archive_preserve 12m\n\nvolume /mnt/p\n  subvolume s\n    target /mnt/b\n"
        )
        assert any("archive_preserve" in w for w in warns)
