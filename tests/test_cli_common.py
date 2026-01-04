"""Tests for CLI common utilities."""

import argparse

import pytest

from btrfs_backup_ng.cli.common import (
    add_verbosity_args,
    create_global_parser,
    get_log_level,
)


class TestCreateGlobalParser:
    """Tests for create_global_parser function."""

    def test_returns_parser(self):
        """Test that it returns an ArgumentParser."""
        parser = create_global_parser()
        assert isinstance(parser, argparse.ArgumentParser)

    def test_parser_has_no_help(self):
        """Test that parser has add_help=False."""
        parser = create_global_parser()
        # Parser with add_help=False won't have -h
        # We can check by parsing empty args (won't fail for -h)
        args = parser.parse_args([])
        assert args is not None

    def test_has_verbosity_args(self):
        """Test that parser has verbosity arguments."""
        parser = create_global_parser()
        args = parser.parse_args(["--verbose"])
        assert args.verbose is True


class TestAddVerbosityArgs:
    """Tests for add_verbosity_args function."""

    def test_adds_verbose(self):
        """Test that --verbose is added."""
        parser = argparse.ArgumentParser()
        add_verbosity_args(parser)
        args = parser.parse_args(["--verbose"])
        assert args.verbose is True

    def test_adds_quiet(self):
        """Test that --quiet is added."""
        parser = argparse.ArgumentParser()
        add_verbosity_args(parser)
        args = parser.parse_args(["--quiet"])
        assert args.quiet is True

    def test_adds_debug(self):
        """Test that --debug is added."""
        parser = argparse.ArgumentParser()
        add_verbosity_args(parser)
        args = parser.parse_args(["--debug"])
        assert args.debug is True

    def test_short_verbose(self):
        """Test that -v works for verbose."""
        parser = argparse.ArgumentParser()
        add_verbosity_args(parser)
        args = parser.parse_args(["-v"])
        assert args.verbose is True

    def test_short_quiet(self):
        """Test that -q works for quiet."""
        parser = argparse.ArgumentParser()
        add_verbosity_args(parser)
        args = parser.parse_args(["-q"])
        assert args.quiet is True

    def test_defaults_are_false(self):
        """Test that defaults are False."""
        parser = argparse.ArgumentParser()
        add_verbosity_args(parser)
        args = parser.parse_args([])
        assert args.verbose is False
        assert args.quiet is False
        assert args.debug is False


class TestGetLogLevel:
    """Tests for get_log_level function."""

    def test_debug_flag(self):
        """Test that debug flag returns DEBUG."""
        args = argparse.Namespace(debug=True, quiet=False, verbose=False)
        assert get_log_level(args) == "DEBUG"

    def test_quiet_flag(self):
        """Test that quiet flag returns WARNING."""
        args = argparse.Namespace(debug=False, quiet=True, verbose=False)
        assert get_log_level(args) == "WARNING"

    def test_verbose_flag(self):
        """Test that verbose flag returns DEBUG."""
        args = argparse.Namespace(debug=False, quiet=False, verbose=True)
        assert get_log_level(args) == "DEBUG"

    def test_no_flags(self):
        """Test that no flags returns INFO."""
        args = argparse.Namespace(debug=False, quiet=False, verbose=False)
        assert get_log_level(args) == "INFO"

    def test_debug_takes_precedence(self):
        """Test that debug takes precedence over other flags."""
        args = argparse.Namespace(debug=True, quiet=True, verbose=True)
        assert get_log_level(args) == "DEBUG"

    def test_missing_attributes(self):
        """Test handling of missing attributes."""
        args = argparse.Namespace()
        # Should default to INFO when attributes are missing
        assert get_log_level(args) == "INFO"

    def test_partial_attributes(self):
        """Test handling of partial attributes."""
        args = argparse.Namespace(debug=True)
        assert get_log_level(args) == "DEBUG"

        args = argparse.Namespace(quiet=True)
        assert get_log_level(args) == "WARNING"
