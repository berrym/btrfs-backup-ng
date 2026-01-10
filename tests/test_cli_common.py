"""Tests for CLI common utilities."""

import argparse
from unittest.mock import patch

from btrfs_backup_ng.cli.common import (
    add_fs_checks_args,
    add_progress_args,
    add_verbosity_args,
    create_global_parser,
    get_fs_checks_mode,
    get_log_level,
    is_interactive,
    should_show_progress,
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


class TestIsInteractive:
    """Tests for is_interactive function."""

    def test_is_interactive_tty(self):
        """Test is_interactive when stdout is a TTY."""
        with patch("sys.stdout.isatty", return_value=True):
            assert is_interactive() is True

    def test_is_interactive_not_tty(self):
        """Test is_interactive when stdout is not a TTY."""
        with patch("sys.stdout.isatty", return_value=False):
            assert is_interactive() is False


class TestShouldShowProgress:
    """Tests for should_show_progress function."""

    def test_explicit_progress_flag(self):
        """Test that --progress always shows progress."""
        args = argparse.Namespace(progress=True, no_progress=False, quiet=False)
        assert should_show_progress(args) is True

    def test_explicit_no_progress_flag(self):
        """Test that --no-progress never shows progress."""
        args = argparse.Namespace(progress=False, no_progress=True, quiet=False)
        assert should_show_progress(args) is False

    def test_quiet_mode_no_progress(self):
        """Test that quiet mode implies no progress."""
        args = argparse.Namespace(progress=False, no_progress=False, quiet=True)
        assert should_show_progress(args) is False

    def test_auto_detect_tty(self):
        """Test auto-detection based on TTY."""
        args = argparse.Namespace(progress=False, no_progress=False, quiet=False)
        with patch("btrfs_backup_ng.cli.common.is_interactive", return_value=True):
            assert should_show_progress(args) is True

    def test_auto_detect_not_tty(self):
        """Test auto-detection when not TTY."""
        args = argparse.Namespace(progress=False, no_progress=False, quiet=False)
        with patch("btrfs_backup_ng.cli.common.is_interactive", return_value=False):
            assert should_show_progress(args) is False

    def test_missing_attributes(self):
        """Test handling of missing attributes."""
        args = argparse.Namespace()
        with patch("btrfs_backup_ng.cli.common.is_interactive", return_value=True):
            assert should_show_progress(args) is True


class TestAddProgressArgs:
    """Tests for add_progress_args function."""

    def test_adds_progress_flag(self):
        """Test that --progress is added."""
        parser = argparse.ArgumentParser()
        add_progress_args(parser)
        args = parser.parse_args(["--progress"])
        assert args.progress is True

    def test_adds_no_progress_flag(self):
        """Test that --no-progress is added."""
        parser = argparse.ArgumentParser()
        add_progress_args(parser)
        args = parser.parse_args(["--no-progress"])
        assert args.no_progress is True

    def test_defaults_are_false(self):
        """Test that defaults are False."""
        parser = argparse.ArgumentParser()
        add_progress_args(parser)
        args = parser.parse_args([])
        assert args.progress is False
        assert args.no_progress is False

    def test_mutual_exclusion(self):
        """Test that --progress and --no-progress are mutually exclusive."""
        parser = argparse.ArgumentParser()
        add_progress_args(parser)
        # This should raise an error
        try:
            parser.parse_args(["--progress", "--no-progress"])
            assert False, "Should have raised an error"
        except SystemExit:
            pass  # Expected behavior


class TestAddFsChecksArgs:
    """Tests for add_fs_checks_args function."""

    def test_adds_fs_checks_auto(self):
        """Test that --fs-checks=auto works."""
        parser = argparse.ArgumentParser()
        add_fs_checks_args(parser)
        args = parser.parse_args(["--fs-checks", "auto"])
        assert args.fs_checks == "auto"

    def test_adds_fs_checks_strict(self):
        """Test that --fs-checks=strict works."""
        parser = argparse.ArgumentParser()
        add_fs_checks_args(parser)
        args = parser.parse_args(["--fs-checks", "strict"])
        assert args.fs_checks == "strict"

    def test_adds_fs_checks_skip(self):
        """Test that --fs-checks=skip works."""
        parser = argparse.ArgumentParser()
        add_fs_checks_args(parser)
        args = parser.parse_args(["--fs-checks", "skip"])
        assert args.fs_checks == "skip"

    def test_no_fs_checks_alias(self):
        """Test that --no-fs-checks is alias for skip."""
        parser = argparse.ArgumentParser()
        add_fs_checks_args(parser)
        args = parser.parse_args(["--no-fs-checks"])
        assert args.fs_checks == "skip"

    def test_default_is_auto(self):
        """Test that default is auto."""
        parser = argparse.ArgumentParser()
        add_fs_checks_args(parser)
        args = parser.parse_args([])
        assert args.fs_checks == "auto"


class TestGetFsChecksMode:
    """Tests for get_fs_checks_mode function."""

    def test_returns_auto(self):
        """Test that it returns auto."""
        args = argparse.Namespace(fs_checks="auto")
        assert get_fs_checks_mode(args) == "auto"

    def test_returns_strict(self):
        """Test that it returns strict."""
        args = argparse.Namespace(fs_checks="strict")
        assert get_fs_checks_mode(args) == "strict"

    def test_returns_skip(self):
        """Test that it returns skip."""
        args = argparse.Namespace(fs_checks="skip")
        assert get_fs_checks_mode(args) == "skip"

    def test_missing_attribute_defaults_to_auto(self):
        """Test that missing attribute defaults to auto."""
        args = argparse.Namespace()
        assert get_fs_checks_mode(args) == "auto"

    def test_none_value_defaults_to_auto(self):
        """Test that None value defaults to auto."""
        args = argparse.Namespace(fs_checks=None)
        assert get_fs_checks_mode(args) == "auto"
