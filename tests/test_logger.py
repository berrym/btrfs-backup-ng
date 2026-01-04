"""Tests for logger module."""

import logging


from btrfs_backup_ng.__logger__ import (
    RichLogger,
    create_logger,
    logger,
    set_level,
)


class TestSetLevel:
    """Tests for set_level function."""

    def test_set_level_with_string(self):
        """Test setting level with string."""
        set_level("DEBUG")
        assert logger.level == logging.DEBUG

    def test_set_level_with_constant(self):
        """Test setting level with logging constant."""
        set_level(logging.WARNING)
        assert logger.level == logging.WARNING

    def test_set_level_case_insensitive(self):
        """Test that string levels are case insensitive."""
        set_level("debug")
        assert logger.level == logging.DEBUG

        set_level("INFO")
        assert logger.level == logging.INFO

    def test_set_level_invalid_string(self):
        """Test that invalid string defaults to INFO."""
        set_level("INVALID")
        assert logger.level == logging.INFO

    def test_set_level_updates_handlers(self):
        """Test that handlers are also updated."""
        # Add a handler
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        set_level("ERROR")

        for h in logger.handlers:
            assert h.level == logging.ERROR

        # Clean up
        logger.removeHandler(handler)


class TestRichLogger:
    """Tests for RichLogger class."""

    def test_singleton_pattern(self):
        """Test that RichLogger is a singleton."""
        logger1 = RichLogger()
        logger2 = RichLogger()
        assert logger1 is logger2

    def test_initial_message(self):
        """Test that initial message is set."""
        rich_logger = RichLogger()
        assert "btrfs-backup-ng" in rich_logger.messages[0]

    def test_write_message(self):
        """Test writing a message."""
        rich_logger = RichLogger()
        len(rich_logger.messages)
        rich_logger.write("test message")
        assert "test message" in list(rich_logger.messages)

    def test_write_multiline(self):
        """Test writing multiline message."""
        rich_logger = RichLogger()
        rich_logger.write("line1\nline2\nline3")
        messages = list(rich_logger.messages)
        assert "line1" in messages
        assert "line2" in messages
        assert "line3" in messages

    def test_write_returns_zero(self):
        """Test that write returns 0."""
        rich_logger = RichLogger()
        result = rich_logger.write("test")
        assert result == 0

    def test_flush_does_nothing(self):
        """Test that flush is a no-op."""
        rich_logger = RichLogger()
        # Should not raise
        rich_logger.flush()

    def test_max_messages(self):
        """Test that messages are limited to maxlen."""
        rich_logger = RichLogger()
        # Write more than maxlen messages
        for i in range(30):
            rich_logger.write(f"message {i}")
        # Should be capped at 20
        assert len(rich_logger.messages) <= 20


class TestCreateLogger:
    """Tests for create_logger function."""

    def test_create_logger_no_live_layout(self):
        """Test creating logger without live layout."""
        create_logger(live_layout=False)
        assert logger.handlers
        assert logger.level != 0

    def test_create_logger_with_live_layout(self):
        """Test creating logger with live layout."""
        create_logger(live_layout=True)
        assert logger.handlers

    def test_create_logger_with_level_string(self):
        """Test creating logger with level as string."""
        create_logger(live_layout=False, level="DEBUG")
        assert logger.level == logging.DEBUG

    def test_create_logger_with_level_constant(self):
        """Test creating logger with level as constant."""
        create_logger(live_layout=False, level=logging.WARNING)
        assert logger.level == logging.WARNING

    def test_create_logger_clears_handlers(self):
        """Test that create_logger clears existing handlers."""
        # Add multiple handlers
        for _ in range(3):
            logger.addHandler(logging.StreamHandler())

        create_logger(live_layout=False)

        # Should only have the new RichHandler
        assert len(logger.handlers) == 1

    def test_create_logger_propagate_false(self):
        """Test that propagate is set to False."""
        create_logger(live_layout=False)
        assert logger.propagate is False


class TestLoggerBasicUsage:
    """Tests for basic logger usage."""

    def test_logger_exists(self):
        """Test that logger is created."""
        assert logger is not None
        assert logger.name == "btrfs-backup-ng"

    def test_logger_can_log(self):
        """Test that logger can log messages."""
        # Ensure we have a handler
        create_logger(live_layout=False, level="DEBUG")

        # These should not raise
        logger.debug("debug message")
        logger.info("info message")
        logger.warning("warning message")
        logger.error("error message")

    def test_logger_respects_level(self):
        """Test that logger respects log level."""
        create_logger(live_layout=False, level="ERROR")

        # At ERROR level, INFO messages should be filtered
        # This is hard to test without capturing output, but we verify the level
        assert logger.level == logging.ERROR
