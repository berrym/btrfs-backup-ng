"""Tests for wizard utilities (Rich-based prompts and displays)."""

from pathlib import Path
from unittest import mock

import pytest

from btrfs_backup_ng.cli.wizard_utils import (
    console,
    display_btrbk_detected,
    display_btrbk_import,
    display_config_preview,
    display_next_steps,
    display_section_header,
    display_snapper_configs,
    display_wizard_header,
    find_btrbk_config,
    prompt,
    prompt_bool,
    prompt_choice,
    prompt_int,
    prompt_selection,
)


class TestPrompt:
    """Tests for Rich-based prompt function."""

    def test_returns_user_input(self):
        with mock.patch("rich.prompt.Prompt.ask", return_value="test_value"):
            result = prompt("Enter value")
        assert result == "test_value"

    def test_returns_default_on_empty_input(self):
        with mock.patch("rich.prompt.Prompt.ask", return_value="default"):
            result = prompt("Enter value", default="default")
        assert result == "default"

    def test_raises_keyboard_interrupt_on_eof(self):
        with mock.patch("rich.prompt.Prompt.ask", side_effect=EOFError):
            with pytest.raises(KeyboardInterrupt):
                prompt("Enter value")

    def test_raises_keyboard_interrupt_on_ctrl_c(self):
        with mock.patch("rich.prompt.Prompt.ask", side_effect=KeyboardInterrupt):
            with pytest.raises(KeyboardInterrupt):
                prompt("Enter value")


class TestPromptBool:
    """Tests for Rich-based prompt_bool function."""

    def test_returns_true(self):
        with mock.patch("rich.prompt.Confirm.ask", return_value=True):
            result = prompt_bool("Confirm?")
        assert result is True

    def test_returns_false(self):
        with mock.patch("rich.prompt.Confirm.ask", return_value=False):
            result = prompt_bool("Confirm?")
        assert result is False

    def test_uses_default(self):
        with mock.patch("rich.prompt.Confirm.ask", return_value=True) as mock_ask:
            prompt_bool("Confirm?", default=True)
            mock_ask.assert_called_once()
            # Check default was passed
            call_kwargs = mock_ask.call_args[1]
            assert call_kwargs.get("default") is True

    def test_raises_keyboard_interrupt_on_eof(self):
        with mock.patch("rich.prompt.Confirm.ask", side_effect=EOFError):
            with pytest.raises(KeyboardInterrupt):
                prompt_bool("Confirm?")


class TestPromptInt:
    """Tests for Rich-based prompt_int function."""

    def test_returns_valid_int(self):
        with mock.patch("rich.prompt.IntPrompt.ask", return_value=5):
            result = prompt_int("Enter number", default=10)
        assert result == 5

    def test_rejects_out_of_range_then_accepts_valid(self):
        # First call returns out of range, second returns valid
        with mock.patch("rich.prompt.IntPrompt.ask", side_effect=[999, 5]):
            result = prompt_int("Enter number", default=10, min_val=0, max_val=100)
        assert result == 5

    def test_raises_keyboard_interrupt_on_eof(self):
        with mock.patch("rich.prompt.IntPrompt.ask", side_effect=EOFError):
            with pytest.raises(KeyboardInterrupt):
                prompt_int("Enter number", default=10)


class TestPromptChoice:
    """Tests for Rich-based prompt_choice function."""

    def test_returns_choice_by_number(self):
        with mock.patch("rich.prompt.Prompt.ask", return_value="2"):
            result = prompt_choice("Choose", ["a", "b", "c"])
        assert result == "b"

    def test_returns_choice_by_value(self):
        with mock.patch("rich.prompt.Prompt.ask", return_value="b"):
            result = prompt_choice("Choose", ["a", "b", "c"])
        assert result == "b"

    def test_returns_default_on_empty(self):
        with mock.patch("rich.prompt.Prompt.ask", return_value="b"):
            result = prompt_choice("Choose", ["a", "b", "c"], default="b")
        assert result == "b"

    def test_rejects_invalid_then_accepts_valid(self):
        with mock.patch("rich.prompt.Prompt.ask", side_effect=["99", "invalid", "2"]):
            result = prompt_choice("Choose", ["a", "b", "c"])
        assert result == "b"

    def test_raises_keyboard_interrupt_on_eof(self):
        with mock.patch("rich.prompt.Prompt.ask", side_effect=EOFError):
            with pytest.raises(KeyboardInterrupt):
                prompt_choice("Choose", ["a", "b", "c"])


class TestPromptSelection:
    """Tests for Rich-based prompt_selection function."""

    def test_returns_selected_indices(self):
        items = [
            {"path": "/home", "type": "user data"},
            {"path": "/var", "type": "variable"},
        ]
        columns = [("path", "Path"), ("type", "Type")]

        with mock.patch("rich.prompt.Prompt.ask", return_value="1,2"):
            result = prompt_selection("Test", items, columns)
        assert result == [0, 1]

    def test_returns_all_on_all_input(self):
        items = [
            {"path": "/home", "type": "user data"},
            {"path": "/var", "type": "variable"},
        ]
        columns = [("path", "Path"), ("type", "Type")]

        with mock.patch("rich.prompt.Prompt.ask", return_value="all"):
            result = prompt_selection("Test", items, columns)
        assert result == [0, 1]

    def test_returns_default_on_empty(self):
        items = [
            {"path": "/home", "type": "user data"},
            {"path": "/var", "type": "variable"},
        ]
        columns = [("path", "Path"), ("type", "Type")]

        with mock.patch("rich.prompt.Prompt.ask", return_value=""):
            result = prompt_selection("Test", items, columns, default_indices=[0])
        assert result == [0]

    def test_skips_invalid_selections(self):
        items = [
            {"path": "/home", "type": "user data"},
            {"path": "/var", "type": "variable"},
        ]
        columns = [("path", "Path"), ("type", "Type")]

        with mock.patch("rich.prompt.Prompt.ask", return_value="1,99,2"):
            result = prompt_selection("Test", items, columns)
        # Should skip 99 as invalid
        assert result == [0, 1]

    def test_retries_on_empty_selection(self):
        items = [
            {"path": "/home", "type": "user data"},
        ]
        columns = [("path", "Path"), ("type", "Type")]

        with mock.patch("rich.prompt.Prompt.ask", side_effect=["", "", "1"]):
            result = prompt_selection("Test", items, columns)
        assert result == [0]

    def test_raises_keyboard_interrupt_on_eof(self):
        items = [{"path": "/home", "type": "user data"}]
        columns = [("path", "Path"), ("type", "Type")]

        with mock.patch("rich.prompt.Prompt.ask", side_effect=EOFError):
            with pytest.raises(KeyboardInterrupt):
                prompt_selection("Test", items, columns)


class TestDisplayFunctions:
    """Tests for display functions (no user input)."""

    def test_display_wizard_header(self, capsys):
        """Test wizard header displays correctly."""
        display_wizard_header("Test Title", "Test subtitle")
        # Rich outputs to its own console, so we just verify no errors

    def test_display_section_header(self, capsys):
        """Test section header displays correctly."""
        display_section_header("Section Name")
        # Just verify no errors

    def test_display_next_steps(self, capsys):
        """Test next steps panel displays correctly."""
        display_next_steps(["Step 1", "Step 2", "Step 3"])
        # Just verify no errors

    def test_display_config_preview(self, capsys):
        """Test config preview displays correctly."""
        config = "[global]\nsnapshot_dir = '.snapshots'\n"
        display_config_preview(config)
        # Just verify no errors

    def test_display_snapper_configs_empty(self, capsys):
        """Test snapper display with empty list."""
        display_snapper_configs([])
        # Should not raise, just return early

    def test_display_snapper_configs(self, capsys):
        """Test snapper configs display correctly."""

        # Create mock snapper config objects
        class MockSnapperConfig:
            def __init__(self, name, subvolume):
                self.name = name
                self.subvolume = subvolume

        configs = [
            MockSnapperConfig("root", "/"),
            MockSnapperConfig("home", "/home"),
        ]
        display_snapper_configs(configs)
        # Just verify no errors

    def test_display_btrbk_import_with_warnings(self, capsys):
        """Test btrbk import display with warnings."""
        volumes = [
            {"path": "/home", "targets": [{"path": "/mnt/backup/home"}]},
        ]
        warnings = ["Warning 1", "Warning 2"]
        display_btrbk_import(volumes, warnings)
        # Just verify no errors

    def test_display_btrbk_import_no_targets(self, capsys):
        """Test btrbk import display with volumes without targets."""
        volumes = [
            {"path": "/home", "targets": []},
        ]
        display_btrbk_import(volumes, [])
        # Just verify no errors

    def test_display_btrbk_import_multiple_targets(self, capsys):
        """Test btrbk import display with multiple targets."""
        volumes = [
            {
                "path": "/home",
                "targets": [
                    {"path": "/mnt/backup/home"},
                    {"path": "ssh://server:/backup/home"},
                ],
            },
        ]
        display_btrbk_import(volumes, [])
        # Just verify no errors


class TestFindBtrbkConfig:
    """Tests for find_btrbk_config function."""

    def test_finds_etc_btrbk_conf(self):
        """Test finding /etc/btrbk/btrbk.conf."""

        # Mock exists to return True for first standard location
        def mock_exists(path):
            return str(path) == "/etc/btrbk/btrbk.conf"

        with mock.patch.object(Path, "exists", mock_exists):
            result = find_btrbk_config()

        assert result is not None
        assert str(result) == "/etc/btrbk/btrbk.conf"

    def test_finds_etc_btrbk_conf_fallback(self):
        """Test finding /etc/btrbk.conf as fallback."""

        # Mock exists to return True only for the fallback location
        def mock_exists(path):
            return str(path) == "/etc/btrbk.conf"

        with mock.patch.object(Path, "exists", mock_exists):
            result = find_btrbk_config()

        assert result is not None
        assert str(result) == "/etc/btrbk.conf"

    def test_returns_none_when_not_found(self):
        """Test returns None when no btrbk config exists."""
        with mock.patch.object(Path, "exists", return_value=False):
            result = find_btrbk_config()
        assert result is None


class TestDisplayBtrbkDetected:
    """Tests for display_btrbk_detected function."""

    def test_returns_import_choice(self):
        """Test returns 'import' when user chooses import."""
        with mock.patch(
            "btrfs_backup_ng.cli.wizard_utils.prompt_choice",
            return_value="import",
        ):
            result = display_btrbk_detected(Path("/etc/btrbk/btrbk.conf"))
        assert result == "import"

    def test_returns_detect_choice(self):
        """Test returns 'detect' when user chooses detect."""
        with mock.patch(
            "btrfs_backup_ng.cli.wizard_utils.prompt_choice",
            return_value="detect",
        ):
            result = display_btrbk_detected(Path("/etc/btrbk/btrbk.conf"))
        assert result == "detect"

    def test_returns_manual_choice(self):
        """Test returns 'manual' when user chooses manual."""
        with mock.patch(
            "btrfs_backup_ng.cli.wizard_utils.prompt_choice",
            return_value="manual",
        ):
            result = display_btrbk_detected(Path("/etc/btrbk/btrbk.conf"))
        assert result == "manual"
