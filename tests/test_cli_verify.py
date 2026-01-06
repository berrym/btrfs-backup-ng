"""Tests for CLI verify command."""

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

from btrfs_backup_ng.cli.verify import (
    _display_json,
    _display_report,
    execute,
)
from btrfs_backup_ng.core.verify import VerifyLevel, VerifyReport, VerifyResult


def _make_result(
    name,
    passed=True,
    message="",
    details=None,
    duration=0.0,
    level=VerifyLevel.METADATA,
):
    """Helper to create VerifyResult with required fields."""
    return VerifyResult(
        snapshot_name=name,
        level=level,
        passed=passed,
        message=message,
        duration_seconds=duration,
        details=details or {},
    )


def _make_report(
    level=VerifyLevel.METADATA, location="/backup", results=None, errors=None
):
    """Helper to create VerifyReport with results."""
    report = VerifyReport(level=level, location=location)
    if results:
        report.results.extend(results)
    if errors:
        report.errors.extend(errors)
    return report


class TestExecute:
    """Tests for execute function."""

    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_endpoint_creation_failure(self, mock_choose):
        """Test handling of endpoint creation failure."""
        mock_choose.side_effect = Exception("Connection failed")

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        result = execute(args)

        assert result == 2

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_metadata_verification_success(self, mock_choose, mock_verify):
        """Test successful metadata verification."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report(results=[_make_result("snap-1", passed=True)])
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        result = execute(args)

        assert result == 0
        mock_verify.assert_called_once()

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_metadata_verification_with_failures(self, mock_choose, mock_verify):
        """Test metadata verification with failures returns 1."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report(
            results=[
                _make_result("snap-1", passed=True),
                _make_result("snap-2", passed=False, message="Corrupt"),
            ]
        )
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        result = execute(args)

        assert result == 1

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_verification_with_errors_returns_2(self, mock_choose, mock_verify):
        """Test verification with errors returns 2."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report(errors=["Something went wrong"])
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        result = execute(args)

        assert result == 2

    @patch("btrfs_backup_ng.cli.verify.verify_stream")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_stream_verification(self, mock_choose, mock_verify):
        """Test stream level verification."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report(
            level=VerifyLevel.STREAM,
            results=[_make_result("snap-1", level=VerifyLevel.STREAM)],
        )
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="stream",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        result = execute(args)

        assert result == 0
        mock_verify.assert_called_once()

    @patch("btrfs_backup_ng.cli.verify.verify_full")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_full_verification(self, mock_choose, mock_verify):
        """Test full level verification."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report(
            level=VerifyLevel.FULL,
            results=[_make_result("snap-1", level=VerifyLevel.FULL)],
        )
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="full",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir="/tmp/verify",
            no_cleanup=False,
        )

        result = execute(args)

        assert result == 0
        mock_verify.assert_called_once()
        # Check temp_dir was passed
        call_kwargs = mock_verify.call_args[1]
        assert call_kwargs["temp_dir"] == Path("/tmp/verify")
        assert call_kwargs["cleanup"] is True

    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_full_verification_remote_requires_temp_dir(self, mock_choose):
        """Test full verification of remote backup requires temp_dir."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        args = argparse.Namespace(
            level="full",
            location="ssh://backup-server/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
            ssh_sudo=False,
            ssh_key=None,
        )

        result = execute(args)

        assert result == 2

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_verification_exception(self, mock_choose, mock_verify):
        """Test handling of verification exception."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep
        mock_verify.side_effect = Exception("Verification error")

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        result = execute(args)

        assert result == 2

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_verification_keyboard_interrupt(self, mock_choose, mock_verify):
        """Test handling of keyboard interrupt during verification."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep
        mock_verify.side_effect = KeyboardInterrupt()

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        result = execute(args)

        assert result == 2

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_quiet_mode_no_progress(self, mock_choose, mock_verify):
        """Test quiet mode disables progress callback."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report()
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=True,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        execute(args)

        # In quiet mode, on_progress should be None
        call_kwargs = mock_verify.call_args[1]
        assert call_kwargs["on_progress"] is None

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_specific_snapshot(self, mock_choose, mock_verify):
        """Test verifying a specific snapshot."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report()
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot="snap-2024-01-01",
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        execute(args)

        call_kwargs = mock_verify.call_args[1]
        assert call_kwargs["snapshot_name"] == "snap-2024-01-01"

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_ssh_endpoint_options(self, mock_choose, mock_verify):
        """Test SSH endpoint options are passed correctly."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report()
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="metadata",
            location="ssh://user@host/backup",
            prefix="home-",
            fs_checks="skip",  # New mode system
            snapshot=None,
            quiet=True,
            json=False,
            temp_dir=None,
            no_cleanup=False,
            ssh_sudo=True,
            ssh_key="/path/to/key",
        )

        execute(args)

        # Check endpoint was created with SSH options
        call_args = mock_choose.call_args
        endpoint_kwargs = call_args[0][1]
        assert endpoint_kwargs["ssh_sudo"] is True
        assert endpoint_kwargs["ssh_identity_file"] == "/path/to/key"
        assert endpoint_kwargs["fs_checks"] == "skip"
        assert endpoint_kwargs["snap_prefix"] == "home-"

    @patch("btrfs_backup_ng.cli.verify.verify_full")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_no_cleanup_option(self, mock_choose, mock_verify):
        """Test --no-cleanup option is passed correctly."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report(level=VerifyLevel.FULL)
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="full",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir="/tmp/verify",
            no_cleanup=True,
        )

        execute(args)

        call_kwargs = mock_verify.call_args[1]
        assert call_kwargs["cleanup"] is False


class TestDisplayReport:
    """Tests for _display_report function."""

    def test_display_passing_results(self, capsys):
        """Test displaying passing verification results."""
        report = _make_report(
            results=[
                _make_result("snap-1", passed=True),
                _make_result("snap-2", passed=True),
            ]
        )
        report.completed_at = report.started_at + 1.5

        args = argparse.Namespace(json=False)
        _display_report(report, args)

        captured = capsys.readouterr()
        assert "snap-1" in captured.out
        assert "snap-2" in captured.out
        assert "PASS" in captured.out
        assert "2 passed" in captured.out
        assert "0 failed" in captured.out
        assert "All verifications passed" in captured.out

    def test_display_failing_results(self, capsys):
        """Test displaying failing verification results."""
        report = _make_report(
            level=VerifyLevel.STREAM,
            results=[
                _make_result("snap-1", passed=True, level=VerifyLevel.STREAM),
                _make_result(
                    "snap-2",
                    passed=False,
                    message="Stream corrupted",
                    level=VerifyLevel.STREAM,
                ),
            ],
        )
        report.completed_at = report.started_at + 2.0

        args = argparse.Namespace(json=False)
        _display_report(report, args)

        captured = capsys.readouterr()
        assert "PASS" in captured.out
        assert "FAIL" in captured.out
        assert "Stream corrupted" in captured.out
        assert "1 passed" in captured.out
        assert "1 failed" in captured.out
        assert "found issues" in captured.out

    def test_display_with_errors(self, capsys):
        """Test displaying report with errors."""
        report = _make_report(
            level=VerifyLevel.FULL, errors=["Connection lost", "Timeout expired"]
        )
        report.completed_at = report.started_at + 0.5

        args = argparse.Namespace(json=False)
        _display_report(report, args)

        captured = capsys.readouterr()
        assert "Errors" in captured.out
        assert "Connection lost" in captured.out
        assert "Timeout expired" in captured.out

    def test_display_with_details(self, capsys):
        """Test displaying results with details."""
        report = _make_report(
            results=[
                _make_result("snap-1", passed=True, details={"is_base": True}),
                _make_result("snap-2", passed=True, details={"parent": "snap-1"}),
            ]
        )
        report.completed_at = report.started_at + 1.0

        args = argparse.Namespace(json=False)
        _display_report(report, args)

        captured = capsys.readouterr()
        assert "Base snapshot" in captured.out
        assert "Parent: snap-1" in captured.out

    def test_display_json_format(self, capsys):
        """Test JSON output format."""
        report = _make_report(
            results=[
                _make_result("snap-1", passed=True, message="OK", duration=1.5),
            ]
        )
        report.completed_at = report.started_at + 1.5

        args = argparse.Namespace(json=True)
        _display_report(report, args)

        captured = capsys.readouterr()
        assert '"level": "metadata"' in captured.out
        assert '"location": "/backup"' in captured.out
        assert '"passed": 1' in captured.out
        assert '"snapshot": "snap-1"' in captured.out


class TestDisplayJson:
    """Tests for _display_json function."""

    def test_json_output_structure(self, capsys):
        """Test JSON output has correct structure."""
        report = _make_report(
            level=VerifyLevel.STREAM,
            location="/mnt/backup",
            results=[
                _make_result(
                    "test-snap",
                    passed=False,
                    message="Checksum mismatch",
                    duration=2.5,
                    details={"expected": "abc", "got": "xyz"},
                    level=VerifyLevel.STREAM,
                ),
            ],
            errors=["Warning: slow transfer"],
        )
        report.completed_at = report.started_at + 5.0

        _display_json(report)

        captured = capsys.readouterr()
        import json

        data = json.loads(captured.out)

        assert data["level"] == "stream"
        assert data["location"] == "/mnt/backup"
        assert data["summary"]["passed"] == 0
        assert data["summary"]["failed"] == 1
        assert data["summary"]["total"] == 1
        assert len(data["results"]) == 1
        assert data["results"][0]["snapshot"] == "test-snap"
        assert data["results"][0]["passed"] is False
        assert data["results"][0]["message"] == "Checksum mismatch"
        assert data["results"][0]["duration_seconds"] == 2.5
        assert data["results"][0]["details"]["expected"] == "abc"
        assert data["errors"] == ["Warning: slow transfer"]

    def test_json_duration_seconds(self, capsys):
        """Test JSON includes duration_seconds."""
        report = _make_report(level=VerifyLevel.FULL)
        report.completed_at = report.started_at + 10.5

        _display_json(report)

        captured = capsys.readouterr()
        import json

        data = json.loads(captured.out)
        assert "duration_seconds" in data


class TestEndpointPreparation:
    """Tests for endpoint preparation in execute."""

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_local_endpoint_path_resolution(self, mock_choose, mock_verify):
        """Test local path is resolved correctly."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report()
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="metadata",
            location="/home/user/backups",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=True,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        execute(args)

        # Check endpoint kwargs include path
        call_args = mock_choose.call_args
        endpoint_kwargs = call_args[0][1]
        assert "path" in endpoint_kwargs
        assert endpoint_kwargs["path"] == Path("/home/user/backups").resolve()

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_endpoint_prepare_called(self, mock_choose, mock_verify):
        """Test endpoint.prepare() is called."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report()
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=True,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        execute(args)

        mock_ep.prepare.assert_called_once()

    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_endpoint_prepare_failure(self, mock_choose):
        """Test handling of endpoint.prepare() failure."""
        mock_ep = MagicMock()
        mock_ep.prepare.side_effect = Exception("SSH connection failed")
        mock_choose.return_value = mock_ep

        args = argparse.Namespace(
            level="metadata",
            location="ssh://server/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
            ssh_sudo=False,
            ssh_key=None,
        )

        result = execute(args)

        assert result == 2


class TestProgressCallback:
    """Tests for progress callback handling."""

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_progress_callback_provided(self, mock_choose, mock_verify):
        """Test progress callback is provided when not quiet."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report()
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        execute(args)

        call_kwargs = mock_verify.call_args[1]
        assert call_kwargs["on_progress"] is not None
        # Test calling the progress callback doesn't raise
        call_kwargs["on_progress"](1, 5, "snap-1")

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_progress_callback_none_when_quiet(self, mock_choose, mock_verify):
        """Test progress callback is None when quiet."""
        mock_ep = MagicMock()
        mock_choose.return_value = mock_ep

        report = _make_report()
        mock_verify.return_value = report

        args = argparse.Namespace(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=True,
            json=False,
            temp_dir=None,
            no_cleanup=False,
        )

        execute(args)

        call_kwargs = mock_verify.call_args[1]
        assert call_kwargs["on_progress"] is None
