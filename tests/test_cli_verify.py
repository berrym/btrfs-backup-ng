"""Tests for CLI verify command."""

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

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
    status=None,
):
    """Helper to create VerifyResult with required fields. Sets the unified
    details['status'] (ok when passed, failed otherwise) unless overridden."""
    details = dict(details or {})
    details.setdefault("status", status or ("ok" if passed else "failed"))
    return VerifyResult(
        snapshot_name=name,
        level=level,
        passed=passed,
        message=message,
        duration_seconds=duration,
        details=details,
    )


def _make_report(
    level=VerifyLevel.METADATA,
    location="/backup",
    results=None,
    errors=None,
    available=None,
):
    """Helper to create VerifyReport with results."""
    report = VerifyReport(level=level, location=location)
    if results:
        report.results.extend(results)
    if errors:
        report.errors.extend(errors)
    report.available = available if available is not None else len(report.results)
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
        assert "2 verified" in captured.out
        assert "0 failed" in captured.out
        assert "checked 2 of 2 snapshots" in captured.out
        assert "All 2 checked snapshot(s) verified" in captured.out

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
        assert "1 verified" in captured.out
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


class TestGeneralVerifyAgainstRawTarget:
    """R8a: the GENERAL ``verify`` command (not ``raw verify``) must consult the sealed
    sha256 on raw:// targets. Before R8a a bare ``verify raw://X`` ran the metadata level
    and reported 'All verifications passed' for a CORRUPT stream -- a false all-clear.
    These drive the real ``execute()`` with a real RawEndpoint (choose_endpoint builds
    it), mocking nothing about the checksum path -- closing the exact coverage gap the
    R8 audit flagged (no test drove general verify against any raw target)."""

    @staticmethod
    def _build_raw(path, name):
        from btrfs_backup_ng.endpoint.raw import RawEndpoint

        ep = RawEndpoint(config={"path": str(path)})
        src = path / (name + ".src")
        src.write_bytes(b"cli-verify-payload-" * 300)
        with open(src, "rb") as f:
            ep.receive(f, snapshot_name=name).communicate()
        ep.commit_receive()
        src.unlink()

    @staticmethod
    def _args(location, **kw):
        base = dict(
            level=None,
            location=location,
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=True,
            json=False,
            temp_dir=None,
            no_cleanup=False,
            ssh_sudo=False,
            ssh_key=None,
            ssh_auth_sock=None,
            timestamp_format=None,
        )
        base.update(kw)
        return argparse.Namespace(**base)

    def test_default_level_detects_raw_corruption(self, tmp_path, capsys):
        """THE gap test: seal a checksum, corrupt the stream, run the GENERAL verify at
        its DEFAULT level against raw://<dir> -> exit 1 + CORRUPT. Mutation guard: revert
        the raw-branch wiring (falls back to metadata) and this reports success (exit 0)."""
        self._build_raw(tmp_path, "good")
        self._build_raw(tmp_path, "bad")
        bad = tmp_path / "bad.btrfs"
        bad.write_bytes(bad.read_bytes() + b"CORRUPTION")

        rc = execute(self._args(f"raw://{tmp_path}"))

        out = capsys.readouterr().out
        assert rc == 1
        assert "CORRUPT" in out

    def test_default_level_resolves_to_stream_and_passes_on_intact_raw(
        self, tmp_path, capsys
    ):
        """An intact raw backup passes at the default level (exit 0), and the default
        actually resolved to the sealed-checksum check (report level == stream, status
        ok) -- not the byte-blind metadata listing."""
        import json

        self._build_raw(tmp_path, "ok1")

        rc = execute(self._args(f"raw://{tmp_path}", json=True))

        data = json.loads(capsys.readouterr().out)
        assert rc == 0
        assert data["level"] == "stream"  # default -> the checksum check, not metadata
        assert data["results"][0]["details"]["checksum_status"] == "ok"

    def test_legacy_no_checksum_is_unverifiable_not_false_pass(self, tmp_path, capsys):
        """A legacy stream with no recorded checksum is reported 'unverifiable' (exit 0
        but explicitly not a clean 'verified') -- never a silent green on an unchecked
        stream. Mutation guard: reporting it as ok/passed with no status fails."""
        import json

        self._build_raw(tmp_path, "legacy")
        meta = tmp_path / "legacy.btrfs.meta"
        doc = json.loads(meta.read_text())
        doc["checksum"]["value"] = None
        meta.write_text(json.dumps(doc))

        rc = execute(self._args(f"raw://{tmp_path}", json=True))

        data = json.loads(capsys.readouterr().out)
        assert rc == 0
        assert data["results"][0]["details"]["checksum_status"] == "unverifiable"

    def test_explicit_metadata_level_bypasses_raw_checksum_branch(self, tmp_path):
        """An explicit --level metadata on a raw target still routes to verify_metadata
        (an operator override); the raw-checksum branch owns only stream/full. Proves the
        default->stream RESOLUTION is what closes the false-pass, not a blanket override."""
        self._build_raw(tmp_path, "m1")
        with (
            patch("btrfs_backup_ng.cli.verify.verify_metadata") as mv,
            patch("btrfs_backup_ng.cli.verify.verify_raw_checksums") as mrc,
        ):
            mv.return_value = _make_report(results=[_make_result("m1", passed=True)])
            execute(self._args(f"raw://{tmp_path}", level="metadata"))
            mv.assert_called_once()
            mrc.assert_not_called()

    def test_json_output_is_clean_stdout_without_quiet(self, tmp_path, capsys):
        """`verify raw://X --json` (WITHOUT --quiet) must emit ONLY parseable JSON on
        stdout -- the human header/progress lines are suppressed in json mode. Mutation
        guard: reverting the `human` gate re-pollutes stdout so json.loads raises."""
        import json

        self._build_raw(tmp_path, "j1")

        execute(self._args(f"raw://{tmp_path}", json=True, quiet=False))

        out = capsys.readouterr().out
        data = json.loads(out)  # would raise if the header lines leaked onto stdout
        assert data["level"] == "stream"
        assert data["results"][0]["details"]["checksum_status"] == "ok"

    def test_explicit_stream_level_on_raw_routes_to_checksums(self, tmp_path, capsys):
        """An EXPLICIT --level stream on a raw target routes to the sealed-checksum check
        (not btrfs send on a stream file): a corrupt stream -> exit 1 + CORRUPT. Mutation
        guard: narrowing the raw routing to exclude STREAM would call verify_stream (btrfs
        send), producing a non-CORRUPT failure and tripping the assertion."""
        self._build_raw(tmp_path, "s")
        stream = tmp_path / "s.btrfs"
        stream.write_bytes(stream.read_bytes() + b"CORRUPTION")
        rc = execute(self._args(f"raw://{tmp_path}", level="stream"))
        out = capsys.readouterr().out
        assert rc == 1
        assert "CORRUPT" in out

    def test_explicit_full_level_on_raw_routes_to_checksums_not_restore(
        self, tmp_path, capsys
    ):
        """An EXPLICIT --level full on a raw target ALSO routes to the sealed-checksum
        check, NOT verify_full's btrfs restore (which cannot restore a raw stream into a
        temp subvolume). Mutation guard: if the raw routing check were `level == STREAM`,
        full would fall through to verify_full and fail with a restore/temp-dir error,
        not a CORRUPT verdict."""
        self._build_raw(tmp_path, "f")
        stream = tmp_path / "f.btrfs"
        stream.write_bytes(stream.read_bytes() + b"CORRUPTION")
        rc = execute(self._args(f"raw://{tmp_path}", level="full"))
        out = capsys.readouterr().out
        assert rc == 1
        assert "CORRUPT" in out

    def test_raw_ssh_corrupt_verdict_surfaces_via_general_verify(
        self, tmp_path, capsys
    ):
        """End-to-end through the general verify stack for raw+ssh: a mismatching remote
        digest -> exit 1, 'corrupt' status, and the consistency-only trust caveat (the
        digest came from an untrusted remote). Guards the general-verify -> SSHRawEndpoint
        -> verify_raw_checksums chain, not just the primitive in isolation."""
        import json

        from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint
        from btrfs_backup_ng.endpoint.raw_metadata import RawSnapshot

        ep = SSHRawEndpoint(config={"path": "/backups", "hostname": "nas"})
        snap = RawSnapshot(
            name="r", stream_path=Path("/backups/r.btrfs"), checksum_value="a" * 64
        )
        ep.list_snapshots = lambda flush_cache=False: [snap]
        ep.compute_stream_checksum = lambda s: "b" * 64  # remote reports a mismatch
        ep.prepare = lambda: None

        with patch(
            "btrfs_backup_ng.cli.verify.endpoint.choose_endpoint", return_value=ep
        ):
            rc = execute(self._args("raw+ssh://nas/backups", json=True))

        data = json.loads(capsys.readouterr().out)
        assert rc == 1
        res = data["results"][0]
        assert res["details"]["checksum_status"] == "corrupt"
        assert res["details"]["trust"] == "consistency-only (remote hash)"

    def test_raw_ssh_location_recognized_and_threads_ssh_creds(self, tmp_path):
        """A raw+ssh:// location is recognized (NOT mangled into a local path) and threads
        ssh_sudo/ssh_key/ssh_auth_sock into the endpoint kwargs. Mutation guard: dropping
        the raw+ssh routing branch drops the ssh creds (a sudo/agent-required remote verify
        would silently fail) and stuffs a bogus 'path' from Path(url).resolve()."""
        captured: dict = {}

        def fake_choose(location, kwargs, source=False):
            captured.update(kwargs)
            raise RuntimeError("stop after capturing kwargs")

        with patch(
            "btrfs_backup_ng.cli.verify.endpoint.choose_endpoint",
            side_effect=fake_choose,
        ):
            rc = execute(
                self._args(
                    "raw+ssh://backup@nas:/backups",
                    ssh_sudo=True,
                    ssh_key="/k/id",
                    ssh_auth_sock="/run/agent.sock",
                )
            )

        assert rc == 2  # choose_endpoint raised -> caught -> return 2
        assert captured.get("ssh_sudo") is True
        assert captured.get("ssh_key") == "/k/id"
        assert captured.get("ssh_auth_sock") == "/run/agent.sock"
        assert "path" not in captured  # a raw scheme must not get a resolved local path


class TestR8eHonestOutput:
    """R8e: unverifiable is visually distinct (not green PASS), the summary is honest about
    'checked N of M' with a --all hint, and the JSON carries a top-level tri-state verdict."""

    def test_unverifiable_is_distinct_from_pass(self, capsys):
        """A report with ok + unverifiable + failed renders three DISTINCT statuses, and
        the conclusion is NOT 'All verified' (unverifiable is not a clean pass). Mutation
        guard: rendering unverifiable as green PASS / counting it as verified breaks this."""
        report = _make_report(
            level=VerifyLevel.FULL,
            results=[
                _make_result("ok-snap", passed=True, status="ok", message="verified"),
                _make_result(
                    "unv-snap", passed=True, status="unverifiable", message="no room"
                ),
                _make_result(
                    "bad-snap", passed=False, status="failed", message="corrupt"
                ),
            ],
        )
        args = argparse.Namespace(json=False)
        _display_report(report, args)

        out = capsys.readouterr().out
        assert "UNVERIFIABLE" in out
        assert "1 verified" in out and "1 failed" in out and "1 unverifiable" in out
        assert "found issues" in out  # a failure present -> verdict fail
        assert "All" not in out.split("Summary")[-1] or "verified" in out

    def test_summary_states_checked_of_available_with_all_hint(self, capsys):
        """When only a subset was checked (available > checked), the summary says so and
        points at --all. Mutation guard: dropping report.available or the hint hides the
        sampling."""
        report = _make_report(
            level=VerifyLevel.STREAM,
            results=[_make_result("latest", passed=True, status="ok")],
            available=7,
        )
        args = argparse.Namespace(json=False)
        _display_report(report, args)

        out = capsys.readouterr().out
        assert "checked 1 of 7 snapshots" in out
        assert "--all" in out

    def test_json_verdict_pass(self, capsys):
        import json

        report = _make_report(
            results=[_make_result("a", passed=True, status="ok")], available=1
        )
        _display_json(report)
        data = json.loads(capsys.readouterr().out)
        assert data["verdict"] == "pass"
        assert data["summary"]["verified"] == 1
        assert data["summary"]["available"] == 1
        assert data["results"][0]["status"] == "ok"

    def test_json_verdict_unverifiable(self, capsys):
        import json

        report = _make_report(
            results=[_make_result("a", passed=True, status="unverifiable")]
        )
        _display_json(report)
        data = json.loads(capsys.readouterr().out)
        assert data["verdict"] == "unverifiable"
        assert data["summary"]["unverifiable"] == 1

    def test_json_verdict_fail_on_empty_errored_run(self, capsys):
        """The audit's exact false-positive: an empty/errored run must NOT read as success.
        verdict=='fail' even though summary.failed==0. Mutation guard: a naive
        success=failed==0 would say pass here."""
        import json

        report = _make_report(results=[], errors=["No snapshots found"])
        _display_json(report)
        data = json.loads(capsys.readouterr().out)
        assert data["verdict"] == "fail"
        assert data["summary"]["failed"] == 0  # the trap the verdict avoids
        assert data["errors"] == ["No snapshots found"]


class TestR8eReviewFixes:
    """Guards for the R8e adversarial-review fixes."""

    def test_all_hint_suppressed_when_snapshot_named(self, capsys):
        """--snapshot is mutually exclusive with --all, so the 'pass --all' hint must NOT
        appear when the user named one snapshot (it would be unfollowable). Mutation guard:
        dropping the args.snapshot check prints the misleading hint."""
        report = _make_report(
            level=VerifyLevel.STREAM,
            results=[_make_result("snap-3", passed=True, status="ok")],
            available=9,
        )
        args = argparse.Namespace(json=False, snapshot="snap-3")
        _display_report(report, args)
        out = capsys.readouterr().out
        assert "--all" not in out

    def test_all_hint_shown_without_snapshot(self, capsys):
        """Without --snapshot, a subset check DOES show the --all hint (control for above)."""
        report = _make_report(
            level=VerifyLevel.STREAM,
            results=[_make_result("latest", passed=True, status="ok")],
            available=9,
        )
        args = argparse.Namespace(json=False, snapshot=None)
        _display_report(report, args)
        assert "--all" in capsys.readouterr().out

    def test_empty_errored_run_has_no_misleading_results_line(self, capsys):
        """An empty/errored run shows the error, NOT a 'checked 0 of 0' Results line that
        reads like 'no backups exist'."""
        report = _make_report(errors=["Verification failed: connection refused"])
        args = argparse.Namespace(json=False)
        _display_report(report, args)
        out = capsys.readouterr().out
        assert "connection refused" in out
        assert "checked 0 of 0" not in out

    def test_json_status_fallback_never_falsely_ok(self, capsys):
        """A passed result with NO details['status'] must NOT be reported 'ok' in JSON
        (that would falsely claim it was verified) -- it degrades to 'unverifiable'.
        Mutation guard: the old 'ok' if passed fallback would report 'ok' here."""
        import json

        # A result deliberately missing details['status'] (simulates a future path drift).
        r = VerifyResult("x", VerifyLevel.METADATA, passed=True, details={})
        report = _make_report(results=[r])
        report.results[0].details.pop("status", None)  # ensure absent
        _display_json(report)
        data = json.loads(capsys.readouterr().out)
        assert data["results"][0]["status"] != "ok"
        assert data["results"][0]["status"] == "unverifiable"


class TestVerifyNoDecryptThreading:
    """verify does NOT thread a decryption keyring/cipher into the endpoint.

    Raw verification recomputes the sha256 of the stored (encrypted) stream file
    and compares it to the sealed sidecar checksum -- it never decodes the stream
    -- so no keyring is needed. This guards against re-adding inert decrypt flags
    that would silently do nothing (a regression toward CLI theater).
    """

    @staticmethod
    def _args(**over):
        base = dict(
            level="metadata",
            location="/backup",
            prefix="",
            fs_checks="auto",
            snapshot=None,
            quiet=False,
            json=False,
            temp_dir=None,
            no_cleanup=False,
            timestamp_format=None,
            ssh_sudo=False,
            ssh_key=None,
            ssh_auth_sock=None,
            ssh_host_key_policy=None,
        )
        base.update(over)
        return argparse.Namespace(**base)

    @patch("btrfs_backup_ng.cli.verify.verify_metadata")
    @patch("btrfs_backup_ng.cli.verify.endpoint.choose_endpoint")
    def test_verify_endpoint_gets_no_decrypt_keys(self, mock_choose, mock_verify):
        mock_choose.return_value = MagicMock()
        mock_verify.return_value = _make_report(
            results=[_make_result("snap-1", passed=True)]
        )
        execute(self._args())
        kwargs = mock_choose.call_args.args[1]
        assert "gpg_keyring" not in kwargs
        assert "openssl_cipher" not in kwargs

    def test_verify_parser_has_no_decrypt_flags(self):
        """The verify subcommand must not expose --gpg-keyring/--openssl-cipher."""
        from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

        parser = create_subcommand_parser()
        # An unknown flag makes argparse exit(2); confirm both are rejected.
        for flag in ("--gpg-keyring", "--openssl-cipher"):
            with pytest.raises(SystemExit):
                parser.parse_args(["verify", "raw://backups", flag, "x"])
