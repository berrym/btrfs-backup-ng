"""`doctor` reported zero warnings for a config its own loader complained about.

``execute_doctor`` loads the configuration itself and passes the resulting
object to ``Doctor``. ``_check_config_valid`` only loads when it was handed no
config, so on the CLI path the loader's warnings were collected, logged, and
then dropped: they never became findings, were never counted, and never
influenced the exit code. An empty configuration produced

    Summary: 4 passed, 0 warnings, 0 errors

and exit 0, for a file the loader had described as having no volumes at all.
This is the defect class the release is about -- a check whose adverse verdict
is discarded -- in the command an operator runs precisely when something looks
wrong.

A trap worth stating, because it nearly produced a test that proved nothing:
the warning text IS present on stdout even with the defect, because
``execute_doctor`` logs each warning through ``logger.warning`` before
discarding it. Asserting "the message appears in the output" passes against
broken code. These tests assert on the report -- findings, counts, exit code --
which is the thing that was actually wrong.
"""

from __future__ import annotations

import argparse
import contextlib
import io

import pytest

from btrfs_backup_ng.cli.doctor import execute_doctor
from btrfs_backup_ng.config import load_config
from btrfs_backup_ng.core.doctor import (
    DiagnosticCategory,
    DiagnosticSeverity,
    Doctor,
)

EMPTY_CONFIG_WARNING = "No volumes configured"


class StubConfig:
    """Stands in for a loaded Config with ONE healthy enabled volume.

    _check_config_valid asks for both the declared and the enabled volumes; a
    config with none enabled now (correctly) draws its own WARN, which would
    pollute the loader-warning mapping these tests pin -- so the stub models
    the healthy case.
    """

    def __init__(self):
        self.volumes = [object()]

    def get_enabled_volumes(self):
        return list(self.volumes)


def _doctor_args(config_path, **overrides):
    args = argparse.Namespace(
        config=str(config_path),
        check=["config"],
        volume=None,
        quiet=True,
        json=False,
        fix=False,
        interactive=False,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _run_doctor(config_path, **overrides):
    """Run the CLI entry point, returning its exit code and captured output."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exit_code = execute_doctor(_doctor_args(config_path, **overrides))
    return exit_code, buffer.getvalue()


@pytest.fixture
def empty_config(tmp_path):
    """A configuration that loads successfully and warns about having no volumes."""
    path = tmp_path / "empty.toml"
    path.write_text("")
    return path


class TestLoaderWarningsReachTheReport:
    def test_warning_changes_the_exit_code(self, empty_config):
        """Exit 1, not 0: a warned-about config is not a clean bill of health."""
        exit_code, _ = _run_doctor(empty_config)
        assert exit_code == 1, (
            "doctor exited 0 for a config the loader warned about; the warning "
            "was collected by execute_doctor and then dropped"
        )

    def test_warning_is_a_counted_finding_not_just_a_log_line(self, empty_config):
        """The report must carry it. stdout carried it even when broken."""
        doctor = Doctor(
            config=StubConfig(),
            config_path=empty_config,
            config_warnings=[EMPTY_CONFIG_WARNING],
        )
        findings = doctor._check_config_valid()
        warnings = [f for f in findings if f.severity == DiagnosticSeverity.WARN]
        assert len(warnings) == 1
        assert EMPTY_CONFIG_WARNING in warnings[0].message
        assert warnings[0].check_name == "config_valid"
        assert warnings[0].category == DiagnosticCategory.CONFIG

    def test_every_warning_is_reported_not_only_the_first(self, tmp_path):
        supplied = ["first problem", "second problem", "third problem"]
        config_path = tmp_path / "c.toml"
        config_path.write_text("")
        doctor = Doctor(
            config=StubConfig(), config_path=config_path, config_warnings=supplied
        )
        messages = [
            f.message
            for f in doctor._check_config_valid()
            if f.severity == DiagnosticSeverity.WARN
        ]
        assert len(messages) == len(supplied)
        for warning in supplied:
            assert any(warning in message for message in messages)

    def test_no_warnings_means_no_warning_findings(self, tmp_path):
        """The fix must not invent findings for a config that loaded cleanly."""
        config_path = tmp_path / "c.toml"
        config_path.write_text("")
        doctor = Doctor(config=StubConfig(), config_path=config_path)
        assert doctor.config_warnings == []
        warnings = [
            f
            for f in doctor._check_config_valid()
            if f.severity == DiagnosticSeverity.WARN
        ]
        assert warnings == []

    def test_self_loading_doctor_reports_each_warning_exactly_once(self, empty_config):
        """With no config supplied, Doctor loads it; the two paths must not double up."""
        doctor = Doctor(config=None, config_path=empty_config)
        messages = [
            f.message
            for f in doctor._check_config_valid()
            if f.severity == DiagnosticSeverity.WARN
        ]
        assert sum(EMPTY_CONFIG_WARNING in m for m in messages) == 1


class TestUnloadableConfigStillRuns:
    """Threading the warnings must not break the path where loading failed."""

    def test_config_error_does_not_leave_the_list_unbound(self, tmp_path):
        """execute_doctor references config_warnings after the try, on every path.

        A ConfigError skips the tuple assignment inside the try, so without the
        binding above it the Doctor call raises NameError and `doctor` dies on
        exactly the malformed config it exists to diagnose.
        """
        bad = tmp_path / "bad.toml"
        bad.write_text("this is not = = toml [[[\n")
        exit_code, _ = _run_doctor(bad)
        assert exit_code == 2, "a malformed config should be reported, not crash"


class TestAConfigThatBacksUpNothingIsNotBlessed:
    """`run` and `prune` operate only on ENABLED volumes, so a valid config
    with none enabled -- or none declared -- performs no backups at all,
    while doctor said "Configuration is valid" with a clean summary and exit
    0. Reproduced: one volume with enabled = false gave "Summary: 3 passed,
    0 warnings, 0 errors". A verdict of all-clear for a machine backing up
    nothing is the exact false-comfort doctor exists to prevent."""

    def _doctor_for(self, tmp_path, toml):
        cfg = tmp_path / "c.toml"
        cfg.write_text(toml)
        config, warnings = load_config(cfg)
        return Doctor(config=config, config_path=cfg, config_warnings=warnings)

    def test_all_volumes_disabled_draws_a_warning(self, tmp_path):
        doctor = self._doctor_for(
            tmp_path,
            """
[[volumes]]
path = "/home"
enabled = false

[[volumes.targets]]
path = "/mnt/backup"
""",
        )
        findings = doctor._check_config_valid()
        warns = [f for f in findings if f.severity == DiagnosticSeverity.WARN]
        assert any("back up NOTHING" in f.message for f in warns), findings
        assert any("disabled" in f.message for f in warns)
        assert not any(
            f.severity == DiagnosticSeverity.OK and f.check_name == "config_valid"
            for f in findings
        ), "doctor blessed a config that backs up nothing"

    def test_zero_declared_volumes_draws_a_warning(self, tmp_path):
        doctor = self._doctor_for(tmp_path, 'snapshot_prefix = "x-"\n')
        findings = doctor._check_config_valid()
        assert any(
            f.severity == DiagnosticSeverity.WARN and "declares no volumes" in f.message
            for f in findings
        ), findings

    def test_an_enabled_volume_still_gets_the_ok(self, tmp_path):
        doctor = self._doctor_for(
            tmp_path,
            """
[[volumes]]
path = "/home"

[[volumes.targets]]
path = "/mnt/backup"
""",
        )
        findings = doctor._check_config_valid()
        assert any(
            f.severity == DiagnosticSeverity.OK and f.check_name == "config_valid"
            for f in findings
        )
        assert not any("back up NOTHING" in f.message for f in findings)
