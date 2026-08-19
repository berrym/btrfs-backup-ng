"""`doctor` must not report OK for a check it did not perform.

Two checks returned DiagnosticSeverity.OK with the message "requires endpoint
access" and a note reading "Full implementation pending". Their bodies were a
comment and an append -- nothing inspected anything. An operator running
`doctor` before trusting this tool with their data saw:

    [OK]  Parent chain check requires endpoint access
    [OK]  Snapshot date check requires endpoint access

which reads as "your parent chains and snapshot dates are fine". They had not
been looked at. That is the same defect as the rest of this work -- a check that
did not happen, reported as one that did -- in the command whose entire purpose
is to be believed.

The structural test below is the point of this file. Fixing two instances is
worth little if the pattern can come back; this fails on any future check that
describes itself as unimplemented while claiming a pass.
"""

from __future__ import annotations

import ast
import inspect
import io

import pytest

from btrfs_backup_ng.core import doctor as doctor_mod
from btrfs_backup_ng.core.doctor import (
    DiagnosticCategory,
    DiagnosticSeverity,
    Doctor,
)

UNIMPLEMENTED_WORDING = (
    "not implemented",
    "implementation pending",
    "placeholder",
    "for now, return",
    "would require",
    "would need",
    "todo",
)


def _check_methods():
    source = io.open(inspect.getfile(doctor_mod), encoding="utf-8").read()
    lines = source.split("\n")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith("_check"):
            body = "\n".join(lines[node.lineno - 1 : node.end_lineno])
            yield node.name, body


class TestNoCheckClaimsAPassItDidNotEarn:
    def test_an_unimplemented_check_never_reports_ok(self):
        offenders = []
        for name, body in _check_methods():
            lowered = body.lower()
            says_unimplemented = any(w in lowered for w in UNIMPLEMENTED_WORDING)
            claims_ok = "DiagnosticSeverity.OK" in body
            if says_unimplemented and claims_ok:
                offenders.append(name)
        assert not offenders, (
            "these checks describe themselves as unimplemented yet report OK, "
            f"which tells an operator a check passed that never ran: {offenders}"
        )

    def test_the_two_known_stubs_report_info(self):
        doctor = Doctor(config=object())
        for method in (doctor._check_parent_chains, doctor._check_snapshot_dates):
            findings = method()
            assert findings, f"{method.__name__} produced no finding at all"
            for finding in findings:
                assert finding.severity is DiagnosticSeverity.INFO, (
                    f"{method.__name__} reported {finding.severity}"
                )

    @pytest.mark.parametrize(
        "method_name", ["_check_parent_chains", "_check_snapshot_dates"]
    )
    def test_the_message_says_it_was_not_checked(self, method_name):
        """The old wording ("requires endpoint access") explained a limitation
        while still reading like a pass. The operator has to be told plainly
        that nothing was examined."""
        doctor = Doctor(config=object())
        finding = getattr(doctor, method_name)()[0]
        assert "NOT checked" in finding.message, finding.message
        assert finding.details.get("implemented") is False

    def test_an_unimplemented_check_does_not_change_the_exit_code(self):
        """A gap in coverage is not a fault in the system: `doctor` should not
        start failing because a check is missing."""
        from btrfs_backup_ng.core.doctor import DiagnosticFinding, DiagnosticReport

        report = DiagnosticReport()
        report.add_finding(
            DiagnosticFinding(
                category=DiagnosticCategory.SNAPSHOTS,
                severity=DiagnosticSeverity.INFO,
                check_name="x",
                message="was NOT checked",
            )
        )
        assert report.exit_code == 0

    def test_a_check_with_no_config_stays_silent(self):
        """Guard against over-correcting: with nothing configured there is
        nothing to report, not an INFO line per check."""
        doctor = Doctor(config=None)
        assert doctor._check_parent_chains() == []
        assert doctor._check_snapshot_dates() == []
