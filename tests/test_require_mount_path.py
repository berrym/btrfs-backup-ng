"""`require_mount` accepts a mount point, not just a boolean.

Reported in #100: with several machines backing up into subdirectories of one
drive, targets look like `/mnt/backup/box1` while the drive is at `/mnt/backup`.
`is_mounted` compares for EQUALITY, so `require_mount = true` can only ever pass
when the target IS the mount point -- the subdirectory case could not be
protected at all, and the reporter's only option was to turn the check off.

`require_mount = "/mnt/backup"` says what is actually meant. Both halves are
enforced: the named path must be mounted, AND the target must live under it.
Asserting a drive the target is not written to would report success while
protecting nothing, which is worse than no check at all.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.cli.common import assert_target_mounted
from btrfs_backup_ng.config.loader import ConfigError, _parse_require_mount


def _gate(target, require_mount, mounted=(), fs_type="btrfs"):
    """Drive the gate with a controlled mount table.

    fs_type is stubbed because the gate refuses memory-backed filesystems, and
    these fixtures use paths under /tmp, which is tmpfs on a systemd machine.
    That rejection has its own tests in test_mount_table_parsing.py.
    """
    mounted = {str(m) for m in mounted}
    with (
        patch.object(__util__, "is_mounted", lambda p: str(p) in mounted),
        patch.object(__util__, "get_mount_info", lambda p: {"fs_type": fs_type}),
    ):
        assert_target_mounted(target, require_mount)


class TestTheSubdirectoryCaseNowWorks:
    def test_a_subdirectory_of_a_mounted_drive_passes(self):
        """THE reported case. `true` cannot express this and never could."""
        _gate("/mnt/backup/box1", "/mnt/backup", mounted=["/mnt/backup"])

    def test_the_mount_point_itself_still_passes(self):
        _gate("/mnt/backup", "/mnt/backup", mounted=["/mnt/backup"])

    def test_a_deeper_subdirectory_passes(self):
        _gate("/mnt/backup/box1/home", "/mnt/backup", mounted=["/mnt/backup"])

    def test_the_drive_being_absent_still_aborts(self):
        """The whole point of the option."""
        with pytest.raises(__util__.AbortError, match="is not mounted"):
            _gate("/mnt/backup/box1", "/mnt/backup", mounted=[])

    def test_a_raw_target_under_the_mount_passes(self):
        """raw:// targets are checked too since the gate was unified."""
        _gate("raw:///mnt/backup/box1", "/mnt/backup", mounted=["/mnt/backup"])


class TestAMountThatProtectsNothingIsRefused:
    """The footgun: naming a drive the target is not written to."""

    def test_a_target_outside_the_named_mount_aborts(self):
        with pytest.raises(__util__.AbortError, match="is not inside"):
            _gate("/somewhere/else", "/mnt/backup", mounted=["/mnt/backup"])

    def test_it_aborts_even_when_the_named_mount_is_present(self):
        """This is the dangerous shape: the check would PASS on the mount test
        alone, so the operator sees a green run while the backup goes somewhere
        the drive has nothing to do with."""
        with pytest.raises(__util__.AbortError, match="protecting nothing"):
            _gate("/var/backups", "/mnt/backup", mounted=["/mnt/backup", "/var"])

    def test_a_sibling_with_a_shared_prefix_is_not_inside(self):
        """String prefixes are not path containment: /mnt/backup2 is not under
        /mnt/backup, though it starts with it."""
        with pytest.raises(__util__.AbortError, match="is not inside"):
            _gate("/mnt/backup2/box1", "/mnt/backup", mounted=["/mnt/backup"])


class TestTheBooleanFormIsUnchanged:
    def test_true_still_requires_the_target_itself_to_be_a_mount(self):
        _gate("/mnt/backup", True, mounted=["/mnt/backup"])

    def test_true_still_refuses_a_subdirectory(self):
        """Unchanged, deliberately -- existing configs must behave as before.

        The refusal is the behaviour under test. The MESSAGE now diagnoses the
        case rather than saying "is not mounted", because with the drive
        connected that reads as advice to fix something that is not broken; see
        TestTheUpgradeErrorDiagnosesItself.
        """
        with pytest.raises(__util__.AbortError) as caught:
            _gate("/mnt/backup/box1", True, mounted=["/mnt/backup"])
        assert 'require_mount = "/mnt/backup"' in str(caught.value)

    def test_false_checks_nothing(self):
        _gate("/anywhere/at/all", False, mounted=[])

    @pytest.mark.parametrize(
        "uri", ["ssh://user@host:/backups", "raw+ssh://user@host/backups"]
    )
    def test_remote_targets_stay_exempt(self, uri):
        _gate(uri, "/mnt/backup", mounted=[])


class TestBadValuesAreCoercedAndNamed:
    """Most malformed values are coerced and warned about, not rejected.

    The mount gate already refuses an unusable value per target, fail-closed,
    quoting it -- so a ConfigError at load adds no safety and takes the WHOLE
    file down, stopping `list`, `status` and `doctor` as well as `run`, for
    configs that worked on 0.9.6. But a coercion nobody is told about is the
    "recognised and silently ignored" shape this project keeps removing, so each
    one is named with what it was read as.
    """

    def test_a_path_is_accepted(self):
        assert _parse_require_mount("/mnt/backup") == "/mnt/backup"

    @pytest.mark.parametrize("value", [True, False])
    def test_booleans_are_accepted(self, value):
        assert _parse_require_mount(value) is value

    @pytest.mark.parametrize(
        ("value", "expected"),
        [(1, True), (0, False), (1.5, True), (0.0, False)],
    )
    def test_numbers_are_coerced_by_truthiness(self, value, expected):
        """Exactly what 0.9.6 did with them, and the only reading they have."""
        assert _parse_require_mount(value) is expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("true", True),
            ("True", True),
            ("yes", True),
            ("on", True),
            ("1", True),
            ("false", False),
            ("FALSE", False),
            ("no", False),
            ("off", False),
            ("0", False),
        ],
    )
    def test_quoted_booleans_are_coerced(self, value, expected):
        assert _parse_require_mount(value) is expected

    def test_a_relative_path_is_kept_for_the_gate_to_refuse(self):
        """Not an error: the gate refuses this target with the value quoted."""
        assert _parse_require_mount("mnt/backup") == "mnt/backup"
        with pytest.raises(__util__.AbortError):
            _gate("/mnt/backup/box1", "mnt/backup", mounted=["/mnt/backup"])


class TestTheTwoValuesThatStillFailLoudly:
    """Coercing these would be a guess whose wrong answer is dangerous."""

    @pytest.mark.parametrize("value", ["", "   ", "\t"])
    def test_an_empty_string_is_rejected(self, value):
        """Both 0.9.6 and the gate read it as falsy, so it turns the check OFF
        silently -- the accident require_mount exists to prevent. It nearly
        always means a variable that expanded to nothing."""
        with pytest.raises(ConfigError, match="turn the mount check OFF"):
            _parse_require_mount(value)

    @pytest.mark.parametrize("value", [["/mnt/backup"], {"a": 1}, None])
    def test_uninterpretable_types_are_rejected(self, value):
        with pytest.raises(ConfigError, match="must be true, false, or a mount"):
            _parse_require_mount(value)


class TestAVacuousMountLoadsButCannotRun:
    """`require_mount = "/"` would pass unconditionally. It is warned about at
    load and refused by the gate, rather than stopping the file loading."""

    @pytest.mark.parametrize("value", ["/", "//", "/.", "/mnt/.."])
    def test_it_loads(self, value):
        assert _parse_require_mount(value) == value

    @pytest.mark.parametrize("value", ["/", "/.", "/mnt/.."])
    def test_the_gate_refuses_it(self, value):
        with pytest.raises(__util__.AbortError, match="protect nothing|always pass"):
            _gate("/mnt/backup/box1", value, mounted=["/"])


class TestTheConfigWriterPreservesIt:
    @staticmethod
    def _emit(require_mount):
        """Run the real writer and return the require_mount line it produced."""
        from btrfs_backup_ng.cli.config_cmd import _generate_config_from_wizard

        toml = _generate_config_from_wizard(
            {
                "snapshot_dir": "/snapshots",
                "timestamp_format": "%Y%m%d-%H%M%S",
                "incremental": True,
                "parallel_volumes": 1,
                "parallel_targets": 1,
                "log_file": "",
                "transaction_log": "",
                "volumes": [
                    {
                        "path": "/home",
                        "targets": [
                            {"path": "/mnt/backup/box1", "require_mount": require_mount}
                        ],
                    }
                ],
            }
        )
        return [ln for ln in toml.splitlines() if ln.startswith("require_mount")]

    def test_a_mount_point_survives_a_round_trip(self):
        """`if target.get("require_mount"): lines.append("require_mount = true")`
        rewrote a configured path as a boolean, turning the subdirectory setup
        back into a check that cannot pass.

        Drives the real writer rather than asserting on its source: the first
        version of this test checked that the emit line EXISTS in the module
        text, which kept passing when the branch guarding it was disabled.
        """
        assert self._emit("/mnt/backup") == ['require_mount = "/mnt/backup"'], (
            "the writer rewrote a configured mount point, so it no longer says "
            "which drive must be mounted"
        )

    def test_the_written_value_loads_back_unchanged(self):
        """Round-trip, not just emission."""
        line = self._emit("/mnt/backup")[0]
        value = line.split("=", 1)[1].strip().strip('"')
        assert _parse_require_mount(value) == "/mnt/backup"

    def test_true_still_emits_a_boolean(self):
        assert self._emit(True) == ["require_mount = true"]

    def test_false_emits_nothing(self):
        assert self._emit(False) == []
