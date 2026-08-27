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
        """Unchanged, deliberately -- existing configs must behave as before."""
        with pytest.raises(__util__.AbortError, match="is not mounted"):
            _gate("/mnt/backup/box1", True, mounted=["/mnt/backup"])

    def test_false_checks_nothing(self):
        _gate("/anywhere/at/all", False, mounted=[])

    @pytest.mark.parametrize(
        "uri", ["ssh://user@host:/backups", "raw+ssh://user@host/backups"]
    )
    def test_remote_targets_stay_exempt(self, uri):
        _gate(uri, "/mnt/backup", mounted=[])


class TestBadValuesFailAtConfigLoad:
    """Not at backup time. This guard stops a backup landing on the root
    filesystem, so a value that cannot be honoured must fail before the run."""

    def test_a_path_is_accepted(self):
        assert _parse_require_mount("/mnt/backup") == "/mnt/backup"

    @pytest.mark.parametrize("value", [True, False])
    def test_booleans_are_accepted(self, value):
        assert _parse_require_mount(value) is value

    def test_an_empty_string_is_rejected(self):
        """Reads as an expansion that produced nothing. Treating it as false
        would disable the safety check silently."""
        with pytest.raises(ConfigError, match="empty string"):
            _parse_require_mount("")

    def test_a_relative_path_is_rejected(self):
        with pytest.raises(ConfigError, match="absolute"):
            _parse_require_mount("mnt/backup")

    @pytest.mark.parametrize("value", [123, 1.5, ["/mnt/backup"], {"a": 1}])
    def test_other_types_are_rejected(self, value):
        with pytest.raises(ConfigError, match="must be true, false, or a mount"):
            _parse_require_mount(value)


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


class TestAVacuousMountIsRefused:
    """`require_mount = "/"` would pass unconditionally: root is always mounted
    and every path is under it. That is the containment footgun from the other
    direction -- a check that looks like protection and provides none."""

    @pytest.mark.parametrize("value", ["/", "//", "///"])
    def test_root_is_refused_at_config_load(self, value):
        with pytest.raises(ConfigError, match="would always pass"):
            _parse_require_mount(value)

    def test_a_real_mount_point_is_still_accepted(self):
        assert _parse_require_mount("/mnt") == "/mnt"
