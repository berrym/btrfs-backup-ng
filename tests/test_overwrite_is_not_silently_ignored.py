"""`--overwrite` is not implemented, and says so.

It was built on this branch and then withdrawn. Replacing a snapshot means
deleting it before receiving its replacement -- a received subvolume cannot be
renamed or moved, so there is no staging and no atomic swap -- and four
adversarial passes each found another way for that window to end with the
operator holding neither copy. The last was a corrupt backup: every check of
whether the replacement could be delivered ran after the deletion, so a restore
destroyed the last good copy to make room for something it could not deliver,
then advised re-running, which failed identically every time.

The flag stays and reports. Accepting it and doing nothing is the defect this
project has spent several releases removing: an option recognised and silently
ignored. An operator who passes it to refresh a stale copy would otherwise
believe it had been refreshed.
"""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

from btrfs_backup_ng.cli import restore as restore_cli


def _args(tmp_path, **kw):
    base = dict(
        source=str(tmp_path),
        destination=str(tmp_path),
        overwrite=True,
        in_place=False,
        yes_i_know_what_i_am_doing=False,
        dry_run=False,
        fs_checks="skip",
        prefix="",
    )
    base.update(kw)
    return argparse.Namespace(**base)


class TestTheFlagIsNotSilentlyIgnored:
    def test_passing_overwrite_says_it_is_not_applied(self, tmp_path, caplog):
        with patch.object(
            restore_cli, "validate_restore_destination", lambda *a, **k: None
        ):
            with patch.object(restore_cli, "_prepare_backup_endpoint", MagicMock()):
                with patch.object(restore_cli, "_prepare_local_endpoint", MagicMock()):
                    with patch.object(
                        restore_cli,
                        "restore_snapshots",
                        lambda *a, **k: {
                            "restored": 0,
                            "skipped": 0,
                            "failed": 0,
                            "errors": [],
                        },
                    ):
                        with caplog.at_level("WARNING"):
                            restore_cli._execute_main_restore(_args(tmp_path))
        assert "not supported in this release" in caplog.text, (
            "the flag was accepted and silently did nothing"
        )
        assert "NOT applied" in caplog.text

    def test_it_is_reported_even_when_something_else_is_wrong(self, tmp_path, caplog):
        """An operator with a bad destination should still learn the flag does
        nothing, rather than discovering it on the next run."""
        with caplog.at_level("WARNING"):
            restore_cli._execute_main_restore(
                _args(tmp_path, destination="/nonexistent", source="/nonexistent")
            )
        assert "not supported in this release" in caplog.text

    def test_the_rest_of_the_restore_still_runs(self, tmp_path):
        """Refusing the whole run would be worse: a restore that brings back the
        snapshots that ARE missing is still useful."""
        ran = {}

        with patch.object(
            restore_cli, "validate_restore_destination", lambda *a, **k: None
        ):
            with patch.object(restore_cli, "_prepare_backup_endpoint", MagicMock()):
                with patch.object(restore_cli, "_prepare_local_endpoint", MagicMock()):

                    def _restore(*a, **kw):
                        ran["skip_existing"] = kw.get("skip_existing")
                        return {
                            "restored": 1,
                            "skipped": 0,
                            "failed": 0,
                            "errors": [],
                        }

                    with patch.object(restore_cli, "restore_snapshots", _restore):
                        rc = restore_cli._execute_main_restore(_args(tmp_path))

        assert rc == 0, "the run was refused instead of continuing"
        assert ran.get("skip_existing") is True, (
            "existing snapshots must be left alone, since replacing them is not "
            "supported -- skip_existing=False would attempt exactly that"
        )


class TestTheDestructiveMachineryIsGone:
    def test_no_replacement_helpers_remain(self):
        """A partially-removed feature is worse than either state."""
        import inspect

        from btrfs_backup_ng.core import restore as core_restore

        source = inspect.getsource(core_restore)
        for gone in (
            "_replace_at_destination",
            "_ReplaceFailed",
            "_replacement_size",
            "replaced_names",
        ):
            assert gone not in source, (
                f"{gone} survived the removal of --overwrite; the feature is "
                f"half-present"
            )
