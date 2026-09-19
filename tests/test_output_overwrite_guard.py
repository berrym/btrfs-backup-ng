"""`-o` replaced an existing configuration without asking.

`config import -o FILE` had no existence check of any kind: it wrote, printed
"Configuration written to:", and exited 0 over whatever was there. `config init
-o FILE` had a check, but only inside `if ... and interactive`, so every
non-interactive invocation -- a script, a cron job, a Makefile, anyone who typed
the command without `-i` -- silently replaced the file too.

The file being replaced is a configuration: the statement of which subvolumes
matter and how long their history is kept. Losing it is not losing a scratch
artifact, and the loss is silent and immediate.

Both now refuse an existing target and say how to proceed. `--force` overwrites.
The interactive prompt is unchanged, so a TTY user is still asked rather than
told to re-run.
"""

from __future__ import annotations

import argparse

import pytest

from btrfs_backup_ng.cli import config_cmd

ORIGINAL = "PRECIOUS ORIGINAL\n"
BTRBK_SOURCE = "volume /mnt/data\n  subvolume home\n    target /backup\n"


@pytest.fixture
def btrbk_conf(tmp_path):
    path = tmp_path / "btrbk.conf"
    path.write_text(BTRBK_SOURCE)
    return path


def _import(btrbk_conf, output, force=False):
    return config_cmd._import_config(
        argparse.Namespace(
            btrbk_config=str(btrbk_conf), output=str(output), force=force
        )
    )


def _init(output, force=False, interactive=False):
    return config_cmd._init_config(
        argparse.Namespace(output=str(output), force=force, interactive=interactive)
    )


class TestClobberPredicate:
    def test_no_output_never_clobbers(self):
        assert config_cmd._output_would_clobber(None, force=False) is False

    def test_missing_file_does_not_clobber(self, tmp_path):
        target = tmp_path / "absent.toml"
        assert config_cmd._output_would_clobber(str(target), force=False) is False

    def test_existing_file_clobbers(self, tmp_path):
        target = tmp_path / "present.toml"
        target.write_text(ORIGINAL)
        assert config_cmd._output_would_clobber(str(target), force=False) is True

    def test_force_permits_the_clobber(self, tmp_path):
        target = tmp_path / "present.toml"
        target.write_text(ORIGINAL)
        assert config_cmd._output_would_clobber(str(target), force=True) is False


class TestImportRefusesToClobber:
    def test_refuses_and_leaves_the_file_byte_for_byte(self, tmp_path, btrbk_conf):
        target = tmp_path / "out.toml"
        target.write_text(ORIGINAL)

        assert _import(btrbk_conf, target) == 1
        assert target.read_text() == ORIGINAL

    def test_force_replaces_it(self, tmp_path, btrbk_conf):
        target = tmp_path / "out.toml"
        target.write_text(ORIGINAL)

        assert _import(btrbk_conf, target, force=True) == 0
        assert target.read_text() != ORIGINAL
        assert "btrfs-backup-ng" in target.read_text()

    def test_a_new_file_is_written_without_force(self, tmp_path, btrbk_conf):
        target = tmp_path / "fresh.toml"

        assert _import(btrbk_conf, target) == 0
        assert target.exists()

    def test_refusal_happens_before_the_conversion_runs(self, tmp_path, monkeypatch):
        """Fail fast: do not parse a btrbk config we have already decided not to write."""
        import btrfs_backup_ng.btrbk_import as importer

        def explode(_path):
            raise AssertionError("conversion ran despite the refusal")

        monkeypatch.setattr(importer, "import_btrbk_config", explode)

        source = tmp_path / "btrbk.conf"
        source.write_text(BTRBK_SOURCE)
        target = tmp_path / "out.toml"
        target.write_text(ORIGINAL)

        assert _import(source, target) == 1


class TestInitRefusesToClobberWhenNotInteractive:
    def test_refuses_and_leaves_the_file(self, tmp_path):
        target = tmp_path / "config.toml"
        target.write_text(ORIGINAL)

        assert _init(target) == 1
        assert target.read_text() == ORIGINAL

    def test_force_replaces_it(self, tmp_path):
        target = tmp_path / "config.toml"
        target.write_text(ORIGINAL)

        assert _init(target, force=True) == 0
        assert target.read_text() != ORIGINAL

    def test_a_new_file_is_written_without_force(self, tmp_path):
        target = tmp_path / "fresh.toml"

        assert _init(target) == 0
        assert target.exists()


class TestTheFlagIsActuallyReachable:
    """A guard the CLI cannot express is a guard nobody can get past."""

    @pytest.mark.parametrize("command", ["import", "init"])
    def test_force_is_accepted_on_the_command_line(self, command, tmp_path):
        from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

        argv = ["config", command]
        if command == "import":
            argv.append(str(tmp_path / "btrbk.conf"))
        argv += ["-o", str(tmp_path / "out.toml"), "--force"]

        args = create_subcommand_parser().parse_args(argv)
        assert args.force is True
