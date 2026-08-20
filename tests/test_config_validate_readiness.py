"""`config validate` must not green-light a config that cannot back anything up.

Measured before this change, against a config whose every path was fictional:

    Warnings:
      - Volume '/another/missing/one' has no targets configured

    Configuration is valid.
      Volumes: 2   Enabled: 2   Targets: 1
    exit=0

"Configuration is valid" and exit 0, for sources that do not exist and are not
btrfs. It validated syntax and structure and printed a word that means far more
than that to the person about to trust it with their data.

`validate` now also answers the cheap local question -- can these sources
actually be read from this machine -- and says which half it is talking about.
Remote targets are deliberately NOT probed: that needs the network and belongs
to `doctor`.
"""

from __future__ import annotations

import argparse


from btrfs_backup_ng.cli.config_cmd import (
    _validate_config,
    _volume_readiness_problems,
)


def _write(tmp_path, body):
    path = tmp_path / "config.toml"
    path.write_text(body)
    return path


def _args(path):
    return argparse.Namespace(config=str(path))


class TestTheLocalReadinessProbe:
    def test_a_missing_source_is_a_problem(self, tmp_path):
        volume = type("V", (), {"path": str(tmp_path / "nope")})()
        problems = _volume_readiness_problems(volume)
        assert problems and "does not exist" in problems[0]

    def test_a_file_where_a_subvolume_belongs_is_a_problem(self, tmp_path):
        target = tmp_path / "afile"
        target.write_text("x")
        volume = type("V", (), {"path": str(target)})()
        problems = _volume_readiness_problems(volume)
        assert problems and "not a directory" in problems[0]

    def test_an_existing_directory_is_reported_on_its_filesystem(self, tmp_path):
        """tmp_path is usually NOT btrfs, so this should say so -- and must not
        crash when it cannot tell."""
        volume = type("V", (), {"path": str(tmp_path)})()
        problems = _volume_readiness_problems(volume)
        assert problems == [] or "btrfs" in problems[0]

    def test_a_probe_failure_is_not_a_verdict(self, tmp_path, monkeypatch):
        """If the filesystem cannot be determined, that is not evidence the
        volume is broken -- do not invent a problem."""
        from btrfs_backup_ng.cli import config_cmd

        monkeypatch.setattr(
            config_cmd, "_volume_readiness_problems", _volume_readiness_problems
        )
        import btrfs_backup_ng.__util__ as util

        monkeypatch.setattr(
            util, "is_btrfs", lambda p: (_ for _ in ()).throw(OSError("boom"))
        )
        volume = type("V", (), {"path": str(tmp_path)})()
        assert _volume_readiness_problems(volume) == []


class TestValidateReportsBothHalves:
    def test_fictional_sources_do_not_get_a_green_light(self, tmp_path, capsys):
        cfg = _write(
            tmp_path,
            '[[volumes]]\npath = "/definitely/not/here"\n\n'
            '[[volumes.targets]]\npath = "/mnt/backup"\n',
        )
        rc = _validate_config(_args(cfg))
        out = capsys.readouterr().out
        # Not zero: this is not a green light. Specifically 2, which says the
        # FILE is fine and this machine cannot run it -- distinct from 1, which
        # means the file itself must be edited.
        assert rc != 0, "an unusable config exited 0"
        assert rc == 2, rc
        assert "cannot be backed up from this machine" in out
        assert "/definitely/not/here" in out

    def test_it_no_longer_claims_bare_validity(self, tmp_path, capsys):
        """The old wording was 'Configuration is valid.' full stop, which is the
        sentence that did the damage."""
        cfg = _write(
            tmp_path,
            '[[volumes]]\npath = "/definitely/not/here"\n\n'
            '[[volumes.targets]]\npath = "/mnt/backup"\n',
        )
        _validate_config(_args(cfg))
        out = capsys.readouterr().out
        assert "Configuration is valid." not in out
        assert "syntax and structure" in out

    def test_it_says_which_half_it_checked(self, tmp_path, capsys):
        cfg = _write(
            tmp_path,
            '[[volumes]]\npath = "/definitely/not/here"\n\n'
            '[[volumes.targets]]\npath = "/mnt/backup"\n',
        )
        _validate_config(_args(cfg))
        out = capsys.readouterr().out
        assert "Checked locally only" in out
        assert "doctor" in out

    def test_a_usable_source_still_passes(self, tmp_path, capsys):
        """Guard against over-correcting: a real directory must not be failed."""
        source = tmp_path / "src"
        source.mkdir()
        cfg = _write(
            tmp_path,
            f'[[volumes]]\npath = "{source}"\n\n'
            '[[volumes.targets]]\npath = "/mnt/backup"\n',
        )

        # Pretend the source is on btrfs; the point here is the pass path.
        import btrfs_backup_ng.__util__ as util

        original = util.is_btrfs
        util.is_btrfs = lambda p: True
        try:
            rc = _validate_config(_args(cfg))
        finally:
            util.is_btrfs = original
        out = capsys.readouterr().out
        assert rc == 0, out
        assert "look usable from this machine" in out

    def test_remote_targets_are_not_probed(self, tmp_path, capsys):
        """validate stays offline. A network probe here would make a config
        check hang on an unreachable host."""
        source = tmp_path / "src"
        source.mkdir()
        cfg = _write(
            tmp_path,
            f'[[volumes]]\npath = "{source}"\n\n'
            '[[volumes.targets]]\npath = "ssh://unreachable.invalid:/backups"\n',
        )
        import btrfs_backup_ng.__util__ as util

        original = util.is_btrfs
        util.is_btrfs = lambda p: True
        try:
            rc = _validate_config(_args(cfg))
        finally:
            util.is_btrfs = original
        assert rc == 0
        assert "unreachable" not in capsys.readouterr().out.lower()

    def test_a_broken_config_file_still_errors(self, tmp_path, capsys):
        cfg = _write(tmp_path, "this is not toml {{{")
        assert _validate_config(_args(cfg)) == 1


class TestTheExitStatusDistinguishesTheTwoFailures:
    """ "Cannot run here" is not the same as "your file is wrong".

    Both returned 1, so a caller could not tell a config that must be edited
    from a perfectly good config being checked on a machine that does not host
    its volumes -- which is the normal case when editing a NAS's config on a
    laptop, or validating in CI. A correct configuration failed its own check
    with nothing to fix.
    """

    def _validate(self, tmp_path, body, name="config.toml"):
        config = tmp_path / name
        config.write_text(body, encoding="utf-8")
        return _validate_config(_args(config))

    def test_an_invalid_file_exits_1(self, tmp_path):
        assert self._validate(tmp_path, "[[volumes]]\npath =\n") == 1

    def test_a_valid_file_this_machine_cannot_run_exits_2(self, tmp_path):
        source = tmp_path / "not-a-subvolume"
        source.mkdir()
        code = self._validate(
            tmp_path,
            f'[[volumes]]\npath = "{source}"\n'
            f'snapshot_dir = "{source}/.snapshots"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path / "dest"}"\n',
        )
        assert code == 2, "a valid file was reported the same way as a broken one"

    def test_the_two_codes_are_different(self, tmp_path):
        source = tmp_path / "plain"
        source.mkdir()
        unusable = self._validate(
            tmp_path,
            f'[[volumes]]\npath = "{source}"\n'
            f'snapshot_dir = "{source}/.snapshots"\n\n'
            f'[[volumes.targets]]\npath = "{tmp_path / "d"}"\n',
            name="usable.toml",
        )
        invalid = self._validate(tmp_path, "[[volumes]]\npath =\n", name="invalid.toml")
        assert unusable != invalid
