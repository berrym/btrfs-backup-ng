"""``restore --in-place`` refuses and restores nothing, in every mode.

The flag was accepted and then ignored. The ordinary restore ran, landed the
snapshot NESTED at ``DESTINATION/<name>``, replaced nothing, and exited 0 --
while the README documented ``--in-place`` as a disaster-recovery strategy
with copy-paste commands. The config-driven path (``--volume``) never read
the flag at all. Until in-place restore is implemented (staged receive,
verification against the backup's identity, then the swap), the command
refuses ahead of every mode, before any endpoint is prepared, and names the
manual procedure that does work.
"""

from __future__ import annotations

import pytest

from btrfs_backup_ng.cli.dispatcher import main


def _restore(argv, capsys):
    """Run the CLI and return (rc, output with the console's wrapping undone)."""
    rc = main(["restore", *argv])
    captured = capsys.readouterr()
    return rc, " ".join((captured.out + captured.err).split())


class TestTheRefusal:
    def test_refuses_with_the_reason_and_restores_nothing(self, tmp_path, capsys):
        src = tmp_path / "backups"
        src.mkdir()
        dest = tmp_path / "home"
        dest.mkdir()
        rc, text = _restore(
            [str(src), str(dest), "--in-place", "--yes-i-know-what-i-am-doing"],
            capsys,
        )
        assert rc == 2
        assert "not implemented" in text
        assert "nothing was restored" in text
        assert "Strategy 2" in text
        assert list(dest.iterdir()) == [], "the refusal touched the destination"

    def test_refuses_before_any_endpoint_is_prepared(self, tmp_path, capsys):
        """A source that does not exist would fail at prepare; the refusal
        is what fires, which proves nothing was opened first."""
        rc, text = _restore(
            [
                "ssh://nobody@nowhere.invalid/backups",
                str(tmp_path / "home"),
                "--in-place",
            ],
            capsys,
        )
        assert rc == 2
        assert "not implemented" in text
        assert "nowhere.invalid" not in text, "an endpoint was prepared first"
        assert not (tmp_path / "home").exists()

    def test_the_confirmation_flag_does_not_unlock_it(self, tmp_path, capsys):
        rc, text = _restore(
            [
                str(tmp_path),
                str(tmp_path / "d"),
                "--in-place",
                "--yes-i-know-what-i-am-doing",
            ],
            capsys,
        )
        assert rc == 2 and "not implemented" in text

    def test_dry_run_refuses_too(self, tmp_path, capsys):
        """The README's own recipe ran a --dry-run first; a dry run that
        pretends the flag works is the same lie one step earlier."""
        rc, text = _restore(
            [str(tmp_path), str(tmp_path / "d"), "--in-place", "--dry-run"], capsys
        )
        assert rc == 2 and "not implemented" in text

    def test_the_config_driven_mode_refuses_as_well(
        self, tmp_path, capsys, monkeypatch
    ):
        """--volume never read the flag; it must not silently ignore it either."""
        cfg = tmp_path / "c.toml"
        cfg.write_text(
            f'[[volumes]]\npath = "{tmp_path}/home"\n[[volumes.targets]]\npath = "{tmp_path}/dst"\n'
        )
        rc = main(
            ["-c", str(cfg), "restore", "--volume", f"{tmp_path}/home", "--in-place"]
        )
        captured = capsys.readouterr()
        text = " ".join((captured.out + captured.err).split())
        assert rc == 2
        assert "not implemented" in text


def test_without_the_flag_nothing_changes(tmp_path, capsys):
    """The refusal is keyed on the flag alone: a restore without it proceeds
    to its ordinary path (here, failing later on a missing source)."""
    src = tmp_path / "missing"
    rc, text = _restore([str(src), str(tmp_path / "home")], capsys)
    assert "not implemented in this release" not in text


@pytest.mark.parametrize("flag", ["--in-place"])
def test_the_help_says_it_is_not_implemented(flag, capsys):
    with pytest.raises(SystemExit):
        main(["restore", "--help"])
    out = " ".join(capsys.readouterr().out.split())
    idx = out.index(f"{flag} ")
    assert "Not implemented" in out[idx : idx + 400], out[idx : idx + 400]
