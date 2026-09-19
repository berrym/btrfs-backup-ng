"""The shell completions are generated; this fails when they drift.

Three dialects of the same fact were maintained by hand, and they drifted: an
audit before 0.9.7 found 89 flag/shell combinations the CLI accepted and no
completion offered. Among them was `--newest-only`, which is the escape hatch
for that release's change to `run`'s default -- the one flag a user most needed
to discover.

Generating them once would only relocate the problem, because the next flag
added would drift again. This test is the part that makes it stick: regenerate
from the parser and compare. If it fails, run

    python -c "from btrfs_backup_ng.cli.completion_gen import generate; \\
               import pathlib; \\
               [pathlib.Path(f'completions/btrfs-backup-ng.{s}').write_text(generate(s)) \\
                for s in ('bash','zsh','fish')]"

Man pages are deliberately not generated -- their prose says things argparse
cannot -- and are covered by test_docs_cover_every_flag.py instead.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from btrfs_backup_ng.cli.completion_gen import GENERATORS, generate

COMPLETIONS = Path(__file__).resolve().parent.parent / "completions"


@pytest.mark.parametrize("shell", sorted(GENERATORS))
def test_checked_in_completion_matches_the_generator(shell):
    path = COMPLETIONS / f"btrfs-backup-ng.{shell}"
    assert path.exists(), f"{path} is missing"

    expected = generate(shell)
    actual = path.read_text()

    assert actual == expected, (
        f"{path.name} is out of date with the argument parser. "
        f"Regenerate it rather than editing it by hand -- see this module's docstring."
    )


@pytest.mark.parametrize("shell", sorted(GENERATORS))
def test_every_flag_the_cli_accepts_is_offered(shell):
    """The property the generator exists to guarantee, asserted independently.

    Comparing generated-to-checked-in proves they agree; it does not prove the
    generator is right. This asserts coverage against the parser directly, so a
    generator that quietly dropped a flag would still fail.
    """
    import re

    from btrfs_backup_ng.cli.completion_gen import build_tree
    from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

    def wanted(node, out=None):
        out = set() if out is None else out
        for flag in node["flags"]:
            for long in flag["long"]:
                if long != "--help":
                    out.add(long)
        for sub in node["subcommands"].values():
            wanted(sub, out)
        return out

    text = (COMPLETIONS / f"btrfs-backup-ng.{shell}").read_text()
    if shell == "fish":
        offered = {"--" + m for m in re.findall(r"-l ([a-z0-9-]+)", text)}
    else:
        offered = set(re.findall(r"--[a-z0-9-]+", text))

    missing = sorted(wanted(build_tree(create_subcommand_parser())) - offered)
    assert not missing, f"{shell} completion does not offer: {missing}"


def test_generated_completions_are_not_empty_shells():
    """A generator that emitted a valid but empty file would pass the above."""
    for shell in GENERATORS:
        text = generate(shell)
        assert len(text.splitlines()) > 50, f"{shell} output is implausibly short"
        assert "btrfs-backup-ng" in text


def test_an_unknown_shell_is_refused():
    with pytest.raises(ValueError, match="unknown shell"):
        generate("powershell")


# ---------------------------------------------------------------------------
# Generating a syntactically broken completion is entirely possible, and the
# comparison tests above would happily confirm the broken file matches the
# broken generator. These run the real shells.
# ---------------------------------------------------------------------------
SYNTAX_CHECK = {
    "bash": ["bash", "-n"],
    "zsh": ["zsh", "-n"],
    "fish": ["fish", "--no-execute"],
}


@pytest.mark.parametrize("shell", sorted(SYNTAX_CHECK))
def test_the_generated_completion_parses_in_its_own_shell(shell):
    import shutil
    import subprocess

    if shutil.which(shell) is None:
        pytest.skip(f"{shell} is not installed, so its syntax cannot be checked")

    path = COMPLETIONS / f"btrfs-backup-ng.{shell}"
    result = subprocess.run(
        SYNTAX_CHECK[shell] + [str(path)], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, (
        f"{path.name} is not valid {shell}:\n{result.stderr or result.stdout}"
    )


def test_bash_completion_actually_offers_run_flags():
    """Parsing is not working. This sources it and asks for real completions."""
    import shutil
    import subprocess

    if shutil.which("bash") is None:
        pytest.skip("bash is not installed")

    script = (
        f"source {COMPLETIONS / 'btrfs-backup-ng.bash'}\n"
        "COMP_WORDS=(btrfs-backup-ng run --); COMP_CWORD=2\n"
        "_btrfs_backup_ng\n"
        'printf "%s\\n" "${COMPREPLY[@]}"\n'
    )
    out = subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, timeout=60
    ).stdout
    assert "--newest-only" in out, f"bash offered no --newest-only for run:\n{out}"
    assert "--dry-run" in out


def test_fish_completion_actually_offers_run_flags():
    import shutil
    import subprocess

    if shutil.which("fish") is None:
        pytest.skip("fish is not installed")

    script = (
        f"source {COMPLETIONS / 'btrfs-backup-ng.fish'}; "
        "complete -C 'btrfs-backup-ng run --'"
    )
    out = subprocess.run(
        ["fish", "-c", script], capture_output=True, text=True, timeout=60
    ).stdout
    assert "--newest-only" in out, f"fish offered no --newest-only for run:\n{out}"
