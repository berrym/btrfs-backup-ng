"""A global option given before the subcommand must reach the subcommand.

``btrfs-backup-ng -c config.toml restore --list raw+ssh://host:/backups`` used to
lose the config file entirely: argparse parses a subcommand into a fresh
namespace and copies every key onto the parent's, so a subparser that also
defines ``--config`` wrote its own ``None`` default over the value already
supplied.  Restore then had no SSH key and failed against a destination it had
just backed up to successfully.

The other half of the contract matters just as much: several subcommands reuse
``-c`` for a snapper config NAME rather than a config FILE, and a value must
never be carried across that boundary.
"""

from __future__ import annotations

import argparse

import pytest

from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

LOCATION = "raw+ssh://user@host:/backups"


def _walk_subparsers(
    parser: argparse.ArgumentParser, prefix: tuple[str, ...] = ()
) -> list[tuple[tuple[str, ...], argparse.ArgumentParser]]:
    found = []
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        for name, sub in action.choices.items():
            found.append(((*prefix, name), sub))
            found.extend(_walk_subparsers(sub, (*prefix, name)))
    return found


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["-c", "/global.toml", "restore", "--list", LOCATION], "/global.toml"),
        (["restore", "--list", LOCATION, "-c", "/sub.toml"], "/sub.toml"),
        (
            ["-c", "/global.toml", "restore", "--list", LOCATION, "-c", "/sub.toml"],
            "/sub.toml",
        ),
        (["restore", "--list", LOCATION], None),
        (["-c", "/global.toml", "estimate"], "/global.toml"),
        (["estimate", "-c", "/sub.toml"], "/sub.toml"),
    ],
)
def test_config_file_reaches_the_subcommand(
    argv: list[str], expected: str | None
) -> None:
    """Before the subcommand it survives; after it wins; absent it stays None."""
    assert create_subcommand_parser().parse_args(argv).config == expected


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["--quiet", "verify", LOCATION], True),
        (["verify", LOCATION, "--quiet"], True),
        (["verify", LOCATION], False),
        (["--quiet", "doctor"], True),
        (["doctor"], False),
    ],
)
def test_quiet_reaches_the_subcommand(argv: list[str], expected: bool) -> None:
    assert create_subcommand_parser().parse_args(argv).quiet is expected


@pytest.mark.parametrize(
    "argv",
    [
        ["-c", "/global.toml", "snapper", "list"],
        ["-c", "/global.toml", "snapper", "status"],
        ["-c", "/global.toml", "snapper", "generate-config"],
    ],
)
def test_a_config_file_never_arrives_as_a_snapper_config_name(
    argv: list[str],
) -> None:
    """``snapper list -c NAME`` names a snapper config, not a file.

    Carrying the global ``-c FILE`` into it would hand ``/global.toml`` to code
    that looks up a snapper config called ``root``.
    """
    assert create_subcommand_parser().parse_args(argv).config is None


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["snapper", "list", "-c", "root"], "root"),
        (["snapper", "status", "-c", "home"], "home"),
        (["snapper", "restore", "raw://src", "root"], "root"),
        (["snapper", "restore", "raw://src"], None),
        (["-c", "/global.toml", "snapper", "restore", "raw://src", "root"], "root"),
    ],
)
def test_snapper_config_names_still_work(argv: list[str], expected: str | None) -> None:
    """Both the option and the positional spelling keep their own meaning."""
    assert create_subcommand_parser().parse_args(argv).config == expected


def test_every_config_file_option_is_covered() -> None:
    """Guard the surface: a new subcommand taking ``-c FILE`` is covered automatically.

    Every subparser option that means the config file -- judged by the same
    metavar as the global option -- is parsed for real with the global spelling,
    and the value must arrive intact.  Options that reuse the name for something
    else are asserted to stay untouched.
    """
    parser = create_subcommand_parser()
    global_config = next(a for a in parser._actions if a.dest == "config")

    same_meaning = 0
    different_meaning = 0
    for path, sub in _walk_subparsers(parser):
        for action in sub._actions:
            if action.dest != "config" or not action.option_strings:
                continue
            argv = ["-c", "/probe.toml", *path]
            # Satisfy required positionals, never one landing in `config`.
            for positional in sub._actions:
                if positional.option_strings or positional.dest == "help":
                    continue
                if positional.dest == "config":
                    break
                if positional.nargs in (None, 1):
                    argv.append(LOCATION)
            try:
                parsed = create_subcommand_parser().parse_args(argv)
            except SystemExit:  # pragma: no cover - subcommand this probe can't satisfy
                continue

            where = " ".join(path)
            if action.metavar == global_config.metavar:
                assert parsed.config == "/probe.toml", (
                    f"'{where}' discarded the config file given before the subcommand"
                )
                same_meaning += 1
            else:
                assert parsed.config is None, (
                    f"'{where}' takes -c {action.metavar}, but the global config "
                    f"file leaked into it as {parsed.config!r}"
                )
                different_meaning += 1

    assert same_meaning, "no config-file subcommand was exercised; the walk is broken"
    assert different_meaning, "the NAME-flavoured subcommands were not exercised"
