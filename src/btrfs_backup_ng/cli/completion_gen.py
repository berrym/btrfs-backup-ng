"""Shell completions generated from the argument parser itself.

The completions used to be maintained by hand in three shells. They drifted:
an audit before 0.9.7 found 89 flag/shell combinations the CLI accepted and no
completion offered, including ``--newest-only``, the escape hatch for that
release's change to ``run``'s default. Hand-maintaining three dialects of the
same fact is three chances to be wrong, and the audit that found the drift had
two bugs of its own before it gave a trustworthy answer.

So the parser is the single source and these are derived from it.
``tests/test_completions_up_to_date.py`` regenerates and compares, which is what
actually keeps them honest -- generating once without a check just relocates the
staleness.

Man pages are deliberately NOT generated: their prose (DESCRIPTION, EXAMPLES,
RECOVERY FROM FAILURES) says things an argparse help string cannot, and throwing
that away to win consistency would be a bad trade. They are covered instead by
``tests/test_docs_cover_every_flag.py``, which requires every flag to appear
without dictating how it is described.
"""

from __future__ import annotations

import argparse
from typing import Any

HEADER = "Generated from the argument parser; edit the parser, not this file."

# A value that names something on disk should complete as a path. argparse does
# not record that, so it is inferred from the metavar, which is the only signal
# available and is consistent across this CLI.
_PATH_HINTS = ("FILE", "PATH", "DIR", "SOCK", "KEY", "KEYRING", "TARGET", "SOURCE")


def _is_path(metavar: str | None) -> bool:
    return bool(metavar) and any(h in str(metavar).upper() for h in _PATH_HINTS)


def _flag_info(action: argparse.Action) -> dict[str, Any]:
    takes_value = not isinstance(
        action,
        (
            argparse._StoreTrueAction,
            argparse._StoreFalseAction,
            argparse._CountAction,
            argparse._HelpAction,
            argparse._VersionAction,
        ),
    )
    metavar = action.metavar if isinstance(action.metavar, str) else None
    if takes_value and not metavar and not action.choices:
        metavar = (action.dest or "value").upper()
    return {
        "long": [o for o in action.option_strings if o.startswith("--")],
        "short": [o for o in action.option_strings if not o.startswith("--")],
        "help": " ".join((action.help or "").split()).replace("%%", "%"),
        "takes_value": takes_value,
        "choices": [str(c) for c in action.choices] if action.choices else [],
        "metavar": metavar,
    }


def build_tree(parser: argparse.ArgumentParser) -> dict[str, Any]:
    """Walk a parser into {flags, subcommands} with everything a shell needs."""
    node: dict[str, Any] = {"flags": [], "subcommands": {}}
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for name, sub in action.choices.items():
                node["subcommands"][name] = build_tree(sub)
        elif action.option_strings:
            node["flags"].append(_flag_info(action))
    return node


def _curated_values() -> dict[str, list[str]]:
    """Value lists for flags argparse leaves unconstrained.

    Each is read from the project's own canonical source rather than written
    out here, so a method added to the compression table or a category added to
    the doctor appears in all three shells without anyone remembering to. The
    hand-written completions carried a literal list of compression methods that
    had already drifted from the table.
    """
    from ..core.compression import COMPRESSION_CHOICES
    from ..core.doctor import DiagnosticCategory

    return {
        "--compress": list(COMPRESSION_CHOICES),
        "--check": [c.value for c in DiagnosticCategory],
        # snapper's --encrypt takes the same methods raw's does, but declares a
        # free-form metavar; mirror the constrained one.
        "--encrypt": ["gpg", "openssl_enc"],
        # snapper snapshot kinds, as validated by the config loader.
        "--type": ["single", "pre", "post"],
    }


def _values_for(flag: dict[str, Any]) -> list[str]:
    """Choices for a flag: argparse's own, else a curated canonical list."""
    if flag["choices"]:
        return flag["choices"]
    curated = _curated_values()
    for long in flag["long"]:
        if long in curated:
            return curated[long]
    return []


def _q(text: str) -> str:
    """Quote for a single-quoted shell string."""
    return text.replace("'", "'\\''")


def _all_options(flags: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for f in flags:
        out.extend(f["short"])
        out.extend(f["long"])
    return out


# --------------------------------------------------------------------- bash
def generate_bash(tree: dict[str, Any]) -> str:
    lines = [
        "# bash completion for btrfs-backup-ng",
        f"# {HEADER}",
        "# Install: source this file, or place it in /etc/bash_completion.d/",
        "",
        "_btrfs_backup_ng() {",
        "    local cur prev words cword",
        '    cur="${COMP_WORDS[COMP_CWORD]}"',
        '    prev="${COMP_WORDS[COMP_CWORD-1]}"',
        "",
        f'    local commands="{" ".join(sorted(tree["subcommands"]))}"',
        f'    local global_opts="{" ".join(_all_options(tree["flags"]))}"',
        "",
    ]

    # value completion for any flag that takes one
    value_cases: list[str] = []
    for cmd, node in sorted(tree["subcommands"].items()):
        contexts = [(cmd, node)] + [
            (f"{cmd} {s}", sn) for s, sn in sorted(node["subcommands"].items())
        ]
        for _, ctx in contexts:
            for f in ctx["flags"]:
                opts = " ".join(f["short"] + f["long"])
                if not f["takes_value"] or not opts:
                    continue
                if f["choices"]:
                    value_cases.append(
                        f"        {'|'.join(f['short'] + f['long'])})\n"
                        f'            COMPREPLY=($(compgen -W "{" ".join(f["choices"])}" -- "$cur")); return ;;'
                    )
                elif _is_path(f["metavar"]):
                    value_cases.append(
                        f"        {'|'.join(f['short'] + f['long'])})\n"
                        f'            COMPREPLY=($(compgen -f -- "$cur")); return ;;'
                    )

    seen: set[str] = set()
    deduped = []
    for case in value_cases:
        key = case.split(")")[0]
        if key not in seen:
            seen.add(key)
            deduped.append(case)

    lines.append('    case "$prev" in')
    lines.extend(deduped)
    lines.append("    esac")
    lines.append("")

    # per-command option sets
    lines.append('    local cmd="" sub=""')
    lines.append("    local i")
    lines.append("    for ((i=1; i<COMP_CWORD; i++)); do")
    lines.append('        case "${COMP_WORDS[i]}" in')
    lines.append("            -*) ;;")
    lines.append(
        '            *) if [ -z "$cmd" ]; then cmd="${COMP_WORDS[i]}";'
        ' elif [ -z "$sub" ]; then sub="${COMP_WORDS[i]}"; fi ;;'
    )
    lines.append("        esac")
    lines.append("    done")
    lines.append("")
    lines.append('    if [ -z "$cmd" ]; then')
    lines.append('        COMPREPLY=($(compgen -W "$commands $global_opts" -- "$cur"))')
    lines.append("        return")
    lines.append("    fi")
    lines.append("")
    lines.append('    case "$cmd" in')
    for cmd, node in sorted(tree["subcommands"].items()):
        own = " ".join(_all_options(node["flags"]))
        lines.append(f"        {cmd})")
        if node["subcommands"]:
            subs = " ".join(sorted(node["subcommands"]))
            lines.append('            case "$sub" in')
            for sub, snode in sorted(node["subcommands"].items()):
                sopts = " ".join(_all_options(snode["flags"]))
                lines.append(f"                {sub})")
                lines.append(
                    f'                    COMPREPLY=($(compgen -W "{sopts} {own} $global_opts" -- "$cur")) ;;'
                )
            lines.append("                *)")
            lines.append(
                f'                    COMPREPLY=($(compgen -W "{subs} {own} $global_opts" -- "$cur")) ;;'
            )
            lines.append("            esac ;;")
        else:
            lines.append(
                f'            COMPREPLY=($(compgen -W "{own} $global_opts" -- "$cur")) ;;'
            )
    lines.append("    esac")
    lines.append("}")
    lines.append("")
    lines.append("complete -F _btrfs_backup_ng btrfs-backup-ng")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------- zsh
def generate_zsh(tree: dict[str, Any]) -> str:
    lines = [
        "#compdef btrfs-backup-ng",
        f"# {HEADER}",
        "",
        "_btrfs_backup_ng() {",
        '    local curcontext="$curcontext" state line',
        "    typeset -A opt_args",
        "",
        "    _arguments -C \\",
    ]
    for f in tree["flags"]:
        for o in f["short"] + f["long"]:
            lines.append(f"        '{o}[{_q(f['help'])}]' \\")
    lines.append("        '1: :->command' \\")
    lines.append("        '*:: :->args' && return 0")
    lines.append("")
    lines.append("    case $state in")
    lines.append("        command)")
    lines.append("            local -a cmds")
    lines.append("            cmds=(")
    for cmd in sorted(tree["subcommands"]):
        lines.append(f"                '{cmd}'")
    lines.append("            )")
    lines.append("            _describe 'command' cmds")
    lines.append("            ;;")
    lines.append("        args)")
    lines.append("            case $words[1] in")
    for cmd, node in sorted(tree["subcommands"].items()):
        lines.append(f"                {cmd})")
        if node["subcommands"]:
            lines.append("                    if (( CURRENT == 2 )); then")
            lines.append("                        local -a subs")
            lines.append("                        subs=(")
            for sub in sorted(node["subcommands"]):
                lines.append(f"                            '{sub}'")
            lines.append("                        )")
            lines.append("                        _describe 'subcommand' subs")
            lines.append("                    else")
            lines.append("                        case $words[2] in")
            for sub, snode in sorted(node["subcommands"].items()):
                lines.append(f"                            {sub})")
                lines.append("                                _arguments \\")
                for f in snode["flags"] + node["flags"]:
                    for o in f["short"] + f["long"]:
                        spec = f"'{o}[{_q(f['help'])}]"
                        if f["takes_value"]:
                            vals = _values_for(f)
                            if vals:
                                spec += f":value:({' '.join(vals)})"
                            elif _is_path(f["metavar"]):
                                spec += ":file:_files"
                            else:
                                spec += f":{(f['metavar'] or 'value').lower()}:"
                        lines.append(f"                                    {spec}' \\")
                lines.append("                                    '*:: :->rest'")
                lines.append("                                ;;")
            lines.append("                        esac")
            lines.append("                    fi")
        else:
            lines.append("                    _arguments \\")
            for f in node["flags"]:
                for o in f["short"] + f["long"]:
                    spec = f"'{o}[{_q(f['help'])}]"
                    if f["takes_value"]:
                        vals = _values_for(f)
                        if vals:
                            spec += f":value:({' '.join(vals)})"
                        elif _is_path(f["metavar"]):
                            spec += ":file:_files"
                        else:
                            spec += f":{(f['metavar'] or 'value').lower()}:"
                    lines.append(f"                        {spec}' \\")
            lines.append("                        '*:: :->rest'")
        lines.append("                    ;;")
    lines.append("            esac")
    lines.append("            ;;")
    lines.append("    esac")
    lines.append("}")
    lines.append("")
    lines.append('_btrfs_backup_ng "$@"')
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------- fish
def generate_fish(tree: dict[str, Any]) -> str:
    lines = [
        "# fish completion for btrfs-backup-ng",
        f"# {HEADER}",
        "",
        "complete -c btrfs-backup-ng -f",
        "",
        "function __fish_btrfs_backup_ng_no_subcommand",
        "    set -l cmd (commandline -opc)",
        "    test (count $cmd) -eq 1",
        "end",
        "",
        "function __fish_btrfs_backup_ng_using_command",
        "    set -l cmd (commandline -opc)",
        '    test (count $cmd) -ge 2 -a "$cmd[2]" = "$argv[1]"',
        "end",
        "",
        "function __fish_btrfs_backup_ng_using_subcommand",
        "    set -l cmd (commandline -opc)",
        '    test (count $cmd) -ge 3 -a "$cmd[2]" = "$argv[1]" -a "$cmd[3]" = "$argv[2]"',
        "end",
        "",
    ]

    def flag_line(cond: str, f: dict[str, Any]) -> list[str]:
        out = []
        for long in f["long"]:
            parts = ["complete -c btrfs-backup-ng"]
            if cond:
                parts.append(f"-n '{cond}'")
            for s in f["short"]:
                parts.append(f"-s {s.lstrip('-')}")
            parts.append(f"-l {long.lstrip('-')}")
            if f["help"]:
                parts.append(f"-d '{_q(f['help'])}'")
            if f["takes_value"]:
                vals = _values_for(f)
                if vals:
                    parts.append(f"-x -a '{' '.join(vals)}'")
                elif _is_path(f["metavar"]):
                    parts.append("-r -F")
                else:
                    parts.append("-x")
            out.append(" ".join(parts))
        return out

    for f in tree["flags"]:
        lines.extend(flag_line("", f))
    lines.append("")

    for cmd, node in sorted(tree["subcommands"].items()):
        lines.append(
            f"complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_no_subcommand' "
            f"-a {cmd} -d '{_q(cmd)}'"
        )
    lines.append("")

    for cmd, node in sorted(tree["subcommands"].items()):
        for f in node["flags"]:
            lines.extend(flag_line(f"__fish_btrfs_backup_ng_using_command {cmd}", f))
        for sub, snode in sorted(node["subcommands"].items()):
            lines.append(
                f"complete -c btrfs-backup-ng -n '__fish_btrfs_backup_ng_using_command {cmd}' "
                f"-a {sub} -d '{_q(sub)}'"
            )
            for f in snode["flags"]:
                lines.extend(
                    flag_line(f"__fish_btrfs_backup_ng_using_subcommand {cmd} {sub}", f)
                )
        lines.append("")

    return "\n".join(lines) + "\n"


GENERATORS = {
    "bash": generate_bash,
    "zsh": generate_zsh,
    "fish": generate_fish,
}


def generate(shell: str) -> str:
    """Return the completion script for ``shell``, built from the real parser."""
    from .dispatcher import create_subcommand_parser

    if shell not in GENERATORS:
        raise ValueError(
            f"unknown shell {shell!r}; expected one of {sorted(GENERATORS)}"
        )
    return GENERATORS[shell](build_tree(create_subcommand_parser()))
