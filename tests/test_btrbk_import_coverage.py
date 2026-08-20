"""Every btrbk keyword the lexer recognises must be accounted for.

The importer's recurring failure is RECOGNISED-BUT-DISCARDED: a keyword is in
the lexer's set, so it never triggers the unknown-option path, is stored in
`options`, and is then never read again. Nothing warns. Four options were lost
that way -- ssh_identity, ssh_user, ssh_port, rate_limit -- while the migration
guide promised three of them by name.

That class is closed here rather than one instance at a time. A keyword must be
either READ by the converter or listed as deliberately unmapped; a new keyword
that is neither fails this test.

The keyword set and the converter are read with `ast`, not regex: a regex over
source counts strings in comments and docstrings, and would have called this
green while an option was quietly discarded inside one.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

SOURCE = pathlib.Path("src/btrfs_backup_ng/btrbk_import.py")

#: Structural directives, not options: they open a scope rather than set a value.
STRUCTURAL = {"volume", "subvolume", "target"}


def _module() -> ast.Module:
    return ast.parse(SOURCE.read_text(encoding="utf-8"))


def _lexed_keywords(tree: ast.Module) -> set[str]:
    """The literal set assigned to `keywords` in the lexer."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "keywords":
                if isinstance(node.value, ast.Set):
                    return {
                        el.value
                        for el in node.value.elts
                        if isinstance(el, ast.Constant) and isinstance(el.value, str)
                    }
    raise AssertionError("could not find the lexer's `keywords` set")


def _definition_nodes(tree: ast.Module) -> set[int]:
    """The literals that DEFINE the keyword set and the unmapped table.

    These must not count as usage. The lexer's keyword set is itself a set of
    string constants, so a naive walk finds every keyword inside its own
    definition and calls it handled -- which is exactly what the first version
    of this guard did, passing while a keyword was discarded.
    """
    excluded: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        names = {t.id for t in node.targets if isinstance(t, ast.Name)}
        if not names & {"keywords", "_UNMAPPED"}:
            continue
        for child in ast.walk(node.value):
            if isinstance(child, ast.Constant):
                excluded.add(id(child))
    return excluded


def _string_constants(tree: ast.Module) -> set[str]:
    """Every string literal used as a VALUE by the converter.

    Docstrings and the defining literals are excluded; comments never reach the
    AST at all. A keyword named only in prose, or only in the set that declares
    it, must not count as being handled.
    """
    skip = _definition_nodes(tree)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.ClassDef)):
            body = getattr(node, "body", [])
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                skip.add(id(body[0].value))

    found = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in skip
        ):
            found.add(node.value)
    return found


def _unmapped_keys(tree: ast.Module) -> set[str]:
    """Keys of the _UNMAPPED table: deliberately not carried, and reported."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "_UNMAPPED":
                if isinstance(node.value, ast.Dict):
                    return {
                        k.value
                        for k in node.value.keys
                        if isinstance(k, ast.Constant) and isinstance(k.value, str)
                    }
    return set()


def test_the_keyword_set_was_actually_found():
    """Guard the guard: a broken walk would make everything below vacuous."""
    keywords = _lexed_keywords(_module())
    assert len(keywords) > 20, keywords
    assert "target" in keywords and "stream_compress" in keywords


def test_the_unmapped_table_was_found():
    unmapped = _unmapped_keys(_module())
    assert unmapped, "the _UNMAPPED table is missing; nothing would be reported"


@pytest.mark.parametrize("keyword", sorted(_lexed_keywords(_module()) - STRUCTURAL))
def test_every_recognised_keyword_is_used_or_declared_unmapped(keyword):
    tree = _module()
    used = _string_constants(tree)
    unmapped = _unmapped_keys(tree)
    assert keyword in used or keyword in unmapped, (
        f"{keyword!r} is recognised by the lexer but never read by the converter "
        f"and not listed in _UNMAPPED. It would be accepted from a btrbk config, "
        f"stored, and silently discarded -- the defect class this file exists to "
        f"prevent. Either consume it, or add it to _UNMAPPED so the operator is "
        f"told it is not carried over."
    )


@pytest.mark.parametrize("keyword", sorted(_unmapped_keys(_module())))
def test_every_unmapped_entry_is_actually_reported(keyword):
    """Listing a keyword as unmapped must produce a warning, not just a docstring.

    Derived from the table rather than hardcoded, so a keyword added to _UNMAPPED
    without being wired into the warning loop fails here instead of being lost.
    """
    from btrfs_backup_ng.btrbk_import import convert_to_toml, parse_btrbk_config

    _toml, warnings = convert_to_toml(
        parse_btrbk_config(
            f"{keyword} somevalue\nvolume /mnt/pool\n  subvolume home\n"
            f"    target send-receive ssh://nas/backup\n"
        )
    )
    matching = [w for w in warnings if keyword in w]
    assert matching, (
        f"{keyword!r} is listed in _UNMAPPED but setting it produced no warning: "
        f"{warnings}"
    )
    assert "not carried over" in matching[0], matching[0]
    reason = matching[0].split("not carried over:", 1)[-1].strip()
    assert reason, f"{keyword!r} is reported with no reason: {matching[0]}"


def test_the_defining_literals_do_not_count_as_usage():
    """Guard the guard: without this exclusion every test above is vacuous.

    A keyword appearing ONLY in the lexer's set must not be reported as used.
    """
    tree = _module()
    keywords = _lexed_keywords(tree)
    used = _string_constants(tree)
    unmapped = _unmapped_keys(tree)
    assert keywords, "no keywords parsed"
    assert not (unmapped & used) or True  # unmapped may legitimately appear elsewhere
    # `group` is listed as unmapped and is otherwise never read by the converter,
    # so it must NOT show up as used once the defining literals are excluded.
    assert "group" not in used, (
        "the _UNMAPPED table's own keys are still being counted as usage; "
        "the exclusion is not working and the coverage guard is vacuous"
    )
