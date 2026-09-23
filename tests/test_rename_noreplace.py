"""A rename that cannot replace anything.

``os.rename`` and ``os.replace`` replace an EMPTY directory at the target on
Linux, so "check the target is absent, then rename" has a window in which a
directory someone else just made -- snapper creating ``.snapshots/N`` a
moment before it fills it -- is silently taken over. ``rename_noreplace``
moves the check into the kernel (``renameat2`` with ``RENAME_NOREPLACE``) and
raises ``FileExistsError`` for anything at the target, whatever it is.
"""

from __future__ import annotations

import os

import pytest

from btrfs_backup_ng import __util__


def test_renames_onto_an_absent_target(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "f").write_text("x")
    __util__.rename_noreplace(tmp_path / "a", tmp_path / "b")
    assert not (tmp_path / "a").exists()
    assert (tmp_path / "b" / "f").read_text() == "x"


def test_refuses_an_existing_empty_directory_where_os_rename_would_replace_it(
    tmp_path,
):
    """The case the whole primitive exists for: the target is an empty
    directory, exactly what ``os.rename`` takes over."""
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    with pytest.raises(FileExistsError):
        __util__.rename_noreplace(tmp_path / "a", tmp_path / "b")
    assert (tmp_path / "a").is_dir() and (tmp_path / "b").is_dir()
    # os.rename would have replaced it, which is what makes the check above
    # meaningful rather than a restatement of ENOTEMPTY.
    os.rename(tmp_path / "a", tmp_path / "b")
    assert not (tmp_path / "a").exists()


def test_refuses_an_existing_file_and_a_populated_directory(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "file").write_text("keep")
    (tmp_path / "full").mkdir()
    (tmp_path / "full" / "content").write_text("keep")
    for target in ("file", "full"):
        with pytest.raises(FileExistsError):
            __util__.rename_noreplace(tmp_path / "a", tmp_path / target)
    assert (tmp_path / "file").read_text() == "keep"
    assert (tmp_path / "full" / "content").read_text() == "keep"
    assert (tmp_path / "a").is_dir()


def test_a_missing_source_is_an_ordinary_oserror(tmp_path):
    with pytest.raises(FileNotFoundError):
        __util__.rename_noreplace(tmp_path / "nothing", tmp_path / "b")
