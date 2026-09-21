"""A remote (ssh://) SOURCE in legacy mode is refused with a reason, not with argparse noise.

The legacy help text used to describe an ``ssh://`` source form. It has never
been dispatched: the dispatcher routes any first argument containing ``://``
to the subcommand parser, which rejected it as "invalid choice" and listed
every subcommand -- a message that says nothing about why. Taking a snapshot
on a remote host is not implemented (#108). Until it is, the invocation is
refused in plain words, and the help no longer promises it.
"""

from __future__ import annotations

import subprocess
import sys

from btrfs_backup_ng.cli.dispatcher import main


def test_an_ssh_source_is_refused_with_the_reason(capsys):
    rc = main(["ssh://user@host/home", "/mnt/backup"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "remote source" in err
    assert "not supported" in err
    assert "invalid choice" not in err


def test_the_refusal_names_where_ssh_is_accepted(capsys):
    main(["ssh://host/home", "/mnt/backup"])
    err = capsys.readouterr().err
    assert "accepted for destinations" in err


def test_the_legacy_help_no_longer_promises_a_remote_source():
    out = subprocess.run(
        [sys.executable, "-m", "btrfs_backup_ng", "/src", "--help"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    assert "Subvolume to back up" in out
    assert "ssh://[user@]host[:port]/path" not in out.split("destinations")[0]
