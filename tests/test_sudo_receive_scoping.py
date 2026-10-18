"""The compressed receive must not need permission to run a shell as root.

`sudo -S sh -c '<decompress> | btrfs receive'` asks sudoers for permission to
run `sh`. The sudoers recipe this project's own README gives grants only
/usr/bin/btrfs, so a host configured exactly as documented refused the backup:
"Sorry, user is not allowed to execute '/usr/sbin/sh -c ...'". Measured in a
container across three remote shells.

Reordering to `<decompress> | sudo -S btrfs receive` does NOT fix it: `sudo -S`
reads the password from its own stdin, so the decompressor would consume the
password line instead ("not in gzip format", measured) and break every
password-sudo transfer, not just restricted ones.
"""

import re

from btrfs_backup_ng.endpoint.ssh import _build_receive_command

DEST = "/backup/dest"


def _cmd(**kw):
    return _build_receive_command(DEST, **kw)


def test_the_scoped_path_elevates_btrfs_not_a_shell():
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert f"sudo -n btrfs receive {DEST}" in command, (
        "the preferred path does not scope sudo to the btrfs binary"
    )


def test_the_decompressor_never_precedes_the_password_reader():
    """The ordering trap: a decompressor in front of `sudo -S` eats the password."""
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert "gzip -dc | sudo -S btrfs" not in command


def test_the_password_is_read_before_anything_consumes_the_stream():
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert command.index('dd bs=1 count="$__bbng_n"') < command.index("gzip -dc")


def test_the_decision_is_made_inside_the_backgrounded_group():
    """The probe and both branches run in the group that `exec 3<&0` feeds, so
    the stream they read is the transfer and nothing primes a credential cache
    outside it: with no tty sudo keys its ticket on the parent process, and a
    prime in a pipeline stage was measured never to take effect."""
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert command.index("exec 3<&0") < command.index("sudo -n btrfs --version")
    assert "sudo -S -v" not in command


def test_the_capability_probe_cannot_consume_the_stream():
    """The probe runs inside the guarded group, so its stdin IS the transfer.
    A stub btrfs that read stdin regardless swallowed the whole payload and made
    the product look broken; the real one does not read it, but a probe sharing
    the payload's file description must not depend on that."""
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert "btrfs --version </dev/null" in command


def test_the_fallback_prefixes_the_decompressed_stream():
    """Where the credential cache is refused (timestamp_timeout=0) the
    decompressor runs unelevated and its OUTPUT carries the password line, so
    `sudo -S` eats that line and btrfs receive gets the decompressed stream."""
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert (
        '{ dd bs=1 count="$__bbng_n" 2>/dev/null; gzip -dc; } | sudo -S btrfs receive'
        in command
    ), "the fallback does not prefix the decompressed stream with the password"


def test_no_path_ever_elevates_a_shell():
    """The whole defect was asking sudoers for permission to run `sh`. Neither
    branch may do it: root runs exactly one known binary, btrfs."""
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert "sudo -S sh" not in command
    assert "sudo -n sh" not in command
    assert "sudo sh" not in command


def test_the_password_is_never_held_by_the_shell():
    """dd passes each copy of the password straight to sudo, so it can reach
    neither an argument list nor a `$(...)`, where Fedora's dash 0.5.13.1
    damaged non-ASCII bytes. The one value taken off stdin into the shell is
    the four-digit count."""
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert "__bbng_pw" not in command
    substitutions = re.findall(r"\$\((?!\()", command)  # not $(( arithmetic
    assert len(substitutions) == 1, substitutions
    assert "__bbng_n=$(dd bs=1 count=4 2>/dev/null)" in command


def test_a_passwordless_host_is_untouched():
    """The NOPASSWD path already scoped sudo correctly; it must not change."""
    command = _cmd(use_sudo=True, password_on_stdin=False, decompress="gzip")

    assert f"sudo -n btrfs receive {DEST}" in command
    assert "__bbng_pw" not in command
    assert "sudo -S" not in command


def test_an_unelevated_host_is_untouched():
    command = _cmd(use_sudo=False, password_on_stdin=False, decompress="gzip")

    assert "sudo" not in command


def test_the_uncompressed_path_is_untouched():
    """No decompressor means no pipeline, so none of this applies."""
    command = _cmd(use_sudo=True, password_on_stdin=True)

    assert f"sudo -S btrfs receive {DEST}" in command
    assert "__bbng_pw" not in command


def test_orphan_protection_survives():
    """The trap/kill group is what stops a dropped connection orphaning the
    receive; restructuring the command must not drop it."""
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert 'trap "trap - HUP INT TERM; kill 0" HUP INT TERM' in command
    assert 'wait "$pid"' in command
    assert "exec 3<&0" in command, "the stream is not redirected into the group"
