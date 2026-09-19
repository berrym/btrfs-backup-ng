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

    assert command.index("read -r __bbng_pw") < command.index("gzip -dc")


def test_priming_happens_inside_the_backgrounded_group():
    """With no tty, sudo keys its credential ticket on the PARENT PID. The
    transfer runs in a backgrounded subshell, so priming outside that subshell
    records a ticket its `sudo -n` cannot see -- measured as "a password is
    required" on bash and dash, payload never delivered."""
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert command.index("exec 3<&0") < command.index("sudo -S -v"), (
        "credential priming runs before the subshell is created, so the ticket "
        "is recorded against the wrong parent pid"
    )


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

    assert '{ printf "%s\\n" "$__bbng_pw"; gzip -dc; } | sudo -S btrfs receive' in (
        command
    ), "the fallback does not prefix the decompressed stream with the password"


def test_no_path_ever_elevates_a_shell():
    """The whole defect was asking sudoers for permission to run `sh`. Neither
    branch may do it: root runs exactly one known binary, btrfs."""
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert "sudo -S sh" not in command
    assert "sudo -n sh" not in command
    assert "sudo sh" not in command


def test_the_password_never_reaches_a_process_argument_list():
    """printf is a builtin in dash, bash and busybox ash (checked in all three),
    so the value stays out of `ps`. `echo` would also mangle a backslash."""
    command = _cmd(use_sudo=True, password_on_stdin=True, decompress="gzip")

    assert 'printf "%s\\n" "$__bbng_pw"' in command
    assert "echo $__bbng_pw" not in command
    assert 'echo "$__bbng_pw"' not in command


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
