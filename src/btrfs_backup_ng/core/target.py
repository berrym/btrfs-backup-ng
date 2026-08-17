"""One authority on what a target path is, and what it supports.

Sixteen places decided "is this remote?" by writing ``path.startswith("ssh://")``.
``raw+ssh://`` matches none of them, so every one of those sites silently treated a
remote raw target as a local path. That produced, among others:

* ``core/doctor.py`` reporting "Target path does not exist" for a perfectly good
  ``raw+ssh://`` target, because it fell through to a local ``Path.exists()``;
* ``cli/restore.py`` dropping every ``--ssh-*`` option for ``raw+ssh://`` sources,
  so the remote listing ran unprivileged and reported "No snapshots found" with
  exit 0.

Scheme detection is therefore not a formatting detail -- it is where several
user-visible bugs live. This module is the single place that decides, and the
capability flags exist so that replacing a call site *fixes* its bug rather than
faithfully preserving it.

The classification mirrors ``endpoint.choose_endpoint``, which is the runtime
authority on which endpoint a path produces; :func:`parse_target` must not
disagree with it. ``tests/test_target_scheme.py`` pins them together.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

__all__ = ["TargetKind", "TargetScheme", "parse_target"]

_SSH_PREFIX = "ssh://"
_RAW_PREFIX = "raw://"
_RAW_SSH_PREFIX = "raw+ssh://"
_SHELL_PREFIX = "shell://"


class TargetKind(str, Enum):
    """What a target path resolves to.

    ``UNSUPPORTED`` is deliberately distinct from ``LOCAL``: a bare
    ``user@host:/path`` looks like an SSH destination and is even detected as one
    by ``endpoint.choose_endpoint``'s internal helper, but constructing an
    endpoint from it raises ValueError. Classifying it as local would send it
    down a filesystem path that silently does the wrong thing -- exactly the
    failure mode this module exists to end.
    """

    LOCAL = "local"
    SSH = "ssh"
    RAW = "raw"
    RAW_SSH = "raw+ssh"

    SHELL = "shell"
    """Legacy ``shell://cmd``. Recognised, but NOT currently constructible.

    ``endpoint.choose_endpoint`` sets ``config["source"] = True`` unconditionally
    for a shell spec, and ``ShellEndpoint.__init__`` raises "Shell can't be used
    as source" whenever that is truthy -- so every route through choose_endpoint
    raises ValueError, with or without ``source=True``. Kept as a distinct kind
    so the scheme is named rather than mistaken for a local path; the test suite
    pins the ValueError, so repairing choose_endpoint will fail that test and
    force this note to be updated.
    """

    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class TargetScheme:
    """A classified target path and the capabilities that follow from it."""

    uri: str
    kind: TargetKind
    path: str
    """The filesystem path at the destination, without scheme or host.

    Empty for SHELL (which carries a command) and UNSUPPORTED.
    """

    reason: str = ""
    """Why an UNSUPPORTED path was rejected. Empty otherwise."""

    user: str | None = None
    """Username from the authority, if the URI carried one."""

    host: str | None = None
    """Hostname alone, without the user, port, or IPv6 brackets.

    Stored unbracketed because that is what ``ssh`` accepts as a destination:
    ``ssh '[::1]' echo hi`` fails with "Could not resolve hostname [::1]", while
    ``ssh '::1' echo hi`` connects. Brackets exist in a URI only to delimit the
    literal from a ``:port`` suffix, and that job is done once parsing is.
    """

    port: int | None = None
    """Explicit port from ``ssh://host:2222/path``. None when unspecified.

    A colon that is only a separator -- ``ssh://host:/path``, the form the README
    and the shipped examples use -- is not a port and must not be read as one.
    """

    # -- capabilities ------------------------------------------------------ #

    @property
    def is_remote(self) -> bool:
        """Reached over SSH, so a local filesystem call cannot inspect it.

        The predicate the sixteen ``startswith("ssh://")`` sites were reaching
        for. ``raw+ssh://`` is remote; ``raw://`` is not.
        """
        return self.kind in (TargetKind.SSH, TargetKind.RAW_SSH)

    @property
    def is_raw(self) -> bool:
        """Stores btrfs send streams as files rather than receiving subvolumes."""
        return self.kind in (TargetKind.RAW, TargetKind.RAW_SSH)

    @property
    def needs_ssh_options(self) -> bool:
        """Whether ssh_sudo / ssh_port / host-key policy must be threaded.

        ``cli/restore.py`` gated this on ``startswith("ssh://")``, so a
        ``raw+ssh://`` source got none of them and its remote listing ran
        unprivileged.

        NOT sufficient on its own for the identity file -- the two remote
        endpoints read it under different config keys. Use
        :attr:`ssh_identity_config_key`, or a caller will thread a key the
        endpoint ignores and authentication will fail with no explanation.
        """
        return self.is_remote

    @property
    def ssh_destination(self) -> str | None:
        """What to hand ``ssh`` as its destination: ``user@host`` or ``host``.

        Exists because callers were passing the whole URI where a host was
        expected. ``core/doctor.py`` called
        ``test_ssh_connection("ssh://user@host:/backups")``, which runs
        ``ssh ssh://user@host:/backups echo ...`` and always fails -- so a
        reachable target was reported unreachable. Measured: the full URI returns
        False, ``user@host`` returns True.

        None for targets that are not reached over SSH.
        """
        if not self.is_remote or not self.host:
            return None
        return f"{self.user}@{self.host}" if self.user else self.host

    @property
    def ssh_identity_config_key(self) -> str | None:
        """The config key this target's endpoint reads the SSH identity file from.

        ``SSHEndpoint`` accepts either name, but ``SSHRawEndpoint`` reads only
        ``ssh_key`` and ignores ``ssh_identity_file`` -- measured, not assumed.
        So a site that threads one name for every remote target silently drops
        ``--ssh-key`` for ``raw+ssh://``: the connection is attempted with no
        ``-i`` flag at all.

        ``cli/verify.py`` already gets this right with separate branches per
        scheme, and ``cli/common.py`` threads both names. Neither should be
        "simplified" into a single ``needs_ssh_options`` check.

        None for targets that take no SSH options.
        """
        if self.kind is TargetKind.RAW_SSH:
            return "ssh_key"
        if self.kind is TargetKind.SSH:
            return "ssh_identity_file"
        return None

    @property
    def supports_mount_check(self) -> bool:
        """Whether ``require_mount`` is meaningful.

        README documents it as applying to local targets -- which includes
        ``raw://`` on an external drive, the case it exists for. ``cli/run.py``
        excluded raw entirely, so an unmounted USB target was recreated on the
        root filesystem and the source pruned as though the backup succeeded.
        """
        return self.kind in (TargetKind.LOCAL, TargetKind.RAW)

    @property
    def supports_compress(self) -> bool:
        """Whether ``compress`` does anything.

        Verified behaviour of one documented option, three ways: raw targets
        compress correctly (the compressor is part of the stream written to the
        file); ``ssh://`` silently discards the setting; a local btrfs target
        feeds compressed bytes to ``btrfs receive`` and the transfer fails.
        Only raw targets support it, so the other two should be rejected at
        config load rather than surprising someone at 3am.
        """
        return self.is_raw


def parse_target(uri: str | None) -> TargetScheme:
    """Classify a target path. Never raises; UNSUPPORTED carries the reason.

    Callers get a verdict rather than an exception because most of them are
    diagnostics or validation, and a scheme error should be reported in their own
    terms, not unwound through a traceback.
    """
    if uri is None:
        return TargetScheme("", TargetKind.UNSUPPORTED, "", "target path is not set")

    # Deliberately NOT stripped. choose_endpoint does not strip, so stripping
    # here would classify a padded path differently from the endpoint actually
    # built from it: ' ssh://user@host:/mnt/usb' would be judged remote (and so
    # exempt from the require_mount gate) while choose_endpoint builds a LOCAL
    # endpoint writing under the working directory. Normalisation belongs to the
    # producer -- config/loader.py strips when it reads the config -- so that one
    # string reaches every consumer. A padded path that reaches here anyway is
    # classified as written, which fails closed.
    text = str(uri)
    if not text.strip():
        return TargetScheme(text, TargetKind.UNSUPPORTED, "", "target path is empty")

    # Order matters: raw+ssh:// must be tested before raw://, or it is misread as
    # a local raw target whose path begins with "ssh://".
    if text.startswith(_RAW_SSH_PREFIX):
        user, host, port, path = _parse_remote(text, _RAW_SSH_PREFIX)
        return TargetScheme(
            text, TargetKind.RAW_SSH, path, user=user, host=host, port=port
        )
    if text.startswith(_RAW_PREFIX):
        remainder = text[len(_RAW_PREFIX) :]
        if not remainder.startswith("/"):
            # `raw://mnt/nas/x` is a footgun, not a shorthand. choose_endpoint
            # rewrites it to `file://mnt/nas/x`, so urlparse takes `mnt` as the
            # NETLOC and the endpoint silently writes to /nas/x -- one path
            # component short of what the operator wrote. cli/raw_cmd.py reads it
            # a third way, as the relative path `mnt/nas/x`. Rather than encode
            # one of three disagreeing readings, refuse the form and say how to
            # write it unambiguously.
            return TargetScheme(
                text,
                TargetKind.UNSUPPORTED,
                "",
                "raw:// needs an absolute path: write raw:///mnt/... with three slashes",
            )
        return TargetScheme(text, TargetKind.RAW, remainder)
    if text.startswith(_SSH_PREFIX):
        user, host, port, path = _parse_remote(text, _SSH_PREFIX)
        return TargetScheme(text, TargetKind.SSH, path, user=user, host=host, port=port)
    if text.startswith(_SHELL_PREFIX):
        return TargetScheme(text, TargetKind.SHELL, "")

    if text.startswith("/"):
        return TargetScheme(text, TargetKind.LOCAL, text)

    # Anything else with a colon before the first slash looks like the bare
    # user@host:/path form. choose_endpoint detects that shape but then fails to
    # build an endpoint from it, so name it rather than letting it masquerade as
    # a relative local path.
    head = text.split("/", 1)[0]
    if ":" in head and ("@" in head or "." in head.split(":", 1)[0]):
        return TargetScheme(
            text,
            TargetKind.UNSUPPORTED,
            "",
            "bare user@host:/path is not a supported target; use ssh://user@host:/path",
        )

    # A relative path. Legal, if unusual, for a local destination.
    return TargetScheme(text, TargetKind.LOCAL, text)


def _local_path(uri: str, prefix: str) -> str:
    """Strip a local scheme prefix. ``raw:///mnt/x`` and ``raw://mnt/x`` both work."""
    remainder = uri[len(prefix) :]
    return remainder if remainder.startswith("/") else "/" + remainder


def _parse_remote(
    uri: str, prefix: str
) -> tuple[str | None, str | None, int | None, str]:
    """Split ``[user@]host[:port]/path`` into (user, host, port, path).

    Both ``ssh://host:/path`` and ``ssh://host/path`` are accepted; the colon
    form is what the README and the shipped examples use. A trailing colon is a
    separator, not an empty port, and a numeric segment after the host is a port.
    IPv6 literals are bracketed, so the brackets delimit the host and any colon
    after ``]`` is the port.
    """
    remainder = uri[len(prefix) :]
    if not remainder:
        return None, None, None, ""

    slash = remainder.find("/")
    if slash == -1:
        authority, path = remainder, ""
    else:
        authority, path = remainder[:slash], remainder[slash:]

    user: str | None = None
    if "@" in authority:
        user, _, authority = authority.partition("@")
        user = user or None

    port: int | None = None
    if authority.endswith("]"):
        # Bracketed IPv6 with no port.
        host: str | None = authority
    elif "]" in authority:
        # Bracketed IPv6 followed by ':' -- either ':port' or the bare separator.
        head, _, tail = authority.rpartition(":")
        if tail == "":
            host = head  # `[::1]:` -- separator, not a port
        elif tail.isdigit():
            host, port = head, int(tail)
        else:
            host = authority
    elif ":" in authority:
        head, _, tail = authority.rpartition(":")
        if tail == "":
            host = head  # `host:` -- the separator form, no port
        elif tail.isdigit():
            host, port = head, int(tail)
        else:
            host = authority  # not a port; leave the authority intact
    else:
        host = authority

    # Drop the delimiters now that they have done their job; ssh cannot resolve
    # a bracketed literal.
    if host and host.startswith("[") and host.endswith("]"):
        host = host[1:-1]

    return user, (host or None), port, path
