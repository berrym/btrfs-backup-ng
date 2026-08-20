"""Target classification, and the capabilities that follow from it.

Sixteen sites decided "is this remote?" with ``path.startswith("ssh://")``.
``raw+ssh://`` matches none of them, which is why ``doctor`` reported a false
"Target path does not exist" for a working remote raw target and why ``restore``
dropped every ``--ssh-*`` option for ``raw+ssh://`` sources.

Two properties are pinned here:

1. Every accepted target form classifies correctly, including the one the naive
   check got wrong.
2. :func:`parse_target` agrees with ``endpoint.choose_endpoint``, which is the
   runtime authority on which endpoint a path actually produces. A predicate that
   disagrees with the thing it describes is worse than no predicate, so that
   agreement is asserted directly rather than assumed.
"""

from __future__ import annotations

import pytest

from btrfs_backup_ng.core.target import TargetKind, parse_target

# (uri, kind, remote, raw, mount_check, compress)
FORMS = [
    pytest.param(
        "/mnt/backup", TargetKind.LOCAL, False, False, True, False, id="local-abs"
    ),
    pytest.param(
        "/mnt/my backup", TargetKind.LOCAL, False, False, True, False, id="local-space"
    ),
    pytest.param(
        "backups/home", TargetKind.LOCAL, False, False, True, False, id="local-relative"
    ),
    pytest.param(
        "ssh://host:/backups", TargetKind.SSH, True, False, False, True, id="ssh-colon"
    ),
    pytest.param(
        "ssh://user@host:/backups",
        TargetKind.SSH,
        True,
        False,
        False,
        True,
        id="ssh-user",
    ),
    pytest.param(
        "ssh://user@host/backups",
        TargetKind.SSH,
        True,
        False,
        False,
        True,
        id="ssh-no-colon",
    ),
    pytest.param(
        "ssh://user@host:2222/backups",
        TargetKind.SSH,
        True,
        False,
        False,
        True,
        id="ssh-port",
    ),
    pytest.param(
        "raw:///mnt/nas/backups",
        TargetKind.RAW,
        False,
        True,
        True,
        True,
        id="raw-triple-slash",
    ),
    pytest.param(
        "raw+ssh://user@host/backups",
        TargetKind.RAW_SSH,
        True,
        True,
        False,
        True,
        id="rawssh",
    ),
    pytest.param(
        "raw+ssh://user@host:/backups",
        TargetKind.RAW_SSH,
        True,
        True,
        False,
        True,
        id="rawssh-colon",
    ),
    pytest.param(
        "shell://cat > file", TargetKind.SHELL, False, False, False, False, id="shell"
    ),
]


@pytest.mark.parametrize(
    ("uri", "kind", "remote", "raw", "mount_check", "compress"), FORMS
)
def test_classification_and_capabilities(uri, kind, remote, raw, mount_check, compress):
    t = parse_target(uri)
    assert t.kind is kind, f"{uri!r} classified as {t.kind}"
    assert t.is_remote is remote, f"{uri!r} is_remote"
    assert t.is_raw is raw, f"{uri!r} is_raw"
    assert t.supports_mount_check is mount_check, f"{uri!r} supports_mount_check"
    assert t.supports_compress is compress, f"{uri!r} supports_compress"
    # needs_ssh_options is the predicate cli/restore.py got wrong; it tracks remoteness.
    assert t.needs_ssh_options is remote


class TestTheBugThisModuleExistsFor:
    """raw+ssh:// is remote. The naive check said it wasn't."""

    def test_rawssh_is_remote_unlike_the_naive_check(self):
        uri = "raw+ssh://user@host/backups"
        assert uri.startswith("ssh://") is False, "premise of the bug"
        assert parse_target(uri).is_remote is True

    def test_rawssh_needs_ssh_options(self):
        """cli/restore.py:455 gated --ssh-* on startswith('ssh://')."""
        assert parse_target("raw+ssh://user@host/backups").needs_ssh_options is True

    def test_rawssh_is_not_inspectable_with_a_local_path_call(self):
        """core/doctor.py:641 fell through to Path.exists() and reported a false error."""
        assert parse_target("raw+ssh://user@host/backups").is_remote is True

    def test_local_raw_supports_the_mount_check(self):
        """cli/run.py excluded raw entirely; README:698 scopes require_mount to local."""
        assert parse_target("raw:///mnt/usb/backups").supports_mount_check is True
        assert parse_target("raw+ssh://host/backups").supports_mount_check is False

    def test_compress_is_supported_wherever_an_endpoint_implements_it(self):
        """One documented option, three verified behaviours.

        raw compresses at rest; ssh:// compresses over the wire and decompresses
        in the remote command; a LOCAL btrfs destination would compress only to
        decompress on the same machine, so core.operations drops it.

        ssh:// answered False here for as long as nothing decompressed on the
        remote. It does now, and this is the property that says so.
        """
        assert parse_target("raw:///mnt/x").supports_compress is True
        assert parse_target("raw+ssh://host/x").supports_compress is True
        assert parse_target("ssh://host:/x").supports_compress is True
        assert parse_target("/mnt/x").supports_compress is False


class TestUnsupportedIsNamed:
    """A form that cannot work must not masquerade as a local path."""

    @pytest.mark.parametrize("uri", ["user@host:/backups", "host.example.com:/backups"])
    def test_bare_ssh_pattern_is_rejected_with_a_reason(self, uri):
        t = parse_target(uri)
        assert t.kind is TargetKind.UNSUPPORTED, f"{uri!r} -> {t.kind}"
        assert "ssh://" in t.reason, t.reason
        assert t.is_remote is False and t.supports_mount_check is False

    @pytest.mark.parametrize("uri", [None, "", "   "])
    def test_missing_path_is_named(self, uri):
        t = parse_target(uri)
        assert t.kind is TargetKind.UNSUPPORTED
        assert t.reason


class TestPathExtraction:
    @pytest.mark.parametrize(
        ("uri", "expected"),
        [
            ("/mnt/backup", "/mnt/backup"),
            ("raw:///mnt/nas/x", "/mnt/nas/x"),
            ("ssh://host:/backups/home", "/backups/home"),
            ("ssh://user@host/backups/home", "/backups/home"),
            ("ssh://user@host:2222/backups", "/backups"),
            ("raw+ssh://user@host/mnt/nas", "/mnt/nas"),
            ("raw+ssh://user@host:/mnt/nas", "/mnt/nas"),
        ],
    )
    def test_destination_path_is_extracted(self, uri, expected):
        assert parse_target(uri).path == expected


class TestAgreesWithChooseEndpoint:
    """The parser must not disagree with the runtime authority.

    ``choose_endpoint`` decides which endpoint a path really produces. If this
    module's verdict diverges, callers that trust it will route work to the wrong
    place -- which is precisely the class of bug it was written to end.
    """

    @pytest.mark.parametrize(
        ("uri", "expect_endpoint_substring"),
        [
            ("/mnt/backup", "LocalEndpoint"),
            ("ssh://user@host:/backups", "SSHEndpoint"),
            ("raw:///mnt/nas/backups", "RawEndpoint"),
            ("raw+ssh://user@host/backups", "SSHRawEndpoint"),
        ],
    )
    def test_kind_matches_the_endpoint_actually_built(
        self, uri, expect_endpoint_substring, tmp_path, monkeypatch
    ):
        from btrfs_backup_ng.endpoint import choose_endpoint

        monkeypatch.setenv("HOME", str(tmp_path))
        endpoint = choose_endpoint(uri, {"path": uri, "snap_prefix": ""})
        built = type(endpoint).__name__
        assert built == expect_endpoint_substring, f"{uri} built {built}"

        scheme = parse_target(uri)
        # The destination path must agree too. Comparing only endpoint CLASSES is
        # what let raw://<relative> diverge unnoticed: the parser said /mnt/nas/x
        # while the endpoint wrote to /nas/x.
        assert scheme.path == str(endpoint.config["path"]), (
            f"{uri}: parse_target path={scheme.path!r} but the endpoint uses "
            f"{str(endpoint.config['path'])!r}"
        )
        # Remoteness must agree with whether an SSH-capable endpoint was built.
        assert scheme.is_remote is ("SSH" in built), (
            f"{uri}: parse_target says is_remote={scheme.is_remote} but "
            f"choose_endpoint built {built}"
        )
        # Rawness must agree with whether a raw endpoint was built.
        assert scheme.is_raw is ("Raw" in built), (
            f"{uri}: parse_target says is_raw={scheme.is_raw} but "
            f"choose_endpoint built {built}"
        )

    def test_bare_form_that_choose_endpoint_rejects_is_unsupported_here(
        self, tmp_path, monkeypatch
    ):
        """parse_target must not call a form local that choose_endpoint refuses."""
        from btrfs_backup_ng.endpoint import choose_endpoint

        monkeypatch.setenv("HOME", str(tmp_path))
        uri = "user@host:/backups"
        with pytest.raises(ValueError):
            choose_endpoint(uri, {"path": uri, "snap_prefix": ""})
        assert parse_target(uri).kind is TargetKind.UNSUPPORTED


class TestFormsTheRuntimeCannotBuild:
    """Named, not blessed: a scheme that cannot construct must not look supported."""

    @pytest.mark.parametrize("uri", ["raw://mnt/nas/x", "raw://backup/data"])
    def test_raw_with_a_relative_remainder_is_refused(self, uri):
        """choose_endpoint silently eats the first path component of this form.

        `raw://mnt/nas/x` becomes `file://mnt/nas/x`, so urlparse treats `mnt` as
        the netloc and the endpoint writes to /nas/x. cli/raw_cmd.py reads it a
        third way. Three disagreeing readings means the form must be refused, not
        arbitrated.
        """
        t = parse_target(uri)
        assert t.kind is TargetKind.UNSUPPORTED, f"{uri} -> {t.kind}"
        assert "three slashes" in t.reason, t.reason

    def test_shell_is_recognised_but_not_constructible(self, tmp_path, monkeypatch):
        """Pins reality: every route through choose_endpoint raises for shell://.

        If choose_endpoint is ever repaired, this test fails and forces the
        TargetKind.SHELL docstring to be corrected rather than left stale.
        """
        from btrfs_backup_ng.endpoint import choose_endpoint

        monkeypatch.setenv("HOME", str(tmp_path))
        assert parse_target("shell://cat > f").kind is TargetKind.SHELL
        for kwargs in ({}, {"source": True}):
            with pytest.raises(ValueError, match="Shell can't be used as source"):
                choose_endpoint(
                    "shell://cat > f", {"path": "x", "snap_prefix": ""}, **kwargs
                )


class TestIdentityFileKeyPerScheme:
    """The two remote endpoints read the identity file under DIFFERENT keys."""

    def test_ssh_and_rawssh_disagree(self):
        assert (
            parse_target("ssh://u@h:/b").ssh_identity_config_key == "ssh_identity_file"
        )
        assert parse_target("raw+ssh://u@h/b").ssh_identity_config_key == "ssh_key"

    def test_local_targets_take_no_identity(self):
        assert parse_target("/mnt/x").ssh_identity_config_key is None
        assert parse_target("raw:///mnt/x").ssh_identity_config_key is None

    def test_the_key_each_scheme_names_is_the_one_its_endpoint_reads(
        self, tmp_path, monkeypatch
    ):
        """Measured against the real endpoints, because assuming this is the bug.

        Threading `ssh_identity_file` at a raw+ssh target yields an ssh command
        with no -i flag at all, and nothing reports it.
        """
        from btrfs_backup_ng.endpoint import choose_endpoint

        monkeypatch.setenv("HOME", str(tmp_path))
        for uri in ("ssh://u@h:/b", "raw+ssh://u@h/b"):
            key = parse_target(uri).ssh_identity_config_key
            assert key is not None
            ep = choose_endpoint(
                uri,
                {"path": uri, "snap_prefix": "", "ssh_sudo": True, key: "/key/path"},
            )
            seen = (
                getattr(ep, "ssh_key", None)
                or ep.config.get("ssh_identity_file")
                or ep.config.get("ssh_key")
            )
            assert seen == "/key/path", (
                f"{uri}: threading {key!r} did not reach the endpoint (saw {seen!r})"
            )


class TestWhitespaceIsNotNormalisedHere:
    """parse_target must classify the string it is given, byte for byte.

    Stripping inside the predicate looks harmless and is not: choose_endpoint
    does not strip, so a padded path would be CLASSIFIED as one thing and BUILT
    as another. Measured before the fix: ``' ssh://user@host:/mnt/usb'`` was
    judged remote -- and therefore exempt from the require_mount gate -- while
    choose_endpoint built a LocalEndpoint writing under the working directory.
    Under systemd that directory is ``/``.

    Normalisation is the config loader's job (see TestConfigLoaderNormalises...
    in tests/test_config.py), so one string reaches every consumer.
    """

    @pytest.mark.parametrize(
        "padded",
        [
            " ssh://user@host:/mnt/usb",
            " raw+ssh://user@nas:/mnt/usb",
            " raw:///mnt/usb",
            "/mnt/usb ",
            "\t/mnt/usb\n",
        ],
    )
    def test_a_padded_path_is_not_silently_cleaned(self, padded):
        scheme = parse_target(padded)
        assert scheme.uri == padded
        # Padding makes it not a recognised scheme, so it is treated as a local
        # path exactly as choose_endpoint treats it. The point is agreement, not
        # any particular verdict.
        assert scheme.kind is TargetKind.LOCAL
        assert scheme.is_remote is False

    @pytest.mark.parametrize(
        "padded", [" ssh://user@host:/mnt/usb", "/mnt/usb ", " raw:///mnt/usb"]
    )
    def test_a_padded_path_agrees_with_choose_endpoint(
        self, padded, tmp_path, monkeypatch
    ):
        """The invariant this module claims: never disagree with the builder."""
        from btrfs_backup_ng.endpoint import choose_endpoint

        monkeypatch.setenv("HOME", str(tmp_path))
        scheme = parse_target(padded)
        endpoint = choose_endpoint(padded, {}, source=False)
        built_remote = "SSH" in type(endpoint).__name__
        assert scheme.is_remote == built_remote, (
            f"{padded!r}: parse_target says remote={scheme.is_remote} but "
            f"choose_endpoint built {type(endpoint).__name__}"
        )

    @pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
    def test_a_blank_path_is_still_unsupported(self, blank):
        """Refusing to strip must not make whitespace look like a real path."""
        scheme = parse_target(blank)
        assert scheme.kind is TargetKind.UNSUPPORTED
        assert scheme.path == ""


class TestIPv6LiteralsAreHandedToSshUnbracketed:
    """``ssh`` cannot resolve a bracketed literal.

    Measured: ``ssh '[::1]' echo hi`` fails with "Could not resolve hostname
    [::1]", while ``ssh '::1' echo hi`` connects. Brackets delimit the literal
    from a ``:port`` suffix inside a URI and have no meaning past parsing, so
    carrying them into ssh_destination made doctor report a working IPv6 target
    unreachable.
    """

    @pytest.mark.parametrize(
        ("uri", "host", "port", "destination"),
        [
            ("ssh://[::1]:/tmp", "::1", None, "::1"),
            ("raw+ssh://[::1]:/tmp", "::1", None, "::1"),
            ("ssh://[2001:db8::5]:2222/b", "2001:db8::5", 2222, "2001:db8::5"),
            ("ssh://user@[::1]/b", "::1", None, "user@::1"),
            (
                "raw+ssh://user@[2001:db8::5]:2222/b",
                "2001:db8::5",
                2222,
                "user@2001:db8::5",
            ),
        ],
    )
    def test_brackets_are_stripped_but_the_port_still_parses(
        self, uri, host, port, destination
    ):
        scheme = parse_target(uri)
        assert scheme.host == host
        assert scheme.port == port
        assert scheme.ssh_destination == destination
        assert "[" not in (scheme.ssh_destination or "")

    def test_an_ipv6_path_is_still_extracted(self):
        assert parse_target("ssh://[::1]:2222/mnt/backups").path == "/mnt/backups"
        assert parse_target("ssh://[::1]:/mnt/backups").path == "/mnt/backups"


class TestPortIsParsedNotGuessed:
    """A separator colon is not a port, and a real port must survive.

    Dropping the port silently sends every probe and transfer to 22. The
    ``host:/path`` form the README and shipped examples use must not be read as
    a port, and ``host:2222/path`` must be.
    """

    @pytest.mark.parametrize(
        ("uri", "expected_port"),
        [
            ("ssh://host:/backups", None),
            ("ssh://host/backups", None),
            ("ssh://host:2222/backups", 2222),
            ("ssh://user@host:2222/backups", 2222),
            ("raw+ssh://user@host:2222/backups", 2222),
            ("raw+ssh://user@host:/backups", None),
        ],
    )
    def test_port(self, uri, expected_port):
        assert parse_target(uri).port == expected_port

    @pytest.mark.parametrize(
        ("uri", "expected_host"),
        [
            ("ssh://host:2222/backups", "host"),
            ("ssh://user@host:2222/backups", "host"),
            ("ssh://host:/backups", "host"),
        ],
    )
    def test_the_port_does_not_leak_into_the_host(self, uri, expected_host):
        scheme = parse_target(uri)
        assert scheme.host == expected_host
        assert ":" not in (scheme.ssh_destination or "").split("@")[-1]
