"""A refused sudo must never look like an empty or intact raw+ssh target.

A ``raw+ssh://`` target stores btrfs send streams as ORDINARY FILES; no btrfs
command ever runs on the remote. So ``ssh_sudo`` there elevates file utilities --
mkdir, find, cat, stat, rm -- and the sudoers recipe this project's own README
gives (``NOPASSWD: /usr/bin/btrfs``) permits none of them.

Measured against a real host configured exactly that way: a target holding one
backup listed as **zero snapshots and exited 0**. Every test here pins a path
where "we were not allowed to look" was reported as "there is nothing there",
because for a backup tool those are opposite answers and only one of them is
survivable.

The mechanism is subtle enough to be worth stating: ``sudo`` exits 1 on an
authentication refusal, and ``find`` exits 1 for perfectly ordinary reasons, so
the exit status cannot separate them. Only sudo's stderr can -- which is why
``sudo -n`` (fail immediately, say why) and not discarding that stderr are both
load-bearing rather than cosmetic.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint import raw as raw_mod
from btrfs_backup_ng.endpoint.raw import (
    _ELEVATION_SENTINEL,
    SSHRawEndpoint,
    _is_sudo_denial,
)

# What sudo actually writes when it refuses; captured from a real remote.
SUDO_DENIED = "sudo: a password is required"


def _endpoint(**config):
    base = {"path": "/backup", "hostname": "nas", "ssh_sudo": True}
    base.update(config)
    return SSHRawEndpoint(config=base)


class TestSudoDenialIsRecognised:
    @pytest.mark.parametrize(
        "stderr",
        [
            "sudo: a password is required",
            "sudo: a terminal is required to read the password; either use ssh's -t option",
            "sudo: no askpass program specified",
            "sudo: user is not in the sudoers file",
            "sudo: sorry, user backup is not allowed to execute '/bin/find' as root",
            "sudo: no tty present and no askpass program specified",
            # Verbatim from sudo 1.9.17's catalog. Each of these was MISSED by the
            # first marker list, and each made a populated target list as empty at
            # the three sites that have no sentinel.
            "sudo: sorry, you must have a tty to run sudo",
            "sudo: no valid sudoers sources found, quitting",
            "sudo: sudoers specifies that root is not allowed to sudo",
            "sudo: effective uid is not 0, is /usr/bin/sudo on a nosuid filesystem?",
            # Not sudo speaking, but elevation is equally impossible.
            "bash: sudo: command not found",
        ],
    )
    def test_real_sudo_refusals_are_detected(self, stderr):
        assert _is_sudo_denial(stderr) is True

    @pytest.mark.parametrize(
        ("locale", "stderr"),
        [
            ("de_DE.UTF-8", "sudo: Ein Passwort ist notwendig"),
            ("fr_FR.UTF-8", "sudo: il est nécessaire de saisir un mot de passe"),
            ("ja_JP.UTF-8", "sudo: パスワードが必要です"),
            ("ru_RU.UTF-8", "sudo: требуется указать пароль"),
        ],
    )
    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Detection matches sudo's ENGLISH wording, which is safe only because "
            "_elevate pins LC_ALL=C so sudo always speaks English -- see "
            "TestSudoMessagesArePinnedToAPredictableLanguage. These strings are "
            "kept as measured evidence that the localisation is real (captured "
            "from a live remote under each locale) and as a tripwire: if the "
            "LC_ALL=C pin is ever dropped, matching English is no longer "
            "sufficient and this is what the remote would actually send."
        ),
    )
    def test_a_refusal_in_another_language_is_still_a_refusal(self, locale, stderr):
        """sudo localises its messages; the false all-clear must not come back.

        Every string here was captured from a real remote by re-running the same
        refused command under that locale. Matching the English wording would
        have left every non-English server reporting a populated backup target as
        empty -- the exact bug this module exists to prevent, restricted to the
        users least likely to be reading English logs.
        """
        assert _is_sudo_denial(stderr) is True, (
            f"a {locale} sudo refusal was not recognised; the listing would "
            f"silently report zero backups"
        )

    @pytest.mark.parametrize(
        "stderr",
        [
            # sudo's own catalog formats these WITHOUT the program-name prefix,
            # so a rule keyed on "sudo:" would let a denied user through -- the
            # exact false all-clear this module exists to prevent.
            "Sorry, user backup is not allowed to execute '/bin/find' as root on nas.",
            "backup is not in the sudoers file.",
            "Sorry, user backup may not run sudo on nas.",
        ],
    )
    def test_denials_without_the_sudo_prefix_are_still_caught(self, stderr):
        assert _is_sudo_denial(stderr) is True

    @pytest.mark.parametrize(
        "stderr",
        [
            # sudo warns about ITSELF and then runs the command anyway. Reading
            # these as refusals turns a working elevation into a hard abort.
            "sudo: unable to load /usr/lib64/libsss_sudo.so: cannot open shared object file",
            "sudo: unable to initialize SSS source. Is SSSD installed on your machine?",
            "sudo: setrlimit(RLIMIT_CORE): Operation not permitted",
            "sudo: /etc/sudo.conf is owned by uid 1000, should be 0",
        ],
    )
    def test_sudo_warnings_that_still_run_the_command_are_not_denials(self, stderr):
        """The SSSD pair fires on any RHEL/Fedora host whose nsswitch.conf still
        lists `sss` after sssd was removed -- common, and not a permission
        problem. Treating it as one broke `raw backfill-metadata` outright."""
        assert _is_sudo_denial(stderr) is False

    @pytest.mark.parametrize(
        "stderr",
        [
            "",
            "find: '/backup/gone': No such file or directory",
            "cat: /backup/x.btrfs: Permission denied",
            "ssh: connect to host nas port 22: No route to host",
        ],
    )
    def test_ordinary_failures_are_not_mistaken_for_a_refusal(self, stderr):
        """Over-matching would turn routine errors into hard aborts."""
        assert _is_sudo_denial(stderr) is False


class TestListingRefusesToReportAnEmptyTarget:
    """The safety-critical case: backups exist, sudo says no, listing said zero."""

    def test_a_refused_find_raises_instead_of_returning_no_snapshots(self):
        # elevated=True with the sentinel ABSENT is what a refusal looks like:
        # sudo never handed over the shell, so nothing echoed the marker.
        result = MagicMock(returncode=1, stdout="", stderr=SUDO_DENIED)
        with pytest.raises(RuntimeError) as excinfo:
            raw_mod._check_remote_listing(result, "nas", "/backup", elevated=True)

        message = str(excinfo.value)
        assert "NOT an empty target" in message
        # The operator needs to know it is the FILE tools, not btrfs -- following
        # the README's btrfs-only sudoers is what puts them here.
        assert "btrfs" in message
        assert "ssh_sudo" in message or "chown" in message

    def test_an_empty_but_reachable_target_is_still_empty(self):
        """The guard must not turn a genuinely empty target into an error."""
        raw_mod._check_remote_listing(
            MagicMock(returncode=0, stdout="", stderr=""), "nas", "/backup"
        )

    def test_an_ordinary_nonzero_still_only_warns(self):
        """A missing directory is not a refusal; it must not abort the run."""
        raw_mod._check_remote_listing(
            MagicMock(returncode=1, stdout="", stderr="find: no such file"),
            "nas",
            "/backup",
        )

    def test_an_unreachable_host_still_raises(self):
        with pytest.raises(RuntimeError, match="NOT an empty target"):
            raw_mod._check_remote_listing(
                MagicMock(returncode=255, stdout="", stderr="No route to host"),
                "nas",
                "/backup",
            )

    def test_list_snapshots_propagates_the_refusal(self):
        """End to end: the endpoint must not hand back [] when sudo refused."""
        ep = _endpoint()
        denial = MagicMock(returncode=1, stdout="", stderr=SUDO_DENIED)
        with patch.object(raw_mod.subprocess, "run", return_value=denial):
            with pytest.raises(RuntimeError, match="NOT an empty target"):
                ep.list_snapshots(flush_cache=True)


class TestSudoStderrSurvivesTheRedirect:
    """The redirect must not discard the one piece of evidence that classifies.

    ``find ... 2>/dev/null`` written flat binds to the whole ``sudo find ...``,
    so sudo's refusal goes to /dev/null too and the guard sees rc=1 with an empty
    stderr -- which it reads as an empty target. Verified on a real remote:
    ``sudo find ... 2>/dev/null`` gives rc=1 stderr='', while
    ``sudo -n find ...`` gives rc=1 stderr='sudo: a password is required'.
    """

    @staticmethod
    def _remote_commands(ep, **kwargs):
        sent = []

        def fake_run(cmd, **_):
            sent.append(cmd[-1])
            # A real elevated shell prints the sentinel; without it the guard
            # correctly refuses to believe the listing.
            return MagicMock(returncode=0, stdout="", stderr=_ELEVATION_SENTINEL)

        with patch.object(raw_mod.subprocess, "run", side_effect=fake_run):
            ep.list_snapshots(flush_cache=True, **kwargs)
        return sent

    def test_elevated_find_keeps_the_redirect_inside_sh_c(self):
        sent = self._remote_commands(_endpoint())
        finds = [c for c in sent if "find" in c]
        assert finds, "no find command was issued"
        for cmd in finds:
            assert cmd.startswith("LC_ALL=C sudo -n sh -c "), cmd
            # The redirect must be inside the quoted inner command, never applied
            # to the sudo invocation itself.
            assert not cmd.rstrip().endswith("2>/dev/null"), (
                f"stderr of the sudo invocation is being discarded: {cmd}"
            )

    def test_the_unelevated_command_shape_is_unchanged(self):
        """No sudo means no stderr to protect, so do not rewrite the wire format."""
        sent = self._remote_commands(_endpoint(ssh_sudo=False))
        finds = [c for c in sent if "find" in c]
        assert finds
        for cmd in finds:
            assert cmd.startswith("find "), cmd
            assert "sh -c" not in cmd


class TestElevationIsNonInteractive:
    """Without -n, sudo tries to prompt on a connection that has no tty."""

    def test_every_elevated_command_uses_sudo_n(self):
        ep = _endpoint()
        assert ep._elevate("find /backup") == "LC_ALL=C sudo -n find /backup"

    def test_no_elevation_when_ssh_sudo_is_off(self):
        assert _endpoint(ssh_sudo=False)._elevate("find /backup") == "find /backup"

    def test_the_capability_probe_is_not_elevated(self):
        """It asks whether tools EXIST, which root does not change.

        Elevating it meant a restrictive sudoers policy failed the probe before
        any real work, and the user was told the remote "does not provide the
        POSIX tools raw+ssh needs" -- naming the wrong cause entirely.
        """
        ep = _endpoint()
        sent = []

        def fake_run(cmd, **_):
            sent.append(cmd[-1])
            return MagicMock(returncode=0, stdout=b"RAWSSHOK\n", stderr=b"")

        with patch.object(raw_mod.subprocess, "run", side_effect=fake_run):
            with patch.object(ep, "_check_tools", return_value=[]):
                ep._prepare()

        probes = [c for c in sent if "command -v" in c]
        assert probes, "the POSIX-tool probe was not issued"
        for probe in probes:
            assert "sudo" not in probe, f"capability probe was elevated: {probe}"

    def test_the_directory_creation_is_still_elevated(self):
        """Guard against over-correcting: mkdir does touch the backup location."""
        ep = _endpoint()
        sent = []

        def fake_run(cmd, **_):
            sent.append(cmd[-1])
            return MagicMock(returncode=0, stdout=b"RAWSSHOK\n", stderr=b"")

        with patch.object(raw_mod.subprocess, "run", side_effect=fake_run):
            with patch.object(ep, "_check_tools", return_value=[]):
                ep._prepare()

        assert any(c.startswith("LC_ALL=C sudo -n mkdir") for c in sent), sent


class TestPrepareExplainsWhatRawSshNeeds:
    def test_a_refused_mkdir_names_the_file_tools_not_btrfs(self):
        ep = _endpoint()
        error = subprocess.CalledProcessError(1, "ssh", stderr=SUDO_DENIED.encode())
        with patch.object(raw_mod.subprocess, "run", side_effect=error):
            with pytest.raises(__util__.AbortError) as excinfo:
                ep._prepare()

        message = str(excinfo.value)
        assert SUDO_DENIED in message
        # The README's recipe is btrfs-only; saying so is the whole point.
        assert "btrfs" in message
        assert "chown" in message or "ownership" in message

    def test_an_unrelated_mkdir_failure_is_not_relabelled(self):
        """Only a sudo refusal gets the sudo explanation."""
        ep = _endpoint()
        error = subprocess.CalledProcessError(
            1, "ssh", stderr=b"mkdir: cannot create directory: Read-only file system"
        )
        with patch.object(raw_mod.subprocess, "run", side_effect=error):
            with pytest.raises(subprocess.CalledProcessError):
                ep._prepare()


class TestSidecarCheckDoesNotInventAbsence:
    """`test -f` exits 1 for a missing file; so does a refused sudo."""

    def test_a_refused_check_raises_rather_than_reporting_no_sidecar(self):
        ep = _endpoint()
        snapshot = MagicMock(metadata_path=Path("/backup/x.meta"))
        denial = MagicMock(returncode=1, stdout="", stderr=SUDO_DENIED)
        with patch.object(ep, "_exec_remote_command", return_value=denial):
            with pytest.raises(RuntimeError, match="overwrite"):
                ep.sidecar_exists(snapshot)

    def test_a_benign_sudo_warning_on_success_does_not_abort(self):
        """Not every `sudo:` line is a refusal.

        "sudo: unable to resolve host <name>" is printed on any box whose
        hostname is missing from /etc/hosts, and sudo runs the command anyway.
        Reading the text without first checking the exit code would abort a
        working backup over a cosmetic warning -- trading a false all-clear for
        a false alarm.
        """
        ep = _endpoint()
        snapshot = MagicMock(metadata_path=Path("/backup/x.meta"))
        warned = MagicMock(
            returncode=0, stdout="", stderr="sudo: unable to resolve host nas"
        )
        with patch.object(ep, "_exec_remote_command", return_value=warned):
            assert ep.sidecar_exists(snapshot) is True

    def test_a_benign_warning_with_a_real_absence_still_reports_absent(self):
        """rc=1 from `test -f` plus a resolve warning is a missing file.

        Measured with real sudo in stock Debian and Ubuntu containers, with no
        `Defaults fqdn`: an unresolvable hostname produces this warning and sudo
        RUNS the command anyway. Treating it as a refusal would abort every
        backfill on such a host, blaming a sudoers policy that is already fine.
        """
        ep = _endpoint()
        snapshot = MagicMock(metadata_path=Path("/backup/x.meta"))
        warned = MagicMock(
            returncode=1, stdout="", stderr="sudo: unable to resolve host nas"
        )
        with patch.object(ep, "_exec_remote_command", return_value=warned):
            assert ep.sidecar_exists(snapshot) is False

    def test_a_genuinely_missing_sidecar_is_still_reported_absent(self):
        ep = _endpoint()
        snapshot = MagicMock(metadata_path=Path("/backup/x.meta"))
        missing = MagicMock(returncode=1, stdout="", stderr="")
        with patch.object(ep, "_exec_remote_command", return_value=missing):
            assert ep.sidecar_exists(snapshot) is False

    def test_a_present_sidecar_is_reported_present(self):
        ep = _endpoint()
        snapshot = MagicMock(metadata_path=Path("/backup/x.meta"))
        present = MagicMock(returncode=0, stdout="", stderr="")
        with patch.object(ep, "_exec_remote_command", return_value=present):
            assert ep.sidecar_exists(snapshot) is True

    def test_the_check_is_elevated_exactly_once(self):
        """Wrapping it here AND in _exec_remote_command produced nested sudo.

        A sudoers policy permitting the outer invocation still failed on the
        inner one, so the pre-write re-check could not succeed at all.
        """
        ep = _endpoint()
        snapshot = MagicMock(metadata_path=Path("/backup/x.meta"))
        sent = []

        def fake_run(cmd, **_):
            sent.append(cmd[-1])
            return MagicMock(returncode=0, stdout=b"", stderr=b"")

        with patch.object(raw_mod.subprocess, "run", side_effect=fake_run):
            ep.sidecar_exists(snapshot)

        assert len(sent) == 1
        assert sent[0].count("sudo") == 1, f"nested elevation: {sent[0]}"


class TestPerFileStatFailuresAreClassified:
    """A refused stat is not a per-file accident; every stat in the loop fails.

    The sidecar-less second pass stats each stream to date it. Skipping a stream
    it cannot stat is right when the file vanished mid-listing, and wrong when
    sudo refused: then the listing shrinks silently toward zero and looks like a
    target holding no backups.
    """

    @staticmethod
    def _list_with_stat_result(stat_result):
        ep = _endpoint()
        calls = [
            # The two finds are elevated, so they carry the sentinel proving the
            # remote shell ran; only the per-stream stat varies.
            MagicMock(returncode=0, stdout="", stderr=_ELEVATION_SENTINEL),
            MagicMock(
                returncode=0, stdout="/backup/x.btrfs\n", stderr=_ELEVATION_SENTINEL
            ),
            stat_result,
        ]
        with patch.object(raw_mod.subprocess, "run", side_effect=calls):
            return ep.list_snapshots(flush_cache=True)

    def test_a_refused_stat_raises_rather_than_shrinking_the_listing(self):
        denial = subprocess.CalledProcessError(1, "ssh", stderr=SUDO_DENIED)
        with pytest.raises(RuntimeError, match="INCOMPLETE"):
            self._list_with_stat_result(denial)

    def test_a_vanished_file_is_still_skipped(self):
        """The legitimate reason to skip must keep working."""
        gone = subprocess.CalledProcessError(
            1, "ssh", stderr="stat: cannot stat '/backup/x.btrfs': No such file"
        )
        assert self._list_with_stat_result(gone) == []

    def test_a_stattable_stream_is_still_listed(self):
        ok = MagicMock(returncode=0, stdout="1700000000 4096\n", stderr="")
        assert len(self._list_with_stat_result(ok)) == 1


class TestRetentionDoesNotSilentlyStopRunning:
    def test_a_refused_delete_aborts_instead_of_reporting_success(self):
        """Prune otherwise finishes 'successfully' having removed nothing.

        Every delete in the run fails for the same reason, so the retention
        policy stops applying while the operator is told it ran -- and the
        destination fills up until a backup fails for space instead.
        """
        ep = _endpoint()
        snapshot = MagicMock(
            stream_path=Path("/backup/s.btrfs"),
            metadata_path=Path("/backup/s.btrfs.meta"),
            name="s",
        )
        snapshot.get_name.return_value = "s"
        error = subprocess.CalledProcessError(1, "ssh", stderr=SUDO_DENIED.encode())
        with patch.object(raw_mod.subprocess, "run", side_effect=error):
            with pytest.raises(__util__.AbortError, match="Retention did NOT run"):
                ep._delete_snapshots_locked([snapshot])


class TestSudoMessagesArePinnedToAPredictableLanguage:
    """LC_ALL=C so the diagnostic an operator reads (and forwards) is stable.

    Correctness does not depend on this -- detection keys on the untranslated
    ``sudo:`` prefix -- but a German host reporting "Ein Passwort ist notwendig"
    in an English log is a support burden, and the C locale also keeps any
    numeric output the callers parse free of locale formatting.
    """

    def test_elevated_commands_pin_the_locale(self):
        assert _endpoint()._elevate("rm -f /backup/x").startswith("LC_ALL=C sudo -n ")

    def test_the_unelevated_command_is_untouched(self):
        """No sudo means no message to pin; do not perturb the working path."""
        assert (
            _endpoint(ssh_sudo=False)._elevate("rm -f /backup/x") == "rm -f /backup/x"
        )


class TestElevationIsProvenNotGuessed:
    """The listing guard requires POSITIVE evidence that the remote shell ran.

    Enumerating sudo's refusals was tried and lost. The marker list missed
    "sorry, you must have a tty to run sudo" (a requiretty policy), "no valid
    sudoers sources found, quitting" (a fat-fingered /etc/sudoers), and
    "sudo: command not found" (sudo not installed) -- all real strings from
    sudo 1.9.17's own catalog. Each made `raw verify` print "0 ok, 0 corrupt"
    and exit 0 for a target holding backups it never read, which is the precise
    false all-clear this module exists to prevent. `cli/raw_cmd.py` never calls
    prepare(), so `raw list`/`verify`/`backfill-metadata` reach this guard with
    no earlier gate to catch the failure.

    So the rule is inverted: the elevated wrapper prints a sentinel after the
    inner command, and its ABSENCE means elevation failed -- independent of
    wording, locale, exit status, sudo version, and whether sudo exists at all.
    """

    @pytest.mark.parametrize(
        "stderr",
        [
            "sudo: a password is required",
            "sudo: sorry, you must have a tty to run sudo",
            "sudo: no valid sudoers sources found, quitting",
            "sudo: sudoers specifies that root is not allowed to sudo",
            "sudo: effective uid is not 0, is /usr/bin/sudo on a nosuid filesystem?",
            "bash: sudo: command not found",
            "sudo: irgendein unbekannter fehler",  # never-seen wording, any language
            "",  # no diagnostic at all
        ],
    )
    def test_any_failure_to_run_the_command_is_an_error_not_an_empty_target(
        self, stderr
    ):
        result = MagicMock(returncode=1, stdout="", stderr=stderr)
        with pytest.raises(RuntimeError, match="NOT an empty target"):
            raw_mod._check_remote_listing(result, "nas", "/backup", elevated=True)

    def test_the_sentinel_makes_an_empty_target_genuinely_empty(self):
        """Elevation succeeded and there was nothing there: not an error."""
        raw_mod._check_remote_listing(
            MagicMock(returncode=0, stdout="", stderr=_ELEVATION_SENTINEL),
            "nas",
            "/backup",
            elevated=True,
        )

    def test_an_ordinary_find_failure_after_a_real_run_still_only_warns(self):
        """find exits 1/2 routinely; once the shell demonstrably ran, that is
        a listing detail rather than a permission failure."""
        raw_mod._check_remote_listing(
            MagicMock(
                returncode=1,
                stdout="",
                stderr=f"find: '/backup/x': No such file\n{_ELEVATION_SENTINEL}",
            ),
            "nas",
            "/backup",
            elevated=True,
        )

    def test_an_unelevated_listing_is_unaffected(self):
        """No sudo means no sentinel to expect; the working path must not break."""
        raw_mod._check_remote_listing(
            MagicMock(returncode=0, stdout="", stderr=""), "nas", "/backup"
        )

    def test_the_sentinel_never_leaks_into_a_user_facing_message(self):
        """An internal marker in an operator-facing log is noise that gets
        pasted into bug reports.

        Asserts on what reaches the logger rather than on caplog: this project
        configures logging itself, so caplog captures nothing here.
        """
        recorded = []
        with patch.object(
            raw_mod.logger, "warning", lambda msg, *a, **k: recorded.append(a)
        ):
            raw_mod._check_remote_listing(
                MagicMock(
                    returncode=1, stdout="", stderr=f"find: oops\n{_ELEVATION_SENTINEL}"
                ),
                "nas",
                "/backup",
                elevated=True,
            )
        assert recorded, "no warning was emitted"
        text = " ".join(str(x) for x in recorded[0])
        assert _ELEVATION_SENTINEL not in text
        assert "find: oops" in text

    def test_the_elevated_command_actually_emits_the_sentinel(self):
        """The guard is only sound if the wrapper really appends it."""
        ep = _endpoint()
        wrapped = ep._elevate_shell("find /backup -name '*.meta' 2>/dev/null")
        assert _ELEVATION_SENTINEL in wrapped
        # After the inner command, and on stderr, so a failing find still proves
        # the shell ran.
        assert wrapped.index("find") < wrapped.index(_ELEVATION_SENTINEL)
        assert ">&2" in wrapped

    def test_the_unelevated_command_carries_no_sentinel(self):
        assert (
            _endpoint(ssh_sudo=False)._elevate_shell("find /backup") == "find /backup"
        )


class TestTheSentinelDoesNotMaskTheInnerExitStatus:
    """Appending the sentinel must not swallow the wrapped command's status.

    `sh -c 'find ...; echo S >&2'` exits with the ECHO's status -- always 0. That
    would turn a genuine find failure (a missing directory, an unreadable target)
    into a successful listing of an empty target, which is exactly the false
    all-clear the sentinel was added to prevent. Measured: without the capture,
    a find over a nonexistent directory returned rc=0 and the guard reported an
    empty target with no warning at all -- strictly worse than before the
    sentinel, because the unelevated path still logged one.
    """

    def test_the_wrapper_captures_and_reraises_the_inner_status(self):
        wrapped = _endpoint()._elevate_shell("find /backup -type f")
        assert "__bbng_rc=$?" in wrapped
        assert "exit $__bbng_rc" in wrapped
        # Order matters: capture immediately after the command, exit last.
        assert wrapped.index("__bbng_rc=$?") < wrapped.index(_ELEVATION_SENTINEL)
        assert wrapped.index(_ELEVATION_SENTINEL) < wrapped.index("exit $__bbng_rc")

    @pytest.mark.parametrize(
        ("inner", "expected_rc"),
        # `(exit 2)` not `exit 2`: the bare builtin terminates the wrapper shell
        # before the sentinel prints. That is a real limitation -- an inner
        # command that exits the shell is reported as "never run" -- but the only
        # inner commands are `find ... 2>/dev/null`, which cannot do it.
        [("false", 1), ("true", 0), ("(exit 2)", 2)],
    )
    def test_a_real_shell_propagates_the_inner_status(self, inner, expected_rc):
        """Runs the actual emitted string through a real shell.

        Asserting on the string alone would pass for a wrapper that is subtly
        wrong; this executes it.
        """
        import subprocess as sp

        wrapped = _endpoint()._elevate_shell(inner)
        # Strip the sudo prefix; the shell semantics are what is under test.
        script = wrapped.split("sudo -n sh -c ", 1)[1]
        proc = sp.run(["sh", "-c", f"sh -c {script}"], capture_output=True, text=True)
        assert proc.returncode == expected_rc, proc
        assert _ELEVATION_SENTINEL in proc.stderr

    def test_a_failed_elevated_listing_is_not_reported_as_an_empty_target(self):
        """End to end: find failed, the shell ran, so this is not 'empty'."""
        ep = _endpoint()
        failed = MagicMock(
            returncode=1,
            stdout="",
            stderr=f"find: '/backup': No such file or directory\n{_ELEVATION_SENTINEL}",
        )
        recorded = []
        with patch.object(
            raw_mod.logger, "warning", lambda msg, *a, **k: recorded.append(a)
        ):
            with patch.object(raw_mod.subprocess, "run", return_value=failed):
                assert ep.list_snapshots(flush_cache=True) == []
        assert recorded, (
            "a failing elevated find produced no warning; its exit status was "
            "masked and the empty result looks authoritative"
        )


class TestNoMessageLeaksTheInternalMarker:
    """Every operator-facing string must be sanitised, not just the warning.

    The marker is an implementation detail of the elevation probe. Leaking it
    into an error puts it in bug reports and support threads, where it reads as
    part of the failure.
    """

    @pytest.mark.parametrize(
        ("returncode", "stderr", "match"),
        [
            (255, "No route to host", "Cannot reach"),
            (1, "sudo: a password is required", "never run"),
            (1, "anything at all", "never run"),
        ],
    )
    def test_error_messages_are_sanitised(self, returncode, stderr, match):
        """Both surviving raise-sites fire only when the sentinel is ABSENT."""
        result = MagicMock(returncode=returncode, stdout="", stderr=stderr)
        with pytest.raises(RuntimeError) as excinfo:
            raw_mod._check_remote_listing(result, "nas", "/backup", elevated=True)
        assert match in str(excinfo.value)
        assert _ELEVATION_SENTINEL not in str(excinfo.value)


class TestElevationIsProvenNotReGuessed:
    """Once the sentinel is present, elevation is proven -- do not overrule it.

    The guard used to consult sudo's message text even after the sentinel had
    demonstrated the shell ran. That branch was unreachable while the wrapper
    always exited 0; re-raising the inner status made it live, and it then
    turned ordinary failures into false sudo diagnoses. Measured with real sudo
    in stock Debian (1.9.13p3) and Ubuntu (1.9.15p5) containers with no
    `Defaults fqdn`: a host whose name does not resolve emits "sudo: unable to
    resolve host <name>" and RUNS the command. A find failure there was reported
    as a permissions problem and told the operator to fix a correct sudoers.
    """

    @pytest.mark.parametrize(
        "stderr",
        [
            "sudo: unable to resolve host nas: Name or service not known",
            "sudo: unable to send audit message: Operation not permitted",
            "sudo: a password is required",  # even this: the shell demonstrably ran
        ],
    )
    def test_a_sudo_message_never_overrules_the_sentinel(self, stderr):
        """With the sentinel present the command ran; a non-zero status is its own."""
        result = MagicMock(
            returncode=1, stdout="", stderr=f"{stderr}\n{_ELEVATION_SENTINEL}"
        )
        raw_mod._check_remote_listing(result, "nas", "/backup", elevated=True)

    def test_an_inner_255_is_not_blamed_on_the_transport(self):
        """255 means "ssh could not connect" only if the shell never ran.

        _elevate_shell re-raises the inner status, so a command exiting 255 would
        otherwise be reported as an unreachable host while the sentinel proves
        the host was reached.
        """
        result = MagicMock(
            returncode=255, stdout="", stderr=f"tool exited 255\n{_ELEVATION_SENTINEL}"
        )
        raw_mod._check_remote_listing(result, "nas", "/backup", elevated=True)

    def test_a_transport_failure_without_the_sentinel_still_raises(self):
        """Guard against over-correcting: a real 255 must stay loud."""
        with pytest.raises(RuntimeError, match="Cannot reach"):
            raw_mod._check_remote_listing(
                MagicMock(returncode=255, stdout="", stderr="No route to host"),
                "nas",
                "/backup",
                elevated=True,
            )
