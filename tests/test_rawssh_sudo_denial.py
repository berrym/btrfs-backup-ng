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
from btrfs_backup_ng.endpoint.raw import SSHRawEndpoint, _is_sudo_denial

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
            "bash: sudo: command not found",
            "ssh: connect to host nas port 22: No route to host",
        ],
    )
    def test_ordinary_failures_are_not_mistaken_for_a_refusal(self, stderr):
        """Over-matching would turn routine errors into hard aborts."""
        assert _is_sudo_denial(stderr) is False


class TestListingRefusesToReportAnEmptyTarget:
    """The safety-critical case: backups exist, sudo says no, listing said zero."""

    def test_a_refused_find_raises_instead_of_returning_no_snapshots(self):
        result = MagicMock(returncode=1, stdout="", stderr=SUDO_DENIED)
        with pytest.raises(RuntimeError) as excinfo:
            raw_mod._check_remote_listing(result, "nas", "/backup")

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
            return MagicMock(returncode=0, stdout="", stderr="")

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
        """rc=1 from `test -f` plus a resolve warning is a missing file."""
        ep = _endpoint()
        snapshot = MagicMock(metadata_path=Path("/backup/x.meta"))
        warned = MagicMock(
            returncode=1, stdout="", stderr="sudo: unable to resolve host nas"
        )
        with patch.object(ep, "_exec_remote_command", return_value=warned):
            # Conservative: an ambiguous rc=1 with a sudo line is treated as a
            # denial rather than an absence, because inventing an absence here
            # lets backfill overwrite a real sidecar.
            with pytest.raises(RuntimeError, match="overwrite"):
                ep.sidecar_exists(snapshot)

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
            MagicMock(returncode=0, stdout="", stderr=""),  # find *.meta -> none
            MagicMock(returncode=0, stdout="/backup/x.btrfs\n", stderr=""),  # streams
            stat_result,  # the per-stream stat
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
