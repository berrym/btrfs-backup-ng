"""The mount check must actually STOP a backup, not merely exist in the source.

These drive the real `cli.run._backup_volume` and `cli.transfer.execute_transfer`
and assert on whether a transfer HAPPENED. The previous tests for this asserted
on `inspect.getsource` text, and that turned out to be theater: reintroducing the
original defect --

    if not target.path.startswith(("ssh://", "raw://", "raw+ssh://")):
        assert_target_mounted(target.path, target.require_mount)

-- left the whole suite green, all 4183 tests, because the source-text assertion
only inspected the characters AFTER the last "require_mount" and the exemption
sits before it. The tests pinned a phrasing, not a property.

`require_mount` exists to stop a backup landing on the root filesystem when an
external drive is absent, so the property under test is "nothing was transferred",
and it is asserted directly.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import btrfs_backup_ng.cli.run as run_mod
import btrfs_backup_ng.cli.transfer as transfer_mod
from btrfs_backup_ng import __util__
from btrfs_backup_ng.config.schema import (
    Config,
    GlobalConfig,
    TargetConfig,
    VolumeConfig,
)


def _mounted(*paths):
    """Replace is_mounted with an exact-match stub over the given mount points."""
    wanted = {str(p) for p in paths}
    return lambda path: str(path) in wanted


def _real_filesystem(monkeypatch):
    """Report the named mount as a real (non-memory) filesystem.

    These tests run under tmp_path, which on a systemd machine lives on /tmp --
    a tmpfs. The gate refuses memory-backed filesystems, correctly, so without
    this the fixtures are refused for a reason the test is not about. Stubbing
    it keeps the memory-backed rejection covered by its own tests instead of
    leaking into every other one.
    """
    monkeypatch.setattr(__util__, "get_mount_info", lambda path: {"fs_type": "btrfs"})


@pytest.fixture
def rig(tmp_path, monkeypatch):
    """A source, a destination, and spies on everything past the mount check."""
    src = tmp_path / "src"
    src.mkdir()
    dest = tmp_path / "dest"
    dest.mkdir()

    state: dict = {"transferred": []}

    def _fake_endpoint(spec, common_config=None, source=False, **kw):
        m = MagicMock()
        m.snapshot.return_value = MagicMock()
        m.list_snapshots.return_value = [MagicMock()]
        return m

    def _fake_transfer(*a, **k):
        # Records that a transfer was REACHED. If the mount check works, an
        # unmounted target never gets here.
        state["transferred"].append(True)
        return True

    monkeypatch.setattr(run_mod.endpoint, "choose_endpoint", _fake_endpoint)
    monkeypatch.setattr(run_mod, "_transfer_to_target", _fake_transfer)
    monkeypatch.setattr(run_mod, "_prune_after_transfer", lambda *a, **k: True)
    state["src"] = src
    state["dest"] = dest
    state["monkeypatch"] = monkeypatch
    return state


def _run_backup(rig, target_path, require_mount, mounted):
    """Drive the REAL _backup_volume. Returns (ok, errors, transfer_happened)."""
    volume = VolumeConfig(
        path=str(rig["src"]),
        snapshot_prefix="t-",
        snapshot_dir=str(rig["dest"].parent / "snaps"),
        targets=[TargetConfig(path=target_path, require_mount=require_mount)],
    )
    config = Config(global_config=GlobalConfig(), volumes=[volume])
    rig["monkeypatch"].setattr(__util__, "is_mounted", _mounted(*mounted))
    rig["transferred"].clear()
    ok, _stats, errors = run_mod._backup_volume(volume, config, parallel_targets=1)
    return ok, errors, bool(rig["transferred"])


class TestAnAbsentDriveStopsTheBackup:
    """THE property. Each case: the drive is not mounted, so nothing may transfer."""

    @pytest.mark.parametrize("scheme", ["local", "raw"])
    @pytest.mark.parametrize("form", [True, "MOUNT"])
    def test_nothing_is_transferred_to_an_unmounted_local_target(
        self, rig, tmp_path, scheme, form
    ):
        usb = tmp_path / "usb"
        usb.mkdir()
        target = str(usb) if scheme == "local" else f"raw://{usb}"
        require = str(usb) if form == "MOUNT" else True

        ok, errors, transferred = _run_backup(rig, target, require, mounted=[])

        assert not transferred, (
            f"{scheme} target with require_mount={require!r} transferred to an "
            f"UNMOUNTED drive -- this is the backup-onto-the-root-filesystem "
            f"accident require_mount exists to prevent"
        )
        assert ok is False
        assert any("not mounted" in e for e in errors), errors

    @pytest.mark.parametrize("scheme", ["local", "raw"])
    def test_a_mounted_drive_lets_the_backup_through(self, rig, tmp_path, scheme):
        """The guard must not refuse a working setup."""
        usb = tmp_path / "usb"
        usb.mkdir()
        target = str(usb) if scheme == "local" else f"raw://{usb}"

        ok, errors, transferred = _run_backup(rig, target, True, mounted=[usb])

        assert transferred, f"a MOUNTED {scheme} target was refused: {errors}"
        assert ok, errors

    def test_a_subdirectory_under_a_mounted_drive_is_allowed(self, rig, tmp_path):
        """The #100 case, end to end."""
        usb = tmp_path / "usb"
        (usb / "box1").mkdir(parents=True)
        _real_filesystem(rig["monkeypatch"])

        ok, errors, transferred = _run_backup(
            rig, str(usb / "box1"), str(usb), mounted=[usb]
        )

        assert transferred, f"the reported setup was refused: {errors}"
        assert ok, errors

    def test_a_subdirectory_is_stopped_when_the_drive_is_absent(self, rig, tmp_path):
        usb = tmp_path / "usb"
        (usb / "box1").mkdir(parents=True)

        ok, errors, transferred = _run_backup(
            rig, str(usb / "box1"), str(usb), mounted=[]
        )

        assert not transferred, "transferred to a subdirectory of an absent drive"
        assert ok is False

    @pytest.mark.parametrize(
        "target", ["ssh://user@host:/backups", "raw+ssh://user@host/backups"]
    )
    def test_remote_targets_are_not_blocked_by_the_local_mount_table(self, rig, target):
        """A local mount table cannot answer for a remote filesystem, so consulting
        it would abort every remote backup."""
        ok, errors, transferred = _run_backup(rig, target, True, mounted=[])
        assert transferred, f"a remote target was blocked by the mount check: {errors}"


class TestTheTransferCommandEnforcesItToo:
    """`transfer` shares the gate; it must reach the same verdict as `run`.

    Drives the real `execute_transfer` rather than calling the gate directly.
    The first version of this class called `assert_target_mounted` itself, which
    proved the gate works while saying nothing about whether the command invokes
    it -- so disabling the call in execute_transfer left these green. Exactly the
    weakness that made the source-text tests worthless, reproduced in a new place.
    """

    def _drive(self, tmp_path, monkeypatch, target_path, require_mount, mounted):
        """Run the REAL execute_transfer. Returns True if a sync was reached."""
        data = tmp_path / "data"
        (data / ".snapshots").mkdir(parents=True, exist_ok=True)
        volume = VolumeConfig(
            path=str(data),
            snapshot_prefix="data-",
            snapshot_dir=".snapshots",
            targets=[TargetConfig(path=target_path, require_mount=require_mount)],
        )
        config = Config(global_config=GlobalConfig(), volumes=[volume])

        synced: list = []
        monkeypatch.setattr(
            transfer_mod, "sync_snapshots", lambda *a, **k: synced.append(True)
        )
        monkeypatch.setattr(
            transfer_mod, "find_config_file", lambda *a, **k: str(tmp_path / "c.toml")
        )
        monkeypatch.setattr(transfer_mod, "load_config", lambda *a, **k: (config, []))
        endpoint = MagicMock()
        endpoint.list_snapshots.return_value = [MagicMock()]
        monkeypatch.setattr(
            transfer_mod.endpoint, "choose_endpoint", lambda *a, **k: endpoint
        )
        monkeypatch.setattr(__util__, "is_mounted", _mounted(*mounted))

        args = MagicMock()
        args.config = None
        args.volume = None
        args.dry_run = False
        args.compress = None
        args.rate_limit = None
        args.no_check_space = True
        args.force = False
        args.safety_margin = 10.0

        transfer_mod.execute_transfer(args)
        return bool(synced)

    @pytest.mark.parametrize("scheme", ["local", "raw"])
    @pytest.mark.parametrize("form", [True, "MOUNT"])
    def test_an_unmounted_target_transfers_nothing(
        self, tmp_path, monkeypatch, scheme, form
    ):
        usb = tmp_path / "usb"
        usb.mkdir()
        target = str(usb) if scheme == "local" else f"raw://{usb}"
        require = str(usb) if form == "MOUNT" else True

        assert not self._drive(tmp_path, monkeypatch, target, require, []), (
            f"transfer synced to an UNMOUNTED {scheme} target with "
            f"require_mount={require!r}"
        )

    @pytest.mark.parametrize("scheme", ["local", "raw"])
    def test_a_mounted_target_is_allowed(self, tmp_path, monkeypatch, scheme):
        usb = tmp_path / "usb"
        usb.mkdir()
        target = str(usb) if scheme == "local" else f"raw://{usb}"
        assert self._drive(tmp_path, monkeypatch, target, True, [usb]), (
            f"a MOUNTED {scheme} target was refused"
        )

    def test_a_subdirectory_under_a_mounted_drive_is_allowed(
        self, tmp_path, monkeypatch
    ):
        usb = tmp_path / "usb"
        (usb / "box1").mkdir(parents=True)
        _real_filesystem(monkeypatch)
        assert self._drive(tmp_path, monkeypatch, str(usb / "box1"), str(usb), [usb])


class TestValidationIsWiredIntoTheLoader:
    """The validator must be REACHED by loading a real config file.

    `_parse_require_mount` was only ever tested by calling it directly, so
    reverting the loader's call site back to a raw `data.get("require_mount",
    False)` survived every test in the suite -- the validation could have been
    disconnected entirely and nothing would have noticed. A bad value would then
    reach TargetConfig unchecked and fail, if at all, part-way through a backup.
    """

    @staticmethod
    def _load(tmp_path, literal):
        from btrfs_backup_ng.config.loader import load_config

        cfg = tmp_path / "c.toml"
        cfg.write_text(
            "[global]\n"
            f'snapshot_dir = "{tmp_path}/snaps"\n\n'
            "[[volumes]]\n"
            f'path = "{tmp_path}"\n\n'
            "[[volumes.targets]]\n"
            f'path = "{tmp_path}/dest"\n'
            f"require_mount = {literal}\n"
        )
        return load_config(str(cfg))

    @pytest.mark.parametrize(
        "literal",
        ['""', '"   "', "[1]", '"mnt/backup"', '"/"', '"/mnt/.."', "123", "1.5"],
    )
    def test_the_dangerous_values_still_fail_at_load(self, tmp_path, literal):
        """Only the two classes that cannot be coerced safely: an empty string,
        which silently turns the check OFF, and a type with no interpretation."""
        from btrfs_backup_ng.config.loader import ConfigError

        if literal in ('""', '"   "', "[1]"):
            with pytest.raises(ConfigError):
                self._load(tmp_path, literal)
            return
        # Everything else loaded on 0.9.6 and the gate refuses the unusable ones
        # per target, so erroring here would stop the whole file for no added
        # safety. They must still be NAMED -- a silent coercion is the defect
        # this project keeps removing.
        _config, warnings = self._load(tmp_path, literal)
        assert any("require_mount" in w for w in warnings), (
            f"{literal} was coerced silently; the operator is never told"
        )

    def test_a_mount_point_loads_and_reaches_the_target(self, tmp_path):
        config, _warnings = self._load(tmp_path, f'"{tmp_path}"')
        assert config.volumes[0].targets[0].require_mount == str(tmp_path)

    @pytest.mark.parametrize(
        ("literal", "expected"), [("true", True), ("false", False)]
    )
    def test_booleans_still_load(self, tmp_path, literal, expected):
        config, _warnings = self._load(tmp_path, literal)
        assert config.volumes[0].targets[0].require_mount is expected


class TestTheWizardDerivesAUsableValue:
    """The wizard offers this check for /mnt/... paths, which are overwhelmingly
    SUBDIRECTORIES -- and it could only emit `true`, which requires the target
    itself to be a mount point. It generated configs that abort with the drive
    correctly connected, and the shipped examples taught the same shape."""

    @staticmethod
    def _derive(target, mounted):
        from unittest.mock import patch

        from btrfs_backup_ng.cli.config_cmd import _derive_require_mount

        with patch.object(__util__, "is_mounted", _mounted(*mounted)):
            return _derive_require_mount(target)

    def test_a_subdirectory_gets_the_mount_point(self):
        assert self._derive("/mnt/usb-drive/backup", ["/mnt/usb-drive"]) == (
            "/mnt/usb-drive"
        )

    def test_a_mount_point_target_keeps_true(self):
        assert self._derive("/mnt/usb-backup", ["/mnt/usb-backup"]) is True

    def test_a_deep_target_finds_the_real_mount(self):
        assert self._derive("/mnt/backup/box1/home", ["/mnt/backup"]) == "/mnt/backup"

    def test_it_declines_to_guess_when_nothing_is_mounted(self):
        """With the drive unplugged the mount table cannot answer, so `true` --
        the documented default -- is emitted rather than a fabricated path.

        An earlier version fell back to the target's PARENT, which invented mount
        points that can never exist: a target of /mnt/backup yielded "/mnt", and
        /run/media/<user>/<drive> yielded "/run/media/<user>". Both produce a
        config that aborts forever WITH THE DRIVE CONNECTED. Guessing wrong is
        worse than not guessing: when `true` is wrong, the runtime error names
        the mounted drive and the line to write instead.
        """
        assert self._derive("/mnt/usb-drive/backup", []) is True

    @pytest.mark.parametrize("target", ["/mnt/backup", "/run/media/mberry/USB-DRIVE"])
    def test_it_never_invents_a_mount_point_that_cannot_exist(self, target):
        """/mnt and /run/media/<user> are directories, never mount points."""
        derived = self._derive(target, [])
        assert derived is True, (
            f"the wizard invented {derived!r} as a mount point for {target}; "
            f"that config aborts forever even with the drive connected"
        )

    def test_a_raw_uri_target_is_seen_through(self):
        """A raw:// URI is not an absolute PATH, so an is_absolute() check bailed
        to `true` for exactly the scheme whose targets are usually
        subdirectories -- and raw:// is the scheme that newly became subject to
        this check."""
        assert self._derive("raw:///mnt/usb-backup/home", ["/mnt/usb-backup"]) == (
            "/mnt/usb-backup"
        )

    def test_it_never_names_a_memory_backed_filesystem(self):
        """/run is tmpfs and always mounted, so naming it produces a check that
        always passes -- which the gate then refuses outright."""
        from unittest.mock import patch

        from btrfs_backup_ng.cli.config_cmd import _derive_require_mount

        with (
            patch.object(__util__, "is_mounted", _mounted("/run")),
            patch.object(__util__, "get_mount_info", lambda p: {"fs_type": "tmpfs"}),
        ):
            assert _derive_require_mount("/run/media/mberry/USB/backups") is True

    def test_an_unreadable_mount_table_does_not_crash_the_wizard(self):
        """The wizard never read /proc/mounts before this change."""
        from unittest.mock import patch

        from btrfs_backup_ng.cli.config_cmd import _derive_require_mount

        with patch.object(__util__, "MOUNTS_FILE", "/nonexistent/mounts"):
            # Cannot crash, and cannot invent a mount point it has no evidence
            # for -- `true` is the documented default.
            assert _derive_require_mount("/mnt/usb/backup") is True

    def test_what_it_derives_always_loads(self):
        """A wizard that emits an unloadable value is worse than one that emits
        a useless one."""
        from btrfs_backup_ng.config.loader import _parse_require_mount

        for target in ("/mnt/usb-drive/backup", "/mnt/backup/box1/home", "/mnt/x"):
            value = self._derive(target, [])
            assert _parse_require_mount(value) == value


class TestAMountPointIsRefusedForARemoteTarget:
    """`require_mount = "/mnt/backup"` asserts a mount on THIS machine. For an
    ssh:// target the data lands elsewhere, so the local mount table says nothing
    about it -- the gate exempts remote targets, and the value was accepted and
    silently did nothing. The loader already refuses `encrypt` on a non-raw
    target for exactly this reason.
    """

    @staticmethod
    def _load(tmp_path, target, literal):
        from btrfs_backup_ng.config.loader import load_config

        cfg = tmp_path / "c.toml"
        cfg.write_text(
            "[global]\n"
            f'snapshot_dir = "{tmp_path}/snaps"\n\n'
            "[[volumes]]\n"
            f'path = "{tmp_path}"\n\n'
            "[[volumes.targets]]\n"
            f'path = "{target}"\n'
            f"require_mount = {literal}\n"
        )
        return load_config(str(cfg))

    @pytest.mark.parametrize(
        "target",
        ["ssh://backup@server:/backups/box1", "raw+ssh://backup@server/backups/box1"],
    )
    def test_a_string_on_a_remote_target_warns(self, tmp_path, target):
        """It has no effect there -- the gate exempts remote targets, because a
        local mount table says nothing about a remote filesystem. Warned rather
        than refused: the README already documents require_mount as having no
        effect on ssh:// targets, and the sibling precedent in the same loader
        (compress not applied to a same-machine target) warns for this shape."""
        _config, warnings = self._load(tmp_path, target, '"/mnt/backup"')
        assert any("require_mount" in w and "no effect" in w for w in warnings), (
            f"a setting that does nothing here was accepted silently: {warnings}"
        )

    @pytest.mark.parametrize(
        "target",
        ["ssh://backup@server:/backups/box1", "raw+ssh://backup@server/backups/box1"],
    )
    def test_true_on_a_remote_target_still_loads(self, tmp_path, target):
        """It has always been accepted and ignored. Turning that into a hard
        error would stop existing configs loading for a setting that was never
        doing anything."""
        config, _warnings = self._load(tmp_path, target, "true")
        assert config.volumes[0].targets[0].require_mount is True

    @pytest.mark.parametrize("target", ["/mnt/backup/box1", "raw:///mnt/backup/box1"])
    def test_a_string_on_a_local_target_is_unaffected(self, tmp_path, target):
        config, _warnings = self._load(tmp_path, target, '"/mnt/backup"')
        assert config.volumes[0].targets[0].require_mount == "/mnt/backup"

    def test_the_bare_user_at_host_form_is_left_to_the_gate(self, tmp_path):
        """`user@host:/path` parses as UNSUPPORTED rather than remote, so the
        loader does not reject it here -- the gate refuses it fail-closed, with a
        message about the path being undeterminable, which is the accurate
        reason.

        Note on the scheme predicate: this check uses `parse_target(...).is_remote`
        rather than `path.startswith(("ssh://", "raw+ssh://"))`. Those two agree
        on every form that exists today, so no test can distinguish them and none
        here pretends to. The authority is used because scheme-by-prefix is how
        raw:// ended up exempt from the mount check in `run`, and a remote scheme
        added to core/target.py later would be missed by a hardcoded tuple.
        """
        config, _warnings = self._load(tmp_path, "user@host:/backups", '"/mnt/backup"')
        assert config.volumes[0].targets[0].require_mount == "/mnt/backup"

        with pytest.raises(__util__.AbortError, match="cannot be determined"):
            from btrfs_backup_ng.cli.common import assert_target_mounted

            assert_target_mounted("user@host:/backups", "/mnt/backup")


class TestAConfigThatCanNeverRunIsNamedAtLoad:
    """A target that is not inside the mount it names aborts on EVERY run.

    That is decidable at load with no I/O, so `config validate` says it rather
    than blessing the file and leaving the operator to find out at backup time.
    It is a warning, not an error: the gate already refuses the target
    fail-closed, and stopping the whole file would take `list`, `status` and
    `doctor` down with `run` over one misconfigured target.
    """

    @staticmethod
    def _load(tmp_path, target, require_mount):
        from btrfs_backup_ng.config.loader import load_config

        cfg = tmp_path / "c.toml"
        cfg.write_text(
            "[global]\n"
            f'snapshot_dir = "{tmp_path}/snaps"\n\n'
            "[[volumes]]\n"
            f'path = "{tmp_path}"\n\n'
            "[[volumes.targets]]\n"
            f'path = "{target}"\n'
            f'require_mount = "{require_mount}"\n'
        )
        return load_config(str(cfg))

    def test_a_target_outside_its_named_mount_is_warned_about(self, tmp_path):
        _config, warnings = self._load(tmp_path, "/mnt/usb/box1", "/mnt/somewhere-else")
        assert any("require_mount" in w and "not inside" in w for w in warnings), (
            f"a config that can never run was accepted without comment: {warnings}"
        )

    def test_it_still_loads_so_other_targets_keep_working(self, tmp_path):
        config, _warnings = self._load(tmp_path, "/mnt/usb/box1", "/mnt/elsewhere")
        assert config.volumes[0].targets[0].require_mount == "/mnt/elsewhere"

    def test_the_gate_still_refuses_that_target(self, tmp_path):
        """The warning is not the protection -- this is."""
        from unittest.mock import patch

        from btrfs_backup_ng.cli.common import assert_target_mounted

        with patch.object(__util__, "is_mounted", _mounted("/mnt/elsewhere")):
            with pytest.raises(__util__.AbortError, match="not inside"):
                assert_target_mounted("/mnt/usb/box1", "/mnt/elsewhere")

    def test_a_correct_pairing_is_not_warned_about(self, tmp_path):
        _config, warnings = self._load(tmp_path, "/mnt/usb/box1", "/mnt/usb")
        assert not [
            w for w in warnings if "require_mount" in w and "not inside" in w
        ], f"a valid configuration was flagged: {warnings}"
