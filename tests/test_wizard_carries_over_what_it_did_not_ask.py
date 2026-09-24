"""A wizard saving over an existing configuration keeps everything it did not
ask about in THAT run.

The first carry-over used one static set of "asked" keys per wizard, while
the wizards ask conditionally. Measured against a configuration with every
key set: `config detect --wizard` with "Configure global settings?" declined
replaced snapshot_dir, timestamp_format, incremental, the parallel counts,
retention, log_file and the notifications with hard-coded defaults, mostly
without saying so; a raw+ssh:// target lost its ssh_sudo (the sudo question
is asked only for ssh://) and a target outside /mnt lost its require_mount
(that question is asked only under /mnt or on a USB path); a snapper volume
re-entered through `config init -i` became native and lost [volumes.snapper];
webhook headers and timeouts were dropped; two volumes sharing a path
collapsed to the last; --force skipped the carry-over along with the
question; a changed answer was never reported; and the diff summary read the
wizard's raw answers instead of what would be written.

Each wizard now records the keys it actually prompted for, per volume and
target, and the carry-over keeps every other key from the existing file.
These tests drive the real wizards with the prompts answered.
"""

from __future__ import annotations

import dataclasses
import tomllib
import types
from pathlib import Path
from unittest import mock

import pytest

from btrfs_backup_ng.cli import config_cmd
from btrfs_backup_ng.cli.config_cmd import (
    Asked,
    WizardConfig,
    _show_config_diff_summary,
    carry_over_existing,
)
from btrfs_backup_ng.config import loader
from btrfs_backup_ng.config.loader import load_config
from btrfs_backup_ng.detection import (
    BackupSuggestion,
    BtrfsMountInfo,
    DetectedSubvolume,
    DetectionResult,
    SubvolumeClass,
)

SSH = "ssh://backup@nas:/backups/home"
RAW_SSH = "raw+ssh://backup@nas:/raw/home"

#: Every key the loader knows, set to a non-default value. A key added to the
#: loader fails test_every_loader_key_is_exercised until it is here.
EVERY_KEY = f"""
[global]
snapshot_dir = ".snaps"
timestamp_format = "%Y-%m-%dT%H%M%S"
incremental = false
log_file = "/var/log/bbng.log"
transaction_log = "/var/log/bbng-tx.log"
transfer_timeout = 7200
transfer_stall_timeout = 300
parallel_volumes = 8
parallel_targets = 5
quiet = true
verbose = false
btrfs_debug = true

[global.retention]
min = "3d"
hourly = 12
daily = 30
weekly = 8
monthly = 24
yearly = 5

[global.notifications.email]
enabled = true
smtp_host = "smtp.example.net"
smtp_port = 465
smtp_user = "mailer"
smtp_password = "hunter2"
smtp_tls = "ssl"
from_addr = "bbng@example.net"
to_addrs = ["ops@example.net", "root@example.net"]
on_success = true
on_failure = true
timeout = 45

[global.notifications.webhook]
enabled = true
url = "https://hooks.example.net/bbng"
method = "PUT"
headers = {{ Authorization = "Bearer abc", X-Origin = "bbng" }}
on_success = true
on_failure = false
timeout = 20

[[volumes]]
path = "/home"
snapshot_prefix = "home-"
snapshot_dir = ".snaps-home"
enabled = true

[volumes.retention]
min = "2d"
daily = 14
keep = 0

[volumes.source_retention]
min = "12h"
hourly = 48

[[volumes.targets]]
path = "/backup/home"
require_mount = "/backup"
optional = true
compress = "zstd"
rate_limit = "10M"

[volumes.targets.retention]
min = "7d"
weekly = 12

[[volumes.targets]]
path = "{SSH}"
ssh_sudo = true
ssh_port = 2222
ssh_key = "/root/.ssh/backup_key"
ssh_auth_sock = "/run/agent.sock"
ssh_password_auth = false
skip_remote_lock = true
ssh_host_key_policy = "strict"

[[volumes.targets]]
path = "{RAW_SSH}"
ssh_sudo = true
encrypt = "gpg"
gpg_recipient = "ops@example.net"
gpg_keyring = "/root/.gnupg/backup.kbx"

[[volumes]]
path = "/"
source = "snapper"

[volumes.snapper]
config_name = "root"
include_types = ["single", "pre"]
exclude_cleanup = ["number"]
min_age = "2h"

[[volumes.targets]]
path = "/mnt/backup/root"
require_mount = "/mnt/backup"

[[volumes]]
path = "/home"
snapshot_prefix = "home2-"

[[volumes.targets]]
path = "/backup/home-second"
openssl_cipher = "aes-128-cbc"
encrypt = "none"
"""


def _load(tmp_path: Path, name: str, text: str):
    path = tmp_path / name
    path.write_text(text)
    config, warnings = load_config(path)
    assert not [w for w in warnings if "Unknown config key" in w], warnings
    return config


class Responder:
    """Answers the wizard's prompts by question text. A list answers the
    question in turn; anything else answers it every time; an unlisted
    question gets its default."""

    def __init__(self, **answers):
        self.answers = {k.replace("_", " "): v for k, v in answers.items()}
        self.asked: list[str] = []

    def _answer(self, message: str, default):
        self.asked.append(message)
        for key, value in self.answers.items():
            if key.lower() in message.lower():
                if isinstance(value, list):
                    assert value, f"ran out of answers for {message!r}"
                    return value.pop(0)
                return value
        return default

    def prompt(self, message, default=""):
        return self._answer(message, default)

    def prompt_bool(self, message, default=True):
        return self._answer(message, default)

    def prompt_int(self, message, default, min_val=0, max_val=100):
        return self._answer(message, default)

    def prompt_choice(self, message, choices, default=""):
        return self._answer(message, default or choices[0])

    def install(self, monkeypatch):
        for name in ("prompt", "prompt_bool", "prompt_int", "prompt_choice"):
            monkeypatch.setattr(config_cmd, name, getattr(self, name))


def _fields_equal(a, b, *, except_paths: set[str] = frozenset(), where=""):
    """Every dataclass field of ``a`` equals ``b``'s, recursively, except
    the named dotted paths. Returns the differing paths."""
    diffs = []
    if isinstance(a, types.SimpleNamespace):
        for name in vars(a):
            diffs += _fields_equal(
                getattr(a, name),
                getattr(b, name),
                except_paths=except_paths,
                where=f"{where}{name}.",
            )
    elif dataclasses.is_dataclass(a):
        for f in dataclasses.fields(a):
            path = f"{where}{f.name}"
            if path in except_paths:
                continue
            diffs += _fields_equal(
                getattr(a, f.name),
                getattr(b, f.name),
                except_paths=except_paths,
                where=path + ".",
            )
    elif isinstance(a, list) and a and dataclasses.is_dataclass(a[0]):
        if len(a) != len(b):
            return [f"{where}: {len(a)} vs {len(b)} entries"]
        for i, (x, y) in enumerate(zip(a, b)):
            diffs += _fields_equal(
                x, y, except_paths=except_paths, where=f"{where}{i}."
            )
    elif a != b:
        diffs.append(f"{where.rstrip('.')}: {a!r} -> {b!r}")
    return diffs


def test_every_loader_key_is_exercised():
    data = tomllib.loads(EVERY_KEY)
    assert set(data["global"]) >= loader._KNOWN_GLOBAL_KEYS
    notifications = data["global"]["notifications"]
    assert set(notifications["email"]) == loader._KNOWN_EMAIL_KEYS
    assert set(notifications["webhook"]) == loader._KNOWN_WEBHOOK_KEYS
    volume_keys = set().union(*(set(v) for v in data["volumes"]))
    assert volume_keys >= loader._KNOWN_VOLUME_KEYS
    target_keys = set().union(
        *(set(t) for v in data["volumes"] for t in v.get("targets", []))
    )
    assert target_keys >= loader._KNOWN_TARGET_KEYS
    assert set(data["volumes"][1]["snapper"]) == loader._KNOWN_SNAPPER_KEYS


class TestTheDetectionWizard:
    def _result(self):
        home = DetectedSubvolume(
            id=256,
            path="/home",
            mount_point="/home",
            classification=SubvolumeClass.USER_DATA,
        )
        root = DetectedSubvolume(
            id=5, path="/", mount_point="/", classification=SubvolumeClass.SYSTEM_ROOT
        )
        return DetectionResult(
            filesystems=[
                BtrfsMountInfo(
                    device="/dev/sda1",
                    mount_point="/home",
                    subvol_path="/home",
                    subvol_id=256,
                )
            ],
            subvolumes=[home, root],
            suggestions=[
                BackupSuggestion(subvolume=home, suggested_prefix="home-", priority=1),
                BackupSuggestion(subvolume=root, suggested_prefix="root-", priority=2),
            ],
        )

    def _snapper_root(self):
        cfg = types.SimpleNamespace(name="root", subvolume="/", path="/")
        return [cfg], {"/": cfg}

    def _run(self, tmp_path, monkeypatch, responder: Responder, existing: str):
        path = tmp_path / "config.toml"
        path.write_text(existing)
        responder.install(monkeypatch)
        monkeypatch.setattr(config_cmd, "find_config_file", lambda *_: str(path))
        monkeypatch.setattr(config_cmd, "find_btrbk_config", lambda: None)
        monkeypatch.setattr(config_cmd, "prompt_selection", lambda **k: [0, 1])
        monkeypatch.setattr(config_cmd, "prompt_snapshot_prefix", lambda d: d)
        monkeypatch.setattr(
            config_cmd, "_derive_require_mount", lambda p: str(Path(p).parent)
        )
        configs, path_map = self._snapper_root()
        scanner = mock.MagicMock()
        scanner.list_configs.return_value = configs
        monkeypatch.setattr("btrfs_backup_ng.snapper.SnapperScanner", lambda: scanner)
        monkeypatch.setattr(
            "btrfs_backup_ng.detection.reclassify_with_snapper", lambda r, c: path_map
        )
        rc = config_cmd._run_detection_wizard(self._result())
        assert rc == 0
        return path

    def test_declining_global_settings_keeps_every_global_option(
        self, tmp_path, monkeypatch
    ):
        """The same volumes and targets re-entered, global settings declined:
        the written file loads to exactly what was there."""
        responder = Responder(
            Target_path=[
                "/backup/home",
                SSH,
                RAW_SSH,
                "",
                "/mnt/backup/root",
                "",
            ],
            Add_another_target=True,
            Use_sudo=True,
            Require_mount_check=True,
            Use_snapper=True,
            Encrypt_this_raw_target="gpg",
            GPG_recipient="ops@example.net",
            GPG_keyring="/root/.gnupg/backup.kbx",
            Configure_global_settings=False,
            View_changes=False,
            What_would_you_like="save",
            Save_to=str(tmp_path / "config.toml"),
            Overwrite=True,
        )
        path = self._run(tmp_path, monkeypatch, responder, EVERY_KEY)
        before = _load(tmp_path, "before.toml", EVERY_KEY)
        after = _load(tmp_path, "after.toml", path.read_text())
        # The detection result offers /home and / once each; the second /home
        # of the existing file is not in the new configuration and is
        # reported as removed. Everything else is identical.
        assert [v.path for v in after.volumes] == ["/home", "/"]
        diffs = _fields_equal(
            types.SimpleNamespace(
                global_config=before.global_config, volumes=before.volumes[:2]
            ),
            types.SimpleNamespace(
                global_config=after.global_config, volumes=after.volumes
            ),
        )
        assert diffs == [], diffs

    def test_accepting_global_settings_replaces_only_what_was_asked(
        self, tmp_path, monkeypatch
    ):
        responder = Responder(
            Target_path=["/backup/home", "", "/mnt/backup/root", ""],
            Use_snapper=True,
            Require_mount_check=True,
            Configure_global_settings=True,
            Minimum_retention_period="9d",
            Configure_email_notifications=False,
            View_changes=False,
            What_would_you_like="save",
            Save_to=str(tmp_path / "config.toml"),
            Overwrite=True,
        )
        path = self._run(tmp_path, monkeypatch, responder, EVERY_KEY)
        after = _load(tmp_path, "after.toml", path.read_text())
        g = after.global_config
        # Asked: retention (whole) and the email gate, declined.
        assert g.retention.min == "9d" and g.retention.daily == 7
        assert g.notifications.email.enabled is False
        # Not asked in the detection wizard: kept.
        assert g.snapshot_dir == ".snaps"
        assert g.timestamp_format == "%Y-%m-%dT%H%M%S"
        assert g.incremental is False
        assert g.parallel_volumes == 8
        assert g.log_file == "/var/log/bbng.log"
        assert g.notifications.webhook.enabled is True
        assert g.notifications.webhook.headers == {
            "Authorization": "Bearer abc",
            "X-Origin": "bbng",
        }
        assert g.notifications.webhook.timeout == 20


class TestTheInitWizard:
    def _run(self, tmp_path, monkeypatch, responder: Responder, existing: str, **args):
        path = tmp_path / "config.toml"
        path.write_text(existing)
        responder.install(monkeypatch)
        monkeypatch.setattr(config_cmd.sys.stdin, "isatty", lambda: True)
        prefixes = iter(["home-", "root-", "home2-"])
        monkeypatch.setattr(
            config_cmd, "prompt_snapshot_prefix", lambda d: next(prefixes)
        )
        self.reports = []
        monkeypatch.setattr(config_cmd, "_show_carry_over", self.reports.append)
        monkeypatch.setattr(
            config_cmd, "_derive_require_mount", lambda p: str(Path(p).parent)
        )
        ns = types.SimpleNamespace(
            interactive=True, output=str(path), force=args.get("force", False)
        )
        assert config_cmd._init_config(ns) == 0
        return path

    def _answers(self, tmp_path, **extra):
        answers = dict(
            Snapshot_directory_name=".snaps",
            Timestamp_format="%Y-%m-%dT%H%M%S",
            Use_incremental=False,
            Log_file_path="/var/log/bbng.log",
            Transaction_log_path="/var/log/bbng-tx.log",
            Max_parallel_volumes=8,
            Max_parallel_targets=5,
            Minimum_retention_period="3d",
            Hourly_snapshots=12,
            Daily_snapshots=30,
            Weekly_snapshots=8,
            Monthly_snapshots=24,
            Yearly_snapshots=5,
            Configure_email_notifications=True,
            SMTP_host="smtp.example.net",
            SMTP_port=465,
            SMTP_security="ssl",
            SMTP_username="mailer",
            SMTP_password="hunter2",
            From_address="bbng@example.net",
            To_addresses="ops@example.net, root@example.net",
            Configure_webhook_notifications=True,
            Webhook_URL="https://hooks.example.net/bbng",
            HTTP_method="PUT",
            Notify_on_success=True,
            Notify_on_failure=[True, False],
            Volume_path=["/home", "/", "/home", ""],
            Target_path=[
                "/backup/home",
                SSH,
                RAW_SSH,
                "",
                "/mnt/backup/root",
                "",
                "/backup/home-second",
                "",
            ],
            Add_another_target=True,
            Add_another_volume=True,
            Use_sudo=True,
            Require_mount_check=True,
            Encrypt_this_raw_target="gpg",
            GPG_recipient="ops@example.net",
            GPG_keyring="/root/.gnupg/backup.kbx",
            Overwrite=True,
        )
        answers.update(extra)
        return Responder(**answers)

    def test_the_same_answers_leave_the_configuration_unchanged(
        self, tmp_path, monkeypatch
    ):
        """Every question answered as the file already says, every volume
        and target re-entered: the file loads to exactly what it was, the
        second /home included, and the snapper volume is still snapper."""
        path = self._run(tmp_path, monkeypatch, self._answers(tmp_path), EVERY_KEY)
        before = _load(tmp_path, "before.toml", EVERY_KEY)
        after = _load(tmp_path, "after.toml", path.read_text())
        # `config init -i` asks a prefix for every volume, the snapper one
        # included; that answer is written, and is the one difference.
        diffs = _fields_equal(before, after, except_paths={"volumes.1.snapshot_prefix"})
        assert diffs == [], diffs
        assert after.volumes[1].snapshot_prefix == "root-"
        assert after.volumes[1].source == "snapper"
        assert after.volumes[1].snapper is not None
        assert after.volumes[1].snapper.config_name == "root"
        assert [v.path for v in after.volumes] == ["/home", "/", "/home"]
        assert after.volumes[2].targets[0].openssl_cipher == "aes-128-cbc"

    def test_a_changed_answer_wins_and_is_reported(self, tmp_path, monkeypatch):
        responder = self._answers(tmp_path, Max_parallel_volumes=2, Use_sudo=False)
        path = self._run(tmp_path, monkeypatch, responder, EVERY_KEY)
        after = _load(tmp_path, "after.toml", path.read_text())
        assert after.global_config.parallel_volumes == 2
        assert after.volumes[0].targets[1].ssh_sudo is False
        # The raw+ssh target was never asked about sudo: kept.
        assert after.volumes[0].targets[2].ssh_sudo is True
        (report,) = self.reports
        assert "global.parallel_volumes: 8 -> 2" in report.replaced
        assert f"target {SSH}: ssh_sudo: True -> False" in report.replaced

    def test_force_skips_the_question_not_the_carry_over(self, tmp_path, monkeypatch):
        responder = self._answers(tmp_path)

        def never(*a, **k):
            raise AssertionError("--force must not ask whether to overwrite")

        responder.answers[" overwrite"] = None
        path = tmp_path / "config.toml"
        path.write_text(EVERY_KEY)
        responder.install(monkeypatch)
        real_prompt_bool = responder.prompt_bool

        def prompt_bool(message, default=True):
            if "Overwrite" in message:
                never()
            return real_prompt_bool(message, default)

        monkeypatch.setattr(config_cmd, "prompt_bool", prompt_bool)
        monkeypatch.setattr(config_cmd.sys.stdin, "isatty", lambda: True)
        monkeypatch.setattr(config_cmd, "prompt_snapshot_prefix", lambda d: d)
        monkeypatch.setattr(
            config_cmd, "_derive_require_mount", lambda p: str(Path(p).parent)
        )
        ns = types.SimpleNamespace(interactive=True, output=str(path), force=True)
        assert config_cmd._init_config(ns) == 0
        after = _load(tmp_path, "after.toml", path.read_text())
        assert after.volumes[0].targets[1].ssh_port == 2222
        assert after.volumes[0].targets[1].ssh_host_key_policy == "strict"
        assert after.volumes[0].targets[0].require_mount == "/backup"
        assert after.global_config.notifications.webhook.timeout == 20

    def test_declining_a_gate_removes_the_block_and_says_so(
        self, tmp_path, monkeypatch
    ):
        """The email gate is a question about the email block: answering no
        means no email, and the old block is named as removed."""
        responder = self._answers(
            tmp_path, Configure_email_notifications=False, Notify_on_failure=False
        )
        path = self._run(tmp_path, monkeypatch, responder, EVERY_KEY)
        after = _load(tmp_path, "after.toml", path.read_text())
        assert after.global_config.notifications.email.enabled is False
        assert after.global_config.notifications.webhook.enabled is True
        (report,) = self.reports
        assert "global.notifications.email = {...} (removed)" in report.replaced


class TestCarryOverDirectly:
    def _asked_for(self, *volumes: tuple[str, ...]) -> Asked:
        asked = Asked()
        asked.global_key("snapshot_dir")
        for vi, target_keys in enumerate(volumes):
            asked.volume(vi, "path", "snapshot_prefix")
            asked.target(vi, 0, "path", *target_keys)
        return asked

    def test_two_volumes_sharing_a_path_keep_their_own_options(self, tmp_path):
        new = (
            '[global]\nsnapshot_dir = ".snapshots"\n\n'
            '[[volumes]]\npath = "/home"\nsnapshot_prefix = "home-"\n\n'
            '[[volumes.targets]]\npath = "/backup/home"\n\n'
            '[[volumes]]\npath = "/"\nsnapshot_prefix = "root-"\n\n'
            '[[volumes.targets]]\npath = "/mnt/backup/root"\n\n'
            '[[volumes]]\npath = "/home"\nsnapshot_prefix = "home2-"\n\n'
            '[[volumes.targets]]\npath = "/backup/home-second"\n'
        )
        merged, report = carry_over_existing(
            new, EVERY_KEY, self._asked_for((), (), ())
        )
        data = tomllib.loads(merged)
        assert [v["path"] for v in data["volumes"]] == ["/home", "/", "/home"]
        first, _root, second = data["volumes"]
        assert first["targets"][0]["require_mount"] == "/backup"
        assert first["snapshot_dir"] == ".snaps-home"
        assert second["targets"][0]["openssl_cipher"] == "aes-128-cbc"
        assert "snapshot_dir" not in second
        # The first /home's two remote targets are not in the new
        # configuration and are named; nothing of the second /home is.
        assert len(report.removed) == 2
        assert all("of volume /home" in line for line in report.removed)

    def test_an_unasked_default_the_wizard_wrote_is_overridden(self, tmp_path):
        """The generator writes defaults for keys the detection wizard never
        asks; the existing value must win over such a default."""
        new = (
            '[global]\nsnapshot_dir = ".snapshots"\ntimestamp_format = "%Y%m%d"\n'
            'parallel_volumes = 2\n\n[global.retention]\nmin = "1d"\ndaily = 7\n\n'
            '[[volumes]]\npath = "/home"\n\n[[volumes.targets]]\npath = "/backup/home"\n'
        )
        asked = Asked()
        asked.volume(0, "path")
        asked.target(0, 0, "path")
        merged, report = carry_over_existing(new, EVERY_KEY, asked)
        g = tomllib.loads(merged)["global"]
        assert g["snapshot_dir"] == ".snaps"
        assert g["timestamp_format"] == "%Y-%m-%dT%H%M%S"
        assert g["parallel_volumes"] == 8
        assert g["retention"] == {
            "min": "3d",
            "hourly": 12,
            "daily": 30,
            "weekly": 8,
            "monthly": 24,
            "yearly": 5,
        }
        assert any("global.retention" in line for line in report.kept)

    def test_plain_content_is_authoritative_for_what_it_sets(self):
        """A caller without a record (the btrbk import) replaces the keys it
        sets and keeps the rest."""
        new = (
            '[global]\nsnapshot_dir = ".imported"\n\n[global.retention]\n'
            'min = "all"\n\n[[volumes]]\npath = "/home"\n\n'
            '[[volumes.targets]]\npath = "/backup/home"\n'
        )
        merged, _ = carry_over_existing(new, EVERY_KEY)
        data = tomllib.loads(merged)
        assert data["global"]["snapshot_dir"] == ".imported"
        assert data["global"]["retention"] == {"min": "all"}
        assert data["global"]["log_file"] == "/var/log/bbng.log"
        assert data["global"]["notifications"]["webhook"]["timeout"] == 20
        assert data["volumes"][0]["targets"][0]["require_mount"] == "/backup"

    def test_the_record_refuses_a_key_no_wizard_asks(self):
        asked = Asked()
        with pytest.raises(AssertionError):
            asked.target(0, 0, "ssh_port")
        with pytest.raises(AssertionError):
            asked.global_key("transfer_timeout")


class TestTheDiffSummaryReadsWhatWouldBeWritten:
    def test_a_carried_retention_is_not_a_change(self, capsys):
        new = (
            '[global]\nsnapshot_dir = ".snapshots"\n\n[global.retention]\n'
            'min = "1d"\ndaily = 7\n\n[[volumes]]\npath = "/home"\n\n'
            '[[volumes.targets]]\npath = "/backup/home"\n'
        )
        asked = Asked()
        asked.volume(0, "path")
        asked.target(0, 0, "path")
        compared, _ = carry_over_existing(new, EVERY_KEY, asked)
        _show_config_diff_summary(EVERY_KEY, compared)
        out = capsys.readouterr().out
        assert "Modify retention" not in out
        assert "Remove email notifications" not in out

    def test_an_answered_retention_is(self, capsys):
        new = (
            '[global]\nsnapshot_dir = ".snapshots"\n\n[global.retention]\n'
            'min = "1d"\ndaily = 7\n\n[[volumes]]\npath = "/home"\n\n'
            '[[volumes.targets]]\npath = "/backup/home"\n'
        )
        asked = Asked()
        asked.global_key("retention")
        asked.notification("email")
        asked.volume(0, "path")
        asked.target(0, 0, "path")
        compared, _ = carry_over_existing(new, EVERY_KEY, asked)
        _show_config_diff_summary(EVERY_KEY, compared)
        out = capsys.readouterr().out
        assert "daily: 30 -> 7" in out
        assert "- Remove email notifications" in out


def test_wizard_config_carries_its_record():
    asked = Asked()
    wizard = WizardConfig("[global]\n", asked)
    assert wizard.asked is asked and wizard.content == "[global]\n"
