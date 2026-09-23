"""Saving the wizard's configuration over an existing one keeps what it does
not ask about.

The wizard builds its configuration from its prompts alone and writes only
the options it asks about. Saved over an existing file it used to replace it
outright, so a target's `ssh_host_key_policy = "strict"` became trust on first
use, its `ssh_key` and `ssh_port` vanished and the next run could not
authenticate, and an `optional` drive became a required one -- behind an
"Overwrite?" prompt whose default summary showed none of it. Volumes and
targets are now matched by path and every option the wizard does not ask
about is carried over; its answers still win for everything it does ask; and
what cannot be carried (a volume or target the new configuration does not
have) is named before the prompt.
"""

from __future__ import annotations

import dataclasses
import glob
import tomllib
import types

import pytest

from btrfs_backup_ng.__util__ import dump_toml
from btrfs_backup_ng.cli import config_cmd
from btrfs_backup_ng.cli.config_cmd import (
    _WIZARD_GLOBAL_KEYS,
    _WIZARD_TARGET_KEYS,
    _WIZARD_VOLUME_KEYS,
    _generate_config_from_wizard,
    carry_over_existing,
)
from btrfs_backup_ng.config.loader import load_config
from btrfs_backup_ng.config.schema import GlobalConfig, TargetConfig, VolumeConfig

SSH_TARGET = "ssh://backup@nas:/backups/home"

# One non-default value for every TargetConfig option the wizard does not ask
# about. A field added to the schema fails
# test_every_target_field_is_either_asked_or_carried until it is listed here
# or in _WIZARD_TARGET_KEYS -- the writer cannot quietly fall behind again.
TARGET_VALUES = {
    "ssh_port": 2222,
    "ssh_key": "/root/.ssh/backup_key",
    "ssh_auth_sock": "/run/agent.sock",
    "ssh_password_auth": False,
    "skip_remote_lock": True,
    "ssh_host_key_policy": "strict",
    "compress": "zstd",
    "rate_limit": "10M",
    "optional": True,
}
# Asked-about for raw targets only, and not applicable to an ssh:// target.
TARGET_NOT_APPLICABLE_TO_SSH = {
    "encrypt",
    "gpg_recipient",
    "gpg_keyring",
    "openssl_cipher",
}


def _existing(
    tmp_path, target_extra: str = "", global_extra: str = "", more: str = ""
) -> str:
    target_lines = "\n".join(
        f"{k} = {'true' if v is True else 'false' if v is False else repr(v).replace(chr(39), chr(34))}"
        for k, v in TARGET_VALUES.items()
    )
    return (
        "[global]\n"
        'snapshot_dir = ".snapshots"\n'
        'timestamp_format = "%Y%m%d-%H%M%S"\n'
        "incremental = true\n"
        "parallel_volumes = 2\n"
        "parallel_targets = 3\n"
        f"{global_extra}"
        "\n[global.retention]\n"
        'min = "1d"\n'
        "daily = 7\n"
        "\n[[volumes]]\n"
        'path = "/home"\n'
        'snapshot_prefix = "home-"\n'
        "\n[volumes.source_retention]\n"
        "hourly = 48\n"
        "\n[[volumes.targets]]\n"
        f'path = "{SSH_TARGET}"\n'
        "ssh_sudo = true\n"
        f"{target_lines}\n"
        f"{target_extra}"
        "\n[volumes.targets.retention]\n"
        "daily = 30\n"
        f"{more}"
    )


def _wizard(**overrides) -> str:
    """What the wizard writes when the operator answers for /home and its ssh target."""
    data = {
        "snapshot_dir": ".snapshots",
        "timestamp_format": "%Y%m%d-%H%M%S",
        "incremental": True,
        "log_file": "",
        "transaction_log": "",
        "parallel_volumes": 2,
        "parallel_targets": 3,
        "retention": {
            "min": "1d",
            "hourly": 24,
            "daily": 7,
            "weekly": 4,
            "monthly": 12,
            "yearly": 0,
        },
        "volumes": [
            {
                "path": "/home",
                "snapshot_prefix": "home-",
                "targets": [{"path": SSH_TARGET, "ssh_sudo": True}],
            }
        ],
    }
    data.update(overrides)
    return _generate_config_from_wizard(data)


def _load(tmp_path, name, text):
    path = tmp_path / name
    path.write_text(text)
    config, _ = load_config(path)
    return config


class TestTheSerializer:
    @pytest.mark.parametrize("example", sorted(glob.glob("examples/*.toml")))
    def test_every_shipped_example_round_trips(self, example):
        with open(example, "rb") as f:
            data = tomllib.load(f)
        assert tomllib.loads(dump_toml(data)) == data

    def test_strings_that_need_escaping_round_trip(self):
        data = {
            "global": {"log_file": 'C:\\odd "path"\t\n'},
            "volumes": [{"path": "/a b"}],
        }
        assert tomllib.loads(dump_toml(data)) == data

    def test_an_unwritable_value_is_refused(self):
        with pytest.raises(TypeError):
            dump_toml({"x": object()})


class TestWhatIsKept:
    def test_every_target_field_is_either_asked_or_carried(self):
        fields = {f.name for f in dataclasses.fields(TargetConfig)}
        # retention is carried as its own table (see the next test)
        covered = set(TARGET_VALUES) | _WIZARD_TARGET_KEYS | {"retention"}
        assert fields - covered == set(), (
            "a TargetConfig option is neither asked by the wizard nor exercised "
            "by this test's carry-over values"
        )

    def test_every_option_the_wizard_does_not_ask_about_survives(self, tmp_path):
        existing = _existing(tmp_path)
        merged, _report = carry_over_existing(_wizard(), existing)
        old = _load(tmp_path, "old.toml", existing).volumes[0].targets[0]
        new = _load(tmp_path, "new.toml", merged).volumes[0].targets[0]
        for field in dataclasses.fields(TargetConfig):
            if field.name in TARGET_NOT_APPLICABLE_TO_SSH:
                continue
            assert getattr(new, field.name) == getattr(old, field.name), field.name

    def test_the_wizard_alone_would_have_lost_them(self, tmp_path):
        # The defect this fixes, pinned: without the carry-over these differ.
        old = _load(tmp_path, "old.toml", _existing(tmp_path)).volumes[0].targets[0]
        bare = _load(tmp_path, "bare.toml", _wizard()).volumes[0].targets[0]
        assert bare.ssh_host_key_policy != old.ssh_host_key_policy
        assert bare.optional != old.optional
        assert bare.ssh_key != old.ssh_key

    def test_volume_and_global_options_survive(self, tmp_path):
        existing = _existing(
            tmp_path, global_extra="quiet = true\ntransfer_timeout = 3600\n"
        )
        merged, _ = carry_over_existing(_wizard(), existing)
        config = _load(tmp_path, "m.toml", merged)
        assert config.global_config.quiet is True
        assert config.global_config.transfer_timeout == 3600
        assert config.volumes[0].source_retention is not None
        assert config.volumes[0].source_retention.hourly == 48

    def test_the_report_names_what_was_kept(self, tmp_path):
        _, report = carry_over_existing(_wizard(), _existing(tmp_path))
        kept = "\n".join(report.kept)
        assert f"target {SSH_TARGET}: ssh_host_key_policy = strict" in kept
        assert f"target {SSH_TARGET}: optional = True" in kept


class TestTheWizardsAnswersWin:
    def test_a_blank_log_file_is_not_brought_back(self, tmp_path):
        existing = _existing(tmp_path, global_extra='log_file = "/var/log/old.log"\n')
        merged, report = carry_over_existing(_wizard(log_file=""), existing)
        assert "log_file" not in tomllib.loads(merged)["global"]
        assert any("global.log_file" in line for line in report.replaced)

    def test_an_answered_option_takes_the_new_value(self, tmp_path):
        merged, _ = carry_over_existing(
            _wizard(parallel_volumes=5), _existing(tmp_path)
        )
        assert tomllib.loads(merged)["global"]["parallel_volumes"] == 5

    def test_declined_email_is_not_brought_back(self, tmp_path):
        more = (
            "\n[global.notifications.email]\nenabled = true\n"
            'smtp_host = "smtp.old"\nto_addrs = ["a@b.c"]\n'
        )
        existing = _existing(tmp_path).replace(
            "\n[global.retention]", more + "\n[global.retention]", 1
        )
        merged, report = carry_over_existing(_wizard(), existing)
        assert "notifications" not in tomllib.loads(merged)["global"]
        assert any("notifications.email" in line for line in report.replaced)

    def test_a_retention_keep_does_not_override_the_answered_buckets(self, tmp_path):
        existing = _existing(tmp_path).replace(
            'min = "1d"\ndaily = 7\n', 'min = "1d"\nkeep = 30\n'
        )
        merged, report = carry_over_existing(_wizard(), existing)
        assert "keep" not in tomllib.loads(merged)["global"]["retention"]
        assert any("keep" in line for line in report.replaced)


class TestWhatCannotBeCarried:
    def test_a_target_the_new_configuration_lacks_is_named_with_its_options(
        self, tmp_path
    ):
        more = '\n[[volumes.targets]]\npath = "/mnt/old"\nrequire_mount = "/mnt"\n'
        _, report = carry_over_existing(_wizard(), _existing(tmp_path, more=more))
        removed = "\n".join(report.removed)
        assert "target /mnt/old of volume /home" in removed
        assert "require_mount = /mnt" in removed

    def test_a_volume_the_new_configuration_lacks_is_named(self, tmp_path):
        more = '\n[[volumes]]\npath = "/data"\n\n[[volumes.targets]]\npath = "/mnt/d"\n'
        merged, report = carry_over_existing(_wizard(), _existing(tmp_path, more=more))
        assert any(
            "volume /data and its 1 target(s)" in line for line in report.removed
        )
        assert [v["path"] for v in tomllib.loads(merged)["volumes"]] == ["/home"]

    def test_a_target_whose_path_changed_takes_nothing_from_the_old_one(self, tmp_path):
        wizard = _wizard(
            volumes=[
                {
                    "path": "/home",
                    "snapshot_prefix": "home-",
                    "targets": [{"path": "ssh://backup@nas:/elsewhere"}],
                }
            ]
        )
        merged, report = carry_over_existing(wizard, _existing(tmp_path))
        target = tomllib.loads(merged)["volumes"][0]["targets"][0]
        assert "ssh_key" not in target
        assert any(SSH_TARGET in line for line in report.removed)


class TestThroughTheSavePaths:
    """The real save paths, with the prompts answered."""

    def _answer(self, monkeypatch, *, overwrite: bool):
        monkeypatch.setattr(config_cmd, "prompt_bool", lambda *a, **k: overwrite)

    def test_config_init_interactive_over_an_existing_output(
        self, tmp_path, monkeypatch
    ):
        target = tmp_path / "config.toml"
        target.write_text(_existing(tmp_path))
        monkeypatch.setattr(config_cmd.sys.stdin, "isatty", lambda: True)
        monkeypatch.setattr(config_cmd, "_run_interactive_wizard", _wizard)
        self._answer(monkeypatch, overwrite=True)
        args = types.SimpleNamespace(interactive=True, output=str(target), force=False)
        assert config_cmd._init_config(args) == 0
        written = (
            _load(tmp_path, "check.toml", target.read_text()).volumes[0].targets[0]
        )
        assert written.ssh_host_key_policy == "strict"
        assert written.optional is True

    def test_save_wizard_config_over_an_existing_file(self, tmp_path, monkeypatch):
        target = tmp_path / "config.toml"
        target.write_text(_existing(tmp_path))
        monkeypatch.setattr(config_cmd, "prompt_choice", lambda *a, **k: "save")
        monkeypatch.setattr(config_cmd, "prompt", lambda *a, **k: str(target))
        self._answer(monkeypatch, overwrite=True)
        assert config_cmd._save_wizard_config(_wizard()) == 0
        written = (
            _load(tmp_path, "check.toml", target.read_text()).volumes[0].targets[0]
        )
        assert written.ssh_key == "/root/.ssh/backup_key"

    def test_declining_writes_nothing(self, tmp_path, monkeypatch):
        target = tmp_path / "config.toml"
        original = _existing(tmp_path)
        target.write_text(original)
        monkeypatch.setattr(config_cmd, "prompt_choice", lambda *a, **k: "save")
        monkeypatch.setattr(config_cmd, "prompt", lambda *a, **k: str(target))
        self._answer(monkeypatch, overwrite=False)
        config_cmd._save_wizard_config(_wizard())
        assert target.read_text() == original

    def test_an_unreadable_existing_file_is_said_and_left_to_the_prompt(
        self, tmp_path, monkeypatch, capsys
    ):
        target = tmp_path / "config.toml"
        target.write_text("this is [not toml")
        monkeypatch.setattr(config_cmd, "prompt_choice", lambda *a, **k: "save")
        monkeypatch.setattr(config_cmd, "prompt", lambda *a, **k: str(target))
        self._answer(monkeypatch, overwrite=True)
        assert config_cmd._save_wizard_config(_wizard()) == 0
        assert "nothing can be kept from it" in " ".join(
            capsys.readouterr().out.split()
        )
        assert tomllib.loads(target.read_text())["volumes"][0]["path"] == "/home"

    def test_every_wizard_save_asks_through_the_carry_over(self):
        """Each function that saves a wizard configuration over an existing
        file asks through _confirm_overwrite, so none can replace a file
        without carrying its options over and saying what it drops."""
        import inspect

        for fn in (
            config_cmd._init_config,
            config_cmd._save_wizard_config,
            config_cmd._run_detection_wizard,
        ):
            source = inspect.getsource(fn)
            assert "_confirm_overwrite(" in source, fn.__name__
            assert 'Overwrite?", False)' not in source, fn.__name__


def test_the_asked_sets_name_real_schema_fields():
    global_fields = {f.name for f in dataclasses.fields(GlobalConfig)}
    volume_fields = {f.name for f in dataclasses.fields(VolumeConfig)}
    target_fields = {f.name for f in dataclasses.fields(TargetConfig)}
    assert _WIZARD_GLOBAL_KEYS <= global_fields
    assert _WIZARD_VOLUME_KEYS <= volume_fields | {"source"}
    assert _WIZARD_TARGET_KEYS <= target_fields
