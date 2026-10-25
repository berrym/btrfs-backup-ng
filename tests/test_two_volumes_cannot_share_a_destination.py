"""Two volumes with one snapshot_prefix and one destination do not load.

The shape from the report: two subvolumes, both configured with
``snapshot_prefix = "home-"``, both sent to one raw target. The two produce the
same snapshot names in the same run. The destination holds one snapshot per
name, so whichever lands second is refused as another source's, or, where the
record of the first cannot say whose it is, taken for the second and never
sent, while both volumes report success. The loader now refuses such a config
by name: the two volumes, the shared prefix, the destination, and the remedy.

The destination is compared as it will be reached, not as it was spelled: a
local path resolved on this machine, a remote one by store, host, port and
normalised path. Nothing touches the network at load; a remote alias through a
symlink is the raw store's business at commit.
"""

from __future__ import annotations

import argparse
import os

import pytest

from btrfs_backup_ng.config.loader import ConfigError, load_config


def _volume(path, target, prefix=None, **keys):
    lines = [f'path = "{path}"']
    if prefix is not None:
        lines.append(f'snapshot_prefix = "{prefix}"')
    for key, value in keys.items():
        if isinstance(value, bool):
            lines.append(f"{key} = {'true' if value else 'false'}")
        else:
            lines.append(f"{key} = {value}")
    target_lines = [f'path = "{target}"'] if isinstance(target, str) else target
    return (
        "[[volumes]]\n"
        + "\n".join(lines)
        + "\n\n[[volumes.targets]]\n"
        + "\n".join(target_lines)
        + "\n"
    )


def _load(tmp_path, *volumes):
    path = tmp_path / "config.toml"
    path.write_text("\n".join(volumes))
    return load_config(path)


def _refused(tmp_path, *volumes) -> str:
    with pytest.raises(ConfigError) as caught:
        _load(tmp_path, *volumes)
    return str(caught.value)


class TestTwoVolumesSharingNamesAndADestinationAreRefused:
    def test_the_shape_from_the_report_does_not_load(self, tmp_path):
        message = _refused(
            tmp_path,
            _volume("/home", "raw:///mnt/nas/backups", prefix="home-"),
            _volume("/srv/home", "raw:///mnt/nas/backups", prefix="home-"),
        )
        assert "'/home'" in message and "'/srv/home'" in message
        assert "'home-<timestamp>'" in message
        assert "'raw:///mnt/nas/backups'" in message
        assert "snapshot_prefix" in message
        assert "not backed up" in message

    @pytest.mark.parametrize(
        "target",
        [
            "/mnt/backup",
            "raw:///mnt/backup",
            "ssh://backup@nas:/backups",
            "raw+ssh://backup@nas:/backups",
        ],
    )
    def test_every_destination_that_files_snapshots_by_name_is_covered(
        self, tmp_path, target
    ):
        message = _refused(
            tmp_path,
            _volume("/home", target, prefix="data-"),
            _volume("/srv", target, prefix="data-"),
        )
        assert f"'{target}'" in message

    def test_a_derived_prefix_is_the_effective_one(self, tmp_path):
        """/home derives "home-"; a volume that spells the same prefix collides."""
        message = _refused(
            tmp_path,
            _volume("/home", "/mnt/backup"),
            _volume("/srv/home", "/mnt/backup", prefix="home-"),
        )
        assert "'/home'" in message and "'/srv/home'" in message

    def test_two_derived_prefixes_can_collide(self, tmp_path):
        """/a-b and /a/b both derive "a-b-"."""
        message = _refused(
            tmp_path,
            _volume("/a-b", "/mnt/backup"),
            _volume("/a/b", "/mnt/backup"),
        )
        assert "'a-b-<timestamp>'" in message

    def test_an_empty_prefix_shared_is_refused_too(self, tmp_path):
        message = _refused(
            tmp_path,
            _volume("/home", "/mnt/backup", prefix=""),
            _volume("/srv", "/mnt/backup", prefix=""),
        )
        assert "'<timestamp>'" in message

    def test_a_disabled_volume_is_still_refused(self, tmp_path):
        """The trap is in the file; re-enabling the volume must not spring it."""
        _refused(
            tmp_path,
            _volume("/home", "/mnt/backup", prefix="home-"),
            _volume("/srv", "/mnt/backup", prefix="home-", enabled=False),
        )

    def test_the_message_names_the_colliding_pair_only(self, tmp_path):
        message = _refused(
            tmp_path,
            _volume("/home", "/mnt/backup", prefix="home-"),
            _volume("/var/log", "/mnt/backup", prefix="logs-"),
            _volume("/srv", "/mnt/backup", prefix="home-"),
        )
        assert "'/home'" in message and "'/srv'" in message
        assert "/var/log" not in message

    def test_a_collision_on_a_second_target_is_found(self, tmp_path):
        first = _volume("/home", "/mnt/one", prefix="home-")
        first += '\n[[volumes.targets]]\npath = "/mnt/two"\n'
        message = _refused(tmp_path, first, _volume("/srv", "/mnt/two", prefix="home-"))
        assert "'/mnt/two'" in message


class TestWhatIsNotACollisionStillLoads:
    def test_distinct_prefixes_on_one_destination(self, tmp_path):
        config, _ = _load(
            tmp_path,
            _volume("/home", "/mnt/backup", prefix="home-"),
            _volume("/srv", "/mnt/backup", prefix="srv-"),
        )
        assert len(config.volumes) == 2

    def test_one_prefix_on_distinct_destinations(self, tmp_path):
        config, _ = _load(
            tmp_path,
            _volume("/home", "/mnt/backup/home", prefix="data-"),
            _volume("/srv", "/mnt/backup/srv", prefix="data-"),
        )
        assert len(config.volumes) == 2

    def test_a_raw_store_and_a_btrfs_store_at_one_path_are_two_destinations(
        self, tmp_path
    ):
        """A raw store files streams, a btrfs one receives subvolumes; neither
        lists the other's, so the names do not meet."""
        config, _ = _load(
            tmp_path,
            _volume("/home", "/mnt/backup", prefix="data-"),
            _volume("/srv", "raw:///mnt/backup", prefix="data-"),
        )
        assert len(config.volumes) == 2

    def test_a_snapper_volume_is_not_compared(self, tmp_path):
        """A snapper layout is keyed by snapshot number, not by name."""
        config, _ = _load(
            tmp_path,
            _volume("/home", "/mnt/backup", prefix="home-"),
            _volume("/srv", "/mnt/backup", prefix="home-", source='"snapper"'),
        )
        assert config.volumes[1].is_snapper_source()

    def test_one_volume_listed_twice_is_the_duplicate_warning_not_this(self, tmp_path):
        config, warnings = _load(
            tmp_path,
            _volume("/home", "/mnt/backup", prefix="home-"),
            _volume("/home", "/mnt/backup", prefix="home-"),
        )
        assert len(config.volumes) == 2
        assert any("duplicate volume" in w.lower() for w in warnings)

    def test_the_sample_config_still_loads(self, config_file):
        config, _ = load_config(config_file)
        assert len(config.volumes) == 2


class TestTheDestinationIsComparedAsItWillBeReached:
    def test_a_trailing_slash_is_the_same_local_directory(self, tmp_path):
        _refused(
            tmp_path,
            _volume("/home", "/mnt/backup/", prefix="data-"),
            _volume("/srv", "/mnt/backup", prefix="data-"),
        )

    def test_a_symlink_to_the_same_local_directory(self, tmp_path):
        real = tmp_path / "real"
        real.mkdir()
        alias = tmp_path / "alias"
        os.symlink(real, alias)
        message = _refused(
            tmp_path,
            _volume("/home", str(real), prefix="data-"),
            _volume("/srv", str(alias), prefix="data-"),
        )
        assert f"'{real}'" in message and f"(also written '{alias}')" in message

    def test_the_user_the_separator_colon_and_a_trailing_slash_do_not_matter(
        self, tmp_path
    ):
        _refused(
            tmp_path,
            _volume("/home", "ssh://alice@nas:/backups/", prefix="data-"),
            _volume("/srv", "ssh://nas/backups", prefix="data-"),
        )

    def test_the_host_is_compared_as_ssh_sees_it(self, tmp_path):
        _refused(
            tmp_path,
            _volume("/home", "ssh://NAS:/backups", prefix="data-"),
            _volume("/srv", "ssh://nas:/backups", prefix="data-"),
        )

    def test_a_port_in_the_url_and_the_same_port_as_an_option_are_one_host(
        self, tmp_path
    ):
        _refused(
            tmp_path,
            _volume("/home", "ssh://nas:2222/backups", prefix="data-"),
            _volume(
                "/srv",
                ['path = "ssh://nas:/backups"', "ssh_port = 2222"],
                prefix="data-",
            ),
        )

    def test_different_ports_are_different_hosts(self, tmp_path):
        config, _ = _load(
            tmp_path,
            _volume("/home", "ssh://nas:2222/backups", prefix="data-"),
            _volume("/srv", "ssh://nas:/backups", prefix="data-"),
        )
        assert len(config.volumes) == 2

    def test_different_hosts_with_one_path_are_different_destinations(self, tmp_path):
        config, _ = _load(
            tmp_path,
            _volume("/home", "ssh://nas:/backups", prefix="data-"),
            _volume("/srv", "ssh://vault:/backups", prefix="data-"),
        )
        assert len(config.volumes) == 2

    def test_a_raw_remote_and_a_btrfs_remote_at_one_path_are_two_destinations(
        self, tmp_path
    ):
        config, _ = _load(
            tmp_path,
            _volume("/home", "ssh://nas:/backups", prefix="data-"),
            _volume("/srv", "raw+ssh://nas:/backups", prefix="data-"),
        )
        assert len(config.volumes) == 2


class TestTheRefusalReachesTheOperator:
    def test_config_validate_reports_it_and_exits_one(self, tmp_path, capsys):
        from btrfs_backup_ng.cli.config_cmd import _validate_config

        path = tmp_path / "config.toml"
        path.write_text(
            _volume("/home", "raw:///mnt/nas/backups", prefix="home-")
            + _volume("/srv/home", "raw:///mnt/nas/backups", prefix="home-")
        )
        rc = _validate_config(argparse.Namespace(config=str(path)))
        out = capsys.readouterr().out
        assert rc == 1
        assert "Configuration error:" in out
        assert "'/home'" in out and "'/srv/home'" in out
        assert "snapshot_prefix" in out
