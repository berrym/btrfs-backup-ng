"""Legacy mode (`btrfs-backup-ng SOURCE DEST`): its backup locations must exist,
and --snapshot-folder finally places snapshots where it says.

Two defects, one commit each way:

The creation half. `prepare_source_endpoint` built the snapshot tree with
parents=True BEFORE anyone looked at the source, so -- reproduced on real
btrfs with the shipped CLI -- a missing source got a tree of directories
built beside it before the run failed, and an absolute -f folder was built
wherever it pointed, exit 0. A backup location is a statement that something
is there: the source and an absolute folder must exist, and only what lies
below the source is created, one component at a time.

The placement half. -f computed a directory and set the endpoint's `path`,
but never passed `snapshot_folder`, so Endpoint.snapshot() fell back to its
own default and every legacy-mode snapshot since 1573551 landed in
<source>/.snapshots regardless of -f. The default is now that directory
outright, so users who never passed -f see no change; -f is passed through
so it is honoured; and a user whose chain sits in <source>/.snapshots while
-f names an empty folder is REFUSED, with three remedies, rather than handed
a silent full send from a cron job -- `--accept-full-send` is the third and
is needed at most once.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__, _legacy_main


@pytest.fixture
def isolated_endpoint(monkeypatch):
    """The endpoint's own checks are covered elsewhere; here only the logic
    in front of them is under test. Records the kwargs the endpoint got."""
    created = MagicMock()
    seen = {}

    def choose(spec, kwargs, source=False):
        seen["spec"] = spec
        seen["kwargs"] = dict(kwargs)
        return created

    monkeypatch.setattr(_legacy_main.endpoint, "choose_endpoint", choose)
    created.seen = seen
    return created


def _options(source, **extra):
    opts = {
        "source": str(source),
        "convert_rw": False,
        "sync": False,
        "btrfs_debug": False,
        "ssh_opt": [],
        "ssh_sudo": False,
        "snapshot_prefix": "box-",
    }
    opts.update(extra)
    return opts


def _chain(folder, prefix="box-", n=2):
    folder.mkdir(parents=True, exist_ok=True)
    for i in range(n):
        (folder / f"{prefix}2026010{i + 1}-000000").mkdir()


class TestTheSourceIsLookedAtFirst:
    def test_a_missing_source_is_refused_and_nothing_is_built_beside_it(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "data" / "home"
        with pytest.raises(__util__.AbortError) as e:
            _legacy_main.prepare_source_endpoint(_options(source))
        assert "Source" in str(e.value) and "Nothing was created" in str(e.value)
        assert not (tmp_path / "data").exists()
        assert sorted(p.name for p in tmp_path.iterdir()) == []
        isolated_endpoint.prepare.assert_not_called()

    def test_a_missing_source_with_an_absolute_folder_creates_nothing_either(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "gone"
        folder = tmp_path / "never"
        with pytest.raises(__util__.AbortError, match="Source"):
            _legacy_main.prepare_source_endpoint(
                _options(source, snapshot_folder=str(folder))
            )
        assert not folder.exists()


class TestTheDefaultIsDotSnapshotsInsideTheSource:
    def test_the_default_folder_is_created_under_the_source_and_passed_through(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "home"
        source.mkdir()
        _legacy_main.prepare_source_endpoint(_options(source))
        expected = (source / ".snapshots").resolve()
        assert expected.is_dir()
        kwargs = isolated_endpoint.seen["kwargs"]
        assert kwargs["path"] == expected
        assert kwargs["snapshot_folder"] == str(expected), (
            "snapshot_folder not passed: the endpoint would snapshot elsewhere"
        )
        assert not (tmp_path / ".btrfs-backup-ng").exists(), (
            "the old, never-used tree beside the source was built again"
        )
        isolated_endpoint.prepare.assert_called_once()

    def test_the_settings_default_matches(self):
        assert _legacy_main.LEGACY_SNAPSHOT_FOLDER == ".snapshots"


class TestAnExplicitSnapshotFolderIsHonoured:
    def test_an_absolute_folder_that_exists_is_used_as_given(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "home"
        source.mkdir()
        folder = tmp_path / "bigdisk" / "snaps"
        folder.mkdir(parents=True)
        _legacy_main.prepare_source_endpoint(
            _options(source, snapshot_folder=str(folder))
        )
        kwargs = isolated_endpoint.seen["kwargs"]
        assert kwargs["path"] == folder.resolve()
        assert kwargs["snapshot_folder"] == str(folder.resolve())
        assert sorted(p.name for p in folder.iterdir()) == [], (
            "nothing is appended below an absolute folder"
        )

    def test_an_absolute_folder_that_does_not_exist_is_refused_and_not_created(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "home"
        source.mkdir()
        folder = tmp_path / "unmounted" / "snaps"
        with pytest.raises(__util__.AbortError) as e:
            _legacy_main.prepare_source_endpoint(
                _options(source, snapshot_folder=str(folder))
            )
        assert "Snapshot folder" in str(e.value)
        assert not (tmp_path / "unmounted").exists()

    def test_a_relative_folder_is_created_under_the_source(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "home"
        source.mkdir()
        _legacy_main.prepare_source_endpoint(
            _options(source, snapshot_folder="snaps/legacy")
        )
        expected = (source / "snaps" / "legacy").resolve()
        assert expected.is_dir()
        assert isolated_endpoint.seen["kwargs"]["snapshot_folder"] == str(expected)

    def test_a_relative_folder_that_climbs_out_of_the_source_must_exist(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "vol" / "home"
        source.mkdir(parents=True)
        with pytest.raises(__util__.AbortError, match="Snapshot folder"):
            _legacy_main.prepare_source_endpoint(
                _options(source, snapshot_folder="../elsewhere")
            )
        assert not (tmp_path / "vol" / "elsewhere").exists()


class TestASilentNewChainIsRefused:
    """The chain is in <source>/.snapshots because -f was a no-op; the user
    passes -f as they always did; the folder is empty. Proceeding would make
    the next transfer a full send. Refuse, name the three remedies, create
    nothing."""

    def test_refuses_with_all_three_remedies_and_creates_nothing(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "home"
        source.mkdir()
        _chain(source / ".snapshots")
        with pytest.raises(__util__.AbortError) as e:
            _legacy_main.prepare_source_endpoint(
                _options(source, snapshot_folder="snaps")
            )
        text = str(e.value)
        assert "FULL send" in text and "Nothing was created" in text
        # same filesystem: create the folder, then move the chain
        assert f"mkdir -p {source / 'snaps'} && mv " in text and "(1)" in text
        assert f"--snapshot-folder {source / '.snapshots'}" in text  # keep it
        assert "--accept-full-send" in text and "(3)" in text  # accept once
        assert not (source / "snaps").exists()
        isolated_endpoint.prepare.assert_not_called()

    def test_an_absolute_empty_folder_is_refused_the_same_way(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "home"
        source.mkdir()
        _chain(source / ".snapshots")
        folder = tmp_path / "bigdisk"
        folder.mkdir()
        with pytest.raises(__util__.AbortError, match="FULL send"):
            _legacy_main.prepare_source_endpoint(
                _options(source, snapshot_folder=str(folder))
            )
        isolated_endpoint.prepare.assert_not_called()

    def test_accept_full_send_proceeds_and_the_folder_is_then_used(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "home"
        source.mkdir()
        _chain(source / ".snapshots")
        _legacy_main.prepare_source_endpoint(
            _options(source, snapshot_folder="snaps", accept_full_send=True)
        )
        assert (source / "snaps").is_dir()
        assert isolated_endpoint.seen["kwargs"]["snapshot_folder"] == str(
            (source / "snaps").resolve()
        )

    def test_the_flag_is_needed_only_once(self, tmp_path, isolated_endpoint):
        """Once the folder holds a snapshot for the prefix there is a chain to
        continue, so the second run needs no flag."""
        source = tmp_path / "home"
        source.mkdir()
        _chain(source / ".snapshots")
        _chain(source / "snaps", n=1)
        _legacy_main.prepare_source_endpoint(_options(source, snapshot_folder="snaps"))
        isolated_endpoint.prepare.assert_called_once()

    def test_pointing_at_the_default_chain_is_remedy_two(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "home"
        source.mkdir()
        _chain(source / ".snapshots")
        _legacy_main.prepare_source_endpoint(
            _options(source, snapshot_folder=str(source / ".snapshots"))
        )
        isolated_endpoint.prepare.assert_called_once()

    def test_another_prefix_s_chain_does_not_count(self, tmp_path, isolated_endpoint):
        source = tmp_path / "home"
        source.mkdir()
        _chain(source / ".snapshots", prefix="otherbox-")
        _legacy_main.prepare_source_endpoint(_options(source, snapshot_folder="snaps"))
        isolated_endpoint.prepare.assert_called_once()

    def test_no_chain_anywhere_means_a_first_run_and_no_refusal(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "home"
        source.mkdir()
        _legacy_main.prepare_source_endpoint(_options(source, snapshot_folder="snaps"))
        isolated_endpoint.prepare.assert_called_once()

    def test_the_hostname_prefix_default_is_what_is_looked_for(
        self, tmp_path, isolated_endpoint
    ):
        source = tmp_path / "home"
        source.mkdir()
        opts = _options(source, snapshot_folder="snaps")
        del opts["snapshot_prefix"]
        _chain(source / ".snapshots", prefix=f"{os.uname()[1]}-")
        with pytest.raises(__util__.AbortError, match="FULL send"):
            _legacy_main.prepare_source_endpoint(opts)


class TestTheFlagExists:
    def test_accept_full_send_parses_and_defaults_off(self):
        import argparse

        gp = [argparse.ArgumentParser(add_help=False)]
        opts = _legacy_main.parse_options(gp, ["/src", "/dst"])
        assert opts["accept_full_send"] is False
        opts = _legacy_main.parse_options(
            gp, ["/src", "/dst", "-f", "/snaps", "--accept-full-send"]
        )
        assert opts["accept_full_send"] is True
        assert opts["snapshot_folder"] == "/snaps"


class TestTheRefusalIsReported:
    def test_legacy_main_prints_the_diagnosis_not_only_process_aborted(
        self, tmp_path, monkeypatch
    ):
        """The top-level handler logged only "Process aborted by user or
        error" and dropped the exception text, so every refusal in legacy
        mode -- including the endpoint's own from 0.9.7 -- arrived without
        its cause or remedy."""
        source = tmp_path / "missing"
        message = __util__.missing_backup_location_message("Source", source)

        def refuse(options):
            raise __util__.AbortError(message)

        log = MagicMock()
        monkeypatch.setattr(_legacy_main, "logger", log)
        monkeypatch.setattr(_legacy_main, "parse_options", lambda gp, task: {})
        monkeypatch.setattr(_legacy_main, "create_logger", lambda *a, **k: None)
        monkeypatch.setattr(_legacy_main, "run_task", refuse)
        rc = _legacy_main.legacy_main([str(source), str(tmp_path)])
        assert rc == 1
        logged = [str(c.args[0]) % tuple(c.args[1:]) for c in log.error.call_args_list]
        assert any("Nothing was created" in line for line in logged), logged
        assert any("Process aborted" in line for line in logged), logged
