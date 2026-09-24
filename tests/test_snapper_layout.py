"""The snapper layout: one slot lifecycle for both directions.

A snapper backup to a btrfs target and a snapper restore into a local config
land their copies the same way -- receive into ``.snapshots/<n>.incoming``,
write the slot's ``info.xml`` into it, rename the directory to
``.snapshots/<n>`` -- and they used to do it twice: once inline in the backup
send, once in a restore function with its own pipes and three info.xml
branches. ``SnapperLayout`` is the one object both run, so a slot appears
complete or not at all, whichever direction filled it, and a mutation that
breaks the publish fails the backup tests here and the restore tests in
``test_snapper_restore_as_transfer.py``.

The shell scripts the slot functions run are executed for REAL against plain
directories (``btrfs subvolume delete ... || true`` is a harmless no-op on a
plain directory; the renames and removals are the real work), so every
assertion below is about what is on disk, never about a string.
"""

from __future__ import annotations

import contextlib
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from btrfs_backup_ng import __util__
from btrfs_backup_ng.core import operations as ops
from btrfs_backup_ng.core.layout import SnapperLayout
from btrfs_backup_ng.endpoint.local import LocalEndpoint


def _real_shell(ep, script):
    r = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    return r.returncode, r.stdout


@pytest.fixture
def real_shell(monkeypatch):
    monkeypatch.setattr(ops, "_snapper_run_shell", _real_shell)


def _endpoint(base: Path) -> LocalEndpoint:
    base.mkdir(parents=True, exist_ok=True)
    return LocalEndpoint(
        config={"path": str(base), "snap_prefix": "", "fs_checks": "skip"}
    )


def _received(layout: SnapperLayout, number: int, marker: str = "NEW") -> Path:
    """What a ``btrfs receive`` into the open slot leaves: ``<n>.incoming/snapshot``."""
    landing = Path(layout.incoming_dir(number)) / "snapshot"
    landing.mkdir()
    (landing / "marker").write_text(marker)
    return landing


class TestOpeningASlot:
    def test_the_incoming_slot_is_made_below_the_target_and_is_the_receive_path(
        self, tmp_path, real_shell
    ):
        ep = _endpoint(tmp_path / "target")
        layout = SnapperLayout(ep)
        layout.open_slot(7)
        incoming = tmp_path / "target" / ".snapshots" / "7.incoming"
        assert incoming.is_dir()
        assert str(ep.config["path"]) == str(incoming), (
            "a receive through the endpoint must land in the slot"
        )
        assert layout.open_number == 7

    def test_a_stale_incoming_from_a_crashed_run_is_cleared_first(
        self, tmp_path, real_shell
    ):
        stale = tmp_path / "target" / ".snapshots" / "7.incoming" / "snapshot"
        stale.mkdir(parents=True)
        (stale / "half").write_text("of a receive")
        layout = SnapperLayout(_endpoint(tmp_path / "target"))
        layout.open_slot(7)
        assert (tmp_path / "target" / ".snapshots" / "7.incoming").is_dir()
        assert not stale.exists(), "the crashed run's partial was kept in the slot"

    def test_a_missing_target_is_refused_before_any_lock_is_taken(
        self, tmp_path, real_shell, monkeypatch
    ):
        """The slot lock lives under the target; taken first, a missing target
        reads as a lock directory that could not be created. Mutation guard:
        move the existence check after the lock and this fails."""
        monkeypatch.setattr(
            ops,
            "_receiving_lock",
            lambda *a, **k: pytest.fail("a lock was taken under a missing target"),
        )
        ep = LocalEndpoint(
            config={
                "path": str(tmp_path / "unmounted" / "target"),
                "snap_prefix": "",
                "fs_checks": "skip",
            }
        )
        layout = SnapperLayout(ep)
        with pytest.raises(__util__.SnapshotTransferError, match="does not exist"):
            layout.open_slot(7)
        assert not (tmp_path / "unmounted").exists()
        assert layout.open_number is None

    def test_the_slot_lock_names_the_slots_snapshot_under_the_target(
        self, tmp_path, real_shell, monkeypatch
    ):
        """The lock is named for exactly what the transfer beneath will lock,
        and it is held from the open through the publish."""
        base = tmp_path / "target"
        events: list = []

        @contextlib.contextmanager
        def fake_lock(endpoint, destination, lock_root=""):
            events.append(("enter", destination, lock_root))
            yield
            events.append(("exit", destination, lock_root))

        monkeypatch.setattr(ops, "_receiving_lock", fake_lock)
        layout = SnapperLayout(_endpoint(base))
        layout.open_slot(7)
        expected = f"{base}/.snapshots/7.incoming/snapshot"
        assert events == [("enter", expected, str(base))]
        _received(layout, 7)
        layout.publish()
        assert events == [("enter", expected, str(base)), ("exit", expected, str(base))]

    def test_a_second_open_while_one_is_open_is_refused(self, tmp_path, real_shell):
        layout = SnapperLayout(_endpoint(tmp_path / "target"))
        layout.open_slot(7)
        with pytest.raises(RuntimeError, match="still open"):
            layout.open_slot(8)


class TestPublishing:
    def test_publish_renames_the_slot_into_place_with_its_info_xml(
        self, tmp_path, real_shell
    ):
        base = tmp_path / "target"
        ep = _endpoint(base)
        layout = SnapperLayout(ep)
        layout.open_slot(7)
        _received(layout, 7)

        number = layout.publish(info_xml=b"<snapshot><num>7</num></snapshot>")

        assert number == 7
        slot = base / ".snapshots" / "7"
        assert (slot / "snapshot" / "marker").read_text() == "NEW"
        assert (slot / "info.xml").read_bytes() == b"<snapshot><num>7</num></snapshot>"
        assert not (base / ".snapshots" / "7.incoming").exists()
        assert str(ep.config["path"]) == str(base), "the endpoint was left at the slot"
        assert layout.open_number is None

    def test_the_info_xml_is_inside_the_slot_when_the_slot_appears(
        self, tmp_path, real_shell, monkeypatch
    ):
        """Written into ``.incoming`` BEFORE the rename, so no published slot
        is ever without its metadata. Mutation guard: writing after the
        rename leaves ``.incoming/info.xml`` absent at publish time."""
        base = tmp_path / "target"
        seen = {}
        real_publish = ops._snapper_publish_slot

        def watching_publish(endpoint, number):
            seen["info_present"] = (
                base / ".snapshots" / f"{number}.incoming" / "info.xml"
            ).is_file()
            return real_publish(endpoint, number)

        monkeypatch.setattr(ops, "_snapper_publish_slot", watching_publish)
        layout = SnapperLayout(_endpoint(base))
        layout.open_slot(3)
        _received(layout, 3)
        layout.publish(info_xml=b"<x/>")
        assert seen["info_present"] is True

    def test_publish_without_info_xml_writes_none(self, tmp_path, real_shell):
        base = tmp_path / "target"
        layout = SnapperLayout(_endpoint(base))
        layout.open_slot(7)
        _received(layout, 7)
        layout.publish()
        assert (base / ".snapshots" / "7" / "snapshot").is_dir()
        assert not (base / ".snapshots" / "7" / "info.xml").exists()

    def test_a_publish_that_finds_no_received_copy_abandons_the_slot(
        self, tmp_path, real_shell
    ):
        """The receive produced nothing: publish refuses, the temp goes, and a
        slot that was already there is not disturbed."""
        base = tmp_path / "target"
        existing = base / ".snapshots" / "7" / "snapshot"
        existing.mkdir(parents=True)
        (existing / "marker").write_text("OLD")
        ep = _endpoint(base)
        layout = SnapperLayout(ep)
        layout.open_slot(7)

        with pytest.raises(__util__.SnapshotTransferError):
            layout.publish(info_xml=b"<x/>")

        assert (existing / "marker").read_text() == "OLD"
        assert not (base / ".snapshots" / "7.incoming").exists()
        assert not (base / ".snapshots" / "7.stale").exists()
        assert str(ep.config["path"]) == str(base)
        assert layout.open_number is None

    def test_publish_with_nothing_open_is_an_error(self, tmp_path, real_shell):
        layout = SnapperLayout(_endpoint(tmp_path / "target"))
        with pytest.raises(RuntimeError, match="no snapper slot is open"):
            layout.publish()


class TestPublishingFresh:
    """The restore direction's publish: never replaces, lands under the
    number free at that moment, and says so."""

    def _layout(self, tmp_path):
        base = tmp_path / "config"
        ep = _endpoint(base)
        (base / ".snapshots").mkdir()

        def next_number():
            return (
                max(
                    (
                        int(p.name)
                        for p in (base / ".snapshots").iterdir()
                        if p.name.isdigit()
                    ),
                    default=0,
                )
                + 1
            )

        return base, SnapperLayout(ep, next_number=next_number)

    def test_lands_under_the_number_free_at_publish_with_that_numbers_info_xml(
        self, tmp_path, real_shell
    ):
        base, layout = self._layout(tmp_path)
        layout.open_slot(1)
        _received(layout, 1)
        # snapper takes 1 while the receive is in flight.
        taken = base / ".snapshots" / "1" / "snapshot"
        taken.mkdir(parents=True)
        (taken / "marker").write_text("snapper's")
        asked: list[int] = []

        def xml_for(n):
            asked.append(n)
            return f"<snapshot><num>{n}</num></snapshot>".encode()

        number = layout.publish_fresh(xml_for)
        assert number == 2
        assert asked == [2], (
            "info.xml was written for a number the copy did not land under"
        )
        assert (base / ".snapshots" / "2" / "info.xml").read_bytes() == (
            b"<snapshot><num>2</num></snapshot>"
        )
        assert (base / ".snapshots" / "2" / "snapshot" / "marker").read_text() == "NEW"
        assert (taken / "marker").read_text() == "snapper's"
        assert not (base / ".snapshots" / "1.incoming").exists()
        assert not (base / ".snapshots" / "1.stale").exists()
        assert layout.open_number is None

    def test_an_empty_directory_made_between_the_choice_and_the_rename_is_not_replaced(
        self, tmp_path, real_shell
    ):
        """The window a re-read cannot close: the number is chosen, then
        snapper's ``mkdir`` lands, then the rename runs. ``os.rename`` takes
        the empty directory over; the no-replace rename refuses and the copy
        goes to the next number."""
        base, layout = self._layout(tmp_path)
        chosen: list[int] = []
        real_next = layout._next_number

        def next_then_mkdir():
            n = real_next()
            chosen.append(n)
            if len(chosen) == 1:
                (base / ".snapshots" / str(n)).mkdir()
            return n

        layout._next_number = next_then_mkdir
        layout.open_slot(1)
        _received(layout, 1)
        assert layout.publish_fresh(lambda n: None) == 2
        assert chosen == [1, 2]
        assert list((base / ".snapshots" / "1").iterdir()) == []
        assert (base / ".snapshots" / "2" / "snapshot" / "marker").read_text() == "NEW"

    def test_a_publish_that_cannot_rename_abandons_the_slot(
        self, tmp_path, real_shell, monkeypatch
    ):
        base, layout = self._layout(tmp_path)
        layout.open_slot(1)
        _received(layout, 1)

        def unsupported(src, dst):
            raise OSError(22, "no RENAME_NOREPLACE here")

        monkeypatch.setattr(__util__, "rename_noreplace", unsupported)
        with pytest.raises(__util__.SnapshotTransferError, match="could not publish"):
            layout.publish_fresh(lambda n: None)
        assert not (base / ".snapshots" / "1.incoming").exists()
        assert not (base / ".snapshots" / "1").exists()
        assert layout.open_number is None

    def test_the_backup_directions_publish_still_replaces_a_recycled_number(
        self, tmp_path, real_shell
    ):
        """The two publishes differ on purpose: a backup target's occupied
        slot is a recycled snapper number and is replaced; a live config's is
        someone's snapshot and is not."""
        base = tmp_path / "target"
        old = base / ".snapshots" / "7" / "snapshot"
        old.mkdir(parents=True)
        (old / "marker").write_text("OLD")
        layout = SnapperLayout(_endpoint(base))
        layout.open_slot(7)
        _received(layout, 7)
        assert layout.publish() == 7
        assert (base / ".snapshots" / "7" / "snapshot" / "marker").read_text() == "NEW"


class TestOneRestoreAtATime:
    def test_the_restore_lock_refuses_a_second_holder_and_is_released_after(
        self, tmp_path, real_shell
    ):
        base = tmp_path / "config"
        (base / ".snapshots").mkdir(parents=True)
        first = SnapperLayout(_endpoint(base))
        second = SnapperLayout(_endpoint(base))
        with first.writer_lock("Restoring into snapper config 'c'"):
            with pytest.raises(RuntimeError, match="another operation holds the lock"):
                with second.writer_lock("Restoring into snapper config 'c'"):
                    pass
        with second.writer_lock("Restoring into snapper config 'c'"):
            pass

    def test_sweep_removes_only_incoming_temps(self, tmp_path, real_shell):
        base = tmp_path / "config"
        snaps = base / ".snapshots"
        (snaps / "3" / "snapshot").mkdir(parents=True)
        (snaps / "4.incoming" / "snapshot").mkdir(parents=True)
        (snaps / "9.incoming").mkdir()
        (snaps / "notes.txt").write_text("keep")
        layout = SnapperLayout(_endpoint(base))
        assert layout.sweep_stale_temps() == ["4.incoming", "9.incoming"]
        assert sorted(p.name for p in snaps.iterdir()) == ["3", "notes.txt"]


class TestAbandoning:
    def test_abandon_removes_only_the_incoming_and_restores_the_path(
        self, tmp_path, real_shell
    ):
        base = tmp_path / "target"
        published = base / ".snapshots" / "7" / "snapshot"
        published.mkdir(parents=True)
        (published / "marker").write_text("OLD")
        ep = _endpoint(base)
        layout = SnapperLayout(ep)
        layout.open_slot(8)
        _received(layout, 8, "PARTIAL")

        layout.abandon()

        assert not (base / ".snapshots" / "8.incoming").exists()
        assert not (base / ".snapshots" / "8").exists()
        assert (published / "marker").read_text() == "OLD"
        assert str(ep.config["path"]) == str(base)
        assert layout.open_number is None

    def test_a_failed_receive_into_a_recycled_number_never_touches_the_published_slot(
        self, tmp_path, real_shell
    ):
        """The backup direction reuses the source's number, so the slot being
        filled may already hold a good backup. Abandoning the receive removes
        the temp and nothing else."""
        base = tmp_path / "target"
        published = base / ".snapshots" / "7" / "snapshot"
        published.mkdir(parents=True)
        (published / "marker").write_text("OLD")
        layout = SnapperLayout(_endpoint(base))
        layout.open_slot(7)
        _received(layout, 7, "PARTIAL")
        layout.abandon()
        assert (published / "marker").read_text() == "OLD"
        assert not (base / ".snapshots" / "7.incoming").exists()

    def test_abandon_with_nothing_open_is_a_no_op(self, tmp_path, real_shell):
        layout = SnapperLayout(_endpoint(tmp_path / "target"))
        layout.abandon()
        assert not (tmp_path / "target" / ".snapshots").exists()

    def test_finish_abandons_a_slot_a_failed_receive_left_open(
        self, tmp_path, real_shell
    ):
        base = tmp_path / "target"
        layout = SnapperLayout(_endpoint(base))
        layout.open_slot(1)
        _received(layout, 1, "PARTIAL")
        layout.finish()
        assert not (base / ".snapshots" / "1.incoming").exists()
        assert not (base / ".snapshots" / "1").exists()


class TestTheBackupDirectionRunsTheLayout:
    """``send_snapper_snapshot`` opens, fills, publishes and abandons through
    the layout; its own inline sequence is gone."""

    def _snapper_snapshot(self, tmp_path, number=5, info_xml=b"<snapshot/>"):
        src = tmp_path / "src" / ".snapshots" / str(number)
        src.mkdir(parents=True)
        if info_xml is not None:
            (src / "info.xml").write_bytes(info_xml)
        return SimpleNamespace(
            number=number,
            subvolume_path=src / "snapshot",
            info_xml_path=src / "info.xml",
        )

    @pytest.fixture
    def quiet(self, monkeypatch):
        monkeypatch.setattr(ops, "log_transaction", lambda **k: None)
        monkeypatch.setattr(ops, "_write_snapper_metadata", lambda *a, **k: None)
        monkeypatch.setattr(
            ops, "_create_snapper_snapshot_wrapper", lambda snap, ep=None: MagicMock()
        )

    def test_a_backup_lands_in_the_slot_with_the_sources_info_xml(
        self, tmp_path, real_shell, quiet, monkeypatch
    ):
        base = tmp_path / "target"
        ep = _endpoint(base)

        def fake_send(snapshot, destination_endpoint, parent=None, options=None):
            # The engine's receive lands the subvolume in the slot the
            # endpoint points at.
            (Path(destination_endpoint.config["path"]) / "snapshot").mkdir()

        monkeypatch.setattr(ops, "send_snapshot", fake_send)
        ops.send_snapper_snapshot(
            self._snapper_snapshot(tmp_path, 5, b"<snapshot><num>5</num></snapshot>"),
            ep,
        )
        slot = base / ".snapshots" / "5"
        assert (slot / "snapshot").is_dir()
        assert (slot / "info.xml").read_bytes() == b"<snapshot><num>5</num></snapshot>"
        assert not (base / ".snapshots" / "5.incoming").exists()
        assert str(ep.config["path"]) == str(base)

    def test_a_failed_send_abandons_the_slot(
        self, tmp_path, real_shell, quiet, monkeypatch
    ):
        base = tmp_path / "target"

        def failing_send(snapshot, destination_endpoint, parent=None, options=None):
            (Path(destination_endpoint.config["path"]) / "snapshot").mkdir()
            raise __util__.SnapshotTransferError("the receive died")

        monkeypatch.setattr(ops, "send_snapshot", failing_send)
        with pytest.raises(__util__.SnapshotTransferError, match="the receive died"):
            ops.send_snapper_snapshot(
                self._snapper_snapshot(tmp_path, 5), _endpoint(base)
            )
        assert not (base / ".snapshots" / "5.incoming").exists()
        assert not (base / ".snapshots" / "5").exists()

    def test_a_broken_publish_fails_the_backup(
        self, tmp_path, real_shell, quiet, monkeypatch
    ):
        """One half of the shared-lifecycle proof; the other half is the restore
        test that breaks the same primitive and expects the restore to fail."""
        base = tmp_path / "target"
        monkeypatch.setattr(
            ops,
            "send_snapshot",
            lambda s, d, parent=None, options=None: (
                Path(d.config["path"]) / "snapshot"
            ).mkdir(),
        )

        def broken_publish(endpoint, number):
            raise __util__.SnapshotTransferError("publish broken")

        monkeypatch.setattr(ops, "_snapper_publish_slot", broken_publish)
        with pytest.raises(__util__.SnapshotTransferError, match="publish broken"):
            ops.send_snapper_snapshot(
                self._snapper_snapshot(tmp_path, 5), _endpoint(base)
            )
        assert not (base / ".snapshots" / "5").exists()
        assert not (base / ".snapshots" / "5.incoming").exists()

    def test_a_broken_slot_primitive_fails_the_backup(
        self, tmp_path, real_shell, quiet, monkeypatch
    ):
        """The shared half of the proof: the same primitive, broken, fails
        the restore in test_snapper_restore_as_transfer.py."""

        def broken_prepare(endpoint, number):
            raise __util__.SnapshotTransferError("prepare broken")

        monkeypatch.setattr(ops, "_snapper_prepare_slot", broken_prepare)
        monkeypatch.setattr(
            ops, "send_snapshot", lambda *a, **k: pytest.fail("sent without a slot")
        )
        with pytest.raises(__util__.SnapshotTransferError, match="prepare broken"):
            ops.send_snapper_snapshot(
                self._snapper_snapshot(tmp_path, 5), _endpoint(tmp_path / "target")
            )
        assert not (tmp_path / "target" / ".snapshots" / "5").exists()

    def test_the_inline_lifecycle_is_gone(self):
        import inspect

        source = inspect.getsource(ops.send_snapper_snapshot)
        assert "SnapperLayout(" in source
        for own in (
            "_snapper_prepare_slot(",
            "_snapper_publish_slot(",
            "_receiving_lock(",
            'config["path"] =',
        ):
            assert own not in source, f"the send still runs {own} itself"
