"""R4 Phase 0: snapshots carry their btrfs uuid / received_uuid (identity foundation).

Phase 0 is strictly NON-behavioral: it only ENRICHES snapshot objects with the btrfs
``uuid`` and ``received_uuid`` at enumeration -- local endpoints via ``btrfs subvolume
show`` per snapshot (mount-safe, unambiguous), ssh endpoints from each ``subvolume list
-o -u -R`` line. Identity (`__eq__` / `__lt__` / `find_parent`) is unchanged and nothing
consults the new fields yet -- that is Phase 2. ``subvolume show`` is sudo-escalated
(``sudo -n`` when not root) so a non-root+passwordless-sudo run populates uuids like the
transfer path; population stays best-effort: any failure (no sudo, non-btrfs, older
btrfs-progs) leaves the uuids empty and enumeration working.
"""

from __future__ import annotations

import subprocess
import time

from btrfs_backup_ng import __util__
from btrfs_backup_ng.endpoint.local import LocalEndpoint

# A realistic `btrfs subvolume list -o -u -R` block: a source snapshot (received_uuid
# unset "-") and a received copy (received_uuid populated).
SAMPLE_LIST = (
    "ID 256 gen 9 top level 5 uuid "
    "11111111-1111-1111-1111-111111111111 received_uuid - "
    "path snaps/home-20240101-000000\n"
    "ID 257 gen 10 top level 5 uuid "
    "22222222-2222-2222-2222-222222222222 received_uuid "
    "33333333-3333-3333-3333-333333333333 "
    "path snaps/home-20240102-000000\n"
)


# --------------------------------------------------------------------------- #
# parse_subvolume_list (the single shared parser)
# --------------------------------------------------------------------------- #
def test_parse_subvolume_list_extracts_uuid_and_received_uuid():
    entries = {e["name"]: e for e in __util__.parse_subvolume_list(SAMPLE_LIST)}
    a = entries["home-20240101-000000"]
    b = entries["home-20240102-000000"]
    # uuid and received_uuid must not be confused with each other.
    assert a["uuid"] == "11111111-1111-1111-1111-111111111111"
    assert a["received_uuid"] == ""  # "-" -> empty (source snapshot, never received)
    assert b["uuid"] == "22222222-2222-2222-2222-222222222222"
    assert b["received_uuid"] == "33333333-3333-3333-3333-333333333333"


def test_parse_subvolume_list_name_is_final_path_component():
    entries = __util__.parse_subvolume_list(
        "ID 256 gen 9 top level 5 uuid abc received_uuid - path a/b/c/home-x\n"
    )
    assert entries[0]["name"] == "home-x"
    assert entries[0]["path"] == "a/b/c/home-x"


def test_parse_subvolume_list_tolerates_missing_uuid_columns():
    # Older btrfs-progs without -u -R: only a path, no uuid tokens.
    entries = __util__.parse_subvolume_list(
        "ID 256 gen 9 top level 5 path snaps/home-x\n"
    )
    assert len(entries) == 1
    assert entries[0]["name"] == "home-x"
    assert entries[0]["uuid"] == ""
    assert entries[0]["received_uuid"] == ""


def test_parse_subvolume_list_skips_blank_and_pathless_lines():
    out = "\n   \ngarbage line with no marker\n" + SAMPLE_LIST
    entries = __util__.parse_subvolume_list(out)
    assert {e["name"] for e in entries} == {
        "home-20240101-000000",
        "home-20240102-000000",
    }


# --------------------------------------------------------------------------- #
# Base list_snapshots enrichment (LocalEndpoint) -- best-effort + graceful
# --------------------------------------------------------------------------- #
def _local(path):
    path.mkdir(parents=True, exist_ok=True)
    return LocalEndpoint(
        config={
            "path": path,
            "source": "/src",
            "snapshot_folder": ".snapshots",
            "snap_prefix": "home-",
        }
    )


def _named_snaps(ep, path):
    return [
        __util__.Snapshot(
            path,
            "home-",
            ep,
            time_obj=time.strptime(stamp, "%Y%m%d-%H%M%S"),
            time_format="%Y%m%d-%H%M%S",
        )
        for stamp in ("20240101-000000", "20240102-000000")
    ]


def test_enrichment_uses_subvolume_show_per_snapshot(tmp_path, monkeypatch):
    """Enrichment queries ``btrfs subvolume show <exact path>`` per snapshot, so identity
    is unambiguous (mount-safe -- no cross-referencing of id-5-relative list paths). Each
    snapshot gets exactly its own subvolume's uuid/received_uuid. Mutation guard: dropping
    the ``snap.uuid = ...`` assignment leaves them empty."""
    ep = _local(tmp_path)
    snaps = _named_snaps(ep, tmp_path)
    show_by_path = {
        str(snaps[0].get_path()): (
            "Name: \t\t\thome-20240101-000000\n"
            "\tUUID: \t\t\taaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\n"
            "\tParent UUID: \t\t-\n"
            "\tReceived UUID: \t\t-\n"
        ),
        str(snaps[1].get_path()): (
            "Name: \t\t\thome-20240102-000000\n"
            "\tUUID: \t\t\tbbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\n"
            "\tReceived UUID: \t\tcccccccc-cccc-cccc-cccc-cccccccccccc\n"
        ),
    }

    def fake_run(argv, *a, **k):
        return subprocess.CompletedProcess(
            argv, 0, stdout=show_by_path.get(argv[-1], ""), stderr=""
        )

    monkeypatch.setattr("btrfs_backup_ng.endpoint.common.subprocess.run", fake_run)
    ep._load_subvolume_ids_into(snaps)

    by = {s.get_name(): s for s in snaps}
    assert by["home-20240101-000000"].uuid == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    assert by["home-20240101-000000"].received_uuid == ""  # "-" -> empty
    assert by["home-20240102-000000"].uuid == "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    assert by["home-20240102-000000"].received_uuid == (
        "cccccccc-cccc-cccc-cccc-cccccccccccc"
    )


def test_enrichment_sudo_escalates_when_not_root(tmp_path, monkeypatch):
    """``subvolume show`` needs CAP_SYS_ADMIN, so a NON-root enumeration must sudo-escalate
    (``sudo -n``) exactly like the transfer path -- otherwise a non-root run WITH passwordless
    sudo backs up fine yet reads empty uuids, silently degrading the planner. Mutation guard:
    dropping the escalation leaves the bare ``btrfs`` argv and fails this."""
    ep = _local(tmp_path)
    snaps = _named_snaps(ep, tmp_path)
    captured = []

    def fake_run(argv, *a, **k):
        captured.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr("btrfs_backup_ng.endpoint.common.os.geteuid", lambda: 1000)
    monkeypatch.setattr("btrfs_backup_ng.endpoint.common.subprocess.run", fake_run)
    ep._load_subvolume_ids_into(snaps)

    assert captured, "enrichment ran no commands"
    for argv in captured:
        assert argv[:3] == ["sudo", "-n", "btrfs"], argv
        assert argv[3:5] == ["subvolume", "show"]


def test_enrichment_no_sudo_when_root(tmp_path, monkeypatch):
    """When already root, enrichment runs ``btrfs`` directly -- no needless sudo. Mutation
    guard: unconditionally prepending sudo fails this."""
    ep = _local(tmp_path)
    snaps = _named_snaps(ep, tmp_path)
    captured = []

    def fake_run(argv, *a, **k):
        captured.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr("btrfs_backup_ng.endpoint.common.os.geteuid", lambda: 0)
    monkeypatch.setattr("btrfs_backup_ng.endpoint.common.subprocess.run", fake_run)
    ep._load_subvolume_ids_into(snaps)

    assert captured, "enrichment ran no commands"
    for argv in captured:
        assert argv[0] == "btrfs" and "sudo" not in argv, argv


def test_parse_subvolume_show_uuid_and_received():
    out = (
        "Name: \t\t\thome-x\n"
        "\tUUID: \t\t\te760292b-a4b1-b64a-808e-258e66e91ed5\n"
        "\tParent UUID: \t\t-\n"
        "\tReceived UUID: \t\t-\n"
    )
    ids = __util__.parse_subvolume_show(out)
    assert ids["uuid"] == "e760292b-a4b1-b64a-808e-258e66e91ed5"
    assert ids["received_uuid"] == ""


def test_parse_subvolume_show_received_not_confused_with_uuid_or_parent():
    """UUID must come from the plain ``UUID:`` line, never ``Parent UUID:`` or
    ``Received UUID:``. Mutation guard: a loose ``'UUID' in line`` check grabs the wrong
    one."""
    out = "\tUUID: \t\t\tAAAA\n\tParent UUID: \t\tPPPP\n\tReceived UUID: \t\tRRRR\n"
    ids = __util__.parse_subvolume_show(out)
    assert ids["uuid"] == "AAAA"
    assert ids["received_uuid"] == "RRRR"


def test_ssh_parse_snapshot_list_sets_uuids_per_line():
    """SSHEndpoint._parse_snapshot_list takes each snapshot's uuid/received_uuid from its
    OWN line, so identity is never crossed with a same-named subvolume. Mutation guard:
    dropping the per-line parse leaves the uuids empty."""
    from btrfs_backup_ng.endpoint.ssh import SSHEndpoint

    ep = SSHEndpoint(
        hostname="h",
        config={
            "path": "/remote/bk",
            "snap_prefix": "home-",
            "timestamp_format": "%Y%m%d-%H%M%S",
        },
    )
    output = (
        "ID 357 gen 250 top level 5 received_uuid - uuid "
        "e760292b-a4b1-b64a-808e-258e66e91ed5 path bk/home-20240101-000000\n"
        "ID 358 gen 251 top level 5 received_uuid "
        "e760292b-a4b1-b64a-808e-258e66e91ed5 uuid "
        "3585faf3-e573-b44a-aae5-bc0da3ad585b path bk/home-20240102-000000\n"
    )
    snaps = {s.get_name(): s for s in ep._parse_snapshot_list(output, "/remote/bk")}
    assert snaps["home-20240101-000000"].uuid == (
        "e760292b-a4b1-b64a-808e-258e66e91ed5"
    )
    assert snaps["home-20240101-000000"].received_uuid == ""
    assert snaps["home-20240102-000000"].received_uuid == (
        "e760292b-a4b1-b64a-808e-258e66e91ed5"
    )


def test_enrichment_graceful_on_command_failure(tmp_path, monkeypatch):
    """A non-zero btrfs exit (e.g. non-root) must leave uuids empty, not raise. Mutation
    guard: removing the returncode check would try to parse empty output (still empty) --
    the stronger guard is the exception path below."""
    ep = _local(tmp_path)
    snaps = _named_snaps(ep, tmp_path)

    def fake_run(*a, **k):
        return subprocess.CompletedProcess(a, 1, stdout="", stderr="not permitted")

    monkeypatch.setattr("btrfs_backup_ng.endpoint.common.subprocess.run", fake_run)
    ep._load_subvolume_ids_into(snaps)  # must not raise
    assert all(s.uuid == "" and s.received_uuid == "" for s in snaps)


def test_enrichment_graceful_when_subprocess_raises(tmp_path, monkeypatch):
    """If the btrfs command cannot even be spawned, enumeration must still succeed.
    Mutation guard: removing the try/except makes this raise."""
    ep = _local(tmp_path)
    snaps = _named_snaps(ep, tmp_path)

    def boom(*a, **k):
        raise FileNotFoundError("btrfs not found")

    monkeypatch.setattr("btrfs_backup_ng.endpoint.common.subprocess.run", boom)
    ep._load_subvolume_ids_into(snaps)  # must not raise
    assert all(s.uuid == "" for s in snaps)


# --------------------------------------------------------------------------- #
# Non-behavioral guard: identity is STILL name/time in Phase 0
# --------------------------------------------------------------------------- #
def test_identity_unchanged_despite_differing_uuids(tmp_path):
    """Two snapshots with the same name/time but DIFFERENT uuids must still compare equal
    in Phase 0 (identity is name/time; uuids are carried, not consulted). Mutation guard:
    if __eq__ were switched to uuid-based, this fails -- proving Phase 0 didn't change
    identity."""
    ep = _local(tmp_path)
    t = time.strptime("20240101-000000", "%Y%m%d-%H%M%S")
    a = __util__.Snapshot(
        tmp_path, "home-", ep, time_obj=t, time_format="%Y%m%d-%H%M%S"
    )
    b = __util__.Snapshot(
        tmp_path, "home-", ep, time_obj=t, time_format="%Y%m%d-%H%M%S"
    )
    a.uuid, a.received_uuid = "aaaa", "xxxx"
    b.uuid, b.received_uuid = "bbbb", "yyyy"
    assert a == b  # identity is still (prefix, time_obj)
    # find_parent still treats them as the same snapshot (already-present -> None)
    assert a.find_parent([b]) is None


def test_new_snapshots_default_to_empty_uuids(tmp_path):
    ep = _local(tmp_path)
    s = __util__.Snapshot(tmp_path, "home-", ep)
    assert s.uuid == ""
    assert s.received_uuid == ""
