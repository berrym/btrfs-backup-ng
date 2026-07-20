"""The `raw` command family (0.8.5 PR6a).

`raw list` enumerates a raw target's backups via their .meta sidecars. These tests
cover the target-spec coercion, the human + JSON output, and the error paths.
"""

import argparse
import json

import pytest

from btrfs_backup_ng.cli import raw_cmd
from btrfs_backup_ng.endpoint.raw import RawEndpoint


def _build_backup(path, name, *, encrypt=None, compress=None, cipher="aes-256-cbc"):
    """Write one committed raw backup of a few bytes under ``path``."""
    cfg = {"path": str(path)}
    if encrypt:
        cfg["encrypt"] = encrypt
        cfg["openssl_cipher"] = cipher
    if compress:
        cfg["compress"] = compress
    ep = RawEndpoint(config=cfg)
    src = path / f"{name}.src"
    src.write_bytes(b"payload-" * 64)
    with open(src, "rb") as stdin:
        ep.receive(stdin, snapshot_name=name).communicate()
    ep.commit_receive()
    src.unlink()


def _args(**kw):
    ns = argparse.Namespace()
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


# --------------------------------------------------------------------------- #
# spec coercion
# --------------------------------------------------------------------------- #
def test_coerce_bare_path_becomes_raw_scheme():
    assert raw_cmd._coerce_raw_spec("/backups/x") == "raw:///backups/x"


@pytest.mark.parametrize("spec", ["raw:///backups/x", "raw+ssh://host/backups/x"])
def test_coerce_raw_schemes_pass_through(spec):
    assert raw_cmd._coerce_raw_spec(spec) == spec


def test_coerce_rejects_non_raw_scheme():
    with pytest.raises(ValueError, match="not a raw target"):
        raw_cmd._coerce_raw_spec("ssh://host/path")


def test_human_size():
    assert raw_cmd._human_size(0) == "0 B"
    assert raw_cmd._human_size(512) == "512 B"
    assert raw_cmd._human_size(1024) == "1.0 KiB"
    assert raw_cmd._human_size(1024 * 1024) == "1.0 MiB"


# --------------------------------------------------------------------------- #
# execute_raw dispatch
# --------------------------------------------------------------------------- #
def test_execute_raw_no_action_returns_1(capsys):
    rc = raw_cmd.execute_raw(_args(raw_action=None))
    assert rc == 1
    assert "raw list" in capsys.readouterr().out


def test_execute_raw_dispatches_list(tmp_path, capsys):
    rc = raw_cmd.execute_raw(_args(raw_action="list", target=str(tmp_path), json=False))
    assert rc == 0
    assert "Raw target:" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# raw list output
# --------------------------------------------------------------------------- #
def test_raw_list_human_lists_backups(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "x")
    _build_backup(
        tmp_path, "root.20260101T120000", encrypt="openssl_enc", compress="gzip"
    )
    _build_backup(tmp_path, "home.20260102T120000")

    rc = raw_cmd.execute_raw(_args(raw_action="list", target=str(tmp_path), json=False))
    out = capsys.readouterr().out
    assert rc == 0
    assert "(2 snapshots)" in out
    assert "root.20260101T120000" in out
    assert "home.20260102T120000" in out
    # The encrypting backup shows its cipher and pipeline.
    assert "aes-256-cbc" in out
    assert "openssl_enc" in out
    assert "gzip" in out


def test_raw_list_json_is_valid_and_authoritative(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("BTRFS_BACKUP_PASSPHRASE", "x")
    _build_backup(tmp_path, "root.20260101T120000", encrypt="openssl_enc")

    rc = raw_cmd.execute_raw(_args(raw_action="list", target=str(tmp_path), json=True))
    assert rc == 0
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data, list) and len(data) == 1
    entry = data[0]
    assert entry["name"] == "root.20260101T120000"
    # The JSON is the authoritative v2 sidecar document.
    assert entry["version"] == 2
    assert entry["pipeline"]["encrypt"] == "openssl_enc"
    assert entry["pipeline"]["openssl_cipher"] == "aes-256-cbc"


def test_raw_list_empty_target_is_ok(tmp_path, capsys):
    rc = raw_cmd.execute_raw(_args(raw_action="list", target=str(tmp_path), json=False))
    assert rc == 0
    assert "(0 snapshots)" in capsys.readouterr().out


def test_raw_list_rejects_non_raw_scheme(capsys):
    rc = raw_cmd.execute_raw(
        _args(raw_action="list", target="ssh://host/path", json=False)
    )
    assert rc == 2
    assert "not a raw target" in capsys.readouterr().out


def test_raw_list_warns_on_missing_local_target(tmp_path, capsys):
    """A nonexistent local target warns (on stderr) instead of silently reporting
    an empty target as if it held no backups."""
    missing = tmp_path / "no-such-dir"
    rc = raw_cmd.execute_raw(_args(raw_action="list", target=str(missing), json=False))
    assert rc == 0
    captured = capsys.readouterr()
    assert "does not exist or is not mounted" in captured.err


# --------------------------------------------------------------------------- #
# dispatcher wiring (the command is actually registered and routed)
# --------------------------------------------------------------------------- #
def test_dispatcher_parses_raw_list():
    """The real parser recognizes `raw list TARGET --json --ssh-sudo` and sets the
    expected namespace. A missing subparser registration would fail here."""
    from btrfs_backup_ng.cli.dispatcher import create_subcommand_parser

    parser = create_subcommand_parser()
    args = parser.parse_args(["raw", "list", "/x", "--json", "--ssh-sudo"])
    assert args.command == "raw"
    assert args.raw_action == "list"
    assert args.target == "/x"
    assert args.json is True
    assert args.ssh_sudo is True


def test_dispatcher_routes_raw_to_execute_raw(monkeypatch):
    """run_subcommand must dispatch command=='raw' to raw_cmd.execute_raw. Dropping
    the "raw": cmd_raw handler entry would fail here (not caught by the unit tests
    that call execute_raw directly)."""
    from btrfs_backup_ng.cli import dispatcher

    called = {}

    def fake_execute_raw(a):
        called["args"] = a
        return 0

    monkeypatch.setattr(raw_cmd, "execute_raw", fake_execute_raw)
    rc = dispatcher.run_subcommand(
        _args(command="raw", raw_action="list", target="/x", json=False, version=False)
    )
    assert rc == 0
    assert called["args"].command == "raw"
