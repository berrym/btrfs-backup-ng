"""A destination as `restore --cleanup` meets it, for the tests that drive it.

Directories read as subvolumes, the identity of each is whatever the test puts
in ``rig.identities``, and ``btrfs subvolume delete`` is recorded in
``rig.deleted`` instead of run. Shared by the tests of the cleanup's
authorship rules and of its failures, which build the same destination.
"""

from __future__ import annotations

import json
import subprocess
import types

import btrfs_backup_ng.core.layout as layout_mod
from btrfs_backup_ng.cli import restore as restore_cli
from btrfs_backup_ng.core.layout import marker_dir
from btrfs_backup_ng.endpoint.local import LocalEndpoint

# Taken before any test replaces subprocess.run, for a test that needs the
# real one back after building the rig.
REAL_RUN = subprocess.run


def dead_pid() -> int:
    """The pid of a process that has already exited."""
    proc = subprocess.Popen(["true"])
    proc.wait()
    return proc.pid


def build_rig(tmp_path, monkeypatch) -> types.SimpleNamespace:
    """The destination at ``tmp_path``, with deletions recorded, not run."""
    deleted: list[str] = []
    identities: dict[str, dict | None] = {}

    monkeypatch.setattr(
        restore_cli.__util__, "is_subvolume", lambda p: p.is_dir(), raising=True
    )
    monkeypatch.setattr(layout_mod, "_received_subvolume_shape", lambda p: None)
    monkeypatch.setattr(
        LocalEndpoint, "subvolume_identity", lambda self, p: identities.get(str(p))
    )
    monkeypatch.setattr("builtins.input", lambda *_: "y")

    def fake_run(cmd, *a, **kw):
        deleted.append(cmd[-1])
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    return types.SimpleNamespace(dest=tmp_path, deleted=deleted, identities=identities)


def run_cleanup(dest, dry_run=False) -> int:
    """`restore --cleanup DEST`, as the CLI runs it."""
    args = types.SimpleNamespace(destination=str(dest), dry_run=dry_run)
    return restore_cli._execute_cleanup(args)


def mark(dest, name, pid, token="s-001"):
    """The run marker a restore in flight writes for ``name``."""
    directory = marker_dir(dest)
    directory.mkdir(parents=True, exist_ok=True)
    marker = directory / f"{token}.json"
    marker.write_text(
        json.dumps(
            {
                "format": 1,
                "session": "s",
                "pid": pid,
                "snapshot": name,
                "path": str(dest / name),
                "started": "2026-01-01T00:00:00+00:00",
            }
        )
    )
    return marker
