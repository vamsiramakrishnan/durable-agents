"""`tape migrate` — run store migrations via the Rust server.

The Rust server already migrates on boot; this command surfaces the same
contract as an idempotent admin task. With `--dry-run` it prints the planned
schema target without applying.
"""

from __future__ import annotations

import os
from typing import Optional

import typer

from ..config import TapeProject, find_project_root
from ..util import console, ok, info, warn, fail, which, run_cmd, die


def run(
    store: Optional[str] = typer.Option(None, "--store",
        help="Override TAPE_STORE for this invocation."),
    dry_run: bool = typer.Option(False, "--dry-run"),
    server_binary: Optional[str] = typer.Option(None, "--server-binary"),
):
    root = find_project_root()
    project = TapeProject.load(root / "tape.yaml")
    target_store = store or project.tape.store.url or os.environ.get("TAPE_STORE")
    if not target_store:
        die("no TAPE_STORE configured. Set --store or tape.store.url.")
    binary = server_binary or which("tape-server")
    if not binary:
        die("no tape-server binary found.",
            hint="cargo build --release in tape/server, or `--server-binary <path>`.")

    if dry_run:
        info(f"[dry-run] would run: {binary} --migrate --store {target_store}")
        raise typer.Exit(0)

    rc = run_cmd([binary, "--migrate", "--store", target_store], check=False).returncode
    if rc != 0:
        fail("migration failed.")
        raise typer.Exit(rc)
    ok("migrations applied.")
