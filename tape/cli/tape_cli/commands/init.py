"""`tape init <name>` — scaffold a new Tape project."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import typer

from ..util import console, ok, info, fail, template_env, render_tree

_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,40}$")

TEMPLATES_ROOT = Path(__file__).resolve().parent.parent / "templates"


def run(
    name: str = typer.Argument(..., help="Project name (lowercase, snake/kebab)."),
    tier: str = typer.Option(
        "adk", "--tier",
        help="adk = embedded (tape-adk, no separate server; the default). "
             "server = the scale tier (Rust tape-server + gRPC)."),
    here: bool = typer.Option(False, "--here", help="Scaffold into the current directory instead of `./<name>`."),
    region: str = typer.Option("us-central1", "--region", help="Default GCP region (server tier)."),
    store: str = typer.Option("sqlite", "--store", help="Default store (server tier): sqlite | postgres | alloydb | spanner | bigtable."),
    events: str = typer.Option("none", "--events", help="Default events (server tier): none | pubsub."),
    force: bool = typer.Option(False, "--force", help="Overwrite existing files."),
):
    if not _NAME_RE.match(name):
        fail(f"invalid project name {name!r}",
             hint="use lowercase letters, digits, '-' or '_'; start with a letter.")
        raise typer.Exit(2)
    if tier not in ("adk", "server"):
        fail(f"invalid --tier {tier!r}", hint="use 'adk' or 'server'.")
        raise typer.Exit(2)

    dst = Path.cwd() if here else (Path.cwd() / name)
    if dst.exists() and any(dst.iterdir()) and not force:
        fail(f"{dst} already exists and is not empty.",
             hint="re-run with `--force` to overwrite, or pick a new name.")
        raise typer.Exit(2)
    dst.mkdir(parents=True, exist_ok=True)

    context = {
        "name": name,
        "region": region,
        "store": store,
        "events": events,
    }

    # The embedded tier scaffolds from `project-adk/`; the scale tier from
    # `project/`. The embedded tier is the default — it matches what the
    # docs call the default tier (tape-adk, no separate server).
    template_dir = "project-adk" if tier == "adk" else "project"
    env = template_env(TEMPLATES_ROOT / template_dir)
    written = render_tree(env, TEMPLATES_ROOT / template_dir, dst, context)

    console.print()
    ok(f"scaffolded {dst} ({len(written)} files) — [bold]{tier}[/bold] tier")
    info("")
    info("Next steps:")
    info(f"  [bold]cd {dst.name}[/bold]")
    info("  [bold]pip install -e .[/bold]")
    if tier == "adk":
        info("  [bold]tape dev[/bold]                  # reactors + live journal (no server)")
        info("  [dim]then drive your agent: `adk run app` (or your own script)[/dim]")
        info("")
        info("[dim]Embedded tier: tape-adk extends ADK's DatabaseSessionService. "
             "No separate server. Swap the SQLite db_url in tape.yaml for "
             "postgresql+asyncpg:// to go to production.[/dim]")
    else:
        info("  [bold]tape dev[/bold]                  # local: server + reactors + agent")
        info("  [bold]tape doctor[/bold]               # diagnose your setup")
        info("  [bold]tape provision gcp --dry-run[/bold]   # render Terraform for GCP")
