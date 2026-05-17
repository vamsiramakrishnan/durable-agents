"""`tape enhance <path>` — non-destructively add Tape to an existing ADK project."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from ..util import console, ok, warn, info, fail, template_env, render_tree
from .init import TEMPLATES_ROOT


def _detect_adk(root: Path) -> bool:
    for py in root.rglob("*.py"):
        try:
            text = py.read_text(errors="ignore")
        except Exception:
            continue
        if "from google.adk" in text or "import google.adk" in text:
            return True
    return False


def _detect_agents_cli(root: Path) -> bool:
    """`google/agents-cli` scaffolds usually leave a `agents.yaml` or a
    `pyproject.toml` with a `[tool.agents]` table. We don't import it; we just
    print a friendly note so the user sees we noticed."""
    if (root / "agents.yaml").exists():
        return True
    pp = root / "pyproject.toml"
    if pp.exists() and "[tool.agents" in pp.read_text(errors="ignore"):
        return True
    return False


def run(
    path: str = typer.Argument(".", help="Path to the project root."),
    name: Optional[str] = typer.Option(None, "--name", help="Project name (defaults to directory name)."),
    region: str = typer.Option("us-central1", "--region"),
    store: str = typer.Option("sqlite", "--store"),
    events: str = typer.Option("none", "--events"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Accept all prompts."),
):
    root = Path(path).resolve()
    if not root.exists():
        fail(f"path {root} does not exist.")
        raise typer.Exit(2)

    pname = name or root.name
    is_adk = _detect_adk(root)
    is_agents_cli = _detect_agents_cli(root)

    if is_adk:
        ok("detected ADK imports (`from google.adk import ...`)")
    else:
        warn("could not detect ADK in this project — proceeding anyway.")
    if is_agents_cli:
        ok("detected an `agents-cli`-style scaffold; Tape will live alongside it.")

    has_tape_yaml = (root / "tape.yaml").exists()
    if has_tape_yaml:
        warn("tape.yaml already exists — leaving it alone.")
    else:
        ok("will create tape.yaml")

    # Files we add by default: tape.yaml + app/tape_runtime.py + docker-compose
    # overlays + deploy/gcp/README.md. We never overwrite the user's app code.
    items = [
        ("tape.yaml", "config"),
        ("docker-compose.tape.yaml", "local-dev overlay"),
        ("app/tape_runtime.py", "non-invasive durable_app helper"),
        ("deploy/gcp/README.md", "GCP deploy notes"),
        (".env.example", "environment example"),
    ]
    info("\nPatch summary:")
    for path_, desc in items:
        info(f"  + {path_}  [dim]({desc})[/dim]")

    if not yes and not typer.confirm("\nApply these changes?", default=True):
        info("aborted")
        raise typer.Exit(1)

    context = {"name": pname, "region": region, "store": store, "events": events}
    env = template_env(TEMPLATES_ROOT / "enhance")
    written = render_tree(env, TEMPLATES_ROOT / "enhance", root, context)
    for w in written:
        ok(f"wrote {w.relative_to(root)}")

    info("")
    info("Next steps:")
    info("  In your existing agent module, replace your `App`/`Runner` wiring with:")
    info("    [bold]from app.tape_runtime import build_durable_runner[/bold]")
    info("    [bold]app, runner = build_durable_runner(root_agent=...)[/bold]")
    info("")
    info("  Or call `tape.adk.durable_app(...)` directly. The companion file")
    info("  `app/tape_runtime.py` shows both shapes.")
