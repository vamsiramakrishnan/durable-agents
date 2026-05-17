"""`tape provision gcp ...` — render Terraform, optionally apply."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from ..config import TapeProject, find_project_root
from ..iac import generate, apply
from ..util import console, ok, info, warn, fail

app = typer.Typer(name="provision", help="Render & apply infrastructure.")


@app.command("gcp", help="Render Terraform for GCP — optionally apply.")
def gcp(
    store: Optional[str] = typer.Option(None, "--store",
        help="Override store: alloydb | postgres | spanner | bigtable."),
    events: Optional[str] = typer.Option(None, "--events", help="Override events: pubsub | none."),
    target: Optional[str] = typer.Option(None, "--target", help="Override target: cloud-run | gke."),
    region: Optional[str] = typer.Option(None, "--region"),
    dry_run: bool = typer.Option(True, "--dry-run/--no-dry-run", help="Render only (default)."),
    apply_now: bool = typer.Option(False, "--apply", help="Render and apply via `tofu apply`."),
    out: str = typer.Option("deploy/gcp/terraform", "--out", help="Output directory for Terraform."),
):
    root = find_project_root()
    project = TapeProject.load(root / "tape.yaml")

    # Apply CLI overrides to an in-memory copy of the project config.
    if store:
        project.tape.store.kind = store  # type: ignore[assignment]
    if events:
        project.tape.events.kind = events  # type: ignore[assignment]
    if target:
        project.tape.server.target = target.replace("-", "_")  # type: ignore[assignment]
        project.agent.deployment_target = target.replace("-", "_")  # type: ignore[assignment]
    if region:
        project.gcp.region = region

    out_dir = root / out
    ok(f"rendering Terraform to {out_dir}")
    generate(project, out_dir)

    info("")
    info("Generated:")
    for p in sorted(out_dir.iterdir()):
        info(f"  {p.relative_to(root)}")
    info("")

    if apply_now or not dry_run:
        rc = apply(out_dir, dry_run=False)
        raise typer.Exit(rc)
    rc = apply(out_dir, dry_run=True)
    info("\n[dim]Re-run with `--apply` to apply.[/dim]")
    raise typer.Exit(rc)
