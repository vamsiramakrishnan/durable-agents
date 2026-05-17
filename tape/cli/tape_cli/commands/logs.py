"""`tape logs` — tail Cloud Logging for tape-* services."""

from __future__ import annotations

from typing import Optional

import typer

from ..config import TapeProject, find_project_root
from ..util import console, ok, info, warn, fail, which, run_cmd, die


def run(
    follow: bool = typer.Option(True, "--follow/--no-follow", "-f"),
    service: Optional[str] = typer.Option(None, "--service",
        help="Limit to a specific service: tape-server | tape-reactor-recovery | ..."),
    limit: int = typer.Option(50, "--limit", "-n"),
):
    if not which("gcloud"):
        die("gcloud not installed.")
    root = find_project_root()
    project = TapeProject.load(root / "tape.yaml")
    if not project.gcp.project_id:
        die("gcp.project_id is unset.")

    name_filter = (
        f'resource.labels.service_name="{service}"'
        if service else
        'resource.labels.service_name=~"^tape-"'
    )
    log_filter = (
        f'resource.type="cloud_run_revision" AND {name_filter}'
    )
    cmd = ["gcloud", "logging", "read", log_filter,
           f"--project={project.gcp.project_id}",
           f"--limit={limit}", "--format=value(timestamp,resource.labels.service_name,textPayload,jsonPayload.msg)"]
    if follow:
        cmd = ["gcloud", "alpha", "logging", "tail", log_filter,
               f"--project={project.gcp.project_id}"]
    info("[dim]$ " + " ".join(cmd) + "[/dim]")
    rc = run_cmd(cmd, check=False).returncode
    raise typer.Exit(rc)
