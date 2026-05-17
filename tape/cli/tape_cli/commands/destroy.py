"""`tape destroy gcp` — tear down provisioned infra (via `tofu destroy`)."""

from __future__ import annotations

from pathlib import Path

import typer

from ..config import find_project_root
from ..util import console, ok, info, warn, fail, which, run_cmd, die

app = typer.Typer(name="destroy", help="Tear down provisioned infra.")


@app.command("gcp", help="Run `tofu destroy` against the generated Terraform.")
def gcp(
    out: str = typer.Option("deploy/gcp/terraform", "--dir",
        help="Path to the generated Terraform directory."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation."),
):
    root = find_project_root()
    tf_dir = root / out
    if not tf_dir.exists():
        die(f"no Terraform directory at {tf_dir} — nothing to destroy.")
    if not yes:
        warn(f"This will run `tofu destroy` in {tf_dir}.")
        if not typer.confirm("Continue?", default=False):
            raise typer.Exit(1)
    tool = which("tofu") or which("terraform")
    if not tool:
        die("Neither `tofu` nor `terraform` is installed.")
    rc = run_cmd([tool, "destroy", "-input=false", "-auto-approve",
                  "-var-file=terraform.tfvars.json"],
                 cwd=tf_dir, check=False).returncode
    raise typer.Exit(rc)
