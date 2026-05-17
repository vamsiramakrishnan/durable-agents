"""`tape status` — runs, effects, obligations, reactor lag."""

from __future__ import annotations

from typing import Optional

import typer

from ..config import TapeProject, find_project_root
from ..util import console, ok, info, warn, fail, die, table


def run(
    limit: int = typer.Option(20, "--limit", "-n"),
):
    try:
        import tape
        from tape.client import TapeClient
    except ImportError:
        die("tape-py not installed.", hint="pip install tape-py")

    root = find_project_root()
    project = TapeProject.load(root / "tape.yaml")
    url = project.tape.url
    info(f"Connecting to {url}")
    try:
        with TapeClient(url) as c:
            runs = c.list_runs_to_recover(limit=limit)
            pending = c.list_pending_effects(limit=limit, include_unknown=True, include_pending=True)
    except Exception as ex:
        die(f"failed to query Tape server: {ex}")

    runs_t = table("Runs needing recovery", ["run_id", "status", "lease_owner", "lease_expires"])
    for r in runs.runs:
        runs_t.add_row(r.run_id[:12], str(r.status), r.lease_owner, str(r.lease_expires_at_ms))
    console.print(runs_t)

    eff_t = table("Effects (PENDING/UNKNOWN)", ["effect_key", "tool", "status", "run_id"])
    for e in pending.effects:
        eff_t.add_row(e.idempotency_key[:24], e.tool_name, str(e.status), e.run_id[:12])
    console.print(eff_t)
