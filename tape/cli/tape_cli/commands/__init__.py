"""`tape <command>` modules — each exposes a `run` (or a `app` typer-group).

The split is one file per command so a future contributor can add `aws`
provisioning, an `azure` target, or a `tape signal` admin command without
touching the rest.
"""
