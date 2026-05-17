"""`tape` — the standalone CLI.

Subcommands::

    tape init <name>             scaffold a new project
    tape enhance .               wire Tape into an existing ADK project
    tape dev                     run server + reactors + agent locally
    tape doctor                  diagnose local and (optionally) GCP setup
    tape provision gcp           render & apply Terraform for GCP infra
    tape deploy gcp              build & deploy Tape server + reactors + agent
    tape logs                    tail Cloud Logging for tape-* services
    tape status                  show runs, effects, obligations, reactor lag
    tape destroy gcp             tear down provisioned GCP infra
    tape migrate                 run store migrations
"""

from __future__ import annotations

import typer
from rich.console import Console

from .commands import init as init_cmd
from .commands import enhance as enhance_cmd
from .commands import dev as dev_cmd
from .commands import doctor as doctor_cmd
from .commands import provision as provision_cmd
from .commands import deploy as deploy_cmd
from .commands import logs as logs_cmd
from .commands import status as status_cmd
from .commands import destroy as destroy_cmd
from .commands import migrate as migrate_cmd

console = Console()

app = typer.Typer(
    name="tape",
    help="Tape — a durable-execution substrate for ADK agents on GCP.",
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode="rich",
)


def _version_callback(value: bool) -> None:
    if value:
        from . import __version__
        console.print(f"tape {__version__}")
        raise typer.Exit()


@app.callback(invoke_without_command=True)
def _root(
    ctx: typer.Context,
    version: bool = typer.Option(
        False, "--version", callback=_version_callback, is_eager=True,
        help="Show the version and exit."),
):
    if ctx.invoked_subcommand is None and not version:
        console.print(ctx.get_help())
        raise typer.Exit()


# `tape init <name>` and similar leaf commands are registered as plain commands.
app.command(name="init", help="Scaffold a new Tape project.")(init_cmd.run)
app.command(name="enhance", help="Add Tape to an existing ADK project.")(enhance_cmd.run)
app.command(name="dev", help="Run server + reactors + agent locally.")(dev_cmd.run)
app.command(name="doctor", help="Diagnose local and GCP setup.")(doctor_cmd.run)
app.command(name="logs", help="Tail Cloud Logging for the deployed services.")(logs_cmd.run)
app.command(name="status", help="Show runs / effects / obligations / reactor lag.")(status_cmd.run)
app.command(name="migrate", help="Run schema migrations for the configured store.")(migrate_cmd.run)

# `tape provision gcp ...` / `tape deploy gcp ...` / `tape destroy gcp ...` are
# subcommand groups so we can add other clouds later (or none).
app.add_typer(provision_cmd.app, name="provision", help="Render & apply infrastructure.")
app.add_typer(deploy_cmd.app, name="deploy", help="Build & deploy services.")
app.add_typer(destroy_cmd.app, name="destroy", help="Tear down provisioned infra.")


if __name__ == "__main__":
    app()
