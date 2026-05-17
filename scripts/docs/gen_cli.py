#!/usr/bin/env python3
"""Generate the `tape` CLI reference by introspecting the Typer app.

Output: tape/docs/reference/cli/index.md (single Markdown page).

The page lists every command + subcommand with its help text, options, and
arguments. Edits to the page are overwritten — change the docstrings on the
Typer commands instead and re-run this script.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click  # bundled with typer
import typer
from click.formatting import HelpFormatter

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "tape" / "cli"))

from tape_cli.main import app as tape_app  # noqa: E402


OUT = REPO / "tape" / "docs" / "reference" / "cli" / "index.md"

HEADER = """\
# CLI Reference

!!! info "Generated"
    Generated from the live Typer app by `scripts/docs/gen_cli.py`. To change the content,
    edit the Typer commands themselves and re-run the script.

`tape` is the standalone CLI. It composes the substrate (Tape) and the cloud (GCP) without
making developers learn every seam first.

```bash
pip install -e tape/cli
tape --help
```

"""


def _click_command(item) -> click.Command:
    """typer registers its commands as Click commands; pull them out."""
    # The Typer app's underlying click command tree.
    return typer.main.get_command(tape_app)


def _format_command(cmd: click.Command, *, name: str, depth: int = 0, parent: str = "tape") -> str:
    """Render one Click command + its subcommands recursively."""
    full = f"{parent} {name}".strip()
    heading_level = min(2 + depth, 6)
    lines: list[str] = []
    lines.append(f"{'#' * heading_level} `{full}`")
    lines.append("")

    short = (cmd.short_help or cmd.help or "").strip().splitlines()
    if short:
        lines.append(short[0])
        lines.append("")

    # Arguments + options.
    args = [p for p in cmd.params if isinstance(p, click.Argument)]
    opts = [p for p in cmd.params if isinstance(p, click.Option)]

    # Usage — hand-rendered to avoid Click/Typer version mismatches on
    # `make_metavar(ctx)` (Click 8.2+ vs Typer ≤ 0.15.x).
    pieces = [full]
    if opts:
        pieces.append("[OPTIONS]")
    if isinstance(cmd, click.Group):
        pieces.append("COMMAND [ARGS]...")
    for a in args:
        nargs = getattr(a, "nargs", 1) or 1
        name = a.human_readable_name.upper()
        if nargs == -1:
            pieces.append(f"[{name}]...")
        elif getattr(a, "required", False):
            pieces.append(name)
        else:
            pieces.append(f"[{name}]")
    lines.append("```")
    lines.append("Usage: " + " ".join(pieces))
    lines.append("```")
    lines.append("")

    # Long help / docstring (skip the first line which is shown above).
    if cmd.help and len(cmd.help.strip().splitlines()) > 1:
        body = "\n".join(cmd.help.strip().splitlines()[1:]).strip()
        if body:
            lines.append(body)
            lines.append("")
    if args:
        lines.append("**Arguments**\n")
        lines.append("| Name | Help |")
        lines.append("|---|---|")
        for a in args:
            help_text = (getattr(a, "help", None) or "").replace("|", "\\|").strip()
            lines.append(f"| `{a.human_readable_name}` | {help_text or '—'} |")
        lines.append("")
    if opts:
        lines.append("**Options**\n")
        lines.append("| Flag | Default | Help |")
        lines.append("|---|---|---|")
        for o in opts:
            flags = ", ".join(f"`{x}`" for x in o.opts + o.secondary_opts)
            default = o.default
            if callable(default):
                default = "(computed)"
            default_s = "—" if default is None else f"`{default!r}`"
            help_text = (o.help or "").replace("|", "\\|").strip()
            lines.append(f"| {flags} | {default_s} | {help_text or '—'} |")
        lines.append("")

    # Subcommands.
    if isinstance(cmd, click.Group):
        for sub_name in sorted(cmd.commands):
            sub = cmd.commands[sub_name]
            lines.append(_format_command(sub, name=sub_name, depth=depth + 1, parent=full))

    return "\n".join(lines)


def main() -> int:
    root = typer.main.get_command(tape_app)
    body = _format_command(root, name="", depth=0, parent="tape")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(HEADER + body.replace("# `tape `", "# `tape`") + "\n")
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
