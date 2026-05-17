"""Shared utilities — Rich console helpers, command-runner, template loader."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from rich.console import Console
from rich.table import Table

console = Console()


def ok(msg: str) -> None:
    console.print(f"[green]✓[/green] {msg}")


def warn(msg: str) -> None:
    console.print(f"[yellow]![/yellow] {msg}")


def fail(msg: str, *, hint: Optional[str] = None) -> None:
    console.print(f"[red]✗[/red] {msg}")
    if hint:
        console.print(f"  [dim]Fix: {hint}[/dim]")


def info(msg: str) -> None:
    console.print(msg)


def section(title: str) -> None:
    console.rule(f"[bold]{title}[/bold]")


def which(cmd: str) -> Optional[str]:
    return shutil.which(cmd)


def run_cmd(argv: list[str], *, cwd: Optional[Path] = None, env: Optional[dict] = None,
            check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a command with predictable output. Returns the CompletedProcess."""
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    return subprocess.run(
        argv, cwd=str(cwd) if cwd else None, env=full_env,
        check=check, text=True,
        capture_output=capture,
    )


def template_env(template_dir: Path) -> Environment:
    return Environment(
        loader=FileSystemLoader(str(template_dir)),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        trim_blocks=False,
        lstrip_blocks=False,
    )


def render_tree(env: Environment, src_root: Path, dst_root: Path,
                context: dict, *, dry_run: bool = False) -> list[Path]:
    """Recursively render every file under `src_root` into `dst_root`.

    Filenames ending in `.j2` have the suffix stripped. Directory names with
    `{{name}}`-style placeholders are interpolated against `context`.

    Returns the list of files written (or that would be written, in `--dry-run`).
    """
    written: list[Path] = []
    for src in sorted(src_root.rglob("*")):
        if src.is_dir():
            continue
        rel = src.relative_to(src_root)
        # Interpolate path components.
        parts = []
        for part in rel.parts:
            tpl = env.from_string(part)
            parts.append(tpl.render(**context))
        out_rel = Path(*parts)
        if out_rel.name.endswith(".j2"):
            out_rel = out_rel.with_name(out_rel.name[:-3])
        dst = dst_root / out_rel
        rendered = env.get_template(str(rel).replace("\\", "/")).render(**context)
        if dry_run:
            written.append(dst)
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(rendered)
        written.append(dst)
    return written


def table(title: str, columns: list[str]) -> Table:
    t = Table(title=title, show_lines=False, header_style="bold")
    for c in columns:
        t.add_column(c)
    return t


def die(msg: str, code: int = 2) -> None:
    fail(msg)
    sys.exit(code)


__all__ = [
    "console", "ok", "warn", "fail", "info", "section", "which",
    "run_cmd", "template_env", "render_tree", "table", "die",
]
