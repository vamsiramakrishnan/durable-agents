"""`tape inspect` — make the journal visible.

This command turns the per-run execution journal into something a human can
read. By default — when stdout is a TTY and a `<run-id>` is given — it
launches a **Textual app** (see `_inspector_app.py`): scrollable timeline,
detail pane, live header with lease countdown, history-vs-live divider,
keyboard filters. That is the experiential surface.

For pipelines, CI, and screenshots we keep three lower-overhead modes::

    tape inspect <run-id> --print     # rich one-shot snapshot, no app
    tape inspect <run-id> --summary   # counts + non-zero exit on UNKNOWN
    tape inspect <run-id> --raw       # JSONL stream — pipe to jq
    tape inspect ls                   # recoverable runs (operator's hot set)
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter, deque
from typing import Optional

import grpc
import typer
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ..config import TapeProject, find_project_root
from ..util import console, die, info, ok, warn
from ._journal import (
    Decoded,
    decode_entry,
    fmt_countdown_ms,
    fmt_rel_ms,
    run_status_label,
)


# ── helpers ────────────────────────────────────────────────────────────────


def _resolve_url(url_flag: Optional[str]) -> str:
    """Resolve which tape-server to talk to. Order: --url > $TAPE_URL >
    tape.yaml > the SDK default."""
    if url_flag:
        return url_flag
    env = os.environ.get("TAPE_URL")
    if env:
        return env
    try:
        root = find_project_root()
        return TapeProject.load(root / "tape.yaml").tape.url
    except (FileNotFoundError, Exception):  # noqa: BLE001
        return "tape://localhost:7878"


def _import_client():
    try:
        from tape.client import TapeClient  # noqa: F401
        return TapeClient
    except ImportError:
        die("tape-py not installed.\n  Fix: pip install tape-py")


# ── one-shot render (--print) ──────────────────────────────────────────────


def _render_header(run_state, connect_cursor: int, url: str) -> Panel:
    status_label, status_style = run_status_label(run_state.status)
    now = int(time.time() * 1000)
    kv = Table.grid(padding=(0, 2))
    kv.add_column(style="dim", no_wrap=True)
    kv.add_column()
    kv.add_row("app / user / session",
               f"{run_state.app_name} / {run_state.user_id} / {run_state.session_id}")
    kv.add_row("status", Text(status_label, style=status_style))
    lease_str = (f"[bold]{run_state.lease_owner}[/bold]  "
                 f"[dim]({fmt_countdown_ms(run_state.lease_expires_at_ms, now)})[/dim]"
                 if run_state.lease_owner else "—")
    kv.add_row("lease", lease_str)
    kv.add_row("seq cursor", str(run_state.seq_cursor))
    if run_state.started_at_ms:
        dur = (run_state.ended_at_ms or now) - run_state.started_at_ms
        kv.add_row("duration", fmt_rel_ms(dur).lstrip("+"))
    if run_state.waiting_on_gate:
        kv.add_row("waiting on", Text(run_state.waiting_on_gate, style="blue"))
    return Panel(
        kv,
        title=f"[bold]RUN[/bold] [cyan]{run_state.run_id}[/cyan]",
        subtitle=f"[dim]{url}[/dim]",
        border_style=status_style.split()[-1] if status_style else "white",
        padding=(0, 1),
    )


def _render_body(entries, connect_cursor: int) -> Table:
    t = Table(show_header=True, header_style="bold dim",
              show_edge=False, pad_edge=False, expand=True, padding=(0, 1))
    t.add_column("seq", justify="right", style="dim", width=5)
    t.add_column("+t", justify="right", style="dim", width=8)
    t.add_column(" ", width=1)
    t.add_column("type", width=11)
    t.add_column("status", width=14)
    t.add_column("details", overflow="ellipsis", no_wrap=True)

    if not entries:
        t.add_row("", "", "", Text("…", style="dim"), "",
                  Text("(no journal entries yet)", style="dim"))
        return t

    first_ts = entries[0].ts_ms
    live_divider_inserted = False
    for e in entries:
        d: Decoded = decode_entry(e.kind, e.payload_json, e.trace_id)
        if (not live_divider_inserted and connect_cursor > 0
                and e.seq > connect_cursor):
            t.add_row("", "", "", Text("─── live ───", style="bold dim"),
                      "", Text("", style="dim"))
            live_divider_inserted = True
        rel = fmt_rel_ms(e.ts_ms - first_ts)
        is_history = connect_cursor and e.seq <= connect_cursor
        row_style = "dim" if is_history else ""
        t.add_row(
            Text(str(e.seq), style="dim"),
            Text(rel, style="dim"),
            Text(d.icon, style=row_style or "white"),
            Text(d.type, style=("dim bold" if is_history else "bold")),
            Text(d.status, style=d.style),
            Text.from_markup(d.summary, style=row_style),
        )
    return t


def _drain(client, run_id: str, *, from_seq: int = 0,
           limit: Optional[int] = None, timeout: float = 2.0) -> list:
    """Pull the existing journal once. Stops when the gRPC stream times out
    (= 'no new entries written in <timeout>') or when --limit is hit."""
    out: list = []
    try:
        it = client.subscribe_run(run_id=run_id, from_seq=from_seq, timeout=timeout)
        for entry in it:
            out.append(entry)
            if limit is not None and len(out) >= limit:
                try: it.cancel()
                except Exception: pass  # noqa: BLE001, E701
                break
    except grpc.RpcError as ex:
        if ex.code() != grpc.StatusCode.DEADLINE_EXCEEDED:
            raise
    return out


def _do_print(client, run_id: str, *, from_seq: int, limit: Optional[int],
              url: str) -> None:
    try:
        rs = client.get_run(run_id)
    except grpc.RpcError as ex:
        if ex.code() == grpc.StatusCode.NOT_FOUND:
            die(f"no such run: {run_id}")
        die(f"failed to get run: {ex.code().name} {ex.details()}")
    entries = _drain(client, run_id, from_seq=from_seq, limit=limit)
    console.print(_render_header(rs, rs.seq_cursor, url))
    # In one-shot mode there's no live boundary to draw — passing
    # connect_cursor=0 suppresses the history/live dim split. The TUI keeps
    # the boundary because there it's meaningful (you SEE entries arriving).
    console.print(_render_body(entries, connect_cursor=0))


def _do_raw(client, run_id: str, *, from_seq: int, limit: Optional[int],
            follow: bool) -> None:
    """Emit JSONL — one JournalEntry per line. Pipes cleanly to jq."""
    try:
        rs = client.get_run(run_id)
    except grpc.RpcError as ex:
        if ex.code() == grpc.StatusCode.NOT_FOUND:
            die(f"no such run: {run_id}")
        die(f"failed to get run: {ex.code().name} {ex.details()}")
    # When --no-follow, use a server-side timeout so we don't block forever.
    kwargs = dict(run_id=run_id, from_seq=from_seq)
    if not follow:
        kwargs["timeout"] = 2.0
    try:
        it = client.subscribe_run(**kwargs)
        seen = 0
        try:
            for entry in it:
                out = {
                    "seq": entry.seq,
                    "global_seq": entry.global_seq,
                    "kind": entry.kind,
                    "ts_ms": entry.ts_ms,
                    "subject": entry.subject,
                    "trace_id": entry.trace_id,
                    "span_id": entry.span_id,
                    "parent_span_id": entry.parent_span_id,
                    "schema_version": entry.schema_version,
                    "payload": _safe_inline_json(entry.payload_json),
                }
                print(json.dumps(out, separators=(",", ":")), flush=True)
                seen += 1
                if limit is not None and seen >= limit:
                    try: it.cancel()
                    except Exception: pass  # noqa: BLE001, E701
                    break
        except KeyboardInterrupt:
            try: it.cancel()
            except Exception: pass  # noqa: BLE001, E701
    except grpc.RpcError as ex:
        if ex.code() not in (grpc.StatusCode.CANCELLED,
                             grpc.StatusCode.DEADLINE_EXCEEDED):
            die(f"stream failed: {ex.code().name} {ex.details()}")


def _do_summary(client, run_id: str, *, url: str) -> None:
    try:
        rs = client.get_run(run_id)
    except grpc.RpcError as ex:
        if ex.code() == grpc.StatusCode.NOT_FOUND:
            die(f"no such run: {run_id}")
        die(f"failed to get run: {ex.code().name} {ex.details()}")
    entries = _drain(client, run_id, from_seq=0)
    console.print(_render_header(rs, rs.seq_cursor, url))

    by_kind: Counter = Counter()
    eff_by_status: Counter = Counter()
    ob_by_status: Counter = Counter()
    gate_by_status: Counter = Counter()
    for e in entries:
        by_kind[e.kind] += 1
        d = decode_entry(e.kind, e.payload_json)
        if e.kind == "effect":
            eff_by_status[d.status] += 1
        elif e.kind == "obligation":
            ob_by_status[d.status] += 1
        elif e.kind == "gate":
            gate_by_status[d.status] += 1

    def _ctable(title: str, c: Counter) -> Table:
        t = Table(title=title, title_style="bold dim",
                  show_edge=False, pad_edge=False)
        t.add_column("status"); t.add_column("count", justify="right")
        for k, v in sorted(c.items(), key=lambda kv: -kv[1]):
            t.add_row(k or "(none)", str(v))
        return t

    grid = Table.grid(padding=(0, 2))
    grid.add_row(_ctable("by kind", by_kind),
                 _ctable("effects", eff_by_status),
                 _ctable("obligations", ob_by_status),
                 _ctable("gates", gate_by_status))
    console.print(grid)

    bad_eff = eff_by_status.get("unknown", 0) + eff_by_status.get("failed", 0)
    bad_ob = ob_by_status.get("stuck", 0)
    if bad_eff or bad_ob:
        warn(f"non-clean: {bad_eff} bad effects, {bad_ob} stuck obligations")
        raise typer.Exit(code=1)
    ok("clean.")


def _safe_inline_json(s: str):
    if not s:
        return None
    try:
        return json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return s


# ── `ls` — recoverable runs ─────────────────────────────────────────────────


def _ls(url: str, limit: int) -> None:
    TapeClient = _import_client()
    try:
        with TapeClient(url) as c:
            resp = c.list_runs_to_recover(limit=limit)
    except grpc.RpcError as ex:
        die(f"failed to query tape server: {ex.code().name} {ex.details()}")

    runs = list(resp.runs)
    if not runs:
        info(f"[dim]no recoverable runs at {url}[/dim]")
        info("\n[dim]Hint: `tape inspect ls` shows runs needing attention "
             "(RUNNABLE, expired leases, signal-released WAITING).\n"
             "      Pass an explicit run id to inspect any run:[/dim]\n"
             "      [cyan]tape inspect <run-id>[/cyan]")
        return

    t = Table(title=f"Recoverable runs @ {url}", header_style="bold")
    t.add_column("#", justify="right", style="dim")
    t.add_column("run_id")
    t.add_column("status")
    t.add_column("app/user/session")
    t.add_column("lease owner")
    t.add_column("lease")
    t.add_column("waiting on", style="blue")
    now = int(time.time() * 1000)
    for i, r in enumerate(runs, 1):
        label, style = run_status_label(r.status)
        t.add_row(
            str(i),
            r.run_id,
            Text(label, style=style),
            f"{r.app_name}/{r.user_id}/{r.session_id}",
            r.lease_owner or "[dim]—[/dim]",
            fmt_countdown_ms(r.lease_expires_at_ms, now),
            r.waiting_on_gate or "",
        )
    console.print(t)
    console.print(
        "\n[dim]Inspect any one with:[/dim]  "
        "[cyan]tape inspect <run-id>[/cyan]")


# ── the Textual launcher ───────────────────────────────────────────────────


def _launch_tui(client, run_id: str, *, url: str, from_seq: int) -> None:
    """Spin up the Textual inspector app. Imported lazily so that the
    non-interactive modes (--print, --summary, --raw, ls) don't pay the
    Textual import cost or require it to be installed in CI-like environments."""
    try:
        from ._inspector_app import TapeInspectorApp
    except ImportError as ex:
        die(f"Textual not available ({ex}).\n"
            f"  Fix: pip install 'textual>=0.79'\n"
            f"  Or:  tape inspect {run_id} --print   "
            f"(non-interactive fallback)")
    # Validate the run id eagerly so a typo doesn't drop us into a blank TUI.
    try:
        client.get_run(run_id)
    except grpc.RpcError as ex:
        if ex.code() == grpc.StatusCode.NOT_FOUND:
            die(f"no such run: {run_id}")
        die(f"failed to get run: {ex.code().name} {ex.details()}")
    TapeInspectorApp(client, run_id, url=url, from_seq=from_seq).run()


# ── the typer command ──────────────────────────────────────────────────────
#
# This is a flat command (not a Typer group) so the positional `[RUN_ID]`
# argument doesn't collide with the Click subcommand parser — typer groups
# with positional callback args reject any subsequent `--option` as if it
# were a subcommand name. With no args it falls through to `_ls` (the
# recoverable-runs listing) so users still get a useful default.


def run(
    run_id: Optional[str] = typer.Argument(
        None, help="Run id to inspect. Omit to list recoverable runs."),
    print_mode: bool = typer.Option(
        False, "--print", "-P",
        help="Print a rich snapshot and exit (no Textual app)."),
    raw: bool = typer.Option(
        False, "--raw",
        help="JSONL — one JournalEntry per line. Implies streaming."),
    summary: bool = typer.Option(
        False, "--summary",
        help="Stats only — counts by status, duration. Exits 1 on UNKNOWN."),
    follow: bool = typer.Option(
        True, "--follow/--no-follow", "-f/-F",
        help="With --raw: keep streaming after drain. Default: yes."),
    from_seq: int = typer.Option(
        0, "--from-seq", "-s",
        help="Start streaming from this seq (0 => from the beginning)."),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-n",
        help="Stop after this many entries (used with --raw / --print)."),
    list_runs: bool = typer.Option(
        False, "--list", "-l",
        help="List recoverable runs (same as `tape inspect` with no args)."),
    url: Optional[str] = typer.Option(
        None, "--url",
        help="Override the tape server URL. Default: $TAPE_URL or tape.yaml."),
):
    """Inspect a Tape run's journal.

    Default (TTY + run-id): launches the Textual inspector — scrollable
    timeline, detail pane, live header, search/filter, history-vs-live divider.

    Modes::

        tape inspect                    # list recoverable runs
        tape inspect <id>               # Textual app (interactive)
        tape inspect <id> --print       # rich snapshot, no app (good for CI)
        tape inspect <id> --summary     # counts + non-zero exit on UNKNOWN
        tape inspect <id> --raw         # JSONL stream — pipe to jq
    """
    resolved_url = _resolve_url(url)

    if run_id is None or list_runs:
        _ls(resolved_url, limit=limit or 50)
        return

    TapeClient = _import_client()
    try:
        client = TapeClient(resolved_url)
    except Exception as ex:  # noqa: BLE001
        die(f"failed to connect to {resolved_url}: {ex}")

    try:
        if raw:
            _do_raw(client, run_id, from_seq=from_seq, limit=limit,
                    follow=follow)
        elif summary:
            _do_summary(client, run_id, url=resolved_url)
        elif print_mode or not sys.stdout.isatty():
            # Non-TTY (pipe, CI, file redirect) defaults to --print so output
            # is captured cleanly. Explicit --print honors the same path.
            _do_print(client, run_id, from_seq=from_seq, limit=limit,
                      url=resolved_url)
        else:
            _launch_tui(client, run_id, url=resolved_url, from_seq=from_seq)
    finally:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass
