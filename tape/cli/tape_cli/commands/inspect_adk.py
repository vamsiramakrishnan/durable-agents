"""`tape inspect-adk` — snapshot view of a session's journal in the embedded
(`tape-adk`) path.

Where `tape inspect <run-id>` (the original) talks to `tape-server` over gRPC,
this command reads directly from a `TapeSessionService` SQLAlchemy backend.
The unit of inspection is an ADK **session** (the `(app_name, user_id,
session_id)` triple), not a run, because the embedded model doesn't have a
separate run/lease lifecycle.

Snapshot only for now — no live tail. Run it once to drill into a session
the operator found via `tape doctor --live --db-url …`. The full live TUI
re-pointed at SQL is a follow-up.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from typing import Any, Optional

import typer
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from ..util import console, die, info, ok, warn


def _resolve_db_url(flag: Optional[str]) -> str:
    if flag:
        return flag
    env = os.environ.get("TAPE_ADK_DB_URL")
    if env:
        return env
    die("no --db-url and no $TAPE_ADK_DB_URL — pass --db-url sqlite+aiosqlite:///./tape.db")


def _short(s: Any, n: int = 40) -> str:
    s = str(s or "")
    return s if len(s) <= n else s[: n - 1] + "…"


def _fmt_age_ms(ts_ms: int, now_ms: int) -> str:
    if not ts_ms:
        return "—"
    d = max(0, now_ms - ts_ms)
    if d < 1000: return f"{d}ms"
    if d < 60_000: return f"{d / 1000:.1f}s"
    if d < 3_600_000: return f"{d / 60_000:.1f}m"
    return f"{d / 3_600_000:.1f}h"


# ── status → colour ───────────────────────────────────────────────────────


_STATUS_STYLES = {
    "pending":     "yellow",
    "confirmed":   "bold green",
    "failed":      "bold red",
    "unknown":     "bold red on yellow",
    "committed":   "magenta",
    "compensated": "bold green",
    "stuck":       "bold red on yellow",
}


def _status_style(s: str) -> str:
    return _STATUS_STYLES.get((s or "").lower(), "white")


# ── the snapshot ───────────────────────────────────────────────────────────


async def _snapshot(svc, app_name: str, user_id: str, session_id: str) -> dict:
    """Pull everything the inspector renders, in one pass."""
    from sqlalchemy import select
    from tape_adk.schemas import (
        StorageEffect, StorageObligation, StorageTimer,
    )
    # ADK's own session + events tables.
    from google.adk.sessions.schemas.v1 import StorageSession, StorageEvent

    out: dict = {
        "session": None, "events": [], "effects": [],
        "obligations": [], "timers": [],
    }
    async with svc._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
        sess_row = (await sql.execute(
            select(StorageSession).where(
                StorageSession.app_name == app_name,
                StorageSession.user_id == user_id,
                StorageSession.id == session_id,
            ))).scalars().one_or_none()
        if sess_row is None:
            return out
        out["session"] = {
            "app_name": sess_row.app_name,
            "user_id": sess_row.user_id,
            "id": sess_row.id,
            "state": dict(sess_row.state or {}),
            "update_time": sess_row.update_time,
            "create_time": sess_row.create_time,
        }
        ev_rows = (await sql.execute(
            select(StorageEvent).where(
                StorageEvent.app_name == app_name,
                StorageEvent.user_id == user_id,
                StorageEvent.session_id == session_id,
            ).order_by(StorageEvent.timestamp))).scalars().all()
        out["events"] = [
            {"id": e.id, "invocation_id": e.invocation_id,
             "timestamp": e.timestamp, "event_data": e.event_data}
            for e in ev_rows
        ]
        eff_rows = (await sql.execute(
            select(StorageEffect).where(
                StorageEffect.app_name == app_name,
                StorageEffect.user_id == user_id,
                StorageEffect.session_id == session_id,
            ).order_by(StorageEffect.ts_ms))).scalars().all()
        out["effects"] = [
            {"idempotency_key": e.idempotency_key,
             "invocation_id": e.invocation_id, "tool_name": e.tool_name,
             "decision_index": e.decision_index, "call_index": e.call_index,
             "status": e.status, "semantics": e.semantics,
             "dispatch_mode": e.dispatch_mode,
             "business_key": e.business_key, "connector": e.connector,
             "external_ref": e.external_ref,
             "dispatch_attempts": e.dispatch_attempts,
             "dispatch_claimed_by": e.dispatch_claimed_by,
             "request_json": e.request_json,
             "response_json": e.response_json,
             "error_json": e.error_json, "ts_ms": e.ts_ms}
            for e in eff_rows
        ]
        ob_rows = (await sql.execute(
            select(StorageObligation).where(
                StorageObligation.app_name == app_name,
                StorageObligation.user_id == user_id,
                StorageObligation.session_id == session_id,
            ).order_by(StorageObligation.seq.desc()))).scalars().all()
        out["obligations"] = [
            {"seq": o.seq, "kind": o.kind, "effect_key": o.effect_key,
             "status": o.status, "attempts": o.attempts,
             "max_attempts": o.max_attempts,
             "claimed_by": o.claimed_by,
             "last_error": o.last_error, "ts_ms": o.ts_ms,
             "payload_json": o.payload_json,
             "result_json": o.result_json}
            for o in ob_rows
        ]
        tm_rows = (await sql.execute(
            select(StorageTimer).where(
                StorageTimer.app_name == app_name,
                StorageTimer.user_id == user_id,
                StorageTimer.session_id == session_id,
            ).order_by(StorageTimer.fire_at_ms))).scalars().all()
        out["timers"] = [
            {"timer_id": t.timer_id, "fire_at_ms": t.fire_at_ms,
             "kind": t.kind, "fired": t.fired,
             "payload_json": t.payload_json}
            for t in tm_rows
        ]
    return out


# ── renderers ─────────────────────────────────────────────────────────────


def _header_panel(session: dict, db_url: str) -> Panel:
    kv = Table.grid(padding=(0, 2))
    kv.add_column(style="dim")
    kv.add_column()
    kv.add_row("db", f"[cyan]{db_url}[/cyan]")
    if session is None:
        return Panel(Text("(session not found)", style="red"),
                     title="[bold]session[/bold]", border_style="red")
    kv.add_row("app/user/session",
                f"[bold]{session['app_name']}[/bold] / "
                f"{session['user_id']} / {session['id']}")
    if session.get("state"):
        kv.add_row("state keys",
                    ", ".join(sorted(session["state"].keys())) or "(empty)")
    kv.add_row("created", str(session["create_time"]))
    kv.add_row("updated", str(session["update_time"]))
    return Panel(kv, title=f"[bold]SESSION[/bold] [cyan]{session['id']}[/cyan]",
                 border_style="cyan", padding=(0, 1))


def _events_table(events: list[dict]) -> Table:
    """ADK's own events. Each row shows the authors + a content preview."""
    t = Table(title=f"ADK events ({len(events)})", title_style="bold dim",
              header_style="bold dim", show_edge=False, pad_edge=False,
              expand=True)
    t.add_column("ts", style="dim", no_wrap=True, width=10)
    t.add_column("invocation", overflow="ellipsis", no_wrap=True, max_width=18)
    t.add_column("author", style="cyan", no_wrap=True)
    t.add_column("kind", no_wrap=True)
    t.add_column("preview", overflow="ellipsis", no_wrap=True)
    if not events:
        t.add_row("", "", "", Text("(none)", style="dim"), "")
        return t
    for e in events:
        data = e.get("event_data") or {}
        author = data.get("author", "?")
        # Inspect the content for function_call / function_response / text.
        kind = "text"
        preview = ""
        try:
            content = data.get("content") or {}
            parts = content.get("parts") or []
            for p in parts:
                if "function_call" in p and p["function_call"]:
                    fc = p["function_call"]
                    kind = "function_call"
                    preview = f"{fc.get('name', '?')}({list((fc.get('args') or {}).keys())})"
                    break
                if "function_response" in p and p["function_response"]:
                    fr = p["function_response"]
                    kind = "function_response"
                    preview = f"{fr.get('name', '?')} → {_short(json.dumps(fr.get('response') or {}), 50)}"
                    break
                if "text" in p and p["text"]:
                    kind = "text"
                    preview = _short(p["text"], 60)
                    break
        except Exception:
            preview = "(unparsable event_data)"
        t.add_row(_short(str(e["timestamp"]), 10),
                   _short(e["invocation_id"], 18),
                   author, kind, preview)
    return t


def _effects_table(effects: list[dict], now_ms: int) -> Table:
    t = Table(title=f"Effects ({len(effects)})", title_style="bold",
              header_style="bold dim", show_edge=False, pad_edge=False,
              expand=True)
    t.add_column("key", overflow="ellipsis", no_wrap=True, max_width=30)
    t.add_column("status")
    t.add_column("tool")
    t.add_column("mode")
    t.add_column("business_key", overflow="ellipsis", no_wrap=True, max_width=22)
    t.add_column("external_ref", overflow="ellipsis", no_wrap=True, max_width=18)
    t.add_column("age", justify="right")
    if not effects:
        t.add_row("", Text("(none)", style="dim"), "", "", "", "", "")
        return t
    for e in effects:
        t.add_row(
            e["idempotency_key"],
            Text(e["status"], style=_status_style(e["status"])),
            e["tool_name"],
            f"{e['semantics'][:3]}/{e['dispatch_mode'][:3]}",
            e["business_key"] or "[dim]—[/dim]",
            e["external_ref"] or "[dim]—[/dim]",
            _fmt_age_ms(e["ts_ms"], now_ms),
        )
    return t


def _obligations_table(obligations: list[dict], now_ms: int) -> Table:
    t = Table(title=f"Obligations ({len(obligations)})", title_style="bold",
              header_style="bold dim", show_edge=False, pad_edge=False,
              expand=True)
    t.add_column("seq", style="dim", justify="right")
    t.add_column("kind")
    t.add_column("status")
    t.add_column("effect_key", overflow="ellipsis", no_wrap=True, max_width=28)
    t.add_column("attempts", justify="right")
    t.add_column("age", justify="right")
    if not obligations:
        t.add_row("", "", Text("(none)", style="dim"), "", "", "")
        return t
    for o in obligations:
        t.add_row(
            str(o["seq"]), o["kind"],
            Text(o["status"], style=_status_style(o["status"])),
            o["effect_key"],
            f"{o['attempts']}/{o['max_attempts']}",
            _fmt_age_ms(o["ts_ms"], now_ms),
        )
    return t


def _timers_table(timers: list[dict], now_ms: int) -> Table:
    t = Table(title=f"Timers ({len(timers)})", title_style="bold",
              header_style="bold dim", show_edge=False, pad_edge=False,
              expand=True)
    t.add_column("timer_id", overflow="ellipsis", no_wrap=True, max_width=24)
    t.add_column("kind")
    t.add_column("fired")
    t.add_column("fire_at / overdue", no_wrap=True)
    if not timers:
        t.add_row("", "", Text("(none)", style="dim"), "")
        return t
    for t_ in timers:
        when = (_fmt_age_ms(0, max(0, now_ms - t_["fire_at_ms"]))
                + " ago" if t_["fire_at_ms"] <= now_ms
                else f"in {_fmt_age_ms(0, max(0, t_['fire_at_ms'] - now_ms))}")
        t.add_row(
            t_["timer_id"], t_["kind"],
            "✓" if t_["fired"] else "·",
            when,
        )
    return t


# ── live cross-session journal (the `tape dev` view) ───────────────────────


async def _global_snapshot(svc) -> dict:
    """Cross-session pull — everything recent, for the live `tape dev` view.
    The dev loop creates many sessions; this shows the whole journal, not
    one session."""
    from sqlalchemy import select, desc
    from tape_adk.schemas import StorageEffect, StorageObligation, StorageTimer

    out: dict = {"effects": [], "obligations": [], "timers": []}
    async with svc._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
        eff = (await sql.execute(
            select(StorageEffect).order_by(desc(StorageEffect.ts_ms))
            .limit(40))).scalars().all()
        out["effects"] = [
            {"idempotency_key": e.idempotency_key, "tool_name": e.tool_name,
             "status": e.status, "semantics": e.semantics,
             "dispatch_mode": e.dispatch_mode, "business_key": e.business_key,
             "external_ref": e.external_ref, "ts_ms": e.ts_ms,
             "session_id": e.session_id}
            for e in eff
        ]
        ob = (await sql.execute(
            select(StorageObligation).order_by(desc(StorageObligation.seq))
            .limit(20))).scalars().all()
        out["obligations"] = [
            {"seq": o.seq, "kind": o.kind, "status": o.status,
             "attempts": o.attempts, "max_attempts": o.max_attempts,
             "effect_key": o.effect_key, "ts_ms": o.ts_ms}
            for o in ob
        ]
        tm = (await sql.execute(
            select(StorageTimer).order_by(desc(StorageTimer.fire_at_ms))
            .limit(20))).scalars().all()
        out["timers"] = [
            {"timer_id": t.timer_id, "kind": t.kind, "fired": t.fired,
             "fire_at_ms": t.fire_at_ms}
            for t in tm
        ]
    return out


def _live_panel(snap: dict, db_url: str, *, reactor_note: str = "") -> Group:
    now_ms = int(time.time() * 1000)
    effects = snap["effects"]
    by_status: dict[str, int] = {}
    for e in effects:
        by_status[e["status"]] = by_status.get(e["status"], 0) + 1

    head = Table.grid(padding=(0, 2))
    head.add_column(style="dim")
    head.add_column()
    head.add_row("db", f"[cyan]{db_url}[/cyan]")
    head.add_row("checked", time.strftime("%H:%M:%S", time.localtime()))
    counts = "  ".join(
        f"[{_status_style(s)}]{s}={n}[/{_status_style(s)}]"
        for s, n in sorted(by_status.items())) or "[dim](no effects yet)[/dim]"
    head.add_row("effects", counts)
    n_ob = sum(1 for o in snap["obligations"]
               if o["status"] in ("pending", "committed"))
    n_stuck = sum(1 for o in snap["obligations"] if o["status"] == "stuck")
    head.add_row("obligations",
                 f"open={n_ob}  "
                 + (f"[bold red on yellow]stuck={n_stuck}[/bold red on yellow]"
                    if n_stuck else "[dim]stuck=0[/dim]"))
    if reactor_note:
        head.add_row("reactors", reactor_note)

    eff_t = Table(title="recent effects", title_style="bold dim",
                  header_style="bold dim", show_edge=False, pad_edge=False,
                  expand=True)
    eff_t.add_column("age", style="dim", justify="right", width=8)
    eff_t.add_column("tool")
    eff_t.add_column("status")
    eff_t.add_column("mode", width=8)
    eff_t.add_column("business_key / ref", overflow="ellipsis", no_wrap=True)
    if not effects:
        eff_t.add_row("", Text("waiting for the agent to journal effects…",
                               style="dim"), "", "", "")
    for e in effects:
        ref = e["external_ref"] or e["business_key"] or "—"
        eff_t.add_row(
            _fmt_age_ms(e["ts_ms"], now_ms),
            e["tool_name"],
            Text(e["status"], style=_status_style(e["status"])),
            f"{e['semantics'][:3]}/{e['dispatch_mode'][:3]}",
            str(ref),
        )
    panels = [Panel(head, title="[bold]tape dev[/bold] — live journal",
                    border_style="cyan", padding=(0, 1)), eff_t]
    if snap["obligations"]:
        panels.append(_obligations_table(
            [{**o, "ts_ms": o["ts_ms"]} for o in snap["obligations"]],
            now_ms))
    return Group(*panels)


def live_journal(db_url: str, *, reactor_note: str = "",
                 interval_s: float = 0.6) -> None:
    """Open a live, polling view of the embedded journal. Used by
    `tape inspect-adk --follow` and by `tape dev` (embedded tier).

    SQLite has no LISTEN/NOTIFY, so this polls every `interval_s`. Ctrl-C
    to stop."""
    try:
        from tape_adk import TapeSessionService
    except ImportError:
        die("tape-adk not installed.\n  Fix: pip install tape-adk")
    svc = TapeSessionService(db_url=db_url)

    class _Poll:
        def __init__(_s):
            _s._snap = {"effects": [], "obligations": [], "timers": []}
            _s._next = 0.0
        def __rich__(_s):
            now = time.time()
            if now >= _s._next:
                try:
                    _s._snap = asyncio.run(_global_snapshot(svc))
                except Exception:  # noqa: BLE001
                    pass
                _s._next = now + interval_s
            return _live_panel(_s._snap, db_url, reactor_note=reactor_note)

    try:
        with Live(_Poll(), refresh_per_second=4, console=console,
                  screen=False):
            while True:
                time.sleep(0.25)
    except KeyboardInterrupt:
        pass


# ── the typer command ─────────────────────────────────────────────────────


def run(
    session_id: Optional[str] = typer.Argument(
        None, help="ADK session id to inspect. Omit with --follow for the "
                   "live cross-session view."),
    app_name: Optional[str] = typer.Option(
        None, "--app", help="ADK app name (the `name=` on App). "
                            "Required unless --follow."),
    user_id: Optional[str] = typer.Option(
        None, "--user", help="ADK user id. Required unless --follow."),
    db_url: Optional[str] = typer.Option(
        None, "--db-url",
        help="SQLAlchemy URL of the tape-adk store. "
             "Default: $TAPE_ADK_DB_URL."),
    follow: bool = typer.Option(
        False, "--follow", "-f",
        help="Live cross-session journal view (polls the store). "
             "Ignores the session-id argument."),
    raw: bool = typer.Option(
        False, "--raw",
        help="Emit JSON instead of the rendered panels (for jq / scripts)."),
):
    """Inspect the tape-adk (embedded) journal.

    Two modes:

      tape inspect-adk --follow                  live cross-session view
      tape inspect-adk <id> --app A --user U     snapshot one session

    For multi-run-fleet triage use `tape doctor --live --db-url …`.
    """
    resolved = _resolve_db_url(db_url)

    if follow:
        live_journal(resolved)
        return

    if not session_id or not app_name or not user_id:
        die("snapshot mode needs <session-id> + --app + --user "
            "(or pass --follow for the live view).")
    try:
        from tape_adk import TapeSessionService
    except ImportError:
        die("tape-adk not installed.\n  Fix: pip install tape-adk")

    svc = TapeSessionService(db_url=resolved)
    snap = asyncio.run(_snapshot(svc, app_name, user_id, session_id))
    if snap["session"] is None:
        die(f"session not found: app={app_name} user={user_id} "
            f"id={session_id}")

    if raw:
        print(json.dumps(snap, default=str, indent=2))
        return

    import time
    now_ms = int(time.time() * 1000)
    console.print(_header_panel(snap["session"], resolved))
    console.print(_events_table(snap["events"]))
    console.print(_effects_table(snap["effects"], now_ms))
    console.print(_obligations_table(snap["obligations"], now_ms))
    console.print(_timers_table(snap["timers"], now_ms))

    # Loud-failure exit code: any UNKNOWN effect or STUCK obligation
    # makes this command exit non-zero, mirroring `tape doctor`.
    bad_eff = sum(1 for e in snap["effects"] if e["status"] == "unknown")
    bad_ob = sum(1 for o in snap["obligations"] if o["status"] == "stuck")
    if bad_ob: raise typer.Exit(2)
    if bad_eff: raise typer.Exit(1)
