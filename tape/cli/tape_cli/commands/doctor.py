"""`tape doctor` — local + GCP checks AND operational triage against a live
tape-server.

Two distinct jobs share the verb:

* **environment** (default): Python / ADK / Docker / Cargo / TAPE_URL
  reachable / required GCP APIs — the "can you build and run this" checks.
* **operational** (`--live`): runs needing recovery, effects PENDING beyond
  a threshold, effects in UNKNOWN, stuck obligations, outbox dispatch lag,
  reactor DLQ — the "is the running system healthy" view. The closest
  analogue is `htop` for a workflow engine; `--watch` makes it refresh in
  place so you can stand over it during an incident.
"""

from __future__ import annotations

import importlib
import os
import socket
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import typer
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table as RichTable
from rich.text import Text

from ..config import TapeProject, find_project_root
from ..util import console, ok, warn, fail, info, which, section, die


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ""
    hint: str = ""


def _check(name: str):
    def deco(fn):
        fn._check_name = name
        return fn
    return deco


# ── local checks ────────────────────────────────────────────────────────────

@_check("Python ≥ 3.10")
def _check_python() -> CheckResult:
    py = sys.version_info
    if py >= (3, 10):
        return CheckResult("Python", True, f"{py.major}.{py.minor}.{py.micro}")
    return CheckResult("Python", False, f"{py.major}.{py.minor}.{py.micro}",
                       hint="install Python 3.10+ (3.12 recommended).")


@_check("ADK importable")
def _check_adk() -> CheckResult:
    try:
        import google.adk  # noqa: F401
        return CheckResult("ADK", True)
    except Exception as ex:
        return CheckResult("ADK", False, str(ex), hint="pip install google-adk>=1.30")


@_check("Tape SDK importable")
def _check_tape() -> CheckResult:
    try:
        import tape  # noqa: F401
        from tape.adk import TapePlugin, TapeSessionService, durable_app  # noqa: F401
        return CheckResult("Tape SDK", True)
    except Exception as ex:
        return CheckResult("Tape SDK", False, str(ex), hint="pip install tape-py")


@_check("Docker available")
def _check_docker() -> CheckResult:
    p = which("docker")
    if p:
        return CheckResult("Docker", True, p)
    return CheckResult("Docker", False, hint="install Docker if you want `tape dev` to use compose.")


@_check("Cargo available (for building the server)")
def _check_cargo() -> CheckResult:
    p = which("cargo")
    if p:
        return CheckResult("Cargo", True, p)
    return CheckResult("Cargo", False, hint="optional — only needed to build tape-server from source.")


def _check_tape_server(project: TapeProject) -> CheckResult:
    url = project.tape.url.replace("tape://", "").replace("tapes://", "").replace("grpc://", "")
    host, _, port = url.partition(":")
    port = port.split("/")[0] or ("443" if "tapes://" in project.tape.url else "7878")
    try:
        with socket.create_connection((host, int(port)), timeout=2.0):
            return CheckResult(f"Tape server {project.tape.url}", True)
    except Exception as ex:
        return CheckResult(f"Tape server {project.tape.url}", False, str(ex),
                           hint="`tape dev` to start one locally, or `tape deploy gcp` to ship it.")


def _check_env_var(name: str, required: bool) -> CheckResult:
    val = os.environ.get(name)
    if val:
        return CheckResult(name, True, f"set to {val[:20]}{'…' if len(val) > 20 else ''}")
    return CheckResult(name, not required, "unset",
                       hint=f"export {name}=... in your environment or .env")


# ── GCP checks ──────────────────────────────────────────────────────────────

def _check_gcloud_auth() -> CheckResult:
    if not which("gcloud"):
        return CheckResult("gcloud installed", False, hint="https://cloud.google.com/sdk/docs/install")
    return CheckResult("gcloud installed", True, which("gcloud") or "")


def _check_adc() -> CheckResult:
    try:
        import google.auth
        creds, project = google.auth.default()
        return CheckResult("Application Default Credentials", True,
                           f"project={project}, type={type(creds).__name__}")
    except Exception as ex:
        return CheckResult("Application Default Credentials", False, str(ex),
                           hint="gcloud auth application-default login")


def _check_gcp_apis(project_id: str, apis: list[str]) -> list[CheckResult]:
    if not project_id:
        return [CheckResult("GCP APIs", False, "no project_id set",
                            hint="set gcp.project_id in tape.yaml or GOOGLE_CLOUD_PROJECT")]
    try:
        from google.cloud import serviceusage_v1
        client = serviceusage_v1.ServiceUsageClient()
    except Exception as ex:
        return [CheckResult("GCP APIs", False, str(ex),
                            hint="pip install google-cloud-service-usage, or skip this check.")]
    out: list[CheckResult] = []
    for api in apis:
        try:
            name = f"projects/{project_id}/services/{api}"
            svc = client.get_service(name=name)
            enabled = str(svc.state) == "State.ENABLED" or "ENABLED" in str(svc.state)
            out.append(CheckResult(f"API {api}", enabled,
                                   hint=f"gcloud services enable {api}" if not enabled else ""))
        except Exception as ex:
            out.append(CheckResult(f"API {api}", False, str(ex),
                                   hint=f"gcloud services enable {api}"))
    return out


def _print(results: list[CheckResult]) -> int:
    n_fail = 0
    for r in results:
        if r.ok:
            ok(f"{r.name}" + (f"  [dim]({r.detail})[/dim]" if r.detail else ""))
        else:
            n_fail += 1
            fail(r.name + (f"  [dim]({r.detail})[/dim]" if r.detail else ""), hint=r.hint or None)
    return n_fail


# ── live triage (the operator's hot-incident view) ────────────────────────


def _resolve_url(url_flag: Optional[str]) -> str:
    """Same resolution order as `tape inspect`: --url > $TAPE_URL > tape.yaml >
    the SDK default. Read-only commands shouldn't require a project root."""
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


# Map RunStatus int → label (mirrors proto and _journal.run_status_label).
_RUN_STATUS = {
    0: ("UNSPECIFIED", "dim"),
    1: ("RUNNABLE", "yellow"),
    2: ("RUNNING", "bold cyan"),
    3: ("WAITING", "blue"),
    4: ("TERMINAL", "bold green"),
    5: ("FAILED", "bold red"),
    6: ("COMPENSATING", "magenta"),
    7: ("STUCK", "bold red on yellow"),
    8: ("CANCELLED", "yellow"),
}

_EFFECT_STATUS = {
    0: "UNSPECIFIED", 1: "PENDING", 2: "CONFIRMED", 3: "FAILED", 4: "UNKNOWN",
}

_OBLIGATION_STATUS = {
    0: "UNSPECIFIED", 1: "PENDING", 2: "COMMITTED",
    3: "COMPENSATED", 4: "STUCK",
}


def _age_ms(ts_ms: int, now_ms: int) -> str:
    """Render an age in ms as a compact duration suffix."""
    if ts_ms <= 0:
        return "—"
    d = max(0, now_ms - ts_ms)
    if d < 1_000:
        return f"{d}ms"
    if d < 60_000:
        return f"{d / 1000:.1f}s"
    if d < 3_600_000:
        return f"{d / 60_000:.1f}m"
    return f"{d / 3_600_000:.1f}h"


@dataclass
class _Snapshot:
    """One pull from the server — everything the triage view shows."""
    runs_to_recover: list
    effects_pending_stale: list
    effects_unknown: list
    obligations_stuck: list
    obligations_pending: list
    outbox_ready: list
    timers_due: list
    error: str = ""


def _take_snapshot(client, *, pending_threshold_ms: int) -> _Snapshot:
    """Pull everything we need in one pass. Tolerates partial server support
    (older servers may not implement every RPC — each call is best-effort)."""
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - pending_threshold_ms

    err_parts: list[str] = []

    def _safe(label: str, fn):
        try:
            return fn()
        except Exception as ex:  # noqa: BLE001
            err_parts.append(f"{label}: {ex}")
            return []

    # `list_runs_to_recover` returns RUNNABLE + expired-lease RUNNING + signal-
    # released WAITING — the union of "needs the recovery reactor's attention".
    runs = _safe("list_runs_to_recover",
                 lambda: list(client.list_runs_to_recover(limit=200).runs))
    # `list_pending_effects` with both flags = the reconciler's hot set:
    # PENDING+stale plus UNKNOWN.
    pending_stale = _safe(
        "list_pending_effects(pending)",
        lambda: list(client.list_pending_effects(
            older_than_ms=cutoff, include_pending=True,
            include_unknown=False, limit=200).effects))
    unknowns = _safe(
        "list_pending_effects(unknown)",
        lambda: list(client.list_pending_effects(
            older_than_ms=0, include_pending=False,
            include_unknown=True, limit=200).effects))
    # Drainer queues — stuck (terminal-needs-human) + pending (waiting).
    stuck_ob = _safe(
        "list_unresolved_obligations(stuck)",
        lambda: list(client.list_unresolved_obligations(
            limit=200, include_pending=False, include_stuck=True,
            include_committed_expired=False).obligations))
    pending_ob = _safe(
        "list_unresolved_obligations(pending)",
        lambda: list(client.list_unresolved_obligations(
            limit=200, include_pending=True, include_stuck=False,
            include_committed_expired=True).obligations))
    outbox = _safe(
        "list_effects_to_dispatch",
        lambda: list(client.list_effects_to_dispatch(now_ms=now_ms,
                                                       limit=200).effects))
    timers = _safe(
        "list_due_timers",
        lambda: list(client.list_due_timers(now_ms=now_ms, limit=200,
                                              claim=False).timers))
    return _Snapshot(
        runs_to_recover=runs,
        effects_pending_stale=pending_stale,
        effects_unknown=unknowns,
        obligations_stuck=stuck_ob,
        obligations_pending=pending_ob,
        outbox_ready=outbox,
        timers_due=timers,
        error="; ".join(err_parts),
    )


def _render_runs(snap: _Snapshot) -> RichTable:
    t = RichTable(title=f"Runs needing recovery ({len(snap.runs_to_recover)})",
                  title_style="bold", header_style="bold dim",
                  show_edge=False, pad_edge=False, expand=True)
    t.add_column("run_id", overflow="ellipsis", no_wrap=True, max_width=14)
    t.add_column("status")
    t.add_column("app/user/session", overflow="ellipsis", no_wrap=True)
    t.add_column("lease owner")
    t.add_column("waiting on", style="blue")
    if not snap.runs_to_recover:
        t.add_row("[dim]—[/dim]", Text("✓ none", style="green"),
                  "", "", "")
        return t
    for r in snap.runs_to_recover:
        label, style = _RUN_STATUS.get(int(r.status), (f"?{r.status}", "dim"))
        t.add_row(
            r.run_id,
            Text(label, style=style),
            f"{r.app_name}/{r.user_id}/{r.session_id}",
            r.lease_owner or "[dim]—[/dim]",
            r.waiting_on_gate or "",
        )
    return t


def _render_effects(snap: _Snapshot, *, now_ms: int,
                    pending_threshold_ms: int) -> RichTable:
    n_unk = len(snap.effects_unknown)
    n_stale = len(snap.effects_pending_stale)
    title = (f"Effects · UNKNOWN={n_unk}  "
             f"PENDING>{pending_threshold_ms // 1000}s={n_stale}")
    t = RichTable(title=title, title_style="bold", header_style="bold dim",
                  show_edge=False, pad_edge=False, expand=True)
    t.add_column("effect_key", overflow="ellipsis", no_wrap=True, max_width=28)
    t.add_column("status")
    t.add_column("tool")
    t.add_column("business_key", overflow="ellipsis", no_wrap=True, max_width=24)
    t.add_column("age", justify="right")
    t.add_column("run_id", overflow="ellipsis", no_wrap=True, max_width=12)

    rows = []
    # UNKNOWN first — that's the loudest.
    for e in snap.effects_unknown:
        rows.append((e, True))
    for e in snap.effects_pending_stale:
        rows.append((e, False))
    if not rows:
        t.add_row(Text("✓ none", style="green"), "", "", "", "", "")
        return t
    for e, is_unknown in rows:
        status_label = _EFFECT_STATUS.get(int(e.status), str(e.status))
        style = "bold red on yellow" if is_unknown else "yellow"
        t.add_row(
            e.idempotency_key,
            Text(status_label, style=style),
            e.tool_name,
            e.business_key or "[dim]—[/dim]",
            _age_ms(e.ts_ms, now_ms),
            e.run_id,
        )
    return t


def _render_obligations(snap: _Snapshot, *, now_ms: int) -> RichTable:
    n_stuck = len(snap.obligations_stuck)
    n_pending = len(snap.obligations_pending)
    title = f"Obligations · STUCK={n_stuck}  PENDING/expired={n_pending}"
    t = RichTable(title=title, title_style="bold", header_style="bold dim",
                  show_edge=False, pad_edge=False, expand=True)
    t.add_column("kind")
    t.add_column("status")
    t.add_column("effect_key", overflow="ellipsis", no_wrap=True, max_width=32)
    t.add_column("attempts", justify="right")
    t.add_column("age", justify="right")
    t.add_column("run_id", overflow="ellipsis", no_wrap=True, max_width=12)

    rows = [(o, True) for o in snap.obligations_stuck]
    rows += [(o, False) for o in snap.obligations_pending]
    if not rows:
        t.add_row(Text("✓ none", style="green"), "", "", "", "", "")
        return t
    for o, is_stuck in rows:
        label = _OBLIGATION_STATUS.get(int(o.status), str(o.status))
        style = "bold red on yellow" if is_stuck else "magenta"
        t.add_row(
            o.kind,
            Text(label, style=style),
            o.effect_key,
            f"{o.attempts}/{o.max_attempts}",
            _age_ms(o.ts_ms, now_ms),
            o.run_id,
        )
    return t


def _render_outbox(snap: _Snapshot, *, now_ms: int) -> RichTable:
    n = len(snap.outbox_ready)
    t = RichTable(title=f"Outbox · dispatch-ready ({n})",
                  title_style="bold", header_style="bold dim",
                  show_edge=False, pad_edge=False, expand=True)
    t.add_column("effect_key", overflow="ellipsis", no_wrap=True, max_width=28)
    t.add_column("connector")
    t.add_column("attempts", justify="right")
    t.add_column("claimed by")
    t.add_column("waiting", justify="right")
    if not snap.outbox_ready:
        t.add_row(Text("✓ none", style="green"), "", "", "", "")
        return t
    for e in snap.outbox_ready:
        wait_ms = max(0, now_ms - e.ts_ms)
        t.add_row(
            e.idempotency_key,
            e.connector or "[dim]—[/dim]",
            f"{e.dispatch_attempts}",
            e.dispatch_claimed_by or "[dim]—[/dim]",
            _age_ms(e.ts_ms, now_ms),
        )
    return t


def _render_timers(snap: _Snapshot, *, now_ms: int) -> RichTable:
    n = len(snap.timers_due)
    t = RichTable(title=f"Timers · due ({n})",
                  title_style="bold", header_style="bold dim",
                  show_edge=False, pad_edge=False, expand=True)
    t.add_column("kind")
    t.add_column("timer_id", overflow="ellipsis", no_wrap=True, max_width=24)
    t.add_column("overdue", justify="right")
    t.add_column("run_id", overflow="ellipsis", no_wrap=True, max_width=12)
    if not snap.timers_due:
        t.add_row(Text("✓ none", style="green"), "", "", "")
        return t
    for tm in snap.timers_due:
        overdue = max(0, now_ms - tm.fire_at_ms)
        t.add_row(tm.kind, tm.timer_id, _age_ms(0, overdue), tm.run_id)
    return t


def _render_report(snap: _Snapshot, *, url: str,
                   pending_threshold_ms: int) -> Group:
    now_ms = int(time.time() * 1000)

    # Single-line summary: counts + a green-vs-red verdict on each axis.
    summary = RichTable.grid(padding=(0, 2), expand=True)
    summary.add_column(style="dim bold", no_wrap=True)
    summary.add_column()

    def _row(label: str, n: int, *, danger_above: int = 0):
        good = n == 0 if danger_above == 0 else n <= danger_above
        style = "green" if good else "bold red on yellow"
        glyph = "✓" if good else "!"
        summary.add_row(label, Text(f"{glyph} {n}", style=style))

    summary.add_row("server", Text(url, style="cyan"))
    summary.add_row("checked", time.strftime("%Y-%m-%d %H:%M:%S UTC",
                                              time.gmtime(now_ms / 1000)))
    _row("runs needing recovery", len(snap.runs_to_recover))
    _row("effects UNKNOWN", len(snap.effects_unknown))
    _row(f"effects PENDING > {pending_threshold_ms // 1000}s",
         len(snap.effects_pending_stale))
    _row("obligations STUCK", len(snap.obligations_stuck))
    _row("obligations PENDING/expired", len(snap.obligations_pending),
         danger_above=10)
    _row("outbox dispatch-ready", len(snap.outbox_ready), danger_above=20)
    _row("timers overdue", len(snap.timers_due), danger_above=5)
    if snap.error:
        summary.add_row("[red]errors[/red]",
                         Text(snap.error, style="red"))

    return Group(
        Panel(summary, title="[bold]tape doctor --live[/bold]",
              subtitle="[dim]operational triage[/dim]",
              border_style="cyan", padding=(0, 1)),
        _render_runs(snap),
        _render_effects(snap, now_ms=now_ms,
                        pending_threshold_ms=pending_threshold_ms),
        _render_obligations(snap, now_ms=now_ms),
        _render_outbox(snap, now_ms=now_ms),
        _render_timers(snap, now_ms=now_ms),
    )


def _exit_code_from_snapshot(snap: _Snapshot) -> int:
    """Non-zero only on conditions a human probably wants alerted on:
    STUCK obligations or UNKNOWN effects (the loud failure modes). Lots of
    runs-to-recover or outbox lag are normal in busy systems."""
    if snap.obligations_stuck:
        return 2
    if snap.effects_unknown:
        return 1
    return 0


# ── live triage against tape-adk (the embedded path) ─────────────────────


def _live_triage_adk(*, db_url: str, watch: bool, interval: float,
                     pending_threshold_ms: int) -> None:
    """Same shape as `_live_triage` but runs against a `TapeSessionService`
    (SQLAlchemy) instead of `TapeClient` (gRPC). The tape-adk model is
    session-keyed (no separate run lifecycle / lease), so the "runs needing
    recovery" section is replaced with an ADK-sessions overview."""
    try:
        from tape_adk import TapeSessionService
        from tape_adk.schemas import StorageEffect, StorageObligation, StorageTimer
        from sqlalchemy import select, func as sa_func
    except ImportError:
        die("tape-adk not installed.\n  Fix: pip install tape-adk")

    import asyncio

    async def _snapshot(svc: TapeSessionService) -> _Snapshot:
        """Pull the embedded equivalent of `_take_snapshot`."""
        now = int(time.time() * 1000)
        cutoff = now - pending_threshold_ms
        # Effects: PENDING > threshold and UNKNOWN.
        pending_stale = await svc.list_pending_effects(
            older_than_ms=cutoff, include_pending=True,
            include_unknown=False, limit=200)
        unknowns = await svc.list_pending_effects(
            older_than_ms=0, include_pending=False,
            include_unknown=True, limit=200)
        outbox = await svc.list_effects_to_dispatch(now_ms=now, limit=200)
        timers = await svc.list_due_timers(now_ms=now, limit=200, claim=False)
        stuck_ob = await svc.list_unresolved_obligations(
            limit=200, include_pending=False, include_stuck=True,
            include_committed_expired=False)
        pending_ob = await svc.list_unresolved_obligations(
            limit=200, include_pending=True, include_stuck=False,
            include_committed_expired=True)
        return _Snapshot(
            runs_to_recover=[],   # n/a in the embedded model
            effects_pending_stale=pending_stale,
            effects_unknown=unknowns,
            obligations_stuck=stuck_ob,
            obligations_pending=pending_ob,
            outbox_ready=outbox,
            timers_due=timers,
        )

    async def _session_count(svc: TapeSessionService) -> int:
        """A cheap "is anything alive" signal."""
        from google.adk.sessions.schemas.v1 import StorageSession
        async with svc._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            r = await sql.execute(sa_func.count(StorageSession.id).select())  # noqa: F841
        # SQLAlchemy 2.0+ idiom: select(func.count(col))
        async with svc._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            n = (await sql.execute(
                select(sa_func.count(StorageSession.id)))).scalar() or 0
        return int(n)

    def _render_adk_report(snap: _Snapshot, sess_count: int) -> Group:
        """Same render shape as the gRPC path but with a sessions-overview
        instead of runs-needing-recovery."""
        now_ms = int(time.time() * 1000)
        summary = RichTable.grid(padding=(0, 2), expand=True)
        summary.add_column(style="dim bold", no_wrap=True)
        summary.add_column()

        def _row(label: str, n: int, *, danger_above: int = 0):
            good = n == 0 if danger_above == 0 else n <= danger_above
            style = "green" if good else "bold red on yellow"
            glyph = "✓" if good else "!"
            summary.add_row(label, Text(f"{glyph} {n}", style=style))

        summary.add_row("db", Text(db_url, style="cyan"))
        summary.add_row("checked", time.strftime("%Y-%m-%d %H:%M:%S UTC",
                                                   time.gmtime(now_ms / 1000)))
        summary.add_row("ADK sessions",
                         Text(str(sess_count), style="dim"))
        _row("effects UNKNOWN", len(snap.effects_unknown))
        _row(f"effects PENDING > {pending_threshold_ms // 1000}s",
             len(snap.effects_pending_stale))
        _row("obligations STUCK", len(snap.obligations_stuck))
        _row("obligations PENDING/expired", len(snap.obligations_pending),
             danger_above=10)
        _row("outbox dispatch-ready", len(snap.outbox_ready), danger_above=20)
        _row("timers overdue", len(snap.timers_due), danger_above=5)

        # Adapt the dataclass records into the shape the existing
        # render_effects / render_obligations / render_outbox / render_timers
        # functions expect. They access proto-like fields; our dataclasses
        # have the same field names (status, idempotency_key, tool_name,
        # business_key, ts_ms, run_id-vs-invocation_id, etc.) — only the
        # `run_id` column we display needs the swap.
        from types import SimpleNamespace

        def _eff_adapt(e):
            ns = SimpleNamespace(**e.__dict__)
            ns.run_id = e.invocation_id  # what the renderer displays
            ns.status = _EFFECT_STATUS_RMAP.get(e.status, 0)
            return ns

        def _ob_adapt(o):
            ns = SimpleNamespace(**o.__dict__)
            ns.run_id = o.invocation_id
            ns.status = _OBLIGATION_STATUS_RMAP.get(o.status, 0)
            return ns

        def _t_adapt(t):
            ns = SimpleNamespace(**t.__dict__)
            ns.run_id = t.session_id
            return ns

        adapted = _Snapshot(
            runs_to_recover=[],
            effects_pending_stale=[_eff_adapt(e) for e in snap.effects_pending_stale],
            effects_unknown=[_eff_adapt(e) for e in snap.effects_unknown],
            obligations_stuck=[_ob_adapt(o) for o in snap.obligations_stuck],
            obligations_pending=[_ob_adapt(o) for o in snap.obligations_pending],
            outbox_ready=[_eff_adapt(e) for e in snap.outbox_ready],
            timers_due=[_t_adapt(t) for t in snap.timers_due],
        )

        return Group(
            Panel(summary, title="[bold]tape doctor --live --db-url[/bold]",
                  subtitle="[dim]operational triage (embedded)[/dim]",
                  border_style="cyan", padding=(0, 1)),
            _render_effects(adapted, now_ms=now_ms,
                            pending_threshold_ms=pending_threshold_ms),
            _render_obligations(adapted, now_ms=now_ms),
            _render_outbox(adapted, now_ms=now_ms),
            _render_timers(adapted, now_ms=now_ms),
        )

    svc = TapeSessionService(db_url=db_url)

    async def _one_pass() -> tuple[_Snapshot, int]:
        snap = await _snapshot(svc)
        sess = await _session_count(svc)
        return snap, sess

    if watch:
        # Lazy renderable that re-fetches each tick.
        last_snap, last_sess = asyncio.run(_one_pass())
        class _LivePoll:
            def __init__(_self):
                _self._snap, _self._sess = last_snap, last_sess
                _self._next = time.time() + interval
            def __rich__(_self):
                now = time.time()
                if now >= _self._next:
                    try:
                        _self._snap, _self._sess = asyncio.run(_one_pass())
                    except Exception:  # noqa: BLE001
                        pass
                    _self._next = now + interval
                return _render_adk_report(_self._snap, _self._sess)
        try:
            with Live(_LivePoll(), refresh_per_second=4,
                      console=console, screen=False) as _:
                while True:
                    time.sleep(0.25)
        except KeyboardInterrupt:
            pass
    else:
        snap, sess = asyncio.run(_one_pass())
        console.print(_render_adk_report(snap, sess))
        rc = _exit_code_from_snapshot(snap)
        if rc:
            raise typer.Exit(rc)


# String→int maps so the embedded path can reuse the gRPC renderers
# (which look up status via _EFFECT_STATUS / _OBLIGATION_STATUS dicts).
_EFFECT_STATUS_RMAP = {
    "unspecified": 0, "pending": 1, "confirmed": 2,
    "failed": 3, "unknown": 4,
}
_OBLIGATION_STATUS_RMAP = {
    "unspecified": 0, "pending": 1, "committed": 2,
    "compensated": 3, "stuck": 4,
}


def _live_triage(*, url: Optional[str], watch: bool, interval: float,
                 pending_threshold_ms: int) -> None:
    resolved = _resolve_url(url)
    try:
        from tape.client import TapeClient
    except ImportError:
        die("tape-py not installed.\n  Fix: pip install tape-py")

    try:
        client = TapeClient(resolved)
    except Exception as ex:  # noqa: BLE001
        die(f"failed to connect to {resolved}: {ex}")

    try:
        if watch:
            # Bind a lazy renderable so each Live tick re-pulls the server.
            class _LivePoll:
                def __init__(_self):
                    _self._last = _take_snapshot(
                        client, pending_threshold_ms=pending_threshold_ms)
                    _self._next_pull = time.time() + interval
                def __rich__(_self):
                    now = time.time()
                    if now >= _self._next_pull:
                        _self._last = _take_snapshot(
                            client, pending_threshold_ms=pending_threshold_ms)
                        _self._next_pull = now + interval
                    return _render_report(
                        _self._last, url=resolved,
                        pending_threshold_ms=pending_threshold_ms)
            try:
                with Live(_LivePoll(), refresh_per_second=4,
                          console=console, screen=False) as _:
                    while True:
                        time.sleep(0.25)
            except KeyboardInterrupt:
                pass
        else:
            snap = _take_snapshot(client,
                                  pending_threshold_ms=pending_threshold_ms)
            console.print(_render_report(
                snap, url=resolved,
                pending_threshold_ms=pending_threshold_ms))
            rc = _exit_code_from_snapshot(snap)
            if rc:
                raise typer.Exit(rc)
    finally:
        try: client.close()
        except Exception: pass  # noqa: BLE001, E701


# ── entry point ────────────────────────────────────────────────────────────

def run(
    local: bool = typer.Option(True, "--local/--no-local"),
    gcp: bool = typer.Option(False, "--gcp/--no-gcp"),
    agents_cli_aware: bool = typer.Option(False, "--agents-cli-aware",
        help="Also run agents-cli scaffold compatibility checks."),
    live: bool = typer.Option(False, "--live",
        help="Query a running system and report operational health "
             "(UNKNOWN effects, stuck obligations, outbox + timer lag). "
             "Pair with --url (gRPC) or --db-url (tape-adk embedded). "
             "Skips the env checks."),
    watch: bool = typer.Option(False, "--watch", "-w",
        help="With --live: refresh the report in place every --interval "
             "seconds (Ctrl-C to stop). Without --live: noop."),
    interval: float = typer.Option(2.0, "--interval",
        help="Refresh interval in seconds for --watch."),
    pending_threshold_ms: int = typer.Option(
        60_000, "--pending-threshold-ms",
        help="Effects PENDING longer than this are flagged."),
    url: Optional[str] = typer.Option(
        None, "--url",
        help="Tape server URL — selects the gRPC path. "
             "Default: $TAPE_URL or tape.yaml."),
    db_url: Optional[str] = typer.Option(
        None, "--db-url",
        help="SQLAlchemy URL — selects the tape-adk embedded path "
             "(no separate server). Default: $TAPE_ADK_DB_URL. "
             "Mutually exclusive with --url."),
):
    # `--live` is a different verb — it doesn't run env checks. Hand off.
    if live:
        adk_db_url = db_url or os.environ.get("TAPE_ADK_DB_URL")
        if adk_db_url and url:
            die("--url and --db-url are mutually exclusive. Pick one: "
                "--url (gRPC) or --db-url (embedded).")
        if adk_db_url:
            return _live_triage_adk(
                db_url=adk_db_url, watch=watch, interval=interval,
                pending_threshold_ms=pending_threshold_ms)
        return _live_triage(url=url, watch=watch, interval=interval,
                            pending_threshold_ms=pending_threshold_ms)
    project: Optional[TapeProject] = None
    try:
        root = find_project_root()
        project = TapeProject.load(root / "tape.yaml")
        ok(f"Tape project detected — {root}")
    except FileNotFoundError:
        warn("no tape.yaml found; running standalone checks.")

    fails = 0
    if local:
        section("Local")
        local_results = [
            _check_python(),
            _check_adk(),
            _check_tape(),
            _check_docker(),
            _check_cargo(),
        ]
        if project:
            local_results.append(_check_tape_server(project))
            local_results.append(_check_env_var("TAPE_URL", required=False))
            if project.tape.store.kind != "sqlite":
                local_results.append(_check_env_var("TAPE_STORE", required=False))
        fails += _print(local_results)

    if gcp:
        section("GCP")
        gcp_results: list[CheckResult] = [_check_gcloud_auth(), _check_adc()]

        project_id = (project.gcp.project_id if project else "") or os.environ.get("GOOGLE_CLOUD_PROJECT", "")
        if project_id:
            ok(f"project_id = {project_id}")

        apis = ["run.googleapis.com", "artifactregistry.googleapis.com", "secretmanager.googleapis.com"]
        if project and project.tape.events.kind == "pubsub":
            apis.append("pubsub.googleapis.com")
        if project and project.tape.store.kind == "alloydb":
            apis.append("alloydb.googleapis.com")
        if project and project.tape.store.kind == "bigtable":
            apis.append("bigtable.googleapis.com")
        if project and project.tape.store.kind == "spanner":
            apis.append("spanner.googleapis.com")
        gcp_results.extend(_check_gcp_apis(project_id, apis))
        fails += _print(gcp_results)

    if project and agents_cli_aware:
        section("agents-cli compatibility (advisory)")
        adv = []
        if (Path.cwd() / "agents.yaml").exists():
            adv.append(CheckResult("agents.yaml present", True,
                                   "Tape will live alongside your agents-cli scaffold."))
        else:
            adv.append(CheckResult("agents.yaml present", False,
                                   "this is not an agents-cli project — that's fine."))
        fails += _print(adv)

    if project and project.tenancy.mode == "hard_multi_tenant":
        section("Tenancy")
        from tape.tenancy import TenancyConfig
        tc = TenancyConfig(mode=project.tenancy.mode, tenant_id=project.tenancy.tenant_id)  # type: ignore[arg-type]
        for w in tc.warn_if_hard_but_unenforced():
            warn(w)

    info("")
    if fails:
        fail(f"{fails} check(s) failed.")
        raise typer.Exit(1)
    ok("all checks passed.")
