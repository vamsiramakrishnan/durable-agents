"""The replay visualizer — make "replay reads, doesn't write" tangible.

The hardest concept in a durable runtime is what *replay* does. Users read
"the SDK reads the journal instead of re-calling the model" and nod — but
nodding isn't understanding. This screen makes it visible: a side-by-side
view of the run, with **First Run** on the left (what created each entry)
and **Replay** on the right (what re-driving the same agent does to each
entry — read it, short-circuit, no external call).

The mapping is per-kind, mirrors `proto/tape.proto`:

| Kind                   | First Run wrote…              | Replay reads…                                |
|------------------------|-------------------------------|----------------------------------------------|
| `run` (running)        | BeginRun (fresh)              | BeginRun → existing (resumed=true)           |
| `decision`             | RecordDecision (model called) | GetDecision → recorded response (no model)   |
| `effect` (pending)     | BeginEffect (intent written)  | BeginEffect → existing PENDING (no act)      |
| `effect` (confirmed)   | CompleteEffect (call returned)| BeginEffect short-circuits → CONFIRMED       |
| `effect` (failed)      | CompleteEffect (call failed)  | BeginEffect short-circuits → FAILED          |
| `effect` (unknown)     | CompleteEffect (ack lost)     | BeginEffect short-circuits → UNKNOWN; recon. |
| `obligation` (pending) | RegisterCompensation          | ListObligations → already registered         |
| `gate` (waiting)       | AwaitSignal → parked          | AwaitSignal → was parked at this seq         |
| `gate` (released)      | SendSignal arrived            | AwaitSignal → reads delivered resolution     |
| `value`                | WriteValue (CAS commit)       | GetValue → reads at this version             |
| `timer`                | SetTimer (deadline scheduled) | ListDueTimers → existing                     |
| `run` (terminal/...)   | EndRun                        | (no replay — terminal is terminal)            |

Reading it left-to-right teaches the whole replay-as-reads contract.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.coordinate import Coordinate
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Static
from rich.panel import Panel
from rich.table import Table as RichTable
from rich.text import Text

from ._journal import decode_entry, effect_status_label, fmt_rel_ms


# ── per-kind replay semantics ───────────────────────────────────────────────


@dataclass
class ReplayPair:
    """The two halves of one journal entry's story.

    Each field is rich-markup. Both columns are rendered the same width so
    the eye can drift across the row and see the same primitive doing two
    very different things on the two sides.
    """

    first_run: str            # what the first-run agent DID to create this entry
    replay: str               # what a re-drive does INSTEAD (reads / short-circuits)
    first_call: str           # the SDK call the first run made (left badge)
    replay_call: str          # the SDK call a re-drive makes (right badge)
    is_read_only: bool        # True when replay performs no external action


def _safe_loads(s: str) -> dict:
    try:
        v = json.loads(s) if s else {}
    except (json.JSONDecodeError, ValueError):
        return {}
    return v if isinstance(v, dict) else {}


def replay_pair(kind: str, payload_json: str) -> ReplayPair:
    """Map (kind, payload) → the FIRST RUN / REPLAY pair.

    Tolerates both store payload shapes; falls back to a sensible generic
    pair when the kind is unrecognized.
    """
    p = _safe_loads(payload_json)

    if kind == "run":
        status = str(p.get("status", "")).lower()
        if status == "running":
            return ReplayPair(
                first_run="[bold]BeginRun[/bold] — minted a fresh run_id, lease leased",
                replay="[bold]BeginRun[/bold] — returns the existing run_id [dim](resumed=true)[/dim], lease re-leased",
                first_call="[green]write[/green]",
                replay_call="[cyan]read[/cyan]",
                is_read_only=True,
            )
        return ReplayPair(
            first_run=f"[bold]EndRun[/bold] — status → [bold]{status}[/bold]",
            replay=f"[dim]no replay — {status} is terminal[/dim]",
            first_call="[green]write[/green]",
            replay_call="[dim]—[/dim]",
            is_read_only=True,
        )

    if kind == "decision":
        model = p.get("model", "?")
        idx = p.get("decision_index", "?")
        return ReplayPair(
            first_run=f"[bold]RecordDecision[/bold] — called [bold]{model}[/bold] (idx={idx}), persisted the response",
            replay=f"[bold]GetDecision[/bold](decision_index={idx}) — reads the recorded response [bold red]without calling the model[/bold red]",
            first_call="[yellow]model call[/yellow]",
            replay_call="[cyan]read[/cyan]",
            is_read_only=True,
        )

    if kind == "effect":
        status_raw = p.get("status", "")
        status = effect_status_label(status_raw)
        tool = p.get("tool", "") or p.get("tool_name", "")
        if status == "pending":
            return ReplayPair(
                first_run=f"[bold]BeginEffect[/bold]([bold]{tool}[/bold]) — intent journaled, status=[yellow]PENDING[/yellow]",
                replay=f"[bold]BeginEffect[/bold]([bold]{tool}[/bold]) — returns the existing [yellow]PENDING[/yellow] record (the tool body sees the same intent)",
                first_call="[green]write[/green]",
                replay_call="[cyan]read[/cyan]",
                is_read_only=True,
            )
        if status == "confirmed":
            return ReplayPair(
                first_run=f"[bold]CompleteEffect[/bold]([bold]{tool}[/bold]) — tool returned, response journaled, status=[bold green]CONFIRMED[/bold green]",
                replay=f"[bold]BeginEffect[/bold]([bold]{tool}[/bold]) — [bold green]short-circuits[/bold green] on the CONFIRMED record: returns the recorded response [bold red]without calling the tool again[/bold red]",
                first_call="[red]external call[/red]",
                replay_call="[cyan]read[/cyan]",
                is_read_only=True,
            )
        if status == "failed":
            return ReplayPair(
                first_run=f"[bold]CompleteEffect[/bold]([bold]{tool}[/bold]) — tool raised, status=[bold red]FAILED[/bold red]",
                replay=f"[bold]BeginEffect[/bold]([bold]{tool}[/bold]) — short-circuits on FAILED: returns the recorded error [bold red]without calling the tool again[/bold red]",
                first_call="[red]external call[/red]",
                replay_call="[cyan]read[/cyan]",
                is_read_only=True,
            )
        if status == "unknown":
            return ReplayPair(
                first_run=f"[bold]CompleteEffect[/bold]([bold]{tool}[/bold]) — [bold red on yellow]UNKNOWN[/bold red on yellow] (ack lost; the act may or may not have happened)",
                replay=f"[bold]BeginEffect[/bold]([bold]{tool}[/bold]) — short-circuits on UNKNOWN: the [bold]reconciler[/bold] will observe the counterparty before the agent re-acts",
                first_call="[red]external call[/red]",
                replay_call="[cyan]read[/cyan]",
                is_read_only=True,
            )
        return ReplayPair(
            first_run=f"[bold]Effect[/bold]([bold]{tool}[/bold]) status={status}",
            replay=f"[bold]BeginEffect[/bold] — short-circuits on {status}",
            first_call="[green]write[/green]",
            replay_call="[cyan]read[/cyan]",
            is_read_only=True,
        )

    if kind == "obligation":
        ob_kind = p.get("kind", "")
        return ReplayPair(
            first_run=f"[bold]RegisterCompensation[/bold]([bold]{ob_kind}[/bold]) — obligation now waits for the drainer",
            replay=f"[bold]ListObligations[/bold] / [bold]RegisterCompensation[/bold] — idempotent on (run_id, effect_key, kind); existing record returned",
            first_call="[green]write[/green]",
            replay_call="[cyan]read[/cyan]",
            is_read_only=True,
        )

    if kind == "gate":
        gn = p.get("gate", "") or p.get("gate_name", "")
        status = str(p.get("status", "")).lower()
        if status == "waiting":
            return ReplayPair(
                first_run=f"[bold]AwaitSignal[/bold]([bold]{gn}[/bold]) — agent parked, run → [blue]WAITING[/blue]",
                replay=f"[bold]AwaitSignal[/bold]([bold]{gn}[/bold]) — re-drive sees the run is at this gate; resumes when the signal lands",
                first_call="[green]write[/green]",
                replay_call="[cyan]read[/cyan]",
                is_read_only=True,
            )
        return ReplayPair(
            first_run=f"[bold]SendSignal[/bold]([bold]{gn}[/bold]) — resolution committed, run released",
            replay=f"[bold]AwaitSignal[/bold]([bold]{gn}[/bold]) — short-circuits: returns the recorded resolution [bold red]without parking[/bold red]",
            first_call="[green]write[/green]",
            replay_call="[cyan]read[/cyan]",
            is_read_only=True,
        )

    if kind == "value":
        v = p.get("value", p)
        ns = v.get("namespace", "")
        k = v.get("key", "")
        ver = v.get("version", "")
        return ReplayPair(
            first_run=f"[bold]WriteValue[/bold]([bold]{ns}[/bold]/[cyan]{k}[/cyan] v{ver}) — CAS commit",
            replay=f"[bold]GetValue[/bold]([bold]{ns}[/bold]/[cyan]{k}[/cyan]) — reads the version at this point in the journal",
            first_call="[green]write[/green]",
            replay_call="[cyan]read[/cyan]",
            is_read_only=True,
        )

    if kind == "timer":
        tk = p.get("kind", "")
        return ReplayPair(
            first_run=f"[bold]SetTimer[/bold]([bold]{tk}[/bold]) — fire-at deadline persisted",
            replay=f"[bold]SetTimer[/bold] — idempotent on (run_id, timer_id); existing record returned",
            first_call="[green]write[/green]",
            replay_call="[cyan]read[/cyan]",
            is_read_only=True,
        )

    # Generic fallback.
    return ReplayPair(
        first_run=f"[bold]{kind}[/bold]",
        replay=f"[dim]replay reads {kind}[/dim]",
        first_call="[green]write[/green]",
        replay_call="[cyan]read[/cyan]",
        is_read_only=True,
    )


# ── the Screen ──────────────────────────────────────────────────────────────


class ReplayScreen(Screen):
    """Side-by-side first-run / replay diff for a single run's journal.

    Built on the entries the parent app has already streamed — no extra
    gRPC traffic. Two synchronized DataTables: scrolling either side moves
    a synchronized cursor; the bottom panel shows a row count and a
    high-contrast headline ("REPLAY IS READS, NOT WRITES").
    """

    DEFAULT_CSS = """
    ReplayScreen {
        layout: vertical;
    }
    #replay-headline {
        height: 4;
        padding: 0 1;
        border-bottom: solid $accent;
    }
    #replay-body {
        height: 1fr;
    }
    #first-run {
        width: 50%;
    }
    #replay-side {
        width: 50%;
    }
    DataTable {
        height: 1fr;
    }
    #replay-footer {
        dock: bottom;
        height: 3;
    }
    """

    BINDINGS = [
        Binding("escape", "app.pop_screen", "Back to timeline"),
        Binding("q", "app.pop_screen", "Back"),
        Binding("r", "app.pop_screen", "Back", show=False),
        Binding("home", "scroll_home", "First", show=False),
        Binding("end", "scroll_end", "Last", show=False),
    ]

    def __init__(self, run_id: str, entries: list, **kw):
        super().__init__(**kw)
        self.run_id = run_id
        # Snapshot the entries the parent saw — the replay view is a frozen
        # diff over what's been observed, not a live stream.
        self.entries = list(entries)
        self._syncing = False  # re-entrancy guard for the cursor-sync

    def compose(self) -> ComposeResult:
        yield Static(self._headline(), id="replay-headline")
        with Horizontal(id="replay-body"):
            with Vertical(id="first-run"):
                yield self._make_table("first-run-table", "FIRST RUN")
            with Vertical(id="replay-side"):
                yield self._make_table("replay-table", "REPLAY")
        yield Footer()

    def _headline(self) -> Panel:
        body = RichTable.grid(padding=(0, 1), expand=True)
        body.add_column(justify="center")
        body.add_row(Text.from_markup(
            "[bold cyan]REPLAY IS READS, NOT WRITES[/bold cyan]"))
        body.add_row(Text.from_markup(
            "[dim]Every external action on the left becomes a journal read on the right. "
            "Re-driving the agent is safe because the runtime memoizes — one wire, one model call, "
            "one signal — no matter how many times the agent crashes and resumes.[/dim]"))
        return Panel(body, border_style="cyan", padding=(0, 1))

    def _make_table(self, dt_id: str, title: str) -> DataTable:
        t = DataTable(id=dt_id, zebra_stripes=True, cursor_type="row")
        t.add_columns("seq", "+t", "call", "what")
        return t

    async def on_mount(self) -> None:
        # Populate both tables with paired rows.
        first = self.query_one("#first-run-table", DataTable)
        rep = self.query_one("#replay-table", DataTable)
        first_ts = self.entries[0].ts_ms if self.entries else 0

        for e in self.entries:
            pair = replay_pair(e.kind, e.payload_json)
            d = decode_entry(e.kind, e.payload_json)
            rel = fmt_rel_ms(e.ts_ms - first_ts) if first_ts else "+0ms"
            row_key = f"seq-{e.seq}"

            # LEFT — what the first run did.
            first.add_row(
                Text(str(e.seq), style="dim"),
                Text(rel, style="dim"),
                Text.from_markup(pair.first_call),
                Text.from_markup(f"[bold]{d.icon} {d.type}[/bold]  {pair.first_run}"),
                key=row_key,
            )
            # RIGHT — what replay does instead.
            rep.add_row(
                Text(str(e.seq), style="dim"),
                Text(rel, style="dim"),
                Text.from_markup(pair.replay_call),
                Text.from_markup(f"[bold]{d.icon} {d.type}[/bold]  {pair.replay}"),
                key=row_key,
            )

        # Focus the left table so arrow keys drive both.
        first.focus()

    # ── synchronized cursor ────────────────────────────────────────────────

    @on(DataTable.RowHighlighted, "#first-run-table")
    def _left_to_right(self, ev: DataTable.RowHighlighted) -> None:
        if self._syncing:
            return
        self._syncing = True
        try:
            right = self.query_one("#replay-table", DataTable)
            if ev.cursor_row < right.row_count:
                right.move_cursor(row=ev.cursor_row, animate=False)
        finally:
            self._syncing = False

    @on(DataTable.RowHighlighted, "#replay-table")
    def _right_to_left(self, ev: DataTable.RowHighlighted) -> None:
        if self._syncing:
            return
        self._syncing = True
        try:
            left = self.query_one("#first-run-table", DataTable)
            if ev.cursor_row < left.row_count:
                left.move_cursor(row=ev.cursor_row, animate=False)
        finally:
            self._syncing = False

    # ── actions ────────────────────────────────────────────────────────────

    def action_scroll_home(self) -> None:
        for dt_id in ("#first-run-table", "#replay-table"):
            try:
                t = self.query_one(dt_id, DataTable)
                t.move_cursor(row=0)
                t.scroll_home(animate=False)
            except Exception:  # noqa: BLE001
                pass

    def action_scroll_end(self) -> None:
        for dt_id in ("#first-run-table", "#replay-table"):
            try:
                t = self.query_one(dt_id, DataTable)
                if t.row_count:
                    t.move_cursor(row=t.row_count - 1)
                    t.scroll_end(animate=False)
            except Exception:  # noqa: BLE001
                pass


__all__ = ["ReplayScreen", "ReplayPair", "replay_pair"]
