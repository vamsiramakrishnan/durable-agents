"""The full-screen Textual app behind `tape inspect <run-id>`.

This is the runtime made visible. Where Rich.Live would give us a scrolling
console region with no interaction, Textual gives us a real app:

* a journal **timeline** (scrollable DataTable) — decisions, effects, leases,
  obligations, gates, timers, values — every primitive in §IX of the treatise
* a live **status bar** at the top (run status, lease + countdown, seq cursor,
  duration, gate)
* a **detail pane** that follows the cursor — payload pretty-printed with
  syntax highlighting, decoded fields, trace ids
* a **filter row** (`d` / `e` / `o` / `g` / `v`) to slice by kind without
  losing your place
* a **search bar** (`/`) over the rendered summary
* a **history/live divider** — rows present at connect time are dimmed; rows
  arriving after are full color, so you can SEE replay vs live without
  having to mentally diff
* `f` to toggle auto-follow (jump to the newest row), `r` to dump the
  selected entry's raw JSON, `q` / `ctrl+c` / `escape` to quit

Two background workers feed the app:

* a **stream worker** that blocks on `TapeClient.subscribe_run(...)`
  (a gRPC server-streaming RPC) and hands every JournalEntry to the UI
  thread via `call_from_thread`
* a **poll worker** that re-fetches the RunState every 500ms so the lease
  countdown ticks down in real time

This is the durability runtime as a *product surface*, not a doc page.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Optional

import grpc
from rich.console import Group
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table as RichTable
from rich.text import Text

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.coordinate import Coordinate
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Input,
    Static,
)

from ._journal import (
    Decoded,
    decode_entry,
    fmt_countdown_ms,
    fmt_rel_ms,
    kind_icon,
    run_status_label,
)


# ── status bar widget ──────────────────────────────────────────────────────


class StatusBar(Static):
    """The header strip: run state, lease, seq cursor, duration, gate.

    Recomposed every tick from `RunState` and the connect-time cursor. Not a
    reactive widget — we `update()` it explicitly from the poll worker.
    """

    DEFAULT_CSS = """
    StatusBar {
        height: 7;
        padding: 0 1;
        border-bottom: solid $accent;
    }
    """

    def __init__(self, run_id: str, url: str, **kw):
        super().__init__(**kw)
        self.run_id = run_id
        self.url = url
        self.run_state = None
        self.connect_cursor = 0
        self.entries_seen = 0
        self.last_error = ""

    def refresh_from(self, *, run_state=None, connect_cursor=None,
                     entries_seen=None, last_error=None):
        if run_state is not None:
            self.run_state = run_state
        if connect_cursor is not None:
            self.connect_cursor = connect_cursor
        if entries_seen is not None:
            self.entries_seen = entries_seen
        if last_error is not None:
            self.last_error = last_error
        self.update(self._build_panel())

    def _build_panel(self):
        rs = self.run_state
        if rs is None:
            return Panel(Text("…connecting…", style="dim"),
                         title=f"RUN {self.run_id}", border_style="dim",
                         padding=(0, 1))

        status_label, status_style = run_status_label(rs.status)
        now = int(time.time() * 1000)

        kv = RichTable.grid(padding=(0, 2), expand=True)
        kv.add_column(style="dim bold", no_wrap=True, width=18)
        kv.add_column(ratio=2)
        kv.add_column(style="dim bold", no_wrap=True, width=14)
        kv.add_column(ratio=1)

        # Row 1
        kv.add_row(
            "app/user/session",
            f"[bold]{rs.app_name}[/bold] / {rs.user_id} / {rs.session_id}",
            "status",
            Text(status_label, style=status_style),
        )
        # Row 2
        lease_str = (
            f"[bold]{rs.lease_owner}[/bold]  [dim]({fmt_countdown_ms(rs.lease_expires_at_ms, now)})[/dim]"
            if rs.lease_owner else "[dim]—[/dim]"
        )
        cursor_str = f"[bold]{rs.seq_cursor}[/bold]"
        if self.connect_cursor and rs.seq_cursor > self.connect_cursor:
            cursor_str += f"  [dim](connect: {self.connect_cursor})[/dim]"
        kv.add_row("lease", lease_str, "seq cursor", cursor_str)
        # Row 3
        if rs.started_at_ms:
            dur = (rs.ended_at_ms or now) - rs.started_at_ms
            dur_str = fmt_rel_ms(dur).lstrip("+")
        else:
            dur_str = "—"
        entries_str = f"[bold]{self.entries_seen}[/bold]"
        kv.add_row("duration", dur_str, "entries", entries_str)
        # Row 4
        gate_str = (Text(rs.waiting_on_gate, style="blue")
                    if rs.waiting_on_gate else Text("—", style="dim"))
        err_str = (Text(self.last_error, style="red")
                   if self.last_error else Text(""))
        kv.add_row("gate", gate_str, "", err_str)

        return Panel(
            kv,
            title=f"[bold]RUN[/bold] [cyan]{rs.run_id}[/cyan]",
            subtitle=f"[dim]{self.url}[/dim]",
            border_style=status_style.split()[-1] if status_style else "white",
            padding=(0, 1),
        )


# ── detail pane widget ─────────────────────────────────────────────────────


class DetailPane(Static):
    """The right-hand pane: full info for the selected journal entry."""

    DEFAULT_CSS = """
    DetailPane {
        padding: 0 1;
        border: round $accent;
    }
    """

    def show(self, entry, connect_cursor: int):
        """Render the selected entry as a rich Group (header table + payload
        syntax-highlight). Update is called immediately."""
        if entry is None:
            self.update(Text("Select a row to inspect.", style="dim"))
            return

        d = decode_entry(entry.kind, entry.payload_json, entry.trace_id)

        meta = RichTable.grid(padding=(0, 2), expand=False)
        meta.add_column(style="dim bold", no_wrap=True)
        meta.add_column()
        meta.add_row("seq", str(entry.seq))
        if entry.global_seq:
            meta.add_row("global_seq", str(entry.global_seq))
        meta.add_row("kind", f"{kind_icon(entry.kind)}  [bold]{entry.kind}[/bold]")
        if d.status:
            meta.add_row("status", Text(d.status, style=d.style))
        meta.add_row("ts_ms", str(entry.ts_ms))
        meta.add_row("summary", Text.from_markup(d.summary))
        # Replay-vs-live marker — based on the run's cursor at connect time.
        if connect_cursor:
            phase = ("[dim]history (was already in the journal at connect)[/dim]"
                     if entry.seq <= connect_cursor
                     else "[bold cyan]live (arrived after connect)[/bold cyan]")
            meta.add_row("phase", Text.from_markup(phase))
        if entry.subject:
            meta.add_row("subject", f"[dim]{entry.subject}[/dim]")
        if entry.trace_id:
            short_trace = entry.trace_id[:16] + ("…" if len(entry.trace_id) > 16 else "")
            meta.add_row("trace", f"[dim]{short_trace}[/dim]")
            if entry.span_id:
                meta.add_row("span", f"[dim]{entry.span_id}[/dim]")

        # Pretty-print payload_json with syntax highlight.
        payload_panel = None
        if entry.payload_json:
            try:
                pretty = json.dumps(json.loads(entry.payload_json),
                                    indent=2, sort_keys=True)
                payload_panel = Panel(
                    Syntax(pretty, "json", theme="ansi_dark",
                           background_color="default", line_numbers=False,
                           word_wrap=True),
                    title="payload", border_style="dim", padding=(0, 1),
                )
            except (json.JSONDecodeError, ValueError):
                payload_panel = Panel(
                    Text(entry.payload_json, style="dim"),
                    title="payload (raw)", border_style="dim",
                    padding=(0, 1),
                )

        if payload_panel is not None:
            self.update(Group(meta, "", payload_panel))
        else:
            self.update(meta)


# ── the app ────────────────────────────────────────────────────────────────


class JournalEntryArrived(Message):
    def __init__(self, entry):
        super().__init__()
        self.entry = entry


class RunStateArrived(Message):
    def __init__(self, run_state):
        super().__init__()
        self.run_state = run_state


class StreamError(Message):
    def __init__(self, message: str, fatal: bool = False):
        super().__init__()
        self.message = message
        self.fatal = fatal


class StreamClosed(Message):
    pass


class TapeInspectorApp(App):
    """The inspector. One run, one journal, live.

    Bindings are visible in the Footer; everything else is keyboard-driven.
    """

    CSS = """
    Screen {
        layout: vertical;
    }
    StatusBar {
        height: auto;
    }
    #body {
        height: 1fr;
    }
    #left {
        width: 65%;
    }
    #right {
        width: 35%;
    }
    DataTable {
        height: 1fr;
    }
    #search {
        dock: bottom;
        height: 3;
        display: none;
    }
    #search.-visible {
        display: block;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("ctrl+c", "quit", "Quit", show=False),
        Binding("f", "toggle_follow", "Follow"),
        Binding("a", "filter_kind('')", "All"),
        Binding("d", "filter_kind('decision')", "Decisions"),
        Binding("e", "filter_kind('effect')", "Effects"),
        Binding("o", "filter_kind('obligation')", "Obligations"),
        Binding("g", "filter_kind('gate')", "Gates"),
        Binding("v", "filter_kind('value')", "Values"),
        Binding("slash", "focus_search", "Search"),
        Binding("escape", "blur_search", "", show=False),
        Binding("r", "dump_raw", "Raw"),
        Binding("c", "copy_json", "Copy"),
        Binding("end", "scroll_end", "End", show=False),
        Binding("home", "scroll_home", "Home", show=False),
    ]

    TITLE = "tape inspect"

    # Reactive bits — the app re-renders / re-filters when these change.
    follow: reactive[bool] = reactive(True)
    filter_kind: reactive[str] = reactive("")
    search_query: reactive[str] = reactive("")

    def __init__(self, client, run_id: str, *, url: str, from_seq: int = 0):
        super().__init__()
        self.client = client
        self.run_id = run_id
        self.url = url
        self.from_seq = from_seq

        # Journal state — owned by the UI thread, but appended via messages.
        self.entries: list = []                 # ordered by arrival (== by seq)
        self.row_keys_by_seq: dict[int, Any] = {}
        self.connect_cursor: int = 0
        self.first_ts_ms: Optional[int] = None
        self.divider_inserted = False

        # Worker control.
        self._stream_iter = None
        self._poll_stop = threading.Event()

    # ── layout ─────────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield StatusBar(self.run_id, self.url, id="status")
        with Horizontal(id="body"):
            with Vertical(id="left"):
                yield self._make_table()
            yield DetailPane(id="right", expand=True)
        yield Input(placeholder="/search… (Escape to clear)", id="search")
        yield Footer()

    def _make_table(self) -> DataTable:
        t: DataTable = DataTable(id="journal", zebra_stripes=True,
                                 cursor_type="row")
        # Column ratios are tuned for an 80-wide split.
        t.add_columns("seq", "+t", " ", "type", "status", "details")
        return t

    # ── lifecycle ──────────────────────────────────────────────────────────

    async def on_mount(self) -> None:
        # Prime the run state synchronously (best-effort; falls through to
        # poll worker if it fails).
        try:
            rs = self.client.get_run(self.run_id)
            self.connect_cursor = rs.seq_cursor
            self.query_one(StatusBar).refresh_from(
                run_state=rs, connect_cursor=self.connect_cursor)
        except grpc.RpcError as ex:
            if ex.code() == grpc.StatusCode.NOT_FOUND:
                self.exit(message=f"no such run: {self.run_id}")
                return
            self.query_one(StatusBar).refresh_from(
                last_error=f"get_run: {ex.code().name} {ex.details()}")
        # Spawn workers.
        self.run_worker(self._stream_worker, thread=True,
                        exclusive=True, group="stream", name="stream")
        self.run_worker(self._poll_worker, thread=True,
                        exclusive=True, group="poll", name="poll")
        # Periodically refresh just the status bar so the lease countdown
        # ticks even when nothing else is happening.
        self.set_interval(0.5, self._tick_status)
        # Drive focus to the journal table so keystrokes hit our bindings,
        # not the hidden search Input — Input(can_focus=True by default)
        # would otherwise capture every letter the moment the app starts.
        self.query_one("#search", Input).can_focus = False
        self.query_one(DataTable).focus()

    async def on_unmount(self) -> None:
        self._poll_stop.set()
        if self._stream_iter is not None:
            try:
                self._stream_iter.cancel()
            except Exception:  # noqa: BLE001
                pass

    # ── workers ────────────────────────────────────────────────────────────

    def _stream_worker(self) -> None:
        """Blocking gRPC stream; runs in a thread. Posts each JournalEntry as
        a message back to the UI thread."""
        try:
            self._stream_iter = self.client.subscribe_run(
                run_id=self.run_id, from_seq=self.from_seq)
            for entry in self._stream_iter:
                self.post_message(JournalEntryArrived(entry))
        except grpc.RpcError as ex:
            if ex.code() not in (grpc.StatusCode.CANCELLED,
                                 grpc.StatusCode.DEADLINE_EXCEEDED):
                self.post_message(StreamError(
                    f"{ex.code().name}: {ex.details()}", fatal=True))
        finally:
            self.post_message(StreamClosed())

    def _poll_worker(self) -> None:
        while not self._poll_stop.is_set():
            try:
                rs = self.client.get_run(self.run_id)
                self.post_message(RunStateArrived(rs))
            except Exception as ex:  # noqa: BLE001
                self.post_message(StreamError(f"get_run: {ex}", fatal=False))
            self._poll_stop.wait(0.5)

    # ── message handlers ───────────────────────────────────────────────────

    @on(JournalEntryArrived)
    def _on_entry(self, msg: JournalEntryArrived) -> None:
        e = msg.entry
        self.entries.append(e)
        if self.first_ts_ms is None:
            self.first_ts_ms = e.ts_ms

        # Append to the table, respecting the current filter / search.
        table: DataTable = self.query_one(DataTable)
        if self._entry_visible(e):
            self._add_row(table, e)

        # Status counter.
        self.query_one(StatusBar).refresh_from(entries_seen=len(self.entries))

        # Auto-follow scroll.
        if self.follow:
            self._scroll_to_bottom(table)

    @on(RunStateArrived)
    def _on_run_state(self, msg: RunStateArrived) -> None:
        # If we never primed (initial fetch failed), seed the connect_cursor.
        if self.connect_cursor == 0 and msg.run_state.seq_cursor:
            self.connect_cursor = msg.run_state.seq_cursor
        self.query_one(StatusBar).refresh_from(
            run_state=msg.run_state,
            connect_cursor=self.connect_cursor)

    @on(StreamError)
    def _on_stream_error(self, msg: StreamError) -> None:
        self.query_one(StatusBar).refresh_from(last_error=msg.message)
        if msg.fatal:
            self.bell()

    @on(StreamClosed)
    def _on_stream_closed(self, _: StreamClosed) -> None:
        self.query_one(StatusBar).refresh_from(
            last_error="[dim]stream closed (run terminal or server reset)[/dim]")

    def _tick_status(self) -> None:
        # Re-render the bar from current state to refresh the countdown.
        sb = self.query_one(StatusBar)
        sb.refresh_from()  # no-op kwargs; forces a re-render

    # ── row + table helpers ────────────────────────────────────────────────

    def _entry_visible(self, e) -> bool:
        if self.filter_kind and e.kind != self.filter_kind:
            return False
        if self.search_query:
            d = decode_entry(e.kind, e.payload_json, e.trace_id)
            hay = " ".join([e.kind, d.status, d.summary,
                            e.subject, e.payload_json])
            if self.search_query.lower() not in hay.lower():
                return False
        return True

    def _add_row(self, table: DataTable, e) -> None:
        d: Decoded = decode_entry(e.kind, e.payload_json, e.trace_id)
        # Insert the live divider exactly once, the first time we add a row
        # whose seq is past the connect cursor.
        if (not self.divider_inserted
                and self.connect_cursor
                and e.seq > self.connect_cursor):
            divider = Text("─── live (entries arriving after connect) ───",
                           style="bold dim", justify="left")
            table.add_row("", "", "", divider, "", "",
                          key=f"divider-{self.connect_cursor}")
            self.divider_inserted = True

        rel = fmt_rel_ms(e.ts_ms - self.first_ts_ms) if self.first_ts_ms else "+0ms"
        is_history = self.connect_cursor and e.seq <= self.connect_cursor
        dim_prefix = "dim " if is_history else ""

        seq_cell = Text(str(e.seq), style="dim")
        time_cell = Text(rel, style="dim")
        icon_cell = Text(d.icon, style=(dim_prefix + "white").strip())
        type_cell = Text(d.type, style=(dim_prefix + "bold").strip())
        status_cell = Text(d.status, style=d.style)
        details_cell = Text.from_markup(
            d.summary, style="dim" if is_history else "")

        row_key = f"seq-{e.seq}"
        table.add_row(seq_cell, time_cell, icon_cell, type_cell,
                      status_cell, details_cell, key=row_key)
        self.row_keys_by_seq[e.seq] = row_key

    def _rebuild_table(self) -> None:
        """Wipe + re-add rows respecting current filter / search. Used when
        the filter changes."""
        table: DataTable = self.query_one(DataTable)
        table.clear()
        self.divider_inserted = False
        self.row_keys_by_seq.clear()
        for e in self.entries:
            if self._entry_visible(e):
                self._add_row(table, e)
        if self.follow:
            self._scroll_to_bottom(table)

    def _scroll_to_bottom(self, table: DataTable) -> None:
        if table.row_count == 0:
            return
        try:
            table.move_cursor(row=table.row_count - 1)
            table.scroll_end(animate=False)
        except Exception:  # noqa: BLE001
            pass

    # ── selection → detail pane ────────────────────────────────────────────

    @on(DataTable.RowHighlighted)
    def _on_row_highlight(self, ev: DataTable.RowHighlighted) -> None:
        # Decode the row key → entry seq (skip the divider).
        key = ev.row_key.value if ev.row_key is not None else None
        if not key or not key.startswith("seq-"):
            return
        try:
            seq = int(key[4:])
        except ValueError:
            return
        # Find entry with that seq.
        entry = next((e for e in self.entries if e.seq == seq), None)
        self.query_one(DetailPane).show(entry, self.connect_cursor)

    # ── actions ────────────────────────────────────────────────────────────

    def action_toggle_follow(self) -> None:
        self.follow = not self.follow
        if self.follow:
            self._scroll_to_bottom(self.query_one(DataTable))
        self.notify(f"follow: {'on' if self.follow else 'off'}",
                    timeout=1, severity="information")

    def action_filter_kind(self, kind: str) -> None:
        self.filter_kind = kind
        self._rebuild_table()
        label = kind or "all kinds"
        self.notify(f"filter: {label}", timeout=1, severity="information")

    def action_focus_search(self) -> None:
        search = self.query_one("#search", Input)
        search.add_class("-visible")
        search.can_focus = True
        search.focus()

    def action_blur_search(self) -> None:
        search = self.query_one("#search", Input)
        search.value = ""
        search.remove_class("-visible")
        search.can_focus = False
        self.search_query = ""
        self._rebuild_table()
        self.query_one(DataTable).focus()

    @on(Input.Submitted, "#search")
    def _on_search_submit(self, ev: Input.Submitted) -> None:
        self.search_query = ev.value
        self._rebuild_table()
        self.query_one(DataTable).focus()

    @on(Input.Changed, "#search")
    def _on_search_changed(self, ev: Input.Changed) -> None:
        # Live-filter as the user types (cheap — the lists are small).
        self.search_query = ev.value
        self._rebuild_table()

    def action_dump_raw(self) -> None:
        """Open a modal-ish display with the selected entry's raw JSON.
        For now: notify with a short preview, full dump goes to stdout on quit
        if --raw was set instead. The interactive path is best paired with
        `tape inspect <id> --raw | jq` for serious inspection."""
        table = self.query_one(DataTable)
        if table.cursor_row < 0 or table.cursor_row >= len(self.entries):
            return
        key = table.coordinate_to_cell_key(Coordinate(table.cursor_row, 0)).row_key
        if not key or not key.value or not key.value.startswith("seq-"):
            return
        try:
            seq = int(key.value[4:])
        except ValueError:
            return
        entry = next((e for e in self.entries if e.seq == seq), None)
        if entry is None:
            return
        pretty = (entry.payload_json
                  if entry.payload_json.startswith("{") else
                  json.dumps({"payload": entry.payload_json}))
        self.notify(
            pretty[:160] + ("…" if len(pretty) > 160 else ""),
            title=f"seq {entry.seq} · {entry.kind}",
            timeout=6,
        )

    def action_copy_json(self) -> None:
        """Push the selected entry's full JSON to the system clipboard
        (best effort — Textual's clipboard works in most terminals)."""
        table = self.query_one(DataTable)
        if table.cursor_row < 0 or table.cursor_row >= len(self.entries):
            return
        key = table.coordinate_to_cell_key(Coordinate(table.cursor_row, 0)).row_key
        if not key or not key.value or not key.value.startswith("seq-"):
            return
        try:
            seq = int(key.value[4:])
        except ValueError:
            return
        entry = next((e for e in self.entries if e.seq == seq), None)
        if entry is None:
            return
        payload = {
            "seq": entry.seq,
            "global_seq": entry.global_seq,
            "kind": entry.kind,
            "ts_ms": entry.ts_ms,
            "subject": entry.subject,
            "trace_id": entry.trace_id,
            "payload_json": entry.payload_json,
        }
        text = json.dumps(payload, indent=2)
        try:
            self.copy_to_clipboard(text)
            self.notify("copied to clipboard", timeout=1)
        except Exception:  # noqa: BLE001
            self.notify("clipboard unavailable; use `tape inspect <id> --raw`",
                        severity="warning", timeout=3)

    def action_scroll_end(self) -> None:
        self._scroll_to_bottom(self.query_one(DataTable))

    def action_scroll_home(self) -> None:
        table = self.query_one(DataTable)
        try:
            table.move_cursor(row=0)
            table.scroll_home(animate=False)
        except Exception:  # noqa: BLE001
            pass


__all__ = ["TapeInspectorApp"]
