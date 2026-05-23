"""`tape demo <scenario>` — the "show me durability" command.

This is the conversion moment for the runtime: a self-contained, 60-second,
no-setup tour of what Tape actually does under failure. The user runs ONE
command:

    tape demo crash-resume

…and gets, in real time, in a single terminal:

* a tape-server they didn't have to start;
* a synthetic "treasury agent" they didn't have to write;
* a file-backed fake bank they didn't have to wire up;
* a journal animating phase-by-phase in the right pane;
* a crash (we actually exit a subprocess mid-effect — no fake);
* a recovery + re-drive that reads the journal instead of re-calling the bank;
* a ledger inspection that proves exactly-one wire ended up on disk.

The demo deliberately does NOT depend on `google-adk` (the real agent does;
that's why the existing `examples/treasury/run.py` can't double as the demo).
We talk straight to `TapeClient` to write the journal entries an agent
*would* write, so the demo runs on a fresh `pip install tape-cli` with no
extra deps and no LLM key.

What the user SEES (renders into `rich.live.Live(Layout(...))`):

  ┌─ tape demo crash-resume ──────────────────────────────────────────────┐
  ├─ Phases (left) ──────────────┬─ Journal (right, live) ────────────────┤
  │ ✓ tape-server up             │ seq  +t      kind        status        │
  │ ✓ run begun                  │   1  +0ms    run         running       │
  │ ✓ decision recorded          │   2  +12ms   decision    recorded      │
  │ ✓ wire intent → PENDING      │   3  +15ms   effect      pending       │
  │ ✗ CRASH @ execute_sweep      │ ─── crash ───────────────────────────── │
  │ ⋯ recovery in flight         │                                        │
  │ ✓ resume — re-drive          │   4  +250ms  effect      confirmed     │
  │ ✓ ONE wire on disk           │   5  +252ms  run         terminal      │
  ├─ Bank ledger ─────────────────┴────────────────────────────────────────┤
  │   wire-0001  $2,000,000  acct-1                                       │
  │   (exactly one wire — even though the agent crashed mid-call)         │
  └────────────────────────────────────────────────────────────────────────┘

Why this matters: until you can WATCH the journal animate through the crash
and back, "durable execution" is a phrase. Once you've seen it, it's a system.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections import deque
from contextlib import suppress
from pathlib import Path
from typing import Optional

import grpc
import typer
from rich.console import Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ..util import console, die, info, ok, warn
from ._journal import (
    decode_entry,
    fmt_rel_ms,
    run_status_label,
)


# ── Typer surface ──────────────────────────────────────────────────────────


app = typer.Typer(name="demo",
                  help="Theatrical, self-contained demos of the runtime.",
                  no_args_is_help=True, add_completion=False)


# ── fake-bank ledger (file-backed, survives the simulated crash) ───────────
#
# The whole point of the demo is that the bank's ledger and the Tape journal
# are two *independent* persisted things — the journal records intent, the
# ledger records reality. The crash interrupts the chain between them; the
# re-drive on the journal reads the bank's reality via observation and
# closes the loop. We model the bank's ledger as a JSON file keyed by
# idempotency_key, because that's exactly what the spec assumes.


class FileBank:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("{}")

    def _read(self) -> dict:
        try:
            return json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}

    def _write(self, data: dict) -> None:
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2))
        os.replace(tmp, self.path)

    def wire(self, *, idempotency_key: str, amount_minor: int,
             account_id: str) -> dict:
        data = self._read()
        if idempotency_key in data:
            return data[idempotency_key]
        rec = {
            "wire_id": f"wire-{len(data) + 1:04d}",
            "amount_minor": amount_minor,
            "account_id": account_id,
            "idempotency_key": idempotency_key,
        }
        data[idempotency_key] = rec
        self._write(data)
        return rec

    def wire_by_business_key(self, *, business_key: str, amount_minor: int,
                             account_id: str) -> dict:
        """The non-idempotent bank's contract: dedupe on business_key (NOT on
        our run-derived idempotency_key, which the upstream wouldn't know
        about). If a wire with this business_key already lives in the ledger,
        return that record; otherwise mint a fresh wire_id and store it.

        This is what makes the outbox + reconciler path safe: the bank's own
        ledger is the source of truth, and the reconciler observes it via
        business_key to decide whether to re-issue or short-circuit."""
        data = self._read()
        for rec in data.values():
            if rec.get("business_key") == business_key:
                return rec
        rec = {
            "wire_id": f"wire-{len(data) + 1:04d}",
            "amount_minor": amount_minor,
            "account_id": account_id,
            "business_key": business_key,
        }
        # Key the row by business_key — that's the bank's natural dedup key
        # for non-idempotent operations.
        data[business_key] = rec
        self._write(data)
        return rec

    def find_by_business_key(self, business_key: str) -> Optional[dict]:
        """The connector's observe() — the bank's view of a logical operation,
        looked up by the key the counterparty would use to dedupe."""
        for rec in self._read().values():
            if rec.get("business_key") == business_key:
                return rec
        return None

    def all_wires(self) -> list[dict]:
        return list(self._read().values())


# ── server lifecycle ───────────────────────────────────────────────────────


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _find_server_binary() -> Optional[str]:
    """Locate a built tape-server binary. We accept either profile because the
    demo is fine with debug perf; what we don't tolerate is users having to
    pre-build."""
    here = Path(__file__).resolve()
    repo_candidates = [
        here.parents[5] / "tape" / "server" / "target" / "release" / "tape-server",
        here.parents[5] / "tape" / "server" / "target" / "debug" / "tape-server",
        Path.cwd() / "tape" / "server" / "target" / "release" / "tape-server",
        Path.cwd() / "tape" / "server" / "target" / "debug" / "tape-server",
        Path("/usr/local/bin/tape-server"),
    ]
    for p in repo_candidates:
        if p.exists() and p.is_file() and os.access(p, os.X_OK):
            return str(p.resolve())
    import shutil
    found = shutil.which("tape-server")
    return found


def _wait_until_listening(port: int, deadline_s: float = 10.0) -> bool:
    deadline = time.time() + deadline_s
    while time.time() < deadline:
        with suppress(Exception):
            s = socket.create_connection(("127.0.0.1", port), timeout=0.3)
            s.close()
            return True
        time.sleep(0.1)
    return False


# ── the agent (a scripted sequence of TapeClient calls) ───────────────────
#
# We split the "agent" into two callable halves so the crash-and-resume is
# real: phase A runs to completion (records intent + writes to the bank) and
# then exits via `os._exit(137)`. Phase B starts fresh, finds the run in a
# recoverable state, re-drives it from the journal, and completes the effect.
# That's the exact contract `tape.recover_once()` codifies — we just
# transcribe it by hand to keep the demo dep-free.

INVOCATION_ID_ENV = "TAPE_DEMO_INVOCATION"
SESSION_ID_ENV = "TAPE_DEMO_SESSION"


def _phase_a_first_run(*, url: str, ledger_path: Path, crash: bool) -> str:
    """Phase A — the "agent" runs to the point of crash.

    Writes:
      run(running) → decision#0 → effect#0(pending) → bank.ledger += wire
      → (optional) os._exit(137)  -or-  effect#0(confirmed) + run(terminal)
    """
    from tape.client import TapeClient
    from tape._gen.tape_pb2 import EFFECT_STATUS_CONFIRMED

    invocation_id = os.environ[INVOCATION_ID_ENV]
    session_id = os.environ[SESSION_ID_ENV]

    bank = FileBank(ledger_path)

    with TapeClient(url) as c:
        # `BeginRun` is idempotent on invocation_id — phase B re-issues the
        # same one and gets the existing run back.
        r = c.begin_run(
            app_name="treasury-demo", user_id="cfo",
            session_id=session_id, invocation_id=invocation_id,
            lease_owner=f"demo-pid-{os.getpid()}", lease_ttl_ms=15_000)
        run_id = r.run_id
        # Emit the run_id to the parent IMMEDIATELY so the UI can start
        # streaming the journal while we're still writing it. If we hit
        # os._exit() further down, we still want the parent to know which
        # run to recover.
        if os.environ.get("TAPE_DEMO_EMIT_RUN_ID", "0") == "1":
            sys.stdout.write(f"RUN_ID={run_id}\n")
            sys.stdout.flush()

        # decision#0 — the agent "decides" to sweep.
        # record_decision is idempotent on (run_id, decision_index): a replay
        # will get the same record back instead of re-calling the model.
        c.record_decision(
            run_id=run_id, decision_index=0,
            model="demo/oracle",
            request_json='{"q":"close the book"}',
            response_json='{"call":"execute_sweep","amount":2000000}',
            rationale="excess USD; sweep to MMF")

        # effect#0 — the wire INTENT (PENDING) is journaled BEFORE we touch
        # the bank. That's what makes the journal a leading edge: even if
        # the bank ledger and the journal disagree later, intent was always
        # recorded first.
        eff = c.begin_effect(
            run_id=run_id, decision_index=0,
            tool_name="bank.wire", call_index=0,
            request_json='{"account_id":"acct-1","amount_minor":2000000}')
        # Effect already CONFIRMED? Then phase A is being run on a journal
        # that already finished — short-circuit (nothing left to do).
        if eff.status == EFFECT_STATUS_CONFIRMED:
            return run_id

        # The "real" external action: write to the bank's ledger keyed by
        # the idempotency key. THIS is what the journal protects.
        bank.wire(idempotency_key=eff.idempotency_key,
                  amount_minor=2_000_000, account_id="acct-1")

        if crash:
            # The deploy/OOM/SIGTERM moment the runtime is designed to
            # survive: the wire is on disk, the journal still says PENDING,
            # we never returned to write `complete_effect`. We use _exit
            # (no atexit, no flush) — exactly the spec.
            #
            # Important: phase A is a subprocess so this kills only the
            # subprocess, not the demo UI.
            sys.stdout.write("CRASH\n"); sys.stdout.flush()
            os._exit(137)

        # No-crash path: close the effect. Used by phase B when re-driving.
        c.complete_effect(
            run_id=run_id, idempotency_key=eff.idempotency_key,
            status=EFFECT_STATUS_CONFIRMED,
            response_json=json.dumps({"wire_id": f"wire-from-{eff.idempotency_key[-4:]}"}))
        c.end_run(run_id=run_id)
        return run_id


def _phase_b_recover(*, url: str, ledger_path: Path, run_id: str) -> dict:
    """Phase B — re-drive the journal.

    Re-issues the same agent calls. Each one short-circuits on the journal
    (no model call, no second bank call) — *except* `complete_effect`, which
    reads the bank ledger to see what happened during the crash and flips
    the effect to CONFIRMED.

    Returns a small dict describing what was re-driven (for the UI).
    """
    from tape.client import TapeClient
    from tape._gen.tape_pb2 import (
        EFFECT_STATUS_CONFIRMED,
        EFFECT_STATUS_PENDING,
    )

    invocation_id = os.environ[INVOCATION_ID_ENV]
    session_id = os.environ[SESSION_ID_ENV]
    bank = FileBank(ledger_path)
    result = {"replayed": [], "actions": []}

    with TapeClient(url) as c:
        # Re-issue BeginRun with the same invocation_id — server returns the
        # existing run_id and the cursor we should re-drive to.
        r = c.begin_run(
            app_name="treasury-demo", user_id="cfo",
            session_id=session_id, invocation_id=invocation_id,
            lease_owner=f"demo-pid-{os.getpid()}", lease_ttl_ms=15_000)
        assert r.run_id == run_id, "different run_id on re-drive (should not happen)"
        result["replayed"].append(f"BeginRun → existing run_id (resumed={r.resumed})")

        # Re-issue the decision — GetDecision returns the recorded one, no
        # model call. (The agent code would just call record_decision again;
        # it's idempotent on (run_id, decision_index).)
        c.record_decision(
            run_id=run_id, decision_index=0, model="demo/oracle",
            request_json='{"q":"close the book"}',
            response_json='{"call":"execute_sweep","amount":2000000}')
        result["replayed"].append("decision#0 → read from journal (no model call)")

        # Re-issue the effect intent — BeginEffect returns the existing
        # PENDING record (no second bank.wire).
        eff = c.begin_effect(
            run_id=run_id, decision_index=0,
            tool_name="bank.wire", call_index=0,
            request_json='{"account_id":"acct-1","amount_minor":2000000}')
        result["replayed"].append(
            f"effect#0 → read from journal (status={eff.status})")

        if eff.status == EFFECT_STATUS_CONFIRMED:
            result["actions"].append("nothing to do — effect already CONFIRMED")
            return result

        # The reconciliation step: observe the bank to find out what really
        # happened during the crash. Here we know (it's our fake bank), but
        # in real life the connector's observe() would query the upstream
        # for `idempotency_key` and tell us. The bank's own ledger is
        # the source of truth.
        ledger = {w["idempotency_key"]: w for w in bank.all_wires()}
        wire = ledger.get(eff.idempotency_key)
        if wire is None:
            # Could happen if the bank's ledger was wiped between phases —
            # we'd issue a fresh wire here.
            result["actions"].append("bank ledger empty — issuing wire fresh")
            wire = bank.wire(idempotency_key=eff.idempotency_key,
                             amount_minor=2_000_000, account_id="acct-1")
        else:
            result["actions"].append(
                f"observed bank ledger: {wire['wire_id']} already lives there")

        c.complete_effect(
            run_id=run_id, idempotency_key=eff.idempotency_key,
            status=EFFECT_STATUS_CONFIRMED,
            response_json=json.dumps({"wire_id": wire["wire_id"]}))
        result["actions"].append("effect#0 → CONFIRMED")

        c.end_run(run_id=run_id)
        result["actions"].append("run → TERMINAL")

    return result


def _spawn_phase_a(*, url: str, ledger_path: Path, crash: bool,
                   invocation_id: str, session_id: str) -> tuple[subprocess.Popen, str]:
    """Run phase A in a subprocess so the crash is REAL (os._exit)."""
    env = dict(os.environ)
    env["TAPE_URL"] = url
    env[INVOCATION_ID_ENV] = invocation_id
    env[SESSION_ID_ENV] = session_id
    env["TAPE_DEMO_LEDGER"] = str(ledger_path)
    env["TAPE_DEMO_CRASH"] = "1" if crash else "0"
    env["TAPE_DEMO_EMIT_RUN_ID"] = "1"
    # PYTHONPATH so the subprocess can find tape + tape_cli without install.
    here = Path(__file__).resolve()
    repo_root = here.parents[5]
    extra_paths = [
        str(repo_root / "tape" / "sdk" / "python"),
        str(repo_root / "tape" / "cli"),
    ]
    env["PYTHONPATH"] = os.pathsep.join(
        extra_paths + [env.get("PYTHONPATH", "")])
    cmd = [sys.executable, "-m", "tape_cli.commands.demo", "--phase-a"]
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    # First line of stdout: the run_id, then "CRASH" if it crashed.
    run_id = ""
    while True:
        line = proc.stdout.readline()
        if not line:
            break
        line = line.strip()
        if line.startswith("RUN_ID="):
            run_id = line[len("RUN_ID="):]
            break
    return proc, run_id


# When invoked as `python -m tape_cli.commands.demo --phase-a`, run phase A
# and exit. (We can't import phase_a as a function from a subprocess; this
# is the simplest portable spawn.)
def _phase_a_main_for_subprocess():
    url = os.environ["TAPE_URL"]
    ledger = Path(os.environ["TAPE_DEMO_LEDGER"])
    crash = os.environ.get("TAPE_DEMO_CRASH", "0") == "1"
    # _phase_a_first_run emits RUN_ID=<id> on stdout immediately after the
    # server hands us the run_id, BEFORE the crash. So even when this
    # subprocess crashes via os._exit, the parent already has the id.
    _phase_a_first_run(url=url, ledger_path=ledger, crash=crash)
    sys.exit(0)


# ── the UI ─────────────────────────────────────────────────────────────────


class _DemoUI:
    """Rich Layout-driven stage manager for the demo.

    Holds three pieces of state, all locked behind `self.lock`:

      * `phases`     — the list of phase labels + their done/active/pending state
      * `entries`    — every JournalEntry streamed back from the server
      * `bank_wires` — the current contents of the fake bank ledger

    A background thread streams the journal via `subscribe_run`, exactly the
    same plumbing the Inspector uses, so the demo's middle pane is a faithful
    preview of what `tape inspect <id>` shows.
    """

    PHASE_PENDING = "pending"
    PHASE_ACTIVE = "active"
    PHASE_DONE = "done"
    PHASE_FAIL = "fail"

    def __init__(self, *, url: str, ledger_path: Path,
                 title: str = "demo",
                 default_subtitle: str = "self-contained · exits 0 iff exactly-one wire",
                 ledger_ok_suffix: str = "— exactly once",
                 run_id: Optional[str] = None):
        self.url = url
        self.ledger_path = ledger_path
        self.title = title
        self.default_subtitle = default_subtitle
        self.ledger_ok_suffix = ledger_ok_suffix
        self.run_id = run_id
        self.phases: list[dict] = []
        self.entries: deque = deque(maxlen=200)
        self.first_ts: Optional[int] = None
        self.crash_after_seq: Optional[int] = None  # marker for the crash divider
        self.lock = threading.RLock()
        self.stop_flag = threading.Event()
        self._stream_iter = None
        self.headline: str = ""

    # ── phases ─────────────────────────────────────────────────────────────

    def add_phases(self, *labels: str) -> None:
        with self.lock:
            for label in labels:
                self.phases.append({"label": label, "state": self.PHASE_PENDING,
                                    "detail": ""})

    def start_phase(self, idx: int, *, detail: str = "") -> None:
        with self.lock:
            self.phases[idx]["state"] = self.PHASE_ACTIVE
            if detail:
                self.phases[idx]["detail"] = detail

    def finish_phase(self, idx: int, *, ok: bool = True, detail: str = "") -> None:
        with self.lock:
            self.phases[idx]["state"] = self.PHASE_DONE if ok else self.PHASE_FAIL
            if detail:
                self.phases[idx]["detail"] = detail

    def set_headline(self, text: str) -> None:
        with self.lock:
            self.headline = text

    def mark_crash(self) -> None:
        """Record the seq of the LAST journal entry seen before crash, so the
        body table can draw the visual divider."""
        with self.lock:
            if self.entries:
                self.crash_after_seq = self.entries[-1].seq

    # ── journal stream ─────────────────────────────────────────────────────

    def attach_run(self, run_id: str) -> None:
        self.run_id = run_id
        threading.Thread(target=self._stream_worker, daemon=True,
                         name="demo-stream").start()

    def _stream_worker(self) -> None:
        from tape.client import TapeClient
        try:
            client = TapeClient(self.url)
        except Exception:  # noqa: BLE001
            return
        # Re-subscribe loop: when the server closes the stream (run terminal),
        # we briefly reconnect so phase-B entries also appear.
        from_seq = 0
        while not self.stop_flag.is_set() and self.run_id is not None:
            try:
                it = client.subscribe_run(run_id=self.run_id,
                                          from_seq=from_seq, timeout=1.5)
                self._stream_iter = it
                for entry in it:
                    if self.stop_flag.is_set():
                        break
                    with self.lock:
                        # Dedup — subscribe_run's from_seq is inclusive, so a
                        # re-subscribe (after a stream timeout) would re-emit
                        # the last entry we already have.
                        if self.entries and entry.seq <= self.entries[-1].seq:
                            continue
                        self.entries.append(entry)
                        if self.first_ts is None:
                            self.first_ts = entry.ts_ms
                        # Advance the cursor PAST the last entry we saw so the
                        # next subscribe call only yields fresh ones.
                        from_seq = entry.seq + 1
            except grpc.RpcError as ex:
                if ex.code() not in (grpc.StatusCode.DEADLINE_EXCEEDED,
                                     grpc.StatusCode.CANCELLED):
                    break
            # Short pause before resuming the stream — keeps CPU quiet.
            self.stop_flag.wait(0.2)
        with suppress(Exception):
            client.close()

    # ── ledger ─────────────────────────────────────────────────────────────

    def bank_wires(self) -> list[dict]:
        try:
            return FileBank(self.ledger_path).all_wires()
        except Exception:  # noqa: BLE001
            return []

    # ── rendering ──────────────────────────────────────────────────────────

    def _phases_panel(self) -> Panel:
        with self.lock:
            phases = list(self.phases)
            headline = self.headline
        t = Table.grid(padding=(0, 1), expand=True)
        t.add_column(width=3)
        t.add_column()
        for p in phases:
            if p["state"] == self.PHASE_DONE:
                glyph = Text("✓", style="bold green")
                lstyle = "bold"
            elif p["state"] == self.PHASE_ACTIVE:
                glyph = Text("⋯", style="bold cyan")
                lstyle = "bold cyan"
            elif p["state"] == self.PHASE_FAIL:
                glyph = Text("✗", style="bold red on yellow")
                lstyle = "bold red"
            else:
                glyph = Text("·", style="dim")
                lstyle = "dim"
            # Labels are stored as rich markup strings.
            row = Text.from_markup(p["label"], style=lstyle)
            if p["detail"]:
                row.append("\n  ")
                row.append_text(Text.from_markup(p["detail"], style="dim"))
            t.add_row(glyph, row)
        subtitle = headline or self.default_subtitle
        return Panel(t, title=f"[bold]{self.title}[/bold]",
                     subtitle=f"[dim]{subtitle}[/dim]",
                     border_style="cyan", padding=(0, 1))

    def _journal_panel(self) -> Panel:
        with self.lock:
            entries = list(self.entries)
            crash_after = self.crash_after_seq
            first_ts = self.first_ts
        t = Table(show_header=True, header_style="bold dim", show_edge=False,
                  pad_edge=False, expand=True, padding=(0, 1))
        t.add_column("seq", justify="right", style="dim", width=4)
        t.add_column("+t", justify="right", style="dim", width=7)
        t.add_column(" ", width=1)
        t.add_column("kind", width=10)
        t.add_column("status", width=11)
        t.add_column("details", overflow="ellipsis", no_wrap=True)
        if not entries:
            t.add_row("", "", "", Text("…", style="dim"), "",
                      Text("waiting for the journal…", style="dim"))
        divider_drawn = False
        for e in entries:
            d = decode_entry(e.kind, e.payload_json, e.trace_id)
            if (crash_after is not None and not divider_drawn
                    and e.seq > crash_after):
                t.add_row(
                    "", "", "",
                    Text("─── crash + recovery ───",
                         style="bold red on yellow"),
                    "", Text("(seq above survived; below is the re-drive)",
                            style="dim"))
                divider_drawn = True
            rel = fmt_rel_ms(e.ts_ms - first_ts) if first_ts else "+0ms"
            t.add_row(
                Text(str(e.seq), style="dim"),
                Text(rel, style="dim"),
                Text(d.icon, style="white"),
                Text(d.type, style="bold"),
                Text(d.status, style=d.style),
                Text.from_markup(d.summary),
            )
        return Panel(t, title="[bold]journal[/bold]  [dim](live)[/dim]",
                     border_style="white", padding=(0, 1))

    def _ledger_panel(self) -> Panel:
        wires = self.bank_wires()
        if not wires:
            body = Text("(bank ledger is empty)", style="dim")
        else:
            t = Table(show_header=True, header_style="bold dim", show_edge=False,
                      pad_edge=False, expand=True)
            t.add_column("wire_id")
            t.add_column("amount", justify="right")
            t.add_column("account")
            t.add_column("idempotency_key", style="dim", overflow="ellipsis",
                         no_wrap=True)
            for w in wires:
                amount = f"${w['amount_minor']:,.0f}"
                t.add_row(
                    Text(w["wire_id"], style="bold green"),
                    amount,
                    w["account_id"],
                    w.get("idempotency_key", ""),
                )
            body = t
        n_forward = sum(1 for w in wires if not str(w.get("wire_id", "")).startswith("reverse"))
        subtitle_style = ("bold green" if n_forward == 1
                          else "bold red" if n_forward > 1 else "dim")
        subtitle = (f"{n_forward} forward wire{'s' if n_forward != 1 else ''} on disk"
                    + (f"  {self.ledger_ok_suffix}" if n_forward == 1 else ""))
        return Panel(body, title="[bold]bank ledger[/bold] [dim](file-backed)[/dim]",
                     subtitle=Text(subtitle, style=subtitle_style),
                     border_style="green", padding=(0, 1))

    def render(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="top", ratio=3),
            Layout(self._ledger_panel(), name="ledger", size=10),
        )
        layout["top"].split_row(
            Layout(self._phases_panel(), name="phases", ratio=2),
            Layout(self._journal_panel(), name="journal", ratio=3),
        )
        return layout

    # ── run-loop helpers ───────────────────────────────────────────────────

    def stop(self) -> None:
        self.stop_flag.set()
        if self._stream_iter is not None:
            with suppress(Exception):
                self._stream_iter.cancel()


# ── orchestrator ───────────────────────────────────────────────────────────


@app.command(name="crash-resume",
             help="Crash an agent mid-effect, recover, prove exactly-one wire.")
def crash_resume(
    pause_s: float = typer.Option(
        0.6, "--pause", "-p",
        help="Pause between phases (seconds). Lower = faster demo."),
    keep_after: bool = typer.Option(
        False, "--keep",
        help="Don't tear down the server / ledger when the demo finishes (so you can `tape inspect`)."),
    server_binary: Optional[str] = typer.Option(
        None, "--server-binary",
        help="Path to a built `tape-server` (default: auto-locate in the repo)."),
):
    """Spin up a server + run a self-contained 'crash mid-effect, recover,
    end with exactly one wire on disk' scenario, live, in this terminal.

    Exits 0 if and only if the bank ledger ends with exactly one forward wire.
    """
    binary = server_binary or _find_server_binary()
    if not binary:
        die("no tape-server binary found.\n"
            "  Build with: `cd tape/server && cargo build`")

    workdir = Path(tempfile.mkdtemp(prefix="tape-demo-"))
    db_path = workdir / "tape.db"
    ledger_path = workdir / "bank.json"
    invocation_id = f"demo-{uuid.uuid4().hex[:10]}"
    session_id = f"sess-{uuid.uuid4().hex[:6]}"

    port = _free_port()
    url = f"tape://127.0.0.1:{port}"

    info(f"[dim]workdir: {workdir}[/dim]")
    info(f"[dim]server : {binary}[/dim]")
    info(f"[dim]url    : {url}[/dim]\n")

    server_proc = subprocess.Popen(
        [binary, "--listen", f"127.0.0.1:{port}",
         "--store", f"sqlite:{db_path}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not _wait_until_listening(port):
        server_proc.terminate()
        die(f"tape-server didn't come up on :{port}")

    os.environ[INVOCATION_ID_ENV] = invocation_id
    os.environ[SESSION_ID_ENV] = session_id

    ui = _DemoUI(url=url, ledger_path=ledger_path,
                 title="demo crash-resume",
                 default_subtitle="idempotent + inline · agent crashes mid-effect, replay completes it",
                 ledger_ok_suffix="— exactly once, even though the agent crashed")
    ui.add_phases(
        "tape-server up",
        "decision recorded",
        "wire dispatched (PENDING)",
        "bank ledger gains the wire",
        "agent CRASHES (os._exit) before ACK",
        "recovery: find + re-drive run",
        "replay READS the journal — no 2nd model call, no 2nd wire",
        "effect → CONFIRMED · run → TERMINAL",
        "verify: exactly ONE forward wire on disk",
    )
    ui.finish_phase(0, detail=f"listening on 127.0.0.1:{port}")

    exit_code = 0

    try:
        with Live(ui.render, refresh_per_second=8, console=console,
                  screen=False) as live:
            # Bind the renderable to the UI so each tick recomputes.
            class _R:
                def __rich__(_self): return ui.render()
            live.update(_R())

            # ── Phase A (subprocess) ───────────────────────────────────────
            ui.start_phase(1)
            time.sleep(pause_s)

            phase_a, run_id = _spawn_phase_a(
                url=url, ledger_path=ledger_path, crash=True,
                invocation_id=invocation_id, session_id=session_id)

            # As soon as we have a run_id, start streaming the journal so the
            # body pane lights up while phase A is still running.
            if run_id:
                ui.attach_run(run_id)

            # Wait for the subprocess to either finish or crash. The UI thread
            # is already pumping; we just block here.
            stderr_tail = ""
            try:
                stderr_tail = phase_a.stdout.read() or ""
            except Exception:  # noqa: BLE001
                stderr_tail = ""
            phase_a.wait(timeout=20)

            # Give the stream a beat to catch up on the last entries.
            time.sleep(0.5)

            # Phase 2-5 narrate from the journal we just observed.
            with ui.lock:
                kinds_seen = [e.kind for e in ui.entries]
            ui.finish_phase(1, detail="decision#0 (model=demo/oracle) recorded")
            ui.finish_phase(2, detail="effect#0 (bank.wire) PENDING — intent committed")
            ui.finish_phase(3, detail="bank wrote wire-0001 keyed by idempotency_key")
            time.sleep(pause_s)
            ui.finish_phase(4, ok=False,
                            detail=f"exit code {phase_a.returncode} — the ack never came back")
            ui.mark_crash()
            time.sleep(pause_s * 1.5)

            if not run_id:
                # Couldn't get a run_id from phase A — fail loud.
                ui.set_headline("[red]demo aborted: phase A produced no run_id[/red]")
                exit_code = 2
                raise typer.Exit(2)

            # ── Phase B (in-process) ───────────────────────────────────────
            ui.start_phase(5)
            time.sleep(pause_s)
            result = _phase_b_recover(url=url, ledger_path=ledger_path,
                                      run_id=run_id)
            time.sleep(pause_s)
            ui.finish_phase(5, detail="found the run; resumed under a new lease")

            # Show what the re-drive did.
            ui.finish_phase(6, detail=" · ".join(result["replayed"]))
            time.sleep(pause_s)
            ui.finish_phase(7, detail=" · ".join(result["actions"]))
            time.sleep(pause_s)

            # ── Phase 8 — verify ───────────────────────────────────────────
            ui.start_phase(8)
            wires = ui.bank_wires()
            n = len(wires)
            if n == 1:
                ui.finish_phase(8, detail=f"bank ledger: 1 wire ({wires[0]['wire_id']})")
                ui.set_headline(
                    "[bold green]✓ exactly one wire — durability proved.[/bold green]  "
                    f"`tape inspect {run_id}` to explore.")
                exit_code = 0
            else:
                ui.finish_phase(8, ok=False,
                                detail=f"bank ledger has {n} wires — expected 1")
                ui.set_headline(
                    f"[bold red]✗ demo FAILED: {n} wires (expected 1)[/bold red]")
                exit_code = 1
            # Hold the final frame so the user can read it.
            time.sleep(pause_s * 2)

    except typer.Exit:
        raise
    except Exception as ex:  # noqa: BLE001
        warn(f"demo crashed: {ex}")
        exit_code = 2
    finally:
        ui.stop()
        if not keep_after:
            with suppress(Exception):
                server_proc.terminate()
                server_proc.wait(timeout=3)
            with suppress(Exception):
                import shutil
                shutil.rmtree(workdir, ignore_errors=True)
        else:
            ok(f"keeping workdir: {workdir}")
            ok(f"server still up: {url}  (pid {server_proc.pid})")
            ok(f"inspect with:  tape inspect <run-id> --url {url}")

    raise typer.Exit(exit_code)


# ── unknown-reconcile scenario ─────────────────────────────────────────────
#
# Where crash-resume covers the **idempotent + inline** path (the agent crashes
# mid-effect, the journal still says PENDING, the re-drive observes the bank's
# own ledger and completes), this demo covers the harder, unique-to-Tape path:
# **non-idempotent + outbox + UNKNOWN + reconciler**.
#
# The agent never touches the bank. It writes an *intent* row (`BeginEffect`
# with semantics=NON_IDEMPOTENT, dispatch_mode=OUTBOX, business_key="…"). A
# dedicated outbox dispatcher claims the row under a CAS lease, calls the
# bank, and reports the result. We then simulate the network glitch that
# loses the ack: the wire LANDS on disk but the dispatcher's `dispatch()`
# returns `UNKNOWN`. The server transitions the effect to UNKNOWN (the
# loudest status in the runtime — "may have happened, may not"). A
# reconciler tick queries the bank by business_key, sees the wire, and
# calls `RecordExternalObservation(CONFIRMED, external_ref=wire_id)`. The
# effect flips to CONFIRMED. Exactly one wire on disk.
#
# This is the scenario the roadmap calls out specifically — "crash after
# dispatch before ACK · show UNKNOWN · show reconciliation" — and the path
# that distinguishes Tape's contract from "just retry harder".


def _run_unknown_reconcile_scenario(*, url: str, ledger_path: Path,
                                    ui: "_DemoUI",
                                    pause_s: float) -> tuple[str, int]:
    """The full in-process scenario. Returns (run_id, exit_code)."""
    from tape.client import TapeClient
    from tape._gen.tape_pb2 import (
        EFFECT_DISPATCH_MODE_OUTBOX,
        EFFECT_RESOLUTION_CONFIRMED,
        EFFECT_SEMANTICS_NON_IDEMPOTENT,
    )

    invocation_id = f"unknown-{uuid.uuid4().hex[:10]}"
    session_id = f"sess-{uuid.uuid4().hex[:6]}"
    business_key = "acct-1:2000000:2026-05-18"
    bank = FileBank(ledger_path)

    with TapeClient(url) as c:
        # ── Phase: agent records decision + journals an OUTBOX intent ──────
        ui.start_phase(1); time.sleep(pause_s)
        r = c.begin_run(
            app_name="treasury-demo-unknown", user_id="cfo",
            session_id=session_id, invocation_id=invocation_id,
            lease_owner=f"demo-pid-{os.getpid()}", lease_ttl_ms=30_000)
        run_id = r.run_id
        ui.attach_run(run_id)
        # Give the streamer a beat to subscribe before we start writing.
        time.sleep(0.15)

        c.record_decision(
            run_id=run_id, decision_index=0, model="demo/oracle",
            request_json='{"q":"close the book"}',
            response_json='{"call":"execute_sweep","amount":2000000}',
            rationale="excess USD; sweep to MMF")
        ui.finish_phase(1, detail="decision#0 (model=demo/oracle) recorded")
        time.sleep(pause_s)

        ui.start_phase(2); time.sleep(pause_s)
        eff = c.begin_effect(
            run_id=run_id, decision_index=0,
            tool_name="bank.wire", call_index=0,
            request_json=json.dumps({"account_id": "acct-1",
                                     "amount_minor": 2_000_000}),
            semantics=EFFECT_SEMANTICS_NON_IDEMPOTENT,
            dispatch_mode=EFFECT_DISPATCH_MODE_OUTBOX,
            business_key=business_key,
            connector="bank.wire")
        ui.finish_phase(2,
                        detail=f"effect#0 PENDING — intent only; bank NOT yet called  "
                               f"(business_key={business_key})")
        time.sleep(pause_s)

        # ── Phase: outbox dispatcher (in-process, one tick) ────────────────
        ui.start_phase(3); time.sleep(pause_s)
        # CAS lease so a peer dispatcher can't double-fire.
        claim = c.claim_effect_dispatch(
            run_id=run_id, idempotency_key=eff.idempotency_key,
            claimer="demo-dispatcher", lease_ttl_ms=30_000)
        if not claim.acquired:
            ui.finish_phase(3, ok=False,
                            detail="couldn't acquire dispatch lease")
            return run_id, 2
        ui.finish_phase(3, detail=f"lease acquired by demo-dispatcher")
        time.sleep(pause_s)

        # The dispatcher calls the bank. The wire DOES land — the bank's
        # ledger gains a row keyed by business_key. In a real connector this
        # is an HTTPS POST; here it's a file write.
        ui.start_phase(4); time.sleep(pause_s)
        wire_record = bank.wire_by_business_key(
            business_key=business_key, amount_minor=2_000_000,
            account_id="acct-1")
        ui.finish_phase(4,
                        detail=f"bank ledger now has {wire_record['wire_id']} "
                               f"keyed by business_key")
        time.sleep(pause_s)

        # SIMULATE: the wire landed but the network glitched on the way back.
        # The dispatcher reports the dispatch as UNKNOWN — passing
        # next_dispatch_at_ms=0 tells the server "do NOT re-dispatch; this
        # is for the reconciler now".
        ui.start_phase(5); time.sleep(pause_s)
        c.record_dispatch_attempt(
            run_id=run_id, idempotency_key=eff.idempotency_key,
            error="simulated lost ack (network glitch after wire landed)",
            next_dispatch_at_ms=0)
        ui.finish_phase(5,
                        detail="record_dispatch_attempt(next_dispatch_at_ms=0) — "
                               "the server transitions PENDING → UNKNOWN")
        time.sleep(pause_s * 1.5)

        # ── Phase: reconciler (in-process, one tick) ───────────────────────
        ui.start_phase(6); time.sleep(pause_s)
        pending = c.list_pending_effects(
            include_pending=False, include_unknown=True, limit=10)
        unknown_effs = [e for e in pending.effects
                        if e.run_id == run_id
                        and e.idempotency_key == eff.idempotency_key]
        if not unknown_effs:
            ui.finish_phase(6, ok=False,
                            detail="server didn't report the UNKNOWN — flake or version skew")
            return run_id, 2
        ui.finish_phase(6, detail=f"found 1 UNKNOWN effect via list_pending_effects")
        time.sleep(pause_s)

        ui.start_phase(7); time.sleep(pause_s)
        # The reconciler's observe() — ask the bank by business_key.
        observed = bank.find_by_business_key(business_key)
        if observed is None:
            # Would never happen in this demo (we just wrote it) but the
            # connector contract handles "ABSENT" — in real life we'd need
            # human approval to re-issue.
            ui.finish_phase(7, ok=False,
                            detail="bank says ABSENT — needs human approval")
            return run_id, 2
        ui.finish_phase(7,
                        detail=f"bank.observe(business_key) → CONFIRMED, "
                               f"external_ref={observed['wire_id']}")
        time.sleep(pause_s)

        ui.start_phase(8); time.sleep(pause_s)
        c.record_external_observation(
            run_id=run_id, idempotency_key=eff.idempotency_key,
            resolution=EFFECT_RESOLUTION_CONFIRMED,
            external_ref=observed["wire_id"],
            response_json=json.dumps({"wire_id": observed["wire_id"]}))
        ui.finish_phase(8,
                        detail="record_external_observation(CONFIRMED) — "
                               "the server transitions UNKNOWN → CONFIRMED")
        time.sleep(pause_s)

        c.end_run(run_id=run_id)

    # ── Verify ─────────────────────────────────────────────────────────────
    ui.start_phase(9); time.sleep(pause_s)
    wires = ui.bank_wires()
    if len(wires) == 1:
        ui.finish_phase(9, detail=f"bank ledger: 1 wire ({wires[0]['wire_id']})")
        ui.set_headline(
            f"[bold green]✓ exactly one wire — UNKNOWN survived.[/bold green]  "
            f"`tape inspect {run_id}` to explore.")
        return run_id, 0
    ui.finish_phase(9, ok=False,
                    detail=f"bank ledger has {len(wires)} wires — expected 1")
    ui.set_headline(
        f"[bold red]✗ demo FAILED: {len(wires)} wires (expected 1)[/bold red]")
    return run_id, 1


@app.command(name="unknown-reconcile",
             help="Non-idempotent + OUTBOX + UNKNOWN + reconciler — the full ambiguity loop.")
def unknown_reconcile(
    pause_s: float = typer.Option(
        0.7, "--pause", "-p",
        help="Pause between phases (seconds). UNKNOWN is the loudest signal "
             "in the runtime — slowing this down a notch is fine."),
    keep_after: bool = typer.Option(
        False, "--keep",
        help="Don't tear down the server / ledger when the demo finishes."),
    server_binary: Optional[str] = typer.Option(
        None, "--server-binary",
        help="Path to a built `tape-server` (default: auto-locate)."),
):
    """The roadmap calls this scenario out by name: 'crash after dispatch
    before ACK · show UNKNOWN · show reconciliation'.

    What you see, phase by phase:

      1. tape-server up
      2. agent records decision + opens a NON_IDEMPOTENT/OUTBOX effect.
         CRUCIALLY the agent never touches the bank — only the intent
         is journaled. (status = PENDING)
      3. outbox dispatcher claims the dispatch slot under a CAS lease
      4. dispatcher calls the bank — the wire LANDS on disk
      5. simulated network glitch loses the ack; dispatcher reports
         next_dispatch_at_ms=0 → server transitions PENDING → UNKNOWN
      6. reconciler lists pending/unknown effects, finds ours
      7. reconciler observes the bank by business_key — wire IS there
      8. record_external_observation(CONFIRMED, external_ref) →
         server transitions UNKNOWN → CONFIRMED
      9. verify: bank ledger has exactly one wire

    Exits 0 iff exactly one wire on disk. UNKNOWN status is rendered in
    bold red on yellow — the loudest badge in the runtime.
    """
    binary = server_binary or _find_server_binary()
    if not binary:
        die("no tape-server binary found.\n"
            "  Build with: `cd tape/server && cargo build`")

    workdir = Path(tempfile.mkdtemp(prefix="tape-demo-unknown-"))
    db_path = workdir / "tape.db"
    ledger_path = workdir / "bank.json"
    port = _free_port()
    url = f"tape://127.0.0.1:{port}"

    info(f"[dim]workdir: {workdir}[/dim]")
    info(f"[dim]server : {binary}[/dim]")
    info(f"[dim]url    : {url}[/dim]\n")

    server_proc = subprocess.Popen(
        [binary, "--listen", f"127.0.0.1:{port}",
         "--store", f"sqlite:{db_path}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not _wait_until_listening(port):
        server_proc.terminate()
        die(f"tape-server didn't come up on :{port}")

    ui = _DemoUI(url=url, ledger_path=ledger_path,
                 title="demo unknown-reconcile",
                 default_subtitle="NON-IDEMPOTENT + OUTBOX · ack lost on the wire · reconciler resolves UNKNOWN",
                 ledger_ok_suffix="— UNKNOWN survived, exactly one wire")
    ui.add_phases(
        "tape-server up",
        "agent: record decision",
        "agent: open OUTBOX effect (NON-IDEMPOTENT) → PENDING (no bank call)",
        "outbox dispatcher: claim dispatch lease (CAS)",
        "dispatcher → bank.wire: wire LANDS keyed by business_key",
        "ack lost (network glitch) — record_dispatch_attempt → UNKNOWN",
        "reconciler: list pending/unknown effects, find ours",
        "reconciler: observe(business_key) → bank says CONFIRMED",
        "RecordExternalObservation(CONFIRMED) — effect → CONFIRMED",
        "verify: exactly ONE wire on disk",
    )
    ui.finish_phase(0, detail=f"listening on 127.0.0.1:{port}")

    exit_code = 0
    run_id = ""

    try:
        with Live(ui.render, refresh_per_second=8, console=console,
                  screen=False) as live:
            class _R:
                def __rich__(_self): return ui.render()
            live.update(_R())

            # The whole scenario runs in this process — no crash needed
            # because the failure we're modelling is a *network glitch*,
            # not a process death.
            try:
                run_id, exit_code = _run_unknown_reconcile_scenario(
                    url=url, ledger_path=ledger_path, ui=ui, pause_s=pause_s)
            except Exception as ex:  # noqa: BLE001
                warn(f"scenario crashed: {ex}")
                exit_code = 2

            # Hold the final frame so the user can read it.
            time.sleep(pause_s * 3)
    finally:
        ui.stop()
        if not keep_after:
            with suppress(Exception):
                server_proc.terminate()
                server_proc.wait(timeout=3)
            with suppress(Exception):
                import shutil
                shutil.rmtree(workdir, ignore_errors=True)
        else:
            ok(f"keeping workdir: {workdir}")
            ok(f"server still up: {url}  (pid {server_proc.pid})")
            if run_id:
                ok(f"inspect with:  tape inspect {run_id} --url {url}")

    raise typer.Exit(exit_code)


# ── tape-adk (embedded) scenario ───────────────────────────────────────────
#
# A third demo that proves the architectural realignment: the SAME UNKNOWN →
# reconcile loop, but running against `tape-adk` (which writes into ADK's
# own DatabaseSessionService via SQLAlchemy) instead of the Rust gRPC server.
# No server subprocess; one Python process holding both the journal and
# the reactor logic. The user-visible contract is identical — exactly one
# wire on disk after UNKNOWN survives — but the operational story is much
# smaller.


@app.command(name="tape-adk-embedded",
             help="UNKNOWN→reconcile loop running against tape-adk (no separate server).")
def tape_adk_embedded(
    pause_s: float = typer.Option(
        0.5, "--pause", "-p",
        help="Pause between phases (seconds)."),
    db_path: Optional[str] = typer.Option(
        None, "--db",
        help="SQLite file path (default: temp file in a fresh workdir)."),
    keep_after: bool = typer.Option(
        False, "--keep",
        help="Don't tear down the workdir / DB when finished."),
):
    """Self-contained demo that uses `tape-adk` (the SQL-embedded form) for
    the journal — no `tape-server` subprocess, no gRPC. The flow is the
    same as `tape demo unknown-reconcile` but the storage is ADK's
    `DatabaseSessionService` extended with `tape-adk`'s effect ledger and
    reactor library.

    Proves the architectural realignment in vivo: same contract, smaller
    operational story, runs anywhere ADK runs.
    """
    try:
        from tape_adk import (  # noqa: F401
            EffectDispatchMode,
            EffectSemantics,
            EffectStatus,
            TapeSessionService,
            dispatch_outbox_once,
            reconcile_once,
        )
        from tape_adk.connectors import (
            DispatchResult, ObservationResult,
        )
    except ImportError:
        die("tape-adk not installed.\n  Fix: pip install tape-adk")

    import asyncio
    import shutil

    workdir = Path(db_path).parent if db_path else \
        Path(tempfile.mkdtemp(prefix="tape-adk-demo-"))
    workdir.mkdir(parents=True, exist_ok=True)
    db = Path(db_path) if db_path else (workdir / "tape.db")
    ledger_path = workdir / "bank.json"
    db_url = f"sqlite+aiosqlite:///{db}"

    info(f"[dim]workdir: {workdir}[/dim]")
    info(f"[dim]db url : {db_url}[/dim]\n")

    # A small in-line connector that mirrors the unknown-reconcile demo.
    bank = FileBank(ledger_path)
    business_key = "acct-1:2000000:2026-05-20"

    class _DemoBankConnector:
        name = "bank.wire"
        n_dispatches = 0

        async def dispatch(self, eff):
            self.n_dispatches += 1
            req = eff.request_json or {}
            wire = bank.wire_by_business_key(
                business_key=eff.business_key,
                amount_minor=req.get("amount_minor", 0),
                account_id=req.get("account_id", "?"))
            # First dispatch: simulate lost ack.
            if self.n_dispatches == 1:
                return DispatchResult(
                    status="unknown",
                    error={"reason": "simulated lost ack"})
            return DispatchResult(
                status="confirmed", external_ref=wire["wire_id"],
                response={"wire_id": wire["wire_id"]})

        async def observe(self, eff):
            rec = bank.find_by_business_key(eff.business_key or "")
            if rec is None:
                return ObservationResult(status="absent")
            return ObservationResult(
                status="confirmed", external_ref=rec["wire_id"],
                response={"wire_id": rec["wire_id"]})

        async def compensate(self, ob):  # pragma: no cover — not exercised
            return None

    connector = _DemoBankConnector()
    ui = _DemoUI(url=db_url, ledger_path=ledger_path,
                 title="demo tape-adk-embedded",
                 default_subtitle=("EMBEDDED · TapeSessionService on SQLAlchemy · "
                                   "no separate server"),
                 ledger_ok_suffix=(
                     "— same UNKNOWN→reconcile contract, in-process"))
    ui.add_phases(
        "TapeSessionService up (sqlite + aiosqlite)",
        "session created",
        "begin_effect(NON-IDEMPOTENT + OUTBOX) — PENDING in tape_effects",
        "dispatch_outbox_once: claim + bank.dispatch returns UNKNOWN",
        "tape_effects row → UNKNOWN (loud signal)",
        "reconcile_once: observe(business_key) → bank says CONFIRMED",
        "record_external_observation(CONFIRMED) — row → CONFIRMED",
        "verify: exactly ONE wire on disk",
    )

    async def _run() -> tuple[Optional[str], int]:
        svc = TapeSessionService(db_url=db_url)

        ui.start_phase(0); await asyncio.sleep(pause_s)
        sess = await svc.create_session(
            app_name="treasury-adk-demo", user_id="cfo",
            session_id=f"s-{uuid.uuid4().hex[:6]}", state={})
        ui.finish_phase(0, detail="DatabaseSessionService initialised")
        ui.finish_phase(1, detail=f"session={sess.id}")
        await asyncio.sleep(pause_s)

        ui.start_phase(2); await asyncio.sleep(pause_s)
        eff = await svc.begin_effect(
            app_name=sess.app_name, user_id=sess.user_id,
            session_id=sess.id, invocation_id=f"inv-{uuid.uuid4().hex[:8]}",
            decision_index=0, tool_name="bank.wire", call_index=0,
            request_json={"amount_minor": 2_000_000, "account_id": "acct-1"},
            semantics=EffectSemantics.NON_IDEMPOTENT,
            dispatch_mode=EffectDispatchMode.OUTBOX,
            business_key=business_key, connector="bank.wire")
        ui.finish_phase(2, detail=f"key={eff.idempotency_key[-30:]}")
        await asyncio.sleep(pause_s)

        ui.start_phase(3); await asyncio.sleep(pause_s)
        r1 = await dispatch_outbox_once(
            svc, connectors={"bank.wire": connector},
            claimer="demo-dispatcher")
        outcomes = [x.get("outcome") for x in r1]
        if "unknown" not in outcomes:
            ui.finish_phase(3, ok=False,
                            detail=f"expected unknown, got {outcomes}")
            return None, 2
        ui.finish_phase(3, detail="dispatcher → bank → UNKNOWN")
        ui.finish_phase(4, detail="row.status = unknown")
        await asyncio.sleep(pause_s)

        ui.start_phase(5); await asyncio.sleep(pause_s)
        r2 = await reconcile_once(svc, connectors={"bank.wire": connector})
        outcomes2 = [x.get("outcome") for x in r2]
        if "confirmed" not in outcomes2:
            ui.finish_phase(5, ok=False,
                            detail=f"expected confirmed, got {outcomes2}")
            return None, 2
        ui.finish_phase(5,
                        detail="bank.observe(business_key) → CONFIRMED")
        ui.finish_phase(6, detail="row.status = confirmed")
        await asyncio.sleep(pause_s)

        ui.start_phase(7); await asyncio.sleep(pause_s)
        wires = ui.bank_wires()
        if len(wires) == 1:
            ui.finish_phase(7, detail=f"bank ledger: 1 wire ({wires[0]['wire_id']})")
            ui.set_headline(
                f"[bold green]✓ exactly one wire — UNKNOWN survived "
                f"(embedded path).[/bold green]")
            return sess.id, 0
        ui.finish_phase(7, ok=False,
                        detail=f"bank ledger has {len(wires)} — expected 1")
        return None, 1

    exit_code = 0
    session_id: Optional[str] = None
    try:
        with Live(ui.render, refresh_per_second=8, console=console,
                  screen=False) as live:
            class _R:
                def __rich__(_self): return ui.render()
            live.update(_R())
            try:
                session_id, exit_code = asyncio.run(_run())
            except Exception as ex:  # noqa: BLE001
                warn(f"scenario crashed: {ex}")
                exit_code = 2
            time.sleep(pause_s * 3)
    finally:
        ui.stop()
        if not keep_after:
            with suppress(Exception):
                shutil.rmtree(workdir, ignore_errors=True)
        else:
            ok(f"keeping workdir: {workdir}")
            if session_id:
                ok(f"inspect with:  tape inspect-adk {session_id} "
                   f"--db-url {db_url}")
    raise typer.Exit(exit_code)


# Entry-point shim so the subprocess invocation works:
#   python -m tape_cli.commands.demo --phase-a
if __name__ == "__main__":  # pragma: no cover — subprocess entry only
    if "--phase-a" in sys.argv:
        _phase_a_main_for_subprocess()
    else:
        app()
