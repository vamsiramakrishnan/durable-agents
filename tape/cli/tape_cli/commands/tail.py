"""`tape tail` — live cross-run journal stream.

Where `tape inspect <run>` focuses on one run's timeline, `tape tail` shows
*every run's* journal events in real time, filtered by subject pattern,
kind, or run id. It's the global view: what's the runtime doing right now,
across everything that's executing?

Examples::

    tape tail                                    # everything, live
    tape tail --subject '/tape/effect/**'        # only effect-bus subjects
    tape tail --subject '/tape/effect/*/unknown' # only effects going UNKNOWN
    tape tail --kind effect                      # legacy kind filter
    tape tail --run abc123                       # one run (same as inspect, simpler render)
    tape tail --raw                              # JSONL — pipe to jq
    tape tail --predicate 'subject.contains("treasury")'  # CEL filter (server-side)

This is the closest thing to `tail -f /var/log/runtime` for a stochastic
orchestrator — the "show me the runtime working" surface the roadmap calls out.
"""

from __future__ import annotations

import json
import os
import time
from typing import Optional

import grpc
import typer
from rich.console import Console
from rich.table import Table
from rich.text import Text

from ..config import TapeProject, find_project_root
from ..util import console, die, info, warn
from ._journal import decode_entry, fmt_rel_ms


def _resolve_url(url_flag: Optional[str]) -> str:
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


def _print_header(url: str, subject: str, kind: str, run: str,
                  predicate: str, from_global_seq: int):
    """A one-line header that explains exactly what's being streamed."""
    bits = [f"[bold cyan]{url}[/bold cyan]"]
    if subject:
        bits.append(f"subject=[bold]{subject}[/bold]")
    if kind:
        bits.append(f"kind=[bold]{kind}[/bold]")
    if run:
        bits.append(f"run=[bold]{run}[/bold]")
    if predicate:
        bits.append(f"predicate=[bold]{predicate}[/bold]")
    if from_global_seq:
        bits.append(f"from_global_seq={from_global_seq}")
    console.rule(" · ".join(bits))


def _render_row(entry, base_ts_ms: Optional[int]) -> Text:
    """One Rich Text line per entry — type+status+summary, with subject + run
    suffixed dim. We render as a single line (rather than a Table row) so the
    stream can be infinitely long without rich.Table's repaint overhead."""
    d = decode_entry(entry.kind, entry.payload_json, entry.trace_id)

    rel = fmt_rel_ms(entry.ts_ms - base_ts_ms) if base_ts_ms else "+0ms"
    txt = Text.assemble(
        (f"{entry.global_seq:>6}", "dim"),
        "  ",
        (f"{rel:>8}", "dim"),
        "  ",
        (d.icon, "white"),
        " ",
        (f"{d.type:<10}", "bold"),
        "  ",
        (f"{d.status:<11}", d.style or "white"),
        "  ",
    )
    txt.append_text(Text.from_markup(d.summary))
    suffix_bits = []
    if getattr(entry, "run_id", ""):
        suffix_bits.append(("run=" + entry.run_id[:12]))
    if entry.subject:
        suffix_bits.append("subj=" + entry.subject)
    if suffix_bits:
        txt.append("   ")
        txt.append("  ".join(suffix_bits), style="dim")
    return txt


def run(
    subject: str = typer.Option(
        "", "--subject", "-s",
        help="Subject pattern, e.g. '/tape/effect/**'. Supports * (one segment) and ** (rest)."),
    kind: str = typer.Option(
        "", "--kind", "-k",
        help="Legacy filter (decision|effect|obligation|gate|value|run|event)."),
    run_id: str = typer.Option(
        "", "--run", "-r",
        help="Restrict to one run id."),
    predicate: str = typer.Option(
        "", "--predicate", "-p",
        help="Server-side CEL predicate on the event (see tape-event-bus.md)."),
    from_global_seq: int = typer.Option(
        0, "--from-global-seq", "-g",
        help="Resume from this global_seq (0 => from earliest)."),
    from_ts_ms: int = typer.Option(
        0, "--from-ts-ms",
        help="Legacy SubscribeEvents-style ts-cursor (only used when no subject)."),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-n",
        help="Stop after this many entries."),
    raw: bool = typer.Option(
        False, "--raw",
        help="Emit JSONL — one EventEntry per line, no formatting."),
    url: Optional[str] = typer.Option(
        None, "--url",
        help="Override the tape server URL. Default: $TAPE_URL or tape.yaml."),
):
    """Tail the journal across all runs — the runtime's live event stream.

    Picks the subject-routed bus (SubscribeBySubject) when --subject or
    --predicate is given, falling back to the legacy SubscribeEvents stream
    otherwise. Both flow through the same WAL — the subject bus just runs
    your filter server-side instead of client-side.
    """
    resolved_url = _resolve_url(url)
    TapeClient = _import_client()

    try:
        client = TapeClient(resolved_url)
    except Exception as ex:  # noqa: BLE001
        die(f"failed to connect to {resolved_url}: {ex}")

    use_subject_bus = bool(subject or predicate)
    if use_subject_bus:
        # Default subject pattern when only a predicate was given: match all.
        pat = subject or "/tape/**"
        try:
            it = client.subscribe_by_subject(
                subject_pattern=pat, predicate_cel=predicate,
                from_global_seq=from_global_seq)
        except grpc.RpcError as ex:
            die(f"subscribe_by_subject failed: {ex.code().name} {ex.details()}")
    else:
        try:
            it = client.subscribe_events(
                from_ts_ms=from_ts_ms, run_id=run_id, kind=kind,
                from_global_seq=from_global_seq)
        except grpc.RpcError as ex:
            die(f"subscribe_events failed: {ex.code().name} {ex.details()}")

    # --raw output must be strict JSONL so it pipes cleanly to jq; suppress
    # the human-facing rule in that mode.
    if not raw:
        _print_header(resolved_url, subject or kind, kind, run_id,
                      predicate, from_global_seq)

    seen = 0
    base_ts: Optional[int] = None
    try:
        for entry in it:
            # Client-side post-filter for --run when using subject bus
            # (legacy SubscribeEvents already handles run_id server-side).
            if run_id and use_subject_bus and entry.run_id != run_id:
                continue
            if base_ts is None:
                base_ts = entry.ts_ms
            if raw:
                out = {
                    "global_seq": entry.global_seq,
                    "run_id": entry.run_id,
                    "seq": entry.seq,
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
            else:
                console.print(_render_row(entry, base_ts))
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
            warn(f"stream ended: {ex.code().name} {ex.details()}")
    finally:
        try: client.close()
        except Exception: pass  # noqa: BLE001, E701

    if seen == 0:
        info("[dim]…no entries received.[/dim]")


def _safe_inline_json(s: str):
    if not s:
        return None
    try:
        return json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return s
