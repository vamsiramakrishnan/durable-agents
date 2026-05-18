"""Shared decoder + formatters for `tape inspect` and `tape tail`.

The Tape journal is one append-only stream of entries (kind+payload_json). The
inspector's job is to turn that stream into something a human can read at a
glance: one icon + one badge + one short summary per entry. That decoding lives
here so both `inspect` (per-run timeline) and `tail` (cross-run stream) share
the same vocabulary.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Optional


# ── kind icons (the visual vocabulary of the journal) ──────────────────────
# One glyph per primitive, picked to be distinct in a monospace column.
_KIND_ICONS: dict[str, str] = {
    "run":        "▶",
    "decision":   "●",
    "effect":     "◆",
    "obligation": "↩",
    "gate":       "⏸",
    "value":      "≡",
    "event":      "✦",
    "timer":      "⏱",
}


def kind_icon(kind: str) -> str:
    return _KIND_ICONS.get(kind, "·")


# ── status → style (rich markup colors) ────────────────────────────────────
# The Inspector's job is to make failure VISIBLE. Status colors:
#   green   = good / confirmed / terminal happy
#   yellow  = in-flight (pending)
#   red     = UNKNOWN — the hardest failure mode in the system, called out bold
#   magenta = compensation in flight
#   cyan    = a decision / new activity
#   blue    = waiting on a human (gate)
#   dim     = informational, replayed-or-historical
_STATUS_STYLES: dict[str, str] = {
    # effects
    "pending":    "yellow",
    "confirmed":  "bold green",
    "failed":     "bold red",
    "unknown":    "bold red on yellow",   # the loudest signal in the runtime
    # obligations
    "committed":  "magenta",
    "compensated": "bold green",
    "stuck":      "bold red on yellow",
    # gates
    "waiting":    "blue",
    "released":   "bold green",
    # run lifecycle
    "running":    "bold cyan",
    "terminal":   "bold green",
    "cancelled":  "yellow",
    "compensating": "magenta",
    "ended":      "dim",
    # generic
    "recorded":   "cyan",
    "ok":         "green",
    "":           "dim",
}


def status_style(status: str) -> str:
    return _STATUS_STYLES.get((status or "").lower(), "white")


# ── run-status enum → label + style ────────────────────────────────────────
# Mirrors RunStatus in the proto (proto/tape.proto §lifecycle).
_RUN_STATUS_LABELS = {
    0: ("UNSPECIFIED", "dim"),
    1: ("RUNNABLE",    "yellow"),
    2: ("RUNNING",     "bold cyan"),
    3: ("WAITING",     "blue"),
    4: ("TERMINAL",    "bold green"),
    5: ("FAILED",      "bold red"),
    6: ("COMPENSATING", "magenta"),
    7: ("STUCK",       "bold red on yellow"),
    8: ("CANCELLED",   "yellow"),
}


def run_status_label(status: int) -> tuple[str, str]:
    return _RUN_STATUS_LABELS.get(int(status), (f"?{status}", "dim"))


# ── effect-status enum → label (proto/tape.proto §effect ledger) ───────────
_EFFECT_STATUS_LABELS = {
    0: "unspecified",
    1: "pending",
    2: "confirmed",
    3: "failed",
    4: "unknown",
}


def effect_status_label(status: int | str) -> str:
    """Accept either the enum int or the lower-case string. Both shapes appear
    in payload_json across stores (sim stringifies, sql may write either)."""
    if isinstance(status, str):
        return status.lower()
    return _EFFECT_STATUS_LABELS.get(int(status), str(status))


# ── time formatters ────────────────────────────────────────────────────────


def fmt_rel_ms(delta_ms: int) -> str:
    """Format a millisecond duration as a compact relative time."""
    if delta_ms < 0:
        return f"-{fmt_rel_ms(-delta_ms)}"
    if delta_ms < 1_000:
        return f"+{delta_ms}ms"
    if delta_ms < 60_000:
        return f"+{delta_ms / 1000:.1f}s"
    if delta_ms < 3_600_000:
        return f"+{delta_ms / 60_000:.1f}m"
    return f"+{delta_ms / 3_600_000:.1f}h"


def fmt_countdown_ms(expires_at_ms: int, now_ms: Optional[int] = None) -> str:
    """Format a lease/gate countdown. Returns 'EXPIRED' in past tense, the
    remaining time prefixed with `~` otherwise."""
    n = now_ms if now_ms is not None else int(time.time() * 1000)
    if expires_at_ms <= 0:
        return "—"
    delta = expires_at_ms - n
    if delta <= 0:
        return f"EXPIRED {fmt_rel_ms(-delta)} ago"
    return f"~{fmt_rel_ms(delta).lstrip('+')}"


# ── payload decoding (the meat) ────────────────────────────────────────────


@dataclass
class Decoded:
    """The display-ready shape of a JournalEntry.

    - `type`:   one short noun ("decision", "effect", ...)
    - `status`: one short adjective ("pending", "confirmed", ...) — '' when n/a
    - `style`:  rich markup style for the status badge
    - `icon`:   single-character glyph for the type column
    - `summary`: a one-line description (already tagged with rich markup)
    - `trace_id`: pulled up for display (empty when missing)
    """

    type: str
    status: str
    style: str
    icon: str
    summary: str
    trace_id: str = ""


def _safe_loads(s: str) -> dict:
    """Return a dict no matter what. Older server versions or buggy custom
    code can write null / list / scalar payloads; we'd rather degrade
    gracefully than crash the inspector."""
    try:
        v = json.loads(s) if s else {}
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}
    return v if isinstance(v, dict) else {}


def _short(s: str, n: int = 40) -> str:
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


def decode_entry(kind: str, payload_json: str, trace_id: str = "") -> Decoded:
    """Turn (kind, payload_json) into a Decoded ready for rendering.

    Be tolerant: the in-memory store (test/sim) and the SQL store emit slightly
    different payload shapes (ad-hoc format!() strings vs serde_json). The
    decoder reads what's there, gracefully degrades when fields are missing.
    """
    p = _safe_loads(payload_json)
    icon = kind_icon(kind)

    if kind == "run":
        s = str(p.get("status", "")).lower()
        app = p.get("app", "")
        user = p.get("user", "")
        sess = p.get("session", "")
        rid = p.get("run_id", "")
        bits = []
        if app:  bits.append(app)
        if user: bits.append(user)
        if sess: bits.append(_short(sess, 18))
        loc = " / ".join(bits) if bits else _short(rid, 16)
        return Decoded("run", s, status_style(s), icon, loc, trace_id)

    if kind == "decision":
        model = p.get("model", "")
        idx = p.get("decision_index", "?")
        # Sql store: rationale + policy_version present; sim: model + policy_version.
        rationale = p.get("rationale", "")
        bits = [f"[bold]{model}[/bold]" if model else "model=?"]
        bits.append(f"idx={idx}")
        if rationale:
            bits.append(f"[dim]{_short(rationale, 60)}[/dim]")
        return Decoded("decision", "recorded", status_style("recorded"), icon,
                       "  ".join(bits), trace_id)

    if kind == "effect":
        st_raw = p.get("status", "")
        st = effect_status_label(st_raw)
        tool = p.get("tool", "") or p.get("tool_name", "")
        bk = p.get("business_key", "")
        ek = p.get("idempotency_key", "")
        xref = p.get("external_ref", "")
        # Build the summary: tool name (always) + the most useful identity
        # available (external_ref > business_key > short idempotency_key).
        ident = ""
        if xref:
            ident = f" → [cyan]{_short(xref, 28)}[/cyan]"
        elif bk:
            ident = f"  [dim]({_short(bk, 36)})[/dim]"
        elif ek:
            ident = f"  [dim]{_short(ek, 36)}[/dim]"
        # Surface dispatch_mode + semantics when non-default, so the operator
        # can see at a glance which effects are outbox/non-idempotent.
        tags = []
        # proto: 2 = NON_IDEMPOTENT, 2 = OUTBOX
        if int(p.get("semantics", 0) or 0) == 2:
            tags.append("[red]NI[/red]")
        if int(p.get("dispatch_mode", 0) or 0) == 2:
            tags.append("[yellow]OUT[/yellow]")
        tag_s = (" " + " ".join(tags)) if tags else ""
        return Decoded("effect", st, status_style(st), icon,
                       f"[bold]{tool}[/bold]{tag_s}{ident}",
                       trace_id)

    if kind == "obligation":
        st = str(p.get("status", "")).lower()
        k = p.get("kind", "")
        ek = p.get("effect_key", "")
        return Decoded("obligation", st, status_style(st), icon,
                       f"[bold]{k}[/bold]  [dim]for {_short(ek, 32)}[/dim]",
                       trace_id)

    if kind == "gate":
        st = str(p.get("status", "")).lower()
        gn = p.get("gate", "") or p.get("gate_name", "")
        return Decoded("gate", st, status_style(st), icon,
                       f"[bold]{gn}[/bold]", trace_id)

    if kind == "value":
        # Sql store wraps under "value": { namespace, key, value_json, version }
        v = p.get("value", p)
        ns = v.get("namespace", "")
        k = v.get("key", "")
        ver = v.get("version", "")
        deleted = bool(v.get("deleted", False))
        st = "deleted" if deleted else ""
        summary = f"[bold]{ns}[/bold]/[cyan]{k}[/cyan]"
        if ver != "":
            summary += f"  [dim]v{ver}[/dim]"
        return Decoded("value", st, status_style(st), icon, summary, trace_id)

    if kind == "event":
        # Generic event — show a payload preview.
        preview = _short(payload_json or "", 60)
        return Decoded("event", "", "dim", icon, f"[dim]{preview}[/dim]",
                       trace_id)

    if kind == "timer":
        tid = p.get("timer_id", "")
        tk = p.get("kind", "")
        fire = p.get("fire_at_ms", "")
        return Decoded("timer", "", "dim", icon,
                       f"[bold]{tk}[/bold]  {tid}  [dim]fire@{fire}[/dim]",
                       trace_id)

    # Fallback — unknown kind. Show what we have.
    return Decoded(kind or "?", "", "dim", icon,
                   f"[dim]{_short(payload_json or '', 60)}[/dim]", trace_id)


__all__ = [
    "Decoded", "decode_entry",
    "kind_icon", "status_style",
    "run_status_label", "effect_status_label",
    "fmt_rel_ms", "fmt_countdown_ms",
]
