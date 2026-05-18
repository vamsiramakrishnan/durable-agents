"""Snapshot — capture a run's journal and canonicalize it for equality checks.

A `Snapshot` is a list of `(kind, canonical_payload)` pairs in journal-seq
order. "Canonical" means timestamps are stripped, run-scoped identifiers
are remapped to stable indices, and everything else is preserved
byte-for-byte. Two snapshots compare equal when the underlying journals
record the same logical history — which is exactly what the DST claim is.

Use::

    snap = tape.chaos.snapshot.capture(client, run_id)
    assert snap == previous_snap, snap.diff(previous_snap)

`replay()` builds on this.

**Scope.** The journal records *summaries*: kind, decision_index, tool
name, status, business_key, idempotency_key. Full bodies
(`response_json`, decision payload, effect request) live in the
projection tables (`tape_decisions`, `tape_effects`). Two snapshots
compare equal when these summaries match; payload-body drift slips
through. For full-payload comparison, walk the projections directly via
`get_decision` / `get_effect` after capture. (Phase 3 adds a
`DeepSnapshot` that includes projection bodies.)
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, List, Tuple

from ..client import TapeClient


# Keys that are inherently run-scoped or wall-clock — remove them before
# comparing journals. The substantive content (kind, decision_index, tool
# name, status, business_key, idempotency key suffix, etc.) is kept.
_STRIP_KEYS = frozenset({
    "ts_ms", "started_at_ms", "ended_at_ms", "last_update_time_ms",
    "lease_expires_at_ms", "claim_expires_at_ms", "dispatch_claim_expires_at_ms",
    "next_dispatch_at_ms", "next_attempt_at_ms", "fire_at_ms",
    # The lease owner is hostname:pid — varies per run.
    "lease_owner", "claimed_by", "dispatch_claimed_by",
    # Trace IDs are random per run.
    "trace_id", "span_id", "parent_span_id",
    # The seq is monotonic but absolute; we encode position by list index.
    "seq", "global_seq",
    # invocation_id is the agent's run identifier (the dedup key for the
    # BeginRun call). Two replay passes need *different* invocation_ids to
    # produce two distinct runs, so it's necessarily different per pass —
    # but it's not part of the logical journal content.
    "invocation_id",
})


def _canonical(value: Any, run_id_map: dict[str, str]) -> Any:
    """Strip timestamps + remap run-scoped identifiers. Recursive."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if k in _STRIP_KEYS:
                continue
            out[k] = _canonical(v, run_id_map)
        return out
    if isinstance(value, list):
        return [_canonical(v, run_id_map) for v in value]
    if isinstance(value, str):
        # Remap run-IDs and idempotency keys that start with one (the SDK
        # mints idempotency keys as `<run_id>/decision-<i>/<tool>/<call>`).
        for raw, canonical in run_id_map.items():
            if raw and raw in value:
                return value.replace(raw, canonical)
        return value
    return value


@dataclass(frozen=True)
class JournalLine:
    """One canonicalized journal entry."""
    kind: str            # "decision" | "effect" | "obligation" | "gate" | "value" | "run" | "event"
    payload: tuple       # sorted-tuple form of canonical payload, for hashing

    def __repr__(self) -> str:
        return f"<{self.kind}: {dict(self.payload)}>"


def _to_tuple(d: dict) -> tuple:
    """Sorted-key tuple form of a dict — hashable and equal across orderings."""
    items = []
    for k in sorted(d.keys()):
        v = d[k]
        if isinstance(v, dict):
            items.append((k, _to_tuple(v)))
        elif isinstance(v, list):
            items.append((k, tuple(_to_tuple(x) if isinstance(x, dict) else x for x in v)))
        else:
            items.append((k, v))
    return tuple(items)


@dataclass(frozen=True)
class Snapshot:
    """A run's journal, canonicalized for equality comparison.

    Two snapshots compare equal when the underlying runs recorded the same
    logical history. Timestamps, lease owners, trace IDs and the absolute
    `run_id` are normalized away; the position in the list encodes the seq.
    """
    run_id: str            # the original run_id (for the report — not used in ==)
    lines: tuple           # tuple[JournalLine]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Snapshot):
            return NotImplemented
        return self.lines == other.lines

    def __hash__(self) -> int:
        return hash(self.lines)

    def __len__(self) -> int:
        return len(self.lines)

    def diff(self, other: "Snapshot") -> List[Tuple[int, str, JournalLine, JournalLine]]:
        """Return a per-position diff list. Each entry is
        `(index, kind_op, mine, theirs)` where `kind_op` is `==`, `!=`, `<` (extra
        on the right) or `>` (extra on the left)."""
        out = []
        for i in range(max(len(self.lines), len(other.lines))):
            a = self.lines[i] if i < len(self.lines) else None
            b = other.lines[i] if i < len(other.lines) else None
            if a is None:
                out.append((i, "<", None, b))
            elif b is None:
                out.append((i, ">", a, None))
            elif a == b:
                continue  # only emit divergences for brevity
            else:
                out.append((i, "!=", a, b))
        return out


@dataclass(frozen=True)
class DeepSnapshot:
    """A snapshot that walks the full projection tables, not just the
    journal summaries. Closes the gap noted in `Snapshot`: catches drift
    inside `response_json`, `request_json`, `payload_json`, and the full
    `tape_decisions` / `tape_effects` / `tape_obligations` rows.

    More expensive than `Snapshot` (N+1 round trips: one per decision,
    one per effect's GetEffect for full payload, one ListObligations).
    Use when journal-summary equality has held and you need to confirm
    payload-body determinism."""
    run_id: str
    decisions: tuple        # tuple[canonical decision tuples]
    effects: tuple          # tuple[canonical effect tuples]
    obligations: tuple      # tuple[canonical obligation tuples]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DeepSnapshot):
            return NotImplemented
        return (self.decisions == other.decisions
                and self.effects == other.effects
                and self.obligations == other.obligations)

    def __hash__(self) -> int:
        return hash((self.decisions, self.effects, self.obligations))


def capture_deep(client: TapeClient, run_id: str, *,
                 canonical_run_id: str = "run-1",
                 max_decisions: int = 1_000) -> DeepSnapshot:
    """Capture the run's full projection rows for body-level comparison.
    Walks decisions by index (until `not found`), effects by listing
    pending + walking the journal for confirmed ones, and obligations via
    `list_obligations`."""
    run_id_map = {run_id: canonical_run_id}

    def _canon_dict(d: dict) -> tuple:
        cleaned = _canonical(d, run_id_map)
        if not isinstance(cleaned, dict):
            return (("_value", cleaned),)
        return _to_tuple(cleaned)

    # Decisions: get_decision(i) until not found.
    decisions: list[tuple] = []
    for i in range(max_decisions):
        try:
            got = client.get_decision(run_id=run_id, decision_index=i)
        except Exception:
            break
        if not got.found:
            break
        d = got.decision
        decisions.append(_canon_dict({
            "decision_index": d.decision_index,
            "model": d.model,
            "request_json": d.request_json,
            "response_json": d.response_json,
            "policy_version": d.policy_version,
            "rationale": d.rationale,
        }))

    # Effects: walk the journal once, collecting unique effect keys, then
    # GetEffect each one for the authoritative row.
    effects: list[tuple] = []
    import time
    deadline = time.monotonic() + 3.0
    seen_keys: set[str] = set()
    it = client.subscribe_run(run_id=run_id, from_seq=0)
    try:
        for entry in it:
            if entry.kind == "effect":
                try:
                    p = json.loads(entry.payload_json or "{}")
                except Exception:
                    continue
                key = str(p.get("idempotency_key") or "")
                if key:
                    seen_keys.add(key)
            if entry.kind == "run":
                try:
                    p = json.loads(entry.payload_json or "{}")
                except Exception:
                    p = {}
                if (p.get("status") or "").lower() in {"terminal", "failed", "cancelled", "stuck"}:
                    break
            if time.monotonic() > deadline:
                break
    finally:
        try:
            it.cancel()
        except Exception:
            pass

    for key in sorted(seen_keys):
        try:
            got = client.get_effect(run_id=run_id, idempotency_key=key)
        except Exception:
            continue
        if not got.found:
            continue
        e = got.effect
        effects.append(_canon_dict({
            "tool_name": e.tool_name,
            "idempotency_key": e.idempotency_key,
            "status": e.status,
            "request_json": e.request_json,
            "response_json": e.response_json,
            "error_json": e.error_json,
            "semantics": e.semantics,
            "dispatch_mode": e.dispatch_mode,
            "business_key": e.business_key,
            "connector": e.connector,
            "external_ref": e.external_ref,
            "decision_index": e.decision_index,
        }))

    # Obligations: list_obligations + canonicalise.
    obligations: list[tuple] = []
    try:
        resp = client.list_obligations(run_id=run_id, only_unresolved=False)
        for o in resp.obligations:
            obligations.append(_canon_dict({
                "kind": o.kind,
                "effect_key": o.effect_key,
                "status": o.status,
                "payload_json": o.payload_json,
                "attempts": o.attempts,
                "max_attempts": o.max_attempts,
                "last_error": o.last_error,
                "result_json": o.result_json,
                "compensator_ref": o.compensator_ref,
            }))
    except Exception:
        pass

    return DeepSnapshot(
        run_id=run_id,
        decisions=tuple(decisions),
        effects=tuple(effects),
        obligations=tuple(obligations),
    )


def capture(client: TapeClient, run_id: str, *,
            deadline_s: float = 5.0,
            canonical_run_id: str = "run-1") -> Snapshot:
    """Stream the journal for `run_id` via `SubscribeRun(from=0)` and
    canonicalize each entry. Stops at the deadline OR when a `run` entry
    with a terminal status appears.

    `deadline_s` is a wall-clock cap (the stream is long-lived even after
    the run finishes — the server only ends it on client cancellation).
    `canonical_run_id` is what the original run_id gets remapped to in the
    snapshot — leave it at the default for two-run replay comparison."""
    run_id_map = {run_id: canonical_run_id}
    deadline = time.monotonic() + deadline_s
    lines: list[JournalLine] = []
    terminal_statuses = {"terminal", "failed", "cancelled", "stuck"}

    it = client.subscribe_run(run_id=run_id, from_seq=0)
    try:
        for entry in it:
            try:
                payload = json.loads(entry.payload_json or "{}")
            except Exception:
                payload = {"_raw": entry.payload_json}
            canonical_payload = _canonical(payload, run_id_map)
            if isinstance(canonical_payload, dict):
                tup = _to_tuple(canonical_payload)
            else:
                tup = (("_value", canonical_payload),)
            lines.append(JournalLine(kind=entry.kind, payload=tup))

            # Stop early if we just observed the run reaching a terminal state.
            if entry.kind == "run":
                status = (payload.get("status") or "").lower()
                if status in terminal_statuses:
                    break
            if time.monotonic() > deadline:
                break
    finally:
        try:
            it.cancel()
        except Exception:
            pass

    return Snapshot(run_id=run_id, lines=tuple(lines))


__all__ = ["Snapshot", "JournalLine", "capture", "DeepSnapshot", "capture_deep"]
