"""The outbox reactor — dispatches PENDING + OUTBOX effects through their
registered connectors.

The loop:

    list effects to dispatch (PENDING + OUTBOX + due)
    for each:
        claim (atomic CAS lease)
        look up the connector
        dispatch through it
        record result:
          confirmed     → complete_effect(CONFIRMED) + register compensation
                          (if a compensate kind is registered for the tool)
          failed        → record_dispatch_attempt(next_at_ms=backoff)
                          (eventually exhaust → terminal FAILED via
                           record_external_observation(FAILED))
          unknown       → record_dispatch_attempt(next_at_ms=0)  → status UNKNOWN
                          (the reconciler resolves; do NOT blindly retry —
                           that is the entire safety claim for non-idempotent
                           upstreams)

Safety: the dispatcher refuses to re-dispatch a row whose semantics is
NON_IDEMPOTENT and whose status is anything other than PENDING. The server-side
CAS in claim_effect_dispatch enforces this, but we also assert on the result
record so a bug in store config can't downgrade the guarantee silently.
"""

from __future__ import annotations

import json
import os
import socket
import time
from typing import Any, Callable, Dict, List, Optional

from ..client import (
    TapeClient,
    DEFAULT_URL,
    EFFECT_STATUS_CONFIRMED,
    EFFECT_STATUS_FAILED,
    EFFECT_STATUS_UNKNOWN,
    EFFECT_STATUS_PENDING,
    EFFECT_SEMANTICS_NON_IDEMPOTENT,
    EFFECT_SEMANTICS_IDEMPOTENT,
)
from .. import connectors as _connectors
from ..effect import register_compensator, get_compensator


def _claimer_id() -> str:
    return os.environ.get("TAPE_DISPATCH_CLAIMER",
                          f"{socket.gethostname()}:{os.getpid()}")


def _backoff_ms(attempt: int, *, base_s: float = 1.0, max_s: float = 60.0) -> int:
    """Exponential backoff with a 60s cap, in milliseconds. Used when a
    connector returns `failed` and the outbox loop is going to retry."""
    delay_s = min(base_s * (2 ** max(attempt - 1, 0)), max_s)
    return int(delay_s * 1000)


def dispatch_one(eff, *, client: TapeClient, claimer: str,
                 dispatch_max_attempts: int = 5) -> Dict[str, Any]:
    """Run one effect through its connector. Returns a per-effect outcome dict."""
    out: Dict[str, Any] = {
        "run_id": eff.run_id,
        "idempotency_key": eff.idempotency_key,
        "connector": eff.connector,
        "tool": eff.tool_name,
        "semantics": int(eff.semantics),
    }

    # Look up the connector by name. If it's missing, leave the effect alone
    # (a later process with the connector loaded will pick it up); record the
    # absence so an operator can see it.
    connector = _connectors.get(eff.connector)
    if connector is None:
        out["status"] = "skipped"
        out["reason"] = f"no connector registered: {eff.connector!r}"
        return out

    # Atomic CAS lease. The server returns acquired=False if someone else won.
    claim = client.claim_effect_dispatch(run_id=eff.run_id,
                                         idempotency_key=eff.idempotency_key,
                                         claimer=claimer)
    if not claim.acquired:
        out["status"] = "skipped"
        out["reason"] = "lease contended"
        return out

    # Re-read the effect inside the lease so we use the up-to-date row.
    cur = claim.effect

    # Belt-and-suspenders: refuse to re-dispatch a NON_IDEMPOTENT effect whose
    # status is not PENDING. The server's CAS guarantees this; we double-check.
    if cur.status != EFFECT_STATUS_PENDING:
        out["status"] = "skipped"
        out["reason"] = f"unexpected status after claim: {cur.status}"
        return out

    try:
        result = _connectors.call_dispatch(connector, cur)
    except Exception as ex:
        # Connector-side exception: treat as UNKNOWN for non-idempotent (a
        # second call might land twice), as a retryable failure for idempotent.
        is_non_idem = (cur.semantics == EFFECT_SEMANTICS_NON_IDEMPOTENT)
        if is_non_idem:
            client.record_dispatch_attempt(run_id=cur.run_id,
                                           idempotency_key=cur.idempotency_key,
                                           error=f"connector raised: {type(ex).__name__}: {ex}",
                                           next_dispatch_at_ms=0)
            out["status"] = "unknown"
            out["error"] = str(ex)
            return out
        # idempotent → exponential backoff schedule
        next_at = int(time.time() * 1000) + _backoff_ms(cur.dispatch_attempts + 1)
        client.record_dispatch_attempt(run_id=cur.run_id,
                                       idempotency_key=cur.idempotency_key,
                                       error=f"connector raised: {type(ex).__name__}: {ex}",
                                       next_dispatch_at_ms=next_at)
        out["status"] = "retry-scheduled"
        out["error"] = str(ex)
        return out

    if result.status == "confirmed":
        client.complete_effect(
            run_id=cur.run_id, idempotency_key=cur.idempotency_key,
            status=EFFECT_STATUS_CONFIRMED,
            response_json=json.dumps({"external_ref": result.external_ref,
                                       **(result.response or {})}, default=str))
        # If the tool declared a compensator (via @tape.effect(compensate=...)),
        # register it now that the effect has confirmed — same pattern as the
        # inline path, but driven by the reactor.
        comp = get_compensator(cur.tool_name)
        if comp is not None:
            try:
                kind = getattr(comp, "__name__", "compensate")
                client.register_compensation(
                    run_id=cur.run_id, effect_key=cur.idempotency_key, kind=kind,
                    payload_json=json.dumps({**(result.response or {}),
                                             "external_ref": result.external_ref}, default=str),
                    max_attempts=0)
            except Exception:
                pass
        out["status"] = "confirmed"
        out["external_ref"] = result.external_ref
        return out

    if result.status == "unknown":
        # The whole point of the plan: do not retry blindly. Drive the effect
        # into UNKNOWN and let the reconciler ask the counterparty.
        client.record_dispatch_attempt(
            run_id=cur.run_id, idempotency_key=cur.idempotency_key,
            error=json.dumps(result.error or {"reason": "ack lost"}, default=str),
            next_dispatch_at_ms=0)
        out["status"] = "unknown"
        return out

    # failed
    attempts = cur.dispatch_attempts + 1
    if attempts >= dispatch_max_attempts:
        # Exhausted: mark FAILED via the observation path (preserves the audit
        # entry the reconciler would have written and avoids the half-state).
        client.record_external_observation(
            run_id=cur.run_id, idempotency_key=cur.idempotency_key,
            resolution=__import__('tape').EFFECT_RESOLUTION_FAILED  # type: ignore[attr-defined]
            if hasattr(__import__('tape'), 'EFFECT_RESOLUTION_FAILED')
            else 2,   # EFFECT_RESOLUTION_FAILED
            error_json=json.dumps({"final": True, "attempts": attempts,
                                    "last": result.error}, default=str))
        out["status"] = "failed"
        out["attempts"] = attempts
        return out
    next_at = result.retry_at_ms or (int(time.time() * 1000) + _backoff_ms(attempts))
    client.record_dispatch_attempt(
        run_id=cur.run_id, idempotency_key=cur.idempotency_key,
        error=json.dumps(result.error or {}, default=str),
        next_dispatch_at_ms=next_at)
    out["status"] = "retry-scheduled"
    out["next_at_ms"] = next_at
    out["attempts"] = attempts
    return out


def outbox_dispatch_once(url: str = DEFAULT_URL, *, connector: str = "",
                         limit: int = 200, claimer: str = "",
                         dispatch_max_attempts: int = 5,
                         client: Optional[TapeClient] = None) -> List[Dict[str, Any]]:
    """One pass of the outbox dispatcher. Returns a list of per-effect outcomes."""
    c = client or TapeClient(url)
    claimer = claimer or _claimer_id()
    out: List[Dict[str, Any]] = []
    try:
        effects = c.list_effects_to_dispatch(connector=connector, limit=limit).effects
        for e in effects:
            try:
                out.append(dispatch_one(e, client=c, claimer=claimer,
                                        dispatch_max_attempts=dispatch_max_attempts))
            except Exception as ex:  # noqa: BLE001
                out.append({"run_id": e.run_id, "idempotency_key": e.idempotency_key,
                            "status": "error", "error": str(ex)})
    finally:
        if client is None:
            c.close()
    return out


def run_outbox_dispatcher(url: str = DEFAULT_URL, *, connector: str = "",
                          interval_s: float = 1.0, claimer: str = "",
                          dispatch_max_attempts: int = 5,
                          once: bool = False,
                          on_tick: Optional[Callable[[List[Dict[str, Any]]], None]] = None) -> None:
    """Run the outbox dispatcher forever (or once). Connectors must be
    registered (via `tape.connectors.register(...)`) before this loop starts;
    a missing connector causes the effect to be left in PENDING for the next
    process that does have it."""
    c = TapeClient(url)
    try:
        while True:
            try:
                results = outbox_dispatch_once(url, connector=connector,
                                               claimer=claimer or _claimer_id(),
                                               dispatch_max_attempts=dispatch_max_attempts,
                                               client=c)
            except Exception as ex:  # noqa: BLE001
                import sys
                print(f"[tape outbox] tick error: {ex}", file=sys.stderr, flush=True)
                results = []
            if on_tick is not None:
                on_tick(results)
            if once:
                return
            time.sleep(interval_s)
    finally:
        c.close()


# ── CLI ─────────────────────────────────────────────────────────────────────

def _main(argv=None) -> int:
    import argparse, importlib
    p = argparse.ArgumentParser(prog="tape.reactors.outbox",
                                description="Run Tape's outbox dispatcher.")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--connector", default="",
                   help="restrict to one connector name (matches @tape.effect(connector=...))")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--max-attempts", type=int, default=5,
                   help="give up on a connector failure after N attempts (then mark the effect FAILED)")
    p.add_argument("--claimer", default="",
                   help="identity recorded as `dispatch_claimed_by`")
    p.add_argument("--load", action="append", default=[],
                   help="module:attr to import at startup so its connectors register")
    p.add_argument("--once", action="store_true")
    args = p.parse_args(argv)

    for spec in args.load:
        mod_name, _, attr = spec.partition(":")
        try:
            obj = importlib.import_module(mod_name)
            if attr:
                getattr(obj, attr, None)
        except Exception as ex:  # noqa: BLE001
            import sys
            print(f"[tape] --load {spec}: {ex}", file=sys.stderr, flush=True)

    run_outbox_dispatcher(args.url, connector=args.connector, interval_s=args.interval,
                          claimer=args.claimer, dispatch_max_attempts=args.max_attempts,
                          once=args.once,
                          on_tick=lambda r: print(json.dumps({"outbox": r}, default=str)))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
