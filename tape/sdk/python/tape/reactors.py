"""Reactors — the WAL-driven side of Tape.

A *reactor* watches the journal and reacts. Three ship in the box, and they're
all idempotent (the lease + replay properties make a double-run harmless), so you
run as many copies as you like behind a load balancer:

  * the **recovery reactor** — re-drives RUNNABLE runs, RUNNING runs whose lease
    is stale, and WAITING runs whose gate was signalled (`recover_once`);
  * the **reconciler reactor** — for every UNKNOWN effect (and, optionally, every
    long-PENDING one), calls the per-tool status check registered via
    `@tape.effect(status_check=...)` and resolves the effect to CONFIRMED or
    FAILED (`reconcile_once`);
  * the **timer reactor** — fires due timers: `gate_timeout` (release a parked
    run with a timeout resolution), `redrive` (re-invoke a run), `reconcile`
    (resolve a specific effect), or your own kinds via a callback
    (`fire_due_timers_once`).

`run_reactors(runner=..., url=...)` loops over all three. The recovery and timer
`redrive` actions need the agent itself (to call `runner.run_async`), so a
reactor process is an agent process with the SDK — typically a small sidecar:

    # reactors.py
    from my_app import build_runner
    import tape.reactors
    tape.reactors.run_reactors(runner=build_runner(), url="tape://tape:7878")

or `python -m tape.reactors --runner-from my_app:build_runner --url tape://...`.

Separately, `run_event_fanout(url, sink)` tails the cross-run journal
(`SubscribeEvents`) and hands each entry to `sink` — wire that to Pub/Sub /
Kafka / a webhook to publish the WAL. (On the Bigtable backend the cross-run tail
is "use Bigtable change streams" — see design-principles/tape.md §12 — so this
function yields nothing there; the per-run `SubscribeRun` still works.)
"""

from __future__ import annotations

import json
import time
from typing import Any, Callable, Optional

from .client import (
    TapeClient,
    DEFAULT_URL,
    EFFECT_STATUS_CONFIRMED,
    EFFECT_STATUS_FAILED,
    EFFECT_STATUS_UNKNOWN,
    EFFECT_STATUS_PENDING,
    RUN_STATUS_TERMINAL,
)
from .effect import get_status_check
from ._recover import resume as _resume


# ── the reconciler reactor ──────────────────────────────────────────────────

def reconcile_once(url: str = DEFAULT_URL, *, reconcile_pending_after_ms: int = 0,
                   client: Optional[TapeClient] = None) -> list[dict]:
    """Resolve UNKNOWN effects (and PENDING ones older than `reconcile_pending_after_ms`,
    if > 0) via the registered status checks. Returns a list of {key, resolved}."""
    c = client or TapeClient(url)
    out: list[dict] = []
    include_pending = reconcile_pending_after_ms > 0
    older = (int(time.time() * 1000) - reconcile_pending_after_ms) if include_pending else 0
    effects = c.list_pending_effects(older_than_ms=older, include_pending=include_pending,
                                     include_unknown=True, limit=500).effects
    for e in effects:
        check = get_status_check(e.tool_name)
        if check is None:
            continue  # no status check for this tool — leave it for a human / the re-drive
        try:
            res = check(e.idempotency_key)
        except Exception as ex:  # noqa: BLE001
            out.append({"key": e.idempotency_key, "resolved": f"check-error: {ex}"})
            continue
        found = (res.get("found", True) if isinstance(res, dict) else bool(res))
        if found:
            c.reconcile_effect(run_id=e.run_id, idempotency_key=e.idempotency_key,
                               resolved_status=EFFECT_STATUS_CONFIRMED,
                               response_json=json.dumps(res, default=str) if isinstance(res, dict) else "{}")
            out.append({"key": e.idempotency_key, "resolved": "confirmed"})
        elif e.status == EFFECT_STATUS_UNKNOWN:
            # The counterparty says it definitively didn't land — and the run is
            # done (UNKNOWN only persists past run end), so mark it FAILED.
            c.reconcile_effect(run_id=e.run_id, idempotency_key=e.idempotency_key,
                               resolved_status=EFFECT_STATUS_FAILED,
                               error_json=json.dumps({"reconciled": "absent at counterparty"}))
            out.append({"key": e.idempotency_key, "resolved": "failed"})
        # PENDING + not-found: leave it — the recovery re-drive will re-attempt it.
    if client is None:
        c.close()
    return out


# ── the timer reactor ───────────────────────────────────────────────────────

def fire_due_timers_once(url: str = DEFAULT_URL, *, runner: Any = None,
                         on_timer: Optional[Callable[[Any], None]] = None,
                         client: Optional[TapeClient] = None) -> list[dict]:
    """Claim and fire due timers. Returns a list of {run_id, timer_id, kind, action}."""
    c = client or TapeClient(url)
    out: list[dict] = []
    for t in c.list_due_timers(claim=True, limit=500).timers:
        payload = {}
        try:
            payload = json.loads(t.payload_json) if t.payload_json else {}
        except Exception:
            payload = {}
        action = "ignored"
        try:
            if t.kind == "gate_timeout":
                gate = payload.get("gate", "")
                resolution = {"timed_out": True, **(payload.get("resolution") or {})}
                c.send_signal(run_id=t.run_id, gate_name=gate, resolution_json=json.dumps(resolution))
                action = f"signalled {gate} (timeout)"
            elif t.kind == "redrive" and runner is not None:
                run = c.get_run(t.run_id)
                _resume(run.invocation_id, runner=runner, user_id=run.user_id, session_id=run.session_id)
                action = "re-driven"
            elif t.kind == "reconcile":
                key = payload.get("key", "")
                eff = c.get_effect(run_id=t.run_id, idempotency_key=key)
                if eff.found and eff.effect.status in (EFFECT_STATUS_PENDING, EFFECT_STATUS_UNKNOWN):
                    check = get_status_check(eff.effect.tool_name)
                    if check is not None:
                        res = check(key)
                        found = (res.get("found", True) if isinstance(res, dict) else bool(res))
                        c.reconcile_effect(run_id=t.run_id, idempotency_key=key,
                                           resolved_status=EFFECT_STATUS_CONFIRMED if found else EFFECT_STATUS_FAILED,
                                           response_json=json.dumps(res, default=str) if (found and isinstance(res, dict)) else "{}")
                        action = f"reconciled {key} -> {'confirmed' if found else 'failed'}"
            elif on_timer is not None:
                on_timer(t)
                action = "delegated"
        except Exception as ex:  # noqa: BLE001
            action = f"error: {ex}"
        out.append({"run_id": t.run_id, "timer_id": t.timer_id, "kind": t.kind, "action": action})
    if client is None:
        c.close()
    return out


# ── recovery (re-exported from _recover for one-stop-shopping) ───────────────

from ._recover import recover_once  # noqa: E402,F401


# ── the loop ────────────────────────────────────────────────────────────────

def run_reactors(*, runner: Any = None, url: str = DEFAULT_URL, recover: bool = True,
                 reconcile: bool = True, timers: bool = True, interval_s: float = 2.0,
                 reconcile_pending_after_s: float = 0.0, once: bool = False,
                 on_tick: Optional[Callable[[dict], None]] = None) -> None:
    """Loop: each tick, run the enabled reactors. Returns after one tick if `once`."""
    client = TapeClient(url)
    try:
        while True:
            tick: dict = {}
            if recover and runner is not None:
                try:
                    tick["recovered"] = recover_once(runner=runner, url=url)
                except Exception as ex:  # noqa: BLE001
                    tick["recover_error"] = str(ex)
            if reconcile:
                try:
                    tick["reconciled"] = reconcile_once(url, reconcile_pending_after_ms=int(reconcile_pending_after_s * 1000), client=client)
                except Exception as ex:  # noqa: BLE001
                    tick["reconcile_error"] = str(ex)
            if timers:
                try:
                    tick["timers_fired"] = fire_due_timers_once(url, runner=runner, client=client)
                except Exception as ex:  # noqa: BLE001
                    tick["timer_error"] = str(ex)
            if on_tick is not None:
                on_tick(tick)
            if once:
                return
            time.sleep(interval_s)
    finally:
        client.close()


# ── the WAL fan-out ─────────────────────────────────────────────────────────

def run_event_fanout(url: str = DEFAULT_URL, *, sink: Callable[[Any], None],
                     from_ts_ms: int = 0, run_id: str = "", kind: str = "") -> None:
    """Tail the cross-run journal and hand each EventEntry to `sink` — wire `sink`
    to Pub/Sub / Kafka / a webhook to publish the WAL. Blocks; at-least-once
    (the boundary entry may repeat — `sink` should de-dup on (run_id, seq) if it
    cares). On the Bigtable backend a cross-run tail isn't available — use Bigtable
    change streams instead (the stream simply yields nothing there)."""
    c = TapeClient(url)
    try:
        for entry in c.subscribe_events(from_ts_ms=from_ts_ms, run_id=run_id, kind=kind):
            sink(entry)
    finally:
        c.close()


# ── CLI ─────────────────────────────────────────────────────────────────────

def _main(argv=None) -> int:
    import argparse
    import importlib

    p = argparse.ArgumentParser(prog="tape.reactors", description="Run Tape's reactors (recovery, reconciler, timers).")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--runner-from", help="module:attr that yields an ADK Runner (a callable factory, or the Runner itself)")
    p.add_argument("--no-recover", action="store_true")
    p.add_argument("--no-reconcile", action="store_true")
    p.add_argument("--no-timers", action="store_true")
    p.add_argument("--reconcile-pending-after", type=float, default=0.0, help="also reconcile PENDING effects older than N seconds")
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--once", action="store_true")
    args = p.parse_args(argv)

    runner = None
    if args.runner_from:
        mod_name, _, attr = args.runner_from.partition(":")
        obj = getattr(importlib.import_module(mod_name), attr or "runner")
        runner = obj() if callable(obj) else obj

    run_reactors(runner=runner, url=args.url, recover=not args.no_recover, reconcile=not args.no_reconcile,
                 timers=not args.no_timers, interval_s=args.interval,
                 reconcile_pending_after_s=args.reconcile_pending_after, once=args.once,
                 on_tick=lambda t: print(json.dumps({"tick": t}, default=str)))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
