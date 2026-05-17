"""Reactors — the WAL-driven side of Tape.

A *reactor* watches the journal and reacts. Four ship in the box, and they're
all idempotent (the lease + replay properties make a double-run harmless), so you
run as many copies as you like behind a load balancer:

  * the **recovery reactor** — re-drives RUNNABLE runs, RUNNING runs whose lease
    is stale, and WAITING runs whose gate was signalled (`recover_once`);
  * the **reconciler reactor** — for every UNKNOWN effect (and, optionally, every
    long-PENDING one), calls the per-tool status check registered via
    `@tape.effect(status_check=...)` and resolves the effect to CONFIRMED or
    FAILED (`reconcile_once`);
  * the **obligations reactor** — drains ready-to-run PENDING obligations and
    reclaims COMMITTED rows whose lease has expired, with bounded retry/backoff
    via the drainer state machine (`compensate_once` for polling,
    `run_compensations_event_driven` for the SubscribeEvents stream);
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
from ._recover import resume as _resume, compensate_one as _compensate_one, _compensator_id
from ._gen import tape_pb2 as pb


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

def fire_due_timers_once(url: str = DEFAULT_URL, *, runner: Any = None, redrive_fn: Any = None,
                         on_timer: Optional[Callable[[Any], None]] = None,
                         client: Optional[TapeClient] = None) -> list[dict]:
    """Claim and fire due timers. Returns a list of {run_id, timer_id, kind, action}.
    `redrive_fn` (e.g. one that calls the Agent Engine `:streamQuery` API) is used
    for `redrive` timers when there's no local `runner`."""
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
            elif t.kind == "redrive" and (runner is not None or redrive_fn is not None):
                run = c.get_run(t.run_id)
                _resume(run.invocation_id, runner=runner, redrive_fn=redrive_fn,
                        user_id=run.user_id, session_id=run.session_id)
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


# ── the obligations reactor ─────────────────────────────────────────────────

def compensate_once(url: str = DEFAULT_URL, *, claimer: str = "", limit: int = 200,
                    include_committed_expired: bool = True,
                    client: Optional[TapeClient] = None) -> list[dict]:
    """Drain one batch of due obligations across all runs. Picks up ready-to-run
    PENDING (with `next_attempt_at_ms <= now`) and — by default — COMMITTED rows
    whose lease has expired (a previous drainer crashed mid-act). Returns a list
    of per-obligation outcome dicts (see `compensate_one`)."""
    c = client or TapeClient(url)
    out: list[dict] = []
    claimer = claimer or _compensator_id()
    try:
        rows = c.list_unresolved_obligations(
            limit=limit, include_pending=True, include_stuck=False,
            include_committed_expired=include_committed_expired).obligations
        for ob in rows:
            r = _compensate_one(ob, client=c, claimer=claimer)
            r.update({"run_id": ob.run_id, "obligation_seq": ob.seq})
            out.append(r)
    finally:
        if client is None:
            c.close()
    return out


def run_compensations_event_driven(url: str = DEFAULT_URL, *, claimer: str = "",
                                    from_ts_ms: int = 0, idle_window_s: float = 0.5,
                                    catchup: bool = True,
                                    on_event: Optional[Callable[[dict], None]] = None) -> None:
    """Subscribe to the `kind="obligation"` event stream and drain on every
    transition that puts work in the queue (registered, retry-scheduled). On
    each event we don't trust the payload to point us at the right row — we
    just call `compensate_once`, which sees everything ready *now*. That keeps
    correctness in one place (the drainer state machine in the server) and
    makes the reactor immune to lost / duplicate / reordered events.

    Set `catchup=True` (default) to drain whatever's already queued before
    blocking on the stream — important when the reactor starts after a backlog
    has built up. `from_ts_ms=0` means "from the head of the stream forward."

    This is the event-driven path. For a polling-only deployment (no
    SubscribeEvents in your store, e.g. Bigtable in some configurations), call
    `compensate_once` on a timer instead — both close the same queue, with
    different latency characteristics.
    """
    import grpc as _grpc
    c = TapeClient(url)
    try:
        if catchup:
            for r in compensate_once(url, claimer=claimer, client=c):
                if on_event is not None:
                    on_event(r)
        while True:
            req = pb.SubscribeEventsRequest(from_ts_ms=from_ts_ms, run_id="", kind="obligation")
            stream = c.stub.SubscribeEvents(req, timeout=idle_window_s)
            saw_event = False
            try:
                for entry in stream:
                    saw_event = True
                    from_ts_ms = max(from_ts_ms, entry.ts_ms)
                    # Don't drain on every single entry — that's a thundering herd
                    # if many obligations transition at once. The "saw any event"
                    # signal is enough; we drain the whole queue below.
                    try:
                        kind_label = ""
                        if entry.payload_json:
                            import json as _json
                            kind_label = _json.loads(entry.payload_json).get("transition", "")
                    except Exception:
                        kind_label = ""
                    if on_event is not None:
                        on_event({"event": "obligation", "transition": kind_label, "ts_ms": entry.ts_ms})
            except _grpc.RpcError as ex:
                code = getattr(ex, "code", lambda: None)()
                if code not in (_grpc.StatusCode.DEADLINE_EXCEEDED, _grpc.StatusCode.CANCELLED):
                    raise
            finally:
                try:
                    stream.cancel()
                except Exception:
                    pass
            if saw_event:
                # One event in the window → drain. The drainer is idempotent
                # on (run_id, obligation_seq) (claim is a CAS), so concurrent
                # reactors are safe.
                for r in compensate_once(url, claimer=claimer, client=c):
                    if on_event is not None:
                        on_event(r)
            # Else: empty window. Loop back to SubscribeEvents (the per-call
            # timeout is the heartbeat — we don't add a sleep on top of it).
    finally:
        c.close()


# ── recovery (re-exported from _recover for one-stop-shopping) ───────────────

from ._recover import recover_once  # noqa: E402,F401


# ── the loop ────────────────────────────────────────────────────────────────

def run_reactors(*, runner: Any = None, redrive_fn: Any = None, url: str = DEFAULT_URL,
                 recover: bool = True, reconcile: bool = True, timers: bool = True,
                 compensations: bool = True,
                 interval_s: float = 2.0, reconcile_pending_after_s: float = 0.0,
                 claimer: str = "",
                 once: bool = False, on_tick: Optional[Callable[[dict], None]] = None) -> None:
    """Loop: each tick, run the enabled reactors. Returns after one tick if `once`.
    Pass `runner=` for a local ADK Runner, or `redrive_fn=` (e.g. one that calls
    the Vertex AI Agent Engine `:streamQuery` API) when the agent is deployed
    elsewhere — recovery still works, it just re-invokes through that callback.

    `compensations=True` enables the obligations reactor on the same polling
    cadence as the others. For event-driven draining (lower latency, less
    polling pressure), call `run_compensations_event_driven` in its own
    process / thread instead — both close the same queue."""
    client = TapeClient(url)
    can_redrive = runner is not None or redrive_fn is not None
    try:
        while True:
            tick: dict = {}
            if recover and can_redrive:
                try:
                    tick["recovered"] = recover_once(runner=runner, redrive_fn=redrive_fn, url=url)
                except Exception as ex:  # noqa: BLE001
                    tick["recover_error"] = str(ex)
            if reconcile:
                try:
                    tick["reconciled"] = reconcile_once(url, reconcile_pending_after_ms=int(reconcile_pending_after_s * 1000), client=client)
                except Exception as ex:  # noqa: BLE001
                    tick["reconcile_error"] = str(ex)
            if timers:
                try:
                    tick["timers_fired"] = fire_due_timers_once(url, runner=runner, redrive_fn=redrive_fn, client=client)
                except Exception as ex:  # noqa: BLE001
                    tick["timer_error"] = str(ex)
            if compensations:
                try:
                    tick["compensations"] = compensate_once(url, claimer=claimer, client=client)
                except Exception as ex:  # noqa: BLE001
                    tick["compensation_error"] = str(ex)
            if on_tick is not None:
                on_tick(tick)
            if once:
                return
            time.sleep(interval_s)
    finally:
        client.close()


# ── outbox relay (durable cursor; the WAL → external-system bridge) ────────
#
# Cursor format: `{"last_global_seq": N}` — a single integer over the new
# monotonic `tape_journal.global_seq` column (see design-principles/
# tape-event-bus.md). The legacy `{"from_ts_ms", "last_run_id", "last_seq"}`
# shape is detected on read and one-shot-migrated to `{"last_global_seq": 0}`
# (i.e. re-read from the start), which is the correct conservative choice:
# the relay's at-least-once contract already requires the downstream sink to
# dedup on (run_id, seq), so a re-read is harmless.

import logging as _logging

_log = _logging.getLogger("tape.reactors.outbox")


def _read_cursor(path):
    """Returns `last_global_seq` (int). If the file is in the legacy format
    (`from_ts_ms` / `last_run_id` / `last_seq`), migrates it to the new shape
    by resetting to 0 and logging a warning — the downstream sink is expected
    to be idempotent, so a one-time re-read is harmless."""
    import os, json as _json
    if not path or not os.path.exists(path):
        return 0
    try:
        d = _json.load(open(path))
    except Exception:
        return 0
    if "last_global_seq" in d:
        try:
            return int(d.get("last_global_seq", 0))
        except Exception:
            return 0
    # Legacy cursor: convert + warn.
    if any(k in d for k in ("from_ts_ms", "last_run_id", "last_seq")):
        _log.warning(
            "tape outbox: legacy cursor at %s (keys=%s); migrating to "
            "{last_global_seq:0} and re-reading from the start (downstream "
            "sink should dedup on (run_id, seq))",
            path, sorted(d.keys()))
        _write_cursor(path, 0)
        return 0
    return 0


def _write_cursor(path, last_global_seq):
    import os, json as _json
    if not path:
        return
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        _json.dump({"last_global_seq": int(last_global_seq)}, f)
    os.replace(tmp, path)


def outbox_relay_tick(url: str, sink: Any, *, cursor_path: str = "",
                     run_id: str = "", kind: str = "",
                     subject_pattern: str = "", predicate_cel: str = "",
                     batch_limit: int = 512,
                     client: Optional[TapeClient] = None,
                     idle_window_s: float = 0.5) -> int:
    """One pass of the relay. Reads journal entries since the cursor via
    `SubscribeBySubject` (cursored by `global_seq`), calls `sink.publish(entry)`
    for each, advances the cursor. Returns the number of entries published.
    Subject filter (`subject_pattern`, default `/tape/**`) + optional CEL
    predicate are evaluated server-side. The cursor is monotonic on
    `global_seq`, so dedup is just `entry.global_seq > last_global_seq`.

    `run_id` / `kind` are retained for back-compat: when set, they're filtered
    client-side after the subject-routed stream — preferable to translating
    them into subject patterns because the canonical subject grammar is the
    authoritative filter."""
    c = client or TapeClient(url)
    try:
        last_seq = _read_cursor(cursor_path)
        from ._gen import tape_pb2 as _pb
        pat = subject_pattern or "/tape/**"
        # Per-call timeout: the server-side stream waits for more entries.
        # A short deadline surfaces DEADLINE_EXCEEDED when the server pauses
        # (no more entries) — our signal to stop the batch.
        import grpc as _grpc
        stream = c.subscribe_by_subject(
            subject_pattern=pat, predicate_cel=predicate_cel,
            from_global_seq=last_seq, timeout=idle_window_s)
        n = 0
        try:
            for entry in stream:
                # Skip the boundary entry that the server may re-emit at the
                # cursor.
                if entry.global_seq <= last_seq:
                    continue
                if run_id and entry.run_id != run_id:
                    continue
                if kind and entry.kind != kind:
                    continue
                sink.publish(entry)
                last_seq = entry.global_seq
                n += 1
                if n >= batch_limit:
                    break
        except _grpc.RpcError as e:
            # DEADLINE_EXCEEDED is the normal "no more entries in this window" exit;
            # CANCELLED can show up after stream.cancel(). Anything else propagates.
            code = getattr(e, "code", lambda: None)()
            if code not in (_grpc.StatusCode.DEADLINE_EXCEEDED, _grpc.StatusCode.CANCELLED):
                raise
        finally:
            try:
                stream.cancel()
            except Exception:
                pass
        if n:
            _write_cursor(cursor_path, last_seq)
        return n
    finally:
        if client is None:
            c.close()


def run_outbox_relay(url: str, sink: Any, *, cursor_path: str = "", run_id: str = "",
                    kind: str = "", subject_pattern: str = "", predicate_cel: str = "",
                    interval_s: float = 1.0, once: bool = False) -> None:
    """Loop `outbox_relay_tick` forever (or once). The cursor is durable in
    `cursor_path` (a local JSON file), so a relay restart resumes from where it
    stopped. Run multiple relays = multiple sinks (one cursor file each).

    `subject_pattern` (default `/tape/**`) and `predicate_cel` (default empty)
    are forwarded to `SubscribeBySubject` so the server does the filtering."""
    c = TapeClient(url)
    try:
        while True:
            try:
                outbox_relay_tick(url, sink, cursor_path=cursor_path, run_id=run_id,
                                  kind=kind, subject_pattern=subject_pattern,
                                  predicate_cel=predicate_cel, client=c)
            except Exception as ex:  # noqa: BLE001
                import sys
                print(f"[tape outbox] tick error: {ex}", file=sys.stderr, flush=True)
            if once:
                return
            time.sleep(interval_s)
    finally:
        c.close()
        try:
            sink.close()
        except Exception:
            pass


# ── the WAL fan-out (inline callback; for one-process consumers) ───────────

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

    p = argparse.ArgumentParser(prog="tape.reactors", description="Run Tape's reactors (recovery, reconciler, timers, compensations).")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--runner-from", help="module:attr that yields an ADK Runner (a callable factory, or the Runner itself)")
    p.add_argument("--no-recover", action="store_true")
    p.add_argument("--no-reconcile", action="store_true")
    p.add_argument("--no-timers", action="store_true")
    p.add_argument("--no-compensations", action="store_true")
    p.add_argument("--reconcile-pending-after", type=float, default=0.0, help="also reconcile PENDING effects older than N seconds")
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--claimer", default="", help="identity recorded as `claimed_by` on obligations this reactor drains")
    p.add_argument("--once", action="store_true")
    p.add_argument("--compensations-only", action="store_true",
                   help="run the event-driven obligations reactor only (subscribes to kind=\"obligation\" — no polling)")
    p.add_argument("--load", action="append", default=[],
                   help="module:attr to import at startup so its @tape.effect compensators register in this process (repeat to load several)")
    args = p.parse_args(argv)

    # Optional eager loads — make sure @tape.effect-registered compensators are
    # in this process's registry before the drainer claims anything.
    for spec in args.load:
        mod_name, _, attr = spec.partition(":")
        try:
            obj = importlib.import_module(mod_name)
            if attr:
                getattr(obj, attr, None)  # touch the attr if present
        except Exception as ex:  # noqa: BLE001
            print(f"[tape] --load {spec}: {ex}", file=__import__('sys').stderr, flush=True)

    runner = None
    if args.runner_from:
        mod_name, _, attr = args.runner_from.partition(":")
        obj = getattr(importlib.import_module(mod_name), attr or "runner")
        runner = obj() if callable(obj) else obj

    if args.compensations_only:
        run_compensations_event_driven(args.url, claimer=args.claimer,
                                       on_event=lambda r: print(json.dumps(r, default=str)))
        return 0

    run_reactors(runner=runner, url=args.url, recover=not args.no_recover, reconcile=not args.no_reconcile,
                 timers=not args.no_timers, compensations=not args.no_compensations,
                 interval_s=args.interval, reconcile_pending_after_s=args.reconcile_pending_after,
                 claimer=args.claimer, once=args.once,
                 on_tick=lambda t: print(json.dumps({"tick": t}, default=str)))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
