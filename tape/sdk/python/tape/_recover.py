"""The recovery loop, as a library function.

`tape.recover_once(runner=...)` finds every run that needs re-driving — RUNNABLE,
or RUNNING with a stale lease, or WAITING with a delivered-but-unconsumed signal —
and re-invokes it via `runner.run_async(..., invocation_id=...)`. ADK re-drives
the agent; `TapePlugin` replays the recorded decisions and short-circuits the
confirmed effects; the run reconstructs and continues, once.

`tape.compensate_run(run_id)` walks a run's obligations newest-first and runs
each registered inverse, marking each `compensated` (or `stuck` + raising if one
fails).
"""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import socket
import time
from typing import Any, Callable, Optional

from .client import TapeClient, DEFAULT_URL, RUN_STATUS_TERMINAL, RUN_STATUS_FAILED, RUN_STATUS_STUCK
from .effect import get_compensator
from ._gen import tape_pb2 as pb


# ── compensator resolution (in-process registry first, then compensator_ref) ───

def _resolve_compensator(ob: Any) -> Optional[Callable]:
    """Try the in-process registry by `kind`; fall back to `compensator_ref` —
    "module:attr" — for generic drainer processes that don't import the agent's
    module at boot. This is the registry-portability fix: a worker that runs
    `python -m tape.reactors --drain-compensations` can resolve `treasury.agent:
    reverse_wire` on demand, without the user wiring a runner factory."""
    fn = get_compensator(ob.kind)
    if fn is not None:
        return fn
    ref = getattr(ob, "compensator_ref", "") or ""
    if not ref or ":" not in ref:
        return None
    mod, _, attr = ref.partition(":")
    try:
        m = importlib.import_module(mod)
    except Exception:
        return None
    # qualname can be "Class.method" — walk the path
    obj: Any = m
    for part in attr.split("."):
        obj = getattr(obj, part, None)
        if obj is None:
            return None
    return obj if callable(obj) else None


def _compensator_id() -> str:
    """A stable-ish id for the drainer process (used as the claim's `claimed_by`)."""
    return os.environ.get("TAPE_CLAIMER") or f"{socket.gethostname()}/{os.getpid()}"


# ── retry backoff for the drainer's own attempts ──────────────────────────────

def _backoff_ms(attempts: int, *, base_ms: int = 1_000, cap_ms: int = 5 * 60_000) -> int:
    """Exponential backoff with a sane cap: 1s, 2s, 4s, 8s, ... up to `cap_ms`."""
    delay = base_ms * (1 << max(0, attempts - 1))
    return min(delay, cap_ms)


def _drain(agen) -> list:
    async def _run():
        out = []
        async for ev in agen:
            out.append(ev)
        return out

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Caller is already in an event loop; spin a fresh one in a thread.
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                return ex.submit(lambda: asyncio.new_event_loop().run_until_complete(_run())).result()
    except RuntimeError:
        pass
    return asyncio.run(_run())


class _Run:
    """A minimal RunState-shaped object handed to a `redrive_fn`."""
    def __init__(self, run_id="", app_name="", user_id="", session_id="", invocation_id=""):
        self.run_id, self.app_name = run_id, app_name
        self.user_id, self.session_id, self.invocation_id = user_id, session_id, invocation_id


def _redrive(run: Any, *, runner: Any = None, redrive_fn: Any = None) -> None:
    """Re-invoke a run. With `redrive_fn` (e.g. one that calls the Vertex AI
    Agent Engine `:streamQuery` API), `redrive_fn(run)` is called; otherwise the
    local `runner.run_async(invocation_id=…)` path is used (the run finishes /
    re-suspends as it goes)."""
    if redrive_fn is not None:
        redrive_fn(run)
        return
    if runner is None:
        raise ValueError("recover/redrive needs either `runner=` (a local ADK Runner) or `redrive_fn=`")
    _drain(runner.run_async(user_id=run.user_id, session_id=run.session_id, invocation_id=run.invocation_id))


def resume(invocation_id: str, *, runner: Any = None, redrive_fn: Any = None,
           user_id: str = "", session_id: str = "") -> None:
    """Re-invoke one crashed/parked run by its ADK invocation_id."""
    _redrive(_Run(invocation_id=invocation_id, user_id=user_id, session_id=session_id),
             runner=runner, redrive_fn=redrive_fn)


def recover_once(*, runner: Any = None, redrive_fn: Any = None, url: str = DEFAULT_URL,
                 limit: int = 50) -> list[dict]:
    """Re-drive every recoverable run. Returns a list of {run_id, invocation_id}."""
    c = TapeClient(url)
    try:
        runs = c.list_runs_to_recover(limit=limit).runs
    finally:
        c.close()
    out = []
    for r in runs:
        _redrive(r, runner=runner, redrive_fn=redrive_fn)
        out.append({"run_id": r.run_id, "invocation_id": r.invocation_id})
    return out


def compensate_one(ob: Any, *, client: TapeClient, claimer: str = "",
                   lease_ttl_ms: int = 60_000) -> dict:
    """Drain a single obligation: claim → execute the inverse → record the
    outcome. Used by both `compensate_run` (the synchronous all-at-once path)
    and the obligations reactor (the event-driven drain).

    Returns one of:
      {"outcome": "skipped",    "reason": "claim-lost"}      — another drainer won
      {"outcome": "skipped",    "reason": "missing"}          — obligation vanished
      {"outcome": "compensated","kind": ..., "result": ...}   — inverse succeeded
      {"outcome": "scheduled",  "kind": ..., "attempts": N,
                                "next_attempt_at_ms": ms}     — retry queued
      {"outcome": "stuck",      "kind": ..., "attempts": N,
                                "error": str}                 — terminal STUCK

    The drainer's loop never raises on a single obligation's failure — STUCK is
    a state, not an exception. The caller decides whether to escalate.
    """
    claimer = claimer or _compensator_id()
    cl = client.claim_obligation(run_id=ob.run_id, obligation_seq=ob.seq,
                                 claimer=claimer, lease_ttl_ms=lease_ttl_ms)
    if not cl.acquired:
        # Either someone else claimed it, or it's no longer claimable (already
        # COMPENSATED/STUCK). Either way, this drainer is done with it.
        return {"outcome": "skipped", "reason": "claim-lost",
                "current_status": (cl.obligation.status if cl.obligation else 0)}

    current = cl.obligation
    if current is None:
        return {"outcome": "skipped", "reason": "missing"}
    fn = _resolve_compensator(current)
    if fn is None:
        # No inverse available *anywhere* — this is a permanent failure of the
        # registration; mark STUCK immediately so an operator notices, instead
        # of looping forever on a missing compensator.
        client.resolve_obligation(run_id=current.run_id, obligation_seq=current.seq,
                                  status=pb.OBLIGATION_STATUS_STUCK,
                                  result_json=json.dumps({"error":
                                      f"no compensator registered for kind={current.kind!r} (compensator_ref={current.compensator_ref!r})"}))
        return {"outcome": "stuck", "kind": current.kind, "attempts": current.attempts,
                "error": "no-compensator"}

    payload: dict = {}
    try:
        payload = json.loads(current.payload_json) if current.payload_json else {}
    except Exception:
        payload = {}

    try:
        result = fn(**payload) if isinstance(payload, dict) else fn(payload)
    except Exception as ex:
        # The inverse failed. Bump the attempt counter and let the server decide
        # whether to schedule a retry (PENDING + backoff) or terminally STUCK.
        next_attempts = current.attempts + 1
        if next_attempts >= max(1, current.max_attempts):
            next_at = 0  # tell the server "this is terminal"
        else:
            next_at = int(time.time() * 1000) + _backoff_ms(next_attempts)
        updated = client.record_obligation_attempt(
            run_id=current.run_id, obligation_seq=current.seq,
            error=str(ex), next_attempt_at_ms=next_at)
        outcome = "stuck" if updated.status == pb.OBLIGATION_STATUS_STUCK else "scheduled"
        return {"outcome": outcome, "kind": current.kind, "attempts": updated.attempts,
                "error": str(ex), "next_attempt_at_ms": updated.next_attempt_at_ms}

    serialised = json.dumps(result if isinstance(result, (dict, list, str, int, float, bool)) or result is None else str(result))
    client.resolve_obligation(run_id=current.run_id, obligation_seq=current.seq,
                              status=pb.OBLIGATION_STATUS_COMPENSATED,
                              result_json=serialised)
    return {"outcome": "compensated", "kind": current.kind, "result": result}


def compensate_run(run_id: str, *, url: str = DEFAULT_URL, claimer: str = "",
                   max_passes: int = 32) -> dict:
    """Drain a single run's obligations LIFO, with retry/backoff via the drainer
    state machine. Multiple passes let scheduled retries fire within this call;
    after `max_passes` the function returns whatever's left (still PENDING or
    STUCK) for the caller / the reactor to pick up later.

    The final run status mirrors what the operator should see:
      * all COMPENSATED → RUN_STATUS_FAILED       (the run failed, but cleanly)
      * any STUCK       → RUN_STATUS_STUCK        (needs a human)
      * still PENDING   → RUN_STATUS_COMPENSATING (the reactor will keep draining)
    """
    c = TapeClient(url)
    try:
        c.end_run(run_id=run_id, status=pb.RUN_STATUS_COMPENSATING)
        compensated: list[str] = []
        stuck: list[str] = []
        scheduled: list[str] = []
        for _ in range(max(1, max_passes)):
            obs = [
                ob for ob in c.list_obligations(run_id=run_id, only_unresolved=True).obligations
                if ob.status == pb.OBLIGATION_STATUS_PENDING
                and ob.next_attempt_at_ms <= int(time.time() * 1000)
            ]
            if not obs:
                # Nothing eligible right now. If any are PENDING-but-not-due,
                # leave them for the reactor; if all are done, we're done.
                break
            for ob in obs:  # already newest-first
                r = compensate_one(ob, client=c, claimer=claimer)
                if r["outcome"] == "compensated":
                    compensated.append(r.get("kind", ""))
                elif r["outcome"] == "stuck":
                    stuck.append(r.get("kind", ""))
                elif r["outcome"] == "scheduled":
                    scheduled.append(r.get("kind", ""))
            # Loop again — scheduled retries may have already become due.

        leftover = [
            ob for ob in c.list_obligations(run_id=run_id, only_unresolved=True).obligations
            if ob.status == pb.OBLIGATION_STATUS_PENDING
        ]
        if stuck:
            final = pb.RUN_STATUS_STUCK
        elif leftover:
            final = pb.RUN_STATUS_COMPENSATING  # reactor will keep at it
        else:
            final = pb.RUN_STATUS_FAILED
        c.end_run(run_id=run_id, status=final,
                  detail_json=json.dumps({"compensated": compensated, "stuck": stuck,
                                          "scheduled": scheduled, "leftover": len(leftover)}))
        out = {"compensated": compensated, "stuck": stuck, "scheduled": scheduled,
               "leftover": len(leftover), "final_status": final}
        if stuck:
            raise RuntimeError(f"compensation stuck for {run_id}: {stuck} — needs a human")
        return out
    finally:
        c.close()
