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
import json
from typing import Any, Optional

from .client import TapeClient, DEFAULT_URL, RUN_STATUS_TERMINAL, RUN_STATUS_FAILED, RUN_STATUS_STUCK
from .effect import get_compensator
from ._gen import tape_pb2 as pb


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


def compensate_run(run_id: str, *, url: str = DEFAULT_URL) -> dict:
    """Run a run's compensations LIFO. Raises if any inverse fails (the
    obligation is marked STUCK first)."""
    c = TapeClient(url)
    try:
        c.end_run(run_id=run_id, status=pb.RUN_STATUS_COMPENSATING)
        obs = c.list_obligations(run_id=run_id, only_unresolved=True).obligations
        compensated, stuck = [], []
        for ob in obs:  # already newest-first
            fn = get_compensator(ob.kind)
            payload = {}
            try:
                payload = json.loads(ob.payload_json) if ob.payload_json else {}
            except Exception:
                payload = {}
            if fn is None:
                c.resolve_obligation(run_id=run_id, obligation_seq=ob.seq,
                                     status=pb.OBLIGATION_STATUS_STUCK,
                                     result_json=json.dumps({"error": f"no compensator registered for '{ob.kind}'"}))
                stuck.append(ob.kind)
                continue
            try:
                result = fn(**payload) if isinstance(payload, dict) else fn(payload)
                c.resolve_obligation(run_id=run_id, obligation_seq=ob.seq,
                                     status=pb.OBLIGATION_STATUS_COMPENSATED,
                                     result_json=json.dumps(result if isinstance(result, (dict, list, str, int, float, bool)) or result is None else str(result)))
                compensated.append(ob.kind)
            except Exception as e:
                c.resolve_obligation(run_id=run_id, obligation_seq=ob.seq,
                                     status=pb.OBLIGATION_STATUS_STUCK,
                                     result_json=json.dumps({"error": str(e)}))
                stuck.append(ob.kind)
        final = pb.RUN_STATUS_STUCK if stuck else pb.RUN_STATUS_FAILED
        c.end_run(run_id=run_id, status=final, detail_json=json.dumps({"compensated": compensated, "stuck": stuck}))
        if stuck:
            raise RuntimeError(f"compensation stuck for {run_id}: {stuck} — needs a human")
        return {"compensated": compensated, "stuck": stuck}
    finally:
        c.close()
