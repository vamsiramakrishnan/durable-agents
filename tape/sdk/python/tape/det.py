"""Journaled non-determinism — give the boundary a door.

The treatise's rule: *anything non-deterministic is a runtime call or an
activity result.* `TapePlugin` already journals every model call and every tool
call (read-only ones included), so most of the boundary is covered. What it
can't see is non-determinism smuggled in *outside* the framework — `time.time()`
in a tool body, `random.random()`, `uuid4()`, a file read, an inline HTTP GET.
Those drift the replay just as badly as a non-journaled write.

`tape.sample(tool_context, fn, *args)` is the door: call `fn`, record its
result on Tape, and on a re-drive return the recorded result instead of calling
`fn` again — "make any call an activity." `tape.now()`, `tape.uuid()`,
`tape.random()` are the common ones pre-wrapped.

The key is `<run_id>/sample-<name>-<k>` — the k-th `sample(name=...)` in the run,
counted by call order (same anchor as decisions/effects; same determinism
caveat — if the *count* of sample() calls depends on a sample()'s value, the
agent is non-deterministic and nothing can save it). Returns are JSON-encoded;
non-JSON returns come back as their `str()` on replay.
"""

from __future__ import annotations

import json
import random as _random
import threading
import time as _time
import uuid as _uuid
from typing import Any, Callable

from .client import TapeClient, DEFAULT_URL, EFFECT_STATUS_CONFIRMED
from .effect import run_id_of

_lock = threading.Lock()
_counters: dict[tuple[str, str], int] = {}
_clients: dict[str, TapeClient] = {}


def _client(url: str) -> TapeClient:
    with _lock:
        c = _clients.get(url)
        if c is None:
            c = TapeClient(url)
            _clients[url] = c
        return c


def _next(run_id: str, name: str) -> int:
    with _lock:
        n = _counters.get((run_id, name), 0)
        _counters[(run_id, name)] = n + 1
        return n


def sample(tool_context: Any, fn: Callable[..., Any], *fn_args: Any,
           name: str = "sample", url: str = DEFAULT_URL, **fn_kwargs: Any) -> Any:
    """Call `fn(*fn_args, **fn_kwargs)` once per run, journaled. On re-drive,
    returns the recorded result without calling `fn` again."""
    run_id = run_id_of(tool_context)
    if not run_id:
        return fn(*fn_args, **fn_kwargs)  # Tape not active here
    n = _next(run_id, name)
    key = f"{run_id}/sample-{name}-{n}"
    c = _client(url)
    try:
        be = c.begin_effect(run_id=run_id, decision_index=-1, tool_name=f"tape:sample:{name}",
                            call_index=n, request_json="{}", custom_key=key)
    except Exception:
        return fn(*fn_args, **fn_kwargs)
    if be.status == EFFECT_STATUS_CONFIRMED and be.response_json:
        try:
            return json.loads(be.response_json)["v"]
        except Exception:
            pass
    v = fn(*fn_args, **fn_kwargs)
    try:
        c.complete_effect(run_id=run_id, idempotency_key=key, status=EFFECT_STATUS_CONFIRMED,
                          response_json=json.dumps({"v": v}, default=str))
    except Exception:
        pass
    return v


def now(tool_context: Any, **kw: Any) -> float:
    """Wall-clock seconds, journaled — `time.time()` through the door."""
    return sample(tool_context, _time.time, name="now", **kw)


def uuid(tool_context: Any, **kw: Any) -> str:
    """A random UUID string, journaled — `uuid4()` through the door."""
    return sample(tool_context, lambda: str(_uuid.uuid4()), name="uuid", **kw)


def random(tool_context: Any, **kw: Any) -> float:
    """A random float in [0, 1), journaled — `random.random()` through the door."""
    return sample(tool_context, _random.random, name="random", **kw)
