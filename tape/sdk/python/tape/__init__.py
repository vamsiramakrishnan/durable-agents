"""Tape — a durable-execution substrate for ADK agents (Python SDK).

Quick start::

    from google.adk.runners import Runner
    from google.adk.apps import App
    from google.adk.apps.app import ResumabilityConfig
    from tape.adk import TapePlugin, TapeSessionService
    import tape

    app = App(name="treasury", root_agent=agent, plugins=[TapePlugin(budget=tape.Budget(usd_cap=50))],
              resumability_config=ResumabilityConfig(is_resumable=True))
    runner = Runner(app=app, session_service=TapeSessionService("tape://localhost:7878"))

See ../../../design-principles/tape.md for the design, and ../../examples/treasury
for a worked example.
"""

from __future__ import annotations

import json as _json

from .client import (
    TapeClient,
    DEFAULT_URL,
    RUN_STATUS_RUNNABLE,
    RUN_STATUS_RUNNING,
    RUN_STATUS_WAITING,
    RUN_STATUS_TERMINAL,
    RUN_STATUS_FAILED,
    RUN_STATUS_STUCK,
    RUN_STATUS_CANCELLED,
    EFFECT_STATUS_PENDING,
    EFFECT_STATUS_CONFIRMED,
    EFFECT_STATUS_FAILED,
    EFFECT_STATUS_UNKNOWN,
)
from .effect import effect, idempotency_key, run_id_of, get_compensator, get_status_check
from .outbox import outbox_tool, outbox_meta_of, OutboxConfigError
from .retry import RetryPolicy
from .tenancy import TenancyConfig, TenancyMode
from .gates import AckLost, gate, gate_tool
from .budget import Budget, with_budget
from .det import sample, now, uuid, random
from ._recover import resume, recover_once, compensate_run, compensate_one
from ._gen import tape_pb2 as pb
from .reactions import (
    on,
    on_value_change,
    on_value_deleted,
    on_effect_confirmed,
    on_effect_failed,
    on_effect_unknown,
    on_decision_recorded,
    on_gate,
    on_run,
    register_all,
    run_dispatcher,
    run_pubsub_bridge,
)

__all__ = [
    "TapeClient", "DEFAULT_URL", "pb",
    "effect", "idempotency_key", "run_id_of",
    "outbox_tool", "outbox_meta_of", "OutboxConfigError",
    "TenancyConfig", "TenancyMode",
    "RetryPolicy",
    "AckLost", "gate", "gate_tool",
    "Budget", "with_budget",
    "sample", "now", "uuid", "random",
    "resume", "recover_once", "compensate_run", "compensate_one", "send_signal", "set_timer", "cancel_timer",
    "cancel_run", "is_cancelled", "heartbeat", "policy_is",
    "set_value", "get_value", "watch_value", "delete_value",
    "get_compensator", "get_status_check",
    "RUN_STATUS_RUNNABLE", "RUN_STATUS_RUNNING", "RUN_STATUS_WAITING", "RUN_STATUS_TERMINAL",
    "RUN_STATUS_FAILED", "RUN_STATUS_STUCK",
    "EFFECT_STATUS_PENDING", "EFFECT_STATUS_CONFIRMED", "EFFECT_STATUS_FAILED", "EFFECT_STATUS_UNKNOWN",
    # event-bus decorators + runners (see design-principles/tape-event-bus.md)
    "on", "on_value_change", "on_value_deleted",
    "on_effect_confirmed", "on_effect_failed", "on_effect_unknown",
    "on_decision_recorded", "on_gate", "on_run",
    "register_all", "run_dispatcher", "run_pubsub_bridge",
]

__version__ = "0.1.0"


def send_signal(gate_name, *, run_id="", app_name="", user_id="", session_id="",
                resolution=None, url: str = DEFAULT_URL):
    """Release a run parked on a gate (or stash the resolution for when it parks)."""
    with TapeClient(url) as c:
        return c.send_signal(run_id=run_id, app_name=app_name, user_id=user_id,
                             session_id=session_id, gate_name=gate_name,
                             resolution_json=_json.dumps(resolution or {}))


def set_timer(*, run_id, fire_at_ms, kind, timer_id="", payload=None, url: str = DEFAULT_URL):
    """Set (or replace) a timer. When `fire_at_ms` passes, the timer reactor
    fires it — `kind` "gate_timeout" / "redrive" / "reconcile" are handled
    built-in; others are delegated to your `on_timer` callback. Idempotent on
    (run_id, timer_id)."""
    with TapeClient(url) as c:
        return c.set_timer(run_id=run_id, timer_id=timer_id, fire_at_ms=fire_at_ms,
                           kind=kind, payload_json=_json.dumps(payload or {}))


def cancel_timer(*, run_id, timer_id, url: str = DEFAULT_URL):
    with TapeClient(url) as c:
        return c.cancel_timer(run_id=run_id, timer_id=timer_id)


def cancel_run(run_id: str, *, reason: str = "", url: str = DEFAULT_URL):
    """Cancel a run. Cooperative: the plugin (with `check_cancellation=True`) — or
    your tool code via `tape.is_cancelled(tool_context)` — sees CANCELLED at the
    next model/tool boundary and bails."""
    with TapeClient(url) as c:
        return c.end_run(run_id=run_id, status=RUN_STATUS_CANCELLED,
                         detail_json=_json.dumps({"cancelled": True, "reason": reason}))


def is_cancelled(tool_context, *, url: str = DEFAULT_URL) -> bool:
    """Read-only check from inside a tool body. One gRPC roundtrip per call —
    use sparingly (or rely on `TapePlugin(check_cancellation=True)`)."""
    from .effect import run_id_of
    rid = run_id_of(tool_context)
    if not rid:
        return False
    with TapeClient(url) as c:
        try:
            return c.get_run(rid).status == RUN_STATUS_CANCELLED
        except Exception:
            return False


def heartbeat(tool_context, *, lease_ttl_ms: int = 120_000, url: str = DEFAULT_URL):
    """Extend the run's lease — for use inside a long-running tool body so the
    recovery reactor doesn't decide the run is stale and re-drive it concurrently."""
    from .effect import run_id_of
    rid = run_id_of(tool_context)
    if not rid:
        return None
    import os as _os, socket as _socket
    owner = _os.environ.get("TAPE_LEASE_OWNER", f"{_socket.gethostname()}:{_os.getpid()}")
    with TapeClient(url) as c:
        return c.resume_run(run_id=rid, lease_owner=owner, lease_ttl_ms=lease_ttl_ms)


def set_value(namespace: str, key: str, value, *, if_version: int = -1,
              writer: str = "", url: str = DEFAULT_URL):
    """Write `value` (JSON-serializable) at `(namespace, key)`. Optional CAS
    via `if_version` (-1 = unconditional, 0 = create-only, >0 = expect that
    exact version). Returns the post-write `ValueRecord` whose `version` is
    the new monotonic version. Anyone watching the key gets pushed the new
    record."""
    with TapeClient(url) as c:
        return c.write_value(namespace=namespace, key=key,
                             value_json=_json.dumps(value),
                             if_version=if_version, writer=writer)


def get_value(namespace: str, key: str, *, url: str = DEFAULT_URL):
    """Read the current `(namespace, key)`. Returns the `GetValueResponse`
    (`.found`, `.value`, `.value.version` etc.); the value isn't auto-decoded
    so callers can choose."""
    with TapeClient(url) as c:
        return c.get_value(namespace=namespace, key=key)


def watch_value(namespace: str, key: str, *, from_version: int = 0,
                url: str = DEFAULT_URL):
    """Stream `ValueEvent`s for `(namespace, key)` starting at `from_version`
    (0 = current snapshot + future changes). Each event carries `value` (the
    new `ValueRecord`) plus `prev_version` and `prev_value_json` so the
    receiver sees the transition (X: 70 → 90). The iterator is a long-lived
    gRPC streaming response; iterate it in a thread, and `.cancel()` it to
    stop."""
    c = TapeClient(url)
    return c.watch_value(namespace=namespace, key=key, from_version=from_version)


def delete_value(namespace: str, key: str, *, url: str = DEFAULT_URL):
    """Tombstone the key. Watchers see one final event with `value.deleted = True`."""
    with TapeClient(url) as c:
        return c.delete_value(namespace=namespace, key=key)


def policy_is(tool_context, version: str) -> bool:
    """Branch on the recorded policy version (Temporal-style `workflow.patched`):
    `if tape.policy_is(tool_context, "cfo-policy-2026.05"): …`. The version is
    whatever you put in session.state["policy_version"] when starting the run; the
    plugin records it on every decision."""
    try:
        return tool_context.state.get("policy_version", "") == version
    except Exception:
        return False
