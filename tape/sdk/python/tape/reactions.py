"""`@tape.on(...)` — the event-bus user surface.

This module is the Python-side companion to the event-bus rebuild described in
`design-principles/tape-event-bus.md`. The shape of the surface:

  * Declarative decorators (`@tape.on`, `@tape.on_value_change`, …) collect a
    process-global registry of `ReactionDef`s with their handler functions. The
    decorators are **declaration only** — they do NOT call the server. Registration
    happens when you invoke `register_all(url)` (explicit) or `run_dispatcher(url)`
    (registers, then loops).

  * `run_dispatcher(url, owner=…)` is the in-process reference dispatcher: for
    every registered TASK reaction, claim a bounded batch from the server, fire
    handlers with backpressure (max_concurrency / rate_limit_per_s / debounce_ms),
    and complete/nack each task. The retry policy (`retry_max`, `dlq_after_n`) is
    enforced by the SERVER — the dispatcher just calls `NackTask(permanent=…)`
    once `attempts >= dlq_after_n`. AGENT reactions are handled entirely on the
    server (matching entries create runs); PUBLISH reactions are pulled by the
    Pub/Sub bridge (`run_pubsub_bridge`).

  * Subject convenience wrappers (`on_value_change`, `on_effect_confirmed`,
    `on_run`, …) construct the right subject pattern and call `@tape.on(...)`.

  * Subject path segments are URL-encoded except for the wildcards `*` and `**`.
    `key="*"` → one-segment wildcard; `key="**"` → rest wildcard; anything else
    is treated as a literal segment and percent-encoded.

This module is intentionally optional: nothing else in the SDK depends on it.
You can use the Tape journal / values / effects perfectly well without ever
importing `tape.reactions`.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Optional

from ._gen import tape_pb2 as pb
from .client import (
    DEFAULT_URL,
    HANDLER_KIND_AGENT,
    HANDLER_KIND_PUBLISH,
    HANDLER_KIND_TASK,
    TapeClient,
)

_log = logging.getLogger("tape.reactions")


# ── registry ────────────────────────────────────────────────────────────────

@dataclass
class ReactionDef:
    """One `@tape.on(...)` declaration, before it's registered on the server."""
    subject_pattern: str
    handler: Optional[Callable[[Any], Any]] = None
    handler_kind: int = HANDLER_KIND_TASK
    predicate_cel: str = ""
    agent_app: str = ""
    publish_target: str = ""
    max_concurrency: int = 1
    rate_limit_per_s: int = 0
    debounce_ms: int = 0
    retry_max: int = 5
    retry_backoff_ms: int = 1000
    dlq_after_n: int = 5
    num_shards: int = 1
    name: str = ""
    reaction_id: str = ""
    # Populated when the reaction is registered on the server.
    server_reaction_id: str = ""


# Process-global registry: every `@tape.on(...)` call appends here. Tests can
# clear it via `_REGISTRY.clear()` between cases.
_REGISTRY: list[ReactionDef] = []


def _clear_registry() -> None:
    """Drop every registered reaction (test-only helper)."""
    _REGISTRY.clear()


def get_registry() -> list[ReactionDef]:
    return list(_REGISTRY)


# ── decorators ──────────────────────────────────────────────────────────────

def _resolve_handler_kind(agent: Optional[str], publish: Optional[str]) -> tuple[int, str, str]:
    """Pick (handler_kind, agent_app, publish_target) from the two mutually
    exclusive options. Default = TASK."""
    if agent and publish:
        raise ValueError("@tape.on(...): pass either agent= OR publish=, not both")
    if agent:
        return HANDLER_KIND_AGENT, str(agent), ""
    if publish:
        return HANDLER_KIND_PUBLISH, "", str(publish)
    return HANDLER_KIND_TASK, "", ""


def on(subject_pattern: str, *, predicate: Optional[str] = None,
       agent: Optional[str] = None, publish: Optional[str] = None,
       max_concurrency: int = 1, rate_limit_per_s: int = 0,
       debounce_ms: int = 0, retry_max: int = 5, retry_backoff_ms: int = 1000,
       dlq_after_n: int = 5, num_shards: int = 1, name: str = "",
       reaction_id: str = "") -> Callable[[Callable], Callable]:
    """Register a reaction at decoration time.

    `subject_pattern` follows the path-style grammar described in the design
    doc (e.g. `/tape/value/changed/treasury/**`).

    Exactly one of `agent=<app_name>`, `publish=<broker_url>`, or neither
    (meaning a TASK handler) may be set. TASK handlers run the decorated
    function in the dispatcher process; AGENT handlers re-invoke an ADK app
    on the server; PUBLISH handlers create tasks for a broker bridge.

    `predicate` is an optional CEL expression evaluated server-side against
    the envelope `{run_id, subject, kind, payload, ...}`.

    Per-reaction back-pressure knobs (`max_concurrency`, `rate_limit_per_s`,
    `debounce_ms`, `retry_max`, `dlq_after_n`) are honoured by the in-proc
    dispatcher / enforced by the server (retries / DLQ).
    """
    kind, agent_app, publish_target = _resolve_handler_kind(agent, publish)

    def deco(fn: Callable) -> Callable:
        rd = ReactionDef(
            subject_pattern=subject_pattern,
            handler=fn,
            handler_kind=kind,
            predicate_cel=predicate or "",
            agent_app=agent_app,
            publish_target=publish_target,
            max_concurrency=max(1, int(max_concurrency)),
            rate_limit_per_s=int(rate_limit_per_s),
            debounce_ms=int(debounce_ms),
            retry_max=int(retry_max),
            retry_backoff_ms=int(retry_backoff_ms),
            dlq_after_n=int(dlq_after_n),
            num_shards=max(1, int(num_shards)),
            name=name or fn.__name__,
            reaction_id=reaction_id,
        )
        _REGISTRY.append(rd)
        # Attach the definition to the function so callers can introspect.
        fn._tape_reaction = rd  # type: ignore[attr-defined]
        return fn

    return deco


# ── subject helpers / convenience wrappers ─────────────────────────────────

def _seg(s: str) -> str:
    """URL-encode a single subject segment. Pass `*` or `**` through unchanged
    (they're the grammar's wildcards). Anything else — including slashes,
    spaces, colons — is percent-encoded so user-chosen keys can't break the
    grammar."""
    if s in ("*", "**"):
        return s
    return urllib.parse.quote(s, safe="")


def on_value_change(namespace: str, key: str = "*", **kwargs):
    """Fire when a value in `(namespace, key)` is written. `key="*"` matches one
    segment; `key="**"` matches any remaining segments."""
    pattern = f"/tape/value/changed/{_seg(namespace)}/{_seg(key)}"
    return on(pattern, **kwargs)


def on_value_deleted(namespace: str, key: str = "*", **kwargs):
    pattern = f"/tape/value/deleted/{_seg(namespace)}/{_seg(key)}"
    return on(pattern, **kwargs)


def on_effect_confirmed(tool: str = "*", **kwargs):
    pattern = f"/tape/effect/confirmed/{_seg(tool)}/**"
    return on(pattern, **kwargs)


def on_effect_failed(tool: str = "*", **kwargs):
    pattern = f"/tape/effect/failed/{_seg(tool)}/**"
    return on(pattern, **kwargs)


def on_effect_unknown(tool: str = "*", **kwargs):
    pattern = f"/tape/effect/unknown/{_seg(tool)}/**"
    return on(pattern, **kwargs)


def on_decision_recorded(**kwargs):
    return on("/tape/decision/recorded/**", **kwargs)


def on_gate(gate: str, verb: str = "released", **kwargs):
    """Fire on a gate lifecycle event. `verb="released"` (default) matches
    `/tape/gate/released/<gate>/**`; `verb="waiting"` matches `waiting`."""
    pattern = f"/tape/gate/{_seg(verb)}/{_seg(gate)}/**"
    return on(pattern, **kwargs)


def on_run(status: str = "terminal", **kwargs):
    """Fire on run-lifecycle events. `status="terminal"` (default) matches
    `/tape/run/terminal/**`; pass `"failed"`, `"running"`, etc."""
    pattern = f"/tape/run/{_seg(status)}/**"
    return on(pattern, **kwargs)


# ── registration ───────────────────────────────────────────────────────────

def register_all(url: str = DEFAULT_URL, *, prefix: str = "") -> list[pb.Reaction]:
    """Call `RegisterReaction` for every reaction in the process registry.
    Returns the list of persisted `Reaction` protos (with the server-assigned
    `reaction_id` filled in). `prefix` is prepended to each reaction's `name`
    so concurrent test runs don't collide on the human label.

    Idempotent on `reaction_id` — the server upserts by id. If a reaction
    declared `reaction_id=""`, the server mints a stable id and we record it
    back onto the `ReactionDef`."""
    out: list[pb.Reaction] = []
    with TapeClient(url) as c:
        for rd in _REGISTRY:
            r = c.register_reaction(
                reaction_id=rd.reaction_id,
                name=(prefix + rd.name) if prefix else rd.name,
                subject_pattern=rd.subject_pattern,
                predicate_cel=rd.predicate_cel,
                handler_kind=rd.handler_kind,
                agent_app=rd.agent_app,
                publish_target=rd.publish_target,
                max_concurrency=rd.max_concurrency,
                rate_limit_per_s=rd.rate_limit_per_s,
                debounce_ms=rd.debounce_ms,
                retry_max=rd.retry_max,
                retry_backoff_ms=rd.retry_backoff_ms,
                dlq_after_n=rd.dlq_after_n,
                num_shards=rd.num_shards,
            )
            rd.server_reaction_id = r.reaction_id
            out.append(r)
    return out


# ── backpressure primitives ────────────────────────────────────────────────

class _TokenBucket:
    """Tiny thread-safe token bucket — capacity == rate, refill 1 token every
    1/rate seconds. `acquire()` blocks until a token is available. `rate <= 0`
    disables limiting (acquire is a no-op)."""

    def __init__(self, rate_per_s: int):
        self.rate = max(0, int(rate_per_s))
        self._tokens = float(self.rate)
        self._last = time.monotonic()
        self._lk = threading.Lock()

    def acquire(self) -> None:
        if self.rate <= 0:
            return
        while True:
            with self._lk:
                now = time.monotonic()
                elapsed = now - self._last
                self._last = now
                self._tokens = min(float(self.rate), self._tokens + elapsed * self.rate)
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                need = (1.0 - self._tokens) / self.rate
            time.sleep(min(0.25, need))


class _Debouncer:
    """`(reaction_id, subject)` coalescer: returns True the first time a
    subject is seen within the window, False subsequently. Window resets on
    each "True" hit, so a stream of identical subjects at sub-window cadence
    yields one call per `debounce_ms`."""

    def __init__(self, window_ms: int):
        self.window = max(0, int(window_ms)) / 1000.0
        self._last: dict[str, float] = {}
        self._lk = threading.Lock()

    def allow(self, subject: str) -> bool:
        if self.window <= 0:
            return True
        now = time.monotonic()
        with self._lk:
            last = self._last.get(subject, -1.0)
            if last < 0 or (now - last) >= self.window:
                self._last[subject] = now
                return True
            return False


# ── OTel propagation (lazy) ────────────────────────────────────────────────

_otel_available: Optional[bool] = None


def _otel_span_ctx(trace_id_hex: str, parent_span_id_hex: str):
    """Return a context with a non-recording span whose `(trace_id, span_id)`
    match the source entry — so the dispatcher's span is a child of it. Returns
    `None` if OpenTelemetry isn't installed or the ids are malformed."""
    global _otel_available
    if not trace_id_hex or not parent_span_id_hex:
        return None
    if _otel_available is False:
        return None
    try:
        from opentelemetry import trace as _trace
        from opentelemetry.trace import (
            NonRecordingSpan,
            SpanContext,
            TraceFlags,
            set_span_in_context,
        )
        _otel_available = True
        try:
            tid_int = int(trace_id_hex, 16)
            sid_int = int(parent_span_id_hex, 16)
        except ValueError:
            return None
        sc = SpanContext(trace_id=tid_int, span_id=sid_int, is_remote=True,
                         trace_flags=TraceFlags(0x01))
        return set_span_in_context(NonRecordingSpan(sc))
    except Exception:
        _otel_available = False
        return None


def _otel_tracer():
    try:
        from opentelemetry import trace as _trace
        return _trace.get_tracer("tape.reactions")
    except Exception:
        return None


# ── dispatcher ─────────────────────────────────────────────────────────────

def _default_owner() -> str:
    """Stable per-process identifier used as the task lease owner."""
    return os.environ.get(
        "TAPE_DISPATCHER_OWNER",
        f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}",
    )


def _payload_handler(rd: ReactionDef):
    """Make a callable that takes a `pb.Task` and runs the user handler,
    decoding the payload into a dict (best-effort)."""
    fn = rd.handler

    def envelope_of(task) -> Any:
        # Mirrors the CEL envelope on the server, so a handler can use the
        # same field names server-side predicates use.
        payload = {}
        if task.payload_json:
            try:
                payload = json.loads(task.payload_json)
            except Exception:  # noqa: BLE001
                payload = {"raw": task.payload_json}
        return {
            "task_id": task.task_id,
            "reaction_id": task.reaction_id,
            "subject": task.subject,
            "source_run_id": task.source_run_id,
            "source_global_seq": task.source_global_seq,
            "attempts": task.attempts,
            "trace_id": task.trace_id,
            "parent_span_id": task.parent_span_id,
            "payload": payload,
            "payload_json": task.payload_json,
        }

    def run(task) -> None:
        if fn is None:
            return
        env = envelope_of(task)
        ctx = _otel_span_ctx(task.trace_id, task.parent_span_id)
        if ctx is not None:
            tracer = _otel_tracer()
            if tracer is not None:
                with tracer.start_as_current_span("tape.task", context=ctx):
                    fn(env)
                    return
        fn(env)

    return run


def _dispatch_one(rd: ReactionDef, client: TapeClient, owner: str,
                  executor: ThreadPoolExecutor, bucket: _TokenBucket,
                  debouncer: _Debouncer, run_handler: Callable[[Any], None],
                  task) -> None:
    """Submit `task` to the executor, calling `complete_task` or `nack_task`
    based on outcome. Debounce-skips complete the task as a no-op so the
    server doesn't re-lease it (see module docstring)."""
    # Debounce decision: COMPLETE the task as a no-op rather than nack-permanent.
    # Rationale: a debounced trigger is the handler *choosing* to skip — it's
    # not an error, and we don't want it cluttering the DLQ. A no-op complete
    # leaves a clean audit trail (status=DONE, attempts unchanged).
    if not debouncer.allow(task.subject):
        try:
            client.complete_task(task_id=task.task_id, owner=owner)
        except Exception as ex:  # noqa: BLE001
            _log.warning("debounce-complete failed for task %s: %s", task.task_id, ex)
        return

    def _work():
        bucket.acquire()
        try:
            run_handler(task)
            client.complete_task(task_id=task.task_id, owner=owner)
        except Exception as ex:  # noqa: BLE001
            # The server records the attempt count; on ClaimTasks it already
            # incremented attempts for the in-flight claim, so by the time
            # we're here `task.attempts` reflects the attempt that just failed.
            # Promote to DLQ once that count crosses the configured threshold.
            permanent = task.attempts >= rd.dlq_after_n
            try:
                client.nack_task(task_id=task.task_id, owner=owner,
                                 error=f"{type(ex).__name__}: {ex}",
                                 permanent=permanent)
            except Exception as ex2:  # noqa: BLE001
                _log.warning("nack failed for task %s: %s", task.task_id, ex2)

    executor.submit(_work)


def run_dispatcher(url: str = DEFAULT_URL, *, owner: str = "",
                   poll_interval_s: float = 0.5, once: bool = False,
                   register: bool = True, prefix: str = "",
                   claim_max: int = 16, lease_ms: int = 60_000) -> None:
    """In-proc dispatcher: claim → run → ack for every TASK reaction.

    The loop:

      1. (Optionally, once) call `RegisterReaction` for every `@tape.on`-decorated
         handler in the registry. Skip with `register=False` if you registered
         out of band (e.g. from another process).
      2. For each TASK reaction, `ClaimTasks(reaction_id, …, max=claim_max,
         lease_ms=lease_ms)`. Submit each returned task to a per-reaction
         `ThreadPoolExecutor` (size = `max_concurrency`).
      3. Inside the worker: enforce `rate_limit_per_s` via token bucket, drop
         a re-trigger of the same subject inside `debounce_ms` (complete it as
         a no-op), run the handler, `CompleteTask` on success or
         `NackTask(permanent=…)` on failure.

    AGENT and PUBLISH reactions are not driven by this loop — the server
    creates the runs / tasks; the Pub/Sub bridge pulls PUBLISH tasks.

    `once=True` returns after a single pass over every reaction (handy for
    tests). `register=False` skips the initial `RegisterReaction` calls (use
    if you have a separate process owning registration)."""
    owner = owner or _default_owner()
    if register:
        register_all(url, prefix=prefix)

    # Build the per-reaction state once. Only TASK reactions are dispatched
    # locally — AGENT runs are created server-side, PUBLISH tasks belong to
    # the bridge.
    state: dict[str, dict] = {}
    for rd in _REGISTRY:
        if rd.handler_kind != HANDLER_KIND_TASK:
            continue
        if not rd.server_reaction_id:
            continue
        state[rd.server_reaction_id] = {
            "rd": rd,
            "executor": ThreadPoolExecutor(max_workers=rd.max_concurrency,
                                           thread_name_prefix=f"tape-r-{rd.name}"),
            "bucket": _TokenBucket(rd.rate_limit_per_s),
            "debouncer": _Debouncer(rd.debounce_ms),
            "run_handler": _payload_handler(rd),
        }

    client = TapeClient(url)
    try:
        while True:
            did_any = False
            for rid, st in state.items():
                rd = st["rd"]
                try:
                    tasks = client.claim_tasks(
                        reaction_id=rid, shard=-1, owner=owner,
                        lease_ms=lease_ms, max=claim_max)
                except Exception as ex:  # noqa: BLE001
                    _log.warning("claim failed for %s: %s", rid, ex)
                    continue
                for t in tasks:
                    did_any = True
                    _dispatch_one(rd, client, owner, st["executor"],
                                  st["bucket"], st["debouncer"],
                                  st["run_handler"], t)
            if once:
                return
            if not did_any:
                time.sleep(poll_interval_s)
    finally:
        # Drain executors BEFORE closing the gRPC channel — workers still need
        # to call `complete_task` / `nack_task` on outcomes.
        for st in state.values():
            st["executor"].shutdown(wait=True)
        client.close()


# ── Pub/Sub bridge ─────────────────────────────────────────────────────────

def run_pubsub_bridge(url: str = DEFAULT_URL, *, project: str, topic: str,
                      reaction_id: str = "", owner: str = "",
                      once: bool = False, poll_interval_s: float = 0.5,
                      claim_max: int = 32, lease_ms: int = 60_000) -> None:
    """Pull PUBLISH-kind tasks from Tape and publish them to a Pub/Sub topic.

    If `reaction_id` is empty, every registered PUBLISH reaction is pulled
    in turn. The Pub/Sub message body is the task's `payload_json` (UTF-8
    bytes); attributes carry `tape-task-id`, `tape-reaction-id`,
    `tape-subject`, `tape-global-seq`, `tape-trace-id`. The Pub/Sub
    `ordering_key` is the source `run_id`, so per-run order is preserved at
    the subscriber (if it enabled ordered delivery).

    Lazy-imports `google-cloud-pubsub` — raises `RuntimeError` if it's not
    installed. `once=True` returns after one pass (handy for tests)."""
    try:
        from google.cloud import pubsub_v1  # type: ignore
    except Exception as ex:  # noqa: BLE001
        raise RuntimeError(
            "run_pubsub_bridge requires google-cloud-pubsub; "
            "pip install google-cloud-pubsub"
        ) from ex

    publisher = pubsub_v1.PublisherClient(
        publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True))
    topic_path = publisher.topic_path(project, topic)
    owner = owner or _default_owner()

    # Resolve the list of reaction ids to pull. If the user gave one explicitly,
    # honour it; otherwise pull every registered PUBLISH reaction (registering
    # them first if they haven't been).
    rids: list[str] = []
    if reaction_id:
        rids = [reaction_id]
    else:
        for rd in _REGISTRY:
            if rd.handler_kind != HANDLER_KIND_PUBLISH:
                continue
            if not rd.server_reaction_id:
                register_all(url)
            if rd.server_reaction_id:
                rids.append(rd.server_reaction_id)

    try:
        with TapeClient(url) as c:
            while True:
                did_any = False
                for rid in rids:
                    try:
                        tasks = c.claim_tasks(reaction_id=rid, shard=-1, owner=owner,
                                              lease_ms=lease_ms, max=claim_max)
                    except Exception as ex:  # noqa: BLE001
                        _log.warning("claim failed for %s: %s", rid, ex)
                        continue
                    for t in tasks:
                        did_any = True
                        attrs = {
                            "tape-task-id": t.task_id,
                            "tape-reaction-id": t.reaction_id,
                            "tape-subject": t.subject,
                            "tape-global-seq": str(t.source_global_seq),
                            "tape-trace-id": t.trace_id or "",
                        }
                        try:
                            fut = publisher.publish(
                                topic_path,
                                (t.payload_json or "").encode("utf-8"),
                                ordering_key=str(t.source_run_id or ""),
                                **attrs,
                            )
                            fut.result(timeout=10.0)
                            c.complete_task(task_id=t.task_id, owner=owner)
                        except Exception as ex:  # noqa: BLE001
                            # Defer the DLQ decision to the server: it knows
                            # the reaction's `dlq_after_n` and will promote
                            # the task to DLQ once attempts exceed it. We just
                            # report this attempt as a transient failure.
                            try:
                                c.nack_task(task_id=t.task_id, owner=owner,
                                            error=f"pubsub-publish: {ex}",
                                            permanent=False)
                            except Exception as ex2:  # noqa: BLE001
                                _log.warning("nack failed for task %s: %s", t.task_id, ex2)
                if once:
                    return
                if not did_any:
                    time.sleep(poll_interval_s)
    finally:
        try:
            publisher.stop()
        except Exception:
            pass


__all__ = [
    "ReactionDef",
    "on",
    "on_value_change",
    "on_value_deleted",
    "on_effect_confirmed",
    "on_effect_failed",
    "on_effect_unknown",
    "on_decision_recorded",
    "on_gate",
    "on_run",
    "register_all",
    "run_dispatcher",
    "run_pubsub_bridge",
    "get_registry",
]
