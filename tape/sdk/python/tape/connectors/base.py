"""Connectors — the bridge between Tape's outbox and a real upstream system.

A *connector* is the unit of "do the actual call". For idempotent upstreams
this is mostly pass-through; for non-idempotent ones it's the only safe place
the call is allowed to happen, because:

  * the call site (the agent's tool body) is intent-only,
  * the connector is invoked by the outbox reactor under a CAS lease,
  * a connector failure is recorded as either a deterministic FAILED, a
    schedulable retry, or an explicit UNKNOWN (the reconciler picks up from
    there via `observe()`).

Connectors are registered by string name (matching `@tape.effect(connector=…)`
on the tool, or `@tape.outbox_tool(connector=…)`). The outbox reactor looks
them up by name when it draws a row.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Literal, Optional, Protocol, runtime_checkable


# ── result types ────────────────────────────────────────────────────────────

@dataclass
class DispatchResult:
    """What happened when the connector called the counterparty.

    * ``confirmed`` — the call landed; record the result and (optionally) the
      external_ref the counterparty returned.
    * ``failed``    — the call definitively did not land. Either deterministic
      (a 4xx the connector can interpret) or, after retries are exhausted,
      a final "give up" terminal.
    * ``unknown``   — we don't know. The ack was lost, or the timeout window
      closed before we heard back. **For non-idempotent upstreams this is the
      only safe non-confirmed outcome the connector may return** — the outbox
      will mark the effect UNKNOWN and stop dispatching; the reconciler then
      uses `observe()` to ask the counterparty what really happened.
    """
    status: Literal["confirmed", "failed", "unknown"]
    external_ref: str = ""
    response: dict = field(default_factory=dict)
    error: dict = field(default_factory=dict)
    # If the connector knows when to retry (e.g. a Retry-After hint), it may
    # set this; the outbox reactor uses it as `next_dispatch_at_ms`. 0 means
    # "use the default backoff schedule".
    retry_at_ms: int = 0


@dataclass
class ObservationResult:
    """What the connector saw at the counterparty for a given (idempotency_key
    or business_key). Maps to `EffectResolution` on the wire."""
    status: Literal["confirmed", "absent", "duplicate", "failed", "stuck"]
    external_ref: str = ""
    response: dict = field(default_factory=dict)


@dataclass
class CompensationResult:
    """What happened when the connector ran the inverse for a confirmed effect."""
    status: Literal["compensated", "failed"]
    response: dict = field(default_factory=dict)
    error: dict = field(default_factory=dict)


# ── the connector protocol ──────────────────────────────────────────────────

@runtime_checkable
class EffectConnector(Protocol):
    """A connector knows how to talk to one upstream. The three methods cover
    the three places Tape touches the outside world for a non-idempotent effect:

      * `dispatch(effect)`       — perform the call once (outbox reactor)
      * `observe(effect)`        — ask "what happened?" (reconciler)
      * `compensate(obligation)` — run the inverse (obligations reactor)

    Implementations should be **stateless** between calls; everything that must
    survive a crash lives in Tape's tables. Synchronous and async impls are
    both supported — the reactor awaits with `_maybe_await` (see below)."""

    name: str

    def dispatch(self, effect) -> DispatchResult: ...
    def observe(self, effect) -> ObservationResult: ...
    def compensate(self, obligation) -> CompensationResult: ...


# ── registry ────────────────────────────────────────────────────────────────

_REGISTRY: Dict[str, EffectConnector] = {}


def register(connector: EffectConnector, *, name: str = "") -> EffectConnector:
    """Register a connector. The name defaults to `connector.name` (so most
    callers just `tape.connectors.register(MyConnector(...))`). Overwriting an
    existing registration is allowed — late-binding is fine for tests."""
    n = name or getattr(connector, "name", "")
    if not n:
        raise ValueError("connector.name is required (or pass name=)")
    _REGISTRY[n] = connector
    return connector


def get(name: str) -> Optional[EffectConnector]:
    return _REGISTRY.get(name)


def all_registered() -> Dict[str, EffectConnector]:
    return dict(_REGISTRY)


def clear() -> None:
    """Test helper — clears the registry so a test can register fresh fakes."""
    _REGISTRY.clear()


# ── tiny helper: run a sync or async callable transparently ─────────────────

import asyncio


async def _maybe_await(value: Any):
    if asyncio.iscoroutine(value):
        return await value
    return value


def call_dispatch(connector: EffectConnector, effect) -> DispatchResult:
    """Run `connector.dispatch(effect)`, sync or async, and return the result."""
    result = connector.dispatch(effect)
    if asyncio.iscoroutine(result):
        return asyncio.get_event_loop().run_until_complete(result)
    return result


def call_observe(connector: EffectConnector, effect) -> ObservationResult:
    result = connector.observe(effect)
    if asyncio.iscoroutine(result):
        return asyncio.get_event_loop().run_until_complete(result)
    return result


def call_compensate(connector: EffectConnector, obligation) -> CompensationResult:
    result = connector.compensate(obligation)
    if asyncio.iscoroutine(result):
        return asyncio.get_event_loop().run_until_complete(result)
    return result
