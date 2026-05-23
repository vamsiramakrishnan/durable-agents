"""Connector protocol — how the outbox dispatcher talks to a counterparty,
and how the reconciler asks the counterparty what really happened.

A connector implements three operations against one upstream system
(`bank.wire`, `payment.charge`, `email.send`, …):

* `dispatch(effect)` — actually call the upstream. Returns CONFIRMED
  (success), UNKNOWN (call may have landed but the ack was lost), FAILED
  (the upstream definitively didn't do it), or raises (treated as
  retry-after-backoff).

* `observe(effect)` — ask the upstream by `business_key` whether the
  operation lives in its records. Returns CONFIRMED + external_ref,
  FAILED, ABSENT, or DUPLICATE. This is the reconciler's only window
  into the counterparty's reality.

* `compensate(obligation)` — run the inverse (reverse a wire, refund a
  charge). Returns COMPENSATED or FAILED.

Connector authors typically build these by wrapping the upstream's HTTP
client. The connector is the ONE place in the system that's allowed to
call the upstream — the agent's tool body never does (that's what makes
the OUTBOX contract structural).

The shapes here are intentionally tiny and match the existing
`tape/sdk/python/tape/connectors/base.py` so a connector can be shared
between the gRPC and embedded paths.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Protocol, runtime_checkable

from .service import EffectRecord, ObligationRecord


# ── result shapes ──────────────────────────────────────────────────────────


@dataclass
class DispatchResult:
    """What a single dispatch attempt produced. `status` drives the server
    -side transition: confirmed → effect done; unknown → reconciler takes
    over; failed → re-dispatch after backoff; absent only on observe()."""

    status: str    # 'confirmed' | 'unknown' | 'failed'
    external_ref: str = ""
    response: Optional[Any] = None
    error: Optional[Any] = None
    # Backoff hint for the dispatcher — only honored when status='failed'.
    # 0 means "use the dispatcher's default exponential backoff".
    retry_after_ms: int = 0


@dataclass
class ObservationResult:
    """What `observe(business_key)` found on the counterparty's side."""

    status: str    # 'confirmed' | 'failed' | 'absent' | 'duplicate'
    external_ref: str = ""
    response: Optional[Any] = None
    error: Optional[Any] = None
    # When status='duplicate' + the upstream's view of the rogue record's
    # external_ref. The reconciler uses this to register a compensation.
    compensate_kind: str = ""


@dataclass
class CompensationResult:
    """What the inverse-operation call did."""

    status: str    # 'compensated' | 'failed'
    response: Optional[Any] = None
    error: Optional[Any] = None
    # Hint for the drainer's backoff on failure.
    retry_after_ms: int = 0


# ── the protocol ───────────────────────────────────────────────────────────


@runtime_checkable
class Connector(Protocol):
    """Implement three async methods and you're a connector. The reactor
    library does the rest — claiming, transitioning, retrying with
    backoff, and recording the result against the journal.

    `name` is the registry key — the same string used in `outbox_tool(
    connector="bank.wire")`. The reconciler / outbox dispatcher dispatch
    on `effect.connector` to find the right implementation.
    """

    name: str

    async def dispatch(self, effect: EffectRecord) -> DispatchResult: ...

    async def observe(self, effect: EffectRecord) -> ObservationResult: ...

    async def compensate(
        self, obligation: ObligationRecord
    ) -> CompensationResult: ...


# ── tiny built-in connectors for tests + smoke runs ────────────────────────


@dataclass
class LogConnector:
    """A no-op connector that logs every call. Useful for tests and demos
    where you don't want to wire up a real upstream."""

    name: str = "log"
    dispatches: list[EffectRecord] = field(default_factory=list)
    observations: list[EffectRecord] = field(default_factory=list)
    compensations: list[ObligationRecord] = field(default_factory=list)

    async def dispatch(self, effect: EffectRecord) -> DispatchResult:
        self.dispatches.append(effect)
        return DispatchResult(status="confirmed",
                               external_ref=f"log-{effect.idempotency_key[:8]}")

    async def observe(self, effect: EffectRecord) -> ObservationResult:
        self.observations.append(effect)
        return ObservationResult(status="absent")

    async def compensate(self, obligation: ObligationRecord) -> CompensationResult:
        self.compensations.append(obligation)
        return CompensationResult(status="compensated")


__all__ = [
    "Connector",
    "DispatchResult",
    "ObservationResult",
    "CompensationResult",
    "LogConnector",
]
