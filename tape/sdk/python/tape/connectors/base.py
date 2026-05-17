"""The connector protocol and the small set of value types that go on the wire."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Protocol


class DispatchOutcome(str, Enum):
    CONFIRMED = "confirmed"     # dispatch landed and the counterparty acknowledged
    PENDING = "pending"         # accepted; await async resolution (reactor will reconcile)
    UNKNOWN = "unknown"         # network-level loss; reconcile via observe()
    FAILED = "failed"           # rejected by counterparty (4xx-class)


class ObservationOutcome(str, Enum):
    CONFIRMED = "confirmed"     # the effect did land
    ABSENT = "absent"           # the effect did not land
    DUPLICATE = "duplicate"     # the effect landed *more than once* — compensation due
    STUCK = "stuck"             # counterparty inconclusive; defer to human
    UNKNOWN = "unknown"         # observation itself failed; retry


class CompensationOutcome(str, Enum):
    COMPENSATED = "compensated"
    PENDING = "pending"
    STUCK = "stuck"
    FAILED = "failed"


@dataclass
class EffectRecord:
    """A side-effect intent the outbox reactor wants dispatched."""

    run_id: str
    idempotency_key: str
    tool_name: str
    connector: str
    payload: Any
    business_key: str = ""
    attempt: int = 1
    semantics: str = "idempotent"
    tenant_id: str = ""
    app_name: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass
class ObligationRecord:
    """A compensation obligation registered when a forward effect confirmed."""

    run_id: str
    effect_key: str
    kind: str
    payload: Any
    attempt: int = 1
    compensator_ref: str = ""
    tenant_id: str = ""


@dataclass
class DispatchResult:
    outcome: DispatchOutcome
    response: Any = None
    error: str = ""
    dispatch_id: str = ""
    retry_after_ms: int = 0


@dataclass
class ObservationResult:
    outcome: ObservationOutcome
    response: Any = None
    error: str = ""
    count: int = 0


@dataclass
class CompensationResult:
    outcome: CompensationOutcome
    response: Any = None
    error: str = ""


class Connector(Protocol):
    """The interface every capability connector implements.

    All three methods MUST be idempotent on `(run_id, idempotency_key)` — the
    reactor will retry, and a connector that double-dispatches is a connector
    that violates the contract Tape is built to provide.
    """

    name: str

    async def dispatch(self, effect: EffectRecord) -> DispatchResult: ...

    async def observe(self, effect: EffectRecord) -> ObservationResult: ...

    async def compensate(self, obligation: ObligationRecord) -> CompensationResult: ...
