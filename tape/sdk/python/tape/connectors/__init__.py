"""tape.connectors — adapters that bridge Tape's outbox to real upstreams.

See `base.py` for the protocol; `http.py` for a generic HTTP POST connector;
`pubsub.py` for a Google Pub/Sub connector (lazy import — google-cloud-pubsub
is not a hard dependency of the SDK).
"""

from __future__ import annotations

from .base import (
    EffectConnector,
    DispatchResult,
    ObservationResult,
    CompensationResult,
    register,
    get,
    all_registered,
    clear,
    call_dispatch,
    call_observe,
    call_compensate,
)

__all__ = [
    "EffectConnector",
    "DispatchResult",
    "ObservationResult",
    "CompensationResult",
    "register",
    "get",
    "all_registered",
    "clear",
    "call_dispatch",
    "call_observe",
    "call_compensate",
]
