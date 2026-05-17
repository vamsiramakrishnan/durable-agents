"""Capability connectors — the things the outbox reactor actually calls.

A `Connector` knows how to:

  * `dispatch(effect)`         — perform (or enqueue) the side effect;
  * `observe(effect)`          — ask the counterparty about an UNKNOWN by
                                  business key or id;
  * `compensate(obligation)`   — run the inverse for a duplicate.

Built-in connectors:

  * `HttpConnector`        — POST a JSON intent to an HTTPS endpoint.
  * `PubSubConnector`      — publish the intent as a Pub/Sub message.
  * `CloudTasksConnector`  — enqueue a Cloud Tasks HTTP target.
  * `LogConnector`         — append the intent to a JSON-lines file (tests / demos).

Register your own in the global `CONNECTORS` registry::

    from tape.connectors import CONNECTORS, HttpConnector

    CONNECTORS.register("bank.wire", HttpConnector(url="https://bank.example/wires"))

The registry is process-local; for fleet-wide registration, register in
`app/connectors.py` (the project scaffold generates this file) and have your
reactor process import it at startup.
"""

from __future__ import annotations

from .base import (
    Connector,
    DispatchResult,
    ObservationResult,
    CompensationResult,
    EffectRecord,
    ObligationRecord,
    DispatchOutcome,
    ObservationOutcome,
    CompensationOutcome,
)
from .registry import CONNECTORS, ConnectorRegistry
from .log import LogConnector
from .http import HttpConnector
from .pubsub import PubSubConnector
from .cloud_tasks import CloudTasksConnector

__all__ = [
    "Connector",
    "ConnectorRegistry",
    "CONNECTORS",
    "DispatchResult",
    "ObservationResult",
    "CompensationResult",
    "EffectRecord",
    "ObligationRecord",
    "DispatchOutcome",
    "ObservationOutcome",
    "CompensationOutcome",
    "LogConnector",
    "HttpConnector",
    "PubSubConnector",
    "CloudTasksConnector",
]
