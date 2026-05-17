"""A connector that records dispatches/observations to a JSON-lines file.

Useful for tests, demos, and the non-idempotent-bank example — anywhere you
want to see the outbox reactor's choreography without standing up a real
upstream.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict
from typing import Any

from .base import (
    Connector,
    DispatchResult,
    DispatchOutcome,
    ObservationResult,
    ObservationOutcome,
    CompensationResult,
    CompensationOutcome,
    EffectRecord,
    ObligationRecord,
)


class LogConnector:
    name = "log"

    def __init__(self, path: str = "/tmp/tape-outbox.jsonl"):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    def _append(self, kind: str, body: dict) -> None:
        with open(self.path, "a", buffering=1) as fp:
            fp.write(json.dumps({"kind": kind, "ts_ms": int(time.time() * 1000), **body}) + "\n")

    async def dispatch(self, effect: EffectRecord) -> DispatchResult:
        self._append("dispatch", asdict(effect))
        return DispatchResult(outcome=DispatchOutcome.CONFIRMED, response={"logged": True})

    async def observe(self, effect: EffectRecord) -> ObservationResult:
        self._append("observe", asdict(effect))
        return ObservationResult(outcome=ObservationOutcome.CONFIRMED, count=1)

    async def compensate(self, obligation: ObligationRecord) -> CompensationResult:
        self._append("compensate", asdict(obligation))
        return CompensationResult(outcome=CompensationOutcome.COMPENSATED)
