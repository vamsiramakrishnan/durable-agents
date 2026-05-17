"""HTTP connector — POST the intent payload to an HTTPS endpoint.

Headers:
  X-Tape-Idempotency-Key  the run/decision-derived key the counterparty must use
                          to dedup.
  X-Tape-Business-Key     when supplied by `@tape.outbox_tool(business_key=...)`.
  X-Tape-Run-Id           for traceability.
  X-Tape-Attempt          the dispatch attempt number.

A 2xx response is `CONFIRMED`. A 4xx is `FAILED` (won't retry — the counterparty
rejected it). A 5xx or network error is `UNKNOWN` (the dispatch may or may not
have landed; the reactor will call `observe()`).
"""

from __future__ import annotations

import json
from typing import Any, Optional

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


class HttpConnector:
    def __init__(
        self,
        *,
        url: str,
        name: Optional[str] = None,
        observe_url: Optional[str] = None,
        compensate_url: Optional[str] = None,
        timeout_s: float = 30.0,
        headers: Optional[dict] = None,
    ):
        self.name = name or "http"
        self.url = url
        self.observe_url = observe_url
        self.compensate_url = compensate_url
        self.timeout_s = timeout_s
        self.headers = headers or {}

    async def _post(self, url: str, body: Any, effect_or_ob: Any) -> tuple[int, Any, str]:
        try:
            import httpx
        except ImportError as ex:  # pragma: no cover
            raise RuntimeError(
                "HttpConnector requires the `httpx` package — `pip install httpx`."
            ) from ex
        hdrs = dict(self.headers)
        hdrs["Content-Type"] = "application/json"
        hdrs["X-Tape-Idempotency-Key"] = getattr(effect_or_ob, "idempotency_key",
                                                  getattr(effect_or_ob, "effect_key", ""))
        hdrs["X-Tape-Run-Id"] = getattr(effect_or_ob, "run_id", "")
        bk = getattr(effect_or_ob, "business_key", "")
        if bk:
            hdrs["X-Tape-Business-Key"] = bk
        attempt = getattr(effect_or_ob, "attempt", 1)
        hdrs["X-Tape-Attempt"] = str(attempt)
        async with httpx.AsyncClient(timeout=self.timeout_s) as cli:
            resp = await cli.post(url, content=json.dumps(body), headers=hdrs)
            try:
                payload = resp.json()
            except Exception:
                payload = resp.text
            return resp.status_code, payload, ""

    async def dispatch(self, effect: EffectRecord) -> DispatchResult:
        try:
            status, body, _ = await self._post(self.url, effect.payload, effect)
        except Exception as ex:
            return DispatchResult(outcome=DispatchOutcome.UNKNOWN, error=str(ex))
        if 200 <= status < 300:
            return DispatchResult(outcome=DispatchOutcome.CONFIRMED, response=body)
        if 400 <= status < 500:
            return DispatchResult(outcome=DispatchOutcome.FAILED, response=body,
                                  error=f"http {status}")
        return DispatchResult(outcome=DispatchOutcome.UNKNOWN, response=body,
                              error=f"http {status}")

    async def observe(self, effect: EffectRecord) -> ObservationResult:
        if not self.observe_url:
            return ObservationResult(outcome=ObservationOutcome.UNKNOWN,
                                     error="no observe_url configured")
        try:
            status, body, _ = await self._post(
                self.observe_url,
                {"idempotency_key": effect.idempotency_key,
                 "business_key": effect.business_key,
                 "payload": effect.payload},
                effect,
            )
        except Exception as ex:
            return ObservationResult(outcome=ObservationOutcome.UNKNOWN, error=str(ex))
        if status == 200 and isinstance(body, dict):
            count = int(body.get("count", 0))
            if count == 0:
                return ObservationResult(outcome=ObservationOutcome.ABSENT, response=body, count=0)
            if count == 1:
                return ObservationResult(outcome=ObservationOutcome.CONFIRMED, response=body, count=1)
            return ObservationResult(outcome=ObservationOutcome.DUPLICATE, response=body, count=count)
        return ObservationResult(outcome=ObservationOutcome.UNKNOWN, response=body,
                                 error=f"http {status}")

    async def compensate(self, obligation: ObligationRecord) -> CompensationResult:
        if not self.compensate_url:
            return CompensationResult(outcome=CompensationOutcome.STUCK,
                                      error="no compensate_url configured")
        try:
            status, body, _ = await self._post(self.compensate_url, obligation.payload, obligation)
        except Exception as ex:
            return CompensationResult(outcome=CompensationOutcome.PENDING, error=str(ex))
        if 200 <= status < 300:
            return CompensationResult(outcome=CompensationOutcome.COMPENSATED, response=body)
        if 400 <= status < 500:
            return CompensationResult(outcome=CompensationOutcome.FAILED, response=body,
                                      error=f"http {status}")
        return CompensationResult(outcome=CompensationOutcome.PENDING, response=body,
                                  error=f"http {status}")
