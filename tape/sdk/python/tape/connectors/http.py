"""HTTPConnector — POST the effect's request payload to a configured endpoint,
attaching idempotency / tape headers so the receiver can dedupe.

Intended for upstreams that *do* support idempotency keys (Stripe, most modern
payment APIs, S3 multipart uploads, …). For upstreams that *don't*, use the
PubSub connector + a downstream worker that records a business-key lock — or
write a custom connector that wraps the API's own dedupe pattern.

This connector uses `urllib.request` so it has no third-party dep. For
production, drop in `httpx` or `requests` via a subclass — the protocol is the
only thing the outbox reactor cares about.
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from .base import DispatchResult, ObservationResult, CompensationResult


@dataclass
class HTTPConnector:
    """Generic HTTP outbox connector.

      * `name`           — match key for `@tape.effect(connector=…)`.
      * `endpoint`       — full URL to POST to.
      * `headers`        — extra static headers (auth, content-type, …).
      * `timeout_s`      — request timeout.
      * `observe_endpoint` — optional URL for `GET ?key=…`; the response JSON
        guides the observation result (`status` field expected: confirmed |
        absent | duplicate | failed | stuck).
      * `compensate_endpoint` — optional URL for `POST` to run the inverse.
      * `success_codes`  — HTTP codes that count as confirmed (default 2xx).
      * `duplicate_code` — HTTP code that maps to ObservationResult.duplicate
        (default 409). 409 + a JSON body with `external_ref` is the common
        idempotency-conflict pattern.
    """
    name: str
    endpoint: str
    headers: Dict[str, str] = None  # type: ignore[assignment]
    timeout_s: float = 10.0
    observe_endpoint: str = ""
    compensate_endpoint: str = ""
    success_codes: tuple = (200, 201, 202, 204)
    duplicate_code: int = 409

    def __post_init__(self) -> None:
        if self.headers is None:
            self.headers = {}

    # ── dispatch ────────────────────────────────────────────────────────────
    def dispatch(self, effect) -> DispatchResult:
        """POST `effect.request_json` to `endpoint`. Adds:
          * `Idempotency-Key: <effect.idempotency_key>` — the standard header
            most providers dedupe on
          * `X-Tape-Run-Id`, `X-Tape-Effect-Key`, `X-Tape-Business-Key`
        Returns confirmed on 2xx, unknown on timeout / network error (the
        ack was lost — the reconciler must resolve), failed on a definitive
        4xx/5xx that isn't a timeout."""
        try:
            body = effect.request_json.encode("utf-8") if effect.request_json else b"{}"
            req = urllib.request.Request(self.endpoint, data=body, method="POST",
                                          headers=self._build_headers(effect))
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                code = resp.getcode()
                body_text = resp.read().decode("utf-8", errors="replace")
                try:
                    body_json = json.loads(body_text) if body_text else {}
                except Exception:
                    body_json = {"_raw": body_text}
                if code == self.duplicate_code:
                    # Idempotency conflict — the upstream knows about the key,
                    # so it's effectively confirmed (this is the dedupe path).
                    return DispatchResult(
                        status="confirmed",
                        external_ref=str(body_json.get("external_ref") or body_json.get("id") or ""),
                        response={"http_code": code, **body_json})
                if code in self.success_codes:
                    return DispatchResult(
                        status="confirmed",
                        external_ref=str(body_json.get("external_ref") or body_json.get("id") or ""),
                        response={"http_code": code, **body_json})
                return DispatchResult(
                    status="failed",
                    response={"http_code": code, **body_json},
                    error={"http_code": code, "body": body_text})
        except (socket.timeout, TimeoutError) as ex:
            # Lost-ack window: the call MAY have landed. Surface as UNKNOWN.
            return DispatchResult(status="unknown",
                                  error={"type": type(ex).__name__, "message": str(ex)})
        except urllib.error.HTTPError as ex:
            code = ex.code
            try:
                body_text = ex.read().decode("utf-8", errors="replace")
                body_json = json.loads(body_text) if body_text else {}
            except Exception:
                body_text, body_json = "", {}
            if code == self.duplicate_code:
                return DispatchResult(
                    status="confirmed",
                    external_ref=str(body_json.get("external_ref") or body_json.get("id") or ""),
                    response={"http_code": code, **body_json})
            # 4xx (auth, malformed) is deterministic FAILED; 5xx is also FAILED
            # for this attempt (the outbox loop's backoff will retry until the
            # connector explicitly gives up).
            return DispatchResult(
                status="failed",
                response={"http_code": code, **body_json},
                error={"http_code": code, "body": body_text})
        except (urllib.error.URLError, OSError) as ex:
            # Network-level failure before the request reached the upstream.
            # Almost always safe to retry — but the call MAY have crossed the
            # wire and the ack was lost on the way back. The safe choice for a
            # non-idempotent upstream is UNKNOWN; the dispatcher uses the
            # connector's hint to decide whether to retry (effect.semantics).
            return DispatchResult(status="unknown",
                                  error={"type": type(ex).__name__, "message": str(ex)})

    # ── observe ─────────────────────────────────────────────────────────────
    def observe(self, effect) -> ObservationResult:
        """Ask the upstream: "did the operation with this key happen?". The
        observation endpoint is expected to return JSON like:

            {"status": "confirmed" | "absent" | "duplicate" | "failed" | "stuck",
             "external_ref": "<optional>", "...": "..."}

        If no observe_endpoint is set, returns `absent` (the reconciler will
        treat that per-semantics — see record_external_observation)."""
        if not self.observe_endpoint:
            return ObservationResult(status="absent")
        key = effect.idempotency_key
        # Some APIs key on the business key; the connector decides what to ask.
        bk = effect.business_key
        url = f"{self.observe_endpoint}?key={urllib.parse.quote(key)}&business_key={urllib.parse.quote(bk)}"
        try:
            req = urllib.request.Request(url, method="GET",
                                          headers=self._build_headers(effect))
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                body_text = resp.read().decode("utf-8", errors="replace")
                body_json = json.loads(body_text) if body_text else {}
                status = str(body_json.get("status") or "absent")
                if status not in ("confirmed", "absent", "duplicate", "failed", "stuck"):
                    status = "stuck"
                return ObservationResult(
                    status=status,   # type: ignore[arg-type]
                    external_ref=str(body_json.get("external_ref") or ""),
                    response=body_json)
        except Exception as ex:
            return ObservationResult(status="stuck", response={"error": str(ex)})

    # ── compensate ──────────────────────────────────────────────────────────
    def compensate(self, obligation) -> CompensationResult:
        if not self.compensate_endpoint:
            return CompensationResult(status="failed",
                                      error={"reason": "no compensate_endpoint configured"})
        try:
            body = obligation.payload_json.encode("utf-8") if obligation.payload_json else b"{}"
            req = urllib.request.Request(self.compensate_endpoint, data=body, method="POST",
                                          headers={**self.headers,
                                                   "Content-Type": "application/json",
                                                   "Idempotency-Key": obligation.effect_key})
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                code = resp.getcode()
                if code in self.success_codes:
                    return CompensationResult(status="compensated",
                                              response={"http_code": code})
                return CompensationResult(status="failed",
                                          error={"http_code": code})
        except Exception as ex:
            return CompensationResult(status="failed",
                                      error={"type": type(ex).__name__, "message": str(ex)})

    # ── helpers ─────────────────────────────────────────────────────────────
    def _build_headers(self, effect) -> Dict[str, str]:
        h = {
            "Content-Type": "application/json",
            "Idempotency-Key": effect.idempotency_key,
            "X-Tape-Run-Id": effect.run_id,
            "X-Tape-Effect-Key": effect.idempotency_key,
            "X-Tape-Business-Key": effect.business_key,
        }
        h.update(self.headers)
        return h
