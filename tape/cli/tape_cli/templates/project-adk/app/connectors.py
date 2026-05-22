"""Connectors — the one place allowed to call a non-idempotent upstream.

Each connector implements three methods:

* `dispatch(effect)` — actually perform the call. Return CONFIRMED on
  success, UNKNOWN if the call may have landed but the ack was lost,
  FAILED if it definitively did not happen.
* `observe(effect)` — ask the upstream, by `business_key`, what really
  happened. The reconciler calls this to resolve an UNKNOWN.
* `compensate(obligation)` — run the inverse (refund, reversal).

The outbox reactor (started by `tape dev`) dispatches through the
connector keyed by name. The agent's tool body never touches the upstream.
"""

from __future__ import annotations

from tape_adk import (
    CompensationResult,
    DispatchResult,
    ObservationResult,
)


class PaymentConnector:
    """A stand-in payment connector. Replace `dispatch` / `observe` /
    `compensate` with real upstream calls — keyed on `effect.business_key`,
    which is the upstream's own idempotency key.

    This stub keeps an in-memory ledger so `tape dev` works out of the box.
    A real connector would call the payment API instead.
    """

    name = "payment"

    def __init__(self) -> None:
        self._ledger: dict[str, str] = {}  # business_key -> charge_id

    async def dispatch(self, effect) -> DispatchResult:
        bk = effect.business_key or ""
        if bk in self._ledger:
            # Already charged — the upstream's dedupe. Return the same id.
            return DispatchResult(status="confirmed",
                                  external_ref=self._ledger[bk],
                                  response={"charge_id": self._ledger[bk]})
        charge_id = f"ch_{len(self._ledger) + 1:06d}"
        self._ledger[bk] = charge_id
        return DispatchResult(status="confirmed", external_ref=charge_id,
                              response={"charge_id": charge_id})

    async def observe(self, effect) -> ObservationResult:
        bk = effect.business_key or ""
        if bk in self._ledger:
            return ObservationResult(status="confirmed",
                                     external_ref=self._ledger[bk])
        return ObservationResult(status="absent")

    async def compensate(self, obligation) -> CompensationResult:
        ref = (obligation.payload_json or {}).get("external_ref", "")
        return CompensationResult(status="compensated",
                                  response={"refunded": ref})


# The registry the reactor loop dispatches through. `tape.yaml`'s
# `embedded.connectors` points at this attribute.
CONNECTORS = {"payment": PaymentConnector()}
