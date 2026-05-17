"""Register a `bank.wire` connector that knows how to dispatch / observe /
compensate against the fake bank. In production, swap `LocalBankConnector`
for `HttpConnector(url=https://bank.example/wires, ...)`.
"""

from __future__ import annotations

from tape.connectors import (
    CONNECTORS,
    DispatchResult, DispatchOutcome,
    ObservationResult, ObservationOutcome,
    CompensationResult, CompensationOutcome,
    EffectRecord, ObligationRecord,
)

from . import fake_bank


class LocalBankConnector:
    name = "bank.wire"

    async def dispatch(self, effect: EffectRecord) -> DispatchResult:
        try:
            res = fake_bank.wire(business_key=effect.business_key, payload=effect.payload)
            return DispatchResult(outcome=DispatchOutcome.CONFIRMED, response=res,
                                  dispatch_id=res["id"])
        except Exception as ex:
            return DispatchResult(outcome=DispatchOutcome.UNKNOWN, error=str(ex))

    async def observe(self, effect: EffectRecord) -> ObservationResult:
        res = fake_bank.lookup(business_key=effect.business_key)
        if res["count"] == 0:
            return ObservationResult(outcome=ObservationOutcome.ABSENT, response=res, count=0)
        if res["count"] == 1:
            return ObservationResult(outcome=ObservationOutcome.CONFIRMED, response=res, count=1)
        return ObservationResult(outcome=ObservationOutcome.DUPLICATE, response=res,
                                 count=res["count"])

    async def compensate(self, obligation: ObligationRecord) -> CompensationResult:
        # The obligation payload should carry the wire_id of the duplicate.
        wire_id = (obligation.payload or {}).get("wire_id")
        if not wire_id:
            return CompensationResult(outcome=CompensationOutcome.STUCK,
                                      error="no wire_id on obligation")
        res = fake_bank.reverse(wire_id=wire_id)
        if res.get("reversed"):
            return CompensationResult(outcome=CompensationOutcome.COMPENSATED, response=res)
        return CompensationResult(outcome=CompensationOutcome.STUCK, response=res)


if "bank.wire" not in CONNECTORS:
    CONNECTORS.register("bank.wire", LocalBankConnector())
