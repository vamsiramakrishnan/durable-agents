"""The bank connector: how Tape's outbox calls the non-idempotent bank.

The connector is the ONE place in the example that's allowed to call the bank
directly. It runs in the outbox reactor process under a CAS lease — the
agent's tool body never touches the bank.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

import tape.connectors as connectors
from tape.connectors.base import (DispatchResult, ObservationResult,
                                  CompensationResult)

from .bank import bank


_DISPATCH_INJECT_UNKNOWN = "TAPE_BANK_DISPATCH_INJECT_UNKNOWN"
_DISPATCH_BEFORE_RECORD_CRASH = "TAPE_BANK_DISPATCH_CRASH_AFTER_WIRE"


@dataclass
class BankConnector:
    """A real connector for the file-backed fake bank.

    Faults the example exercises (set via env vars so `run.py` can flip them
    without changing the code):

      * `TAPE_BANK_DISPATCH_INJECT_UNKNOWN=1`     — the bank IS called, but the
        connector returns `unknown` (simulating a lost ack). The outbox loop
        drives the effect to UNKNOWN and stops; the reconciler resolves via
        `observe()`.
      * `TAPE_BANK_DISPATCH_CRASH_AFTER_WIRE=1`   — the bank IS called, then
        the connector raises (process-level crash from the reactor's view).
        The lease expires and a later dispatcher reclaims; the safety claim
        is that it sees the prior wire via observe() and does NOT issue a
        second wire.
    """

    name: str = "bank.wire"

    def dispatch(self, effect):
        req = json.loads(effect.request_json or "{}")
        # Land the wire.
        wire_id = bank.wire(
            account=req["account"],
            amount_minor=req["amount_minor"],
            beneficiary=req["beneficiary"],
            date=req["date"],
            business_key=effect.business_key,
        )
        if os.environ.get(_DISPATCH_INJECT_UNKNOWN) == "1":
            # The wire landed but the ack got lost. UNKNOWN — no retry, the
            # reconciler will pick this up.
            return DispatchResult(status="unknown",
                                  error={"reason": "simulated lost ack"})
        if os.environ.get(_DISPATCH_BEFORE_RECORD_CRASH) == "1":
            raise RuntimeError("simulated dispatcher crash AFTER the wire landed")
        return DispatchResult(status="confirmed", external_ref=wire_id,
                              response={"wire_id": wire_id})

    def observe(self, effect):
        rows = bank.search_by_business_key(effect.business_key)
        if not rows:
            return ObservationResult(status="absent")
        if len(rows) > 1:
            return ObservationResult(status="duplicate",
                                     external_ref=rows[0]["wire_id"])
        return ObservationResult(status="confirmed",
                                 external_ref=rows[0]["wire_id"])

    def compensate(self, obligation):
        try:
            payload = json.loads(obligation.payload_json or "{}")
            wid = payload.get("external_ref") or payload.get("wire_id")
            if not wid:
                return CompensationResult(status="failed",
                                          error={"reason": "no wire_id to reverse"})
            rev = bank.reverse(wid)
            return CompensationResult(status="compensated",
                                      response={"reversal_id": rev})
        except Exception as ex:
            return CompensationResult(status="failed",
                                      error={"type": type(ex).__name__, "message": str(ex)})


# Auto-register on import — the outbox reactor's --load <module>:<attr> path
# uses this side effect to wire the connector into the registry. Importing
# this module is enough.
register_singleton = connectors.register(BankConnector())
