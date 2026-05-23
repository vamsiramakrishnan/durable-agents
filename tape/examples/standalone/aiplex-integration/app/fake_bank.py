"""The treasury agent's flaky upstream — stays in-memory so the example runs
offline. Not a counterparty model; just enough surface to demonstrate the
non-idempotent / outbox path. See `app/agent.py` for the @tape.effect wiring."""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field


@dataclass
class _Bank:
    """An in-memory bank counterparty. Idempotent on wire_id (the caller
    derives one from the @tape.effect business_key); a second wire with the
    same id is a no-op and returns the original record."""

    _wires: dict[str, dict] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def wire(self, *, account_id: str, amount_minor: int, target: str,
             business_key: str) -> dict:
        with self._lock:
            existing = self._wires.get(business_key)
            if existing is not None:
                return existing
            wire_id = f"wire-{uuid.uuid4().hex[:8]}"
            rec = {"wire_id": wire_id, "account_id": account_id,
                   "amount_minor": amount_minor, "target": target,
                   "business_key": business_key, "status": "confirmed"}
            self._wires[business_key] = rec
            return rec

    def wire_status(self, business_key: str) -> dict | None:
        """The reconciler's hook: ask the counterparty whether a logical
        operation by this business key has landed. Returns None if absent."""
        with self._lock:
            return self._wires.get(business_key)


bank = _Bank()
