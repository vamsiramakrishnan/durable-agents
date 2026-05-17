"""A deliberately non-idempotent fake bank.

`wire(...)` accepts no idempotency key and lands one wire per call. The whole
point of the example is to make THIS bank safe through Tape's outbox +
reconciliation, without changing the bank itself.

The state is a JSON file so the demo can crash, restart, and check the
ledger from outside.
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from pathlib import Path


class FakeNonIdempotentBank:
    """File-backed counter. Each `wire` writes a row; no key support."""

    def __init__(self, ledger_path: Path):
        self.path = Path(ledger_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("[]")
        self._lock = threading.Lock()

    def _load(self) -> list:
        try:
            return json.loads(self.path.read_text() or "[]")
        except Exception:
            return []

    def _save(self, rows: list) -> None:
        tmp = str(self.path) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(rows, f, indent=2)
        os.replace(tmp, self.path)

    def wire(self, *, account: str, amount_minor: int, beneficiary: str,
             date: str, business_key: str = "") -> str:
        """Land a wire. Returns the bank's identifier (wire_id). NOT IDEMPOTENT
        — a second call with the same arguments lands a second wire."""
        with self._lock:
            rows = self._load()
            wire_id = f"wire-{uuid.uuid4().hex[:8]}"
            rows.append({
                "wire_id": wire_id, "account": account,
                "amount_minor": amount_minor, "beneficiary": beneficiary,
                "date": date, "business_key": business_key,
            })
            self._save(rows)
            return wire_id

    def search_by_business_key(self, business_key: str) -> list:
        if not business_key:
            return []
        return [r for r in self._load() if r.get("business_key") == business_key]

    def all_wires(self) -> list:
        return self._load()

    def reverse(self, wire_id: str) -> str:
        with self._lock:
            rows = self._load()
            rows.append({"wire_id": f"reverse-{uuid.uuid4().hex[:8]}",
                          "reverses": wire_id})
            self._save(rows)
            return f"reverse-{wire_id}"


def _default_ledger_path() -> Path:
    base = os.environ.get("TAPE_EXAMPLE_DIR", "/tmp/tape-nonidem-example")
    return Path(base) / "bank.json"


bank = FakeNonIdempotentBank(_default_ledger_path())
