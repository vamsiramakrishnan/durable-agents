"""A fake non-idempotent bank — accepts wires, returns ids, exposes lookup
and reverse endpoints. The "two wires" scenario is forced by setting
TAPE_FAKE_BANK_DUPLICATE=1 — the bank pretends to process the same business
key twice (an honest, ugly upstream).
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Optional


LEDGER_DIR = Path(os.environ.get("TAPE_EXAMPLE_DIR", "/tmp/tape-bank-demo"))
LEDGER_DIR.mkdir(parents=True, exist_ok=True)
LEDGER = LEDGER_DIR / "wires.json"


def _load() -> list[dict]:
    if not LEDGER.exists():
        return []
    try:
        return json.loads(LEDGER.read_text() or "[]")
    except Exception:
        return []


def _save(rows: list[dict]) -> None:
    LEDGER.write_text(json.dumps(rows, indent=2))


def reset() -> None:
    if LEDGER.exists():
        LEDGER.unlink()


def wire(*, business_key: str, payload: dict) -> dict:
    """The upstream is honest about being non-idempotent — it processes
    every call. The CALLER must dedup."""
    rows = _load()
    new_id = f"wire-{uuid.uuid4().hex[:8]}"
    rows.append({"id": new_id, "business_key": business_key, **payload})
    if os.environ.get("TAPE_FAKE_BANK_DUPLICATE") == "1":
        # Force a duplicate to exercise the compensation branch.
        rows.append({"id": f"wire-{uuid.uuid4().hex[:8]}",
                     "business_key": business_key, **payload, "duplicate": True})
    _save(rows)
    return {"id": new_id, "status": "ACCEPTED"}


def lookup(*, business_key: str) -> dict:
    rows = _load()
    matches = [r for r in rows if r["business_key"] == business_key]
    return {"count": len(matches), "ids": [r["id"] for r in matches]}


def reverse(*, wire_id: str) -> dict:
    rows = _load()
    new_rows = [r for r in rows if r["id"] != wire_id]
    if len(new_rows) == len(rows):
        return {"reversed": False, "reason": "not_found"}
    _save(new_rows)
    return {"reversed": True, "wire_id": wire_id}
