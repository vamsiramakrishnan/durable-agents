"""Injectable fakes for the treasury example — a bank, a broker, a GL.

Each keeps a *file-backed* ledger keyed by idempotency key, so it dedups across
process restarts (this is "the floor": the counterparty's own idempotency). Each
also has a crash hook (`TAPE_CRASH_AFTER=<tool_name>` in the environment) that
calls `os._exit()` *after* the side effect is durably recorded but *before* the
call returns — exactly the window the spec cares about.

The kill-and-resume test relies on:
  * the bank's ledger surviving the crash (it's a file);
  * the same idempotency key arriving on the re-run (Tape mints a stable key);
  so the second call is deduped: one wire, not two.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional


def _ledger_dir() -> Path:
    d = Path(os.environ.get("TAPE_EXAMPLE_DIR", "/tmp/tape-treasury"))
    d.mkdir(parents=True, exist_ok=True)
    return d


def _maybe_crash(tool_name: str) -> None:
    if os.environ.get("TAPE_CRASH_AFTER", "") == tool_name:
        # Hard exit — no atexit, no flush of anything not already on disk. This
        # is the deploy/OOM/crash the journal is supposed to survive.
        os._exit(137)


class _FileLedger:
    """A tiny key -> record store, persisted as JSON."""

    def __init__(self, name: str):
        self.path = _ledger_dir() / f"{name}.json"

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except Exception:
                return {}
        return {}

    def _save(self, data: dict) -> None:
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2))
        os.replace(tmp, self.path)

    def get(self, key: str) -> Optional[dict]:
        return self._load().get(key)

    def put_if_absent(self, key: str, record: dict) -> dict:
        data = self._load()
        if key in data:
            return data[key]
        data[key] = record
        self._save(data)
        return record

    def all(self) -> dict:
        return self._load()


class FakeBank:
    def __init__(self):
        self.ledger = _FileLedger("bank")

    def wire(self, account_id: str, amount_minor: int, target_mmf: str, *, idempotency_key: str) -> str:
        existing = self.ledger.get(idempotency_key)
        if existing is not None:
            return existing["wire_id"]
        wire_id = f"wire-{len(self.ledger.all()) + 1:04d}"
        self.ledger.put_if_absent(idempotency_key, {
            "wire_id": wire_id, "account_id": account_id,
            "amount_minor": amount_minor, "target_mmf": target_mmf})
        _maybe_crash("execute_sweep")  # the money has moved; the ack has not been returned
        return wire_id

    def reverse(self, wire_id: str, **_) -> str:
        rev = self.ledger.get(f"reverse:{wire_id}")
        if rev is not None:
            return rev["reversal_id"]
        reversal_id = f"reversal-of-{wire_id}"
        self.ledger.put_if_absent(f"reverse:{wire_id}", {"reversal_id": reversal_id})
        return reversal_id

    def wire_status(self, idempotency_key: str) -> dict:
        rec = self.ledger.get(idempotency_key)
        return {"found": rec is not None, "wire_id": rec["wire_id"] if rec else None}

    def count(self) -> int:
        return sum(1 for k in self.ledger.all() if not k.startswith("reverse:"))


class FakeBroker:
    def __init__(self):
        self.ledger = _FileLedger("broker")

    def place(self, instrument: str, notional_minor: int, *, idempotency_key: str) -> str:
        existing = self.ledger.get(idempotency_key)
        if existing is not None:
            return existing["order_id"]
        order_id = f"order-{len(self.ledger.all()) + 1:04d}"
        self.ledger.put_if_absent(idempotency_key, {
            "order_id": order_id, "instrument": instrument, "notional_minor": notional_minor})
        _maybe_crash("execute_hedge")
        return order_id

    def order_status(self, idempotency_key: str) -> dict:
        rec = self.ledger.get(idempotency_key)
        return {"found": rec is not None, "order_id": rec["order_id"] if rec else None}


class FakeGL:
    def __init__(self):
        self.ledger = _FileLedger("gl")

    def post(self, entries: list, *, idempotency_key: str) -> str:
        existing = self.ledger.get(idempotency_key)
        if existing is not None:
            return existing["batch_id"]
        batch_id = f"gl-batch-{len(self.ledger.all()) + 1:04d}"
        self.ledger.put_if_absent(idempotency_key, {"batch_id": batch_id, "entries": entries})
        _maybe_crash("post_gl")
        return batch_id

    def count(self) -> int:
        return len(self.ledger.all())


def reset_ledgers() -> None:
    """Wipe the file ledgers — call at the start of a fresh demo/test run."""
    for name in ("bank", "broker", "gl"):
        p = _ledger_dir() / f"{name}.json"
        if p.exists():
            p.unlink()


# Module-level singletons so the agent's tool bodies and the test see the same
# (file-backed) ledgers.
bank = FakeBank()
broker = FakeBroker()
gl = FakeGL()
