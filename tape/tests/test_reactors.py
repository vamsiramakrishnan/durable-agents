"""Reactors + timers: the timer reactor fires a gate_timeout timer (releasing a
parked run), and the reconciler reactor resolves an UNKNOWN effect via the
registered status check."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SDK_PY = ROOT / "tape" / "sdk" / "python"
EXAMPLES = ROOT / "tape" / "examples"
sys.path.insert(0, str(SDK_PY))
sys.path.insert(0, str(EXAMPLES))


def _query(db: str, sql: str, params=()):
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def _now_ms() -> int:
    return int(time.time() * 1000)


def test_timer_reactor_fires_a_gate_timeout(tape_server):
    import tape
    import tape.reactors as reactors
    from tape.client import TapeClient
    import tape.client as tc

    url = tape_server["url"]
    c = TapeClient(url)
    # a placeholder run to hang the timer/gate off
    run = c.begin_run(app_name="t", user_id="u", session_id="s-timer", invocation_id="inv-timer",
                      lease_owner="test", lease_ttl_ms=60_000)
    rid = run.run_id
    # a timer that's already due, asking the reactor to time the gate out
    c.set_timer(run_id=rid, fire_at_ms=_now_ms() - 1000, kind="gate_timeout",
                payload_json=json.dumps({"gate": "cfo-approval", "resolution": {"approved": False}}))
    # fire due timers
    fired = reactors.fire_due_timers_once(url)
    assert any(f["kind"] == "gate_timeout" and "signalled" in f["action"] for f in fired), fired
    # the gate is now delivered with the timeout resolution
    aw = c.await_signal(run_id=rid, gate_name="cfo-approval", payload_json="{}")
    assert aw.delivered
    res = json.loads(aw.resolution_json)
    assert res.get("timed_out") is True and res.get("approved") is False
    # the timer is fired (no longer due)
    assert all(t.run_id != rid for t in c.list_due_timers(now_ms=_now_ms() + 5000, claim=False).timers)
    c.close()


def test_reconciler_resolves_an_unknown_effect(example_env):
    """The bank's ack is lost on the sweep -> the effect goes UNKNOWN -> the
    reconciler calls the registered status check (bank.wire_status) and resolves
    it to CONFIRMED. One wire; one confirmed record."""
    env = example_env["env"]
    db = example_env["db"]
    ledger_dir = example_env["ledger_dir"]
    url = example_env["url"]

    # run the treasury agent with a lost-ack on the sweep
    acklost_env = dict(env, TAPE_ACKLOST_AFTER="execute_sweep")
    subprocess.run([sys.executable, "-m", "treasury.run", "--reset"], env=acklost_env,
                   cwd=example_env["cwd"], capture_output=True, text=True, timeout=120)
    # the bank moved the money exactly once...
    bank = json.loads((ledger_dir / "bank.json").read_text())
    assert sum(1 for k in bank if not k.startswith("reverse:")) == 1
    # ...and Tape recorded the sweep effect as UNKNOWN
    import tape.client as tc
    rows = _query(db, "SELECT run_id, idempotency_key, status FROM tape_effects WHERE tool_name='execute_sweep'")
    assert len(rows) == 1, rows
    run_id, key, status = rows[0]
    assert status == tc.EFFECT_STATUS_UNKNOWN, f"expected UNKNOWN, got {status}"

    # now run the reconciler — it needs the registered status check, so import the agent module
    os.environ["TAPE_EXAMPLE_DIR"] = str(ledger_dir)  # so the in-process bank reads the same ledger
    import importlib
    treasury_agent = importlib.import_module("treasury.agent")  # registers @tape.effect(status_check=bank.wire_status)
    from tape.effect import get_status_check
    assert get_status_check("execute_sweep") is not None, "the status check should be registered by importing the agent"
    import tape.reactors as reactors
    resolved = reactors.reconcile_once(url, reconcile_pending_after_ms=0)
    assert any(r["key"] == key and r["resolved"] == "confirmed" for r in resolved), resolved

    # the effect is now CONFIRMED, and the wire still happened exactly once
    rows = _query(db, "SELECT status, response_json FROM tape_effects WHERE tool_name='execute_sweep'")
    assert rows[0][0] == tc.EFFECT_STATUS_CONFIRMED
    bank = json.loads((ledger_dir / "bank.json").read_text())
    assert sum(1 for k in bank if not k.startswith("reverse:")) == 1
    _ = treasury_agent
