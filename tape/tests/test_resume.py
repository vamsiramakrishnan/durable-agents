"""The headline test: an ADK agent crashes mid-run; on re-drive the run
reconstructs and finishes — one wire, one GL batch, one effect record.

This is the treatise's treasury claim, made executable. The crash lands *after*
the bank has recorded the wire (durably, in its file ledger) but *before* the
tool returns — exactly the window the spec calls the one place uncertainty
lives. On resume, ADK re-executes the pending function call; Tape's effect ledger
already holds a `pending` row for it (keyed off ADK's stable function_call_id, so
the key matches); the tool body runs again but passes the *same* idempotency key
to the bank, which dedups. Net: the money moved once.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SDK_PY = ROOT / "tape" / "sdk" / "python"
sys.path.insert(0, str(SDK_PY))


def _run_example(env, *args, expect_crash=False, cwd=None):
    proc = subprocess.run(
        [sys.executable, "-m", "treasury.run", *args],
        env=env, cwd=cwd, capture_output=True, text=True, timeout=120)
    if expect_crash:
        assert proc.returncode != 0, f"expected the example to crash; got rc=0\n{proc.stdout}\n{proc.stderr}"
    else:
        assert proc.returncode == 0, f"example failed (rc={proc.returncode})\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    return proc


def _bank_wire_count(ledger_dir: Path) -> int:
    import json
    p = ledger_dir / "bank.json"
    if not p.exists():
        return 0
    data = json.loads(p.read_text())
    return sum(1 for k in data if not k.startswith("reverse:"))


def _query(db: str, sql: str, params=()):
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


@pytest.mark.parametrize("crash_after", ["execute_sweep", "post_gl"])
def test_crash_then_resume_makes_the_book_close_once(example_env, crash_after):
    """Crash mid-run at two different points — during the sweep, and during the
    GL post — and assert in both cases that re-driving the run produces exactly
    one wire and one GL batch. One wire, one record. (The third point — crashing
    *before* any effect's intent is written — is the trivially safe case: the
    re-drive simply starts the effect for the first time.)"""
    env = example_env["env"]
    db = example_env["db"]
    ledger_dir = example_env["ledger_dir"]

    from tape.client import TapeClient
    import tape.client as tc
    import json

    # 1. First run, crashing right after `crash_after`'s side effect lands.
    crash_env = dict(env, TAPE_CRASH_AFTER=crash_after)
    _run_example(crash_env, expect_crash=True, cwd=example_env["cwd"])

    # 2. A recoverable run should exist, with the crashed tool's effect PENDING.
    client = TapeClient(example_env["url"])
    deadline = time.time() + 10
    runs = []
    while time.time() < deadline:
        runs = list(client.list_runs_to_recover(limit=50).runs)
        runs = [r for r in runs if r.session_id == env["TAPE_SESSION"]]
        if runs:
            break
        time.sleep(0.2)
    assert runs, "no recoverable run after the crash"
    run = runs[0]
    assert run.status != tc.RUN_STATUS_TERMINAL
    run_id = run.run_id
    crashed_rows = _query(db, "SELECT status FROM tape_effects WHERE run_id=? AND tool_name=?", (run_id, crash_after))
    assert len(crashed_rows) == 1 and crashed_rows[0][0] == tc.EFFECT_STATUS_PENDING

    # 3. Resume.
    _run_example(env, "--recover", cwd=example_env["cwd"])

    # 4. One wire, one GL batch, both effects CONFIRMED, run TERMINAL.
    assert _bank_wire_count(ledger_dir) == 1, "the wire must not have happened twice"
    gl_path = ledger_dir / "gl.json"
    assert gl_path.exists() and len(json.loads(gl_path.read_text())) == 1, "exactly one GL batch"

    fresh = client.get_run(run_id)
    assert fresh.status == tc.RUN_STATUS_TERMINAL, f"run should be TERMINAL, is {fresh.status}"

    effects = {t: (s, r) for t, s, r in _query(
        db, "SELECT tool_name, status, response_json FROM tape_effects WHERE run_id=?", (run_id,))}
    assert effects["execute_sweep"][0] == tc.EFFECT_STATUS_CONFIRMED
    assert "wire_id" in (effects["execute_sweep"][1] or "")
    assert effects["post_gl"][0] == tc.EFFECT_STATUS_CONFIRMED
    assert _query(db, "SELECT COUNT(*) FROM tape_decisions WHERE run_id=?", (run_id,))[0][0] >= 1
    client.close()


def test_clean_run_records_the_full_journal(example_env):
    """A no-crash run still leaves a complete journal: decisions + effects."""
    env = example_env["env"]
    db = example_env["db"]
    ledger_dir = example_env["ledger_dir"]
    from tape.client import TapeClient
    import tape.client as tc

    _run_example(env, cwd=example_env["cwd"])

    assert _bank_wire_count(ledger_dir) == 1
    client = TapeClient(example_env["url"])
    # find the run for this test's session
    rows = _query(db, "SELECT run_id, status FROM tape_runs WHERE session_id=? ORDER BY started_at_ms DESC LIMIT 1",
                  (env["TAPE_SESSION"],))
    assert rows, "a run should have been recorded"
    run_id, status = rows[0]
    assert status == tc.RUN_STATUS_TERMINAL
    effects = _query(db, "SELECT tool_name, status FROM tape_effects WHERE run_id=?", (run_id,))
    by_tool = {t: s for t, s in effects}
    assert by_tool.get("execute_sweep") == tc.EFFECT_STATUS_CONFIRMED
    assert by_tool.get("post_gl") == tc.EFFECT_STATUS_CONFIRMED
    client.close()
