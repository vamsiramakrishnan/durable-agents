"""Tests for `tape doctor --live` — operational triage against a running server."""

from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SDK_PY = ROOT / "tape" / "sdk" / "python"
CLI = ROOT / "tape" / "cli"
sys.path.insert(0, str(SDK_PY))
sys.path.insert(0, str(CLI))


def _run_doctor(url: str, *extra: str):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(SDK_PY), str(CLI),
                                          env.get("PYTHONPATH", "")])
    env["COLUMNS"] = "200"
    return subprocess.run(
        [sys.executable, "-m", "tape_cli.main", "doctor", "--live",
         "--url", url, *extra],
        env=env, capture_output=True, text=True, timeout=15)


# ── snapshot rendering on a clean server ─────────────────────────────────


def test_live_triage_clean_server_exits_zero(tape_server):
    """A pristine server has nothing to flag — every section says '✓ none',
    the summary header shows all green, exit code is 0."""
    r = _run_doctor(tape_server["url"])
    out = r.stdout + r.stderr
    assert r.returncode == 0, out[-2000:]
    assert "operational triage" in out
    assert "runs needing recovery" in out
    assert "effects UNKNOWN" in out
    assert "obligations STUCK" in out
    assert "outbox" in out.lower()
    assert "timers" in out.lower()
    # The clean indicator
    assert "✓" in out


# ── UNKNOWN effect bubbles up + flips the exit code ─────────────────────


def test_live_triage_unknown_effect_bubbles_exit_one(tape_server):
    """An UNKNOWN effect is the loud failure mode — `tape doctor --live`
    exits 1 so CI / monitoring can alert on it without parsing output."""
    from tape.client import TapeClient
    from tape._gen.tape_pb2 import EFFECT_STATUS_UNKNOWN
    with TapeClient(tape_server["url"]) as c:
        r = c.begin_run(app_name="t", user_id="u", session_id="s",
                         invocation_id=f"inv-{uuid.uuid4().hex[:8]}",
                         lease_owner="x")
        c.record_decision(run_id=r.run_id, decision_index=0, model="m")
        eff = c.begin_effect(run_id=r.run_id, decision_index=0,
                              tool_name="bank.x", call_index=0)
        c.complete_effect(run_id=r.run_id,
                          idempotency_key=eff.idempotency_key,
                          status=EFFECT_STATUS_UNKNOWN, error_json='{}')

    res = _run_doctor(tape_server["url"])
    out = res.stdout + res.stderr
    assert res.returncode == 1, out[-2000:]
    # The UNKNOWN counter in the summary header is non-zero.
    assert "effects UNKNOWN" in out
    # The effect itself rendered in the details table.
    assert "UNKNOWN" in out
    assert eff.idempotency_key in out or eff.idempotency_key[:24] in out


# ── stuck obligation bubbles up to exit code 2 ──────────────────────────


def test_live_triage_stuck_obligation_exits_two(tape_server):
    """STUCK obligations are even louder than UNKNOWN — a compensation has
    given up retries and needs a human. Exit code 2."""
    from tape.client import TapeClient
    from tape._gen.tape_pb2 import OBLIGATION_STATUS_STUCK
    with TapeClient(tape_server["url"]) as c:
        r = c.begin_run(app_name="t", user_id="u", session_id="s",
                         invocation_id=f"inv-{uuid.uuid4().hex[:8]}",
                         lease_owner="x")
        ob = c.register_compensation(
            run_id=r.run_id, effect_key="some-key",
            kind="reverse_wire", payload_json="{}")
        # Mark it STUCK via the resolve endpoint (the operator-triage state).
        c.resolve_obligation(run_id=r.run_id, obligation_seq=ob.seq,
                             status=OBLIGATION_STATUS_STUCK,
                             result_json='{"reason":"test"}')

    res = _run_doctor(tape_server["url"])
    out = res.stdout + res.stderr
    assert res.returncode == 2, out[-2000:]
    assert "STUCK" in out
    assert "reverse_wire" in out


# ── --watch wires up without crashing ───────────────────────────────────


def test_live_triage_watch_runs_without_crashing(tape_server):
    """`--watch` opens a Rich.Live region that refreshes until SIGINT.
    Rich.Live owns the terminal and clears its scrollback on exit, so
    we can't reliably capture rendered output from a terminated subprocess.
    Instead: confirm the command stays alive for ~2 seconds (which it
    wouldn't if Live initialization crashed), then terminate cleanly."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(SDK_PY), str(CLI),
                                          env.get("PYTHONPATH", "")])
    env["COLUMNS"] = "200"
    p = subprocess.Popen(
        [sys.executable, "-m", "tape_cli.main", "doctor", "--live",
         "--watch", "--interval", "1.0",
         "--url", tape_server["url"]],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        text=True)
    # Let two refresh intervals elapse — proves the loop kicked over.
    time.sleep(2.0)
    # If the subprocess died, p.poll() returns a non-None exit code.
    early_exit = p.poll()
    p.terminate()
    try:
        _, stderr = p.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill()
        _, stderr = p.communicate()
    assert early_exit is None, (
        f"--watch subprocess exited early with rc={early_exit}; "
        f"stderr:\n{stderr[-1500:]}")


# ── env (non-live) path still works ─────────────────────────────────────


def test_doctor_help_lists_live_flag():
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(SDK_PY), str(CLI),
                                          env.get("PYTHONPATH", "")])
    # Force Rich/Typer to render --help as plain text. Without this, CI's
    # FORCE_COLOR-style env makes Typer wrap each `--flag` in ANSI styles
    # (e.g. `\x1b[1;36m-\x1b[0m\x1b[1;36m-live\x1b[0m`), so the substring
    # "--live" no longer appears contiguously in stdout.
    env["NO_COLOR"] = "1"
    env["TERM"] = "dumb"
    p = subprocess.run(
        [sys.executable, "-m", "tape_cli.main", "doctor", "--help"],
        env=env, capture_output=True, text=True, timeout=10)
    assert p.returncode == 0
    assert "--live" in p.stdout
    assert "--watch" in p.stdout
    assert "--pending-threshold-ms" in p.stdout
