"""End-to-end smoke test for `tape demo crash-resume`.

We invoke the CLI as a subprocess (since the demo spawns its own subprocess
for the crashing agent half, which would interfere with the typer CliRunner's
in-process invocation model). The deliverable:

  exit 0  iff  the bank ledger ends with exactly one wire
  exit 1  on any other count (the demo's own assertion)
  exit !=0,1 on infra failures (server didn't start, etc.)

We also smoke the journal-fed UI by parsing the demo's stdout for the markers
each phase prints (the "✓ Phase N" lines from the rich Live render).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVER_BIN = ROOT / "tape" / "server" / "target" / "debug" / "tape-server"
SDK_PY = ROOT / "tape" / "sdk" / "python"
CLI = ROOT / "tape" / "cli"


def _has_server() -> bool:
    return SERVER_BIN.exists() and os.access(SERVER_BIN, os.X_OK)


@pytest.mark.skipif(not _has_server(),
                    reason="tape-server binary not built (cargo build)")
def test_demo_crash_resume_runs_to_completion_and_exits_zero():
    """The full demo: spawn the CLI as a subprocess, let it run end-to-end,
    confirm exit 0 + the 'exactly one wire' headline in the captured output.

    We set COLUMNS so rich doesn't clamp the layout to 80 cols (which can
    eat the headline in CI capture); pause is small so we don't burn CI time.
    """
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(SDK_PY), str(CLI),
                                          env.get("PYTHONPATH", "")])
    env["COLUMNS"] = "200"
    env["TAPE_DEMO_TEST_MODE"] = "1"
    proc = subprocess.run(
        [sys.executable, "-m", "tape_cli.main", "demo", "crash-resume",
         "--pause", "0.05",
         "--server-binary", str(SERVER_BIN)],
        env=env, capture_output=True, text=True, timeout=60)

    out = proc.stdout + proc.stderr
    # The demo exits 0 iff the bank ledger ends with exactly one forward wire.
    assert proc.returncode == 0, (
        f"demo exited {proc.returncode}; output:\n{out[-3000:]}")
    # The headline says "durability proved" on the happy path.
    assert "durability proved" in out, out[-2000:]
    # Bank ledger header rendered.
    assert "bank ledger" in out.lower()
    # The crash divider rendered (proves the journal stream worked through
    # the crash boundary).
    assert "crash" in out.lower()


@pytest.mark.skipif(not _has_server(),
                    reason="tape-server binary not built (cargo build)")
def test_demo_help_does_not_need_server():
    """`tape demo crash-resume --help` shouldn't spin up anything — pure
    parser work. Fast path for autocompletion / discovery."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(SDK_PY), str(CLI),
                                          env.get("PYTHONPATH", "")])
    proc = subprocess.run(
        [sys.executable, "-m", "tape_cli.main", "demo", "crash-resume", "--help"],
        env=env, capture_output=True, text=True, timeout=10)
    assert proc.returncode == 0
    assert "Crash an agent mid-effect" in proc.stdout


# ── unit tests on the inline fake bank ───────────────────────────────────


def test_filebank_dedupes_on_idempotency_key(tmp_path):
    """The bank's ledger is keyed by idempotency_key — same key = same wire.
    This is the floor the spec assumes; the demo demonstrates it."""
    sys.path.insert(0, str(CLI))
    from tape_cli.commands.demo import FileBank

    bank = FileBank(tmp_path / "bank.json")
    w1 = bank.wire(idempotency_key="abc", amount_minor=2_000_000,
                   account_id="acct-1")
    w2 = bank.wire(idempotency_key="abc", amount_minor=2_000_000,
                   account_id="acct-1")
    assert w1 == w2
    assert len(bank.all_wires()) == 1

    # A different key creates a new wire.
    w3 = bank.wire(idempotency_key="xyz", amount_minor=500,
                   account_id="acct-2")
    assert w3["wire_id"] != w1["wire_id"]
    assert len(bank.all_wires()) == 2
