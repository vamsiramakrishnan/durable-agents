"""End-to-end demo: run the agent against a real Tape server, watch the
non-idempotent bank get called exactly once even when the dispatcher
"crashes" mid-call.

    # in one terminal:
    tape/server/target/debug/tape-server --listen 127.0.0.1:7878 --store sqlite:/tmp/tape-nonidem.db

    # in another:
    cd tape && TAPE_URL=tape://127.0.0.1:7878 TAPE_EXAMPLE_DIR=/tmp/tape-nonidem \
        python -m examples.non_idempotent_bank.run

Options:
  --inject-unknown   the connector returns `unknown` once (lost ack); the
                     reconciler resolves via observe()
  --crash-after-wire the connector raises after writing the wire; the
                     dispatcher's lease expires, a later dispatcher reclaims
                     and observe() short-circuits — no second wire
  --reset            wipe the bank ledger and the Tape DB before starting

Either way: the bank ledger ends up with EXACTLY ONE wire for the same
business key.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
import time
from pathlib import Path

import tape
from tape.client import TapeClient
from tape.reactors import outbox as outbox_reactor
from tape.reactors import reconcile_once

from .agent import build_runner
from .bank import bank


def _async_run(runner, *, user_id: str, session_id: str, invocation_id: str,
               message: str = "wire the money"):
    from google.adk.runners import RunConfig
    from google.genai import types

    async def go():
        sess = await runner.session_service.create_session(
            app_name=runner.app.name, user_id=user_id, session_id=session_id)
        content = types.Content(role="user", parts=[types.Part(text=message)])
        events = runner.run_async(
            user_id=user_id, session_id=sess.id,
            new_message=content,
            run_config=RunConfig(),
        )
        async for _ in events:
            pass
    asyncio.run(go())


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="non_idempotent_bank.run",
                                description="Outbox + reconciliation demo for a non-idempotent bank.")
    p.add_argument("--url", default=os.environ.get("TAPE_URL", "tape://localhost:7878"))
    p.add_argument("--reset", action="store_true",
                   help="wipe the example dir + bank ledger before starting")
    p.add_argument("--inject-unknown", action="store_true",
                   help="connector returns `unknown` once (simulated lost ack)")
    p.add_argument("--crash-after-wire", action="store_true",
                   help="connector raises after writing the wire")
    args = p.parse_args(argv)

    ledger_dir = Path(os.environ.get("TAPE_EXAMPLE_DIR", "/tmp/tape-nonidem"))
    if args.reset and ledger_dir.exists():
        shutil.rmtree(ledger_dir, ignore_errors=True)
    ledger_dir.mkdir(parents=True, exist_ok=True)

    runner = build_runner(url=args.url)

    # 1. Agent run — produces a PENDING outbox effect for the wire. No bank
    #    call yet.
    user_id = "cfo"
    session_id = f"sess-{int(time.time())}"
    invocation_id = f"inv-{int(time.time())}"
    print(f"[demo] running agent: session={session_id} invocation={invocation_id}")
    _async_run(runner, user_id=user_id, session_id=session_id, invocation_id=invocation_id)
    print(f"[demo] bank wires after agent run: {len(bank.all_wires())}  (expect 0)")

    # 2. Outbox dispatcher — runs the connector, may fault per flags.
    if args.inject_unknown:
        os.environ["TAPE_BANK_DISPATCH_INJECT_UNKNOWN"] = "1"
    if args.crash_after_wire:
        os.environ["TAPE_BANK_DISPATCH_CRASH_AFTER_WIRE"] = "1"

    print("[demo] outbox dispatcher tick #1")
    try:
        r1 = outbox_reactor.outbox_dispatch_once(args.url)
    except Exception as ex:
        print(f"[demo]   dispatcher crashed: {ex}")
        r1 = [{"status": "crashed", "error": str(ex)}]
    print(f"[demo]   results: {json.dumps(r1, default=str)}")
    print(f"[demo]   bank wires: {len(bank.all_wires())}")

    # 3. Clear the fault flags and run the second dispatcher tick — the
    #    safety claim is that a NON_IDEMPOTENT effect parked as UNKNOWN
    #    is NOT re-dispatched.
    os.environ.pop("TAPE_BANK_DISPATCH_INJECT_UNKNOWN", None)
    os.environ.pop("TAPE_BANK_DISPATCH_CRASH_AFTER_WIRE", None)
    # If --crash-after-wire was used, sleep enough for the lease to expire.
    if args.crash_after_wire:
        print("[demo] waiting for the dispatch lease to expire (61s)...")
        time.sleep(61)
    print("[demo] outbox dispatcher tick #2 (no faults)")
    r2 = outbox_reactor.outbox_dispatch_once(args.url)
    print(f"[demo]   results: {json.dumps(r2, default=str)}")
    print(f"[demo]   bank wires: {len(bank.all_wires())}")

    # 4. Reconciler — for UNKNOWN effects, asks the bank via observe().
    print("[demo] reconciler tick")
    rec = reconcile_once(args.url)
    print(f"[demo]   results: {json.dumps(rec, default=str)}")

    # 5. Final ledger.
    wires = bank.all_wires()
    print(f"[demo] bank ledger ({len(wires)} entries):")
    for w in wires:
        print(f"  {w}")
    if len([w for w in wires if not w.get('reverses')]) != 1:
        print("[demo] FAIL: expected exactly one forward wire")
        return 1
    print("[demo] OK: exactly one forward wire, even under the injected fault")
    return 0


if __name__ == "__main__":
    sys.exit(main())
