"""Tape quickstart — Python. The same scenario as the TS / Go / Java siblings.

    python examples/quickstart.py
"""

from __future__ import annotations

import json
import os
import sys
import time

# Make the SDK importable from a fresh clone without installing.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tape", "sdk", "python"))

from tape.client import (                          # noqa: E402
    TapeClient,
    EFFECT_STATUS_CONFIRMED,
)

LANG = "python"
URL  = os.environ.get("TAPE_URL", "tape://127.0.0.1:7878")


def main() -> int:
    print(f"[quickstart/{LANG}] connecting to {URL}")
    with TapeClient(URL) as c:
        invocation = f"qs-{LANG}-{int(time.time())}"
        run = c.begin_run(
            app_name="quickstart", user_id="quickstart-user",
            session_id=invocation, invocation_id=invocation,
            lease_owner=f"qs-{LANG}", lease_ttl_ms=60_000,
        )
        print(f"[quickstart/{LANG}] begin_run    → run-id={run.run_id}")

        c.record_decision(
            run_id=run.run_id, decision_index=0,
            model="quickstart", request_json="{}", response_json="{}",
        )
        print(f"[quickstart/{LANG}] record_decision  decision_index=0")

        be = c.begin_effect(
            run_id=run.run_id, decision_index=0,
            tool_name="hello", call_index=0,
            request_json=json.dumps({"who": LANG}),
        )
        print(f"[quickstart/{LANG}] begin_effect   → key={be.idempotency_key}  status={be.status}")

        c.complete_effect(
            run_id=run.run_id, idempotency_key=be.idempotency_key,
            status=EFFECT_STATUS_CONFIRMED,
            response_json=json.dumps({"ok": True, "who": LANG}),
        )
        print(f"[quickstart/{LANG}] complete_effect → status=CONFIRMED")

        eff = c.get_effect(run_id=run.run_id, idempotency_key=be.idempotency_key).effect
        print(f"[quickstart/{LANG}] get_effect     status={eff.status}  response={eff.response_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
