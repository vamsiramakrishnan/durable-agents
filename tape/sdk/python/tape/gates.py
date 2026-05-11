"""Gates — a human-or-event approval is a durable suspend-until-signal.

`tape.gate_tool("cfo-approval")` returns a `LongRunningFunctionTool` you add to
the agent. When the model calls it, the run parks (`status = WAITING`); from
anywhere — a webhook, a CLI, another service — `tape.send_signal("cfo-approval",
run_id=..., resolution={...})` releases it, the recovery loop re-invokes the run,
the tool function re-runs, finds the delivered signal, and returns its resolution
to the model.
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

from .client import TapeClient, DEFAULT_URL
from ._gen import tape_pb2 as pb


class AckLost(Exception):
    """Raise from a tool body when a side effect's acknowledgement was lost
    (the request may or may not have landed). Tape records the effect as
    `UNKNOWN`; the reconciler resolves it by asking the counterparty."""


_PENDING = object()


def _client() -> TapeClient:
    return TapeClient(os.environ.get("TAPE_URL", DEFAULT_URL))


def gate(gate_name: str, *, tool_context: Any, risk: str = "irreversible",
         payload: Optional[dict] = None) -> dict:
    """The body of a gate tool. Returns the resolution dict if a signal has been
    delivered; otherwise parks the run and returns a pending marker so ADK
    suspends the invocation."""
    run_id = tool_context.state.get("temp:_tape_run_id", "")
    if not run_id:
        return {"resolved": True, "gate": gate_name, "note": "tape not active; auto-resolved"}
    c = _client()
    try:
        resp = c.await_signal(run_id=run_id, gate_name=gate_name,
                              payload_json=json.dumps({"risk": risk, **(payload or {})}))
        if resp.delivered:
            try:
                resolution = json.loads(resp.resolution_json) if resp.resolution_json else {}
            except Exception:
                resolution = {}
            return {"resolved": True, "gate": gate_name, **resolution}
        return {"resolved": False, "status": "pending", "gate": gate_name,
                "note": f"awaiting signal '{gate_name}'"}
    finally:
        c.close()


def gate_tool(gate_name: str, *, risk: str = "irreversible"):
    """Build a `LongRunningFunctionTool` for this gate."""
    from google.adk.tools import LongRunningFunctionTool

    def _gate(tool_context) -> dict:  # the function name shows up to the model as the tool name
        return gate(gate_name, tool_context=tool_context, risk=risk)

    _gate.__name__ = f"await_{gate_name.replace('-', '_')}"
    _gate.__doc__ = f"Pause until the '{gate_name}' approval/event arrives, then return its resolution."
    return LongRunningFunctionTool(func=_gate)
