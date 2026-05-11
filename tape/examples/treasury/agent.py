"""The treasury agent, Tape-backed.

The tool bodies are plain — `bank.wire(...)`, `broker.place(...)`, `gl.post(...)`
— with `@tape.effect(...)` declaring the inverse and the reconciler's status
check. The ceremony from the treatise (idempotency key, journal check, intent
write, three-way outcome, session-state write, journal write, compensation
register, provenance) is all in `TapePlugin` and the Tape server.

The model here is a tiny *scripted policy* (`ScriptedLlm`) so the example runs
without an API key and the kill-and-resume test is deterministic. Swap it for
`"gemini-2.5-pro"` to run it for real.
"""

from __future__ import annotations

from typing import AsyncGenerator

from google.adk.agents import LlmAgent
from google.adk.tools import FunctionTool
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.genai import types

import tape

from .fake_bank import bank, broker, gl

POLICY_VERSION = "cfo-policy-2026.05"


# ── tools — plain bodies + a decorator ──────────────────────────────────────

def reverse_wire(wire_id: str, **kwargs) -> dict:
    return {"reversal_id": bank.reverse(wire_id)}


def reverse_hedge(order_id: str, **kwargs) -> dict:
    return {"note": f"hedge {order_id} would be unwound here"}


@tape.effect(compensate=reverse_wire, status_check=bank.wire_status)
def execute_sweep(account_id: str, amount_minor: int, target_mmf: str,
                  rationale: str, tool_context) -> dict:
    """Sweep excess cash to a whitelisted money-market fund per CFO policy."""
    key = tape.idempotency_key(tool_context)
    wire_id = bank.wire(account_id, amount_minor, target_mmf, idempotency_key=key)
    return {"wire_id": wire_id, "amount_minor": amount_minor, "target_mmf": target_mmf}


@tape.effect(compensate=reverse_hedge, status_check=broker.order_status)
def execute_hedge(notional_minor: int, instrument: str, rationale: str, tool_context) -> dict:
    """Place the FX hedge for today's swept position."""
    key = tape.idempotency_key(tool_context)
    order_id = broker.place(instrument, notional_minor, idempotency_key=key)
    return {"order_id": order_id, "instrument": instrument, "notional_minor": notional_minor}


@tape.effect()
def post_gl(entries: list, rationale: str, tool_context) -> dict:
    """Post the general-ledger batch for the day's treasury activity."""
    key = tape.idempotency_key(tool_context)
    batch_id = gl.post(entries, idempotency_key=key)
    return {"batch_id": batch_id, "n_entries": len(entries)}


# ── the model: a deterministic policy over the conversation state ───────────

def _has_response_for(llm_request: LlmRequest, tool_name: str) -> bool:
    for content in (llm_request.contents or []):
        for part in (getattr(content, "parts", None) or []):
            fr = getattr(part, "function_response", None)
            if fr is not None and getattr(fr, "name", None) == tool_name:
                return True
    return False


def _fn_call(name: str, args: dict) -> LlmResponse:
    return LlmResponse(content=types.Content(
        role="model", parts=[types.Part(function_call=types.FunctionCall(name=name, args=args))]))


def _say(text: str) -> LlmResponse:
    return LlmResponse(content=types.Content(role="model", parts=[types.Part(text=text)]))


class ScriptedLlm(BaseLlm):
    """Responds to the conversation, not a counter — so a re-drive that re-asks
    the same question gets the same answer, and the run reconstructs cleanly."""

    model: str = "scripted-treasury-policy"

    @staticmethod
    def supported_models() -> list[str]:
        return [r"scripted-.*"]

    async def generate_content_async(
        self, llm_request: LlmRequest, stream: bool = False
    ) -> AsyncGenerator[LlmResponse, None]:
        if not _has_response_for(llm_request, "execute_sweep"):
            yield _fn_call("execute_sweep", {
                "account_id": "ACME-OPS-001", "amount_minor": 200_000_000,
                "target_mmf": "MMF-WHITELIST-A",
                "rationale": "Operating balance exceeds the CFO policy cap; sweep the excess."})
            return
        if not _has_response_for(llm_request, "post_gl"):
            yield _fn_call("post_gl", {
                "entries": [{"acct": "1010", "cr": 200_000_000}, {"acct": "1500", "dr": 200_000_000}],
                "rationale": "Record the sweep in the general ledger."})
            return
        yield _say("Book closed for the day. One sweep, one GL batch — recorded.")


# ── the agent ───────────────────────────────────────────────────────────────

def build_agent() -> LlmAgent:
    return LlmAgent(
        name="treasury_agent",
        model=ScriptedLlm(),
        instruction=("Apply the CFO policy in session.state to today's positions. "
                     "Sweep excess cash, then post the general ledger."),
        tools=[FunctionTool(execute_sweep), FunctionTool(execute_hedge), FunctionTool(post_gl)],
    )
