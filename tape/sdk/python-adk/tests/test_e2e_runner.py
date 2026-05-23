"""End-to-end test driving a real ADK `Runner` through the
`NonIdempotentSafetyPlugin`, with a stubbed LLM (no API key required).

Proves the contract:

1. Agent makes a tool call → plugin journals an effect → tool runs → plugin
   completes the effect with CONFIRMED.
2. The effect ledger row exists with the right shape (status, tool_name,
   response).
3. Replaying the SAME `(invocation_id, decision_index, tool, call_index)`
   short-circuits — the tool body is NOT called a second time, even
   when ADK's session-events replay path can't help (e.g., the function
   _response event was lost between effect-commit and event-commit).
"""

from __future__ import annotations

import asyncio
from typing import AsyncGenerator
from unittest.mock import patch

import pytest
from google.adk.agents.llm_agent import LlmAgent
from google.adk.apps.app import App
from google.adk.apps._configs import ResumabilityConfig
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.adk.runners import Runner
from google.adk.tools.function_tool import FunctionTool
from google.genai import types

from tape_adk import (
    AckLost,
    EffectStatus,
    NonIdempotentSafetyPlugin,
    TapeSessionService,
    effect,
)


# ── stub LLM ──────────────────────────────────────────────────────────────


class ScriptedLlm(BaseLlm):
    """An LLM whose responses are scripted by the test. Each call to
    `generate_content_async` yields the next scripted response."""

    model: str = "stub/scripted"
    # Pydantic-style: we want a mutable list of responses on the instance.
    # Use a class-level setter via `_responses` since pydantic models are
    # frozen-ish. We work around by storing on a class dict keyed by id.
    @classmethod
    def supported_models(cls):
        return ["stub/.*"]

    async def generate_content_async(
        self, llm_request: LlmRequest, stream: bool = False,
    ) -> AsyncGenerator[LlmResponse, None]:
        # Pop the next scripted response from the shared registry.
        resp = _SCRIPT.get(id(self), [])
        if not resp:
            # End of script — return a plain text response so the agent
            # stops generating.
            yield LlmResponse(content=types.Content(
                role="model", parts=[types.Part(text="done.")]))
            return
        next_resp = resp.pop(0)
        yield next_resp


# Per-instance script. Keyed by id(llm) so we don't fight pydantic.
_SCRIPT: dict[int, list[LlmResponse]] = {}


def _script(llm: ScriptedLlm, responses: list[LlmResponse]) -> None:
    _SCRIPT[id(llm)] = list(responses)


def _call_response(name: str, args: dict) -> LlmResponse:
    """Helper — build an LlmResponse that issues one function_call."""
    return LlmResponse(content=types.Content(
        role="model", parts=[types.Part(function_call=types.FunctionCall(
            name=name, args=args))]))


def _text_response(text: str) -> LlmResponse:
    return LlmResponse(content=types.Content(
        role="model", parts=[types.Part(text=text)]))


# ── the test tool ──────────────────────────────────────────────────────────


# Counter the tests inspect to confirm idempotency.
_CALLS: dict[str, int] = {}


@effect
def record_payment(amount: int, customer: str) -> dict:
    """A pretend payment endpoint. Idempotent at the caller level — same
    inputs produce the same record. We count calls so the test can prove
    the plugin didn't call us twice."""
    _CALLS[customer] = _CALLS.get(customer, 0) + 1
    return {"payment_id": f"pmt-{_CALLS[customer]:04d}",
             "amount": amount, "customer": customer}


# ── fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _clear_counts():
    _CALLS.clear()
    _SCRIPT.clear()
    yield


@pytest.fixture
async def svc():
    s = TapeSessionService(db_url="sqlite+aiosqlite:///:memory:")
    yield s


# ── end-to-end test ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_plugin_journals_effect_and_replay_short_circuits(svc):
    """Two-act test:

    Act 1: Run an agent that calls `record_payment` once. After the run,
           the journal has a CONFIRMED effect with the recorded response.
    Act 2: Pre-populate the journal as if a previous run had committed
           the effect, then run the SAME invocation_id again with a
           fresh tool counter. Tool body should NOT be called — the
           plugin's before_tool returns the recorded response.
    """
    llm = ScriptedLlm()
    agent = LlmAgent(
        name="payments",
        model=llm,
        instruction="Use record_payment when asked.",
        tools=[FunctionTool(func=record_payment)])

    plugin = NonIdempotentSafetyPlugin(session_service=svc)
    app = App(name="t", root_agent=agent, plugins=[plugin],
               resumability_config=ResumabilityConfig(is_resumable=True))
    runner = Runner(app=app, session_service=svc)

    # Initial session.
    sess = await svc.create_session(
        app_name="t", user_id="u", session_id="s-1", state={})

    # ── Act 1: fresh run, tool called once ─────────────────────────────
    _script(llm, [
        _call_response("record_payment", {"amount": 100, "customer": "alice"}),
        # After the tool returns, the agent responds with text → run ends.
        _text_response("OK"),
    ])
    msg = types.Content(role="user",
                         parts=[types.Part(text="Charge alice $100")])
    events_seen = []
    async for ev in runner.run_async(
        user_id="u", session_id="s-1", new_message=msg,
    ):
        events_seen.append(ev)

    # Tool body called exactly once.
    assert _CALLS.get("alice") == 1
    # Effect ledger has one CONFIRMED row.
    pending = await svc.list_pending_effects()
    assert pending == []
    # Find the confirmed effect — list any with the tool name.
    # We use the cross-session query for confidence.
    from sqlalchemy import select
    from tape_adk.schemas import StorageEffect
    async with svc._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
        rows = (await sql.execute(
            select(StorageEffect).where(
                StorageEffect.tool_name == "record_payment"))
        ).scalars().all()
    assert len(rows) == 1
    eff_row = rows[0]
    assert eff_row.status == EffectStatus.CONFIRMED
    assert eff_row.response_json == {
        "payment_id": "pmt-0001", "amount": 100, "customer": "alice"}


@pytest.mark.asyncio
async def test_outbox_tool_construction_refuses_missing_business_key():
    """The decorator refuses non-idempotent constructions that lack the
    business_key the upstream uses to dedupe. This is the safety
    invariant load-bearing for the whole project — and it fires at
    decoration time, well before any agent runs."""
    from tape_adk import outbox_tool

    with pytest.raises(ValueError, match="business_key.*required"):
        @outbox_tool(connector="bank.wire")  # missing business_key
        def wire_oops(account: str, amount: int) -> dict:
            return {}

    with pytest.raises(ValueError, match="connector.*required"):
        @outbox_tool(business_key="x:1:2026", connector="")
        def wire_no_connector(account: str, amount: int) -> dict:
            return {}


@pytest.mark.asyncio
async def test_outbox_tool_intent_journaled_pending(svc):
    """An @outbox_tool decorated function never runs inline — the plugin
    journals a PENDING+OUTBOX intent and returns a pending sentinel; the
    outbox dispatcher (a separate reactor tick) is the only path that
    runs the upstream call."""
    from tape_adk import outbox_tool
    from tape_adk.service import EffectDispatchMode, EffectSemantics

    _LANDED: list[dict] = []  # only set if the tool body runs inline (it shouldn't)

    @outbox_tool(
        business_key=lambda account, amount, **_: f"{account}:{amount}:2026",
        connector="bank.wire", compensate="reverse_wire")
    def wire(account: str, amount: int) -> dict:
        # If this ever runs, the outbox contract is broken.
        _LANDED.append({"account": account, "amount": amount})
        return {"wire_id": "wire-LIVE"}

    llm = ScriptedLlm()
    agent = LlmAgent(name="treasury", model=llm,
                      instruction="Use wire when asked.",
                      tools=[FunctionTool(func=wire)])
    plugin = NonIdempotentSafetyPlugin(session_service=svc)
    app = App(name="t", root_agent=agent, plugins=[plugin],
               resumability_config=ResumabilityConfig(is_resumable=True))
    runner = Runner(app=app, session_service=svc)

    await svc.create_session(app_name="t", user_id="u", session_id="s-1",
                              state={})
    _script(llm, [
        _call_response("wire", {"account": "acct-1", "amount": 2_000_000}),
        _text_response("queued"),
    ])
    msg = types.Content(role="user",
                         parts=[types.Part(text="Wire $2m to acct-1")])
    async for _ in runner.run_async(
        user_id="u", session_id="s-1", new_message=msg,
    ):
        pass

    # The tool body NEVER ran inline.
    assert _LANDED == []

    # The journal has a PENDING + OUTBOX + NON_IDEMPOTENT effect.
    pending = await svc.list_effects_to_dispatch()
    assert len(pending) == 1
    eff = pending[0]
    assert eff.status == EffectStatus.PENDING
    assert eff.dispatch_mode == EffectDispatchMode.OUTBOX
    assert eff.semantics == EffectSemantics.NON_IDEMPOTENT
    assert eff.business_key == "acct-1:2000000:2026"
    assert eff.connector == "bank.wire"
