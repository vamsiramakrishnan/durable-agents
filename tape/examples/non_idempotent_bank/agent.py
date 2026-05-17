"""The agent — a tiny scripted ADK runner so the example runs without an API key.

The tool is `wire_money`, declared as `@tape.outbox_tool(...)` — its body
builds the intent payload only and never touches the bank. TapePlugin records
the intent (with the business_key for cross-run dedupe); the outbox reactor
performs the actual wire via the registered connector.
"""

from __future__ import annotations

import os
from typing import AsyncGenerator

from google.adk.agents import LlmAgent
from google.adk.apps import App
from google.adk.apps.app import ResumabilityConfig
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.adk.runners import Runner
from google.adk.tools import FunctionTool
from google.genai import types

import tape
from tape.adk import TapePlugin, TapeSessionService

# Importing the connectors module registers BankConnector with tape.connectors.
# The agent process itself does NOT need this — the outbox reactor does. We
# import it here so a single-process demo (the `run.py --inline-reactor` mode)
# can call it from the same process.
from . import connectors  # noqa: F401


def _business_key(*, account: str, amount_minor: int, beneficiary: str, date: str,
                  tool_context=None) -> str:
    """The bank's dedupe key — the agent owns this contract. Stable across
    re-drives because the inputs are stable (they came from the model's
    journaled decision)."""
    return f"{account}:{amount_minor}:{beneficiary}:{date}"


@tape.outbox_tool(
    connector="bank.wire",
    semantics="non_idempotent",
    business_key=_business_key,
)
def wire_money(account: str, amount_minor: int, beneficiary: str, date: str,
               tool_context) -> dict:
    """Build the intent for a wire. The tool body MUST NOT call the bank —
    `tape.outbox_tool(dispatch='outbox')` means TapePlugin returns a synthetic
    "accepted" marker to the agent and the outbox reactor performs the wire.

    Returning a payload here is fine — it ends up as the journaled
    `request_json` the connector reads at dispatch time."""
    return {
        "account": account,
        "amount_minor": amount_minor,
        "beneficiary": beneficiary,
        "date": date,
    }


# ── a deterministic scripted model so the demo runs without an API key ─────

class _ScriptedLlm(BaseLlm):
    """One model turn: call wire_money(...). After the tool returns, finish."""
    model: str = "scripted"

    async def generate_content_async(self, llm_request: LlmRequest,
                                      stream: bool = False) -> AsyncGenerator[LlmResponse, None]:
        for content in (llm_request.contents or []):
            for part in (getattr(content, "parts", None) or []):
                if getattr(part, "function_response", None) is not None:
                    yield LlmResponse(content=types.Content(
                        role="model",
                        parts=[types.Part(text="wire requested; tape will dispatch it durably")]))
                    return
        yield LlmResponse(content=types.Content(
            role="model",
            parts=[types.Part(function_call=types.FunctionCall(
                name="wire_money",
                args={"account": "acct-001", "amount_minor": 100000,
                       "beneficiary": "vendor-x", "date": "2026-05-17"}))]))


agent = LlmAgent(
    name="treasurer",
    model=_ScriptedLlm(),
    instruction="Wire money via the outbox-backed tool.",
    tools=[FunctionTool(func=wire_money)],
)


def build_runner(*, url: str = "", session_service=None) -> Runner:
    """Build the ADK Runner with the Tape plugin wired in. `url` defaults to
    TAPE_URL from the environment."""
    url = url or os.environ.get("TAPE_URL", "tape://localhost:7878")
    app = App(
        name="non_idempotent_bank",
        root_agent=agent,
        plugins=[TapePlugin(url=url)],
        resumability_config=ResumabilityConfig(is_resumable=True),
    )
    return Runner(
        app=app,
        session_service=session_service or TapeSessionService(url),
    )
