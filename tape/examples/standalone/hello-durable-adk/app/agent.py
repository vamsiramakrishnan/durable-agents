"""The smallest durable ADK agent. 15 lines."""

from __future__ import annotations

import tape
from tape.adk import durable_app
from google.adk.agents import LlmAgent
from google.adk.tools import FunctionTool


@tape.effect()
def say_hello(name: str, tool_context) -> dict:
    return {"greeting": f"hello, {name}",
            "idempotency_key": tape.idempotency_key(tool_context)}


root_agent = LlmAgent(
    name="hello_agent", model="gemini-2.5-flash",
    instruction="Greet whoever the user names, using say_hello.",
    tools=[FunctionTool(say_hello)],
)

app, runner = durable_app(name="hello", agent=root_agent,
                          budget=tape.Budget(usd_cap=5))


def build_runner():
    _, r = durable_app(name="hello", agent=root_agent,
                       budget=tape.Budget(usd_cap=5))
    return r
