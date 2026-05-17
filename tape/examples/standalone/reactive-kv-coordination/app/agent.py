"""Two roles coordinating through Tape's KV. The producer writes FX rate
transitions; the consumer watches the key and re-prices on each transition.

Run the producer:  python -m app.producer
Run the consumer:  python -m app.consumer
"""

from __future__ import annotations

import tape
from tape.adk import durable_app
from google.adk.agents import LlmAgent
from google.adk.tools import FunctionTool


@tape.effect()
def set_fx_rate(symbol: str, rate: float, tool_context) -> dict:
    rec = tape.set_value("fx", symbol, rate, writer=tape.run_id_of(tool_context))
    return {"symbol": symbol, "rate": rate, "version": rec.version}


@tape.effect()
def get_fx_rate(symbol: str, tool_context) -> dict:
    resp = tape.get_value("fx", symbol)
    return {"symbol": symbol, "found": resp.found,
            "value": resp.value.value_json if resp.found else None}


root_agent = LlmAgent(
    name="kv_agent", model="gemini-2.5-flash",
    instruction="You're a small KV agent — call set_fx_rate / get_fx_rate as asked.",
    tools=[FunctionTool(set_fx_rate), FunctionTool(get_fx_rate)],
)


def build_runner():
    _app, runner = durable_app(name="kv", agent=root_agent,
                               budget=tape.Budget(usd_cap=5))
    return runner
