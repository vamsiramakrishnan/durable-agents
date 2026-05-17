"""Tools for the {{ name }} agent.

Two patterns are shown:

  * a plain `@tape.effect(...)` tool whose body does its own (idempotent) IO;
  * a `@tape.outbox_tool(...)` tool that returns a JSON intent and lets the
    outbox reactor dispatch it. Use this for non-idempotent upstreams.

Replace these stubs with your real tools.
"""

from __future__ import annotations

import tape


def _say_hello(name: str) -> dict:
    return {"greeting": f"hello, {name}", "from": "{{ name }}_agent"}


@tape.effect()
def say_hello(name: str, tool_context) -> dict:
    """An idempotent example — safe to retry."""
    key = tape.idempotency_key(tool_context)
    out = _say_hello(name)
    out["idempotency_key"] = key
    return out


def all_tools():
    return [say_hello]
