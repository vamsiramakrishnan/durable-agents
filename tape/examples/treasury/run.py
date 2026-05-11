"""Run the treasury agent against a Tape server.

    # one-shot:
    python -m tape.examples.treasury.run

    # crash mid-run (the kill-and-resume demo uses this):
    TAPE_CRASH_AFTER=execute_sweep python -m tape.examples.treasury.run

    # then resume the crashed run:
    python -m tape.examples.treasury.run --recover

Environment:
    TAPE_URL          tape://host:port            (default tape://localhost:7878)
    TAPE_APP          app name                    (default "treasury")
    TAPE_USER         user id                     (default "cfo")
    TAPE_SESSION      session id                  (default a fixed id so re-runs share it)
    TAPE_CRASH_AFTER  a tool name; os._exit() after that tool's side effect lands
"""

from __future__ import annotations

import asyncio
import os
import sys

from google.adk.runners import Runner
from google.adk.apps import App
from google.adk.apps.app import ResumabilityConfig
from google.genai import types

import tape
from tape.adk import TapePlugin, TapeSessionService

from .agent import build_agent, POLICY_VERSION
from .fake_bank import bank, gl, reset_ledgers

APP = os.environ.get("TAPE_APP", "treasury")
USER = os.environ.get("TAPE_USER", "cfo")
SESSION = os.environ.get("TAPE_SESSION", "demo-session-1")
URL = os.environ.get("TAPE_URL", "tape://localhost:7878")


def _build_runner() -> Runner:
    app = App(
        name=APP, root_agent=build_agent(),
        plugins=[TapePlugin(URL, budget=tape.Budget(usd_cap=0.0, token_cap=0))],
        resumability_config=ResumabilityConfig(is_resumable=True),
    )
    return Runner(app=app, session_service=TapeSessionService(URL))


async def _first_run() -> None:
    runner = _build_runner()
    await runner.session_service.create_session(
        app_name=APP, user_id=USER, session_id=SESSION,
        state={"cfo_policy_version": POLICY_VERSION, "policy_version": POLICY_VERSION})
    msg = types.Content(role="user", parts=[types.Part(text="Close the book for today.")])
    async for event in runner.run_async(user_id=USER, session_id=SESSION, new_message=msg):
        _print_event(event)
    _summary("after first run")


async def _recover() -> None:
    runner = _build_runner()
    results = tape.recover_once(runner=runner, url=URL)
    print(f"[recover] re-drove {len(results)} run(s): {results}")
    _summary("after recover")


def _print_event(event) -> None:
    author = getattr(event, "author", "?")
    if getattr(event, "content", None) and getattr(event.content, "parts", None):
        for p in event.content.parts:
            if getattr(p, "function_call", None):
                print(f"  [{author}] call {p.function_call.name}({dict(p.function_call.args or {})})")
            elif getattr(p, "function_response", None):
                print(f"  [{author}] result {p.function_response.name} -> {dict(p.function_response.response or {})}")
            elif getattr(p, "text", None):
                print(f"  [{author}] {p.text}")


def _summary(label: str) -> None:
    print(f"--- {label}: bank wires = {bank.count()}, gl batches = {gl.count()} ---")


def main() -> None:
    if "--reset" in sys.argv:
        reset_ledgers()
        print("[reset] cleared the example ledgers")
    if "--recover" in sys.argv:
        asyncio.run(_recover())
    else:
        asyncio.run(_first_run())


if __name__ == "__main__":
    main()
