"""`durable_app` — the 15-line ADK developer experience.

Wires an ADK `Agent` into Tape with one call::

    from tape.adk import durable_app
    import tape

    app, runner = durable_app(
        name="treasury",
        agent=root_agent,
        budget=tape.Budget(usd_cap=50),
    )

What it does:
  * defaults `tape_url` from `TAPE_URL` (falling back to `tape://localhost:7878`);
  * installs `TapePlugin` (with the supplied `budget` / cancel-check);
  * installs `TapeSessionService` against the same URL;
  * enables ADK `ResumabilityConfig(is_resumable=True)`;
  * returns the `(App, Runner)` pair.

`app_kwargs` and `runner_kwargs` are forwarded verbatim so power users can pass
extra plugins, sub-agents, or a different session-service constructor without
losing the ergonomics.
"""

from __future__ import annotations

import os
from typing import Any, Optional, Tuple

from .identity import RunIdentity
from .plugin import TapePlugin
from .session import TapeSessionService

DEFAULT_TAPE_URL = "tape://localhost:7878"


def _resolve_url(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    return os.environ.get("TAPE_URL") or DEFAULT_TAPE_URL


def durable_app(
    *,
    name: str,
    agent: Any,
    tape_url: Optional[str] = None,
    budget: Optional[Any] = None,
    resumable: bool = True,
    check_cancellation: bool = True,
    identity: Optional[RunIdentity] = None,
    require_identity: Optional[bool] = None,
    app_kwargs: Optional[dict] = None,
    runner_kwargs: Optional[dict] = None,
) -> Tuple[Any, Any]:
    """Return `(App, Runner)` for an ADK agent backed by Tape.

    The agent's tool bodies stay plain. Pass `@tape.effect(...)` decorators on
    tools whose UNKNOWN you want resolved or whose forward action needs an
    inverse; pass `@tape.outbox_tool(...)` for non-idempotent upstreams that must
    be journaled-first, dispatched-by-reactor.

    Identity:
      `identity` attaches AIPlex-style tenant / actor / subject / agent_id /
      scopes / labels to every run this app starts. If unset, defaults to
      `RunIdentity.from_env()` — so an AIPlex-deployed agent gets its identity
      "for free" from the `AIPLEX_*` env vars without the user code having to
      know about them. Pass `identity=RunIdentity()` (empty) to opt out
      explicitly.

      ``require_identity`` (default: read from `AIPLEX_REQUIRE_IDENTITY=1`)
      makes the constructor refuse to build the app when the identity is
      missing the AIPlex audit-anchor fields (tenant_id / actor / agent_id).
      AIPlex's deploy engine sets this env var on every Tape-backed pod —
      so a typo in the AIPLEX_* env vars crashes the pod loudly at boot
      instead of writing headless runs the compactor will retain forever.
    """
    from google.adk.apps import App
    from google.adk.apps.app import ResumabilityConfig
    from google.adk.runners import Runner

    url = _resolve_url(tape_url)
    if identity is None:
        identity = RunIdentity.from_env(strict=require_identity)
    elif require_identity is True or (
        require_identity is None and os.environ.get("AIPLEX_REQUIRE_IDENTITY") == "1"
    ):
        # Caller passed an explicit identity AND strict mode is on —
        # still validate so a hand-rolled `RunIdentity(tenant_id="")`
        # doesn't slip past the check.
        identity.validate()

    plugins = []
    if app_kwargs and app_kwargs.get("plugins"):
        plugins.extend(app_kwargs["plugins"])
    plugins.append(TapePlugin(url, budget=budget,
                              check_cancellation=check_cancellation,
                              identity=identity))

    app_init: dict = dict(app_kwargs or {})
    app_init.pop("plugins", None)
    app_init.setdefault("name", name)
    app_init.setdefault("root_agent", agent)
    if resumable:
        app_init.setdefault("resumability_config", ResumabilityConfig(is_resumable=True))

    app = App(plugins=plugins, **app_init)

    runner_init: dict = dict(runner_kwargs or {})
    runner_init.setdefault("session_service", TapeSessionService(url))
    runner_init.setdefault("app", app)

    runner = Runner(**runner_init)
    return app, runner


__all__ = ["durable_app", "DEFAULT_TAPE_URL", "RunIdentity"]
