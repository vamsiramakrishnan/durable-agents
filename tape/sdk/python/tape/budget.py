"""Budget — a spend cap that lives on the run, not in a wrapper.

`TapePlugin(budget=tape.Budget(usd_cap=50, token_cap=2_000_000))` is the simple
form. `tape.with_budget(...)` produces a `RunConfig` whose `custom_metadata`
carries the cap, for when you'd rather pass it per-invocation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Budget:
    usd_cap: float = 0.0      # <= 0  => no USD cap
    token_cap: int = 0        # <= 0  => no token cap


def with_budget(*, usd_cap: float = 0.0, token_cap: int = 0):
    """Return a `google.adk.agents.run_config.RunConfig` carrying this budget in
    `custom_metadata["tape_budget"]`. Pass it as `run_config=` to `runner.run`.
    """
    from google.adk.agents.run_config import RunConfig

    return RunConfig(custom_metadata={"tape_budget": {"usd_cap": usd_cap, "token_cap": token_cap}})


def budget_from_run_config(run_config) -> Optional[Budget]:
    md = getattr(run_config, "custom_metadata", None) if run_config is not None else None
    if isinstance(md, dict) and "tape_budget" in md:
        b = md["tape_budget"] or {}
        return Budget(usd_cap=float(b.get("usd_cap", 0.0)), token_cap=int(b.get("token_cap", 0)))
    return None
