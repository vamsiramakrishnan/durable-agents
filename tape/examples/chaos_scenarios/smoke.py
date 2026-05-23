"""smoke.py — the simplest scenario.

No faults; a no-op body; one invariant. Used to verify the chaos surface
is wired up at all. If this one fails, every other scenario in this
directory will fail — start here when debugging.

    tape chaos run tape/examples/chaos_scenarios/smoke.py
"""

from __future__ import annotations

import tape.chaos as chaos


SCENARIO = chaos.Scenario(
    name="smoke",
    faults=(),
    invariants=(chaos.no_stuck_obligations,),
    seed=42,
)


def body(client, session):
    # A no-op body: a clean server with no obligations means the
    # no_stuck_obligations invariant should pass.
    pass
