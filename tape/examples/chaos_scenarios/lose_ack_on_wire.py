"""lose_ack_on_wire.py — the canonical UNKNOWN → reconcile loop.

A `lose_ack` connector fault makes the `bank.wire` connector return
UNKNOWN exactly once before behaving normally. The scenario asserts:

* `no_stuck_obligations` — nothing ends up stuck;
* `exactly_one(connector="bank.wire")` — even though the call's ack was
  lost and the connector was effectively called twice, exactly ONE wire
  lives in the bank's ledger (because the upstream dedupes on
  `business_key`).

This is the same scenario behind `tape demo unknown-reconcile`, just
driven through the chaos framework so it gates CI on more than one
shape.

    tape chaos run tape/examples/chaos_scenarios/lose_ack_on_wire.py

`bank.wire` must be a registered connector when the scenario runs — the
body imports `examples.non_idempotent_bank.connectors` to get it
registered as a side effect. If you've moved that module, adjust the
import.
"""

from __future__ import annotations

import tape.chaos as chaos


SCENARIO = chaos.Scenario(
    name="lose-ack-on-wire",
    faults=(
        chaos.lose_ack(connector="bank.wire"),
    ),
    invariants=(
        chaos.no_stuck_obligations,
        chaos.no_blind_non_idempotent_retry,
        chaos.exactly_one(connector="bank.wire"),
    ),
    seed=42,
)


def body(client, session):
    # Importing the example registers the connector as a side effect.
    # Replace with your own connector module for a real agent.
    try:
        import examples.non_idempotent_bank.connectors  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "couldn't import examples.non_idempotent_bank.connectors — "
            "add tape/examples to PYTHONPATH or adapt this scenario to "
            "register your own bank.wire connector.")

    # Drive your agent here. For a real scenario, this is where you'd
    # invoke the runner with a user message; the chaos session has
    # wrapped the connector by the time body() runs, so the agent sees
    # the wrapped one.
    #
    # For the example, we leave the body empty — the outbox + reconciler
    # reactors (running in the agent process, or as a sidecar) do the
    # work. Add an `await runner.run_async(...)` call here when you wire
    # this against your own agent.
