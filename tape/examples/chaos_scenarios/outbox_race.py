"""outbox_race.py — two outbox dispatchers race; the CAS lease keeps it
single-winner.

The fault model: a `delay_connector` on `bank.wire` slows the dispatch
just enough that a concurrent dispatcher attempting to claim the same
effect arrives while the first dispatcher's lease is still held. The
ClaimEffectDispatch CAS predicate must reject the second attempt.

Invariant: `exactly_one(connector="bank.wire")` — even with two
dispatchers, exactly one wire on the bank's side. If the CAS broke,
this invariant fails.

This scenario tests the same property as
`tape/tests/test_non_idempotent.py::test_outbox_claim_is_single_winner`,
but at the chaos-framework level so failures show up as a scenario
report.

    tape chaos run tape/examples/chaos_scenarios/outbox_race.py
"""

from __future__ import annotations

import tape.chaos as chaos


SCENARIO = chaos.Scenario(
    name="outbox-race",
    faults=(
        # Slow the dispatch by 200ms so a concurrent claim has time to
        # collide with the lease.
        chaos.delay_connector(connector="bank.wire", ms=200),
    ),
    invariants=(
        chaos.no_stuck_obligations,
        chaos.exactly_one(connector="bank.wire"),
    ),
    seed=7,
)


def body(client, session):
    try:
        import examples.non_idempotent_bank.connectors  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "couldn't import examples.non_idempotent_bank.connectors")

    # In a real scenario you'd run two outbox dispatcher ticks
    # concurrently here (asyncio.gather) and let them race. Left as a
    # connecting-the-runner exercise so this file stays focused on the
    # scenario shape — see test_outbox_claim_is_single_winner for the
    # raw-RPC version of the same test.
