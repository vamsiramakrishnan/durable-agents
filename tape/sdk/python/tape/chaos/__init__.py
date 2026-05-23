"""TapeChaos — Layer 3 of the fault-injection stack (the user surface).

See `design-principles/chaos.md` for the full design. The shape, in one
breath:

  * **Scenarios** are declarative bundles of (faults, invariants, seed).
  * **Faults** target either the server's failpoint catalogue (one of the
    46 named sites in `tape/server/src/chaos.rs`) or a connector (via
    `wrap_connector` — replaces the `TAPE_BANK_DISPATCH_INJECT_UNKNOWN`
    env-var soup with a declarative wrapper).
  * **Invariants** are predicates over Tape's journal projections — the
    journal *is* the oracle. No parallel test ledger.
  * **Sessions** apply the faults, hand back a context, and check the
    invariants when the scope exits.

Headline pattern::

    import tape.chaos as chaos

    scen = chaos.scenario(
        name="bank-wire-survives-crash",
        seed=42,
        faults=[
            chaos.crash("tape::begin_effect::post_db",
                        probability=1.0, after_n=1),
            chaos.lose_ack(tool="execute_sweep", probability=0.3),
        ],
        invariants=[
            chaos.invariant.exactly_one(connector="bank.wire"),
            chaos.invariant.no_stuck_obligations,
            chaos.invariant.no_blind_non_idempotent_retry,
            chaos.invariant.no_budget_overrun,
        ],
    )

    with chaos.session(scen, url="tape://localhost:7878") as sess:
        run_my_agent()              # tool bodies stay plain
    print(sess.report)              # ChaosReport(passed=True, …)

The faults that target server failpoints set the `FAILPOINTS` env var
before the server starts (or, when a tape-server is already running, an
in-process registry the SDK reads on the **caller's** failpoint when the
caller is e.g. a reactor in the same Python process). This is the v1
delivery model; Phase 2 will add a `ChaosService` gRPC to drive faults
into a remote server programmatically. See `design-principles/chaos.md
§4`.

This module is **optional**: nothing else in the SDK depends on
`tape.chaos`. Importing it has no side effects.
"""

from __future__ import annotations

from .scenarios import (
    Fault,
    Scenario,
    ChaosReport,
    crash,
    delay,
    error,
    lose_ack,
    duplicate,
    delay_connector,
    scenario,
    session,
    run_scenario,
    failpoints_env,
)
from . import invariants as invariant
from .invariants import (
    Invariant,
    InvariantResult,
    no_stuck_obligations,
    no_blind_non_idempotent_retry,
    no_budget_overrun,
    no_orphan_compensation,
    exactly_one,
)
from .connectors import wrap_connector, ChaosConnector
from .snapshot import (
    Snapshot,
    JournalLine,
    capture as capture_snapshot,
    DeepSnapshot,
    capture_deep,
)
from .replay import replay, replayable, ReplayReport
from .lineage import (
    LineageNode,
    LineageGraph,
    derive_scenarios,
    LDFIReport,
    run_all as ldfi_run_all,
)
from .reliability import ReliabilitySurface, Recorder, score
# Phase 4 — agent-layer proxies. Imported lazily-named (`chaos.proxy.delay`)
# to avoid clashing with `chaos.delay`, which is the *server-failpoint*
# delay (different layer). Use `chaos.model_proxy(...)` / `chaos.mcp_proxy(...)`
# to construct.
from . import proxies as proxy   # noqa: F401  — chaos.proxy.delay/inject_status/...
from .proxies import (
    ChaosProxy,
    ProxyFault,
    model_proxy,
    mcp_proxy,
)

__all__ = [
    "Fault",
    "Scenario",
    "ChaosReport",
    # server-failpoint faults
    "crash",
    "delay",
    "error",
    # connector faults
    "lose_ack",
    "duplicate",
    "delay_connector",
    # scenario surface
    "scenario",
    "session",
    "run_scenario",
    "failpoints_env",
    # connector wrap
    "wrap_connector",
    "ChaosConnector",
    # invariants — class, factories, and a namespace alias
    "Invariant",
    "InvariantResult",
    "no_stuck_obligations",
    "no_blind_non_idempotent_retry",
    "no_budget_overrun",
    "no_orphan_compensation",
    "exactly_one",
    "invariant",   # kept as a namespace alias for `tape.chaos.invariants`
    # snapshot + replay (Phase 2 — bit-for-bit determinism check)
    "Snapshot",
    "JournalLine",
    "capture_snapshot",
    "replay",
    "replayable",
    "ReplayReport",
    # phase 3 — LDFI + reliability surface + deep replay
    "DeepSnapshot",
    "capture_deep",
    "LineageNode",
    "LineageGraph",
    "derive_scenarios",
    "LDFIReport",
    "ldfi_run_all",
    "ReliabilitySurface",
    "Recorder",
    "score",
    # phase 4 — agent-layer chaos proxies
    "ChaosProxy",
    "ProxyFault",
    "model_proxy",
    "mcp_proxy",
    "proxy",      # namespace: chaos.proxy.delay, .inject_status, .tool_shadow, ...
]
