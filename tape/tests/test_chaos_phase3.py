"""TapeChaos — Phase 3: LDFI + ReliabilitySurface + DeepSnapshot tests.

The headline claim: from one successful run, we *derive* the catalog of
chaos scenarios — no test author had to enumerate them.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SDK_PY = ROOT / "tape" / "sdk" / "python"
sys.path.insert(0, str(SDK_PY))


# ── Lineage graph ───────────────────────────────────────────────────────────

def _drive_baseline(client) -> str:
    """Drive one tiny successful run: begin_run + decision[0] + effect[0]
    pending → confirmed + register_compensation + end_run."""
    from tape.client import EFFECT_STATUS_CONFIRMED

    inv = "inv-ldfi-baseline"
    run = client.begin_run(app_name="a", user_id="u",
                            session_id="s", invocation_id=inv).run_id
    client.record_decision(run_id=run, decision_index=0, model="m",
                            request_json="{}", response_json='{"plan":1}',
                            rationale="", policy_version="p1")
    be = client.begin_effect(run_id=run, decision_index=0, tool_name="wire",
                              call_index=0, request_json="{}", custom_key="")
    client.complete_effect(run_id=run, idempotency_key=be.idempotency_key,
                            status=EFFECT_STATUS_CONFIRMED,
                            response_json='{"wire_id":"w-1"}', error_json="")
    client.register_compensation(run_id=run, effect_key=be.idempotency_key,
                                  kind="reverse_wire", payload_json="{}")
    client.end_run(run_id=run)
    return run


def test_lineage_graph_reads_decisions_effects_obligations(tape_server):
    """LineageGraph should pick up every distinct journal kind on a real run
    and assign each node a breaking failpoint from the catalogue."""
    import tape.chaos as chaos
    from tape.client import TapeClient

    client = TapeClient(tape_server["url"])
    try:
        run = _drive_baseline(client)
        graph = chaos.LineageGraph.from_run(client, run, deadline_s=3.0)
    finally:
        client.close()

    kinds = {n.kind for n in graph.nodes}
    assert "run" in kinds, f"expected 'run' kind; got {kinds}"
    assert "decision" in kinds, f"expected 'decision' kind; got {kinds}"
    assert "effect" in kinds, f"expected 'effect' kind; got {kinds}"
    assert "obligation" in kinds, f"expected 'obligation' kind; got {kinds}"

    # Every node should have a breaking_failpoint from the v1 catalogue.
    for n in graph.nodes:
        assert n.breaking_failpoint.startswith("tape::"), \
            f"node {n.kind}@{n.seq} has no breaking_failpoint: {n.breaking_failpoint!r}"


def test_lineage_edges_link_effects_to_their_decisions(tape_server):
    """Each effect's `parent_seq` should point to its decision's seq."""
    import tape.chaos as chaos
    from tape.client import TapeClient

    client = TapeClient(tape_server["url"])
    try:
        run = _drive_baseline(client)
        graph = chaos.LineageGraph.from_run(client, run, deadline_s=3.0)
    finally:
        client.close()

    decisions = {n.seq for n in graph.of_kind("decision")}
    for eff in graph.of_kind("effect"):
        # parent_seq=0 is allowed for an effect with decision_index=-1
        # (e.g. tape.sample()), but our baseline run has decision_index=0
        # everywhere — every effect must point back.
        assert eff.parent_seq in decisions, \
            f"effect@{eff.seq} parent {eff.parent_seq} not in {decisions}"


# ── Minimal cuts + derive_scenarios ────────────────────────────────────────

def test_minimal_cuts_singleton_per_node():
    """At max_size=1, every node with a breaking_failpoint is its own cut."""
    import tape.chaos as chaos
    from tape.chaos.lineage import LineageNode

    # Build a synthetic graph (no server needed for the unit test).
    g = chaos.LineageGraph(run_id="r-1", nodes=[
        LineageNode(seq=1, kind="run", payload={"status": "running"},
                    breaking_failpoint="tape::begin_run::post_db"),
        LineageNode(seq=2, kind="decision", payload={"decision_index": 0},
                    parent_seq=1, breaking_failpoint="tape::record_decision::post_db"),
        LineageNode(seq=3, kind="effect", payload={"idempotency_key": "k1", "status": "pending"},
                    parent_seq=2, breaking_failpoint="tape::begin_effect::post_db"),
        LineageNode(seq=4, kind="effect", payload={"idempotency_key": "k1", "status": "confirmed"},
                    parent_seq=2, breaking_failpoint="tape::complete_effect::post_db"),
        LineageNode(seq=5, kind="obligation", payload={"effect_key": "k1"},
                    parent_seq=3, breaking_failpoint="tape::register_compensation::post_db"),
    ])
    cuts = g.minimal_cuts(max_size=1)
    assert len(cuts) == 5
    assert all(len(c) == 1 for c in cuts)


def test_derive_scenarios_translates_cuts_to_crash_faults():
    """Every cut must produce a Scenario with one server-layer `crash`
    fault per node, targeting that node's breaking_failpoint."""
    import tape.chaos as chaos
    from tape.chaos.lineage import LineageNode

    g = chaos.LineageGraph(run_id="r-1", nodes=[
        LineageNode(seq=2, kind="decision", payload={},
                    breaking_failpoint="tape::record_decision::post_db"),
        LineageNode(seq=3, kind="effect", payload={"status": "pending"},
                    parent_seq=2,
                    breaking_failpoint="tape::begin_effect::post_db"),
    ])
    derived = chaos.derive_scenarios(g, invariants=(chaos.invariant.no_stuck_obligations,))
    assert len(derived) == 2
    targets = {f.target for s in derived for f in s.faults}
    assert "tape::record_decision::post_db" in targets
    assert "tape::begin_effect::post_db" in targets
    # The invariant is threaded through.
    for s in derived:
        assert len(s.invariants) == 1


def test_ldfi_run_all_aggregates_results():
    """`run_all` should collect per-scenario results and count survivals.
    Use a stub runner returning fake ChaosReports — no server needed."""
    import tape.chaos as chaos
    from tape.chaos.invariants import InvariantResult

    class FakeReport:
        def __init__(self, name, ok):
            self.scenario_name = name
            self.invariant_results = [
                InvariantResult(name="i1", passed=ok),
                InvariantResult(name="i2", passed=ok),
            ]
            self.passed = ok
            self.notes = []

    scenarios = [
        chaos.scenario(name=f"s{i}", faults=(chaos.crash("tape::begin_run::post_db"),))
        for i in range(4)
    ]
    # Pretend three survived, one broke.
    outcomes = [True, True, False, True]
    runner = lambda s: FakeReport(s.name, outcomes[int(s.name[1:])])

    rep = chaos.ldfi_run_all(scenarios, runner, baseline_run_id="r-base")
    assert rep.derived_count == 4
    assert rep.survived_count == 3
    assert len(rep.broken_scenarios) == 1
    assert rep.broken_scenarios[0][0] == "s2"


# ── Reliability surface ────────────────────────────────────────────────────

def test_reliability_recorder_computes_surface():
    """k = scenarios, ε = invariant violation rate, λ = recovery rate."""
    import tape.chaos as chaos
    from tape.chaos.invariants import InvariantResult

    class FakeReport:
        def __init__(self, name, passed):
            self.scenario_name = name
            self.passed = passed
            self.invariant_results = [
                InvariantResult(name="i", passed=passed),
            ]
            self.notes = []

    rec = chaos.Recorder()
    rec.add(FakeReport("a", True), terminal=True)
    rec.add(FakeReport("b", True), terminal=True)
    rec.add(FakeReport("c", False), terminal=False)
    rec.add(FakeReport("d", True), terminal=True)

    s = rec.surface
    assert s.k == 4
    assert s.epsilon == pytest.approx(0.25)   # 1 of 4 violated invariants
    assert s.lam == pytest.approx(0.75)        # 3 of 4 reached terminal


def test_reliability_to_markdown_renders_table():
    """The Markdown report contains the surface line + per-scenario row."""
    import tape.chaos as chaos
    from tape.chaos.invariants import InvariantResult

    class FakeReport:
        scenario_name = "soak::wire-survives-crash"
        passed = False
        notes: list = []
        invariant_results = [
            InvariantResult(name="exactly_one", passed=False, detail="dup keys: {b: 2}"),
        ]

    rec = chaos.Recorder()
    rec.add(FakeReport(), terminal=True)
    md = rec.to_markdown(title="Phase 3 — soak")
    assert "Reliability Surface" in md
    assert "R(k=1," in md
    assert "soak::wire-survives-crash" in md
    assert "exactly_one" in md


# ── DeepSnapshot — full-projection equality ────────────────────────────────

def test_deep_snapshot_catches_response_drift(tape_server):
    """The body-level claim that snapshot.capture (summary-only) misses:
    two runs with the same shape but different `response_json` on a
    decision should produce non-equal DeepSnapshots."""
    import tape.chaos as chaos
    from tape.client import TapeClient, EFFECT_STATUS_CONFIRMED

    url = tape_server["url"]
    client = TapeClient(url)
    try:
        # Two runs, identical shape, different decision response.
        def _drive(invocation: str, response: str) -> str:
            r = client.begin_run(app_name="t", user_id="u",
                                  session_id="deep", invocation_id=invocation).run_id
            client.record_decision(run_id=r, decision_index=0, model="m",
                                    request_json="{}", response_json=response,
                                    rationale="", policy_version="p1")
            be = client.begin_effect(run_id=r, decision_index=0,
                                      tool_name="t", call_index=0,
                                      request_json="{}", custom_key="")
            client.complete_effect(run_id=r, idempotency_key=be.idempotency_key,
                                    status=EFFECT_STATUS_CONFIRMED,
                                    response_json="{}", error_json="")
            client.end_run(run_id=r)
            return r

        r1 = _drive("inv-deep-1", '{"plan":"sweep"}')
        r2 = _drive("inv-deep-2", '{"plan":"hedge"}')

        deep1 = chaos.capture_deep(client, r1)
        deep2 = chaos.capture_deep(client, r2)
    finally:
        client.close()

    # Summary-only snapshot would consider these equal; DeepSnapshot
    # MUST catch the response_json drift.
    assert deep1 != deep2, "DeepSnapshot should catch response_json drift"


def test_deep_snapshot_equal_when_truly_equal(tape_server):
    """Two runs that did identical things (down to response_json) must
    produce equal DeepSnapshots."""
    import tape.chaos as chaos
    from tape.client import TapeClient, EFFECT_STATUS_CONFIRMED

    url = tape_server["url"]
    client = TapeClient(url)
    try:
        def _drive(invocation: str) -> str:
            r = client.begin_run(app_name="t", user_id="u",
                                  session_id="deep-eq", invocation_id=invocation).run_id
            client.record_decision(run_id=r, decision_index=0, model="m",
                                    request_json="{}", response_json='{"x":1}',
                                    rationale="", policy_version="p1")
            be = client.begin_effect(run_id=r, decision_index=0,
                                      tool_name="t", call_index=0,
                                      request_json="{}", custom_key="")
            client.complete_effect(run_id=r, idempotency_key=be.idempotency_key,
                                    status=EFFECT_STATUS_CONFIRMED,
                                    response_json='{"y":2}', error_json="")
            client.end_run(run_id=r)
            return r

        r1 = _drive("inv-deep-eq-1")
        r2 = _drive("inv-deep-eq-2")
        deep1 = chaos.capture_deep(client, r1)
        deep2 = chaos.capture_deep(client, r2)
    finally:
        client.close()

    assert deep1 == deep2, \
        f"identical-shape runs should produce equal DeepSnapshots; got:\n{deep1}\n{deep2}"
