"""TapeChaos — Phase 1 surface tests.

These tests exercise the SDK-side fault-injection surface (`tape.chaos`)
without requiring `--features chaos` on the server. They cover:

  * rendering server-layer scenarios into `FAILPOINTS` env-var specs;
  * connector wrap — `ChaosConnector` decorating a real connector with
    `lose_ack` / `duplicate` / `delay` faults;
  * the `session(...)` context manager applying + restoring connector
    wraps;
  * the invariant library reading from a real Tape server's journal.

The DST + failpoint-server tests come in Phase 2.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SDK_PY = ROOT / "tape" / "sdk" / "python"
sys.path.insert(0, str(SDK_PY))


# ── FAILPOINTS rendering — pure, no server needed ──────────────────────────

def test_failpoints_env_renders_panic_and_sleep_and_return():
    import tape.chaos as chaos

    scen = chaos.scenario(
        name="render-test",
        faults=[
            chaos.crash("tape::begin_effect::post_db"),
            chaos.crash("tape::send_signal::pre_db", probability=0.5),
            chaos.crash("tape::end_run::post_db", after_n=2),
            chaos.delay("tape::resume_run::pre_db", ms=500),
            chaos.error("tape::write_value::post_db", msg="simulated-db"),
        ],
    )
    spec = chaos.failpoints_env(scen)
    parts = set(spec.split(";"))
    assert "tape::begin_effect::post_db=panic" in parts
    assert "tape::send_signal::pre_db=0.5*panic" in parts
    assert "tape::end_run::post_db=2*off->panic" in parts
    assert "tape::resume_run::pre_db=sleep(500)" in parts
    assert "tape::write_value::post_db=return(simulated-db)" in parts


def test_failpoints_env_omits_connector_faults():
    import tape.chaos as chaos

    scen = chaos.scenario(
        name="connector-only",
        faults=[
            chaos.lose_ack(connector="bank.wire", probability=0.3),
            chaos.duplicate(connector="bank.wire", probability=0.1),
        ],
    )
    # No server-layer faults => empty spec.
    assert chaos.failpoints_env(scen) == ""


# ── ChaosConnector wrap — declarative replacement for env-var soup ─────────

class _StubBank:
    """A minimal `EffectConnector` stand-in. Records every dispatch in
    `wires` so the test can assert on duplicate calls."""

    name = "bank.wire"

    def __init__(self):
        self.wires = []  # list of business_keys actually wired

    def dispatch(self, effect):
        from tape.connectors.base import DispatchResult
        self.wires.append(effect.business_key)
        return DispatchResult(status="confirmed", external_ref=f"wire-{len(self.wires)}",
                              response={"wire_id": f"wire-{len(self.wires)}"})

    def observe(self, effect):
        from tape.connectors.base import ObservationResult
        hits = [w for w in self.wires if w == effect.business_key]
        if not hits:
            return ObservationResult(status="absent")
        return ObservationResult(status="confirmed", external_ref=f"wire-{self.wires.index(hits[0]) + 1}")

    def compensate(self, obligation):
        from tape.connectors.base import CompensationResult
        return CompensationResult(status="compensated", response={})


def _fake_effect(business_key="acct1:1000:2026-05-17", connector="bank.wire"):
    """Build a duck-typed effect record the connector contract accepts."""
    class E:
        pass
    e = E()
    e.run_id = "r-1"
    e.idempotency_key = "k-1"
    e.tool_name = "wire_money"
    e.request_json = '{}'
    e.connector = connector
    e.business_key = business_key
    e.external_ref = ""
    return e


def test_chaos_connector_lose_ack_turns_confirmed_into_unknown():
    import tape.chaos as chaos
    from tape.chaos.connectors import ChaosConnector

    bank = _StubBank()
    wrapped = ChaosConnector(
        inner=bank,
        faults=(chaos.lose_ack(connector="bank.wire", probability=1.0),),
        rng=random.Random(42),
    )

    result = wrapped.dispatch(_fake_effect())
    assert result.status == "unknown", \
        "lose_ack must mutate confirmed -> unknown so the reconciler resolves"
    # The inner call STILL ran — the request landed, only the ack got dropped.
    assert len(bank.wires) == 1, "the request must land (the floor); only the ack is lost"


def test_chaos_connector_lose_ack_probability_zero_passes_through():
    import tape.chaos as chaos
    from tape.chaos.connectors import ChaosConnector

    bank = _StubBank()
    wrapped = ChaosConnector(
        inner=bank,
        faults=(chaos.lose_ack(connector="bank.wire", probability=0.0),),
        rng=random.Random(42),
    )
    result = wrapped.dispatch(_fake_effect())
    assert result.status == "confirmed"
    assert len(bank.wires) == 1


def test_chaos_connector_duplicate_forces_observation_to_duplicate():
    import tape.chaos as chaos
    from tape.chaos.connectors import ChaosConnector

    bank = _StubBank()
    wrapped = ChaosConnector(
        inner=bank,
        faults=(chaos.duplicate(connector="bank.wire", probability=1.0),),
        rng=random.Random(7),
    )
    # First dispatch lands a wire so observe() has something to find.
    wrapped.dispatch(_fake_effect())
    obs = wrapped.observe(_fake_effect())
    assert obs.status == "duplicate", \
        "the duplicate fault must surface as ObservationResult.duplicate so the reconciler compensates"


def test_chaos_connector_delay_blocks_dispatch():
    import time

    import tape.chaos as chaos
    from tape.chaos.connectors import ChaosConnector

    bank = _StubBank()
    wrapped = ChaosConnector(
        inner=bank,
        faults=(chaos.delay_connector(connector="bank.wire", ms=120),),
        rng=random.Random(),
    )
    t0 = time.monotonic()
    wrapped.dispatch(_fake_effect())
    elapsed_ms = (time.monotonic() - t0) * 1000.0
    assert elapsed_ms >= 100, f"delay fault should add ~120ms; saw {elapsed_ms:.0f}ms"


def test_chaos_connector_preserves_name():
    """The wrapped connector has to register under the same name — that's
    how the outbox reactor routes to it."""
    import tape.chaos as chaos
    from tape.chaos.connectors import ChaosConnector

    wrapped = ChaosConnector(inner=_StubBank(),
                              faults=(chaos.lose_ack(connector="bank.wire"),))
    assert wrapped.name == "bank.wire"


# ── Session context manager — applies + restores connector wraps ───────────

def test_session_applies_connector_wrap_and_restores_on_exit():
    import tape.chaos as chaos
    from tape import connectors

    bank = _StubBank()
    connectors.register(bank)
    try:
        scen = chaos.scenario(
            name="wrap-and-restore",
            seed=1,
            faults=[chaos.lose_ack(connector="bank.wire", probability=1.0)],
        )
        with chaos.session(scen, url="tape://127.0.0.1:0") as sess:
            # Inside the session the registry holds the wrapped one.
            wrapped = connectors.get("bank.wire")
            assert isinstance(wrapped, chaos.ChaosConnector)
            r = wrapped.dispatch(_fake_effect())
            assert r.status == "unknown"
        # After the session the original is back.
        assert connectors.get("bank.wire") is bank
        r2 = connectors.get("bank.wire").dispatch(_fake_effect())
        assert r2.status == "confirmed"
    finally:
        connectors.clear()


def test_session_records_missing_connector_in_notes():
    """If a scenario references a connector that isn't registered, the session
    should log it in the report — not silently skip."""
    import tape.chaos as chaos
    from tape import connectors

    connectors.clear()
    scen = chaos.scenario(
        name="missing-connector",
        faults=[chaos.lose_ack(connector="never-registered", probability=1.0)],
    )
    with chaos.session(scen, url="tape://127.0.0.1:0") as sess:
        pass
    notes = " ".join(sess.report.notes)
    assert "never-registered" in notes


# ── Invariants — read against a live server ────────────────────────────────

def test_no_stuck_obligations_on_clean_server(tape_server):
    """Against a freshly-started Tape server the journal is empty; the
    invariant should pass trivially."""
    import tape.chaos as chaos
    from tape.client import TapeClient

    client = TapeClient(tape_server["url"])
    try:
        result = chaos.invariant.no_stuck_obligations.check(client=client, run_id=None)
    finally:
        client.close()
    assert result.passed, f"clean server should have no stuck obligations; got: {result}"
