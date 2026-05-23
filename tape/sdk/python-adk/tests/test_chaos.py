"""Embedded-tier chaos tests — mirrors `tape/tests/test_chaos.py` against
the SQLAlchemy store. Proves the same invariants the gRPC chaos suite
proves, plus the strict-faults false-positive guard."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from tape_adk import (
    CompensationResult,
    DispatchResult,
    EffectDispatchMode,
    EffectResolution,
    EffectSemantics,
    EffectStatus,
    ObligationStatus,
    ObservationResult,
    TapeSessionService,
    chaos,
)


# ── a configurable fake bank connector (test fixture) ──────────────────────


@dataclass
class _LedgerConnector:
    """A trivial idempotent ledger. CONFIRMED on dispatch; mirrors the
    business_key dedupe a real bank's API provides."""
    name: str = "bank.wire"
    ledger: dict[str, str] = field(default_factory=dict)
    delay_ms: int = 0

    async def dispatch(self, effect) -> DispatchResult:
        if self.delay_ms:
            await asyncio.sleep(self.delay_ms / 1000.0)
        bk = effect.business_key or effect.idempotency_key
        wid = self.ledger.setdefault(bk, f"w-{len(self.ledger):04d}")
        return DispatchResult(status="confirmed", external_ref=wid,
                              response={"wire_id": wid})

    async def observe(self, effect) -> ObservationResult:
        bk = effect.business_key or effect.idempotency_key
        if bk in self.ledger:
            return ObservationResult(status="confirmed",
                                     external_ref=self.ledger[bk])
        return ObservationResult(status="absent")

    async def compensate(self, obligation) -> CompensationResult:
        return CompensationResult(status="compensated")


@pytest.fixture
async def svc():
    yield TapeSessionService(db_url="sqlite+aiosqlite:///:memory:")


# ── ChaosConnector: the fault mechanism, in isolation ─────────────────────


async def test_lose_ack_flips_confirmed_to_unknown(svc):
    """`lose_ack` mechanism: the inner connector CONFIRMS, the wrapper
    rewrites the result to UNKNOWN. The inner call still happened —
    the wrapper doesn't undo it; it just hides the ack from the agent."""
    inner = _LedgerConnector()
    wrapped = chaos.ChaosConnector(
        inner=inner,
        faults=(chaos.lose_ack(connector="bank.wire", probability=1.0),),
    )

    from tape_adk.service import EffectRecord
    effect = EffectRecord(
        app_name="a", user_id="u", session_id="s",
        idempotency_key="k", invocation_id="inv-1",
        decision_index=0, tool_name="wire", call_index=0,
        status="pending", semantics="non_idempotent",
        dispatch_mode="outbox",
        business_key="bk-1", connector="bank.wire", external_ref=None,
        dispatch_attempts=0, next_dispatch_at_ms=0,
        dispatch_claimed_by=None, dispatch_claim_expires_at_ms=0,
        last_dispatch_error=None,
        request_json={}, response_json=None, error_json=None, ts_ms=0)
    result = await wrapped.dispatch(effect)
    assert result.status == "unknown"
    # The inner call did land (the wrapper's contract).
    assert inner.ledger == {"bk-1": "w-0000"}


async def test_delay_connector_blocks_dispatch(svc):
    """`delay` waits before the inner call. Cheap to verify with a
    short sleep + a wall-clock check."""
    import time as _time
    inner = _LedgerConnector()
    wrapped = chaos.ChaosConnector(
        inner=inner,
        faults=(chaos.delay_connector(connector="bank.wire", ms=80),),
    )
    from tape_adk.service import EffectRecord
    effect = EffectRecord(
        app_name="a", user_id="u", session_id="s", idempotency_key="k",
        invocation_id="inv-1", decision_index=0, tool_name="wire",
        call_index=0, status="pending", semantics="idempotent",
        dispatch_mode="inline",
        business_key=None, connector="bank.wire", external_ref=None,
        dispatch_attempts=0, next_dispatch_at_ms=0,
        dispatch_claimed_by=None, dispatch_claim_expires_at_ms=0,
        last_dispatch_error=None,
        request_json={}, response_json=None, error_json=None, ts_ms=0)
    t0 = _time.monotonic()
    await wrapped.dispatch(effect)
    assert (_time.monotonic() - t0) >= 0.07   # honoured the delay


async def test_tool_scoped_fault_only_fires_on_matching_tool(svc):
    """Tool-scoped chaos: when `tool=` is set (and `target=` empty), the
    wrapper only applies the fault if `effect.tool_name == tool`. This
    is what makes tool-scoped chaos work across multiple connectors
    without the user having to map tool → connector by hand."""
    inner = _LedgerConnector()
    wrapped = chaos.ChaosConnector(
        inner=inner,
        faults=(chaos.lose_ack(tool="wire", probability=1.0),),
    )
    from tape_adk.service import EffectRecord
    def _eff(tool):
        return EffectRecord(
            app_name="a", user_id="u", session_id="s",
            idempotency_key=f"k-{tool}", invocation_id="inv",
            decision_index=0, tool_name=tool, call_index=0,
            status="pending", semantics="non_idempotent",
            dispatch_mode="outbox",
            business_key=f"bk-{tool}", connector="bank.wire",
            external_ref=None, dispatch_attempts=0, next_dispatch_at_ms=0,
            dispatch_claimed_by=None, dispatch_claim_expires_at_ms=0,
            last_dispatch_error=None,
            request_json={}, response_json=None, error_json=None, ts_ms=0)
    r_wire = await wrapped.dispatch(_eff("wire"))
    r_post = await wrapped.dispatch(_eff("post_gl"))
    assert r_wire.status == "unknown"        # tool matches → fault fires
    assert r_post.status == "confirmed"      # tool doesn't match → passthrough


# ── strict_faults: the silent-skip false-positive guard ───────────────────


async def test_strict_faults_fails_on_missing_connector(svc):
    """The mechanism: a scenario whose declared connector-targeted fault
    has no connector to attach to FAILS the scenario instead of silently
    passing. Same fix as the gRPC SDK's strict_faults — applied here to
    the embedded path."""
    scen = chaos.Scenario(
        name="missing-target",
        faults=(chaos.lose_ack(connector="bank.wire"),),
        invariants=(chaos.no_stuck_obligations,),
    )
    async def body(c): pass
    report = await chaos.run(scen, body,
                             db_url="sqlite+aiosqlite:///:memory:",
                             connectors={})  # empty — bank.wire missing
    assert report.passed is False
    assert any("strict_faults" in str(r) for r in report.invariant_results)


async def test_strict_faults_off_allows_skip(svc):
    """`strict_faults=False` opts out — declared faults that can't be
    applied just record notes. For users with environment-conditional
    faults."""
    scen = chaos.Scenario(
        name="optional-target",
        faults=(chaos.lose_ack(connector="bank.wire"),),
        invariants=(chaos.no_stuck_obligations,),
        strict_faults=False,
    )
    async def body(c): pass
    report = await chaos.run(scen, body,
                             db_url="sqlite+aiosqlite:///:memory:",
                             connectors={})
    assert report.passed is True
    assert any("not in `connectors` dict" in n for n in report.notes)


# ── invariants: read the embedded tables ──────────────────────────────────


async def test_no_stuck_obligations_passes_on_clean_store(svc):
    scen = chaos.Scenario(name="smoke",
                          invariants=(chaos.no_stuck_obligations,))
    async def body(c): pass
    report = await chaos.run(
        scen, body, db_url="sqlite+aiosqlite:///:memory:",
        connectors={})
    assert report.passed is True


async def test_no_stuck_obligations_fails_when_one_is_stuck():
    """If an obligation has reached STUCK, the invariant must catch it.
    This is the actual contract — STUCK means a compensation gave up
    and needs human triage; tests should fail loudly."""
    svc = TapeSessionService(db_url="sqlite+aiosqlite:///:memory:")
    await svc.create_session(app_name="a", user_id="u", session_id="s",
                             state={})
    ob = await svc.register_compensation(
        app_name="a", user_id="u", session_id="s",
        effect_key="ek", kind="reverse_wire", max_attempts=1)
    await svc.resolve_obligation(seq=ob.seq, status=ObligationStatus.STUCK)

    scen = chaos.Scenario(name="stuck",
                          invariants=(chaos.no_stuck_obligations,))
    async def body(c): pass
    report = await chaos.run(scen, body, svc=svc, connectors={})
    assert report.passed is False
    assert any("stuck" in str(r).lower()
               for r in report.invariant_results)


async def test_exactly_one_invariant():
    """`exactly_one(connector=…)` reads the embedded effects table
    directly. One CONFIRMED row → pass. Two → fail."""
    svc = TapeSessionService(db_url="sqlite+aiosqlite:///:memory:")
    await svc.create_session(app_name="a", user_id="u", session_id="s",
                             state={})
    e = await svc.begin_effect(
        app_name="a", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="bk-1", connector="bank.wire")
    await svc.complete_effect(
        app_name="a", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        status=EffectStatus.CONFIRMED, response_json={"id": "1"})

    scen = chaos.Scenario(
        name="one",
        invariants=(chaos.exactly_one(connector="bank.wire"),))
    async def body(c): pass
    report = await chaos.run(scen, body, svc=svc, connectors={})
    assert report.passed is True


# ── end-to-end: lose_ack → reconcile loop drives an UNKNOWN to CONFIRMED ──


async def test_lose_ack_e2e_with_reconciler():
    """The full UNKNOWN→reconcile loop driven through a chaos scenario.
    Two SDKs do this for the gRPC tier; this is the embedded-tier
    version. Asserts exactly_one(bank.wire) ends true."""
    from tape_adk import dispatch_outbox_once, reconcile_once

    svc = TapeSessionService(db_url="sqlite+aiosqlite:///:memory:")
    await svc.create_session(app_name="a", user_id="u", session_id="s",
                             state={})
    eff = await svc.begin_effect(
        app_name="a", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="bk-1", connector="bank.wire")
    bank = _LedgerConnector()

    scen = chaos.Scenario(
        name="unknown-then-reconcile",
        faults=(chaos.lose_ack(connector="bank.wire", probability=1.0),),
        invariants=(
            chaos.no_stuck_obligations,
            chaos.exactly_one(connector="bank.wire"),
        ),
    )

    async def body(wrapped_connectors):
        # Two reactor ticks: dispatch (gets UNKNOWN), then reconcile.
        r1 = await dispatch_outbox_once(svc, connectors=wrapped_connectors,
                                        claimer="d-1")
        assert any(x.get("outcome") == "unknown" for x in r1), r1
        # The bank's ledger has exactly one wire (the inner call landed).
        assert len(bank.ledger) == 1
        # The reconciler observes the unwrapped bank and resolves the
        # UNKNOWN. The wrapped observe() would only mutate to DUPLICATE,
        # so for the reconciler path we pass the unwrapped dict.
        r2 = await reconcile_once(svc, connectors={"bank.wire": bank})
        assert any(x.get("outcome") == "confirmed" for x in r2), r2

    report = await chaos.run(scen, body, svc=svc,
                             connectors={"bank.wire": bank})
    assert report.passed, report
    # And exactly one wire on the bank's side: the property the whole
    # contract exists to enforce.
    assert len(bank.ledger) == 1


# ── invariant API uniformity (matches the gRPC SDK fix) ───────────────────


def test_invariant_callable_uniformly():
    """Both `no_stuck_obligations` and `no_stuck_obligations()` work —
    same fix as the gRPC SDK. `exactly_one(connector=…)` stays a factory
    because it takes args; calling a parameter-free invariant with args
    is a TypeError with a clear message."""
    bare = chaos.no_stuck_obligations
    called = chaos.no_stuck_obligations()
    assert bare is called   # singleton; calling returns self
    with pytest.raises(TypeError):
        chaos.no_stuck_obligations(extra="oops")
