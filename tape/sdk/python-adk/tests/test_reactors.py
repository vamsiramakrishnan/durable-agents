"""Reactor-loop tests — the full UNKNOWN→reconcile and crash→resume
scenarios end-to-end against the embedded SQLite store, with mock
connectors. These prove the embedded path delivers the same contract the
Rust server does."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import pytest

from tape_adk import (
    CompensationResult,
    DispatchResult,
    EffectDispatchMode,
    EffectResolution,
    EffectSemantics,
    EffectStatus,
    EffectRecord,
    ObligationRecord,
    ObligationStatus,
    ObservationResult,
    TapeSessionService,
    dispatch_outbox_once,
    drain_obligations_once,
    fire_due_timers_once,
    reconcile_once,
)


# ── a configurable test connector ──────────────────────────────────────────


@dataclass
class FakeBank:
    """Models the real bank's contract: dedupe on business_key. The
    connector wraps this."""

    ledger: dict[str, dict] = field(default_factory=dict)  # business_key → row

    def wire(self, business_key: str, amount: int, account: str) -> dict:
        if business_key in self.ledger:
            return self.ledger[business_key]
        wid = f"wire-{len(self.ledger) + 1:04d}"
        rec = {"wire_id": wid, "amount": amount, "account": account,
                "business_key": business_key}
        self.ledger[business_key] = rec
        return rec

    def find(self, business_key: str) -> dict | None:
        return self.ledger.get(business_key)

    def reverse(self, wire_id: str) -> dict:
        return {"reversal_id": f"rev-of-{wire_id}"}


@dataclass
class BankConnector:
    """Configurable fault injector for the bank. Mirrors the existing
    non_idempotent_bank example's connector — but standalone for tests."""

    bank: FakeBank
    name: str = "bank.wire"
    # Inject UNKNOWN on the first dispatch (the lost-ack case).
    inject_unknown_once: bool = False
    # Inject a generic exception on the first dispatch (the retry case).
    raise_once: bool = False
    # Number of dispatches we've seen — for the "_once" flags.
    n_dispatches: int = 0

    async def dispatch(self, effect: EffectRecord) -> DispatchResult:
        self.n_dispatches += 1
        req = effect.request_json or {}
        bk = effect.business_key or ""
        # ALWAYS write the wire to the bank — it's the "the call landed"
        # part. The faults below model what happens AFTER the wire lands.
        wire = self.bank.wire(
            business_key=bk,
            amount=req.get("amount", 0),
            account=req.get("account", "?"))
        if self.inject_unknown_once and self.n_dispatches == 1:
            return DispatchResult(status="unknown",
                                   error={"reason": "simulated lost ack"})
        if self.raise_once and self.n_dispatches == 1:
            raise RuntimeError("simulated transient network error")
        return DispatchResult(status="confirmed",
                               external_ref=wire["wire_id"],
                               response={"wire_id": wire["wire_id"]})

    async def observe(self, effect: EffectRecord) -> ObservationResult:
        bk = effect.business_key or ""
        rec = self.bank.find(bk)
        if rec is None:
            return ObservationResult(status="absent")
        return ObservationResult(status="confirmed",
                                  external_ref=rec["wire_id"],
                                  response={"wire_id": rec["wire_id"]})

    async def compensate(self, obligation: ObligationRecord) -> CompensationResult:
        wid = (obligation.payload_json or {}).get("external_ref")
        if not wid:
            return CompensationResult(status="failed",
                                       error={"reason": "no wire_id"})
        rev = self.bank.reverse(wid)
        return CompensationResult(status="compensated", response=rev)


@pytest.fixture
async def svc():
    s = TapeSessionService(db_url="sqlite+aiosqlite:///:memory:")
    yield s


@pytest.fixture
async def session(svc):
    await svc.create_session(app_name="t", user_id="u", session_id="s",
                              state={})


# ── the full UNKNOWN → reconcile loop ──────────────────────────────────────


@pytest.mark.asyncio
async def test_full_unknown_reconcile_loop(svc, session):
    """Inject an UNKNOWN on first dispatch. The outbox marks the effect
    UNKNOWN. The reconciler observes the bank by business_key, finds the
    wire, transitions to CONFIRMED. Exactly one wire on disk."""
    bank = FakeBank()
    connector = BankConnector(bank=bank, inject_unknown_once=True)

    # Agent journals intent.
    e = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        request_json={"amount": 2_000_000, "account": "acct-1"},
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="acct1:2m:2026-05-18", connector="bank.wire")
    assert e.status == EffectStatus.PENDING

    # Tick 1: outbox dispatches. Bank gets the wire, but the connector
    # returns UNKNOWN. Effect transitions to UNKNOWN.
    r1 = await dispatch_outbox_once(
        svc, connectors={"bank.wire": connector}, claimer="d-1")
    assert any(x.get("outcome") == "unknown" for x in r1), r1
    eff = await svc.get_effect(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key)
    assert eff.status == EffectStatus.UNKNOWN

    # Bank has the wire (the call DID land).
    assert len(bank.ledger) == 1

    # Tick 2: reconciler observes. Bank says CONFIRMED. Effect → CONFIRMED.
    r2 = await reconcile_once(svc, connectors={"bank.wire": connector})
    assert any(x.get("outcome") == "confirmed" for x in r2), r2
    eff = await svc.get_effect(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key)
    assert eff.status == EffectStatus.CONFIRMED
    assert eff.external_ref == "wire-0001"

    # Crucially: still exactly one wire on the bank's side. The "exactly
    # once" guarantee held under the UNKNOWN ambiguity.
    assert len(bank.ledger) == 1


# ── outbox loop with backoff on generic failure ────────────────────────────


@pytest.mark.asyncio
async def test_outbox_backs_off_on_generic_exception(svc, session):
    """A connector that raises gets a retry-after-backoff; the effect
    stays PENDING with `next_dispatch_at_ms > now`."""
    connector = BankConnector(bank=FakeBank(), raise_once=True)
    e = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        request_json={"amount": 100, "account": "x"},
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="x:100:2026", connector="bank.wire")

    now = int(time.time() * 1000)
    r1 = await dispatch_outbox_once(
        svc, connectors={"bank.wire": connector},
        claimer="d-1", default_backoff_ms=10_000)
    assert any(x.get("outcome") == "exception" for x in r1)

    eff = await svc.get_effect(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key)
    assert eff.status == EffectStatus.PENDING
    assert eff.next_dispatch_at_ms > now
    assert eff.dispatch_attempts == 1


# ── outbox CAS race: two dispatchers, exactly one wire ─────────────────────


@pytest.mark.asyncio
async def test_two_dispatchers_dispatch_each_effect_at_most_once(svc, session):
    """Two concurrent dispatchers reading the same queue — the CAS lease
    ensures each effect is dispatched at most once. The bank's ledger
    proves exactly-once on its side."""
    bank = FakeBank()
    connector = BankConnector(bank=bank)

    # Three effects, all eligible.
    for i in range(3):
        await svc.begin_effect(
            app_name="t", user_id="u", session_id="s",
            invocation_id=f"inv-{i}", decision_index=0,
            tool_name="bank.wire", call_index=0,
            request_json={"amount": i, "account": "x"},
            semantics=EffectSemantics.NON_IDEMPOTENT,
            dispatch_mode=EffectDispatchMode.OUTBOX,
            business_key=f"x:{i}:2026", connector="bank.wire")

    # Two concurrent dispatchers — same DB, different claimer names.
    r1, r2 = await asyncio.gather(
        dispatch_outbox_once(svc, connectors={"bank.wire": connector},
                              claimer="d-1"),
        dispatch_outbox_once(svc, connectors={"bank.wire": connector},
                              claimer="d-2"),
    )
    confirmed = [x for x in (r1 + r2) if x.get("outcome") == "confirmed"]
    # 3 effects, each dispatched exactly once.
    assert len(confirmed) == 3
    # Bank's ledger agrees: 3 wires.
    assert len(bank.ledger) == 3
    # All effects are CONFIRMED.
    pending = await svc.list_pending_effects()
    assert pending == []


# ── compensation drain end-to-end ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_drain_compensates_pending_obligation(svc, session):
    """Register an obligation, run the drainer, confirm it compensates."""
    bank = FakeBank()
    bank.wire(business_key="x:1:2026", amount=1, account="x")
    connector = BankConnector(bank=bank)

    ob = await svc.register_compensation(
        app_name="t", user_id="u", session_id="s",
        effect_key="ek-1", kind="bank.wire",
        payload_json={"external_ref": "wire-0001"})
    r = await drain_obligations_once(
        svc, connectors={"bank.wire": connector}, claimer="dr-1")
    assert any(x.get("outcome") == "compensated" for x in r), r

    after = await svc.list_obligations(
        app_name="t", user_id="u", session_id="s",
        only_unresolved=False)
    assert len(after) == 1
    assert after[0].status == ObligationStatus.COMPENSATED


# ── DUPLICATE flow: reconciler atomically registers compensation, drainer reverses ─


@pytest.mark.asyncio
async def test_duplicate_flow_registers_and_drains_compensation(svc, session):
    """End-to-end: connector.observe returns DUPLICATE → server atomically
    registers a 'reverse_wire' obligation → drainer reverses it.

    Models the worst-case for non-idempotent retries: the bank somehow
    has two records under the same business_key (e.g. an earlier failed
    retry actually landed). Tape compensates the surplus rather than
    leaving the system in a duplicate state."""
    bank = FakeBank()
    # Pre-populate the bank with two records under the same business_key.
    # (We can't go through the normal wire() API since it dedupes; mutate
    # the ledger directly for the test.)
    bank.ledger["x:1:2026"] = {"wire_id": "wire-A", "business_key": "x:1:2026",
                                "amount": 1, "account": "x"}
    bank.ledger["x:1:2026:dup"] = {"wire_id": "wire-B",
                                    "business_key": "x:1:2026",
                                    "amount": 1, "account": "x"}

    class DuplicateReportingConnector(BankConnector):
        async def observe(self, effect):
            return ObservationResult(status="duplicate",
                                      external_ref="wire-B",
                                      compensate_kind="bank.wire")

    connector = DuplicateReportingConnector(bank=bank)

    e = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        request_json={"amount": 1, "account": "x"},
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="x:1:2026", connector="bank.wire")
    # Force it to UNKNOWN so the reconciler picks it up.
    await svc.record_dispatch_attempt(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        error="ack lost", next_dispatch_at_ms=0)

    # Reconcile — sees DUPLICATE, registers compensation atomically.
    r1 = await reconcile_once(
        svc, connectors={"bank.wire": connector})
    assert any(x.get("outcome") == "duplicate" for x in r1), r1
    # Obligation is now PENDING.
    obs = await svc.list_obligations(
        app_name="t", user_id="u", session_id="s")
    assert len(obs) == 1
    assert obs[0].kind == "bank.wire"
    assert obs[0].status == ObligationStatus.PENDING

    # Drain — compensates the duplicate.
    r2 = await drain_obligations_once(
        svc, connectors={"bank.wire": connector}, claimer="dr-1")
    assert any(x.get("outcome") == "compensated" for x in r2), r2

    # Effect is CONFIRMED (the agent's view: the wire happened).
    eff = await svc.get_effect(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key)
    assert eff.status == EffectStatus.CONFIRMED


# ── timer firing ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fire_due_timers_invokes_dispatcher(svc, session):
    fired_ids: list[str] = []

    async def on_fire(t):
        fired_ids.append(t.timer_id)

    now = int(time.time() * 1000)
    await svc.set_timer(
        app_name="t", user_id="u", session_id="s",
        timer_id="t-1", fire_at_ms=now - 100, kind="redrive")
    await svc.set_timer(
        app_name="t", user_id="u", session_id="s",
        timer_id="t-future", fire_at_ms=now + 60_000, kind="redrive")

    r = await fire_due_timers_once(svc, dispatcher=on_fire)
    assert fired_ids == ["t-1"]
    assert any(x.get("outcome") == "fired" for x in r)
