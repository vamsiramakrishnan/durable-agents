"""End-to-end tests against an in-memory SQLite-backed TapeSessionService.

These tests assert the contract from `tape/docs/design/failure-modes.md` —
but against the SQLAlchemy implementation rather than the Rust gRPC server.
Same invariants, embedded transport.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from tape_adk import (
    EffectDispatchMode,
    EffectResolution,
    EffectSemantics,
    EffectStatus,
    ObligationStatus,
    TapeSessionService,
)


@pytest.fixture
async def svc():
    """Fresh in-memory DB per test — keeps tests independent + fast."""
    s = TapeSessionService(db_url="sqlite+aiosqlite:///:memory:")
    yield s
    # SQLAlchemy AsyncEngine doesn't need explicit teardown for :memory:.


@pytest.fixture
async def session(svc):
    sess = await svc.create_session(
        app_name="t", user_id="u", session_id="s", state={})
    return sess


# ── effect ledger basics ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_begin_effect_is_idempotent_on_key(svc, session):
    """Calling begin_effect twice with the same derived key returns the same
    PENDING row. This is the replay-time short-circuit — the contract that
    makes BeginEffect safe to call freely from the SDK's plugin layer."""
    e1 = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        request_json={"amount": 2_000_000})
    e2 = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        request_json={"amount": 2_000_000})
    assert e1.idempotency_key == e2.idempotency_key
    assert e2.status == EffectStatus.PENDING
    # Critical: no second row inserted — only one effect ledger entry.
    pending = await svc.list_pending_effects()
    assert len(pending) == 1


@pytest.mark.asyncio
async def test_complete_effect_is_terminal_idempotent(svc, session):
    """complete_effect on an already-confirmed effect is a no-op that returns
    the current row (matches proto's semantics)."""
    e = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0)
    r1 = await svc.complete_effect(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        status=EffectStatus.CONFIRMED,
        response_json={"wire_id": "w-1"})
    assert r1.status == EffectStatus.CONFIRMED
    # Second call shouldn't overwrite.
    r2 = await svc.complete_effect(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        status=EffectStatus.FAILED, error_json={"err": "x"})
    assert r2.status == EffectStatus.CONFIRMED  # unchanged
    assert r2.response_json == {"wire_id": "w-1"}


# ── the load-bearing safety invariant ──────────────────────────────────────


@pytest.mark.asyncio
async def test_non_idempotent_inline_is_refused(svc, session):
    """The whole project exists to prevent this combination."""
    with pytest.raises(ValueError, match="NON_IDEMPOTENT.*OUTBOX"):
        await svc.begin_effect(
            app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
            decision_index=0, tool_name="bank.wire", call_index=0,
            semantics=EffectSemantics.NON_IDEMPOTENT,
            dispatch_mode=EffectDispatchMode.INLINE)


@pytest.mark.asyncio
async def test_outbox_without_connector_is_refused(svc, session):
    with pytest.raises(ValueError, match="OUTBOX.*connector"):
        await svc.begin_effect(
            app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
            decision_index=0, tool_name="bank.wire", call_index=0,
            semantics=EffectSemantics.NON_IDEMPOTENT,
            dispatch_mode=EffectDispatchMode.OUTBOX)


@pytest.mark.asyncio
async def test_business_key_dedup_across_runs(svc):
    """The (connector, business_key) UNIQUE constraint enforces cross-run
    dedup: two different invocations with the same business_key for the
    same connector fail loudly."""
    await svc.create_session(app_name="t", user_id="u", session_id="s1",
                              state={})
    await svc.create_session(app_name="t", user_id="u", session_id="s2",
                              state={})

    bk = "acct1:2m:2026-05-18"
    await svc.begin_effect(
        app_name="t", user_id="u", session_id="s1", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key=bk, connector="bank.wire")
    with pytest.raises(ValueError, match="business_key.*already exists"):
        await svc.begin_effect(
            app_name="t", user_id="u", session_id="s2",
            invocation_id="inv-2", decision_index=0, tool_name="bank.wire",
            call_index=0,
            semantics=EffectSemantics.NON_IDEMPOTENT,
            dispatch_mode=EffectDispatchMode.OUTBOX,
            business_key=bk, connector="bank.wire")


# ── CAS lease — the primitive ADK doesn't have ─────────────────────────────


@pytest.mark.asyncio
async def test_claim_effect_dispatch_single_winner(svc, session):
    """Two would-be dispatchers race for the same effect. Exactly one wins."""
    e = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="acct:2m:2026", connector="bank.wire")

    # Hit the same row concurrently (asyncio.gather → both coroutines run
    # interleaved). SQLite's serialised transactions + our CAS predicate
    # mean exactly one rowcount=1.
    r1, r2 = await asyncio.gather(
        svc.claim_effect_dispatch(
            app_name="t", user_id="u", session_id="s",
            idempotency_key=e.idempotency_key, claimer="dispatcher-A",
            lease_ttl_ms=60_000),
        svc.claim_effect_dispatch(
            app_name="t", user_id="u", session_id="s",
            idempotency_key=e.idempotency_key, claimer="dispatcher-B",
            lease_ttl_ms=60_000),
    )
    acquired_count = sum(1 for r in (r1, r2) if r[0])
    assert acquired_count == 1, (r1, r2)
    # Whichever won, the lease is held by exactly one claimer.
    eff = (r1[1] if r1[0] else r2[1])
    assert eff is not None and eff.dispatch_claimed_by in (
        "dispatcher-A", "dispatcher-B")


@pytest.mark.asyncio
async def test_expired_dispatch_lease_is_reclaimable(svc, session):
    """A claim with a past expiry should be reclaimable by another claimer."""
    e = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="acct:2m:2026", connector="bank.wire")

    # Win with a very short TTL so it's already expired by the time we
    # try to re-claim.
    acq, _ = await svc.claim_effect_dispatch(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key, claimer="A",
        lease_ttl_ms=1)
    assert acq
    # Pretend the original claimer crashed. With a now_ms in the future,
    # the lease appears expired.
    future = int(time.time() * 1000) + 1000
    acq2, eff = await svc.claim_effect_dispatch(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key, claimer="B",
        lease_ttl_ms=60_000, now_ms=future)
    assert acq2
    assert eff.dispatch_claimed_by == "B"


# ── UNKNOWN transition — the loud failure mode ─────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_attempt_zero_transitions_to_unknown(svc, session):
    """`next_dispatch_at_ms=0` is the load-bearing case — it tells the server
    to flip PENDING → UNKNOWN so the outbox stops retrying and the
    reconciler takes over."""
    e = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="acct:2m:2026", connector="bank.wire")
    r = await svc.record_dispatch_attempt(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        error="simulated lost ack", next_dispatch_at_ms=0)
    assert r.status == EffectStatus.UNKNOWN
    assert r.dispatch_attempts == 1
    assert r.dispatch_claimed_by in (None, "")

    # UNKNOWN effects must surface in list_pending_effects so the
    # reconciler can pick them up.
    unknowns = await svc.list_pending_effects(
        include_pending=False, include_unknown=True)
    assert len(unknowns) == 1
    assert unknowns[0].status == EffectStatus.UNKNOWN


@pytest.mark.asyncio
async def test_dispatch_attempt_positive_reschedules(svc, session):
    """A positive next_dispatch_at_ms keeps the effect PENDING — the outbox
    will retry after the backoff."""
    e = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="acct:2m:2026", connector="bank.wire")
    future = int(time.time() * 1000) + 60_000
    r = await svc.record_dispatch_attempt(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        error="connection refused", next_dispatch_at_ms=future)
    assert r.status == EffectStatus.PENDING
    assert r.next_dispatch_at_ms == future
    assert r.dispatch_attempts == 1


# ── reconciler write path ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_external_observation_confirmed_resolves_unknown(svc, session):
    """The full UNKNOWN → CONFIRMED loop: simulate a lost ack, reconciler
    observes the bank, marks CONFIRMED."""
    e = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="acct:2m:2026", connector="bank.wire")
    await svc.record_dispatch_attempt(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        error="ack lost", next_dispatch_at_ms=0)
    r = await svc.record_external_observation(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        resolution=EffectResolution.CONFIRMED,
        external_ref="wire-0001",
        response_json={"wire_id": "wire-0001"})
    assert r.status == EffectStatus.CONFIRMED
    assert r.external_ref == "wire-0001"


@pytest.mark.asyncio
async def test_duplicate_observation_atomically_registers_compensation(
    svc, session
):
    """The critical atomicity: when the reconciler sees a duplicate, the
    effect transitions to CONFIRMED **and** an obligation is registered in
    the SAME transaction. We assert both are present after the call."""
    e = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="acct:2m:2026", connector="bank.wire")
    await svc.record_dispatch_attempt(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        error="ack lost", next_dispatch_at_ms=0)
    r = await svc.record_external_observation(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        resolution=EffectResolution.DUPLICATE,
        external_ref="wire-A",
        compensate_on_duplicate_kind="reverse_wire")
    assert r.status == EffectStatus.CONFIRMED
    obs = await svc.list_obligations(
        app_name="t", user_id="u", session_id="s")
    assert len(obs) == 1
    assert obs[0].kind == "reverse_wire"
    assert obs[0].status == ObligationStatus.PENDING
    assert obs[0].effect_key == e.idempotency_key


@pytest.mark.asyncio
async def test_absent_for_non_idempotent_stays_unknown(svc, session):
    """ABSENT on non-idempotent means "bank doesn't have it" — but for
    NON_IDEMPOTENT we can't safely re-issue (might double-wire if there's
    a stale state). Effect stays UNKNOWN for human triage."""
    e = await svc.begin_effect(
        app_name="t", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="acct:2m:2026", connector="bank.wire")
    await svc.record_dispatch_attempt(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        error="ack lost", next_dispatch_at_ms=0)
    r = await svc.record_external_observation(
        app_name="t", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        resolution=EffectResolution.ABSENT)
    assert r.status == EffectStatus.UNKNOWN


# ── obligation ledger ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_register_compensation_is_idempotent(svc, session):
    """Idempotent on (session, effect_key, kind)."""
    o1 = await svc.register_compensation(
        app_name="t", user_id="u", session_id="s",
        effect_key="ek-1", kind="reverse_wire", payload_json={"amount": 1})
    o2 = await svc.register_compensation(
        app_name="t", user_id="u", session_id="s",
        effect_key="ek-1", kind="reverse_wire", payload_json={"amount": 2})
    assert o1.seq == o2.seq  # same row, not a duplicate
    obs = await svc.list_obligations(
        app_name="t", user_id="u", session_id="s")
    assert len(obs) == 1


@pytest.mark.asyncio
async def test_claim_obligation_single_winner(svc, session):
    """Two compensators race for the same obligation. Exactly one wins."""
    o = await svc.register_compensation(
        app_name="t", user_id="u", session_id="s",
        effect_key="ek-1", kind="reverse_wire")
    r1, r2 = await asyncio.gather(
        svc.claim_obligation(seq=o.seq, claimer="A", lease_ttl_ms=60_000),
        svc.claim_obligation(seq=o.seq, claimer="B", lease_ttl_ms=60_000),
    )
    assert sum(1 for r in (r1, r2) if r[0]) == 1


@pytest.mark.asyncio
async def test_record_obligation_attempt_retries_then_stucks(svc, session):
    """N-1 failed attempts keep it PENDING with backoff; the Nth flips it to
    STUCK. Matches `test_obligations.py::test_record_attempt_retries_then_stucks`
    from the existing test suite."""
    o = await svc.register_compensation(
        app_name="t", user_id="u", session_id="s",
        effect_key="ek-1", kind="reverse_wire", max_attempts=3)
    future = int(time.time() * 1000) + 10_000

    r1 = await svc.record_obligation_attempt(
        seq=o.seq, error="boom", next_attempt_at_ms=future)
    assert r1.status == ObligationStatus.PENDING
    assert r1.attempts == 1

    r2 = await svc.record_obligation_attempt(
        seq=o.seq, error="boom again", next_attempt_at_ms=future)
    assert r2.status == ObligationStatus.PENDING
    assert r2.attempts == 2

    r3 = await svc.record_obligation_attempt(
        seq=o.seq, error="boom 3", next_attempt_at_ms=future)
    assert r3.status == ObligationStatus.STUCK
    assert r3.attempts == 3


@pytest.mark.asyncio
async def test_terminal_now_attempt_forces_stuck(svc, session):
    """`next_attempt_at_ms=0` is "give up now", regardless of remaining
    attempts. The drainer uses this for irrecoverable business errors."""
    o = await svc.register_compensation(
        app_name="t", user_id="u", session_id="s",
        effect_key="ek-1", kind="reverse_wire", max_attempts=10)
    r = await svc.record_obligation_attempt(
        seq=o.seq, error="business rule says no", next_attempt_at_ms=0)
    assert r.status == ObligationStatus.STUCK
    assert r.attempts == 1


# ── timer registry ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_set_timer_idempotent_on_timer_id(svc, session):
    t1 = await svc.set_timer(
        app_name="t", user_id="u", session_id="s",
        timer_id="redrive-1", fire_at_ms=12345, kind="redrive")
    t2 = await svc.set_timer(
        app_name="t", user_id="u", session_id="s",
        timer_id="redrive-1", fire_at_ms=99999, kind="redrive")
    # Same row, original fire_at preserved (idempotent on key).
    assert t1.fire_at_ms == t2.fire_at_ms


@pytest.mark.asyncio
async def test_list_due_timers_claim_marks_fired(svc, session):
    """`claim=True` atomically marks returned timers fired so peer reactors
    don't re-fire them."""
    now = int(time.time() * 1000)
    await svc.set_timer(
        app_name="t", user_id="u", session_id="s",
        timer_id="due-1", fire_at_ms=now - 1000, kind="redrive")
    await svc.set_timer(
        app_name="t", user_id="u", session_id="s",
        timer_id="future-1", fire_at_ms=now + 60_000, kind="redrive")

    due = await svc.list_due_timers(now_ms=now, claim=True)
    assert len(due) == 1
    assert due[0].timer_id == "due-1"

    # Second call sees none because the first claimed it.
    due2 = await svc.list_due_timers(now_ms=now, claim=False)
    assert due2 == []


# ── reactive KV ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_write_value_cas(svc):
    """if_version mismatch is a stale-write that raises."""
    v1 = await svc.write_value(
        namespace="treasury", key="fx_rate",
        value_json={"USD": 1.0}, if_version=0)
    assert v1.version == 1

    # Stale write — caller thinks it's still at v1 but storage advanced.
    v2 = await svc.write_value(
        namespace="treasury", key="fx_rate",
        value_json={"USD": 1.01}, if_version=1)
    assert v2.version == 2

    with pytest.raises(ValueError, match="stale CAS"):
        await svc.write_value(
            namespace="treasury", key="fx_rate",
            value_json={"USD": 1.02}, if_version=1)


# ── cross-session list queries ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_unresolved_obligations_includes_pending_and_committed_expired(
    svc,
):
    """The drainer's feed: PENDING-ready by default, plus COMMITTED-expired
    (which is the lease-takeover path). Optional include_stuck for triage."""
    await svc.create_session(app_name="t", user_id="u", session_id="s1",
                              state={})
    o1 = await svc.register_compensation(
        app_name="t", user_id="u", session_id="s1",
        effect_key="ek-1", kind="reverse_wire")

    # Claim it with a short TTL so it appears expired immediately.
    await svc.claim_obligation(seq=o1.seq, claimer="A", lease_ttl_ms=1)

    future = int(time.time() * 1000) + 1000
    rows = await svc.list_unresolved_obligations(now_ms=future)
    # Both branches of the OR — COMMITTED-expired — should pick this up.
    assert any(o.seq == o1.seq for o in rows)
