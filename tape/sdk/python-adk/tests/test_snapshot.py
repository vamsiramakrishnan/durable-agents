"""Effect-ledger snapshot rows — the durable short-circuit that survives
compaction.

The contract: `take_snapshot` captures terminal effects into a per-session
JSON blob; `begin_effect` falls back to that blob when the live row is
gone, so the compactor is free to prune underlying rows without breaking
the idempotency-key short-circuit."""

from __future__ import annotations

import pytest
from sqlalchemy import delete

from tape_adk import (
    EffectDispatchMode,
    EffectSemantics,
    EffectStatus,
    TapeSessionService,
)
from tape_adk.compact import CompactionPolicy, compact_once
from tape_adk.schemas import StorageEffect


@pytest.fixture
async def svc():
    s = TapeSessionService(db_url="sqlite+aiosqlite:///:memory:")
    await s.create_session(app_name="a", user_id="u", session_id="s",
                            state={})
    yield s


async def _confirmed_effect(svc, *, key: str, response: dict,
                              invocation: str = "inv-1",
                              call_index: int = 0,
                              business_key: str | None = None,
                              connector: str | None = None):
    """Make a CONFIRMED outbox effect. `key` is the idempotency_key
    derived from (invocation, decision_index, tool, call_index) so we
    pin `call_index` here to keep them distinct."""
    e = await svc.begin_effect(
        app_name="a", user_id="u", session_id="s", invocation_id=invocation,
        decision_index=0, tool_name="bank.wire", call_index=call_index,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key=business_key or key,
        connector=connector or "bank.wire")
    await svc.complete_effect(
        app_name="a", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        status=EffectStatus.CONFIRMED, response_json=response)
    return e.idempotency_key


# ── basic ─────────────────────────────────────────────────────────────────


async def test_take_snapshot_captures_terminal_effects(svc):
    """A fresh snapshot covers every terminal effect under the session."""
    k1 = await _confirmed_effect(svc, key="k1",
                                   response={"id": "wire-1"}, call_index=0)
    k2 = await _confirmed_effect(svc, key="k2",
                                   response={"id": "wire-2"}, call_index=1)
    r = await svc.take_snapshot(app_name="a", user_id="u", session_id="s")
    assert r["captured"] == 2
    assert r["merged_total"] == 2

    snap = await svc.get_snapshot(app_name="a", user_id="u", session_id="s")
    assert snap is not None
    assert set(snap.effects_json.keys()) == {k1, k2}
    assert snap.effects_json[k1]["response_json"] == {"id": "wire-1"}
    assert snap.effects_json[k2]["status"] == EffectStatus.CONFIRMED


async def test_snapshot_excludes_non_terminal_effects(svc):
    """A PENDING effect is NOT captured — only terminal-state rows are
    safe to short-circuit on."""
    # PENDING — never completed.
    await svc.begin_effect(
        app_name="a", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="k-pending", connector="bank.wire")
    r = await svc.take_snapshot(app_name="a", user_id="u", session_id="s")
    assert r["captured"] == 0


async def test_repeated_snapshot_is_cumulative(svc):
    """Second `take_snapshot` MERGES — it doesn't reset. The snapshot row
    accumulates across calls, last-write-wins per idempotency_key."""
    k1 = await _confirmed_effect(svc, key="k1",
                                   response={"v": 1}, call_index=0)
    r1 = await svc.take_snapshot(app_name="a", user_id="u",
                                   session_id="s")
    assert r1["merged_total"] == 1

    k2 = await _confirmed_effect(svc, key="k2",
                                   response={"v": 2}, call_index=1)
    r2 = await svc.take_snapshot(app_name="a", user_id="u",
                                   session_id="s")
    assert r2["captured"] == 2  # both rows still terminal, both captured
    assert r2["merged_total"] == 2

    snap = await svc.get_snapshot(app_name="a", user_id="u",
                                    session_id="s")
    assert set(snap.effects_json.keys()) == {k1, k2}


# ── the load-bearing invariant: short-circuit survives row deletion ─────


async def test_begin_effect_short_circuits_via_snapshot_after_row_pruned(svc):
    """The whole point. Snapshot the effect, manually delete the live
    row (simulating the compactor), and verify `begin_effect` with the
    same derived key returns the snapshot data instead of creating a
    fresh PENDING row.

    If this test fails the compactor can break the idempotency
    contract — the bug the snapshot exists to prevent."""
    k = await _confirmed_effect(svc, key="k1",
                                  response={"id": "wire-1"}, call_index=0)
    await svc.take_snapshot(app_name="a", user_id="u", session_id="s")

    # Brute-force delete the live row (no compactor TTL nonsense — we
    # want to test the fallback path, not the policy).
    async with svc._write_lock(), \
            svc._rollback_on_exception_session() as sql:
        await sql.execute(
            delete(StorageEffect).where(
                StorageEffect.idempotency_key == k))
        await sql.commit()
    assert (await svc.get_effect(app_name="a", user_id="u",
                                  session_id="s",
                                  idempotency_key=k)) is None

    # Now `begin_effect` with the same (invocation, decision, tool,
    # call_index) — which derives to the same idempotency_key —
    # should NOT create a new PENDING row. It should return the
    # snapshot's captured CONFIRMED record.
    e = await svc.begin_effect(
        app_name="a", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="k1", connector="bank.wire")
    assert e.idempotency_key == k
    assert e.status == EffectStatus.CONFIRMED
    assert e.response_json == {"id": "wire-1"}

    # And the live row is STILL gone — no resurrection.
    assert (await svc.get_effect(app_name="a", user_id="u",
                                  session_id="s",
                                  idempotency_key=k)) is None


async def test_begin_effect_prefers_live_row_over_snapshot(svc):
    """When BOTH the live row and a snapshot entry exist for the same
    key, the live row wins — it's authoritative. Snapshot is purely a
    fallback for the row-pruned case."""
    k = await _confirmed_effect(svc, key="k1",
                                  response={"id": "live"}, call_index=0)
    # Take snapshot, then mutate the snapshot to disagree with the
    # live row. `begin_effect` should still return the live row.
    await svc.take_snapshot(app_name="a", user_id="u", session_id="s")
    snap = await svc.get_snapshot(app_name="a", user_id="u",
                                    session_id="s")
    snap_data = dict(snap.effects_json)
    snap_data[k] = {**snap_data[k],
                     "response_json": {"id": "stale-snapshot"}}
    async with svc._write_lock(), \
            svc._rollback_on_exception_session() as sql:
        snap.effects_json = snap_data
        sql.add(snap)
        await sql.commit()

    e = await svc.begin_effect(
        app_name="a", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="k1", connector="bank.wire")
    assert e.response_json == {"id": "live"}


# ── snapshot + compactor: the integration that makes pruning safe ───────


async def test_snapshot_then_compact_then_begin_effect_short_circuits(svc):
    """End-to-end: snapshot, compact (which prunes the underlying row),
    then `begin_effect` still short-circuits. This is the real
    operator path."""
    k = await _confirmed_effect(svc, key="k1",
                                  response={"id": "wire-1"}, call_index=0)
    await svc.take_snapshot(app_name="a", user_id="u", session_id="s")

    # Compact with effect_ttl=0 so the (just-confirmed) effect is
    # immediately eligible for pruning. The snapshot row is NOT in
    # the compactor's purview — it isn't touched.
    result = await compact_once(
        svc, policy=CompactionPolicy(effect_ttl_ms=0,
                                       session_ttl_ms=10**12,
                                       archive_terminal_obligations=False,
                                       archive_fired_timers=False))
    assert result.effects_pruned == 1

    # Live row gone; snapshot row remains.
    assert (await svc.get_effect(app_name="a", user_id="u",
                                  session_id="s",
                                  idempotency_key=k)) is None
    assert (await svc.get_snapshot(app_name="a", user_id="u",
                                     session_id="s")) is not None

    # Short-circuit through the snapshot.
    e = await svc.begin_effect(
        app_name="a", user_id="u", session_id="s", invocation_id="inv-1",
        decision_index=0, tool_name="bank.wire", call_index=0,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key="k1", connector="bank.wire")
    assert e.status == EffectStatus.CONFIRMED
    assert e.response_json == {"id": "wire-1"}


# ── watermark ──────────────────────────────────────────────────────────────


async def test_take_snapshot_respects_up_to_ts_ms(svc):
    """`up_to_ts_ms` bounds the read window — effects with `ts_ms`
    beyond the watermark are NOT captured. The watermark on the
    snapshot row reflects what's been captured so a later snapshot
    knows where to resume from."""
    await _confirmed_effect(svc, key="k-early",
                              response={"v": 1}, call_index=0)
    # Snapshot at ts=1 — far in the past, so neither effect is included.
    r = await svc.take_snapshot(app_name="a", user_id="u",
                                  session_id="s", up_to_ts_ms=1)
    assert r["captured"] == 0
    assert r["up_to_ts_ms"] == 1


async def test_snapshot_handles_no_effects_gracefully(svc):
    """`take_snapshot` on a session with zero terminal effects creates a
    snapshot row with an empty map — safe and idempotent."""
    r = await svc.take_snapshot(app_name="a", user_id="u", session_id="s")
    assert r["captured"] == 0
    assert r["merged_total"] == 0
    snap = await svc.get_snapshot(app_name="a", user_id="u",
                                    session_id="s")
    assert snap is not None
    assert snap.effects_json == {}


async def test_get_snapshot_returns_none_for_no_snapshot(svc):
    """No snapshot taken — `get_snapshot` is `None`, not an empty row."""
    assert (await svc.get_snapshot(app_name="a", user_id="u",
                                     session_id="s")) is None
