"""`continue_as_new` — close one invocation chapter, start another.

Same safety invariant as the compactor (compensable-window pinning): an
old invocation's effect that has an active obligation pointing at it is
NOT pruned, even when continue_as_new asks to wipe the slate."""

from __future__ import annotations

import pytest

from tape_adk import (
    EffectDispatchMode,
    EffectSemantics,
    EffectStatus,
    ObligationStatus,
    TapeSessionService,
)


@pytest.fixture
async def svc():
    _call_seq.clear()   # fresh counter per test
    s = TapeSessionService(db_url="sqlite+aiosqlite:///:memory:")
    await s.create_session(app_name="a", user_id="u", session_id="s",
                            state={})
    yield s


_call_seq: dict[str, int] = {}

async def _confirmed_effect(svc, *, invocation: str, key: str):
    # Each call gets a fresh call_index so the derived idempotency_key
    # is unique (begin_effect derives the key from
    # (invocation, decision_index, tool, call_index)).
    ci = _call_seq.get(invocation, 0)
    _call_seq[invocation] = ci + 1
    e = await svc.begin_effect(
        app_name="a", user_id="u", session_id="s", invocation_id=invocation,
        decision_index=0, tool_name="bank.wire", call_index=ci,
        semantics=EffectSemantics.NON_IDEMPOTENT,
        dispatch_mode=EffectDispatchMode.OUTBOX,
        business_key=key, connector="bank.wire")
    await svc.complete_effect(
        app_name="a", user_id="u", session_id="s",
        idempotency_key=e.idempotency_key,
        status=EffectStatus.CONFIRMED, response_json={"id": key})
    return e.idempotency_key


# ── happy path ────────────────────────────────────────────────────────────


async def test_prunes_old_invocations_terminal_effects(svc):
    """The old invocation's CONFIRMED effects are wiped; the new
    invocation starts clean."""
    keys = [await _confirmed_effect(svc, invocation="inv-old",
                                     key=f"k-{i}") for i in range(3)]
    # Sanity: 3 rows exist.
    for k in keys:
        assert (await svc.get_effect(app_name="a", user_id="u",
                                      session_id="s",
                                      idempotency_key=k)) is not None

    r = await svc.continue_as_new(
        app_name="a", user_id="u", session_id="s",
        old_invocation_id="inv-old", new_invocation_id="inv-new")
    assert r["effects_pruned"] == 3
    for k in keys:
        assert (await svc.get_effect(app_name="a", user_id="u",
                                      session_id="s",
                                      idempotency_key=k)) is None


async def test_keeps_other_invocations_effects(svc):
    """Only effects under `old_invocation_id` are pruned. A different
    invocation's effects are untouched."""
    k_old = await _confirmed_effect(svc, invocation="inv-old", key="k-old")
    k_other = await _confirmed_effect(svc, invocation="inv-other",
                                       key="k-other")

    r = await svc.continue_as_new(
        app_name="a", user_id="u", session_id="s",
        old_invocation_id="inv-old", new_invocation_id="inv-new")
    assert r["effects_pruned"] == 1
    assert (await svc.get_effect(app_name="a", user_id="u",
                                  session_id="s",
                                  idempotency_key=k_old)) is None
    # The other invocation's effect is still there.
    assert (await svc.get_effect(app_name="a", user_id="u",
                                  session_id="s",
                                  idempotency_key=k_other)) is not None


# ── safety: pinning by an active obligation ──────────────────────────────


async def test_does_not_prune_pinned_effect_even_under_old_invocation(svc):
    """An effect with an active obligation IS NOT pruned — same
    compensable-window pinning the compactor enforces. The mechanism
    is the same SQL NOT EXISTS predicate; the operation just narrows
    by invocation_id on top."""
    key = await _confirmed_effect(svc, invocation="inv-old", key="k-1")
    await svc.register_compensation(
        app_name="a", user_id="u", session_id="s",
        effect_key=key, kind="reverse_wire")   # PENDING (active)

    r = await svc.continue_as_new(
        app_name="a", user_id="u", session_id="s",
        old_invocation_id="inv-old", new_invocation_id="inv-new")
    assert r["effects_pruned"] == 0
    assert r["obligations_kept"] == 1
    # Effect still there — the compensator may still need its external_ref.
    assert (await svc.get_effect(app_name="a", user_id="u",
                                  session_id="s",
                                  idempotency_key=key)) is not None


# ── state carry ──────────────────────────────────────────────────────────


async def test_carried_state_is_readable_under_new_invocation_id(svc):
    """`carried_state` lands as a `tape_values` row at the documented
    namespace/key. The new invocation reads it on startup to pick up
    where the old left off."""
    await svc.continue_as_new(
        app_name="a", user_id="u", session_id="s",
        old_invocation_id="inv-old", new_invocation_id="inv-new",
        carried_state={"checkpoint": "after sweep", "balance": 42})

    val = await svc.get_value(
        namespace="tape:continue-as-new:s", key="inv-new")
    assert val is not None
    assert val.value_json == {"checkpoint": "after sweep", "balance": 42}
    assert val.writer == "continue_as_new"


async def test_continue_as_new_is_atomic(svc):
    """Prune + state-write happen in one transaction. We can't observe
    a half-state from outside, but we can verify both effects landed
    after one call."""
    key = await _confirmed_effect(svc, invocation="inv-old", key="k-1")
    r = await svc.continue_as_new(
        app_name="a", user_id="u", session_id="s",
        old_invocation_id="inv-old", new_invocation_id="inv-new",
        carried_state={"x": 1})
    assert r["effects_pruned"] == 1
    assert r["state_written"] is True
    # Both observable.
    assert (await svc.get_effect(app_name="a", user_id="u",
                                  session_id="s",
                                  idempotency_key=key)) is None
    val = await svc.get_value(
        namespace="tape:continue-as-new:s", key="inv-new")
    assert val is not None


async def test_no_prune_when_prune_old_false(svc):
    """`prune_old=False` makes continue-as-new a pure state-carry — for
    callers who want to keep the old invocation's history."""
    key = await _confirmed_effect(svc, invocation="inv-old", key="k-1")
    r = await svc.continue_as_new(
        app_name="a", user_id="u", session_id="s",
        old_invocation_id="inv-old", new_invocation_id="inv-new",
        carried_state={"x": 1}, prune_old=False)
    assert r["effects_pruned"] == 0
    assert r["state_written"] is True
    assert (await svc.get_effect(app_name="a", user_id="u",
                                  session_id="s",
                                  idempotency_key=key)) is not None


# ── idempotency: carrying state twice updates, doesn't duplicate ─────────


async def test_repeated_continue_as_new_updates_state(svc):
    """Calling continue_as_new twice with the same new_invocation_id is
    semantically 'update the carried state' — there's at most one
    tape_values row per (namespace, key)."""
    await svc.continue_as_new(
        app_name="a", user_id="u", session_id="s",
        old_invocation_id="inv-1", new_invocation_id="inv-2",
        carried_state={"step": 1})
    await svc.continue_as_new(
        app_name="a", user_id="u", session_id="s",
        old_invocation_id="inv-2", new_invocation_id="inv-2",
        carried_state={"step": 2})
    val = await svc.get_value(
        namespace="tape:continue-as-new:s", key="inv-2")
    assert val.value_json == {"step": 2}
    assert val.version == 2
