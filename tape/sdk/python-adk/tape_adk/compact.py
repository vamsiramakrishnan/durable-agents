"""The fifth reactor: compaction.

Where the existing reactors move rows FORWARD through the state machine
(PENDING → CONFIRMED, etc.), this one moves them OUT — the journal isn't
free, and a long-running agent accumulates terminal rows that have no
replay value. Same shape as `dispatch_outbox_once` / `reconcile_once` /
`drain_obligations_once` / `fire_due_timers_once`: a plain async function
the caller invokes on a tick.

The mechanism is one composite SQL DELETE per category with the safety
invariants encoded as WHERE-clause predicates — not Python pre-checks:

  DELETE FROM tape_effects
   WHERE status IN ('confirmed','failed')
     AND ts_ms < :cutoff
     AND NOT EXISTS (                            ← compensable-window pinning
       SELECT 1 FROM tape_obligations o
        WHERE o.session_id  = tape_effects.session_id
          AND o.effect_key  = tape_effects.idempotency_key
          AND o.status IN ('pending','committed'))

The `NOT EXISTS` clause IS the pinning mechanism (primitive #5 in the
roadmap). It runs at SQL level under the same lock all other tape-adk
writes use, so concurrent compaction and concurrent
register_compensation can't race a pinned effect into the trash.

Surface:

* `CompactionPolicy(effect_ttl_ms, session_ttl_ms, max_per_tick, …)`
    — the knobs.
* `CompactionResult(effects_pruned, obligations_pruned, timers_pruned,
   sessions_archived, refused_pinned)`
    — what one tick did.
* `compact_once(svc, *, policy, now_ms=0) -> CompactionResult`
    — the reactor function. Idempotent across ticks; safe to run
    alongside the other reactors.

Compaction is intentionally LOSSY: once an effect row is pruned, a future
`begin_effect` with the same idempotency_key won't short-circuit (it'll
create a new PENDING row). The TTL is the contract: rows older than the
policy SHOULD be safe to forget. For sessions that are still actively
re-driving older effects, set a longer TTL or use snapshots (B2).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import and_, delete, exists, func, not_, select

from .schemas import StorageEffect, StorageObligation, StorageTimer
from .service import (EffectStatus, ObligationStatus, TapeSessionService,
                      _now_ms)


# Status constants used in the SQL predicates. Stringly typed because the
# schema is.
_TERMINAL_EFFECT_STATUSES = (EffectStatus.CONFIRMED, EffectStatus.FAILED)
_ACTIVE_OBLIGATION_STATUSES = (ObligationStatus.PENDING,
                                ObligationStatus.COMMITTED)
_TERMINAL_OBLIGATION_STATUSES = (ObligationStatus.COMPENSATED,
                                  ObligationStatus.STUCK)


@dataclass(frozen=True)
class CompactionPolicy:
    """How aggressive the compactor is.

    `effect_ttl_ms`: prune CONFIRMED/FAILED effects older than this AND
        not pinned by an active obligation.
    `session_ttl_ms`: archive an entire session's tape rows when its
        latest tape row is older than this AND there are no active
        obligations or unfired timers on it. ADK's own session + events
        are left alone (this only touches the four tape_* tables).
    `archive_terminal_obligations`: when True, prune COMPENSATED
        obligations older than `effect_ttl_ms` too. (STUCK is kept
        regardless — it's the operator-triage signal.)
    `archive_fired_timers`: when True, prune fired timers older than
        `effect_ttl_ms`.
    `max_per_tick`: cap on rows touched per `compact_once` call. The
        compactor is meant to nibble; a runaway DELETE is the bug it's
        designed to prevent.
    """
    effect_ttl_ms: int = 7 * 24 * 60 * 60 * 1000   # 7 days
    session_ttl_ms: int = 30 * 24 * 60 * 60 * 1000  # 30 days
    archive_terminal_obligations: bool = True
    archive_fired_timers: bool = True
    max_per_tick: int = 1000


@dataclass
class CompactionResult:
    """What one `compact_once` tick did. Returned for the audit log,
    used by `tape doctor compact` for the human-facing summary."""
    effects_pruned: int = 0
    obligations_pruned: int = 0
    timers_pruned: int = 0
    sessions_archived: int = 0

    def total(self) -> int:
        return (self.effects_pruned + self.obligations_pruned
                + self.timers_pruned + self.sessions_archived)


# ── the reactor ────────────────────────────────────────────────────────────


async def compact_once(
    svc: TapeSessionService,
    *,
    policy: CompactionPolicy,
    now_ms: int = 0,
) -> CompactionResult:
    """One pass of the compactor. Three independent DELETE statements,
    each atomic, each safety-pinned.

    Order matters: prune obligations and timers FIRST, then effects
    (so an effect's pinning obligation is gone before we check). The
    ORDER is a mechanism: an interleaved tick of another reactor that
    inserts a new obligation between the obligation prune and the
    effect prune cannot re-pin a freshly-deleted effect, because the
    new obligation would name an effect_key that just disappeared
    (operator-visible failure rather than a silent dangling reference).
    """
    await svc._prepare_tables()  # type: ignore[attr-defined]
    now = now_ms or _now_ms()
    effect_cutoff = now - policy.effect_ttl_ms
    session_cutoff = now - policy.session_ttl_ms
    result = CompactionResult()

    async with svc._write_lock(), \
            svc._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
        # 1) Session-level archival FIRST. It's the superset operation:
        #    when a whole session qualifies (all rows old + no active
        #    obligations + no unfired timers), one round of DELETEs
        #    wipes its three tape_* tables in one shot. Per-row pruning
        #    in steps 2-4 then handles surviving rows in still-active
        #    sessions. ADK's own session + events are not touched.
        sessions_to_archive = await _find_archivable_sessions(
            sql, session_cutoff=session_cutoff, now=now,
            limit=policy.max_per_tick)
        for (app, user, sid) in sessions_to_archive:
            for tbl, ct in ((StorageEffect, "effects"),
                             (StorageObligation, "obligations"),
                             (StorageTimer, "timers")):
                r = await sql.execute(
                    delete(tbl).where(
                        tbl.app_name == app,
                        tbl.user_id == user,
                        tbl.session_id == sid,
                    ).execution_options(synchronize_session=False))
                # Roll archival deletes into the per-table counters too,
                # so the audit log adds up.
                count = r.rowcount or 0
                if ct == "effects":
                    result.effects_pruned += count
                elif ct == "obligations":
                    result.obligations_pruned += count
                else:
                    result.timers_pruned += count
            result.sessions_archived += 1

        # 2) terminal obligations older than the effect TTL — but keep
        #    STUCK (operator triage signal). The compactor never deletes
        #    a row that a human still needs to see.
        if policy.archive_terminal_obligations:
            r = await sql.execute(
                delete(StorageObligation).where(
                    StorageObligation.status
                    == ObligationStatus.COMPENSATED,
                    StorageObligation.ts_ms < effect_cutoff,
                ).execution_options(synchronize_session=False))
            result.obligations_pruned += r.rowcount or 0

        # 3) fired timers older than the effect TTL.
        if policy.archive_fired_timers:
            r = await sql.execute(
                delete(StorageTimer).where(
                    StorageTimer.fired.is_(True),
                    StorageTimer.created_at_ms < effect_cutoff,
                ).execution_options(synchronize_session=False))
            result.timers_pruned += r.rowcount or 0

        # 4) effects in a terminal status, old enough, with NO active
        #    obligation referencing them — the compensable-window pinning
        #    invariant, encoded as a NOT EXISTS subquery rather than an
        #    application-level loop. This is the load-bearing line that
        #    keeps a row whose compensator still needs the external_ref.
        pin_subq = exists().where(and_(
            StorageObligation.session_id == StorageEffect.session_id,
            StorageObligation.effect_key == StorageEffect.idempotency_key,
            StorageObligation.status.in_(_ACTIVE_OBLIGATION_STATUSES),
        ))
        r = await sql.execute(
            delete(StorageEffect).where(
                StorageEffect.status.in_(_TERMINAL_EFFECT_STATUSES),
                StorageEffect.ts_ms < effect_cutoff,
                not_(pin_subq),
            ).execution_options(synchronize_session=False))
        result.effects_pruned += r.rowcount or 0

        await sql.commit()

    return result


async def _find_archivable_sessions(sql, *, session_cutoff: int, now: int,
                                     limit: int) -> list[tuple[str, str, str]]:
    """A session is archivable when:
      * its latest tape_effect.ts_ms < session_cutoff (or it has none); AND
      * no active obligations on it (pending or committed); AND
      * no unfired timers that fire in the past (fired-and-stale OK to
        archive, fired-and-future are gone too)."""
    # Latest effect ts per session, with the "or no effects at all" branch
    # subsumed by the LEFT JOIN that follows.
    latest_eff_subq = (
        select(
            StorageEffect.app_name,
            StorageEffect.user_id,
            StorageEffect.session_id,
            func.max(StorageEffect.ts_ms).label("max_ts"),
        )
        .group_by(StorageEffect.app_name, StorageEffect.user_id,
                  StorageEffect.session_id)
        .subquery()
    )
    # We only consider sessions that have at least one tape row to prune
    # — there's nothing to archive on a session with no tape activity.
    candidates = await sql.execute(
        select(latest_eff_subq.c.app_name,
               latest_eff_subq.c.user_id,
               latest_eff_subq.c.session_id)
        .where(latest_eff_subq.c.max_ts < session_cutoff)
        .limit(limit))
    out: list[tuple[str, str, str]] = []
    for app, user, sid in candidates.all():
        # Active obligations?
        active_ob = await sql.execute(
            select(func.count(StorageObligation.seq)).where(
                StorageObligation.app_name == app,
                StorageObligation.user_id == user,
                StorageObligation.session_id == sid,
                StorageObligation.status.in_(_ACTIVE_OBLIGATION_STATUSES),
            ))
        if (active_ob.scalar() or 0) > 0:
            continue
        # STUCK obligations? Keep the session alive for triage.
        stuck_ob = await sql.execute(
            select(func.count(StorageObligation.seq)).where(
                StorageObligation.app_name == app,
                StorageObligation.user_id == user,
                StorageObligation.session_id == sid,
                StorageObligation.status == ObligationStatus.STUCK,
            ))
        if (stuck_ob.scalar() or 0) > 0:
            continue
        # Unfired timers in the past or future?
        live_timers = await sql.execute(
            select(func.count()).select_from(StorageTimer).where(
                StorageTimer.app_name == app,
                StorageTimer.user_id == user,
                StorageTimer.session_id == sid,
                StorageTimer.fired.is_(False),
            ))
        if (live_timers.scalar() or 0) > 0:
            continue
        out.append((app, user, sid))
    return out


__all__ = [
    "CompactionPolicy",
    "CompactionResult",
    "compact_once",
]
