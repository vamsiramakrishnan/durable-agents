"""Reactors as plain async functions over `TapeSessionService`.

Where the Rust `tape-server` runs reactors as separate Cloud Run services,
the embedded form ships them as **library functions** the operator runs
from any container — a sidecar in the ADK deployment, a Cloud Run Job, a
Kubernetes CronJob, the FastAPI server's startup hook, whatever. One
SQLAlchemy engine handle, four small loops.

The four reactors mirror the Rust server's:

* `dispatch_outbox_once(svc, connectors, claimer)` — finds PENDING+OUTBOX
  effects whose `next_dispatch_at_ms <= now` and whose lease is free /
  expired; claims one (CAS), calls the connector's `dispatch()`, records
  the result.

* `reconcile_once(svc, connectors)` — finds UNKNOWN effects (and PENDING
  effects older than a threshold), asks the connector's `observe()` what
  the counterparty says, records the resolution. The only path that
  resolves an UNKNOWN.

* `drain_obligations_once(svc, connectors)` — finds PENDING and
  COMMITTED-expired obligations, claims one (CAS), runs the connector's
  `compensate()`, records the result.

* `fire_due_timers_once(svc, dispatcher)` — finds due timers, atomically
  claims them, hands them to a dispatcher callback. (Timers are simpler
  than the others because the work isn't external — fire_at means
  "transition this run / call this handler.")

Run them on a schedule:

    async def main():
        svc = TapeSessionService(db_url=...)
        connectors = {"bank.wire": BankConnector()}
        while True:
            await dispatch_outbox_once(svc, connectors=connectors,
                                       claimer="dispatcher-1")
            await reconcile_once(svc, connectors=connectors)
            await drain_obligations_once(svc, connectors=connectors)
            await fire_due_timers_once(svc, dispatcher=on_fire)
            await asyncio.sleep(1)

Each `*_once` does at most `max=<limit>` items per tick, so a busy loop
self-rate-limits naturally. Crash-safety is built in: claims have TTLs,
so a process that dies mid-tick releases its work to the next runner.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Awaitable, Callable, Mapping, Optional

from .connectors import (
    CompensationResult,
    Connector,
    DispatchResult,
    ObservationResult,
)
from .service import (
    EffectDispatchMode,
    EffectResolution,
    EffectStatus,
    ObligationStatus,
    TapeSessionService,
)

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


# ── outbox dispatcher ──────────────────────────────────────────────────────


async def dispatch_outbox_once(
    svc: TapeSessionService,
    *,
    connectors: Mapping[str, Connector],
    claimer: str,
    limit: int = 50,
    lease_ttl_ms: int = 60_000,
    default_backoff_ms: int = 5_000,
    max_backoff_ms: int = 300_000,
) -> list[dict]:
    """One tick of the outbox loop. Returns a small audit of what it did
    (one dict per effect touched) — useful for tests and the doctor view.
    """
    results: list[dict] = []
    now = _now_ms()
    effects = await svc.list_effects_to_dispatch(now_ms=now, limit=limit)
    for eff in effects:
        if eff.connector not in connectors:
            results.append({"key": eff.idempotency_key,
                             "skip": f"no connector for {eff.connector!r}"})
            continue
        acquired, _ = await svc.claim_effect_dispatch(
            app_name=eff.app_name, user_id=eff.user_id,
            session_id=eff.session_id,
            idempotency_key=eff.idempotency_key,
            claimer=claimer, lease_ttl_ms=lease_ttl_ms, now_ms=now)
        if not acquired:
            results.append({"key": eff.idempotency_key,
                             "skip": "lost the claim"})
            continue
        # Re-read the effect now that we hold the lease (in case the row
        # mutated between list and claim — shouldn't, but be defensive).
        fresh = await svc.get_effect(
            app_name=eff.app_name, user_id=eff.user_id,
            session_id=eff.session_id,
            idempotency_key=eff.idempotency_key)
        if fresh is None or fresh.status != EffectStatus.PENDING:
            results.append({"key": eff.idempotency_key,
                             "skip": "not PENDING after claim"})
            continue

        connector = connectors[fresh.connector]
        try:
            outcome: DispatchResult = await connector.dispatch(fresh)
        except Exception as ex:  # noqa: BLE001
            # Generic failure — backoff and retry.
            attempts = fresh.dispatch_attempts + 1
            backoff = min(
                default_backoff_ms * (2 ** max(0, attempts - 1)),
                max_backoff_ms,
            )
            await svc.record_dispatch_attempt(
                app_name=fresh.app_name, user_id=fresh.user_id,
                session_id=fresh.session_id,
                idempotency_key=fresh.idempotency_key,
                error=f"{type(ex).__name__}: {ex}",
                next_dispatch_at_ms=now + backoff)
            results.append({"key": fresh.idempotency_key,
                             "outcome": "exception",
                             "backoff_ms": backoff})
            continue

        if outcome.status == "confirmed":
            await svc.complete_effect(
                app_name=fresh.app_name, user_id=fresh.user_id,
                session_id=fresh.session_id,
                idempotency_key=fresh.idempotency_key,
                status=EffectStatus.CONFIRMED,
                response_json=outcome.response)
            # If the connector reports an external_ref, attach it.
            if outcome.external_ref:
                # The effect is already CONFIRMED; attach external_ref via
                # the observation path (idempotent CONFIRMED is a no-op on
                # status; we just want the ref). Simplest: a direct UPDATE.
                async with svc._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
                    from .schemas import StorageEffect
                    from sqlalchemy import update as sa_update
                    await sql.execute(
                        sa_update(StorageEffect).where(
                            StorageEffect.app_name == fresh.app_name,
                            StorageEffect.user_id == fresh.user_id,
                            StorageEffect.session_id == fresh.session_id,
                            StorageEffect.idempotency_key == fresh.idempotency_key,
                        ).values(external_ref=outcome.external_ref))
                    await sql.commit()
            results.append({"key": fresh.idempotency_key,
                             "outcome": "confirmed",
                             "external_ref": outcome.external_ref})
        elif outcome.status == "unknown":
            # Lost ack — flip to UNKNOWN, reconciler takes over.
            await svc.record_dispatch_attempt(
                app_name=fresh.app_name, user_id=fresh.user_id,
                session_id=fresh.session_id,
                idempotency_key=fresh.idempotency_key,
                error=str(outcome.error or "ack lost"),
                next_dispatch_at_ms=0)
            results.append({"key": fresh.idempotency_key,
                             "outcome": "unknown"})
        elif outcome.status == "failed":
            # Definitive failure — but a connector may know it's not
            # retriable. We model both: positive retry_after_ms = retry;
            # 0 = use default exponential backoff; negative = give up
            # (terminal FAILED).
            if outcome.retry_after_ms < 0:
                await svc.complete_effect(
                    app_name=fresh.app_name, user_id=fresh.user_id,
                    session_id=fresh.session_id,
                    idempotency_key=fresh.idempotency_key,
                    status=EffectStatus.FAILED,
                    error_json=outcome.error)
                results.append({"key": fresh.idempotency_key,
                                 "outcome": "failed-terminal"})
            else:
                attempts = fresh.dispatch_attempts + 1
                backoff = outcome.retry_after_ms or min(
                    default_backoff_ms * (2 ** max(0, attempts - 1)),
                    max_backoff_ms,
                )
                await svc.record_dispatch_attempt(
                    app_name=fresh.app_name, user_id=fresh.user_id,
                    session_id=fresh.session_id,
                    idempotency_key=fresh.idempotency_key,
                    error=str(outcome.error or "dispatch failed"),
                    next_dispatch_at_ms=now + backoff)
                results.append({"key": fresh.idempotency_key,
                                 "outcome": "failed-retry",
                                 "backoff_ms": backoff})
        else:
            logger.warning("connector returned unknown status %r for %s",
                            outcome.status, fresh.idempotency_key)
    return results


# ── reconciler ─────────────────────────────────────────────────────────────


async def reconcile_once(
    svc: TapeSessionService,
    *,
    connectors: Mapping[str, Connector],
    stale_pending_ms: int = 0,
    limit: int = 50,
) -> list[dict]:
    """One tick of the reconciler loop. Walks UNKNOWN effects (and
    optionally stale PENDING effects), asks the connector's observe(),
    transitions the row.

    `stale_pending_ms > 0` includes PENDING effects older than that age —
    useful when the outbox dispatcher is suspected stuck.
    """
    results: list[dict] = []
    cutoff = _now_ms() - stale_pending_ms if stale_pending_ms > 0 else 0
    effects = await svc.list_pending_effects(
        older_than_ms=cutoff,
        include_pending=stale_pending_ms > 0,
        include_unknown=True,
        limit=limit)
    for eff in effects:
        connector = connectors.get(eff.connector or "")
        if connector is None:
            results.append({"key": eff.idempotency_key,
                             "skip": f"no connector for {eff.connector!r}"})
            continue
        try:
            obs: ObservationResult = await connector.observe(eff)
        except Exception as ex:  # noqa: BLE001
            results.append({"key": eff.idempotency_key,
                             "skip": f"observe raised: {ex}"})
            continue

        await svc.record_external_observation(
            app_name=eff.app_name, user_id=eff.user_id,
            session_id=eff.session_id,
            idempotency_key=eff.idempotency_key,
            resolution=obs.status,
            external_ref=obs.external_ref,
            response_json=obs.response, error_json=obs.error,
            compensate_on_duplicate_kind=obs.compensate_kind)
        results.append({"key": eff.idempotency_key,
                         "outcome": obs.status,
                         "external_ref": obs.external_ref})
    return results


# ── compensation drainer ───────────────────────────────────────────────────


async def drain_obligations_once(
    svc: TapeSessionService,
    *,
    connectors: Mapping[str, Connector],
    claimer: str = "drainer",
    limit: int = 50,
    lease_ttl_ms: int = 60_000,
    default_backoff_ms: int = 5_000,
    max_backoff_ms: int = 300_000,
) -> list[dict]:
    """One tick of the compensation drain. LIFO order matches the proto
    semantics: most-recently-registered runs first."""
    results: list[dict] = []
    now = _now_ms()
    obligations = await svc.list_unresolved_obligations(
        now_ms=now, limit=limit,
        include_pending=True, include_committed_expired=True,
        include_stuck=False)
    for ob in obligations:
        # We don't have a connector key on the obligation directly. Look
        # up via the effect's connector if reachable, falling back to ob.kind.
        eff = None
        if ob.effect_key:
            # Find the effect on this session.
            eff = await svc.get_effect(
                app_name=ob.app_name, user_id=ob.user_id,
                session_id=ob.session_id,
                idempotency_key=ob.effect_key)
        connector_name = (eff.connector if eff and eff.connector
                           else ob.kind)
        connector = connectors.get(connector_name)
        if connector is None:
            results.append({"seq": ob.seq,
                             "skip": f"no connector for {connector_name!r}"})
            continue

        acquired, _ = await svc.claim_obligation(
            seq=ob.seq, claimer=claimer, lease_ttl_ms=lease_ttl_ms,
            now_ms=now)
        if not acquired:
            results.append({"seq": ob.seq, "skip": "lost the claim"})
            continue

        try:
            outcome: CompensationResult = await connector.compensate(ob)
        except Exception as ex:  # noqa: BLE001
            attempts = ob.attempts + 1
            backoff = min(
                default_backoff_ms * (2 ** max(0, attempts - 1)),
                max_backoff_ms,
            )
            await svc.record_obligation_attempt(
                seq=ob.seq, error=f"{type(ex).__name__}: {ex}",
                next_attempt_at_ms=now + backoff)
            results.append({"seq": ob.seq,
                             "outcome": "exception",
                             "backoff_ms": backoff})
            continue

        if outcome.status == "compensated":
            await svc.resolve_obligation(
                seq=ob.seq, status=ObligationStatus.COMPENSATED,
                result_json=outcome.response)
            results.append({"seq": ob.seq, "outcome": "compensated"})
        elif outcome.status == "failed":
            backoff = outcome.retry_after_ms or min(
                default_backoff_ms * (2 ** max(0, ob.attempts)),
                max_backoff_ms,
            )
            await svc.record_obligation_attempt(
                seq=ob.seq, error=str(outcome.error or "compensate failed"),
                next_attempt_at_ms=now + backoff)
            results.append({"seq": ob.seq,
                             "outcome": "failed-retry",
                             "backoff_ms": backoff})
        else:
            logger.warning("compensate returned %r for seq %d",
                            outcome.status, ob.seq)
    return results


# ── timer firer ────────────────────────────────────────────────────────────


async def fire_due_timers_once(
    svc: TapeSessionService,
    *,
    dispatcher: Optional[Callable[[Any], Awaitable[None]]] = None,
    limit: int = 100,
) -> list[dict]:
    """Claim all due timers and hand each to `dispatcher` (if provided).
    With `dispatcher=None` the timers are just marked fired — useful when
    a downstream watcher does its own polling on `tape_timers.fired`."""
    timers = await svc.list_due_timers(now_ms=_now_ms(), limit=limit,
                                         claim=True)
    out: list[dict] = []
    for t in timers:
        if dispatcher is not None:
            try:
                await dispatcher(t)
                out.append({"timer_id": t.timer_id, "outcome": "fired"})
            except Exception as ex:  # noqa: BLE001
                out.append({"timer_id": t.timer_id,
                             "outcome": f"dispatcher raised: {ex}"})
        else:
            out.append({"timer_id": t.timer_id, "outcome": "marked-fired"})
    return out


__all__ = [
    "dispatch_outbox_once",
    "reconcile_once",
    "drain_obligations_once",
    "fire_due_timers_once",
]
