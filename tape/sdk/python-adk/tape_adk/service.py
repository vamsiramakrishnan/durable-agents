"""`TapeSessionService` — ADK's `DatabaseSessionService` extended with an
effect ledger, obligation ledger, server-side timer registry, and reactive KV.

The design constraint: **every write goes through the same SQLAlchemy engine
ADK already owns**, under the same per-session asyncio lock + `with_for_update`
row lock + `_storage_update_marker` optimistic-concurrency check ADK uses for
`append_event`. That way "append an event AND record the effect's status
change" is one transaction, not two — and the gap that exists today between
ADK's events table and a parallel Tape store goes away.

The methods on this class are async because ADK's `DatabaseSessionService` is
async. They mirror the proto RPCs (`BeginEffect`, `CompleteEffect`,
`ClaimEffectDispatch`, `RecordDispatchAttempt`, `RecordExternalObservation`,
`RegisterCompensation`, `ListPendingEffects`, `ListEffectsToDispatch`,
`ListUnresolvedObligations`, `ClaimObligation`, `ResolveObligation`,
`RecordObligationAttempt`, `SetTimer`, `ListDueTimers`, `WriteValue`,
`GetValue`) — same semantics, embedded transport.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError

from google.adk.sessions.database_session_service import DatabaseSessionService

from .schemas import (
    StorageEffect,
    StorageEffectSnapshot,
    StorageObligation,
    StorageTimer,
    StorageValue,
)


# A note about concurrent writes on SQLite + ADK's StaticPool:
#
# ADK configures SQLite with StaticPool — one shared connection across all
# async sessions. SQLAlchemy checks that connection out per-statement, so
# ANY two concurrent writes (two `complete_effect`s, a CAS racing a
# `record_dispatch_attempt`, …) can interleave their BEGIN/UPDATE/COMMIT
# operations and lose a commit. The first cut of this file only locked the
# two `claim_*` methods; the two-dispatcher test then proved that wasn't
# enough — `complete_effect` races too, leaving an effect PENDING after the
# dispatcher reported it confirmed.
#
# The fix: a single per-service write lock (`_write_lock`) held by EVERY
# mutating method, gated to SQLite. On Postgres (default pool, real
# per-session connections, MVCC, `with_for_update`) the lock is a no-op —
# concurrency is the database's job. On SQLite, which is single-writer
# anyway, serialising writes in-process costs nothing real and makes the
# embedded path correct for one Python process driving its own reactors.
# Cross-process SQLite is out of scope; use Postgres for that.
#
# (The Go embedded SDK uses real pooled connections + SQLite's native
# write lock, and the TS one uses synchronous better-sqlite3 — neither
# needs this; verified by stress runs. It's a SQLAlchemy-StaticPool
# artefact specific to the Python path.)
_SQLITE_CAS_LOCK_ATTR = "_tape_sqlite_write_lock"


# ── status enums (string-typed; mirror proto enums) ────────────────────────


class EffectStatus:
    PENDING = "pending"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    UNKNOWN = "unknown"


class EffectSemantics:
    IDEMPOTENT = "idempotent"
    NON_IDEMPOTENT = "non_idempotent"
    OBSERVE_ONLY = "observe_only"


class EffectDispatchMode:
    INLINE = "inline"
    OUTBOX = "outbox"


class EffectResolution:
    """What the counterparty said about an effect (reconciler's input)."""
    CONFIRMED = "confirmed"
    FAILED = "failed"
    ABSENT = "absent"      # not found — for idempotent, safe to re-issue
    DUPLICATE = "duplicate"  # more than one — compensation required
    STUCK = "stuck"         # operator triage


class ObligationStatus:
    PENDING = "pending"
    COMMITTED = "committed"
    COMPENSATED = "compensated"
    STUCK = "stuck"


# ── records (lightweight dataclasses returned by the service) ───────────────


@dataclass
class EffectRecord:
    app_name: str
    user_id: str
    session_id: str
    idempotency_key: str
    invocation_id: str
    decision_index: int
    tool_name: str
    call_index: int
    status: str
    semantics: str
    dispatch_mode: str
    business_key: Optional[str]
    connector: Optional[str]
    external_ref: Optional[str]
    dispatch_attempts: int
    next_dispatch_at_ms: int
    dispatch_claimed_by: Optional[str]
    dispatch_claim_expires_at_ms: int
    last_dispatch_error: Optional[str]
    request_json: Optional[Any]
    response_json: Optional[Any]
    error_json: Optional[Any]
    ts_ms: int

    @classmethod
    def from_row(cls, r: StorageEffect) -> "EffectRecord":
        return cls(
            app_name=r.app_name, user_id=r.user_id, session_id=r.session_id,
            idempotency_key=r.idempotency_key, invocation_id=r.invocation_id,
            decision_index=r.decision_index, tool_name=r.tool_name,
            call_index=r.call_index, status=r.status,
            semantics=r.semantics, dispatch_mode=r.dispatch_mode,
            business_key=r.business_key, connector=r.connector,
            external_ref=r.external_ref,
            dispatch_attempts=r.dispatch_attempts,
            next_dispatch_at_ms=r.next_dispatch_at_ms,
            dispatch_claimed_by=r.dispatch_claimed_by,
            dispatch_claim_expires_at_ms=r.dispatch_claim_expires_at_ms,
            last_dispatch_error=r.last_dispatch_error,
            request_json=r.request_json, response_json=r.response_json,
            error_json=r.error_json, ts_ms=r.ts_ms,
        )


@dataclass
class ObligationRecord:
    seq: int
    app_name: str
    user_id: str
    session_id: str
    invocation_id: str
    effect_key: str
    kind: str
    payload_json: Optional[Any]
    status: str
    attempts: int
    max_attempts: int
    next_attempt_at_ms: int
    last_error: Optional[str]
    claimed_by: Optional[str]
    claim_expires_at_ms: int
    compensator_ref: Optional[str]
    result_json: Optional[Any]
    ts_ms: int

    @classmethod
    def from_row(cls, r: StorageObligation) -> "ObligationRecord":
        return cls(
            seq=r.seq, app_name=r.app_name, user_id=r.user_id,
            session_id=r.session_id, invocation_id=r.invocation_id,
            effect_key=r.effect_key, kind=r.kind,
            payload_json=r.payload_json, status=r.status,
            attempts=r.attempts, max_attempts=r.max_attempts,
            next_attempt_at_ms=r.next_attempt_at_ms,
            last_error=r.last_error, claimed_by=r.claimed_by,
            claim_expires_at_ms=r.claim_expires_at_ms,
            compensator_ref=r.compensator_ref,
            result_json=r.result_json, ts_ms=r.ts_ms,
        )


@dataclass
class TimerRecord:
    app_name: str
    user_id: str
    session_id: str
    timer_id: str
    fire_at_ms: int
    kind: str
    payload_json: Optional[Any]
    fired: bool
    created_at_ms: int

    @classmethod
    def from_row(cls, r: StorageTimer) -> "TimerRecord":
        return cls(
            app_name=r.app_name, user_id=r.user_id, session_id=r.session_id,
            timer_id=r.timer_id, fire_at_ms=r.fire_at_ms, kind=r.kind,
            payload_json=r.payload_json, fired=r.fired,
            created_at_ms=r.created_at_ms,
        )


def _now_ms() -> int:
    return int(time.time() * 1000)


# ── the service ─────────────────────────────────────────────────────────────


class TapeSessionService(DatabaseSessionService):
    """`DatabaseSessionService` + effect/obligation/timer/KV ledgers.

    Drop-in replacement. The ADK methods (`create_session`, `get_session`,
    `list_sessions`, `delete_session`, `append_event`) are inherited unchanged.

    The added methods all participate in the same async SQLAlchemy session as
    `append_event`, behind the same per-session lock. So an agent that does:

        # before_tool(name='bank.wire', args={...})
        plugin.session_service.begin_effect(session=…, semantics='non_idempotent',
                                            dispatch_mode='outbox',
                                            business_key='acct:2m:2026-05-18')
        plugin.session_service.append_event(session, function_call_event)

    commits both rows in one transaction — or neither, on crash. The
    same-process atomicity gap that exists between today's gRPC TapePlugin
    and ADK's DatabaseSessionService is closed by construction.
    """

    @asynccontextmanager
    async def _write_lock(self):
        """Serialize *every mutating operation* in-process on SQLite.

        ADK configures SQLite with `StaticPool` — one shared connection
        across all async sessions. SQLAlchemy checks that connection out
        per-statement, so two concurrent writes (a `complete_effect` racing
        another dispatcher's `complete_effect`, a CAS racing a
        `record_dispatch_attempt`, …) can interleave their BEGIN/UPDATE/
        COMMIT operations and lose a commit. The original implementation
        only locked the two `claim_*` methods; the two-dispatcher test
        proved that wasn't enough — `complete_effect` races too.

        The fix: a single per-service write lock, held by every mutating
        method, gated to SQLite. On Postgres (real per-session connections,
        MVCC, row-level locks) the lock is a no-op — concurrency is the
        database's job there. On SQLite, which is single-writer anyway,
        serialising writes in-process costs nothing real and makes the
        embedded path correct for a single Python process driving its own
        reactors. Cross-process SQLite is out of scope (SQLite isn't a
        multi-writer database); use Postgres for that."""
        is_sqlite = self.db_engine.dialect.name == "sqlite"
        if not is_sqlite:
            yield
            return
        lock = getattr(self, _SQLITE_CAS_LOCK_ATTR, None)
        if lock is None:
            lock = asyncio.Lock()
            setattr(self, _SQLITE_CAS_LOCK_ATTR, lock)
        async with lock:
            yield

    # ── effect ledger ────────────────────────────────────────────────────

    async def begin_effect(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        invocation_id: str,
        decision_index: int,
        tool_name: str,
        call_index: int = 0,
        request_json: Optional[Any] = None,
        custom_key: str = "",
        semantics: str = EffectSemantics.IDEMPOTENT,
        dispatch_mode: str = EffectDispatchMode.INLINE,
        business_key: Optional[str] = None,
        connector: Optional[str] = None,
    ) -> EffectRecord:
        """Idempotent. If an effect with this idempotency_key already exists,
        returns the existing record (the replay-time short-circuit).
        Otherwise creates a fresh PENDING row.

        Server-side safety: refuses NON_IDEMPOTENT + INLINE — that combination
        is the bug the whole project exists to prevent.
        """
        if (semantics == EffectSemantics.NON_IDEMPOTENT
                and dispatch_mode == EffectDispatchMode.INLINE):
            raise ValueError(
                "begin_effect: NON_IDEMPOTENT effects must use OUTBOX dispatch")
        if (dispatch_mode == EffectDispatchMode.OUTBOX
                and not connector):
            raise ValueError(
                "begin_effect: OUTBOX dispatch requires a `connector` name")

        key = (custom_key
               or f"{invocation_id}/decision-{decision_index}/{tool_name}/{call_index}")

        await self._prepare_tables()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            # Try to read an existing row first — idempotent on replay.
            existing = (await sql.execute(
                select(StorageEffect).where(
                    StorageEffect.app_name == app_name,
                    StorageEffect.user_id == user_id,
                    StorageEffect.session_id == session_id,
                    StorageEffect.idempotency_key == key,
                ))).scalars().one_or_none()
            if existing is not None:
                return EffectRecord.from_row(existing)

            # Snapshot fallback: the live row may have been pruned by the
            # compactor. If we have a terminal-state snapshot for this
            # key, synthesise the short-circuit EffectRecord from it so
            # the caller sees the same idempotent behaviour they'd see
            # with the row still present. No row is created here — the
            # snapshot IS the durable record.
            snap = (await sql.execute(
                select(StorageEffectSnapshot).where(
                    StorageEffectSnapshot.app_name == app_name,
                    StorageEffectSnapshot.user_id == user_id,
                    StorageEffectSnapshot.session_id == session_id,
                ))).scalars().one_or_none()
            if snap is not None and snap.effects_json:
                captured = snap.effects_json.get(key) if isinstance(
                    snap.effects_json, dict) else None
                if captured is not None:
                    return EffectRecord(
                        app_name=app_name, user_id=user_id,
                        session_id=session_id, idempotency_key=key,
                        invocation_id=captured.get("invocation_id", ""),
                        decision_index=captured.get("decision_index", -1),
                        tool_name=captured.get("tool_name", tool_name),
                        call_index=captured.get("call_index", call_index),
                        status=captured.get("status",
                                            EffectStatus.CONFIRMED),
                        semantics=captured.get("semantics", semantics),
                        dispatch_mode=captured.get("dispatch_mode",
                                                    dispatch_mode),
                        business_key=captured.get("business_key"),
                        connector=captured.get("connector"),
                        external_ref=captured.get("external_ref"),
                        dispatch_attempts=0, next_dispatch_at_ms=0,
                        dispatch_claimed_by=None,
                        dispatch_claim_expires_at_ms=0,
                        last_dispatch_error=None,
                        request_json=captured.get("request_json"),
                        response_json=captured.get("response_json"),
                        error_json=captured.get("error_json"),
                        ts_ms=captured.get("ts_ms", 0),
                    )

            now = _now_ms()
            row = StorageEffect(
                app_name=app_name, user_id=user_id, session_id=session_id,
                idempotency_key=key, invocation_id=invocation_id,
                decision_index=decision_index, tool_name=tool_name,
                call_index=call_index, status=EffectStatus.PENDING,
                semantics=semantics, dispatch_mode=dispatch_mode,
                business_key=business_key or None,
                connector=connector or None, external_ref=None,
                dispatch_attempts=0, next_dispatch_at_ms=0,
                dispatch_claimed_by=None,
                dispatch_claim_expires_at_ms=0,
                last_dispatch_error=None,
                request_json=request_json, response_json=None, error_json=None,
                ts_ms=now,
            )
            sql.add(row)
            try:
                await sql.commit()
            except IntegrityError as ex:
                # Most likely: (connector, business_key) UNIQUE clash —
                # another run already journaled this logical operation. The
                # caller MUST treat this as "someone else owns this work".
                await sql.rollback()
                raise ValueError(
                    f"begin_effect: business_key already exists for "
                    f"connector={connector!r}: {ex.orig}"
                ) from None
            return EffectRecord.from_row(row)

    async def complete_effect(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        idempotency_key: str,
        status: str,
        response_json: Optional[Any] = None,
        error_json: Optional[Any] = None,
    ) -> Optional[EffectRecord]:
        """Flip an effect's terminal status. Idempotent — if the effect is
        already CONFIRMED/FAILED/UNKNOWN, this is a no-op that returns the
        current row (matches the proto's semantics)."""
        if status not in (EffectStatus.CONFIRMED, EffectStatus.FAILED,
                           EffectStatus.UNKNOWN):
            raise ValueError(f"complete_effect: invalid status {status!r}")

        await self._prepare_tables()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            row = (await sql.execute(
                select(StorageEffect).where(
                    StorageEffect.app_name == app_name,
                    StorageEffect.user_id == user_id,
                    StorageEffect.session_id == session_id,
                    StorageEffect.idempotency_key == idempotency_key,
                ))).scalars().one_or_none()
            if row is None:
                return None
            # Already terminal? Return as-is.
            if row.status != EffectStatus.PENDING:
                return EffectRecord.from_row(row)
            row.status = status
            row.response_json = response_json
            row.error_json = error_json
            row.ts_ms = _now_ms()
            # Clear the dispatch lease — terminal effects never get
            # re-dispatched.
            row.dispatch_claimed_by = None
            row.dispatch_claim_expires_at_ms = 0
            await sql.commit()
            return EffectRecord.from_row(row)

    async def get_effect(
        self, *, app_name: str, user_id: str, session_id: str,
        idempotency_key: str,
    ) -> Optional[EffectRecord]:
        await self._prepare_tables()
        async with self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            row = (await sql.execute(
                select(StorageEffect).where(
                    StorageEffect.app_name == app_name,
                    StorageEffect.user_id == user_id,
                    StorageEffect.session_id == session_id,
                    StorageEffect.idempotency_key == idempotency_key,
                ))).scalars().one_or_none()
            return EffectRecord.from_row(row) if row else None

    # ── outbox: dispatch claim (CAS) + attempt recording ──────────────────

    async def claim_effect_dispatch(
        self, *, app_name: str, user_id: str, session_id: str,
        idempotency_key: str, claimer: str, lease_ttl_ms: int = 60_000,
        now_ms: int = 0,
    ) -> tuple[bool, Optional[EffectRecord]]:
        """Atomic CAS lease on the dispatch slot.

        The CAS predicate: row is PENDING + OUTBOX + dispatch-eligible
        (`next_dispatch_at_ms <= now`) + the existing lease (if any) has
        expired. Implementation is one UPDATE with the predicate inline
        and `rowcount=1` means "we won."

        Returns (acquired, effect). When acquired=False, effect is still
        the current row (so the caller can see WHY it lost — useful for
        the doctor/triage view)."""
        await self._prepare_tables()
        now = now_ms or _now_ms()
        expires = now + lease_ttl_ms

        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            # The CAS itself — a single conditional UPDATE.
            stmt = (
                update(StorageEffect)
                .where(
                    StorageEffect.app_name == app_name,
                    StorageEffect.user_id == user_id,
                    StorageEffect.session_id == session_id,
                    StorageEffect.idempotency_key == idempotency_key,
                    StorageEffect.status == EffectStatus.PENDING,
                    StorageEffect.dispatch_mode == EffectDispatchMode.OUTBOX,
                    StorageEffect.next_dispatch_at_ms <= now,
                    # Lease is free if claimer is empty/NULL OR expired.
                    ((StorageEffect.dispatch_claimed_by.is_(None)) |
                     (StorageEffect.dispatch_claimed_by == "") |
                     (StorageEffect.dispatch_claim_expires_at_ms <= now)),
                )
                .values(
                    dispatch_claimed_by=claimer,
                    dispatch_claim_expires_at_ms=expires,
                )
            )
            result = await sql.execute(stmt)
            acquired = result.rowcount == 1
            await sql.commit()

            # Whether or not we won, return the current row.
            row = (await sql.execute(
                select(StorageEffect).where(
                    StorageEffect.app_name == app_name,
                    StorageEffect.user_id == user_id,
                    StorageEffect.session_id == session_id,
                    StorageEffect.idempotency_key == idempotency_key,
                ))).scalars().one_or_none()
            return acquired, (EffectRecord.from_row(row) if row else None)

    async def record_dispatch_attempt(
        self, *, app_name: str, user_id: str, session_id: str,
        idempotency_key: str, error: str, next_dispatch_at_ms: int,
    ) -> Optional[EffectRecord]:
        """Report a failed dispatch.

        `next_dispatch_at_ms = 0` is the load-bearing case: it transitions
        the effect to UNKNOWN (the reconciler will resolve it). Any positive
        value schedules a retry — the effect stays PENDING.
        """
        await self._prepare_tables()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            row = (await sql.execute(
                select(StorageEffect).where(
                    StorageEffect.app_name == app_name,
                    StorageEffect.user_id == user_id,
                    StorageEffect.session_id == session_id,
                    StorageEffect.idempotency_key == idempotency_key,
                ))).scalars().one_or_none()
            if row is None:
                return None
            row.dispatch_attempts = (row.dispatch_attempts or 0) + 1
            row.last_dispatch_error = error
            row.dispatch_claimed_by = None
            row.dispatch_claim_expires_at_ms = 0
            if next_dispatch_at_ms <= 0:
                # Terminal-for-outbox — the reconciler takes over.
                row.status = EffectStatus.UNKNOWN
                row.next_dispatch_at_ms = 0
            else:
                row.next_dispatch_at_ms = next_dispatch_at_ms
            row.ts_ms = _now_ms()
            await sql.commit()
            return EffectRecord.from_row(row)

    async def record_external_observation(
        self, *, app_name: str, user_id: str, session_id: str,
        idempotency_key: str, resolution: str,
        external_ref: str = "",
        response_json: Optional[Any] = None,
        error_json: Optional[Any] = None,
        compensate_on_duplicate_kind: str = "",
    ) -> Optional[EffectRecord]:
        """The reconciler's write path. Maps `EffectResolution` →
        `EffectStatus` and, on DUPLICATE, atomically registers a
        compensation obligation if `compensate_on_duplicate_kind` is set."""
        await self._prepare_tables()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            row = (await sql.execute(
                select(StorageEffect).where(
                    StorageEffect.app_name == app_name,
                    StorageEffect.user_id == user_id,
                    StorageEffect.session_id == session_id,
                    StorageEffect.idempotency_key == idempotency_key,
                ))).scalars().one_or_none()
            if row is None:
                return None

            now = _now_ms()
            if resolution == EffectResolution.CONFIRMED:
                row.status = EffectStatus.CONFIRMED
                row.external_ref = external_ref or row.external_ref
                row.response_json = response_json
            elif resolution == EffectResolution.FAILED:
                row.status = EffectStatus.FAILED
                row.error_json = error_json
            elif resolution == EffectResolution.ABSENT:
                # For non-idempotent: stays UNKNOWN — needs human approval.
                # For idempotent: caller may re-issue; we leave status as-is.
                if row.semantics == EffectSemantics.NON_IDEMPOTENT:
                    row.status = EffectStatus.UNKNOWN
                # The caller can also pass an error_json to record the
                # observation cleanly.
                if error_json is not None:
                    row.error_json = error_json
            elif resolution == EffectResolution.DUPLICATE:
                row.status = EffectStatus.CONFIRMED
                row.external_ref = external_ref or row.external_ref
                row.response_json = response_json
                if compensate_on_duplicate_kind:
                    # Atomically register the compensation in the SAME
                    # transaction. This is the proto's
                    # `compensate_on_duplicate_kind` shortcut.
                    sql.add(StorageObligation(
                        app_name=app_name, user_id=user_id,
                        session_id=session_id,
                        invocation_id=row.invocation_id,
                        effect_key=row.idempotency_key,
                        kind=compensate_on_duplicate_kind,
                        payload_json={
                            "external_ref": external_ref or row.external_ref,
                            "reason": "duplicate observed by reconciler",
                        },
                        status=ObligationStatus.PENDING,
                        attempts=0, max_attempts=5,
                        next_attempt_at_ms=now, ts_ms=now,
                    ))
            elif resolution == EffectResolution.STUCK:
                row.status = EffectStatus.FAILED
                row.error_json = error_json or {
                    "resolution": "stuck",
                    "detail": "reconciler couldn't resolve",
                }
            else:
                raise ValueError(f"unknown resolution: {resolution!r}")

            row.ts_ms = now
            await sql.commit()
            return EffectRecord.from_row(row)

    # ── reconciler / outbox queues ────────────────────────────────────────

    async def list_pending_effects(
        self, *, older_than_ms: int = 0, include_pending: bool = True,
        include_unknown: bool = True, limit: int = 200,
    ) -> list[EffectRecord]:
        """The reconciler's hot set: PENDING (older than `older_than_ms`)
        plus UNKNOWN. Cross-session."""
        await self._prepare_tables()
        async with self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            statuses = []
            if include_pending: statuses.append(EffectStatus.PENDING)
            if include_unknown: statuses.append(EffectStatus.UNKNOWN)
            if not statuses:
                return []
            stmt = select(StorageEffect).where(
                StorageEffect.status.in_(statuses))
            if older_than_ms > 0 and include_pending and not include_unknown:
                # Only filter on ts when scoping to PENDING; UNKNOWN should
                # always surface immediately.
                stmt = stmt.where(StorageEffect.ts_ms < older_than_ms)
            elif older_than_ms > 0 and include_pending:
                # Mixed: filter PENDING by age but always include UNKNOWN.
                stmt = stmt.where(
                    (StorageEffect.status == EffectStatus.UNKNOWN)
                    | ((StorageEffect.status == EffectStatus.PENDING)
                       & (StorageEffect.ts_ms < older_than_ms)))
            stmt = stmt.order_by(StorageEffect.ts_ms).limit(limit)
            rows = (await sql.execute(stmt)).scalars().all()
            return [EffectRecord.from_row(r) for r in rows]

    async def list_effects_to_dispatch(
        self, *, now_ms: int = 0, connector: str = "", limit: int = 200,
    ) -> list[EffectRecord]:
        """The outbox dispatcher's hot set: PENDING + OUTBOX +
        next_dispatch_at_ms <= now + (lease free or expired)."""
        await self._prepare_tables()
        now = now_ms or _now_ms()
        async with self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            stmt = select(StorageEffect).where(
                StorageEffect.status == EffectStatus.PENDING,
                StorageEffect.dispatch_mode == EffectDispatchMode.OUTBOX,
                StorageEffect.next_dispatch_at_ms <= now,
                ((StorageEffect.dispatch_claimed_by.is_(None)) |
                 (StorageEffect.dispatch_claimed_by == "") |
                 (StorageEffect.dispatch_claim_expires_at_ms <= now)),
            )
            if connector:
                stmt = stmt.where(StorageEffect.connector == connector)
            stmt = stmt.order_by(StorageEffect.ts_ms).limit(limit)
            rows = (await sql.execute(stmt)).scalars().all()
            return [EffectRecord.from_row(r) for r in rows]

    # ── obligation ledger ─────────────────────────────────────────────────

    async def register_compensation(
        self, *, app_name: str, user_id: str, session_id: str,
        invocation_id: str = "",
        effect_key: str, kind: str,
        payload_json: Optional[Any] = None,
        compensator_ref: Optional[str] = None,
        max_attempts: int = 5,
    ) -> ObligationRecord:
        """Idempotent on (session, effect_key, kind) — a second call returns
        the existing row instead of creating a duplicate."""
        await self._prepare_tables()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            existing = (await sql.execute(
                select(StorageObligation).where(
                    StorageObligation.app_name == app_name,
                    StorageObligation.user_id == user_id,
                    StorageObligation.session_id == session_id,
                    StorageObligation.effect_key == effect_key,
                    StorageObligation.kind == kind,
                ))).scalars().one_or_none()
            if existing is not None:
                return ObligationRecord.from_row(existing)
            now = _now_ms()
            row = StorageObligation(
                app_name=app_name, user_id=user_id,
                session_id=session_id, invocation_id=invocation_id,
                effect_key=effect_key, kind=kind,
                payload_json=payload_json,
                status=ObligationStatus.PENDING,
                attempts=0, max_attempts=max_attempts or 5,
                next_attempt_at_ms=now,
                last_error=None, claimed_by=None,
                claim_expires_at_ms=0,
                compensator_ref=compensator_ref,
                result_json=None, ts_ms=now,
            )
            sql.add(row)
            await sql.commit()
            return ObligationRecord.from_row(row)

    async def list_obligations(
        self, *, app_name: str, user_id: str, session_id: str,
        only_unresolved: bool = True, status_filter: str = "",
    ) -> list[ObligationRecord]:
        """Per-session, LIFO (`seq DESC`)."""
        await self._prepare_tables()
        async with self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            stmt = select(StorageObligation).where(
                StorageObligation.app_name == app_name,
                StorageObligation.user_id == user_id,
                StorageObligation.session_id == session_id,
            )
            if status_filter:
                stmt = stmt.where(StorageObligation.status == status_filter)
            elif only_unresolved:
                stmt = stmt.where(StorageObligation.status.in_(
                    [ObligationStatus.PENDING, ObligationStatus.COMMITTED]))
            stmt = stmt.order_by(StorageObligation.seq.desc())
            rows = (await sql.execute(stmt)).scalars().all()
            return [ObligationRecord.from_row(r) for r in rows]

    async def list_unresolved_obligations(
        self, *, now_ms: int = 0, limit: int = 500,
        include_pending: bool = True, include_stuck: bool = False,
        include_committed_expired: bool = True,
    ) -> list[ObligationRecord]:
        """Cross-session drainer feed. Mirrors the proto's
        `ListUnresolvedObligationsRequest` semantics: PENDING-ready +
        COMMITTED-expired by default; flip `include_stuck` for triage."""
        await self._prepare_tables()
        now = now_ms or _now_ms()
        async with self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            conds = []
            if include_pending:
                conds.append(
                    (StorageObligation.status == ObligationStatus.PENDING)
                    & (StorageObligation.next_attempt_at_ms <= now))
            if include_committed_expired:
                conds.append(
                    (StorageObligation.status == ObligationStatus.COMMITTED)
                    & (StorageObligation.claim_expires_at_ms <= now))
            if include_stuck:
                conds.append(
                    StorageObligation.status == ObligationStatus.STUCK)
            if not conds:
                return []
            from functools import reduce
            from operator import or_
            stmt = select(StorageObligation).where(
                reduce(or_, conds)).order_by(
                StorageObligation.seq.desc()).limit(limit)
            rows = (await sql.execute(stmt)).scalars().all()
            return [ObligationRecord.from_row(r) for r in rows]

    async def claim_obligation(
        self, *, seq: int, claimer: str, lease_ttl_ms: int = 60_000,
        now_ms: int = 0,
    ) -> tuple[bool, Optional[ObligationRecord]]:
        """Atomic CAS — single winner, same shape as `claim_effect_dispatch`.
        Also reclaims COMMITTED rows whose claim_expires_at_ms <= now."""
        await self._prepare_tables()
        now = now_ms or _now_ms()
        expires = now + lease_ttl_ms
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            stmt = (
                update(StorageObligation)
                .where(
                    StorageObligation.seq == seq,
                    (
                        ((StorageObligation.status == ObligationStatus.PENDING)
                         & (StorageObligation.next_attempt_at_ms <= now))
                        | ((StorageObligation.status == ObligationStatus.COMMITTED)
                           & (StorageObligation.claim_expires_at_ms <= now))
                    ),
                )
                .values(
                    status=ObligationStatus.COMMITTED,
                    claimed_by=claimer,
                    claim_expires_at_ms=expires,
                )
            )
            result = await sql.execute(stmt)
            acquired = result.rowcount == 1
            await sql.commit()
            row = (await sql.execute(
                select(StorageObligation).where(
                    StorageObligation.seq == seq))).scalars().one_or_none()
            return acquired, (ObligationRecord.from_row(row) if row else None)

    async def record_obligation_attempt(
        self, *, seq: int, error: str, next_attempt_at_ms: int,
    ) -> Optional[ObligationRecord]:
        """Report a failed compensation attempt. `next_attempt_at_ms=0`
        forces STUCK (terminal-now). Otherwise: bump attempts; if
        `attempts >= max_attempts` mark STUCK; else reschedule PENDING."""
        await self._prepare_tables()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            row = (await sql.execute(
                select(StorageObligation).where(
                    StorageObligation.seq == seq))).scalars().one_or_none()
            if row is None:
                return None
            row.attempts = (row.attempts or 0) + 1
            row.last_error = error
            row.claimed_by = None
            row.claim_expires_at_ms = 0
            row.ts_ms = _now_ms()
            if next_attempt_at_ms <= 0 or row.attempts >= row.max_attempts:
                row.status = ObligationStatus.STUCK
                row.next_attempt_at_ms = 0
            else:
                row.status = ObligationStatus.PENDING
                row.next_attempt_at_ms = next_attempt_at_ms
            await sql.commit()
            return ObligationRecord.from_row(row)

    async def resolve_obligation(
        self, *, seq: int, status: str,
        result_json: Optional[Any] = None,
    ) -> Optional[ObligationRecord]:
        """Terminal transition: COMPENSATED (success) or STUCK (failure)."""
        if status not in (ObligationStatus.COMPENSATED,
                           ObligationStatus.STUCK):
            raise ValueError(
                f"resolve_obligation: status must be COMPENSATED or STUCK, "
                f"got {status!r}")
        await self._prepare_tables()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            row = (await sql.execute(
                select(StorageObligation).where(
                    StorageObligation.seq == seq))).scalars().one_or_none()
            if row is None:
                return None
            row.status = status
            row.result_json = result_json
            row.claimed_by = None
            row.claim_expires_at_ms = 0
            row.ts_ms = _now_ms()
            await sql.commit()
            return ObligationRecord.from_row(row)

    # ── timers ────────────────────────────────────────────────────────────

    async def set_timer(
        self, *, app_name: str, user_id: str, session_id: str,
        timer_id: str, fire_at_ms: int, kind: str,
        payload_json: Optional[Any] = None,
    ) -> TimerRecord:
        """Idempotent on (session, timer_id) — a second `set_timer` with the
        same id returns the existing record."""
        await self._prepare_tables()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            existing = (await sql.execute(
                select(StorageTimer).where(
                    StorageTimer.app_name == app_name,
                    StorageTimer.user_id == user_id,
                    StorageTimer.session_id == session_id,
                    StorageTimer.timer_id == timer_id,
                ))).scalars().one_or_none()
            if existing is not None:
                return TimerRecord.from_row(existing)
            now = _now_ms()
            row = StorageTimer(
                app_name=app_name, user_id=user_id,
                session_id=session_id, timer_id=timer_id,
                fire_at_ms=fire_at_ms, kind=kind,
                payload_json=payload_json, fired=False,
                created_at_ms=now,
            )
            sql.add(row)
            await sql.commit()
            return TimerRecord.from_row(row)

    async def list_due_timers(
        self, *, now_ms: int = 0, limit: int = 200, claim: bool = False,
    ) -> list[TimerRecord]:
        """Returns timers with `fire_at_ms <= now` and `fired == False`.
        If `claim=True`, atomically marks them fired in the same txn so peer
        timer reactors don't re-fire."""
        await self._prepare_tables()
        now = now_ms or _now_ms()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            stmt = select(StorageTimer).where(
                StorageTimer.fired.is_(False),
                StorageTimer.fire_at_ms <= now,
            ).order_by(StorageTimer.fire_at_ms).limit(limit)
            rows = (await sql.execute(stmt)).scalars().all()
            results = [TimerRecord.from_row(r) for r in rows]
            if claim and rows:
                for r in rows:
                    r.fired = True
                await sql.commit()
            return results

    async def cancel_timer(
        self, *, app_name: str, user_id: str, session_id: str,
        timer_id: str,
    ) -> bool:
        await self._prepare_tables()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            result = await sql.execute(
                delete(StorageTimer).where(
                    StorageTimer.app_name == app_name,
                    StorageTimer.user_id == user_id,
                    StorageTimer.session_id == session_id,
                    StorageTimer.timer_id == timer_id,
                ))
            await sql.commit()
            return result.rowcount > 0

    # ── reactive KV (proto §WriteValue / WatchValue / GetValue) ──────────

    async def write_value(
        self, *, namespace: str, key: str, value_json: Optional[Any],
        if_version: int = -1, writer: str = "",
    ) -> "StorageValue":
        """Optimistic-CAS write. `if_version < 0` disables CAS (last writer
        wins). `if_version == current_version` advances; mismatch raises."""
        await self._prepare_tables()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            existing = (await sql.execute(
                select(StorageValue).where(
                    StorageValue.namespace == namespace,
                    StorageValue.key == key,
                ))).scalars().one_or_none()
            now = _now_ms()
            if existing is None:
                if if_version >= 0 and if_version != 0:
                    raise ValueError(
                        f"write_value: if_version={if_version} but no prior "
                        f"row exists (version 0)")
                row = StorageValue(
                    namespace=namespace, key=key, value_json=value_json,
                    version=1, ts_ms=now, writer=writer or None,
                    deleted=False,
                )
                sql.add(row)
                await sql.commit()
                return row
            if if_version >= 0 and if_version != existing.version:
                raise ValueError(
                    f"write_value: stale CAS — if_version={if_version}, "
                    f"current={existing.version}")
            existing.value_json = value_json
            existing.version = (existing.version or 0) + 1
            existing.ts_ms = now
            existing.writer = writer or existing.writer
            existing.deleted = False
            await sql.commit()
            return existing

    # ── continue-as-new (mechanism #4 in the compaction roadmap) ────────

    async def take_snapshot(
        self,
        *,
        app_name: str, user_id: str, session_id: str,
        up_to_ts_ms: int = 0,
    ) -> dict:
        """Capture terminal effects under this session into the per-session
        snapshot row. Merges with the existing snapshot — re-calling this
        is the cumulative way to keep the snapshot current as new effects
        confirm.

        After a snapshot, the compactor can safely prune the underlying
        terminal effect rows: `begin_effect` falls back to the snapshot's
        JSON map for the idempotency-key short-circuit, so re-dispatch is
        prevented even when the source row is gone.

        `up_to_ts_ms=0` (default) captures everything with a terminal
        status. Pass an explicit watermark to bound the snapshot for
        large sessions (the watermark goes into `up_to_ts_ms` on the row
        so operators can see how fresh the snapshot is).

        Returns `{captured: N, merged_total: M, up_to_ts_ms: T}`.
        The snapshot row is read by `begin_effect` as a fallback when
        the live effect row is gone — that's the *only* place the
        snapshot is on the read path."""
        await self._prepare_tables()
        watermark = up_to_ts_ms or _now_ms()
        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            # Read all terminal effects under this session up to the
            # watermark. The set is bounded by the session's effect
            # count — operators with very large sessions should pass a
            # watermark to limit the read window.
            q = select(StorageEffect).where(
                StorageEffect.app_name == app_name,
                StorageEffect.user_id == user_id,
                StorageEffect.session_id == session_id,
                StorageEffect.status.in_(
                    (EffectStatus.CONFIRMED, EffectStatus.FAILED)),
                StorageEffect.ts_ms <= watermark,
            )
            rows = (await sql.execute(q)).scalars().all()

            # Map each effect to the minimum data the short-circuit needs.
            # Keep the per-effect blob small — the snapshot row is one
            # JSON column and we don't want it to balloon.
            captured: dict[str, Any] = {}
            for r in rows:
                captured[r.idempotency_key] = {
                    "status": r.status,
                    "semantics": r.semantics,
                    "dispatch_mode": r.dispatch_mode,
                    "business_key": r.business_key,
                    "connector": r.connector,
                    "external_ref": r.external_ref,
                    "request_json": r.request_json,
                    "response_json": r.response_json,
                    "error_json": r.error_json,
                    "invocation_id": r.invocation_id,
                    "decision_index": r.decision_index,
                    "tool_name": r.tool_name,
                    "call_index": r.call_index,
                    "ts_ms": r.ts_ms,
                }

            now = _now_ms()
            snap = (await sql.execute(
                select(StorageEffectSnapshot).where(
                    StorageEffectSnapshot.app_name == app_name,
                    StorageEffectSnapshot.user_id == user_id,
                    StorageEffectSnapshot.session_id == session_id,
                ))).scalars().one_or_none()
            if snap is None:
                merged = dict(captured)
                sql.add(StorageEffectSnapshot(
                    app_name=app_name, user_id=user_id,
                    session_id=session_id,
                    effects_json=merged,
                    up_to_ts_ms=watermark,
                    effects_count=len(merged),
                    created_at_ms=now, updated_at_ms=now,
                ))
            else:
                # Set-union, last-write-wins: a later snapshot that has
                # a different status for the same key (e.g. UNKNOWN that
                # the reconciler later flipped to CONFIRMED) overwrites.
                merged = dict(snap.effects_json or {})
                merged.update(captured)
                snap.effects_json = merged
                snap.up_to_ts_ms = max(snap.up_to_ts_ms or 0, watermark)
                snap.effects_count = len(merged)
                snap.updated_at_ms = now

            await sql.commit()
            return {
                "captured": len(captured),
                "merged_total": len(merged),
                "up_to_ts_ms": watermark,
            }

    async def get_snapshot(
        self, *, app_name: str, user_id: str, session_id: str,
    ) -> Optional["StorageEffectSnapshot"]:
        """Read the snapshot row for inspection / debugging. Returns the
        full ORM row (with `.effects_json` already a dict). `None` if no
        snapshot has been taken for this session."""
        await self._prepare_tables()
        async with self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            return (await sql.execute(
                select(StorageEffectSnapshot).where(
                    StorageEffectSnapshot.app_name == app_name,
                    StorageEffectSnapshot.user_id == user_id,
                    StorageEffectSnapshot.session_id == session_id,
                ))).scalars().one_or_none()

    async def continue_as_new(
        self,
        *,
        app_name: str, user_id: str, session_id: str,
        old_invocation_id: str,
        new_invocation_id: str,
        carried_state: Optional[Any] = None,
        prune_old: bool = True,
    ) -> dict:
        """End one invocation chapter, start a new one in the same ADK
        session, with optional state carried forward.

        Atomic — one SQL transaction commits the prune + the carried-state
        write together. Temporal's `continue-as-new` mapped onto the
        embedded model: there's no separate "run" lifecycle to close
        (ADK's session is the long-lived unit), only an `invocation_id`
        to retire. The caller continues issuing RPCs under
        `new_invocation_id`.

        `prune_old=True` (default): delete the old invocation's terminal
        effects that aren't pinned by an active obligation. Same NOT
        EXISTS guard as the compactor — the pinning mechanism doesn't
        get a special case here. Effects in non-terminal states under
        the old invocation are kept (a still-PENDING effect under a
        retired invocation is a bug elsewhere; surface it, don't
        silently delete it).

        `carried_state`, when provided, is written to a `tape_values` row
        at `namespace='tape:continue-as-new:<session_id>'`,
        `key=<new_invocation_id>` — a small protocol the new invocation
        can read on startup to pick up where the old one left off.

        Returns a dict with `effects_pruned` + `state_written` for the
        audit log."""
        await self._prepare_tables()
        from sqlalchemy import and_, delete, exists, not_
        from .schemas import StorageEffect, StorageObligation, StorageValue

        result = {"effects_pruned": 0, "state_written": False,
                  "obligations_kept": 0}
        now = _now_ms()

        async with self._write_lock(), \
                self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]

            # Prune old-invocation terminal effects, pinning-respecting.
            if prune_old:
                pin_subq = exists().where(and_(
                    StorageObligation.session_id
                    == StorageEffect.session_id,
                    StorageObligation.effect_key
                    == StorageEffect.idempotency_key,
                    StorageObligation.status.in_(
                        ("pending", "committed")),
                ))
                r = await sql.execute(
                    delete(StorageEffect).where(
                        StorageEffect.app_name == app_name,
                        StorageEffect.user_id == user_id,
                        StorageEffect.session_id == session_id,
                        StorageEffect.invocation_id == old_invocation_id,
                        StorageEffect.status.in_(
                            ("confirmed", "failed")),
                        not_(pin_subq),
                    ).execution_options(synchronize_session=False))
                result["effects_pruned"] = r.rowcount or 0

                # Surface any obligations that still pin OLD-invocation
                # effects — they're the reason this continue_as_new
                # didn't fully reset the slate. The pinning relationship
                # is via `effect_key` → `idempotency_key`, not the
                # obligation's own invocation_id; an obligation
                # registered in a later invocation can still pin an
                # earlier invocation's row.
                pinned = (await sql.execute(
                    select(StorageObligation).where(
                        StorageObligation.app_name == app_name,
                        StorageObligation.user_id == user_id,
                        StorageObligation.session_id == session_id,
                        StorageObligation.status.in_(
                            ("pending", "committed")),
                        StorageObligation.effect_key.in_(
                            select(StorageEffect.idempotency_key).where(
                                StorageEffect.app_name == app_name,
                                StorageEffect.user_id == user_id,
                                StorageEffect.session_id == session_id,
                                StorageEffect.invocation_id
                                == old_invocation_id,
                            )),
                    ))).scalars().all()
                result["obligations_kept"] = len(pinned)

            # Carry state forward as a tape_value if requested.
            if carried_state is not None:
                ns = f"tape:continue-as-new:{session_id}"
                existing = (await sql.execute(
                    select(StorageValue).where(
                        StorageValue.namespace == ns,
                        StorageValue.key == new_invocation_id,
                    ))).scalars().one_or_none()
                if existing is None:
                    sql.add(StorageValue(
                        namespace=ns, key=new_invocation_id,
                        value_json=carried_state, version=1,
                        ts_ms=now, writer="continue_as_new", deleted=False))
                else:
                    existing.value_json = carried_state
                    existing.version = (existing.version or 0) + 1
                    existing.ts_ms = now
                    existing.writer = "continue_as_new"
                    existing.deleted = False
                result["state_written"] = True

            await sql.commit()

        return result

    async def get_value(
        self, *, namespace: str, key: str,
    ) -> Optional["StorageValue"]:
        await self._prepare_tables()
        async with self._rollback_on_exception_session() as sql:  # type: ignore[attr-defined]
            return (await sql.execute(
                select(StorageValue).where(
                    StorageValue.namespace == namespace,
                    StorageValue.key == key,
                ))).scalars().one_or_none()


__all__ = [
    "TapeSessionService",
    "EffectStatus", "EffectSemantics", "EffectDispatchMode",
    "EffectResolution", "ObligationStatus",
    "EffectRecord", "ObligationRecord", "TimerRecord",
]
