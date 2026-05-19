"""SQLAlchemy tables for the effect ledger, obligation ledger, server-side
timer registry, and reactive KV — sitting alongside ADK's `sessions` /
`events` / `app_states` / `user_states` tables and using the **same**
declarative `Base`, so `create_all()` on the ADK base sets them up too.

The tables are intentionally **siblings** of ADK's, not children — keyed by
the same `(app_name, user_id, session_id)` triple where it makes sense, with
a foreign key into `sessions` so the cascade-delete behaviour ADK already
ships propagates.

Schema decisions worth calling out:

* `tape_effects.idempotency_key` is the natural per-effect identifier — it
  matches the proto's `EffectRecord.idempotency_key` and is the dedup key
  the SDK derives from `(run_id, decision_index, tool, call_index)` or a
  caller-supplied `custom_key`.

* `tape_effects.(connector, business_key)` has a partial unique index when
  both are non-empty — this is what enforces the non-idempotent contract's
  "no two effects can share a business key for the same connector" rule.
  (SQLite's partial-index syntax differs slightly; we emit the constraint
  as a UNIQUE(connector, business_key) constraint and rely on the SDK to
  pass NULLs rather than empty strings when the dedup doesn't apply.)

* `tape_obligations.seq` is autoincrement and is the LIFO drain order;
  the proto-side `seq` is per-run, but per-table here is functionally
  equivalent because obligations are addressed cross-run only by the
  drainer reactor (which sorts by `seq DESC` anyway).

* The CAS lease columns (`dispatch_claimed_by`, `dispatch_claim_expires_at_ms`
  on effects; `claimed_by`, `claim_expires_at_ms` on obligations) are
  load-bearing — the row-level CAS we run against them is the primitive
  ADK's session lock can't express. See `cas.py` for the UPDATE…WHERE
  pattern.
"""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

# We extend ADK's own declarative Base — that way `Base.metadata.create_all()`
# on the ADK base creates BOTH the ADK tables and these. One migration story.
from google.adk.sessions.schemas.v1 import (
    Base,
    DEFAULT_MAX_KEY_LENGTH,
    DEFAULT_MAX_VARCHAR_LENGTH,
    DynamicJSON,
)


# ── effect ledger ───────────────────────────────────────────────────────────


class StorageEffect(Base):
    """One row per (run/invocation) × tool call. Mirrors `EffectRecord` in
    `tape/proto/tape.proto`.

    The status state machine — `pending` → (`confirmed` | `failed` |
    `unknown`) — is enforced by the service methods, not by the schema.
    """

    __tablename__ = "tape_effects"

    # Composite primary key: keyed on (session, idempotency_key). The
    # idempotency_key alone would suffice for uniqueness but adding the
    # session columns lets us cluster effects with their session for the
    # common per-session query path.
    app_name: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )
    user_id: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )
    session_id: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )
    idempotency_key: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_VARCHAR_LENGTH), primary_key=True
    )

    # Provenance — which invocation + decision authorised this effect.
    invocation_id: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_VARCHAR_LENGTH)
    )
    decision_index: Mapped[int] = mapped_column(Integer, default=-1)
    tool_name: Mapped[str] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH))
    call_index: Mapped[int] = mapped_column(Integer, default=0)

    # State machine — see `tape/proto/tape.proto` §EffectStatus
    status: Mapped[str] = mapped_column(String(16), index=True)

    # Contract — see proto §EffectSemantics + §EffectDispatchMode
    semantics: Mapped[str] = mapped_column(String(16), default="idempotent")
    dispatch_mode: Mapped[str] = mapped_column(String(16), default="inline")

    # Outbox extras (NULL or empty for inline effects). The (connector,
    # business_key) uniqueness is what makes the non-idempotent contract
    # safe across runs — the bank's own dedup key can't be duplicated
    # in our ledger.
    business_key: Mapped[Optional[str]] = mapped_column(
        String(DEFAULT_MAX_VARCHAR_LENGTH), nullable=True
    )
    connector: Mapped[Optional[str]] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), nullable=True
    )
    external_ref: Mapped[Optional[str]] = mapped_column(
        String(DEFAULT_MAX_VARCHAR_LENGTH), nullable=True
    )
    dispatch_attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_dispatch_at_ms: Mapped[int] = mapped_column(BigInteger, default=0)
    dispatch_claimed_by: Mapped[Optional[str]] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), nullable=True
    )
    dispatch_claim_expires_at_ms: Mapped[int] = mapped_column(
        BigInteger, default=0
    )
    last_dispatch_error: Mapped[Optional[str]] = mapped_column(
        DynamicJSON, nullable=True
    )

    # Payloads — opaque JSON blobs the SDK serialises in/out of.
    request_json: Mapped[Optional[Any]] = mapped_column(DynamicJSON, nullable=True)
    response_json: Mapped[Optional[Any]] = mapped_column(DynamicJSON, nullable=True)
    error_json: Mapped[Optional[Any]] = mapped_column(DynamicJSON, nullable=True)

    ts_ms: Mapped[int] = mapped_column(BigInteger)

    __table_args__ = (
        ForeignKeyConstraint(
            ["app_name", "user_id", "session_id"],
            ["sessions.app_name", "sessions.user_id", "sessions.id"],
            ondelete="CASCADE",
        ),
        # Cross-run dedup on (connector, business_key). Both NULL → no
        # uniqueness (NULLs are distinct in standard SQL). This is the
        # constraint that makes "no two effects can wire the same logical
        # operation" structural rather than convention.
        UniqueConstraint(
            "connector", "business_key",
            name="uq_tape_effects_connector_business_key",
        ),
        # Indexes the reactor queries need:
        Index("ix_tape_effects_status_ts", "status", "ts_ms"),
        Index(
            "ix_tape_effects_dispatch_ready",
            "dispatch_mode", "status", "next_dispatch_at_ms",
        ),
        Index("ix_tape_effects_invocation", "invocation_id"),
    )


# ── obligation ledger ───────────────────────────────────────────────────────


class StorageObligation(Base):
    """One row per registered compensation. The drainer reactor pulls these
    in LIFO order (by `seq DESC`), runs the inverse, and marks them
    `compensated` or `stuck`. Mirrors `ObligationRecord` in the proto."""

    __tablename__ = "tape_obligations"

    seq: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    app_name: Mapped[str] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH))
    user_id: Mapped[str] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH))
    session_id: Mapped[str] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH))
    invocation_id: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_VARCHAR_LENGTH), default=""
    )

    # Which effect this is the inverse of.
    effect_key: Mapped[str] = mapped_column(String(DEFAULT_MAX_VARCHAR_LENGTH))
    # The handler name (e.g. "reverse_wire") — looked up in an in-process
    # registry OR resolved via `compensator_ref` ("module:attr") for cross
    # -process drainers.
    kind: Mapped[str] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH))
    payload_json: Mapped[Optional[Any]] = mapped_column(DynamicJSON, nullable=True)

    status: Mapped[str] = mapped_column(String(16), index=True, default="pending")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=5)
    next_attempt_at_ms: Mapped[int] = mapped_column(BigInteger, default=0)
    last_error: Mapped[Optional[str]] = mapped_column(
        DynamicJSON, nullable=True
    )

    claimed_by: Mapped[Optional[str]] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), nullable=True
    )
    claim_expires_at_ms: Mapped[int] = mapped_column(BigInteger, default=0)

    compensator_ref: Mapped[Optional[str]] = mapped_column(
        String(DEFAULT_MAX_VARCHAR_LENGTH), nullable=True
    )
    result_json: Mapped[Optional[Any]] = mapped_column(DynamicJSON, nullable=True)

    ts_ms: Mapped[int] = mapped_column(BigInteger)

    __table_args__ = (
        ForeignKeyConstraint(
            ["app_name", "user_id", "session_id"],
            ["sessions.app_name", "sessions.user_id", "sessions.id"],
            ondelete="CASCADE",
        ),
        # Idempotent registration on (effect_key, kind) per session — a
        # second `register_compensation` with the same kind for the same
        # effect should return the existing row, not create a duplicate.
        UniqueConstraint(
            "app_name", "user_id", "session_id", "effect_key", "kind",
            name="uq_tape_obligations_effect_kind_per_session",
        ),
        Index("ix_tape_obligations_status_next", "status", "next_attempt_at_ms"),
    )


# ── timer registry ──────────────────────────────────────────────────────────


class StorageTimer(Base):
    """Server-side timer registry. Used by gate-timeouts and any reactor
    that wants to schedule work at a future wall-clock instant. Mirrors
    `TimerRecord` in the proto."""

    __tablename__ = "tape_timers"

    app_name: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )
    user_id: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )
    session_id: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )
    timer_id: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_VARCHAR_LENGTH), primary_key=True
    )

    fire_at_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    kind: Mapped[str] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH))
    payload_json: Mapped[Optional[Any]] = mapped_column(DynamicJSON, nullable=True)
    fired: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at_ms: Mapped[int] = mapped_column(BigInteger)

    __table_args__ = (
        ForeignKeyConstraint(
            ["app_name", "user_id", "session_id"],
            ["sessions.app_name", "sessions.user_id", "sessions.id"],
            ondelete="CASCADE",
        ),
        Index("ix_tape_timers_due", "fired", "fire_at_ms"),
    )


# ── reactive KV (the "coordinate through state" surface) ────────────────────


class StorageValue(Base):
    """Reactive key/value store with monotonic per-key versioning. Mirrors
    `ValueRecord` in the proto. CAS writes use `if_version` to refuse stale
    overwrites."""

    __tablename__ = "tape_values"

    namespace: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )
    key: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_VARCHAR_LENGTH), primary_key=True
    )

    value_json: Mapped[Optional[Any]] = mapped_column(DynamicJSON, nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=0)
    ts_ms: Mapped[int] = mapped_column(BigInteger)
    writer: Mapped[Optional[str]] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), nullable=True
    )
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)


__all__ = [
    "Base",
    "StorageEffect",
    "StorageObligation",
    "StorageTimer",
    "StorageValue",
]
