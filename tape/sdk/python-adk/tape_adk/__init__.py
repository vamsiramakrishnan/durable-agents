"""tape-adk — ADK's DatabaseSessionService extended with the non-idempotent
safety contract, outbox dispatch, reconciler, and compensation ledger.

The public surface is small:

  TapeSessionService    — the DatabaseSessionService subclass.
                          Drop in where you'd use ADK's own.
  EffectStatus, EffectSemantics, EffectDispatchMode, EffectResolution,
  ObligationStatus      — the state-machine vocabulary.
  EffectRecord, ObligationRecord, TimerRecord
                        — lightweight return shapes.

Reactor library functions live in `tape_adk.reactors`. The ADK plugin lives
in `tape_adk.plugin` (Phase 2).
"""

from __future__ import annotations

from .service import (
    EffectDispatchMode,
    EffectRecord,
    EffectResolution,
    EffectSemantics,
    EffectStatus,
    ObligationRecord,
    ObligationStatus,
    TapeSessionService,
    TimerRecord,
)
from .connectors import (
    CompensationResult,
    Connector,
    DispatchResult,
    LogConnector,
    ObservationResult,
)
from .reactors import (
    dispatch_outbox_once,
    drain_obligations_once,
    fire_due_timers_once,
    reconcile_once,
)
from .decorators import effect, meta_of, outbox_tool
from .plugin import AckLost, NonIdempotentSafetyPlugin
from .compact import CompactionPolicy, CompactionResult, compact_once
from .schemas import StorageEffectSnapshot
from . import chaos

__all__ = [
    # service
    "TapeSessionService",
    "EffectStatus",
    "EffectSemantics",
    "EffectDispatchMode",
    "EffectResolution",
    "ObligationStatus",
    "EffectRecord",
    "ObligationRecord",
    "TimerRecord",
    # connectors
    "Connector",
    "DispatchResult",
    "ObservationResult",
    "CompensationResult",
    "LogConnector",
    # reactors
    "dispatch_outbox_once",
    "reconcile_once",
    "drain_obligations_once",
    "fire_due_timers_once",
    # decorators
    "effect",
    "outbox_tool",
    "meta_of",
    # plugin
    "NonIdempotentSafetyPlugin",
    "AckLost",
    # chaos
    "chaos",
    # compaction
    "CompactionPolicy",
    "CompactionResult",
    "compact_once",
    # snapshot
    "StorageEffectSnapshot",
]
