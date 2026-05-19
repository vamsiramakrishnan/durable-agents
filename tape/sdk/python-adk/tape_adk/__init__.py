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

__all__ = [
    "TapeSessionService",
    "EffectStatus",
    "EffectSemantics",
    "EffectDispatchMode",
    "EffectResolution",
    "ObligationStatus",
    "EffectRecord",
    "ObligationRecord",
    "TimerRecord",
]
