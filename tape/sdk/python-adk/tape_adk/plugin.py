"""`NonIdempotentSafetyPlugin` — an ADK `BasePlugin` that rides ADK's tool
callbacks to journal effects into `TapeSessionService`.

The contract this plugin adds to a vanilla ADK Runner:

* For tools decorated `@effect`: before the tool runs, journal an intent
  (BeginEffect with IDEMPOTENT/INLINE semantics). After the tool returns,
  complete the effect with CONFIRMED + the result. On exception: complete
  with FAILED (or UNKNOWN if the exception is a recognised "ack-lost"
  signal). On replay (agent re-driven for the same invocation), the
  intent already exists — if it's CONFIRMED, the recorded response is
  returned without calling the tool body.

* For tools decorated `@outbox_tool`: refuse to call the tool inline.
  Journal an OUTBOX intent (NON_IDEMPOTENT semantics), return a "pending"
  response so ADK records it in events. The outbox dispatcher reactor
  picks up the intent, calls the connector, completes the effect.

* For tools NOT decorated: pass through. The plugin is opt-in per-tool —
  agents that have no effect-ledger needs aren't affected.

The plugin uses a `TapeSessionService` directly, not over gRPC. Every
write goes through ADK's own SQLAlchemy engine — so the function_call
event and the effect intent commit atomically (or at minimum: they
commit through the same engine, behind the same per-session lock; there's
no two-database divergence to worry about).
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from google.adk.plugins.base_plugin import BasePlugin
from google.adk.tools.base_tool import BaseTool
from google.adk.tools.tool_context import ToolContext

from .decorators import meta_of
from .service import (
    EffectDispatchMode,
    EffectRecord,
    EffectSemantics,
    EffectStatus,
    TapeSessionService,
)


logger = logging.getLogger(__name__)


class AckLost(Exception):
    """Raised by tool bodies to signal "the upstream call landed, but we
    couldn't confirm it" — the plugin maps this to EffectStatus.UNKNOWN
    on the effect ledger, which kicks the reconciler in. Tools that don't
    have a way to distinguish UNKNOWN from FAILED don't need to raise it;
    a regular Exception lands as FAILED."""


class NonIdempotentSafetyPlugin(BasePlugin):
    """Drop-in BasePlugin that wires ADK tool callbacks → TapeSessionService.

    Construct with the same `TapeSessionService` you pass as
    `session_service` to your `Runner`. The plugin reads tool metadata
    set by `@effect` / `@outbox_tool` to decide whether to journal each
    call.
    """

    def __init__(self, *, session_service: TapeSessionService,
                 name: str = "tape_non_idempotent_safety"):
        super().__init__(name=name)
        self.svc = session_service
        # Per-invocation state for keying effects to decisions + call_index.
        # The ADK plugin manager calls before_run_callback before any tool
        # callbacks for the same invocation, so we initialise these there.
        self._dnext: dict[str, int] = {}    # invocation_id → next decision idx
        self._dlast: dict[str, int] = {}    # invocation_id → last seen decision idx
        self._tcount: dict[tuple, int] = {}  # (inv, dec_idx, tool) → next call_index

    # ── lifecycle ─────────────────────────────────────────────────────────

    async def before_run_callback(self, *, invocation_context):
        inv = invocation_context.invocation_id
        # On resume, count decisions already in the session's events so the
        # decision index lines up with the journal's effect rows. Without
        # this the second turn would start at decision_index=0 and clash
        # with the journal's existing decision_index=0 row.
        prior = 0
        try:
            for ev in (invocation_context.session.events or []):
                if (ev.invocation_id == inv
                        and getattr(ev, "get_function_calls", None)
                        and ev.get_function_calls()):
                    # A function-call event implies a decision was made.
                    # We count those instead of model-response events
                    # because not every model response yields a tool call,
                    # but every tool call is attributable to one decision.
                    prior = max(prior, 1)
                    break
        except Exception:  # noqa: BLE001
            pass
        self._dnext[inv] = prior
        self._dlast[inv] = max(prior - 1, -1)

    async def after_run_callback(self, *, invocation_context):
        inv = invocation_context.invocation_id
        self._dnext.pop(inv, None)
        self._dlast.pop(inv, None)
        # Clean up any per-tool call counters for this invocation.
        for k in list(self._tcount.keys()):
            if k[0] == inv:
                self._tcount.pop(k, None)

    # ── decisions ────────────────────────────────────────────────────────

    async def after_model_callback(self, *, callback_context, llm_response):
        """Each model response that yields function calls is a 'decision'.
        We bump the per-invocation decision counter so subsequent tool
        callbacks key their effects against the correct decision_index."""
        inv = callback_context.invocation_id
        # Only count responses that include function calls; pure-text
        # responses don't authorise effects.
        has_calls = bool(getattr(llm_response, "get_function_calls", None)
                          and llm_response.get_function_calls())
        if not has_calls:
            return None
        idx = self._dnext.get(inv, 0)
        self._dnext[inv] = idx + 1
        self._dlast[inv] = idx
        return None

    # ── tools ────────────────────────────────────────────────────────────

    async def before_tool_callback(
        self, *, tool: BaseTool, tool_args: dict[str, Any],
        tool_context: ToolContext,
    ) -> Optional[dict]:
        """Journal an intent before the tool runs. If the existing record
        is already CONFIRMED (replay path), return its recorded response —
        ADK treats that as the tool's result and skips the body entirely.

        For @outbox_tool: don't let the tool run inline. Return a 'pending'
        sentinel that ADK records as the function_response; the outbox
        dispatcher resolves it later.
        """
        meta = meta_of(tool)
        if meta is None:
            return None  # not Tape-tracked

        inv = tool_context.invocation_id
        decision_idx = self._dlast.get(inv, 0)
        call_key = (inv, decision_idx, tool.name)
        call_index = self._tcount.get(call_key, 0)
        self._tcount[call_key] = call_index + 1

        # We need app_name / user_id / session_id off the invocation
        # context (which Context exposes via _invocation_context).
        ictx = tool_context._invocation_context  # type: ignore[attr-defined]
        app_name = ictx.app_name
        user_id = ictx.user_id
        session_id = ictx.session.id

        # Resolve business_key from args (only relevant for outbox tools).
        business_key = None
        if meta.business_key_fn or meta.business_key_static:
            business_key = meta.resolve_business_key(tool_args or {})

        try:
            eff: EffectRecord = await self.svc.begin_effect(
                app_name=app_name, user_id=user_id, session_id=session_id,
                invocation_id=inv, decision_index=decision_idx,
                tool_name=tool.name, call_index=call_index,
                request_json=tool_args,
                semantics=meta.semantics,
                dispatch_mode=meta.dispatch_mode,
                business_key=business_key,
                connector=meta.connector,
            )
        except ValueError as ex:
            # Construction-time invariants the service refuses (NI+inline,
            # outbox-without-connector, duplicate business_key). Surface
            # via the after_tool_error path conceptually — but we can't
            # let the tool run, so return a failure dict.
            logger.warning("tape: begin_effect refused for %s: %s",
                            tool.name, ex)
            return {"status": "failed",
                    "error": f"tape refused effect: {ex}"}

        # Stash the effect key on the tool_context.state for after_tool /
        # on_tool_error to pick up. ADK's tool_context exposes a state
        # dict scoped to the current turn — we use a `temp:` prefix so
        # it doesn't get persisted into long-term session state.
        tool_context.state[f"temp:tape_effect_key:{tool.name}:{call_index}"] = (
            eff.idempotency_key)
        tool_context.state[f"temp:tape_call_index:{tool.name}"] = call_index

        # Short-circuit on a previously-completed effect (the replay path).
        if eff.status == EffectStatus.CONFIRMED:
            response = eff.response_json
            return response if isinstance(response, dict) else {
                "tape_replay": response}
        if eff.status == EffectStatus.FAILED:
            return {"status": "failed",
                    "error": eff.error_json,
                    "tape_replay": True}
        if eff.status == EffectStatus.UNKNOWN:
            # The reconciler hasn't resolved this yet. The agent shouldn't
            # re-act; return a pending sentinel and let the reconciler
            # close the loop out-of-band.
            return {"status": "unknown",
                    "tape_replay": True,
                    "idempotency_key": eff.idempotency_key}

        # OUTBOX intent — never let the tool run inline. Return a pending
        # response that ADK records.
        if eff.dispatch_mode == EffectDispatchMode.OUTBOX:
            return {"status": "pending",
                    "idempotency_key": eff.idempotency_key,
                    "business_key": business_key,
                    "note": ("the outbox dispatcher will execute this "
                              "call; the result will land via a separate "
                              "function_response event")}

        # PENDING + INLINE — let ADK call the tool body. We complete in
        # after_tool_callback.
        return None

    async def after_tool_callback(
        self, *, tool: BaseTool, tool_args: dict[str, Any],
        tool_context: ToolContext, result: dict,
    ) -> Optional[dict]:
        """Record the tool's result onto the journaled effect."""
        meta = meta_of(tool)
        if meta is None:
            return None
        if meta.dispatch_mode == EffectDispatchMode.OUTBOX:
            # Outbox effects don't have an inline call — `result` here is
            # the pending dict we returned in before_tool_callback. The
            # outbox dispatcher completes the effect.
            return None

        ictx = tool_context._invocation_context  # type: ignore[attr-defined]
        # Find the effect key we stashed.
        # The call_index for this completion matches the one we stashed
        # in before_tool. Look it up.
        call_index_key = f"temp:tape_call_index:{tool.name}"
        call_index = tool_context.state.get(call_index_key)
        if call_index is None:
            logger.warning("tape: no stashed call_index for %s — "
                            "after_tool_callback can't complete the effect",
                            tool.name)
            return None
        effect_key = tool_context.state.get(
            f"temp:tape_effect_key:{tool.name}:{call_index}")
        if effect_key is None:
            return None

        await self.svc.complete_effect(
            app_name=ictx.app_name, user_id=ictx.user_id,
            session_id=ictx.session.id,
            idempotency_key=effect_key,
            status=EffectStatus.CONFIRMED,
            response_json=result,
        )
        # If the tool registered a compensation kind, register it now —
        # one transaction's worth of obligation registration per successful
        # CONFIRMED. Idempotent on (session, effect_key, kind).
        if meta.compensate:
            await self.svc.register_compensation(
                app_name=ictx.app_name, user_id=ictx.user_id,
                session_id=ictx.session.id,
                invocation_id=ictx.invocation_id,
                effect_key=effect_key, kind=meta.compensate,
                payload_json={"args": tool_args, "result": result})
        return None

    async def on_tool_error_callback(
        self, *, tool: BaseTool, tool_args: dict[str, Any],
        tool_context: ToolContext, error: Exception,
    ) -> Optional[dict]:
        """Map the exception to a terminal effect status. AckLost → UNKNOWN
        (reconciler resolves). Everything else → FAILED."""
        meta = meta_of(tool)
        if meta is None:
            return None
        if meta.dispatch_mode == EffectDispatchMode.OUTBOX:
            return None

        ictx = tool_context._invocation_context  # type: ignore[attr-defined]
        call_index_key = f"temp:tape_call_index:{tool.name}"
        call_index = tool_context.state.get(call_index_key)
        if call_index is None:
            return None
        effect_key = tool_context.state.get(
            f"temp:tape_effect_key:{tool.name}:{call_index}")
        if effect_key is None:
            return None

        if isinstance(error, AckLost):
            await self.svc.complete_effect(
                app_name=ictx.app_name, user_id=ictx.user_id,
                session_id=ictx.session.id,
                idempotency_key=effect_key,
                status=EffectStatus.UNKNOWN,
                error_json={"type": "AckLost", "message": str(error)},
            )
        else:
            await self.svc.complete_effect(
                app_name=ictx.app_name, user_id=ictx.user_id,
                session_id=ictx.session.id,
                idempotency_key=effect_key,
                status=EffectStatus.FAILED,
                error_json={"type": type(error).__name__,
                             "message": str(error)},
            )
        return None


__all__ = ["NonIdempotentSafetyPlugin", "AckLost"]
