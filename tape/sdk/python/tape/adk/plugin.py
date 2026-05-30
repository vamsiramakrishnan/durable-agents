"""TapePlugin — the ADK `BasePlugin` that turns the runner's callbacks into a
journal.

  before_run        -> BeginRun (+ SetBudget)            ; per-invocation counters reset
  before_model      -> AdmitBudget; GetDecision  (replay a recorded LlmResponse)
  after_model       -> RecordDecision; ChargeBudget(tokens)
  before_tool       -> AdmitBudget; BeginEffect  (short-circuit a CONFIRMED effect)
  after_tool        -> CompleteEffect(CONFIRMED); RegisterCompensation
  on_tool_error     -> CompleteEffect(FAILED | UNKNOWN)
  after_run         -> EndRun(TERMINAL)

Position is by call order: the k-th model call gets decision_index k-1; a tool
call is keyed to the most-recent decision plus a per-(decision, tool) call index.
That alignment holds as long as the agent re-drives the same way — which it does,
because every decision is replayed (see design-principles/tape.md §6.5, and the
determinism caveat there).
"""

from __future__ import annotations

import json
import os
import socket
from typing import Any, Optional

import grpc
from google.adk.plugins.base_plugin import BasePlugin

from ..client import TapeClient, DEFAULT_URL
from .._gen import tape_pb2 as pb
from ..budget import Budget, budget_from_run_config
from ..effect import (effect_meta_of, tool_name_of, register_compensator,
                      _semantics_to_pb, _dispatch_to_pb, _resolve_business_key,
                      ScopeDenied)
from ..gates import AckLost
from .identity import RunIdentity

def _default_lease_ms() -> int:
    try:
        return int(os.environ.get("TAPE_LEASE_MS", "120000"))
    except ValueError:
        return 120_000


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return json.dumps(str(obj))


def _refusal_response(reason: str):
    """An LlmResponse with no function calls — the agent loop will treat it as a
    final answer and stop."""
    from google.adk.models.llm_response import LlmResponse
    from google.genai import types

    return LlmResponse(content=types.Content(
        role="model", parts=[types.Part(text=f"[tape] stopping: {reason}")]))


class TapePlugin(BasePlugin):
    def __init__(self, url: str = DEFAULT_URL, *, client: Optional[TapeClient] = None,
                 budget: Optional[Budget] = None, lease_owner: Optional[str] = None,
                 lease_ttl_ms: Optional[int] = None,
                 check_cancellation: bool = False, cancel_check_interval_s: float = 5.0,
                 identity: Optional[RunIdentity] = None,
                 name: str = "tape"):
        super().__init__(name=name)
        self._client = client or TapeClient(url)
        self._budget = budget
        self._owner = lease_owner or f"{socket.gethostname()}:{os.getpid()}"
        self._lease_ttl_ms = lease_ttl_ms if lease_ttl_ms is not None else _default_lease_ms()
        self._check_cancel = check_cancellation
        self._cancel_check_interval_s = cancel_check_interval_s
        # Identity is attached to every BeginRun this plugin issues. Defaults
        # to an empty RunIdentity (works for local dev / non-AIPlex callers);
        # AIPlex-deployed agents pass `RunIdentity.from_env()`.
        self._identity = identity if identity is not None else RunIdentity()
        self._last_cancel_check: dict[str, float] = {}    # invocation_id -> ts
        # per-invocation bookkeeping
        self._run: dict[str, str] = {}            # invocation_id -> run_id
        self._dnext: dict[str, int] = {}          # invocation_id -> next decision index
        self._dlast: dict[str, int] = {}          # invocation_id -> most recent decision index
        self._tcount: dict[tuple, int] = {}       # (invocation_id, decision_idx, tool) -> calls so far
        self._has_budget: set[str] = set()        # run_ids with a cap set
        # Authorization grant set per run (AIPlex integration PR 2). Populated
        # at before_run_callback time from the run's RunState.scopes — the
        # server is the source of truth, so re-drives in a new process pick
        # up the same grants.
        self._run_scopes: dict[str, frozenset[str]] = {}  # run_id -> {scope, ...}

    # ── helpers ─────────────────────────────────────────────────────────────

    def _run_id(self, ctx) -> Optional[str]:
        return self._run.get(getattr(ctx, "invocation_id", None))

    def _resolve_budget(self, invocation_context) -> Optional[Budget]:
        rc = getattr(invocation_context, "run_config", None)
        b = budget_from_run_config(rc)
        return b if b is not None else self._budget

    # ── run lifecycle ───────────────────────────────────────────────────────

    async def before_run_callback(self, *, invocation_context):
        s = invocation_context.session
        inv = invocation_context.invocation_id
        resp = self._client.begin_run(
            app_name=s.app_name, user_id=s.user_id, session_id=s.id, invocation_id=inv,
            lease_owner=self._owner, lease_ttl_ms=self._lease_ttl_ms,
            tenant_id=self._identity.tenant_id,
            actor=self._identity.actor,
            subject=self._identity.subject,
            agent_id=self._identity.agent_id,
            aiplex_instance_id=self._identity.aiplex_instance_id,
            gateway_route=self._identity.gateway_route,
            scopes=self._identity.scopes,
            labels=self._identity.labels)
        self._run[inv] = resp.run_id
        # On a re-drive, the next model call is NOT decision 0 — it's whatever
        # comes after the decisions already recorded. Count them so before_model
        # doesn't spuriously replay early decisions (which would re-fire tools).
        self._dnext[inv] = self._count_decisions(resp.run_id) if resp.resumed else 0
        # Cache the run's authorization grants for the scoped-effect pre-check
        # (see before_tool_callback). For a fresh run we already know the
        # grants locally; for a re-drive in a new process we ask the server.
        if resp.resumed:
            try:
                rs = self._client.get_run(resp.run_id)
                self._run_scopes[resp.run_id] = frozenset(rs.scopes)
            except Exception:
                # If we can't fetch the grants, fall back to whatever we were
                # told locally — the server still re-checks at BeginEffect.
                self._run_scopes[resp.run_id] = frozenset(self._identity.scopes)
        else:
            self._run_scopes[resp.run_id] = frozenset(self._identity.scopes)
        b = self._resolve_budget(invocation_context)
        if b is not None and (b.usd_cap > 0 or b.token_cap > 0):
            self._client.set_budget(run_id=resp.run_id, usd_cap=b.usd_cap, token_cap=b.token_cap)
            self._has_budget.add(resp.run_id)
        return None

    async def after_run_callback(self, *, invocation_context):
        inv = invocation_context.invocation_id
        run_id = self._run.get(inv)
        if run_id:
            try:
                cur = self._client.get_run(run_id)
                # Don't clobber a WAITING/FAILED/STUCK/COMPENSATING run.
                if cur.status in (pb.RUN_STATUS_RUNNING, pb.RUN_STATUS_RUNNABLE):
                    self._client.end_run(run_id=run_id, status=pb.RUN_STATUS_TERMINAL)
            except Exception:
                pass
        if run_id:
            self._run_scopes.pop(run_id, None)
        self._run.pop(inv, None)
        self._dnext.pop(inv, None)
        self._dlast.pop(inv, None)
        for k in [k for k in list(self._tcount) if k[0] == inv]:
            self._tcount.pop(k, None)

    # ── decisions ───────────────────────────────────────────────────────────

    async def before_model_callback(self, *, callback_context, llm_request):
        inv = getattr(callback_context, "invocation_id", None)
        run_id = self._run.get(inv)
        if not run_id:
            return None
        idx = self._dnext.get(inv, 0)
        self._dnext[inv] = idx + 1
        self._dlast[inv] = idx
        if self._check_cancel and self._should_check_cancel(inv):
            try:
                if self._client.get_run(run_id).status == pb.RUN_STATUS_CANCELLED:
                    return _refusal_response("run cancelled")
            except Exception:
                pass
        if run_id in self._has_budget:
            adm = self._client.admit_budget(run_id=run_id)
            if not adm.admitted:
                return _refusal_response(adm.reason or "budget exhausted")
        try:
            got = self._client.get_decision(run_id=run_id, decision_index=idx)
        except Exception:
            got = None
        if got is not None and got.found:
            from google.adk.models.llm_response import LlmResponse
            try:
                return LlmResponse.model_validate_json(got.decision.response_json)
            except Exception:
                # Couldn't deserialize a recorded decision — fall through to a
                # fresh call rather than crash. (Logged below.)
                pass
        return None

    async def after_model_callback(self, *, callback_context, llm_response):
        inv = getattr(callback_context, "invocation_id", None)
        run_id = self._run.get(inv)
        if not run_id:
            return None
        idx = self._dlast.get(inv)
        if idx is None:
            return None
        policy_version = ""
        try:
            policy_version = str(callback_context.state.get("policy_version", "") or "")
        except Exception:
            pass
        try:
            self._client.record_decision(
                run_id=run_id, decision_index=idx,
                model=str(getattr(llm_response, "model_version", "") or ""),
                response_json=llm_response.model_dump_json(exclude_none=True),
                policy_version=policy_version)
        except Exception:
            pass
        if run_id in self._has_budget:
            usage = getattr(llm_response, "usage_metadata", None)
            tok = int(getattr(usage, "total_token_count", 0) or 0) if usage else 0
            if tok:
                try:
                    self._client.charge_budget(run_id=run_id, tokens=tok)
                except Exception:
                    pass
        return None

    # ── tools / effects ─────────────────────────────────────────────────────

    async def before_tool_callback(self, *, tool, tool_args, tool_context):
        inv = getattr(tool_context, "invocation_id", None)
        run_id = self._run.get(inv)
        if not run_id:
            return None
        dec_idx = self._dlast.get(inv, -1)
        tname = tool_name_of(tool)
        tkey = (inv, dec_idx, tname)
        call_idx = self._tcount.get(tkey, 0)
        self._tcount[tkey] = call_idx + 1

        if run_id in self._has_budget:
            adm = self._client.admit_budget(run_id=run_id)
            if not adm.admitted:
                return {"error": f"tape: budget exhausted — {adm.reason}"}

        meta = effect_meta_of(tool) or {}
        # The idempotency key. Default = run/fc-<function_call_id>: ADK's id for
        # the function call the model emitted, which is part of the persisted
        # event — so when ADK re-executes that pending call on a re-drive, the
        # key matches and the counterparty dedups. It names the decision's
        # output, not a hash of the inputs (treatise §IX ①). If the id is
        # missing, the server falls back to run/decision-<i>/<tool>/<call_index>.
        # `key_from=` overrides both.
        custom_key = ""
        fcid = getattr(tool_context, "function_call_id", None) or ""
        if fcid:
            custom_key = f"{run_id}/fc-{fcid}"
        if meta.get("key_from"):
            try:
                k = str(meta["key_from"](tool_args, tool_context) or "")
                if k:
                    custom_key = k
            except Exception:
                pass
        # Outbox contract: declare the effect's semantics + dispatch mode + the
        # business-level dedupe key. The server enforces NON_IDEMPOTENT+OUTBOX
        # and uniqueness on (connector, business_key) — see proto.
        semantics_str = meta.get("semantics") or "idempotent"
        dispatch_str = meta.get("dispatch") or "inline"
        connector = str(meta.get("connector") or "")
        bk = _resolve_business_key(meta.get("business_key"), tool_args, tool_context)
        # Authorization (AIPlex integration PR 2). Pre-check the effect's
        # declared scope against the run's granted scopes BEFORE issuing
        # begin_effect — the tool body never runs on a denial. The server
        # re-checks at BeginEffect so an outdated SDK can't bypass authz.
        scope = str(meta.get("scope") or "")
        if scope:
            granted = self._run_scopes.get(run_id, frozenset())
            if scope not in granted:
                # Surface as a typed error so the agent loop can handle it
                # uniformly. Return shape mirrors the budget-exhaustion path
                # above (a dict the model sees as the tool result).
                exc = ScopeDenied(scope=scope, tool=tname, granted=sorted(granted))
                return {"error": str(exc), "scope_denied": True,
                        "required_scope": scope, "tool": tname}
        try:
            resp = self._client.begin_effect(
                run_id=run_id, decision_index=dec_idx, tool_name=tname, call_index=call_idx,
                request_json=_safe_json(tool_args), custom_key=custom_key,
                semantics=_semantics_to_pb(semantics_str),
                dispatch_mode=_dispatch_to_pb(dispatch_str),
                business_key=bk, connector=connector, scope=scope)
        except grpc.RpcError as ex:
            # Server-side scope denial — typed PermissionDenied. Treat the
            # same way as the SDK pre-check so the tool body never runs.
            if getattr(ex, "code", lambda: None)() == grpc.StatusCode.PERMISSION_DENIED:
                return {"error": f"tape: begin_effect denied: {ex.details()}",
                        "scope_denied": True, "required_scope": scope, "tool": tname}
            # Other transport errors: keep the existing split.
            if semantics_str == "non_idempotent" or dispatch_str == "outbox":
                return {"error": f"tape: begin_effect refused: {ex}"}
            return None
        except Exception as ex:
            # Split the failure handling by contract so the new strictness
            # doesn't reduce the v1 behaviour:
            #  * For idempotent + inline (the v1 default), a transient gRPC
            #    blip should let the body run — the counterparty's idempotency
            #    key handling is what guarantees safety on retry; failing the
            #    tool just because Tape is briefly unreachable is a regression
            #    versus the prior behaviour, which returned None here.
            #  * For non-idempotent or outbox, the body MUST NOT run without a
            #    journaled intent (that's the safety claim of the whole
            #    Phase 1+2 work), so surface the error to the agent.
            if semantics_str == "non_idempotent" or dispatch_str == "outbox":
                return {"error": f"tape: begin_effect refused: {ex}"}
            return None
        try:
            tool_context.state["temp:_tape_idempotency_key"] = resp.idempotency_key
            tool_context.state["temp:_tape_run_id"] = run_id
            tool_context.state["temp:_tape_business_key"] = bk
            tool_context.state["temp:_tape_effect_semantics"] = semantics_str
        except Exception:
            pass
        if resp.status == pb.EFFECT_STATUS_CONFIRMED:
            try:
                return json.loads(resp.response_json) if resp.response_json else {}
            except Exception:
                return {}
        # OUTBOX dispatch: the tool body must NOT perform external IO. Record
        # intent (which BeginEffect just did) and return a synthetic accepted
        # marker; the outbox reactor will run the connector exactly once.
        if dispatch_str == "outbox" and resp.status == pb.EFFECT_STATUS_PENDING:
            try:
                external_ref = ""
                # On re-drive after a connector has already observed the call,
                # external_ref may be on the stored row — make it accessible.
                eff = self._client.get_effect(run_id=run_id, idempotency_key=resp.idempotency_key)
                if eff.found and eff.effect.external_ref:
                    external_ref = eff.effect.external_ref
                    tool_context.state["temp:_tape_external_ref"] = external_ref
            except Exception:
                pass
            return {
                "tape_status": "accepted",
                "effect_key": resp.idempotency_key,
                "dispatch_mode": "outbox",
                "connector": connector,
                "business_key": bk,
            }
        # INLINE PENDING (fresh, or a prior crash) / FAILED / UNKNOWN -> let the body run.
        return None

    async def after_tool_callback(self, *, tool, tool_args, tool_context, result):
        run_id, key = self._tool_keys(tool_context)
        if not run_id or not key:
            return None
        meta = effect_meta_of(tool) or {}
        # Outbox: the synthetic "accepted" marker we returned in before_tool
        # surfaces here. The body did NOT run; the effect is still PENDING and
        # belongs to the outbox reactor + reconciler. Do NOT mark CONFIRMED, do
        # NOT register the compensation eagerly (that happens when the reactor
        # observes a real confirmed dispatch, or when the reconciler maps a
        # DUPLICATE observation to compensation). Just thread through.
        is_outbox_accepted = (
            (meta.get("dispatch") == "outbox")
            and isinstance(result, dict)
            and result.get("tape_status") == "accepted"
        )
        if is_outbox_accepted:
            return None
        try:
            self._client.complete_effect(run_id=run_id, idempotency_key=key,
                                         status=pb.EFFECT_STATUS_CONFIRMED, response_json=_safe_json(result))
        except Exception:
            pass
        comp = meta.get("compensate")
        if comp is not None:
            kind = getattr(comp, "__name__", "compensate")
            register_compensator(kind, comp)
            if meta.get("compensation_payload"):
                try:
                    payload = meta["compensation_payload"](tool_args, result) or {}
                except Exception:
                    payload = {}
            else:
                payload = {**(tool_args or {}), **(result if isinstance(result, dict) else {})}
            try:
                self._client.register_compensation(
                    run_id=run_id, effect_key=key, kind=kind,
                    payload_json=_safe_json(payload),
                    compensator_ref=str(meta.get("compensator_ref") or ""),
                    max_attempts=int(meta.get("max_attempts") or 0))
            except Exception:
                pass
        return None

    async def on_tool_error_callback(self, *, tool, tool_args, tool_context, error):
        run_id, key = self._tool_keys(tool_context)
        if not run_id or not key:
            return None
        status = pb.EFFECT_STATUS_UNKNOWN if isinstance(error, AckLost) else pb.EFFECT_STATUS_FAILED
        try:
            self._client.complete_effect(run_id=run_id, idempotency_key=key, status=status,
                                         error_json=json.dumps({"type": type(error).__name__, "message": str(error)}))
        except Exception:
            pass
        return None  # don't swallow the error

    # ── internal ────────────────────────────────────────────────────────────

    def _should_check_cancel(self, inv: Optional[str]) -> bool:
        if inv is None:
            return False
        import time as _t
        now = _t.time()
        last = self._last_cancel_check.get(inv, 0.0)
        if now - last < self._cancel_check_interval_s:
            return False
        self._last_cancel_check[inv] = now
        return True

    def _count_decisions(self, run_id: str, hard_cap: int = 10_000) -> int:
        n = 0
        while n < hard_cap:
            try:
                got = self._client.get_decision(run_id=run_id, decision_index=n)
            except Exception:
                break
            if not got.found:
                break
            n += 1
        return n

    def _tool_keys(self, tool_context) -> tuple[Optional[str], Optional[str]]:
        run_id = None
        key = None
        try:
            run_id = tool_context.state.get("temp:_tape_run_id")
            key = tool_context.state.get("temp:_tape_idempotency_key")
        except Exception:
            pass
        if not run_id:
            run_id = self._run.get(getattr(tool_context, "invocation_id", None))
        return run_id, key
