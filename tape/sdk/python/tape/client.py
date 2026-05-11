"""TapeClient — a thin, synchronous client over the `tape.v1` gRPC service.

This is the layer the ADK adapter (and your own code, when you need it) talks
to. Every mutating call is idempotent on the server, so retrying is always safe.
"""

from __future__ import annotations

import os
from typing import Optional

import grpc

from ._gen import tape_pb2 as pb
from ._gen import tape_pb2_grpc as pb_grpc

# Re-export the enums at module level for ergonomic use.
RUN_STATUS_UNSPECIFIED = pb.RUN_STATUS_UNSPECIFIED
RUN_STATUS_RUNNABLE = pb.RUN_STATUS_RUNNABLE
RUN_STATUS_RUNNING = pb.RUN_STATUS_RUNNING
RUN_STATUS_WAITING = pb.RUN_STATUS_WAITING
RUN_STATUS_TERMINAL = pb.RUN_STATUS_TERMINAL
RUN_STATUS_FAILED = pb.RUN_STATUS_FAILED
RUN_STATUS_COMPENSATING = pb.RUN_STATUS_COMPENSATING
RUN_STATUS_STUCK = pb.RUN_STATUS_STUCK

EFFECT_STATUS_PENDING = pb.EFFECT_STATUS_PENDING
EFFECT_STATUS_CONFIRMED = pb.EFFECT_STATUS_CONFIRMED
EFFECT_STATUS_FAILED = pb.EFFECT_STATUS_FAILED
EFFECT_STATUS_UNKNOWN = pb.EFFECT_STATUS_UNKNOWN

OBLIGATION_STATUS_COMMITTED = pb.OBLIGATION_STATUS_COMMITTED
OBLIGATION_STATUS_COMPENSATED = pb.OBLIGATION_STATUS_COMPENSATED
OBLIGATION_STATUS_STUCK = pb.OBLIGATION_STATUS_STUCK

DEFAULT_URL = os.environ.get("TAPE_URL", "tape://localhost:7878")


def _target(url: str) -> str:
    if url.startswith("tape://"):
        return url[len("tape://") :]
    if url.startswith("grpc://"):
        return url[len("grpc://") :]
    return url


class TapeClient:
    def __init__(self, url: str = DEFAULT_URL):
        self.url = url
        self.channel = grpc.insecure_channel(_target(url))
        self.stub = pb_grpc.TapeStub(self.channel)

    def close(self) -> None:
        self.channel.close()

    def __enter__(self) -> "TapeClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ── run lifecycle ───────────────────────────────────────────────────────

    def begin_run(self, *, app_name, user_id, session_id, invocation_id,
                  lease_owner="", lease_ttl_ms=120_000):
        return self.stub.BeginRun(pb.BeginRunRequest(
            app_name=app_name, user_id=user_id, session_id=session_id,
            invocation_id=invocation_id, lease_owner=lease_owner, lease_ttl_ms=lease_ttl_ms))

    def resume_run(self, *, run_id, lease_owner="", lease_ttl_ms=120_000):
        return self.stub.ResumeRun(pb.ResumeRunRequest(
            run_id=run_id, lease_owner=lease_owner, lease_ttl_ms=lease_ttl_ms))

    def end_run(self, *, run_id, status=RUN_STATUS_TERMINAL, detail_json=""):
        return self.stub.EndRun(pb.EndRunRequest(run_id=run_id, status=status, detail_json=detail_json))

    def get_run(self, run_id):
        return self.stub.GetRun(pb.GetRunRequest(run_id=run_id))

    def list_runs_to_recover(self, *, limit=100, now_ms=0):
        return self.stub.ListRunsToRecover(pb.ListRunsToRecoverRequest(limit=limit, now_ms=now_ms))

    def subscribe_run(self, *, run_id, from_seq=0):
        return self.stub.SubscribeRun(pb.SubscribeRunRequest(run_id=run_id, from_seq=from_seq))

    # ── decision ledger ─────────────────────────────────────────────────────

    def record_decision(self, *, run_id, decision_index, model="", request_json="",
                        response_json="", rationale="", policy_version=""):
        return self.stub.RecordDecision(pb.RecordDecisionRequest(
            run_id=run_id, decision_index=decision_index, model=model, request_json=request_json,
            response_json=response_json, rationale=rationale, policy_version=policy_version))

    def get_decision(self, *, run_id, decision_index):
        return self.stub.GetDecision(pb.GetDecisionRequest(run_id=run_id, decision_index=decision_index))

    # ── effect ledger ───────────────────────────────────────────────────────

    def begin_effect(self, *, run_id, decision_index, tool_name, call_index=0,
                     request_json="", custom_key=""):
        return self.stub.BeginEffect(pb.BeginEffectRequest(
            run_id=run_id, decision_index=decision_index, tool_name=tool_name,
            call_index=call_index, request_json=request_json, custom_key=custom_key))

    def complete_effect(self, *, run_id, idempotency_key, status, response_json="", error_json=""):
        return self.stub.CompleteEffect(pb.CompleteEffectRequest(
            run_id=run_id, idempotency_key=idempotency_key, status=status,
            response_json=response_json, error_json=error_json))

    def get_effect(self, *, run_id, idempotency_key):
        return self.stub.GetEffect(pb.GetEffectRequest(run_id=run_id, idempotency_key=idempotency_key))

    def reconcile_effect(self, *, run_id, idempotency_key, resolved_status, response_json="", error_json=""):
        return self.stub.ReconcileEffect(pb.ReconcileEffectRequest(
            run_id=run_id, idempotency_key=idempotency_key, resolved_status=resolved_status,
            response_json=response_json, error_json=error_json))

    # ── obligations / compensation ──────────────────────────────────────────

    def register_compensation(self, *, run_id, effect_key, kind, payload_json=""):
        return self.stub.RegisterCompensation(pb.RegisterCompensationRequest(
            run_id=run_id, effect_key=effect_key, kind=kind, payload_json=payload_json))

    def list_obligations(self, *, run_id, only_unresolved=True):
        return self.stub.ListObligations(pb.ListObligationsRequest(run_id=run_id, only_unresolved=only_unresolved))

    def resolve_obligation(self, *, run_id, obligation_seq, status, result_json=""):
        return self.stub.ResolveObligation(pb.ResolveObligationRequest(
            run_id=run_id, obligation_seq=obligation_seq, status=status, result_json=result_json))

    # ── budget ──────────────────────────────────────────────────────────────

    def set_budget(self, *, run_id, usd_cap=0.0, token_cap=0):
        return self.stub.SetBudget(pb.SetBudgetRequest(run_id=run_id, usd_cap=usd_cap, token_cap=token_cap))

    def admit_budget(self, *, run_id, usd_estimate=0.0, token_estimate=0):
        return self.stub.AdmitBudget(pb.AdmitBudgetRequest(
            run_id=run_id, usd_estimate=usd_estimate, token_estimate=token_estimate))

    def charge_budget(self, *, run_id, usd=0.0, tokens=0):
        return self.stub.ChargeBudget(pb.ChargeBudgetRequest(run_id=run_id, usd=usd, tokens=tokens))

    # ── gates / signals ─────────────────────────────────────────────────────

    def await_signal(self, *, run_id, gate_name, payload_json=""):
        return self.stub.AwaitSignal(pb.AwaitSignalRequest(
            run_id=run_id, gate_name=gate_name, payload_json=payload_json))

    def send_signal(self, *, run_id="", app_name="", user_id="", session_id="", gate_name, resolution_json=""):
        return self.stub.SendSignal(pb.SendSignalRequest(
            run_id=run_id, app_name=app_name, user_id=user_id, session_id=session_id,
            gate_name=gate_name, resolution_json=resolution_json))

    # ── ADK SessionService shim ─────────────────────────────────────────────

    def create_session(self, *, app_name, user_id, session_id="", state_json="{}"):
        return self.stub.CreateSession(pb.CreateSessionRequest(
            app_name=app_name, user_id=user_id, session_id=session_id, state_json=state_json))

    def get_session(self, *, app_name, user_id, session_id, max_events=0):
        return self.stub.GetSession(pb.GetSessionRequest(
            app_name=app_name, user_id=user_id, session_id=session_id, max_events=max_events))

    def list_sessions(self, *, app_name, user_id=""):
        return self.stub.ListSessions(pb.ListSessionsRequest(app_name=app_name, user_id=user_id))

    def delete_session(self, *, app_name, user_id, session_id):
        return self.stub.DeleteSession(pb.DeleteSessionRequest(
            app_name=app_name, user_id=user_id, session_id=session_id))

    def append_event(self, *, app_name, user_id, session_id, event, state_delta_json="{}"):
        return self.stub.AppendEvent(pb.AppendEventRequest(
            app_name=app_name, user_id=user_id, session_id=session_id,
            event=event, state_delta_json=state_delta_json))
