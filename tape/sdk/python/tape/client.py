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
RUN_STATUS_CANCELLED = pb.RUN_STATUS_CANCELLED

EFFECT_STATUS_PENDING = pb.EFFECT_STATUS_PENDING
EFFECT_STATUS_CONFIRMED = pb.EFFECT_STATUS_CONFIRMED
EFFECT_STATUS_FAILED = pb.EFFECT_STATUS_FAILED
EFFECT_STATUS_UNKNOWN = pb.EFFECT_STATUS_UNKNOWN

OBLIGATION_STATUS_COMMITTED = pb.OBLIGATION_STATUS_COMMITTED
OBLIGATION_STATUS_COMPENSATED = pb.OBLIGATION_STATUS_COMPENSATED
OBLIGATION_STATUS_STUCK = pb.OBLIGATION_STATUS_STUCK

DEFAULT_URL = os.environ.get("TAPE_URL", "tape://localhost:7878")


def _target(url: str) -> str:
    """The host:port for a gRPC channel. `tapes://h` -> `h:443` (TLS), `tape://h`
    / `grpc://h` / bare `h:p` -> as-is (plaintext)."""
    if url.startswith("tapes://"):
        h = url[len("tapes://"):]
        return h if ":" in h else f"{h}:443"
    if url.startswith("tape://"):
        return url[len("tape://"):]
    if url.startswith("grpc://"):
        return url[len("grpc://"):]
    return url


def _is_tls(url: str) -> bool:
    return url.startswith("tapes://") or url.startswith("grpcs://")


def _audience_for(url: str) -> str:
    """The OIDC audience for a Cloud Run-style IAM-protected endpoint: the full
    https:// service URL (host without port)."""
    host = _target(url).split(":")[0]
    return f"https://{host}"


class _GoogleIdTokenPlugin(grpc.AuthMetadataPlugin):
    """A gRPC call-credentials plugin that attaches `authorization: Bearer <id-token>`.
    Lazily uses Application Default Credentials (Cloud Run / GCE / GKE Workload
    Identity / a service-account key) to mint an ID token for `audience`. If
    google-auth isn't available or the fetch fails, it sends no auth header
    (so TLS-without-IAM still works) and warns once."""

    def __init__(self, audience: str):
        self._aud = audience
        self._token = ""
        self._exp = 0.0
        self._warned = False

    def _fetch(self) -> str:
        import time as _time
        now = _time.time()
        if self._token and now < self._exp - 60:
            return self._token
        try:
            import base64
            import json as _json
            import google.auth.transport.requests as gar
            from google.oauth2 import id_token as _idt

            tok = _idt.fetch_id_token(gar.Request(), self._aud)
            # decode the JWT exp (no signature check — we only need the expiry)
            payload = _json.loads(base64.urlsafe_b64decode(tok.split(".")[1] + "=="))
            self._exp = float(payload.get("exp", now + 1800))
            self._token = tok
        except Exception as ex:  # noqa: BLE001
            if not self._warned:
                self._warned = True
                import warnings
                warnings.warn(f"tape: could not mint a Google ID token for {self._aud}: {ex}; "
                              "proceeding without auth (fine if the endpoint isn't IAM-protected)")
            self._token, self._exp = "", float("inf")  # don't keep retrying every call
        return self._token

    def __call__(self, context, callback):
        tok = self._fetch()
        callback((("authorization", f"Bearer {tok}"),) if tok else (), None)


class TapeClient:
    """Synchronous gRPC client over the `tape.v1` service.

    URL schemes: `tape://host:port` (plaintext — self-hosted / k8s / local),
    `tapes://host` (TLS on :443 — Cloud Run / any HTTPS endpoint). On a TLS
    channel, if the endpoint is IAM-protected (e.g. an internal Cloud Run
    service), Tape attaches a Google ID token automatically — pass `auth=False`
    to disable, or `id_token=<str>` to supply your own, or `audience=<url>` to
    override the derived one."""

    def __init__(self, url: str = DEFAULT_URL, *, auth: bool = True,
                 audience: str = "", id_token: str = ""):
        self.url = url
        target = _target(url)
        if _is_tls(url):
            chan_creds = grpc.ssl_channel_credentials()
            call_creds = None
            if id_token:
                call_creds = grpc.access_token_call_credentials(id_token)
            elif auth:
                aud = audience or os.environ.get("TAPE_AUDIENCE", "") or _audience_for(url)
                call_creds = grpc.metadata_call_credentials(_GoogleIdTokenPlugin(aud))
            creds = grpc.composite_channel_credentials(chan_creds, call_creds) if call_creds else chan_creds
            self.channel = grpc.secure_channel(target, creds)
        else:
            self.channel = grpc.insecure_channel(target)
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

    # ── reconciliation ──────────────────────────────────────────────────────

    def list_pending_effects(self, *, older_than_ms=0, include_pending=True, include_unknown=True, limit=200):
        return self.stub.ListPendingEffects(pb.ListPendingEffectsRequest(
            older_than_ms=older_than_ms, include_pending=include_pending,
            include_unknown=include_unknown, limit=limit))

    # ── timers ──────────────────────────────────────────────────────────────

    def set_timer(self, *, run_id, fire_at_ms, kind, timer_id="", payload_json=""):
        return self.stub.SetTimer(pb.SetTimerRequest(
            run_id=run_id, timer_id=timer_id, fire_at_ms=fire_at_ms, kind=kind, payload_json=payload_json))

    def cancel_timer(self, *, run_id, timer_id):
        return self.stub.CancelTimer(pb.CancelTimerRequest(run_id=run_id, timer_id=timer_id))

    def list_due_timers(self, *, now_ms=0, limit=200, claim=False):
        return self.stub.ListDueTimers(pb.ListDueTimersRequest(now_ms=now_ms, limit=limit, claim=claim))

    # ── the WAL tail ────────────────────────────────────────────────────────

    def subscribe_events(self, *, from_ts_ms=0, run_id="", kind=""):
        return self.stub.SubscribeEvents(pb.SubscribeEventsRequest(from_ts_ms=from_ts_ms, run_id=run_id, kind=kind))

    # ── reactive key-value store (treatise §IX ⑥: coordinate through state) ──

    def write_value(self, *, namespace, key, value_json, if_version=-1, writer=""):
        return self.stub.WriteValue(pb.WriteValueRequest(
            namespace=namespace, key=key, value_json=value_json,
            if_version=if_version, writer=writer))

    def get_value(self, *, namespace, key):
        return self.stub.GetValue(pb.GetValueRequest(namespace=namespace, key=key))

    def watch_value(self, *, namespace, key, from_version=0):
        return self.stub.WatchValue(pb.WatchValueRequest(
            namespace=namespace, key=key, from_version=from_version))

    def delete_value(self, *, namespace, key):
        return self.stub.DeleteValue(pb.DeleteValueRequest(namespace=namespace, key=key))

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
