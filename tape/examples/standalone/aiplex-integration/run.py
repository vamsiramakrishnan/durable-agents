"""Drive the AIPlex integration story against a running tape-server.

Two ways to use this file:

  1.  As a runnable script — start tape-server locally, export the
      AIPLEX_* env vars (or take the defaults below), and run
      `python run.py`. The script prints what it's doing at each step so
      the integration points are visible: identity threaded onto the run,
      scoped effect admitted, denied effect rejected with the policy
      journal entry.

  2.  As a reference for the AIPlex controller — every block below maps
      to something AIPlex does at deploy time (set env vars), at run time
      (start the agent process), or at observation time (read the
      journal). See tape/docs/integrations/aiplex.md for the prose.

The script does NOT need an LLM API key; it drives Tape's gRPC surface
directly. To see the same flow under a real ADK runner, point
`app.agent:root_agent` at gemini-2.5-flash and invoke via
`adk run --app app.agent:root_agent ...`.
"""

from __future__ import annotations

import os
import sys
import uuid

import grpc

import tape
from tape import client as client_mod
from tape.adk.identity import RunIdentity


def main() -> int:
    # ── Step 1: identity ─────────────────────────────────────────────────
    # In an AIPlex deployment the controller has already exported these
    # before our process started; we just call `from_env()`. For the
    # standalone demo, fall back to plausible defaults so a clone-and-run
    # still produces a meaningful trace.
    os.environ.setdefault("AIPLEX_TENANT_ID", "acme")
    os.environ.setdefault("AIPLEX_ACTOR",
                          "spiffe://aiplex/ns/treasury/sa/agent")
    os.environ.setdefault("AIPLEX_SUBJECT", "vamsi@example.com")
    os.environ.setdefault("AIPLEX_AGENT_ID", "aiplex-treasury")
    os.environ.setdefault("AIPLEX_INSTANCE_ID", f"inst-{uuid.uuid4().hex[:6]}")
    os.environ.setdefault("AIPLEX_ROUTE", "/a2a/treasury")
    # Only the read scope is granted. The bank_wire effect declares
    # scope="mcp:tools:bank_wire" — it will be denied. Add it to AIPLEX_SCOPES
    # to flip to the happy path.
    os.environ.setdefault("AIPLEX_SCOPES", "mcp:tools:read_balance")
    os.environ.setdefault("AIPLEX_LABELS",
                          "aiplex.plane=a2a,aiplex.policy=treasury-2026.05")

    identity = RunIdentity.from_env()

    print("=== AIPlex identity ===")
    print(f"  tenant_id          : {identity.tenant_id}")
    print(f"  actor              : {identity.actor}")
    print(f"  subject            : {identity.subject}")
    print(f"  agent_id           : {identity.agent_id}")
    print(f"  aiplex_instance_id : {identity.aiplex_instance_id}")
    print(f"  gateway_route      : {identity.gateway_route}")
    print(f"  scopes             : {identity.scopes}")
    print(f"  labels             : {identity.labels}")
    print()

    url = os.environ.get("TAPE_URL", "tape://localhost:7878")
    print(f"=== Tape server: {url} ===\n")

    with client_mod.TapeClient(url) as c:
        # ── Step 2: begin the run with full identity ─────────────────────
        # In a real ADK app, durable_app(identity=...) does this for you.
        # Here we drive the wire directly so the integration is explicit.
        invocation_id = f"inv-{uuid.uuid4().hex[:8]}"
        run = c.begin_run(
            app_name="aiplex_treasury",
            user_id=identity.subject,
            session_id=f"sess-{uuid.uuid4().hex[:8]}",
            invocation_id=invocation_id,
            lease_owner=f"demo-pid-{os.getpid()}",
            lease_ttl_ms=60_000,
            tenant_id=identity.tenant_id,
            actor=identity.actor,
            subject=identity.subject,
            agent_id=identity.agent_id,
            aiplex_instance_id=identity.aiplex_instance_id,
            gateway_route=identity.gateway_route,
            scopes=identity.scopes,
            labels=identity.labels,
        )
        run_id = run.run_id
        print(f"=== BeginRun -> run_id={run_id} ===\n")

        # Decision 0 anchors the effect's idempotency key.
        c.record_decision(
            run_id=run_id, decision_index=0,
            model="scripted-treasury-policy",
            request_json="{}",
            response_json='{"plan":["read_balance","bank_wire"]}',
            rationale="treasury policy fired",
            policy_version="aiplex-treasury-2026.05",
        )

        # ── Step 3: the scoped effect that IS granted ────────────────────
        print("=== Effect 1: read_balance "
              "(scope=mcp:tools:read_balance, GRANTED) ===")
        e1 = c.begin_effect(
            run_id=run_id, decision_index=0,
            tool_name="read_balance", call_index=0,
            request_json='{"account_id":"acct-42"}',
            scope="mcp:tools:read_balance",
        )
        print(f"  status={_status_name(e1.status)} key={e1.idempotency_key}\n")
        c.complete_effect(
            run_id=run_id, idempotency_key=e1.idempotency_key,
            status=client_mod.EFFECT_STATUS_CONFIRMED,
            response_json='{"balance_minor":100000000,"currency":"USD"}',
        )

        # ── Step 4: the scoped effect that is NOT granted ────────────────
        # bank_wire requires "mcp:tools:bank_wire"; AIPLEX_SCOPES grants only
        # "mcp:tools:read_balance". The server denies with PermissionDenied
        # and journals a `kind="policy"` violation entry.
        print("=== Effect 2: bank_wire "
              "(scope=mcp:tools:bank_wire, NOT GRANTED) ===")
        try:
            c.begin_effect(
                run_id=run_id, decision_index=0,
                tool_name="bank_wire", call_index=0,
                request_json='{"account_id":"acct-42","amount_minor":50000000,'
                             '"target":"FZDXX","business_key":"sweep-2026-05-23-acct42"}',
                semantics=client_mod.EFFECT_SEMANTICS_NON_IDEMPOTENT,
                dispatch_mode=client_mod.EFFECT_DISPATCH_MODE_OUTBOX,
                connector="bank.wire",
                business_key="sweep-2026-05-23-acct42",
                scope="mcp:tools:bank_wire",
            )
            print("  UNEXPECTED: server admitted the unscoped effect")
            return 2
        except grpc.RpcError as ex:
            assert ex.code() == grpc.StatusCode.PERMISSION_DENIED, ex
            print(f"  DENIED with PermissionDenied: {ex.details()}\n")

        # ── Step 5: read the journal back ────────────────────────────────
        print("=== Run journal ===")
        from tape._gen import tape_pb2 as pb
        from tape._gen import tape_pb2_grpc as pb_grpc
        from tape.client import _target as _target_url

        ch = grpc.insecure_channel(_target_url(url))
        stub = pb_grpc.TapeStub(ch)
        try:
            it = stub.SubscribeRun(
                pb.SubscribeRunRequest(run_id=run_id, from_seq=0),
                timeout=2.0)
            for entry in it:
                marker = "!" if entry.kind == "policy" else " "
                print(f"  {marker} seq={entry.seq:>2} kind={entry.kind:<10} "
                      f"{entry.payload_json[:100]}")
        except grpc.RpcError:
            pass
        finally:
            ch.close()

        c.end_run(run_id=run_id, status=client_mod.RUN_STATUS_TERMINAL)
        print(f"\n=== EndRun({run_id}) ===")
        print("\nThe `policy` entry above is what AIPlex's audit ingestion "
              "would surface as a scope-denied attempt in the run timeline.")
    return 0


def _status_name(status: int) -> str:
    return {
        client_mod.EFFECT_STATUS_PENDING: "PENDING",
        client_mod.EFFECT_STATUS_CONFIRMED: "CONFIRMED",
        client_mod.EFFECT_STATUS_FAILED: "FAILED",
        client_mod.EFFECT_STATUS_UNKNOWN: "UNKNOWN",
    }.get(status, f"unknown({status})")


if __name__ == "__main__":
    sys.exit(main())
