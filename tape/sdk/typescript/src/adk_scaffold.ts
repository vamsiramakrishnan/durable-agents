// tape-ts — ADK adapter scaffold.
//
// The contract is ../../proto/tape.proto; the working reference is the Python
// adapter in ../../python/tape/adk/. Finishing this is mechanical — the shapes
// below are the whole job.
//
// TODO(tape-ts): generate the client (regen.sh), then implement:

// import { Runner /* from the ADK TS port */ } from "...";
// import { TapeStub } from "./_gen/tape_grpc";

export interface TapeClientLike {
  beginRun(req: { appName: string; userId: string; sessionId: string; invocationId: string; leaseOwner?: string; leaseTtlMs?: number }): Promise<{ runId: string; resumed: boolean; nextSeq: number }>;
  recordDecision(req: { runId: string; decisionIndex: number; responseJson: string; model?: string; policyVersion?: string }): Promise<unknown>;
  getDecision(req: { runId: string; decisionIndex: number }): Promise<{ found: boolean; decision?: { responseJson: string } }>;
  beginEffect(req: { runId: string; decisionIndex: number; toolName: string; callIndex?: number; requestJson?: string; customKey?: string }): Promise<{ idempotencyKey: string; status: number; responseJson: string }>;
  completeEffect(req: { runId: string; idempotencyKey: string; status: number; responseJson?: string; errorJson?: string }): Promise<unknown>;
  registerCompensation(req: { runId: string; effectKey: string; kind: string; payloadJson?: string }): Promise<unknown>;
  endRun(req: { runId: string; status: number; detailJson?: string }): Promise<unknown>;
  // ...plus admitBudget / chargeBudget / awaitSignal / sendSignal / the SessionService shim.
}

// class TapePlugin extends BasePlugin {
//   beforeRunCallback(ctx)   -> beginRun;  reset per-invocation counters
//   beforeModelCallback(ctx) -> getDecision(idx); if found, return the recorded LlmResponse
//   afterModelCallback(ctx)  -> recordDecision(idx, response)
//   beforeToolCallback(ctx)  -> beginEffect; if CONFIRMED, return the recorded result dict
//   afterToolCallback(ctx)   -> completeEffect(CONFIRMED); registerCompensation if declared
//   onToolErrorCallback(ctx) -> completeEffect(FAILED | UNKNOWN)
//   afterRunCallback(ctx)    -> endRun(TERMINAL)
// }
// class TapeSessionService extends BaseSessionService {
//   appendEvent(session, event) -> super.appendEvent(...); then client.appendEvent(... ONE txn server-side)
// }

export const SCAFFOLD = true;
