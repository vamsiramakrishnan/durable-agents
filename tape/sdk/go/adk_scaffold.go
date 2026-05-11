// tape-go — ADK adapter scaffold.
//
// Contract: ../../proto/tape.proto. Reference: the Python adapter in
// ../../python/tape/adk/. Finishing this is mechanical.
//
// TODO(tape-go): run regen.sh, then implement TapePlugin + TapeSessionService:
//
//   beforeRun        -> BeginRun;  reset per-invocation decision/tool counters
//   beforeModel      -> GetDecision(idx); if found, return the recorded LlmResponse
//   afterModel       -> RecordDecision(idx, response); ChargeBudget(tokens)
//   beforeTool       -> AdmitBudget; BeginEffect; if CONFIRMED, return the recorded result
//   afterTool        -> CompleteEffect(CONFIRMED); RegisterCompensation if declared
//   onToolError      -> CompleteEffect(FAILED | UNKNOWN)
//   afterRun         -> EndRun(TERMINAL)
//   appendEvent      -> the ADK event + state delta + the tape projection, one server-side txn
//
// plus tape.RecoverOnce(runner) — list ListRunsToRecover, re-invoke each with its invocation_id.

package tape

// Scaffold marker so `go build` has something to chew on.
const Scaffold = true
