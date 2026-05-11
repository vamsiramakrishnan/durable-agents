package dev.tape;

// tape-java — ADK adapter scaffold.
//
// Contract: ../../../../../../proto/tape.proto. Reference: the Python adapter in
// ../../../../../../python/tape/adk/. Finishing this is mechanical.
//
// TODO(tape-java): generate the gRPC client, then implement TapePlugin +
// TapeSessionService against the ADK Java plugin / SessionService surfaces:
//
//   beforeRun    -> BeginRun;   reset per-invocation counters
//   beforeModel  -> GetDecision(idx); if found, return the recorded LlmResponse
//   afterModel   -> RecordDecision(idx, response); ChargeBudget(tokens)
//   beforeTool   -> AdmitBudget; BeginEffect; if CONFIRMED, return the recorded result
//   afterTool    -> CompleteEffect(CONFIRMED); RegisterCompensation if declared
//   onToolError  -> CompleteEffect(FAILED | UNKNOWN)
//   afterRun     -> EndRun(TERMINAL)
//   appendEvent  -> the ADK event + state delta + the tape projection, one server-side txn
public final class AdkScaffold {
    private AdkScaffold() {}
    public static final boolean SCAFFOLD = true;
}
