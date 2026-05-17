# tape (Java)

The Java SDK for [Tape](../../../design-principles/tape.md) — a durable-execution
substrate for ADK agents.

```bash
cd tape/sdk/java
mvn test         # generates the gRPC stubs from src/main/proto/tape.proto via the
                 # protobuf-maven-plugin, builds, and runs the smoke test that
                 # spawns the Rust tape-server (in-memory) and round-trips the lifecycle
```

```java
import dev.tape.*;
import dev.tape.proto.*;

try (TapeClient c = new TapeClient("tape://localhost:7878")) {
  BeginRunResponse r = c.beginRun("treasury", "cfo", "2026-05-11", "inv-1", "me", 60_000);

  BeginEffectResponse be = c.beginEffect(r.getRunId(), 0, "execute_sweep", 0, "{}", "");
  if (be.getStatus() == EffectStatus.EFFECT_STATUS_CONFIRMED) {
    return; // already done on a prior attempt; be.getResponseJson() has the recorded result
  }
  // ... call bank.wire(..., idempotency_key=be.getIdempotencyKey()) ...
  c.completeEffect(r.getRunId(), be.getIdempotencyKey(),
      EffectStatus.EFFECT_STATUS_CONFIRMED, "{\"wire_id\":\"w1\"}", "");
}
```

`tapes://host` opens a TLS channel; pass a Google OIDC ID token (minted however
your app does — `google-auth-library-java`, GCP metadata server, etc.) via the
`Options.idToken(...)` builder so a Cloud Run IAM-protected endpoint accepts the
caller. The caller's service account needs `roles/run.invoker`.

### What's wired

A full `dev.tape.TapeClient` covering every RPC (run lifecycle, decisions,
effects with the dedup short-circuit, obligations, budget, gates, timers,
reconciliation, the WAL tail, sessions, **reactions / tasks / subject-routed
bus**) plus a JUnit 5 smoke test (`mvn test`) that round-trips the full
lifecycle against a real `tape-server`. gRPC stubs are generated at build time
via the `protobuf-maven-plugin` (proto + grpc-java).

### Reactions — the event-bus surface

See [`../../design-principles/tape-event-bus.md`](../../../design-principles/tape-event-bus.md)
for the model. The Java surface mirrors the Python `tape.reactions` module:

```java
import dev.tape.*;
import java.time.Duration;

// 1. Declare reactions at startup.
Reactions.onValueChange("treasury", "fx_rate",
    env -> {
        // env.payload  → parsed task.payload_json (Map<String,Object>)
        // env.subject(), env.sourceRunId(), env.attempts(), env.traceId() …
        System.out.println("fx_rate changed: " + env.payloadJson);
    },
    rd -> {
        rd.predicate = "double(payload.value.value_json) > 1.10";
        rd.maxConcurrency = 8;
        rd.debounceMs = 500;
    });

Reactions.onEffectConfirmed("execute_sweep",
    /* AGENT handlers have no body — the server creates the run */ null,
    rd -> { rd.agent = "treasury-followup"; rd.maxConcurrency = 4; });

// 2. Run the in-proc dispatcher: ClaimTasks → handler → CompleteTask / NackTask.
try (TapeClient c = new TapeClient("tape://localhost:7878")) {
    Reactions.runDispatcher(c, new Reactions.RunDispatcherOpts()
            .pollInterval(Duration.ofMillis(500)));
}

// Or: start the Pub/Sub bridge for PUBLISH-kind reactions. Soft-requires
// google-cloud-pubsub on the classpath; throws if it isn't there.
try (TapeClient c = new TapeClient("tape://localhost:7878")) {
    Reactions.runPubSubBridge(c, new Reactions.RunPubSubBridgeOpts()
            .project("my-proj").topic("tape-tasks"));
}
```

Per-reaction back-pressure (`maxConcurrency`, `rateLimitPerS`, `debounceMs`,
`retryMax`, `dlqAfterN`) is honoured by the in-proc dispatcher and enforced by
the server (retries / DLQ).

Lower-level: every new RPC is also on `TapeClient` directly —
`registerReaction(RegisterReactionOpts)`, `claimTasks(ClaimTasksOpts)`,
`completeTask`, `nackTask`, `listTasks`, `subscribeBySubject(pattern,
predicateCel, fromGlobalSeq)`. The Java SDK does **not** ship subject helpers
(`@on_value_change` etc.) as a separate package — they're static methods on
`Reactions`.

### What's a scaffold

A `TapePlugin` / `TapeSessionService` for the Java ADK port — mechanical work on
top of the wired client (the Python adapter in
[`../python/tape/adk/`](../python/tape/adk/) is the reference). And the higher-
level reactor helpers (`RecoverOnce` / `ReconcileOnce` / `FireDueTimersOnce` /
`RunReactors`) — pattern-port from the Go SDK in this folder.
