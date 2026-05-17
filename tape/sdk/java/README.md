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
reconciliation, the WAL tail, sessions) plus a JUnit 5 smoke test (`mvn test`)
that round-trips the full lifecycle against a real `tape-server`. gRPC stubs are
generated at build time via the `protobuf-maven-plugin` (proto + grpc-java).

### What's a scaffold

A `TapePlugin` / `TapeSessionService` for the Java ADK port — mechanical work on
top of the wired client (the Python adapter in
[`../python/tape/adk/`](../python/tape/adk/) is the reference). And the higher-
level reactor helpers (`RecoverOnce` / `ReconcileOnce` / `FireDueTimersOnce` /
`RunReactors`) — pattern-port from the Go SDK in this folder.
