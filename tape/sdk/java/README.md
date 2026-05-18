# tape (Java)

The Java SDK for [Tape](../../../design-principles/tape.md) — a durable-execution
substrate for ADK agents.

|                                                            |
|------------------------------------------------------------|
| **Install** · Maven artifact `dev.tape:tape:0.1.0` *(or, from a clone: `cd tape/sdk/java && mvn install`)* |
| **30-second example** · the snippet below ↓                |
| **Reference** · <https://vamsiramakrishnan.github.io/durable-agents/reference/java/javadoc/> |
| **What's wired** · `TapeClient` + every RPC, `DurableApp.wire`, `OutboxTool`, connectors (Log/HTTP/PubSub/CloudTasks), **outbox dispatcher** (`dev.tape.cli.TapeOutbox`), **sinks** (Log/Webhook/PubSub), **ADK adapter** (`dev.tape.adk.TapePlugin` + `TapeSessionService`) |
| **Parity** · idiom parity with Python · see [`SDK_PARITY.md`](../../../SDK_PARITY.md) |
| **Contribute** · `make sdk-test-java` · `make sdk-parity` · [`CLAUDE.md`](../../../CLAUDE.md) |

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

### Standalone DX — parity with `tape-py`

The Java SDK ships the same standalone-DX surface as Python's
`tape.adk.durable_app` / `@tape.outbox_tool` / `tape.connectors` /
`tape.obs` / `tape.tenancy`.

#### `DurableApp` — the wiring entrypoint

```java
import dev.tape.DurableApp;
import dev.tape.TapeClient;

try (DurableApp app = DurableApp.wire(new DurableApp.Config()
        .name("treasury")
        .budget(new DurableApp.Budget(50.0, 2_000_000)))) {
    var run = app.client().beginRun(
        app.name(), "cfo", "s1", "inv-1", app.leaseOwner(), app.leaseTtlMs());
}
```

`DurableApp` honours `$TAPE_URL` and `$TAPE_LEASE_MS`. When the Java ADK
port lands, its `Runner` constructor will accept a `DurableApp` directly.

#### `OutboxTool` — non-idempotent upstreams, enforced

```java
import dev.tape.OutboxTool;
import java.util.Map;

OutboxTool wire = OutboxTool.builder("wire_money", "bank.wire")
    .semantics(OutboxTool.Semantics.NON_IDEMPOTENT)
    .businessKey(p -> p.get("account") + ":" + p.get("amount") + ":" + p.get("date"))
    .waitForResult(true)
    .build();
// `NON_IDEMPOTENT` without businessKey / statusCheck / compensate /
// humanGate throws OutboxConfigError at build() time.

Map<String, Object> env = wire.envelope(Map.of(
    "account", "ACME-1", "amount", 100_000,
    "beneficiary", "MMF-A", "date", "2026-05-17"));
assert OutboxTool.isEnvelope(env);
```

#### Capability connectors

```java
import dev.tape.connectors.*;

ConnectorRegistry.DEFAULT.register("bank.wire", new HttpConnector(new HttpConnector.Opts()
    .url("https://bank.example/wires")
    .observeUrl("https://bank.example/wires/lookup")
    .compensateUrl("https://bank.example/wires/reverse")));

// Built-ins: LogConnector (deps-free), HttpConnector (java.net.http),
// PubSubConnector (reflective; needs google-cloud-pubsub on classpath),
// CloudTasksConnector (reflective; needs google-cloud-tasks on classpath).
```

#### Observability + tenancy

```java
import dev.tape.Obs;
import dev.tape.Tenancy;

Obs.logJson("effect.dispatched", Map.of(
    "run_id", "r-1", "tool", "wire_money", "reactor", "outbox"));

Obs.setSpanHook((name, attrs) -> err -> { /* open + close span via your tracer */ });

Tenancy.Config t = new Tenancy.Config(Tenancy.Mode.HARD_MULTI_TENANT, "x");
t.warnIfHardButUnenforced().forEach(System.err::println);
```

### ADK adapter

`dev.tape.adk.TapePlugin` (extends `com.google.adk.plugins.BasePlugin`) and
`dev.tape.adk.TapeSessionService` (implements
`com.google.adk.sessions.BaseSessionService`) wire Tape into an ADK runner the
same way Python does:

```java
TapeClient client = new TapeClient("tape://localhost:7878");
Runner runner = Runner.builder(...)
    .plugins(List.of(new TapePlugin(client)))
    .sessionService(new TapeSessionService(client))
    .build();
```

Or, the bundled shortcut:

```java
try (TapeAdkApp app = TapeAdkApp.wire(new DurableApp.Config().name("treasury"))) {
  Runner runner = Runner.builder(...)
      .plugins(List.of(app.plugin()))
      .sessionService(app.sessionService())
      .build();
}
```

`google-adk` is a `provided`-scope dependency, so non-ADK callers of
`TapeClient` aren't forced to take it on; agents that use the adapter pull
`com.google.adk:google-adk` in themselves.

Higher-level reactor helpers (`RecoverOnce` / `ReconcileOnce` /
`FireDueTimersOnce` / `RunReactors`) are pattern-portable from the Go SDK in
this folder.

## Parity

The Python SDK is the reference; this SDK aims for **idiom parity** (not
verbatim parity). See [`../../../SDK_PARITY.md`](../../../SDK_PARITY.md) for
the live scorecard. G1 (outbox daemon), G2 (Webhook/PubSub sinks), G3
(cross-SDK parity harness), and G4 (ADK adapter) are all green — see the
"What's wired" row above for the concrete classes.

## Contribute

`make sdk-test-java` runs the round-trip smoke test; `make sdk-parity` runs
the cross-SDK parity harness (drives the same scenario through
Python/TS/Go/Java and asserts identical journal state). See
[`../../../CLAUDE.md`](../../../CLAUDE.md).
