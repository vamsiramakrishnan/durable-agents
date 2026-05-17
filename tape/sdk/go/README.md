# tape (Go)

The Go SDK for [Tape](../../../design-principles/tape.md) — a durable-execution
substrate for ADK agents.

```bash
cd tape/sdk/go
PATH=/tmp/gobin:$PATH ./regen.sh     # regenerate ./tapepb from ./tape.proto (needs protoc + protoc-gen-go + protoc-gen-go-grpc)
go test -timeout 60s ./...           # spawns the Rust tape-server (in-memory) and round-trips the lifecycle
```

```go
import (
    "context"
    "time"

    tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
    pb "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

func main() {
    ctx := context.Background()
    c, _ := tape.Dial("tape://localhost:7878")
    defer c.Close()

    r, _ := c.BeginRun(ctx, tape.BeginRunOpts{
        AppName: "treasury", UserID: "cfo", SessionID: "2026-05-11",
        InvocationID: "inv-1", LeaseOwner: "me",
    })

    be, _ := c.BeginEffect(ctx, tape.BeginEffectOpts{
        RunID: r.RunId, DecisionIndex: 0, ToolName: "execute_sweep",
        CallIndex: 0, RequestJSON: "{}",
    })
    if int32(be.Status) == tape.EffectStatusConfirmed {
        return // already done on a prior attempt; the recorded result is in be.ResponseJson
    }
    // ...call bank.wire(..., idempotency_key=be.IdempotencyKey)...
    _, _ = c.CompleteEffect(ctx, r.RunId, be.IdempotencyKey,
        tape.EffectStatusConfirmed, `{"wire_id":"w1"}`, "")
}
```

`tapes://host` opens a TLS channel; an automatic Google ID-token interceptor for
Cloud Run IAM is sketched in `auth.go` (commented two-liner using
`google.golang.org/api/idtoken` — uncomment and add it to `go.mod` to enable, or
pass a static `Options.IDToken`).

### What's wired

A full `tape.Client` covering every RPC (run lifecycle, decisions, effects with
the dedup short-circuit, obligations, budget, gates, timers, reconciliation, the
WAL tail, sessions), reactor helpers (`RecoverOnce`, `ReconcileOnce`,
`FireDueTimersOnce`, `RunReactors`, `RunEventFanout`), compensator/status-check
registries, and a smoke test (`go test`) that round-trips the full lifecycle
against a real `tape-server`.

### Reactions (the event bus)

The Go SDK ships the same event-bus surface as the Python SDK: declare
reactions, push them to the server, then run an in-process dispatcher (or the
Pub/Sub bridge for fan-out to a broker). See
[`design-principles/tape-event-bus.md`](../../../design-principles/tape-event-bus.md)
for the wire contract.

```go
import (
    "context"

    tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
)

func main() {
    ctx := context.Background()

    // ── declare reactions at startup ──────────────────────────────────────
    // Re-price the book whenever an FX rate above $1.10 lands. The handler
    // runs in this process (TASK-kind reaction).
    tape.OnValueChange("treasury", "fx_rate",
        func(ctx context.Context, env *tape.Envelope) error {
            // env.Task is the *pb.Task; env.Payload is the parsed payload_json.
            return reprice(ctx, env.Payload)
        },
        tape.ReactionDef{
            Predicate:         `double(payload.value.value_json) > 1.10`,
            MaxConcurrency:    8,
            DebounceMs:        500,
            BootstrapFromHead: true, // skip backlog on first start
        },
    )

    // Fan failed-effect events out to Pub/Sub (PUBLISH-kind reaction; the
    // dispatcher is RunPubSubBridge, compiled in with `-tags pubsub`).
    tape.OnEffectFailed("*",
        nil,
        tape.ReactionDef{Publish: "pubsub://my-proj/incident-stream"},
    )

    c, _ := tape.Dial("tape://localhost:7878")
    defer c.Close()

    // ── push declarations to the server, then start dispatching ──────────
    _, _ = tape.RegisterAll(ctx, c, "")
    _ = tape.RunDispatcher(ctx, c, tape.RunDispatcherOpts{})
}
```

The dispatcher honours each reaction's `MaxConcurrency` (per-reaction
semaphore), `RateLimitPerS` (`golang.org/x/time/rate` token bucket), and
`DebounceMs` (per-subject coalescing — a debounced trigger is completed as a
no-op, never DLQ'd). Retry/DLQ semantics are enforced by the server:
`RunDispatcher` only escalates to `NackTask(permanent=true)` once
`attempts >= DLQAfterN`.

`BootstrapFromHead: true` seeds the per-shard cursors at the current journal
head, so the reaction sees only entries written AFTER registration — set it
for alert-on-new-events reactions; leave it false for replay-style work
(audit projections, cold-start rebuilds).

OTel propagation is opt-in: call `tape.SetOTelHooks(...)` to install a
hook that opens a child span from the task's `(trace_id, parent_span_id)`.

For raw access to the new RPCs without the registry, the `Client` exposes
`RegisterReaction`, `DeregisterReaction`, `ListReactions`, `ClaimTasks`,
`CompleteTask`, `NackTask`, `ListTasks`, and `SubscribeBySubject`.

`RunPubSubBridge` is a build-tagged optional add-on. The default build returns
`ErrPubSubNotBuilt` so the SDK stays light. To enable, add the Pub/Sub
dependency to your module and build with the `pubsub` tag:

```bash
go get cloud.google.com/go/pubsub
go build -tags pubsub ./...
```

### What's a scaffold

A `TapePlugin` / `TapeSessionService` for the Go port of ADK — mechanical work
once that port settles; the Python adapter is the reference.
