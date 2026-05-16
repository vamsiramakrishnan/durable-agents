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

### What's a scaffold

A `TapePlugin` / `TapeSessionService` for the Go port of ADK — mechanical work
once that port settles; the Python adapter is the reference.
