# Go — `tape-go`

```bash
go get github.com/vamsiramakrishnan/durable-agents/tape/sdk/go
```

```go
import (
    "context"
    tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
    "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/connectors"
)

func main() {
    ctx := context.Background()
    d, _ := tape.NewDurableApp(ctx, tape.DurableConfig{
        Name:   "treasury",
        Budget: tape.Budget{USDCap: 50, TokenCap: 2_000_000},
    })
    defer d.Close()
    // ... use d.Client ...
}
```

The full package reference is generated at build time from godoc via
[`gomarkdoc`](https://github.com/princjef/gomarkdoc):

- [**Package reference**](api.md)
- [Pub/Sub connector](https://pkg.go.dev/cloud.google.com/go/pubsub) is gated behind the `pubsub`
  build tag; Cloud Tasks behind `cloudtasks`. See [Connectors guide](../../non-idempotent-upstreams.md#connectors).
