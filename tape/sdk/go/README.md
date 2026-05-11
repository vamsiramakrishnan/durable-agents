# tape-go (scaffold)

The Go SDK for [Tape](../../../design-principles/tape.md). **Status: scaffold.**
The contract is [`../../proto/tape.proto`](../../proto/tape.proto); the Python
adapter in [`../python/tape/adk/`](../python/tape/adk/) is the reference for the
`TapePlugin` / `TapeSessionService` wiring (the `TODO` here).

```bash
./regen.sh        # protoc-gen-go + protoc-gen-go-grpc -> ./tapepb
```

What ships when finished: `client.go` (`TapeClient` over the generated stub),
`adk/plugin.go` (`TapePlugin` implementing the ADK-Go plugin interface), and
`adk/session.go` (`TapeSessionService`). The protocol is the stable surface
until then.
