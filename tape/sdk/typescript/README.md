# tape-ts (scaffold)

The TypeScript SDK for [Tape](../../../design-principles/tape.md). **Status:
scaffold.** The contract is [`../../proto/tape.proto`](../../proto/tape.proto);
the gRPC client is generated from it; the ADK adapter (`TapePlugin`,
`TapeSessionService` for the ADK TypeScript port) mirrors the Python one in
[`../python/tape/adk/`](../python/tape/adk/) — that wiring is the `TODO` here.

```bash
# generate the client (requires @bufbuild/protoc-gen-es or grpc-tools):
./regen.sh
```

What ships when this is finished:
- `src/client.ts` — `TapeClient` over the generated stub (every RPC, idempotent retries).
- `src/adk/plugin.ts` — `TapePlugin`: `beforeRun` → `BeginRun`; `before/afterModel` → record/replay decisions; `before/afterTool` + `onToolError` → the effect ledger; `afterRun` → `EndRun`.
- `src/adk/session.ts` — `TapeSessionService` routing `appendEvent` through Tape.
- `src/recover.ts` — `recoverOnce({ runner })`.

Until then, generate the client and call the server directly — the protocol is
the stable surface.
