# SDK Parity — Python · TypeScript · Go · Java

Tape ships in **two tiers**, both honoring the same logical schema and
invariants:

* **Scale tier** — Rust `tape-server` + gRPC clients in four languages.
  Use for Bigtable / Spanner, high write throughput, or sharing the
  journal across non-ADK clients.
* **Default tier** — `tape-adk`, a Python package that extends ADK's
  own `DatabaseSessionService` with the four primitives ADK doesn't
  have (`UNKNOWN` status, outbox dispatch, reconciler, compensation
  ledger). One database, one process, one `pip install`.

The **protocol is the contract** for the scale tier; the **logical schema
+ invariants** are the contract that connects both tiers. Any feature
reachable by RPC on the server is reachable from every SDK; any feature
reachable on `tape-adk` is reachable via direct SQL on the equivalent
embedded backend in the other languages.

> If you're looking at the per-language wiring table, the canonical one lives in
> [`tape/README.md`](tape/README.md#same-dx-in-go-typescript-and-java). This
> file extends it with **status, gaps, owners, and next steps**.

---

## TL;DR — scale tier (gRPC clients)

| | Python | TypeScript | Go | Java |
|---|:--:|:--:|:--:|:--:|
| gRPC client (every RPC) | ✅ | ✅ | ✅ | ✅ |
| `BeginEffect` / `CompleteEffect` short-circuit on confirmed | ✅ | ✅ | ✅ | ✅ |
| `@effect` / retry policy sugar | ✅ | ✅ | ✅ | ✅ |
| `durableApp(...)` one-call wiring | ✅ | ✅ | ✅ | ✅ |
| `outboxTool(...)` (non-idempotent dispatch) | ✅ | ✅ | ✅ | ✅ |
| `non_idempotent` safety enforcement at construction | ✅ | ✅ | ✅ | ✅ |
| Connectors: `log`, `http` | ✅ | ✅ | ✅ | ✅ |
| Connectors: `pubsub`, `cloudtasks` | ✅ | ✅ (lazy) | ✅ (build tags) | ✅ (reflective) |
| Reactions / event-bus subscribe | ✅ | ✅ | ✅ | ✅ |
| Tenancy config + DESIGN-ONLY warnings | ✅ | ✅ | ✅ | ✅ |
| Observability: structured logs + OTel span hook | ✅ | ✅ | ✅ | ✅ |
| ADK adapter (`TapePlugin`, `TapeSessionService`) | ✅ | — *(no ADK-TS)* | — *(no ADK-Go integration)* | ✅ `dev.tape.adk` |
| Built-in outbox-reactor runner | ✅ | ✅ `tape-outbox-ts` | ✅ `cmd/tape-outbox` | ✅ `dev.tape.cli.TapeOutbox` |
| Built-in sinks (`Log`, `Webhook`, `PubSub`) | ✅ | ✅ all three | ✅ all three (PubSub via `-tags pubsub`) | ✅ all three (PubSub reflective) |
| CLI (`tape init/dev/doctor/provision/deploy`) | ✅ | — *(call from Python CLI)* | — | — |
| Cross-SDK parity test harness | ✅ drives all four | ✅ green | ✅ green | ✅ green |
| End-user docs reference (auto-gen) | ✅ mkdocstrings | ✅ typedoc | ✅ gomarkdoc | ✅ javadoc |

## TL;DR — default tier (embedded SQL, no separate server)

| | Python (`tape-adk`) | TypeScript | Go | Java |
|---|:--:|:--:|:--:|:--:|
| Embedded `SessionService` that extends host framework's session store | ✅ extends `DatabaseSessionService` | 🟡 standalone (no ADK-TS) — scaffolded | 🟡 standalone (no ADK-Go integration yet) — scaffolded | 🟡 standalone — scaffolded |
| SQL schema (effects · obligations · timers · values) | ✅ four `Storage*` tables on ADK's `Base` | 🟡 schema only | 🟡 schema only | 🟡 schema only |
| 14 ledger methods (`begin_effect`, `complete_effect`, `claim_*`, etc.) | ✅ | 🟡 stubs | 🟡 stubs | 🟡 stubs |
| Row-level CAS for outbox / obligation claims | ✅ + asyncio.Lock on SQLite | 🟡 | 🟡 | 🟡 |
| Reactor library (4 async loops as plain functions) | ✅ | 🟡 | 🟡 | 🟡 |
| Connector protocol (dispatch / observe / compensate) | ✅ + `LogConnector` | 🟡 | 🟡 | 🟡 |
| ADK plugin (`NonIdempotentSafetyPlugin`) | ✅ + `@effect` / `@outbox_tool` | — *(no ADK-TS)* | — *(needs ADK-Go integration plan)* | — *(needs ADK-Java integration plan)* |
| Reactor CLI (`python -m tape_adk`) | ✅ `tape-adk-reactors` | 🟡 | 🟡 | 🟡 |
| E2E test against the host framework's real runner | ✅ ADK `Runner` + `ResumabilityConfig` | — | — | — |

✅ shipping · 🟡 scaffolded / contract-compatible · — n/a by design

The **scaffolded** rows mean: the SDK has the schema definition + a
service skeleton + the connector / reactor types so a user can read what
the contract looks like in their language, but the implementation hasn't
been driven through an end-to-end e2e test against a real host runtime
yet. Phase 1 of full default-tier parity is publishing these as
contract-compatible modules; Phase 2 is wiring each language's host
agent framework where one exists (ADK-Java and ADK-Go are separate
projects whose integration paths need their own design work).

---

## How to think about parity

The Python SDK is the **reference**. It pioneers the surface area, then the
other three follow. Parity does **not** mean every Python module exists in
TS/Go/Java verbatim — some surfaces (the ADK plugin, the Typer CLI, mkdocstrings
tooling) are framework-bound to Python. Parity means:

1. **Protocol parity:** every gRPC RPC is reachable. (✅ done.)
2. **Idiom parity:** the same primitive uses the language's natural shape
   — Python decorators, Go option structs, TS option objects, Java builders.
3. **Safety parity:** every safety invariant the Python SDK enforces locally
   (e.g. `non_idempotent` without a `business_key` ⇒ refuse to build the tool)
   is enforced in every other SDK *at construction time*, not just on the
   server.
4. **Operational parity:** the reactor runners, sinks, and connectors needed
   to actually *operate* a durable agent ship out of the box.

The wire protocol enforces the safety contract at `BeginEffect` time so even
an old SDK can't slip through — but a polite SDK refuses *before* the round
trip.

---

## Open gaps (the roadmap)

### ~~G1. Outbox-reactor runners in TS / Go / Java~~ ✅ Shipped
Python's `tape.reactors.outbox` is the reference. Each of TS/Go/Java now
ships a packaged daemon entrypoint with the same dispatch loop and the same
`non_idempotent` safety check:

- **TypeScript** — `tape/sdk/typescript/bin/tape-outbox-ts.ts` (also `npm run outbox`). New module: `src/outbox_reactor.ts` (`runOutboxDispatcher` / `outboxDispatchOnce` / `dispatchOne`).
- **Go** — `tape/sdk/go/cmd/tape-outbox/main.go`. New module: `outbox_dispatcher.go` (`RunOutboxDispatcher` / `OutboxDispatchOnce` / `DispatchOne`).
- **Java** — `dev.tape.cli.TapeOutbox` (runnable via `java -cp ... dev.tape.cli.TapeOutbox`, or `mvn exec:java`). New module: `dev.tape.reactors.OutboxReactor`.

Each accepts `--url`, `--connector`, `--interval`, `--max-attempts`,
`--claimer`, `--once`, plus a `--register-log-connector` flag for tests / demos.

**Acceptance:** see G3 below — the four-language harness is green.

---

### ~~G2. Sink parity (`Webhook`, `PubSub`) in TS / Go / Java~~ ✅ Shipped
Each of TS/Go/Java now ships `LogSink`, `WebhookSink`, and `PubSubSink`
matching the Python surface:

- **TypeScript** — `src/sinks.ts`: `LogSink`, `WebhookSink` (POST + retries + `X-Tape-Event-Id`), `PubSubSink` (lazy-imports `@google-cloud/pubsub`).
- **Go** — `sinks/sinks.go` + `sinks/pubsub.go` + `sinks/pubsub_real.go` (build-tag pattern matching the existing `connectors/pubsub`).
- **Java** — `dev.tape.sinks.{Sink,LogSink,WebhookSink,PubSubSink}` (PubSub via reflection so `google-cloud-pubsub` stays runtime-optional).

`WebhookSink` sets `X-Tape-Event-Id: <run_id>/<seq>` so receivers can dedup;
`PubSubSink` sets `orderingKey = run_id` and `tape-event-id` as a message
attribute. Combined with the at-least-once `runEventFanout` relays already in
each SDK, that's exactly-once-effective delivery.

---

### ~~G3. Cross-SDK parity test harness~~ ✅ Shipped
`tape/tests/parity/` drives **one scenario** through every language and
asserts identical journal state.

The scenario:
1. The Python harness creates a fresh run + decision + a PENDING+OUTBOX
   effect with `semantics=NON_IDEMPOTENT`, `connector="log"`.
2. The language under test runs **one pass** of its outbox dispatcher with
   `--register-log-connector --once`.
3. The Python harness polls the effect and asserts `status == CONFIRMED`.

Local: `make sdk-parity` runs all four. CI: `.github/workflows/sdk-tests.yml`
adds a `sdk-parity` job that runs after the per-SDK jobs. Test file:
`tape/tests/parity/test_outbox_parity.py`. Scenario builder:
`tape/tests/parity/scenario.py`.

**Status:** 4/4 green on the local matrix. Each language test cleanly
`skip`s when its toolchain is absent.

---

### ~~G4. Java ADK adapter~~ ✅ Shipped
`com.google.adk:google-adk:1.2.0` is on Maven Central, so the Java ADK is
reachable. The adapter lives in `dev.tape.adk`:

- **`TapePlugin`** (`extends BasePlugin`) — wires `beforeRunCallback` →
  `BeginRun`, `afterModelCallback` → `RecordDecision`,
  `beforeToolCallback` → `BeginEffect` (with the CONFIRMED short-circuit on
  re-drive), `afterToolCallback` → `CompleteEffect(CONFIRMED)`,
  `onToolErrorCallback` → `CompleteEffect(FAILED)`,
  `afterRunCallback` → `EndRun(TERMINAL)`.
- **`TapeSessionService`** (`implements BaseSessionService`) — routes
  `createSession` / `getSession` / `listSessions` / `deleteSession` /
  `listEvents` / `appendEvent` through Tape's gRPC contract. ADK's
  in-memory `super.appendEvent` runs first (state-delta application,
  `temp:` filter, last-update bookkeeping); committed events are then
  persisted to Tape in one round-trip.
- **`TapeAdkApp.wire(...)`** — the Java mirror of Python's
  `tape.adk.durable_app(...)` — returns a `{plugin, sessionService}` pair
  sharing one `TapeClient`.

`google-adk` is a `provided`-scope dependency, so non-ADK callers of
`TapeClient` are not forced to pull it in. Agents that use the adapter add
`com.google.adk:google-adk` themselves.

The smoke test (`dev.tape.adk.TapeAdkAdapterTest`) verifies the session
service round-trip against a real Rust `tape-server` + the ADK
`Session.builder` / `Event.fromJsonString` contract. Model replay
(short-circuit a recorded LlmResponse on re-drive) and budget admit/charge
are additive and tracked as follow-ups; they don't change the wiring
contract above.

**Acceptance:** `mvn test` is green (19 tests pass; 3 are the adapter
smoke). For a full agent-runner end-to-end, the recommended next step is to
add a `tape/examples/standalone/hello-durable-adk-java/` scaffold that
mirrors the Python `hello-durable-adk` example.

---

### ~~G5. SDK README structural consistency~~ ✅ Shipped
Each SDK README now opens with the same six-row reference table:

| Install | 30-second example | Reference | What's wired | Parity | Contribute |
|---|---|---|---|---|---|

A reader can scan all four READMEs in seconds and see the same shape. The
substantive prose beneath (which intentionally differs per-language to match
the idiom) stays as-is.

---

### G6. One-command per-SDK test target
**Status:** to run all SDK tests you have to know per-language invocations.

**Deliverable:** root `Makefile` targets:

```
make sdk-test-python   # pytest against the Rust server (in-memory)
make sdk-test-ts       # node --test
make sdk-test-go       # go test
make sdk-test-java     # mvn test
make sdk-test-all      # all four, parallel where independent
```

**Acceptance:** CI runs `make sdk-test-all` and the dev loop on a fresh
clone is `./setup.sh && make sdk-test-all`.

✅ **Shipped in this PR.** See `Makefile`.

---

### G7. CLI parity (deferred — see decision log)
The Typer CLI (`tape init/dev/doctor/provision/deploy`) stays Python-only by
design: it provisions cloud infrastructure and scaffolds projects, both
language-agnostic artifacts. A Go/TS/Java reimplementation would be the wrong
investment. The *agent process* can be in any of the four languages; the
*infra scaffold* is one tool, like `terraform` or `helm`.

---

## Done across the three PRs (devex + SDK parity)

| Surface | Was | Now |
|---|---|---|
| One-command setup | `pip install -e tape/sdk/python -e tape/cli` (after manual Rust + just install) | `./setup.sh` |
| Binary installer | none | `curl -fsSL .../install.sh \| sh` |
| Root Makefile | none | `make help` lists every target |
| `.mise.toml` | none | rust + python + node + go + java + just pinned |
| `.env.example` | none | every `TAPE_*` env var documented |
| Root `docker-compose.yml` | only under `tape/` | root → top-level `docker compose up` works |
| `CLAUDE.md` | none | onboarding doc for future Claude sessions |
| SDK parity matrix | scattered | this file |
| Per-SDK test target | per-language invocation | `make sdk-test-{python,ts,go,java,all}` |
| Release workflow | docs only | `.github/workflows/release.yml` for cross-platform `tape-server` binaries |
| **Outbox-reactor daemons (G1)** | Python only | All four — `tape-outbox-ts` · `cmd/tape-outbox` · `dev.tape.cli.TapeOutbox` |
| **Webhook/PubSub sinks (G2)** | Python only | All four (TS native fetch · Go `-tags pubsub` · Java reflective) |
| **Cross-SDK parity harness (G3)** | none | `tape/tests/parity/` — `make sdk-parity` runs all four; CI `sdk-parity` job in `.github/workflows/sdk-tests.yml` |
| **Java ADK adapter (G4)** | none | `dev.tape.adk.{TapePlugin,TapeSessionService,TapeAdkApp}` over `com.google.adk:google-adk:1.2.0` (provided scope) |
| **Per-SDK README parity (G5)** | drifted | uniform 6-column reference table at the top of all four SDK READMEs |

---

## How to contribute

Pick a gap (G1–G6). Each gap has a deliverable, an acceptance criterion, and
a target SDK. Open a PR with the language's normal layout under
`tape/sdk/<lang>/`. The parity matrix above is the source of truth — if you
add a primitive, update both the matrix and `tape/README.md`'s wiring table.
