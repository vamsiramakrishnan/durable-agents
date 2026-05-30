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
| ADK adapter (`TapePlugin`, `TapeSessionService`) | ✅ | — *(no ADK-TS)* | ✅ `adkplugin` (separate module) | ✅ `dev.tape.adk` |
| Built-in outbox-reactor runner | ✅ | ✅ `tape-outbox-ts` | ✅ `cmd/tape-outbox` | ✅ `dev.tape.cli.TapeOutbox` |
| Built-in sinks (`Log`, `Webhook`, `PubSub`) | ✅ | ✅ all three | ✅ all three (PubSub via `-tags pubsub`) | ✅ all three (PubSub reflective) |
| CLI (`tape init/dev/doctor/provision/deploy`) | ✅ | — *(call from Python CLI)* | — | — |
| Cross-SDK parity test harness | ✅ drives all four | ✅ green | ✅ green | ✅ green |
| End-user docs reference (auto-gen) | ✅ mkdocstrings | ✅ typedoc | ✅ gomarkdoc | ✅ javadoc |

## TL;DR — default tier (embedded SQL, no separate server)

| | Python (`tape-adk`) | TypeScript | Go | Java |
|---|:--:|:--:|:--:|:--:|
| Embedded session/effect store | ✅ extends ADK's `DatabaseSessionService` | ✅ standalone `src/embedded/` | ✅ standalone `embedded/` pkg | ✅ standalone `dev.tape.embedded` |
| SQL schema (effects · obligations · timers · values) | ✅ four `Storage*` tables | ✅ column-identical | ✅ column-identical | ✅ column-identical |
| Ledger methods (`begin_effect`, `complete_effect`, `claim_*`, …) | ✅ 19 | ✅ 19 | ✅ 19 | ✅ 19 |
| Row-level CAS for outbox / obligation claims | ✅ + asyncio.Lock on SQLite | ✅ + Mutex on SQLite | ✅ + sync.Mutex on SQLite | ✅ + ReentrantLock on SQLite |
| Reactor library (4 loops as plain functions) | ✅ | ✅ | ✅ | ✅ |
| Connector protocol (dispatch / observe / compensate) + `LogConnector` | ✅ | ✅ | ✅ | ✅ |
| Decorators / construction-time refusal (`@effect` / `@outbox_tool`) | ✅ | ✅ HOF wrappers | ✅ HOF + `ErrOutboxToolConfig` | ✅ builder + `IllegalArgumentException` |
| Embedded test suite (same invariants) | ✅ 29 | ✅ 30 | ✅ 28 (`-race`) | ✅ 32 |
| ADK plugin (`NonIdempotentSafetyPlugin`) | ✅ + e2e vs real `Runner` | — *(no ADK-TS)* | ✅ `adkplugin.NewTapePlugin` + e2e vs real `Runner` | ✅ + e2e vs real `Runner` |
| Reactor CLI (`python -m tape_adk`) | ✅ `tape-adk-reactors` | — *(call the funcs directly)* | — *(call the funcs directly)* | — *(call the funcs directly)* |
| E2E test against a host framework's real runner | ✅ ADK `Runner` + `ResumabilityConfig` | — *(no host framework)* | ✅ ADK-Go `runner.Runner` + scripted `model.LLM` | ✅ ADK-Java `Runner` + scripted `BaseLlm` |
| Embedded chaos (faults, invariants, scenarios) | ✅ `tape_adk.chaos` | ✅ `src/embedded/chaos.ts` | ✅ `embedded/chaos.go` | ✅ `dev.tape.embedded.chaos` |
| Compactor reactor (5th — TTL + NOT EXISTS pinning) | ✅ `compact_once` | ✅ `compactOnce` | ✅ `CompactOnce` | ✅ `Compactor.compactOnce` |
| `continue_as_new` (Temporal-pattern invocation reset) | ✅ | ✅ `continueAsNew` | ✅ `ContinueAsNew` | ✅ `continueAsNew` |
| Effect-ledger snapshot rows (compaction-safe short-circuit) | ✅ `take_snapshot` + fallback in `begin_effect` | ✅ `takeSnapshot` + fallback in `beginEffect` | ✅ `TakeSnapshot` + fallback in `BeginEffect` | ✅ `takeSnapshot` + fallback in `beginEffect` |

✅ shipping · 🟡 design pending · — n/a by design

**The embedded contract is at parity in all four languages.** The schema
is column-identical across languages — a Python writer and a Go / TS /
Java reader against the same SQLite file are mutually compatible. Every
language's embedded test suite proves the same invariants: idempotent
`begin_effect`, terminal-idempotent `complete_effect`, refusal of
NON_IDEMPOTENT+INLINE and OUTBOX-without-connector, `(connector,
business_key)` cross-run uniqueness, single-winner CAS under real
concurrency, expired-lease reclaim, the `next_dispatch_at_ms=0` → UNKNOWN
transition, `observe(CONFIRMED)` resolving UNKNOWN, atomic
DUPLICATE→compensation, ABSENT-on-NON_IDEMPOTENT staying UNKNOWN,
retries-then-STUCK, terminal-now forcing STUCK, timer claim semantics,
`WriteValue` CAS, and the full UNKNOWN→reconcile loop with
two-dispatchers-three-effects each-dispatched-once.

**The host-framework plugin row is now closed in three of four languages.**
Python's `NonIdempotentSafetyPlugin` extends ADK-Python's `BasePlugin`;
ADK-Java ships `dev.tape.adk`; ADK-Go ships `adkplugin.NewTapePlugin`,
which rides ADK-Go's `plugin.Plugin` tool callbacks
(`BeforeToolCallback` / `AfterToolCallback` / `OnToolErrorCallback` +
`BeforeRunCallback`) — each verified by an e2e test driving a real
`runner.Runner` with a scripted `model.LLM`. The `adkplugin` package is
a *separate Go module* so the heavy ADK-Go dependency stays optional:
embedded-only users (`go build ./embedded/`) never pull it. TS has no
ADK to plug into (the embedded module is standalone-by-design). The
embedded store + reactors + connectors are usable today by any
Go / Java / Node agent, ADK or not.

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

### G10. Effect scope enforcement (AIPlex integration PR 2) — partial

Python is the reference. The wire contract (`BeginEffectRequest.scope`,
`EffectRecord.scope`) is plumbed in every SDK; the **decoration-time
refusal** ("non_idempotent without scope is a config error") only ships
in Python. Other SDKs leave the safety check to the server's
`PermissionDenied` at `BeginEffect` time — defence-in-depth, but the
mistake surfaces at run time instead of code-review time.

| SDK        | Wire field on BeginEffect | Server defence-in-depth | Decoration-time refusal |
| ---------- | :-----------------------: | :---------------------: | :---------------------: |
| Python     |             ✅             |            ✅            |  ✅ `@tape.effect(scope=)` |
| Java       |             ✅             |            ✅            |  ✖ (planned)              |
| Go         |             ✅             |            ✅            |  ✖ (planned)              |
| TypeScript |             ✅             |            ✅            |  ✖ (planned)              |

**To close:** mirror the Python `_validate_semantics` rule into the
Java `Effect`/`OutboxTool` builders, the Go `effect.New` / `outbox.New`
constructors, and the TS `effect()` factory. Each refuses
`semantics=NON_IDEMPOTENT` without a `scope` at construction unless
`allowUnsafe=true`.

---

### G8. Run identity (AIPlex integration PR 1) — partial
`BeginRunRequest` and `RunState` gained seven identity fields (`tenant_id`,
`actor`, `subject`, `agent_id`, `aiplex_instance_id`, `gateway_route`,
`scopes`, `labels`). Python is the reference; the other three SDKs have the
call-surface plumbed but the developer-ergonomics layer varies.

| SDK        | Wire fields exposed | Env-derived `RunIdentity` helper |
| ---------- | :-----------------: | :------------------------------: |
| Python     |          ✅          |  ✅ `tape.adk.identity.RunIdentity.from_env()`  |
| Java       |          ✅          |  ✅ `dev.tape.RunIdentity.fromEnv()`            |
| Go         |          ✅          |  ✖ (callers thread fields into `BeginRunOpts` manually) |
| TypeScript |          ✅          |  ✖ (dynamic `beginRun({...})` call surface)     |

**To close:** add a `RunIdentity` helper to Go (`tape/sdk/go/identity.go`)
and TS (`tape/sdk/typescript/src/identity.ts`) mirroring the Python /
Java surfaces. Tracked alongside AIPlex integration PR 1.

---

### G9. Run identity in the `tape-adk` embedded tier
The scale-tier wire protocol (`BeginRunRequest` / `RunState`) carries
identity (G8). The default-tier `tape-adk` package — which extends
ADK's `DatabaseSessionService` directly without going through the gRPC
server — does not yet. Its embedded SQL schema and `begin_*` paths
have no `tenant_id` / `actor` / `subject` / `agent_id` columns, so
runs that live in the embedded tier cannot be queried by AIPlex along
those axes.

| Surface | Scale tier (gRPC) | Default tier (`tape-adk`) |
| --- | :---: | :---: |
| Identity on `BeginRunRequest` / `begin_run`         | ✅ | ✖ |
| Identity columns on the run/effect SQL schema       | ✅ | ✖ |
| Indexed by `(tenant_id, agent_id)` for run timeline | ✅ | ✖ |

**Not blocking for AIPlex integration:** AIPlex targets the scale tier
(Tape server + gRPC), so PRs 4–10 land against the gRPC contract that
already has identity. This gap only matters if someone wants to use
the embedded tier behind AIPlex.

**To close:** mirror the scale-tier columns onto `tape_adk`'s
embedded SQL schema, thread identity through `begin_effect` and the
session writes, and add a `from_env()`-equivalent on the Python /
TS / Go / Java embedded clients. Open as a follow-up PR after PR 1
merges.

---

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

### ~~G8. Compaction primitives — the 5-mechanism set~~ ✅ Shipped

A long-running agent accumulates terminal rows. Without a forgetting
mechanism the journal grows without bound; without the *right*
forgetting mechanism, the idempotency-key short-circuit breaks and the
agent re-dispatches confirmed effects. Five interlocking primitives,
each shipped across all four embedded SDKs:

1. **Terminal-state effect pruning** — `compact_once` deletes
   CONFIRMED/FAILED effects older than `effect_ttl_ms`. One composite
   SQL `DELETE`, not an application-level loop.
2. **Session-level archival** — when the latest tape row is older
   than `session_ttl_ms` AND there are no active obligations or
   unfired timers, the whole session's tape rows go in one shot.
3. **Effect-ledger snapshot rows** — `take_snapshot` captures
   terminal effects into a cumulative per-session JSON blob.
   `begin_effect` falls back to the snapshot when the live row is
   gone, so the compactor is free to prune underlying rows without
   breaking the idempotency contract.
4. **`continue_as_new`** — atomically prune one invocation's
   terminal+unpinned effects and write `carried_state` to the
   `tape:continue-as-new:<session>` namespace. The next invocation
   reads it on startup. Temporal's pattern, embedded-tier shape.
5. **Compensable-window pinning** — encoded as a `NOT EXISTS`
   subquery in the compactor's WHERE clause: an effect with an
   ACTIVE obligation (PENDING or COMMITTED) referencing it is
   NEVER pruned, regardless of TTL. The safety invariant lives in
   the SQL predicate, not in language-level pre-checks.

| | Python | TypeScript | Go | Java |
|---|:--:|:--:|:--:|:--:|
| Compactor reactor (`compact_once` + `CompactionPolicy`) | ✅ | ✅ | ✅ | ✅ |
| Session archival | ✅ | ✅ | ✅ | ✅ |
| Snapshot rows + `begin_effect` fallback | ✅ | ✅ | ✅ | ✅ |
| `continue_as_new` atomic prune + state-carry | ✅ | ✅ | ✅ | ✅ |
| `NOT EXISTS` pinning (verbatim across SDKs) | ✅ | ✅ | ✅ | ✅ |
| Test count (chaos + compact + continue_as_new + snapshot) | ✅ 38 | ✅ 38 | ✅ 38 | ✅ 40 |

The SQL pinning predicate is reproduced byte-for-byte across all four
SDKs so the safety contract is *the same DELETE* on every backend.
Cross-SDK SQLite-file compatibility extends to the snapshot table
(`tape_effect_snapshots`) — a Python writer and a Go/TS/Java reader
against the same file see the same short-circuit data.

Reference commits on `claude/tape-inspector-6HE7j`:
- Python (reference): `95ac335` chaos · `deaf050` compactor · `56b8951` continue_as_new · `def3097` snapshot
- TS: `20823ff` (chaos/compact/can) · `2570b52` snapshot
- Go: `39df32e` (chaos/compact/can) · `de813f5` snapshot
- Java: `788641b` (chaos/compact/can) · `b31de54` snapshot

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
| **Compaction primitives (G8)** | none | All five (`compact_once`, snapshot, `continue_as_new`, session archival, NOT EXISTS pinning) shipped in all four embedded SDKs; SQL pinning predicate reproduced byte-for-byte |

---

## How to contribute

Pick a gap (G1–G6). Each gap has a deliverable, an acceptance criterion, and
a target SDK. Open a PR with the language's normal layout under
`tape/sdk/<lang>/`. The parity matrix above is the source of truth — if you
add a primitive, update both the matrix and `tape/README.md`'s wiring table.
