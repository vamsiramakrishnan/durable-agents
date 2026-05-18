# SDK Parity — Python · TypeScript · Go · Java

Tape ships four SDKs against one wire protocol (`tape/proto/tape.proto`). The
**protocol is the contract**: any feature reachable by RPC is reachable from
every language. This document is the live parity scorecard and the roadmap to
closing the remaining gaps.

> If you're looking at the per-language wiring table, the canonical one lives in
> [`tape/README.md`](tape/README.md#same-dx-in-go-typescript-and-java). This
> file extends it with **status, gaps, owners, and next steps**.

---

## TL;DR

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
| ADK adapter (`TapePlugin`, `TapeSessionService`) | ✅ | — *(no ADK)* | — *(no ADK)* | ⚠️ planned, see roadmap |
| Built-in outbox-reactor runner | ✅ | ✅ `tape-outbox-ts` | ✅ `cmd/tape-outbox` | ✅ `dev.tape.cli.TapeOutbox` |
| Built-in sinks (`Log`, `Webhook`, `PubSub`) | ✅ | ✅ all three | ✅ all three (PubSub via `-tags pubsub`) | ✅ all three (PubSub reflective) |
| CLI (`tape init/dev/doctor/provision/deploy`) | ✅ | — *(call from Python CLI)* | — | — |
| Cross-SDK parity test harness | ✅ drives all four | ✅ green | ✅ green | ✅ green |
| End-user docs reference (auto-gen) | ✅ mkdocstrings | ✅ typedoc | ✅ gomarkdoc | ✅ javadoc |

✅ shipping · ⚠️ partial · — n/a by design

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

### G4. Java ADK adapter
**Status:** Google has published a Java ADK
([google.github.io/adk-docs/java](https://google.github.io/adk-docs/)) since
the Python adapter shipped. Tape's TS/Go ADK adapters are out of scope (no
non-Python ADK at the time of writing for those languages), but **Java is
now reachable**.

**Deliverable:** `dev.tape.adk.TapePlugin` + `dev.tape.adk.TapeSessionService`
mirroring the Python surface; `DurableApp.wire(...)` already exists as the
non-ADK wiring entrypoint.

**Acceptance:** the treasury example runs end-to-end with a Java root agent
against the same Tape server.

---

### G5. SDK README structural consistency
**Status:** the four SDK READMEs have drifted in shape. Python has 51 lines,
Go has 237, TS has 181, Java has 176.

**Deliverable:** a shared README skeleton with the same six sections in the
same order:

1. **Install**
2. **30-second example**
3. **Wire it into an agent**
4. **Reference** (link to the auto-generated language docs)
5. **What's wired** (the per-feature checklist for that SDK)
6. **Contribute** (link to this file)

**Acceptance:** all four READMEs lint-pass a markdown structure check that
ships with `make docs`.

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

## Done across both PRs (devex + SDK parity)

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

---

## How to contribute

Pick a gap (G1–G6). Each gap has a deliverable, an acceptance criterion, and
a target SDK. Open a PR with the language's normal layout under
`tape/sdk/<lang>/`. The parity matrix above is the source of truth — if you
add a primitive, update both the matrix and `tape/README.md`'s wiring table.
